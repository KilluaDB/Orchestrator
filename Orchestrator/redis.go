package Orchestrator

import (
	"context"
	"fmt"
	"log"
	"net/netip"
	"time"

	"github.com/moby/moby/api/types/network"
	"github.com/moby/moby/client"
)

// syncContainerDataFromDocker queries Docker engine (source of truth) for all containers
// on the specified network, extracts their IP addresses, and syncs to Redis.
// Also handles the case where network was deleted and containers need to be reconnected.
func (o *Orchestrator) syncContainerDataFromDocker(ctx context.Context, networkID string) error {
	if err := o.checkRedisConnection(ctx); err != nil {
		return fmt.Errorf("redis client unavailable: %w", err)
	}

	fmt.Println("Syncing container data from Docker (source of truth) to Redis...")

	// Get all containers from Docker
	containers, err := o.dockerClient.ContainerList(ctx, client.ContainerListOptions{
		All: true, // Include stopped containers
	})
	if err != nil {
		return fmt.Errorf("failed to list containers: %w", err)
	}

	o.assignmentsMu.Lock()
	defer o.assignmentsMu.Unlock()

	syncedCount := 0
	skippedCount := 0
	reconnectedCount := 0

	// Get all existing keys from Redis to track what should be removed or reconnected
	redisKeys, _ := o.redisClient.Keys(ctx, o.config.RedisKeyPrefix+"*").Result()
	redisContainerMap := make(map[string]bool)
	for _, key := range redisKeys {
		containerID := key[len(o.config.RedisKeyPrefix):]
		redisContainerMap[containerID] = true
	}

	// Process each container from Docker
	for _, container := range containers.Items {
		containerID := container.ID

		// Inspect container to get network details
		inspect, err := o.dockerClient.ContainerInspect(ctx, containerID, client.ContainerInspectOptions{})
		if err != nil {
			log.Printf("Warning: Failed to inspect container %s: %v", containerID[:12], err)
			skippedCount++
			continue
		}

		// Check if container is on our network
		networkSettings, exists := inspect.Container.NetworkSettings.Networks[o.config.NetworkName]
		if !exists || !networkSettings.IPAddress.IsValid() {
			// Container not on our network - check if it's in Redis (was disconnected)
			if redisContainerMap[containerID] {
				// Container exists in Redis but is not on the network
				// This likely means the network was deleted and recreated
				// Try to reconnect if container is running
				if inspect.Container.State.Running {
					log.Printf("Container %s is running but disconnected from network %s. Attempting to reconnect...", containerID[:12], o.config.NetworkName)

					// Get the IP that was stored in Redis (if any)
					oldIP, _ := o.redisClient.Get(ctx, o.config.RedisKeyPrefix+containerID).Result()

					// Reconnect container to network
					endpointConfig := &network.EndpointSettings{
						NetworkID: networkID,
					}
					if oldIP != "" {
						// Try to use the old IP if possible
						if ip, err := netip.ParseAddr(oldIP); err == nil {
							endpointConfig.IPAddress = ip
						}
					}

					if _, err := o.dockerClient.NetworkConnect(ctx, networkID, client.NetworkConnectOptions{
						Container:      containerID,
						EndpointConfig: endpointConfig,
					}); err != nil {
						log.Printf("Warning: Failed to reconnect container %s to network: %v", containerID[:12], err)
						skippedCount++
						continue
					}

					// Re-inspect to get the new IP
					inspect, err = o.dockerClient.ContainerInspect(ctx, containerID, client.ContainerInspectOptions{})
					if err != nil {
						log.Printf("Warning: Failed to re-inspect container %s after reconnect: %v", containerID[:12], err)
						skippedCount++
						continue
					}

					networkSettings, exists = inspect.Container.NetworkSettings.Networks[o.config.NetworkName]
					if !exists || !networkSettings.IPAddress.IsValid() {
						log.Printf("Warning: Container %s reconnected but still no valid IP", containerID[:12])
						skippedCount++
						continue
					}

					reconnectedCount++
					log.Printf("Successfully reconnected container %s to network %s", containerID[:12], o.config.NetworkName)
				} else {
					// Container is stopped, try to reconnect to network and restart it
					log.Printf("Container %s is stopped and not on network. Attempting to reconnect and restart...", containerID[:12])

					// Get the IP that was stored in Redis (if any)
					oldIP, _ := o.redisClient.Get(ctx, o.config.RedisKeyPrefix+containerID).Result()

					// Reconnect container to network
					endpointConfig := &network.EndpointSettings{
						NetworkID: networkID,
					}
					if oldIP != "" {
						// Try to use the old IP if possible
						if ip, err := netip.ParseAddr(oldIP); err == nil {
							endpointConfig.IPAddress = ip
						}
					}

					if _, err := o.dockerClient.NetworkConnect(ctx, networkID, client.NetworkConnectOptions{
						Container:      containerID,
						EndpointConfig: endpointConfig,
					}); err != nil {
						log.Printf("Warning: Failed to reconnect container %s to network: %v", containerID[:12], err)
						skippedCount++
						continue
					}

					// Restart the container
					if err := o.restartStoppedContainer(ctx, containerID, inspect); err != nil {
						log.Printf("Warning: Failed to restart container %s: %v", containerID[:12], err)
						skippedCount++
						continue
					}

					// Re-inspect to get the new IP
					inspect, err = o.dockerClient.ContainerInspect(ctx, containerID, client.ContainerInspectOptions{})
					if err != nil {
						log.Printf("Warning: Failed to re-inspect container %s after restart: %v", containerID[:12], err)
						skippedCount++
						continue
					}

					networkSettings, exists = inspect.Container.NetworkSettings.Networks[o.config.NetworkName]
					if !exists || !networkSettings.IPAddress.IsValid() {
						log.Printf("Warning: Container %s restarted but still no valid IP", containerID[:12])
						skippedCount++
						continue
					}

					reconnectedCount++
					log.Printf("Successfully reconnected and restarted container %s", containerID[:12])
				}
			} else {
				// Container not on our network and not in Redis, skip it
				continue
			}
		}

		// Check if container is stopped but on our network - restart it
		if !inspect.Container.State.Running && exists && networkSettings.IPAddress.IsValid() {
			log.Printf("Container %s is stopped but on network. Attempting to restart...", containerID[:12])
			if err := o.restartStoppedContainer(ctx, containerID, inspect); err != nil {
				log.Printf("Warning: Failed to restart container %s: %v", containerID[:12], err)
				skippedCount++
				continue
			}
			log.Printf("Successfully restarted container %s", containerID[:12])
		}

		ip := networkSettings.IPAddress.String()

		// Store in memory
		o.assignments[containerID] = ip

		// Store/update in Redis
		key := o.config.RedisKeyPrefix + containerID
		if err := o.redisClient.Set(ctx, key, ip, 0).Err(); err != nil {
			log.Printf("Warning: Failed to store container %s -> %s in Redis: %v", containerID[:12], ip, err)
		} else {
			syncedCount++
		}

		// Mark as processed (remove from map)
		delete(redisContainerMap, containerID)
	}

	// Remove containers from Redis that no longer exist in Docker
	removedCount := 0
	for containerID := range redisContainerMap {
		key := o.config.RedisKeyPrefix + containerID
		if err := o.redisClient.Del(ctx, key).Err(); err != nil {
			log.Printf("Warning: Failed to remove stale container %s from Redis: %v", containerID[:12], err)
		} else {
			removedCount++
		}
		// Also remove from in-memory map if present
		delete(o.assignments, containerID)
	}

	fmt.Printf("Synced %d container IP assignments from Docker to Redis", syncedCount)
	if reconnectedCount > 0 {
		fmt.Printf(", reconnected %d containers to network", reconnectedCount)
	}
	if skippedCount > 0 {
		fmt.Printf(", skipped %d containers", skippedCount)
	}
	if removedCount > 0 {
		fmt.Printf(", removed %d stale entries from Redis", removedCount)
	}
	fmt.Println()
	return nil
}

// extractResourceLimitsFromInspect extracts resource limits from container inspection
func (o *Orchestrator) extractResourceLimitsFromInspect(inspect client.ContainerInspectResult) ResourceLimits {
	limits := ResourceLimits{}

	if inspect.Container.HostConfig != nil {
		resources := inspect.Container.HostConfig.Resources
		limits.Memory = resources.Memory
		limits.MemorySwap = resources.MemorySwap
		limits.CPUQuota = resources.CPUQuota
		limits.CPUShares = resources.CPUShares
	}

	// Set default values if not specified
	if limits.Memory == 0 {
		limits.Memory = 512 * 1024 * 1024 // 512MB default
	}
	if limits.MemorySwap == 0 {
		limits.MemorySwap = limits.Memory * 2
	}
	if limits.CPUQuota == 0 {
		limits.CPUQuota = 100000 // 1 CPU default
	}
	if limits.CPUShares == 0 {
		limits.CPUShares = 1024 // Default CPU shares
	}

	// Set default storage and network limits if not available
	if limits.StorageSize == 0 {
		limits.StorageSize = 10 * 1024 * 1024 * 1024 // 10GB default
	}
	if limits.NetworkRx == 0 {
		limits.NetworkRx = 100 * 1024 * 1024 // 100MB/s default
	}
	if limits.NetworkTx == 0 {
		limits.NetworkTx = 100 * 1024 * 1024 // 100MB/s default
	}

	return limits
}

// restartStoppedContainer restarts a stopped container and starts resource monitoring
func (o *Orchestrator) restartStoppedContainer(ctx context.Context, containerID string, inspect client.ContainerInspectResult) error {
	// Start the container
	if _, err := o.dockerClient.ContainerStart(ctx, containerID, client.ContainerStartOptions{}); err != nil {
		return fmt.Errorf("container start: %w", err)
	}

	// Extract resource limits from container inspection
	limits := o.extractResourceLimitsFromInspect(inspect)

	// Get container name
	containerName := inspect.Container.Name
	if containerName == "" {
		containerName = containerID[:12]
	}

	// Start resource monitoring in background
	monitorInterval := time.Duration(o.config.MonitorInterval) * time.Second
	go o.monitorContainerResources(ctx, containerID, limits, containerName, monitorInterval)

	log.Printf("Container %s restarted and monitoring started", containerID[:12])
	return nil
}

// restartStoppedContainersOnNetwork restarts all stopped containers on the orchestrator's network
// This function works even without Redis
func (o *Orchestrator) restartStoppedContainersOnNetwork(ctx context.Context, networkID string) error {
	fmt.Println("Checking for stopped containers on network to restart...")

	// Get all containers from Docker
	containers, err := o.dockerClient.ContainerList(ctx, client.ContainerListOptions{
		All: true, // Include stopped containers
	})
	if err != nil {
		return fmt.Errorf("failed to list containers: %w", err)
	}

	restartedCount := 0
	skippedCount := 0

	o.assignmentsMu.Lock()
	defer o.assignmentsMu.Unlock()

	// Process each container
	for _, container := range containers.Items {
		containerID := container.ID

		// Inspect container to get network details
		inspect, err := o.dockerClient.ContainerInspect(ctx, containerID, client.ContainerInspectOptions{})
		if err != nil {
			log.Printf("Warning: Failed to inspect container %s: %v", containerID[:12], err)
			skippedCount++
			continue
		}

		// Check if container is on our network
		networkSettings, exists := inspect.Container.NetworkSettings.Networks[o.config.NetworkName]
		if !exists || !networkSettings.IPAddress.IsValid() {
			continue // Not on our network, skip
		}

		// Check if container is stopped
		if !inspect.Container.State.Running {
			log.Printf("Found stopped container %s on network. Attempting to restart...", containerID[:12])
			if err := o.restartStoppedContainer(ctx, containerID, inspect); err != nil {
				log.Printf("Warning: Failed to restart container %s: %v", containerID[:12], err)
				skippedCount++
				continue
			}

			// Store IP in memory
			ip := networkSettings.IPAddress.String()
			o.assignments[containerID] = ip
			restartedCount++
			log.Printf("Successfully restarted container %s", containerID[:12])
		} else {
			// Container is running, just store the IP in memory
			ip := networkSettings.IPAddress.String()
			o.assignments[containerID] = ip
		}
	}

	if restartedCount > 0 {
		fmt.Printf("Restarted %d stopped containers on network", restartedCount)
		if skippedCount > 0 {
			fmt.Printf(", skipped %d containers", skippedCount)
		}
		fmt.Println()
	} else if skippedCount > 0 {
		fmt.Printf("Skipped %d containers\n", skippedCount)
	}

	return nil
}
