package orchestr

import (
	"context"
	"fmt"
	"log"
	"net/netip"

	"github.com/moby/moby/api/types/network"
	"github.com/moby/moby/client"
)

// syncContainerDataFromDocker queries Docker engine (source of truth) for all containers
// on the specified network, extracts their IP addresses, and syncs to Redis.
// Also handles the case where network was deleted and containers need to be reconnected.
func (o *Orchestrator) syncContainerDataFromDocker(ctx context.Context, networkID string) error {
	if o.redisClient == nil {
		return fmt.Errorf("redis client not available")
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
					// Container is stopped, skip it
					log.Printf("Container %s is stopped and not on network, skipping", containerID[:12])
					continue
				}
			} else {
				// Container not on our network and not in Redis, skip it
				continue
			}
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
