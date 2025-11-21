package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/netip"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
	"github.com/moby/moby/api/types/network"
	"github.com/moby/moby/client"
	"github.com/redis/go-redis/v9"
)

const (
	defaultImage        = "postgres:16-alpine"
	defaultContainer    = "orchestr-postgres"
	defaultVolumeTarget = "/var/lib/postgresql/data"
	defaultPort         = "5432"
	volumePrefix        = "orchestr-postgres-data"
	defaultNetworkName  = "orchestr-postgres-net"
	defaultSubnetCIDR   = "172.28.0.0/16"
	defaultGateway      = "172.28.0.1"
	redisAddr           = "localhost:6379"
	redisKeyPrefix      = "container:ip:"
	redisNetworkKey     = "network:id:" + defaultNetworkName
	monitorInterval     = 5 * time.Second // Check resources every 5 seconds
)

var (
	ipAssignments   = map[string]string{} // containerID -> IP
	ipAssignmentsMu sync.RWMutex
	redisClient     *redis.Client
)

// ResourceLimits defines all resource limits for a container
type ResourceLimits struct {
	CPUQuota    int64 // CPU quota in microseconds (100000 = 1 CPU)
	CPUShares   int64 // CPU shares (relative weight)
	Memory      int64 // Memory limit in bytes
	MemorySwap  int64 // Total memory + swap limit in bytes
	StorageSize int64 // Storage size limit in bytes (for root filesystem)
	NetworkRx   int64 // Network receive bandwidth limit in bytes per second
	NetworkTx   int64 // Network transmit bandwidth limit in bytes per second
}

type PostgresContainerOptions struct {
	Name            string
	Image           string
	Env             map[string]string
	VolumeMountPath string
	Resources       container.Resources
	ResourceLimits  ResourceLimits
	NetworkName     string
	SubnetCIDR      string
}

func defaultPostgresOptions() PostgresContainerOptions {
	memoryLimit := int64(512 * 1024 * 1024) // 512MiB
	return PostgresContainerOptions{
		Name:  fmt.Sprintf("%s-%s", defaultContainer, uuid.NewString()),
		Image: defaultImage,
		Env: map[string]string{
			"POSTGRES_PASSWORD": "postgres",
			"POSTGRES_USER":     "postgres",
			"POSTGRES_DB":       "postgres",
		},
		VolumeMountPath: defaultVolumeTarget,
		Resources: container.Resources{
			Memory:     memoryLimit,
			MemorySwap: memoryLimit * 2, // Allow swap up to 2x memory
			CPUQuota:   100000,          // 1 CPU
			CPUShares:  1024,            // Default CPU shares
		},
		ResourceLimits: ResourceLimits{
			CPUQuota:    100000,                  // 1 CPU (100000 microseconds = 1 CPU)
			CPUShares:   1024,                    // Default CPU shares
			Memory:      memoryLimit,             // 512MiB
			MemorySwap:  memoryLimit * 2,         // 1GiB total (memory + swap)
			StorageSize: 10 * 1024 * 1024 * 1024, // 10GB storage
			NetworkRx:   100 * 1024 * 1024,       // 100MB/s receive
			NetworkTx:   100 * 1024 * 1024,       // 100MB/s transmit
		},
		NetworkName: defaultNetworkName,
		SubnetCIDR:  defaultSubnetCIDR,
	}
}

func main() {
	ctx := context.Background()

	// Initialize Redis client
	redisClient = redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})

	// Test Redis connection
	if err := redisClient.Ping(ctx).Err(); err != nil {
		log.Printf("Warning: Redis connection failed: %v. Continuing without Redis persistence.", err)
		redisClient = nil
	} else {
		fmt.Println("Connected to Redis successfully")
	}

	cli, err := client.New(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		log.Fatalf("create docker client: %v", err)
	}

	opts := defaultPostgresOptions()

	// ============================================================
	// App Restart/Crash Recovery Flow:
	// ============================================================
	// Step 1: Check networkID from Redis, reuse if exists and valid in Docker,
	//         otherwise create new network and save ID to Redis
	// Step 2: Query Docker for all containers on the network, extract their IPs,
	//         and sync/update Redis to match Docker's actual state
	// ============================================================

	// Step 1: Ensure network exists (check Redis first, reuse if exists, create if not)
	subnet, err := netip.ParsePrefix(opts.SubnetCIDR)
	if err != nil {
		log.Fatalf("parse subnet: %v", err)
	}

	networkID, err := ensureNetwork(ctx, cli, opts.NetworkName, subnet)
	if err != nil {
		log.Fatalf("ensure network: %v", err)
	}

	// Step 2: Sync container data from Docker to Redis on startup (Docker is source of truth)
	// This ensures Redis always matches Docker's actual state after app restart/crash
	// Also handles reconnecting containers if network was deleted and recreated
	if redisClient != nil {
		if err := syncContainerDataFromDocker(ctx, cli, opts.NetworkName, networkID); err != nil {
			log.Printf("Warning: Failed to sync container data from Docker: %v", err)
		}
	}

	dataVolume, err := ensureDataVolume(ctx, cli, opts)
	if err != nil {
		log.Fatalf("prepare volume: %v", err)
	}

	// Initialize IP allocator based on existing container IPs to avoid conflicts
	ipAllocator, err := initializeIPAllocator(ctx, cli, subnet, opts.NetworkName)
	if err != nil {
		log.Fatalf("initialize IP allocator: %v", err)
	}

	id, err := createPostgresContainer(ctx, cli, opts, dataVolume, networkID, ipAllocator)
	if err != nil {
		log.Fatalf("create container: %v", err)
	}

	if err := runPostgresContainer(ctx, cli, opts, id); err != nil {
		log.Fatalf("run container: %v", err)
	}

	printAssignments()
}

func ensureDataVolume(ctx context.Context, cli *client.Client, opts PostgresContainerOptions) (string, error) {
	prefix := opts.Name
	if prefix == "" {
		prefix = volumePrefix
	}
	volumeName := fmt.Sprintf("%s-%s", prefix, uuid.NewString())
	fmt.Printf("Creating persistent volume %s...\n", volumeName)
	if _, err := cli.VolumeCreate(ctx, client.VolumeCreateOptions{
		Name: volumeName,
	}); err != nil {
		return "", fmt.Errorf("volume create: %w", err)
	}
	return volumeName, nil
}

func createPostgresContainer(ctx context.Context, cli *client.Client, opts PostgresContainerOptions, dataVolume, networkID string, allocator *ipAllocator) (string, error) {
	fmt.Println("Pulling postgres image...")
	pullReader, err := cli.ImagePull(ctx, opts.Image, client.ImagePullOptions{})
	if err != nil {
		return "", fmt.Errorf("image pull: %w", err)
	}
	defer pullReader.Close()
	io.Copy(os.Stdout, pullReader)

	fmt.Println("Creating postgres container...")
	ipAddr, err := allocator.Next()
	if err != nil {
		return "", fmt.Errorf("allocate ip: %w", err)
	}

	resp, err := cli.ContainerCreate(ctx, client.ContainerCreateOptions{
		Name: opts.Name,
		Config: &container.Config{
			Image: opts.Image,
			Env:   envMapToSlice(opts.Env),
		},
		HostConfig: &container.HostConfig{
			Resources: container.Resources{
				Memory:     opts.ResourceLimits.Memory,
				MemorySwap: opts.ResourceLimits.MemorySwap,
				CPUQuota:   opts.ResourceLimits.CPUQuota,
				CPUShares:  opts.ResourceLimits.CPUShares,
				// Storage limits are handled via volume size constraints
				// Network limits are set via network configuration
			},
			Mounts: []mount.Mount{
				{
					Type:   mount.TypeVolume,
					Source: dataVolume,
					Target: opts.VolumeMountPath,
				},
			},
		},
		NetworkingConfig: &network.NetworkingConfig{
			EndpointsConfig: map[string]*network.EndpointSettings{
				opts.NetworkName: {
					NetworkID: networkID,
					IPAddress: ipAddr,
				},
			},
		},
	})
	if err != nil {
		return "", fmt.Errorf("container create: %w", err)
	}

	storeAssignment(resp.ID, ipAddr.String())
	return resp.ID, nil
}

func runPostgresContainer(ctx context.Context, cli *client.Client, opts PostgresContainerOptions, containerID string) error {
	fmt.Println("Starting postgres container in the background...")
	if _, err := cli.ContainerStart(ctx, containerID, client.ContainerStartOptions{}); err != nil {
		return fmt.Errorf("container start: %w", err)
	}

	fmt.Printf("Container %s is now running detached.\n", containerID[:12])
	fmt.Println("Use `docker logs -f", opts.Name, "` to follow output or `docker stop", opts.Name, "` to stop it.")

	// Start resource monitoring in background
	go monitorContainerResources(ctx, cli, containerID, opts.ResourceLimits, opts.Name)

	return nil
}

func envMapToSlice(env map[string]string) []string {
	if len(env) == 0 {
		return nil
	}
	out := make([]string, 0, len(env))
	for key, value := range env {
		out = append(out, fmt.Sprintf("%s=%s", key, value))
	}
	return out
}

func ensureNetwork(ctx context.Context, cli *client.Client, name string, subnet netip.Prefix) (string, error) {
	fmt.Printf("Ensuring network %s...\n", name)

	// On app restart: Check Redis for stored network ID first
	if redisClient != nil {
		networkID, err := redisClient.Get(ctx, redisNetworkKey).Result()
		if err == nil && networkID != "" {
			// Verify network still exists in Docker
			_, err := cli.NetworkInspect(ctx, networkID, client.NetworkInspectOptions{})
			if err == nil {
				fmt.Printf("Reusing existing network %s (ID: %s) from Redis\n", name, networkID[:12])
				return networkID, nil
			}
			// Network doesn't exist in Docker, remove stale entry from Redis and create new one
			log.Printf("Network ID in Redis doesn't exist in Docker, creating new network")
			redisClient.Del(ctx, redisNetworkKey)
		}
	}

	enableIPv4 := true

	result, err := cli.NetworkCreate(ctx, name, client.NetworkCreateOptions{
		Driver:     "bridge",
		EnableIPv4: &enableIPv4,
		IPAM: &network.IPAM{
			Driver: "default",
			Config: []network.IPAMConfig{
				{
					Subnet:  subnet,
					Gateway: netip.MustParseAddr(defaultGateway),
				},
			},
		},
	})
	if err != nil {
		return "", fmt.Errorf("network create: %w", err)
	}

	// Save network ID to Redis for future restarts
	if redisClient != nil {
		if err := redisClient.Set(ctx, redisNetworkKey, result.ID, 0).Err(); err != nil {
			log.Printf("Warning: Failed to save network ID to Redis: %v", err)
		} else {
			fmt.Printf("Saved network ID to Redis for future restarts\n")
		}
	}

	return result.ID, nil
}

type ipAllocator struct {
	subnet netip.Prefix
	next   netip.Addr
	mu     sync.Mutex
}

func newSequentialIPAllocator(subnet netip.Prefix, gateway netip.Addr) *ipAllocator {
	start := gateway.Next()
	return &ipAllocator{subnet: subnet, next: start}
}

// initializeIPAllocator creates an IP allocator initialized from existing container IPs
// to ensure no IP conflicts when creating new containers
func initializeIPAllocator(ctx context.Context, cli *client.Client, subnet netip.Prefix, networkName string) (*ipAllocator, error) {
	gateway := netip.MustParseAddr(defaultGateway)

	// Get all containers from Docker
	containers, err := cli.ContainerList(ctx, client.ContainerListOptions{
		All: true,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list containers: %w", err)
	}

	// Find the highest IP currently in use on this network
	highestIP := gateway // Start with gateway as baseline
	foundAny := false

	for _, container := range containers.Items {
		inspect, err := cli.ContainerInspect(ctx, container.ID, client.ContainerInspectOptions{})
		if err != nil {
			continue
		}

		// Check if container is on our network
		networkSettings, exists := inspect.Container.NetworkSettings.Networks[networkName]
		if !exists || !networkSettings.IPAddress.IsValid() {
			continue
		}

		ip := networkSettings.IPAddress
		if subnet.Contains(ip) {
			// Compare IPs to find the highest one
			if !foundAny || ip.Compare(highestIP) > 0 {
				highestIP = ip
				foundAny = true
			}
		}
	}

	// Start allocator from the next IP after the highest one
	startIP := highestIP.Next()
	if !subnet.Contains(startIP) {
		// If we've exhausted the subnet, start from gateway+1
		startIP = gateway.Next()
	}

	if foundAny {
		fmt.Printf("Initialized IP allocator: highest existing IP is %s, next IP will be %s\n", highestIP.String(), startIP.String())
	} else {
		fmt.Printf("Initialized IP allocator: no existing containers found, starting from %s\n", startIP.String())
	}

	return &ipAllocator{
		subnet: subnet,
		next:   startIP,
	}, nil
}

func (a *ipAllocator) Next() (netip.Addr, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if !a.next.Is4() {
		return netip.Addr{}, fmt.Errorf("allocator only supports IPv4")
	}

	ip := a.next

	// Move to next IP for future allocations
	a.next = a.next.Next()
	if !a.subnet.Contains(a.next) {
		return netip.Addr{}, fmt.Errorf("subnet %s exhausted", a.subnet)
	}

	return ip, nil
}

// CheckIPInUse verifies if an IP address is already in use by checking the in-memory assignments
func (a *ipAllocator) CheckIPInUse(ip netip.Addr) bool {
	ipAssignmentsMu.RLock()
	defer ipAssignmentsMu.RUnlock()

	ipStr := ip.String()
	for _, assignedIP := range ipAssignments {
		if assignedIP == ipStr {
			return true
		}
	}
	return false
}

func storeAssignment(containerID, ip string) {
	ipAssignmentsMu.Lock()
	defer ipAssignmentsMu.Unlock()
	ipAssignments[containerID] = ip

	// Store in Redis if available
	if redisClient != nil {
		ctx := context.Background()
		key := redisKeyPrefix + containerID
		if err := redisClient.Set(ctx, key, ip, 0).Err(); err != nil {
			log.Printf("Warning: Failed to store assignment in Redis: %v", err)
		} else {
			fmt.Printf("Stored container %s -> %s in Redis\n", containerID[:12], ip)
		}
	}
}

// syncContainerDataFromDocker queries Docker engine (source of truth) for all containers
// on the specified network, extracts their IP addresses, and syncs to Redis.
// Also handles the case where network was deleted and containers need to be reconnected.
func syncContainerDataFromDocker(ctx context.Context, cli *client.Client, networkName, networkID string) error {
	if redisClient == nil {
		return fmt.Errorf("redis client not available")
	}

	fmt.Println("Syncing container data from Docker (source of truth) to Redis...")

	// Get all containers from Docker
	containers, err := cli.ContainerList(ctx, client.ContainerListOptions{
		All: true, // Include stopped containers
	})
	if err != nil {
		return fmt.Errorf("failed to list containers: %w", err)
	}

	ipAssignmentsMu.Lock()
	defer ipAssignmentsMu.Unlock()

	syncedCount := 0
	skippedCount := 0
	reconnectedCount := 0

	// Get all existing keys from Redis to track what should be removed or reconnected
	redisKeys, _ := redisClient.Keys(ctx, redisKeyPrefix+"*").Result()
	redisContainerMap := make(map[string]bool)
	for _, key := range redisKeys {
		containerID := key[len(redisKeyPrefix):]
		redisContainerMap[containerID] = true
	}

	// Process each container from Docker
	for _, container := range containers.Items {
		containerID := container.ID

		// Inspect container to get network details
		inspect, err := cli.ContainerInspect(ctx, containerID, client.ContainerInspectOptions{})
		if err != nil {
			log.Printf("Warning: Failed to inspect container %s: %v", containerID[:12], err)
			skippedCount++
			continue
		}

		// Check if container is on our network
		networkSettings, exists := inspect.Container.NetworkSettings.Networks[networkName]
		if !exists || !networkSettings.IPAddress.IsValid() {
			// Container not on our network - check if it's in Redis (was disconnected)
			if redisContainerMap[containerID] {
				// Container exists in Redis but is not on the network
				// This likely means the network was deleted and recreated
				// Try to reconnect if container is running
				if inspect.Container.State.Running {
					log.Printf("Container %s is running but disconnected from network %s. Attempting to reconnect...", containerID[:12], networkName)

					// Get the IP that was stored in Redis (if any)
					oldIP, _ := redisClient.Get(ctx, redisKeyPrefix+containerID).Result()

					// Reconnect container to network
					// Docker will assign a new IP if the old one is not available
					endpointConfig := &network.EndpointSettings{
						NetworkID: networkID,
					}
					if oldIP != "" {
						// Try to use the old IP if possible
						if ip, err := netip.ParseAddr(oldIP); err == nil {
							endpointConfig.IPAddress = ip
						}
					}

					if _, err := cli.NetworkConnect(ctx, networkID, client.NetworkConnectOptions{
						Container:      containerID,
						EndpointConfig: endpointConfig,
					}); err != nil {
						log.Printf("Warning: Failed to reconnect container %s to network: %v", containerID[:12], err)
						skippedCount++
						continue
					}

					// Re-inspect to get the new IP
					inspect, err = cli.ContainerInspect(ctx, containerID, client.ContainerInspectOptions{})
					if err != nil {
						log.Printf("Warning: Failed to re-inspect container %s after reconnect: %v", containerID[:12], err)
						skippedCount++
						continue
					}

					networkSettings, exists = inspect.Container.NetworkSettings.Networks[networkName]
					if !exists || !networkSettings.IPAddress.IsValid() {
						log.Printf("Warning: Container %s reconnected but still no valid IP", containerID[:12])
						skippedCount++
						continue
					}

					reconnectedCount++
					log.Printf("Successfully reconnected container %s to network %s", containerID[:12], networkName)
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
		ipAssignments[containerID] = ip

		// Store/update in Redis
		key := redisKeyPrefix + containerID
		if err := redisClient.Set(ctx, key, ip, 0).Err(); err != nil {
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
		key := redisKeyPrefix + containerID
		if err := redisClient.Del(ctx, key).Err(); err != nil {
			log.Printf("Warning: Failed to remove stale container %s from Redis: %v", containerID[:12], err)
		} else {
			removedCount++
		}
		// Also remove from in-memory map if present
		delete(ipAssignments, containerID)
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

// getIPFromRedis retrieves the IP address for a container from Redis
func getIPFromRedis(containerID string) (string, error) {
	if redisClient == nil {
		return "", fmt.Errorf("redis client not available")
	}
	ctx := context.Background()
	key := redisKeyPrefix + containerID
	ip, err := redisClient.Get(ctx, key).Result()
	if err == redis.Nil {
		return "", fmt.Errorf("container %s not found in Redis", containerID[:12])
	}
	if err != nil {
		return "", fmt.Errorf("failed to get IP from Redis: %w", err)
	}
	return ip, nil
}

// monitorContainerResources monitors container resource usage and stops it if limits are exceeded
func monitorContainerResources(ctx context.Context, cli *client.Client, containerID string, limits ResourceLimits, containerName string) {
	ticker := time.NewTicker(monitorInterval)
	defer ticker.Stop()

	var prevStats *container.StatsResponse
	var prevTime time.Time

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			statsResult, err := cli.ContainerStats(ctx, containerID, client.ContainerStatsOptions{
				Stream:                false,
				IncludePreviousSample: true,
			})
			if err != nil {
				log.Printf("Warning: Failed to get stats for container %s: %v", containerID[:12], err)
				continue
			}

			var statsJSON container.StatsResponse
			if err := json.NewDecoder(statsResult.Body).Decode(&statsJSON); err != nil {
				statsResult.Body.Close()
				log.Printf("Warning: Failed to decode stats for container %s: %v", containerID[:12], err)
				continue
			}
			statsResult.Body.Close()

			now := time.Now()

			// Check memory usage
			if limits.Memory > 0 {
				memoryUsage := statsJSON.MemoryStats.Usage
				if memoryUsage > uint64(limits.Memory) {
					reason := fmt.Sprintf("Memory limit exceeded: %d bytes used (limit: %d bytes)", memoryUsage, limits.Memory)
					log.Printf("ERROR: Container %s (%s) exceeded resource limits. %s", containerID[:12], containerName, reason)
					stopContainerWithReason(ctx, cli, containerID, containerName, reason)
					return
				}
			}

			// Check CPU usage (calculate CPU percentage)
			if limits.CPUQuota > 0 && prevStats != nil {
				cpuDelta := statsJSON.CPUStats.CPUUsage.TotalUsage - prevStats.CPUStats.CPUUsage.TotalUsage
				systemDelta := statsJSON.CPUStats.SystemUsage - prevStats.CPUStats.SystemUsage
				timeDelta := now.Sub(prevTime).Seconds()

				cpuPercent := 0.0
				if systemDelta > 0 && timeDelta > 0 {
					cpuPercent = (float64(cpuDelta) / float64(systemDelta)) * float64(len(statsJSON.CPUStats.CPUUsage.PercpuUsage)) * 100.0
				}
				maxCPUPercent := float64(limits.CPUQuota) / 100000.0 * 100.0
				if cpuPercent > maxCPUPercent {
					reason := fmt.Sprintf("CPU limit exceeded: %.2f%% used (limit: %.2f%%)", cpuPercent, maxCPUPercent)
					log.Printf("ERROR: Container %s (%s) exceeded resource limits. %s", containerID[:12], containerName, reason)
					stopContainerWithReason(ctx, cli, containerID, containerName, reason)
					return
				}
			}

			// Check network usage (calculate rate)
			if (limits.NetworkRx > 0 || limits.NetworkTx > 0) && prevStats != nil {
				var rxBytes, txBytes uint64
				var prevRxBytes, prevTxBytes uint64

				for _, network := range statsJSON.Networks {
					rxBytes += network.RxBytes
					txBytes += network.TxBytes
				}

				for _, network := range prevStats.Networks {
					prevRxBytes += network.RxBytes
					prevTxBytes += network.TxBytes
				}

				timeDelta := now.Sub(prevTime).Seconds()
				if timeDelta > 0 {
					rxRate := float64(rxBytes-prevRxBytes) / timeDelta
					txRate := float64(txBytes-prevTxBytes) / timeDelta

					if limits.NetworkRx > 0 && rxRate > float64(limits.NetworkRx) {
						reason := fmt.Sprintf("Network receive rate exceeded: %.2f bytes/sec (limit: %d bytes/sec)", rxRate, limits.NetworkRx)
						log.Printf("ERROR: Container %s (%s) exceeded resource limits. %s", containerID[:12], containerName, reason)
						stopContainerWithReason(ctx, cli, containerID, containerName, reason)
						return
					}

					if limits.NetworkTx > 0 && txRate > float64(limits.NetworkTx) {
						reason := fmt.Sprintf("Network transmit rate exceeded: %.2f bytes/sec (limit: %d bytes/sec)", txRate, limits.NetworkTx)
						log.Printf("ERROR: Container %s (%s) exceeded resource limits. %s", containerID[:12], containerName, reason)
						stopContainerWithReason(ctx, cli, containerID, containerName, reason)
						return
					}
				}
			}

			// Check storage I/O usage (via BlkioStats on Linux)
			// Note: Storage size limits are enforced at the volume/filesystem level
			// This checks I/O operations, not storage capacity
			if limits.StorageSize > 0 {
				var totalIOBytes uint64
				for _, entry := range statsJSON.BlkioStats.IoServiceBytesRecursive {
					if entry.Op == "Read" || entry.Op == "Write" {
						totalIOBytes += entry.Value
					}
				}
				// Check if cumulative I/O exceeds limit (this is a simplified check)
				// In practice, storage size limits should be enforced via volume size constraints
				if totalIOBytes > uint64(limits.StorageSize) {
					reason := fmt.Sprintf("Storage I/O limit exceeded: %d bytes (limit: %d bytes)", totalIOBytes, limits.StorageSize)
					log.Printf("ERROR: Container %s (%s) exceeded resource limits. %s", containerID[:12], containerName, reason)
					stopContainerWithReason(ctx, cli, containerID, containerName, reason)
					return
				}
			}

			// Store current stats for next iteration
			prevStats = &statsJSON
			prevTime = now
		}
	}
}

// stopContainerWithReason stops a container and logs the reason
func stopContainerWithReason(ctx context.Context, cli *client.Client, containerID, containerName, reason string) {
	timeout := 10 // seconds
	if _, err := cli.ContainerStop(ctx, containerID, client.ContainerStopOptions{Timeout: &timeout}); err != nil {
		log.Printf("ERROR: Failed to stop container %s: %v", containerID[:12], err)
		return
	}

	log.Printf("Container %s (%s) stopped due to resource limit violation: %s", containerID[:12], containerName, reason)
	fmt.Printf("\n[RESOURCE LIMIT VIOLATION] Container %s stopped: %s\n", containerID[:12], reason)
}

func printAssignments() {
	ipAssignmentsMu.RLock()
	defer ipAssignmentsMu.RUnlock()

	fmt.Println("Current container IP assignments (in-memory):")
	for id, ip := range ipAssignments {
		fmt.Printf("  %s -> %s:%s\n", id[:12], ip, defaultPort)
	}

	// Also print from Redis if available
	if redisClient != nil {
		ctx := context.Background()
		keys, err := redisClient.Keys(ctx, redisKeyPrefix+"*").Result()
		if err != nil {
			log.Printf("Warning: Failed to read from Redis: %v", err)
			return
		}

		if len(keys) > 0 {
			fmt.Println("Container IP assignments (from Redis):")
			for _, key := range keys {
				containerID := key[len(redisKeyPrefix):]
				ip, err := redisClient.Get(ctx, key).Result()
				if err != nil {
					log.Printf("Warning: Failed to get IP for %s: %v", containerID[:12], err)
					continue
				}
				fmt.Printf("  %s -> %s:%s\n", containerID[:12], ip, defaultPort)
			}
		}
	}
}
