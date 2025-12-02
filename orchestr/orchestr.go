package orchestr

import (
	"context"
	"fmt"
	"log"
	"net/netip"
	"sync"

	"github.com/moby/moby/client"
	"github.com/redis/go-redis/v9"
)

// Orchestrator manages Docker containers with network isolation and resource monitoring
type Orchestrator struct {
	config       *Config
	dockerClient *client.Client
	redisClient  *redis.Client
	ipAllocator  *IPAllocator
	assignments  map[string]string // containerID -> IP
	assignmentsMu sync.RWMutex
}

// New creates a new Orchestrator instance
func New(config *Config) (*Orchestrator, error) {
	if config == nil {
		config = DefaultConfig()
	}

	orch := &Orchestrator{
		config:      config,
		assignments: make(map[string]string),
	}

	// Initialize Docker client
	var err error
	if config.DockerHost != "" {
		orch.dockerClient, err = client.New(client.WithHost(config.DockerHost), client.WithAPIVersionNegotiation())
	} else {
		orch.dockerClient, err = client.New(client.FromEnv, client.WithAPIVersionNegotiation())
	}
	if err != nil {
		return nil, fmt.Errorf("create docker client: %w", err)
	}

	// Initialize Redis client
	orch.redisClient = redis.NewClient(&redis.Options{
		Addr: config.RedisAddr,
	})

	// Test Redis connection
	ctx := context.Background()
	if err := orch.redisClient.Ping(ctx).Err(); err != nil {
		log.Printf("Warning: Redis connection failed: %v. Continuing without Redis persistence.", err)
		orch.redisClient = nil
	}

	return orch, nil
}

// Initialize sets up the network and syncs container data from Docker
func (o *Orchestrator) Initialize(ctx context.Context) error {
	// Step 1: Ensure network exists
	subnet, err := netip.ParsePrefix(o.config.SubnetCIDR)
	if err != nil {
		return fmt.Errorf("parse subnet: %w", err)
	}

	networkID, err := o.ensureNetwork(ctx, subnet)
	if err != nil {
		return fmt.Errorf("ensure network: %w", err)
	}

	// Step 2: Sync container data from Docker to Redis
	if o.redisClient != nil {
		if err := o.syncContainerDataFromDocker(ctx, networkID); err != nil {
			log.Printf("Warning: Failed to sync container data from Docker: %v", err)
		}
	}

	// Step 3: Initialize IP allocator based on existing containers
	o.ipAllocator, err = o.initializeIPAllocator(ctx, subnet)
	if err != nil {
		return fmt.Errorf("initialize IP allocator: %w", err)
	}

	return nil
}

// CreateContainer creates a new container with the specified options
func (o *Orchestrator) CreateContainer(ctx context.Context, opts ContainerOptions) (string, error) {
	// Use config defaults if not provided in opts
	if opts.NetworkName == "" {
		opts.NetworkName = o.config.NetworkName
	}
	if opts.SubnetCIDR == "" {
		opts.SubnetCIDR = o.config.SubnetCIDR
	}

	// Ensure network exists
	subnet, err := netip.ParsePrefix(opts.SubnetCIDR)
	if err != nil {
		return "", fmt.Errorf("parse subnet: %w", err)
	}

	networkID, err := o.ensureNetwork(ctx, subnet)
	if err != nil {
		return "", fmt.Errorf("ensure network: %w", err)
	}

	// Ensure data volume
	dataVolume, err := o.ensureDataVolume(ctx, opts)
	if err != nil {
		return "", fmt.Errorf("ensure volume: %w", err)
	}

	// Initialize IP allocator if not already done
	if o.ipAllocator == nil {
		o.ipAllocator, err = o.initializeIPAllocator(ctx, subnet)
		if err != nil {
			return "", fmt.Errorf("initialize IP allocator: %w", err)
		}
	}

	// Create container
	containerID, err := o.createContainer(ctx, opts, dataVolume, networkID)
	if err != nil {
		return "", err
	}

	// Start container
	if err := o.StartContainer(ctx, containerID, opts); err != nil {
		return "", fmt.Errorf("start container: %w", err)
	}

	return containerID, nil
}

// StartContainer starts a container and begins resource monitoring
func (o *Orchestrator) StartContainer(ctx context.Context, containerID string, opts ContainerOptions) error {
	return o.startContainer(ctx, containerID, opts)
}

// StopContainer stops a container
func (o *Orchestrator) StopContainer(ctx context.Context, containerID string) error {
	timeout := 10
	_, err := o.dockerClient.ContainerStop(ctx, containerID, client.ContainerStopOptions{Timeout: &timeout})
	return err
}

// GetContainerIP returns the IP address of a container
func (o *Orchestrator) GetContainerIP(containerID string) (string, bool) {
	o.assignmentsMu.RLock()
	defer o.assignmentsMu.RUnlock()
	ip, exists := o.assignments[containerID]
	return ip, exists
}

// GetContainerIPFromRedis retrieves the IP address from Redis
func (o *Orchestrator) GetContainerIPFromRedis(ctx context.Context, containerID string) (string, error) {
	if o.redisClient == nil {
		return "", fmt.Errorf("redis client not available")
	}
	key := o.config.RedisKeyPrefix + containerID
	ip, err := o.redisClient.Get(ctx, key).Result()
	if err == redis.Nil {
		return "", fmt.Errorf("container %s not found in Redis", containerID[:12])
	}
	if err != nil {
		return "", fmt.Errorf("failed to get IP from Redis: %w", err)
	}
	return ip, nil
}

// ListContainers returns all container IP assignments
func (o *Orchestrator) ListContainers() map[string]string {
	o.assignmentsMu.RLock()
	defer o.assignmentsMu.RUnlock()
	
	result := make(map[string]string)
	for id, ip := range o.assignments {
		result[id] = ip
	}
	return result
}

// Close closes the orchestrator and cleans up resources
func (o *Orchestrator) Close() error {
	if o.redisClient != nil {
		return o.redisClient.Close()
	}
	return nil
}

