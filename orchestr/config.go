package orchestr

import (
	"github.com/moby/moby/api/types/container"
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

// ContainerOptions defines options for creating a container
type ContainerOptions struct {
	Name            string
	Image           string
	Env             map[string]string
	VolumeMountPath string
	Resources       container.Resources
	ResourceLimits  ResourceLimits
	NetworkName     string
	SubnetCIDR      string
}

// Config holds the orchestrator configuration
type Config struct {
	// Docker client options
	DockerHost string // Docker daemon socket (empty = use default)

	// Redis configuration
	RedisAddr      string // Redis server address (default: "localhost:6379")
	RedisKeyPrefix string // Prefix for Redis keys (default: "container:ip:")

	// Network configuration
	NetworkName string // Network name (default: "orchestr-postgres-net")
	SubnetCIDR  string // Subnet CIDR (default: "172.28.0.0/16")
	Gateway     string // Gateway IP (default: "172.28.0.1")

	// Volume configuration
	VolumePrefix string // Prefix for volume names (default: "orchestr-postgres-data")

	// Monitoring configuration
	MonitorInterval int // Resource monitoring interval in seconds (default: 5)
}

// DefaultConfig returns a configuration with sensible defaults
func DefaultConfig() *Config {
	return &Config{
		RedisAddr:       "localhost:6379",
		RedisKeyPrefix:  "container:ip:",
		NetworkName:     "orchestr-postgres-net",
		SubnetCIDR:      "172.28.0.0/16",
		Gateway:         "172.28.0.1",
		VolumePrefix:    "orchestr-postgres-data",
		MonitorInterval: 5,
	}
}
