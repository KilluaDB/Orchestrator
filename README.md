# Orchestr - Docker Container Orchestration Package

A Go package for managing Docker containers with network isolation, resource monitoring, and Redis persistence.

## Features

- **Network Management**: Automatic network creation with persistence across restarts
- **IP Allocation**: Sequential IP allocation with conflict detection
- **Resource Monitoring**: CPU, memory, storage, and network monitoring with automatic container stopping on limit violations
- **Redis Persistence**: Container IP mappings stored in Redis for crash recovery
- **Container Recovery**: Automatic reconnection of containers if network is deleted

## Installation

```bash
go get github.com/KilluaDB/orchest
```

## Usage

### Basic Example

```go
package main

import (
    "context"
    "github.com/KilluaDB/orchest/orchestr"
    "github.com/moby/moby/api/types/container"
)

func main() {
    ctx := context.Background()
    
    // Create orchestrator with default config
    config := orchestr.DefaultConfig()
    orch, err := orchestr.New(config)
    if err != nil {
        log.Fatal(err)
    }
    defer orch.Close()
    
    // Initialize network and sync existing containers
    if err := orch.Initialize(ctx); err != nil {
        log.Fatal(err)
    }
    
    // Create container options
    opts := orchestr.ContainerOptions{
        Name:  "my-container",
        Image: "postgres:16-alpine",
        Env: map[string]string{
            "POSTGRES_PASSWORD": "postgres",
        },
        ResourceLimits: orchestr.ResourceLimits{
            Memory:  512 * 1024 * 1024, // 512MiB
            CPUQuota: 100000,            // 1 CPU
        },
    }
    
    // Create and start container
    containerID, err := orch.CreateContainer(ctx, opts)
    if err != nil {
        log.Fatal(err)
    }
    
    // Get container IP
    ip, _ := orch.GetContainerIP(containerID)
    fmt.Printf("Container IP: %s\n", ip)
}
```

### Custom Configuration

```go
config := &orchestr.Config{
    RedisAddr:      "localhost:6379",
    NetworkName:    "my-network",
    SubnetCIDR:     "172.28.0.0/16",
    Gateway:        "172.28.0.1",
    MonitorInterval: 5, // seconds
}
```

## API Reference

### Orchestrator

- `New(config *Config) (*Orchestrator, error)` - Create a new orchestrator
- `Initialize(ctx context.Context) error` - Initialize network and sync containers
- `CreateContainer(ctx context.Context, opts ContainerOptions) (string, error)` - Create and start a container
- `StartContainer(ctx context.Context, containerID string, opts ContainerOptions) error` - Start a container
- `StopContainer(ctx context.Context, containerID string) error` - Stop a container
- `GetContainerIP(containerID string) (string, bool)` - Get container IP from memory
- `GetContainerIPFromRedis(ctx context.Context, containerID string) (string, error)` - Get container IP from Redis
- `ListContainers() map[string]string` - List all container IP assignments
- `Close() error` - Close the orchestrator

### Types

- `Config` - Orchestrator configuration
- `ContainerOptions` - Container creation options
- `ResourceLimits` - Resource limits for monitoring


