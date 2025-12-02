package main

import (
	"context"
	"fmt"
	"log"

	"github.com/KilluaDB/orchest/orchestr"
	"github.com/google/uuid"
	"github.com/moby/moby/api/types/container"
)

const (
	defaultImage        = "postgres:16-alpine"
	defaultContainer    = "orchestr-postgres"
	defaultVolumeTarget = "/var/lib/postgresql/data"
	defaultPort         = "5432"
)

func main() {
	ctx := context.Background()

	// Create orchestrator with default config
	config := orchestr.DefaultConfig()
	orch, err := orchestr.New(config)
	if err != nil {
		log.Fatalf("Failed to create orchestrator: %v", err)
	}
	defer orch.Close()

	// Initialize network and sync container data
	if err := orch.Initialize(ctx); err != nil {
		log.Fatalf("Failed to initialize orchestrator: %v", err)
	}

	// Create container options
	opts := createPostgresOptions()

	// Create and start container
	containerID, err := orch.CreateContainer(ctx, opts)
	if err != nil {
		log.Fatalf("Failed to create container: %v", err)
	}

	fmt.Printf("\nContainer created successfully: %s\n", containerID[:12])

	// Get container IP
	ip, exists := orch.GetContainerIP(containerID)
	if exists {
		fmt.Printf("Container IP: %s:%s\n", ip, defaultPort)
	}

	// List all containers
	containers := orch.ListContainers()
	fmt.Println("\nAll container IP assignments:")
	for id, ip := range containers {
		fmt.Printf("  %s -> %s:%s\n", id[:12], ip, defaultPort)
	}
}

func createPostgresOptions() orchestr.ContainerOptions {
	memoryLimit := int64(512 * 1024 * 1024) // 512MiB
	return orchestr.ContainerOptions{
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
			MemorySwap: memoryLimit * 2,
			CPUQuota:   100000,
			CPUShares:  1024,
		},
		ResourceLimits: orchestr.ResourceLimits{
			CPUQuota:    100000,
			CPUShares:   1024,
			Memory:      memoryLimit,
			MemorySwap:  memoryLimit * 2,
			StorageSize: 10 * 1024 * 1024 * 1024,
			NetworkRx:   100 * 1024 * 1024,
			NetworkTx:   100 * 1024 * 1024,
		},
		// NetworkName and SubnetCIDR will use config defaults if empty
	}
}
