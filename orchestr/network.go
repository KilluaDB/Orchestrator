package orchestr

import (
	"context"
	"fmt"
	"log"
	"net/netip"

	"github.com/moby/moby/api/types/network"
	"github.com/moby/moby/client"
	"github.com/redis/go-redis/v9"
)

// ensureNetwork ensures the network exists, checking Docker API first, then Redis for persistence.
// The NetworkID is always stored in Redis for persistence, and the function returns the network ID.
func (o *Orchestrator) ensureNetwork(ctx context.Context, subnet netip.Prefix) (string, error) {
	fmt.Printf("Ensuring network %s...\n", o.config.NetworkName)

	networkKey := "network:id:" + o.config.NetworkName

	// Step 1: Check if network exists in Docker using NetworkList
	networks, err := o.dockerClient.NetworkList(ctx, client.NetworkListOptions{
		Filters: client.Filters{}.Add("name", o.config.NetworkName),
	})
	if err != nil {
		log.Printf("Warning: Failed to list networks: %v", err)
	} else if len(networks.Items) > 0 {
		// Network exists in Docker, use it
		networkID := networks.Items[0].ID
		fmt.Printf("Found existing network %s (ID: %s) in Docker\n", o.config.NetworkName, networkID[:12])

		// Update Redis with the network ID
		if err := o.redisClient.Set(ctx, networkKey, networkID, 0).Err(); err != nil {
			return "", fmt.Errorf("failed to save network ID to Redis: %w", err)
		}
		fmt.Printf("Updated network ID in Redis\n")

		return networkID, nil
	}

	// Step 2: Network doesn't exist in Docker, check Redis for stored network ID
	networkID, err := o.redisClient.Get(ctx, networkKey).Result()
	if err == nil && networkID != "" {
		// Verify network still exists in Docker by ID
		_, err := o.dockerClient.NetworkInspect(ctx, networkID, client.NetworkInspectOptions{})
		if err == nil {
			fmt.Printf("Reusing existing network %s (ID: %s) from Redis\n", o.config.NetworkName, networkID[:12])
			// Ensure network ID is still in Redis (refresh persistence)
			if err := o.redisClient.Set(ctx, networkKey, networkID, 0).Err(); err != nil {
				return "", fmt.Errorf("failed to refresh network ID in Redis: %w", err)
			}
			return networkID, nil
		}
		// Network doesn't exist in Docker, remove stale entry from Redis
		log.Printf("Network ID in Redis doesn't exist in Docker, removing stale entry")
		if err := o.redisClient.Del(ctx, networkKey).Err(); err != nil {
			log.Printf("Warning: Failed to remove stale network ID from Redis: %v", err)
		}
	} else if err != nil && err != redis.Nil {
		// Redis error (not just key not found)
		return "", fmt.Errorf("failed to get network ID from Redis: %w", err)
	}

	// Step 3: Network doesn't exist, create it
	fmt.Printf("Creating new network %s...\n", o.config.NetworkName)
	enableIPv4 := true

	result, err := o.dockerClient.NetworkCreate(ctx, o.config.NetworkName, client.NetworkCreateOptions{
		Driver:     "bridge",
		EnableIPv4: &enableIPv4,
		IPAM: &network.IPAM{
			Driver: "default",
			Config: []network.IPAMConfig{
				{
					Subnet:  subnet,
					Gateway: netip.MustParseAddr(o.config.Gateway),
				},
			},
		},
	})
	if err != nil {
		// When NetworkCreate fails (e.g., network already exists), check if the network exists by name
		// This handles race conditions where network was created between our check and create
		networks, listErr := o.dockerClient.NetworkList(ctx, client.NetworkListOptions{
			Filters: client.Filters{}.Add("name", o.config.NetworkName),
		})
		if listErr == nil && len(networks.Items) > 0 {
			networkID := networks.Items[0].ID
			fmt.Printf("Network already exists, using existing network %s (ID: %s)\n", o.config.NetworkName, networkID[:12])

			// Always update Redis with network ID to ensure persistence
			if err := o.redisClient.Set(ctx, networkKey, networkID, 0).Err(); err != nil {
				return "", fmt.Errorf("failed to save network ID to Redis: %w", err)
			}
			fmt.Printf("Saved network ID to Redis for persistence\n")

			return networkID, nil
		}
		return "", fmt.Errorf("network create: %w", err)
	}

	// Save network ID to Redis for future restarts
	if err := o.redisClient.Set(ctx, networkKey, result.ID, 0).Err(); err != nil {
		return "", fmt.Errorf("failed to save network ID to Redis: %w", err)
	}
	fmt.Printf("Saved network ID to Redis for future restarts\n")

	fmt.Printf("Successfully created network %s (ID: %s)\n", o.config.NetworkName, result.ID[:12])
	return result.ID, nil
}
