package orchestr

import (
	"context"
	"fmt"
	"log"
	"net/netip"

	"github.com/moby/moby/api/types/network"
	"github.com/moby/moby/client"
)

// ensureNetwork ensures the network exists, checking Docker API first, then Redis for persistence
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

		// Update Redis with the network ID if we have a Redis client
		if o.redisClient != nil {
			if err := o.redisClient.Set(ctx, networkKey, networkID, 0).Err(); err != nil {
				log.Printf("Warning: Failed to save network ID to Redis: %v", err)
			} else {
				fmt.Printf("Updated network ID in Redis\n")
			}
		}

		return networkID, nil
	}

	// Step 2: Network doesn't exist in Docker, check Redis for stored network ID
	if o.redisClient != nil {
		networkID, err := o.redisClient.Get(ctx, networkKey).Result()
		if err == nil && networkID != "" {
			// Verify network still exists in Docker by ID
			_, err := o.dockerClient.NetworkInspect(ctx, networkID, client.NetworkInspectOptions{})
			if err == nil {
				fmt.Printf("Reusing existing network %s (ID: %s) from Redis\n", o.config.NetworkName, networkID[:12])
				return networkID, nil
			}
			// Network doesn't exist in Docker, remove stale entry from Redis
			log.Printf("Network ID in Redis doesn't exist in Docker, removing stale entry")
			o.redisClient.Del(ctx, networkKey)
		}
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

			// Update Redis only if it doesn't exist in Redis
			if o.redisClient != nil {
				existingID, err := o.redisClient.Get(ctx, networkKey).Result()
				if err != nil || existingID == "" {
					// Network ID doesn't exist in Redis, save it
					if err := o.redisClient.Set(ctx, networkKey, networkID, 0).Err(); err != nil {
						log.Printf("Warning: Failed to save network ID to Redis: %v", err)
					} else {
						fmt.Printf("Saved network ID to Redis\n")
					}
				}
			}

			return networkID, nil
		}
		return "", fmt.Errorf("network create: %w", err)
	}

	// Save network ID to Redis for future restarts
	if o.redisClient != nil {
		if err := o.redisClient.Set(ctx, networkKey, result.ID, 0).Err(); err != nil {
			log.Printf("Warning: Failed to save network ID to Redis: %v", err)
		} else {
			fmt.Printf("Saved network ID to Redis for future restarts\n")
		}
	}

	fmt.Printf("Successfully created network %s (ID: %s)\n", o.config.NetworkName, result.ID[:12])
	return result.ID, nil
}
