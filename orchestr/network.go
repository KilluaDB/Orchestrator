package orchestr

import (
	"context"
	"fmt"
	"log"
	"net/netip"

	"github.com/moby/moby/api/types/network"
	"github.com/moby/moby/client"
)

// ensureNetwork ensures the network exists, checking Redis first for persistence
func (o *Orchestrator) ensureNetwork(ctx context.Context, subnet netip.Prefix) (string, error) {
	fmt.Printf("Ensuring network %s...\n", o.config.NetworkName)

	networkKey := "network:id:" + o.config.NetworkName

	// On app restart: Check Redis for stored network ID first
	if o.redisClient != nil {
		networkID, err := o.redisClient.Get(ctx, networkKey).Result()
		if err == nil && networkID != "" {
			// Verify network still exists in Docker
			_, err := o.dockerClient.NetworkInspect(ctx, networkID, client.NetworkInspectOptions{})
			if err == nil {
				fmt.Printf("Reusing existing network %s (ID: %s) from Redis\n", o.config.NetworkName, networkID[:12])
				return networkID, nil
			}
			// Network doesn't exist in Docker, remove stale entry from Redis and create new one
			log.Printf("Network ID in Redis doesn't exist in Docker, creating new network")
			o.redisClient.Del(ctx, networkKey)
		}
	}

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

	return result.ID, nil
}

