package orchestrator

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
	"github.com/moby/moby/api/types/network"
	"github.com/moby/moby/client"
)

// ensureDataVolume creates a unique volume for a container
func (o *Orchestrator) ensureDataVolume(ctx context.Context, opts ContainerOptions) (string, error) {
	prefix := opts.Name
	if prefix == "" {
		prefix = o.config.VolumePrefix
	}
	volumeName := fmt.Sprintf("%s-%s", prefix, uuid.NewString())
	fmt.Printf("Creating persistent volume %s...\n", volumeName)
	if _, err := o.dockerClient.VolumeCreate(ctx, client.VolumeCreateOptions{
		Name: volumeName,
	}); err != nil {
		return "", fmt.Errorf("volume create: %w", err)
	}
	return volumeName, nil
}

// createContainer creates a new container with the specified options
func (o *Orchestrator) createContainer(ctx context.Context, opts ContainerOptions, dataVolume, networkID string) (string, error) {
	fmt.Println("Pulling container image...")
	pullReader, err := o.dockerClient.ImagePull(ctx, opts.Image, client.ImagePullOptions{})
	if err != nil {
		return "", fmt.Errorf("image pull: %w", err)
	}
	defer pullReader.Close()
	io.Copy(os.Stdout, pullReader)

	fmt.Println("Creating container...")
	ipAddr, err := o.ipAllocator.Next()
	if err != nil {
		return "", fmt.Errorf("allocate ip: %w", err)
	}

	resp, err := o.dockerClient.ContainerCreate(ctx, client.ContainerCreateOptions{
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

	if err := o.storeAssignment(resp.ID, ipAddr.String()); err != nil {
		return "", fmt.Errorf("failed to store container assignment: %w", err)
	}
	return resp.ID, nil
}

// startContainer starts a container and begins resource monitoring
func (o *Orchestrator) startContainer(ctx context.Context, containerID string, opts ContainerOptions) error {
	fmt.Println("Starting container in the background...")
	if _, err := o.dockerClient.ContainerStart(ctx, containerID, client.ContainerStartOptions{}); err != nil {
		return fmt.Errorf("container start: %w", err)
	}

	fmt.Printf("Container %s is now running detached.\n", containerID[:12])
	fmt.Printf("Use `docker logs -f %s` to follow output or `docker stop %s` to stop it.\n", opts.Name, opts.Name)

	// Start resource monitoring in background
	monitorInterval := time.Duration(o.config.MonitorInterval) * time.Second
	go o.monitorContainerResources(ctx, containerID, opts.ResourceLimits, opts.Name, monitorInterval)

	return nil
}

// storeAssignment stores container ID to IP mapping in memory and Redis
func (o *Orchestrator) storeAssignment(containerID, ip string) error {
	o.assignmentsMu.Lock()
	defer o.assignmentsMu.Unlock()
	o.assignments[containerID] = ip

	// Store in Redis
	ctx := context.Background()
	if err := o.checkRedisConnection(ctx); err != nil {
		return fmt.Errorf("redis client unavailable: %w", err)
	}
	key := o.config.RedisKeyPrefix + containerID
	if err := o.redisClient.Set(ctx, key, ip, 0).Err(); err != nil {
		return fmt.Errorf("failed to store assignment in Redis: %w", err)
	}
	fmt.Printf("Stored container %s -> %s in Redis\n", containerID[:12], ip)
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
