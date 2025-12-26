package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
)

// monitorContainerResources monitors container resource usage and stops it if limits are exceeded
func (o *Orchestrator) monitorContainerResources(ctx context.Context, containerID string, limits ResourceLimits, containerName string, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var prevStats *container.StatsResponse
	var prevTime time.Time

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			statsResult, err := o.dockerClient.ContainerStats(ctx, containerID, client.ContainerStatsOptions{
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
					o.stopContainerWithReason(ctx, containerID, containerName, reason)
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
					o.stopContainerWithReason(ctx, containerID, containerName, reason)
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
						o.stopContainerWithReason(ctx, containerID, containerName, reason)
						return
					}

					if limits.NetworkTx > 0 && txRate > float64(limits.NetworkTx) {
						reason := fmt.Sprintf("Network transmit rate exceeded: %.2f bytes/sec (limit: %d bytes/sec)", txRate, limits.NetworkTx)
						log.Printf("ERROR: Container %s (%s) exceeded resource limits. %s", containerID[:12], containerName, reason)
						o.stopContainerWithReason(ctx, containerID, containerName, reason)
						return
					}
				}
			}

			// Check storage I/O usage (via BlkioStats on Linux)
			if limits.StorageSize > 0 {
				var totalIOBytes uint64
				for _, entry := range statsJSON.BlkioStats.IoServiceBytesRecursive {
					if entry.Op == "Read" || entry.Op == "Write" {
						totalIOBytes += entry.Value
					}
				}
				if totalIOBytes > uint64(limits.StorageSize) {
					reason := fmt.Sprintf("Storage I/O limit exceeded: %d bytes (limit: %d bytes)", totalIOBytes, limits.StorageSize)
					log.Printf("ERROR: Container %s (%s) exceeded resource limits. %s", containerID[:12], containerName, reason)
					o.stopContainerWithReason(ctx, containerID, containerName, reason)
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
func (o *Orchestrator) stopContainerWithReason(ctx context.Context, containerID, containerName, reason string) {
	timeout := 10 // seconds
	if _, err := o.dockerClient.ContainerStop(ctx, containerID, client.ContainerStopOptions{Timeout: &timeout}); err != nil {
		log.Printf("ERROR: Failed to stop container %s: %v", containerID[:12], err)
		return
	}

	log.Printf("Container %s (%s) stopped due to resource limit violation: %s", containerID[:12], containerName, reason)
	fmt.Printf("\n[RESOURCE LIMIT VIOLATION] Container %s stopped: %s\n", containerID[:12], reason)
}
