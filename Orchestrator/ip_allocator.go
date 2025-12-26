package Orchestrator

import (
	"context"
	"fmt"
	"net/netip"
	"sync"

	"github.com/moby/moby/client"
)

// IPAllocator manages IP address allocation for containers
type IPAllocator struct {
	subnet netip.Prefix
	next   netip.Addr
	mu     sync.Mutex
}

// Next returns the next available IP address
func (a *IPAllocator) Next() (netip.Addr, error) {
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

// CheckIPInUse verifies if an IP address is already in use
func (a *IPAllocator) CheckIPInUse(ip netip.Addr, assignments map[string]string) bool {
	ipStr := ip.String()
	for _, assignedIP := range assignments {
		if assignedIP == ipStr {
			return true
		}
	}
	return false
}

// initializeIPAllocator creates an IP allocator initialized from existing container IPs
func (o *Orchestrator) initializeIPAllocator(ctx context.Context, subnet netip.Prefix) (*IPAllocator, error) {
	gateway := netip.MustParseAddr(o.config.Gateway)

	// Get all containers from Docker
	containers, err := o.dockerClient.ContainerList(ctx, client.ContainerListOptions{
		All: true,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list containers: %w", err)
	}

	// Find the highest IP currently in use on this network
	highestIP := gateway // Start with gateway as baseline
	foundAny := false

	for _, container := range containers.Items {
		inspect, err := o.dockerClient.ContainerInspect(ctx, container.ID, client.ContainerInspectOptions{})
		if err != nil {
			continue
		}

		// Check if container is on our network
		networkSettings, exists := inspect.Container.NetworkSettings.Networks[o.config.NetworkName]
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

	return &IPAllocator{
		subnet: subnet,
		next:   startIP,
	}, nil
}
