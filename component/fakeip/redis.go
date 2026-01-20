package fakeip

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"time"

	"github.com/metacubex/mihomo/log"
	"github.com/redis/go-redis/v9"
)

// RedisConfig defines Redis connection configuration
type RedisConfig struct {
	Addr         string   // Master address (read-write)
	ReplicaAddrs []string // Replica addresses (read-only)
	Username     string   // Redis username
	Password     string   // Redis password
	DB           int      // Redis database number
}

// redisStore implements store interface using Redis backend
type redisStore struct {
	master   *redis.Client   // Master client (read-write)
	replicas []*redis.Client // Replica clients (read-only), master at end
	ctx      context.Context
	prefix   string // Key prefix: "fakeip" or "fakeip6"
}

// newRedisStore creates a new Redis-based store
func newRedisStore(config *RedisConfig, ipNet netip.Prefix) (*redisStore, error) {
	if config.Addr == "" {
		return nil, errors.New("redis address is required")
	}

	ctx := context.Background()

	// Create master client
	master := redis.NewClient(&redis.Options{
		Addr:         config.Addr,
		Username:     config.Username,
		Password:     config.Password,
		DB:           config.DB,
		MaxRetries:   -1, // Disable retry
		DialTimeout:  time.Second,
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
	})

	if err := master.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis master connection failed: %w", err)
	}

	// Create replica clients with deduplication
	seen := make(map[string]bool)

	var replicas []*redis.Client
	for _, addr := range config.ReplicaAddrs {
		if addr == "" || seen[addr] {
			if addr != "" && addr != config.Addr {
				log.Warnln("[FakeIP] Duplicate address %s, skipping", addr)
			}
			continue
		}

		replica := redis.NewClient(&redis.Options{
			Addr:         addr,
			Username:     config.Username,
			Password:     config.Password,
			DB:           config.DB,
			MaxRetries:   -1, // Disable retry
			DialTimeout:  time.Second,
			ReadTimeout:  time.Second,
			WriteTimeout: time.Second,
		})

		if err := replica.Ping(ctx).Err(); err != nil {
			log.Warnln("[FakeIP] Redis replica %s connection failed: %v", addr, err)
			continue
		}

		seen[addr] = true
		replicas = append(replicas, replica)
	}

	// Append master as last fallback for reads
	if !seen[config.Addr] {
		replicas = append(replicas, master)
	}

	// Determine prefix based on IP version
	prefix := "fakeip"
	if ipNet.Addr().Is6() {
		prefix = "fakeip6"
	}

	store := &redisStore{
		master:   master,
		replicas: replicas,
		ctx:      ctx,
		prefix:   prefix,
	}

	log.Infoln("[FakeIP] Redis store initialized: %s, master=%s (DB:%d), read_targets=%d",
		prefix, config.Addr, config.DB, len(replicas))

	return store, nil
}

// GetByHost retrieves IP by hostname, tries replicas then master
func (r *redisStore) GetByHost(host string) (netip.Addr, bool) {
	key := r.prefix + ":" + "host:" + host

	for i, replica := range r.replicas {
		val, err := replica.Get(r.ctx, key).Result()
		if err == nil {
			// Move successful replica to front for faster subsequent queries
			if i > 0 {
				r.replicas[0], r.replicas[i] = r.replicas[i], r.replicas[0]
			}
			ip, err := netip.ParseAddr(val)
			if err != nil {
				log.Warnln("[FakeIP] Invalid IP in Redis: %s", val)
				return netip.Addr{}, false
			}
			return ip, true
		}

		// Key doesn't exist, no need to check other replicas
		if errors.Is(err, redis.Nil) {
			return netip.Addr{}, false
		}

		// Network error, try next replica
		log.Warnln("[FakeIP] Failed to get host %s from replica %d: %v", host, i, err)
	}

	return netip.Addr{}, false
}

// PutByHost stores host->ip mapping
func (r *redisStore) PutByHost(host string, ip netip.Addr) error {
	hostKey := r.prefix + ":" + "host:" + host

	err := r.master.Set(r.ctx, hostKey, ip.String(), 0).Err()
	if err != nil {
		log.Warnln("[FakeIP] Failed to store host->ip mapping: %v", err)
		return err
	}
	return nil
}

// GetByIP retrieves hostname by IP, tries replicas then master
func (r *redisStore) GetByIP(ip netip.Addr) (string, bool) {
	key := r.prefix + ":" + "ip:" + ip.String()

	for i, replica := range r.replicas {
		val, err := replica.Get(r.ctx, key).Result()
		if err == nil {
			// Move successful replica to front for faster subsequent queries
			if i > 0 {
				r.replicas[0], r.replicas[i] = r.replicas[i], r.replicas[0]
			}
			return val, true
		}

		// Key doesn't exist, no need to check other replicas
		if errors.Is(err, redis.Nil) {
			return "", false
		}

		// Network error, try next replica
		log.Warnln("[FakeIP] Failed to get ip %s from replica %d: %v", ip, i, err)
	}

	return "", false
}

// PutByIP stores ip->host mapping
func (r *redisStore) PutByIP(ip netip.Addr, host string) error {
	ipKey := r.prefix + ":" + "ip:" + ip.String()

	err := r.master.Set(r.ctx, ipKey, host, 0).Err()
	if err != nil {
		log.Warnln("[FakeIP] Failed to store ip->host mapping: %v", err)
		return err
	}
	return nil
}

// DelByIP deletes both ip->host and host->ip mappings atomically
func (r *redisStore) DelByIP(ip netip.Addr) error {
	ipKey := r.prefix + ":" + "ip:" + ip.String()

	// Use Lua script to atomically delete both mappings
	deleteScript := `
local ip_key = KEYS[1]
local host = redis.call("GET", ip_key)
if host then
	local host_key = ARGV[1] .. host
	redis.call("DEL", ip_key)
	redis.call("DEL", host_key)
	return 1
else
	redis.call("DEL", ip_key)
	return 0
end
`

	hostKeyPrefix := r.prefix + ":" + "host:"

	_, err := r.master.Eval(r.ctx, deleteScript, []string{ipKey}, hostKeyPrefix).Result()
	if err != nil {
		log.Warnln("[FakeIP] Failed to delete mappings for IP %s: %v", ip, err)
		return err
	}

	return nil
}

// Exist checks if IP exists in store
func (r *redisStore) Exist(ip netip.Addr) bool {
	key := r.prefix + ":" + "ip:" + ip.String()

	for i, replica := range r.replicas {
		_, err := replica.Get(r.ctx, key).Result()
		if err == nil {
			// Move successful replica to front for faster subsequent queries
			if i > 0 {
				r.replicas[0], r.replicas[i] = r.replicas[i], r.replicas[0]
			}
			return true
		}

		// Key doesn't exist, no need to check other replicas
		if errors.Is(err, redis.Nil) {
			return false
		}

		// Network error, try next replica
		log.Warnln("[FakeIP] Failed to check existence of ip %s from replica %d: %v", ip, i, err)
	}

	return false
}

// CloneTo is a no-op for Redis (data is shared)
func (r *redisStore) CloneTo(store store) {}

// FlushFakeIP clears all FakeIP mappings atomically
func (r *redisStore) FlushFakeIP() error {
	pattern := r.prefix + ":" + "*"

	flushScript := `
local keys = redis.call('KEYS', ARGV[1])
for i = 1, #keys do
	redis.call('DEL', keys[i])
end
return #keys
`

	result, err := r.master.Eval(r.ctx, flushScript, []string{}, pattern).Result()
	if err != nil {
		return fmt.Errorf("failed to flush fakeip: %w", err)
	}

	count := int(result.(int64))
	log.Infoln("[FakeIP] Cleared %d keys from Redis", count)
	return nil
}

// Close closes Redis connections
func (r *redisStore) Close() error {
	if err := r.master.Close(); err != nil {
		log.Warnln("[FakeIP] Failed to close master: %v", err)
	}

	for _, replica := range r.replicas {
		if replica == r.master {
			continue // Skip master, already closed
		}
		if err := replica.Close(); err != nil {
			log.Warnln("[FakeIP] Failed to close replica: %v", err)
		}
	}

	return nil
}
