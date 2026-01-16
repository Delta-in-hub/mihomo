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

// Lua script: atomically store bidirectional host<->ip mapping
const storeMappingScript = `
redis.call("SET", KEYS[1], ARGV[2])
redis.call("SET", KEYS[2], ARGV[1])
return "OK"
`

// Lua script: atomically delete bidirectional ip<->host mapping
const deleteMappingScript = `
local host = redis.call("GET", KEYS[1])
if host then
	local hostKey = string.sub(host, 1, -1)
	redis.call("DEL", hostKey)
end
redis.call("DEL", KEYS[1])
return "OK"
`

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
		DialTimeout:  2 * time.Second,
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
			DialTimeout:  2 * time.Second,
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

	for _, replica := range r.replicas {
		val, err := replica.Get(r.ctx, key).Result()
		if err == nil {
			ip, err := netip.ParseAddr(val)
			if err != nil {
				log.Warnln("[FakeIP] Invalid IP in Redis: %s", val)
				return netip.Addr{}, false
			}
			return ip, true
		}
	}

	return netip.Addr{}, false
}

// PutByHost stores bidirectional host<->ip mapping atomically
func (r *redisStore) PutByHost(host string, ip netip.Addr) error {
	hostKey := r.prefix + ":" + "host:" + host
	ipKey := r.prefix + ":" + "ip:" + ip.String()

	_, err := r.master.Eval(r.ctx, storeMappingScript, []string{hostKey, ipKey}, host, ip.String()).Result()
	if err != nil {
		log.Warnln("[FakeIP] Failed to store mapping: %v", err)
		return err
	}
	return nil
}

// GetByIP retrieves hostname by IP, tries replicas then master
func (r *redisStore) GetByIP(ip netip.Addr) (string, bool) {
	key := r.prefix + ":" + "ip:" + ip.String()

	for _, replica := range r.replicas {
		val, err := replica.Get(r.ctx, key).Result()
		if err == nil {
			return val, true
		}
	}

	return "", false
}

// PutByIP stores bidirectional ip<->host mapping atomically
func (r *redisStore) PutByIP(ip netip.Addr, host string) error {
	ipKey := r.prefix + ":" + "ip:" + ip.String()
	hostKey := r.prefix + ":" + "host:" + host

	_, err := r.master.Eval(r.ctx, storeMappingScript, []string{hostKey, ipKey}, host, ip.String()).Result()
	if err != nil {
		log.Warnln("[FakeIP] Failed to store mapping: %v", err)
		return err
	}
	return nil
}

// DelByIP atomically deletes bidirectional ip<->host mapping
func (r *redisStore) DelByIP(ip netip.Addr) error {
	ipKey := r.prefix + ":" + "ip:" + ip.String()

	_, err := r.master.Eval(r.ctx, deleteMappingScript, []string{ipKey}).Result()
	if err != nil {
		log.Warnln("[FakeIP] Failed to delete mapping: %v", err)
		return err
	}
	return nil
}

// Exist checks if IP exists in store
func (r *redisStore) Exist(ip netip.Addr) bool {
	key := r.prefix + ":" + "ip:" + ip.String()

	for _, replica := range r.replicas {
		_, err := replica.Get(r.ctx, key).Result()
		if err == nil {
			return true
		}
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
