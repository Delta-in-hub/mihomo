package fakeip

import (
	"errors"
	"net/netip"
	"strings"
	"sync"

	"github.com/metacubex/mihomo/component/profile/cachefile"
	"github.com/metacubex/mihomo/log"

	"go4.org/netipx"
)

const (
	offsetKey = "key-offset-fake-ip"
	cycleKey  = "key-cycle-fake-ip"
)

type store interface {
	GetByHost(host string) (netip.Addr, bool)
	PutByHost(host string, ip netip.Addr) error
	GetByIP(ip netip.Addr) (string, bool)
	PutByIP(ip netip.Addr, host string) error
	DelByIP(ip netip.Addr) error
	Exist(ip netip.Addr) bool
	CloneTo(store)
	FlushFakeIP() error
}

// Pool is an implementation about fake ip generator without storage
type Pool struct {
	gateway netip.Addr
	first   netip.Addr
	last    netip.Addr
	offset  netip.Addr
	cycle   bool
	mux     sync.Mutex
	ipnet   netip.Prefix
	store   store
	role    string // "master" or "replica"
}

// Lookup return a fake ip with host
func (p *Pool) Lookup(host string) netip.Addr {
	p.mux.Lock()
	defer p.mux.Unlock()

	// RFC4343: DNS Case Insensitive, we SHOULD return result with all cases.
	host = strings.ToLower(host)
	if ip, exist := p.store.GetByHost(host); exist {
		return ip
	}

	// Replica mode: only read existing mappings, don't allocate new ones
	if p.role == "replica" {
		return netip.Addr{}
	}

	ip, err := p.get(host)
	if err != nil {
		return netip.Addr{}
	}
	err = p.store.PutByHost(host, ip)
	if err != nil {
		return netip.Addr{}
	}
	return ip
}

// LookBack return host with the fake ip
func (p *Pool) LookBack(ip netip.Addr) (string, bool) {
	p.mux.Lock()
	defer p.mux.Unlock()

	return p.store.GetByIP(ip)
}

// Exist returns if given ip exists in fake-ip pool
func (p *Pool) Exist(ip netip.Addr) bool {
	p.mux.Lock()
	defer p.mux.Unlock()

	return p.store.Exist(ip)
}

// Gateway return gateway ip
func (p *Pool) Gateway() netip.Addr {
	return p.gateway
}

// Broadcast return the last ip
func (p *Pool) Broadcast() netip.Addr {
	return p.last
}

// IPNet return raw ipnet
func (p *Pool) IPNet() netip.Prefix {
	return p.ipnet
}

// CloneFrom clone cache from old pool
func (p *Pool) CloneFrom(o *Pool) {
	o.store.CloneTo(p.store)
}

func (p *Pool) get(host string) (netip.Addr, error) {
	p.offset = p.offset.Next()

	if !p.offset.Less(p.last) {
		p.cycle = true
		p.offset = p.first
	}

	if p.cycle || p.store.Exist(p.offset) {
		err := p.store.DelByIP(p.offset)
		if err != nil {
			return netip.Addr{}, err
		}
	}

	err := p.store.PutByIP(p.offset, host)
	if err != nil {
		return netip.Addr{}, err
	}
	return p.offset, nil
}

func (p *Pool) FlushFakeIP() error {
	// Replica mode: read-only, don't allow flush
	if p.role == "replica" {
		return errors.New("replica mode does not support flush operation")
	}

	err := p.store.FlushFakeIP()
	if err == nil {
		p.cycle = false
		p.offset = p.first.Prev()
	}
	return err
}

func (p *Pool) StoreState() {
	if s, ok := p.store.(*cachefileStore); ok {
		_ = s.PutByHost(offsetKey, p.offset)
		if p.cycle {
			_ = s.PutByHost(cycleKey, p.offset)
		}
	} else if s, ok := p.store.(*redisStore); ok {
		if p.role == "replica" {
			log.Warnln("replica mode does not support StoreState operation")
			return
		}
		_ = s.PutByHost(offsetKey, p.offset)
		if p.cycle {
			_ = s.PutByHost(cycleKey, p.offset)
		}
	}
}

func (p *Pool) restoreState() {
	if s, ok := p.store.(*cachefileStore); ok {
		if _, exist := s.GetByHost(cycleKey); exist {
			p.cycle = true
		}

		if offset, exist := s.GetByHost(offsetKey); exist {
			if p.ipnet.Contains(offset) {
				p.offset = offset
			} else {
				_ = p.FlushFakeIP()
			}
		} else if s.Exist(p.first) {
			_ = p.FlushFakeIP()
		}
	} else if s, ok := p.store.(*redisStore); ok {
		if _, exist := s.GetByHost(cycleKey); exist {
			p.cycle = true
		}

		if offset, exist := s.GetByHost(offsetKey); exist {
			if p.ipnet.Contains(offset) {
				p.offset = offset
			} else {
				_ = p.FlushFakeIP()
			}
		} else if s.Exist(p.first) {
			_ = p.FlushFakeIP()
		}
	}
}

type Options struct {
	IPNet netip.Prefix

	// Size sets the maximum number of entries in memory
	// and does not work if Persistence is true
	Size int

	// Persistence will save the data to disk.
	// Size will not work and record will be fully stored.
	Persistence bool

	// Redis config for distributed FakeIP storage
	// If set, Redis will be used instead of file-based persistence
	Redis *RedisConfig

	// Role: "master" or "replica". Replica mode is read-only
	Role string
}

// New return Pool instance
func New(options Options) (*Pool, error) {
	var (
		hostAddr = options.IPNet.Masked().Addr()
		gateway  = hostAddr.Next()
		first    = gateway.Next().Next().Next() // default start with 198.18.0.4
		last     = netipx.PrefixLastIP(options.IPNet)
	)

	if !options.IPNet.IsValid() || !first.IsValid() || !first.Less(last) {
		return nil, errors.New("ipnet don't have valid ip")
	}

	// Normalize role, default to "master"
	role := options.Role
	if role != "replica" && role != "master" {
		role = "master"
	}

	pool := &Pool{
		gateway: gateway,
		first:   first,
		last:    last,
		offset:  first.Prev(),
		cycle:   false,
		ipnet:   options.IPNet,
		role:    role,
	}

	// 选择存储后端：Redis > 文件持久化 > 内存
	if options.Redis != nil {
		store, err := newRedisStore(options.Redis, options.IPNet)
		if err != nil {
			return nil, err
		}
		pool.store = store
	} else if options.Persistence {
		pool.store = newCachefileStore(cachefile.Cache(), options.IPNet)
	} else {
		pool.store = newMemoryStore(options.Size)
	}

	pool.restoreState()

	return pool, nil
}
