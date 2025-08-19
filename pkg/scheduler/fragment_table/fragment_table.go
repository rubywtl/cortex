package fragment_table

import (
	"sync"
	"time"

	"github.com/cortexproject/cortex/pkg/distributed_execution"
)

type fragmentEntry struct {
	addr      string
	createdAt time.Time
}

type FragmentTable struct {
	mappings   map[distributed_execution.FragmentKey]*fragmentEntry
	mu         sync.RWMutex
	expiration time.Duration
}

func NewFragmentTable(expiration time.Duration) *FragmentTable {
	ft := &FragmentTable{
		mappings:   make(map[distributed_execution.FragmentKey]*fragmentEntry),
		expiration: expiration,
	}

	go ft.periodicCleanup()

	return ft
}

func (f *FragmentTable) AddMapping(queryID uint64, fragmentID uint64, addr string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	key := distributed_execution.MakeFragmentKey(queryID, fragmentID)
	f.mappings[key] = &fragmentEntry{
		addr:      addr,
		createdAt: time.Now(),
	}
}

func (f *FragmentTable) GetAllChildAddresses(queryID uint64, fragmentIDs []uint64) ([]string, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	addresses := make([]string, 0, len(fragmentIDs))

	for _, fragmentID := range fragmentIDs {
		key := distributed_execution.MakeFragmentKey(queryID, fragmentID)
		if entry, ok := f.mappings[key]; ok {
			addresses = append(addresses, entry.addr)
		} else {
			return nil, false
		}
	}
	return addresses, true
}

func (f *FragmentTable) GetChildAddr(queryID uint64, fragmentID uint64) (string, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	key := distributed_execution.MakeFragmentKey(queryID, fragmentID)
	if entry, ok := f.mappings[key]; ok {
		return entry.addr, true
	}
	return "", false
}

func (f *FragmentTable) ClearMappings(queryID uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()

	count := 0
	keysToDelete := make([]distributed_execution.FragmentKey, 0)
	for key := range f.mappings {
		if key.GetQueryID() == queryID {
			keysToDelete = append(keysToDelete, key)
			count++
		}
	}

	for _, key := range keysToDelete {
		delete(f.mappings, key)
	}
}

func (f *FragmentTable) cleanupExpired() {
	f.mu.Lock()
	defer f.mu.Unlock()

	now := time.Now()
	expiredCount := 0
	keysToDelete := make([]distributed_execution.FragmentKey, 0)

	for key, entry := range f.mappings {
		if now.Sub(entry.createdAt) > f.expiration {
			keysToDelete = append(keysToDelete, key)
			expiredCount++
		}
	}

	for _, key := range keysToDelete {
		delete(f.mappings, key)
	}
}

func (f *FragmentTable) periodicCleanup() {
	ticker := time.NewTicker(f.expiration / 2)
	defer ticker.Stop()

	for range ticker.C {
		f.cleanupExpired()
	}
}
