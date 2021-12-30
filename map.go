package smap

import (
	"crypto/sha1"
	"sync"
)

type Shard struct {
	sync.RWMutex
	m map[string]interface{}
}

type SharedMap []*Shard

func (m SharedMap) Get(key string) interface{} {
	shard := m.getShard(key)
	shard.RLock()
	defer shard.RUnlock()

	return shard.m[key]
}

func (m SharedMap) Set(key string, val interface{}) {
	shard := m.getShard(key)
	shard.Lock()
	defer shard.Unlock()

	shard.m[key] = val
}

func (m SharedMap) Delete(key string) {
	shard := m.getShard(key)
	shard.Lock()
	defer shard.Unlock()

	delete(shard.m, key)
}

func (m SharedMap) Keys() []string {
	keys := make([]string, 0)
	mutex := sync.Mutex{}
	wg := sync.WaitGroup{}
	wg.Add(len(m))

	for _, s := range m {
		go func(shard *Shard) {
			shard.RLock()

			for key := range shard.m {
				mutex.Lock()
				keys = append(keys, key)
				mutex.Unlock()
			}

			shard.RUnlock()
			wg.Done()
		}(s)
	}

	wg.Wait()

	return keys
}

func (m SharedMap) getShardIndex(key string) int {
	checksum := sha1.Sum([]byte(key))
	hash := int(checksum[17])

	return hash % len(m)
}

func (m SharedMap) getShard(key string) *Shard {
	i := m.getShardIndex(key)

	return m[i]
}
