// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "olap/lru_cache.h"

#include <stdio.h>
#include <stdlib.h>

#include <sstream>
#include <string>

#include <rapidjson/document.h>

#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/olap_index.h"
#include "olap/row_block.h"
#include "olap/utils.h"

using std::string;
using std::stringstream;

namespace doris {

uint32_t CacheKey::hash(const char* data, size_t n, uint32_t seed) const {
    // Similar to murmur hash
    const uint32_t m = 0xc6a4a793;
    const uint32_t r = 24;
    const char* limit = data + n;
    uint32_t h = seed ^ (n * m);

    // Pick up four bytes at a time
    while (data + 4 <= limit) {
        uint32_t w = _decode_fixed32(data);
        data += 4;
        h += w;
        h *= m;
        h ^= (h >> 16);
    }

    // Pick up remaining bytes
    switch (limit - data) {
        case 3:
            h += static_cast<unsigned char>(data[2]) << 16;

            // fall through
        case 2:
            h += static_cast<unsigned char>(data[1]) << 8;

            // fall through
        case 1:
            h += static_cast<unsigned char>(data[0]);
            h *= m;
            h ^= (h >> r);
            break;

        default:
            break;
    }

    return h;
}

Cache::~Cache() {
}

// LRU cache implementation
LRUHandle* HandleTable::lookup(const CacheKey& key, uint32_t hash) {
    return *_find_pointer(key, hash);
}

LRUHandle* HandleTable::insert(LRUHandle* h) {
    LRUHandle** ptr = _find_pointer(h->key(), h->hash);
    LRUHandle* old = *ptr;
    h->next_hash = (old == NULL ? NULL : old->next_hash);
    *ptr = h;

    if (old == NULL) {
        ++_elems;

        if (_elems > _length) {
            // Since each cache entry is fairly large, we aim for a small
            // average linked list length (<= 1).
            if (!_resize()) {
                return NULL;
            }
        }
    }

    return old;
}

LRUHandle* HandleTable::remove(const CacheKey& key, uint32_t hash) {
    LRUHandle** ptr = _find_pointer(key, hash);
    LRUHandle* result = *ptr;

    if (result != NULL) {
        *ptr = result->next_hash;
        --_elems;
    }

    return result;
}

LRUHandle** HandleTable::_find_pointer(const CacheKey& key, uint32_t hash) {
    LRUHandle** ptr = &_list[hash & (_length - 1)];

    while (*ptr != NULL &&
            ((*ptr)->hash != hash || key != (*ptr)->key())) {
        ptr = &(*ptr)->next_hash;
    }

    return ptr;
}

bool HandleTable::_resize() {
    uint32_t new_length = 4;

    while (new_length < _elems) {
        new_length *= 2;
    }

    LRUHandle** new_list = new(std::nothrow) LRUHandle*[new_length];

    if (NULL == new_list) {
        LOG(FATAL) << "failed to malloc new hash list. new_length=" << new_length;
        return false;
    }

    memset(new_list, 0, sizeof(new_list[0]) * new_length);
    uint32_t count = 0;

    for (uint32_t i = 0; i < _length; i++) {
        LRUHandle* h = _list[i];

        while (h != NULL) {
            LRUHandle* next = h->next_hash;
            CacheKey key = h->key();
            uint32_t hash = h->hash;
            LRUHandle** ptr = &new_list[hash & (new_length - 1)];
            h->next_hash = *ptr;
            *ptr = h;
            h = next;
            count++;
        }
    }

    if (_elems != count) {
        delete [] new_list;
        LOG(FATAL) << "_elems not match new count. elems=" << _elems
                   << ", count=" << count;
        return false;
    }

    delete [] _list;
    _list = new_list;
    _length = new_length;
    return true;
}

LRUCache::LRUCache() : _usage(0), _last_id(0), _lookup_count(0),
    _hit_count(0) {
        // Make empty circular linked list
        _lru.next = &_lru;
        _lru.prev = &_lru;
    }

LRUCache::~LRUCache() {
    prune();
}

bool LRUCache::_unref(LRUHandle* e) {
    DCHECK(e->refs > 0);
    e->refs--;
    return e->refs == 0;
}

void LRUCache::_lru_remove(LRUHandle* e) {
    e->next->prev = e->prev;
    e->prev->next = e->next;
    e->prev = e->next = nullptr;
}

void LRUCache::_lru_append(LRUHandle* list, LRUHandle* e) {
    // Make "e" newest entry by inserting just before *list
    e->next = list;
    e->prev = list->prev;
    e->prev->next = e;
    e->next->prev = e;
}

Cache::Handle* LRUCache::lookup(const CacheKey& key, uint32_t hash) {
    MutexLock l(&_mutex);
    ++_lookup_count;
    LRUHandle* e = _table.lookup(key, hash);
    if (e != nullptr) {
        // we get it from _table, so in_cache must be true
        DCHECK(e->in_cache);
        if (e->refs == 1) {
            // only in LRU free list, remove it from list
            _lru_remove(e);
        }
        e->refs++;
        ++_hit_count;
    }
    return reinterpret_cast<Cache::Handle*>(e);
}

void LRUCache::release(Cache::Handle* handle) {
    if (handle == nullptr) {
        return;
    }
    LRUHandle* e = reinterpret_cast<LRUHandle*>(handle);
    bool last_ref = false;
    {
        MutexLock l(&_mutex);
        last_ref = _unref(e);
        if (last_ref) {
            _usage -= e->charge;
        } else if (e->in_cache && e->refs == 1) {
            // only exists in cache
            if (_usage > _capacity) {
                // take this opportunity and remove the item
                _table.remove(e->key(), e->hash);
                e->in_cache = false;
                _unref(e);
                _usage -= e->charge;
                last_ref = true;
            } else {
                // put it to LRU free list
                _lru_append(&_lru, e);
            }
        }
    }

    // free handle out of mutex
    if (last_ref) {
        e->free();
    }
}

void LRUCache::_evict_from_lru(size_t charge, std::vector<LRUHandle*>* deleted) {
    LRUHandle* cur = &_lru;
    // 1. evict normal cache entries
    while (_usage + charge > _capacity && cur->next != &_lru) {
        LRUHandle* old = cur->next;
        if (old->priority == CachePriority::DURABLE) {
            cur = cur->next;
            continue;
        }
        _evict_one_entry(old);
        deleted->push_back(old);
    }
    // 2. evict durable cache entries if need
    while (_usage + charge > _capacity && _lru.next != &_lru) {
        LRUHandle* old = _lru.next;
        DCHECK(old->priority == CachePriority::DURABLE);
        _evict_one_entry(old);
        deleted->push_back(old);
    }
}

void LRUCache::_evict_one_entry(LRUHandle* e) {
    DCHECK(e->in_cache);
    DCHECK(e->refs == 1); // LRU list contains elements which may be evicted
    _lru_remove(e);
    _table.remove(e->key(), e->hash);
    e->in_cache = false;
    _unref(e);
    _usage -= e->charge;
}

Cache::Handle* LRUCache::insert(
        const CacheKey& key, uint32_t hash, void* value, size_t charge,
        void (*deleter)(const CacheKey& key, void* value),
        CachePriority priority) {

    LRUHandle* e = reinterpret_cast<LRUHandle*>(
            malloc(sizeof(LRUHandle) - 1 + key.size()));
    e->value = value;
    e->deleter = deleter;
    e->charge = charge;
    e->key_length = key.size();
    e->hash = hash;
    e->refs = 2;  // one for the returned handle, one for LRUCache.
    e->next = e->prev = nullptr;
    e->in_cache = true;
    e->priority = priority;
    memcpy(e->key_data, key.data(), key.size());
    std::vector<LRUHandle*> last_ref_list;
    {
        MutexLock l(&_mutex);

        // Free the space following strict LRU policy until enough space
        // is freed or the lru list is empty
        _evict_from_lru(charge, &last_ref_list);

        // insert into the cache
        // note that the cache might get larger than its capacity if not enough
        // space was freed
        auto old = _table.insert(e);
        _usage += charge;
        if (old != nullptr) {
            old->in_cache = false;
            if (_unref(old)) {
                _usage -= old->charge;
                // old is on LRU because it's in cache and its reference count
                // was just 1 (Unref returned 0)
                _lru_remove(old);
                last_ref_list.push_back(old);
            }
        }
    }

    // we free the entries here outside of mutex for
    // performance reasons
    for (auto entry : last_ref_list) {
        entry->free();
    }

    return reinterpret_cast<Cache::Handle*>(e);
}

void LRUCache::erase(const CacheKey& key, uint32_t hash) {
    LRUHandle* e = nullptr;
    bool last_ref = false;
    {
        MutexLock l(&_mutex);
        e = _table.remove(key, hash);
        if (e != nullptr) {
            last_ref = _unref(e);
            if (last_ref) {
                _usage -= e->charge;
                if (e->in_cache) {
                    // locate in free list
                    _lru_remove(e);
                }
            }
            e->in_cache = false;
        }
    }
    // free handle out of mutex, when last_ref is true, e must not be nullptr
    if (last_ref) {
        e->free();
    }
}

int LRUCache::prune() {
    std::vector<LRUHandle*> last_ref_list;
    {
        MutexLock l(&_mutex);
        while (_lru.next != &_lru) {
            LRUHandle* old = _lru.next;
            DCHECK(old->in_cache);
            DCHECK(old->refs == 1);  // LRU list contains elements which may be evicted
            _lru_remove(old);
            _table.remove(old->key(), old->hash);
            old->in_cache = false;
            _unref(old);
            _usage -= old->charge;
            last_ref_list.push_back(old);
        }
    }
    for (auto entry : last_ref_list) {
        entry->free();
    }
    return last_ref_list.size();
}

inline uint32_t ShardedLRUCache::_hash_slice(const CacheKey& s) {
    return s.hash(s.data(), s.size(), 0);
}

uint32_t ShardedLRUCache::_shard(uint32_t hash) {
    return hash >> (32 - kNumShardBits);
}

ShardedLRUCache::ShardedLRUCache(size_t capacity)
    : _last_id(0) {
        const size_t per_shard = (capacity + (kNumShards - 1)) / kNumShards;

        for (int s = 0; s < kNumShards; s++) {
            _shards[s].set_capacity(per_shard);
        }
    }

Cache::Handle* ShardedLRUCache::insert(
        const CacheKey& key,
        void* value,
        size_t charge,
        void (*deleter)(const CacheKey& key, void* value),
        CachePriority priority) {
    const uint32_t hash = _hash_slice(key);
    return _shards[_shard(hash)].insert(key, hash, value, charge, deleter, priority);
}

Cache::Handle* ShardedLRUCache::lookup(const CacheKey& key) {
    const uint32_t hash = _hash_slice(key);
    return _shards[_shard(hash)].lookup(key, hash);
}

void ShardedLRUCache::release(Handle* handle) {
    LRUHandle* h = reinterpret_cast<LRUHandle*>(handle);
    _shards[_shard(h->hash)].release(handle);
}

void ShardedLRUCache::erase(const CacheKey& key) {
    const uint32_t hash = _hash_slice(key);
    _shards[_shard(hash)].erase(key, hash);
}

void* ShardedLRUCache::value(Handle* handle) {
    return reinterpret_cast<LRUHandle*>(handle)->value;
}

Slice ShardedLRUCache::value_slice(Handle* handle) {
    auto lru_handle = reinterpret_cast<LRUHandle*>(handle);
    return Slice((char*)lru_handle->value, lru_handle->charge);
}

uint64_t ShardedLRUCache::new_id() {
    MutexLock l(&_id_mutex);
    return ++(_last_id);
}

void ShardedLRUCache::prune() {
    int num_prune = 0;
    for (int s = 0; s < kNumShards; s++) {
        num_prune += _shards[s].prune();
    }
}

size_t ShardedLRUCache::get_memory_usage() {
    size_t total_usage = 0;
    for (int s = 0; s < kNumShards; s++) {
        total_usage += _shards[s].get_usage();
    }
    return total_usage;
}

void ShardedLRUCache::get_cache_status(rapidjson::Document* document) {
    size_t shard_count = sizeof(_shards) / sizeof(LRUCache);

    for (uint32_t i = 0; i < shard_count; ++i) {
        size_t capacity = _shards[i].get_capacity();
        size_t usage = _shards[i].get_usage();
        rapidjson::Value shard_info(rapidjson::kObjectType);
        shard_info.AddMember("capacity", static_cast<double>(capacity), document->GetAllocator());
        shard_info.AddMember("usage", static_cast<double>(usage), document->GetAllocator());

        float usage_ratio = 0.0f;

        if (0 != capacity) {
            usage_ratio = static_cast<float>(usage) / static_cast<float>(capacity);
        }

        shard_info.AddMember("usage_ratio", usage_ratio, document->GetAllocator());

        size_t lookup_count = _shards[i].get_lookup_count();
        size_t hit_count = _shards[i].get_hit_count();
        shard_info.AddMember("lookup_count", static_cast<double>(lookup_count), document->GetAllocator());
        shard_info.AddMember("hit_count", static_cast<double>(hit_count), document->GetAllocator());

        float hit_ratio = 0.0f;

        if (0 != lookup_count) {
            hit_ratio = static_cast<float>(hit_count) / static_cast<float>(lookup_count);
        }

        shard_info.AddMember("hit_ratio", hit_ratio, document->GetAllocator());
        document->PushBack(shard_info, document->GetAllocator());
    }

}

Cache* new_lru_cache(size_t capacity) {
    return new ShardedLRUCache(capacity);
}

}  // namespace doris
