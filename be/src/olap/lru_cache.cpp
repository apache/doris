// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "olap/lru_cache.h"

#include <rapidjson/document.h>
#include <stdio.h>
#include <stdlib.h>

#include <sstream>
#include <string>

#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/olap_index.h"
#include "olap/row_block.h"
#include "olap/utils.h"
#include "util/doris_metrics.h"

using std::string;
using std::stringstream;

namespace doris {

DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(capacity, MetricUnit::BYTES);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(usage, MetricUnit::BYTES);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(usage_ratio, MetricUnit::NOUNIT);
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(lookup_count, MetricUnit::OPERATIONS);
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(hit_count, MetricUnit::OPERATIONS);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(hit_ratio, MetricUnit::NOUNIT);

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

Cache::~Cache() {}

HandleTable::~HandleTable() {
    for (uint32_t i = 0; i < _length; i++) {
        delete _list[i];
    }
    delete[] _list;
}

// LRU cache implementation
LRUHandle* HandleTable::lookup(const CacheKey& key, uint32_t hash) {
    return *_find_pointer(key, hash);
}

LRUHandle* HandleTable::insert(LRUHandle* h) {
    LRUHandle* old = remove(h->key(), h->hash);
    LRUHandle* head = _list[h->hash & (_length - 1)];

    _head_insert(head, h);
    ++_elems;

    if (old == nullptr) {
        if (_elems > _length) {
            // Since each cache entry is fairly large, we aim for a small
            // average linked list length (<= 1).
            _resize();
        }
    }

    return old;
}

LRUHandle* HandleTable::remove(const CacheKey& key, uint32_t hash) {
    LRUHandle** ptr = _find_pointer(key, hash);
    LRUHandle* result = *ptr;

    remove(result);

    return result;
}

void HandleTable::remove(const LRUHandle* h) {
    if (h != nullptr) {
        if (h->next_hash != nullptr) {
            h->next_hash->prev_hash = h->prev_hash;
        }
        DCHECK(h->prev_hash != nullptr);
        h->prev_hash->next_hash = h->next_hash;
        --_elems;
    }
}

LRUHandle** HandleTable::_find_pointer(const CacheKey& key, uint32_t hash) {
    LRUHandle** ptr = &(_list[hash & (_length - 1)]->next_hash);
    while (*ptr != nullptr && ((*ptr)->hash != hash || key != (*ptr)->key())) {
        ptr = &(*ptr)->next_hash;
    }

    return ptr;
}

void HandleTable::_head_insert(LRUHandle* head, LRUHandle* handle) {
    handle->next_hash = head->next_hash;
    if (handle->next_hash != nullptr) {
        handle->next_hash->prev_hash = handle;
    }
    handle->prev_hash = head;
    head->next_hash = handle;
}

void HandleTable::_resize() {
    uint32_t new_length = 16;
    while (new_length < _elems * 1.5) {
        new_length *= 2;
    }

    LRUHandle** new_list = new (std::nothrow) LRUHandle*[new_length];
    memset(new_list, 0, sizeof(new_list[0]) * new_length);
    for (uint32_t i = 0; i < new_length; i++) {
        // The first node in the linked-list is a dummy node used for
        // inserting new node mainly.
        new_list[i] = new LRUHandle();
    }

    uint32_t count = 0;
    for (uint32_t i = 0; i < _length; i++) {
        LRUHandle* h = _list[i]->next_hash;
        while (h != nullptr) {
            LRUHandle* next = h->next_hash;
            uint32_t hash = h->hash;
            LRUHandle* head = new_list[hash & (new_length - 1)];
            _head_insert(head, h);
            h = next;
            count++;
        }
    }

    DCHECK_EQ(_elems, count);
    for (uint32_t i = 0; i < _length; i++) {
        delete _list[i];
    }
    delete[] _list;
    _list = new_list;
    _length = new_length;
}

LRUCache::LRUCache(LRUCacheType type) : _type(type) {
    // Make empty circular linked list
    _lru_normal.next = &_lru_normal;
    _lru_normal.prev = &_lru_normal;
    _lru_durable.next = &_lru_durable;
    _lru_durable.prev = &_lru_durable;
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
            _usage -= e->total_size;
        } else if (e->in_cache && e->refs == 1) {
            // only exists in cache
            if (_usage > _capacity) {
                // take this opportunity and remove the item
                _table.remove(e);
                e->in_cache = false;
                _unref(e);
                _usage -= e->total_size;
                last_ref = true;
            } else {
                // put it to LRU free list
                if (e->priority == CachePriority::NORMAL) {
                    _lru_append(&_lru_normal, e);
                } else if (e->priority == CachePriority::DURABLE) {
                    _lru_append(&_lru_durable, e);
                }
            }
        }
    }

    // free handle out of mutex
    if (last_ref) {
        e->free();
    }
}

void LRUCache::_evict_from_lru(size_t total_size, LRUHandle** to_remove_head) {
    // 1. evict normal cache entries
    while (_usage + total_size > _capacity && _lru_normal.next != &_lru_normal) {
        LRUHandle* old = _lru_normal.next;
        DCHECK(old->priority == CachePriority::NORMAL);
        _evict_one_entry(old);
        old->next = *to_remove_head;
        *to_remove_head = old;
    }
    // 2. evict durable cache entries if need
    while (_usage + total_size > _capacity && _lru_durable.next != &_lru_durable) {
        LRUHandle* old = _lru_durable.next;
        DCHECK(old->priority == CachePriority::DURABLE);
        _evict_one_entry(old);
        old->next = *to_remove_head;
        *to_remove_head = old;
    }
}

void LRUCache::_evict_one_entry(LRUHandle* e) {
    DCHECK(e->in_cache);
    DCHECK(e->refs == 1); // LRU list contains elements which may be evicted
    _lru_remove(e);
    _table.remove(e);
    e->in_cache = false;
    _unref(e);
    _usage -= e->total_size;
}

Cache::Handle* LRUCache::insert(const CacheKey& key, uint32_t hash, void* value, size_t charge,
                                void (*deleter)(const CacheKey& key, void* value),
                                CachePriority priority) {
    size_t handle_size = sizeof(LRUHandle) - 1 + key.size();
    LRUHandle* e = reinterpret_cast<LRUHandle*>(malloc(handle_size));
    e->value = value;
    e->deleter = deleter;
    e->charge = charge;
    e->key_length = key.size();
    e->total_size = (_type == LRUCacheType::SIZE ? handle_size + charge : 1);
    e->hash = hash;
    e->refs = 2; // one for the returned handle, one for LRUCache.
    e->next = e->prev = nullptr;
    e->in_cache = true;
    e->priority = priority;
    memcpy(e->key_data, key.data(), key.size());
    LRUHandle* to_remove_head = nullptr;
    {
        MutexLock l(&_mutex);

        // Free the space following strict LRU policy until enough space
        // is freed or the lru list is empty
        _evict_from_lru(e->total_size, &to_remove_head);

        // insert into the cache
        // note that the cache might get larger than its capacity if not enough
        // space was freed
        auto old = _table.insert(e);
        _usage += e->total_size;
        if (old != nullptr) {
            old->in_cache = false;
            if (_unref(old)) {
                _usage -= old->total_size;
                // old is on LRU because it's in cache and its reference count
                // was just 1 (Unref returned 0)
                _lru_remove(old);
                old->next = to_remove_head;
                to_remove_head = old;
            }
        }
    }

    // we free the entries here outside of mutex for
    // performance reasons
    while (to_remove_head != nullptr) {
        LRUHandle* next = to_remove_head->next;
        to_remove_head->free();
        to_remove_head = next;
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
                _usage -= e->total_size;
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

int64_t LRUCache::prune() {
    LRUHandle* to_remove_head = nullptr;
    {
        MutexLock l(&_mutex);
        while (_lru_normal.next != &_lru_normal) {
            LRUHandle* old = _lru_normal.next;
            _evict_one_entry(old);
            old->next = to_remove_head;
            to_remove_head = old;
        }
        while (_lru_durable.next != &_lru_durable) {
            LRUHandle* old = _lru_durable.next;
            _evict_one_entry(old);
            old->next = to_remove_head;
            to_remove_head = old;
        }
    }
    int64_t pruned_count = 0;
    while (to_remove_head != nullptr) {
        ++pruned_count;
        LRUHandle* next = to_remove_head->next;
        to_remove_head->free();
        to_remove_head = next;
    }
    return pruned_count;
}

int64_t LRUCache::prune_if(CacheValuePredicate pred) {
    LRUHandle* to_remove_head = nullptr;
    {
        MutexLock l(&_mutex);
        LRUHandle* p = _lru_normal.next;
        while (p != &_lru_normal) {
            LRUHandle* next = p->next;
            if (pred(p->value)) {
                _evict_one_entry(p);
                p->next = to_remove_head;
                to_remove_head = p;
            }
            p = next;
        }

        p = _lru_durable.next;
        while (p != &_lru_durable) {
            LRUHandle* next = p->next;
            if (pred(p->value)) {
                _evict_one_entry(p);
                p->next = to_remove_head;
                to_remove_head = p;
            }
            p = next;
        }
    }
    int64_t pruned_count = 0;
    while (to_remove_head != nullptr) {
        ++pruned_count;
        LRUHandle* next = to_remove_head->next;
        to_remove_head->free();
        to_remove_head = next;
    }
    return pruned_count;
}

inline uint32_t ShardedLRUCache::_hash_slice(const CacheKey& s) {
    return s.hash(s.data(), s.size(), 0);
}

uint32_t ShardedLRUCache::_shard(uint32_t hash) {
    return hash >> (32 - kNumShardBits);
}

ShardedLRUCache::ShardedLRUCache(const std::string& name, size_t total_capacity, LRUCacheType type,
                                 std::shared_ptr<MemTracker> parent)
        : _name(name),
          _last_id(1),
          _mem_tracker(MemTracker::CreateTracker(-1, name, parent, true, false,
                                                 MemTrackerLevel::OVERVIEW)) {
    const size_t per_shard = (total_capacity + (kNumShards - 1)) / kNumShards;
    for (int s = 0; s < kNumShards; s++) {
        _shards[s] = new LRUCache(type);
        _shards[s]->set_capacity(per_shard);
    }

    _entity = DorisMetrics::instance()->metric_registry()->register_entity(
            std::string("lru_cache:") + name, {{"name", name}});
    _entity->register_hook(name, std::bind(&ShardedLRUCache::update_cache_metrics, this));
    INT_GAUGE_METRIC_REGISTER(_entity, capacity);
    INT_GAUGE_METRIC_REGISTER(_entity, usage);
    INT_DOUBLE_METRIC_REGISTER(_entity, usage_ratio);
    INT_ATOMIC_COUNTER_METRIC_REGISTER(_entity, lookup_count);
    INT_ATOMIC_COUNTER_METRIC_REGISTER(_entity, hit_count);
    INT_DOUBLE_METRIC_REGISTER(_entity, hit_ratio);
}

ShardedLRUCache::~ShardedLRUCache() {
    for (int s = 0; s < kNumShards; s++) {
        delete _shards[s];
    }
    _entity->deregister_hook(_name);
    DorisMetrics::instance()->metric_registry()->deregister_entity(_entity);
    _mem_tracker->Release(_mem_tracker->consumption());
}

Cache::Handle* ShardedLRUCache::insert(const CacheKey& key, void* value, size_t charge,
                                       void (*deleter)(const CacheKey& key, void* value),
                                       CachePriority priority) {
    const uint32_t hash = _hash_slice(key);
    return _shards[_shard(hash)]->insert(key, hash, value, charge, deleter, priority);
}

Cache::Handle* ShardedLRUCache::lookup(const CacheKey& key) {
    const uint32_t hash = _hash_slice(key);
    return _shards[_shard(hash)]->lookup(key, hash);
}

void ShardedLRUCache::release(Handle* handle) {
    LRUHandle* h = reinterpret_cast<LRUHandle*>(handle);
    _shards[_shard(h->hash)]->release(handle);
}

void ShardedLRUCache::erase(const CacheKey& key) {
    const uint32_t hash = _hash_slice(key);
    _shards[_shard(hash)]->erase(key, hash);
}

void* ShardedLRUCache::value(Handle* handle) {
    return reinterpret_cast<LRUHandle*>(handle)->value;
}

Slice ShardedLRUCache::value_slice(Handle* handle) {
    auto lru_handle = reinterpret_cast<LRUHandle*>(handle);
    return Slice((char*)lru_handle->value, lru_handle->charge);
}

uint64_t ShardedLRUCache::new_id() {
    return _last_id.fetch_add(1, std::memory_order_relaxed);
}

int64_t ShardedLRUCache::prune() {
    int64_t num_prune = 0;
    for (int s = 0; s < kNumShards; s++) {
        num_prune += _shards[s]->prune();
    }
    return num_prune;
}

int64_t ShardedLRUCache::prune_if(CacheValuePredicate pred) {
    int64_t num_prune = 0;
    for (int s = 0; s < kNumShards; s++) {
        num_prune += _shards[s]->prune_if(pred);
    }
    return num_prune;
}

void ShardedLRUCache::update_cache_metrics() const {
    size_t total_capacity = 0;
    size_t total_usage = 0;
    size_t total_lookup_count = 0;
    size_t total_hit_count = 0;
    for (int i = 0; i < kNumShards; i++) {
        total_capacity += _shards[i]->get_capacity();
        total_usage += _shards[i]->get_usage();
        total_lookup_count += _shards[i]->get_lookup_count();
        total_hit_count += _shards[i]->get_hit_count();
    }

    capacity->set_value(total_capacity);
    usage->set_value(total_usage);
    lookup_count->set_value(total_lookup_count);
    hit_count->set_value(total_hit_count);
    usage_ratio->set_value(total_capacity == 0 ? 0 : ((double)total_usage / total_capacity));
    hit_ratio->set_value(total_lookup_count == 0 ? 0
                                                 : ((double)total_hit_count / total_lookup_count));

    _mem_tracker->Consume(total_usage - _mem_tracker->consumption());
}

Cache* new_lru_cache(const std::string& name, size_t capacity,
                     std::shared_ptr<MemTracker> parent_tracker) {
    return new ShardedLRUCache(name, capacity, LRUCacheType::SIZE, parent_tracker);
}

Cache* new_typed_lru_cache(const std::string& name, size_t capacity, LRUCacheType type,
                           std::shared_ptr<MemTracker> parent_tracker) {
    return new ShardedLRUCache(name, capacity, type, parent_tracker);
}

} // namespace doris
