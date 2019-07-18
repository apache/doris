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

    // assert(new_list);
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

    //assert(_elems == count);
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
        _in_use.next = &_in_use;
        _in_use.prev = &_in_use;
    }

LRUCache::~LRUCache() {
    assert(_in_use.next == &_in_use);  // Error if caller has an unreleased handle
    for (LRUHandle* e = _lru.next; e != &_lru;) {
        LRUHandle* next = e->next;
        assert(e->in_cache);
        e->in_cache = false;
        assert(e->refs == 1);  // Invariant of _lru list.
        _unref(e);
        e = next;
    }
}

void LRUCache::_ref(LRUHandle* e) {
    if (e->refs == 1 && e->in_cache) {  // If on _lru list, move to _in_use list.
        _lru_remove(e);
        _lru_append(&_in_use, e);
    }
    e->refs++;
}

void LRUCache::_unref(LRUHandle* e) {
    // assert(e->refs > 0);
    if (e->refs <= 0) {
        LOG(FATAL) << "e->refs > 0, i do not know why, anyway, is something wrong."
                   << "e->refs=" << e->refs;
        return;
    }
    e->refs--;
    if (e->refs == 0) { // Deallocate.
        assert(!e->in_cache);
        (*e->deleter)(e->key(), e->value);
        free(e);
    } else if (e->in_cache && e->refs == 1) {  // No longer in use; move to lru_ list.
        _lru_remove(e);
        _lru_append(&_lru, e);
    }
}

void LRUCache::_lru_remove(LRUHandle* e) {
    e->next->prev = e->prev;
    e->prev->next = e->next;
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
    LRUHandle* e = _tablet.lookup(key, hash);

    if (e != NULL) {
        ++_hit_count;
        _ref(e);
    }

    return reinterpret_cast<Cache::Handle*>(e);
}

void LRUCache::release(Cache::Handle* handle) {
    MutexLock l(&_mutex);
    _unref(reinterpret_cast<LRUHandle*>(handle));
}

Cache::Handle* LRUCache::insert(
        const CacheKey& key, uint32_t hash, void* value, size_t charge,
        void (*deleter)(const CacheKey& key, void* value)) {
    MutexLock l(&_mutex);

    LRUHandle* e = reinterpret_cast<LRUHandle*>(
            malloc(sizeof(LRUHandle)-1 + key.size()));
    e->value = value;
    e->deleter = deleter;
    e->charge = charge;
    e->key_length = key.size();
    e->hash = hash;
    e->in_cache = false;
    e->refs = 1;  // for the returned handle.
    memcpy(e->key_data, key.data(), key.size());

    if (_capacity > 0) {
        e->refs++;  // for the cache's reference.
        e->in_cache = true;
        _lru_append(&_in_use, e);
        _usage += charge;
        _finish_erase(_tablet.insert(e));
    } // else don't cache.  (Tests use capacity_==0 to turn off caching.)

    while (_usage > _capacity && _lru.next != &_lru) {
        LRUHandle* old = _lru.next;
        assert(old->refs == 1);
        bool erased = _finish_erase(_tablet.remove(old->key(), old->hash));
        if (!erased) {  // to avoid unused variable when compiled NDEBUG
            assert(erased);
        }
    }

    return reinterpret_cast<Cache::Handle*>(e);
}

// If e != NULL, finish removing *e from the cache; it has already been removed
// from the hash tablet.  Return whether e != NULL.  Requires mutex_ held.
bool LRUCache::_finish_erase(LRUHandle* e) {
    if (e != NULL) {
        assert(e->in_cache);
        _lru_remove(e);
        e->in_cache = false;
        _usage -= e->charge;
        _unref(e);
    }
    return e != NULL;
}

void LRUCache::erase(const CacheKey& key, uint32_t hash) {
    MutexLock l(&_mutex);
    _finish_erase(_tablet.remove(key, hash));
}

int LRUCache::prune() {
    MutexLock l(&_mutex);
    int num_prune = 0;
    while (_lru.next != &_lru) {
        LRUHandle* e = _lru.next;
        assert(e->refs == 1);
        bool erased = _finish_erase(_tablet.remove(e->key(), e->hash));
        if (!erased) {  // to avoid unused variable when compiled NDEBUG
            assert(erased);
        }
        num_prune++;
    }
    return num_prune;
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
        void (*deleter)(const CacheKey& key, void* value)) {
    const uint32_t hash = _hash_slice(key);
    return _shards[_shard(hash)].insert(key, hash, value, charge, deleter);
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

uint64_t ShardedLRUCache::new_id() {
    MutexLock l(&_id_mutex);
    return ++(_last_id);
}

void ShardedLRUCache::prune() {
    int num_prune = 0;
    for (int s = 0; s < kNumShards; s++) {
        num_prune += _shards[s].prune();
    }
    LOG(INFO) << "prune file descriptor:" <<  num_prune;
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
