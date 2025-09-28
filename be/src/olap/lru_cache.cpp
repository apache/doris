// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "olap/lru_cache.h"

#include <stdlib.h>

#include <mutex>
#include <new>
#include <sstream>
#include <string>

#include "gutil/bits.h"
#include "util/doris_metrics.h"
#include "util/time.h"

using std::string;
using std::stringstream;

namespace doris {

DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(cache_capacity, MetricUnit::BYTES);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(cache_usage, MetricUnit::BYTES);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(cache_element_count, MetricUnit::NOUNIT);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(cache_usage_ratio, MetricUnit::NOUNIT);
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(cache_lookup_count, MetricUnit::OPERATIONS);
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(cache_hit_count, MetricUnit::OPERATIONS);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(cache_hit_ratio, MetricUnit::NOUNIT);

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
    delete[] _list;
}

// LRU cache implementation
LRUHandle* HandleTable::lookup(const CacheKey& key, uint32_t hash) {
    return *_find_pointer(key, hash);
}

LRUHandle* HandleTable::insert(LRUHandle* h) {
    LRUHandle** ptr = _find_pointer(h->key(), h->hash);
    LRUHandle* old = *ptr;
    h->next_hash = old ? old->next_hash : nullptr;
    *ptr = h;

    if (old == nullptr) {
        ++_elems;
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

    if (result != nullptr) {
        *ptr = result->next_hash;
        _elems--;
    }

    return result;
}

bool HandleTable::remove(const LRUHandle* h) {
    LRUHandle** ptr = &(_list[h->hash & (_length - 1)]);
    while (*ptr != nullptr && *ptr != h) {
        ptr = &(*ptr)->next_hash;
    }

    LRUHandle* result = *ptr;
    if (result != nullptr) {
        *ptr = result->next_hash;
        _elems--;
        return true;
    }
    return false;
}

LRUHandle** HandleTable::_find_pointer(const CacheKey& key, uint32_t hash) {
    LRUHandle** ptr = &(_list[hash & (_length - 1)]);
    while (*ptr != nullptr && ((*ptr)->hash != hash || key != (*ptr)->key())) {
        ptr = &(*ptr)->next_hash;
    }

    return ptr;
}

void HandleTable::_resize() {
    uint32_t new_length = 16;
    while (new_length < _elems * 1.5) {
        new_length *= 2;
    }

    LRUHandle** new_list = new (std::nothrow) LRUHandle*[new_length];
    memset(new_list, 0, sizeof(new_list[0]) * new_length);

    uint32_t count = 0;
    for (uint32_t i = 0; i < _length; i++) {
        LRUHandle* h = _list[i];
        while (h != nullptr) {
            LRUHandle* next = h->next_hash;
            uint32_t hash = h->hash;
            LRUHandle** ptr = &new_list[hash & (new_length - 1)];
            h->next_hash = *ptr;
            *ptr = h;
            h = next;
            count++;
        }
    }

    DCHECK_EQ(_elems, count);
    delete[] _list;
    _list = new_list;
    _length = new_length;
}

uint32_t HandleTable::element_count() const {
    return _elems;
}

LRUCache::LRUCache(LRUCacheType type, bool is_lru_k) : _type(type), _is_lru_k(is_lru_k) {
    // Make empty circular linked list
    _lru_normal.next = &_lru_normal;
    _lru_normal.prev = &_lru_normal;
    _lru_durable.next = &_lru_durable;
    _lru_durable.prev = &_lru_durable;
}

LRUCache::~LRUCache() {
    prune();
}

PrunedInfo LRUCache::set_capacity(size_t capacity) {
    LRUHandle* last_ref_list = nullptr;
    {
        std::lock_guard l(_mutex);
        if (capacity > _capacity) {
            _capacity = capacity;
            return {0, 0};
        }
        _capacity = capacity;
        _evict_from_lru(0, &last_ref_list);
    }

    int64_t pruned_count = 0;
    int64_t pruned_size = 0;
    while (last_ref_list != nullptr) {
        ++pruned_count;
        pruned_size += last_ref_list->total_size;
        LRUHandle* next = last_ref_list->next;
        last_ref_list->free();
        last_ref_list = next;
    }
    return {pruned_count, pruned_size};
}

uint64_t LRUCache::get_lookup_count() {
    std::lock_guard l(_mutex);
    return _lookup_count;
}

uint64_t LRUCache::get_hit_count() {
    std::lock_guard l(_mutex);
    return _hit_count;
}

size_t LRUCache::get_usage() {
    std::lock_guard l(_mutex);
    return _usage;
}

size_t LRUCache::get_capacity() {
    std::lock_guard l(_mutex);
    return _capacity;
}

size_t LRUCache::get_element_count() {
    std::lock_guard l(_mutex);
    return _table.element_count();
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

    if (_cache_value_check_timestamp) {
        if (e->priority == CachePriority::NORMAL) {
            auto pair = std::make_pair(_cache_value_time_extractor(e->value), e);
            auto found_it = _sorted_normal_entries_with_timestamp.find(pair);
            if (found_it != _sorted_normal_entries_with_timestamp.end()) {
                _sorted_normal_entries_with_timestamp.erase(found_it);
            }
        } else if (e->priority == CachePriority::DURABLE) {
            auto pair = std::make_pair(_cache_value_time_extractor(e->value), e);
            auto found_it = _sorted_durable_entries_with_timestamp.find(pair);
            if (found_it != _sorted_durable_entries_with_timestamp.end()) {
                _sorted_durable_entries_with_timestamp.erase(found_it);
            }
        }
    }
}

void LRUCache::_lru_append(LRUHandle* list, LRUHandle* e) {
    // Make "e" newest entry by inserting just before *list
    e->next = list;
    e->prev = list->prev;
    e->prev->next = e;
    e->next->prev = e;

    // _cache_value_check_timestamp is true,
    // means evict entry will depends on the timestamp asc set,
    // the timestamp is updated by higher level caller,
    // and the timestamp of hit entry is different with the insert entry,
    // that is why need check timestamp to evict entry,
    // in order to keep the survival time of hit entries
    // longer than the entries just inserted,
    // so use asc set to sorted these entries's timestamp and LRUHandle*
    if (_cache_value_check_timestamp) {
        if (e->priority == CachePriority::NORMAL) {
            _sorted_normal_entries_with_timestamp.insert(
                    std::make_pair(_cache_value_time_extractor(e->value), e));
        } else if (e->priority == CachePriority::DURABLE) {
            _sorted_durable_entries_with_timestamp.insert(
                    std::make_pair(_cache_value_time_extractor(e->value), e));
        }
    }
}

Cache::Handle* LRUCache::lookup(const CacheKey& key, uint32_t hash) {
    std::lock_guard l(_mutex);
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
        e->last_visit_time = UnixMillis();
    }

    // If key not exist in cache, and is lru k cache, and key in visits list,
    // then move the key to beginning of the visits list.
    // key in visits list indicates that the key has been inserted once after the cache is full.
    if (e == nullptr && _is_lru_k) {
        auto it = _visits_lru_cache_map.find(hash);
        if (it != _visits_lru_cache_map.end()) {
            _visits_lru_cache_list.splice(_visits_lru_cache_list.begin(), _visits_lru_cache_list,
                                          it->second);
        }
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
        std::lock_guard l(_mutex);
        // if last_ref is true, key may have been evict from the cache,
        // or if it is lru k, first insert of key may have failed.
        last_ref = _unref(e);
        if (e->in_cache && e->refs == 1) {
            // only exists in cache
            if (_usage > _capacity) {
                // take this opportunity and remove the item
                bool removed = _table.remove(e);
                DCHECK(removed);
                e->in_cache = false;
                _unref(e);
                // `entry->in_cache = false` and `_usage -= entry->total_size;` and `_unref(entry)` should appear together.
                // see the comment for old entry in `LRUCache::insert`.
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

void LRUCache::_evict_from_lru_with_time(size_t total_size, LRUHandle** to_remove_head) {
    // 1. evict normal cache entries
    while ((_usage + total_size > _capacity || _check_element_count_limit()) &&
           !_sorted_normal_entries_with_timestamp.empty()) {
        auto entry_pair = _sorted_normal_entries_with_timestamp.begin();
        LRUHandle* remove_handle = entry_pair->second;
        DCHECK(remove_handle != nullptr);
        DCHECK(remove_handle->priority == CachePriority::NORMAL);
        _evict_one_entry(remove_handle);
        remove_handle->next = *to_remove_head;
        *to_remove_head = remove_handle;
    }

    // 2. evict durable cache entries if need
    while ((_usage + total_size > _capacity || _check_element_count_limit()) &&
           !_sorted_durable_entries_with_timestamp.empty()) {
        auto entry_pair = _sorted_durable_entries_with_timestamp.begin();
        LRUHandle* remove_handle = entry_pair->second;
        DCHECK(remove_handle != nullptr);
        DCHECK(remove_handle->priority == CachePriority::DURABLE);
        _evict_one_entry(remove_handle);
        remove_handle->next = *to_remove_head;
        *to_remove_head = remove_handle;
    }
}

void LRUCache::_evict_from_lru(size_t total_size, LRUHandle** to_remove_head) {
    // 1. evict normal cache entries
    while ((_usage + total_size > _capacity || _check_element_count_limit()) &&
           _lru_normal.next != &_lru_normal) {
        LRUHandle* old = _lru_normal.next;
        DCHECK(old->priority == CachePriority::NORMAL);
        _evict_one_entry(old);
        old->next = *to_remove_head;
        *to_remove_head = old;
    }
    // 2. evict durable cache entries if need
    while ((_usage + total_size > _capacity || _check_element_count_limit()) &&
           _lru_durable.next != &_lru_durable) {
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
    bool removed = _table.remove(e);
    DCHECK(removed);
    e->in_cache = false;
    _unref(e);
    // `entry->in_cache = false` and `_usage -= entry->total_size;` and `_unref(entry)` should appear together.
    // see the comment for old entry in `LRUCache::insert`.
    _usage -= e->total_size;
}

bool LRUCache::_check_element_count_limit() {
    return _element_count_capacity != 0 && _table.element_count() >= _element_count_capacity;
}

// After cache is full,
// 1.Return false. If key has been inserted into the visits list before,
// key is allowed to be inserted into cache this time (this will trigger cache evict),
// and key is removed from the visits list.
// 2. Return true. If key not in visits list, insert it into visits list.
bool LRUCache::_lru_k_insert_visits_list(size_t total_size, visits_lru_cache_key visits_key) {
    if (_usage + total_size > _capacity ||
        _check_element_count_limit()) { // this line no lock required
        auto it = _visits_lru_cache_map.find(visits_key);
        if (it != _visits_lru_cache_map.end()) {
            _visits_lru_cache_usage -= it->second->second;
            _visits_lru_cache_list.erase(it->second);
            _visits_lru_cache_map.erase(it);
        } else {
            // _visits_lru_cache_list capacity is same as the cache itself.
            // If _visits_lru_cache_list is full, some keys will also be evict.
            while (_visits_lru_cache_usage + total_size > _capacity &&
                   _visits_lru_cache_usage != 0) {
                DCHECK(!_visits_lru_cache_map.empty());
                _visits_lru_cache_usage -= _visits_lru_cache_list.back().second;
                _visits_lru_cache_map.erase(_visits_lru_cache_list.back().first);
                _visits_lru_cache_list.pop_back();
            }
            // 1. If true, insert key at the beginning of _visits_lru_cache_list.
            // 2. If false, it means total_size > cache _capacity, preventing this insert.
            if (_visits_lru_cache_usage + total_size <= _capacity) {
                _visits_lru_cache_list.emplace_front(visits_key, total_size);
                _visits_lru_cache_map[visits_key] = _visits_lru_cache_list.begin();
                _visits_lru_cache_usage += total_size;
            }
            return true;
        }
    }
    return false;
}

Cache::Handle* LRUCache::insert(const CacheKey& key, uint32_t hash, void* value, size_t charge,
                                CachePriority priority) {
    size_t handle_size = sizeof(LRUHandle) - 1 + key.size();
    auto* e = reinterpret_cast<LRUHandle*>(malloc(handle_size));
    e->value = value;
    e->charge = charge;
    e->key_length = key.size();
    // if LRUCacheType::NUMBER, charge not add handle_size,
    // because charge at this time is no longer the memory size, but an weight.
    e->total_size = (_type == LRUCacheType::SIZE ? handle_size + charge : charge);
    e->hash = hash;
    e->refs = 1; // only one for the returned handle.
    e->next = e->prev = nullptr;
    e->in_cache = false;
    e->priority = priority;
    e->type = _type;
    memcpy(e->key_data, key.data(), key.size());
    e->last_visit_time = UnixMillis();

    LRUHandle* to_remove_head = nullptr;
    {
        std::lock_guard l(_mutex);

        if (_is_lru_k && _lru_k_insert_visits_list(e->total_size, hash)) {
            return reinterpret_cast<Cache::Handle*>(e);
        }

        // Free the space following strict LRU policy until enough space
        // is freed or the lru list is empty
        if (_cache_value_check_timestamp) {
            _evict_from_lru_with_time(e->total_size, &to_remove_head);
        } else {
            _evict_from_lru(e->total_size, &to_remove_head);
        }

        // insert into the cache
        // note that the cache might get larger than its capacity if not enough
        // space was freed
        auto* old = _table.insert(e);
        e->in_cache = true;
        _usage += e->total_size;
        e->refs++; // one for the returned handle, one for LRUCache.
        if (old != nullptr) {
            old->in_cache = false;
            // `entry->in_cache = false` and `_usage -= entry->total_size;` and `_unref(entry)` should appear together.
            // Whether the reference of the old entry is 0, the cache usage is subtracted here,
            // because the old entry has been removed from the cache and should not be counted in the cache capacity,
            // but the memory of the old entry is still tracked by the cache memory_tracker.
            // After all the old handles are released, the old entry will be freed and the memory of the old entry
            // will be released from the cache memory_tracker.
            _usage -= old->total_size;
            // if false, old entry is being used externally, just ref-- and sub _usage,
            if (_unref(old)) {
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
        std::lock_guard l(_mutex);
        e = _table.remove(key, hash);
        if (e != nullptr) {
            last_ref = _unref(e);
            // if last_ref is false or in_cache is false, e must not be in lru
            if (last_ref && e->in_cache) {
                // locate in free list
                _lru_remove(e);
            }
            e->in_cache = false;
            // `entry->in_cache = false` and `_usage -= entry->total_size;` and `_unref(entry)` should appear together.
            // see the comment for old entry in `LRUCache::insert`.
            _usage -= e->total_size;
        }
    }
    // free handle out of mutex, when last_ref is true, e must not be nullptr
    if (last_ref) {
        e->free();
    }
}

PrunedInfo LRUCache::prune() {
    LRUHandle* to_remove_head = nullptr;
    {
        std::lock_guard l(_mutex);
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
    int64_t pruned_size = 0;
    while (to_remove_head != nullptr) {
        ++pruned_count;
        pruned_size += to_remove_head->total_size;
        LRUHandle* next = to_remove_head->next;
        to_remove_head->free();
        to_remove_head = next;
    }
    return {pruned_count, pruned_size};
}

PrunedInfo LRUCache::prune_if(CachePrunePredicate pred, bool lazy_mode) {
    LRUHandle* to_remove_head = nullptr;
    {
        std::lock_guard l(_mutex);
        LRUHandle* p = _lru_normal.next;
        while (p != &_lru_normal) {
            LRUHandle* next = p->next;
            if (pred(p)) {
                _evict_one_entry(p);
                p->next = to_remove_head;
                to_remove_head = p;
            } else if (lazy_mode) {
                break;
            }
            p = next;
        }

        p = _lru_durable.next;
        while (p != &_lru_durable) {
            LRUHandle* next = p->next;
            if (pred(p)) {
                _evict_one_entry(p);
                p->next = to_remove_head;
                to_remove_head = p;
            } else if (lazy_mode) {
                break;
            }
            p = next;
        }
    }
    int64_t pruned_count = 0;
    int64_t pruned_size = 0;
    while (to_remove_head != nullptr) {
        ++pruned_count;
        pruned_size += to_remove_head->total_size;
        LRUHandle* next = to_remove_head->next;
        to_remove_head->free();
        to_remove_head = next;
    }
    return {pruned_count, pruned_size};
}

void LRUCache::for_each_entry(const std::function<void(const LRUHandle*)>& visitor) {
    std::lock_guard l(_mutex);
    for (LRUHandle* p = _lru_normal.next; p != &_lru_normal; p = p->next) {
        visitor(p);
    }
    for (LRUHandle* p = _lru_durable.next; p != &_lru_durable; p = p->next) {
        visitor(p);
    }
}

void LRUCache::set_cache_value_time_extractor(CacheValueTimeExtractor cache_value_time_extractor) {
    _cache_value_time_extractor = cache_value_time_extractor;
}

void LRUCache::set_cache_value_check_timestamp(bool cache_value_check_timestamp) {
    _cache_value_check_timestamp = cache_value_check_timestamp;
}

inline uint32_t ShardedLRUCache::_hash_slice(const CacheKey& s) {
    return s.hash(s.data(), s.size(), 0);
}

ShardedLRUCache::ShardedLRUCache(const std::string& name, size_t capacity, LRUCacheType type,
                                 uint32_t num_shards, uint32_t total_element_count_capacity,
                                 bool is_lru_k)
        : _name(name),
          _num_shard_bits(Bits::FindLSBSetNonZero(num_shards)),
          _num_shards(num_shards),
          _shards(nullptr),
          _last_id(1),
          _capacity(capacity) {
    CHECK(num_shards > 0) << "num_shards cannot be 0";
    CHECK_EQ((num_shards & (num_shards - 1)), 0)
            << "num_shards should be power of two, but got " << num_shards;

    const size_t per_shard = (capacity + (_num_shards - 1)) / _num_shards;
    const size_t per_shard_element_count_capacity =
            (total_element_count_capacity + (_num_shards - 1)) / _num_shards;
    LRUCache** shards = new (std::nothrow) LRUCache*[_num_shards];
    for (int s = 0; s < _num_shards; s++) {
        shards[s] = new LRUCache(type, is_lru_k);
        shards[s]->set_capacity(per_shard);
        shards[s]->set_element_count_capacity(per_shard_element_count_capacity);
    }
    _shards = shards;

    _entity = DorisMetrics::instance()->metric_registry()->register_entity(
            std::string("lru_cache:") + name, {{"name", name}});
    _entity->register_hook(name, std::bind(&ShardedLRUCache::update_cache_metrics, this));
    INT_GAUGE_METRIC_REGISTER(_entity, cache_capacity);
    INT_GAUGE_METRIC_REGISTER(_entity, cache_usage);
    INT_GAUGE_METRIC_REGISTER(_entity, cache_element_count);
    DOUBLE_GAUGE_METRIC_REGISTER(_entity, cache_usage_ratio);
    INT_COUNTER_METRIC_REGISTER(_entity, cache_lookup_count);
    INT_COUNTER_METRIC_REGISTER(_entity, cache_hit_count);
    DOUBLE_GAUGE_METRIC_REGISTER(_entity, cache_hit_ratio);

    _hit_count_bvar.reset(new bvar::Adder<uint64_t>("doris_cache", _name));
    _hit_count_per_second.reset(new bvar::PerSecond<bvar::Adder<uint64_t>>(
            "doris_cache", _name + "_persecond", _hit_count_bvar.get(), 60));
    _lookup_count_bvar.reset(new bvar::Adder<uint64_t>("doris_cache", _name));
    _lookup_count_per_second.reset(new bvar::PerSecond<bvar::Adder<uint64_t>>(
            "doris_cache", _name + "_persecond", _lookup_count_bvar.get(), 60));
}

ShardedLRUCache::ShardedLRUCache(const std::string& name, size_t capacity, LRUCacheType type,
                                 uint32_t num_shards,
                                 CacheValueTimeExtractor cache_value_time_extractor,
                                 bool cache_value_check_timestamp,
                                 uint32_t total_element_count_capacity, bool is_lru_k)
        : ShardedLRUCache(name, capacity, type, num_shards, total_element_count_capacity,
                          is_lru_k) {
    for (int s = 0; s < _num_shards; s++) {
        _shards[s]->set_cache_value_time_extractor(cache_value_time_extractor);
        _shards[s]->set_cache_value_check_timestamp(cache_value_check_timestamp);
    }
}

ShardedLRUCache::~ShardedLRUCache() {
    _entity->deregister_hook(_name);
    DorisMetrics::instance()->metric_registry()->deregister_entity(_entity);
    if (_shards) {
        for (int s = 0; s < _num_shards; s++) {
            delete _shards[s];
        }
        delete[] _shards;
    }
}

PrunedInfo ShardedLRUCache::set_capacity(size_t capacity) {
    std::lock_guard l(_mutex);
    PrunedInfo pruned_info;
    const size_t per_shard = (capacity + (_num_shards - 1)) / _num_shards;
    for (int s = 0; s < _num_shards; s++) {
        PrunedInfo info = _shards[s]->set_capacity(per_shard);
        pruned_info.pruned_count += info.pruned_count;
        pruned_info.pruned_size += info.pruned_size;
    }
    _capacity = capacity;
    return pruned_info;
}

size_t ShardedLRUCache::get_capacity() {
    std::lock_guard l(_mutex);
    return _capacity;
}

Cache::Handle* ShardedLRUCache::insert(const CacheKey& key, void* value, size_t charge,
                                       CachePriority priority) {
    const uint32_t hash = _hash_slice(key);
    return _shards[_shard(hash)]->insert(key, hash, value, charge, priority);
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

uint64_t ShardedLRUCache::new_id() {
    return _last_id.fetch_add(1, std::memory_order_relaxed);
}

PrunedInfo ShardedLRUCache::prune() {
    PrunedInfo pruned_info;
    for (int s = 0; s < _num_shards; s++) {
        PrunedInfo info = _shards[s]->prune();
        pruned_info.pruned_count += info.pruned_count;
        pruned_info.pruned_size += info.pruned_size;
    }
    return pruned_info;
}

PrunedInfo ShardedLRUCache::prune_if(CachePrunePredicate pred, bool lazy_mode) {
    PrunedInfo pruned_info;
    for (int s = 0; s < _num_shards; s++) {
        PrunedInfo info = _shards[s]->prune_if(pred, lazy_mode);
        pruned_info.pruned_count += info.pruned_count;
        pruned_info.pruned_size += info.pruned_size;
    }
    return pruned_info;
}

void ShardedLRUCache::for_each_entry(const std::function<void(const LRUHandle*)>& visitor) {
    for (int s = 0; s < _num_shards; s++) {
        _shards[s]->for_each_entry(visitor);
    }
}

int64_t ShardedLRUCache::get_usage() {
    size_t total_usage = 0;
    for (int i = 0; i < _num_shards; i++) {
        total_usage += _shards[i]->get_usage();
    }
    return total_usage;
}

size_t ShardedLRUCache::get_element_count() {
    size_t total_element_count = 0;
    for (int i = 0; i < _num_shards; i++) {
        total_element_count += _shards[i]->get_element_count();
    }
    return total_element_count;
}

void ShardedLRUCache::update_cache_metrics() const {
    size_t capacity = 0;
    size_t total_usage = 0;
    size_t total_lookup_count = 0;
    size_t total_hit_count = 0;
    size_t total_element_count = 0;
    for (int i = 0; i < _num_shards; i++) {
        capacity += _shards[i]->get_capacity();
        total_usage += _shards[i]->get_usage();
        total_lookup_count += _shards[i]->get_lookup_count();
        total_hit_count += _shards[i]->get_hit_count();
        total_element_count += _shards[i]->get_element_count();
    }

    cache_capacity->set_value(capacity);
    cache_usage->set_value(total_usage);
    cache_element_count->set_value(total_element_count);
    cache_lookup_count->set_value(total_lookup_count);
    cache_hit_count->set_value(total_hit_count);
    cache_usage_ratio->set_value(capacity == 0 ? 0 : ((double)total_usage / capacity));
    cache_hit_ratio->set_value(
            total_lookup_count == 0 ? 0 : ((double)total_hit_count / total_lookup_count));
}

Cache::Handle* DummyLRUCache::insert(const CacheKey& key, void* value, size_t charge,
                                     CachePriority priority) {
    size_t handle_size = sizeof(LRUHandle);
    auto* e = reinterpret_cast<LRUHandle*>(malloc(handle_size));
    e->value = value;
    e->charge = charge;
    e->key_length = 0;
    e->total_size = 0;
    e->hash = 0;
    e->refs = 1; // only one for the returned handle
    e->next = e->prev = nullptr;
    e->in_cache = false;
    return reinterpret_cast<Cache::Handle*>(e);
}

void DummyLRUCache::release(Cache::Handle* handle) {
    if (handle == nullptr) {
        return;
    }
    auto* e = reinterpret_cast<LRUHandle*>(handle);
    e->free();
}

void* DummyLRUCache::value(Handle* handle) {
    return reinterpret_cast<LRUHandle*>(handle)->value;
}

} // namespace doris
