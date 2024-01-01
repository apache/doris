// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <assert.h>
#include <butil/macros.h>
#include <glog/logging.h>
#include <gtest/gtest_prod.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include <atomic>
#include <functional>
#include <memory>
#include <set>
#include <string>
#include <utility>

#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/thread_context.h"
#include "util/doris_metrics.h"
#include "util/metrics.h"
#include "util/slice.h"

namespace doris {

#define OLAP_CACHE_STRING_TO_BUF(cur, str, r_len)                  \
    do {                                                           \
        if (r_len > str.size()) {                                  \
            memcpy(cur, str.c_str(), str.size());                  \
            r_len -= str.size();                                   \
            cur += str.size();                                     \
        } else {                                                   \
            LOG(WARNING) << "construct cache key buf not enough."; \
            return CacheKey(nullptr, 0);                           \
        }                                                          \
    } while (0)

#define OLAP_CACHE_NUMERIC_TO_BUF(cur, numeric, r_len)             \
    do {                                                           \
        if (r_len > sizeof(numeric)) {                             \
            memcpy(cur, &numeric, sizeof(numeric));                \
            r_len -= sizeof(numeric);                              \
            cur += sizeof(numeric);                                \
        } else {                                                   \
            LOG(WARNING) << "construct cache key buf not enough."; \
            return CacheKey(nullptr, 0);                           \
        }                                                          \
    } while (0)

class Cache;
class LRUCachePolicy;

enum LRUCacheType {
    SIZE,  // The capacity of cache is based on the size of cache entry.
    NUMBER // The capacity of cache is based on the number of cache entry.
};

static constexpr LRUCacheType DEFAULT_LRU_CACHE_TYPE = LRUCacheType::SIZE;
static constexpr uint32_t DEFAULT_LRU_CACHE_NUM_SHARDS = 16;
static constexpr size_t DEFAULT_LRU_CACHE_ELEMENT_COUNT_CAPACITY = 0;

class CacheKey {
public:
    CacheKey() : _data(nullptr), _size(0) {}
    // Create a slice that refers to d[0,n-1].
    CacheKey(const char* d, size_t n) : _data(d), _size(n) {}

    // Create a slice that refers to the contents of "s"
    CacheKey(const std::string& s) : _data(s.data()), _size(s.size()) {}

    // Create a slice that refers to s[0,strlen(s)-1]
    CacheKey(const char* s) : _data(s), _size(strlen(s)) {}

    ~CacheKey() {}

    // Return a pointer to the beginning of the referenced data
    const char* data() const { return _data; }

    // Return the length (in bytes) of the referenced data
    size_t size() const { return _size; }

    // Return true if the length of the referenced data is zero
    bool empty() const { return _size == 0; }

    // Return the ith byte in the referenced data.
    // REQUIRES: n < size()
    char operator[](size_t n) const {
        assert(n < size());
        return _data[n];
    }

    // Change this slice to refer to an empty array
    void clear() {
        _data = nullptr;
        _size = 0;
    }

    // Drop the first "n" bytes from this slice.
    void remove_prefix(size_t n) {
        assert(n <= size());
        _data += n;
        _size -= n;
    }

    // Return a string that contains the copy of the referenced data.
    std::string to_string() const { return std::string(_data, _size); }

    bool operator==(const CacheKey& other) const {
        return ((size() == other.size()) && (memcmp(data(), other.data(), size()) == 0));
    }

    bool operator!=(const CacheKey& other) const { return !(*this == other); }

    int compare(const CacheKey& b) const {
        const size_t min_len = (_size < b._size) ? _size : b._size;
        int r = memcmp(_data, b._data, min_len);
        if (r == 0) {
            if (_size < b._size) {
                r = -1;
            } else if (_size > b._size) {
                r = +1;
            }
        }
        return r;
    }

    uint32_t hash(const char* data, size_t n, uint32_t seed) const;

    // Return true if "x" is a prefix of "*this"
    bool starts_with(const CacheKey& x) const {
        return ((_size >= x._size) && (memcmp(_data, x._data, x._size) == 0));
    }

private:
    uint32_t _decode_fixed32(const char* ptr) const {
        // Load the raw bytes
        uint32_t result;
        memcpy(&result, ptr, sizeof(result)); // gcc optimizes this to a plain load
        return result;
    }

    const char* _data = nullptr;
    size_t _size;
};

// The entry with smaller CachePriority will evict firstly
enum class CachePriority { NORMAL = 0, DURABLE = 1 };

using CacheValuePredicate = std::function<bool(const void*)>;
// CacheValueTimeExtractor can extract timestamp
// in cache value through the specified function,
// such as last_visit_time in InvertedIndexSearcherCache::CacheValue
using CacheValueTimeExtractor = std::function<int64_t(const void*)>;

class Cache {
public:
    Cache() {}

    // Destroys all existing entries by calling the "deleter"
    // function that was passed to the constructor.
    virtual ~Cache();

    // Opaque handle to an entry stored in the cache.
    struct Handle {};

    // Insert a mapping from key->value into the cache and assign it
    // the specified charge against the total cache capacity.
    //
    // Returns a handle that corresponds to the mapping.  The caller
    // must call this->release(handle) when the returned mapping is no
    // longer needed.
    //
    // When the inserted entry is no longer needed, the key and
    // value will be passed to "deleter".
    virtual Handle* insert(const CacheKey& key, void* value, size_t charge,
                           void (*deleter)(const CacheKey& key, void* value),
                           CachePriority priority = CachePriority::NORMAL, size_t bytes = -1) = 0;

    // If the cache has no mapping for "key", returns nullptr.
    //
    // Else return a handle that corresponds to the mapping.  The caller
    // must call this->release(handle) when the returned mapping is no
    // longer needed.
    virtual Handle* lookup(const CacheKey& key) = 0;

    // Release a mapping returned by a previous Lookup().
    // REQUIRES: handle must not have been released yet.
    // REQUIRES: handle must have been returned by a method on *this.
    virtual void release(Handle* handle) = 0;

    // Return the value encapsulated in a handle returned by a
    // successful lookup().
    // REQUIRES: handle must not have been released yet.
    // REQUIRES: handle must have been returned by a method on *this.
    virtual void* value(Handle* handle) = 0;

    // Return the value in Slice format encapsulated in the given handle
    // returned by a successful lookup()
    virtual Slice value_slice(Handle* handle) = 0;

    // If the cache contains entry for key, erase it.  Note that the
    // underlying entry will be kept around until all existing handles
    // to it have been released.
    virtual void erase(const CacheKey& key) = 0;

    // Return a new numeric id.  May be used by multiple clients who are
    // sharing the same cache to partition the key space.  Typically the
    // client will allocate a new id at startup and prepend the id to
    // its cache keys.
    virtual uint64_t new_id() = 0;

    // Remove all cache entries that are not actively in use.  Memory-constrained
    // applications may wish to call this method to reduce memory usage.
    // Default implementation of Prune() does nothing.  Subclasses are strongly
    // encouraged to override the default implementation.  A future release of
    // leveldb may change prune() to a pure abstract method.
    // return num of entries being pruned.
    virtual int64_t prune() { return 0; }

    // Same as prune(), but the entry will only be pruned if the predicate matched.
    // NOTICE: the predicate should be simple enough, or the prune_if() function
    // may hold lock for a long time to execute predicate.
    virtual int64_t prune_if(CacheValuePredicate pred, bool lazy_mode = false) { return 0; }

    virtual int64_t mem_consumption() = 0;

    virtual int64_t get_usage() = 0;

    virtual size_t get_total_capacity() = 0;

private:
    DISALLOW_COPY_AND_ASSIGN(Cache);
};

// An entry is a variable length heap-allocated structure.  Entries
// are kept in a circular doubly linked list ordered by access time.
struct LRUHandle {
    void* value = nullptr;
    void (*deleter)(const CacheKey&, void* value);
    struct LRUHandle* next_hash = nullptr; // next entry in hash table
    struct LRUHandle* next = nullptr;      // next entry in lru list
    struct LRUHandle* prev = nullptr;      // previous entry in lru list
    size_t charge;
    size_t key_length;
    size_t total_size; // including key length
    size_t bytes;      // Used by LRUCacheType::NUMBER, LRUCacheType::SIZE equal to total_size.
    bool in_cache;     // Whether entry is in the cache.
    uint32_t refs;
    uint32_t hash; // Hash of key(); used for fast sharding and comparisons
    CachePriority priority = CachePriority::NORMAL;
    MemTrackerLimiter* mem_tracker;
    LRUCacheType type;
    char key_data[1]; // Beginning of key

    CacheKey key() const {
        // For cheaper lookups, we allow a temporary Handle object
        // to store a pointer to a key in "value".
        if (next == this) {
            return *(reinterpret_cast<CacheKey*>(value));
        } else {
            return CacheKey(key_data, key_length);
        }
    }

    void free() {
        (*deleter)(key(), value);
        if (bytes != 0) { // DummyLRUCache bytes always equal to 0
            THREAD_MEM_TRACKER_TRANSFER_FROM(bytes, mem_tracker);
            DorisMetrics::instance()->lru_cache_memory_bytes->increment(-bytes);
        }
        ::free(this);
    }
};

// We provide our own simple hash tablet since it removes a whole bunch
// of porting hacks and is also faster than some of the built-in hash
// tablet implementations in some of the compiler/runtime combinations
// we have tested.  E.g., readrandom speeds up by ~5% over the g++
// 4.4.3's builtin hashtable.

class HandleTable {
public:
    HandleTable() : _length(0), _elems(0), _list(nullptr) { _resize(); }

    ~HandleTable();

    LRUHandle* lookup(const CacheKey& key, uint32_t hash);

    LRUHandle* insert(LRUHandle* h);

    // Remove element from hash table by "key" and "hash".
    LRUHandle* remove(const CacheKey& key, uint32_t hash);

    // Remove element from hash table by "h", it would be faster
    // than the function above.
    // Return whether h is found and removed.
    bool remove(const LRUHandle* h);

    uint32_t element_count() const;

private:
    FRIEND_TEST(CacheTest, HandleTableTest);

    // The tablet consists of an array of buckets where each bucket is
    // a linked list of cache entries that hash into the bucket.
    uint32_t _length;
    uint32_t _elems;
    LRUHandle** _list = nullptr;

    // Return a pointer to slot that points to a cache entry that
    // matches key/hash.  If there is no such cache entry, return a
    // pointer to the trailing slot in the corresponding linked list.
    LRUHandle** _find_pointer(const CacheKey& key, uint32_t hash);

    void _resize();
};

// pair first is timestatmp, put <timestatmp, LRUHandle*> into asc set,
// when need to free space, can first evict the begin of the set,
// because the begin element's timestamp is the oldest.
using LRUHandleSortedSet = std::set<std::pair<int64_t, LRUHandle*>>;

// A single shard of sharded cache.
class LRUCache {
public:
    LRUCache(LRUCacheType type);
    ~LRUCache();

    // Separate from constructor so caller can easily make an array of LRUCache
    void set_capacity(size_t capacity) { _capacity = capacity; }
    void set_element_count_capacity(uint32_t element_count_capacity) {
        _element_count_capacity = element_count_capacity;
    }

    // Like Cache methods, but with an extra "hash" parameter.
    // Must call release on the returned handle pointer.
    Cache::Handle* insert(const CacheKey& key, uint32_t hash, void* value, size_t charge,
                          void (*deleter)(const CacheKey& key, void* value),
                          MemTrackerLimiter* tracker,
                          CachePriority priority = CachePriority::NORMAL, size_t bytes = -1);
    Cache::Handle* lookup(const CacheKey& key, uint32_t hash);
    void release(Cache::Handle* handle);
    void erase(const CacheKey& key, uint32_t hash);
    int64_t prune();
    int64_t prune_if(CacheValuePredicate pred, bool lazy_mode = false);

    void set_cache_value_time_extractor(CacheValueTimeExtractor cache_value_time_extractor);
    void set_cache_value_check_timestamp(bool cache_value_check_timestamp);

    uint64_t get_lookup_count() const { return _lookup_count; }
    uint64_t get_hit_count() const { return _hit_count; }
    size_t get_usage() const { return _usage; }
    size_t get_capacity() const { return _capacity; }

private:
    void _lru_remove(LRUHandle* e);
    void _lru_append(LRUHandle* list, LRUHandle* e);
    bool _unref(LRUHandle* e);
    void _evict_from_lru(size_t total_size, LRUHandle** to_remove_head);
    void _evict_from_lru_with_time(size_t total_size, LRUHandle** to_remove_head);
    void _evict_one_entry(LRUHandle* e);
    bool _check_element_count_limit();

private:
    LRUCacheType _type;

    // Initialized before use.
    size_t _capacity = 0;

    // _mutex protects the following state.
    std::mutex _mutex;
    size_t _usage = 0;

    // Dummy head of LRU list.
    // Entries have refs==1 and in_cache==true.
    // _lru_normal.prev is newest entry, _lru_normal.next is oldest entry.
    LRUHandle _lru_normal;
    // _lru_durable.prev is newest entry, _lru_durable.next is oldest entry.
    LRUHandle _lru_durable;

    HandleTable _table;

    uint64_t _lookup_count = 0; // cache查找总次数
    uint64_t _hit_count = 0;    // 命中cache的总次数

    CacheValueTimeExtractor _cache_value_time_extractor;
    bool _cache_value_check_timestamp = false;
    LRUHandleSortedSet _sorted_normal_entries_with_timestamp;
    LRUHandleSortedSet _sorted_durable_entries_with_timestamp;

    uint32_t _element_count_capacity = 0;
};

class ShardedLRUCache : public Cache {
public:
    virtual ~ShardedLRUCache();
    virtual Handle* insert(const CacheKey& key, void* value, size_t charge,
                           void (*deleter)(const CacheKey& key, void* value),
                           CachePriority priority = CachePriority::NORMAL,
                           size_t bytes = -1) override;
    virtual Handle* lookup(const CacheKey& key) override;
    virtual void release(Handle* handle) override;
    virtual void erase(const CacheKey& key) override;
    virtual void* value(Handle* handle) override;
    Slice value_slice(Handle* handle) override;
    virtual uint64_t new_id() override;
    virtual int64_t prune() override;
    int64_t prune_if(CacheValuePredicate pred, bool lazy_mode = false) override;
    int64_t mem_consumption() override;
    int64_t get_usage() override;
    size_t get_total_capacity() override { return _total_capacity; };

private:
    // LRUCache can only be created and managed with LRUCachePolicy.
    friend class LRUCachePolicy;

    explicit ShardedLRUCache(const std::string& name, size_t total_capacity, LRUCacheType type,
                             uint32_t num_shards, uint32_t element_count_capacity);
    explicit ShardedLRUCache(const std::string& name, size_t total_capacity, LRUCacheType type,
                             uint32_t num_shards,
                             CacheValueTimeExtractor cache_value_time_extractor,
                             bool cache_value_check_timestamp, uint32_t element_count_capacity);

    void update_cache_metrics() const;

    static std::string lru_cache_type_string(LRUCacheType type) {
        switch (type) {
        case LRUCacheType::SIZE:
            return "size";
        case LRUCacheType::NUMBER:
            return "number";
        default:
            LOG(FATAL) << "not match type of lru cache:" << static_cast<int>(type);
        }
    }

private:
    static uint32_t _hash_slice(const CacheKey& s);
    uint32_t _shard(uint32_t hash) {
        return _num_shard_bits > 0 ? (hash >> (32 - _num_shard_bits)) : 0;
    }

    std::string _name;
    const int _num_shard_bits;
    const uint32_t _num_shards;
    LRUCache** _shards = nullptr;
    std::atomic<uint64_t> _last_id;
    size_t _total_capacity;

    std::unique_ptr<MemTrackerLimiter> _mem_tracker;
    std::shared_ptr<MetricEntity> _entity;
    IntGauge* cache_capacity = nullptr;
    IntGauge* cache_usage = nullptr;
    DoubleGauge* cache_usage_ratio = nullptr;
    IntAtomicCounter* cache_lookup_count = nullptr;
    IntAtomicCounter* cache_hit_count = nullptr;
    DoubleGauge* cache_hit_ratio = nullptr;
    // bvars
    std::unique_ptr<bvar::Adder<uint64_t>> _hit_count_bvar;
    std::unique_ptr<bvar::PerSecond<bvar::Adder<uint64_t>>> _hit_count_per_second;
    std::unique_ptr<bvar::Adder<uint64_t>> _lookup_count_bvar;
    std::unique_ptr<bvar::PerSecond<bvar::Adder<uint64_t>>> _lookup_count_per_second;
};

// Compatible with ShardedLRUCache usage, but will not actually cache.
class DummyLRUCache : public Cache {
public:
    // Must call release on the returned handle pointer.
    Handle* insert(const CacheKey& key, void* value, size_t charge,
                   void (*deleter)(const CacheKey& key, void* value),
                   CachePriority priority = CachePriority::NORMAL, size_t bytes = -1) override;
    Handle* lookup(const CacheKey& key) override { return nullptr; };
    void release(Handle* handle) override;
    void erase(const CacheKey& key) override {};
    void* value(Handle* handle) override;
    Slice value_slice(Handle* handle) override;
    uint64_t new_id() override { return 0; };
    int64_t prune() override { return 0; };
    int64_t prune_if(CacheValuePredicate pred, bool lazy_mode = false) override { return 0; };
    int64_t mem_consumption() override { return 0; };
    int64_t get_usage() override { return 0; };
    size_t get_total_capacity() override { return 0; };
};

} // namespace doris
