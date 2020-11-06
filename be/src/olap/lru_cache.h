// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef DORIS_BE_SRC_OLAP_LRU_CACHE_H
#define DORIS_BE_SRC_OLAP_LRU_CACHE_H

#include <stdint.h>
#include <string.h>

#include <string>
#include <vector>

#include <gtest/gtest_prod.h>
#include <rapidjson/document.h>

#include "olap/olap_common.h"
#include "util/metrics.h"
#include "util/mutex.h"
#include "util/slice.h"

namespace doris {

#define OLAP_CACHE_STRING_TO_BUF(cur, str, r_len)   \
    do{ \
        if (r_len > str.size()) {   \
            memcpy(cur, str.c_str(), str.size());   \
            r_len -= str.size();   \
            cur += str.size();  \
        } else {    \
            OLAP_LOG_WARNING("construct cache key buf not enough.");    \
            return CacheKey(NULL, 0);   \
        }   \
    } while (0)

#define OLAP_CACHE_NUMERIC_TO_BUF(cur, numeric, r_len)   \
    do {    \
        if (r_len > sizeof(numeric)) {   \
            memcpy(cur, &numeric, sizeof(numeric));   \
            r_len -= sizeof(numeric);   \
            cur += sizeof(numeric);     \
        } else {    \
            OLAP_LOG_WARNING("construct cache key buf not enough.");    \
            return CacheKey(NULL, 0);    \
        }   \
    } while (0)

    class Cache;
    class CacheKey;

    // Create a new cache with a specified name and a fixed size capacity.  This implementation
    // of Cache uses a least-recently-used eviction policy.
    extern Cache* new_lru_cache(const std::string& name, size_t capacity);

    class CacheKey {
        public:
            CacheKey() : _data(NULL), _size(0) {}
            // Create a slice that refers to d[0,n-1].
            CacheKey(const char* d, size_t n) : _data(d), _size(n) {}

            // Create a slice that refers to the contents of "s"
            CacheKey(const std::string& s) : _data(s.data()), _size(s.size()) { }

            // Create a slice that refers to s[0,strlen(s)-1]
            CacheKey(const char* s) : _data(s), _size(strlen(s)) { }

            ~CacheKey() {}

            // Return a pointer to the beginning of the referenced data
            const char* data() const {
                return _data;
            }

            // Return the length (in bytes) of the referenced data
            size_t size() const {
                return _size;
            }

            // Return true iff the length of the referenced data is zero
            bool empty() const {
                return _size == 0;
            }

            // Return the ith byte in the referenced data.
            // REQUIRES: n < size()
            char operator[](size_t n) const {
                assert(n < size());
                return _data[n];
            }

            // Change this slice to refer to an empty array
            void clear() {
                _data = NULL;
                _size = 0;
            }

            // Drop the first "n" bytes from this slice.
            void remove_prefix(size_t n) {
                assert(n <= size());
                _data += n;
                _size -= n;
            }

            // Return a string that contains the copy of the referenced data.
            std::string to_string() const {
                return std::string(_data, _size);
            }

            inline bool operator==(const CacheKey& other) const {
                return ((size() == other.size()) &&
                        (memcmp(data(), other.data(), size()) == 0));
            }

            inline bool operator!=(const CacheKey& other) const {
                return !(*this == other);
            }

            inline int compare(const CacheKey& b) const {
                const size_t min_len = (_size < b._size) ? _size : b._size;
                int r = memcmp(_data, b._data, min_len);
                if (r == 0) {
                    if (_size < b._size) {
                        r = -1;
                    }
                    else if (_size > b._size) {
                        r = +1;
                    }
                }
                return r;
            }

            uint32_t hash(const char* data, size_t n, uint32_t seed) const;


            // Return true iff "x" is a prefix of "*this"
            bool starts_with(const CacheKey& x) const {
                return ((_size >= x._size) &&
                        (memcmp(_data, x._data, x._size) == 0));
            }

        private:
            uint32_t _decode_fixed32(const char* ptr) const {
                // Load the raw bytes
                uint32_t result;
                memcpy(&result, ptr, sizeof(result));  // gcc optimizes this to a plain load
                return result;
            }

            const char* _data;
            size_t _size;
    };

    // The entry with smaller CachePriority will evict firstly
    enum class CachePriority {
        NORMAL = 0,
        DURABLE = 1
    };

    class Cache {
        public:
            Cache() {}

            // Destroys all existing entries by calling the "deleter"
            // function that was passed to the constructor.
            virtual ~Cache();

            // Opaque handle to an entry stored in the cache.
            struct Handle {
            };

            // Insert a mapping from key->value into the cache and assign it
            // the specified charge against the total cache capacity.
            //
            // Returns a handle that corresponds to the mapping.  The caller
            // must call this->release(handle) when the returned mapping is no
            // longer needed.
            //
            // When the inserted entry is no longer needed, the key and
            // value will be passed to "deleter".
            virtual Handle* insert(
                    const CacheKey& key,
                    void* value, size_t charge,
                    void (*deleter)(const CacheKey& key, void* value),
                    CachePriority priority = CachePriority::NORMAL) = 0;

            // If the cache has no mapping for "key", returns NULL.
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
            virtual void prune() {}

        private:
            DISALLOW_COPY_AND_ASSIGN(Cache);
    };

    // An entry is a variable length heap-allocated structure.  Entries
    // are kept in a circular doubly linked list ordered by access time.
    typedef struct LRUHandle {
        void* value;
        void (*deleter)(const CacheKey&, void* value);
        LRUHandle* next_hash = nullptr;  // next entry in hash table
        LRUHandle* prev_hash = nullptr;  // previous entry in hash table
        LRUHandle* next = nullptr;       // next entry in lru list
        LRUHandle* prev = nullptr;       // previous entry in lru list
        size_t charge;
        size_t key_length;
        bool in_cache;      // Whether entry is in the cache.
        uint32_t refs;
        uint32_t hash;      // Hash of key(); used for fast sharding and comparisons
        CachePriority priority = CachePriority::NORMAL;
        char key_data[1];   // Beginning of key

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
            ::free(this);
        }

    } LRUHandle;

    // We provide our own simple hash tablet since it removes a whole bunch
    // of porting hacks and is also faster than some of the built-in hash
    // tablet implementations in some of the compiler/runtime combinations
    // we have tested.  E.g., readrandom speeds up by ~5% over the g++
    // 4.4.3's builtin hashtable.

    class HandleTable {
        public:
            HandleTable() : _length(0), _elems(0), _list(NULL) {
                _resize();
            }

            ~HandleTable();

            LRUHandle* lookup(const CacheKey& key, uint32_t hash);

            LRUHandle* insert(LRUHandle* h);

            // Remove element from hash table by "key" and "hash".
            LRUHandle* remove(const CacheKey& key, uint32_t hash);

            // Remove element from hash table by "h", it would be faster
            // than the function above.
            void remove(const LRUHandle* h);

        private:
            FRIEND_TEST(CacheTest, HandleTableTest);

            // The tablet consists of an array of buckets where each bucket is
            // a linked list of cache entries that hash into the bucket.
            uint32_t _length;
            uint32_t _elems;
            LRUHandle** _list;

            // Return a pointer to slot that points to a cache entry that
            // matches key/hash.  If there is no such cache entry, return a
            // pointer to the trailing slot in the corresponding linked list.
            LRUHandle** _find_pointer(const CacheKey& key, uint32_t hash);

            // Insert "handle" after "head".
            void _head_insert(LRUHandle* head, LRUHandle* handle);

            void _resize();
    };

    // A single shard of sharded cache.
    class LRUCache {
        public:
            LRUCache();
            ~LRUCache();

            // Separate from constructor so caller can easily make an array of LRUCache
            void set_capacity(size_t capacity) {
                _capacity = capacity;
            }

            // Like Cache methods, but with an extra "hash" parameter.
            Cache::Handle* insert(
                    const CacheKey& key,
                    uint32_t hash,
                    void* value,
                    size_t charge,
                    void (*deleter)(const CacheKey& key, void* value),
                    CachePriority priority = CachePriority::NORMAL);
            Cache::Handle* lookup(const CacheKey& key, uint32_t hash);
            void release(Cache::Handle* handle);
            void erase(const CacheKey& key, uint32_t hash);
            int prune();

            uint64_t get_lookup_count() const {
                return _lookup_count;
            }
            uint64_t get_hit_count() const {
                return _hit_count;
            }
            size_t get_usage() const {
                return _usage;
            }
            size_t get_capacity() const {
                return _capacity;
            }

        private:
            void _lru_remove(LRUHandle* e);
            void _lru_append(LRUHandle* list, LRUHandle* e);
            bool _unref(LRUHandle* e);
            void _evict_from_lru(size_t charge, LRUHandle** to_remove_head);
            void _evict_one_entry(LRUHandle* e);

            // Initialized before use.
            size_t _capacity = 0;

            // _mutex protects the following state.
            Mutex _mutex;
            size_t _usage = 0;

            // Dummy head of LRU list.
            // lru.prev is newest entry, lru.next is oldest entry.
            // Entries have refs==1 and in_cache==true.
            LRUHandle _lru;

            HandleTable _table;

            uint64_t _lookup_count = 0;    // cache查找总次数
            uint64_t _hit_count = 0;       // 命中cache的总次数
    };

    static const int kNumShardBits = 4;
    static const int kNumShards = 1 << kNumShardBits;

    class ShardedLRUCache : public Cache {
        public:
            explicit ShardedLRUCache(const std::string& name, size_t total_capacity);
            // TODO(fdy): 析构时清除所有cache元素
            virtual ~ShardedLRUCache();
            virtual Handle* insert(
                    const CacheKey& key,
                    void* value,
                    size_t charge,
                    void (*deleter)(const CacheKey& key, void* value),
                    CachePriority priority = CachePriority::NORMAL);
            virtual Handle* lookup(const CacheKey& key);
            virtual void release(Handle* handle);
            virtual void erase(const CacheKey& key);
            virtual void* value(Handle* handle);
            Slice value_slice(Handle* handle) override;
            virtual uint64_t new_id();
            virtual void prune();

        private:
            void update_cache_metrics() const;

        private:
            static inline uint32_t _hash_slice(const CacheKey& s);
            static uint32_t _shard(uint32_t hash);

            std::string _name;
            LRUCache _shards[kNumShards];
            std::atomic<uint64_t> _last_id;

            std::shared_ptr<MetricEntity> _entity = nullptr;
            IntGauge* capacity = nullptr;
            IntGauge* usage = nullptr;
            DoubleGauge* usage_ratio = nullptr;
            IntAtomicCounter* lookup_count = nullptr;
            IntAtomicCounter* hit_count = nullptr;
            DoubleGauge* hit_ratio = nullptr;
    };

}  // namespace doris

#endif // DORIS_BE_SRC_OLAP_LRU_CACHE_H
