// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// This file is copied from
// https://github.com/apache/impala/blob/master/be/src/util/lru-multi-cache.h
// and modified by Doris

#pragma once

#include <boost/intrusive/list.hpp>
#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <tuple>
#include <unordered_map>

#include "gutil/macros.h"
#include "util/spinlock.h"

namespace doris {

/// LruMultiCache is a threadsafe Least Recently Used Cache built on std::unordered_map
/// and boost::intrusive::list
///
/// Features
///   - can store multiple objects with the same key
///   - provides unique access to the freshest available stored object
///   - soft capacity to limit stored objects
///   - EvictOlderThan() function to age out unused objects
///   - O(1) operations (except EvictOlderThan(), which can trigger multiple evictions)
///   - automatic release of objects with RAII accessor
///   - explicit release/destroy through accessor
///   - prevents aging by removing empty owning lists
///
/// Limitations
///   - User has to guarantee the cache outlives the accessors
///   - The cache can't force the capacity limit, it can overshoot in case all the objects
///   are in use and we emplace more than the capacity, releases will evict the objects in
///   this case
///   - A bit heavy weight for simple usage (e.g. no thread safety, no unique access, no
///   multi storage, no timestamp based eviction), it was designed to store
///   HdfsFileHandles
///   - Only emplace of objects are supported and it locks the cache during construction.
///   - Inefficient memory cache usage
///
/// Design
///          _ _ _ _ _
///         |_|_|_|_|_|     unordered_map buckets
///          |       |
///       _ _|    _ _|_
///      |0|1|   |0|0|1|    owning lists, 0 is available, 1 is in use
///        \     / /
///       (LRU calculations)
///               |
///        _    _ V  _
///       |0|->|0|->|0|     LRU list
///
///
///
///   Cache
///     - std::unordered_map stores std::list of the objects extended with internal
///     information, these lists are the owners
///     - Owning lists are maintaining LRU order only for available objects (order does
///     not matter for currently used objects) to support O(1) get, the least recently
///     used, currently available element is at the front of the list, if there is one
///     - boost::intrusive::list maintains an LRU list of the currently available objects,
///     this is an intrusive (as known as not owning) list, used for eviction
///
///   Accessor
///     - RAII object which provides unique access to and object and releases
///     automatically
///     - Support explicit release and destroy too
///

template <typename KeyType, typename ValueType>
class LruMultiCache {
public:
    /// Definition is at the bottom, because it uses type defitions from private scope
    class Accessor;

    explicit LruMultiCache(size_t capacity = 0);

    /// Making sure the cache stays in place, reference of it is used for release/destroy
    LruMultiCache(LruMultiCache&&) = delete;
    LruMultiCache& operator=(LruMultiCache&&) = delete;

    DISALLOW_COPY_AND_ASSIGN(LruMultiCache);

    /// Returns the number of stored objects in O(1) time
    size_t size();

    /// Returns the number of owned lists in O(1) time
    size_t number_of_keys();

    void set_capacity(size_t new_capacity);

    /// Returns a unique accessor to the freshest available objects
    [[nodiscard]] Accessor get(const KeyType& key);

    /// Emplace a new object and return a unique accessor to it.
    /// Variadic template is used to forward the rest of the arguments to the stored
    /// object's constructor
    template <typename... Args>
    [[nodiscard]] Accessor emplace_and_get(const KeyType& key, Args&&... args);

    /// Evicts available objects with too old release time
    void evict_older_than(uint64_t oldest_allowed_timestamp);

    /// Number of available objects, O(n) complexity, for testing purposes
    size_t number_of_available_objects();

    /// Force rehash by increase bucket count, for testing purposes
    void rehash();

private:
    /// Doubly linked list and auto_unlink is used for O(1) remove from LRU list, in case of
    /// get and evict.
    typedef boost::intrusive::list_member_hook<
            boost::intrusive::link_mode<boost::intrusive::auto_unlink>>
            link_type;

    /// Internal type storing everything needed for O(1) operations
    struct ValueType_internal {
        typedef std::list<ValueType_internal> Container_internal;

        /// Variadic template is used to support emplace
        template <typename... Args>
        explicit ValueType_internal(LruMultiCache& cache, const KeyType& key,
                                    Container_internal& container, Args&&... args);

        bool is_available();

        /// Member hook for LRU list
        link_type member_hook;

        /// std::unordered_map::iterators are invalidated during rehash, but it has stable
        /// references
        /// LruMultiCache reference is used during release/destroy.
        LruMultiCache& cache;

        /// Key is used to remove empty owning list in case of eviction or destruction of the
        /// last element
        const KeyType& key;

        /// Container reference and iterator is used during release/destroy.
        /// Container reference could be spared by using key, but this is faster, spares a
        /// hash call
        Container_internal& container;
        typename Container_internal::iterator it;

        /// This is the object the user wants to cache
        ValueType value;

        /// Timestamp of the last release of the object, used only by EvictOlderThan()
        uint64_t timestamp_seconds;
    };

    /// Owning list typedef
    typedef std::list<ValueType_internal> Container;

    /// Hash table typedef
    typedef std::unordered_map<KeyType, Container> HashTableType;

    typedef boost::intrusive::member_hook<ValueType_internal, link_type,
                                          &ValueType_internal::member_hook>
            MemberHookOption;

    /// No constant time size to support self unlink, cache size is tracked by the class
    typedef boost::intrusive::list<ValueType_internal, MemberHookOption,
                                   boost::intrusive::constant_time_size<false>>
            LruListType;

    void release(ValueType_internal* p_value_internal);
    void destroy(ValueType_internal* p_value_internal);

    /// Used by public functions, if an object became available but the cache if over
    /// capacity, it removes it
    void _evict_one_if_needed();

    /// Used by EvictOlderThan()
    void _evict_one(ValueType_internal& value_internal);

    HashTableType _hash_table;
    LruListType _lru_list;

    size_t _capacity;
    size_t _size;

    /// Protects access to cache. No need for read/write cache as there is no costly
    /// pure read operation
    SpinLock _lock;

public:
    /// RAII Accessor to give unqiue access for a cached object
    class Accessor {
    public:
        /// Default construction is used to support usage as in/out parameter
        Accessor(ValueType_internal* p_value_internal = nullptr);

        /// Similar interface to unique_ptr, only movable
        Accessor(Accessor&&);
        Accessor& operator=(Accessor&&);

        DISALLOW_COPY_AND_ASSIGN(Accessor);

        /// Automatic release in destructor
        ~Accessor();

        /// Returns a pointer to the object to the user.
        /// Returns nullptr if it's an empty accessor;
        ValueType* get();

        /// Returns a pointer to the stored key;
        /// Returns nullptr if it's an empty accessor;
        KeyType* get_key() const;

        /// Explicit release of the object
        void release();

        /// Explicit destruction of the object
        void destroy();

    private:
        ValueType_internal* _p_value_internal = nullptr;
    };
};

} // namespace doris
