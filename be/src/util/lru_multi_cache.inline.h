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
// https://github.com/apache/impala/blob/master/be/src/util/lru-multi-cache.inline.h
// and modified by Doris

#pragma once

#include <glog/logging.h>

#include "util/hash_util.hpp"
#include "util/lru_multi_cache.h"
#include "util/time.h"

namespace doris {

template <typename KeyType, typename ValueType>
template <typename... Args>
LruMultiCache<KeyType, ValueType>::ValueType_internal::ValueType_internal(
        LruMultiCache& cache, const KeyType& key, Container_internal& container, Args&&... args)
        : cache(cache),
          key(key),
          container(container),
          value(std::forward<Args>(args)...),
          timestamp_seconds(MonotonicSeconds()) {}

template <typename KeyType, typename ValueType>
bool LruMultiCache<KeyType, ValueType>::ValueType_internal::is_available() {
    return member_hook.is_linked();
}

template <typename KeyType, typename ValueType>
LruMultiCache<KeyType, ValueType>::Accessor::Accessor(ValueType_internal* p_value_internal)
        : _p_value_internal(p_value_internal) {}

template <typename KeyType, typename ValueType>
LruMultiCache<KeyType, ValueType>::Accessor::Accessor(Accessor&& rhs) {
    _p_value_internal = std::move(rhs._p_value_internal);
    rhs._p_value_internal = nullptr;
}
template <typename KeyType, typename ValueType>
auto LruMultiCache<KeyType, ValueType>::Accessor::operator=(Accessor&& rhs) -> Accessor& {
    _p_value_internal = std::move(rhs._p_value_internal);
    rhs._p_value_internal = nullptr;
    return (*this);
}

template <typename KeyType, typename ValueType>
LruMultiCache<KeyType, ValueType>::Accessor::~Accessor() {
    release();
}

template <typename KeyType, typename ValueType>
ValueType* LruMultiCache<KeyType, ValueType>::Accessor::get() {
    if (_p_value_internal) {
        return &(_p_value_internal->value);
    }

    return nullptr;
}

template <typename KeyType, typename ValueType>
KeyType* LruMultiCache<KeyType, ValueType>::Accessor::get_key() const {
    if (_p_value_internal) {
        return const_cast<KeyType*>(&(_p_value_internal->key));
    }

    return nullptr;
}

template <typename KeyType, typename ValueType>
void LruMultiCache<KeyType, ValueType>::Accessor::release() {
    /// Nullptr check as it has to be dereferenced to get the cache reference
    /// No nullptr check is needed inside LruMultiCache::Release()
    if (_p_value_internal) {
        LruMultiCache& cache = _p_value_internal->cache;
        cache.release(_p_value_internal);
        _p_value_internal = nullptr;
    }
}

template <typename KeyType, typename ValueType>
void LruMultiCache<KeyType, ValueType>::Accessor::destroy() {
    /// Nullptr check as it has to be dereferenced to get the cache reference
    /// No nullptr check is needed inside LruMultiCache::destroy()
    if (_p_value_internal) {
        LruMultiCache& cache = _p_value_internal->cache;
        cache.destroy(_p_value_internal);
        _p_value_internal = nullptr;
    }
}

template <typename KeyType, typename ValueType>
LruMultiCache<KeyType, ValueType>::LruMultiCache(size_t capacity) : _capacity(capacity), _size(0) {}

template <typename KeyType, typename ValueType>
size_t LruMultiCache<KeyType, ValueType>::size() {
    std::lock_guard<SpinLock> g(_lock);
    return _size;
}

template <typename KeyType, typename ValueType>
size_t LruMultiCache<KeyType, ValueType>::number_of_keys() {
    std::lock_guard<SpinLock> g(_lock);
    return _hash_table.size();
}

template <typename KeyType, typename ValueType>
void LruMultiCache<KeyType, ValueType>::set_capacity(size_t new_capacity) {
    std::lock_guard<SpinLock> g(_lock);
    _capacity = new_capacity;
}

template <typename KeyType, typename ValueType>
auto LruMultiCache<KeyType, ValueType>::get(const KeyType& key) -> Accessor {
    std::lock_guard<SpinLock> g(_lock);
    auto hash_table_it = _hash_table.find(key);

    // No owning list found with this key, the caller will have to create a new object
    // with EmplaceAndGet()
    if (hash_table_it == _hash_table.end()) return Accessor();

    Container& container = hash_table_it->second;

    // Empty containers are deleted automatiacally
    DCHECK(!container.empty());

    // All the available elements are in the front, only need to check the first
    auto container_it = container.begin();

    // No available object found, the caller will have to create a new one with
    // EmplaceAndGet()
    if (!container_it->is_available()) return Accessor();

    // Move the object to the back of the owning list as it is no longer available.
    container.splice(container.end(), container, container_it);

    // Remove the element from the LRU list as it is no longer available
    container_it->member_hook.unlink();

    return Accessor(&(*container_it));
}

template <typename KeyType, typename ValueType>
template <typename... Args>
auto LruMultiCache<KeyType, ValueType>::emplace_and_get(const KeyType& key, Args&&... args)
        -> Accessor {
    std::lock_guard<SpinLock> g(_lock);

    // creates default container if there isn't one
    Container& container = _hash_table[key];

    // Get the reference of the key stored in unordered_map, the parameter could be
    // temporary object but std::unordered_map has stable references
    const KeyType& stored_key = _hash_table.find(key)->first;

    // Place it as the last entry for the owning list, as it just got reserved
    auto container_it = container.emplace(container.end(), (*this), stored_key, container,
                                          std::forward<Args>(args)...);

    // Only can set this after emplace
    container_it->it = container_it;

    _size++;

    // Need to remove the oldest available if the cache is over the capacity
    _evict_one_if_needed();

    return Accessor(&(*container_it));
}

template <typename KeyType, typename ValueType>
void LruMultiCache<KeyType, ValueType>::release(ValueType_internal* p_value_internal) {
    std::lock_guard<SpinLock> g(_lock);

    // This only can be used by the accessor, which already checks for nullptr
    DCHECK(p_value_internal);

    // Has to be currently not available
    DCHECK(!p_value_internal->is_available());

    p_value_internal->timestamp_seconds = MonotonicSeconds();

    Container& container = p_value_internal->container;

    // Move the object to the front, keep LRU relation in owning list too to
    // be able to age out unused objects
    container.splice(container.begin(), container, p_value_internal->it);

    // Add the object to LRU list too as it is now available for usage
    _lru_list.push_front(container.front());

    // In case we overshot the capacity already, the cache can evict the oldest one
    _evict_one_if_needed();
}

template <typename KeyType, typename ValueType>
void LruMultiCache<KeyType, ValueType>::destroy(ValueType_internal* p_value_internal) {
    std::lock_guard<SpinLock> g(_lock);

    // This only can be used by the accessor, which already checks for nullptr
    DCHECK(p_value_internal);

    // Has to be currently not available
    DCHECK(!p_value_internal->is_available());

    Container& container = p_value_internal->container;

    if (container.size() == 1) {
        // Last element, owning list can be removed to prevent aging
        _hash_table.erase(p_value_internal->key);
    } else {
        // Remove from owning list
        container.erase(p_value_internal->it);
    }

    _size--;
}

template <typename KeyType, typename ValueType>
size_t LruMultiCache<KeyType, ValueType>::number_of_available_objects() {
    std::lock_guard<SpinLock> g(_lock);
    return _lru_list.size();
}

template <typename KeyType, typename ValueType>
void LruMultiCache<KeyType, ValueType>::rehash() {
    std::lock_guard<SpinLock> g(_lock);
    _hash_table.rehash(_hash_table.bucket_count() + 1);
}

template <typename KeyType, typename ValueType>
void LruMultiCache<KeyType, ValueType>::_evict_one(ValueType_internal& value_internal) {
    // SpinLock is locked by the caller evicting function
    // _lock.DCheckLocked();

    // Has to be available to evict
    DCHECK(value_internal.is_available());

    // Remove from LRU cache
    value_internal.member_hook.unlink();

    Container& container = value_internal.container;

    if (container.size() == 1) {
        // Last element, owning list can be removed to prevent aging
        _hash_table.erase(value_internal.key);
    } else {
        // Remove from owning list
        container.erase(value_internal.it);
    }

    _size--;
}

template <typename KeyType, typename ValueType>
void LruMultiCache<KeyType, ValueType>::_evict_one_if_needed() {
    // SpinLock is locked by the caller public function
    // _lock.DCheckLocked();

    if (!_lru_list.empty() && _size > _capacity) {
        _evict_one(_lru_list.back());
    }
}

template <typename KeyType, typename ValueType>
void LruMultiCache<KeyType, ValueType>::evict_older_than(uint64_t oldest_allowed_timestamp) {
    std::lock_guard<SpinLock> g(_lock);

    // Stop eviction if
    //   - there are no more available (i.e. evictable) objects
    //   - cache size is below capacity and the oldest object is not older than the limit
    while (!_lru_list.empty() &&
           (_size > _capacity || _lru_list.back().timestamp_seconds < oldest_allowed_timestamp)) {
        _evict_one(_lru_list.back());
    }
}

} // namespace doris
