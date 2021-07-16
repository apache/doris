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

#pragma once

#include "vec/common/arena.h"
#include "vec/common/string_ref.h"

/**
  * In some aggregation scenarios, when adding a key to the hash table, we
  * start with a temporary key object, and if it turns out to be a new key,
  * we must make it persistent (e.g. copy to an Arena) and use the resulting
  * persistent object as hash table key. This happens only for StringRef keys,
  * because other key types are stored by value, but StringRef is a pointer-like
  * type: the actual data are stored elsewhere. Even for StringRef, we don't
  * make a persistent copy of the key in each of the following cases:
  * 1) the aggregation method doesn't use temporary keys, so they're persistent
  *    from the start;
  * 1) the key is already present in the hash table;
  * 3) that particular key is stored by value, e.g. a short StringRef key in
  *    StringHashMap.
  *
  * In the past, the caller was responsible for making the key persistent after
  * in was inserted. emplace() returned whether the key is new or not, so the
  * caller only stored new keys (this is case (2) from the above list). However,
  * now we are adding a compound hash table for StringRef keys, so case (3)
  * appears. The decision about persistence now depends on some properties of
  * the key, and the logic of this decision is tied to the particular hash table
  * implementation. This means that the hash table user now doesn't have enough
  * data and logic to make this decision by itself.
  *
  * To support these new requirements, we now manage key persistence by passing
  * a special key holder to emplace(), which has the functions to make the key
  * persistent or to discard it. emplace() then calls these functions at the
  * appropriate moments.
  *
  * This approach has the following benefits:
  * - no extra runtime branches in the caller to make the key persistent.
  * - no additional data is stored in the hash table itself, which is important
  *   when it's used in aggregate function states.
  * - no overhead when the key memory management isn't needed: we just pass the
  *   bare key without any wrapper to emplace(), and the default callbacks do
  *   nothing.
  *
  * This file defines the default key persistence functions, as well as two
  * different key holders and corresponding functions for storing StringRef
  * keys to Arena.
  */

/**
  * Returns the key. Can return the temporary key initially.
  * After the call to key_holder_persist_key(), must return the persistent key.
  */
template <typename Key>
inline Key& ALWAYS_INLINE key_holder_get_key(Key&& key) {
    return key;
}

/**
  * Make the key persistent. key_holder_get_key() must return the persistent key
  * after this call.
  */
template <typename Key>
inline void ALWAYS_INLINE key_holder_persist_key(Key&&) {}

/**
  * Discard the key. Calling key_holder_get_key() is ill-defined after this.
  */
template <typename Key>
inline void ALWAYS_INLINE key_holder_discard_key(Key&&) {}

namespace doris::vectorized {

/**
  * ArenaKeyHolder is a key holder for hash tables that serializes a StringRef
  * key to an Arena.
  */
struct ArenaKeyHolder {
    StringRef key;
    Arena& pool;
};

} // namespace doris::vectorized

inline StringRef& ALWAYS_INLINE key_holder_get_key(doris::vectorized::ArenaKeyHolder& holder) {
    return holder.key;
}

inline void ALWAYS_INLINE key_holder_persist_key(doris::vectorized::ArenaKeyHolder& holder) {
    // Hash table shouldn't ask us to persist a zero key
    assert(holder.key.size > 0);
    holder.key.data = holder.pool.insert(holder.key.data, holder.key.size);
}

inline void ALWAYS_INLINE key_holder_discard_key(doris::vectorized::ArenaKeyHolder&) {}

namespace doris::vectorized {

/**
  * SerializedKeyHolder is a key holder for a StringRef key that is already
  * serialized to an Arena. The key must be the last allocation in this Arena,
  * and is discarded by rolling back the allocation.
  */
struct SerializedKeyHolder {
    StringRef key;
    Arena& pool;
};

} // namespace doris::vectorized

inline StringRef& ALWAYS_INLINE key_holder_get_key(doris::vectorized::SerializedKeyHolder& holder) {
    return holder.key;
}

inline void ALWAYS_INLINE key_holder_persist_key(doris::vectorized::SerializedKeyHolder&) {}

inline void ALWAYS_INLINE key_holder_discard_key(doris::vectorized::SerializedKeyHolder& holder) {
    [[maybe_unused]] void* new_head = holder.pool.rollback(holder.key.size);
    assert(new_head == holder.key.data);
    holder.key.data = nullptr;
    holder.key.size = 0;
}
