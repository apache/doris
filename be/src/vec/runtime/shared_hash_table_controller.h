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

#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <variant>
#include <vector>

#include "vec/exec/join/join_op.h"

namespace doris {

class RuntimeState;
class TUniqueId;

template <typename ExprCtxType>
class RuntimeFilterSlotsBase;

namespace vectorized {

class VExprContext;

template <typename RowRefListType>
struct SerializedHashTableContext {
    using Mapped = RowRefListType;
    using HashTable = HashMap<StringRef, Mapped>;
    using HashTablePtr = std::shared_ptr<HashMap<StringRef, Mapped>>;
    using State = ColumnsHashing::HashMethodSerialized<typename HashTable::value_type, Mapped>;
    using Iter = typename HashTable::iterator;

    HashTable hash_table;
    Iter iter;
    bool inited = false;

    void init_once() {
        if (!inited) {
            inited = true;
            iter = hash_table.begin();
        }
    }
};

template <typename HashMethod>
struct IsSerializedHashTableContextTraits {
    constexpr static bool value = false;
};

template <typename Value, typename Mapped>
struct IsSerializedHashTableContextTraits<ColumnsHashing::HashMethodSerialized<Value, Mapped>> {
    constexpr static bool value = true;
};

// T should be UInt32 UInt64 UInt128
template <class T, typename RowRefListType>
struct PrimaryTypeHashTableContext {
    using Mapped = RowRefListType;
    using HashTable = HashMap<T, Mapped, HashCRC32<T>>;
    using HashTablePtr = std::shared_ptr<HashMap<T, Mapped, HashCRC32<T>>>;
    using State =
            ColumnsHashing::HashMethodOneNumber<typename HashTable::value_type, Mapped, T, false>;
    using Iter = typename HashTable::iterator;

    HashTable hash_table;
    Iter iter;
    bool inited = false;

    void init_once() {
        if (!inited) {
            inited = true;
            iter = hash_table.begin();
        }
    }
};

// TODO: use FixedHashTable instead of HashTable
template <typename RowRefListType>
using I8HashTableContext = PrimaryTypeHashTableContext<UInt8, RowRefListType>;
template <typename RowRefListType>
using I16HashTableContext = PrimaryTypeHashTableContext<UInt16, RowRefListType>;
template <typename RowRefListType>
using I32HashTableContext = PrimaryTypeHashTableContext<UInt32, RowRefListType>;
template <typename RowRefListType>
using I64HashTableContext = PrimaryTypeHashTableContext<UInt64, RowRefListType>;
template <typename RowRefListType>
using I128HashTableContext = PrimaryTypeHashTableContext<UInt128, RowRefListType>;
template <typename RowRefListType>
using I256HashTableContext = PrimaryTypeHashTableContext<UInt256, RowRefListType>;

template <class T, bool has_null, typename RowRefListType>
struct FixedKeyHashTableContext {
    using Mapped = RowRefListType;
    using HashTable = HashMap<T, Mapped, HashCRC32<T>>;
    using HashTablePtr = std::shared_ptr<HashMap<T, Mapped, HashCRC32<T>>>;
    using State = ColumnsHashing::HashMethodKeysFixed<typename HashTable::value_type, T, Mapped,
                                                      has_null, false>;
    using Iter = typename HashTable::iterator;

    HashTable hash_table;
    Iter iter;
    bool inited = false;

    void init_once() {
        if (!inited) {
            inited = true;
            iter = hash_table.begin();
        }
    }
};

template <bool has_null, typename RowRefListType>
using I64FixedKeyHashTableContext = FixedKeyHashTableContext<UInt64, has_null, RowRefListType>;

template <bool has_null, typename RowRefListType>
using I128FixedKeyHashTableContext = FixedKeyHashTableContext<UInt128, has_null, RowRefListType>;

template <bool has_null, typename RowRefListType>
using I256FixedKeyHashTableContext = FixedKeyHashTableContext<UInt256, has_null, RowRefListType>;

using HashTableVariants = std::variant<
        std::monostate, SerializedHashTableContext<RowRefList>, I8HashTableContext<RowRefList>,
        I16HashTableContext<RowRefList>, I32HashTableContext<RowRefList>,
        I64HashTableContext<RowRefList>, I128HashTableContext<RowRefList>,
        I256HashTableContext<RowRefList>, I64FixedKeyHashTableContext<true, RowRefList>,
        I64FixedKeyHashTableContext<false, RowRefList>,
        I128FixedKeyHashTableContext<true, RowRefList>,
        I128FixedKeyHashTableContext<false, RowRefList>,
        I256FixedKeyHashTableContext<true, RowRefList>,
        I256FixedKeyHashTableContext<false, RowRefList>,
        SerializedHashTableContext<RowRefListWithFlag>, I8HashTableContext<RowRefListWithFlag>,
        I16HashTableContext<RowRefListWithFlag>, I32HashTableContext<RowRefListWithFlag>,
        I64HashTableContext<RowRefListWithFlag>, I128HashTableContext<RowRefListWithFlag>,
        I256HashTableContext<RowRefListWithFlag>,
        I64FixedKeyHashTableContext<true, RowRefListWithFlag>,
        I64FixedKeyHashTableContext<false, RowRefListWithFlag>,
        I128FixedKeyHashTableContext<true, RowRefListWithFlag>,
        I128FixedKeyHashTableContext<false, RowRefListWithFlag>,
        I256FixedKeyHashTableContext<true, RowRefListWithFlag>,
        I256FixedKeyHashTableContext<false, RowRefListWithFlag>,
        SerializedHashTableContext<RowRefListWithFlags>, I8HashTableContext<RowRefListWithFlags>,
        I16HashTableContext<RowRefListWithFlags>, I32HashTableContext<RowRefListWithFlags>,
        I64HashTableContext<RowRefListWithFlags>, I128HashTableContext<RowRefListWithFlags>,
        I256HashTableContext<RowRefListWithFlags>,
        I64FixedKeyHashTableContext<true, RowRefListWithFlags>,
        I64FixedKeyHashTableContext<false, RowRefListWithFlags>,
        I128FixedKeyHashTableContext<true, RowRefListWithFlags>,
        I128FixedKeyHashTableContext<false, RowRefListWithFlags>,
        I256FixedKeyHashTableContext<true, RowRefListWithFlags>,
        I256FixedKeyHashTableContext<false, RowRefListWithFlags>>;

using HashTableVariantsPtr = std::shared_ptr<HashTableVariants>;

struct SharedHashTableEntry {
    SharedHashTableEntry(
            Status status_, HashTableVariantsPtr hash_table_ptr_,
            const std::vector<Block>& build_blocks_,
            std::shared_ptr<RuntimeFilterSlotsBase<VExprContext>> runtime_filter_slots_,
            std::shared_ptr<Arena> arena_, std::shared_ptr<ObjectPool> runtime_filter_pool_)
            : status(status_),
              hash_table_ptr(hash_table_ptr_),
              build_blocks(build_blocks_),
              runtime_filter_slots(runtime_filter_slots_),
              arena(arena_),
              runtime_filter_pool(runtime_filter_pool_) {}
    SharedHashTableEntry(SharedHashTableEntry&& entry)
            : status(entry.status),
              hash_table_ptr(entry.hash_table_ptr),
              build_blocks(entry.build_blocks),
              runtime_filter_slots(entry.runtime_filter_slots),
              arena(entry.arena),
              runtime_filter_pool(entry.runtime_filter_pool) {}

    static SharedHashTableEntry empty_entry_with_status(const Status& status) {
        return SharedHashTableEntry(status, nullptr, {}, nullptr, nullptr, nullptr);
    }

    Status status;
    HashTableVariantsPtr hash_table_ptr;
    std::vector<Block> build_blocks;
    std::shared_ptr<RuntimeFilterSlotsBase<VExprContext>> runtime_filter_slots;
    std::shared_ptr<Arena> arena;
    std::shared_ptr<ObjectPool> runtime_filter_pool;
};

class SharedHashTableController {
public:
    bool should_build_hash_table(RuntimeState* state, int my_node_id);
    void acquire_ref_count(RuntimeState* state, int my_node_id);
    SharedHashTableEntry& wait_for_hash_table(int my_node_id);
    Status release_ref_count(RuntimeState* state, int my_node_id);
    Status release_ref_count_if_need(TUniqueId fragment_id, Status status);
    void put_hash_table(SharedHashTableEntry&& entry, int my_node_id);
    Status wait_for_closable(RuntimeState* state, int my_node_id);

    // Single-thread operation
    void set_short_circuit_for_null_in_probe_side(bool short_circuit_for_null_in_probe_side) {
        _short_circuit_for_null_in_probe_side = short_circuit_for_null_in_probe_side;
    }

    bool short_circuit_for_null_in_probe_side() const {
        return _short_circuit_for_null_in_probe_side;
    }

private:
    // If the fragment instance was supposed to build hash table, but it didn't build.
    // To avoid deadlocking other fragment instances,
    // here need to put an empty SharedHashTableEntry with canceled status.
    void _put_an_empty_entry_if_need(Status status, TUniqueId fragment_id, int node_id);

private:
    std::mutex _mutex;
    std::condition_variable _cv;
    std::map<int /*node id*/, TUniqueId /*fragment id*/> _builder_fragment_ids;
    std::map<int /*node id*/, SharedHashTableEntry> _hash_table_entries;
    std::map<int /*node id*/, std::vector<TUniqueId>> _ref_fragments;
    bool _short_circuit_for_null_in_probe_side;
};

} // namespace vectorized
} // namespace doris