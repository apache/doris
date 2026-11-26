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

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <limits>
#include <memory>
#include <random>
#include <type_traits>
#include <utility>

#include "common/check.h"
#include "common/compare.h"
#include "common/exception.h"
#include "core/assert_cast.h"
#include "core/column/column.h"
#include "core/column/column_array.h"
#include "core/column/column_decimal.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_struct.h"
#include "core/column/column_vector.h"
#include "core/custom_allocator.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type/primitive_type.h"
#include "core/memcmp_small.h"
#include "core/pod_array.h"
#include "core/string_buffer.hpp"
#include "core/string_ref.h"
#include "core/types.h"
#include "exprs/aggregate/aggregate_function.h"

namespace doris {

/// A string payload's position in its state's arena.
struct StringSpan {
    size_t offset = 0;
    size_t length = 0;
};

/// The bytes of one group's string values. A StringRef cannot be stored in a slot: the arena
/// reallocates and would invalidate the pointers, so a slot keeps a span instead and materializes a
/// view on access. Fixed-width value types inherit EmptyTopKArena instead, so this costs them
/// nothing.
struct StringTopKArena {
    /// The initial allocation is small on purpose: the arena grows with the bytes actually retained,
    /// unlike a Doris column's container, whose PaddedPODArray allocates a 4 KiB initial chunk as
    /// soon as it holds one value.
    /// PaddedPODArray, not PODArray: comparisons go through memcmp_small_allow_overflow15, which
    /// loads a full 16-byte vector even for a one-byte string, the same tail allowance
    /// ColumnString's chars buffer provides.
    PaddedPODArray<char, 32> bytes;
    // Sum of the live slots' payload and of the payload evicted since the last compaction. The
    // slot counters alone cannot bound the memory: replacing 1024 1 MiB strings with short ones
    // leaves dead == live by slot count while ~1 GiB of discarded bytes stay in the arena.
    size_t live_payload_bytes = 0;
    size_t dead_payload_bytes = 0;
};

/// The payload holder of a fixed-width value type: its values live in their slots, so its states
/// own no arena at all.
struct EmptyTopKArena {};

/// A per-state seed for the treap priorities. A fixed sequence is predictable from the input: an
/// adversarial input can feed the n-th generated priority as the n-th smallest value, making the
/// value order match the priority order and turning the treap into a chain of depth n. The
/// process-random base and the counter keep the sequence independent of the data.
inline uint32_t next_topk_priority_seed() {
    static std::atomic<uint32_t> counter {0};
    static const uint32_t base = [] {
        std::random_device device;
        return static_cast<uint32_t>(device());
    }();
    uint32_t z = base + counter.fetch_add(1, std::memory_order_relaxed) * 0x9E3779B9U;
    z = (z ^ (z >> 16)) * 0x21F0AAADU;
    z = (z ^ (z >> 15)) * 0x735A2D97U;
    return z ^ (z >> 15);
}

/**
 * State shared by min_topk and max_topk, for one value type.
 *
 * The distinct values sit in slots (see the stores above), with one Slot record per value carrying
 * the row count and the links of a treap ordered best-first (ascending values for MIN, descending
 * for MAX). A row costs one O(log k) descent: it either finds its value and bumps the count or
 * links a new slot at the leaf. A sorted vector would shift O(k) elements for the same insert.
 *
 * The treap stores explicit child links because, unlike a heap, its shape is not the complete tree
 * that an array index can describe: rotations move entries, and the in-order sequence is the value
 * order rather than the arrival order. A heap could evict and append in O(log k), but it cannot
 * find an existing value without a hash index or a linear scan, and it cannot produce the sorted
 * result without sorting. The ordered tree is the structure that keeps find, insert and eviction
 * all at O(log k) with comparisons only.
 *
 * An entry that is evicted keeps its slot and value until the next compaction. The tree never
 * points at a dead slot.
 *
 * With k == 0 the state holds at most the single extreme value and its row count; the caller
 * derives truncation from the list length, so the state carries no separate bound.
 */
template <PrimitiveType T>
struct AggregateTopKCountData
        : std::conditional_t<is_string_type(T), StringTopKArena, EmptyTopKArena> {
    using CppType = typename PrimitiveTypeTraits<T>::CppType;
    static constexpr bool IS_STRING = is_string_type(T);
    /// A value as the state stores it: the value itself for fixed-width types, the span of its
    /// bytes in the arena for strings.
    using Payload = std::conditional_t<IS_STRING, StringSpan, CppType>;
    /// A value read out of a state: a view valid until that state's arena changes.
    using ValueRef = std::conditional_t<IS_STRING, StringRef, const CppType&>;
    /// A value copied out of a state, for building the sorted list a rebuild starts from.
    using ValueCopy = std::conditional_t<IS_STRING, StringRef, CppType>;

    static constexpr int32_t NO_SLOT = -1;
    // Row counts are never negative, so this cannot collide with a live entry.
    static constexpr int64_t DEAD_COUNT = -1;

    /// One running value: its count, its payload and its treap links. Keeping the payload in the
    /// slot is what makes a group cost one heap allocation instead of two: the slot array carries
    /// the values with it.
    struct Slot {
        int64_t count = 0;
        int32_t left = NO_SLOT;
        int32_t right = NO_SLOT;
        uint32_t priority = 0;
        Payload payload {};
    };

    /// One (value, count) entry of the sorted list a state is rebuilt from.
    struct Entry {
        ValueCopy value {};
        int64_t count = 0;
    };

    // k is the aggregate's count argument, which is never negative, so -1 can stand in for "this
    // partial state has not seen its first row yet" and the state needs no separate flag.
    static constexpr int64_t NO_K = -1;
    int64_t k = NO_K;
    // Indexed by slot; an evicted slot stays as a dead entry until compaction.
    DorisVector<Slot> slots;
    int32_t root = NO_SLOT;
    // The last live slot in the aggregate's order: the value an incoming row has to beat to enter
    // a full list. Cached so that rejecting a worse value costs one comparison, not a descent.
    int32_t worst = NO_SLOT;
    // Both counters are bounded by the slot count, which the int32_t links already cap, so 32 bits
    // are enough; keeping them (and the seed) 32-bit is what holds the state to 56 bytes.
    uint32_t live = 0;
    // Slots evicted since the last compaction.
    uint32_t dead = 0;
    uint32_t rng = next_topk_priority_seed();

    AggregateTopKCountData() = default;
    AggregateTopKCountData(const AggregateTopKCountData&) = delete;
    AggregateTopKCountData& operator=(const AggregateTopKCountData&) = delete;
};

/**
 * min_topk(x, k) / max_topk(x, k).
 *
 * Result: ARRAY<STRUCT<value T, count BIGINT>>. NULL inputs are ignored: add validates k for
 * every row and then skips a NULL x, so the aggregate deliberately does not use the
 * nullable-argument wrapper. The array is ordered by value (ascending for MIN), holds at most k
 * distinct values with count > 0, and carries no truncation flag: the caller infers it from the
 * length (a list of exactly k entries is treated as possibly truncated).
 *
 * With k = 0 the array holds at most the extreme value and its row count, which lets the
 * append-only IVM path compute MIN/MAX from the delta extreme alone.
 */
template <PrimitiveType T, bool IsMin>
class AggregateFunctionTopK final
        : public IAggregateFunctionDataHelper<AggregateTopKCountData<T>,
                                              AggregateFunctionTopK<T, IsMin>, false>,
          public MultiExpression,
          public NotNullableAggregateFunction {
public:
    using State = AggregateTopKCountData<T>;
    using Base = IAggregateFunctionDataHelper<State, AggregateFunctionTopK<T, IsMin>, false>;
    using Slot = typename State::Slot;
    using Payload = typename State::Payload;
    using ValueRef = typename State::ValueRef;
    using ValueCopy = typename State::ValueCopy;
    using Entry = typename State::Entry;
    using ColumnType = typename PrimitiveTypeTraits<T>::ColumnType;
    static constexpr bool IS_STRING = State::IS_STRING;
    static constexpr int32_t NO_SLOT = State::NO_SLOT;

    explicit AggregateFunctionTopK(const DataTypes& argument_types_) : Base(argument_types_) {
        DCHECK_EQ(argument_types_.size(), 2);
    }

    String get_name() const override { return IsMin ? "min_topk" : "max_topk"; }

    DataTypePtr get_return_type() const override {
        return make_return_type(remove_nullable(this->argument_types[0]));
    }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena& arena) const override {
        auto& state = Base::data(place);
        // k is read for every row, including rows where x is NULL: an evaluated count that is
        // NULL or out of range must fail the query instead of being skipped like a NULL value.
        check_k(state, *columns[1], row_num);
        const IColumn* value_column = columns[0];
        if (const auto* nullable = check_and_get_column<ColumnNullable>(value_column)) {
            if (nullable->is_null_at(row_num)) {
                return;
            }
            value_column = &nullable->get_nested_column();
        }
        add_value(state, *value_column, row_num);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena&) const override {
        auto& state = Base::data(place);
        const auto& other = Base::data(rhs);
        // A partial state without a k never saw any row, so it carries no information.
        if (other.k == State::NO_K) {
            return;
        }
        if (state.k == State::NO_K) {
            state.k = other.k;
        } else if (UNLIKELY(state.k != other.k)) {
            // Ordinary partial/final aggregation always shares the same constant k, but the
            // public _state/_merge/_union combinators do not encode k in the AggState type, so a
            // UNION ALL of states built with different literal k can reach this merge.
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "{} states have incompatible counts: {} vs {}", get_name(), state.k,
                            other.k);
        }
        if (state.k == 0) {
            merge_extreme(state, other);
            return;
        }
        merge_values(state, other);
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        const auto& state = Base::data(place);
        // The serialized form keeps the historical flag byte: it is derived from k in the state.
        buf.write_binary(state.k != State::NO_K);
        buf.write_binary(state.k);
        // The slots are in arrival order and may include dead ones, while the serialized form is
        // the sorted (value, count) list the reader rebuilds the tree from.
        DorisVector<int32_t> order;
        collect_in_order(state, order);
        auto sorted_values = remove_nullable(this->argument_types[0])->create_column();
        sorted_values->reserve(order.size());
        DorisVector<int64_t> sorted_counts;
        sorted_counts.reserve(order.size());
        for (int32_t slot : order) {
            insert_value(*sorted_values, value_of(state, slot));
            sorted_counts.push_back(state.slots[slot].count);
        }
        auto size = static_cast<int64_t>(sorted_counts.size());
        buf.write_binary(size);
        write_column(buf, *sorted_values);
        if (size != 0) {
            // An empty list keeps an empty vector, whose data() is null; passing it to memcpy is
            // undefined even for a zero length.
            buf.write(reinterpret_cast<const char*>(sorted_counts.data()),
                      sorted_counts.size() * sizeof(int64_t));
        }
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena&) const override {
        auto& state = Base::data(place);
        bool has_k = false;
        buf.read_binary(has_k);
        buf.read_binary(state.k);
        if (!has_k) {
            state.k = State::NO_K;
        }
        int64_t size = 0;
        buf.read_binary(size);
        auto values = read_column(buf);
        DCHECK_EQ(static_cast<int64_t>(values->size()), size);
        DorisVector<int64_t> counts(static_cast<size_t>(size));
        if (size != 0) {
            buf.read(reinterpret_cast<char*>(counts.data()), size * sizeof(int64_t));
        }
        DorisVector<Entry> entries;
        entries.reserve(static_cast<size_t>(size));
        for (int64_t row = 0; row != size; ++row) {
            entries.push_back(Entry {value_at(*values, static_cast<size_t>(row)), counts[row]});
        }
        assign_sorted(state, std::move(entries));
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        const auto& state = Base::data(place);
        auto& result_array = assert_cast<ColumnArray&>(to);
        // The array element and both fields are nullable in the planned type, but a produced entry
        // never is.
        auto& pair_nullable = assert_cast<ColumnNullable&>(result_array.get_data());
        auto& pair_column = assert_cast<ColumnStruct&>(pair_nullable.get_nested_column());
        auto& value_column = assert_cast<ColumnNullable&>(pair_column.get_column(0));
        auto& count_column = assert_cast<ColumnNullable&>(pair_column.get_column(1));
        DorisVector<int32_t> order;
        collect_in_order(state, order);
        for (int32_t slot : order) {
            insert_value(value_column.get_nested_column(), value_of(state, slot));
            value_column.get_null_map_data().push_back(0);
            assert_cast<ColumnInt64&>(count_column.get_nested_column())
                    .get_data()
                    .push_back(state.slots[slot].count);
            count_column.get_null_map_data().push_back(0);
            pair_nullable.get_null_map_data().push_back(0);
        }
        result_array.get_offsets().push_back(pair_column.size());
    }

    static DataTypePtr make_return_type(const DataTypePtr& value_type) {
        // Both fields are nullable in the planned type: a consumer reads a field through
        // element_at, which is always nullable, and named_struct then records nullable fields.
        // Produced entries never contain a NULL field.
        auto pair_type = std::make_shared<DataTypeStruct>(
                DataTypes {make_nullable(value_type),
                           make_nullable(std::make_shared<DataTypeInt64>())},
                Strings {"value", "count"});
        // ArrayType defaults contains_null to true on the FE side, so the pair element is nullable
        // in the planned type even though the aggregate never produces a NULL entry.
        return std::make_shared<DataTypeArray>(make_nullable(pair_type));
    }

private:
    /// The value of the row in `column`. The returned view is valid while the column is alive.
    static ValueRef value_at(const IColumn& column, size_t row_num) {
        if constexpr (IS_STRING) {
            return assert_cast<const ColumnType&, TypeCheckOnRelease::DISABLE>(column).get_data_at(
                    row_num);
        } else {
            return assert_cast<const ColumnType&, TypeCheckOnRelease::DISABLE>(column)
                    .get_data()[row_num];
        }
    }

    /// Exactly the comparison the value column would apply: CompareFloat for FLOAT/DOUBLE through
    /// Compare::compare (the column uses the same helper), the same memcmp helper ColumnString
    /// uses for strings, and the column's scale-independent ordering for every other type.
    static int compare_values(ValueRef lhs, ValueRef rhs) {
        if constexpr (IS_STRING) {
            return memcmp_small_allow_overflow15(
                    reinterpret_cast<const uint8_t*>(lhs.data), lhs.size,
                    reinterpret_cast<const uint8_t*>(rhs.data), rhs.size);
        } else {
            return Compare::compare(lhs, rhs);
        }
    }

    /// The value stored in `slot`.
    static ValueRef value_of(const State& state, int32_t slot) {
        const Payload& payload = state.slots[slot].payload;
        if constexpr (IS_STRING) {
            return StringRef(state.bytes.data() + payload.offset, payload.length);
        } else {
            return payload;
        }
    }

    /// Copies `value` into `slot`'s payload, appending its bytes to the arena for strings.
    static void set_value(State& state, int32_t slot, ValueRef value) {
        if constexpr (IS_STRING) {
            Payload& payload = state.slots[slot].payload;
            payload.offset = state.bytes.size();
            payload.length = value.size;
            if (value.size != 0) {
                state.bytes.insert(value.data, value.data + value.size);
            }
            state.live_payload_bytes += value.size;
        } else {
            state.slots[slot].payload = value;
        }
    }

    /// Replaces the state's only stored value (the k == 0 replace path). A string's arena is reset
    /// with it, so the discarded bytes do not accumulate across replacements.
    static void replace_value(State& state, int32_t slot, ValueRef value) {
        if constexpr (IS_STRING) {
            state.bytes.clear();
            state.live_payload_bytes = 0;
            state.dead_payload_bytes = 0;
        }
        set_value(state, slot, value);
    }

    /// Called when `slot` is evicted: a string's bytes stay in the arena until the next compaction.
    static void evict_value(State& state, int32_t slot) {
        if constexpr (IS_STRING) {
            const size_t length = state.slots[slot].payload.length;
            state.live_payload_bytes -= length;
            state.dead_payload_bytes += length;
        }
    }

    /// Whether the discarded string payload outgrew the live one and a compaction should reclaim it.
    static bool evicted_payload_exceeds_live(const State& state) {
        if constexpr (IS_STRING) {
            return state.dead_payload_bytes > state.live_payload_bytes;
        }
        return false;
    }

    static Slot make_slot(int64_t count, uint32_t priority) {
        Slot slot;
        slot.count = count;
        slot.priority = priority;
        return slot;
    }

    static void insert_value(IColumn& column, ValueRef value) {
        if constexpr (IS_STRING) {
            assert_cast<ColumnString&>(column).insert_data(value.data, value.size);
        } else {
            assert_cast<ColumnType&>(column).get_data().push_back(value);
        }
    }

    /// True when `cmp` puts the left operand before the right one in the aggregate's order.
    static bool comes_before(int cmp) { return IsMin ? cmp < 0 : cmp > 0; }

    /// True when `cmp`, which compares a value with the worst retained one, puts it after it: the
    /// value is worse and cannot enter the list.
    static bool is_worse(int cmp) { return IsMin ? cmp > 0 : cmp < 0; }

    /// splitmix32. The priorities only have to be independent enough to keep the treap balanced;
    /// a per-state sequence keeps the shape reproducible for the same insertion order.
    static uint32_t next_priority(State& state) {
        uint32_t z = (state.rng += 0x9E3779B9U);
        z = (z ^ (z >> 16)) * 0x21F0AAADU;
        z = (z ^ (z >> 15)) * 0x735A2D97U;
        return z ^ (z >> 15);
    }

    static int compare_slots(const State& state, int32_t lhs, int32_t rhs) {
        return compare_values(value_of(state, lhs), value_of(state, rhs));
    }

    /// The treap links are int32_t, so a state can index at most INT32_MAX + 1 slots.
    static constexpr size_t MAX_SLOTS =
            static_cast<size_t>(std::numeric_limits<int32_t>::max()) + 1;

    /// k has no upper bound and the FE cannot see the state size, so the representational limit is
    /// enforced where a state actually grows. A merge can exceed it as well: two states each
    /// within the limit can hold more distinct values together than one can index.
    static void check_slot_capacity(size_t size) {
        if (UNLIKELY(size > MAX_SLOTS)) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "min_topk/max_topk cannot keep more than {} distinct values",
                            static_cast<int64_t>(MAX_SLOTS));
        }
    }

    /// Appends (value, count) as the next slot.
    static int32_t append_slot(State& state, ValueRef value, int64_t count) {
        check_slot_capacity(state.slots.size() + 1);
        auto slot = static_cast<int32_t>(state.slots.size());
        state.slots.push_back(make_slot(count, next_priority(state)));
        set_value(state, slot, value);
        ++state.live;
        return slot;
    }

    static int32_t append_slot(State& state, const IColumn& column, size_t row_num, int64_t count) {
        return append_slot(state, value_at(column, row_num), count);
    }

    /// Rotates the subtree rooted at `node` right: the left child becomes the root and `node` its
    /// right child. The in-order sequence is unchanged, so the treap stays ordered.
    static int32_t rotate_right(State& state, int32_t node) {
        int32_t pivot = state.slots[node].left;
        state.slots[node].left = state.slots[pivot].right;
        state.slots[pivot].right = node;
        return pivot;
    }

    static int32_t rotate_left(State& state, int32_t node) {
        int32_t pivot = state.slots[node].right;
        state.slots[node].right = state.slots[pivot].left;
        state.slots[pivot].left = node;
        return pivot;
    }

    /// One descent that performs the whole per-row update: an equal value bumps its count and
    /// reports no insertion; a missing value is linked as a new slot at the leaf and rotated up
    /// while the recursion unwinds. `inserted_slot` receives the new slot, NO_SLOT on a hit.
    /// The count is the number of rows the caller adds (1 for add, the singleton's count for a
    /// merge).
    static int32_t add_or_increment(State& state, int32_t node, ValueRef value, int64_t count,
                                    int32_t* inserted_slot) {
        if (node == State::NO_SLOT) {
            *inserted_slot = append_slot(state, value, count);
            return *inserted_slot;
        }
        int cmp = compare_values(value_of(state, node), value);
        if (cmp == 0) {
            state.slots[node].count = checked_add(state.slots[node].count, count);
            return node;
        }
        if (comes_before(cmp)) {
            // The stored value is the better one, so the input belongs in the right subtree.
            state.slots[node].right =
                    add_or_increment(state, state.slots[node].right, value, count, inserted_slot);
            if (state.slots[state.slots[node].right].priority > state.slots[node].priority) {
                return rotate_left(state, node);
            }
        } else {
            state.slots[node].left =
                    add_or_increment(state, state.slots[node].left, value, count, inserted_slot);
            if (state.slots[state.slots[node].left].priority > state.slots[node].priority) {
                return rotate_right(state, node);
            }
        }
        return node;
    }

    /// Removes the last entry in the aggregate's order (the largest value for MIN) and returns the
    /// new subtree root. That entry has no right child, so its left subtree takes its place; the
    /// child's priority is below the parent's, so no rotation is needed.
    static int32_t erase_worst(State& state, int32_t node, int32_t* removed) {
        if (state.slots[node].right == State::NO_SLOT) {
            *removed = node;
            return state.slots[node].left;
        }
        state.slots[node].right = erase_worst(state, state.slots[node].right, removed);
        return node;
    }

    static void refresh_worst(State& state) {
        int32_t node = state.root;
        while (node != State::NO_SLOT && state.slots[node].right != State::NO_SLOT) {
            node = state.slots[node].right;
        }
        state.worst = node;
    }

    /// The first live slot in the aggregate's order, i.e. the best value the state holds.
    static int32_t best_slot(const State& state) {
        int32_t node = state.root;
        while (node != State::NO_SLOT && state.slots[node].left != State::NO_SLOT) {
            node = state.slots[node].left;
        }
        return node;
    }

    /// Slots in the aggregate's order, i.e. the order of the result array.
    static void collect_in_order(const State& state, DorisVector<int32_t>& slots) {
        slots.reserve(static_cast<size_t>(state.live));
        DorisVector<int32_t> stack;
        stack.reserve(64);
        int32_t node = state.root;
        while (node != State::NO_SLOT || !stack.empty()) {
            while (node != State::NO_SLOT) {
                stack.push_back(node);
                node = state.slots[node].left;
            }
            node = stack.back();
            stack.pop_back();
            // Only live slots are reachable from the root; an evicted slot has no row count.
            DCHECK_GE(state.slots[node].count, 0);
            slots.push_back(node);
            node = state.slots[node].right;
        }
        DCHECK_EQ(static_cast<int64_t>(slots.size()), state.live);
    }

    /// Rebuilds the tree from `values`, already in the aggregate's order.
    /// The treap is built in O(size) as a Cartesian tree over the fresh priorities.
    static void assign_sorted(State& state, DorisVector<Entry> values) {
        const size_t size = values.size();
        check_slot_capacity(size);
        state.slots.assign(size, Slot {});
        for (size_t slot = 0; slot != size; ++slot) {
            state.slots[slot].count = values[slot].count;
        }
        if constexpr (IS_STRING) {
            // The entries may point into this state's own arena, so the new arena is built aside
            // and swapped in before the old one is dropped.
            PaddedPODArray<char, 32> bytes;
            size_t live_bytes = 0;
            for (size_t slot = 0; slot != size; ++slot) {
                const StringRef value = values[slot].value;
                Payload& payload = state.slots[slot].payload;
                payload.offset = bytes.size();
                payload.length = value.size;
                if (value.size != 0) {
                    bytes.insert(value.data, value.data + value.size);
                }
                live_bytes += value.size;
            }
            state.bytes.swap(bytes);
            state.live_payload_bytes = live_bytes;
            state.dead_payload_bytes = 0;
        } else {
            for (size_t slot = 0; slot != size; ++slot) {
                state.slots[slot].payload = values[slot].value;
            }
        }
        state.live = static_cast<uint32_t>(size);
        state.dead = 0;
        DorisVector<int32_t> stack;
        stack.reserve(size);
        for (size_t slot = 0; slot != size; ++slot) {
            state.slots[slot].priority = next_priority(state);
            int32_t last = State::NO_SLOT;
            while (!stack.empty() &&
                   state.slots[stack.back()].priority < state.slots[slot].priority) {
                last = stack.back();
                stack.pop_back();
            }
            state.slots[slot].left = last;
            if (!stack.empty()) {
                state.slots[stack.back()].right = static_cast<int32_t>(slot);
            }
            stack.push_back(static_cast<int32_t>(slot));
        }
        state.root = stack.empty() ? State::NO_SLOT : stack.front();
        refresh_worst(state);
    }

    /// Reclaims the slots evicted since the last compaction. The O(k) rebuild is charged to the
    /// k/2 evictions that produced the dead slots, so it stays O(1) amortized per row.
    static void compact(State& state) {
        DorisVector<int32_t> slots;
        collect_in_order(state, slots);
        DorisVector<Entry> values;
        values.reserve(slots.size());
        for (int32_t slot : slots) {
            values.push_back(Entry {value_of(state, slot), state.slots[slot].count});
        }
        assign_sorted(state, std::move(values));
    }

    /// Reclaims the dead slots or the evicted payload once it outnumbers the live one. Checked
    /// right after an eviction as well, so the last eviction of a group does not keep a large
    /// discarded payload until the next row arrives.
    static void maybe_compact(State& state) {
        if (state.dead > state.live || evicted_payload_exceeds_live(state)) {
            compact(state);
        }
    }

    static void check_k(State& state, const IColumn& count_column, size_t row_num) {
        const IColumn* nested = &count_column;
        if (const auto* nullable = check_and_get_column<ColumnNullable>(&count_column)) {
            if (UNLIKELY(nullable->is_null_at(row_num))) {
                throw Exception(ErrorCode::INVALID_ARGUMENT,
                                "the count of min_topk and max_topk must be a non-null "
                                "constant");
            }
            nested = &nullable->get_nested_column();
        }
        int64_t value =
                assert_cast<const ColumnInt64&, TypeCheckOnRelease::DISABLE>(*nested).get_element(
                        row_num);
        if (state.k == State::NO_K) {
            if (UNLIKELY(value < 0)) {
                throw Exception(ErrorCode::INVALID_ARGUMENT,
                                "the count of min_topk and max_topk must be a non-negative "
                                "constant, but got {}",
                                value);
            }
            state.k = value;
        } else {
            // k comes from the same constant expression on every row and every partial state.
            DCHECK_EQ(state.k, value);
        }
    }

    static int64_t checked_add(int64_t lhs, int64_t rhs) {
        int64_t result = 0;
        if (UNLIKELY(__builtin_add_overflow(lhs, rhs, &result))) {
            throw Exception(ErrorCode::INVALID_ARGUMENT, "min_topk/max_topk value count overflow");
        }
        return result;
    }

    /// k == 0 fast path: only the extreme value is tracked.
    static void add_value(State& state, const IColumn& column, size_t row_num) {
        if (state.k == 0) {
            if (state.live == 0) {
                state.root = append_slot(state, column, row_num, 1);
                state.worst = state.root;
                return;
            }
            int cmp = compare_values(value_at(column, row_num), value_of(state, 0));
            if (cmp == 0) {
                state.slots[0].count = checked_add(state.slots[0].count, 1);
            } else if (comes_before(cmp)) {
                // The new value is better: replace the tracked extreme. The slot and the
                // single-node tree stay in place, only the stored value changes.
                replace_value(state, 0, value_at(column, row_num));
                state.slots[0].count = 1;
            }
            return;
        }

        // Dead slots and evicted payload are reclaimed once they outnumber the live ones.
        maybe_compact(state);

        if (state.live >= state.k) {
            // A full list rejects or absorbs most rows: comparing with the eviction candidate
            // costs one comparison, while the tree descent would cost log2(k).
            int cmp = compare_values(value_at(column, row_num), value_of(state, state.worst));
            if (cmp == 0) {
                state.slots[state.worst].count = checked_add(state.slots[state.worst].count, 1);
                return;
            }
            if (is_worse(cmp)) {
                return;
            }
        }

        int32_t inserted = State::NO_SLOT;
        state.root = add_or_increment(state, state.root, value_at(column, row_num), 1, &inserted);
        if (inserted == State::NO_SLOT) {
            return;
        }
        if (state.live <= state.k) {
            // Not over capacity: the new entry competes with the cached worst for the eviction
            // candidate role.
            if (state.worst == State::NO_SLOT ||
                is_worse(compare_slots(state, inserted, state.worst))) {
                state.worst = inserted;
            }
            return;
        }
        // The list was full and the value is new. It beat the cached worst above, so it cannot be
        // the worst itself; the old worst is still the rightmost entry and leaves the list.
        int32_t removed = State::NO_SLOT;
        state.root = erase_worst(state, state.root, &removed);
        DCHECK_EQ(removed, state.worst);
        evict_value(state, removed);
        state.slots[removed].count = State::DEAD_COUNT;
        --state.live;
        ++state.dead;
        refresh_worst(state);
        maybe_compact(state);
    }

    /// k == 0: the state holds at most the extreme value and its count.
    static void merge_extreme(State& state, const State& other) {
        if (other.live == 0) {
            return;
        }
        if (state.live == 0) {
            state.root = append_slot(state, value_of(other, 0), other.slots[0].count);
            state.worst = state.root;
            return;
        }
        int cmp = compare_values(value_of(state, 0), value_of(other, 0));
        if (cmp == 0) {
            state.slots[0].count = checked_add(state.slots[0].count, other.slots[0].count);
        } else if (is_worse(cmp)) {
            replace_value(state, 0, value_of(other, 0));
            state.slots[0].count = other.slots[0].count;
        }
    }

    /// Merges two ordered lists, summing the counts of equal values and keeping the k best.
    ///
    /// The merge stops as soon as k entries are produced. Both inputs are best-first and hold
    /// distinct values, and equal values have already been combined, so every remaining value is
    /// worse than all retained ones: it can neither enter the result nor overflow it. Stopping
    /// here also avoids materializing a tail that would be truncated away.
    static void merge_values(State& state, const State& other) {
        if (other.live == 0) {
            // The other partial state saw no non-NULL row.
            return;
        }
        // `_state`/`_merge`/`_union` merges one serialized singleton per row, so a single-entry
        // RHS is exactly one add, whether or not the list is full: a retained value (including an
        // interior one) only bumps its count, and a new one is inserted, evicting the worst only
        // when the list was already full. Neither copies the retained values nor rebuilds the
        // tree.
        if (other.live == 1) {
            const int32_t other_only = other.root;
            ValueRef value = value_of(other, other_only);
            if (state.live >= state.k &&
                is_worse(compare_values(value, value_of(state, state.worst)))) {
                // A full list rejects a worse value without touching the tree.
                return;
            }
            int32_t inserted = State::NO_SLOT;
            state.root = add_or_increment(state, state.root, value, other.slots[other_only].count,
                                          &inserted);
            if (inserted == State::NO_SLOT) {
                return;
            }
            if (state.live <= state.k) {
                // There was room: the new entry competes with the cached worst for the eviction
                // candidate role, exactly like the add path.
                if (state.worst == State::NO_SLOT ||
                    is_worse(compare_slots(state, inserted, state.worst))) {
                    state.worst = inserted;
                }
                return;
            }
            // The list was full, so the new entry is one over capacity. It beat the cached worst
            // above, so the old worst is still the last entry and leaves the list.
            int32_t removed = State::NO_SLOT;
            state.root = erase_worst(state, state.root, &removed);
            DCHECK_EQ(removed, state.worst);
            evict_value(state, removed);
            state.slots[removed].count = State::DEAD_COUNT;
            --state.live;
            ++state.dead;
            refresh_worst(state);
            maybe_compact(state);
            return;
        }
        if (state.live >= state.k) {
            // A full state already holds the k best values. If every value of the other state is
            // worse than the worst one retained here, the merge cannot change the result, so skip
            // the collect/rebuild.
            const int32_t other_best = best_slot(other);
            int cmp = compare_values(value_of(other, other_best), value_of(state, state.worst));
            if (is_worse(cmp)) {
                return;
            }
            if (cmp == 0) {
                // The other state's best value is the worst retained one. Its remaining values are
                // distinct and worse, so they cannot enter either: only that count changes, and
                // the tree stays as it is.
                state.slots[state.worst].count =
                        checked_add(state.slots[state.worst].count, other.slots[other_best].count);
                return;
            }
        }
        DorisVector<int32_t> lhs_slots;
        DorisVector<int32_t> rhs_slots;
        collect_in_order(state, lhs_slots);
        collect_in_order(other, rhs_slots);
        DorisVector<Entry> merged;
        merge_sorted_slots(state, lhs_slots, other, rhs_slots, static_cast<size_t>(state.k),
                           merged);
        check_slot_capacity(merged.size());
        assign_sorted(state, std::move(merged));
    }

    /// Merges two best-first slot lists into at most `limit` entries in the aggregate's order,
    /// summing the counts of equal values. Both inputs hold distinct values, so a value appears at
    /// most twice.
    static void merge_sorted_slots(const State& state, const DorisVector<int32_t>& lhs_slots,
                                   const State& other, const DorisVector<int32_t>& rhs_slots,
                                   size_t limit, DorisVector<Entry>& merged) {
        merged.reserve(std::min(limit, lhs_slots.size() + rhs_slots.size()));
        size_t lhs_index = 0;
        size_t rhs_index = 0;
        while (merged.size() < limit &&
               (lhs_index < lhs_slots.size() || rhs_index < rhs_slots.size())) {
            if (rhs_index == rhs_slots.size()) {
                merged.push_back(Entry {value_of(state, lhs_slots[lhs_index]),
                                        state.slots[lhs_slots[lhs_index]].count});
                ++lhs_index;
            } else if (lhs_index == lhs_slots.size()) {
                merged.push_back(Entry {value_of(other, rhs_slots[rhs_index]),
                                        other.slots[rhs_slots[rhs_index]].count});
                ++rhs_index;
            } else {
                ValueRef lhs_value = value_of(state, lhs_slots[lhs_index]);
                ValueRef rhs_value = value_of(other, rhs_slots[rhs_index]);
                int cmp = compare_values(lhs_value, rhs_value);
                // Both lists are ordered best-first (ascending for MIN, descending for MAX).
                const bool lhs_is_better = comes_before(cmp);
                if (lhs_is_better) {
                    merged.push_back(Entry {lhs_value, state.slots[lhs_slots[lhs_index]].count});
                    ++lhs_index;
                } else if (cmp != 0) {
                    // Not better and not equal: the right-hand value is better.
                    merged.push_back(Entry {rhs_value, other.slots[rhs_slots[rhs_index]].count});
                    ++rhs_index;
                } else {
                    merged.push_back(Entry {lhs_value,
                                            checked_add(state.slots[lhs_slots[lhs_index]].count,
                                                        other.slots[rhs_slots[rhs_index]].count)});
                    ++lhs_index;
                    ++rhs_index;
                }
            }
        }
    }

    /// Writes a length-prefixed column blob. The declared length is
    /// get_uncompressed_serialized_bytes(), which is an upper bound: above
    /// SERIALIZED_MEM_SIZE_LIMIT the serde may compress the payload and return an earlier end
    /// pointer, so the reader skips the same declared length and the unused tail is padding.
    void write_column(BufferWritable& buf, const IColumn& column) const {
        const auto value_type = remove_nullable(this->argument_types[0]);
        int64_t capacity =
                value_type->get_uncompressed_serialized_bytes(column, IAggregateFunction::version);
        buf.write_binary(capacity);
        buf.resize(capacity);
        auto* pos = value_type->serialize(column, buf.data(), IAggregateFunction::version);
        DCHECK_LE(pos, buf.data() + capacity);
        buf.add_offset(capacity);
    }

    MutableColumnPtr read_column(BufferReadable& buf) const {
        const auto value_type = remove_nullable(this->argument_types[0]);
        int64_t capacity = 0;
        buf.read_binary(capacity);
        MutableColumnPtr column = value_type->create_column();
        const auto* pos = value_type->deserialize(buf.data(), &column, IAggregateFunction::version);
        DCHECK_LE(pos, buf.data() + capacity);
        buf.add_offset(capacity);
        return column;
    }
};

} // namespace doris
