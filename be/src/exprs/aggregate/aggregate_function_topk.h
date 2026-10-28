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
#include <cstdint>
#include <limits>
#include <memory>
#include <utility>

#include "common/check.h"
#include "common/compare.h"
#include "common/exception.h"
#include "core/assert_cast.h"
#include "core/column/column.h"
#include "core/column/column_array.h"
#include "core/column/column_nullable.h"
#include "core/column/column_struct.h"
#include "core/column/column_vector.h"
#include "core/custom_allocator.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type/primitive_type.h"
#include "core/string_buffer.hpp"
#include "core/types.h"
#include "exprs/aggregate/aggregate_function.h"

namespace doris {

/**
 * State shared by min_topk and max_topk.
 *
 * The distinct values sit in the slots of a column, with one entry per slot carrying the row count
 * and the links of a treap ordered best-first (ascending values for MIN, descending for MAX). A
 * row costs one O(log k) descent: it either finds its value and bumps the count or links a new slot
 * at the leaf. A sorted vector would shift O(k) column elements for the same insert.
 *
 * The treap stores explicit child links because, unlike a heap, its shape is not the complete tree
 * that an array index can describe: rotations move entries, and the in-order sequence is the value
 * order rather than the arrival order. A heap could evict and append in O(log k), but it cannot
 * find an existing value without a hash index or a linear scan, and it cannot produce the sorted
 * result without sorting. The ordered tree is the structure that keeps find, insert and eviction
 * all at O(log k) with comparisons only.
 *
 * An entry that is evicted keeps its slot and value until the next compaction: no column type
 * supports replacing an element in place, so reusing a slot would require rebuilding the column
 * anyway. The tree never points at a dead slot.
 *
 * With k == 0 the state holds at most the single extreme value and its row count; the caller
 * derives truncation from the list length, so the state carries no separate bound.
 */
struct AggregateTopKCountData {
    static constexpr int32_t NO_SLOT = -1;
    // Row counts are never negative, so this cannot collide with a live entry.
    static constexpr int64_t DEAD_COUNT = -1;

    /// One slot's metadata; the value itself lives in `value_column` at the same index, because
    /// its type is only known at runtime.
    struct Slot {
        int64_t count = 0;
        int32_t left = NO_SLOT;
        int32_t right = NO_SLOT;
        uint32_t priority = 0;
    };

    const DataTypePtr value_type;
    bool has_k = false;
    int64_t k = 0;
    MutableColumnPtr value_column;
    // Indexed by slot; evicted slots stay as dead entries until compaction. 24 bytes per slot
    // (1.5 MiB at k = 65536) on top of the value column, and DorisVector keeps the allocation
    // inside the query's memory accounting.
    DorisVector<Slot> slots;
    int32_t root = NO_SLOT;
    // The last live slot in the aggregate's order: the value an incoming row has to beat to enter
    // a full list. Cached so that rejecting a worse value costs one comparison, not a descent.
    int32_t worst = NO_SLOT;
    int64_t live = 0;
    // Slots evicted since the last compaction.
    int64_t dead = 0;
    uint64_t rng = 0x243F6A8885A308D3ULL;

    explicit AggregateTopKCountData(const DataTypes& argument_types_)
            : value_type(remove_nullable(argument_types_[0])),
              value_column(value_type->create_column()) {}

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
        : public IAggregateFunctionDataHelper<AggregateTopKCountData,
                                              AggregateFunctionTopK<T, IsMin>, true>,
          public MultiExpression,
          public NotNullableAggregateFunction {
public:
    using Base = IAggregateFunctionDataHelper<AggregateTopKCountData,
                                              AggregateFunctionTopK<T, IsMin>, true>;

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
        // A partial state without has_k never saw any row, so it carries no information.
        if (!other.has_k) {
            return;
        }
        if (!state.has_k) {
            state.has_k = true;
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
        buf.write_binary(state.has_k);
        buf.write_binary(state.k);
        // The slots are in arrival order and may include dead ones, while the serialized form is
        // the sorted (value, count) list the reader rebuilds the tree from.
        DorisVector<int32_t> slots;
        collect_in_order(state, slots);
        auto sorted_values = state.value_column->clone_empty();
        sorted_values->reserve(slots.size());
        DorisVector<int64_t> sorted_counts;
        sorted_counts.reserve(slots.size());
        for (int32_t slot : slots) {
            sorted_values->insert_from(*state.value_column, slot);
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
        buf.read_binary(state.has_k);
        buf.read_binary(state.k);
        int64_t size = 0;
        buf.read_binary(size);
        auto values = read_column(buf);
        DCHECK_EQ(static_cast<int64_t>(values->size()), size);
        DorisVector<int64_t> counts(static_cast<size_t>(size));
        if (size != 0) {
            buf.read(reinterpret_cast<char*>(counts.data()), size * sizeof(int64_t));
        }
        assign_sorted(state, std::move(values), std::move(counts));
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
        DorisVector<int32_t> slots;
        collect_in_order(state, slots);
        for (int32_t slot : slots) {
            if (const auto* nullable_value =
                        check_and_get_column<ColumnNullable>(state.value_column.get())) {
                // A nullable value argument carries its own null map, which is never set here.
                value_column.insert_from(*nullable_value, slot);
            } else {
                value_column.get_nested_column().insert_from(*state.value_column, slot);
                value_column.get_null_map_data().push_back(0);
            }
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
    /// True when `cmp` puts the left operand before the right one in the aggregate's order.
    static bool comes_before(int cmp) { return IsMin ? cmp < 0 : cmp > 0; }

    /// True when `cmp`, which compares a value with the worst retained one, puts it after it: the
    /// value is worse and cannot enter the list.
    static bool is_worse(int cmp) { return IsMin ? cmp > 0 : cmp < 0; }

    /// splitmix64. The priorities only have to be independent enough to keep the treap balanced;
    /// a per-state sequence keeps the shape reproducible for the same insertion order.
    static uint32_t next_priority(AggregateTopKCountData& state) {
        uint64_t z = (state.rng += 0x9E3779B97F4A7C15ULL);
        z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
        z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
        return static_cast<uint32_t>(z ^ (z >> 31));
    }

    static int compare_slots(const AggregateTopKCountData& state, int32_t lhs, int32_t rhs) {
        return compare_at(*state.value_column, lhs, *state.value_column, rhs);
    }

    /// Appends (value, count) as the next slot. A value never moves once it has a slot: the tree
    /// reorders slots, not column rows.
    static int32_t append_slot(AggregateTopKCountData& state, const IColumn& column, size_t row_num,
                               int64_t count) {
        auto slot = static_cast<int32_t>(state.slots.size());
        DCHECK_LT(slot, std::numeric_limits<int32_t>::max());
        state.value_column->insert_from(column, row_num);
        state.slots.push_back(AggregateTopKCountData::Slot {count, AggregateTopKCountData::NO_SLOT,
                                                            AggregateTopKCountData::NO_SLOT,
                                                            next_priority(state)});
        ++state.live;
        return slot;
    }

    /// Rotates the subtree rooted at `node` right: the left child becomes the root and `node` its
    /// right child. The in-order sequence is unchanged, so the treap stays ordered.
    static int32_t rotate_right(AggregateTopKCountData& state, int32_t node) {
        int32_t pivot = state.slots[node].left;
        state.slots[node].left = state.slots[pivot].right;
        state.slots[pivot].right = node;
        return pivot;
    }

    static int32_t rotate_left(AggregateTopKCountData& state, int32_t node) {
        int32_t pivot = state.slots[node].right;
        state.slots[node].right = state.slots[pivot].left;
        state.slots[pivot].left = node;
        return pivot;
    }

    /// One descent that performs the whole per-row update: an equal value bumps its count and
    /// reports no insertion; a missing value is linked as a new slot at the leaf and rotated up
    /// while the recursion unwinds. `inserted_slot` receives the new slot, NO_SLOT on a hit.
    static int32_t add_or_increment(AggregateTopKCountData& state, int32_t node,
                                    const IColumn& column, size_t row_num, int32_t* inserted_slot) {
        if (node == AggregateTopKCountData::NO_SLOT) {
            *inserted_slot = append_slot(state, column, row_num, 1);
            return *inserted_slot;
        }
        int cmp = compare_at(*state.value_column, node, column, row_num);
        if (cmp == 0) {
            state.slots[node].count = checked_add(state.slots[node].count, 1);
            return node;
        }
        if (comes_before(cmp)) {
            // The stored value is the better one, so the input belongs in the right subtree.
            state.slots[node].right = add_or_increment(state, state.slots[node].right, column,
                                                       row_num, inserted_slot);
            if (state.slots[state.slots[node].right].priority > state.slots[node].priority) {
                return rotate_left(state, node);
            }
        } else {
            state.slots[node].left =
                    add_or_increment(state, state.slots[node].left, column, row_num, inserted_slot);
            if (state.slots[state.slots[node].left].priority > state.slots[node].priority) {
                return rotate_right(state, node);
            }
        }
        return node;
    }

    /// Removes the last entry in the aggregate's order (the largest value for MIN) and returns the
    /// new subtree root. That entry has no right child, so its left subtree takes its place; the
    /// child's priority is below the parent's, so no rotation is needed.
    static int32_t erase_worst(AggregateTopKCountData& state, int32_t node, int32_t* removed) {
        if (state.slots[node].right == AggregateTopKCountData::NO_SLOT) {
            *removed = node;
            return state.slots[node].left;
        }
        state.slots[node].right = erase_worst(state, state.slots[node].right, removed);
        return node;
    }

    static void refresh_worst(AggregateTopKCountData& state) {
        int32_t node = state.root;
        while (node != AggregateTopKCountData::NO_SLOT &&
               state.slots[node].right != AggregateTopKCountData::NO_SLOT) {
            node = state.slots[node].right;
        }
        state.worst = node;
    }

    /// Slots in the aggregate's order, i.e. the order of the result array.
    static void collect_in_order(const AggregateTopKCountData& state, DorisVector<int32_t>& slots) {
        slots.reserve(static_cast<size_t>(state.live));
        DorisVector<int32_t> stack;
        stack.reserve(64);
        int32_t node = state.root;
        while (node != AggregateTopKCountData::NO_SLOT || !stack.empty()) {
            while (node != AggregateTopKCountData::NO_SLOT) {
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

    /// Rebuilds the state from a (value, count) sequence already in the aggregate's order. The
    /// treap is built in O(size) as a Cartesian tree over the fresh priorities.
    static void assign_sorted(AggregateTopKCountData& state, MutableColumnPtr values,
                              DorisVector<int64_t> counts) {
        const size_t size = counts.size();
        DCHECK_EQ(values->size(), size);
        state.value_column = std::move(values);
        state.slots.assign(size, AggregateTopKCountData::Slot {});
        for (size_t slot = 0; slot != size; ++slot) {
            state.slots[slot].count = counts[slot];
        }
        state.live = static_cast<int64_t>(size);
        state.dead = 0;
        DorisVector<int32_t> stack;
        stack.reserve(size);
        for (size_t slot = 0; slot != size; ++slot) {
            state.slots[slot].priority = next_priority(state);
            int32_t last = AggregateTopKCountData::NO_SLOT;
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
        state.root = stack.empty() ? AggregateTopKCountData::NO_SLOT : stack.front();
        refresh_worst(state);
    }

    /// Reclaims the slots evicted since the last compaction. The O(k) rebuild is charged to the
    /// k/2 evictions that produced the dead slots, so it stays O(1) amortized per row.
    static void compact(AggregateTopKCountData& state) {
        DorisVector<int32_t> slots;
        collect_in_order(state, slots);
        auto values = state.value_column->clone_empty();
        values->reserve(slots.size());
        DorisVector<int64_t> counts;
        counts.reserve(slots.size());
        for (int32_t slot : slots) {
            values->insert_from(*state.value_column, slot);
            counts.push_back(state.slots[slot].count);
        }
        assign_sorted(state, std::move(values), std::move(counts));
    }

    static void check_k(AggregateTopKCountData& state, const IColumn& count_column,
                        size_t row_num) {
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
        if (!state.has_k) {
            if (UNLIKELY(value < 0)) {
                throw Exception(ErrorCode::INVALID_ARGUMENT,
                                "the count of min_topk and max_topk must be a non-negative "
                                "constant, but got {}",
                                value);
            }
            state.has_k = true;
            state.k = value;
        } else {
            // k comes from the same constant expression on every row and every partial state.
            DCHECK_EQ(state.k, value);
        }
    }

    /// Ordering of the values. FLOAT/DOUBLE use CompareFloat so that NaN sorts greatest and is
    /// distinct from every number, which keeps the list mergeable and the top-K deterministic;
    /// every other type uses the column's compare_at, i.e. exactly the MIN/MAX comparison.
    static int compare_at(const IColumn& lhs, size_t lhs_row, const IColumn& rhs, size_t rhs_row) {
        if constexpr (is_float_or_double(T)) {
            using ColumnType = typename PrimitiveTypeTraits<T>::ColumnType;
            const auto& left =
                    assert_cast<const ColumnType&, TypeCheckOnRelease::DISABLE>(lhs).get_data();
            const auto& right =
                    assert_cast<const ColumnType&, TypeCheckOnRelease::DISABLE>(rhs).get_data();
            return CompareFloat(left[lhs_row], right[rhs_row]);
        } else {
            return lhs.compare_at(lhs_row, rhs_row, rhs, 1);
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
    static void add_value(AggregateTopKCountData& state, const IColumn& column, size_t row_num) {
        if (state.k == 0) {
            if (state.live == 0) {
                state.root = append_slot(state, column, row_num, 1);
                state.worst = state.root;
                return;
            }
            int cmp = compare_at(column, row_num, *state.value_column, 0);
            if (cmp == 0) {
                state.slots[0].count = checked_add(state.slots[0].count, 1);
            } else if (comes_before(cmp)) {
                // The new value is better: replace the tracked extreme. The slot and the
                // single-node tree stay in place, only the column row changes.
                state.value_column->clear();
                state.value_column->insert_from(column, row_num);
                state.slots[0].count = 1;
            }
            return;
        }

        // Dead slots are reclaimed before they can outnumber the live ones; see compact().
        if (state.dead > state.live) {
            compact(state);
        }

        if (state.live >= state.k) {
            // A full list rejects or absorbs most rows: comparing with the eviction candidate
            // costs one comparison, while the tree descent would cost log2(k).
            int cmp = compare_at(column, row_num, *state.value_column, state.worst);
            if (cmp == 0) {
                state.slots[state.worst].count = checked_add(state.slots[state.worst].count, 1);
                return;
            }
            if (is_worse(cmp)) {
                return;
            }
        }

        int32_t inserted = AggregateTopKCountData::NO_SLOT;
        state.root = add_or_increment(state, state.root, column, row_num, &inserted);
        if (inserted == AggregateTopKCountData::NO_SLOT) {
            return;
        }
        if (state.live <= state.k) {
            // Not over capacity: the new entry competes with the cached worst for the eviction
            // candidate role.
            if (state.worst == AggregateTopKCountData::NO_SLOT ||
                is_worse(compare_slots(state, inserted, state.worst))) {
                state.worst = inserted;
            }
            return;
        }
        // The list was full and the value is new. It beat the cached worst above, so it cannot be
        // the worst itself; the old worst is still the rightmost entry and leaves the list.
        int32_t removed = AggregateTopKCountData::NO_SLOT;
        state.root = erase_worst(state, state.root, &removed);
        DCHECK_EQ(removed, state.worst);
        state.slots[removed].count = AggregateTopKCountData::DEAD_COUNT;
        --state.live;
        ++state.dead;
        refresh_worst(state);
    }

    /// k == 0: the state holds at most the extreme value and its count.
    static void merge_extreme(AggregateTopKCountData& state, const AggregateTopKCountData& other) {
        if (other.live == 0) {
            return;
        }
        if (state.live == 0) {
            state.root = append_slot(state, *other.value_column, 0, other.slots[0].count);
            state.worst = state.root;
            return;
        }
        int cmp = compare_at(*state.value_column, 0, *other.value_column, 0);
        if (cmp == 0) {
            state.slots[0].count = checked_add(state.slots[0].count, other.slots[0].count);
        } else if (is_worse(cmp)) {
            state.value_column->clear();
            state.value_column->insert_from(*other.value_column, 0);
            state.slots[0].count = other.slots[0].count;
        }
    }

    /// Merges two ordered lists, summing the counts of equal values and keeping the k best.
    ///
    /// The merge stops as soon as k entries are produced. Both inputs are best-first and hold
    /// distinct values, and equal values have already been combined, so every remaining value is
    /// worse than all retained ones: it can neither enter the result nor overflow it. Stopping
    /// here also avoids materializing a tail that would be truncated away.
    static void merge_values(AggregateTopKCountData& state, const AggregateTopKCountData& other) {
        if (other.live == 0) {
            // The other partial state saw no non-NULL row.
            return;
        }
        DorisVector<int32_t> lhs_slots;
        DorisVector<int32_t> rhs_slots;
        collect_in_order(state, lhs_slots);
        collect_in_order(other, rhs_slots);
        const auto limit = static_cast<size_t>(state.k);
        auto merged = state.value_column->clone_empty();
        DorisVector<int64_t> merged_counts;
        merged_counts.reserve(std::min(limit, lhs_slots.size() + rhs_slots.size()));
        size_t lhs_index = 0;
        size_t rhs_index = 0;
        while (merged_counts.size() < limit &&
               (lhs_index < lhs_slots.size() || rhs_index < rhs_slots.size())) {
            if (rhs_index == rhs_slots.size()) {
                merged->insert_from(*state.value_column, lhs_slots[lhs_index]);
                merged_counts.push_back(state.slots[lhs_slots[lhs_index]].count);
                ++lhs_index;
            } else if (lhs_index == lhs_slots.size()) {
                merged->insert_from(*other.value_column, rhs_slots[rhs_index]);
                merged_counts.push_back(other.slots[rhs_slots[rhs_index]].count);
                ++rhs_index;
            } else {
                int cmp = compare_at(*state.value_column, lhs_slots[lhs_index], *other.value_column,
                                     rhs_slots[rhs_index]);
                // Both lists are ordered best-first (ascending for MIN, descending for MAX).
                const bool lhs_is_better = comes_before(cmp);
                if (lhs_is_better) {
                    merged->insert_from(*state.value_column, lhs_slots[lhs_index]);
                    merged_counts.push_back(state.slots[lhs_slots[lhs_index]].count);
                    ++lhs_index;
                } else if (cmp != 0) {
                    // Not better and not equal: the right-hand value is better.
                    merged->insert_from(*other.value_column, rhs_slots[rhs_index]);
                    merged_counts.push_back(other.slots[rhs_slots[rhs_index]].count);
                    ++rhs_index;
                } else {
                    merged->insert_from(*state.value_column, lhs_slots[lhs_index]);
                    merged_counts.push_back(checked_add(state.slots[lhs_slots[lhs_index]].count,
                                                        other.slots[rhs_slots[rhs_index]].count));
                    ++lhs_index;
                    ++rhs_index;
                }
            }
        }
        assign_sorted(state, std::move(merged), std::move(merged_counts));
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
