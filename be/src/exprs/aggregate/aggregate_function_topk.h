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
 * The entries are held as two parallel structures: a column with the distinct values (sorted
 * ascending for MIN / descending for MAX, at most k entries) and their non-negative int64 counts.
 * Only values with count > 0 are kept. With k == 0 the list holds at most the single extreme
 * value and its row count; the caller derives truncation from the list length, so the state
 * carries no separate bound.
 */
struct AggregateTopKCountData {
    const DataTypePtr value_type;
    bool has_k = false;
    int64_t k = 0;
    MutableColumnPtr value_column;
    // Tracked so that the per-group counts participate in query memory accounting: at the maximum
    // k this buffer alone is 512 KiB per group.
    DorisVector<int64_t> counts;

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
        auto size = static_cast<int64_t>(state.counts.size());
        buf.write_binary(size);
        write_column(buf, *state.value_column);
        if (size != 0) {
            // An empty list keeps an empty vector, whose data() is null; passing it to memcpy is
            // undefined even for a zero length.
            buf.write(reinterpret_cast<const char*>(state.counts.data()),
                      state.counts.size() * sizeof(int64_t));
        }
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena&) const override {
        auto& state = Base::data(place);
        buf.read_binary(state.has_k);
        buf.read_binary(state.k);
        int64_t size = 0;
        buf.read_binary(size);
        state.value_column = read_column(buf);
        DCHECK_EQ(static_cast<int64_t>(state.value_column->size()), size);
        state.counts.resize(size);
        if (size != 0) {
            buf.read(reinterpret_cast<char*>(state.counts.data()), size * sizeof(int64_t));
        }
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
        for (size_t i = 0; i != state.counts.size(); ++i) {
            if (const auto* nullable_value =
                        check_and_get_column<ColumnNullable>(state.value_column.get())) {
                // A nullable value argument carries its own null map, which is never set here.
                value_column.insert_from(*nullable_value, i);
            } else {
                value_column.get_nested_column().insert_from(*state.value_column, i);
                value_column.get_null_map_data().push_back(0);
            }
            assert_cast<ColumnInt64&>(count_column.get_nested_column())
                    .get_data()
                    .push_back(state.counts[i]);
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

    /// Inserts (value, count) at `pos`, keeping the value column sorted by value.
    static void insert_entry(AggregateTopKCountData& state, size_t pos, const IColumn& column,
                             size_t row_num, int64_t count) {
        if (pos == state.value_column->size()) {
            // Appending needs no rebuild: the state column is mutable, so ascending MIN input
            // (descending MAX input) fills the list without copying the retained prefix.
            state.value_column->insert_from(column, row_num);
            state.counts.push_back(count);
            return;
        }
        auto new_values = state.value_column->clone_empty();
        new_values->reserve(state.value_column->size() + 1);
        for (size_t i = 0; i < pos; ++i) {
            new_values->insert_from(*state.value_column, i);
        }
        new_values->insert_from(column, row_num);
        for (size_t i = pos; i < state.value_column->size(); ++i) {
            new_values->insert_from(*state.value_column, i);
        }
        state.value_column = std::move(new_values);
        state.counts.insert(state.counts.begin() + pos, count);
    }

    /// k == 0 fast path: only the extreme value is tracked.
    static void add_value(AggregateTopKCountData& state, const IColumn& column, size_t row_num) {
        if (state.k == 0) {
            if (state.counts.empty()) {
                state.value_column->insert_from(column, row_num);
                state.counts.push_back(1);
                return;
            }
            int cmp = compare_at(column, row_num, *state.value_column, 0);
            if (cmp == 0) {
                state.counts[0] = checked_add(state.counts[0], 1);
            } else if (IsMin ? cmp < 0 : cmp > 0) {
                state.value_column->clear();
                state.value_column->insert_from(column, row_num);
                state.counts[0] = 1;
            }
            return;
        }

        // The list is ascending for MIN and descending for MAX, so the insertion point is where
        // the stored values stop being better than the new one.
        size_t left = 0;
        size_t right = state.value_column->size();
        while (left < right) {
            size_t mid = left + (right - left) / 2;
            int cmp = compare_at(*state.value_column, mid, column, row_num);
            if (IsMin ? cmp < 0 : cmp > 0) {
                left = mid + 1;
            } else {
                right = mid;
            }
        }
        if (left < state.value_column->size() &&
            compare_at(*state.value_column, left, column, row_num) == 0) {
            state.counts[left] = checked_add(state.counts[left], 1);
            return;
        }

        // A full list already holds the k best values, so a value worse than the last entry would
        // be inserted at the end and popped immediately: it can only flip the truncation flag.
        // Rebating it early avoids cloning and copying the whole retained column for every
        // rejected distinct value (ascending inputs at k = 65536 otherwise copy ~k values each).
        if (left == state.value_column->size() &&
            std::cmp_greater_equal(state.value_column->size(), state.k)) {
            return;
        }

        insert_entry(state, left, column, row_num, 1);
        if (static_cast<int64_t>(state.value_column->size()) > state.k) {
            // More than k distinct values: drop the worst one.
            state.value_column->pop_back(1);
            state.counts.pop_back();
        }
    }

    /// k == 0: the state holds at most the extreme value and its count.
    static void merge_extreme(AggregateTopKCountData& state, const AggregateTopKCountData& other) {
        if (other.counts.empty()) {
            return;
        }
        if (state.counts.empty()) {
            state.value_column->insert_from(*other.value_column, 0);
            state.counts.push_back(other.counts[0]);
            return;
        }
        int cmp = compare_at(*state.value_column, 0, *other.value_column, 0);
        if (cmp == 0) {
            state.counts[0] = checked_add(state.counts[0], other.counts[0]);
        } else if (IsMin ? cmp > 0 : cmp < 0) {
            state.value_column->clear();
            state.value_column->insert_from(*other.value_column, 0);
            state.counts[0] = other.counts[0];
        }
    }

    /// Merges two sorted lists, summing the counts of equal values and keeping the k best.
    ///
    /// The merge stops as soon as k entries are produced. Both inputs are best-first and hold
    /// distinct values, and equal values have already been combined, so every remaining value is
    /// worse than all retained ones: it can neither enter the result nor overflow it. Stopping
    /// here also avoids materializing a tail that would be truncated away.
    static void merge_values(AggregateTopKCountData& state, const AggregateTopKCountData& other) {
        if (other.counts.empty()) {
            // The other partial state saw no non-NULL row.
            return;
        }
        const auto limit = static_cast<size_t>(state.k);
        auto merged = state.value_column->clone_empty();
        DorisVector<int64_t> merged_counts;
        merged_counts.reserve(std::min(limit, state.counts.size() + other.counts.size()));
        size_t lhs_index = 0;
        size_t rhs_index = 0;
        while (merged_counts.size() < limit &&
               (lhs_index < state.counts.size() || rhs_index < other.counts.size())) {
            if (rhs_index == other.counts.size()) {
                merged->insert_from(*state.value_column, lhs_index);
                merged_counts.push_back(state.counts[lhs_index]);
                ++lhs_index;
            } else if (lhs_index == state.counts.size()) {
                merged->insert_from(*other.value_column, rhs_index);
                merged_counts.push_back(other.counts[rhs_index]);
                ++rhs_index;
            } else {
                int cmp =
                        compare_at(*state.value_column, lhs_index, *other.value_column, rhs_index);
                // Both lists are ordered best-first (ascending for MIN, descending for MAX).
                const bool lhs_is_better = IsMin ? cmp < 0 : cmp > 0;
                if (lhs_is_better) {
                    merged->insert_from(*state.value_column, lhs_index);
                    merged_counts.push_back(state.counts[lhs_index]);
                    ++lhs_index;
                } else if (cmp != 0) {
                    // Not better and not equal: the right-hand value is better.
                    merged->insert_from(*other.value_column, rhs_index);
                    merged_counts.push_back(other.counts[rhs_index]);
                    ++rhs_index;
                } else {
                    merged->insert_from(*state.value_column, lhs_index);
                    merged_counts.push_back(
                            checked_add(state.counts[lhs_index], other.counts[rhs_index]));
                    ++lhs_index;
                    ++rhs_index;
                }
            }
        }

        state.value_column = std::move(merged);
        state.counts = std::move(merged_counts);
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
