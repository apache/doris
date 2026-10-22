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

#include <cstdint>
#include <memory>
#include <vector>

#include "common/check.h"
#include "common/compare.h"
#include "common/exception.h"
#include "core/assert_cast.h"
#include "core/column/column.h"
#include "core/column/column_array.h"
#include "core/column/column_nullable.h"
#include "core/column/column_struct.h"
#include "core/column/column_vector.h"
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

/// Upper bound of k, shared with FE TopKCountFunction.MAX_TOPK_K.
constexpr int64_t MAX_TOPK_K = 65536;

/**
 * State shared by topk_min_count and topk_max_count.
 *
 * The entries are held as two parallel structures: a column with the distinct values (sorted
 * ascending for MIN / descending for MAX, at most k entries) and their non-negative int64 counts.
 * Only values with count > 0 are kept. `bound` is a one-row column with the extreme value seen
 * when k == 0, or the k-th value once the list was truncated when k >= 1; `has_bound` is the
 * truncation flag, and for k >= 1 `bound IS NULL` means "the list holds every distinct value".
 */
struct AggregateTopKCountData {
    const DataTypePtr value_type;
    bool has_k = false;
    int64_t k = 0;
    bool has_bound = false;
    MutableColumnPtr bound_column;
    MutableColumnPtr value_column;
    std::vector<int64_t> counts;

    explicit AggregateTopKCountData(const DataTypes& argument_types_)
            : value_type(argument_types_[0]),
              bound_column(value_type->create_column()),
              value_column(value_type->create_column()) {}

    AggregateTopKCountData(const AggregateTopKCountData&) = delete;
    AggregateTopKCountData& operator=(const AggregateTopKCountData&) = delete;
};

/**
 * topk_min_count(x, k) / topk_max_count(x, k).
 *
 * Result: STRUCT<values ARRAY<STRUCT<value T, count BIGINT>>, bound T NULL>. NULL inputs are
 * ignored (the nullable-argument wrapper never calls add for them). `values` is ordered by value
 * (ascending for MIN), holds at most k distinct values, and `bound` is the truncation point: the
 * k-th value when more than k distinct values were seen, otherwise NULL.
 *
 * With k = 0 no list is built: `values` is empty and `bound` is the extreme value seen on this
 * side (NULL when there was no non-NULL input), which lets the append-only IVM path compute
 * MIN/MAX from the delta extreme alone.
 */
template <PrimitiveType T, bool IsMin>
class AggregateFunctionTopKCount final
        : public IAggregateFunctionDataHelper<AggregateTopKCountData,
                                              AggregateFunctionTopKCount<T, IsMin>, true>,
          public MultiExpression,
          public NotNullableAggregateFunction {
public:
    using Base = IAggregateFunctionDataHelper<AggregateTopKCountData,
                                              AggregateFunctionTopKCount<T, IsMin>, true>;

    explicit AggregateFunctionTopKCount(const DataTypes& argument_types_) : Base(argument_types_) {
        DCHECK_EQ(argument_types_.size(), 2);
    }

    String get_name() const override { return IsMin ? "topk_min_count" : "topk_max_count"; }

    DataTypePtr get_return_type() const override {
        return make_return_type(this->argument_types[0]);
    }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena& arena) const override {
        auto& state = Base::data(place);
        // k is a constant expression, but only the first row carries it: the state remembers it so
        // that merge can check both sides used the same capacity. A state without has_k is empty.
        check_k(state, *columns[1], row_num);
        add_value(state, *columns[0], row_num);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena&) const override {
        auto& state = Base::data(place);
        const auto& other = Base::data(rhs);
        // A partial state without has_k never saw a non-NULL row, so it carries no information.
        if (!other.has_k) {
            return;
        }
        if (!state.has_k) {
            state.has_k = true;
            state.k = other.k;
        } else {
            // Both sides are partial states of the same constant k.
            DCHECK_EQ(state.k, other.k);
        }
        if (state.k == 0) {
            merge_bound(state, other);
            return;
        }
        merge_values(state, other);
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        const auto& state = Base::data(place);
        buf.write_binary(state.has_k);
        buf.write_binary(state.k);
        buf.write_binary(state.has_bound);
        if (state.has_bound) {
            write_column(buf, *state.bound_column);
        }
        auto size = static_cast<int64_t>(state.counts.size());
        buf.write_binary(size);
        write_column(buf, *state.value_column);
        buf.write(reinterpret_cast<const char*>(state.counts.data()),
                  state.counts.size() * sizeof(int64_t));
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena&) const override {
        auto& state = Base::data(place);
        buf.read_binary(state.has_k);
        buf.read_binary(state.k);
        buf.read_binary(state.has_bound);
        if (state.has_bound) {
            state.bound_column = read_column(buf);
        } else {
            state.bound_column = state.value_type->create_column();
        }
        int64_t size = 0;
        buf.read_binary(size);
        state.value_column = read_column(buf);
        DCHECK_EQ(static_cast<int64_t>(state.value_column->size()), size);
        state.counts.resize(size);
        buf.read(reinterpret_cast<char*>(state.counts.data()), size * sizeof(int64_t));
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        const auto& state = Base::data(place);
        auto& result_struct = assert_cast<ColumnStruct&>(to);
        auto& values_column = assert_cast<ColumnArray&>(result_struct.get_column(0));
        // The array element is nullable in the planned type (ARRAY<STRUCT<...>> defaults to
        // contains_null = true), but a produced entry never is.
        auto& pair_nullable = assert_cast<ColumnNullable&>(values_column.get_data());
        auto& pair_column = assert_cast<ColumnStruct&>(pair_nullable.get_nested_column());
        auto& value_column = pair_column.get_column(0);
        auto& count_column = assert_cast<ColumnInt64&>(pair_column.get_column(1));
        for (size_t i = 0; i != state.counts.size(); ++i) {
            value_column.insert_from(*state.value_column, i);
            count_column.get_data().push_back(state.counts[i]);
            pair_nullable.get_null_map_data().push_back(0);
        }
        values_column.get_offsets().push_back(pair_column.size());

        auto& bound_column = assert_cast<ColumnNullable&>(result_struct.get_column(1));
        if (state.has_bound) {
            bound_column.get_nested_column().insert_from(*state.bound_column, 0);
            bound_column.get_null_map_data().push_back(0);
        } else {
            bound_column.get_nested_column().insert_default();
            bound_column.get_null_map_data().push_back(1);
        }
    }

    static DataTypePtr make_return_type(const DataTypePtr& value_type) {
        auto pair_type = std::make_shared<DataTypeStruct>(
                DataTypes {value_type, std::make_shared<DataTypeInt64>()},
                Strings {"value", "count"});
        // ArrayType defaults contains_null to true on the FE side, so the pair element is nullable
        // in the planned type even though the aggregate never produces a NULL entry.
        auto values_type = std::make_shared<DataTypeArray>(make_nullable(pair_type));
        return std::make_shared<DataTypeStruct>(DataTypes {values_type, make_nullable(value_type)},
                                                Strings {"values", "bound"});
    }

private:
    static void check_k(AggregateTopKCountData& state, const IColumn& count_column,
                        size_t row_num) {
        DCHECK(!count_column.is_nullable());
        int64_t value = assert_cast<const ColumnInt64&, TypeCheckOnRelease::DISABLE>(count_column)
                                .get_element(row_num);
        if (!state.has_k) {
            if (UNLIKELY(value < 0 || value > MAX_TOPK_K)) {
                throw Exception(ErrorCode::INVALID_ARGUMENT,
                                "the count of topk_min_count and topk_max_count must be a constant "
                                "between 0 and {}, but got {}",
                                MAX_TOPK_K, value);
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
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "topk_min_count/topk_max_count value count overflow");
        }
        return result;
    }

    static void set_bound_from_value(AggregateTopKCountData& state, size_t index) {
        state.bound_column->clear();
        state.bound_column->insert_from(*state.value_column, index);
        state.has_bound = true;
    }

    /// Inserts (value, count) at `pos`, keeping the value column sorted by value.
    static void insert_entry(AggregateTopKCountData& state, size_t pos, const IColumn& column,
                             size_t row_num, int64_t count) {
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
            if (!state.has_bound) {
                state.bound_column->clear();
                state.bound_column->insert_from(column, row_num);
                state.has_bound = true;
                return;
            }
            int cmp = compare_at(column, row_num, *state.bound_column, 0);
            if (IsMin ? cmp < 0 : cmp > 0) {
                state.bound_column->clear();
                state.bound_column->insert_from(column, row_num);
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

        insert_entry(state, left, column, row_num, 1);
        if (static_cast<int64_t>(state.value_column->size()) > state.k) {
            // More than k distinct values: drop the worst one. The k-th (last kept) value is the
            // new truncation point, even when the value just inserted was the dropped one.
            state.value_column->pop_back(1);
            state.counts.pop_back();
            set_bound_from_value(state, state.value_column->size() - 1);
        }
    }

    static void merge_bound(AggregateTopKCountData& state, const AggregateTopKCountData& other) {
        if (!other.has_bound) {
            return;
        }
        if (!state.has_bound) {
            state.bound_column->clear();
            state.bound_column->insert_from(*other.bound_column, 0);
            state.has_bound = true;
            return;
        }
        int cmp = compare_at(*other.bound_column, 0, *state.bound_column, 0);
        if (IsMin ? cmp < 0 : cmp > 0) {
            state.bound_column->clear();
            state.bound_column->insert_from(*other.bound_column, 0);
        }
    }

    /// Merges two sorted lists, summing the counts of equal values and truncating to k.
    static void merge_values(AggregateTopKCountData& state, const AggregateTopKCountData& other) {
        if (other.counts.empty()) {
            // The other partial state saw no non-NULL row, so it has neither entries nor a bound.
            DCHECK(!other.has_bound);
            return;
        }
        auto merged = state.value_column->clone_empty();
        std::vector<int64_t> merged_counts;
        merged_counts.reserve(state.counts.size() + other.counts.size());
        size_t lhs_index = 0;
        size_t rhs_index = 0;
        while (lhs_index < state.counts.size() || rhs_index < other.counts.size()) {
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

        // Once a side is truncated, values outside its bound were dropped, so the union cannot be
        // complete either: the k-th merged value becomes the new truncation point.
        bool truncated = state.has_bound || other.has_bound;
        auto merged_size = static_cast<int64_t>(merged->size());
        if (merged_size > state.k) {
            merged->pop_back(merged_size - state.k);
            merged_counts.resize(state.k);
            truncated = true;
        }
        state.value_column = std::move(merged);
        state.counts = std::move(merged_counts);
        state.has_bound = truncated;
        if (truncated) {
            set_bound_from_value(state, state.value_column->size() - 1);
        } else {
            state.bound_column->clear();
        }
    }

    void write_column(BufferWritable& buf, const IColumn& column) const {
        int64_t size_bytes = this->argument_types[0]->get_uncompressed_serialized_bytes(
                column, IAggregateFunction::version);
        buf.write_binary(size_bytes);
        buf.resize(size_bytes);
        auto* pos =
                this->argument_types[0]->serialize(column, buf.data(), IAggregateFunction::version);
        DCHECK_EQ(pos, buf.data() + size_bytes);
        buf.add_offset(size_bytes);
    }

    MutableColumnPtr read_column(BufferReadable& buf) const {
        int64_t size_bytes = 0;
        buf.read_binary(size_bytes);
        MutableColumnPtr column = this->argument_types[0]->create_column();
        const auto* pos = this->argument_types[0]->deserialize(buf.data(), &column,
                                                               IAggregateFunction::version);
        DCHECK_EQ(pos, buf.data() + size_bytes);
        buf.add_offset(size_bytes);
        return column;
    }
};

} // namespace doris
