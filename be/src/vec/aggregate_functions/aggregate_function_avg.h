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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/AggregateFunctions/AggregateFunctionAvg.h
// and modified by Doris

#pragma once

#include <glog/logging.h>
#include <string.h>

#include <limits>
#include <memory>
#include <ostream>
#include <type_traits>
#include <vector>

#include "runtime/decimalv2_value.h"
#include "runtime/primitive_type.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_fixed_length_object.h"
#include "vec/common/assert_cast.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_fixed_length_object.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"
class Arena;
class BufferReadable;
class BufferWritable;
template <PrimitiveType T>
class ColumnDecimal;
template <PrimitiveType T>
class DataTypeNumber;
template <PrimitiveType T>
class ColumnVector;

template <PrimitiveType T>
struct AggregateFunctionAvgData {
    using ResultType = typename PrimitiveTypeTraits<T>::CppType;
    static constexpr PrimitiveType ResultPType = T;
    ResultType sum {};
    UInt64 count = 0;

    AggregateFunctionAvgData& operator=(const AggregateFunctionAvgData<T>& src) = default;

    template <typename ResultT>
    ResultT result(ResultType multiplier) const {
        if (!count) {
            // null is handled in AggregationNode::_get_without_key_result
            return static_cast<ResultT>(sum);
        }
        // to keep the same result with row vesion; see AggregateFunctions::decimalv2_avg_get_value
        if constexpr (T == TYPE_DECIMALV2 && IsDecimalV2<ResultT>) {
            DecimalV2Value decimal_val_count(count, 0);
            DecimalV2Value decimal_val_sum(sum * multiplier);
            DecimalV2Value cal_ret = decimal_val_sum / decimal_val_count;
            return cal_ret;
        } else {
            if constexpr (T == TYPE_DECIMAL256) {
                return static_cast<ResultT>(sum * multiplier /
                                            typename PrimitiveTypeTraits<T>::CppType(count));
            } else {
                return static_cast<ResultT>(sum * multiplier) / static_cast<ResultT>(count);
            }
        }
    }

    template <typename ResultT>
    ResultT result() const {
        if constexpr (std::is_floating_point_v<ResultT>) {
            if constexpr (std::numeric_limits<ResultT>::is_iec559) {
                return static_cast<ResultT>(sum) /
                       static_cast<ResultT>(count); /// allow division by zero
            }
        }

        if (!count) {
            // null is handled in AggregationNode::_get_without_key_result
            return static_cast<ResultT>(sum);
        }
        return static_cast<ResultT>(sum) / static_cast<ResultT>(count);
    }

    void write(BufferWritable& buf) const {
        buf.write_binary(sum);
        buf.write_binary(count);
    }

    void read(BufferReadable& buf) {
        buf.read_binary(sum);
        buf.read_binary(count);
    }
};

template <PrimitiveType T, PrimitiveType TResult, typename Data>
class AggregateFunctionAvg;

template <PrimitiveType T, PrimitiveType TResult>
constexpr static bool is_valid_avg_types =
        (is_same_or_wider_decimalv3(T, TResult) || (is_decimalv2(T) && is_decimalv2(TResult)) ||
         (is_float_or_double(T) && is_float_or_double(TResult)) ||
         (is_int_or_bool(T) && (is_double(TResult) || is_int(TResult))));
/// Calculates arithmetic mean of numbers.
template <PrimitiveType T, PrimitiveType TResult, typename Data>
    requires(is_valid_avg_types<T, TResult>)
class AggregateFunctionAvg<T, TResult, Data> final
        : public IAggregateFunctionDataHelper<Data, AggregateFunctionAvg<T, TResult, Data>>,
          UnaryExpression,
          NullableAggregateFunction {
public:
    using ResultType = typename PrimitiveTypeTraits<TResult>::CppType;
    using ResultDataType = typename PrimitiveTypeTraits<TResult>::DataType;
    using ColVecType = typename PrimitiveTypeTraits<T>::ColumnType;
    using ColVecResult = typename PrimitiveTypeTraits<TResult>::ColumnType;
    // The result calculated by PercentileApprox is an approximate value,
    // so the underlying storage uses float. The following calls will involve
    // an implicit cast to float.

    using DataType = typename Data::ResultType;

    // consistent with fe/fe-common/src/main/java/org/apache/doris/catalog/ScalarType.java
    static constexpr uint32_t DEFAULT_MIN_AVG_DECIMAL128_SCALE = 4;

    /// ctor for native types
    AggregateFunctionAvg(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<Data, AggregateFunctionAvg<T, TResult, Data>>(
                      argument_types_),
              output_scale(std::max(DEFAULT_MIN_AVG_DECIMAL128_SCALE,
                                    get_decimal_scale(*argument_types_[0]))) {
        if constexpr (is_decimal(T)) {
            multiplier = ResultType(ResultDataType::get_scale_multiplier(
                    output_scale - get_decimal_scale(*argument_types_[0])));
        }
    }

    String get_name() const override { return "avg"; }

    DataTypePtr get_return_type() const override {
        if constexpr (is_decimal(T)) {
            return std::make_shared<ResultDataType>(
                    ResultDataType::max_precision(),
                    std::max(DEFAULT_MIN_AVG_DECIMAL128_SCALE, output_scale));
        } else {
            return std::make_shared<ResultDataType>();
        }
    }

    bool is_trivial() const override { return true; }

    template <bool is_add>
    NO_SANITIZE_UNDEFINED void update_value(AggregateDataPtr __restrict place,
                                            const IColumn** columns, ssize_t row_num) const {
#ifdef __clang__
#pragma clang fp reassociate(on)
#endif
        const auto& column =
                assert_cast<const ColVecType&, TypeCheckOnRelease::DISABLE>(*columns[0]);
        if constexpr (is_add) {
            if constexpr (T == TYPE_DECIMALV2) {
                this->data(place).sum += column.get_data()[row_num];
            } else if constexpr (is_decimal(T)) {
                this->data(place).sum += column.get_data()[row_num].value;
            } else {
                this->data(place).sum += (DataType)column.get_data()[row_num];
            }
            ++this->data(place).count;
        } else {
            if constexpr (T == TYPE_DECIMALV2) {
                this->data(place).sum += -column.get_data()[row_num];
            } else if constexpr (is_decimal(T)) {
                this->data(place).sum -= column.get_data()[row_num].value;
            } else {
                this->data(place).sum -= (DataType)column.get_data()[row_num];
            }
            --this->data(place).count;
        }
    }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena&) const override {
        update_value<true>(place, columns, row_num);
    }

    void reset(AggregateDataPtr place) const override {
        this->data(place).sum = {};
        this->data(place).count = 0;
    }

    NO_SANITIZE_UNDEFINED void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
                                     Arena&) const override {
        if constexpr (T == TYPE_DECIMALV2) {
            this->data(place).sum += this->data(rhs).sum;
        } else if constexpr (is_decimal(T)) {
            this->data(place).sum += this->data(rhs).sum.value;
        } else {
            this->data(place).sum += this->data(rhs).sum;
        }
        this->data(place).count += this->data(rhs).count;
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        this->data(place).write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena&) const override {
        this->data(place).read(buf);
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        auto& column = assert_cast<ColVecResult&>(to);
        if constexpr (is_decimal(T)) {
            column.get_data().push_back(this->data(place).template result<ResultType>(multiplier));
        } else {
            column.get_data().push_back(this->data(place).template result<ResultType>());
        }
    }

    void serialize_to_column(const std::vector<AggregateDataPtr>& places, size_t offset,
                             MutableColumnPtr& dst, const size_t num_rows) const override {
        auto& col = assert_cast<ColumnFixedLengthObject&>(*dst);
        col.set_item_size(sizeof(Data));
        col.resize(num_rows);
        auto* data = col.get_data().data();
        for (size_t i = 0; i != num_rows; ++i) {
            *reinterpret_cast<Data*>(&data[sizeof(Data) * i]) =
                    *reinterpret_cast<Data*>(places[i] + offset);
        }
    }

    void streaming_agg_serialize_to_column(const IColumn** columns, MutableColumnPtr& dst,
                                           const size_t num_rows, Arena&) const override {
        auto* src_data = assert_cast<const ColVecType&>(*columns[0]).get_data().data();
        auto& dst_col = assert_cast<ColumnFixedLengthObject&>(*dst);
        dst_col.set_item_size(sizeof(Data));
        dst_col.resize(num_rows);
        auto* data = dst_col.get_data().data();
        for (size_t i = 0; i != num_rows; ++i) {
            auto& state = *reinterpret_cast<Data*>(&data[sizeof(Data) * i]);
            state.sum = typename Data::ResultType(src_data[i]);
            state.count = 1;
        }
    }

    NO_SANITIZE_UNDEFINED void deserialize_and_merge_from_column_range(
            AggregateDataPtr __restrict place, const IColumn& column, size_t begin, size_t end,
            Arena&) const override {
        DCHECK(end <= column.size() && begin <= end)
                << ", begin:" << begin << ", end:" << end << ", column.size():" << column.size();
        auto& col = assert_cast<const ColumnFixedLengthObject&>(column);
        auto* data = reinterpret_cast<const Data*>(col.get_data().data());
        for (size_t i = begin; i <= end; ++i) {
            this->data(place).sum += data[i].sum;
            this->data(place).count += data[i].count;
        }
    }

    void deserialize_and_merge_vec(const AggregateDataPtr* places, size_t offset,
                                   AggregateDataPtr rhs, const IColumn* column, Arena& arena,
                                   const size_t num_rows) const override {
        const auto& col = assert_cast<const ColumnFixedLengthObject&>(*column);
        const auto* data = col.get_data().data();
        this->merge_vec(places, offset, AggregateDataPtr(data), arena, num_rows);
    }

    void deserialize_and_merge_vec_selected(const AggregateDataPtr* places, size_t offset,
                                            AggregateDataPtr rhs, const IColumn* column,
                                            Arena& arena, const size_t num_rows) const override {
        const auto& col = assert_cast<const ColumnFixedLengthObject&>(*column);
        const auto* data = col.get_data().data();
        this->merge_vec_selected(places, offset, AggregateDataPtr(data), arena, num_rows);
    }

    void serialize_without_key_to_column(ConstAggregateDataPtr __restrict place,
                                         IColumn& to) const override {
        auto& col = assert_cast<ColumnFixedLengthObject&>(to);
        col.set_item_size(sizeof(Data));
        size_t old_size = col.size();
        col.resize(old_size + 1);
        *(reinterpret_cast<Data*>(col.get_data().data()) + old_size) = this->data(place);
    }

    MutableColumnPtr create_serialize_column() const override {
        return ColumnFixedLengthObject::create(sizeof(Data));
    }

    DataTypePtr get_serialized_type() const override {
        return std::make_shared<DataTypeFixedLengthObject>();
    }

    bool supported_incremental_mode() const override { return true; }

    void execute_function_with_incremental(int64_t partition_start, int64_t partition_end,
                                           int64_t frame_start, int64_t frame_end,
                                           AggregateDataPtr place, const IColumn** columns,
                                           Arena& arena, bool previous_is_nul, bool end_is_nul,
                                           bool has_null, UInt8* use_null_result,
                                           UInt8* could_use_previous_result) const override {
        int64_t current_frame_start = std::max<int64_t>(frame_start, partition_start);
        int64_t current_frame_end = std::min<int64_t>(frame_end, partition_end);
        if (current_frame_start >= current_frame_end) {
            *use_null_result = true;
            return;
        }
        if (*could_use_previous_result) {
            auto outcoming_pos = frame_start - 1;
            auto incoming_pos = frame_end - 1;
            if (!previous_is_nul && outcoming_pos >= partition_start &&
                outcoming_pos < partition_end) {
                update_value<false>(place, columns, outcoming_pos);
            }
            if (!end_is_nul && incoming_pos >= partition_start && incoming_pos < partition_end) {
                update_value<true>(place, columns, incoming_pos);
            }
        } else {
            this->add_range_single_place(partition_start, partition_end, frame_start, frame_end,
                                         place, columns, arena, use_null_result,
                                         could_use_previous_result);
        }
    }

    void add_range_single_place(int64_t partition_start, int64_t partition_end, int64_t frame_start,
                                int64_t frame_end, AggregateDataPtr place, const IColumn** columns,
                                Arena& arena, UInt8* use_null_result,
                                UInt8* could_use_previous_result) const override {
        auto current_frame_start = std::max<int64_t>(frame_start, partition_start);
        auto current_frame_end = std::min<int64_t>(frame_end, partition_end);
        if (current_frame_start >= current_frame_end) {
            if (!*could_use_previous_result) {
                *use_null_result = true;
            }
        } else {
            for (size_t row_num = current_frame_start; row_num < current_frame_end; ++row_num) {
                update_value<true>(place, columns, row_num);
            }
            *use_null_result = false;
            *could_use_previous_result = true;
        }
    }

private:
    uint32_t output_scale;
    ResultType multiplier;
};

} // namespace doris::vectorized

#include "common/compile_check_end.h"
