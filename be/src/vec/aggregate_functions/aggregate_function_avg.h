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

#include "common/status.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column_fixed_length_object.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_fixed_length_object.h"
#include "vec/data_types/data_type_number.h"
#include "vec/io/io_helper.h"

namespace doris::vectorized {

template <typename T>
struct AggregateFunctionAvgData {
    T sum = 0;
    UInt64 count = 0;

    AggregateFunctionAvgData& operator=(const AggregateFunctionAvgData<T>& src) {
        sum = src.sum;
        count = src.count;
        return *this;
    }

    template <typename ResultT>
    ResultT result() const {
        if constexpr (std::is_floating_point_v<ResultT>) {
            if constexpr (std::numeric_limits<ResultT>::is_iec559) {
                return static_cast<ResultT>(sum) / count; /// allow division by zero
            }
        }

        if (!count) {
            // null is handled in AggregationNode::_get_without_key_result
            return static_cast<ResultT>(sum);
        }
        if (!config::enable_decimalv3) {
            // to keep the same result with row vesion; see AggregateFunctions::decimalv2_avg_get_value
            if constexpr (std::is_same_v<ResultT, Decimal128> && std::is_same_v<T, Decimal128>) {
                DecimalV2Value decimal_val_count(count, 0);
                DecimalV2Value decimal_val_sum(static_cast<Int128>(sum));
                DecimalV2Value cal_ret = decimal_val_sum / decimal_val_count;
                Decimal128 ret(cal_ret.value());
                return ret;
            }
        }
        return static_cast<ResultT>(sum) / count;
    }

    void write(BufferWritable& buf) const {
        write_binary(sum, buf);
        write_binary(count, buf);
    }

    void read(BufferReadable& buf) {
        read_binary(sum, buf);
        read_binary(count, buf);
    }
};

/// Calculates arithmetic mean of numbers.
template <typename T, typename Data>
class AggregateFunctionAvg final
        : public IAggregateFunctionDataHelper<Data, AggregateFunctionAvg<T, Data>> {
public:
    using ResultType = std::conditional_t<IsDecimalNumber<T>, Decimal128, Float64>;
    using ResultDataType = std::conditional_t<IsDecimalNumber<T>, DataTypeDecimal<Decimal128>,
                                              DataTypeNumber<Float64>>;
    using ColVecType = std::conditional_t<IsDecimalNumber<T>, ColumnDecimal<T>, ColumnVector<T>>;
    using ColVecResult = std::conditional_t<IsDecimalNumber<T>, ColumnDecimal<Decimal128>,
                                            ColumnVector<Float64>>;

    /// ctor for native types
    AggregateFunctionAvg(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<Data, AggregateFunctionAvg<T, Data>>(argument_types_,
                                                                                {}),
              scale(0) {}

    /// ctor for Decimals
    AggregateFunctionAvg(const IDataType& data_type, const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<Data, AggregateFunctionAvg<T, Data>>(argument_types_,
                                                                                {}),
              scale(get_decimal_scale(data_type)) {}

    String get_name() const override { return "avg"; }

    DataTypePtr get_return_type() const override {
        if constexpr (IsDecimalNumber<T>) {
            return std::make_shared<ResultDataType>(ResultDataType::max_precision(), scale);
        } else {
            return std::make_shared<ResultDataType>();
        }
    }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, size_t row_num,
             Arena*) const override {
        const auto& column = static_cast<const ColVecType&>(*columns[0]);
        this->data(place).sum += column.get_data()[row_num];
        ++this->data(place).count;
    }

    void reset(AggregateDataPtr place) const override {
        this->data(place).sum = 0;
        this->data(place).count = 0;
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena*) const override {
        this->data(place).sum += this->data(rhs).sum;
        this->data(place).count += this->data(rhs).count;
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        this->data(place).write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena*) const override {
        this->data(place).read(buf);
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        auto& column = static_cast<ColVecResult&>(to);
        column.get_data().push_back(this->data(place).template result<ResultType>());
    }

    void deserialize_from_column(AggregateDataPtr places, const IColumn& column, Arena* arena,
                                 size_t num_rows) const override {
        auto& col = assert_cast<const ColumnFixedLengthObject&>(column);
        DCHECK(col.size() >= num_rows) << "source column's size should greater than num_rows";
        auto* data = col.get_data().data();
        memcpy(places, data, sizeof(Data) * num_rows);
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
                                           const size_t num_rows, Arena* arena) const override {
        auto* src_data = assert_cast<const ColVecType&>(*columns[0]).get_data().data();
        auto& dst_col = static_cast<ColumnFixedLengthObject&>(*dst);
        dst_col.set_item_size(sizeof(Data));
        dst_col.resize(num_rows);
        auto* data = dst_col.get_data().data();
        for (size_t i = 0; i != num_rows; ++i) {
            auto& state = *reinterpret_cast<Data*>(&data[sizeof(Data) * i]);
            state.sum = src_data[i];
            state.count = 1;
        }
    }

    void deserialize_and_merge_from_column(AggregateDataPtr __restrict place, const IColumn& column,
                                           Arena* arena) const override {
        auto& col = assert_cast<const ColumnFixedLengthObject&>(column);
        const size_t num_rows = column.size();
        DCHECK(col.size() >= num_rows) << "source column's size should greater than num_rows";
        auto* data = reinterpret_cast<const Data*>(col.get_data().data());

        for (size_t i = 0; i != num_rows; ++i) {
            this->data(place).sum += data[i].sum;
            this->data(place).count += data[i].count;
        }
    }

    void serialize_without_key_to_column(ConstAggregateDataPtr __restrict place,
                                         MutableColumnPtr& dst) const override {
        auto& col = assert_cast<ColumnFixedLengthObject&>(*dst);
        col.set_item_size(sizeof(Data));
        col.resize(1);
        *reinterpret_cast<Data*>(col.get_data().data()) = this->data(place);
    }

    MutableColumnPtr create_serialize_column() const override {
        return ColumnFixedLengthObject::create(sizeof(Data));
    }

    DataTypePtr get_serialized_type() const override {
        return std::make_shared<DataTypeFixedLengthObject>();
    }

private:
    UInt32 scale;
};

} // namespace doris::vectorized
