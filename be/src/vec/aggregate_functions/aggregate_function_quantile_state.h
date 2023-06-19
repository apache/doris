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

#include <stddef.h>

#include <memory>
#include <string>

// IWYU pragma: no_include <opentelemetry/common/threadlocal.h>
#include "common/compiler_util.h" // IWYU pragma: keep
#include "util/quantile_state.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column_complex.h"
#include "vec/columns/column_nullable.h"
#include "vec/common/assert_cast.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_quantilestate.h"

namespace doris {
namespace vectorized {
class Arena;
class BufferReadable;
class BufferWritable;
class IColumn;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

struct AggregateFunctionQuantileStateUnionOp {
    static constexpr auto name = "quantile_union";

    template <typename T>
    static void add(QuantileState<T>& res, const T& data, bool& is_first) {
        res.add_value(data);
    }

    template <typename T>
    static void add(QuantileState<T>& res, const QuantileState<T>& data, bool& is_first) {
        if (UNLIKELY(is_first)) {
            res = data;
            is_first = false;
        } else {
            res.merge(data);
        }
    }

    template <typename T>
    static void merge(QuantileState<T>& res, const QuantileState<T>& data, bool& is_first) {
        if (UNLIKELY(is_first)) {
            res = data;
            is_first = false;
        } else {
            res.merge(data);
        }
    }
};

template <typename Op, typename InternalType>
struct AggregateFunctionQuantileStateData {
    using DataType = QuantileState<InternalType>;
    DataType value;
    bool is_first = true;

    template <typename T>
    void add(const T& data) {
        Op::add(value, data, is_first);
    }

    void merge(const DataType& data) { Op::merge(value, data, is_first); }

    void write(BufferWritable& buf) const {
        DataTypeQuantileState<InternalType>::serialize_as_stream(value, buf);
    }

    void read(BufferReadable& buf) {
        DataTypeQuantileState<InternalType>::deserialize_as_stream(value, buf);
    }

    void reset() { is_first = true; }

    DataType& get() { return value; }
};

template <bool arg_is_nullable, typename Op, typename InternalType>
class AggregateFunctionQuantileStateOp final
        : public IAggregateFunctionDataHelper<
                  AggregateFunctionQuantileStateData<Op, InternalType>,
                  AggregateFunctionQuantileStateOp<arg_is_nullable, Op, InternalType>> {
public:
    using ResultDataType = QuantileState<InternalType>;
    using ColVecType = ColumnQuantileState<InternalType>;
    using ColVecResult = ColumnQuantileState<InternalType>;

    String get_name() const override { return Op::name; }

    AggregateFunctionQuantileStateOp(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<
                      AggregateFunctionQuantileStateData<Op, InternalType>,
                      AggregateFunctionQuantileStateOp<arg_is_nullable, Op, InternalType>>(
                      argument_types_) {}

    DataTypePtr get_return_type() const override {
        return std::make_shared<DataTypeQuantileState<InternalType>>();
    }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, size_t row_num,
             Arena*) const override {
        if constexpr (arg_is_nullable) {
            auto& nullable_column = assert_cast<const ColumnNullable&>(*columns[0]);
            if (!nullable_column.is_null_at(row_num)) {
                const auto& column =
                        assert_cast<const ColVecType&>(nullable_column.get_nested_column());
                this->data(place).add(column.get_data()[row_num]);
            }
        } else {
            const auto& column = assert_cast<const ColVecType&>(*columns[0]);
            this->data(place).add(column.get_data()[row_num]);
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena*) const override {
        this->data(place).merge(
                const_cast<AggregateFunctionQuantileStateData<Op, InternalType>&>(this->data(rhs))
                        .get());
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        this->data(place).write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena*) const override {
        this->data(place).read(buf);
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        auto& column = assert_cast<ColVecResult&>(to);
        column.get_data().push_back(
                const_cast<AggregateFunctionQuantileStateData<Op, InternalType>&>(this->data(place))
                        .get());
    }

    void reset(AggregateDataPtr __restrict place) const override { this->data(place).reset(); }
};

AggregateFunctionPtr create_aggregate_function_quantile_state_union(const std::string& name,
                                                                    const DataTypes& argument_types,
                                                                    const bool result_is_nullable);

} // namespace doris::vectorized
