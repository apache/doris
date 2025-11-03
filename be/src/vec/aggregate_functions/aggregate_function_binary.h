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

#include <glog/logging.h>

#include <cmath>

#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_vector.h"
#include "vec/common/string_buffer.hpp"
#include "vec/core/types.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

template <PrimitiveType T, template <PrimitiveType> typename Moments>
struct StatFunc {
    using ColVecT1 = typename PrimitiveTypeTraits<T>::ColumnType;
    using ColVecT2 = ColVecT1;
    using Data = Moments<TYPE_DOUBLE>;
    static constexpr PrimitiveType ResultPrimitiveType = TYPE_DOUBLE;
};

template <typename StatFunc>
struct AggregateFunctionBinary
        : public IAggregateFunctionDataHelper<typename StatFunc::Data,
                                              AggregateFunctionBinary<StatFunc>>,
          MultiExpression,
          NullableAggregateFunction {
    static constexpr PrimitiveType ResultType = StatFunc::ResultPrimitiveType;

    using ColVecT1 = typename StatFunc::ColVecT1;
    using ColVecT2 = typename StatFunc::ColVecT2;
    using ColVecResult = ColumnVector<ResultType>;
    static constexpr UInt32 num_args = 2;

    AggregateFunctionBinary(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<typename StatFunc::Data,
                                           AggregateFunctionBinary<StatFunc>>(argument_types_) {}

    String get_name() const override { return StatFunc::Data::name(); }

    void reset(AggregateDataPtr __restrict place) const override { this->data(place).reset(); }

    DataTypePtr get_return_type() const override {
        return std::make_shared<DataTypeNumber<StatFunc::ResultPrimitiveType>>();
    }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena&) const override {
        this->data(place).add(
                static_cast<typename PrimitiveTypeTraits<ResultType>::ColumnItemType>(
                        static_cast<const ColVecT1&>(*columns[0]).get_data()[row_num]),
                static_cast<typename PrimitiveTypeTraits<ResultType>::ColumnItemType>(
                        static_cast<const ColVecT2&>(*columns[1]).get_data()[row_num]));
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena&) const override {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        this->data(place).write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena&) const override {
        this->data(place).read(buf);
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        const auto& data = this->data(place);
        auto& dst = static_cast<ColVecResult&>(to).get_data();
        dst.push_back(data.get());
    }
};

} // namespace doris::vectorized

#include "common/compile_check_end.h"
