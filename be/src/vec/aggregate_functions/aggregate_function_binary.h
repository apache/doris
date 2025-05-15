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

#include "common/status.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/factory_helpers.h"
#include "vec/aggregate_functions/helpers.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_vector.h"
#include "vec/common/arithmetic_overflow.h"
#include "vec/common/string_buffer.hpp"
#include "vec/core/types.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/io/io_helper.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

template <typename T1, typename T2, template <typename> typename Moments>
struct StatFunc {
    using Type1 = T1;
    using Type2 = T2;
    using ResultType = Float64;
    using Data = Moments<ResultType>;
};

template <typename StatFunc>
struct AggregateFunctionBinary
        : public IAggregateFunctionDataHelper<typename StatFunc::Data,
                                              AggregateFunctionBinary<StatFunc>> {
    using ResultType = typename StatFunc::ResultType;

    using ColVecT1 = ColumnVectorOrDecimal<typename StatFunc::Type1>;
    using ColVecT2 = ColumnVectorOrDecimal<typename StatFunc::Type2>;
    using ColVecResult = ColumnVector<ResultType>;
    static constexpr UInt32 num_args = 2;

    AggregateFunctionBinary(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<typename StatFunc::Data,
                                           AggregateFunctionBinary<StatFunc>>(argument_types_) {}

    String get_name() const override { return StatFunc::Data::name(); }

    void reset(AggregateDataPtr __restrict place) const override { this->data(place).reset(); }

    DataTypePtr get_return_type() const override {
        return std::make_shared<DataTypeNumber<ResultType>>();
    }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena*) const override {
        this->data(place).add(
                static_cast<ResultType>(
                        static_cast<const ColVecT1&>(*columns[0]).get_data()[row_num]),
                static_cast<ResultType>(
                        static_cast<const ColVecT2&>(*columns[1]).get_data()[row_num]));
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena*) const override {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        this->data(place).write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena*) const override {
        this->data(place).read(buf);
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        const auto& data = this->data(place);
        auto& dst = static_cast<ColVecResult&>(to).get_data();
        dst.push_back(data.get());
    }
};

template <template <typename> typename Moments, typename FirstType, typename... TArgs>
AggregateFunctionPtr create_with_two_basic_numeric_types_second(const DataTypePtr& second_type,
                                                                TArgs&&... args) {
    switch (second_type->get_primitive_type()) {
    case PrimitiveType::TYPE_BOOLEAN:
        return creator_without_type::create<
                AggregateFunctionBinary<StatFunc<FirstType, UInt8, Moments>>>(
                std::forward<TArgs>(args)...);
    case PrimitiveType::TYPE_TINYINT:
        return creator_without_type::create<
                AggregateFunctionBinary<StatFunc<FirstType, Int8, Moments>>>(
                std::forward<TArgs>(args)...);
    case PrimitiveType::TYPE_SMALLINT:
        return creator_without_type::create<
                AggregateFunctionBinary<StatFunc<FirstType, Int16, Moments>>>(
                std::forward<TArgs>(args)...);
    case PrimitiveType::TYPE_INT:
        return creator_without_type::create<
                AggregateFunctionBinary<StatFunc<FirstType, Int32, Moments>>>(
                std::forward<TArgs>(args)...);
    case PrimitiveType::TYPE_BIGINT:
        return creator_without_type::create<
                AggregateFunctionBinary<StatFunc<FirstType, Int64, Moments>>>(
                std::forward<TArgs>(args)...);
    case PrimitiveType::TYPE_LARGEINT:
        return creator_without_type::create<
                AggregateFunctionBinary<StatFunc<FirstType, Int128, Moments>>>(
                std::forward<TArgs>(args)...);
    case PrimitiveType::TYPE_FLOAT:
        return creator_without_type::create<
                AggregateFunctionBinary<StatFunc<FirstType, Float32, Moments>>>(
                std::forward<TArgs>(args)...);
    case PrimitiveType::TYPE_DOUBLE:
        return creator_without_type::create<
                AggregateFunctionBinary<StatFunc<FirstType, Float64, Moments>>>(
                std::forward<TArgs>(args)...);
    default:
        return nullptr;
    }
}

template <template <typename> typename Moments, typename... TArgs>
AggregateFunctionPtr create_with_two_basic_numeric_types(const DataTypePtr& first_type,
                                                         const DataTypePtr& second_type,
                                                         TArgs&&... args) {
    switch (first_type->get_primitive_type()) {
    case PrimitiveType::TYPE_BOOLEAN:
        return create_with_two_basic_numeric_types_second<Moments, UInt8>(
                second_type, std::forward<TArgs>(args)...);
    case PrimitiveType::TYPE_TINYINT:
        return create_with_two_basic_numeric_types_second<Moments, Int8>(
                second_type, std::forward<TArgs>(args)...);
    case PrimitiveType::TYPE_SMALLINT:
        return create_with_two_basic_numeric_types_second<Moments, Int16>(
                second_type, std::forward<TArgs>(args)...);
    case PrimitiveType::TYPE_INT:
        return create_with_two_basic_numeric_types_second<Moments, Int32>(
                second_type, std::forward<TArgs>(args)...);
    case PrimitiveType::TYPE_BIGINT:
        return create_with_two_basic_numeric_types_second<Moments, Int64>(
                second_type, std::forward<TArgs>(args)...);
    case PrimitiveType::TYPE_LARGEINT:
        return create_with_two_basic_numeric_types_second<Moments, Int128>(
                second_type, std::forward<TArgs>(args)...);
    case PrimitiveType::TYPE_FLOAT:
        return create_with_two_basic_numeric_types_second<Moments, Float32>(
                second_type, std::forward<TArgs>(args)...);
    case PrimitiveType::TYPE_DOUBLE:
        return create_with_two_basic_numeric_types_second<Moments, Float64>(
                second_type, std::forward<TArgs>(args)...);
    default:
        return nullptr;
    }
}

} // namespace doris::vectorized

#include "common/compile_check_end.h"
