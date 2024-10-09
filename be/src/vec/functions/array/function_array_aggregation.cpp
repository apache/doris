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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/array/arrayAggregation.cpp
// and modified by Doris

#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <type_traits>
#include <utility>

#include "common/status.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/aggregate_function_avg.h"
#include "vec/aggregate_functions/aggregate_function_min_max.h"
#include "vec/aggregate_functions/aggregate_function_product.h"
#include "vec/aggregate_functions/aggregate_function_sum.h"
#include "vec/aggregate_functions/helpers.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_nullable.h"
#include "vec/common/arena.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/functions/array/function_array_join.h"
#include "vec/functions/array/function_array_mapped.h"
#include "vec/functions/simple_function_factory.h"

namespace doris {
namespace vectorized {

enum class AggregateOperation { MIN, MAX, SUM, AVERAGE, PRODUCT };

template <typename Element, AggregateOperation operation>
struct ArrayAggregateResultImpl;

template <typename Element>
struct ArrayAggregateResultImpl<Element, AggregateOperation::MIN> {
    using Result = Element;
};

template <typename Element>
struct ArrayAggregateResultImpl<Element, AggregateOperation::MAX> {
    using Result = Element;
};

template <typename Element>
struct ArrayAggregateResultImpl<Element, AggregateOperation::AVERAGE> {
    using Result = DisposeDecimal<Element, Float64>;
};

template <typename Element>
struct ArrayAggregateResultImpl<Element, AggregateOperation::PRODUCT> {
    using Result = DisposeDecimal<Element, Float64>;
};

template <typename Element>
struct ArrayAggregateResultImpl<Element, AggregateOperation::SUM> {
    using Result = DisposeDecimal<
            Element,
            std::conditional_t<IsFloatNumber<Element>, Float64,
                               std::conditional_t<std::is_same_v<Element, Int128>, Int128, Int64>>>;
};

template <typename Element, AggregateOperation operation>
using ArrayAggregateResult = typename ArrayAggregateResultImpl<Element, operation>::Result;

// For MIN/MAX, the type of result is the same as the type of elements, we can omit the
// template specialization.
template <AggregateOperation operation>
struct AggregateFunctionImpl;

template <>
struct AggregateFunctionImpl<AggregateOperation::SUM> {
    template <typename Element>
    struct TypeTraits {
        using ResultType = ArrayAggregateResult<Element, AggregateOperation::SUM>;
        using AggregateDataType = AggregateFunctionSumData<ResultType>;
        using Function = AggregateFunctionSum<Element, ResultType, AggregateDataType>;
    };
};

template <>
struct AggregateFunctionImpl<AggregateOperation::AVERAGE> {
    template <typename Element>
    struct TypeTraits {
        using ResultType = ArrayAggregateResult<Element, AggregateOperation::AVERAGE>;
        using AggregateDataType = AggregateFunctionAvgData<ResultType>;
        using Function = AggregateFunctionAvg<Element, AggregateDataType>;
        static_assert(std::is_same_v<ResultType, typename Function::ResultType>,
                      "ResultType doesn't match.");
    };
};

template <>
struct AggregateFunctionImpl<AggregateOperation::PRODUCT> {
    template <typename Element>
    struct TypeTraits {
        using ResultType = ArrayAggregateResult<Element, AggregateOperation::PRODUCT>;
        using AggregateDataType = AggregateFunctionProductData<ResultType>;
        using Function = AggregateFunctionProduct<Element, ResultType, AggregateDataType>;
    };
};

template <typename Derived>
struct AggregateFunction {
    template <typename T>
    using Function = typename Derived::template TypeTraits<T>::Function;

    static auto create(const DataTypePtr& data_type_ptr) -> AggregateFunctionPtr {
        return creator_with_type::create<Function>(DataTypes {make_nullable(data_type_ptr)}, true);
    }
};

template <AggregateOperation operation>
struct ArrayAggregateImpl {
    using column_type = ColumnArray;
    using data_type = DataTypeArray;

    static bool _is_variadic() { return false; }

    static size_t _get_number_of_arguments() { return 1; }

    static DataTypePtr get_return_type(const DataTypes& arguments) {
        using Function = AggregateFunction<AggregateFunctionImpl<operation>>;
        const DataTypeArray* data_type_array =
                static_cast<const DataTypeArray*>(remove_nullable(arguments[0]).get());
        auto function = Function::create(data_type_array->get_nested_type());
        if (function) {
            return function->get_return_type();
        } else {
            throw doris::Exception(ErrorCode::INVALID_ARGUMENT,
                                   "Unexpected type {} for aggregation {}",
                                   data_type_array->get_nested_type()->get_name(), operation);
        }
    }

    static Status execute(Block& block, const ColumnNumbers& arguments, size_t result,
                          const DataTypeArray* data_type_array, const ColumnArray& array) {
        ColumnPtr res;
        DataTypePtr type = data_type_array->get_nested_type();
        const IColumn* data = array.get_data_ptr().get();

        const auto& offsets = array.get_offsets();
        if (execute_type<UInt8>(res, type, data, offsets) ||
            execute_type<Int8>(res, type, data, offsets) ||
            execute_type<Int16>(res, type, data, offsets) ||
            execute_type<Int32>(res, type, data, offsets) ||
            execute_type<Int64>(res, type, data, offsets) ||
            execute_type<Int128>(res, type, data, offsets) ||
            execute_type<Float32>(res, type, data, offsets) ||
            execute_type<Float64>(res, type, data, offsets) ||
            execute_type<Decimal32>(res, type, data, offsets) ||
            execute_type<Decimal64>(res, type, data, offsets) ||
            execute_type<Decimal128V2>(res, type, data, offsets) ||
            execute_type<Decimal128V3>(res, type, data, offsets) ||
            execute_type<Date>(res, type, data, offsets) ||
            execute_type<DateTime>(res, type, data, offsets) ||
            execute_type<DateV2>(res, type, data, offsets) ||
            execute_type<DateTimeV2>(res, type, data, offsets)) {
            block.replace_by_position(result, std::move(res));
            return Status::OK();
        } else {
            return Status::RuntimeError("Unexpected column for aggregation: {}", data->get_name());
        }
    }

    template <typename Element>
    static bool execute_type(ColumnPtr& res_ptr, const DataTypePtr& type, const IColumn* data,
                             const ColumnArray::Offsets64& offsets) {
        using ColVecType = ColumnVectorOrDecimal<Element>;
        using ResultType = ArrayAggregateResult<Element, operation>;
        using ColVecResultType = ColumnVectorOrDecimal<ResultType>;
        using Function = AggregateFunction<AggregateFunctionImpl<operation>>;

        const ColVecType* column =
                data->is_nullable()
                        ? check_and_get_column<ColVecType>(
                                  static_cast<const ColumnNullable*>(data)->get_nested_column())
                        : check_and_get_column<ColVecType>(&*data);
        if (!column) {
            return false;
        }

        ColumnPtr res_column;
        if constexpr (IsDecimalNumber<Element>) {
            res_column = ColVecResultType::create(0, column->get_scale());
        } else {
            res_column = ColVecResultType::create();
        }
        res_column = make_nullable(res_column);
        static_cast<ColumnNullable&>(res_column->assume_mutable_ref()).reserve(offsets.size());

        auto function = Function::create(type);
        auto guard = AggregateFunctionGuard(function.get());
        Arena arena;
        auto nullable_column = make_nullable(data->get_ptr());
        const IColumn* columns[] = {nullable_column.get()};
        for (int64_t i = 0; i < offsets.size(); ++i) {
            auto start = offsets[i - 1]; // -1 is ok.
            auto end = offsets[i];
            bool is_empty = (start == end);
            if (is_empty) {
                res_column->assume_mutable()->insert_default();
                continue;
            }
            function->reset(guard.data());
            function->add_batch_range(start, end - 1, guard.data(), columns, &arena,
                                      data->is_nullable());
            function->insert_result_into(guard.data(), res_column->assume_mutable_ref());
        }
        res_ptr = std::move(res_column);
        return true;
    };
};

struct NameArrayMin {
    static constexpr auto name = "array_min";
};

template <>
struct AggregateFunction<AggregateFunctionImpl<AggregateOperation::MIN>> {
    static auto create(const DataTypePtr& data_type_ptr) -> AggregateFunctionPtr {
        return create_aggregate_function_single_value<AggregateFunctionMinData>(
                NameArrayMin::name, {make_nullable(data_type_ptr)}, true);
    }
};

struct NameArrayMax {
    static constexpr auto name = "array_max";
};

template <>
struct AggregateFunction<AggregateFunctionImpl<AggregateOperation::MAX>> {
    static auto create(const DataTypePtr& data_type_ptr) -> AggregateFunctionPtr {
        return create_aggregate_function_single_value<AggregateFunctionMaxData>(
                NameArrayMax::name, {make_nullable(data_type_ptr)}, true);
    }
};

struct NameArraySum {
    static constexpr auto name = "array_sum";
};

struct NameArrayAverage {
    static constexpr auto name = "array_avg";
};

struct NameArrayProduct {
    static constexpr auto name = "array_product";
};

using FunctionArrayMin =
        FunctionArrayMapped<ArrayAggregateImpl<AggregateOperation::MIN>, NameArrayMin>;
using FunctionArrayMax =
        FunctionArrayMapped<ArrayAggregateImpl<AggregateOperation::MAX>, NameArrayMax>;
using FunctionArraySum =
        FunctionArrayMapped<ArrayAggregateImpl<AggregateOperation::SUM>, NameArraySum>;
using FunctionArrayAverage =
        FunctionArrayMapped<ArrayAggregateImpl<AggregateOperation::AVERAGE>, NameArrayAverage>;
using FunctionArrayProduct =
        FunctionArrayMapped<ArrayAggregateImpl<AggregateOperation::PRODUCT>, NameArrayProduct>;

using FunctionArrayJoin = FunctionArrayMapped<ArrayJoinImpl, NameArrayJoin>;

void register_function_array_aggregation(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionArrayMin>();
    factory.register_function<FunctionArrayMax>();
    factory.register_function<FunctionArraySum>();
    factory.register_function<FunctionArrayAverage>();
    factory.register_function<FunctionArrayProduct>();
    factory.register_function<FunctionArrayJoin>();
}

} // namespace vectorized
} // namespace doris
