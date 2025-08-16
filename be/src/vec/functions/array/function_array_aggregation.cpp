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

#include <type_traits>
#include <utility>

#include "common/status.h"
#include "runtime/define_primitive_type.h"
#include "runtime/primitive_type.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/aggregate_function_avg.h"
#include "vec/aggregate_functions/aggregate_function_min_max.h"
#include "vec/aggregate_functions/aggregate_function_product.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
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

namespace doris::vectorized {

enum class AggregateOperation { MIN, MAX, SUM, AVERAGE, PRODUCT };

template <PrimitiveType Element, AggregateOperation operation, bool enable_decimal256 = false>
struct ArrayAggregateResultImpl;

template <PrimitiveType Element>
struct ArrayAggregateResultImpl<Element, AggregateOperation::MIN> {
    static constexpr PrimitiveType Result = Element;
};

template <PrimitiveType Element>
struct ArrayAggregateResultImpl<Element, AggregateOperation::MAX> {
    static constexpr PrimitiveType Result = Element;
};

template <PrimitiveType Element>
struct ArrayAggregateResultImpl<Element, AggregateOperation::AVERAGE> {
    static constexpr PrimitiveType Result =
            Element == TYPE_DECIMALV2 ? TYPE_DECIMALV2
                                      : (is_decimal(Element) ? TYPE_DECIMAL128I : TYPE_DOUBLE);
};

template <PrimitiveType Element>
struct ArrayAggregateResultImpl<Element, AggregateOperation::AVERAGE, true> {
    static constexpr PrimitiveType Result =
            Element == TYPE_DECIMALV2 ? TYPE_DECIMALV2
                                      : (is_decimal(Element) ? TYPE_DECIMAL256 : TYPE_DOUBLE);
};

template <PrimitiveType Element>
struct ArrayAggregateResultImpl<Element, AggregateOperation::PRODUCT> {
    static constexpr PrimitiveType Result =
            Element == TYPE_DECIMALV2 ? TYPE_DECIMALV2
                                      : (is_decimal(Element) ? TYPE_DECIMAL128I : TYPE_DOUBLE);
};

template <PrimitiveType Element>
struct ArrayAggregateResultImpl<Element, AggregateOperation::PRODUCT, true> {
    static constexpr PrimitiveType Result =
            Element == TYPE_DECIMALV2 ? TYPE_DECIMALV2
                                      : (is_decimal(Element) ? TYPE_DECIMAL256 : TYPE_DOUBLE);
};

template <PrimitiveType Element>
struct ArrayAggregateResultImpl<Element, AggregateOperation::SUM> {
    static constexpr PrimitiveType Result =
            Element == TYPE_DECIMALV2
                    ? TYPE_DECIMALV2
                    : (is_decimal(Element) ? TYPE_DECIMAL128I
                       : is_float_or_double(Element)
                               ? TYPE_DOUBLE
                               : (Element == TYPE_LARGEINT ? TYPE_LARGEINT : TYPE_BIGINT));
};
template <PrimitiveType Element>
struct ArrayAggregateResultImpl<Element, AggregateOperation::SUM, true> {
    static constexpr PrimitiveType Result =
            Element == TYPE_DECIMALV2
                    ? TYPE_DECIMALV2
                    : (is_decimal(Element) ? TYPE_DECIMAL256
                       : is_float_or_double(Element)
                               ? TYPE_DOUBLE
                               : (Element == TYPE_LARGEINT ? TYPE_LARGEINT : TYPE_BIGINT));
};

// For MIN/MAX, the type of result is the same as the type of elements, we can omit the
// template specialization.
template <AggregateOperation operation, bool enable_decimal256 = false>
struct AggregateFunctionImpl;

template <>
struct AggregateFunctionImpl<AggregateOperation::SUM> {
    template <PrimitiveType Element>
    struct TypeTraits {
        static constexpr PrimitiveType ResultType =
                ArrayAggregateResultImpl<Element, AggregateOperation::SUM, false>::Result;
        using AggregateDataType = AggregateFunctionSumData<ResultType>;
        using Function = AggregateFunctionSum<Element, ResultType, AggregateDataType>;
    };
};
template <>
struct AggregateFunctionImpl<AggregateOperation::SUM, true> {
    template <PrimitiveType Element>
    struct TypeTraits {
        static constexpr PrimitiveType ResultType =
                ArrayAggregateResultImpl<Element, AggregateOperation::SUM, true>::Result;
        using AggregateDataType = AggregateFunctionSumData<ResultType>;
        using Function = AggregateFunctionSum<Element, ResultType, AggregateDataType>;
    };
};

template <>
struct AggregateFunctionImpl<AggregateOperation::AVERAGE> {
    template <PrimitiveType Element>
    struct TypeTraits {
        static constexpr PrimitiveType ResultType =
                ArrayAggregateResultImpl<Element, AggregateOperation::AVERAGE, false>::Result;
        using AggregateDataType = AggregateFunctionAvgData<ResultType>;
        using Function = AggregateFunctionAvg<Element, AggregateDataType>;
        static_assert(std::is_same_v<typename PrimitiveTypeTraits<ResultType>::ColumnItemType,
                                     typename Function::ResultType>,
                      "ResultType doesn't match.");
    };
};

template <>
struct AggregateFunctionImpl<AggregateOperation::AVERAGE, true> {
    template <PrimitiveType Element>
    struct TypeTraits {
        static constexpr PrimitiveType ResultType =
                ArrayAggregateResultImpl<Element, AggregateOperation::AVERAGE, true>::Result;
        using AggregateDataType = AggregateFunctionAvgData<ResultType>;
        using Function = AggregateFunctionAvg<Element, AggregateDataType>;
        static_assert(std::is_same_v<typename PrimitiveTypeTraits<ResultType>::ColumnItemType,
                                     typename Function::ResultType>,
                      "ResultType doesn't match.");
    };
};

template <>
struct AggregateFunctionImpl<AggregateOperation::PRODUCT> {
    template <PrimitiveType Element>
    struct TypeTraits {
        static constexpr PrimitiveType ResultType =
                ArrayAggregateResultImpl<Element, AggregateOperation::PRODUCT, false>::Result;
        using AggregateDataType = AggregateFunctionProductData<ResultType>;
        using Function = AggregateFunctionProduct<Element, ResultType, AggregateDataType>;
    };
};

template <>
struct AggregateFunctionImpl<AggregateOperation::PRODUCT, true> {
    template <PrimitiveType Element>
    struct TypeTraits {
        static constexpr PrimitiveType ResultType =
                ArrayAggregateResultImpl<Element, AggregateOperation::PRODUCT, true>::Result;
        using AggregateDataType = AggregateFunctionProductData<ResultType>;
        using Function = AggregateFunctionProduct<Element, ResultType, AggregateDataType>;
    };
};

template <typename Derived>
struct AggregateFunction {
    template <PrimitiveType T>
    using Function = typename Derived::template TypeTraits<T>::Function;

    static auto create(const DataTypePtr& data_type_ptr, const AggregateFunctionAttr& attr)
            -> AggregateFunctionPtr {
        return creator_with_type_list<
                TYPE_TINYINT, TYPE_SMALLINT, TYPE_INT, TYPE_BIGINT, TYPE_LARGEINT, TYPE_FLOAT,
                TYPE_DOUBLE, TYPE_DECIMAL32, TYPE_DECIMAL64, TYPE_DECIMAL128I,
                TYPE_DECIMAL256>::create<Function>(DataTypes {make_nullable(data_type_ptr)}, true,
                                                   attr);
    }
};

template <AggregateOperation operation, bool enable_decimal256 = false>
struct ArrayAggregateImpl {
    using column_type = ColumnArray;
    using data_type = DataTypeArray;

    static bool _is_variadic() { return false; }

    static size_t _get_number_of_arguments() { return 1; }

    static DataTypePtr get_return_type(const DataTypes& arguments) {
        using Function = AggregateFunction<AggregateFunctionImpl<operation, enable_decimal256>>;
        const DataTypeArray* data_type_array =
                static_cast<const DataTypeArray*>(remove_nullable(arguments[0]).get());
        auto function = Function::create(data_type_array->get_nested_type(),
                                         {.enable_decimal256 = enable_decimal256,
                                          .is_window_function = false,
                                          .column_names = {}});
        if (function) {
            return function->get_return_type();
        } else {
            throw doris::Exception(ErrorCode::INVALID_ARGUMENT,
                                   "Unexpected type {} for aggregation {}",
                                   data_type_array->get_nested_type()->get_name(), operation);
        }
    }

    static Status execute(Block& block, const ColumnNumbers& arguments, uint32_t result,
                          const DataTypeArray* data_type_array, const ColumnArray& array) {
        ColumnPtr res;
        DataTypePtr type = data_type_array->get_nested_type();
        const IColumn* data = array.get_data_ptr().get();

        const auto& offsets = array.get_offsets();
        if constexpr (operation == AggregateOperation::MAX ||
                      operation == AggregateOperation::MIN) {
            // min/max can only be applied on ip type
            if (execute_type<TYPE_IPV4>(res, type, data, offsets) ||
                execute_type<TYPE_IPV6>(res, type, data, offsets)) {
                block.replace_by_position(result, std::move(res));
                return Status::OK();
            }
        }

        if (execute_type<TYPE_BOOLEAN>(res, type, data, offsets) ||
            execute_type<TYPE_TINYINT>(res, type, data, offsets) ||
            execute_type<TYPE_SMALLINT>(res, type, data, offsets) ||
            execute_type<TYPE_INT>(res, type, data, offsets) ||
            execute_type<TYPE_BIGINT>(res, type, data, offsets) ||
            execute_type<TYPE_LARGEINT>(res, type, data, offsets) ||
            execute_type<TYPE_FLOAT>(res, type, data, offsets) ||
            execute_type<TYPE_DOUBLE>(res, type, data, offsets) ||
            execute_type<TYPE_DECIMAL32>(res, type, data, offsets) ||
            execute_type<TYPE_DECIMAL64>(res, type, data, offsets) ||
            execute_type<TYPE_DECIMAL128I>(res, type, data, offsets) ||
            execute_type<TYPE_DECIMAL256>(res, type, data, offsets) ||
            execute_type<TYPE_DATEV2>(res, type, data, offsets) ||
            execute_type<TYPE_DATETIMEV2>(res, type, data, offsets) ||
            execute_type<TYPE_VARCHAR>(res, type, data, offsets)) {
            block.replace_by_position(result, std::move(res));
            return Status::OK();
        } else {
            return Status::RuntimeError("Unexpected column for aggregation: {}", data->get_name());
        }
    }

    template <typename ColumnType, typename CreateColumnFunc>
    static bool execute_type_impl(ColumnPtr& res_ptr, const DataTypePtr& type, const IColumn* data,
                                  const ColumnArray::Offsets64& offsets,
                                  CreateColumnFunc create_column_func) {
        using Function = AggregateFunction<AggregateFunctionImpl<operation, enable_decimal256>>;

        const ColumnType* column =
                data->is_nullable()
                        ? check_and_get_column<ColumnType>(
                                  static_cast<const ColumnNullable*>(data)->get_nested_column())
                        : check_and_get_column<ColumnType>(&*data);
        if (!column) {
            return false;
        }

        ColumnPtr res_column = create_column_func(column);
        res_column = make_nullable(res_column);
        assert_cast<ColumnNullable&>(res_column->assume_mutable_ref()).reserve(offsets.size());

        auto function = Function::create(type, {.enable_decimal256 = enable_decimal256,
                                                .is_window_function = false,
                                                .column_names = {}});
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
            function->add_batch_range(start, end - 1, guard.data(), columns, arena,
                                      data->is_nullable());
            function->insert_result_into(guard.data(), res_column->assume_mutable_ref());
        }
        res_ptr = std::move(res_column);
        return true;
    }

    template <PrimitiveType Element>
    static bool execute_type(ColumnPtr& res_ptr, const DataTypePtr& type, const IColumn* data,
                             const ColumnArray::Offsets64& offsets) {
        if constexpr (is_string_type(Element)) {
            if (operation == AggregateOperation::SUM || operation == AggregateOperation::PRODUCT ||
                operation == AggregateOperation::AVERAGE) {
                return false;
            }

            auto create_column = [](const ColumnString*) -> ColumnPtr {
                return ColumnString::create();
            };

            return execute_type_impl<ColumnString, decltype(create_column)>(res_ptr, type, data,
                                                                            offsets, create_column);
        } else {
            using ColVecType = typename PrimitiveTypeTraits<Element>::ColumnType;
            static constexpr PrimitiveType ResultType =
                    ArrayAggregateResultImpl<Element, operation, enable_decimal256>::Result;
            using ColVecResultType = typename PrimitiveTypeTraits<ResultType>::ColumnType;

            auto create_column = [](const ColVecType* column) -> ColumnPtr {
                if constexpr (is_decimal(Element)) {
                    return ColVecResultType::create(0, column->get_scale());
                } else {
                    return ColVecResultType::create();
                }
            };

            return execute_type_impl<ColVecType, decltype(create_column)>(res_ptr, type, data,
                                                                          offsets, create_column);
        }
    }
};

struct NameArrayMin {
    static constexpr auto name = "array_min";
};

template <>
struct AggregateFunction<AggregateFunctionImpl<AggregateOperation::MIN>> {
    static auto create(const DataTypePtr& data_type_ptr, const AggregateFunctionAttr& attr)
            -> AggregateFunctionPtr {
        return create_aggregate_function_single_value<AggregateFunctionMinData>(
                NameArrayMin::name, {make_nullable(data_type_ptr)}, true, attr);
    }
};

struct NameArrayMax {
    static constexpr auto name = "array_max";
};

template <>
struct AggregateFunction<AggregateFunctionImpl<AggregateOperation::MAX>> {
    static auto create(const DataTypePtr& data_type_ptr, const AggregateFunctionAttr& attr)
            -> AggregateFunctionPtr {
        return create_aggregate_function_single_value<AggregateFunctionMaxData>(
                NameArrayMax::name, {make_nullable(data_type_ptr)}, true, attr);
    }
};

struct NameArraySum {
    static constexpr auto name = "array_sum";
};
struct NameArraySumDecimal256 {
    static constexpr auto name = "array_sum_decimal256";
};

struct NameArrayAverage {
    static constexpr auto name = "array_avg";
};
struct NameArrayAverageDecimal256 {
    static constexpr auto name = "array_avg_decimal256";
};

struct NameArrayProduct {
    static constexpr auto name = "array_product";
};

struct NameArrayProductDecimal256 {
    static constexpr auto name = "array_product_decimal256";
};

using FunctionArrayMin =
        FunctionArrayMapped<ArrayAggregateImpl<AggregateOperation::MIN>, NameArrayMin>;
using FunctionArrayMax =
        FunctionArrayMapped<ArrayAggregateImpl<AggregateOperation::MAX>, NameArrayMax>;
using FunctionArraySum =
        FunctionArrayMapped<ArrayAggregateImpl<AggregateOperation::SUM>, NameArraySum>;
using FunctionArraySumDecimal256 =
        FunctionArrayMapped<ArrayAggregateImpl<AggregateOperation::SUM, true>,
                            NameArraySumDecimal256>;
using FunctionArrayAverage =
        FunctionArrayMapped<ArrayAggregateImpl<AggregateOperation::AVERAGE>, NameArrayAverage>;
using FunctionArrayAverageDecimal256 =
        FunctionArrayMapped<ArrayAggregateImpl<AggregateOperation::AVERAGE, true>,
                            NameArrayAverageDecimal256>;
using FunctionArrayProduct =
        FunctionArrayMapped<ArrayAggregateImpl<AggregateOperation::PRODUCT>, NameArrayProduct>;

using FunctionArrayProductDecimal256 =
        FunctionArrayMapped<ArrayAggregateImpl<AggregateOperation::PRODUCT, true>,
                            NameArrayProductDecimal256>;

using FunctionArrayJoin = FunctionArrayMapped<ArrayJoinImpl, NameArrayJoin>;

void register_function_array_aggregation(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionArrayMin>();
    factory.register_function<FunctionArrayMax>();
    factory.register_function<FunctionArraySum>();
    factory.register_function<FunctionArraySumDecimal256>();
    factory.register_function<FunctionArrayAverage>();
    factory.register_function<FunctionArrayAverageDecimal256>();
    factory.register_function<FunctionArrayProduct>();
    factory.register_function<FunctionArrayProductDecimal256>();
    factory.register_function<FunctionArrayJoin>();
}

} // namespace doris::vectorized
