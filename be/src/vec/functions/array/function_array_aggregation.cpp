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

#include "common/exception.h"
#include "common/status.h"
#include "olap/column_predicate.h"
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
#include "vec/functions/function.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

enum class AggregateOperation { MIN, MAX, SUM, AVERAGE, PRODUCT };

// For MIN/MAX, the type of result is the same as the type of elements, we can omit the
// template specialization.
template <AggregateOperation operation>
struct AggregateFunctionTraits;

template <AggregateOperation operation>
    requires(operation == AggregateOperation::MIN || operation == AggregateOperation::MAX)
struct AggregateFunctionTraits<operation> {
    template <PrimitiveType Element>
    struct TypeTraits {
        static constexpr PrimitiveType ResultType = Element;
    };
};

template <>
struct AggregateFunctionTraits<AggregateOperation::SUM> {
    template <PrimitiveType Element>
    struct TypeTraits {
        static constexpr PrimitiveType ResultType =
                Element == TYPE_DECIMALV2 ? TYPE_DECIMALV2
                : is_float_or_double(Element)
                        ? TYPE_DOUBLE
                        : (Element == TYPE_LARGEINT ? TYPE_LARGEINT : TYPE_BIGINT);
        using AggregateDataType = AggregateFunctionSumData<ResultType>;
        using Function = AggregateFunctionSum<Element, ResultType, AggregateDataType>;
    };
};
template <>
struct AggregateFunctionTraits<AggregateOperation::AVERAGE> {
    template <PrimitiveType Element>
    struct TypeTraits {
        static constexpr PrimitiveType ResultType =
                Element == TYPE_DECIMALV2 ? TYPE_DECIMALV2 : TYPE_DOUBLE;
        using AggregateDataType = AggregateFunctionAvgData<ResultType>;
        using Function = AggregateFunctionAvg<Element, ResultType, AggregateDataType>;
        static_assert(std::is_same_v<typename PrimitiveTypeTraits<ResultType>::CppType,
                                     typename Function::ResultType>,
                      "ResultType doesn't match.");
    };
};

template <>
struct AggregateFunctionTraits<AggregateOperation::PRODUCT> {
    template <PrimitiveType Element>
    struct TypeTraits {
        static constexpr PrimitiveType ResultType =
                Element == TYPE_DECIMALV2 ? TYPE_DECIMALV2 : Element;
        using AggregateDataType = AggregateFunctionProductData<ResultType>;
        using Function = AggregateFunctionProduct<Element, ResultType, AggregateDataType>;
    };
};

template <AggregateOperation AggOp, typename Derived>
struct ArrayAggregateFunctionCreator {
    template <PrimitiveType T>
    using Function = typename Derived::template TypeTraits<T>::Function;

    static auto create(const DataTypePtr& data_type_ptr, const AggregateFunctionAttr& attr)
            -> AggregateFunctionPtr {
        if constexpr (AggOp == AggregateOperation::MIN || AggOp == AggregateOperation::MAX) {
            return creator_with_type_list<
                    TYPE_TINYINT, TYPE_SMALLINT, TYPE_INT, TYPE_BIGINT, TYPE_LARGEINT, TYPE_FLOAT,
                    TYPE_DOUBLE, TYPE_DECIMAL32, TYPE_DECIMAL64, TYPE_DECIMAL128I,
                    TYPE_DECIMAL256>::create<Function>(DataTypes {make_nullable(data_type_ptr)},
                                                       true, attr);
        } else {
            return creator_with_type_list<
                    TYPE_TINYINT, TYPE_SMALLINT, TYPE_INT, TYPE_BIGINT, TYPE_LARGEINT, TYPE_FLOAT,
                    TYPE_DOUBLE>::create<Function>(DataTypes {make_nullable(data_type_ptr)}, true,
                                                   attr);
        }
    }
};

template <AggregateOperation operation>
struct ArrayAggregateImpl {
    using column_type = ColumnArray;
    using data_type = DataTypeArray;

    static bool _is_variadic() { return false; }

    static size_t _get_number_of_arguments() { return 1; }

    static bool skip_return_type_check() { return false; }

    static DataTypePtr get_return_type(const DataTypes& arguments) {
        using Function =
                ArrayAggregateFunctionCreator<operation, AggregateFunctionTraits<operation>>;
        const DataTypeArray* data_type_array =
                static_cast<const DataTypeArray*>(remove_nullable(arguments[0]).get());
        auto function = Function::create(data_type_array->get_nested_type(),
                                         {.is_window_function = false, .column_names = {}});
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
            execute_type<TYPE_TIMESTAMPTZ>(res, type, data, offsets) ||
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
        using Function =
                ArrayAggregateFunctionCreator<operation, AggregateFunctionTraits<operation>>;

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

        auto function = Function::create(type, {.is_window_function = false, .column_names = {}});
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
            if constexpr ((operation == AggregateOperation::SUM ||
                           operation == AggregateOperation::PRODUCT ||
                           operation == AggregateOperation::AVERAGE) &&
                          (is_date_type(Element) || is_timestamptz_type(Element) ||
                           is_decimalv3(Element))) {
                return false;
            } else {
                using ColVecType = typename PrimitiveTypeTraits<Element>::ColumnType;
                static constexpr PrimitiveType ResultType = AggregateFunctionTraits<
                        operation>::template TypeTraits<Element>::ResultType;
                using ColVecResultType = typename PrimitiveTypeTraits<ResultType>::ColumnType;

                auto create_column = [](const ColVecType* column) -> ColumnPtr {
                    if constexpr (is_decimal(Element)) {
                        return ColVecResultType::create(0, column->get_scale());
                    } else {
                        return ColVecResultType::create();
                    }
                };

                return execute_type_impl<ColVecType, decltype(create_column)>(
                        res_ptr, type, data, offsets, create_column);
            }
        }
    }
};

struct NameArrayMin {
    static constexpr auto name = "array_min";
};

template <>
struct ArrayAggregateFunctionCreator<AggregateOperation::MIN,
                                     AggregateFunctionTraits<AggregateOperation::MIN>> {
    static auto create(const DataTypePtr& data_type_ptr, const AggregateFunctionAttr& attr)
            -> AggregateFunctionPtr {
        return create_aggregate_function_single_value<AggregateFunctionMinData>(
                NameArrayMin::name, {make_nullable(data_type_ptr)}, make_nullable(data_type_ptr),
                true, attr);
    }
};

struct NameArrayMax {
    static constexpr auto name = "array_max";
};

template <>
struct ArrayAggregateFunctionCreator<AggregateOperation::MAX,
                                     AggregateFunctionTraits<AggregateOperation::MAX>> {
    static auto create(const DataTypePtr& data_type_ptr, const AggregateFunctionAttr& attr)
            -> AggregateFunctionPtr {
        return create_aggregate_function_single_value<AggregateFunctionMaxData>(
                NameArrayMax::name, {make_nullable(data_type_ptr)}, make_nullable(data_type_ptr),
                true, attr);
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

template <AggregateOperation operation>
struct AggregateFunctionTraitsWithResultType;

template <>
struct AggregateFunctionTraitsWithResultType<AggregateOperation::SUM> {
    template <PrimitiveType InputType, PrimitiveType ResultType>
    struct TypeTraits {
        using AggregateDataType = AggregateFunctionSumData<ResultType>;
        using Function = AggregateFunctionSum<InputType, ResultType, AggregateDataType>;
    };
};
template <>
struct AggregateFunctionTraitsWithResultType<AggregateOperation::AVERAGE> {
    template <PrimitiveType InputType, PrimitiveType ResultType>
    struct TypeTraits {
        using AggregateDataType = AggregateFunctionAvgData<ResultType>;
        using Function = AggregateFunctionAvg<InputType, ResultType, AggregateDataType>;
    };
};
template <>
struct AggregateFunctionTraitsWithResultType<AggregateOperation::PRODUCT> {
    template <PrimitiveType InputType, PrimitiveType ResultType>
    struct TypeTraits {
        using AggregateDataType = AggregateFunctionProductData<ResultType>;
        using Function = AggregateFunctionProduct<InputType, ResultType, AggregateDataType>;
    };
};
template <typename Derived>
struct ArrayAggregateFunctionCreatorWithResultType {
    template <PrimitiveType InputType, PrimitiveType ResultType>
    using Function = typename Derived::template TypeTraits<InputType, ResultType>::Function;

    static auto create(const DataTypePtr& data_type_ptr, const DataTypePtr& result_type_ptr,
                       const AggregateFunctionAttr& attr) -> AggregateFunctionPtr {
        return creator_with_type_list<
                TYPE_DECIMAL32, TYPE_DECIMAL64, TYPE_DECIMAL128I,
                TYPE_DECIMAL256>::creator_with_result_type<Function>("",
                                                                     DataTypes {make_nullable(
                                                                             data_type_ptr)},
                                                                     result_type_ptr, true, attr);
    }
};
template <AggregateOperation operation, PrimitiveType ResultType>
struct ArrayAggregateImplDecimalV3;
template <AggregateOperation operation, PrimitiveType ResultType>
    requires(operation == AggregateOperation::SUM || operation == AggregateOperation::PRODUCT ||
             operation == AggregateOperation::AVERAGE)
struct ArrayAggregateImplDecimalV3<operation, ResultType> {
    using column_type = ColumnArray;
    using data_type = DataTypeArray;

    static bool _is_variadic() { return false; }

    static size_t _get_number_of_arguments() { return 1; }

    static bool skip_return_type_check() { return true; }

    static DataTypePtr get_return_type(const DataTypes& arguments) {
        throw doris::Exception(
                ErrorCode::NOT_IMPLEMENTED_ERROR,
                "get_return_type is not implemented for ArrayAggregateImplDecimalV3");
        __builtin_unreachable();
    }

    static Status execute(Block& block, const ColumnNumbers& arguments, uint32_t result,
                          const DataTypePtr& result_type, const DataTypeArray* data_type_array,
                          const ColumnArray& array) {
        ColumnPtr res;
        DataTypePtr type = data_type_array->get_nested_type();
        const IColumn* data = array.get_data_ptr().get();

        const auto& offsets = array.get_offsets();

        if (execute_type<TYPE_DECIMAL32>(res, result_type, type, data, offsets) ||
            execute_type<TYPE_DECIMAL64>(res, result_type, type, data, offsets) ||
            execute_type<TYPE_DECIMAL128I>(res, result_type, type, data, offsets) ||
            execute_type<TYPE_DECIMAL256>(res, result_type, type, data, offsets)) {
            block.replace_by_position(result, std::move(res));
            return Status::OK();
        } else {
            return Status::RuntimeError("Unexpected column for aggregation: {}", data->get_name());
        }
    }

    template <typename ColumnType, typename CreateColumnFunc>
    static bool execute_type_impl(ColumnPtr& res_ptr, const DataTypePtr& result_type,
                                  const DataTypePtr& type, const IColumn* data,
                                  const ColumnArray::Offsets64& offsets,
                                  CreateColumnFunc create_column_func) {
        using Function = ArrayAggregateFunctionCreatorWithResultType<
                AggregateFunctionTraitsWithResultType<operation>>;

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

        auto function = Function::create(type, result_type,
                                         {.is_window_function = false, .column_names = {}});
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
    static bool execute_type(ColumnPtr& res_ptr, const DataTypePtr& result_type,
                             const DataTypePtr& type, const IColumn* data,
                             const ColumnArray::Offsets64& offsets) {
        using ColVecType = typename PrimitiveTypeTraits<Element>::ColumnType;
        using ColVecResultType = typename PrimitiveTypeTraits<ResultType>::ColumnType;

        auto create_column = [](const ColVecType* column) -> ColumnPtr {
            return ColVecResultType::create(0, column->get_scale());
        };

        return execute_type_impl<ColVecType, decltype(create_column)>(res_ptr, result_type, type,
                                                                      data, offsets, create_column);
    }
};

template <typename Impl, typename Name>
class FunctionArrayAggDecimalV3 : public IFunction {
public:
    static constexpr auto name = Name::name;
    explicit FunctionArrayAggDecimalV3(DataTypePtr result_type)
            : _result_type(std::move(result_type)) {}

    String get_name() const override { return name; }
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        const auto& typed_column = block.get_by_position(arguments[0]);
        auto ptr = typed_column.column->convert_to_full_column_if_const();
        const typename Impl::column_type* column_array;
        if (ptr->is_nullable()) {
            column_array = check_and_get_column<const typename Impl::column_type>(
                    assert_cast<const ColumnNullable*>(ptr.get())->get_nested_column_ptr().get());
        } else {
            column_array = check_and_get_column<const typename Impl::column_type>(ptr.get());
        }
        const auto* data_type_array =
                assert_cast<const DataTypeArray*>(remove_nullable(typed_column.type).get());
        return Impl::execute(block, arguments, result, _result_type, data_type_array,
                             *column_array);
    }

    bool is_variadic() const override { return Impl::_is_variadic(); }

    size_t get_number_of_arguments() const override { return Impl::_get_number_of_arguments(); }

    bool skip_return_type_check() const override { return Impl::skip_return_type_check(); }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return Impl::get_return_type(arguments);
    }

private:
    DataTypePtr _result_type;
};
template <PrimitiveType ResultType>
struct ArraySumDecimalV3Attributes {
    static_assert(is_decimalv3(ResultType));
    using AggregateDataType = AggregateFunctionSumData<ResultType>;
    using Function = FunctionArrayAggDecimalV3<
            ArrayAggregateImplDecimalV3<AggregateOperation::SUM, ResultType>, NameArraySum>;
};
template <PrimitiveType ResultType>
using ArraySumDecimalV3 = typename ArraySumDecimalV3Attributes<ResultType>::Function;

template <PrimitiveType ResultType>
struct ArrayAvgDecimalV3Attributes {
    static_assert(is_decimalv3(ResultType));
    using AggregateDataType = AggregateFunctionAvgData<ResultType>;
    using Function = FunctionArrayAggDecimalV3<
            ArrayAggregateImplDecimalV3<AggregateOperation::AVERAGE, ResultType>, NameArrayAverage>;
};
template <PrimitiveType ResultType>
using ArrayAvgDecimalV3 = typename ArrayAvgDecimalV3Attributes<ResultType>::Function;

template <PrimitiveType ResultType>
struct ArrayProductDecimalV3Attributes {
    static_assert(is_decimalv3(ResultType));
    using AggregateDataType = AggregateFunctionProductData<ResultType>;
    using Function = FunctionArrayAggDecimalV3<
            ArrayAggregateImplDecimalV3<AggregateOperation::PRODUCT, ResultType>, NameArrayProduct>;
};
template <PrimitiveType ResultType>
using ArrayProductDecimalV3 = typename ArrayProductDecimalV3Attributes<ResultType>::Function;
void register_array_reduce_agg_functions(SimpleFunctionFactory& factory) {
    {
        ArrayAggFunctionCreator creator = [&](const DataTypePtr& result_type) {
            if (is_decimalv3(result_type->get_primitive_type())) {
                return DefaultFunctionBuilder::create_array_agg_function_decimalv3<
                        ArraySumDecimalV3>(result_type);
            } else {
                FunctionBuilderPtr func =
                        std::make_shared<DefaultFunctionBuilder>(FunctionArraySum::create());
                return func;
            }
        };
        factory.register_array_agg_function(NameArraySum::name, creator);
    }
    {
        ArrayAggFunctionCreator creator = [&](const DataTypePtr& result_type) {
            if (is_decimalv3(result_type->get_primitive_type())) {
                return DefaultFunctionBuilder::create_array_agg_function_decimalv3<
                        ArrayAvgDecimalV3>(result_type);
            } else {
                FunctionBuilderPtr func =
                        std::make_shared<DefaultFunctionBuilder>(FunctionArrayAverage::create());
                return func;
            }
        };
        factory.register_array_agg_function(NameArrayAverage::name, creator);
    }
    {
        ArrayAggFunctionCreator creator = [&](const DataTypePtr& result_type) {
            if (is_decimalv3(result_type->get_primitive_type())) {
                return DefaultFunctionBuilder::create_array_agg_function_decimalv3<
                        ArrayProductDecimalV3>(result_type);
            } else {
                FunctionBuilderPtr func =
                        std::make_shared<DefaultFunctionBuilder>(FunctionArrayProduct::create());
                return func;
            }
        };
        factory.register_array_agg_function(NameArrayProduct::name, creator);
    }
}

void register_function_array_aggregation(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionArrayMin>();
    factory.register_function<FunctionArrayMax>();
    factory.register_function<FunctionArrayJoin>();
    register_array_reduce_agg_functions(factory);
}

} // namespace doris::vectorized
