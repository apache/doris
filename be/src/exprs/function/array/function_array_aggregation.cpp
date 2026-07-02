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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <type_traits>
#include <utility>

#include "common/check.h"
#include "common/compare.h"
#include "common/status.h"
#include "core/block/block.h"
#include "core/block/column_numbers.h"
#include "core/call_on_type_index.h"
#include "core/column/column.h"
#include "core/column/column_array.h"
#include "core/column/column_array_view.h"
#include "core/column/column_decimal.h"
#include "core/column/column_nullable.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/define_primitive_type.h"
#include "core/data_type/primitive_type.h"
#include "core/types.h"
#include "exprs/aggregate/aggregate_function_avg.h"
#include "exprs/aggregate/aggregate_function_product.h"
#include "exprs/aggregate/aggregate_function_sum.h"
#include "exprs/function/array/function_array_join.h"
#include "exprs/function/array/function_array_mapped.h"
#include "exprs/function/function.h"
#include "exprs/function/simple_function_factory.h"
#include "util/simd/bits.h"

namespace doris {

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
        static consteval PrimitiveType get_result_type() {
            if constexpr (Element == TYPE_DECIMALV2) {
                return TYPE_DECIMALV2;
            } else if constexpr (is_float_or_double(Element)) {
                return TYPE_DOUBLE;
            } else if constexpr (Element == TYPE_LARGEINT) {
                return TYPE_LARGEINT;
            } else {
                return TYPE_BIGINT;
            }
        }
        static constexpr PrimitiveType ResultType = get_result_type();
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

template <PrimitiveType T>
inline constexpr bool is_array_agg_decimal = is_decimal(T) || T == TYPE_DECIMALV2;

template <PrimitiveType ResultType>
typename PrimitiveTypeTraits<ResultType>::CppType decimal_scale_multiplier(UInt32 scale) {
    typename PrimitiveTypeTraits<ResultType>::CppType multiplier {};
    multiplier.value = PrimitiveTypeTraits<ResultType>::DataType::get_scale_multiplier(scale);
    return multiplier;
}

template <AggregateOperation operation, PrimitiveType InputType, PrimitiveType ResultType>
struct ArrayAggState;

template <PrimitiveType InputType, PrimitiveType ResultType>
struct ArrayAggState<AggregateOperation::SUM, InputType, ResultType> {
    using InputCppType = typename PrimitiveTypeTraits<InputType>::CppType;
    using ResultCppType = typename PrimitiveTypeTraits<ResultType>::CppType;
    using ResultColumnType = typename PrimitiveTypeTraits<ResultType>::ColumnType;

    AggregateFunctionSumData<ResultType> data;
    bool has = false;

    ArrayAggState(UInt32 /*input_scale*/, UInt32 /*result_scale*/) {}

    void reset() {
        data.reset();
        has = false;
    }

    void update(InputCppType value) {
        data.add(ResultCppType(value));
        has = true;
    }

    bool has_value() const { return has; }

    void insert_result_into(IColumn& to) const {
        assert_cast<ResultColumnType&>(to).get_data().push_back(data.get());
    }
};

template <PrimitiveType InputType, PrimitiveType ResultType>
struct ArrayAggState<AggregateOperation::AVERAGE, InputType, ResultType> {
    using InputCppType = typename PrimitiveTypeTraits<InputType>::CppType;
    using ResultCppType = typename PrimitiveTypeTraits<ResultType>::CppType;
    using ResultColumnType = typename PrimitiveTypeTraits<ResultType>::ColumnType;

    AggregateFunctionAvgData<ResultType> data;
    ResultCppType multiplier {};

    explicit ArrayAggState(UInt32 input_scale, UInt32 result_scale) {
        if constexpr (is_array_agg_decimal<InputType>) {
            multiplier = decimal_scale_multiplier<ResultType>(result_scale - input_scale);
        }
    }

    void reset() { data.reset(); }

    void update(InputCppType value) { data.template add<InputType>(value); }

    bool has_value() const { return data.has_value(); }

    void insert_result_into(IColumn& to) const {
        auto& column = assert_cast<ResultColumnType&>(to);
        if constexpr (is_array_agg_decimal<InputType>) {
            column.get_data().push_back(data.template result<ResultCppType>(multiplier));
        } else {
            column.get_data().push_back(data.template result<ResultCppType>());
        }
    }
};

template <PrimitiveType InputType, PrimitiveType ResultType>
struct ArrayAggState<AggregateOperation::PRODUCT, InputType, ResultType> {
    using InputCppType = typename PrimitiveTypeTraits<InputType>::CppType;
    using ResultCppType = typename PrimitiveTypeTraits<ResultType>::CppType;
    using ResultColumnType = typename PrimitiveTypeTraits<ResultType>::ColumnType;

    AggregateFunctionProductData<ResultType> data;
    ResultCppType multiplier {};
    bool has = false;

    explicit ArrayAggState(UInt32 input_scale, UInt32 /*result_scale*/) {
        if constexpr (is_array_agg_decimal<InputType>) {
            multiplier = decimal_scale_multiplier<ResultType>(input_scale);
        }
    }

    void reset() {
        if constexpr (is_array_agg_decimal<InputType>) {
            data.reset(multiplier);
        } else {
            data.reset(ResultCppType(1));
        }
        has = false;
    }

    void update(InputCppType value) {
        data.add(ResultCppType(value), multiplier);
        has = true;
    }

    bool has_value() const { return has; }

    void insert_result_into(IColumn& to) const {
        assert_cast<ResultColumnType&>(to).get_data().push_back(data.get());
    }
};

template <PrimitiveType InputType, bool is_min>
struct ArrayMinMaxState {
    using CppType = typename ColumnElementView<InputType>::ElementType;
    using ResultColumnType = typename PrimitiveTypeTraits<InputType>::ColumnType;

    bool has = false;
    CppType value {};

    ArrayMinMaxState(UInt32 /*input_scale*/, UInt32 /*result_scale*/) {}

    void reset() {
        has = false;
        value = {};
    }

    void update(CppType input) {
        if (!has) {
            value = input;
            has = true;
            return;
        }
        if constexpr (is_min) {
            if (Compare::less(input, value)) {
                value = input;
            }
        } else {
            if (Compare::greater(input, value)) {
                value = input;
            }
        }
    }

    bool has_value() const { return has; }

    void insert_result_into(IColumn& to) const {
        if constexpr (is_string_type(InputType)) {
            assert_cast<ColumnString&>(to).insert_data(value.data, value.size);
        } else {
            assert_cast<ResultColumnType&>(to).get_data().push_back(value);
        }
    }
};

template <PrimitiveType InputType, PrimitiveType ResultType>
struct ArrayAggState<AggregateOperation::MIN, InputType, ResultType>
        : public ArrayMinMaxState<InputType, true> {
    using ArrayMinMaxState<InputType, true>::ArrayMinMaxState;
};

template <PrimitiveType InputType, PrimitiveType ResultType>
struct ArrayAggState<AggregateOperation::MAX, InputType, ResultType>
        : public ArrayMinMaxState<InputType, false> {
    using ArrayMinMaxState<InputType, false>::ArrayMinMaxState;
};

template <AggregateOperation operation, PrimitiveType Element, PrimitiveType ResultType>
bool execute_array_agg_fast_path(ColumnPtr& res_ptr, const ColumnPtr& array_column,
                                 MutableColumnPtr result_column, UInt32 input_scale,
                                 UInt32 result_scale) {
    using State = ArrayAggState<operation, Element, ResultType>;

    auto array_view = ColumnArrayView<Element>::create(array_column);
    auto res_column = ColumnNullable::create(std::move(result_column), ColumnUInt8::create());
    auto& nullable_result = *res_column;
    auto& nested_result = nullable_result.get_nested_column();
    auto& null_map = nullable_result.get_null_map_data();
    nested_result.reserve(array_view.size());
    null_map.reserve(array_view.size());
    const bool has_nested_null =
            array_view.size() > 0 && simd::contain_one(array_view.get_null_map_data(),
                                                       array_view.row_end(array_view.size() - 1));

    State state(input_scale, result_scale);
    for (size_t row = 0; row < array_view.size(); ++row) {
        if (array_view.is_null_at(row)) {
            nullable_result.insert_default();
            continue;
        }

        state.reset();
        if constexpr (is_string_type(Element)) {
            auto array_data = array_view[row];
            for (size_t i = 0; i < array_data.size(); ++i) {
                if (!array_data.is_null_at(i)) {
                    state.update(array_data.value_at(i));
                }
            }
        } else {
            if (has_nested_null) {
                auto array_data = array_view[row];
                for (size_t i = 0; i < array_data.size(); ++i) {
                    if (!array_data.is_null_at(i)) {
                        state.update(array_data.value_at(i));
                    }
                }
            } else {
                const auto* data = array_view.get_data();
                const auto begin = array_view.row_begin(row);
                const auto end = array_view.row_end(row);
                for (size_t i = begin; i < end; ++i) {
                    state.update(data[i]);
                }
            }
        }

        if (state.has_value()) {
            state.insert_result_into(nested_result);
            null_map.push_back(0);
        } else {
            nullable_result.insert_default();
        }
    }

    res_ptr = std::move(res_column);
    return true;
}

template <AggregateOperation operation>
struct ArrayAggregateImpl {
    using column_type = ColumnArray;
    using data_type = DataTypeArray;

    static bool _is_variadic() { return false; }

    static size_t _get_number_of_arguments() { return 1; }

    static Status execute(Block& block, const ColumnNumbers& arguments, uint32_t result,
                          const DataTypeArray* data_type_array, const ColumnArray& array) {
        return execute(block, arguments, result, nullptr, data_type_array, array);
    }

    static Status execute(Block& block, const ColumnNumbers& arguments, uint32_t result,
                          const DataTypePtr& result_type, const DataTypeArray* data_type_array,
                          const ColumnArray& array) {
        ColumnPtr res;
        DataTypePtr type = data_type_array->get_nested_type();
        ColumnPtr array_column = array.get_ptr();

        const auto nested_type = remove_nullable(type)->get_primitive_type();
        const bool matched = dispatch_switch_all(nested_type, [&](auto type_tag) {
            constexpr PrimitiveType element_type = decltype(type_tag)::PType;
            if constexpr (element_type == TYPE_DATE || element_type == TYPE_DATETIME ||
                          element_type == TYPE_TIMEV2 || element_type == TYPE_DECIMALV2) {
                return false;
            } else if constexpr (element_type == TYPE_IPV4 || element_type == TYPE_IPV6) {
                if constexpr (operation != AggregateOperation::MAX &&
                              operation != AggregateOperation::MIN) {
                    return false;
                } else {
                    return execute_type<element_type>(res, type, array_column, result_type);
                }
            } else {
                return execute_type<element_type>(res, type, array_column, result_type);
            }
        });

        if (matched) {
            block.replace_by_position(result, std::move(res));
            return Status::OK();
        } else {
            return Status::RuntimeError("Unexpected column for aggregation: {}",
                                        array.get_data().get_name());
        }
    }

    template <PrimitiveType Element>
    static bool execute_type(ColumnPtr& res_ptr, const DataTypePtr& type,
                             const ColumnPtr& array_column, const DataTypePtr& result_type) {
        if constexpr (is_string_type(Element)) {
            if constexpr (operation == AggregateOperation::SUM ||
                          operation == AggregateOperation::PRODUCT ||
                          operation == AggregateOperation::AVERAGE) {
                return false;
            } else {
                return execute_array_agg_fast_path<operation, Element, Element>(
                        res_ptr, array_column, ColumnString::create(), 0, 0);
            }
        } else {
            if constexpr (operation == AggregateOperation::SUM && Element == TYPE_BOOLEAN) {
                return false;
            } else if constexpr ((operation == AggregateOperation::SUM ||
                                  operation == AggregateOperation::PRODUCT ||
                                  operation == AggregateOperation::AVERAGE) &&
                                 (is_date_type(Element) || is_timestamptz_type(Element))) {
                return false;
            } else if constexpr ((operation == AggregateOperation::SUM ||
                                  operation == AggregateOperation::PRODUCT ||
                                  operation == AggregateOperation::AVERAGE) &&
                                 is_decimalv3(Element)) {
                return execute_decimalv3_type<Element>(res_ptr, type, array_column, result_type);
            } else {
                static constexpr PrimitiveType ResultType = AggregateFunctionTraits<
                        operation>::template TypeTraits<Element>::ResultType;
                using ColVecResultType = typename PrimitiveTypeTraits<ResultType>::ColumnType;

                auto create_column = [](UInt32 scale) -> MutableColumnPtr {
                    if constexpr (is_decimal(Element)) {
                        return ColVecResultType::create(0, scale);
                    } else {
                        return ColVecResultType::create();
                    }
                };

                UInt32 input_scale = 0;
                UInt32 result_scale = 0;
                if constexpr (is_decimal(Element)) {
                    input_scale = get_decimal_scale(*remove_nullable(type));
                    if constexpr (operation == AggregateOperation::AVERAGE) {
                        using AvgFunction = typename AggregateFunctionTraits<
                                operation>::template TypeTraits<Element>::Function;
                        result_scale = std::max(AvgFunction::DEFAULT_MIN_AVG_DECIMAL128_SCALE,
                                                input_scale);
                    } else {
                        result_scale = input_scale;
                    }
                }
                return execute_array_agg_fast_path<operation, Element, ResultType>(
                        res_ptr, array_column, create_column(result_scale), input_scale,
                        result_scale);
            }
        }
    }

    template <PrimitiveType Element>
    static bool execute_decimalv3_type(ColumnPtr& res_ptr, const DataTypePtr& type,
                                       const ColumnPtr& array_column,
                                       const DataTypePtr& result_type) {
        DORIS_CHECK(result_type != nullptr);
        const auto result_primitive_type = remove_nullable(result_type)->get_primitive_type();
        return dispatch_switch_decimalv3(result_primitive_type, [&](auto result_type_tag) {
            constexpr PrimitiveType result_type_value = decltype(result_type_tag)::PType;
            if constexpr (result_type_value == TYPE_DECIMAL128I ||
                          result_type_value == TYPE_DECIMAL256) {
                using ColVecResultType =
                        typename PrimitiveTypeTraits<result_type_value>::ColumnType;

                const auto input_scale = get_decimal_scale(*remove_nullable(type));
                const auto result_scale = get_decimal_scale(*remove_nullable(result_type));
                return execute_array_agg_fast_path<operation, Element, result_type_value>(
                        res_ptr, array_column, ColVecResultType::create(0, result_scale),
                        input_scale, result_scale);
            } else {
                return false;
            }
        });
    }
};

struct NameArrayMin {
    static constexpr auto name = "array_min";
};

struct NameArrayMax {
    static constexpr auto name = "array_max";
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

template <AggregateOperation operation, PrimitiveType ResultType, typename Name>
class FunctionArrayAggDecimalV3 : public IFunction {
public:
    static constexpr auto name = Name::name;
    static_assert(is_decimalv3(ResultType));
    explicit FunctionArrayAggDecimalV3(DataTypePtr result_type)
            : _result_type(std::move(result_type)) {}

    String get_name() const override { return name; }
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        const auto& typed_column = block.get_by_position(arguments[0]);
        auto ptr = typed_column.column->convert_to_full_column_if_const();
        const ColumnArray* column_array;
        if (is_column_nullable(*ptr)) {
            column_array = assert_cast<const ColumnArray*>(
                    assert_cast<const ColumnNullable*>(ptr.get())->get_nested_column_ptr().get());
        } else {
            column_array = assert_cast<const ColumnArray*>(ptr.get());
        }
        const auto* data_type_array =
                assert_cast<const DataTypeArray*>(remove_nullable(typed_column.type).get());
        return ArrayAggregateImpl<operation>::execute(block, arguments, result, _result_type,
                                                      data_type_array, *column_array);
    }

    bool is_variadic() const override { return ArrayAggregateImpl<operation>::_is_variadic(); }

    size_t get_number_of_arguments() const override {
        return ArrayAggregateImpl<operation>::_get_number_of_arguments();
    }

    bool skip_return_type_check() const override { return true; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return _result_type;
    }

private:
    DataTypePtr _result_type;
};

template <AggregateOperation operation, typename Name>
struct ArrayAggDecimalV3Function {
    template <PrimitiveType ResultType>
    using Type = FunctionArrayAggDecimalV3<operation, ResultType, Name>;
};

template <AggregateOperation operation, typename Name, typename Function>
void register_array_reduce_agg_function(SimpleFunctionFactory& factory) {
    ArrayAggFunctionCreator creator = [](const DataTypePtr& result_type) -> FunctionBuilderPtr {
        if (is_decimalv3(result_type->get_primitive_type())) {
            return DefaultFunctionBuilder::create_array_agg_function_decimalv3<
                    ArrayAggDecimalV3Function<operation, Name>::template Type>(result_type);
        } else {
            return std::make_shared<DefaultFunctionBuilder>(Function::create());
        }
    };
    factory.register_array_agg_function(Name::name, creator);
}

void register_array_reduce_agg_functions(SimpleFunctionFactory& factory) {
    register_array_reduce_agg_function<AggregateOperation::SUM, NameArraySum, FunctionArraySum>(
            factory);
    register_array_reduce_agg_function<AggregateOperation::AVERAGE, NameArrayAverage,
                                       FunctionArrayAverage>(factory);
    register_array_reduce_agg_function<AggregateOperation::PRODUCT, NameArrayProduct,
                                       FunctionArrayProduct>(factory);
}

void register_function_array_aggregation(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionArrayMin>();
    factory.register_function<FunctionArrayMax>();
    factory.register_function<FunctionArrayJoin>();
    register_array_reduce_agg_functions(factory);
}

} // namespace doris
