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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Processors/Transforms/WindowTransform.cpp
// and modified by Doris

#include "vec/aggregate_functions/aggregate_function_window.h"

#include <string>
#include <variant>

#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/aggregate_functions/helpers.h"
#include "vec/data_types/data_type.h"
#include "vec/utils/template_helpers.hpp"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

template <template <typename> class AggregateFunctionTemplate,
          template <typename ColVecType, bool, bool> class Data,
          template <typename, bool> class Impl, bool result_is_nullable, bool arg_is_nullable>
AggregateFunctionPtr create_function_lead_lag_first_last(const String& name,
                                                         const DataTypes& argument_types) {
    bool arg_ignore_null_value = false;
    // FE have rewrite case first_value(k1,false)--->first_value(k1)
    // so size is 2, must will be arg_ignore_null_value
    if (argument_types.size() == 2) {
        DCHECK(name == "first_value" || name == "last_value" || name == "nth_value")
                << "invalid function name: " << name;
        arg_ignore_null_value = true;
    }
    switch (argument_types[0]->get_primitive_type()) {
    case PrimitiveType::TYPE_BOOLEAN: {
        if (arg_ignore_null_value) {
            return std::make_shared<AggregateFunctionTemplate<
                    Impl<Data<ColumnUInt8, result_is_nullable, arg_is_nullable>, true>>>(
                    argument_types);
        } else {
            return std::make_shared<AggregateFunctionTemplate<
                    Impl<Data<ColumnUInt8, result_is_nullable, arg_is_nullable>, false>>>(
                    argument_types);
        }
    }
    case PrimitiveType::TYPE_TINYINT: {
        if (arg_ignore_null_value) {
            return std::make_shared<AggregateFunctionTemplate<
                    Impl<Data<ColumnInt8, result_is_nullable, arg_is_nullable>, true>>>(
                    argument_types);
        } else {
            return std::make_shared<AggregateFunctionTemplate<
                    Impl<Data<ColumnInt8, result_is_nullable, arg_is_nullable>, false>>>(
                    argument_types);
        }
    }
    case PrimitiveType::TYPE_SMALLINT: {
        if (arg_ignore_null_value) {
            return std::make_shared<AggregateFunctionTemplate<
                    Impl<Data<ColumnInt16, result_is_nullable, arg_is_nullable>, true>>>(
                    argument_types);
        } else {
            return std::make_shared<AggregateFunctionTemplate<
                    Impl<Data<ColumnInt16, result_is_nullable, arg_is_nullable>, false>>>(
                    argument_types);
        }
    }
    case PrimitiveType::TYPE_INT: {
        if (arg_ignore_null_value) {
            return std::make_shared<AggregateFunctionTemplate<
                    Impl<Data<ColumnInt32, result_is_nullable, arg_is_nullable>, true>>>(
                    argument_types);
        } else {
            return std::make_shared<AggregateFunctionTemplate<
                    Impl<Data<ColumnInt32, result_is_nullable, arg_is_nullable>, false>>>(
                    argument_types);
        }
    }
    case PrimitiveType::TYPE_BIGINT: {
        if (arg_ignore_null_value) {
            return std::make_shared<AggregateFunctionTemplate<
                    Impl<Data<ColumnInt64, result_is_nullable, arg_is_nullable>, true>>>(
                    argument_types);
        } else {
            return std::make_shared<AggregateFunctionTemplate<
                    Impl<Data<ColumnInt64, result_is_nullable, arg_is_nullable>, false>>>(
                    argument_types);
        }
    }
    case PrimitiveType::TYPE_LARGEINT: {
        if (arg_ignore_null_value) {
            return std::make_shared<AggregateFunctionTemplate<
                    Impl<Data<ColumnInt128, result_is_nullable, arg_is_nullable>, true>>>(
                    argument_types);
        } else {
            return std::make_shared<AggregateFunctionTemplate<
                    Impl<Data<ColumnInt128, result_is_nullable, arg_is_nullable>, false>>>(
                    argument_types);
        }
    }
    case PrimitiveType::TYPE_FLOAT: {
        if (arg_ignore_null_value) {
            return std::make_shared<AggregateFunctionTemplate<
                    Impl<Data<ColumnFloat32, result_is_nullable, arg_is_nullable>, true>>>(
                    argument_types);
        } else {
            return std::make_shared<AggregateFunctionTemplate<
                    Impl<Data<ColumnFloat32, result_is_nullable, arg_is_nullable>, false>>>(
                    argument_types);
        }
    }
    case PrimitiveType::TYPE_DOUBLE: {
        if (arg_ignore_null_value) {
            return std::make_shared<AggregateFunctionTemplate<
                    Impl<Data<ColumnFloat64, result_is_nullable, arg_is_nullable>, true>>>(
                    argument_types);
        } else {
            return std::make_shared<AggregateFunctionTemplate<
                    Impl<Data<ColumnFloat64, result_is_nullable, arg_is_nullable>, false>>>(
                    argument_types);
        }
    }
    case PrimitiveType::TYPE_DECIMAL32: {
        if (arg_ignore_null_value) {
            return std::make_shared<AggregateFunctionTemplate<
                    Impl<Data<ColumnDecimal32, result_is_nullable, arg_is_nullable>, true>>>(
                    argument_types);
        } else {
            return std::make_shared<AggregateFunctionTemplate<
                    Impl<Data<ColumnDecimal32, result_is_nullable, arg_is_nullable>, false>>>(
                    argument_types);
        }
    }
    case PrimitiveType::TYPE_DECIMAL64: {
        if (arg_ignore_null_value) {
            return std::make_shared<AggregateFunctionTemplate<
                    Impl<Data<ColumnDecimal64, result_is_nullable, arg_is_nullable>, true>>>(
                    argument_types);
        } else {
            return std::make_shared<AggregateFunctionTemplate<
                    Impl<Data<ColumnDecimal64, result_is_nullable, arg_is_nullable>, false>>>(
                    argument_types);
        }
    }
    case PrimitiveType::TYPE_DECIMAL128I: {
        if (arg_ignore_null_value) {
            return std::make_shared<AggregateFunctionTemplate<
                    Impl<Data<ColumnDecimal128V3, result_is_nullable, arg_is_nullable>, true>>>(
                    argument_types);
        } else {
            return std::make_shared<AggregateFunctionTemplate<
                    Impl<Data<ColumnDecimal128V3, result_is_nullable, arg_is_nullable>, false>>>(
                    argument_types);
        }
    }
    case PrimitiveType::TYPE_DECIMALV2: {
        if (arg_ignore_null_value) {
            return std::make_shared<AggregateFunctionTemplate<
                    Impl<Data<ColumnDecimal128V2, result_is_nullable, arg_is_nullable>, true>>>(
                    argument_types);
        } else {
            return std::make_shared<AggregateFunctionTemplate<
                    Impl<Data<ColumnDecimal128V2, result_is_nullable, arg_is_nullable>, false>>>(
                    argument_types);
        }
    }
    case PrimitiveType::TYPE_DECIMAL256: {
        if (arg_ignore_null_value) {
            return std::make_shared<AggregateFunctionTemplate<
                    Impl<Data<ColumnDecimal256, result_is_nullable, arg_is_nullable>, true>>>(
                    argument_types);
        } else {
            return std::make_shared<AggregateFunctionTemplate<
                    Impl<Data<ColumnDecimal256, result_is_nullable, arg_is_nullable>, false>>>(
                    argument_types);
        }
    }
    case PrimitiveType::TYPE_STRING:
    case PrimitiveType::TYPE_CHAR:
    case PrimitiveType::TYPE_VARCHAR:
    case PrimitiveType::TYPE_JSONB: {
        if (arg_ignore_null_value) {
            return std::make_shared<AggregateFunctionTemplate<
                    Impl<Data<ColumnString, result_is_nullable, arg_is_nullable>, true>>>(
                    argument_types);
        } else {
            return std::make_shared<AggregateFunctionTemplate<
                    Impl<Data<ColumnString, result_is_nullable, arg_is_nullable>, false>>>(
                    argument_types);
        }
    }
    case PrimitiveType::TYPE_DATE: {
        if (arg_ignore_null_value) {
            return std::make_shared<AggregateFunctionTemplate<
                    Impl<Data<ColumnDate, result_is_nullable, arg_is_nullable>, true>>>(
                    argument_types);
        } else {
            return std::make_shared<AggregateFunctionTemplate<
                    Impl<Data<ColumnDate, result_is_nullable, arg_is_nullable>, false>>>(
                    argument_types);
        }
    }
    case PrimitiveType::TYPE_DATETIME: {
        if (arg_ignore_null_value) {
            return std::make_shared<AggregateFunctionTemplate<
                    Impl<Data<ColumnDateTime, result_is_nullable, arg_is_nullable>, true>>>(
                    argument_types);
        } else {
            return std::make_shared<AggregateFunctionTemplate<
                    Impl<Data<ColumnDateTime, result_is_nullable, arg_is_nullable>, false>>>(
                    argument_types);
        }
    }
    case PrimitiveType::TYPE_DATETIMEV2: {
        if (arg_ignore_null_value) {
            return std::make_shared<AggregateFunctionTemplate<
                    Impl<Data<ColumnDateTimeV2, result_is_nullable, arg_is_nullable>, true>>>(
                    argument_types);
        } else {
            return std::make_shared<AggregateFunctionTemplate<
                    Impl<Data<ColumnDateTimeV2, result_is_nullable, arg_is_nullable>, false>>>(
                    argument_types);
        }
    }
    case PrimitiveType::TYPE_DATEV2: {
        if (arg_ignore_null_value) {
            return std::make_shared<AggregateFunctionTemplate<
                    Impl<Data<ColumnDateV2, result_is_nullable, arg_is_nullable>, true>>>(
                    argument_types);
        } else {
            return std::make_shared<AggregateFunctionTemplate<
                    Impl<Data<ColumnDateV2, result_is_nullable, arg_is_nullable>, false>>>(
                    argument_types);
        }
    }
    case PrimitiveType::TYPE_IPV4: {
        if (arg_ignore_null_value) {
            return std::make_shared<AggregateFunctionTemplate<
                    Impl<Data<ColumnIPv4, result_is_nullable, arg_is_nullable>, true>>>(
                    argument_types);
        } else {
            return std::make_shared<AggregateFunctionTemplate<
                    Impl<Data<ColumnIPv4, result_is_nullable, arg_is_nullable>, false>>>(
                    argument_types);
        }
    }
    case PrimitiveType::TYPE_IPV6: {
        if (arg_ignore_null_value) {
            return std::make_shared<AggregateFunctionTemplate<
                    Impl<Data<ColumnIPv6, result_is_nullable, arg_is_nullable>, true>>>(
                    argument_types);
        } else {
            return std::make_shared<AggregateFunctionTemplate<
                    Impl<Data<ColumnIPv6, result_is_nullable, arg_is_nullable>, false>>>(
                    argument_types);
        }
    }
    case PrimitiveType::TYPE_ARRAY: {
        if (arg_ignore_null_value) {
            return std::make_shared<AggregateFunctionTemplate<
                    Impl<Data<ColumnArray, result_is_nullable, arg_is_nullable>, true>>>(
                    argument_types);
        } else {
            return std::make_shared<AggregateFunctionTemplate<
                    Impl<Data<ColumnArray, result_is_nullable, arg_is_nullable>, false>>>(
                    argument_types);
        }
    }
    case PrimitiveType::TYPE_MAP: {
        if (arg_ignore_null_value) {
            return std::make_shared<AggregateFunctionTemplate<
                    Impl<Data<ColumnMap, result_is_nullable, arg_is_nullable>, true>>>(
                    argument_types);
        } else {
            return std::make_shared<AggregateFunctionTemplate<
                    Impl<Data<ColumnMap, result_is_nullable, arg_is_nullable>, false>>>(
                    argument_types);
        }
    }
    case PrimitiveType::TYPE_STRUCT: {
        if (arg_ignore_null_value) {
            return std::make_shared<AggregateFunctionTemplate<
                    Impl<Data<ColumnStruct, result_is_nullable, arg_is_nullable>, true>>>(
                    argument_types);
        } else {
            return std::make_shared<AggregateFunctionTemplate<
                    Impl<Data<ColumnStruct, result_is_nullable, arg_is_nullable>, false>>>(
                    argument_types);
        }
    }
    case PrimitiveType::TYPE_VARIANT: {
        if (arg_ignore_null_value) {
            return std::make_shared<AggregateFunctionTemplate<
                    Impl<Data<ColumnVariant, result_is_nullable, arg_is_nullable>, true>>>(
                    argument_types);
        } else {
            return std::make_shared<AggregateFunctionTemplate<
                    Impl<Data<ColumnVariant, result_is_nullable, arg_is_nullable>, false>>>(
                    argument_types);
        }
    }
    case PrimitiveType::TYPE_BITMAP: {
        if (arg_ignore_null_value) {
            return std::make_shared<AggregateFunctionTemplate<
                    Impl<Data<ColumnBitmap, result_is_nullable, arg_is_nullable>, true>>>(
                    argument_types);
        } else {
            return std::make_shared<AggregateFunctionTemplate<
                    Impl<Data<ColumnBitmap, result_is_nullable, arg_is_nullable>, false>>>(
                    argument_types);
        }
    }
    case PrimitiveType::TYPE_HLL: {
        if (arg_ignore_null_value) {
            return std::make_shared<AggregateFunctionTemplate<
                    Impl<Data<ColumnHLL, result_is_nullable, arg_is_nullable>, true>>>(
                    argument_types);
        } else {
            return std::make_shared<AggregateFunctionTemplate<
                    Impl<Data<ColumnHLL, result_is_nullable, arg_is_nullable>, false>>>(
                    argument_types);
        }
    }
    case PrimitiveType::TYPE_QUANTILE_STATE: {
        if (arg_ignore_null_value) {
            return std::make_shared<AggregateFunctionTemplate<
                    Impl<Data<ColumnQuantileState, result_is_nullable, arg_is_nullable>, true>>>(
                    argument_types);
        } else {
            return std::make_shared<AggregateFunctionTemplate<
                    Impl<Data<ColumnQuantileState, result_is_nullable, arg_is_nullable>, false>>>(
                    argument_types);
        }
    }
    default:
        LOG(WARNING) << "with unknowed type, failed in  create_aggregate_function_" << name
                     << " and type is: " << argument_types[0]->get_name();
        return nullptr;
    }
}

#define CREATE_WINDOW_FUNCTION_WITH_NAME_AND_DATA(CREATE_FUNCTION_NAME, FUNCTION_DATA,             \
                                                  FUNCTION_IMPL)                                   \
    AggregateFunctionPtr CREATE_FUNCTION_NAME(                                                     \
            const std::string& name, const DataTypes& argument_types,                              \
            const bool result_is_nullable, const AggregateFunctionAttr& attr) {                    \
        const bool arg_is_nullable = argument_types[0]->is_nullable();                             \
        AggregateFunctionPtr res = nullptr;                                                        \
                                                                                                   \
        std::visit(                                                                                \
                [&](auto result_is_nullable, auto arg_is_nullable) {                               \
                    res = AggregateFunctionPtr(                                                    \
                            create_function_lead_lag_first_last<WindowFunctionData, FUNCTION_DATA, \
                                                                FUNCTION_IMPL, result_is_nullable, \
                                                                arg_is_nullable>(name,             \
                                                                                 argument_types)); \
                },                                                                                 \
                make_bool_variant(result_is_nullable), make_bool_variant(arg_is_nullable));        \
        if (!res) {                                                                                \
            LOG(WARNING) << " failed in  create_aggregate_function_" << name                       \
                         << " and type is: " << argument_types[0]->get_name();                     \
        }                                                                                          \
        return res;                                                                                \
    }

CREATE_WINDOW_FUNCTION_WITH_NAME_AND_DATA(create_aggregate_function_window_lag, LeadLagData,
                                          WindowFunctionLagImpl);
CREATE_WINDOW_FUNCTION_WITH_NAME_AND_DATA(create_aggregate_function_window_lead, LeadLagData,
                                          WindowFunctionLeadImpl);
CREATE_WINDOW_FUNCTION_WITH_NAME_AND_DATA(create_aggregate_function_window_first, FirstLastData,
                                          WindowFunctionFirstImpl);
CREATE_WINDOW_FUNCTION_WITH_NAME_AND_DATA(create_aggregate_function_window_last, FirstLastData,
                                          WindowFunctionLastImpl);
CREATE_WINDOW_FUNCTION_WITH_NAME_AND_DATA(create_aggregate_function_window_nth_value, NthValueData,
                                          WindowFunctionNthValueImpl);

template <typename AggregateFunctionTemplate>
AggregateFunctionPtr create_empty_arg_window(const std::string& name,
                                             const DataTypes& argument_types,
                                             const bool result_is_nullable,
                                             const AggregateFunctionAttr& attr) {
    if (!argument_types.empty()) {
        throw doris::Exception(
                Status::InternalError("create_window: argument_types must be empty"));
    }
    std::unique_ptr<IAggregateFunction> result =
            std::make_unique<AggregateFunctionTemplate>(argument_types);
    CHECK_AGG_FUNCTION_SERIALIZED_TYPE(AggregateFunctionTemplate);
    return AggregateFunctionPtr(result.release());
}

void register_aggregate_function_window_rank(AggregateFunctionSimpleFactory& factory) {
    factory.register_function("dense_rank", create_empty_arg_window<WindowFunctionDenseRank>);
    factory.register_function("rank", create_empty_arg_window<WindowFunctionRank>);
    factory.register_function("percent_rank", create_empty_arg_window<WindowFunctionPercentRank>);
    factory.register_function("row_number", create_empty_arg_window<WindowFunctionRowNumber>);
    factory.register_function("ntile", creator_without_type::creator<WindowFunctionNTile>);
    factory.register_function("cume_dist", create_empty_arg_window<WindowFunctionCumeDist>);
}

void register_aggregate_function_window_lead_lag_first_last(
        AggregateFunctionSimpleFactory& factory) {
    factory.register_function_both("lead", create_aggregate_function_window_lead);
    factory.register_function_both("lag", create_aggregate_function_window_lag);
    factory.register_function_both("first_value", create_aggregate_function_window_first);
    factory.register_function_both("last_value", create_aggregate_function_window_last);
    factory.register_function_both("nth_value", create_aggregate_function_window_nth_value);
}

} // namespace doris::vectorized
