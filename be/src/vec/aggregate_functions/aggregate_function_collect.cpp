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

#include "vec/aggregate_functions/aggregate_function_collect.h"

#include "common/exception.h"
#include "common/status.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/aggregate_functions/factory_helpers.h"
#include "vec/aggregate_functions/helpers.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

template <PrimitiveType T, bool HasLimit>
AggregateFunctionPtr do_create_agg_function_collect(bool distinct, const DataTypes& argument_types,
                                                    const bool result_is_nullable,

                                                    const AggregateFunctionAttr& attr) {
    if (distinct) {
        if constexpr (T == INVALID_TYPE) {
            throw Exception(ErrorCode::INTERNAL_ERROR,
                            "unexpected type for collect, please check the input");
        } else {
            return creator_without_type::create<AggregateFunctionCollect<
                    AggregateFunctionCollectSetData<T, HasLimit>, HasLimit>>(
                    argument_types, result_is_nullable, attr);
        }
    } else {
        return creator_without_type::create<
                AggregateFunctionCollect<AggregateFunctionCollectListData<T, HasLimit>, HasLimit>>(
                argument_types, result_is_nullable, attr);
    }
}

template <bool HasLimit>
AggregateFunctionPtr create_aggregate_function_collect_impl(const std::string& name,
                                                            const DataTypes& argument_types,
                                                            const bool result_is_nullable,

                                                            const AggregateFunctionAttr& attr) {
    bool distinct = name == "collect_set";

    switch (argument_types[0]->get_primitive_type()) {
    case PrimitiveType::TYPE_BOOLEAN:
        return do_create_agg_function_collect<TYPE_BOOLEAN, HasLimit>(distinct, argument_types,
                                                                      result_is_nullable, attr);
    case PrimitiveType::TYPE_TINYINT:
        return do_create_agg_function_collect<TYPE_TINYINT, HasLimit>(distinct, argument_types,
                                                                      result_is_nullable, attr);
    case PrimitiveType::TYPE_SMALLINT:
        return do_create_agg_function_collect<TYPE_SMALLINT, HasLimit>(distinct, argument_types,
                                                                       result_is_nullable, attr);
    case PrimitiveType::TYPE_INT:
        return do_create_agg_function_collect<TYPE_INT, HasLimit>(distinct, argument_types,
                                                                  result_is_nullable, attr);
    case PrimitiveType::TYPE_BIGINT:
        return do_create_agg_function_collect<TYPE_BIGINT, HasLimit>(distinct, argument_types,
                                                                     result_is_nullable, attr);
    case PrimitiveType::TYPE_LARGEINT:
        return do_create_agg_function_collect<TYPE_LARGEINT, HasLimit>(distinct, argument_types,
                                                                       result_is_nullable, attr);
    case PrimitiveType::TYPE_FLOAT:
        return do_create_agg_function_collect<TYPE_FLOAT, HasLimit>(distinct, argument_types,
                                                                    result_is_nullable, attr);
    case PrimitiveType::TYPE_DOUBLE:
        return do_create_agg_function_collect<TYPE_DOUBLE, HasLimit>(distinct, argument_types,
                                                                     result_is_nullable, attr);
    case PrimitiveType::TYPE_DECIMAL32:
        return do_create_agg_function_collect<TYPE_DECIMAL32, HasLimit>(distinct, argument_types,
                                                                        result_is_nullable, attr);
    case PrimitiveType::TYPE_DECIMAL64:
        return do_create_agg_function_collect<TYPE_DECIMAL64, HasLimit>(distinct, argument_types,
                                                                        result_is_nullable, attr);
    case PrimitiveType::TYPE_DECIMALV2:
        return do_create_agg_function_collect<TYPE_DECIMALV2, HasLimit>(distinct, argument_types,
                                                                        result_is_nullable, attr);
    case PrimitiveType::TYPE_DECIMAL128I:
        return do_create_agg_function_collect<TYPE_DECIMAL128I, HasLimit>(distinct, argument_types,
                                                                          result_is_nullable, attr);
    case PrimitiveType::TYPE_DECIMAL256:
        return do_create_agg_function_collect<TYPE_DECIMAL256, HasLimit>(distinct, argument_types,
                                                                         result_is_nullable, attr);
    case PrimitiveType::TYPE_DATE:
        return do_create_agg_function_collect<TYPE_DATE, HasLimit>(distinct, argument_types,
                                                                   result_is_nullable, attr);
    case PrimitiveType::TYPE_DATETIME:
        return do_create_agg_function_collect<TYPE_DATETIME, HasLimit>(distinct, argument_types,
                                                                       result_is_nullable, attr);
    case PrimitiveType::TYPE_DATEV2:
        return do_create_agg_function_collect<TYPE_DATEV2, HasLimit>(distinct, argument_types,
                                                                     result_is_nullable, attr);
    case PrimitiveType::TYPE_DATETIMEV2:
        return do_create_agg_function_collect<TYPE_DATETIMEV2, HasLimit>(distinct, argument_types,
                                                                         result_is_nullable, attr);
    case PrimitiveType::TYPE_IPV6:
        return do_create_agg_function_collect<TYPE_IPV6, HasLimit>(distinct, argument_types,
                                                                   result_is_nullable, attr);
    case PrimitiveType::TYPE_IPV4:
        return do_create_agg_function_collect<TYPE_IPV4, HasLimit>(distinct, argument_types,
                                                                   result_is_nullable, attr);
    case PrimitiveType::TYPE_STRING:
    case PrimitiveType::TYPE_CHAR:
    case PrimitiveType::TYPE_VARCHAR:
        return do_create_agg_function_collect<TYPE_VARCHAR, HasLimit>(distinct, argument_types,
                                                                      result_is_nullable, attr);
    default:
        // We do not care what the real type is.
        return do_create_agg_function_collect<INVALID_TYPE, HasLimit>(distinct, argument_types,
                                                                      result_is_nullable, attr);
    }
}

AggregateFunctionPtr create_aggregate_function_collect(const std::string& name,
                                                       const DataTypes& argument_types,
                                                       const bool result_is_nullable,
                                                       const AggregateFunctionAttr& attr) {
    assert_arity_range(name, argument_types, 1, 2);
    if (argument_types.size() == 1) {
        return create_aggregate_function_collect_impl<false>(name, argument_types,
                                                             result_is_nullable, attr);
    }
    if (argument_types.size() == 2) {
        return create_aggregate_function_collect_impl<true>(name, argument_types,
                                                            result_is_nullable, attr);
    }
    return nullptr;
}

void register_aggregate_function_collect_list(AggregateFunctionSimpleFactory& factory) {
    // notice: array_agg only differs from collect_list in that array_agg will show null elements in array
    factory.register_function_both("collect_list", create_aggregate_function_collect);
    factory.register_function_both("collect_set", create_aggregate_function_collect);
    factory.register_alias("collect_list", "group_array");
    factory.register_alias("collect_set", "group_uniq_array");

    // Register multi-distinct versions using the same implementation
    factory.register_function_both("multi_distinct_collect_list", create_aggregate_function_collect);
    factory.register_function_both("multi_distinct_array_agg", create_aggregate_function_collect);
}
} // namespace doris::vectorized