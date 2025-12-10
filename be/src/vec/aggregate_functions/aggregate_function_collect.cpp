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
#include "vec/core/call_on_type_index.h"

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

    AggregateFunctionPtr agg_fn;
    auto call = [&](const auto& type) -> bool {
        using DispatcType = std::decay_t<decltype(type)>;
        agg_fn = do_create_agg_function_collect<DispatcType::PType, HasLimit>(
                distinct, argument_types, result_is_nullable, attr);
        return true;
    };

    if (!dispatch_switch_all(argument_types[0]->get_primitive_type(), call)) {
        // We do not care what the real type is.
        agg_fn = do_create_agg_function_collect<INVALID_TYPE, HasLimit>(distinct, argument_types,
                                                                        result_is_nullable, attr);
    }
    return agg_fn;
}

AggregateFunctionPtr create_aggregate_function_collect(const std::string& name,
                                                       const DataTypes& argument_types,
                                                       const DataTypePtr& result_type,
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
}
} // namespace doris::vectorized