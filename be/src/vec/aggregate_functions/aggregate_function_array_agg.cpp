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

#include "vec/aggregate_functions/aggregate_function_array_agg.h"

#include "vec/aggregate_functions/aggregate_function_collect.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/aggregate_functions/helpers.h"
#include "vec/core/call_on_type_index.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

template <PrimitiveType T>
AggregateFunctionPtr do_create_agg_function_collect(const DataTypes& argument_types,
                                                    const bool result_is_nullable,
                                                    const AggregateFunctionAttr& attr) {
    if (argument_types[0]->is_nullable()) {
        return creator_without_type::create_ignore_nullable<
                AggregateFunctionArrayAgg<AggregateFunctionArrayAggData<T>>>(
                argument_types, result_is_nullable, attr);
    } else {
        return creator_without_type::create<
                AggregateFunctionCollect<AggregateFunctionCollectListData<T, false>, false>>(
                argument_types, result_is_nullable, attr);
    }
}

AggregateFunctionPtr create_aggregate_function_array_agg(const std::string& name,
                                                         const DataTypes& argument_types,
                                                         const DataTypePtr& result_type,
                                                         const bool result_is_nullable,
                                                         const AggregateFunctionAttr& attr) {
    AggregateFunctionPtr agg_fn;
    auto call = [&](const auto& type) -> bool {
        using DispatcType = std::decay_t<decltype(type)>;
        agg_fn = do_create_agg_function_collect<DispatcType::PType>(argument_types,
                                                                    result_is_nullable, attr);
        return true;
    };

    if (!dispatch_switch_all(argument_types[0]->get_primitive_type(), call)) {
        // We do not care what the real type is.
        agg_fn = do_create_agg_function_collect<INVALID_TYPE>(argument_types, result_is_nullable,
                                                              attr);
    }
    return agg_fn;
}

void register_aggregate_function_array_agg(AggregateFunctionSimpleFactory& factory) {
    factory.register_function_both("array_agg", create_aggregate_function_array_agg);
}
} // namespace doris::vectorized