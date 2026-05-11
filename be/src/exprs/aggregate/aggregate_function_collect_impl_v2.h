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

#include "core/call_on_type_index.h"
#include "exprs/aggregate/aggregate_function_collect.h"
#include "exprs/aggregate/factory_helpers.h"
#include "exprs/aggregate/helpers.h"

namespace doris {

// For collect_list_v2: complex types use V2 binary serialization; simple/string types
// use the same data structs as v1 (their serialization is already efficient binary).
template <PrimitiveType T, bool HasLimit>
AggregateFunctionPtr do_create_agg_function_collect_v2(bool distinct,
                                                       const DataTypes& argument_types,
                                                       const bool result_is_nullable,
                                                       const AggregateFunctionAttr& attr) {
    if (distinct) {
        if constexpr (T == INVALID_TYPE) {
            // complex types (ARRAY/MAP/STRUCT): use binary serde with serialized-value dedup
            return creator_without_type::create<AggregateFunctionCollect<
                    AggregateFunctionCollectSetDataV2<T, HasLimit>, HasLimit, true>>(
                    argument_types, result_is_nullable, attr);
        } else {
            return creator_without_type::create<AggregateFunctionCollect<
                    AggregateFunctionCollectSetData<T, HasLimit>, HasLimit, true>>(
                    argument_types, result_is_nullable, attr);
        }
    } else {
        if constexpr (T == INVALID_TYPE) {
            // complex types: use V2 binary serialization
            return creator_without_type::create<AggregateFunctionCollect<
                    AggregateFunctionCollectListDataV2<T, HasLimit>, HasLimit, true>>(
                    argument_types, result_is_nullable, attr);
        } else {
            // primitive / string types: same data struct as v1, just different name
            return creator_without_type::create<AggregateFunctionCollect<
                    AggregateFunctionCollectListData<T, HasLimit>, HasLimit, true>>(
                    argument_types, result_is_nullable, attr);
        }
    }
}

template <bool HasLimit>
AggregateFunctionPtr create_aggregate_function_collect_impl_v2(const std::string& name,
                                                               const DataTypes& argument_types,
                                                               const bool result_is_nullable,
                                                               const AggregateFunctionAttr& attr) {
    bool distinct = name == "collect_set_v2";

    AggregateFunctionPtr agg_fn;
    auto call = [&](const auto& type) -> bool {
        using DispatchType = std::decay_t<decltype(type)>;
        agg_fn = do_create_agg_function_collect_v2<DispatchType::PType, HasLimit>(
                distinct, argument_types, result_is_nullable, attr);
        return true;
    };

    if (!dispatch_switch_all(argument_types[0]->get_primitive_type(), call)) {
        // complex type — INVALID_TYPE sentinel triggers the V2 branch
        agg_fn = do_create_agg_function_collect_v2<INVALID_TYPE, HasLimit>(
                distinct, argument_types, result_is_nullable, attr);
    }
    return agg_fn;
}

} // namespace doris
