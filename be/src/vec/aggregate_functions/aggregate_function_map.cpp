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

#include "vec/aggregate_functions/aggregate_function_map.h"

#include "runtime/primitive_type.h"
#include "vec/aggregate_functions/helpers.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

template <typename K>
AggregateFunctionPtr create_agg_function_map_agg(const DataTypes& argument_types,
                                                 const bool result_is_nullable) {
    return creator_without_type::create_ignore_nullable<
            AggregateFunctionMapAgg<AggregateFunctionMapAggData<K>, K>>(argument_types,
                                                                        result_is_nullable);
}

AggregateFunctionPtr create_aggregate_function_map_agg(const std::string& name,
                                                       const DataTypes& argument_types,
                                                       const bool result_is_nullable,
                                                       const AggregateFunctionAttr& attr) {
    WhichDataType type(remove_nullable(argument_types[0]));

#define DISPATCH(TYPE)               \
    if (type.idx == TypeIndex::TYPE) \
        return create_agg_function_map_agg<TYPE>(argument_types, result_is_nullable);

    FOR_NUMERIC_TYPES(DISPATCH)
    FOR_DECIMAL_TYPES(DISPATCH)
#undef DISPATCH

    if (type.idx == TypeIndex::String) {
        return create_agg_function_map_agg<String>(argument_types, result_is_nullable);
    }
    if (type.idx == TypeIndex::DateTime || type.idx == TypeIndex::Date) {
        return create_agg_function_map_agg<Int64>(argument_types, result_is_nullable);
    }
    if (type.idx == TypeIndex::DateV2) {
        return create_agg_function_map_agg<UInt32>(argument_types, result_is_nullable);
    }
    if (type.idx == TypeIndex::DateTimeV2) {
        return create_agg_function_map_agg<UInt64>(argument_types, result_is_nullable);
    }

    LOG(WARNING) << fmt::format("unsupported input type {} for aggregate function {}",
                                argument_types[0]->get_name(), name);
    return nullptr;
}

void register_aggregate_function_map_agg(AggregateFunctionSimpleFactory& factory) {
    factory.register_function_both("map_agg", create_aggregate_function_map_agg);
}

} // namespace doris::vectorized