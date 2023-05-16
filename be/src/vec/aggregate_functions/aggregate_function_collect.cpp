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

#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/aggregate_functions/helpers.h"

namespace doris::vectorized {

#define FOR_DECIMAL_TYPES(M) \
    M(Decimal32)             \
    M(Decimal64)             \
    M(Decimal128)            \
    M(Decimal128I)

template <typename T, typename HasLimit>
AggregateFunctionPtr do_create_agg_function_collect(bool distinct,
                                                    const DataTypePtr& argument_type) {
    if (distinct) {
        return AggregateFunctionPtr(
                new AggregateFunctionCollect<AggregateFunctionCollectSetData<T, HasLimit>,
                                             HasLimit>(argument_type));
    } else {
        return AggregateFunctionPtr(
                new AggregateFunctionCollect<AggregateFunctionCollectListData<T, HasLimit>,
                                             HasLimit>(argument_type));
    }
}

template <typename HasLimit>
AggregateFunctionPtr create_aggregate_function_collect_impl(const std::string& name,
                                                            const DataTypePtr& argument_type,
                                                            const Array&, const bool) {
    bool distinct = false;
    if (name == "collect_set") {
        distinct = true;
    }

    WhichDataType which(argument_type);
#define DISPATCH(TYPE)                \
    if (which.idx == TypeIndex::TYPE) \
        return do_create_agg_function_collect<TYPE, HasLimit>(distinct, argument_type);
    FOR_NUMERIC_TYPES(DISPATCH)
    FOR_DECIMAL_TYPES(DISPATCH)
#undef DISPATCH
    if (which.is_date_or_datetime()) {
        return do_create_agg_function_collect<Int64, HasLimit>(distinct, argument_type);
    } else if (which.is_date_v2()) {
        return do_create_agg_function_collect<UInt32, HasLimit>(distinct, argument_type);
    } else if (which.is_date_time_v2()) {
        return do_create_agg_function_collect<UInt64, HasLimit>(distinct, argument_type);
    } else if (which.is_string()) {
        return do_create_agg_function_collect<StringRef, HasLimit>(distinct, argument_type);
    }

    LOG(WARNING) << fmt::format("unsupported input type {} for aggregate function {}",
                                argument_type->get_name(), name);
    return nullptr;
}

AggregateFunctionPtr create_aggregate_function_collect(const std::string& name,
                                                       const DataTypes& argument_types,
                                                       const Array&,
                                                       const bool result_is_nullable) {
    if (argument_types.size() == 1) {
        return create_aggregate_function_collect_impl<std::false_type>(name, argument_types[0], {},
                                                                       true);
    }
    if (argument_types.size() == 2) {
        return create_aggregate_function_collect_impl<std::true_type>(name, argument_types[0], {},
                                                                      true);
    }
    LOG(WARNING) << fmt::format("number of parameters for aggregate function {}, should be 1 or 2",
                                name);
    return nullptr;
}

void register_aggregate_function_collect_list(AggregateFunctionSimpleFactory& factory) {
    factory.register_function("collect_list", create_aggregate_function_collect);
    factory.register_function("collect_set", create_aggregate_function_collect);
    factory.register_alias("collect_list", "group_array");
    factory.register_alias("collect_set", "group_uniq_array");
}
} // namespace doris::vectorized