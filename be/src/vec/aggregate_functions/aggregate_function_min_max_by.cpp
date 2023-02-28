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

#include "vec/aggregate_functions/aggregate_function_min_max_by.h"

#include "vec/aggregate_functions/aggregate_function_min_max.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/aggregate_functions/factory_helpers.h"
#include "vec/aggregate_functions/helpers.h"
#include "vec/core/types.h"

namespace doris::vectorized {

/// min_by, max_by
template <template <typename> class AggregateFunctionTemplate,
          template <typename, typename> class Data, typename VT>
static IAggregateFunction* create_aggregate_function_min_max_by_impl(
        const DataTypes& argument_types, const bool result_is_nullable) {
    WhichDataType which(remove_nullable(argument_types[1]));

#define DISPATCH(TYPE)                                                            \
    if (which.idx == TypeIndex::TYPE)                                             \
        return creator_without_type::create<                                      \
                AggregateFunctionTemplate<Data<VT, SingleValueDataFixed<TYPE>>>>( \
                result_is_nullable, argument_types);
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH

#define DISPATCH(TYPE)                                                              \
    if (which.idx == TypeIndex::TYPE)                                               \
        return creator_without_type::create<                                        \
                AggregateFunctionTemplate<Data<VT, SingleValueDataDecimal<TYPE>>>>( \
                result_is_nullable, argument_types);
    FOR_DECIMAL_TYPES(DISPATCH)
#undef DISPATCH

    if (which.idx == TypeIndex::String) {
        return creator_without_type::create<
                AggregateFunctionTemplate<Data<VT, SingleValueDataString>>>(result_is_nullable,
                                                                            argument_types);
    }
    if (which.idx == TypeIndex::DateTime || which.idx == TypeIndex::Date) {
        return creator_without_type::create<
                AggregateFunctionTemplate<Data<VT, SingleValueDataFixed<Int64>>>>(
                result_is_nullable, argument_types);
    }
    if (which.idx == TypeIndex::DateV2) {
        return creator_without_type::create<
                AggregateFunctionTemplate<Data<VT, SingleValueDataFixed<UInt32>>>>(
                result_is_nullable, argument_types);
    }
    if (which.idx == TypeIndex::DateTimeV2) {
        return creator_without_type::create<
                AggregateFunctionTemplate<Data<VT, SingleValueDataFixed<UInt64>>>>(
                result_is_nullable, argument_types);
    }
    return nullptr;
}

/// min_by, max_by
template <template <typename> class AggregateFunctionTemplate,
          template <typename, typename> class Data>
static IAggregateFunction* create_aggregate_function_min_max_by(const String& name,
                                                                const DataTypes& argument_types,
                                                                const bool result_is_nullable) {
    assert_binary(name, argument_types);

    WhichDataType which(remove_nullable(argument_types[0]));
#define DISPATCH(TYPE)                                                                    \
    if (which.idx == TypeIndex::TYPE)                                                     \
        return create_aggregate_function_min_max_by_impl<AggregateFunctionTemplate, Data, \
                                                         SingleValueDataFixed<TYPE>>(     \
                argument_types, result_is_nullable);
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH

#define DISPATCH(TYPE)                                                                    \
    if (which.idx == TypeIndex::TYPE)                                                     \
        return create_aggregate_function_min_max_by_impl<AggregateFunctionTemplate, Data, \
                                                         SingleValueDataDecimal<TYPE>>(   \
                argument_types, result_is_nullable);
    FOR_DECIMAL_TYPES(DISPATCH)
#undef DISPATCH

    if (which.idx == TypeIndex::String) {
        return create_aggregate_function_min_max_by_impl<AggregateFunctionTemplate, Data,
                                                         SingleValueDataString>(argument_types,
                                                                                result_is_nullable);
    }
    if (which.idx == TypeIndex::DateTime || which.idx == TypeIndex::Date) {
        return create_aggregate_function_min_max_by_impl<AggregateFunctionTemplate, Data,
                                                         SingleValueDataFixed<Int64>>(
                argument_types, result_is_nullable);
    }
    if (which.idx == TypeIndex::DateV2) {
        return create_aggregate_function_min_max_by_impl<AggregateFunctionTemplate, Data,
                                                         SingleValueDataFixed<UInt32>>(
                argument_types, result_is_nullable);
    }
    if (which.idx == TypeIndex::DateTimeV2) {
        return create_aggregate_function_min_max_by_impl<AggregateFunctionTemplate, Data,
                                                         SingleValueDataFixed<UInt64>>(
                argument_types, result_is_nullable);
    }
    return nullptr;
}

AggregateFunctionPtr create_aggregate_function_max_by(const std::string& name,
                                                      const DataTypes& argument_types,
                                                      const bool result_is_nullable) {
    return AggregateFunctionPtr(create_aggregate_function_min_max_by<AggregateFunctionsMinMaxBy,
                                                                     AggregateFunctionMaxByData>(
            name, argument_types, result_is_nullable));
}

AggregateFunctionPtr create_aggregate_function_min_by(const std::string& name,
                                                      const DataTypes& argument_types,
                                                      const bool result_is_nullable) {
    return AggregateFunctionPtr(create_aggregate_function_min_max_by<AggregateFunctionsMinMaxBy,
                                                                     AggregateFunctionMinByData>(
            name, argument_types, result_is_nullable));
}

void register_aggregate_function_min_max_by(AggregateFunctionSimpleFactory& factory) {
    factory.register_function_both("max_by", create_aggregate_function_max_by);
    factory.register_function_both("min_by", create_aggregate_function_min_by);
}

} // namespace doris::vectorized
