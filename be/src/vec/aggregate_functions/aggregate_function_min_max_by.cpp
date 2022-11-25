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

namespace doris::vectorized {

/// min_by, max_by
template <template <typename, bool> class AggregateFunctionTemplate,
          template <typename, typename> class Data, typename VT>
static IAggregateFunction* create_aggregate_function_min_max_by_impl(
        const DataTypes& argument_types) {
    const DataTypePtr& value_arg_type = argument_types[0];
    const DataTypePtr& key_arg_type = argument_types[1];

    WhichDataType which(key_arg_type);
#define DISPATCH(TYPE)                                                                     \
    if (which.idx == TypeIndex::TYPE)                                                      \
        return new AggregateFunctionTemplate<Data<VT, SingleValueDataFixed<TYPE>>, false>( \
                value_arg_type, key_arg_type);
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
    if (which.idx == TypeIndex::String) {
        return new AggregateFunctionTemplate<Data<VT, SingleValueDataString>, false>(value_arg_type,
                                                                                     key_arg_type);
    }
    if (which.idx == TypeIndex::DateTime || which.idx == TypeIndex::Date) {
        return new AggregateFunctionTemplate<Data<VT, SingleValueDataFixed<Int64>>, false>(
                value_arg_type, key_arg_type);
    }
    if (which.idx == TypeIndex::DateV2) {
        return new AggregateFunctionTemplate<Data<VT, SingleValueDataFixed<UInt32>>, false>(
                value_arg_type, key_arg_type);
    }
    if (which.idx == TypeIndex::Decimal32) {
        return new AggregateFunctionTemplate<Data<VT, SingleValueDataDecimal<Decimal32>>, false>(
                value_arg_type, key_arg_type);
    }
    if (which.idx == TypeIndex::Decimal64) {
        return new AggregateFunctionTemplate<Data<VT, SingleValueDataDecimal<Decimal64>>, false>(
                value_arg_type, key_arg_type);
    }
    if (which.idx == TypeIndex::Decimal128) {
        return new AggregateFunctionTemplate<Data<VT, SingleValueDataDecimal<Decimal128>>, false>(
                value_arg_type, key_arg_type);
    }
    if (which.idx == TypeIndex::Decimal128I) {
        return new AggregateFunctionTemplate<Data<VT, SingleValueDataDecimal<Decimal128I>>, false>(
                value_arg_type, key_arg_type);
    }
    return nullptr;
}

/// min_by, max_by
template <template <typename, bool> class AggregateFunctionTemplate,
          template <typename, typename> class Data>
static IAggregateFunction* create_aggregate_function_min_max_by(const String& name,
                                                                const DataTypes& argument_types,
                                                                const Array& parameters) {
    assert_no_parameters(name, parameters);
    assert_binary(name, argument_types);

    const DataTypePtr& value_arg_type = argument_types[0];

    WhichDataType which(value_arg_type);
#define DISPATCH(TYPE)                                                                    \
    if (which.idx == TypeIndex::TYPE)                                                     \
        return create_aggregate_function_min_max_by_impl<AggregateFunctionTemplate, Data, \
                                                         SingleValueDataFixed<TYPE>>(     \
                argument_types);
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
    if (which.idx == TypeIndex::String) {
        return create_aggregate_function_min_max_by_impl<AggregateFunctionTemplate, Data,
                                                         SingleValueDataString>(argument_types);
    }
    if (which.idx == TypeIndex::DateTime || which.idx == TypeIndex::Date) {
        return create_aggregate_function_min_max_by_impl<AggregateFunctionTemplate, Data,
                                                         SingleValueDataFixed<Int64>>(
                argument_types);
    }
    if (which.idx == TypeIndex::DateV2) {
        return create_aggregate_function_min_max_by_impl<AggregateFunctionTemplate, Data,
                                                         SingleValueDataFixed<UInt32>>(
                argument_types);
    }
    if (which.idx == TypeIndex::Decimal128) {
        return create_aggregate_function_min_max_by_impl<AggregateFunctionTemplate, Data,
                                                         SingleValueDataFixed<DecimalV2Value>>(
                argument_types);
    }
    if (which.idx == TypeIndex::Decimal32) {
        return create_aggregate_function_min_max_by_impl<AggregateFunctionTemplate, Data,
                                                         SingleValueDataFixed<Int32>>(
                argument_types);
    }
    if (which.idx == TypeIndex::Decimal64) {
        return create_aggregate_function_min_max_by_impl<AggregateFunctionTemplate, Data,
                                                         SingleValueDataFixed<Int64>>(
                argument_types);
    }
    if (which.idx == TypeIndex::Decimal128I) {
        return create_aggregate_function_min_max_by_impl<AggregateFunctionTemplate, Data,
                                                         SingleValueDataFixed<Int128I>>(
                argument_types);
    }
    return nullptr;
}

AggregateFunctionPtr create_aggregate_function_max_by(const std::string& name,
                                                      const DataTypes& argument_types,
                                                      const Array& parameters,
                                                      const bool result_is_nullable) {
    return AggregateFunctionPtr(create_aggregate_function_min_max_by<AggregateFunctionsMinMaxBy,
                                                                     AggregateFunctionMaxByData>(
            name, argument_types, parameters));
}

AggregateFunctionPtr create_aggregate_function_min_by(const std::string& name,
                                                      const DataTypes& argument_types,
                                                      const Array& parameters,
                                                      const bool result_is_nullable) {
    return AggregateFunctionPtr(create_aggregate_function_min_max_by<AggregateFunctionsMinMaxBy,
                                                                     AggregateFunctionMinByData>(
            name, argument_types, parameters));
}

void register_aggregate_function_min_max_by(AggregateFunctionSimpleFactory& factory) {
    factory.register_function("max_by", create_aggregate_function_max_by);
    factory.register_function("min_by", create_aggregate_function_min_by);
}

} // namespace doris::vectorized
