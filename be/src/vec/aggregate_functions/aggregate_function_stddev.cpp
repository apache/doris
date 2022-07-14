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

#include "vec/aggregate_functions/aggregate_function_stddev.h"

#include "common/logging.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/aggregate_functions/factory_helpers.h"
#include "vec/aggregate_functions/helpers.h"
namespace doris::vectorized {

template <template <typename, bool> class AggregateFunctionTemplate,
          template <typename> class NameData, template <typename, typename> class Data,
          bool is_stddev, bool is_nullable = false>
static IAggregateFunction* create_function_single_value(const String& name,
                                                        const DataTypes& argument_types,
                                                        const Array& parameters) {
    auto type = argument_types[0].get();
    if (type->is_nullable()) {
        type = assert_cast<const DataTypeNullable*>(type)->get_nested_type().get();
    }

    WhichDataType which(*type);

#define DISPATCH(TYPE)                                                                        \
    if (which.idx == TypeIndex::TYPE)                                                         \
        return new AggregateFunctionTemplate<NameData<Data<TYPE, BaseData<TYPE, is_stddev>>>, \
                                             is_nullable>(argument_types);

    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
    if (which.is_decimal32()) {
        return new AggregateFunctionTemplate<
                NameData<Data<Decimal32, BaseDatadecimal<Decimal32, is_stddev>>>, is_nullable>(
                argument_types);
    }
    if (which.is_decimal64()) {
        return new AggregateFunctionTemplate<
                NameData<Data<Decimal64, BaseDatadecimal<Decimal64, is_stddev>>>, is_nullable>(
                argument_types);
    }
    if (which.is_decimal128()) {
        return new AggregateFunctionTemplate<
                NameData<Data<Decimal128, BaseDatadecimal<Decimal128, is_stddev>>>, is_nullable>(
                argument_types);
    }
    DCHECK(false) << "with unknowed type, failed in  create_aggregate_function_stddev_variance";
    return nullptr;
}

template <bool is_stddev, bool is_nullable>
AggregateFunctionPtr create_aggregate_function_variance_samp(const std::string& name,
                                                             const DataTypes& argument_types,
                                                             const Array& parameters,
                                                             const bool result_is_nullable) {
    return AggregateFunctionPtr(
            create_function_single_value<AggregateFunctionSamp, VarianceSampName, SampData,
                                         is_stddev, is_nullable>(name, argument_types, parameters));
}

template <bool is_stddev, bool is_nullable>
AggregateFunctionPtr create_aggregate_function_stddev_samp(const std::string& name,
                                                           const DataTypes& argument_types,
                                                           const Array& parameters,
                                                           const bool result_is_nullable) {
    return AggregateFunctionPtr(
            create_function_single_value<AggregateFunctionSamp, StddevSampName, SampData, is_stddev,
                                         is_nullable>(name, argument_types, parameters));
}

template <bool is_stddev>
AggregateFunctionPtr create_aggregate_function_variance_pop(const std::string& name,
                                                            const DataTypes& argument_types,
                                                            const Array& parameters,
                                                            const bool result_is_nullable) {
    return AggregateFunctionPtr(
            create_function_single_value<AggregateFunctionPop, VarianceName, PopData, is_stddev>(
                    name, argument_types, parameters));
}

template <bool is_stddev>
AggregateFunctionPtr create_aggregate_function_stddev_pop(const std::string& name,
                                                          const DataTypes& argument_types,
                                                          const Array& parameters,
                                                          const bool result_is_nullable) {
    return AggregateFunctionPtr(
            create_function_single_value<AggregateFunctionPop, StddevName, PopData, is_stddev>(
                    name, argument_types, parameters));
}

void register_aggregate_function_stddev_variance_pop(AggregateFunctionSimpleFactory& factory) {
    factory.register_function("variance", create_aggregate_function_variance_pop<false>);
    factory.register_alias("variance", "var_pop");
    factory.register_alias("variance", "variance_pop");
    factory.register_function("stddev", create_aggregate_function_stddev_pop<true>);
    factory.register_alias("stddev", "stddev_pop");
}

void register_aggregate_function_stddev_variance_samp(AggregateFunctionSimpleFactory& factory) {
    // _samp<bool, bool>: first  indicate is stddev or variance function
    //                    second indicate is arg nullable column
    factory.register_function("variance_samp",
                              create_aggregate_function_variance_samp<false, false>, false);
    factory.register_function("variance_samp", create_aggregate_function_variance_samp<false, true>,
                              true);
    factory.register_alias("variance_samp", "var_samp");
    factory.register_function("stddev_samp", create_aggregate_function_stddev_samp<true, false>,
                              false);
    factory.register_function("stddev_samp", create_aggregate_function_stddev_samp<true, true>,
                              true);
}
} // namespace doris::vectorized
