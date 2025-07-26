// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

#include "vec/aggregate_functions/aggregate_function_regr_union.h"

#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/aggregate_functions/helpers.h"
#include "vec/data_types/data_type.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

template <PrimitiveType T, template <PrimitiveType> class StatFunctionTemplate>
AggregateFunctionPtr type_dispatch_for_aggregate_function_regr(const DataTypes& argument_types,
                                                               const bool& result_is_nullable,
                                                               const AggregateFunctionAttr& attr,
                                                               bool y_nullable_input,
                                                               bool x_nullable_input) {
    if (y_nullable_input) {
        if (x_nullable_input) {
            return creator_without_type::create_ignore_nullable<
                    AggregateFunctionRegrSimple<StatFunctionTemplate<T>, true, true>>(
                    argument_types, result_is_nullable, attr);
        } else {
            return creator_without_type::create_ignore_nullable<
                    AggregateFunctionRegrSimple<StatFunctionTemplate<T>, true, false>>(
                    argument_types, result_is_nullable, attr);
        }
    } else {
        if (x_nullable_input) {
            return creator_without_type::create_ignore_nullable<
                    AggregateFunctionRegrSimple<StatFunctionTemplate<T>, false, true>>(
                    argument_types, result_is_nullable, attr);
        } else {
            return creator_without_type::create_ignore_nullable<
                    AggregateFunctionRegrSimple<StatFunctionTemplate<T>, false, false>>(
                    argument_types, result_is_nullable, attr);
        }
    }
}

template <template <PrimitiveType> class StatFunctionTemplate>
AggregateFunctionPtr create_aggregate_function_regr(const std::string& name,
                                                    const DataTypes& argument_types,
                                                    const bool result_is_nullable,
                                                    const AggregateFunctionAttr& attr) {
    if (argument_types.size() != 2) {
        LOG(WARNING) << "aggregate function " << name << " requires exactly 2 arguments";
        return nullptr;
    }
    if (!result_is_nullable) {
        LOG(WARNING) << "aggregate function " << name << " requires nullable result type";
        return nullptr;
    }

    bool y_nullable_input = argument_types[0]->is_nullable();
    bool x_nullable_input = argument_types[1]->is_nullable();

    if (argument_types[0]->get_primitive_type() == PrimitiveType::TYPE_BOOLEAN &&
        argument_types[1]->get_primitive_type() == PrimitiveType::TYPE_BOOLEAN) {
        return type_dispatch_for_aggregate_function_regr<TYPE_BOOLEAN, StatFunctionTemplate>(
                argument_types, result_is_nullable, attr, y_nullable_input, x_nullable_input);
    } else if (argument_types[0]->get_primitive_type() == PrimitiveType::TYPE_TINYINT &&
               argument_types[1]->get_primitive_type() == PrimitiveType::TYPE_TINYINT) {
        return type_dispatch_for_aggregate_function_regr<TYPE_TINYINT, StatFunctionTemplate>(
                argument_types, result_is_nullable, attr, y_nullable_input, x_nullable_input);
    } else if (argument_types[0]->get_primitive_type() == PrimitiveType::TYPE_SMALLINT &&
               argument_types[1]->get_primitive_type() == PrimitiveType::TYPE_SMALLINT) {
        return type_dispatch_for_aggregate_function_regr<TYPE_SMALLINT, StatFunctionTemplate>(
                argument_types, result_is_nullable, attr, y_nullable_input, x_nullable_input);
    } else if (argument_types[0]->get_primitive_type() == PrimitiveType::TYPE_INT &&
               argument_types[1]->get_primitive_type() == PrimitiveType::TYPE_INT) {
        return type_dispatch_for_aggregate_function_regr<TYPE_INT, StatFunctionTemplate>(
                argument_types, result_is_nullable, attr, y_nullable_input, x_nullable_input);
    } else if (argument_types[0]->get_primitive_type() == PrimitiveType::TYPE_BIGINT &&
               argument_types[1]->get_primitive_type() == PrimitiveType::TYPE_BIGINT) {
        return type_dispatch_for_aggregate_function_regr<TYPE_BIGINT, StatFunctionTemplate>(
                argument_types, result_is_nullable, attr, y_nullable_input, x_nullable_input);
    } else if (argument_types[0]->get_primitive_type() == PrimitiveType::TYPE_LARGEINT &&
               argument_types[1]->get_primitive_type() == PrimitiveType::TYPE_LARGEINT) {
        return type_dispatch_for_aggregate_function_regr<TYPE_LARGEINT, StatFunctionTemplate>(
                argument_types, result_is_nullable, attr, y_nullable_input, x_nullable_input);
    } else if (argument_types[0]->get_primitive_type() == PrimitiveType::TYPE_FLOAT &&
               argument_types[1]->get_primitive_type() == PrimitiveType::TYPE_FLOAT) {
        return type_dispatch_for_aggregate_function_regr<TYPE_FLOAT, StatFunctionTemplate>(
                argument_types, result_is_nullable, attr, y_nullable_input, x_nullable_input);
    } else if (argument_types[0]->get_primitive_type() == PrimitiveType::TYPE_DOUBLE &&
               argument_types[1]->get_primitive_type() == PrimitiveType::TYPE_DOUBLE) {
        return type_dispatch_for_aggregate_function_regr<TYPE_DOUBLE, StatFunctionTemplate>(
                argument_types, result_is_nullable, attr, y_nullable_input, x_nullable_input);
    } else {
        LOG(WARNING) << "unsupported input types " << argument_types[0]->get_name() << " and "
                     << argument_types[1]->get_name() << " for aggregate function " << name;
        return nullptr;
    }
}

void register_aggregate_function_regr_union(AggregateFunctionSimpleFactory& factory) {
    factory.register_function_both("regr_slope", create_aggregate_function_regr<RegrSlopeFunc>);
    factory.register_function_both("regr_intercept",
                                   create_aggregate_function_regr<RegrInterceptFunc>);
}
} // namespace doris::vectorized