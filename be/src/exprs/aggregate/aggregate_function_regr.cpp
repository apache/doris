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

#include "exprs/aggregate/aggregate_function_regr.h"

#include "core/data_type/data_type.h"
#include "core/data_type/define_primitive_type.h"
#include "exprs/aggregate/aggregate_function.h"
#include "exprs/aggregate/aggregate_function_simple_factory.h"
#include "exprs/aggregate/factory_helpers.h"
#include "exprs/aggregate/helpers.h"

namespace doris {

template <RegrFunctionKind kind>
AggregateFunctionPtr create_aggregate_function_regr(const std::string& name,
                                                    const DataTypes& argument_types,
                                                    const DataTypePtr& result_type,
                                                    const bool result_is_nullable,
                                                    const AggregateFunctionAttr& attr) {
    assert_arity_range(name, argument_types, 2, 2);
    DCHECK(argument_types[0]->get_primitive_type() == TYPE_DOUBLE);
    DCHECK(argument_types[1]->get_primitive_type() == TYPE_DOUBLE);
    if constexpr (kind == RegrFunctionKind::regr_count) {
        DCHECK(result_type->get_primitive_type() == TYPE_BIGINT);
        DCHECK(!result_is_nullable);
    } else {
        DCHECK(result_type->get_primitive_type() == TYPE_DOUBLE);
        DCHECK(result_is_nullable);
    }

    bool y_nullable_input = argument_types[0]->is_nullable();
    bool x_nullable_input = argument_types[1]->is_nullable();
    if (y_nullable_input) {
        if (x_nullable_input) {
            return creator_without_type::create_ignore_nullable<
                    AggregateFunctionRegr<TYPE_DOUBLE, kind, true, true>>(argument_types,
                                                                          result_is_nullable, attr);
        } else {
            return creator_without_type::create_ignore_nullable<
                    AggregateFunctionRegr<TYPE_DOUBLE, kind, true, false>>(
                    argument_types, result_is_nullable, attr);
        }
    } else {
        if (x_nullable_input) {
            return creator_without_type::create_ignore_nullable<
                    AggregateFunctionRegr<TYPE_DOUBLE, kind, false, true>>(
                    argument_types, result_is_nullable, attr);
        } else {
            return creator_without_type::create_ignore_nullable<
                    AggregateFunctionRegr<TYPE_DOUBLE, kind, false, false>>(
                    argument_types, result_is_nullable, attr);
        }
    }
}

void register_aggregate_function_regr(AggregateFunctionSimpleFactory& factory) {
    factory.register_function_both(RegrTraits<RegrFunctionKind::regr_avgx>::name,
                                   create_aggregate_function_regr<RegrFunctionKind::regr_avgx>);
    factory.register_function_both(RegrTraits<RegrFunctionKind::regr_avgy>::name,
                                   create_aggregate_function_regr<RegrFunctionKind::regr_avgy>);
    factory.register_function_both(RegrTraits<RegrFunctionKind::regr_count>::name,
                                   create_aggregate_function_regr<RegrFunctionKind::regr_count>);
    factory.register_function_both(RegrTraits<RegrFunctionKind::regr_slope>::name,
                                   create_aggregate_function_regr<RegrFunctionKind::regr_slope>);
    factory.register_function_both(
            RegrTraits<RegrFunctionKind::regr_intercept>::name,
            create_aggregate_function_regr<RegrFunctionKind::regr_intercept>);
    factory.register_function_both(RegrTraits<RegrFunctionKind::regr_sxx>::name,
                                   create_aggregate_function_regr<RegrFunctionKind::regr_sxx>);
    factory.register_function_both(RegrTraits<RegrFunctionKind::regr_syy>::name,
                                   create_aggregate_function_regr<RegrFunctionKind::regr_syy>);
    factory.register_function_both(RegrTraits<RegrFunctionKind::regr_sxy>::name,
                                   create_aggregate_function_regr<RegrFunctionKind::regr_sxy>);
    factory.register_function_both(RegrTraits<RegrFunctionKind::regr_r2>::name,
                                   create_aggregate_function_regr<RegrFunctionKind::regr_r2>);
}

} // namespace doris
