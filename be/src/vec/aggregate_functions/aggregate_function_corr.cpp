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

#include "aggregate_function_corr.h"

#include "runtime/define_primitive_type.h"
#include "vec/aggregate_functions/aggregate_function_binary.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/aggregate_functions/factory_helpers.h"
#include "vec/aggregate_functions/helpers.h"

namespace doris::vectorized {

template <PrimitiveType T>
using CorrMomentStat = StatFunc<T, CorrMoment>;

AggregateFunctionPtr create_aggregate_corr_function(const std::string& name,
                                                    const DataTypes& argument_types,
                                                    const bool result_is_nullable,
                                                    const AggregateFunctionAttr& attr) {
    assert_arity_range(name, argument_types, 2, 2);

    DCHECK(argument_types[0]->get_primitive_type() == argument_types[1]->get_primitive_type());
    return creator_with_type_list<TYPE_DOUBLE>::create<AggregateFunctionBinary, CorrMomentStat>(
            argument_types, result_is_nullable, attr);
}

void register_aggregate_functions_corr(AggregateFunctionSimpleFactory& factory) {
    factory.register_function_both("corr", create_aggregate_corr_function);
}

template <PrimitiveType T>
using CorrWelfordMomentStat = StatFunc<T, CorrMomentWelford>;

AggregateFunctionPtr create_aggregate_corr_welford_function(const std::string& name,
                                                            const DataTypes& argument_types,
                                                            const bool result_is_nullable,
                                                            const AggregateFunctionAttr& attr) {
    assert_arity_range(name, argument_types, 2, 2);

    return creator_with_type_list<TYPE_DOUBLE>::create<AggregateFunctionBinary,
                                                       CorrWelfordMomentStat>(
            argument_types, result_is_nullable, attr);
}

void register_aggregate_functions_corr_welford(AggregateFunctionSimpleFactory& factory) {
    factory.register_function_both("corr_welford", create_aggregate_corr_welford_function);
}

} // namespace doris::vectorized
