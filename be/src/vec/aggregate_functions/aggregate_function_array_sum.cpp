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

#include "vec/aggregate_functions/aggregate_function_array_sum.h"

#include "aggregate_function.h"
#include "vec/aggregate_functions/factory_helpers.h"
#include "vec/aggregate_functions/helpers.h"

namespace doris::vectorized {

template <PrimitiveType T>
AggregateFunctionPtr create_aggregate_function_array_sum_impl(const DataTypes& argument_types,
                                                              const bool result_is_nullable,
                                                              const AggregateFunctionAttr& attr) {
    return creator_without_type::create<
            AggregateFunctionArraySum<AggregateFunctionArraySumData<T>>>(argument_types,
                                                                         result_is_nullable, attr);
}

AggregateFunctionPtr create_aggregate_function_array_sum(const std::string& name,
                                                         const DataTypes& argument_types,
                                                         const bool result_is_nullable,
                                                         const AggregateFunctionAttr& attr) {
    assert_arity_range(name, argument_types, 1, 1);
    auto argument_type = remove_nullable(argument_types[0]);
    if (argument_type->get_primitive_type() != TYPE_ARRAY) {
        LOG(WARNING) << fmt::format("Illegal argument type {} for aggregate function {}",
                                    argument_type->get_name(), name);
        return nullptr;
    }
    const auto& array_type = assert_cast<const DataTypeArray&>(*argument_type);
    const auto& nested_type = remove_nullable(array_type.get_nested_type());
    switch (nested_type->get_primitive_type()) {
#define ADD_TYPE(T)                                                                            \
    case T:                                                                                    \
        return create_aggregate_function_array_sum_impl<T>(argument_types, result_is_nullable, \
                                                           attr);
        ADD_TYPE(PrimitiveType::TYPE_TINYINT)
        ADD_TYPE(PrimitiveType::TYPE_SMALLINT)
        ADD_TYPE(PrimitiveType::TYPE_INT)
        ADD_TYPE(PrimitiveType::TYPE_BIGINT)
        ADD_TYPE(PrimitiveType::TYPE_LARGEINT)
        ADD_TYPE(PrimitiveType::TYPE_FLOAT)
        ADD_TYPE(PrimitiveType::TYPE_DOUBLE)
        ADD_TYPE(PrimitiveType::TYPE_DECIMAL32)
        ADD_TYPE(PrimitiveType::TYPE_DECIMAL64)
        ADD_TYPE(PrimitiveType::TYPE_DECIMAL128I)
        ADD_TYPE(PrimitiveType::TYPE_DECIMALV2)
#undef ADD_TYPE
    default:
        break;
    }
    LOG(WARNING) << fmt::format("unsupported input type {} for aggregate function {}",
                                argument_types[0]->get_name(), name);
    return nullptr;
}

void register_aggregate_function_array_sum(AggregateFunctionSimpleFactory& factory) {
    factory.register_function_both("agg_array_sum", create_aggregate_function_array_sum);
}
} // namespace doris::vectorized