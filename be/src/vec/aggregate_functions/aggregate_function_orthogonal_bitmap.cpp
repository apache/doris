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

#include "vec/aggregate_functions/aggregate_function_orthogonal_bitmap.h"

#include <memory>

#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/aggregate_functions/helpers.h"
#include "vec/data_types/data_type_string.h"

namespace doris::vectorized {

template <template <typename> class Impl>
AggregateFunctionPtr create_aggregate_function_orthogonal(const std::string& name,
                                                          const DataTypes& argument_types,
                                                          const Array& params,
                                                          const bool result_is_nullable) {
    if (argument_types.empty()) {
        LOG(WARNING) << "Incorrect number of arguments for aggregate function " << name;
        return nullptr;
    } else if (argument_types.size() == 1) {
        return std::make_shared<AggFunctionOrthBitmapFunc<Impl<StringValue>>>(argument_types);
    } else {
        const IDataType& argument_type = *argument_types[1];
        AggregateFunctionPtr res(create_with_numeric_type<AggFunctionOrthBitmapFunc, Impl>(
                argument_type, argument_types));

        WhichDataType which(argument_type);

        if (res) {
            return res;
        } else if (which.is_string_or_fixed_string()) {
            return std::make_shared<AggFunctionOrthBitmapFunc<Impl<std::string_view>>>(
                    argument_types);
        }
        LOG(WARNING) << "Incorrect Type " << argument_type.get_name()
                     << " of arguments for aggregate function " << name;
        return nullptr;
    }
}

AggregateFunctionPtr create_aggregate_function_orthogonal_bitmap_intersect(
        const std::string& name, const DataTypes& argument_types, const Array& parameters,
        bool result_is_nullable) {
    return create_aggregate_function_orthogonal<AggOrthBitMapIntersect>(
            name, argument_types, parameters, result_is_nullable);
}

AggregateFunctionPtr create_aggregate_function_orthogonal_bitmap_intersect_count(
        const std::string& name, const DataTypes& argument_types, const Array& parameters,
        bool result_is_nullable) {
    return create_aggregate_function_orthogonal<AggOrthBitMapIntersectCount>(
            name, argument_types, parameters, result_is_nullable);
}

AggregateFunctionPtr create_aggregate_function_intersect_count(const std::string& name,
                                                               const DataTypes& argument_types,
                                                               const Array& parameters,
                                                               bool result_is_nullable) {
    return create_aggregate_function_orthogonal<AggIntersectCount>(name, argument_types, parameters,
                                                                   result_is_nullable);
}

AggregateFunctionPtr create_aggregate_function_orthogonal_bitmap_union_count(
        const std::string& name, const DataTypes& argument_types, const Array& parameters,
        const bool result_is_nullable) {
    return create_aggregate_function_orthogonal<OrthBitmapUnionCountData>(
            name, argument_types, parameters, result_is_nullable);
}

void register_aggregate_function_orthogonal_bitmap(AggregateFunctionSimpleFactory& factory) {
    factory.register_function("orthogonal_bitmap_intersect",
                              create_aggregate_function_orthogonal_bitmap_intersect);

    factory.register_function("orthogonal_bitmap_intersect_count",
                              create_aggregate_function_orthogonal_bitmap_intersect_count);

    factory.register_function("orthogonal_bitmap_union_count",
                              create_aggregate_function_orthogonal_bitmap_union_count);

    factory.register_function("intersect_count", create_aggregate_function_intersect_count);
}
} // namespace doris::vectorized