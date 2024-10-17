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

#include <map>
#include <ostream>

#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/aggregate_functions/helpers.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_nullable.h"

namespace doris {
struct StringRef;
} // namespace doris

namespace doris::vectorized {

template <template <typename> class Impl>
AggregateFunctionPtr create_aggregate_function_orthogonal(const std::string& name,
                                                          const DataTypes& argument_types,

                                                          const bool result_is_nullable,
                                                          const AggregateFunctionAttr& attr) {
    if (argument_types.empty()) {
        LOG(WARNING) << "Incorrect number of arguments for aggregate function " << name;
        return nullptr;
    } else if (argument_types.size() == 1) {
        return creator_without_type::create<AggFunctionOrthBitmapFunc<Impl<StringRef>>>(
                argument_types, result_is_nullable);
    } else {
        WhichDataType which(*remove_nullable(argument_types[1]));

        AggregateFunctionPtr res(
                creator_with_type_base<true, true, false, 1>::create<AggFunctionOrthBitmapFunc,
                                                                     Impl>(argument_types,
                                                                           result_is_nullable));
        if (res) {
            return res;
        } else if (which.is_string_or_fixed_string()) {
            res = creator_without_type::create<AggFunctionOrthBitmapFunc<Impl<std::string_view>>>(
                    argument_types, result_is_nullable);
            return res;
        }

        const IDataType& argument_type = *argument_types[1];
        LOG(WARNING) << "Incorrect Type " << argument_type.get_name()
                     << " of arguments for aggregate function " << name;
        return nullptr;
    }
}

void register_aggregate_function_orthogonal_bitmap(AggregateFunctionSimpleFactory& factory) {
    factory.register_function_both("orthogonal_bitmap_intersect",
                                   create_aggregate_function_orthogonal<AggOrthBitMapIntersect>);
    factory.register_function_both(
            "orthogonal_bitmap_intersect_count",
            create_aggregate_function_orthogonal<AggOrthBitMapIntersectCount>);
    factory.register_function_both("orthogonal_bitmap_union_count",
                                   create_aggregate_function_orthogonal<OrthBitmapUnionCountData>);
    factory.register_function_both("intersect_count",
                                   create_aggregate_function_orthogonal<AggIntersectCount>);
    factory.register_function_both("orthogonal_bitmap_expr_calculate",
                                   create_aggregate_function_orthogonal<AggOrthBitMapExprCal>);
    factory.register_function_both("orthogonal_bitmap_expr_calculate_count",
                                   create_aggregate_function_orthogonal<AggOrthBitMapExprCalCount>);
}

} // namespace doris::vectorized
