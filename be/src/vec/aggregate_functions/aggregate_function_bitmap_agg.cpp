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

#include "vec/aggregate_functions/aggregate_function_bitmap_agg.h"

#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/aggregate_functions/helpers.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_nullable.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

template <bool nullable>
AggregateFunctionPtr create_with_int_data_type(const DataTypes& argument_types) {
    switch (argument_types[0]->get_primitive_type()) {
    case PrimitiveType::TYPE_TINYINT:
        return std::make_shared<AggregateFunctionBitmapAgg<nullable, Int8>>(argument_types);
    case PrimitiveType::TYPE_SMALLINT:
        return std::make_shared<AggregateFunctionBitmapAgg<nullable, Int16>>(argument_types);
    case PrimitiveType::TYPE_INT:
        return std::make_shared<AggregateFunctionBitmapAgg<nullable, Int32>>(argument_types);
    case PrimitiveType::TYPE_BIGINT:
        return std::make_shared<AggregateFunctionBitmapAgg<nullable, Int64>>(argument_types);
    default:
        LOG(WARNING) << "with unknown type, failed in create_with_int_data_type bitmap_union_int"
                     << " and type is: " << argument_types[0]->get_name();
        return nullptr;
    }
}

AggregateFunctionPtr create_aggregate_function_bitmap_agg(const std::string& name,
                                                          const DataTypes& argument_types,
                                                          const bool result_is_nullable,
                                                          const AggregateFunctionAttr& attr) {
    const bool arg_is_nullable = argument_types[0]->is_nullable();
    if (arg_is_nullable) {
        return AggregateFunctionPtr(create_with_int_data_type<true>(argument_types));
    } else {
        return AggregateFunctionPtr(create_with_int_data_type<false>(argument_types));
    }
}

void register_aggregate_function_bitmap_agg(AggregateFunctionSimpleFactory& factory) {
    factory.register_function_both("bitmap_agg", create_aggregate_function_bitmap_agg);
}
} // namespace doris::vectorized