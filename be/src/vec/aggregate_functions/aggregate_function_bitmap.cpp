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

#include "vec/aggregate_functions/aggregate_function_bitmap.h"

#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/aggregate_functions/helpers.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_nullable.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

template <bool nullable, template <bool, typename> class AggregateFunctionTemplate>
AggregateFunctionPtr create_with_int_data_type(const DataTypes& argument_type) {
    auto type = remove_nullable(argument_type[0]);
    WhichDataType which(type);
#define DISPATCH(TYPE)                                                                    \
    if (which.idx == TypeIndex::TYPE) {                                                   \
        return std::make_shared<AggregateFunctionTemplate<nullable, ColumnVector<TYPE>>>( \
                argument_type);                                                           \
    }
    // Keep consistent with the FE definition; the function does not have an int128 type.
    DISPATCH(Int8)
    DISPATCH(Int16)
    DISPATCH(Int32)
    DISPATCH(Int64)
#undef DISPATCH
    LOG(WARNING) << "with unknowed type, failed in create_with_int_data_type bitmap_union_int"
                 << " and type is: " << argument_type[0]->get_name();
    return nullptr;
}

AggregateFunctionPtr create_aggregate_function_bitmap_union_count(
        const std::string& name, const DataTypes& argument_types, const bool result_is_nullable,
        const AggregateFunctionAttr& attr) {
    const bool arg_is_nullable = argument_types[0]->is_nullable();
    if (arg_is_nullable) {
        return std::make_shared<AggregateFunctionBitmapCount<true, ColumnBitmap>>(argument_types);
    } else {
        return std::make_shared<AggregateFunctionBitmapCount<false, ColumnBitmap>>(argument_types);
    }
}

AggregateFunctionPtr create_aggregate_function_bitmap_union_int(const std::string& name,
                                                                const DataTypes& argument_types,
                                                                const bool result_is_nullable,
                                                                const AggregateFunctionAttr& attr) {
    const bool arg_is_nullable = argument_types[0]->is_nullable();
    if (arg_is_nullable) {
        return AggregateFunctionPtr(
                create_with_int_data_type<true, AggregateFunctionBitmapCount>(argument_types));
    } else {
        return AggregateFunctionPtr(
                create_with_int_data_type<false, AggregateFunctionBitmapCount>(argument_types));
    }
}

void register_aggregate_function_bitmap(AggregateFunctionSimpleFactory& factory) {
    factory.register_function_both(
            "bitmap_union", creator_without_type::creator<
                                    AggregateFunctionBitmapOp<AggregateFunctionBitmapUnionOp>>);
    factory.register_function_both(
            "bitmap_intersect",
            creator_without_type::creator<
                    AggregateFunctionBitmapOp<AggregateFunctionBitmapIntersectOp>>);
    factory.register_function_both(
            "group_bitmap_xor",
            creator_without_type::creator<
                    AggregateFunctionBitmapOp<AggregateFunctionGroupBitmapXorOp>>);
    factory.register_function_both("bitmap_union_count",
                                   create_aggregate_function_bitmap_union_count);
    factory.register_function_both("bitmap_union_int", create_aggregate_function_bitmap_union_int);
}
} // namespace doris::vectorized