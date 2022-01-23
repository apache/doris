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

namespace doris::vectorized {

template <bool nullable, template <bool, typename> class AggregateFunctionTemplate>
static IAggregateFunction* createWithIntDataType(const DataTypes& argument_type) {
    auto type = argument_type[0].get();
    if (type->is_nullable()) {
        type = assert_cast<const DataTypeNullable*>(type)->get_nested_type().get();
    }
    WhichDataType which(type);
    if (which.idx == TypeIndex::Int8)
        return new AggregateFunctionTemplate<nullable, ColumnVector<Int8>>(argument_type);
    if (which.idx == TypeIndex::Int16)
        return new AggregateFunctionTemplate<nullable, ColumnVector<Int16>>(argument_type);
    if (which.idx == TypeIndex::Int32)
        return new AggregateFunctionTemplate<nullable, ColumnVector<Int32>>(argument_type);
    if (which.idx == TypeIndex::Int64)
        return new AggregateFunctionTemplate<nullable, ColumnVector<Int64>>(argument_type);
    return nullptr;
}

AggregateFunctionPtr create_aggregate_function_bitmap_union(const std::string& name,
                                                            const DataTypes& argument_types,
                                                            const Array& parameters,
                                                            const bool result_is_nullable) {
    return std::make_shared<AggregateFunctionBitmapOp<AggregateFunctionBitmapUnionOp>>(
            argument_types);
}

AggregateFunctionPtr create_aggregate_function_bitmap_intersect(const std::string& name,
                                                                const DataTypes& argument_types,
                                                                const Array& parameters,
                                                                const bool result_is_nullable) {
    return std::make_shared<AggregateFunctionBitmapOp<AggregateFunctionBitmapIntersectOp>>(
            argument_types);
}
template <bool nullable>
AggregateFunctionPtr create_aggregate_function_bitmap_union_count(const std::string& name,
                                                                  const DataTypes& argument_types,
                                                                  const Array& parameters,
                                                                  const bool result_is_nullable) {
    return std::make_shared<AggregateFunctionBitmapCount<nullable, ColumnBitmap>>(argument_types);
}

template <bool nullable>
AggregateFunctionPtr create_aggregate_function_bitmap_union_int(const std::string& name,
                                                                const DataTypes& argument_types,
                                                                const Array& parameters,
                                                                const bool result_is_nullable) {
    return std::shared_ptr<IAggregateFunction>(
            createWithIntDataType<nullable, AggregateFunctionBitmapCount>(argument_types));
}

void register_aggregate_function_bitmap(AggregateFunctionSimpleFactory& factory) {
    factory.register_function("bitmap_union", create_aggregate_function_bitmap_union);
    factory.register_function("bitmap_intersect", create_aggregate_function_bitmap_intersect);
    factory.register_function("bitmap_union_count",
                              create_aggregate_function_bitmap_union_count<false>);
    factory.register_function("bitmap_union_count",
                              create_aggregate_function_bitmap_union_count<true>, true);

    factory.register_function("bitmap_union_int",
                              create_aggregate_function_bitmap_union_int<false>);
    factory.register_function("bitmap_union_int", create_aggregate_function_bitmap_union_int<true>,
                              true);
}
} // namespace doris::vectorized