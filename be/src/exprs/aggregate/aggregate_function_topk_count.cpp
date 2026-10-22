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

#include "exprs/aggregate/aggregate_function_topk_count.h"

#include "core/data_type/define_primitive_type.h"
#include "exprs/aggregate/aggregate_function_simple_factory.h"
#include "exprs/aggregate/factory_helpers.h"
#include "exprs/aggregate/helpers.h"

namespace doris {

namespace {

template <PrimitiveType T>
using TopKMinCount = AggregateFunctionTopKCount<T, true>;

template <PrimitiveType T>
using TopKMaxCount = AggregateFunctionTopKCount<T, false>;

/// Constructs the aggregate directly instead of using creator_without_type, which would wrap it in
/// a nullable-argument adapter. That adapter drops every row where any argument is NULL *before*
/// add() runs, so an evaluated-but-NULL count would never be validated and the group would
/// silently return an empty state. The function reads k itself for every row, including rows
/// whose x is NULL.
template <template <PrimitiveType> class FunctionTemplate, PrimitiveType... AllowedTypes>
struct TopKCountCreator {
    static AggregateFunctionPtr create(PrimitiveType type, const DataTypes& argument_types) {
        AggregateFunctionPtr result;
        (([&] {
             if (type == AllowedTypes) {
                 result = std::make_shared<FunctionTemplate<AllowedTypes>>(argument_types);
             }
         }()),
         ...);
        return result;
    }
};

// Keep in sync with TopKCountFunction.isSupportedValueType on the FE side.
template <template <PrimitiveType> class FunctionTemplate>
using TopKCount = TopKCountCreator<FunctionTemplate, TYPE_BOOLEAN, TYPE_TINYINT, TYPE_SMALLINT,
                                   TYPE_INT, TYPE_BIGINT, TYPE_LARGEINT, TYPE_FLOAT, TYPE_DOUBLE,
                                   TYPE_DECIMAL32, TYPE_DECIMAL64, TYPE_DECIMALV2, TYPE_DECIMAL128I,
                                   TYPE_DECIMAL256, TYPE_VARCHAR, TYPE_DATE, TYPE_DATETIME,
                                   TYPE_DATEV2, TYPE_DATETIMEV2, TYPE_TIMEV2, TYPE_TIMESTAMP_NS,
                                   TYPE_TIMESTAMPTZ, TYPE_IPV4, TYPE_IPV6, TYPE_UUID>;

template <template <PrimitiveType> class FunctionTemplate>
AggregateFunctionPtr create_topk_count(const std::string& name, const DataTypes& argument_types,
                                       const bool result_is_nullable,
                                       const AggregateFunctionAttr& attr) {
    // The function always returns a non-nullable array, the same contract the generic nullable
    // adapter enforced.
    if (!attr.is_foreach && result_is_nullable) {
        throw Exception(ErrorCode::INTERNAL_ERROR, "{} must return a non-nullable array", name);
    }
    auto type = argument_types[0]->get_primitive_type();
    if (type == TYPE_CHAR || type == TYPE_STRING || type == TYPE_JSONB) {
        type = TYPE_VARCHAR;
    }
    return TopKCount<FunctionTemplate>::create(type, argument_types);
}

AggregateFunctionPtr create_aggregate_function_topk_min_count(const std::string& name,
                                                              const DataTypes& argument_types,
                                                              const DataTypePtr& result_type,
                                                              const bool result_is_nullable,
                                                              const AggregateFunctionAttr& attr) {
    assert_arity_range(name, argument_types, 2, 2);
    return create_topk_count<TopKMinCount>(name, argument_types, result_is_nullable, attr);
}

AggregateFunctionPtr create_aggregate_function_topk_max_count(const std::string& name,
                                                              const DataTypes& argument_types,
                                                              const DataTypePtr& result_type,
                                                              const bool result_is_nullable,
                                                              const AggregateFunctionAttr& attr) {
    assert_arity_range(name, argument_types, 2, 2);
    return create_topk_count<TopKMaxCount>(name, argument_types, result_is_nullable, attr);
}

} // namespace

void register_aggregate_function_topk_count(AggregateFunctionSimpleFactory& factory) {
    factory.register_function_both("topk_min_count", create_aggregate_function_topk_min_count);
    factory.register_function_both("topk_max_count", create_aggregate_function_topk_max_count);
}

} // namespace doris
