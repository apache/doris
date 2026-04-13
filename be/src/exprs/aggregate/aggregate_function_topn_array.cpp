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

#include "core/data_type/data_type.h"
#include "core/data_type/define_primitive_type.h"
#include "exprs/aggregate/aggregate_function_topn.h"
#include "exprs/aggregate/helpers.h"

namespace doris {

template <PrimitiveType T>
using ImplArray = AggregateFunctionTopNImplArray<T, false>;
template <PrimitiveType T>
using ImplArrayWithDefault = AggregateFunctionTopNImplArray<T, true>;

using topn_array_creator =
        creator_with_type_list<TYPE_TINYINT, TYPE_SMALLINT, TYPE_INT, TYPE_BIGINT, TYPE_LARGEINT,
                               TYPE_FLOAT, TYPE_DOUBLE, TYPE_DECIMAL32, TYPE_DECIMAL64,
                               TYPE_DECIMAL128I, TYPE_DECIMAL256, TYPE_VARCHAR, TYPE_DATEV2,
                               TYPE_DATETIMEV2, TYPE_TIMESTAMPTZ, TYPE_IPV4, TYPE_IPV6>;

AggregateFunctionPtr create_aggregate_function_topn_array(const std::string& name,
                                                          const DataTypes& argument_types,
                                                          const DataTypePtr& result_type,
                                                          const bool result_is_nullable,
                                                          const AggregateFunctionAttr& attr) {
    bool has_default_param = (argument_types.size() == 3);
    if (has_default_param) {
        return topn_array_creator::create<AggregateFunctionTopNArray, ImplArrayWithDefault>(
                argument_types, result_is_nullable, attr);
    } else {
        return topn_array_creator::create<AggregateFunctionTopNArray, ImplArray>(
                argument_types, result_is_nullable, attr);
    }
}

} // namespace doris
