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

#include "exprs/aggregate/aggregate_function_datasketches_hll_union_agg.h"

#include <string>

#include "core/data_type/data_type.h"
#include "core/data_type/define_primitive_type.h"
#include "exec/common/hash_table/hash.h" // IWYU pragma: keep
#include "exprs/aggregate/aggregate_function_simple_factory.h"
#include "exprs/aggregate/helpers.h"
namespace doris {
template <template <PrimitiveType> class Data>
AggregateFunctionPtr create_aggregate_function_datasketches_hll_union_agg(
        const std::string& name, const DataTypes& argument_types, const DataTypePtr& result_type,
        const bool result_is_nullable, const AggregateFunctionAttr& attr) {
    return creator_with_type_list<TYPE_STRING, TYPE_VARCHAR, TYPE_VARBINARY>::create<
            AggregateFunctionDataSketchesHllUnionAgg, Data>(argument_types, result_is_nullable,
                                                            attr);
}
void register_aggregate_function_datasketches_HLL_union_agg(
        AggregateFunctionSimpleFactory& factory) {
    AggregateFunctionCreator creator =
            create_aggregate_function_datasketches_hll_union_agg<AggregateFunctionHllSketchData>;
    factory.register_function_both("datasketches_hll_union_agg", creator);
    factory.register_alias("datasketches_hll_union_agg", "ds_hll_estimate");
    factory.register_alias("datasketches_hll_union_agg", "datasketches_hll_estimate");
}
} // namespace doris
