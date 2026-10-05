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
#include "exprs/aggregate/aggregate_function_orthogonal_bitmap.h"
#include "exprs/aggregate/helpers.h"

namespace doris {

// FE guarantees exactly 3 params: (bitmap, varchar_filter, varchar_expr)
AggregateFunctionPtr create_aggregate_function_orth_bitmap_expr_cal_count(
        const std::string& name, const DataTypes& argument_types, const DataTypePtr& result_type,
        const bool result_is_nullable, const AggregateFunctionAttr& attr) {
    if (argument_types.size() != 3) {
        LOG(WARNING) << "Incorrect number of arguments for aggregate function " << name;
        return nullptr;
    }
    return creator_without_type::create<
            AggFunctionOrthBitmapFunc<AggOrthBitMapExprCalCount<TYPE_STRING>, MultiExpression>>(
            argument_types, result_is_nullable, attr);
}

} // namespace doris
