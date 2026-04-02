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

#pragma once

#include <glog/logging.h>

#include "core/data_type/define_primitive_type.h"
#include "core/data_type/primitive_type.h"
#include "exprs/aggregate/aggregate_function.h"
#include "exprs/aggregate/aggregate_function_group_array_set_op.h"
#include "exprs/aggregate/factory_helpers.h"
#include "exprs/aggregate/helpers.h"

namespace doris {
#include "common/compile_check_begin.h"

template <template <PrimitiveType> class ImplNumericData, typename ImplStringData>
inline AggregateFunctionPtr create_aggregate_function_group_array_impl(
        const DataTypes& argument_types, const bool result_is_nullable,
        const AggregateFunctionAttr& attr) {
    const auto& nested_type = remove_nullable(
            assert_cast<const DataTypeArray&>(*(argument_types[0])).get_nested_type());

    auto pt = nested_type->get_primitive_type();
    if (pt == PrimitiveType::TYPE_STRING || pt == PrimitiveType::TYPE_VARCHAR ||
        pt == PrimitiveType::TYPE_CHAR) {
        return creator_without_type::create<AggregateFunctionGroupArraySetOp<ImplStringData>>(
                argument_types, result_is_nullable, attr);
    }

    AggregateFunctionPtr result;
    dispatch_switch_scalar(pt, [&](auto type_holder) {
        using DT = std::decay_t<decltype(type_holder)>;
        result = creator_without_type::create<
                AggregateFunctionGroupArraySetOp<ImplNumericData<DT::PType>>>(
                argument_types, result_is_nullable, attr);
        return true;
    });
    if (!result) {
        LOG(WARNING) << " got invalid of nested type: " << nested_type->get_name();
    }
    return result;
}

} // namespace doris
#include "common/compile_check_end.h"
