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

#include "exprs/aggregate/aggregate_function_min_max_by.h"

#include "core/call_on_type_index.h"
#include "core/data_type/primitive_type.h"
#include "exprs/aggregate/aggregate_function_simple_factory.h"

namespace doris {
#include "common/compile_check_begin.h"
std::unique_ptr<MaxMinValueBase> create_max_min_value(const DataTypePtr& type, int be_version) {
    std::unique_ptr<MaxMinValueBase> result;
    auto call = [&](const auto& dispatch_type) -> bool {
        using DispatchType = std::decay_t<decltype(dispatch_type)>;
        constexpr auto PT = DispatchType::PType;
        if constexpr (is_decimal(PT)) {
            result = std::make_unique<MaxMinValue<SingleValueDataDecimal<PT>>>();
        } else {
            result = std::make_unique<MaxMinValue<SingleValueDataFixed<PT>>>();
        }
        return true;
    };
    if (dispatch_switch_scalar(type->get_primitive_type(), call)) {
        return result;
    }
    switch (type->get_primitive_type()) {
    case PrimitiveType::TYPE_STRING:
    case PrimitiveType::TYPE_CHAR:
    case PrimitiveType::TYPE_VARCHAR:
        return std::make_unique<MaxMinValue<SingleValueDataString>>();
    case PrimitiveType::TYPE_BITMAP:
        return std::make_unique<MaxMinValue<BitmapValueData>>();
    case PrimitiveType::TYPE_ARRAY:
    case PrimitiveType::TYPE_MAP:
    case PrimitiveType::TYPE_STRUCT:
        return std::make_unique<MaxMinValue<SingleValueDataComplexType>>(DataTypes {type},
                                                                         be_version);
    default:
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "Illegal type {} of argument of aggregate function min/max_by",
                               type->get_name());
        return nullptr;
    }
}

void register_aggregate_function_max_min_by(AggregateFunctionSimpleFactory& factory) {
    factory.register_function_both(
            "min_by", create_aggregate_function_min_max_by<AggregateFunctionMinByData>);
    factory.register_function_both(
            "max_by", create_aggregate_function_min_max_by<AggregateFunctionMaxByData>);
}

} // namespace doris

#include "common/compile_check_end.h"
