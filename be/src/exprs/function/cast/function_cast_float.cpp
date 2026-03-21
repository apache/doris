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

#include "core/data_type/data_type_number.h"
#include "exprs/function/cast/cast_to_float.h"

namespace doris::CastWrapper {

template <typename ToDataType>
WrapperType create_float_wrapper(FunctionContext* context, const DataTypePtr& from_type) {
    std::shared_ptr<CastToBase> cast_impl;

    auto make_cast_wrapper = [&](const auto& types) -> bool {
        using Types = std::decay_t<decltype(types)>;
        using FromDataType = typename Types::LeftType;
        if constexpr (type_allow_cast_to_basic_number<FromDataType>) {
            if (context->enable_strict_mode()) {
                cast_impl = std::make_shared<
                        CastToImpl<CastModeType::StrictMode, FromDataType, ToDataType>>();
            } else {
                cast_impl = std::make_shared<
                        CastToImpl<CastModeType::NonStrictMode, FromDataType, ToDataType>>();
            }
            return true;
        } else {
            return false;
        }
    };

    if (!call_on_index_and_data_type<void>(from_type->get_primitive_type(), make_cast_wrapper)) {
        return create_unsupport_wrapper(
                fmt::format("CAST AS number not supported {}", from_type->get_name()));
    }

    return [cast_impl](FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                       uint32_t result, size_t input_rows_count,
                       const NullMap::value_type* null_map = nullptr) {
        return cast_impl->execute_impl(context, block, arguments, result, input_rows_count,
                                       null_map);
    };
}

WrapperType create_float_wrapper(FunctionContext* context, const DataTypePtr& from_type,
                                 PrimitiveType to_type) {
    switch (to_type) {
    case TYPE_FLOAT:
        return create_float_wrapper<DataTypeFloat32>(context, from_type);
    case TYPE_DOUBLE:
        return create_float_wrapper<DataTypeFloat64>(context, from_type);
    default:
        return create_unsupport_wrapper(
                fmt::format("CAST AS float: unsupported to_type {}", type_to_string(to_type)));
    }
}

} // namespace doris::CastWrapper
