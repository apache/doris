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

#include "exprs/function/cast/cast_to_timestamptz.h"

namespace doris::CastWrapper {
WrapperType create_timestamptz_wrapper(FunctionContext* context, const DataTypePtr& from_type) {
    std::shared_ptr<CastToBase> cast_to_timestamptz;

    auto make_timestamptz_wrapper = [&](const auto& types) -> bool {
        using Types = std::decay_t<decltype(types)>;
        using FromDataType = typename Types::LeftType;
        if constexpr (CastUtil::IsBaseCastFromType<FromDataType> ||
                      IsTimeStampTzType<FromDataType>) {
            if (context->enable_strict_mode()) {
                cast_to_timestamptz = std::make_shared<
                        CastToImpl<CastModeType::StrictMode, FromDataType, DataTypeTimeStampTz>>();
            } else {
                cast_to_timestamptz =
                        std::make_shared<CastToImpl<CastModeType::NonStrictMode, FromDataType,
                                                    DataTypeTimeStampTz>>();
            }
            return true;
        } else {
            return false;
        }
    };

    if (!call_on_index_and_data_type<void>(from_type->get_primitive_type(),
                                           make_timestamptz_wrapper)) {
        return create_unsupport_wrapper(
                fmt::format("CAST AS timestamptz not supported {}", from_type->get_name()));
    }

    return [cast_to_timestamptz](FunctionContext* context, Block& block,
                                 const ColumnNumbers& arguments, uint32_t result,
                                 size_t input_rows_count,
                                 const NullMap::value_type* null_map = nullptr) {
        return cast_to_timestamptz->execute_impl(context, block, arguments, result,
                                                 input_rows_count, null_map);
    };
}
} // namespace doris::CastWrapper
