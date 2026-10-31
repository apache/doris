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
WrapperType create_timestamptz_wrapper(const CastOptions& options, const DataTypePtr& from_type) {
    std::shared_ptr<CastToBase> cast_to_timestamptz;

    auto make_timestamptz_wrapper = [&](const auto& types) -> bool {
        using Types = std::decay_t<decltype(types)>;
        using FromDataType = typename Types::LeftType;
        if constexpr (CastUtil::IsBaseCastFromType<FromDataType> ||
                      IsTimeStampTzType<FromDataType> ||
                      std::is_same_v<FromDataType, DataTypeTimeStampNs>) {
            if (options.strict_mode) {
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

    return [cast_to_timestamptz](RuntimeState* state, const ColumnWithTypeAndName& source_column,
                                 ColumnWithTypeAndName& target_column, size_t input_rows_count,
                                 const NullMap::value_type* null_map = nullptr) {
        return cast_to_timestamptz->execute_impl(state, source_column, target_column,
                                                 input_rows_count, null_map);
    };
}
} // namespace doris::CastWrapper
