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

#include "core/data_type/data_type_date.h"
#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type/data_type_date_time.h"
#include "core/data_type/data_type_time.h"
#include "exprs/function/cast/cast_to_date.h"

namespace doris::CastWrapper {

template <typename ToDataType> // must datelike type
WrapperType create_datelike_wrapper(FunctionContext* context, const DataTypePtr& from_type) {
    std::shared_ptr<CastToBase> cast_to_datelike;

    auto make_datelike_wrapper = [&](const auto& types) -> bool {
        using Types = std::decay_t<decltype(types)>;
        using FromDataType = typename Types::LeftType;
        if constexpr (CastUtil::IsPureDigitType<FromDataType> || IsDatelikeTypes<FromDataType> ||
                      IsStringType<FromDataType> ||
                      std::is_same_v<FromDataType, DataTypeTimeStampTz>) {
            if (context->enable_strict_mode()) {
                cast_to_datelike = std::make_shared<
                        CastToImpl<CastModeType::StrictMode, FromDataType, ToDataType>>();
            } else {
                cast_to_datelike = std::make_shared<
                        CastToImpl<CastModeType::NonStrictMode, FromDataType, ToDataType>>();
            }
            return true;
        } else {
            return false;
        }
    };

    if (!call_on_index_and_data_type<void>(from_type->get_primitive_type(),
                                           make_datelike_wrapper)) {
        return create_unsupport_wrapper(fmt::format(
                "CAST AS {} not supported {}", ToDataType {}.get_name(), from_type->get_name()));
    }

    return [cast_to_datelike](FunctionContext* context, Block& block,
                              const ColumnNumbers& arguments, const uint32_t result,
                              size_t input_rows_count,
                              const NullMap::value_type* null_map = nullptr) {
        return cast_to_datelike->execute_impl(context, block, arguments, result, input_rows_count,
                                              null_map);
    };
}

WrapperType create_datelike_wrapper(FunctionContext* context, const DataTypePtr& from_type,
                                    PrimitiveType to_type) {
    switch (to_type) {
    case TYPE_DATE:
        return create_datelike_wrapper<DataTypeDate>(context, from_type);
    case TYPE_DATETIME:
        return create_datelike_wrapper<DataTypeDateTime>(context, from_type);
    case TYPE_DATEV2:
        return create_datelike_wrapper<DataTypeDateV2>(context, from_type);
    case TYPE_DATETIMEV2:
        return create_datelike_wrapper<DataTypeDateTimeV2>(context, from_type);
    case TYPE_TIMEV2:
        return create_datelike_wrapper<DataTypeTimeV2>(context, from_type);
    default:
        return create_unsupport_wrapper(
                fmt::format("CAST AS date: unsupported to_type {}", type_to_string(to_type)));
    }
}

} // namespace doris::CastWrapper
