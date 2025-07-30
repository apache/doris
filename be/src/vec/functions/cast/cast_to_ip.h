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

#include "cast_base.h"
#include "runtime/primitive_type.h"
#include "vec/columns/column_nullable.h"
#include "vec/data_types/data_type_ipv4.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

struct CastToIPv4 {
    static bool from_string(const StringRef& from, IPv4& to, CastParameters&);
};

inline bool CastToIPv4::from_string(const StringRef& from, IPv4& to, CastParameters&) {
    return IPv4Value::from_string(to, from.data, from.size);
}

struct CastToIPv6 {
    static bool from_string(const StringRef& from, IPv6& to, CastParameters&);
    static bool from_ipv4(const IPv4& from, IPv6& to, CastParameters&);
};

inline bool CastToIPv6::from_string(const StringRef& from, IPv6& to, CastParameters&) {
    return IPv6Value::from_string(to, from.data, from.size);
}

inline bool CastToIPv6::from_ipv4(const IPv4& from, IPv6& to, CastParameters&) {
    map_ipv4_to_ipv6(from, reinterpret_cast<UInt8*>(&to));
    return true;
}

template <CastModeType Mode, typename IpDataType>
    requires(IsIPType<IpDataType>)
class CastToImpl<Mode, DataTypeString, IpDataType> : public CastToBase {
public:
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count,
                        const NullMap::value_type* null_map = nullptr) const override {
        const auto* col_from = check_and_get_column<DataTypeString::ColumnType>(
                block.get_by_position(arguments[0]).column.get());

        auto to_type = block.get_by_position(result).type;
        auto serde = remove_nullable(to_type)->get_serde();
        MutableColumnPtr column_to;

        if constexpr (Mode == CastModeType::NonStrictMode) {
            auto to_nullable_type = make_nullable(to_type);
            column_to = to_nullable_type->create_column();
            auto& nullable_col_to = assert_cast<ColumnNullable&>(*column_to);
            RETURN_IF_ERROR(serde->from_string_batch(*col_from, nullable_col_to, {}));
        } else if constexpr (Mode == CastModeType::StrictMode) {
            if (to_type->is_nullable()) {
                return Status::InternalError(
                        "result type should be not nullable when casting string to ip in "
                        "strict cast mode");
            }
            column_to = to_type->create_column();
            RETURN_IF_ERROR(
                    serde->from_string_strict_mode_batch(*col_from, *column_to, {}, null_map));
        } else {
            return Status::InternalError("Unsupported cast mode");
        }

        block.get_by_position(result).column = std::move(column_to);
        return Status::OK();
    }
};

template <CastModeType AllMode>
class CastToImpl<AllMode, DataTypeIPv4, DataTypeIPv6> : public CastToBase {
public:
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count,
                        const NullMap::value_type* null_map = nullptr) const override {
        const auto* col_from = check_and_get_column<DataTypeIPv4::ColumnType>(
                block.get_by_position(arguments[0]).column.get());
        const auto size = col_from->size();
        auto col_to = DataTypeIPv6::ColumnType::create(size);
        auto& to_data = col_to->get_data();
        const auto& from_data = col_from->get_data();
        CastParameters params;
        params.is_strict = (AllMode == CastModeType::StrictMode);

        for (size_t i = 0; i < size; ++i) {
            CastToIPv6::from_ipv4(from_data[i], to_data[i], params);
        }

        block.get_by_position(result).column = std::move(col_to);
        return Status::OK();
    }
};

namespace CastWrapper {

template <typename IpType>
    requires(std::is_same_v<IpType, DataTypeIPv4> || std::is_same_v<IpType, DataTypeIPv6>)
WrapperType create_ip_wrapper(FunctionContext* context, const DataTypePtr& from_type) {
    std::shared_ptr<CastToBase> cast_to_ip;

    auto make_ip_wrapper = [&](const auto& types) -> bool {
        using Types = std::decay_t<decltype(types)>;
        using FromDataType = typename Types::LeftType;
        if constexpr (IsDataTypeNumber<FromDataType> || IsStringType<FromDataType> ||
                      IsIPType<FromDataType>) {
            if (context->enable_strict_mode()) {
                cast_to_ip = std::make_shared<
                        CastToImpl<CastModeType::StrictMode, FromDataType, IpType>>();
            } else {
                cast_to_ip = std::make_shared<
                        CastToImpl<CastModeType::NonStrictMode, FromDataType, IpType>>();
            }
            return true;
        } else {
            return false;
        }
    };

    if (!call_on_index_and_data_type<void>(from_type->get_primitive_type(), make_ip_wrapper)) {
        return create_unsupport_wrapper(
                fmt::format("CAST AS ip not supported {}", from_type->get_name()));
    }

    return [cast_to_ip](FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count,
                        const NullMap::value_type* null_map = nullptr) {
        return cast_to_ip->execute_impl(context, block, arguments, result, input_rows_count,
                                        null_map);
    };
}
#include "common/compile_check_end.h"
}; // namespace CastWrapper

} // namespace doris::vectorized