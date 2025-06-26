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
#include <cstddef>
#include <cstdint>

#include "vec/core/block.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_date.h"
#include "vec/data_types/data_type_date_or_datetime_v2.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_ipv4.h"
#include "vec/data_types/data_type_ipv6.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/data_types/data_type_time.h"
#include "vec/functions/function.h"
#include "vec/functions/function_helpers.h"
namespace doris::vectorized {

struct NameCast {
    static constexpr auto name = "CAST";
};

struct PrecisionScaleArg {
    UInt32 precision;
    UInt32 scale;
};

namespace CastUtil {
// `static_cast_set` is introduced to wrap `static_cast` and handle special cases.
// Doris uses `uint8` to represent boolean values internally.
// Directly `static_cast` to `uint8` can result in non-0/1 values,
// To address this, `static_cast_set` performs an additional check:
//  For `uint8` types, it explicitly uses `static_cast<bool>` to ensure
//  the result is either 0 or 1.
template <typename FromFieldType, typename ToFieldType>
void static_cast_set(ToFieldType& to, const FromFieldType& from) {
    // uint8_t now use as boolean in doris
    if constexpr (std::is_same_v<uint8_t, ToFieldType>) {
        to = static_cast<bool>(from);
    } else {
        to = static_cast<ToFieldType>(from);
    }
}
template <typename T>
constexpr static bool is_signed_integer = false;
template <>
inline constexpr bool is_signed_integer<DataTypeInt8> = true;
template <>
inline constexpr bool is_signed_integer<DataTypeInt16> = true;
template <>
inline constexpr bool is_signed_integer<DataTypeInt32> = true;
template <>
inline constexpr bool is_signed_integer<DataTypeInt64> = true;
template <>
inline constexpr bool is_signed_integer<DataTypeInt128> = true;

template <typename T>
constexpr static bool is_bool = false;
template <>
inline constexpr bool is_bool<DataTypeUInt8> = true;

template <typename T>
constexpr static bool is_integer_or_bool = is_signed_integer<T> || is_bool<T>;

template <typename T>
constexpr static bool is_floating_point = false;
template <>
inline constexpr bool is_floating_point<DataTypeFloat32> = true;
template <>
inline constexpr bool is_floating_point<DataTypeFloat64> = true;

template <typename T>
constexpr static bool is_number = is_integer_or_bool<T> || is_floating_point<T>;

template <typename T>
constexpr static bool is_decimal = false;
template <>
inline constexpr bool is_decimal<DataTypeDecimal<TYPE_DECIMAL32>> = true;
template <>
inline constexpr bool is_decimal<DataTypeDecimal<TYPE_DECIMAL64>> = true;
template <>
inline constexpr bool is_decimal<DataTypeDecimal<TYPE_DECIMALV2>> = true;
template <>
inline constexpr bool is_decimal<DataTypeDecimal<TYPE_DECIMAL128I>> = true;
template <>
inline constexpr bool is_decimal<DataTypeDecimal<TYPE_DECIMAL256>> = true;

template <typename T>
constexpr static bool is_date_time = false;
template <>
inline constexpr bool is_date_time<DataTypeDate> = true;
template <>
inline constexpr bool is_date_time<DataTypeDateTime> = true;
template <>
inline constexpr bool is_date_time<DataTypeDateV2> = true;
template <>
inline constexpr bool is_date_time<DataTypeDateTimeV2> = true;
template <>
inline constexpr bool is_date_time<DataTypeTimeV2> = true;

template <typename T>
constexpr static bool is_ip = false;

template <>
inline constexpr bool is_ip<DataTypeIPv4> = true;
template <>
inline constexpr bool is_ip<DataTypeIPv6> = true;
template <typename T>
constexpr static bool is_string = false;
template <>
inline constexpr bool is_string<DataTypeString> = true;

template <typename T>
constexpr static bool is_base_cast_to_type = is_integer_or_bool<T> || is_floating_point<T> ||
                                             is_decimal<T> || is_date_time<T> || is_ip<T>;

template <typename T>
constexpr static bool is_base_cast_from_type = is_base_cast_to_type<T> || is_string<T>;

} // namespace CastUtil

namespace CastWrapper {

using WrapperType = std::function<Status(FunctionContext*, Block&, const ColumnNumbers&, uint32_t,
                                         size_t, const NullMap::value_type*)>;

using ElementWrappers = std::vector<WrapperType>;

WrapperType create_unsupport_wrapper(const String error_msg);

WrapperType create_unsupport_wrapper(const String from_type_name, const String to_type_name);
//// Generic conversion of any type to String.

Status cast_from_generic_to_jsonb(FunctionContext* context, Block& block,
                                  const ColumnNumbers& arguments, uint32_t result,
                                  size_t input_rows_count,
                                  const NullMap::value_type* null_map = nullptr);

Status cast_from_string_to_generic(FunctionContext* context, Block& block,
                                   const ColumnNumbers& arguments, uint32_t result,
                                   size_t input_rows_count,
                                   const NullMap::value_type* null_map = nullptr);

// prepare_unpack_dictionaries -> prepare_remove_nullable -> prepare_impl

WrapperType prepare_unpack_dictionaries(FunctionContext* context, const DataTypePtr& from_type,
                                        const DataTypePtr& to_type);

WrapperType prepare_remove_nullable(FunctionContext* context, const DataTypePtr& from_type,
                                    const DataTypePtr& to_type, bool skip_not_null_check);

WrapperType prepare_impl(FunctionContext* context, const DataTypePtr& from_type,
                         const DataTypePtr& to_type, bool requested_result_is_nullable);

ElementWrappers get_element_wrappers(FunctionContext* context, const DataTypes& from_element_types,
                                     const DataTypes& to_element_types);

WrapperType create_identity_wrapper(const DataTypePtr&);

WrapperType create_nothing_wrapper(const IDataType* to_type);

} // namespace CastWrapper

enum class CastModeType { StrictMode, NonStrictMode };

inline std::string cast_mode_type_to_string(CastModeType cast_mode) {
    switch (cast_mode) {
    case CastModeType::StrictMode:
        return "StrictMode";
    case CastModeType::NonStrictMode:
        return "NonStrictMode";
    default:
        return "Unknown";
    }
}
inline std::string cast_mode_type_to_string(CastModeType cast_mode, const DataTypePtr& from_type,
                                            const DataTypePtr& to_type) {
    return fmt::format("{}: from {} cast to {}", cast_mode_type_to_string(cast_mode),
                       from_type->get_name(), to_type->get_name());
}

class CastToBase {
public:
    virtual ~CastToBase() = default;
    virtual Status execute_impl(FunctionContext* context, Block& block,
                                const ColumnNumbers& arguments, uint32_t result,
                                size_t input_rows_count,
                                const NullMap::value_type* null_map = nullptr) const = 0;
};

template <CastModeType CastMode, typename FromDataType, typename ToDataType>
class CastToImpl : public CastToBase {
public:
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count,
                        const NullMap::value_type* null_map = nullptr) const override {
        return Status::RuntimeError(
                "not support  {} ",
                cast_mode_type_to_string(CastMode, block.get_by_position(arguments[0]).type,
                                         block.get_by_position(result).type));
    }
};

#ifdef BE_TEST
inline CastWrapper::WrapperType get_cast_wrapper(FunctionContext* context,
                                                 const DataTypePtr& from_type,
                                                 const DataTypePtr& to_type) {
    return CastWrapper::prepare_unpack_dictionaries(context, from_type, to_type);
}
#endif

} // namespace doris::vectorized