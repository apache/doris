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

#include "cast_parameters.h"
#include "vec/core/block.h"
#include "vec/core/call_on_type_index.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_bitmap.h"
#include "vec/data_types/data_type_date.h"
#include "vec/data_types/data_type_date_or_datetime_v2.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_hll.h"
#include "vec/data_types/data_type_ipv4.h"
#include "vec/data_types/data_type_ipv6.h"
#include "vec/data_types/data_type_jsonb.h"
#include "vec/data_types/data_type_map.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/data_types/data_type_struct.h"
#include "vec/data_types/data_type_time.h"
#include "vec/functions/function.h"
#include "vec/functions/function_helpers.h"
#include "vec/io/io_helper.h"

namespace doris::vectorized {

struct NameCast {
    static constexpr auto name = "CAST";
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
constexpr static bool IsPureDigitType =
        IsDataTypeInt<T> || IsDataTypeFloat<T> || IsDataTypeDecimal<T>;

// IsDataTypeNumber include integer, float and boolean.
template <typename T>
constexpr static bool IsBaseCastToType =
        IsDataTypeNumber<T> || IsDataTypeDecimal<T> || IsDatelikeTypes<T> || IsIPType<T>;

template <typename T>
constexpr static bool IsBaseCastFromType = IsBaseCastToType<T> || IsStringType<T>;

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

// string to bitmap or hll object
Status cast_from_string_to_generic(FunctionContext* context, Block& block,
                                   const ColumnNumbers& arguments, uint32_t result,
                                   size_t input_rows_count,
                                   const NullMap::value_type* null_map = nullptr);

Status cast_from_string_to_complex_type(FunctionContext* context, Block& block,
                                        const ColumnNumbers& arguments, uint32_t result,
                                        size_t input_rows_count,
                                        const NullMap::value_type* null_map = nullptr);

Status cast_from_string_to_complex_type_strict_mode(FunctionContext* context, Block& block,
                                                    const ColumnNumbers& arguments, uint32_t result,
                                                    size_t input_rows_count,
                                                    const NullMap::value_type* null_map = nullptr);

// prepare_unpack_dictionaries -> prepare_remove_nullable -> prepare_impl

WrapperType prepare_unpack_dictionaries(FunctionContext* context, const DataTypePtr& from_type,
                                        const DataTypePtr& to_type);

WrapperType prepare_remove_nullable(FunctionContext* context, const DataTypePtr& from_type,
                                    const DataTypePtr& to_type);

WrapperType prepare_impl(FunctionContext* context, const DataTypePtr& from_type,
                         const DataTypePtr& to_type);

ElementWrappers get_element_wrappers(FunctionContext* context, const DataTypes& from_element_types,
                                     const DataTypes& to_element_types);

WrapperType create_identity_wrapper(const DataTypePtr&);

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
