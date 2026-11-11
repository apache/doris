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
#include <functional>
#include <type_traits>
#include <utility>

#include "core/assert_cast.h"
#include "core/block/column_with_type_and_name.h"
#include "core/call_on_type_index.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_bitmap.h"
#include "core/data_type/data_type_date.h"
#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type/data_type_date_time.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_hll.h"
#include "core/data_type/data_type_ipv4.h"
#include "core/data_type/data_type_ipv6.h"
#include "core/data_type/data_type_jsonb.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type/data_type_time.h"
#include "exprs/function/cast/cast_parameters.h"
#include "exprs/function/function.h"
#include "exprs/function/function_helpers.h"

namespace doris {

class RuntimeState;

struct CastOptions {
    bool strict_mode = false;
    bool check_overflow_for_decimal = false;
    bool string_as_jsonb_string = false;
    bool jsonb_string_as_string = false;
};

enum class DatelikeParseMode {
    NON_STRICT,
    STRICT,
};

constexpr bool is_datelike_parse_strict(DatelikeParseMode parse_mode) {
    return parse_mode == DatelikeParseMode::STRICT;
}

enum class DatelikeTargetType {
    DATE,
    DATE_TIME,
};

constexpr bool is_datelike_target_datetime(DatelikeTargetType target_type) {
    return target_type == DatelikeTargetType::DATE_TIME;
}

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

inline ColumnNullable::MutablePtr create_empty_nullable_column(const DataTypePtr& nested_type) {
    // Batch serde paths resize the nested column and null map together.
    return ColumnNullable::create(remove_nullable(nested_type)->create_column(),
                                  ColumnUInt8::create());
}

namespace CastWrapper {

using WrapperType =
        std::function<Status(RuntimeState*, const ColumnWithTypeAndName&, ColumnWithTypeAndName&,
                             size_t, const NullMap::value_type*)>;
using FunctionContextWrapperType =
        std::function<Status(FunctionContext*, const ColumnWithTypeAndName&, ColumnWithTypeAndName&,
                             size_t, const NullMap::value_type*)>;

using ElementWrappers = std::vector<WrapperType>;

WrapperType create_unsupport_wrapper(const String error_msg);

WrapperType create_unsupport_wrapper(const String from_type_name, const String to_type_name);
//// Generic conversion of any type to String.

Status cast_from_generic_to_jsonb(RuntimeState* state, const ColumnWithTypeAndName& source_column,
                                  ColumnWithTypeAndName& target_column, size_t input_rows_count,
                                  const NullMap::value_type* null_map = nullptr);

// string to bitmap or hll object
Status cast_from_string_to_generic(RuntimeState* state, const ColumnWithTypeAndName& source_column,
                                   ColumnWithTypeAndName& target_column, size_t input_rows_count,
                                   const NullMap::value_type* null_map = nullptr);

Status cast_from_string_to_complex_type(RuntimeState* state,
                                        const ColumnWithTypeAndName& source_column,
                                        ColumnWithTypeAndName& target_column,
                                        size_t input_rows_count,
                                        const NullMap::value_type* null_map = nullptr);

Status cast_from_string_to_complex_type_strict_mode(RuntimeState* state,
                                                    const ColumnWithTypeAndName& source_column,
                                                    ColumnWithTypeAndName& target_column,
                                                    size_t input_rows_count,
                                                    const NullMap::value_type* null_map = nullptr);

// bind_cast_executor -> prepare_remove_nullable -> prepare_impl

WrapperType bind_cast_executor(const CastOptions& options, const DataTypePtr& from_type,
                               const DataTypePtr& to_type);

FunctionContextWrapperType bind_cast_executor(FunctionContext* context,
                                              const DataTypePtr& from_type,
                                              const DataTypePtr& to_type);

WrapperType prepare_remove_nullable(const CastOptions& options, const DataTypePtr& from_type,
                                    const DataTypePtr& to_type);

WrapperType prepare_impl(const CastOptions& options, const DataTypePtr& from_type,
                         const DataTypePtr& to_type);

Status execute_wrapper(RuntimeState* state, const WrapperType& wrapper,
                       const ColumnWithTypeAndName& source_column,
                       ColumnWithTypeAndName& target_column, size_t rows);

Status execute_wrapper(FunctionContext* context, const FunctionContextWrapperType& wrapper,
                       const ColumnWithTypeAndName& source_column,
                       ColumnWithTypeAndName& target_column, size_t rows);

ElementWrappers get_element_wrappers(const CastOptions& options,
                                     const DataTypes& from_element_types,
                                     const DataTypes& to_element_types);

/// A child of an ARRAY/MAP/STRUCT column inherits the NULL state of the row it belongs to: when a
/// row is NULL, its children are hidden payloads without SQL semantics (IF/NULLIF keep an arbitrary
/// value there) and a strict cast must not validate them.
struct ChildNullMask {
    /// Column to hand to the child cast. A nullable child column carries the merged NULL mask
    /// itself, because a child cast reads the NULL state of a nullable source from its column.
    ColumnPtr column;
    /// Mask to hand to the child cast when the child column cannot carry it (not nullable child),
    /// so the child cast skips the masked children itself.
    const NullMap::value_type* null_map = nullptr;
    /// Owns `null_map`, so it has to outlive the child cast.
    ColumnPtr mask_holder;
};

/// Merge the NULL mask of the rows of a complex column into the mask of one of its children.
/// `offsets` expands the row aligned `parent_null_map` to a flattened child (ARRAY, MAP), pass
/// nullptr for a child that shares the rows of its parent (STRUCT).
ChildNullMask build_child_null_mask(const NullMap::value_type* parent_null_map,
                                    const IColumn::Offsets64* offsets, const ColumnPtr& child);

/// Whether the mask of `rows` rows marks at least one of them as NULL. A nullable column keeps an
/// all zero mask when it has no NULL row, and there is nothing to inherit from such a column. Callers
/// that build masks for several children of the same parent can check this once and pass nullptr to
/// build_child_null_mask when it is false.
bool has_masked_row(const NullMap::value_type* null_map, size_t rows);

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
    virtual Status execute_impl(RuntimeState* state, const ColumnWithTypeAndName& source_column,
                                ColumnWithTypeAndName& target_column, size_t input_rows_count,
                                const NullMap::value_type* parent_null_map = nullptr) const = 0;
};

template <CastModeType CastMode, typename FromDataType, typename ToDataType>
class CastToImpl : public CastToBase {
public:
    Status execute_impl(RuntimeState* /*state*/, const ColumnWithTypeAndName& source_column,
                        ColumnWithTypeAndName& target_column, size_t /*input_rows_count*/,
                        const NullMap::value_type* /*parent_null_map*/ = nullptr) const override {
        return Status::RuntimeError(
                "not support  {} ",
                cast_mode_type_to_string(CastMode, source_column.type, target_column.type));
    }
};

#ifdef BE_TEST
inline CastWrapper::FunctionContextWrapperType get_cast_wrapper(FunctionContext* context,
                                                                const DataTypePtr& from_type,
                                                                const DataTypePtr& to_type) {
    return CastWrapper::bind_cast_executor(context, from_type, to_type);
}
#endif
} // namespace doris
