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

#include "core/types.h"
#include "exprs/function/cast/cast_base.h"
#include "util/string_parser.hpp"

namespace doris {

struct CastToBool {
    template <class SRC>
    static inline bool from_number(const SRC& from, UInt8& to, CastParameters& params);

    template <class SRC>
    static inline bool from_decimal(const SRC& from, UInt8& to, UInt32 precision, UInt32 scale,
                                    CastParameters& params);

    static inline bool from_string(const StringRef& from, UInt8& to, CastParameters& params);
};

template <>
inline bool CastToBool::from_number(const UInt8& from, UInt8& to, CastParameters&) {
    to = from;
    return true;
}

template <>
inline bool CastToBool::from_number(const Int8& from, UInt8& to, CastParameters&) {
    to = (from != 0);
    return true;
}
template <>
inline bool CastToBool::from_number(const Int16& from, UInt8& to, CastParameters&) {
    to = (from != 0);
    return true;
}
template <>
inline bool CastToBool::from_number(const Int32& from, UInt8& to, CastParameters&) {
    to = (from != 0);
    return true;
}
template <>
inline bool CastToBool::from_number(const Int64& from, UInt8& to, CastParameters&) {
    to = (from != 0);
    return true;
}
template <>
inline bool CastToBool::from_number(const Int128& from, UInt8& to, CastParameters&) {
    to = (from != 0);
    return true;
}

template <>
inline bool CastToBool::from_number(const Float32& from, UInt8& to, CastParameters&) {
    to = (from != 0);
    return true;
}
template <>
inline bool CastToBool::from_number(const Float64& from, UInt8& to, CastParameters&) {
    to = (from != 0);
    return true;
}

template <>
inline bool CastToBool::from_decimal(const Decimal32& from, UInt8& to, UInt32, UInt32,
                                     CastParameters&) {
    to = (from.value != 0);
    return true;
}

template <>
inline bool CastToBool::from_decimal(const Decimal64& from, UInt8& to, UInt32, UInt32,
                                     CastParameters&) {
    to = (from.value != 0);
    return true;
}

template <>
inline bool CastToBool::from_decimal(const DecimalV2Value& from, UInt8& to, UInt32, UInt32,
                                     CastParameters&) {
    to = (from.value() != 0);
    return true;
}

template <>
inline bool CastToBool::from_decimal(const Decimal128V3& from, UInt8& to, UInt32, UInt32,
                                     CastParameters&) {
    to = (from.value != 0);
    return true;
}

template <>
inline bool CastToBool::from_decimal(const Decimal256& from, UInt8& to, UInt32, UInt32,
                                     CastParameters&) {
    to = (from.value != 0);
    return true;
}

inline bool CastToBool::from_string(const StringRef& from, UInt8& to, CastParameters&) {
    StringParser::ParseResult result;
    to = StringParser::string_to_bool(from.data, from.size, &result);
    return result == StringParser::PARSE_SUCCESS;
}

template <CastModeType Mode>
class CastToImpl<Mode, DataTypeString, DataTypeBool> : public CastToBase {
public:
    Status execute_impl(RuntimeState* /*state*/, const ColumnWithTypeAndName& source_column,
                        ColumnWithTypeAndName& target_column, size_t /*input_rows_count*/,
                        const NullMap::value_type* null_map = nullptr) const override {
        const auto* col_from =
                assert_cast<const DataTypeString::ColumnType*>(source_column.column.get());
        auto nested_to_type = remove_nullable(target_column.type);
        auto serde = nested_to_type->get_serde();
        if constexpr (Mode == CastModeType::NonStrictMode) {
            auto nullable_col_to = create_empty_nullable_column(nested_to_type);
            RETURN_IF_ERROR(serde->from_string_batch(*col_from, *nullable_col_to, {}));
            target_column.column = std::move(nullable_col_to);
        } else {
            MutableColumnPtr column_to = nested_to_type->create_column();
            RETURN_IF_ERROR(
                    serde->from_string_strict_mode_batch(*col_from, *column_to, {}, null_map));
            target_column.column = std::move(column_to);
        }
        return Status::OK();
    }
};
template <CastModeType AllMode, typename NumberType>
    requires(IsDataTypeNumber<NumberType>)
class CastToImpl<AllMode, NumberType, DataTypeBool> : public CastToBase {
public:
    Status execute_impl(RuntimeState* state, const ColumnWithTypeAndName& source_column,
                        ColumnWithTypeAndName& target_column, size_t input_rows_count,
                        const NullMap::value_type* null_map = nullptr) const override {
        const auto* col_from =
                assert_cast<const typename NumberType::ColumnType*>(source_column.column.get());
        DataTypeBool::ColumnType::MutablePtr col_to =
                DataTypeBool::ColumnType::create(input_rows_count);

        CastParameters params;
        params.is_strict = (AllMode == CastModeType::StrictMode);
        const auto* from_data = col_from->get_data().data();
        auto* to_data = col_to->get_data().data();
        for (size_t i = 0; i < input_rows_count; ++i) {
            if (null_map != nullptr && null_map[i] != 0) {
                continue;
            }
            CastToBool::from_number(from_data[i], to_data[i], params);
        }

        target_column.column = std::move(col_to);
        return Status::OK();
    }
};

template <CastModeType AllMode, typename DecimalType>
    requires(IsDataTypeDecimal<DecimalType>)
class CastToImpl<AllMode, DecimalType, DataTypeBool> : public CastToBase {
public:
    Status execute_impl(RuntimeState* state, const ColumnWithTypeAndName& source_column,
                        ColumnWithTypeAndName& target_column, size_t input_rows_count,
                        const NullMap::value_type* null_map = nullptr) const override {
        const auto* col_from =
                assert_cast<const typename DecimalType::ColumnType*>(source_column.column.get());
        const auto type_from = source_column.type;
        DataTypeBool::ColumnType::MutablePtr col_to =
                DataTypeBool::ColumnType::create(input_rows_count);

        CastParameters params;
        params.is_strict = (AllMode == CastModeType::StrictMode);

        auto precision = type_from->get_precision();
        auto scale = type_from->get_scale();
        const auto* from_data = col_from->get_data().data();
        auto* to_data = col_to->get_data().data();
        for (size_t i = 0; i < input_rows_count; ++i) {
            if (null_map != nullptr && null_map[i] != 0) {
                continue;
            }
            CastToBool::from_decimal(from_data[i], to_data[i], precision, scale, params);
        }

        target_column.column = std::move(col_to);
        return Status::OK();
    }
};

} // namespace doris
