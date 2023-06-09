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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/DataTypes/getLeastSupertype.cpp
// and modified by Doris

#include "vec/data_types/get_least_supertype.h"

#include <fmt/core.h>
#include <fmt/format.h>
#include <glog/logging.h>
#include <stddef.h>

#include <algorithm>
#include <boost/iterator/iterator_facade.hpp>
#include <memory>
#include <ostream>
#include <string>
#include <vector>

#include "common/status.h"
#include "vec/common/typeid_cast.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_nothing.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"

namespace doris::vectorized {

namespace {

String type_to_string(const DataTypePtr& type) {
    return type->get_name();
}
String type_to_string(const TypeIndex& type) {
    return getTypeName(type);
}

template <typename DataTypes>
String get_exception_message_prefix(const DataTypes& types) {
    std::stringstream res;
    res << "There is no supertype for types ";
    bool first = true;
    for (const auto& type : types) {
        if (!first) res << ", ";
        first = false;
        res << type_to_string(type);
    }
    return res.str();
}

} // namespace

void get_numeric_type(const TypeIndexSet& types, DataTypePtr* type, bool compatible_with_string) {
    auto throw_or_return = [&](std::string_view message, int error_code) {
        if (compatible_with_string) {
            *type = std::make_shared<DataTypeString>();
            return;
        }
        throw doris::Exception(error_code, message);
    };
    bool all_numbers = true;

    size_t max_bits_of_signed_integer = 0;
    size_t max_bits_of_unsigned_integer = 0;
    size_t max_mantissa_bits_of_floating = 0;

    auto maximize = [](size_t& what, size_t value) {
        if (value > what) what = value;
    };

    for (const auto& type : types) {
        if (type == TypeIndex::UInt8)
            maximize(max_bits_of_unsigned_integer, 8);
        else if (type == TypeIndex::UInt16)
            maximize(max_bits_of_unsigned_integer, 16);
        else if (type == TypeIndex::UInt32)
            maximize(max_bits_of_unsigned_integer, 32);
        else if (type == TypeIndex::UInt64)
            maximize(max_bits_of_unsigned_integer, 64);
        else if (type == TypeIndex::UInt128)
            maximize(max_bits_of_unsigned_integer, 128);
        else if (type == TypeIndex::Int8 || type == TypeIndex::Enum8)
            maximize(max_bits_of_signed_integer, 8);
        else if (type == TypeIndex::Int16 || type == TypeIndex::Enum16)
            maximize(max_bits_of_signed_integer, 16);
        else if (type == TypeIndex::Int32)
            maximize(max_bits_of_signed_integer, 32);
        else if (type == TypeIndex::Int64)
            maximize(max_bits_of_signed_integer, 64);
        else if (type == TypeIndex::Int128)
            maximize(max_bits_of_signed_integer, 128);
        else if (type == TypeIndex::Float32)
            maximize(max_mantissa_bits_of_floating, 24);
        else if (type == TypeIndex::Float64)
            maximize(max_mantissa_bits_of_floating, 53);
        else
            all_numbers = false;
    }

    if (max_bits_of_signed_integer || max_bits_of_unsigned_integer ||
        max_mantissa_bits_of_floating) {
        if (!all_numbers) {
            *type = nullptr;
            return throw_or_return(
                    get_exception_message_prefix(types) +
                            " because some of them are numbers and some of them are not",
                    doris::ErrorCode::INVALID_ARGUMENT);
        }

        /// If there are signed and unsigned types of same bit-width, the result must be signed number with at least one more bit.
        /// Example, common of Int32, UInt32 = Int64.

        size_t min_bit_width_of_integer =
                std::max(max_bits_of_signed_integer, max_bits_of_unsigned_integer);

        /// If unsigned is not covered by signed.
        if (max_bits_of_signed_integer &&
            max_bits_of_unsigned_integer >= max_bits_of_signed_integer) {
            ++min_bit_width_of_integer;
        }

        /// If the result must be floating.
        if (max_mantissa_bits_of_floating) {
            size_t min_mantissa_bits =
                    std::max(min_bit_width_of_integer, max_mantissa_bits_of_floating);
            if (min_mantissa_bits <= 24) {
                *type = std::make_shared<DataTypeFloat32>();
                return;
            } else if (min_mantissa_bits <= 53) {
                *type = std::make_shared<DataTypeFloat64>();
                return;
            } else {
                LOG(INFO) << " because some of them are integers and some are floating point "
                             "but there is no floating point type, that can exactly represent "
                             "all required integers";
                *type = nullptr;
                return throw_or_return(
                        get_exception_message_prefix(types) +
                                " because some of them are integers and some are floating point "
                                "but there is no floating point type, that can exactly represent "
                                "all required integers",
                        doris::ErrorCode::INVALID_ARGUMENT);
            }
        }

        /// If the result must be signed integer.
        if (max_bits_of_signed_integer) {
            if (min_bit_width_of_integer <= 8) {
                *type = std::make_shared<DataTypeInt8>();
                return;
            } else if (min_bit_width_of_integer <= 16) {
                *type = std::make_shared<DataTypeInt16>();
                return;
            } else if (min_bit_width_of_integer <= 32) {
                *type = std::make_shared<DataTypeInt32>();
                return;
            } else if (min_bit_width_of_integer <= 64) {
                *type = std::make_shared<DataTypeInt64>();
                return;
            } else {
                LOG(INFO) << " because some of them are signed integers and some are unsigned "
                             "integers, but there is no signed integer type, that can exactly "
                             "represent all required unsigned integer values";
                return throw_or_return(
                        " because some of them are signed integers and some are unsigned "
                        "integers, but there is no signed integer type, that can exactly "
                        "represent all required unsigned integer values",
                        doris::ErrorCode::INVALID_ARGUMENT);
            }
        }

        /// All unsigned.
        {
            if (min_bit_width_of_integer <= 8) {
                *type = std::make_shared<DataTypeUInt8>();
                return;
            } else if (min_bit_width_of_integer <= 16) {
                *type = std::make_shared<DataTypeUInt16>();
                return;
            } else if (min_bit_width_of_integer <= 32) {
                *type = std::make_shared<DataTypeUInt32>();
                return;
            } else if (min_bit_width_of_integer <= 64) {
                *type = std::make_shared<DataTypeUInt64>();
                return;
            } else {
                LOG(WARNING) << "Logical error: "
                             << "but as all data types are unsigned integers, we must have found "
                                "maximum unsigned integer type";
                *type = nullptr;
                return throw_or_return(
                        "Logical error: " + get_exception_message_prefix(types) +
                                " but as all data types are unsigned integers, we must have found "
                                "maximum unsigned integer type",
                        doris::ErrorCode::INVALID_ARGUMENT);
            }
        }
    }
    *type = nullptr;
}

// TODO conflict type resolve
void get_least_supertype(const DataTypes& types, DataTypePtr* type, bool compatible_with_string) {
    /// Trivial cases
    auto throw_or_return = [&](std::string_view message, int error_code) {
        if (compatible_with_string) {
            *type = std::make_shared<DataTypeString>();
            return;
        }
        throw doris::Exception(error_code, String(message));
    };
    if (types.empty()) {
        *type = std::make_shared<DataTypeNothing>();
        return;
    }

    if (types.size() == 1) {
        *type = types[0];
        return;
    }

    /// All types are equal
    {
        bool all_equal = true;
        for (size_t i = 1, size = types.size(); i < size; ++i) {
            if (!types[i]->equals(*types[0])) {
                all_equal = false;
                break;
            }
        }

        if (all_equal) {
            *type = types[0];
            return;
        }
    }

    /// Recursive rules

    /// If there are Nothing types, skip them
    {
        DataTypes non_nothing_types;
        non_nothing_types.reserve(types.size());

        for (const auto& type : types) {
            if (!typeid_cast<const DataTypeNothing*>(type.get())) {
                non_nothing_types.emplace_back(type);
            }
        }

        if (non_nothing_types.size() < types.size()) {
            get_least_supertype(non_nothing_types, type, compatible_with_string);
            return;
        }
    }

    /// For Nullable
    {
        bool have_nullable = false;

        DataTypes nested_types;
        nested_types.reserve(types.size());

        for (const auto& type : types) {
            if (const DataTypeNullable* type_nullable =
                        typeid_cast<const DataTypeNullable*>(type.get())) {
                have_nullable = true;

                if (!type_nullable->only_null()) {
                    nested_types.emplace_back(type_nullable->get_nested_type());
                }
            } else {
                nested_types.emplace_back(type);
            }
        }

        if (have_nullable) {
            DataTypePtr nested_type;
            get_least_supertype(nested_types, &nested_type, compatible_with_string);
            *type = std::make_shared<DataTypeNullable>(nested_type);
            return;
        }
    }

    /// Non-recursive rules

    phmap::flat_hash_set<TypeIndex> type_ids;
    for (const auto& type : types) {
        type_ids.insert(type->get_type_id());
    }

    /// For String and FixedString, or for different FixedStrings, the common type is String.
    /// No other types are compatible with Strings. TODO Enums?
    {
        UInt32 have_string = type_ids.count(TypeIndex::String);
        UInt32 have_fixed_string = type_ids.count(TypeIndex::FixedString);

        if (have_string || have_fixed_string) {
            bool all_strings = type_ids.size() == (have_string + have_fixed_string);
            if (!all_strings && !compatible_with_string) {
                LOG(INFO)
                        << get_exception_message_prefix(types)
                        << " because some of them are String/FixedString and some of them are not";
                return throw_or_return(get_exception_message_prefix(types) +
                                               " because some of them are String/FixedString and "
                                               "some of them are not",
                                       doris::ErrorCode::INVALID_ARGUMENT);
            }

            *type = std::make_shared<DataTypeString>();
            return;
        }
    }

    /// For Date and DateTime, the common type is DateTime. No other types are compatible.
    {
        UInt32 have_date = type_ids.count(TypeIndex::Date);
        UInt32 have_datetime = type_ids.count(TypeIndex::DateTime);

        if (have_date || have_datetime) {
            bool all_date_or_datetime = type_ids.size() == (have_date + have_datetime);
            if (!all_date_or_datetime) {
                LOG(INFO) << get_exception_message_prefix(types)
                          << " because some of them are Date/DateTime and some of them are not";
                return throw_or_return(
                        get_exception_message_prefix(types) +
                                " because some of them are Date/DateTime and some of them are not",
                        doris::ErrorCode::INVALID_ARGUMENT);
            }

            *type = std::make_shared<DataTypeDateTime>();
            return;
        }
    }

    /// Decimals
    {
        UInt32 have_decimal32 = type_ids.count(TypeIndex::Decimal32);
        UInt32 have_decimal64 = type_ids.count(TypeIndex::Decimal64);
        UInt32 have_decimal128 = type_ids.count(TypeIndex::Decimal128);
        UInt32 have_decimal128i = type_ids.count(TypeIndex::Decimal128I);

        if (have_decimal32 || have_decimal64 || have_decimal128 || have_decimal128i) {
            UInt32 num_supported =
                    have_decimal32 + have_decimal64 + have_decimal128 + have_decimal128i;

            std::vector<TypeIndex> int_ids = {
                    TypeIndex::Int8,  TypeIndex::UInt8,  TypeIndex::Int16, TypeIndex::UInt16,
                    TypeIndex::Int32, TypeIndex::UInt32, TypeIndex::Int64, TypeIndex::UInt64};
            std::vector<UInt32> num_ints(int_ids.size(), 0);

            TypeIndex max_int = TypeIndex::Nothing;
            for (size_t i = 0; i < int_ids.size(); ++i) {
                UInt32 num = type_ids.count(int_ids[i]);
                num_ints[i] = num;
                num_supported += num;
                if (num) max_int = int_ids[i];
            }

            if (num_supported != type_ids.size()) {
                LOG(INFO) << get_exception_message_prefix(types)
                          << " because some of them have no lossless convertion to Decimal";
                return throw_or_return(
                        get_exception_message_prefix(types) +
                                " because some of them have no lossless convertion to Decimal",
                        doris::ErrorCode::INVALID_ARGUMENT);
            }

            UInt32 max_scale = 0;
            for (const auto& type : types) {
                UInt32 scale = get_decimal_scale(*type);
                if (scale > max_scale) max_scale = scale;
            }

            UInt32 min_precision = max_scale + least_decimal_precision_for(max_int);

            /// special cases Int32 -> Dec32, Int64 -> Dec64
            if (max_scale == 0) {
                if (max_int == TypeIndex::Int32)
                    min_precision = DataTypeDecimal<Decimal32>::max_precision();
                else if (max_int == TypeIndex::Int64)
                    min_precision = DataTypeDecimal<Decimal64>::max_precision();
            }

            if (min_precision > DataTypeDecimal<Decimal128>::max_precision()) {
                LOG(INFO) << fmt::format("{} because the least supertype is Decimal({},{})",
                                         get_exception_message_prefix(types), min_precision,
                                         max_scale);
                return throw_or_return(get_exception_message_prefix(types) +
                                               fmt::format(" because some of them have no lossless "
                                                           "convertion to Decimal({},{})",
                                                           min_precision, max_scale),
                                       doris::ErrorCode::INVALID_ARGUMENT);
            }

            if (have_decimal128 || min_precision > DataTypeDecimal<Decimal64>::max_precision()) {
                *type = std::make_shared<DataTypeDecimal<Decimal128>>(
                        DataTypeDecimal<Decimal128>::max_precision(), max_scale);
                return;
            }
            if (have_decimal128i || min_precision > DataTypeDecimal<Decimal64>::max_precision()) {
                *type = std::make_shared<DataTypeDecimal<Decimal128I>>(
                        DataTypeDecimal<Decimal128I>::max_precision(), max_scale);
                return;
            }
            if (have_decimal64 || min_precision > DataTypeDecimal<Decimal32>::max_precision()) {
                *type = std::make_shared<DataTypeDecimal<Decimal64>>(
                        DataTypeDecimal<Decimal64>::max_precision(), max_scale);
                return;
            }
            *type = std::make_shared<DataTypeDecimal<Decimal32>>(
                    DataTypeDecimal<Decimal32>::max_precision(), max_scale);
            return;
        }
    }

    /// For numeric types, the most complicated part.
    {
        DataTypePtr numeric_type = nullptr;
        get_numeric_type(type_ids, &numeric_type, compatible_with_string);
        if (numeric_type) {
            *type = numeric_type;
            return;
        }
    }

    /// All other data types (UUID, AggregateFunction, Enum...) are compatible only if they are the same (checked in trivial cases).
    *type = nullptr;
    return throw_or_return(get_exception_message_prefix(types), ErrorCode::INVALID_ARGUMENT);
}

void get_least_supertype(const TypeIndexSet& types, DataTypePtr* type,
                         bool compatible_with_string) {
    auto throw_or_return = [&](std::string_view message, int error_code) {
        if (compatible_with_string) {
            *type = std::make_shared<DataTypeString>();
            return;
        }
        throw doris::Exception(error_code, String(message));
    };
    TypeIndexSet types_set;
    for (const auto& t : types) {
        if (WhichDataType(t).is_nothing()) continue;

        if (!WhichDataType(t).is_simple()) {
            LOG(INFO) << "Cannot get common type by type ids with parametric type"
                      << getTypeName(t);
            *type = nullptr;
            throw doris::Exception(ErrorCode::INVALID_ARGUMENT,
                                   "Cannot get common type by type ids with parametric type {}",
                                   type_to_string(t));
        }

        types_set.insert(t);
    }

    if (types_set.empty()) {
        *type = std::make_shared<DataTypeNothing>();
        return;
    }

    if (types.count(TypeIndex::String)) {
        if (types.size() != 1) {
            LOG(INFO) << " because some of them are String and some of them are not";
            *type = nullptr;
            return throw_or_return(
                    get_exception_message_prefix(types) +
                            " because some of them are String and some of them are not",
                    ErrorCode::INVALID_ARGUMENT);
        }

        *type = std::make_shared<DataTypeString>();
        return;
    }

    /// For numeric types, the most complicated part.
    DataTypePtr numeric_type = nullptr;
    get_numeric_type(types, &numeric_type, compatible_with_string);
    if (numeric_type) {
        *type = numeric_type;
        return;
    }
    /// All other data types (UUID, AggregateFunction, Enum...) are compatible only if they are the same (checked in trivial cases).
    *type = nullptr;
    return throw_or_return(get_exception_message_prefix(types), ErrorCode::INVALID_ARGUMENT);
}

} // namespace doris::vectorized
