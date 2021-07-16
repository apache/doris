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

#include "vec/data_types/get_least_supertype.h"

#include <unordered_set>

#include "vec/common/typeid_cast.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_nothing.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"

namespace doris::vectorized {

namespace {

String get_exception_message_prefix(const DataTypes& types) {
    std::stringstream res;
    res << "There is no supertype for types ";

    bool first = true;
    for (const auto& type : types) {
        if (!first) res << ", ";
        first = false;

        res << type->get_name();
    }

    return res.str();
}
} // namespace

DataTypePtr get_least_supertype(const DataTypes& types) {
    /// Trivial cases

    if (types.empty()) return std::make_shared<DataTypeNothing>();

    if (types.size() == 1) return types[0];

    /// All types are equal
    {
        bool all_equal = true;
        for (size_t i = 1, size = types.size(); i < size; ++i) {
            if (!types[i]->equals(*types[0])) {
                all_equal = false;
                break;
            }
        }

        if (all_equal) return types[0];
    }

    /// Recursive rules

    /// If there are Nothing types, skip them
    {
        DataTypes non_nothing_types;
        non_nothing_types.reserve(types.size());

        for (const auto& type : types)
            if (!typeid_cast<const DataTypeNothing*>(type.get()))
                non_nothing_types.emplace_back(type);

        if (non_nothing_types.size() < types.size()) return get_least_supertype(non_nothing_types);
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

                if (!type_nullable->only_null())
                    nested_types.emplace_back(type_nullable->get_nested_type());
            } else
                nested_types.emplace_back(type);
        }

        if (have_nullable) {
            return std::make_shared<DataTypeNullable>(get_least_supertype(nested_types));
        }
    }

    /// Non-recursive rules

    std::unordered_set<TypeIndex> type_ids;
    for (const auto& type : types) type_ids.insert(type->get_type_id());

    /// For String and FixedString, or for different FixedStrings, the common type is String.
    /// No other types are compatible with Strings. TODO Enums?
    {
        UInt32 have_string = type_ids.count(TypeIndex::String);
        UInt32 have_fixed_string = type_ids.count(TypeIndex::FixedString);

        if (have_string || have_fixed_string) {
            bool all_strings = type_ids.size() == (have_string + have_fixed_string);
            if (!all_strings) {
                LOG(FATAL)
                        << get_exception_message_prefix(types)
                        << " because some of them are String/FixedString and some of them are not";
            }

            return std::make_shared<DataTypeString>();
        }
    }

    /// For Date and DateTime, the common type is DateTime. No other types are compatible.
    {
        UInt32 have_date = type_ids.count(TypeIndex::Date);
        UInt32 have_datetime = type_ids.count(TypeIndex::DateTime);

        if (have_date || have_datetime) {
            bool all_date_or_datetime = type_ids.size() == (have_date + have_datetime);
            if (!all_date_or_datetime) {
                LOG(FATAL) << get_exception_message_prefix(types)
                           << " because some of them are Date/DateTime and some of them are not";
            }

            return std::make_shared<DataTypeDateTime>();
        }
    }

    /// Decimals
    {
        UInt32 have_decimal32 = type_ids.count(TypeIndex::Decimal32);
        UInt32 have_decimal64 = type_ids.count(TypeIndex::Decimal64);
        UInt32 have_decimal128 = type_ids.count(TypeIndex::Decimal128);

        if (have_decimal32 || have_decimal64 || have_decimal128) {
            UInt32 num_supported = have_decimal32 + have_decimal64 + have_decimal128;

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
                LOG(FATAL) << get_exception_message_prefix(types)
                           << " because some of them have no lossless convertion to Decimal";
            }

            UInt32 max_scale = 0;
            for (const auto& type : types) {
                UInt32 scale = get_decimal_scale(*type, 0);
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
                LOG(FATAL) << fmt::format("{} because the least supertype is Decimal({},{})",
                                          get_exception_message_prefix(types), min_precision,
                                          max_scale);
            }

            if (have_decimal128 || min_precision > DataTypeDecimal<Decimal64>::max_precision())
                return std::make_shared<DataTypeDecimal<Decimal128>>(
                        DataTypeDecimal<Decimal128>::max_precision(), max_scale);
            if (have_decimal64 || min_precision > DataTypeDecimal<Decimal32>::max_precision())
                return std::make_shared<DataTypeDecimal<Decimal64>>(
                        DataTypeDecimal<Decimal64>::max_precision(), max_scale);
            return std::make_shared<DataTypeDecimal<Decimal32>>(
                    DataTypeDecimal<Decimal32>::max_precision(), max_scale);
        }
    }

    /// For numeric types, the most complicated part.
    {
        bool all_numbers = true;

        size_t max_bits_of_signed_integer = 0;
        size_t max_bits_of_unsigned_integer = 0;
        size_t max_mantissa_bits_of_floating = 0;

        auto maximize = [](size_t& what, size_t value) {
            if (value > what) what = value;
        };

        for (const auto& type : types) {
            if (typeid_cast<const DataTypeUInt8*>(type.get()))
                maximize(max_bits_of_unsigned_integer, 8);
            else if (typeid_cast<const DataTypeUInt16*>(type.get()))
                maximize(max_bits_of_unsigned_integer, 16);
            else if (typeid_cast<const DataTypeUInt32*>(type.get()))
                maximize(max_bits_of_unsigned_integer, 32);
            else if (typeid_cast<const DataTypeUInt64*>(type.get()))
                maximize(max_bits_of_unsigned_integer, 64);
            else if (typeid_cast<const DataTypeInt8*>(type.get()))
                maximize(max_bits_of_signed_integer, 8);
            else if (typeid_cast<const DataTypeInt16*>(type.get()))
                maximize(max_bits_of_signed_integer, 16);
            else if (typeid_cast<const DataTypeInt32*>(type.get()))
                maximize(max_bits_of_signed_integer, 32);
            else if (typeid_cast<const DataTypeInt64*>(type.get()))
                maximize(max_bits_of_signed_integer, 64);
            else if (typeid_cast<const DataTypeFloat32*>(type.get()))
                maximize(max_mantissa_bits_of_floating, 24);
            else if (typeid_cast<const DataTypeFloat64*>(type.get()))
                maximize(max_mantissa_bits_of_floating, 53);
            else
                all_numbers = false;
        }

        if (max_bits_of_signed_integer || max_bits_of_unsigned_integer ||
            max_mantissa_bits_of_floating) {
            if (!all_numbers) {
                LOG(FATAL) << get_exception_message_prefix(types)
                           << " because some of them are numbers and some of them are not";
            }

            /// If there are signed and unsigned types of same bit-width, the result must be signed number with at least one more bit.
            /// Example, common of Int32, UInt32 = Int64.

            size_t min_bit_width_of_integer =
                    std::max(max_bits_of_signed_integer, max_bits_of_unsigned_integer);

            /// If unsigned is not covered by signed.
            if (max_bits_of_signed_integer &&
                max_bits_of_unsigned_integer >= max_bits_of_signed_integer)
                ++min_bit_width_of_integer;

            /// If the result must be floating.
            if (max_mantissa_bits_of_floating) {
                size_t min_mantissa_bits =
                        std::max(min_bit_width_of_integer, max_mantissa_bits_of_floating);
                if (min_mantissa_bits <= 24)
                    return std::make_shared<DataTypeFloat32>();
                else if (min_mantissa_bits <= 53)
                    return std::make_shared<DataTypeFloat64>();
                else {
                    LOG(FATAL) << get_exception_message_prefix(types)
                               << " because some of them are integers and some are floating point "
                                  "but there is no floating point type, that can exactly represent "
                                  "all required integers";
                }
            }

            /// If the result must be signed integer.
            if (max_bits_of_signed_integer) {
                if (min_bit_width_of_integer <= 8)
                    return std::make_shared<DataTypeInt8>();
                else if (min_bit_width_of_integer <= 16)
                    return std::make_shared<DataTypeInt16>();
                else if (min_bit_width_of_integer <= 32)
                    return std::make_shared<DataTypeInt32>();
                else if (min_bit_width_of_integer <= 64)
                    return std::make_shared<DataTypeInt64>();
                else {
                    LOG(FATAL) << get_exception_message_prefix(types)
                               << " because some of them are signed integers and some are unsigned "
                                  "integers, but there is no signed integer type, that can exactly "
                                  "represent all required unsigned integer values";
                }
            }

            /// All unsigned.
            {
                if (min_bit_width_of_integer <= 8)
                    return std::make_shared<DataTypeUInt8>();
                else if (min_bit_width_of_integer <= 16)
                    return std::make_shared<DataTypeUInt16>();
                else if (min_bit_width_of_integer <= 32)
                    return std::make_shared<DataTypeUInt32>();
                else if (min_bit_width_of_integer <= 64)
                    return std::make_shared<DataTypeUInt64>();
                else {
                    LOG(FATAL) << "Logical error: " << get_exception_message_prefix(types)
                               << "but as all data types are unsigned integers, we must have found "
                                  "maximum unsigned integer type";
                }
            }
        }
    }

    /// All other data types (UUID, AggregateFunction, Enum...) are compatible only if they are the same (checked in trivial cases).
    LOG(FATAL) << get_exception_message_prefix(types);
    return nullptr;
}

} // namespace doris::vectorized
