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

#include <algorithm>
#include <memory>
#include <ostream>
#include <string>
#include <vector>

#include "common/status.h"
#include "vec/aggregate_functions/helpers.h"
#include "vec/common/typeid_cast.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_jsonb.h"
#include "vec/data_types/data_type_nothing.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"

namespace doris::vectorized {

void get_numeric_type(const TypeIndexSet& types, DataTypePtr* type) {
    bool all_numbers = true;

    size_t max_bits_of_signed_integer = 0;
    size_t max_bits_of_unsigned_integer = 0;
    size_t max_mantissa_bits_of_floating = 0;

    auto maximize = [](size_t& what, size_t value) {
        if (value > what) {
            what = value;
        }
    };

    for (const auto& type : types) {
        if (type == TypeIndex::UInt8) {
            maximize(max_bits_of_unsigned_integer, 8);
        } else if (type == TypeIndex::UInt16) {
            maximize(max_bits_of_unsigned_integer, 16);
        } else if (type == TypeIndex::UInt32) {
            maximize(max_bits_of_unsigned_integer, 32);
        } else if (type == TypeIndex::UInt64) {
            maximize(max_bits_of_unsigned_integer, 64);
        } else if (type == TypeIndex::UInt128) {
            maximize(max_bits_of_unsigned_integer, 128);
        } else if (type == TypeIndex::Int8 || type == TypeIndex::Enum8) {
            maximize(max_bits_of_signed_integer, 8);
        } else if (type == TypeIndex::Int16 || type == TypeIndex::Enum16) {
            maximize(max_bits_of_signed_integer, 16);
        } else if (type == TypeIndex::Int32) {
            maximize(max_bits_of_signed_integer, 32);
        } else if (type == TypeIndex::Int64) {
            maximize(max_bits_of_signed_integer, 64);
        } else if (type == TypeIndex::Int128) {
            maximize(max_bits_of_signed_integer, 128);
        } else if (type == TypeIndex::Float32) {
            maximize(max_mantissa_bits_of_floating, 24);
        } else if (type == TypeIndex::Float64) {
            maximize(max_mantissa_bits_of_floating, 53);
        } else {
            all_numbers = false;
        }
    }

    if (max_bits_of_signed_integer || max_bits_of_unsigned_integer ||
        max_mantissa_bits_of_floating) {
        if (!all_numbers) {
            *type = std::make_shared<DataTypeJsonb>();
            return;
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
                VLOG_DEBUG << " because some of them are integers and some are floating point "
                              "but there is no floating point type, that can exactly represent "
                              "all required integers";
                *type = std::make_shared<DataTypeJsonb>();
                return;
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
                VLOG_DEBUG << " because some of them are signed integers and some are unsigned "
                              "integers, but there is no signed integer type, that can exactly "
                              "represent all required unsigned integer values";
                *type = std::make_shared<DataTypeJsonb>();
                return;
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
                *type = std::make_shared<DataTypeJsonb>();
                return;
            }
        }
    }
    *type = nullptr;
}

void get_least_supertype_jsonb(const DataTypes& types, DataTypePtr* type) {
    // For Nullable
    {
        bool have_nullable = false;

        DataTypes nested_types;
        nested_types.reserve(types.size());

        for (const auto& type : types) {
            if (const auto* type_nullable = typeid_cast<const DataTypeNullable*>(type.get())) {
                have_nullable = true;

                nested_types.emplace_back(type_nullable->get_nested_type());
            } else {
                nested_types.emplace_back(type);
            }
        }

        if (have_nullable) {
            DataTypePtr nested_type;
            get_least_supertype_jsonb(nested_types, &nested_type);
            *type = std::make_shared<DataTypeNullable>(nested_type);
            return;
        }
    }

    // For Arrays
    {
        bool have_array = false;
        bool all_arrays = true;
        DataTypes nested_types;
        nested_types.reserve(types.size());
        for (const auto& type : types) {
            if (const auto* type_array = typeid_cast<const DataTypeArray*>(type.get())) {
                have_array = true;
                nested_types.emplace_back(type_array->get_nested_type());
            } else {
                all_arrays = false;
            }
        }
        if (have_array) {
            if (!all_arrays) {
                *type = std::make_shared<DataTypeJsonb>();
                return;
            }
            DataTypePtr nested_type;
            get_least_supertype_jsonb(nested_types, &nested_type);
            /// When on_error == LeastSupertypeOnError::Null and we cannot get least supertype,
            /// nested_type will be nullptr, we should return nullptr in this case.
            if (!nested_type) {
                *type = nullptr;
                return;
            }
            *type = std::make_shared<DataTypeArray>(nested_type);
            return;
        }
    }

    phmap::flat_hash_set<TypeIndex> type_ids;
    for (const auto& type : types) {
        type_ids.insert(type->get_type_id());
    }
    get_least_supertype_jsonb(type_ids, type);
}

void get_least_supertype_jsonb(const TypeIndexSet& types, DataTypePtr* type) {
    if (types.empty()) {
        *type = std::make_shared<DataTypeNothing>();
        return;
    }
    if (types.size() == 1) {
        WhichDataType which(*types.begin());
        if (which.is_nothing()) {
            *type = std::make_shared<DataTypeNothing>();
            return;
        }
#define DISPATCH(TYPE)                                    \
    if (which.idx == TypeIndex::TYPE) {                   \
        *type = std::make_shared<DataTypeNumber<TYPE>>(); \
        return;                                           \
    }
        FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
        if (which.is_string()) {
            *type = std::make_shared<DataTypeString>();
            return;
        }
        if (which.is_json()) {
            *type = std::make_shared<DataTypeJsonb>();
            return;
        }
        *type = std::make_shared<DataTypeJsonb>();
        return;
    }
    if (types.contains(TypeIndex::String)) {
        bool only_string = types.size() == 2 && types.contains(TypeIndex::Nothing);
        if (!only_string) {
            *type = std::make_shared<DataTypeJsonb>();
            return;
        }
        *type = std::make_shared<DataTypeString>();
        return;
    }
    if (types.contains(TypeIndex::JSONB)) {
        bool only_json = types.size() == 2 && types.contains(TypeIndex::Nothing);
        if (!only_json) {
            *type = std::make_shared<DataTypeJsonb>();
            return;
        }
        *type = std::make_shared<DataTypeJsonb>();
        return;
    }

    /// For numeric types, the most complicated part.
    DataTypePtr numeric_type = nullptr;
    get_numeric_type(types, &numeric_type);
    if (numeric_type) {
        *type = numeric_type;
        return;
    }
    /// All other data types (UUID, AggregateFunction, Enum...) are compatible only if they are the same (checked in trivial cases).
    *type = std::make_shared<DataTypeJsonb>();
}

} // namespace doris::vectorized
