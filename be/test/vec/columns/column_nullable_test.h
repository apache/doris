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

#include <gmock/gmock-more-matchers.h>
#include <gtest/gtest.h>

#include <cstddef>
#include <cstdlib>
#include <type_traits>

#include "vec/columns/column.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/data_types/data_type_string.h"

namespace doris::vectorized {

inline std::string generate_random_string(size_t max_length) {
    std::srand(std::time(nullptr)); // use current time as seed for random generator

    if (max_length == 0) {
        return "";
    }

    auto randbyte = []() -> char {
        // generate a random byte, in range [0x00, 0xFF]
        return static_cast<char>(rand() % 256);
    };

    std::string str(max_length, 0);
    std::generate_n(str.begin(), max_length, randbyte);

    return str;
}

inline MutableColumnPtr create_null_map(size_t input_rows_count, bool all_null = false,
                                        bool all_not_null = false) {
    std::srand(std::time(nullptr)); // use current time as seed for random generator
    auto null_map = ColumnUInt8::create();
    for (size_t i = 0; i < input_rows_count; ++i) {
        if (all_null) {
            null_map->insert(vectorized::Field::create_field<TYPE_BOOLEAN>(1));
        } else if (all_not_null) {
            null_map->insert(vectorized::Field::create_field<TYPE_BOOLEAN>(0));
        } else {
            null_map->insert(vectorized::Field::create_field<TYPE_BOOLEAN>(rand() % 2));
        }
    }
    return null_map;
}

template <PrimitiveType T>
inline MutableColumnPtr create_nested_column(size_t input_rows_count) {
    MutableColumnPtr column;
    if constexpr (is_int_or_bool(T)) {
        column = PrimitiveTypeTraits<T>::ColumnType::create();
    } else if constexpr (is_string_type(T)) {
        column = ColumnString::create();
    } else if constexpr (T == TYPE_DECIMAL64) {
        column = ColumnDecimal64::create(0, 6);
    }

    for (size_t i = 0; i < input_rows_count; ++i) {
        if constexpr (is_int_or_bool(T)) {
            column->insert(Field::create_field<T>(
                    rand() %
                    std::numeric_limits<typename PrimitiveTypeTraits<T>::ColumnItemType>::max()));
        } else if constexpr (is_string_type(T)) {
            column->insert(Field::create_field<T>(generate_random_string(rand() % 512)));
        } else if constexpr (T == TYPE_DECIMAL64) {
            column->insert(Field::create_field<T>(
                    DecimalField<Decimal64>(Int64(rand() % std::numeric_limits<Int64>::max()), 6)));
        } else {
            throw std::runtime_error("Unsupported type");
        }
    }

    return column;
}

template <PrimitiveType T>
inline ColumnNullable::MutablePtr create_column_nullable(size_t input_rows_count,
                                                         bool all_null = false,
                                                         bool all_not_null = false) {
    auto null_map = create_null_map(input_rows_count, all_null, all_not_null);
    auto nested_column = create_nested_column<T>(input_rows_count);
    return ColumnNullable::create(std::move(nested_column), std::move(null_map));
}
} // namespace doris::vectorized