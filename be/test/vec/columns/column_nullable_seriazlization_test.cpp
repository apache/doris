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
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/columns_number.h"
#include "vec/common/arena.h"
#include "vec/common/string_ref.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/data_types/data_type_string.h"

using namespace doris;
using namespace doris::vectorized;

static std::string generate_random_string(size_t max_length) {
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

static MutableColumnPtr create_null_map(size_t input_rows_count, bool all_null = false,
                                        bool all_not_null = false) {
    std::srand(std::time(nullptr)); // use current time as seed for random generator
    auto null_map = ColumnUInt8::create();
    for (size_t i = 0; i < input_rows_count; ++i) {
        if (all_null) {
            null_map->insert(1);
        } else if (all_not_null) {
            null_map->insert(0);
        } else {
            null_map->insert(rand() % 2);
        }
    }
    return null_map;
}

template <typename T>
static MutableColumnPtr create_nested_column(size_t input_rows_count) {
    MutableColumnPtr column;
    if constexpr (std::is_integral_v<T>) {
        column = ColumnVector<T>::create();
    } else if constexpr (std::is_same_v<T, String>) {
        column = ColumnString::create();
    } else if constexpr (std::is_same_v<T, Decimal64>) {
        column = ColumnDecimal64::create(0, 6);
    }

    for (size_t i = 0; i < input_rows_count; ++i) {
        if constexpr (std::is_integral_v<T>) {
            column->insert(rand() % std::numeric_limits<T>::max());
        } else if constexpr (std::is_same_v<T, String>) {
            column->insert(generate_random_string(rand() % 512));
        } else if constexpr (std::is_same_v<T, Decimal64>) {
            column->insert(Int64(rand() % std::numeric_limits<Int64>::max()));
        } else {
            throw std::runtime_error("Unsupported type");
        }
    }

    return column;
}

template <typename T>
static ColumnNullable::MutablePtr create_column_nullable(size_t input_rows_count,
                                                         bool all_null = false,
                                                         bool all_not_null = false) {
    auto null_map = create_null_map(input_rows_count, all_null, all_not_null);
    auto nested_column = create_nested_column<T>(input_rows_count);
    return ColumnNullable::create(std::move(nested_column), std::move(null_map));
}

TEST(ColumnNullableSerializationTest, column_nullable_column_vector) {
    const size_t input_rows_count = 4096 * 1000;
    ColumnNullable::Ptr column_nullable = create_column_nullable<Int64>(input_rows_count);
    Arena arena(4096);

    const size_t max_row_byte_size = column_nullable->get_max_row_byte_size();
    auto data = arena.alloc(input_rows_count * column_nullable->get_max_row_byte_size());
    std::vector<StringRef> data_strs(input_rows_count);
    for (size_t i = 0; i < input_rows_count; ++i) {
        data_strs[i] = StringRef(data + i * column_nullable->get_max_row_byte_size(), 0);
    }

    column_nullable->serialize_vec(data_strs, input_rows_count, max_row_byte_size);

    ColumnNullable::MutablePtr result_column =
            ColumnNullable::create(ColumnInt64::create(), ColumnUInt8::create());
    result_column->deserialize_vec(data_strs, input_rows_count);

    for (size_t i = 0; i < input_rows_count; ++i) {
        EXPECT_EQ(column_nullable->get_data_at(i), result_column->get_data_at(i))
                << fmt::format("source {}\nresult {}\n", column_nullable->get_data_at(i),
                               result_column->get_data_at(i));
    }
}

TEST(ColumnNullableSerializationTest, column_nullable_column_vector_all_null) {
    const size_t input_rows_count = 4096 * 1000;
    ColumnNullable::Ptr column_nullable = create_column_nullable<Int64>(input_rows_count, true);
    Arena arena(4096);

    const size_t max_row_byte_size = column_nullable->get_max_row_byte_size();
    auto data = arena.alloc(input_rows_count * column_nullable->get_max_row_byte_size());
    std::vector<StringRef> data_strs(input_rows_count);
    for (size_t i = 0; i < input_rows_count; ++i) {
        data_strs[i] = StringRef(data + i * column_nullable->get_max_row_byte_size(), 0);
    }

    column_nullable->serialize_vec(data_strs, input_rows_count, max_row_byte_size);

    ColumnNullable::MutablePtr result_column =
            ColumnNullable::create(ColumnInt64::create(), ColumnUInt8::create());
    result_column->deserialize_vec(data_strs, input_rows_count);

    for (size_t i = 0; i < input_rows_count; ++i) {
        EXPECT_EQ(column_nullable->get_data_at(i), result_column->get_data_at(i))
                << fmt::format("source {}\nresult {}\n", column_nullable->get_data_at(i),
                               result_column->get_data_at(i));
    }
}

TEST(ColumnNullableSerializationTest, column_nullable_column_vector_all_not_null) {
    const size_t input_rows_count = 4096 * 1000;
    ColumnNullable::Ptr column_nullable =
            create_column_nullable<Int64>(input_rows_count, false, true);
    Arena arena(4096);

    const size_t max_row_byte_size = column_nullable->get_max_row_byte_size();
    auto data = arena.alloc(input_rows_count * column_nullable->get_max_row_byte_size());
    std::vector<StringRef> data_strs(input_rows_count);
    for (size_t i = 0; i < input_rows_count; ++i) {
        data_strs[i] = StringRef(data + i * column_nullable->get_max_row_byte_size(), 0);
    }

    column_nullable->serialize_vec(data_strs, input_rows_count, max_row_byte_size);

    ColumnNullable::MutablePtr result_column =
            ColumnNullable::create(ColumnInt64::create(), ColumnUInt8::create());
    result_column->deserialize_vec(data_strs, input_rows_count);

    for (size_t i = 0; i < input_rows_count; ++i) {
        EXPECT_EQ(column_nullable->get_data_at(i), result_column->get_data_at(i))
                << fmt::format("source {}\nresult {}\n", column_nullable->get_data_at(i),
                               result_column->get_data_at(i));
    }
}

TEST(ColumnNullableSerializationTest, column_nullable_column_string) {
    const size_t input_rows_count = 4096;
    ColumnNullable::Ptr column_nullable = create_column_nullable<String>(input_rows_count);
    Arena arena(4096);

    const size_t max_row_byte_size = column_nullable->get_max_row_byte_size();
    auto data = arena.alloc(input_rows_count * column_nullable->get_max_row_byte_size());
    std::vector<StringRef> data_strs(input_rows_count);
    for (size_t i = 0; i < input_rows_count; ++i) {
        data_strs[i] = StringRef(data + i * column_nullable->get_max_row_byte_size(), 0);
    }

    column_nullable->serialize_vec(data_strs, input_rows_count, max_row_byte_size);

    ColumnNullable::MutablePtr result_column =
            ColumnNullable::create(ColumnString::create(), ColumnUInt8::create());
    result_column->deserialize_vec(data_strs, input_rows_count);

    for (size_t i = 0; i < input_rows_count; ++i) {
        ASSERT_EQ(column_nullable->get_data_at(i), result_column->get_data_at(i))
                << fmt::format("source {}\nresult {}\n", column_nullable->get_data_at(i),
                               result_column->get_data_at(i));
    }
}

TEST(ColumnNullableSerializationTest, column_nullable_column_string_all_null) {
    const size_t input_rows_count = 4096;
    ColumnNullable::Ptr column_nullable = create_column_nullable<String>(input_rows_count, true);
    Arena arena(4096);

    const size_t max_row_byte_size = column_nullable->get_max_row_byte_size();
    auto data = arena.alloc(input_rows_count * column_nullable->get_max_row_byte_size());
    std::vector<StringRef> data_strs(input_rows_count);
    for (size_t i = 0; i < input_rows_count; ++i) {
        data_strs[i] = StringRef(data + i * column_nullable->get_max_row_byte_size(), 0);
    }

    column_nullable->serialize_vec(data_strs, input_rows_count, max_row_byte_size);

    ColumnNullable::MutablePtr result_column =
            ColumnNullable::create(ColumnString::create(), ColumnUInt8::create());
    result_column->deserialize_vec(data_strs, input_rows_count);

    for (size_t i = 0; i < input_rows_count; ++i) {
        ASSERT_EQ(column_nullable->get_data_at(i), result_column->get_data_at(i))
                << fmt::format("source {}\nresult {}\n", column_nullable->get_data_at(i),
                               result_column->get_data_at(i));
    }
}

TEST(ColumnNullableSerializationTest, column_nullable_column_string_all_not_null) {
    const size_t input_rows_count = 4096;
    ColumnNullable::Ptr column_nullable =
            create_column_nullable<String>(input_rows_count, false, true);
    Arena arena(4096);

    const size_t max_row_byte_size = column_nullable->get_max_row_byte_size();
    auto data = arena.alloc(input_rows_count * column_nullable->get_max_row_byte_size());
    std::vector<StringRef> data_strs(input_rows_count);
    for (size_t i = 0; i < input_rows_count; ++i) {
        data_strs[i] = StringRef(data + i * column_nullable->get_max_row_byte_size(), 0);
    }

    column_nullable->serialize_vec(data_strs, input_rows_count, max_row_byte_size);

    ColumnNullable::MutablePtr result_column =
            ColumnNullable::create(ColumnString::create(), ColumnUInt8::create());
    result_column->deserialize_vec(data_strs, input_rows_count);

    for (size_t i = 0; i < input_rows_count; ++i) {
        ASSERT_EQ(column_nullable->get_data_at(i), result_column->get_data_at(i))
                << fmt::format("source {}\nresult {}\n", column_nullable->get_data_at(i),
                               result_column->get_data_at(i));
    }
}

TEST(ColumnNullableSerializationTest, column_nullable_column_decimal) {
    const size_t input_rows_count = 4096 * 1000;
    ColumnNullable::Ptr column_nullable = create_column_nullable<Decimal64>(input_rows_count);
    Arena arena(4096);

    const size_t max_row_byte_size = column_nullable->get_max_row_byte_size();
    auto data = arena.alloc(input_rows_count * column_nullable->get_max_row_byte_size());
    std::vector<StringRef> data_strs(input_rows_count);
    for (size_t i = 0; i < input_rows_count; ++i) {
        data_strs[i] = StringRef(data + i * column_nullable->get_max_row_byte_size(), 0);
    }

    column_nullable->serialize_vec(data_strs, input_rows_count, max_row_byte_size);

    ColumnNullable::MutablePtr result_column =
            ColumnNullable::create(ColumnDecimal64::create(0, 6), ColumnUInt8::create());
    result_column->deserialize_vec(data_strs, input_rows_count);

    for (size_t i = 0; i < input_rows_count; ++i) {
        ASSERT_EQ(column_nullable->get_data_at(i), result_column->get_data_at(i))
                << fmt::format("source {}\nresult {}\n", column_nullable->get_data_at(i),
                               result_column->get_data_at(i));
    }
}

TEST(ColumnNullableSerializationTest, column_nullable_column_decimal_all_null) {
    const size_t input_rows_count = 4096 * 1000;
    ColumnNullable::Ptr column_nullable = create_column_nullable<Decimal64>(input_rows_count, true);
    Arena arena(4096);

    const size_t max_row_byte_size = column_nullable->get_max_row_byte_size();
    auto data = arena.alloc(input_rows_count * column_nullable->get_max_row_byte_size());
    std::vector<StringRef> data_strs(input_rows_count);
    for (size_t i = 0; i < input_rows_count; ++i) {
        data_strs[i] = StringRef(data + i * column_nullable->get_max_row_byte_size(), 0);
    }

    column_nullable->serialize_vec(data_strs, input_rows_count, max_row_byte_size);

    ColumnNullable::MutablePtr result_column =
            ColumnNullable::create(ColumnDecimal64::create(0, 6), ColumnUInt8::create());
    result_column->deserialize_vec(data_strs, input_rows_count);

    for (size_t i = 0; i < input_rows_count; ++i) {
        ASSERT_EQ(column_nullable->get_data_at(i), result_column->get_data_at(i))
                << fmt::format("source {}\nresult {}\n", column_nullable->get_data_at(i),
                               result_column->get_data_at(i));
    }
}

TEST(ColumnNullableSerializationTest, column_nullable_column_decimal_all_not_null) {
    const size_t input_rows_count = 4096 * 1000;
    ColumnNullable::Ptr column_nullable =
            create_column_nullable<Decimal64>(input_rows_count, false, true);
    Arena arena(4096);

    const size_t max_row_byte_size = column_nullable->get_max_row_byte_size();
    auto data = arena.alloc(input_rows_count * column_nullable->get_max_row_byte_size());
    std::vector<StringRef> data_strs(input_rows_count);
    for (size_t i = 0; i < input_rows_count; ++i) {
        data_strs[i] = StringRef(data + i * column_nullable->get_max_row_byte_size(), 0);
    }

    column_nullable->serialize_vec(data_strs, input_rows_count, max_row_byte_size);

    ColumnNullable::MutablePtr result_column =
            ColumnNullable::create(ColumnDecimal64::create(0, 6), ColumnUInt8::create());
    result_column->deserialize_vec(data_strs, input_rows_count);

    for (size_t i = 0; i < input_rows_count; ++i) {
        ASSERT_EQ(column_nullable->get_data_at(i), result_column->get_data_at(i))
                << fmt::format("source {}\nresult {}\n", column_nullable->get_data_at(i),
                               result_column->get_data_at(i));
    }
}

TEST(ColumnNullableSerializationTest, multiple_columns) {
    const size_t input_rows_count = 4096 * 100;
    auto column_nullable_decimal64 = create_column_nullable<Decimal64>(input_rows_count);
    auto column_nullable_int64 = create_column_nullable<Int64>(input_rows_count);
    auto column_nullable_string = create_column_nullable<String>(input_rows_count);

    size_t max_row_byte_size = 0;
    max_row_byte_size += column_nullable_decimal64->get_max_row_byte_size();
    max_row_byte_size += column_nullable_int64->get_max_row_byte_size();
    max_row_byte_size += column_nullable_string->get_max_row_byte_size();

    Arena arena(4096);
    auto data = arena.alloc(input_rows_count * max_row_byte_size);

    std::vector<StringRef> data_strs(input_rows_count);
    for (size_t i = 0; i < input_rows_count; ++i) {
        data_strs[i] = StringRef(data + i * max_row_byte_size, 0);
    }

    column_nullable_decimal64->serialize_vec(data_strs, input_rows_count, max_row_byte_size);
    column_nullable_int64->serialize_vec(data_strs, input_rows_count, max_row_byte_size);
    column_nullable_string->serialize_vec(data_strs, input_rows_count, max_row_byte_size);

    ColumnNullable::MutablePtr result_column_decimal64 =
            ColumnNullable::create(ColumnDecimal64::create(0, 6), ColumnUInt8::create());
    ColumnNullable::MutablePtr result_column_int64 =
            ColumnNullable::create(ColumnInt64::create(), ColumnUInt8::create());
    ColumnNullable::MutablePtr result_column_string =
            ColumnNullable::create(ColumnString::create(), ColumnUInt8::create());

    result_column_decimal64->deserialize_vec(data_strs, input_rows_count);
    result_column_int64->deserialize_vec(data_strs, input_rows_count);
    result_column_string->deserialize_vec(data_strs, input_rows_count);

    for (size_t i = 0; i < input_rows_count; ++i) {
        ASSERT_EQ(column_nullable_decimal64->get_data_at(i),
                  result_column_decimal64->get_data_at(i))
                << fmt::format("source {}\nresult {}\n", column_nullable_decimal64->get_data_at(i),
                               result_column_decimal64->get_data_at(i));
        ASSERT_EQ(column_nullable_int64->get_data_at(i), result_column_int64->get_data_at(i))
                << fmt::format("source {}\nresult {}\n", column_nullable_int64->get_data_at(i),
                               result_column_int64->get_data_at(i));
        ASSERT_EQ(column_nullable_string->get_data_at(i), result_column_string->get_data_at(i))
                << fmt::format("source {}\nresult {}\n", column_nullable_string->get_data_at(i),
                               result_column_string->get_data_at(i));
    }
}

TEST(ColumnNullableSerializationTest, multiple_columns_all_null) {
    const size_t input_rows_count = 4096 * 100;
    auto column_nullable_decimal64 = create_column_nullable<Decimal64>(input_rows_count, true);
    auto column_nullable_int64 = create_column_nullable<Int64>(input_rows_count, true);
    auto column_nullable_string = create_column_nullable<String>(input_rows_count, true);

    size_t max_row_byte_size = 0;
    max_row_byte_size += column_nullable_decimal64->get_max_row_byte_size();
    max_row_byte_size += column_nullable_int64->get_max_row_byte_size();
    max_row_byte_size += column_nullable_string->get_max_row_byte_size();

    Arena arena(4096);
    auto data = arena.alloc(input_rows_count * max_row_byte_size);

    std::vector<StringRef> data_strs(input_rows_count);
    for (size_t i = 0; i < input_rows_count; ++i) {
        data_strs[i] = StringRef(data + i * max_row_byte_size, 0);
    }

    column_nullable_decimal64->serialize_vec(data_strs, input_rows_count, max_row_byte_size);
    column_nullable_int64->serialize_vec(data_strs, input_rows_count, max_row_byte_size);
    column_nullable_string->serialize_vec(data_strs, input_rows_count, max_row_byte_size);

    ColumnNullable::MutablePtr result_column_decimal64 =
            ColumnNullable::create(ColumnDecimal64::create(0, 6), ColumnUInt8::create());
    ColumnNullable::MutablePtr result_column_int64 =
            ColumnNullable::create(ColumnInt64::create(), ColumnUInt8::create());
    ColumnNullable::MutablePtr result_column_string =
            ColumnNullable::create(ColumnString::create(), ColumnUInt8::create());

    result_column_decimal64->deserialize_vec(data_strs, input_rows_count);
    result_column_int64->deserialize_vec(data_strs, input_rows_count);
    result_column_string->deserialize_vec(data_strs, input_rows_count);

    for (size_t i = 0; i < input_rows_count; ++i) {
        ASSERT_EQ(column_nullable_decimal64->get_data_at(i),
                  result_column_decimal64->get_data_at(i))
                << fmt::format("source {}\nresult {}\n", column_nullable_decimal64->get_data_at(i),
                               result_column_decimal64->get_data_at(i));
        ASSERT_EQ(column_nullable_int64->get_data_at(i), result_column_int64->get_data_at(i))
                << fmt::format("source {}\nresult {}\n", column_nullable_int64->get_data_at(i),
                               result_column_int64->get_data_at(i));
        ASSERT_EQ(column_nullable_string->get_data_at(i), result_column_string->get_data_at(i))
                << fmt::format("source {}\nresult {}\n", column_nullable_string->get_data_at(i),
                               result_column_string->get_data_at(i));
    }
}

TEST(ColumnNullableSerializationTest, multiple_columns_all_not_null) {
    const size_t input_rows_count = 4096 * 100;
    auto column_nullable_decimal64 =
            create_column_nullable<Decimal64>(input_rows_count, false, true);
    auto column_nullable_int64 = create_column_nullable<Int64>(input_rows_count, false, true);
    auto column_nullable_string = create_column_nullable<String>(input_rows_count, false, true);

    size_t max_row_byte_size = 0;
    max_row_byte_size += column_nullable_decimal64->get_max_row_byte_size();
    max_row_byte_size += column_nullable_int64->get_max_row_byte_size();
    max_row_byte_size += column_nullable_string->get_max_row_byte_size();

    Arena arena(4096);
    auto data = arena.alloc(input_rows_count * max_row_byte_size);

    std::vector<StringRef> data_strs(input_rows_count);
    for (size_t i = 0; i < input_rows_count; ++i) {
        data_strs[i] = StringRef(data + i * max_row_byte_size, 0);
    }

    column_nullable_decimal64->serialize_vec(data_strs, input_rows_count, max_row_byte_size);
    column_nullable_int64->serialize_vec(data_strs, input_rows_count, max_row_byte_size);
    column_nullable_string->serialize_vec(data_strs, input_rows_count, max_row_byte_size);

    ColumnNullable::MutablePtr result_column_decimal64 =
            ColumnNullable::create(ColumnDecimal64::create(0, 6), ColumnUInt8::create());
    ColumnNullable::MutablePtr result_column_int64 =
            ColumnNullable::create(ColumnInt64::create(), ColumnUInt8::create());
    ColumnNullable::MutablePtr result_column_string =
            ColumnNullable::create(ColumnString::create(), ColumnUInt8::create());

    result_column_decimal64->deserialize_vec(data_strs, input_rows_count);
    result_column_int64->deserialize_vec(data_strs, input_rows_count);
    result_column_string->deserialize_vec(data_strs, input_rows_count);

    for (size_t i = 0; i < input_rows_count; ++i) {
        ASSERT_EQ(column_nullable_decimal64->get_data_at(i),
                  result_column_decimal64->get_data_at(i))
                << fmt::format("source {}\nresult {}\n", column_nullable_decimal64->get_data_at(i),
                               result_column_decimal64->get_data_at(i));
        ASSERT_EQ(column_nullable_int64->get_data_at(i), result_column_int64->get_data_at(i))
                << fmt::format("source {}\nresult {}\n", column_nullable_int64->get_data_at(i),
                               result_column_int64->get_data_at(i));
        ASSERT_EQ(column_nullable_string->get_data_at(i), result_column_string->get_data_at(i))
                << fmt::format("source {}\nresult {}\n", column_nullable_string->get_data_at(i),
                               result_column_string->get_data_at(i));
    }
}