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

#include "column_nullable_test.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/columns_number.h"
#include "vec/common/arena.h"
#include "vec/common/string_ref.h"
#include "vec/core/types.h"

using namespace doris;
using namespace doris::vectorized;

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