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

#include <gtest/gtest.h>

#include <memory>

#include "orc/ColumnPrinter.hh"
#include "vec/columns/column_struct.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/exec/format/orc/vorc_reader.cpp"

namespace doris {
namespace vectorized {
class OrcReaderConvertToOrcLiteralTest : public ::testing::Test {
protected:
    void SetUp() override {}

    void TearDown() override {}
};

TEST_F(OrcReaderConvertToOrcLiteralTest, ConvertTypesTest) {
    // TINYINT test
    {
        int8_t tiny_value = 127;
        StringRef literal_data(reinterpret_cast<char*>(&tiny_value), sizeof(tiny_value));
        auto orc_type_ptr = createPrimitiveType(orc::TypeKind::BYTE);
        auto [success, literal] =
                convert_to_orc_literal<TYPE_TINYINT>(orc_type_ptr.get(), literal_data, 0, 0);
        ASSERT_TRUE(success);
        ASSERT_EQ(literal.getLong(), 127);
    }

    // SMALLINT test
    {
        int16_t small_value = 32000;
        StringRef literal_data(reinterpret_cast<char*>(&small_value), sizeof(small_value));
        auto orc_type_ptr = createPrimitiveType(orc::TypeKind::SHORT);
        auto [success, literal] =
                convert_to_orc_literal<TYPE_SMALLINT>(orc_type_ptr.get(), literal_data, 0, 0);
        ASSERT_TRUE(success);
        ASSERT_EQ(literal.getLong(), 32000);
    }

    // INT test
    {
        int32_t int_value = 2147483647;
        StringRef literal_data(reinterpret_cast<char*>(&int_value), sizeof(int_value));
        auto orc_type_ptr = createPrimitiveType(orc::TypeKind::INT);
        auto [success, literal] =
                convert_to_orc_literal<TYPE_INT>(orc_type_ptr.get(), literal_data, 0, 0);
        ASSERT_TRUE(success);
        ASSERT_EQ(literal.getLong(), 2147483647);
    }

    // BIGINT test
    {
        int64_t big_value = 9223372036854775807LL;
        StringRef literal_data(reinterpret_cast<char*>(&big_value), sizeof(big_value));
        auto orc_type_ptr = createPrimitiveType(orc::TypeKind::LONG);
        auto [success, literal] =
                convert_to_orc_literal<TYPE_BIGINT>(orc_type_ptr.get(), literal_data, 0, 0);
        ASSERT_TRUE(success);
        ASSERT_EQ(literal.getLong(), 9223372036854775807LL);
    }
    // FLOAT test
    {
        float float_value = 3.14159f;
        StringRef literal_data(reinterpret_cast<char*>(&float_value), sizeof(float_value));
        auto orc_type_ptr = createPrimitiveType(orc::TypeKind::FLOAT);
        auto [success, literal] =
                convert_to_orc_literal<TYPE_FLOAT>(orc_type_ptr.get(), literal_data, 0, 0);
        ASSERT_TRUE(success);
        ASSERT_NEAR(literal.getFloat(), 3.14159f, 0.0001);
    }

    // DOUBLE test
    {
        double double_value = 3.14159265358979323846;
        StringRef literal_data(reinterpret_cast<char*>(&double_value), sizeof(double_value));
        auto orc_type_ptr = createPrimitiveType(orc::TypeKind::DOUBLE);
        auto [success, literal] =
                convert_to_orc_literal<TYPE_DOUBLE>(orc_type_ptr.get(), literal_data, 0, 0);
        ASSERT_TRUE(success);
        ASSERT_DOUBLE_EQ(literal.getFloat(), 3.14159265358979323846);
    }
    // STRING test
    {
        std::string str_value = "Hello, World!";
        StringRef literal_data(str_value.data(), str_value.size());
        auto orc_type_ptr = createPrimitiveType(orc::TypeKind::STRING);
        auto [success, literal] =
                convert_to_orc_literal<TYPE_STRING>(orc_type_ptr.get(), literal_data, 0, 0);
        ASSERT_TRUE(success);
        ASSERT_EQ(std::string(literal.getString().data(), literal.getString().length()),
                  "Hello, World!");
    }

    // VARCHAR test
    {
        std::string str_value = "VARCHAR test";
        StringRef literal_data(str_value.data(), str_value.size());
        auto orc_type_ptr = createPrimitiveType(orc::TypeKind::VARCHAR);
        auto [success, literal] =
                convert_to_orc_literal<TYPE_VARCHAR>(orc_type_ptr.get(), literal_data, 0, 0);
        ASSERT_TRUE(success);
        ASSERT_EQ(std::string(literal.getString().data(), literal.getString().length()),
                  "VARCHAR test");
    }

    // DECIMAL32 test
    {
        int32_t decimal32_value = 12345;
        StringRef literal_data(reinterpret_cast<const char*>(&decimal32_value),
                               sizeof(decimal32_value));
        auto orc_type_ptr = createPrimitiveType(orc::TypeKind::DECIMAL);
        auto [success, literal] =
                convert_to_orc_literal<TYPE_DECIMAL32>(orc_type_ptr.get(), literal_data, 9, 4);
        ASSERT_TRUE(success);
        ASSERT_EQ(literal.getDecimal().toString(), "1.2345");
    }

    // DECIMAL64 test
    {
        int64_t decimal64_value = 123456789012345LL;
        StringRef literal_data(reinterpret_cast<const char*>(&decimal64_value),
                               sizeof(decimal64_value));
        auto orc_type_ptr = createPrimitiveType(orc::TypeKind::DECIMAL);
        auto [success, literal] =
                convert_to_orc_literal<TYPE_DECIMAL64>(orc_type_ptr.get(), literal_data, 18, 6);
        ASSERT_TRUE(success);
        ASSERT_EQ(literal.getDecimal().toString(), "123456789.012345");
    }

    // DECIMAL128 test
    {
        int128_t decimal128_value = 1234512345;
        StringRef literal_data(reinterpret_cast<const char*>(&decimal128_value),
                               sizeof(decimal128_value));
        auto orc_type_ptr = createPrimitiveType(orc::TypeKind::DECIMAL);
        auto [success, literal] =
                convert_to_orc_literal<TYPE_DECIMAL128I>(orc_type_ptr.get(), literal_data, 38, 9);
        ASSERT_TRUE(success);
        ASSERT_EQ(literal.getDecimal().toString(), "1.234512345");
    }

    {
        // Normal date
        VecDateTimeValue date_value;
        date_value.from_date_str("2024-03-14", 10);
        StringRef literal_data(reinterpret_cast<const char*>(&date_value), sizeof(date_value));
        auto orc_type_ptr = createPrimitiveType(orc::TypeKind::DATE);
        auto [success, literal] =
                convert_to_orc_literal<TYPE_DATE>(orc_type_ptr.get(), literal_data, 0, 0);
        ASSERT_TRUE(success);

        // Verify converted day offset
        int64_t expected_days = 19796; // Day count for 2024-03-14
        ASSERT_EQ(literal.getDate(), expected_days);

        // Boundary date - minimum value
        date_value.from_date_str("0001-01-01", 10);
        literal_data = StringRef(reinterpret_cast<const char*>(&date_value), sizeof(date_value));
        std::tie(success, literal) =
                convert_to_orc_literal<TYPE_DATE>(orc_type_ptr.get(), literal_data, 0, 0);
        ASSERT_TRUE(success); //-719162
        ASSERT_EQ(literal.getDate(), -719162);

        // Boundary date - maximum value
        date_value.from_date_str("9999-12-31", 10);
        literal_data = StringRef(reinterpret_cast<const char*>(&date_value), sizeof(date_value));
        std::tie(success, literal) =
                convert_to_orc_literal<TYPE_DATE>(orc_type_ptr.get(), literal_data, 0, 0);
        ASSERT_TRUE(success); //
        ASSERT_EQ(literal.getDate(), 2932896);
    }

    // DATETIME type test
    {
        // Normal timestamp
        VecDateTimeValue datetime_value;
        datetime_value.from_date_str("2024-03-14 15:30:45", 19);
        StringRef literal_data(reinterpret_cast<const char*>(&datetime_value),
                               sizeof(datetime_value));
        auto orc_type_ptr = createPrimitiveType(orc::TypeKind::TIMESTAMP);
        auto [success, literal] =
                convert_to_orc_literal<TYPE_DATETIME>(orc_type_ptr.get(), literal_data, 0, 0);
        ASSERT_TRUE(success);

        // Verify seconds and nanoseconds
        ASSERT_EQ(literal.getTimestamp().getMillis(), 1710430245000); //

        // Midnight time
        datetime_value.from_date_str("2024-03-14 00:00:00", 19);
        literal_data =
                StringRef(reinterpret_cast<const char*>(&datetime_value), sizeof(datetime_value));
        std::tie(success, literal) =
                convert_to_orc_literal<TYPE_DATETIME>(orc_type_ptr.get(), literal_data, 0, 0);
        ASSERT_TRUE(success);
        ASSERT_EQ(literal.getTimestamp().getMillis(), 1710374400000); //

        // Leap year handling
        datetime_value.from_date_str("2024-02-29 12:00:00", 19);
        literal_data =
                StringRef(reinterpret_cast<const char*>(&datetime_value), sizeof(datetime_value));
        std::tie(success, literal) =
                convert_to_orc_literal<TYPE_DATETIME>(orc_type_ptr.get(), literal_data, 0, 0);
        ASSERT_TRUE(success);
        ASSERT_EQ(literal.getTimestamp().getMillis(), 1709208000000); //
    }

    // Type mismatch test
    {
        // Try to convert INT type to STRING
        int32_t int_value = 42;
        StringRef literal_data(reinterpret_cast<const char*>(&int_value), sizeof(int_value));
        auto orc_type_ptr = createPrimitiveType(orc::TypeKind::STRING);
        auto [success, literal] =
                convert_to_orc_literal<TYPE_INT>(orc_type_ptr.get(), literal_data, 0, 0);
        ASSERT_FALSE(success);
    }

    // Try to convert FLOAT to DOUBLE
    {
        float float_value = 3.14f;
        StringRef literal_data(reinterpret_cast<const char*>(&float_value), sizeof(float_value));
        auto orc_type_ptr = createPrimitiveType(orc::TypeKind::DOUBLE);
        auto [success, literal] =
                convert_to_orc_literal<TYPE_FLOAT>(orc_type_ptr.get(), literal_data, 0, 0);
        ASSERT_FALSE(success);
    }

    // Try to convert DATE to TIMESTAMP
    {
        VecDateTimeValue date_value;
        date_value.from_date_str("2024-03-14", 10);
        StringRef literal_data(reinterpret_cast<const char*>(&date_value), sizeof(date_value));
        auto orc_type_ptr = createPrimitiveType(orc::TypeKind::TIMESTAMP);
        auto [success, literal] =
                convert_to_orc_literal<TYPE_DATE>(orc_type_ptr.get(), literal_data, 0, 0);
        ASSERT_FALSE(success);
    }

    {
        // TINYINT -> other integer types
        int8_t tiny_value = 42;
        StringRef literal_data(reinterpret_cast<const char*>(&tiny_value), sizeof(tiny_value));

        // TINYINT -> SHORT
        {
            auto orc_type_ptr = createPrimitiveType(orc::TypeKind::SHORT);
            auto [success, literal] =
                    convert_to_orc_literal<TYPE_TINYINT>(orc_type_ptr.get(), literal_data, 0, 0);
            ASSERT_TRUE(success);
        }

        // TINYINT -> INT
        {
            auto orc_type_ptr = createPrimitiveType(orc::TypeKind::INT);
            auto [success, literal] =
                    convert_to_orc_literal<TYPE_TINYINT>(orc_type_ptr.get(), literal_data, 0, 0);
            ASSERT_TRUE(success);
        }

        // TINYINT -> LONG
        {
            auto orc_type_ptr = createPrimitiveType(orc::TypeKind::LONG);
            auto [success, literal] =
                    convert_to_orc_literal<TYPE_TINYINT>(orc_type_ptr.get(), literal_data, 0, 0);
            ASSERT_TRUE(success);
        }
    }

    // 2. Converting between floating point and integer types
    {
        // FLOAT -> integer types
        float float_value = 3.14f;
        StringRef literal_data(reinterpret_cast<const char*>(&float_value), sizeof(float_value));

        // FLOAT -> INT
        {
            auto orc_type_ptr = createPrimitiveType(orc::TypeKind::INT);
            auto [success, literal] =
                    convert_to_orc_literal<TYPE_FLOAT>(orc_type_ptr.get(), literal_data, 0, 0);
            ASSERT_FALSE(success);
        }

        // FLOAT -> LONG
        {
            auto orc_type_ptr = createPrimitiveType(orc::TypeKind::LONG);
            auto [success, literal] =
                    convert_to_orc_literal<TYPE_FLOAT>(orc_type_ptr.get(), literal_data, 0, 0);
            ASSERT_FALSE(success);
        }

        // INT -> FLOAT
        int32_t int_value = 42;
        literal_data = StringRef(reinterpret_cast<const char*>(&int_value), sizeof(int_value));
        {
            auto orc_type_ptr = createPrimitiveType(orc::TypeKind::FLOAT);
            auto [success, literal] =
                    convert_to_orc_literal<TYPE_INT>(orc_type_ptr.get(), literal_data, 0, 0);
            ASSERT_FALSE(success);
        }
    }

    // 3. Conversion between string and numeric types
    {
        // STRING -> numeric types
        std::string str_value = "123";
        StringRef literal_data(str_value.data(), str_value.size());

        // STRING -> INT
        {
            auto orc_type_ptr = createPrimitiveType(orc::TypeKind::INT);
            auto [success, literal] =
                    convert_to_orc_literal<TYPE_STRING>(orc_type_ptr.get(), literal_data, 0, 0);
            ASSERT_FALSE(success);
        }

        // STRING -> FLOAT
        {
            auto orc_type_ptr = createPrimitiveType(orc::TypeKind::FLOAT);
            auto [success, literal] =
                    convert_to_orc_literal<TYPE_STRING>(orc_type_ptr.get(), literal_data, 0, 0);
            ASSERT_FALSE(success);
        }

        // INT -> STRING
        int32_t int_value = 42;
        literal_data = StringRef(reinterpret_cast<const char*>(&int_value), sizeof(int_value));
        {
            auto orc_type_ptr = createPrimitiveType(orc::TypeKind::STRING);
            auto [success, literal] =
                    convert_to_orc_literal<TYPE_INT>(orc_type_ptr.get(), literal_data, 0, 0);
            ASSERT_FALSE(success);
        }
    }

    // 4. Conversion between date/time types and other types
    {
        // DATE -> other types
        VecDateTimeValue date_value;
        date_value.from_date_str("2024-03-14", 10);
        StringRef literal_data(reinterpret_cast<const char*>(&date_value), sizeof(date_value));

        // DATE -> INT
        {
            auto orc_type_ptr = createPrimitiveType(orc::TypeKind::INT);
            auto [success, literal] =
                    convert_to_orc_literal<TYPE_DATE>(orc_type_ptr.get(), literal_data, 0, 0);
            ASSERT_FALSE(success);
        }

        // DATE -> STRING
        {
            auto orc_type_ptr = createPrimitiveType(orc::TypeKind::STRING);
            auto [success, literal] =
                    convert_to_orc_literal<TYPE_DATE>(orc_type_ptr.get(), literal_data, 0, 0);
            ASSERT_FALSE(success);
        }

        // INT -> DATE
        int32_t int_value = 42;
        literal_data = StringRef(reinterpret_cast<const char*>(&int_value), sizeof(int_value));
        {
            auto orc_type_ptr = createPrimitiveType(orc::TypeKind::DATE);
            auto [success, literal] =
                    convert_to_orc_literal<TYPE_INT>(orc_type_ptr.get(), literal_data, 0, 0);
            ASSERT_FALSE(success);
        }
    }

    // 5. Conversion between Decimal and other types
    {
        // DECIMAL -> other types
        int128_t decimal_value = 123456789;
        StringRef literal_data(reinterpret_cast<const char*>(&decimal_value),
                               sizeof(decimal_value));

        // DECIMAL -> INT
        {
            auto orc_type_ptr = createPrimitiveType(orc::TypeKind::INT);
            auto [success, literal] = convert_to_orc_literal<TYPE_DECIMAL128I>(orc_type_ptr.get(),
                                                                               literal_data, 0, 0);
            ASSERT_FALSE(success);
        }

        // DECIMAL -> FLOAT
        {
            auto orc_type_ptr = createPrimitiveType(orc::TypeKind::FLOAT);
            auto [success, literal] = convert_to_orc_literal<TYPE_DECIMAL128I>(orc_type_ptr.get(),
                                                                               literal_data, 0, 0);
            ASSERT_FALSE(success);
        }

        // INT -> DECIMAL
        int32_t int_value = 42;
        literal_data = StringRef(reinterpret_cast<const char*>(&int_value), sizeof(int_value));
        {
            auto orc_type_ptr = createPrimitiveType(orc::TypeKind::DECIMAL);
            auto [success, literal] =
                    convert_to_orc_literal<TYPE_INT>(orc_type_ptr.get(), literal_data, 10, 2);
            ASSERT_FALSE(success);
        }
    }

    // 6. Conversion between BOOLEAN and other types
    {
        // BOOLEAN -> other types
        uint8_t bool_value = true;
        StringRef literal_data(reinterpret_cast<const char*>(&bool_value), sizeof(bool_value));

        // BOOLEAN -> INT
        {
            auto orc_type_ptr = createPrimitiveType(orc::TypeKind::INT);
            auto [success, literal] =
                    convert_to_orc_literal<TYPE_BOOLEAN>(orc_type_ptr.get(), literal_data, 0, 0);
            ASSERT_FALSE(success);
        }

        // BOOLEAN -> STRING
        {
            auto orc_type_ptr = createPrimitiveType(orc::TypeKind::STRING);
            auto [success, literal] =
                    convert_to_orc_literal<TYPE_BOOLEAN>(orc_type_ptr.get(), literal_data, 0, 0);
            ASSERT_FALSE(success);
        }

        // INT -> BOOLEAN
        int32_t int_value = 1;
        literal_data = StringRef(reinterpret_cast<const char*>(&int_value), sizeof(int_value));
        {
            auto orc_type_ptr = createPrimitiveType(orc::TypeKind::BOOLEAN);
            auto [success, literal] =
                    convert_to_orc_literal<TYPE_INT>(orc_type_ptr.get(), literal_data, 0, 0);
            ASSERT_FALSE(success);
        }
    }

    // 7. Conversion between TIMESTAMP and other types
    {
        // TIMESTAMP -> other types
        VecDateTimeValue datetime_value;
        datetime_value.from_date_str("2024-03-14 15:30:45", 19);
        StringRef literal_data(reinterpret_cast<const char*>(&datetime_value),
                               sizeof(datetime_value));

        // TIMESTAMP -> INT
        {
            auto orc_type_ptr = createPrimitiveType(orc::TypeKind::INT);
            auto [success, literal] =
                    convert_to_orc_literal<TYPE_DATETIME>(orc_type_ptr.get(), literal_data, 0, 0);
            ASSERT_FALSE(success);
        }

        // TIMESTAMP -> STRING
        {
            auto orc_type_ptr = createPrimitiveType(orc::TypeKind::STRING);
            auto [success, literal] =
                    convert_to_orc_literal<TYPE_DATETIME>(orc_type_ptr.get(), literal_data, 0, 0);
            ASSERT_FALSE(success);
        }

        // INT -> TIMESTAMP
        int64_t int_value = 1615737045;
        literal_data = StringRef(reinterpret_cast<const char*>(&int_value), sizeof(int_value));
        {
            auto orc_type_ptr = createPrimitiveType(orc::TypeKind::TIMESTAMP);
            auto [success, literal] =
                    convert_to_orc_literal<TYPE_BIGINT>(orc_type_ptr.get(), literal_data, 0, 0);
            ASSERT_FALSE(success);
        }
    }

    // 8. Conversion between VARCHAR and other types
    {
        // VARCHAR -> other types
        std::string varchar_value = "test string";
        StringRef literal_data(varchar_value.data(), varchar_value.size());

        // VARCHAR -> INT
        {
            auto orc_type_ptr = createPrimitiveType(orc::TypeKind::INT);
            auto [success, literal] =
                    convert_to_orc_literal<TYPE_VARCHAR>(orc_type_ptr.get(), literal_data, 0, 0);
            ASSERT_FALSE(success);
        }

        // VARCHAR -> DECIMAL
        {
            auto orc_type_ptr = createPrimitiveType(orc::TypeKind::DECIMAL);
            auto [success, literal] =
                    convert_to_orc_literal<TYPE_VARCHAR>(orc_type_ptr.get(), literal_data, 10, 2);
            ASSERT_FALSE(success);
        }

        // INT -> VARCHAR
        int32_t int_value = 42;
        literal_data = StringRef(reinterpret_cast<const char*>(&int_value), sizeof(int_value));
        {
            auto orc_type_ptr = createPrimitiveType(orc::TypeKind::VARCHAR);
            auto [success, literal] =
                    convert_to_orc_literal<TYPE_INT>(orc_type_ptr.get(), literal_data, 0, 0);
            ASSERT_FALSE(success);
        }
    }
}
} // namespace vectorized
} // namespace doris
