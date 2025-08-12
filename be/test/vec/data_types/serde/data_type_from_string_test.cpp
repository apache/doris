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

#include <memory>

#include "gtest/gtest.h"
#include "runtime/primitive_type.h"
#include "testutil/mock/mock_runtime_state.h"
#include "vec/common/string_ref.h"
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
#include "vec/data_types/serde/data_type_serde.h"

namespace doris::vectorized {

struct DataTypeSerDeFromStringTest : public ::testing::Test {
    void SetUp() override {
        options.converted_from_string = true;
        options.escape_char = '\\';
        options.timezone = &state.timezone_obj();
    }

    DataTypeSerDe::FormatOptions options;
    MockRuntimeState state;
};

TEST_F(DataTypeSerDeFromStringTest, array) {
    auto type = std::make_shared<DataTypeArray>(
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>()));

    auto serde = type->get_serde();

    {
        auto column = type->create_column();

        std::string str = R"([1,2,3])";

        StringRef str_ref {str};

        EXPECT_TRUE(serde->from_string(str_ref, *column, options));

        std::cout << type->to_string(*column, 0) << std::endl;
        EXPECT_EQ(type->to_string(*column, 0), "[1, 2, 3]");
    }

    {
        auto column = type->create_column();

        std::string str = R"([1,2,3])";

        StringRef str_ref {str};

        EXPECT_TRUE(serde->from_string_strict_mode(str_ref, *column, options));

        std::cout << type->to_string(*column, 0) << std::endl;
        EXPECT_EQ(type->to_string(*column, 0), "[1, 2, 3]");
    }

    {
        auto column = type->create_column();

        std::string str = R"([,,])";

        StringRef str_ref {str};

        EXPECT_TRUE(serde->from_string(str_ref, *column, options));

        std::cout << type->to_string(*column, 0) << std::endl;
        EXPECT_EQ(type->to_string(*column, 0), "[NULL, NULL, NULL]");
    }

    {
        auto column = type->create_column();

        std::string str = R"([,,])";

        StringRef str_ref {str};

        EXPECT_FALSE(serde->from_string_strict_mode(str_ref, *column, options));
    }
}

TEST_F(DataTypeSerDeFromStringTest, bitmap) {
    auto type = std::make_shared<DataTypeBitMap>();
    auto serde = type->get_serde();
    std::string str = "0x0102030405060708090a0b0c0d0e0f10111213141516171819";
    auto str_ref = StringRef(str);

    {
        auto column = type->create_column();
        EXPECT_FALSE(serde->from_string(str_ref, *column, options));
    }
    {
        auto column = type->create_column();
        EXPECT_FALSE(serde->from_string_strict_mode(str_ref, *column, options));
    }
}

TEST_F(DataTypeSerDeFromStringTest, datev1) {
    auto type = std::make_shared<DataTypeDate>();
    auto serde = type->get_serde();
    std::string str = "2023-10-01";
    auto str_ref = StringRef(str);

    {
        auto column = type->create_column();
        EXPECT_TRUE(serde->from_string(str_ref, *column, options));
        std::cout << type->to_string(*column, 0) << std::endl;
        EXPECT_EQ(type->to_string(*column, 0), "2023-10-01");
    }
    {
        auto column = type->create_column();
        EXPECT_TRUE(serde->from_string_strict_mode(str_ref, *column, options));
        std::cout << type->to_string(*column, 0) << std::endl;
        EXPECT_EQ(type->to_string(*column, 0), "2023-10-01");
    }

    str = "abc";
    str_ref = StringRef(str);

    {
        auto column = type->create_column();
        EXPECT_FALSE(serde->from_string(str_ref, *column, options));
    }
    {
        auto column = type->create_column();
        EXPECT_FALSE(serde->from_string_strict_mode(str_ref, *column, options));
    }
}

TEST_F(DataTypeSerDeFromStringTest, datetimev1) {
    auto type = std::make_shared<DataTypeDateTime>();
    auto serde = type->get_serde();
    std::string str = "2023-10-01 12:34:56";
    auto str_ref = StringRef(str);

    {
        auto column = type->create_column();
        EXPECT_TRUE(serde->from_string(str_ref, *column, options));
        std::cout << type->to_string(*column, 0) << std::endl;
        EXPECT_EQ(type->to_string(*column, 0), "2023-10-01 12:34:56");
    }
    {
        auto column = type->create_column();
        EXPECT_TRUE(serde->from_string_strict_mode(str_ref, *column, options));
        std::cout << type->to_string(*column, 0) << std::endl;
        EXPECT_EQ(type->to_string(*column, 0), "2023-10-01 12:34:56");
    }

    str = "abc";
    str_ref = StringRef(str);

    {
        auto column = type->create_column();
        EXPECT_FALSE(serde->from_string(str_ref, *column, options));
    }
    {
        auto column = type->create_column();
        EXPECT_FALSE(serde->from_string_strict_mode(str_ref, *column, options));
    }
}

TEST_F(DataTypeSerDeFromStringTest, datev2) {
    auto type = std::make_shared<DataTypeDateV2>();
    auto serde = type->get_serde();
    std::string str = "2023-10-01";
    auto str_ref = StringRef(str);
    {
        auto column = type->create_column();
        EXPECT_TRUE(serde->from_string(str_ref, *column, options));
        std::cout << type->to_string(*column, 0) << std::endl;
        EXPECT_EQ(type->to_string(*column, 0), "2023-10-01");
    }
    {
        auto column = type->create_column();
        EXPECT_TRUE(serde->from_string_strict_mode(str_ref, *column, options));
        std::cout << type->to_string(*column, 0) << std::endl;
        EXPECT_EQ(type->to_string(*column, 0), "2023-10-01");
    }
    str = "abc";
    str_ref = StringRef(str);
    {
        auto column = type->create_column();
        EXPECT_FALSE(serde->from_string(str_ref, *column, options));
    }
    // {
    //     auto column = type->create_column();
    //     EXPECT_FALSE(serde->from_string_strict_mode(str_ref, *column, options));
    // }
}

TEST_F(DataTypeSerDeFromStringTest, datetimev2) {
    auto type = std::make_shared<DataTypeDateTimeV2>(3);
    auto serde = type->get_serde();
    std::string str = "2023-10-01 12:34:56.789";
    auto str_ref = StringRef(str);

    {
        auto column = type->create_column();
        EXPECT_TRUE(serde->from_string(str_ref, *column, options));
        std::cout << type->to_string(*column, 0) << std::endl;
        EXPECT_EQ(type->to_string(*column, 0), "2023-10-01 12:34:56.789");
    }
    {
        auto column = type->create_column();
        EXPECT_TRUE(serde->from_string_strict_mode(str_ref, *column, options));
        std::cout << type->to_string(*column, 0) << std::endl;
        EXPECT_EQ(type->to_string(*column, 0), "2023-10-01 12:34:56.789");
    }
    str = "abc";
    str_ref = StringRef(str);

    {
        auto column = type->create_column();
        EXPECT_FALSE(serde->from_string(str_ref, *column, options));
    }
    {
        auto column = type->create_column();
        EXPECT_FALSE(serde->from_string_strict_mode(str_ref, *column, options));
    }
}

TEST_F(DataTypeSerDeFromStringTest, decimal) {
    auto type = std::make_shared<DataTypeDecimal128>(20, 9);
    auto serde = type->get_serde();

    std::string str = "123456789.123456789";
    auto str_ref = StringRef(str);

    {
        auto column = type->create_column();
        EXPECT_TRUE(serde->from_string(str_ref, *column, options));
        std::cout << type->to_string(*column, 0) << std::endl;
        EXPECT_EQ(type->to_string(*column, 0), "123456789.123456789");
    }
    {
        auto column = type->create_column();
        EXPECT_TRUE(serde->from_string_strict_mode(str_ref, *column, options));
        std::cout << type->to_string(*column, 0) << std::endl;
        EXPECT_EQ(type->to_string(*column, 0), "123456789.123456789");
    }

    str = "abc";
    str_ref = StringRef(str);

    {
        auto column = type->create_column();
        EXPECT_FALSE(serde->from_string(str_ref, *column, options));
    }
    {
        auto column = type->create_column();
        EXPECT_FALSE(serde->from_string_strict_mode(str_ref, *column, options));
    }
}

TEST_F(DataTypeSerDeFromStringTest, hll) {
    auto type = std::make_shared<DataTypeHLL>();
    auto serde = type->get_serde();

    std::string str = "HLL:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0";
    auto str_ref = StringRef(str);

    {
        auto column = type->create_column();
        EXPECT_TRUE(serde->from_string(str_ref, *column, options));
    }
}

TEST_F(DataTypeSerDeFromStringTest, ipv4) {
    auto type = std::make_shared<DataTypeIPv4>();
    auto serde = type->get_serde();

    std::string str = "198.0.0.1";

    auto str_ref = StringRef(str);

    {
        auto column = type->create_column();
        EXPECT_TRUE(serde->from_string(str_ref, *column, options));
        std::cout << type->to_string(*column, 0) << std::endl;
        EXPECT_EQ(type->to_string(*column, 0), "198.0.0.1");
    }

    {
        auto column = type->create_column();
        EXPECT_TRUE(serde->from_string_strict_mode(str_ref, *column, options));
        std::cout << type->to_string(*column, 0) << std::endl;
        EXPECT_EQ(type->to_string(*column, 0), "198.0.0.1");
    }

    str = "abc";
    str_ref = StringRef(str);

    {
        auto column = type->create_column();
        EXPECT_FALSE(serde->from_string(str_ref, *column, options));
    }
    {
        auto column = type->create_column();
        EXPECT_FALSE(serde->from_string_strict_mode(str_ref, *column, options));
    }
}

TEST_F(DataTypeSerDeFromStringTest, ipv6) {
    auto type = std::make_shared<DataTypeIPv6>();
    auto serde = type->get_serde();

    std::string str = "2001:0db8:85a3:0000:0000:8a2e:0370:7334";

    auto str_ref = StringRef(str);

    {
        auto column = type->create_column();
        EXPECT_TRUE(serde->from_string(str_ref, *column, options));
        std::cout << type->to_string(*column, 0) << std::endl;
        EXPECT_EQ(type->to_string(*column, 0), "2001:db8:85a3::8a2e:370:7334");
    }

    {
        auto column = type->create_column();
        EXPECT_TRUE(serde->from_string_strict_mode(str_ref, *column, options));
        std::cout << type->to_string(*column, 0) << std::endl;
        EXPECT_EQ(type->to_string(*column, 0), "2001:db8:85a3::8a2e:370:7334");
    }

    str = "abc";
    str_ref = StringRef(str);

    {
        auto column = type->create_column();
        EXPECT_FALSE(serde->from_string(str_ref, *column, options));
    }
    {
        auto column = type->create_column();
        EXPECT_FALSE(serde->from_string_strict_mode(str_ref, *column, options));
    }
}

TEST_F(DataTypeSerDeFromStringTest, json) {
    auto type = std::make_shared<DataTypeJsonb>();
    auto serde = type->get_serde();

    std::string str = R"({"key1": "value1", "key2": 2, "key3": [1, 2, 3]})";
    auto str_ref = StringRef(str);

    {
        auto column = type->create_column();
        EXPECT_TRUE(serde->from_string(str_ref, *column, options));
        std::cout << type->to_string(*column, 0) << std::endl;
        EXPECT_EQ(type->to_string(*column, 0), R"({"key1":"value1","key2":2,"key3":[1,2,3]})");
    }

    {
        auto column = type->create_column();
        EXPECT_TRUE(serde->from_string_strict_mode(str_ref, *column, options));
        std::cout << type->to_string(*column, 0) << std::endl;
        EXPECT_EQ(type->to_string(*column, 0), R"({"key1":"value1","key2":2,"key3":[1,2,3]})");
    }

    str = "abc";
    str_ref = StringRef(str);

    {
        auto column = type->create_column();
        EXPECT_FALSE(serde->from_string(str_ref, *column, options));
    }
    {
        auto column = type->create_column();
        EXPECT_FALSE(serde->from_string_strict_mode(str_ref, *column, options));
    }
}

TEST_F(DataTypeSerDeFromStringTest, map) {
    auto type = std::make_shared<DataTypeMap>(
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>()),
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()));
    auto serde = type->get_serde();

    std::string str = R"({"key1": "value1", "key2": 2, "key3": [1, 2, 3]})";
    auto str_ref = StringRef(str);

    {
        auto column = type->create_column();
        EXPECT_TRUE(serde->from_string(str_ref, *column, options));
        std::cout << type->to_string(*column, 0) << std::endl;
        EXPECT_EQ(type->to_string(*column, 0), R"({null:"value1", null:"2", null:"[1, 2, 3]"})");
    }

    {
        auto column = type->create_column();
        EXPECT_FALSE(serde->from_string_strict_mode(str_ref, *column, options));
    }

    str = "abc";
    str_ref = StringRef(str);

    {
        auto column = type->create_column();
        EXPECT_FALSE(serde->from_string(str_ref, *column, options));
    }
    {
        auto column = type->create_column();
        EXPECT_FALSE(serde->from_string_strict_mode(str_ref, *column, options));
    }
}

TEST_F(DataTypeSerDeFromStringTest, nullable) {
    auto type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>());
    auto serde = type->get_serde();

    std::string str = "123";
    auto str_ref = StringRef(str);

    {
        auto column = type->create_column();
        EXPECT_TRUE(serde->from_string(str_ref, *column, options));
        std::cout << type->to_string(*column, 0) << std::endl;
        EXPECT_EQ(type->to_string(*column, 0), "123");
    }

    {
        auto column = type->create_column();
        EXPECT_TRUE(serde->from_string_strict_mode(str_ref, *column, options));
        std::cout << type->to_string(*column, 0) << std::endl;
        EXPECT_EQ(type->to_string(*column, 0), "123");
    }

    str = "abc";
    str_ref = StringRef(str);

    {
        auto column = type->create_column();
        EXPECT_TRUE(serde->from_string(str_ref, *column, options));
    }
    {
        auto column = type->create_column();
        EXPECT_FALSE(serde->from_string_strict_mode(str_ref, *column, options));
    }
}

TEST_F(DataTypeSerDeFromStringTest, BoolTest) {
    auto type = std::make_shared<DataTypeBool>();
    auto serde = type->get_serde();

    std::string str = "true";
    auto str_ref = StringRef(str);

    {
        auto column = type->create_column();
        EXPECT_TRUE(serde->from_string(str_ref, *column, options));
        std::cout << type->to_string(*column, 0) << std::endl;
        EXPECT_EQ(type->to_string(*column, 0), "1");
    }

    {
        auto column = type->create_column();
        EXPECT_TRUE(serde->from_string_strict_mode(str_ref, *column, options));
        std::cout << type->to_string(*column, 0) << std::endl;
        EXPECT_EQ(type->to_string(*column, 0), "1");
    }

    str = "false";
    str_ref = StringRef(str);

    {
        auto column = type->create_column();
        EXPECT_TRUE(serde->from_string(str_ref, *column, options));
        std::cout << type->to_string(*column, 0) << std::endl;
        EXPECT_EQ(type->to_string(*column, 0), "0");
    }

    {
        auto column = type->create_column();
        EXPECT_TRUE(serde->from_string_strict_mode(str_ref, *column, options));
        std::cout << type->to_string(*column, 0) << std::endl;
        EXPECT_EQ(type->to_string(*column, 0), "0");
    }

    str = "abc";
    str_ref = StringRef(str);

    {
        auto column = type->create_column();
        EXPECT_FALSE(serde->from_string(str_ref, *column, options));
    }
    {
        auto column = type->create_column();
        EXPECT_FALSE(serde->from_string_strict_mode(str_ref, *column, options));
    }
}

TEST_F(DataTypeSerDeFromStringTest, intTest) {
    auto type = std::make_shared<DataTypeInt64>();
    auto serde = type->get_serde();

    std::string str = "1234567890";
    auto str_ref = StringRef(str);

    {
        auto column = type->create_column();
        EXPECT_TRUE(serde->from_string(str_ref, *column, options));
        std::cout << type->to_string(*column, 0) << std::endl;
        EXPECT_EQ(type->to_string(*column, 0), "1234567890");
    }

    {
        auto column = type->create_column();
        EXPECT_TRUE(serde->from_string_strict_mode(str_ref, *column, options));
        std::cout << type->to_string(*column, 0) << std::endl;
        EXPECT_EQ(type->to_string(*column, 0), "1234567890");
    }

    str = "abc";
    str_ref = StringRef(str);

    {
        auto column = type->create_column();
        EXPECT_FALSE(serde->from_string(str_ref, *column, options));
    }
    {
        auto column = type->create_column();
        EXPECT_FALSE(serde->from_string_strict_mode(str_ref, *column, options));
    }
}

TEST_F(DataTypeSerDeFromStringTest, floatTest) {
    auto type = std::make_shared<DataTypeFloat64>();
    auto serde = type->get_serde();

    std::string str = "123.456";
    auto str_ref = StringRef(str);

    {
        auto column = type->create_column();
        EXPECT_TRUE(serde->from_string(str_ref, *column, options));
        std::cout << type->to_string(*column, 0) << std::endl;
        EXPECT_EQ(type->to_string(*column, 0), "123.456");
    }

    {
        auto column = type->create_column();
        EXPECT_TRUE(serde->from_string_strict_mode(str_ref, *column, options));
        std::cout << type->to_string(*column, 0) << std::endl;
        EXPECT_EQ(type->to_string(*column, 0), "123.456");
    }

    str = "abc";
    str_ref = StringRef(str);

    {
        auto column = type->create_column();
        EXPECT_FALSE(serde->from_string(str_ref, *column, options));
    }
    {
        auto column = type->create_column();
        EXPECT_FALSE(serde->from_string_strict_mode(str_ref, *column, options));
    }
}

TEST_F(DataTypeSerDeFromStringTest, stringTest) {
    auto type = std::make_shared<DataTypeString>();
    auto serde = type->get_serde();
    std::string str = "Hello, World!";
    auto str_ref = StringRef(str);

    {
        auto column = type->create_column();
        EXPECT_TRUE(serde->from_string(str_ref, *column, options));
        std::cout << type->to_string(*column, 0) << std::endl;
        EXPECT_EQ(type->to_string(*column, 0), "Hello, World!");
    }
    {
        auto column = type->create_column();
        EXPECT_TRUE(serde->from_string_strict_mode(str_ref, *column, options));
        std::cout << type->to_string(*column, 0) << std::endl;
        EXPECT_EQ(type->to_string(*column, 0), "Hello, World!");
    }
}

TEST_F(DataTypeSerDeFromStringTest, structTest) {
    auto type = std::make_shared<DataTypeStruct>(
            DataTypes {std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>()),
                       std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>())},
            Strings {"field1", "field2"});
    auto serde = type->get_serde();

    std::string str = R"({"field1": 123, "field2": "Hello"})";

    {
        auto str_ref = StringRef(str);
        auto column = type->create_column();
        EXPECT_TRUE(serde->from_string(str_ref, *column, options));
        std::cout << type->to_string(*column, 0) << std::endl;
        EXPECT_EQ(type->to_string(*column, 0), R"({123, Hello})");
    }

    {
        auto str_ref = StringRef(str);
        auto column = type->create_column();
        EXPECT_TRUE(serde->from_string_strict_mode(str_ref, *column, options));
        std::cout << type->to_string(*column, 0) << std::endl;
        EXPECT_EQ(type->to_string(*column, 0), R"({123, Hello})");
    }
    str = "abc";
    auto str_ref = StringRef(str);

    {
        auto column = type->create_column();
        EXPECT_FALSE(serde->from_string(str_ref, *column, options));
    }
    {
        auto column = type->create_column();
        EXPECT_FALSE(serde->from_string_strict_mode(str_ref, *column, options));
    }
}

TEST_F(DataTypeSerDeFromStringTest, timeTest) {
    auto type = std::make_shared<DataTypeTimeV2>();
    auto serde = type->get_serde();
    std::string str = "12:34:56";
    auto str_ref = StringRef(str);
    {
        auto column = type->create_column();
        EXPECT_TRUE(serde->from_string(str_ref, *column, options));
        std::cout << type->to_string(*column, 0) << std::endl;
        EXPECT_EQ(type->to_string(*column, 0), "12:34:56");
    }
    {
        auto column = type->create_column();
        EXPECT_TRUE(serde->from_string_strict_mode(str_ref, *column, options));
        std::cout << type->to_string(*column, 0) << std::endl;
        EXPECT_EQ(type->to_string(*column, 0), "12:34:56");
    }

    str = "abc";
    str_ref = StringRef(str);

    {
        auto column = type->create_column();
        EXPECT_FALSE(serde->from_string(str_ref, *column, options));
    }
    {
        auto column = type->create_column();
        EXPECT_FALSE(serde->from_string_strict_mode(str_ref, *column, options));
    }
}

} // namespace doris::vectorized