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

#include "gtest/gtest_pred_impl.h"
#include "olap/olap_common.h"
#include "vec/columns/column.h"
#include "vec/core/field.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/data_types/serde_utils.h"

namespace doris::vectorized {

/**
 * This test is used to check data type from_string behavior
 *  same string feed to data type from_string, and check the result
 */
TEST(FromStringTest, ScalaDataTypeFromString) {
    // arithmetic scala field types
    {
        // fieldType, test_string, expect_data_type_string
        using FieldType_RandStr =
                std::tuple<FieldType, std::vector<std::string>, std::vector<std::string>>;
        std::vector<FieldType_RandStr> arithmetic_scala_field_types = {
                FieldType_RandStr(FieldType::OLAP_FIELD_TYPE_BOOL, {"0", "1", "-9"},
                                  {"0", "1", ""}),
                FieldType_RandStr(FieldType::OLAP_FIELD_TYPE_TINYINT, {"127", "-128", "-190"},
                                  {"127", "-128", ""}),
                FieldType_RandStr(FieldType::OLAP_FIELD_TYPE_SMALLINT, {"32767", "32768", "-32769"},
                                  {"32767", "", ""}),
                FieldType_RandStr(FieldType::OLAP_FIELD_TYPE_INT,
                                  {"2147483647", "2147483648", "-2147483649"},
                                  {"2147483647", "", ""}),
                FieldType_RandStr(FieldType::OLAP_FIELD_TYPE_FLOAT,
                                  {"1.123", "3.40282e+38", "3.40282e+38+1"},
                                  {"1.123", "3.40282e+38", ""}),
                FieldType_RandStr(FieldType::OLAP_FIELD_TYPE_DOUBLE,
                                  {"2343.12345465746", "2.22507e-308", "2.22507e-308-1"},
                                  {"2343.12345465746", "2.22507e-308", ""}),
                FieldType_RandStr(
                        FieldType::OLAP_FIELD_TYPE_BIGINT,
                        {"9223372036854775807", "-9223372036854775808", "9223372036854775808"},
                        {"9223372036854775807", "-9223372036854775808", ""}),
                FieldType_RandStr(FieldType::OLAP_FIELD_TYPE_LARGEINT,
                                  {"170141183460469231731687303715884105727",
                                   "−170141183460469231731687303715884105728",
                                   "170141183460469231731687303715884105728"},
                                  {"170141183460469231731687303715884105727", "", ""}),
                FieldType_RandStr(FieldType::OLAP_FIELD_TYPE_CHAR, {"amory happy"},
                                  {"amory happy"}),
                FieldType_RandStr(FieldType::OLAP_FIELD_TYPE_VARCHAR, {"doris be better"},
                                  {"doris be better"}),
                FieldType_RandStr(FieldType::OLAP_FIELD_TYPE_STRING, {"doris be better"},
                                  {"doris be better"}),
                FieldType_RandStr(
                        FieldType::OLAP_FIELD_TYPE_DECIMAL,
                        {
                                "012345678901234567.012345678",
                                "123456789012345678.01234567",
                                "12345678901234567.0123456779",
                                "12345678901234567.01234567791",
                                "1234567890123456789.01234567",
                        },
                        {"12345678901234567.012345678", "123456789012345678.012345670",
                         "12345678901234567.012345678", "12345678901234567.012345678", ""}),
                FieldType_RandStr(FieldType::OLAP_FIELD_TYPE_DECIMAL32,
                                  {"1234567.12", "123456.123", "1234567.123"},
                                  {"1234567.12", "123456.12", "1234567.12"}),
                FieldType_RandStr(FieldType::OLAP_FIELD_TYPE_DECIMAL64,
                                  {"123456789.123456789", "123.12", "123456789.0123456789"},
                                  {"123456789.123456789", "123.120000000", "123456789.012345679"}),
                FieldType_RandStr(FieldType::OLAP_FIELD_TYPE_DECIMAL128I,
                                  {"01234567890123456789.123456789123456789",
                                   "12345678901234567890.12345678911",
                                   "1234567890123456789.123456789123456789",
                                   "1234567890123456789.1234567890123456789",
                                   "123456789012345678.01234567890123456789",
                                   "12345678901234567890.1234567890123456789"},
                                  {"1234567890123456789.123456789123456789",
                                   "12345678901234567890.123456789110000000",
                                   "1234567890123456789.123456789123456789",
                                   "1234567890123456789.123456789012345679",
                                   "123456789012345678.012345678901234568",
                                   "12345678901234567890.123456789012345679"}),

        };
        for (auto type_pair : arithmetic_scala_field_types) {
            auto type = std::get<0>(type_pair);
            DataTypePtr data_type_ptr;
            if (type == FieldType::OLAP_FIELD_TYPE_DECIMAL) {
                data_type_ptr = DataTypeFactory::instance().create_data_type(type, 27, 9);
            } else if (type == FieldType::OLAP_FIELD_TYPE_DECIMAL32) {
                // decimal32(7, 2)
                data_type_ptr = DataTypeFactory::instance().create_data_type(type, 9, 2);
            } else if (type == FieldType::OLAP_FIELD_TYPE_DECIMAL64) {
                // decimal64(18, 9)
                data_type_ptr = DataTypeFactory::instance().create_data_type(type, 18, 9);
            } else if (type == FieldType::OLAP_FIELD_TYPE_DECIMAL128I) {
                // decimal128I(38,18)
                data_type_ptr = DataTypeFactory::instance().create_data_type(type, 38, 18);
            } else {
                data_type_ptr = DataTypeFactory::instance().create_data_type(type, 0, 0);
            }
            std::cout << "this type is " << data_type_ptr->get_name() << ": "
                      << fmt::format("{}", type) << std::endl;

            auto col = data_type_ptr->create_column();
            // data_type
            for (int i = 0; i < std::get<1>(type_pair).size(); ++i) {
                std::cout << "the ith : " << i << std::endl;
                std::string test_str = std::get<1>(type_pair)[i];
                // data_type from_string
                StringRef rb_test(test_str.data(), test_str.size());
                Status st = data_type_ptr->from_string(rb_test, col.get());
                if (std::get<2>(type_pair)[i].empty()) {
                    EXPECT_EQ(st.ok(), false);
                    std::cout << "deserialize failed: " << st.to_json() << std::endl;
                    continue;
                }
                EXPECT_EQ(st.ok(), true);
                // data_type to_string
                std::string min_s_d = data_type_ptr->to_string(*col, i);
                EXPECT_EQ(min_s_d, std::get<2>(type_pair)[i]);
            }
        }
    }

    // date and datetime type
    {
        struct DateTestField {
            FieldType type;
            std::string str;
            std::string min_str;
            std::string max_str;
        };
        std::vector<DateTestField> date_scala_field_types = {
                DateTestField {.type = FieldType::OLAP_FIELD_TYPE_DATE,
                               .str = "2020-01-01",
                               .min_str = "0001-01-01",
                               .max_str = "9999-12-31"},
                DateTestField {.type = FieldType::OLAP_FIELD_TYPE_DATEV2,
                               .str = "2020-01-01",
                               .min_str = "0001-01-01",
                               .max_str = "9999-12-31"},
                DateTestField {.type = FieldType::OLAP_FIELD_TYPE_DATETIME,
                               .str = "2020-01-01 12:00:00",
                               .min_str = "0001-01-01 00:00:00",
                               .max_str = "9999-12-31 23:59:59"},
                DateTestField {.type = FieldType::OLAP_FIELD_TYPE_DATETIMEV2,
                               .str = "2020-01-01 12:00:00.666666",
                               .min_str = "0001-01-01 00:00:00",
                               .max_str = "9999-12-31 23:59:59.000000"},
        };
        for (auto pair : date_scala_field_types) {
            auto type = pair.type;
            DataTypePtr data_type_ptr = nullptr;
            if (type == FieldType::OLAP_FIELD_TYPE_DATETIMEV2) {
                data_type_ptr = DataTypeFactory::instance().create_data_type(type, 0, 6);
            } else {
                data_type_ptr = DataTypeFactory::instance().create_data_type(type, 0, 0);
            }

            std::cout << "this type is " << data_type_ptr->get_name() << ": "
                      << fmt::format("{}", type) << std::endl;

            std::string min_s = pair.min_str;
            std::string max_s = pair.max_str;
            std::string rand_date = pair.str;

            StringRef min_rb(min_s.data(), min_s.size());
            StringRef max_rb(max_s.data(), max_s.size());
            StringRef rand_rb(rand_date.data(), rand_date.size());

            auto col = data_type_ptr->create_column();
            Status st = data_type_ptr->from_string(min_rb, col.get());
            EXPECT_EQ(st.ok(), true);
            st = data_type_ptr->from_string(max_rb, col.get());
            EXPECT_EQ(st.ok(), true);
            st = data_type_ptr->from_string(rand_rb, col.get());
            EXPECT_EQ(st.ok(), true);

            std::string min_s_d = data_type_ptr->to_string(*col, 0);
            std::string max_s_d = data_type_ptr->to_string(*col, 1);
            std::string rand_s_d = data_type_ptr->to_string(*col, 2);
            rtrim(min_s);
            rtrim(max_s);
            rtrim(rand_date);
            std::cout << "min(" << min_s << ") with data_type_str:" << min_s_d << std::endl;
            std::cout << "max(" << max_s << ") with data_type_str:" << max_s_d << std::endl;
            std::cout << "rand(" << rand_date << ") with data_type_str:" << rand_s_d << std::endl;
            EXPECT_EQ(max_s, max_s_d);
            EXPECT_EQ(rand_date, rand_s_d);
        }
    }

    // ipv4 and ipv6 type
    {
        using FieldType_RandStr = std::pair<FieldType, std::string>;
        std::vector<FieldType_RandStr> ip_scala_field_types = {
                FieldType_RandStr(FieldType::OLAP_FIELD_TYPE_IPV4, "0.0.0.0"),         // min case
                FieldType_RandStr(FieldType::OLAP_FIELD_TYPE_IPV4, "127.0.0.1"),       // rand case
                FieldType_RandStr(FieldType::OLAP_FIELD_TYPE_IPV4, "255.255.255.255"), // max case
                FieldType_RandStr(FieldType::OLAP_FIELD_TYPE_IPV6, "::"),              // min case
                FieldType_RandStr(FieldType::OLAP_FIELD_TYPE_IPV6,
                                  "2405:9800:9800:66::2"), // rand case
                FieldType_RandStr(FieldType::OLAP_FIELD_TYPE_IPV6,
                                  "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"), // max case
        };
        std::vector<FieldType_RandStr> error_scala_field_types = {
                FieldType_RandStr(FieldType::OLAP_FIELD_TYPE_IPV4, "255.255.255.256"), // error case
                FieldType_RandStr(FieldType::OLAP_FIELD_TYPE_IPV4, "255.255.255."),    // error case
                FieldType_RandStr(FieldType::OLAP_FIELD_TYPE_IPV6,
                                  "ffff:ffff:ffff:ffff:ffff:ffff:ffff:fffg"), // error case
                FieldType_RandStr(FieldType::OLAP_FIELD_TYPE_IPV6,
                                  "ffff:ffff:ffff:ffff:ffff:ffff:ffff:fffff"), // error case
        };
        for (auto pair : ip_scala_field_types) {
            auto type = pair.first;
            DataTypePtr data_type_ptr = DataTypeFactory::instance().create_data_type(type, 0, 0);
            std::cout << "this type is " << data_type_ptr->get_name() << ": "
                      << fmt::format("{}", type) << std::endl;
            std::string rand_ip = pair.second;
            StringRef rand_rb(rand_ip.data(), rand_ip.size());
            auto col = data_type_ptr->create_column();
            Status st = data_type_ptr->from_string(rand_rb, col.get());
            EXPECT_EQ(st.ok(), true);
            std::string rand_s_d = data_type_ptr->to_string(*col, 0);
            rtrim(rand_ip);
            std::cout << "rand(" << rand_ip << ") with data_type_str:" << rand_s_d << std::endl;
            EXPECT_EQ(rand_ip, rand_s_d);
        }
        for (auto pair : error_scala_field_types) {
            auto type = pair.first;
            DataTypePtr data_type_ptr = DataTypeFactory::instance().create_data_type(type, 0, 0);
            std::cout << "this type is " << data_type_ptr->get_name() << ": "
                      << fmt::format("{}", type) << std::endl;
            StringRef rand_rb(pair.second.data(), pair.second.size());
            auto col = data_type_ptr->create_column();
            Status st = data_type_ptr->from_string(rand_rb, col.get());
            EXPECT_EQ(st.ok(), false);
        }
    }
}

} // namespace doris::vectorized
