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
#include "olap/wrapper_field.h"
#include "vec/columns/column.h"
#include "vec/core/field.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/data_types/serde_utils.h"
#include "vec/io/reader_buffer.h"

namespace doris::vectorized {

/**
 * This test is used to check wrapperField from_string is equal to data type from_string or not
 *  same string feed to wrapperField and data type from_string, and check the result from
 *  wrapperField and data type to_string is equal or not
 */
TEST(FromStringTest, ScalaWrapperFieldVsDataType) {
    // arithmetic scala field types
    {
        // fieldType, test_string, expect_wrapper_field_string, expect_data_type_string
        typedef std::tuple<FieldType, std::vector<string>, std::vector<string>, std::vector<string>>
                FieldType_RandStr;
        std::vector<FieldType_RandStr> arithmetic_scala_field_types = {
                FieldType_RandStr(FieldType::OLAP_FIELD_TYPE_BOOL, {"0", "1", "-9"},
                                  {"0", "1", "1"}, {"0", "1", ""}),
                FieldType_RandStr(FieldType::OLAP_FIELD_TYPE_TINYINT, {"127", "-128", "-190"},
                                  {"127", "-128", "66"}, {"127", "-128", ""}),
                // here if it has overflow , wrapper field will return make max/min value, but data type will just throw error
                FieldType_RandStr(FieldType::OLAP_FIELD_TYPE_SMALLINT, {"32767", "32768", "-32769"},
                                  {"32767", "-32768", "32767"}, {"32767", "", ""}),
                // here if it has overflow , wrapper field will return make max/min value, but data type will just throw error
                FieldType_RandStr(
                        FieldType::OLAP_FIELD_TYPE_INT, {"2147483647", "2147483648", "-2147483649"},
                        {"2147483647", "-2147483648", "2147483647"}, {"2147483647", "", ""}),
                // float ==> float32(32bit)
                // here if it has overflow , wrapper field will return make max/min value, but data type will just throw error
                FieldType_RandStr(
                        FieldType::OLAP_FIELD_TYPE_FLOAT, {"1.123", "3.40282e+38", "3.40282e+38+1"},
                        {"1.123", "3.40282e+38", "3.40282e+38"}, {"1.123", "3.40282e+38", ""}),
                // double ==> float64(64bit)
                // here if it has overflow , wrapper field will return make max/min value, but data type will just throw error
                FieldType_RandStr(FieldType::OLAP_FIELD_TYPE_DOUBLE,
                                  {"2343.12345465746", "2.22507e-308", "2.22507e-308-1"},
                                  {"2343.12345465746", "2.22507e-308", "2.22507e-308"},
                                  {"2343.12345465746", "2.22507e-308", ""}),
                // BIGINT ==> int64_t(64bit)
                // here if it has overflow , wrapper field will return make max/min value, but data type will just throw error
                FieldType_RandStr(
                        FieldType::OLAP_FIELD_TYPE_BIGINT,
                        {"9223372036854775807", "-9223372036854775808", "9223372036854775808"},
                        {"9223372036854775807", "-9223372036854775808", "9223372036854775807"},
                        {"9223372036854775807", "-9223372036854775808", ""}),
                // LARGEINT ==> int128_t(128bit)
                // here if it has overflow , wrapper field will return 0, but data type will just throw error
                FieldType_RandStr(FieldType::OLAP_FIELD_TYPE_LARGEINT,
                                  {"170141183460469231731687303715884105727",
                                   "−170141183460469231731687303715884105728",
                                   "170141183460469231731687303715884105728"},
                                  {"170141183460469231731687303715884105727", "0", "0"},
                                  {"170141183460469231731687303715884105727", "", ""}),
                FieldType_RandStr(FieldType::OLAP_FIELD_TYPE_CHAR, {"amory happy"}, {"amory happy"},
                                  {"amory happy"}),
                FieldType_RandStr(FieldType::OLAP_FIELD_TYPE_VARCHAR, {"doris be better"},
                                  {"doris be better"}, {"doris be better"}),
                FieldType_RandStr(FieldType::OLAP_FIELD_TYPE_STRING, {"doris be better"},
                                  {"doris be better"}, {"doris be better"}),
                // Decimal parse using StringParser which has SUCCESS|OVERFLOW|UNDERFLOW|FAILURE
                // wrapper_field from_string(scale) and data_type from_string(scale) use rounding when meet underflow,
                //  wrapper_field use min/max when meet overflow, but data_type just throw error
                FieldType_RandStr(
                        // decimalv2 will ignore the scale and precision when parse string
                        FieldType::OLAP_FIELD_TYPE_DECIMAL,
                        {
                                "012345678901234567.012345678",
                                // (18, 8)
                                "123456789012345678.01234567",
                                // (17, 10)
                                "12345678901234567.0123456779",
                                // (17, 11)
                                "12345678901234567.01234567791",
                                // (19, 8)
                                "1234567890123456789.01234567",
                        },
                        {"12345678901234567.012345678", "123456789012345678.012345670",
                         "12345678901234567.012345677", "12345678901234567.012345677",
                         "999999999999999999.999999999"},
                        {"12345678901234567.012345678", "123456789012345678.012345670",
                         "12345678901234567.012345678", "12345678901234567.012345678", ""}),
                // decimal32 ==>  decimal32(9,2)
                FieldType_RandStr(FieldType::OLAP_FIELD_TYPE_DECIMAL32,
                                  // (7,2)         (6,3)         (7,3)           (8,1)
                                  {"1234567.12", "123456.123", "1234567.123", "12345679.112345"},
                                  //StringParser res: SUCCESS      UNDERFLOW  UNDERFLOW    OVERFLOW
                                  {"123456712", "12345612", "123456712", "999999999"},
                                  {"1234567.12", "123456.12", "1234567.12", ""}),
                // decimal64 ==> decimal64(18,9)
                FieldType_RandStr(
                        FieldType::OLAP_FIELD_TYPE_DECIMAL64,
                        //(9, 9)                (3,2)       (9, 10)
                        {"123456789.123456789", "123.12", "123456789.0123456789",
                         //(10, 9)
                         "1234567890.123456789"},
                        //StringParser res: SUCCESS               SUCCESS         UNDERFLOW             OVERFLOW
                        {"123456789123456789", "123120000000", "123456789012345679",
                         "999999999999999999"},
                        {"123456789.123456789", "123.120000000", "123456789.012345679", ""}),
                // decimal128I ==> decimal128I(38,18)
                FieldType_RandStr(FieldType::OLAP_FIELD_TYPE_DECIMAL128I,
                                  // (19,18) ==> StringParser::SUCCESS
                                  {"01234567890123456789.123456789123456789",
                                   // (20,11) ==> StringParser::SUCCESS
                                   "12345678901234567890.12345678911",
                                   // (19,18) ==> StringParser::SUCCESS
                                   "1234567890123456789.123456789123456789",
                                   // (19,19) ==> StringParser::UNDERFLOW
                                   "1234567890123456789.1234567890123456789",
                                   // (18, 20) ==> StringParser::UNDERFLOW
                                   "123456789012345678.01234567890123456789",
                                   // (20, 19) ==> StringParser::UNDERFLOW
                                   "12345678901234567890.1234567890123456789",
                                   // (21, 17) ==> StringParser::OVERFLOW
                                   "123456789012345678901.12345678901234567"},
                                  {"1234567890123456789123456789123456789",
                                   "12345678901234567890123456789110000000",
                                   "1234567890123456789123456789123456789",
                                   "1234567890123456789123456789012345679",
                                   "123456789012345678012345678901234568",
                                   "12345678901234567890123456789012345679",
                                   "99999999999999999999999999999999999999"},
                                  {"1234567890123456789.123456789123456789",
                                   "12345678901234567890.123456789110000000",
                                   "1234567890123456789.123456789123456789",
                                   "1234567890123456789.123456789012345679",
                                   "123456789012345678.012345678901234568",
                                   "12345678901234567890.123456789012345679", ""}),

        };
        for (auto type_pair : arithmetic_scala_field_types) {
            auto type = std::get<0>(type_pair);
            DataTypePtr data_type_ptr;
            int precision = 0;
            int scale = 0;
            if (type == FieldType::OLAP_FIELD_TYPE_DECIMAL) {
                data_type_ptr = DataTypeFactory::instance().create_data_type(type, 27, 9);
                precision = 27;
                scale = 9;
            } else if (type == FieldType::OLAP_FIELD_TYPE_DECIMAL32) {
                // decimal32(7, 2)
                data_type_ptr = DataTypeFactory::instance().create_data_type(type, 9, 2);
                precision = 9;
                scale = 2;
            } else if (type == FieldType::OLAP_FIELD_TYPE_DECIMAL64) {
                // decimal64(18, 9)
                data_type_ptr = DataTypeFactory::instance().create_data_type(type, 18, 9);
                precision = 18;
                scale = 9;
            } else if (type == FieldType::OLAP_FIELD_TYPE_DECIMAL128I) {
                // decimal128I(38,18)
                data_type_ptr = DataTypeFactory::instance().create_data_type(type, 38, 18);
                precision = 38;
                scale = 18;
            } else {
                data_type_ptr = DataTypeFactory::instance().create_data_type(type, 0, 0);
            }
            std::cout << "this type is " << data_type_ptr->get_name() << ": "
                      << fmt::format("{}", type) << std::endl;

            // wrapper_field
            for (int i = 0; i < std::get<1>(type_pair).size(); ++i) {
                string test_str = std::get<1>(type_pair)[i];
                std::unique_ptr<WrapperField> wf(WrapperField::create_by_type(type));
                std::cout << "the ith : " << i << " test_str: " << test_str << std::endl;
                // from_string
                Status st = wf->from_string(test_str, precision, scale);
                EXPECT_EQ(st.ok(), true);
                // wrapper field to_string is only for debug
                std::string wfs = wf->to_string();
                EXPECT_EQ(wfs, std::get<2>(type_pair)[i]);
            }

            auto col = data_type_ptr->create_column();
            // data_type
            for (int i = 0; i < std::get<1>(type_pair).size(); ++i) {
                std::cout << "the ith : " << i << std::endl;
                string test_str = std::get<1>(type_pair)[i];
                // data_type from_string
                ReadBuffer rb_test(test_str.data(), test_str.size());
                Status st = data_type_ptr->from_string(rb_test, col);
                if (std::get<3>(type_pair)[i].empty()) {
                    EXPECT_EQ(st.ok(), false);
                    std::cout << "deserialize failed: " << st.to_json() << std::endl;
                    continue;
                }
                EXPECT_EQ(st.ok(), true);
                // data_type to_string
                string min_s_d = data_type_ptr->to_string(*col, i);
                EXPECT_EQ(min_s_d, std::get<3>(type_pair)[i]);
            }
        }
    }

    // date and datetime type
    {
        typedef std::pair<FieldType, string> FieldType_RandStr;
        std::vector<FieldType_RandStr> date_scala_field_types = {
                FieldType_RandStr(FieldType::OLAP_FIELD_TYPE_DATE, "2020-01-01"),
                FieldType_RandStr(FieldType::OLAP_FIELD_TYPE_DATEV2, "2020-01-01"),
                FieldType_RandStr(FieldType::OLAP_FIELD_TYPE_DATETIME, "2020-01-01 12:00:00"),
                FieldType_RandStr(FieldType::OLAP_FIELD_TYPE_DATETIMEV2,
                                  "2020-01-01 12:00:00.666666"),
        };
        for (auto pair : date_scala_field_types) {
            auto type = pair.first;
            DataTypePtr data_type_ptr = nullptr;
            if (type == FieldType::OLAP_FIELD_TYPE_DATETIMEV2) {
                data_type_ptr = DataTypeFactory::instance().create_data_type(type, 0, 6);
            } else {
                data_type_ptr = DataTypeFactory::instance().create_data_type(type, 0, 0);
            }

            std::cout << "this type is " << data_type_ptr->get_name() << ": "
                      << fmt::format("{}", type) << std::endl;

            std::unique_ptr<WrapperField> min_wf(WrapperField::create_by_type(type));
            std::unique_ptr<WrapperField> max_wf(WrapperField::create_by_type(type));
            std::unique_ptr<WrapperField> rand_wf(WrapperField::create_by_type(type));

            min_wf->set_to_min();
            max_wf->set_to_max();
            static_cast<void>(rand_wf->from_string(pair.second, 0, 0));

            string min_s = min_wf->to_string();
            string max_s = max_wf->to_string();
            string rand_date = rand_wf->to_string();

            ReadBuffer min_rb(min_s.data(), min_s.size());
            ReadBuffer max_rb(max_s.data(), max_s.size());
            ReadBuffer rand_rb(rand_date.data(), rand_date.size());

            auto col = data_type_ptr->create_column();
            Status st = data_type_ptr->from_string(min_rb, col);
            EXPECT_EQ(st.ok(), true);
            st = data_type_ptr->from_string(max_rb, col);
            EXPECT_EQ(st.ok(), true);
            st = data_type_ptr->from_string(rand_rb, col);
            EXPECT_EQ(st.ok(), true);

            string min_s_d = data_type_ptr->to_string(*col, 0);
            string max_s_d = data_type_ptr->to_string(*col, 1);
            string rand_s_d = data_type_ptr->to_string(*col, 2);
            rtrim(min_s);
            rtrim(max_s);
            rtrim(rand_date);
            std::cout << "min(" << min_s << ") with datat_ype_str:" << min_s_d << std::endl;
            std::cout << "max(" << max_s << ") with datat_ype_str:" << max_s_d << std::endl;
            std::cout << "rand(" << rand_date << ") with datat_type_str:" << rand_s_d << std::endl;
            // min wrapper field date to_string in macOS and linux system has different result
            //  macOs equals with data type to_string(0000-01-01), but in linux is (0-01-01)
            if (FieldType::OLAP_FIELD_TYPE_DATE == type ||
                FieldType::OLAP_FIELD_TYPE_DATETIME == type) {
                // min wrapper field date to_string in macOS and linux system has different result
                //  macOs equals with data type to_string(0000-01-01), but in linux is (0-01-01)
                std::cout << "wrapper field (" << min_s << ") with data type to_string(" << min_s_d
                          << ")" << std::endl;
            } else {
                EXPECT_EQ(min_s, min_s_d);
            }
            EXPECT_EQ(max_s, max_s_d);
            EXPECT_EQ(rand_date, rand_s_d);
        }
    }

    // ipv4 and ipv6 type
    {
        typedef std::pair<FieldType, string> FieldType_RandStr;
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
            std::unique_ptr<WrapperField> rand_wf(WrapperField::create_by_type(type));
            Status st = rand_wf->from_string(pair.second, 0, 0);
            string rand_ip = rand_wf->to_string();
            ReadBuffer rand_rb(rand_ip.data(), rand_ip.size());
            auto col = data_type_ptr->create_column();
            st = data_type_ptr->from_string(rand_rb, col);
            EXPECT_EQ(st.ok(), true);
            string rand_s_d = data_type_ptr->to_string(*col, 0);
            rtrim(rand_ip);
            std::cout << "rand(" << rand_ip << ") with data_type_str:" << rand_s_d << std::endl;
            EXPECT_EQ(rand_ip, rand_s_d);
        }
        for (auto pair : error_scala_field_types) {
            auto type = pair.first;
            DataTypePtr data_type_ptr = DataTypeFactory::instance().create_data_type(type, 0, 0);
            std::cout << "this type is " << data_type_ptr->get_name() << ": "
                      << fmt::format("{}", type) << std::endl;
            std::unique_ptr<WrapperField> rand_wf(WrapperField::create_by_type(type));
            Status st = rand_wf->from_string(pair.second, 0, 0);
            EXPECT_EQ(st.ok(), false);
            ReadBuffer rand_rb(pair.second.data(), pair.second.size());
            auto col = data_type_ptr->create_column();
            st = data_type_ptr->from_string(rand_rb, col);
            EXPECT_EQ(st.ok(), false);
        }
    }

    // null data type
    {
        DataTypePtr data_type_ptr = DataTypeFactory::instance().create_data_type(
                FieldType::OLAP_FIELD_TYPE_STRING, 0, 0);
        DataTypePtr nullable_ptr = std::make_shared<DataTypeNullable>(data_type_ptr);
        std::unique_ptr<WrapperField> rand_wf(
                WrapperField::create_by_type(FieldType::OLAP_FIELD_TYPE_STRING));
        std::string test_str = generate(128);
        static_cast<void>(rand_wf->from_string(test_str, 0, 0));
        Field string_field(test_str);
        ColumnPtr col = nullable_ptr->create_column_const(0, string_field);
        EXPECT_EQ(rand_wf->to_string(), nullable_ptr->to_string(*col, 0));
    }
}

} // namespace doris::vectorized
