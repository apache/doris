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

#include "olap/wrapper_field.h"
#include "vec/common/string_buffer.hpp"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/data_types/data_type_map.h"
#include "vec/data_types/serde/data_type_serde.h"
#include "vec/data_types/serde_utils.h"
#include "vec/io/reader_buffer.h"

namespace doris::vectorized {
// This test aim to make sense for text serde of data types.
//  we use default formatOption and special formatOption to equal serde for wrapperField.
using string = std::string;
TEST(TestSerdeText, ScalaDataTypeSerdeTextTest) {
    // arithmetic scala field types
    {
        // fieldType, test_string, expect_string
        using FieldType_RandStr = std::tuple<FieldType, std::vector<string>, std::vector<string>>;
        std::vector<FieldType_RandStr> arithmetic_scala_field_types = {
                FieldType_RandStr(FieldType::OLAP_FIELD_TYPE_BOOL, {"0", "1", "-1"},
                                  {"0", "1", ""}),
                FieldType_RandStr(FieldType::OLAP_FIELD_TYPE_TINYINT, {"127", "-128", "-190"},
                                  {"127", "-128", ""}),
                FieldType_RandStr(FieldType::OLAP_FIELD_TYPE_SMALLINT, {"32767", "32768", "-32769"},
                                  {"32767", "", ""}),
                FieldType_RandStr(FieldType::OLAP_FIELD_TYPE_INT,
                                  {"2147483647", "2147483648", "-2147483649"},
                                  {"2147483647", "", ""}),
                // float ==> float32(32bit)
                FieldType_RandStr(FieldType::OLAP_FIELD_TYPE_FLOAT,
                                  {"1.123", "3.40282e+38", "3.40282e+38+1"},
                                  {"1.123", "3.40282e+38", ""}),
                // double ==> float64(64bit)
                FieldType_RandStr(FieldType::OLAP_FIELD_TYPE_DOUBLE,
                                  {"2343.12345465746", "2.22507e-308", "2.22507e-308-1"},
                                  {"2343.12345465746", "2.22507e-308", ""}),
                // BIGINT ==> int64_t(64bit)
                FieldType_RandStr(
                        FieldType::OLAP_FIELD_TYPE_BIGINT,
                        {"9223372036854775807", "-9223372036854775808", "9223372036854775808"},
                        {"9223372036854775807", "-9223372036854775808", ""}),
                // LARGEINT ==> int128_t(128bit)
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
                // decimal ==> decimalv2(decimal<128>(27,9))
                FieldType_RandStr(
                        FieldType::OLAP_FIELD_TYPE_DECIMAL,
                        {
                                // (17, 9)(first 0 will ignore)
                                "012345678901234567.012345678",
                                // (18, 8) (automatically fill 0 for scala)
                                "123456789012345678.01234567",
                                // (17, 10) (rounding last to make it fit)
                                "12345678901234567.0123456779",
                                // (17, 11) (rounding last to make it fit)
                                "12345678901234567.01234567791",
                                // (19, 8) (wrong)
                                "1234567890123456789.01234567",
                        },
                        {"12345678901234567.012345678", "123456789012345678.012345670",
                         "12345678901234567.012345678", "12345678901234567.012345678", ""}),
                // decimal32 ==>  decimal32(9,2)                       (7,2)         (6,3)         (7,3)           (8,1)
                FieldType_RandStr(FieldType::OLAP_FIELD_TYPE_DECIMAL32,
                                  {"1234567.12", "123456.123", "1234567.123", "12345679.1"},
                                  {"1234567.12", "123456.12", "1234567.12", ""}),
                // decimal64 ==> decimal64(18,9)                        (9, 9)                   (3,2)    (9, 10)                  (10, 9)
                FieldType_RandStr(
                        FieldType::OLAP_FIELD_TYPE_DECIMAL64,
                        {"123456789.123456789", "123.12", "123456789.0123456789",
                         "1234567890.123456789"},
                        {"123456789.123456789", "123.120000000", "123456789.012345679", ""}),
                // decimal128I ==> decimal128I(38,18)                     (19,18)
                FieldType_RandStr(FieldType::OLAP_FIELD_TYPE_DECIMAL128I,
                                  {"01234567890123456789.123456789123456789",
                                   // (20,11) (automatically fill 0 for scala)
                                   "12345678901234567890.12345678911",
                                   // (19,18)
                                   "1234567890123456789.123456789123456789",
                                   // (19,19) (rounding last to make it fit)
                                   "1234567890123456789.1234567890123456789",
                                   // (18, 20) (rounding to make it fit)
                                   "123456789012345678.01234567890123456789",
                                   // (20, 19) (wrong)
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
            std::cout << "========= This type is  " << data_type_ptr->get_name() << ": "
                      << fmt::format("{}", type) << std::endl;

            auto col = data_type_ptr->create_column();

            // serde for data types with default FormatOption
            DataTypeSerDe::FormatOptions default_format_option;
            DataTypeSerDeSPtr serde = data_type_ptr->get_serde();

            auto ser_col = ColumnString::create();
            ser_col->reserve(std::get<1>(type_pair).size());
            VectorBufferWriter buffer_writer(*ser_col.get());

            for (int i = 0; i < std::get<1>(type_pair).size(); ++i) {
                string test_str = std::get<1>(type_pair)[i];
                std::cout << "the str : " << test_str << std::endl;
                Slice rb_test(test_str.data(), test_str.size());
                // deserialize
                Status st =
                        serde->deserialize_one_cell_from_json(*col, rb_test, default_format_option);
                if (std::get<2>(type_pair)[i].empty()) {
                    EXPECT_EQ(st.ok(), false);
                    std::cout << "deserialize failed: " << st.to_json() << std::endl;
                    continue;
                }
                EXPECT_EQ(st.ok(), true);
                // serialize
                st = serde->serialize_column_to_text(*col, i, buffer_writer);
                EXPECT_EQ(st.ok(), true);
                buffer_writer.commit();
                EXPECT_EQ(ser_col->get_data_at(ser_col->size() - 1).to_string(),
                          std::get<2>(type_pair)[i]);
            }
        }
    }

    // date and datetime type
    {
        struct DataTestField {
            FieldType type;
            string str;
            string max_str;
            string min_str;
        };
        std::vector<DataTestField> date_scala_field_types = {
                DataTestField {.type = FieldType::OLAP_FIELD_TYPE_DATE,
                               .str = "2020-01-01",
                               .max_str = "9999-12-31",
                               .min_str = "0001-01-01"},
                DataTestField {.type = FieldType::OLAP_FIELD_TYPE_DATE,
                               .str = "2020-01-01",
                               .max_str = "9999-12-31",
                               .min_str = "0001-01-01"},
                DataTestField {.type = FieldType::OLAP_FIELD_TYPE_DATEV2,
                               .str = "2020-01-01",
                               .max_str = "9999-12-31",
                               .min_str = "0001-01-01"},
                DataTestField {.type = FieldType::OLAP_FIELD_TYPE_DATETIME,
                               .str = "2020-01-01 12:00:00",
                               .max_str = "9999-12-31 23:59:59",
                               .min_str = "0001-01-01 00:00:00"},
                DataTestField {.type = FieldType::OLAP_FIELD_TYPE_DATETIMEV2,
                               .str = "2020-01-01 12:00:00",
                               .max_str = "9999-12-31 23:59:59",
                               .min_str = "0001-01-01 00:00:00"},
        };
        for (auto pair : date_scala_field_types) {
            auto type = pair.type;
            DataTypePtr data_type_ptr = DataTypeFactory::instance().create_data_type(type, 0, 0);
            std::cout << "========= This type is  " << data_type_ptr->get_name() << ": "
                      << fmt::format("{}", type) << std::endl;
            string min_s = pair.min_str;
            string max_s = pair.max_str;
            string rand_date = pair.str;

            Slice min_rb(min_s.data(), min_s.size());
            Slice max_rb(max_s.data(), max_s.size());
            Slice rand_rb(rand_date.data(), rand_date.size());

            auto col = data_type_ptr->create_column();
            DataTypeSerDeSPtr serde = data_type_ptr->get_serde();
            // make use c++ lib equals to wrapper field from_string behavior
            DataTypeSerDe::FormatOptions formatOptions;

            Status st = serde->deserialize_one_cell_from_json(*col, min_rb, formatOptions);
            EXPECT_EQ(st.ok(), true);
            st = serde->deserialize_one_cell_from_json(*col, max_rb, formatOptions);
            EXPECT_EQ(st.ok(), true);
            st = serde->deserialize_one_cell_from_json(*col, rand_rb, formatOptions);
            EXPECT_EQ(st.ok(), true);

            auto ser_col = ColumnString::create();
            ser_col->reserve(3);
            VectorBufferWriter buffer_writer(*ser_col.get());
            st = serde->serialize_column_to_text(*col, 0, buffer_writer);
            EXPECT_EQ(st.ok(), true);
            buffer_writer.commit();
            st = serde->serialize_column_to_text(*col, 1, buffer_writer);
            EXPECT_EQ(st.ok(), true);
            buffer_writer.commit();
            st = serde->serialize_column_to_text(*col, 2, buffer_writer);
            EXPECT_EQ(st.ok(), true);
            buffer_writer.commit();
            rtrim(min_s);
            rtrim(max_s);
            rtrim(rand_date);
            StringRef min_s_d = ser_col->get_data_at(0);
            StringRef max_s_d = ser_col->get_data_at(1);
            StringRef rand_s_d = ser_col->get_data_at(2);

            std::cout << "min(" << min_s << ") with data_type_str:" << min_s_d << std::endl;
            std::cout << "max(" << max_s << ") with data_type_str:" << max_s_d << std::endl;
            std::cout << "rand(" << rand_date << ") with data_type_str:" << rand_s_d << std::endl;
            EXPECT_EQ(min_s, min_s_d.to_string());
            EXPECT_EQ(max_s, max_s_d.to_string());
            EXPECT_EQ(rand_date, rand_s_d.to_string());
        }
    }

    // ipv4 and ipv6
    {
        using FieldType_RandStr = std::pair<FieldType, string>;
        std::vector<FieldType_RandStr> ip_scala_field_types = {
                FieldType_RandStr(FieldType::OLAP_FIELD_TYPE_IPV4, "127.0.0.1"),
                FieldType_RandStr(FieldType::OLAP_FIELD_TYPE_IPV6, "2405:9800:9800:66::2")};
        for (auto pair : ip_scala_field_types) {
            auto type = pair.first;
            DataTypePtr data_type_ptr = DataTypeFactory::instance().create_data_type(type, 0, 0);
            std::cout << "========= This type is  " << data_type_ptr->get_name() << ": "
                      << fmt::format("{}", type) << std::endl;

            std::unique_ptr<WrapperField> min_wf(WrapperField::create_by_type(type));
            std::unique_ptr<WrapperField> max_wf(WrapperField::create_by_type(type));
            std::unique_ptr<WrapperField> rand_wf(WrapperField::create_by_type(type));

            min_wf->set_to_min();
            max_wf->set_to_max();
            static_cast<void>(rand_wf->from_string(pair.second, 0, 0));

            string min_s = min_wf->to_string();
            string max_s = max_wf->to_string();
            string rand_ip = rand_wf->to_string();

            Slice min_rb(min_s.data(), min_s.size());
            Slice max_rb(max_s.data(), max_s.size());
            Slice rand_rb(rand_ip.data(), rand_ip.size());

            auto col = data_type_ptr->create_column();
            DataTypeSerDeSPtr serde = data_type_ptr->get_serde();
            // make use c++ lib equals to wrapper field from_string behavior
            DataTypeSerDe::FormatOptions formatOptions;

            Status st = serde->deserialize_one_cell_from_json(*col, min_rb, formatOptions);
            EXPECT_EQ(st.ok(), true);
            st = serde->deserialize_one_cell_from_json(*col, max_rb, formatOptions);
            EXPECT_EQ(st.ok(), true);
            st = serde->deserialize_one_cell_from_json(*col, rand_rb, formatOptions);
            EXPECT_EQ(st.ok(), true);

            auto ser_col = ColumnString::create();
            ser_col->reserve(3);
            VectorBufferWriter buffer_writer(*ser_col.get());
            st = serde->serialize_column_to_text(*col, 0, buffer_writer);
            EXPECT_EQ(st.ok(), true);
            buffer_writer.commit();
            st = serde->serialize_column_to_text(*col, 1, buffer_writer);
            EXPECT_EQ(st.ok(), true);
            buffer_writer.commit();
            st = serde->serialize_column_to_text(*col, 2, buffer_writer);
            EXPECT_EQ(st.ok(), true);
            buffer_writer.commit();
            rtrim(min_s);
            rtrim(max_s);
            rtrim(rand_ip);
            StringRef min_s_d = ser_col->get_data_at(0);
            StringRef max_s_d = ser_col->get_data_at(1);
            StringRef rand_s_d = ser_col->get_data_at(2);

            std::cout << "min(" << min_s << ") with data_type_str:" << min_s_d << std::endl;
            std::cout << "max(" << max_s << ") with data_type_str:" << max_s_d << std::endl;
            std::cout << "rand(" << rand_ip << ") with data_type_str:" << rand_s_d << std::endl;
            EXPECT_EQ(min_s, min_s_d.to_string());
            EXPECT_EQ(max_s, max_s_d.to_string());
            EXPECT_EQ(rand_ip, rand_s_d.to_string());
        }
    }
}

// test for array and map
TEST(TestSerdeText, ComplexTypeSerdeTextTest) {
    // array-scala
    {
        // nested type,test string, expect string(option.converted_from_string=true)
        using FieldType_RandStr = std::tuple<FieldType, std::vector<string>, std::vector<string>>;
        std::vector<FieldType_RandStr> nested_field_types = {
                FieldType_RandStr(FieldType::OLAP_FIELD_TYPE_BOOL,
                                  {"[0, 1,-1,1]", "[true, false]", "[1,true,t]",
                                   "[1, false], [,], [1,true,t]", "[,]"},
                                  {"[0, 1, null, 1]", "[1, 0]", "[1, 1, null]",
                                   "[1, null, null, 1, null]", "[]"}),
                FieldType_RandStr(
                        FieldType::OLAP_FIELD_TYPE_TINYINT,
                        {"[1111, 12, ]", "[ed, 2,]", "[],[]", "[[]]", "[,1 , 3]"},
                        {"[null, 12, null]", "[null, 2, null]", "[null]", "[null]", "[]"}),
                FieldType_RandStr(
                        FieldType::OLAP_FIELD_TYPE_FLOAT,
                        {"[0.33, 0.67, 0]", "[3.40282e+38, 3.40282e+38+1]", "[\"3.40282e+38+1\"]",
                         "[\"3.14\", 0.77]"},
                        {"[0.33, 0.67, 0]", "[3.40282e+38, null]", "[null]", "[3.14, 0.77]"}),
                FieldType_RandStr(
                        FieldType::OLAP_FIELD_TYPE_DOUBLE,
                        {"[3.1415926, 0.878787878, 12.44456475432]",
                         "[2343.12345465746, 2.22507e-308, 2.22507e-308-1, \"2.22507e-308\"]"},
                        {"[3.1415926, 0.878787878, 12.44456475432]",
                         "[2343.12345465746, 2.22507e-308, null, 2.22507e-308]"}),
                FieldType_RandStr(
                        FieldType::OLAP_FIELD_TYPE_STRING,
                        {"[\"hello\", \"world\"]", "['a', 'b', 'c']",
                         "[\"42\",1412341,true,42.43,3.40282e+38+1,alpha:beta:gamma,Earth#42:"
                         "Control#86:Bob#31,17:true:Abe "
                         "Linkedin,BLUE,\"\\N\",\"\u0001\u0002\u0003,\\u0001bc\"]",
                         "[\"heeeee\",null,\"null\",\"\\N\",null,\"sssssssss\"]"},
                        {"[\"hello\", \"world\"]", "[\"a\", \"b\", \"c\"]",
                         "[\"42\", \"1412341\", \"true\", \"42.43\", \"3.40282e+38+1\", "
                         "\"alpha:beta:gamma\", "
                         "\"Earth#42:Control#86:Bob#31\", \"17:true:Abe Linkedin\", \"BLUE\", "
                         "\"\\N\", "
                         "\"\x1\x2\x3,\\u0001bc\"]",
                         "[\"heeeee\", null, \"null\", \"\\N\", null, \"sssssssss\"]"}),
                FieldType_RandStr(FieldType::OLAP_FIELD_TYPE_DATE,
                                  {R"(["2022-07-13","2022-07-13"])",
                                   R"(["2023-07-13","2022-07-13"])", R"([null,"2022-07-13"])"},
                                  {R"(["2022-07-13", "2022-07-13"])",
                                   R"(["2023-07-13", "2022-07-13"])", R"([null, "2022-07-13"])"}),
                FieldType_RandStr(FieldType::OLAP_FIELD_TYPE_DATETIME,
                                  {
                                          R"(["2022-07-13","2022-07-13 12:30:00"])",
                                  },
                                  {
                                          R"(["2022-07-13 00:00:00", "2022-07-13 12:30:00"])",
                                  }),
                FieldType_RandStr(
                        FieldType::OLAP_FIELD_TYPE_DECIMAL,
                        {"[4, 5.5, 6.67]",
                         "[012345678901234567.012345678,123456789012345678.01234567, "
                         "12345678901234567.0123456779,12345678901234567.01234567791,"
                         "1234567890123456789.01234567]",
                         "[\"012345678901234567.012345678\",\"123456789012345678.01234567\", "
                         "\"12345678901234567.0123456779\", "
                         "\"12345678901234567.01234567791\",\"1234567890123456789.01234567\"]",
                         "[\\1234567890123456789.01234567\\]"},
                        {"[4.000000000, 5.500000000, 6.670000000]",
                         "[12345678901234567.012345678, 123456789012345678.012345670, "
                         "12345678901234567.012345678, 12345678901234567.012345678, null]",
                         "[12345678901234567.012345678, 123456789012345678.012345670, "
                         "12345678901234567.012345678, 12345678901234567.012345678, null]",
                         "[null]"}),
        };
        // array type
        for (auto type_pair : nested_field_types) {
            auto type = std::get<0>(type_pair);
            DataTypePtr nested_data_type_ptr;
            if (type == FieldType::OLAP_FIELD_TYPE_DECIMAL) {
                nested_data_type_ptr = DataTypeFactory::instance().create_data_type(type, 27, 9);
            } else {
                nested_data_type_ptr = DataTypeFactory::instance().create_data_type(type, 0, 0);
            }
            DataTypePtr array_data_type_ptr = make_nullable(
                    std::make_shared<DataTypeArray>(make_nullable(nested_data_type_ptr)));

            std::cout << "========= This type is  " << array_data_type_ptr->get_name() << ": "
                      << fmt::format("{}", type) << std::endl;

            auto col = array_data_type_ptr->create_column();
            auto col2 = array_data_type_ptr->create_column();
            auto col3 = array_data_type_ptr->create_column();

            DataTypeSerDeSPtr serde = array_data_type_ptr->get_serde();
            DataTypeSerDeSPtr serde_1 = array_data_type_ptr->get_serde();
            DataTypeSerDe::FormatOptions formatOptions;

            for (int i = 0; i < std::get<1>(type_pair).size(); ++i) {
                std::string rand_str = std::get<1>(type_pair)[i];
                std::string expect_str = std::get<2>(type_pair)[i];
                std::cout << "rand_str:" << rand_str << std::endl;
                std::cout << "expect_str:" << expect_str << std::endl;
                {
                    // from_string
                    ReadBuffer rb(rand_str.data(), rand_str.size());
                    Status status = array_data_type_ptr->from_string(rb, col2.get());
                    EXPECT_EQ(status.ok(), true);
                    auto ser_col = ColumnString::create();
                    ser_col->reserve(1);
                    VectorBufferWriter buffer_writer(*ser_col.get());
                    status = serde->serialize_column_to_text(*col2, i, buffer_writer);
                    EXPECT_EQ(status.ok(), true);
                    buffer_writer.commit();
                    StringRef rand_s_d = ser_col->get_data_at(0);
                    std::cout << "test from string: " << rand_s_d << std::endl;
                }
                {
                    formatOptions.converted_from_string = true;
                    std::cout << "======== change " << formatOptions.converted_from_string
                              << " with rand_str: " << rand_str << std::endl;
                    Slice slice(rand_str.data(), rand_str.size());
                    Status st =
                            serde_1->deserialize_one_cell_from_json(*col3, slice, formatOptions);
                    if (expect_str == "[]") {
                        EXPECT_EQ(st.ok(), true);
                        std::cout << st.to_json() << std::endl;
                    } else {
                        EXPECT_EQ(st.ok(), true);
                        auto ser_col = ColumnString::create();
                        ser_col->reserve(1);
                        VectorBufferWriter buffer_writer(*ser_col.get());
                        st = serde_1->serialize_column_to_text(*col3, i, buffer_writer);
                        EXPECT_EQ(st.ok(), true);
                        buffer_writer.commit();
                        StringRef rand_s_d = ser_col->get_data_at(0);
                        EXPECT_EQ(expect_str, rand_s_d.to_string());
                    }
                }
            }
        }
    }
}
} // namespace doris::vectorized