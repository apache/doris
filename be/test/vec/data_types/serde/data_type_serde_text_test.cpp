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
#include "olap/types.h" // for TypeInfo
#include "olap/wrapper_field.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/common/string_buffer.hpp"
#include "vec/core/field.h"
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
TEST(TextSerde, ScalaDataTypeSerdeTextTest) {
    // arithmetic scala field types
    {
        // fieldType, test_string, expect_string
        typedef std::tuple<FieldType, std::vector<string>, std::vector<string>> FieldType_RandStr;
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
                FieldType_RandStr(FieldType::OLAP_FIELD_TYPE_DECIMAL,
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
                                   "12345678901234567.012345678", "", ""}),
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
            if (type == FieldType::OLAP_FIELD_TYPE_DECIMAL32) {
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
                serde->serialize_one_cell_to_json(*col, i, buffer_writer, default_format_option);
                buffer_writer.commit();
                EXPECT_EQ(ser_col->get_data_at(ser_col->size() - 1).to_string(),
                          std::get<2>(type_pair)[i]);
            }
        }
    }

    // date and datetime type
    {
        typedef std::pair<FieldType, string> FieldType_RandStr;
        std::vector<FieldType_RandStr> date_scala_field_types = {
                FieldType_RandStr(FieldType::OLAP_FIELD_TYPE_DATE, "2020-01-01"),
                FieldType_RandStr(FieldType::OLAP_FIELD_TYPE_DATE, "2020-01-01"),
                FieldType_RandStr(FieldType::OLAP_FIELD_TYPE_DATEV2, "2020-01-01"),
                FieldType_RandStr(FieldType::OLAP_FIELD_TYPE_DATETIME, "2020-01-01 12:00:00"),
                FieldType_RandStr(FieldType::OLAP_FIELD_TYPE_DATETIMEV2,
                                  "2020-01-01 12:00:00.666666"),
        };
        for (auto pair : date_scala_field_types) {
            auto type = pair.first;
            DataTypePtr data_type_ptr = DataTypeFactory::instance().create_data_type(type, 0, 0);
            std::cout << "========= This type is  " << data_type_ptr->get_name() << ": "
                      << fmt::format("{}", type) << std::endl;

            std::unique_ptr<WrapperField> min_wf(WrapperField::create_by_type(type));
            std::unique_ptr<WrapperField> max_wf(WrapperField::create_by_type(type));
            std::unique_ptr<WrapperField> rand_wf(WrapperField::create_by_type(type));

            min_wf->set_to_min();
            max_wf->set_to_max();
            rand_wf->from_string(pair.second, 0, 0);

            string min_s = min_wf->to_string();
            string max_s = max_wf->to_string();
            string rand_date = rand_wf->to_string();

            Slice min_rb(min_s.data(), min_s.size());
            Slice max_rb(max_s.data(), max_s.size());
            Slice rand_rb(rand_date.data(), rand_date.size());

            auto col = data_type_ptr->create_column();
            DataTypeSerDeSPtr serde = data_type_ptr->get_serde();
            // make use c++ lib equals to wrapper field from_string behavior
            DataTypeSerDe::FormatOptions formatOptions;
            formatOptions.date_olap_format = true;

            Status st = serde->deserialize_one_cell_from_json(*col, min_rb, formatOptions);
            EXPECT_EQ(st.ok(), true);
            st = serde->deserialize_one_cell_from_json(*col, max_rb, formatOptions);
            EXPECT_EQ(st.ok(), true);
            st = serde->deserialize_one_cell_from_json(*col, rand_rb, formatOptions);
            EXPECT_EQ(st.ok(), true);

            auto ser_col = ColumnString::create();
            ser_col->reserve(3);
            VectorBufferWriter buffer_writer(*ser_col.get());
            serde->serialize_one_cell_to_json(*col, 0, buffer_writer, formatOptions);
            buffer_writer.commit();
            serde->serialize_one_cell_to_json(*col, 1, buffer_writer, formatOptions);
            buffer_writer.commit();
            serde->serialize_one_cell_to_json(*col, 2, buffer_writer, formatOptions);
            buffer_writer.commit();
            rtrim(min_s);
            rtrim(max_s);
            rtrim(rand_date);
            StringRef min_s_d = ser_col->get_data_at(0);
            StringRef max_s_d = ser_col->get_data_at(1);
            StringRef rand_s_d = ser_col->get_data_at(2);

            std::cout << "min(" << min_s << ") with datat_ype_str:" << min_s_d << std::endl;
            std::cout << "max(" << max_s << ") with datat_ype_str:" << max_s_d << std::endl;
            std::cout << "rand(" << rand_date << ") with datat_type_str:" << rand_s_d << std::endl;
            EXPECT_EQ(min_s, min_s_d.to_string());
            EXPECT_EQ(max_s, max_s_d.to_string());
            EXPECT_EQ(rand_date, rand_s_d.to_string());
        }
    }

    // nullable data type with const column
    {
        DataTypePtr data_type_ptr = DataTypeFactory::instance().create_data_type(
                FieldType::OLAP_FIELD_TYPE_STRING, 0, 0);
        DataTypePtr nullable_ptr = std::make_shared<DataTypeNullable>(data_type_ptr);
        std::unique_ptr<WrapperField> rand_wf(
                WrapperField::create_by_type(FieldType::OLAP_FIELD_TYPE_STRING));
        std::string test_str = generate(128);
        rand_wf->from_string(test_str, 0, 0);
        Field string_field(test_str);
        ColumnPtr col = nullable_ptr->create_column_const(0, string_field);
        DataTypeSerDe::FormatOptions default_format_option;
        DataTypeSerDeSPtr serde = nullable_ptr->get_serde();
        auto ser_col = ColumnString::create();
        ser_col->reserve(1);
        VectorBufferWriter buffer_writer(*ser_col.get());
        serde->serialize_one_cell_to_json(*col, 0, buffer_writer, default_format_option);
        buffer_writer.commit();
        StringRef rand_s_d = ser_col->get_data_at(0);
        EXPECT_EQ(rand_wf->to_string(), rand_s_d.to_string());
    }
}

// test for array and map
TEST(TextSerde, ComplexTypeSerdeTextTest) {
    // array-scala
    {
        // nested type,test string, expect string(option.converted_from_string=false),expect string(option.converted_from_string=true)
        typedef std::tuple<FieldType, std::vector<string>, std::vector<string>, std::vector<string>>
                FieldType_RandStr;
        std::vector<FieldType_RandStr> nested_field_types = {
                FieldType_RandStr(FieldType::OLAP_FIELD_TYPE_BOOL,
                                  {"[0, 1,-1,1]", "[true, false]", "[1,true,t]",
                                   "[1, false], [,], [1,true,t]", "[,]"},
                                  {"[0, 1, NULL, 1]", "[1, 0]", "[1, 1, NULL]",
                                   "[1, NULL, NULL, 1, NULL]", "[]"},
                                  {"[0, 1, NULL, 1]", "[1, 0]", "[1, 1, NULL]",
                                   "[1, NULL, NULL, 1, NULL]", "[]"}),
                FieldType_RandStr(
                        FieldType::OLAP_FIELD_TYPE_TINYINT,
                        {"[1111, 12, ]", "[ed, 2,]", "[],[]", "[[]]", "[,1 , 3]"},
                        {"[NULL, 12, NULL]", "[NULL, 2, NULL]", "[NULL]", "[NULL]", "[]"},
                        {"[NULL, 12, NULL]", "[NULL, 2, NULL]", "[NULL]", "[NULL]", "[]"}),
                FieldType_RandStr(
                        FieldType::OLAP_FIELD_TYPE_FLOAT,
                        {"[0.33, 0.67, 0]", "[3.40282e+38, 3.40282e+38+1]", "[\"3.40282e+38+1\"]",
                         "[\"3.14\", 0.77]"},
                        {"[0.33, 0.67, 0]", "[3.40282e+38, NULL]", "[NULL]", "[NULL, 0.77]"},
                        {"[0.33, 0.67, 0]", "[3.40282e+38, NULL]", "[NULL]", "[3.14, 0.77]"}),
                FieldType_RandStr(
                        FieldType::OLAP_FIELD_TYPE_DOUBLE,
                        {"[3.1415926, 0.878787878, 12.44456475432]",
                         "[2343.12345465746, 2.22507e-308, 2.22507e-308-1, \"2.22507e-308\"]"},
                        {"[3.1415926, 0.878787878, 12.44456475432]",
                         "[2343.12345465746, 2.22507e-308, NULL, NULL]"},
                        {"[3.1415926, 0.878787878, 12.44456475432]",
                         "[2343.12345465746, 2.22507e-308, NULL, 2.22507e-308]"}),
                FieldType_RandStr(
                        FieldType::OLAP_FIELD_TYPE_STRING,
                        {"[\"hello\", \"world\"]", "['a', 'b', 'c']",
                         "[\"42\",1412341,true,42.43,3.40282e+38+1,alpha:beta:gamma,Earth#42:"
                         "Control#86:Bob#31,17:true:Abe "
                         "Linkedin,BLUE,\"\\N\",\"\u0001\u0002\u0003,\\u0001bc\"]",
                         "[\"heeeee\",null,\"NULL\",\"\\N\",null,\"sssssssss\"]"},
                        // last : ["42",1412341,true,42.43,3.40282e+38+1,alpha:beta:gamma,Earth#42:Control#86:Bob#31,17:true:Abe Linkedin,BLUE,"\N",",\u0001bc"]
                        {"[\"hello\", \"world\"]", "[\"a\", \"b\", \"c\"]",
                         "[\"42\", \"1412341\", \"true\", \"42.43\", \"3.40282e+38+1\", "
                         "\"alpha:beta:gamma\", "
                         "\"Earth#42:Control#86:Bob#31\", \"17:true:Abe Linkedin\", \"BLUE\", "
                         "\"\\N\", "
                         "\"\x1\x2\x3,\\u0001bc\"]",
                         "[\"heeeee\", null, \"null\", \"\\N\", null, \"sssssssss\"]"},
                        {"[\"hello\", \"world\"]", "[\"a\", \"b\", \"c\"]",
                         "[\"42\", \"1412341\", \"true\", \"42.43\", \"3.40282e+38+1\", "
                         "\"alpha:beta:gamma\", "
                         "\"Earth#42:Control#86:Bob#31\", \"17:true:Abe Linkedin\", \"BLUE\", "
                         "\"\\N\", "
                         "\"\x1\x2\x3,\\u0001bc\"]",
                         "[\"heeeee\", null, \"null\", \"\\N\", null, \"sssssssss\"]"}),
                FieldType_RandStr(
                        FieldType::OLAP_FIELD_TYPE_DATE,
                        {"[\\\"2022-07-13\\\",\"2022-07-13 12:30:00\"]",
                         "[2022-07-13 12:30:00, \"2022-07-13\"]",
                         "[2022-07-13 12:30:00.000, 2022-07-13]"},
                        {"[NULL, NULL]", "[2022-07-13, NULL]", "[2022-07-13, 2022-07-13]"},
                        {"[NULL, 2022-07-13]", "[2022-07-13, 2022-07-13]",
                         "[2022-07-13, 2022-07-13]"}),
                FieldType_RandStr(
                        FieldType::OLAP_FIELD_TYPE_DATETIME,
                        {
                                "[\"2022-07-13\",\"2022-07-13 12:30:00\"]",
                                "[2022-07-13 12:30:00, \"2022-07-13\", 2022-07-13 12:30:00.0000]",
                                "\\N",
                                "[null,null,null]",
                        },
                        {"[NULL, NULL]", "[2022-07-13 12:30:00, NULL, 2022-07-13 12:30:00]", "NULL",
                         "[NULL, NULL, NULL]"},
                        {"[2022-07-13 00:00:00, 2022-07-13 12:30:00]",
                         "[2022-07-13 12:30:00, 2022-07-13 00:00:00, 2022-07-13 12:30:00]", "NULL",
                         "[NULL, NULL, NULL]"}),
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
                         "12345678901234567.012345678, NULL, NULL]",
                         "[NULL, NULL, NULL, NULL, NULL]", "[NULL]"},
                        {"[4.000000000, 5.500000000, 6.670000000]",
                         "[12345678901234567.012345678, 123456789012345678.012345670, "
                         "12345678901234567.012345678, NULL, NULL]",
                         "[12345678901234567.012345678, 123456789012345678.012345670, "
                         "12345678901234567.012345678, NULL, NULL]",
                         "[NULL]"}),
        };
        // array type
        for (auto type_pair : nested_field_types) {
            auto type = std::get<0>(type_pair);
            DataTypePtr nested_data_type_ptr =
                    DataTypeFactory::instance().create_data_type(type, 0, 0);
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
                std::string expect_str_1 = std::get<3>(type_pair)[i];
                std::cout << "rand_str:" << rand_str << std::endl;
                std::cout << "expect_str:" << expect_str << std::endl;
                std::cout << "expect_str_can_format_from_string:" << expect_str_1 << std::endl;
                {
                    Slice slice(rand_str.data(), rand_str.size());
                    formatOptions.converted_from_string = false;
                    Status st = serde->deserialize_one_cell_from_json(*col, slice, formatOptions);
                    if (expect_str == "[]") {
                        if (st.ok()) {
                            auto& item_column = assert_cast<ColumnNullable&>(
                                    assert_cast<ColumnArray&>(
                                            assert_cast<ColumnNullable&>(*col).get_nested_column())
                                            .get_data());
                            for (auto ix = 0; ix < item_column.size(); ++ix) {
                                if (item_column.is_null_at(ix)) {
                                    std::cout << "idx null:" << ix << std::endl;
                                } else {
                                    std::cout << "idx:" << item_column.get_data_at(ix).to_string()
                                              << std::endl;
                                }
                            }
                        } else {
                            EXPECT_EQ(st.ok(), false);
                            std::cout << st.to_json() << std::endl;
                        }
                    } else {
                        EXPECT_EQ(st.ok(), true);
                        auto ser_col = ColumnString::create();
                        ser_col->reserve(1);
                        VectorBufferWriter buffer_writer(*ser_col.get());
                        serde->serialize_one_cell_to_json(*col, i, buffer_writer, formatOptions);
                        buffer_writer.commit();
                        StringRef rand_s_d = ser_col->get_data_at(0);
                        std::cout << "test : " << rand_s_d << std::endl;
                        EXPECT_EQ(expect_str, rand_s_d.to_string());
                    }
                }
                {
                    // from_string
                    ReadBuffer rb(rand_str.data(), rand_str.size());
                    Status status = array_data_type_ptr->from_string(rb, col2);
                    EXPECT_EQ(status.ok(), true);
                    auto ser_col = ColumnString::create();
                    ser_col->reserve(1);
                    VectorBufferWriter buffer_writer(*ser_col.get());
                    serde->serialize_one_cell_to_json(*col2, i, buffer_writer, formatOptions);
                    buffer_writer.commit();
                    StringRef rand_s_d = ser_col->get_data_at(0);
                    std::cout << "test from string: " << rand_s_d << std::endl;
                    //                    EXPECT_EQ(expect_str, rand_s_d.to_string());
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
                        serde_1->serialize_one_cell_to_json(*col3, i, buffer_writer, formatOptions);
                        buffer_writer.commit();
                        StringRef rand_s_d = ser_col->get_data_at(0);
                        EXPECT_EQ(expect_str_1, rand_s_d.to_string());
                    }
                }
            }
        }
    }

    // map-scala-scala
    {
        // nested key type , nested value type, test string , expect string
        typedef std::tuple<FieldType, FieldType, std::vector<string>, std::vector<string>>
                FieldType_RandStr;
        std::vector<FieldType_RandStr> nested_field_types = {
                FieldType_RandStr(FieldType::OLAP_FIELD_TYPE_BOOL,
                                  FieldType::OLAP_FIELD_TYPE_STRING,
                                  {"{1: \"amory is 7\", 0: \" doris be better \", -1: \"wrong,\"}",
                                   "{\"1\": \"amory is 7\", \"0\": 1}"},
                                  {"{1:\"amory is 7\", 0:\" doris be better \", null:\"wrong,\"}",
                                   "{null:\"amory is 7\", null:\"1\"}"}),
                FieldType_RandStr(
                        FieldType::OLAP_FIELD_TYPE_STRING, FieldType::OLAP_FIELD_TYPE_DOUBLE,
                        {"{\" ,.amory\": 111.2343, \"\": 112., 'dggs': 13.14 , NULL: 12.2222222, "
                         ": NULL\\}",
                         "{\"\": NULL, null: 12.44}", "{{}}", "{{}", "}}", "{}, {}", "\\N",
                         "{null:null,\"null\":null}",
                         "{\"hello "
                         "world\":0.2222222,\"hello2\":null,null:1111.1,\"NULL\":null,\"null\":"
                         "null,\"null\":0.1}"},
                        {"{\" ,.amory\":111.2343, \"\"\"\":112, \"dggs\":13.14, "
                         "null:12.2222222, \"\":null}",
                         "{\"\"\"\":null, null:12.44}", "{}", "{}", "\\N", "{}", "\\N",
                         "{null:null, \"null\":null}",
                         "{\"hello world\":0.2222222, \"hello2\":null, null:1111.1, "
                         "\"null\":null, \"null\":null, "
                         "\"null\":0.1}"}),
                FieldType_RandStr(FieldType::OLAP_FIELD_TYPE_FLOAT,
                                  FieldType::OLAP_FIELD_TYPE_DOUBLE,
                                  {"{0.33: 3.1415926,3.1415926: 22}", "{3.14, 15926: 22}", "{3.14}",
                                   "{222:3444},", "{4.12, 677: 455: 356, 67.6:67.7}",
                                   "{null:null,null:1.0,1.0:null}"},
                                  {"{0.33:3.1415926, 3.1415925:22}", "{NULL:22}", "{}", "NULL",
                                   "{NULL:NULL, 67.6:67.7}", "{NULL:NULL, NULL:1, 1:NULL}"}),
                FieldType_RandStr(
                        FieldType::OLAP_FIELD_TYPE_DATE, FieldType::OLAP_FIELD_TYPE_DATETIME,
                        {"{2022-07-13: 2022-07-13 12:30:00, 2022-07-13 12:30:00: 2022-07-13 "
                         "12:30:00, 2022-07-13 12:30:00.000: 2022-07-13 12:30:00.000, NULL: NULL, "
                         "2022-07-13:'2022-07-13 12:30:00'}",
                         // escaped char ':'
                         "{2022-07-13 12\\:30\\:00: 2022-07-13, 2022-07-13 12\\:30\\:00.000: "
                         "2022-07-13 12:30:00.000, 2022-07-13:\'2022-07-13 12:30:00\'}",
                         "\\N"},
                        {"{2022-07-13:2022-07-13 12:30:00, 2022-07-13:NULL, 2022-07-13:NULL, "
                         "NULL:NULL, 2022-07-13:NULL}",
                         "{2022-07-13:2022-07-13 00:00:00, 2022-07-13:2022-07-13 12:30:00, "
                         "2022-07-13:NULL}",
                         "NULL"}),
                FieldType_RandStr(
                        FieldType::OLAP_FIELD_TYPE_DATETIME, FieldType::OLAP_FIELD_TYPE_DECIMAL,
                        {"{2022-07-13 12:30:00: 12.45675432, 2022-07-13: 12.45675432, NULL: NULL}",
                         "{\"2022-07-13 12:30:00\": \"12.45675432\"}",
                         "{2022-07-13 12\\:30\\:00:12.45675432, 2022-07-13#12:30:00: 12.45675432}",
                         "{2022-07-13 12\\:30\\:00.0000:12.45675432, null:12.34}"},
                        {"{2022-07-13 12:00:00:30.000000000, 2022-07-13 00:00:00:12.456754320, "
                         "NULL:NULL}",
                         "{NULL:NULL}",
                         "{2022-07-13 12:30:00:12.456754320, 2022-07-13 12:00:00:30.000000000}",
                         "{2022-07-13 12:30:00:12.456754320, NULL:12.340000000}"}),
        };

        for (auto type_pair : nested_field_types) {
            auto key_type = std::get<0>(type_pair);
            auto value_type = std::get<1>(type_pair);
            DataTypePtr nested_key_type_ptr =
                    DataTypeFactory::instance().create_data_type(key_type, 0, 0);
            DataTypePtr nested_value_type_ptr =
                    DataTypeFactory::instance().create_data_type(value_type, 0, 0);
            DataTypePtr map_data_type_ptr = make_nullable(std::make_shared<DataTypeMap>(
                    make_nullable(nested_key_type_ptr), make_nullable(nested_value_type_ptr)));

            std::cout << "========= This type is  " << map_data_type_ptr->get_name() << std::endl;

            auto col2 = map_data_type_ptr->create_column();
            DataTypeSerDeSPtr serde = map_data_type_ptr->get_serde();
            DataTypeSerDe::FormatOptions formatOptions;

            for (int i = 0; i < std::get<2>(type_pair).size(); ++i) {
                std::string rand_str = std::get<2>(type_pair)[i];
                std::string expect_str = std::get<3>(type_pair)[i];
                std::cout << "rand_str:" << rand_str << std::endl;
                std::cout << "expect_str:" << expect_str << std::endl;
                {
                    auto col = map_data_type_ptr->create_column();
                    Slice slice(rand_str.data(), rand_str.size());
                    Status st = serde->deserialize_one_cell_from_json(*col, slice, formatOptions);
                    std::cout << st.to_json() << std::endl;
                    if (expect_str.empty()) {
                        EXPECT_FALSE(st.ok());
                        continue;
                    }
                    auto ser_col = ColumnString::create();
                    ser_col->reserve(1);
                    VectorBufferWriter buffer_writer(*ser_col.get());
                    serde->serialize_one_cell_to_json(*col, 0, buffer_writer, formatOptions);
                    buffer_writer.commit();
                    StringRef rand_s_d = ser_col->get_data_at(0);
                    EXPECT_EQ(expect_str, rand_s_d.to_string());
                }
                // from_string
                {
                    ReadBuffer rb(rand_str.data(), rand_str.size());
                    std::cout << "from string rb: " << rb.to_string() << std::endl;
                    Status stat = map_data_type_ptr->from_string(rb, col2);
                    std::cout << stat.to_json() << std::endl;
                    auto ser_col = ColumnString::create();
                    ser_col->reserve(1);
                    VectorBufferWriter buffer_writer(*ser_col.get());
                    serde->serialize_one_cell_to_json(*col2, col2->size() - 1, buffer_writer,
                                                      formatOptions);
                    buffer_writer.commit();
                    StringRef rand_s_d = ser_col->get_data_at(0);
                    std::cout << "test from string: " << rand_s_d.to_string() << std::endl;
                }
            }
        }

        // option with converted_with_string true
        typedef std::tuple<FieldType, FieldType, std::vector<string>, std::vector<string>>
                FieldType_RandStr;
        std::vector<FieldType_RandStr> field_types = {
                FieldType_RandStr(
                        FieldType::OLAP_FIELD_TYPE_DATE, FieldType::OLAP_FIELD_TYPE_DATETIME,
                        {"{2022-07-13: 2022-07-13 12:30:00, 2022-07-13 12:30:00: 2022-07-13 "
                         "12:30:00, 2022-07-13 12:30:00.000: 2022-07-13 12:30:00.000, NULL: NULL, "
                         "2022-07-13:'2022-07-13 12:30:00'}",
                         // escaped char ':'
                         "{2022-07-13 12\\:30\\:00: 2022-07-13, 2022-07-13 12\\:30\\:00.000: "
                         "2022-07-13 12:30:00.000, 2022-07-13:\'2022-07-13 12:30:00\'}"},
                        {"{2022-07-13:2022-07-13 12:30:00, 2022-07-13:NULL, 2022-07-13:NULL, "
                         "NULL:NULL, 2022-07-13:2022-07-13 12:30:00}",
                         "{2022-07-13:2022-07-13 00:00:00, 2022-07-13:2022-07-13 12:30:00, "
                         "2022-07-13:2022-07-13 12:30:00}"}),
                FieldType_RandStr(
                        FieldType::OLAP_FIELD_TYPE_DATETIME, FieldType::OLAP_FIELD_TYPE_DECIMAL,
                        {"{2022-07-13 12:30:00: 12.45675432, 2022-07-13: 12.45675432, NULL: NULL}",
                         "{\"2022-07-13 12:30:00\": \"12.45675432\"}",
                         "{2022-07-13 12\\:30\\:00:12.45675432, 2022-07-13#12:30:00: 12.45675432}",
                         "{2022-07-13 12\\:30\\:00.0000:12.45675432, null:12.34}"},
                        {"{2022-07-13 12:00:00:30.000000000, 2022-07-13 00:00:00:12.456754320, "
                         "NULL:NULL}",
                         "{2022-07-13 12:30:00:12.456754320}",
                         "{2022-07-13 12:30:00:12.456754320, 2022-07-13 12:00:00:30.000000000}",
                         "{2022-07-13 12:30:00:12.456754320, NULL:12.340000000}"}),
        };
        for (auto type_pair : field_types) {
            auto key_type = std::get<0>(type_pair);
            auto value_type = std::get<1>(type_pair);
            DataTypePtr nested_key_type_ptr =
                    DataTypeFactory::instance().create_data_type(key_type, 0, 0);
            DataTypePtr nested_value_type_ptr =
                    DataTypeFactory::instance().create_data_type(value_type, 0, 0);
            DataTypePtr map_data_type_ptr = std::make_shared<DataTypeMap>(
                    make_nullable(nested_key_type_ptr), make_nullable(nested_value_type_ptr));

            std::cout << "========= This type is  " << map_data_type_ptr->get_name() << std::endl;

            auto col2 = map_data_type_ptr->create_column();
            DataTypeSerDeSPtr serde = map_data_type_ptr->get_serde();
            DataTypeSerDe::FormatOptions formatOptions;
            formatOptions.converted_from_string = true;

            for (int i = 0; i < std::get<2>(type_pair).size(); ++i) {
                std::string rand_str = std::get<2>(type_pair)[i];
                std::string expect_str = std::get<3>(type_pair)[i];
                std::cout << "rand_str:" << rand_str << std::endl;
                std::cout << "expect_str:" << expect_str << std::endl;
                {
                    auto col = map_data_type_ptr->create_column();
                    Slice slice(rand_str.data(), rand_str.size());
                    Status st = serde->deserialize_one_cell_from_json(*col, slice, formatOptions);
                    std::cout << st.to_json() << std::endl;
                    if (expect_str.empty()) {
                        EXPECT_FALSE(st.ok());
                        continue;
                    }
                    auto ser_col = ColumnString::create();
                    ser_col->reserve(1);
                    VectorBufferWriter buffer_writer(*ser_col.get());
                    serde->serialize_one_cell_to_json(*col, 0, buffer_writer, formatOptions);
                    buffer_writer.commit();
                    StringRef rand_s_d = ser_col->get_data_at(0);
                    EXPECT_EQ(expect_str, rand_s_d.to_string());
                }
            }
        }
    }
}

TEST(TextSerde, ComplexTypeWithNestedSerdeTextTest) {
    // array-array<string>
    { // nested type,test string, expect string(option.converted_from_string=false), expect_from_string, expect string(option.converted_from_string=true)
        typedef std::tuple<FieldType, std::vector<string>, std::vector<string>, std::vector<string>,
                           std::vector<string>>
                FieldType_RandStr;
        std::vector<FieldType_RandStr> nested_field_types = {
                FieldType_RandStr(
                        FieldType::OLAP_FIELD_TYPE_STRING,
                        {"[[Hello, World],[This, is, a, nested, array],null,[null,null,aaaa]]"},
                        {"[[\"Hello\", \"World\"], [\"This\", \"is\", \"a\", \"nested\", "
                         "\"array\"], null, [null, null, "
                         "\"aaaa\"]]"},
                        {"[null, null, null, null, null, null, null, null, null, null, null]"},
                        {"[[\"Hello\", \"World\"], [\"This\", \"is\", \"a\", \"nested\", "
                         "\"array\"], null, [null, null, "
                         "\"aaaa\"]]"}),
                FieldType_RandStr(
                        FieldType::OLAP_FIELD_TYPE_STRING,
                        {"[[With, special, \"characters\"], [like, @, #, $, % \"^\", &, *, (, ), "
                         "-, _], [=, +, [, ], {, }, |, \\, ;, :, ', '\', <, >, ,, ., /, ?, ~]]"},
                        {"[[\"With\", \"special\", \"characters\"], [\"like\", \"@\", \"#\", "
                         "\"$\", \"% \"^\"\", \"&\", \"*\", \"(\", \")\", \"-\", "
                         "\"_\"], [\"=\", \"+\", \"[, ]\", \"{, }\", \"|\", \"\\\", \";\", "
                         "\":\", \"', '', <, >, ,, ., /, ?, ~\"]]"},
                        {""},
                        {"[[\"With\", \"special\", \"characters\"], [\"like\", \"@\", \"#\", "
                         "\"$\", \"% \"^\"\", \"&\", \"*\", \"(\", \")\", \"-\", "
                         "\"_\"], [\"=\", \"+\", \"[, ]\", \"{, }\", \"|\", \"\\\", \";\", "
                         "\":\", \"', '', <, >, ,, ., /, ?, ~\"]]"})};
        // array type
        for (auto type_pair : nested_field_types) {
            auto type = std::get<0>(type_pair);
            DataTypePtr nested_data_type_ptr =
                    DataTypeFactory::instance().create_data_type(type, 0, 0);
            DataTypePtr nested_array_data_type_ptr =
                    std::make_shared<DataTypeArray>(make_nullable(nested_data_type_ptr));

            DataTypePtr array_data_type_ptr =
                    std::make_shared<DataTypeArray>(make_nullable(nested_array_data_type_ptr));

            std::cout << "========= This type is  " << array_data_type_ptr->get_name() << ": "
                      << fmt::format("{}", type) << std::endl;

            DataTypeSerDeSPtr serde = array_data_type_ptr->get_serde();
            DataTypeSerDeSPtr serde_1 = array_data_type_ptr->get_serde();
            DataTypeSerDe::FormatOptions formatOptions;

            for (int i = 0; i < std::get<1>(type_pair).size(); ++i) {
                std::string rand_str = std::get<1>(type_pair)[i];
                std::string expect_str = std::get<2>(type_pair)[i];
                std::string expect_from_string_str = std::get<3>(type_pair)[i];
                std::string expect_str_1 = std::get<4>(type_pair)[i];
                std::cout << "rand_str:" << rand_str << std::endl;
                std::cout << "expect_str:" << expect_str << std::endl;
                std::cout << "expect_from_str:" << expect_from_string_str << std::endl;
                std::cout << "expect_str_can_format_from_string:" << expect_str << std::endl;
                {
                    // serde
                    auto col = array_data_type_ptr->create_column();
                    Slice slice(rand_str.data(), rand_str.size());
                    formatOptions.converted_from_string = false;
                    Status st = serde->deserialize_one_cell_from_json(*col, slice, formatOptions);
                    if (expect_str == "") {
                        EXPECT_EQ(st.ok(), false);
                        std::cout << st.to_json() << std::endl;
                    } else {
                        EXPECT_EQ(st.ok(), true);
                        auto ser_col = ColumnString::create();
                        ser_col->reserve(1);
                        VectorBufferWriter buffer_writer(*ser_col.get());
                        serde->serialize_one_cell_to_json(*col, 0, buffer_writer, formatOptions);
                        buffer_writer.commit();
                        StringRef rand_s_d = ser_col->get_data_at(0);
                        std::cout << "test : " << rand_s_d << std::endl;
                        EXPECT_EQ(expect_str, rand_s_d.to_string());
                    }
                }
                {
                    // from_string
                    ReadBuffer rb(rand_str.data(), rand_str.size());
                    auto col2 = array_data_type_ptr->create_column();
                    Status status = array_data_type_ptr->from_string(rb, col2);
                    if (expect_from_string_str == "") {
                        EXPECT_EQ(status.ok(), false);
                        std::cout << "test from_string: " << status.to_json() << std::endl;
                    } else {
                        auto ser_col = ColumnString::create();
                        ser_col->reserve(1);
                        VectorBufferWriter buffer_writer(*ser_col.get());
                        serde->serialize_one_cell_to_json(*col2, 0, buffer_writer, formatOptions);
                        buffer_writer.commit();
                        StringRef rand_s_d = ser_col->get_data_at(0);
                        std::cout << "test from string: " << rand_s_d << std::endl;
                        EXPECT_EQ(expect_from_string_str, rand_s_d.to_string());
                    }
                }
                {
                    formatOptions.converted_from_string = true;
                    std::cout << "======== change " << formatOptions.converted_from_string
                              << " with rand_str: " << rand_str << std::endl;
                    auto col3 = array_data_type_ptr->create_column();
                    Slice slice(rand_str.data(), rand_str.size());
                    Status st =
                            serde_1->deserialize_one_cell_from_json(*col3, slice, formatOptions);
                    if (expect_str == "") {
                        EXPECT_EQ(st.ok(), false);
                        std::cout << st.to_json() << std::endl;
                    } else {
                        EXPECT_EQ(st.ok(), true);
                        auto ser_col = ColumnString::create();
                        ser_col->reserve(1);
                        VectorBufferWriter buffer_writer(*ser_col.get());
                        serde_1->serialize_one_cell_to_json(*col3, 0, buffer_writer, formatOptions);
                        buffer_writer.commit();
                        StringRef rand_s_d = ser_col->get_data_at(0);
                        EXPECT_EQ(expect_str_1, rand_s_d.to_string());
                    }
                }
            }
        }
    } // namespace doris::vectorized

    // array-map<string, double>
    {
        // nested type,test string, expect string(option.converted_from_string=false), expect_from_string, expect string(option.converted_from_string=true)
        typedef std::tuple<FieldType, FieldType, std::vector<string>, std::vector<string>,
                           std::vector<string>, std::vector<string>>
                FieldType_RandStr;
        std::vector<FieldType_RandStr> nested_field_types = {FieldType_RandStr(
                FieldType::OLAP_FIELD_TYPE_STRING, FieldType::OLAP_FIELD_TYPE_DOUBLE,
                {"[{\"2cKtIM-L1mOcEm-udR-HcB2\":0.23929040957798242,\"eof2UN-Is0EEuA-H5D-hE58\":0."
                 "42373055809540094,\"FwUSOB-R8rtK9W-BVG-8wYZ\":0.7680704548628841},{\"qDXU9D-"
                 "7orr51d-g80-6t5k\":0.6446245786874659,\"bkLjmx-uZ2Ez7F-536-PGqy\":0."
                 "8880791950937957,\"9Etq4o-FPm37O4-5fk-QWh7\":0.08630489716260481},{\"tu3OMw-"
                 "mzS0jAx-Dnj-Xm3G\":0.1184199213706042,\"XkhTn0-QFLo8Ks-JXR-k4zk\":0."
                 "5181239375482816,\"EYC8Dj-GTTp9iB-b4O-QBkO\":0.4491897722178303},{\"sHFGPg-"
                 "cfA8gya-kfw-IugT\":0.20842299487398452,\"BBQ6e5-OJYRJhC-zki-7rQj\":0."
                 "3050124830713523,\"mKH57V-YmwCNFq-vs8-vUIX\":0.36446683035480754},{\"HfhEMX-"
                 "oAMBJCC-YIC-hCqN\":0.8131454631693608,\"xrnTFd-ikONWik-T7J-sL8J\":0."
                 "37509722558990855,\"SVyEes-77mlzIr-N6c-DkYw\":0.4703053945053086,"
                 "\"null\":0.1,\"null\":0.1,null:null}, {null:0.1, null:null, \"null\":0}]"},
                {"[{\"2cKtIM-L1mOcEm-udR-HcB2\":0.23929040957798242, "
                 "\"eof2UN-Is0EEuA-H5D-hE58\":0.42373055809540094, "
                 "\"FwUSOB-R8rtK9W-BVG-8wYZ\":0.7680704548628841}, "
                 "{\"qDXU9D-7orr51d-g80-6t5k\":0.6446245786874659, "
                 "\"bkLjmx-uZ2Ez7F-536-PGqy\":0.8880791950937957, "
                 "\"9Etq4o-FPm37O4-5fk-QWh7\":0.08630489716260481}, "
                 "{\"tu3OMw-mzS0jAx-Dnj-Xm3G\":0.1184199213706042, "
                 "\"XkhTn0-QFLo8Ks-JXR-k4zk\":0.5181239375482816, "
                 "\"EYC8Dj-GTTp9iB-b4O-QBkO\":0.4491897722178303}, "
                 "{\"sHFGPg-cfA8gya-kfw-IugT\":0.20842299487398452, "
                 "\"BBQ6e5-OJYRJhC-zki-7rQj\":0.3050124830713523, "
                 "\"mKH57V-YmwCNFq-vs8-vUIX\":0.36446683035480754}, "
                 "{\"HfhEMX-oAMBJCC-YIC-hCqN\":0.8131454631693608, "
                 "\"xrnTFd-ikONWik-T7J-sL8J\":0.37509722558990855, "
                 "\"SVyEes-77mlzIr-N6c-DkYw\":0.4703053945053086, \"null\":0.1, \"null\":0.1, "
                 "null:null}, "
                 "{null:0.1, null:null, \"null\":0}]"},
                {""},
                {"[{\"2cKtIM-L1mOcEm-udR-HcB2\":0.23929040957798242, "
                 "\"eof2UN-Is0EEuA-H5D-hE58\":0.42373055809540094, "
                 "\"FwUSOB-R8rtK9W-BVG-8wYZ\":0.7680704548628841}, "
                 "{\"qDXU9D-7orr51d-g80-6t5k\":0.6446245786874659, "
                 "\"bkLjmx-uZ2Ez7F-536-PGqy\":0.8880791950937957, "
                 "\"9Etq4o-FPm37O4-5fk-QWh7\":0.08630489716260481}, "
                 "{\"tu3OMw-mzS0jAx-Dnj-Xm3G\":0.1184199213706042, "
                 "\"XkhTn0-QFLo8Ks-JXR-k4zk\":0.5181239375482816, "
                 "\"EYC8Dj-GTTp9iB-b4O-QBkO\":0.4491897722178303}, "
                 "{\"sHFGPg-cfA8gya-kfw-IugT\":0.20842299487398452, "
                 "\"BBQ6e5-OJYRJhC-zki-7rQj\":0.3050124830713523, "
                 "\"mKH57V-YmwCNFq-vs8-vUIX\":0.36446683035480754}, "
                 "{\"HfhEMX-oAMBJCC-YIC-hCqN\":0.8131454631693608, "
                 "\"xrnTFd-ikONWik-T7J-sL8J\":0.37509722558990855, "
                 "\"SVyEes-77mlzIr-N6c-DkYw\":0.4703053945053086, "
                 "\"null\":0.1, \"null\":0.1, null:null}, {null:0.1, null:null, \"null\":0}]"})};
        for (auto type_pair : nested_field_types) {
            auto key_type = std::get<0>(type_pair);
            DataTypePtr nested_key_data_type_ptr =
                    DataTypeFactory::instance().create_data_type(key_type, 0, 0);
            auto val_type = std::get<1>(type_pair);
            DataTypePtr nested_value_data_type_ptr =
                    DataTypeFactory::instance().create_data_type(val_type, 0, 0);

            DataTypePtr nested_map_data_type_ptr =
                    std::make_shared<DataTypeMap>(make_nullable(nested_key_data_type_ptr),
                                                  make_nullable(nested_value_data_type_ptr));

            DataTypePtr array_data_type_ptr =
                    std::make_shared<DataTypeArray>(make_nullable(nested_map_data_type_ptr));

            std::cout << "========= This type is  " << array_data_type_ptr->get_name() << std::endl;

            DataTypeSerDeSPtr serde = array_data_type_ptr->get_serde();
            DataTypeSerDeSPtr serde_1 = array_data_type_ptr->get_serde();
            DataTypeSerDe::FormatOptions formatOptions;

            for (int i = 0; i < std::get<2>(type_pair).size(); ++i) {
                std::string rand_str = std::get<2>(type_pair)[i];
                std::string expect_str = std::get<3>(type_pair)[i];
                std::string expect_from_string_str = std::get<4>(type_pair)[i];
                std::string expect_str_1 = std::get<5>(type_pair)[i];
                std::cout << "rand_str:" << rand_str << std::endl;
                std::cout << "expect_str:" << expect_str << std::endl;
                std::cout << "expect_from_str:" << expect_from_string_str << std::endl;
                std::cout << "expect_str_can_format_from_string:" << expect_str << std::endl;
                {
                    // serde
                    auto col = array_data_type_ptr->create_column();
                    Slice slice(rand_str.data(), rand_str.size());
                    formatOptions.converted_from_string = false;
                    Status st = serde->deserialize_one_cell_from_json(*col, slice, formatOptions);
                    if (expect_str == "") {
                        EXPECT_EQ(st.ok(), false);
                        std::cout << st.to_json() << std::endl;
                    } else {
                        EXPECT_EQ(st.ok(), true);
                        auto ser_col = ColumnString::create();
                        ser_col->reserve(1);
                        VectorBufferWriter buffer_writer(*ser_col.get());
                        serde->serialize_one_cell_to_json(*col, 0, buffer_writer, formatOptions);
                        buffer_writer.commit();
                        StringRef rand_s_d = ser_col->get_data_at(0);
                        std::cout << "test : " << rand_s_d << std::endl;
                        EXPECT_EQ(expect_str, rand_s_d.to_string());
                    }
                }
                {
                    // from_string
                    ReadBuffer rb(rand_str.data(), rand_str.size());
                    auto col2 = array_data_type_ptr->create_column();
                    Status status = array_data_type_ptr->from_string(rb, col2);
                    if (expect_from_string_str == "") {
                        EXPECT_EQ(status.ok(), false);
                        std::cout << "test from_string: " << status.to_json() << std::endl;
                    } else {
                        auto ser_col = ColumnString::create();
                        ser_col->reserve(1);
                        VectorBufferWriter buffer_writer(*ser_col.get());
                        serde->serialize_one_cell_to_json(*col2, 0, buffer_writer, formatOptions);
                        buffer_writer.commit();
                        StringRef rand_s_d = ser_col->get_data_at(0);
                        std::cout << "test from string: " << rand_s_d << std::endl;
                        EXPECT_EQ(expect_from_string_str, rand_s_d.to_string());
                    }
                }
                {
                    formatOptions.converted_from_string = true;
                    std::cout << "======== change " << formatOptions.converted_from_string
                              << " with rand_str: " << rand_str << std::endl;
                    auto col3 = array_data_type_ptr->create_column();
                    Slice slice(rand_str.data(), rand_str.size());
                    Status st =
                            serde_1->deserialize_one_cell_from_json(*col3, slice, formatOptions);
                    if (expect_str == "") {
                        EXPECT_EQ(st.ok(), false);
                        std::cout << st.to_json() << std::endl;
                    } else {
                        EXPECT_EQ(st.ok(), true);
                        auto ser_col = ColumnString::create();
                        ser_col->reserve(1);
                        VectorBufferWriter buffer_writer(*ser_col.get());
                        serde_1->serialize_one_cell_to_json(*col3, 0, buffer_writer, formatOptions);
                        buffer_writer.commit();
                        StringRef rand_s_d = ser_col->get_data_at(0);
                        EXPECT_EQ(expect_str_1, rand_s_d.to_string());
                    }
                }
            }
        }
    }

    // map-scala-array (map<scala,array<scala>>)
    {
        // nested type,test string, expect string(option.converted_from_string=false), expect_from_string, expect string(option.converted_from_string=true)
        typedef std::tuple<FieldType, FieldType, std::vector<string>, std::vector<string>,
                           std::vector<string>, std::vector<string>>
                FieldType_RandStr;
        std::vector<FieldType_RandStr> nested_field_types = {FieldType_RandStr(
                // map<string,array<double>>
                FieldType::OLAP_FIELD_TYPE_STRING, FieldType::OLAP_FIELD_TYPE_DOUBLE,
                {"{\"5Srn6n-SP9fOS3-khz-Ljwt\":[0.8537551959339321,0.13473869413865858,0."
                 "9806016478238296,0.23014415892941564,0.26853530959759686,0.05484935641143551,0."
                 "11181328816302816,0.26510985318905933,0.6350885463275475,0.18209889263574142],"
                 "\"vrQmBC-2WlpWML-V5S-OLgM\":[0.6982221340596457,0.9260447299229463,0."
                 "12488042737255534,0.8859407191137862,0.03201490973378984,0.8371916387557367,0."
                 "7894434066323907,0.29667576138232743,0.9837777568426148,0.7773721913552772],"
                 "\"3ZbiXK-VvmhFcg-09V-w3g3\":[0.20509046053951785,0.9175575704931109,0."
                 "305788438361256,0.9923240410251069,0.6612939841907548,0.5922056063112593,0."
                 "15750800821536715,0.6374743124669565,0.4158097731627699,0.00302193321816846],"
                 "\"gMswpS-Ele9wHM-Uxp-VxzC\":[0.14378032144751685,0.627919779177473,0."
                 "6188731271454715,0.8088384184584442,0.8169160298605824,0.9051151670055427,0."
                 "558001941204895,0.029409463113641787,0.9532987674717762,0.20833228278241533],"
                 "\"TT9P9f-PXjQnvN-RBx-xRiS\":[0.8276005878909756,0.470950932860423,0."
                 "2442851528127543,0.710599416715854,0.3353731152359334,0.622947602340124,0."
                 "30675353671676797,0.8190741661938367,0.633630372770242,0.9436322366112492],"
                 "\"gLAnZc-oF7PC9o-ryd-MOXr\":[0.9742716809818137,0.9114038616933997,0."
                 "47459239268645104,0.6054569900795078,0.5515590901916287,0.8833310208917589,0."
                 "96476090778518,0.8873874315592357,0.3577701257062156,0.6993447306713452],"
                 "\"zrq6BY-7FJg3hc-Dd1-bAJn\":[0.1038405592062176,0.6757819253774818,0."
                 "6386535502499314,0.23598674876945303,0.11046582465777044,0.6426056925348297,0."
                 "17289073092250662,0.37116009951425233,0.594677969672274,0.49351456402872274],"
                 "\"gCKqtW-bLaoxgZ-CuW-M2re\":[0.934169137905867,0.12015121444469123,0."
                 "5009923777544698,0.4689139716802634,0.7226298925299507,0.33486164698864984,0."
                 "32944768657449996,0.5051366150918063,0.03228636228382431,0.48211773870118435],"
                 "\"SWqhI2-XnF9jVR-dT1-Yrtt\":[0.8005897112110444,0.899180582368993,0."
                 "9232176819588501,0.8615673086606942,0.9248122266449379,0.5586489299212893,0."
                 "40494513773898455,0.4752644689010731,0.6668395567417462,0.9068738374244337],"
                 "\"Z85F6M-cy5K4GP-7I5-5KS9\":[0.34761241187833714,0.46467162849990507,0."
                 "009781307454025168,0.3174295126364216,0.6405423361175397,0.33838144910731327,0."
                 "328860321648657,0.032638966917555856,0.32782524002924884,0.7675689545937956],"
                 "\"rlcnbo-tFg1FfP-ra6-D9Z8\":[0.7450713997349928,0.792502852203968,0."
                 "9034039182796755,0.49131654565079996,0.25223293077647946,0.9827253462450637,0."
                 "1684868582627418,0.0417161505112974,0.8498128570850716,0.8948779001812955]}"},
                {"{\"5Srn6n-SP9fOS3-khz-Ljwt\":[0.8537551959339321, 0.13473869413865858, "
                 "0.9806016478238296, 0.23014415892941564, 0.26853530959759686, "
                 "0.05484935641143551, 0.11181328816302816, 0.26510985318905933, "
                 "0.6350885463275475, 0.18209889263574142], "
                 "\"vrQmBC-2WlpWML-V5S-OLgM\":[0.6982221340596457, 0.9260447299229463, "
                 "0.12488042737255534, 0.8859407191137862, 0.03201490973378984, "
                 "0.8371916387557367, 0.7894434066323907, 0.29667576138232743, 0.9837777568426148, "
                 "0.7773721913552772], \"3ZbiXK-VvmhFcg-09V-w3g3\":[0.20509046053951785, "
                 "0.9175575704931109, 0.305788438361256, 0.9923240410251069, 0.6612939841907548, "
                 "0.5922056063112593, 0.15750800821536715, 0.6374743124669565, 0.4158097731627699, "
                 "0.00302193321816846], \"gMswpS-Ele9wHM-Uxp-VxzC\":[0.14378032144751685, "
                 "0.627919779177473, 0.6188731271454715, 0.8088384184584442, 0.8169160298605824, "
                 "0.9051151670055427, 0.558001941204895, 0.029409463113641787, 0.9532987674717762, "
                 "0.20833228278241533], \"TT9P9f-PXjQnvN-RBx-xRiS\":[0.8276005878909756, "
                 "0.470950932860423, 0.2442851528127543, 0.710599416715854, 0.3353731152359334, "
                 "0.622947602340124, 0.30675353671676797, 0.8190741661938367, 0.633630372770242, "
                 "0.9436322366112492], \"gLAnZc-oF7PC9o-ryd-MOXr\":[0.9742716809818137, "
                 "0.9114038616933997, 0.47459239268645104, 0.6054569900795078, 0.5515590901916287, "
                 "0.8833310208917589, 0.96476090778518, 0.8873874315592357, 0.3577701257062156, "
                 "0.6993447306713452], \"zrq6BY-7FJg3hc-Dd1-bAJn\":[0.1038405592062176, "
                 "0.6757819253774818, 0.6386535502499314, 0.23598674876945303, "
                 "0.11046582465777044, 0.6426056925348297, 0.17289073092250662, "
                 "0.37116009951425233, 0.594677969672274, 0.49351456402872274], "
                 "\"gCKqtW-bLaoxgZ-CuW-M2re\":[0.934169137905867, 0.12015121444469123, "
                 "0.5009923777544698, 0.4689139716802634, 0.7226298925299507, 0.33486164698864984, "
                 "0.32944768657449996, 0.5051366150918063, 0.03228636228382431, "
                 "0.48211773870118435], \"SWqhI2-XnF9jVR-dT1-Yrtt\":[0.8005897112110444, "
                 "0.899180582368993, 0.9232176819588501, 0.8615673086606942, 0.9248122266449379, "
                 "0.5586489299212893, 0.40494513773898455, 0.4752644689010731, 0.6668395567417462, "
                 "0.9068738374244337], \"Z85F6M-cy5K4GP-7I5-5KS9\":[0.34761241187833714, "
                 "0.46467162849990507, 0.009781307454025168, 0.3174295126364216, "
                 "0.6405423361175397, 0.33838144910731327, 0.328860321648657, "
                 "0.032638966917555856, 0.32782524002924884, 0.7675689545937956], "
                 "\"rlcnbo-tFg1FfP-ra6-D9Z8\":[0.7450713997349928, 0.792502852203968, "
                 "0.9034039182796755, 0.49131654565079996, 0.25223293077647946, "
                 "0.9827253462450637, 0.1684868582627418, 0.0417161505112974, 0.8498128570850716, "
                 "0.8948779001812955]}"},
                {""},
                {"{\"5Srn6n-SP9fOS3-khz-Ljwt\":[0.8537551959339321, 0.13473869413865858, "
                 "0.9806016478238296, 0.23014415892941564, 0.26853530959759686, "
                 "0.05484935641143551, 0.11181328816302816, 0.26510985318905933, "
                 "0.6350885463275475, 0.18209889263574142], "
                 "\"vrQmBC-2WlpWML-V5S-OLgM\":[0.6982221340596457, 0.9260447299229463, "
                 "0.12488042737255534, 0.8859407191137862, 0.03201490973378984, "
                 "0.8371916387557367, 0.7894434066323907, 0.29667576138232743, 0.9837777568426148, "
                 "0.7773721913552772], \"3ZbiXK-VvmhFcg-09V-w3g3\":[0.20509046053951785, "
                 "0.9175575704931109, 0.305788438361256, 0.9923240410251069, 0.6612939841907548, "
                 "0.5922056063112593, 0.15750800821536715, 0.6374743124669565, 0.4158097731627699, "
                 "0.00302193321816846], \"gMswpS-Ele9wHM-Uxp-VxzC\":[0.14378032144751685, "
                 "0.627919779177473, 0.6188731271454715, 0.8088384184584442, 0.8169160298605824, "
                 "0.9051151670055427, 0.558001941204895, 0.029409463113641787, 0.9532987674717762, "
                 "0.20833228278241533], \"TT9P9f-PXjQnvN-RBx-xRiS\":[0.8276005878909756, "
                 "0.470950932860423, 0.2442851528127543, 0.710599416715854, 0.3353731152359334, "
                 "0.622947602340124, 0.30675353671676797, 0.8190741661938367, 0.633630372770242, "
                 "0.9436322366112492], \"gLAnZc-oF7PC9o-ryd-MOXr\":[0.9742716809818137, "
                 "0.9114038616933997, 0.47459239268645104, 0.6054569900795078, 0.5515590901916287, "
                 "0.8833310208917589, 0.96476090778518, 0.8873874315592357, 0.3577701257062156, "
                 "0.6993447306713452], \"zrq6BY-7FJg3hc-Dd1-bAJn\":[0.1038405592062176, "
                 "0.6757819253774818, 0.6386535502499314, 0.23598674876945303, "
                 "0.11046582465777044, 0.6426056925348297, 0.17289073092250662, "
                 "0.37116009951425233, 0.594677969672274, 0.49351456402872274], "
                 "\"gCKqtW-bLaoxgZ-CuW-M2re\":[0.934169137905867, 0.12015121444469123, "
                 "0.5009923777544698, 0.4689139716802634, 0.7226298925299507, 0.33486164698864984, "
                 "0.32944768657449996, 0.5051366150918063, 0.03228636228382431, "
                 "0.48211773870118435], \"SWqhI2-XnF9jVR-dT1-Yrtt\":[0.8005897112110444, "
                 "0.899180582368993, 0.9232176819588501, 0.8615673086606942, 0.9248122266449379, "
                 "0.5586489299212893, 0.40494513773898455, 0.4752644689010731, 0.6668395567417462, "
                 "0.9068738374244337], \"Z85F6M-cy5K4GP-7I5-5KS9\":[0.34761241187833714, "
                 "0.46467162849990507, 0.009781307454025168, 0.3174295126364216, "
                 "0.6405423361175397, 0.33838144910731327, 0.328860321648657, "
                 "0.032638966917555856, 0.32782524002924884, 0.7675689545937956], "
                 "\"rlcnbo-tFg1FfP-ra6-D9Z8\":[0.7450713997349928, 0.792502852203968, "
                 "0.9034039182796755, 0.49131654565079996, 0.25223293077647946, "
                 "0.9827253462450637, 0.1684868582627418, 0.0417161505112974, 0.8498128570850716, "
                 "0.8948779001812955]}"})};
        for (auto type_pair : nested_field_types) {
            auto key_type = std::get<0>(type_pair);
            DataTypePtr nested_key_data_type_ptr =
                    DataTypeFactory::instance().create_data_type(key_type, 0, 0);
            auto val_type = std::get<1>(type_pair);
            DataTypePtr nested_value_data_type_ptr =
                    DataTypeFactory::instance().create_data_type(val_type, 0, 0);
            DataTypePtr array_data_type_ptr =
                    std::make_shared<DataTypeArray>(make_nullable(nested_value_data_type_ptr));

            DataTypePtr map_data_type_ptr = std::make_shared<DataTypeMap>(
                    make_nullable(nested_key_data_type_ptr), make_nullable(array_data_type_ptr));

            std::cout << "========= This type is  " << map_data_type_ptr->get_name() << std::endl;

            DataTypeSerDeSPtr serde = map_data_type_ptr->get_serde();
            DataTypeSerDeSPtr serde_1 = map_data_type_ptr->get_serde();
            DataTypeSerDe::FormatOptions formatOptions;

            for (int i = 0; i < std::get<2>(type_pair).size(); ++i) {
                std::string rand_str = std::get<2>(type_pair)[i];
                std::string expect_str = std::get<3>(type_pair)[i];
                std::string expect_from_string_str = std::get<4>(type_pair)[i];
                std::string expect_str_1 = std::get<5>(type_pair)[i];
                std::cout << "rand_str:" << rand_str << std::endl;
                std::cout << "expect_str:" << expect_str << std::endl;
                std::cout << "expect_from_str:" << expect_from_string_str << std::endl;
                std::cout << "expect_str_can_format_from_string:" << expect_str << std::endl;
                {
                    // serde
                    auto col = map_data_type_ptr->create_column();
                    Slice slice(rand_str.data(), rand_str.size());
                    formatOptions.converted_from_string = false;
                    Status st = serde->deserialize_one_cell_from_json(*col, slice, formatOptions);
                    if (expect_str == "") {
                        EXPECT_EQ(st.ok(), false);
                        std::cout << st.to_json() << std::endl;
                    } else {
                        std::cout << "test : " << st.to_json() << std::endl;
                        EXPECT_EQ(st.ok(), true);
                        auto ser_col = ColumnString::create();
                        ser_col->reserve(1);
                        VectorBufferWriter buffer_writer(*ser_col.get());
                        serde->serialize_one_cell_to_json(*col, 0, buffer_writer, formatOptions);
                        buffer_writer.commit();
                        StringRef rand_s_d = ser_col->get_data_at(0);
                        std::cout << "test : " << rand_s_d << std::endl;
                        EXPECT_EQ(expect_str, rand_s_d.to_string());
                    }
                }
                {
                    // from_string
                    ReadBuffer rb(rand_str.data(), rand_str.size());
                    auto col2 = map_data_type_ptr->create_column();
                    Status status = map_data_type_ptr->from_string(rb, col2);
                    if (expect_from_string_str == "") {
                        EXPECT_EQ(status.ok(), false);
                        std::cout << "test from_string: " << status.to_json() << std::endl;
                    } else {
                        auto ser_col = ColumnString::create();
                        ser_col->reserve(1);
                        VectorBufferWriter buffer_writer(*ser_col.get());
                        serde->serialize_one_cell_to_json(*col2, 0, buffer_writer, formatOptions);
                        buffer_writer.commit();
                        StringRef rand_s_d = ser_col->get_data_at(0);
                        std::cout << "test from string: " << rand_s_d << std::endl;
                        EXPECT_EQ(expect_from_string_str, rand_s_d.to_string());
                    }
                }
                {
                    formatOptions.converted_from_string = true;
                    std::cout << "======== change " << formatOptions.converted_from_string
                              << " with rand_str: " << rand_str << std::endl;
                    auto col3 = map_data_type_ptr->create_column();
                    Slice slice(rand_str.data(), rand_str.size());
                    Status st =
                            serde_1->deserialize_one_cell_from_json(*col3, slice, formatOptions);
                    if (expect_str == "") {
                        EXPECT_EQ(st.ok(), false);
                        std::cout << st.to_json() << std::endl;
                    } else {
                        EXPECT_EQ(st.ok(), true);
                        auto ser_col = ColumnString::create();
                        ser_col->reserve(1);
                        VectorBufferWriter buffer_writer(*ser_col.get());
                        serde_1->serialize_one_cell_to_json(*col3, 0, buffer_writer, formatOptions);
                        buffer_writer.commit();
                        StringRef rand_s_d = ser_col->get_data_at(0);
                        EXPECT_EQ(expect_str_1, rand_s_d.to_string());
                    }
                }
            }
        }
    }

    // map-scala-map (map<string,map<string,double>>)
    {
        // nested type,test string, expect string(option.converted_from_string=false), expect_from_string, expect string(option.converted_from_string=true)
        typedef std::tuple<FieldType, FieldType, std::vector<string>, std::vector<string>,
                           std::vector<string>, std::vector<string>>
                FieldType_RandStr;
        std::vector<FieldType_RandStr> nested_field_types = {FieldType_RandStr(
                FieldType::OLAP_FIELD_TYPE_STRING, FieldType::OLAP_FIELD_TYPE_DOUBLE,
                {"{\"5H6iPe-CRvVE5Q-QnG-8WQb\":{},\"stDa6g-GML89aZ-w5u-LBe0\":{\"Vlekcq-LDCMo6f-"
                 "J7U-6rwB\":0.15375824233866453,\"4ljyNE-JMK1bSp-c05-EajL\":0.36153399717116075},"
                 "\"URvXyY-SMttaG4-Zol-mPak\":{\"xVaeqR-cj8I6EM-3Nt-queD\":0.003968938824538082,"
                 "\"Vt2mSs-wacYDvl-qUi-B7kI\":0.6900852274982441,\"i3cJJh-oskdqti-KGU-U6gC\":0."
                 "40773692843073994},\"N3R9TI-jtBPGOQ-uRc-aWAD\":{\"xmGI09-FaCFrrR-O5J-29eu\":0."
                 "7166939407858642,\"fbxIwJ-HLvW94X-tPn-JgKT\":0.05904881148976504,\"ylE7y1-"
                 "wI3UhjR-ecQ-bNfo\":0.9293354174058581,\"zA0pEV-Lm8g4wq-NJc-TDou\":0."
                 "4000067127237942}}"},
                {"{\"5H6iPe-CRvVE5Q-QnG-8WQb\":{}, "
                 "\"stDa6g-GML89aZ-w5u-LBe0\":{\"Vlekcq-LDCMo6f-J7U-6rwB\":0.15375824233866453, "
                 "\"4ljyNE-JMK1bSp-c05-EajL\":0.36153399717116075}, "
                 "\"URvXyY-SMttaG4-Zol-mPak\":{\"xVaeqR-cj8I6EM-3Nt-queD\":0.003968938824538082, "
                 "\"Vt2mSs-wacYDvl-qUi-B7kI\":0.6900852274982441, "
                 "\"i3cJJh-oskdqti-KGU-U6gC\":0.40773692843073994}, "
                 "\"N3R9TI-jtBPGOQ-uRc-aWAD\":{\"xmGI09-FaCFrrR-O5J-29eu\":0.7166939407858642, "
                 "\"fbxIwJ-HLvW94X-tPn-JgKT\":0.05904881148976504, "
                 "\"ylE7y1-wI3UhjR-ecQ-bNfo\":0.9293354174058581, "
                 "\"zA0pEV-Lm8g4wq-NJc-TDou\":0.4000067127237942}}"},
                {""},
                {"{\"5H6iPe-CRvVE5Q-QnG-8WQb\":{}, "
                 "\"stDa6g-GML89aZ-w5u-LBe0\":{\"Vlekcq-LDCMo6f-J7U-6rwB\":0.15375824233866453, "
                 "\"4ljyNE-JMK1bSp-c05-EajL\":0.36153399717116075}, "
                 "\"URvXyY-SMttaG4-Zol-mPak\":{\"xVaeqR-cj8I6EM-3Nt-queD\":0.003968938824538082, "
                 "\"Vt2mSs-wacYDvl-qUi-B7kI\":0.6900852274982441, "
                 "\"i3cJJh-oskdqti-KGU-U6gC\":0.40773692843073994}, "
                 "\"N3R9TI-jtBPGOQ-uRc-aWAD\":{\"xmGI09-FaCFrrR-O5J-29eu\":0.7166939407858642, "
                 "\"fbxIwJ-HLvW94X-tPn-JgKT\":0.05904881148976504, "
                 "\"ylE7y1-wI3UhjR-ecQ-bNfo\":0.9293354174058581, "
                 "\"zA0pEV-Lm8g4wq-NJc-TDou\":0.4000067127237942}}"})};
        for (auto type_pair : nested_field_types) {
            auto key_type = std::get<0>(type_pair);
            DataTypePtr nested_key_data_type_ptr =
                    DataTypeFactory::instance().create_data_type(key_type, 0, 0);
            auto val_type = std::get<1>(type_pair);
            DataTypePtr nested_value_data_type_ptr =
                    DataTypeFactory::instance().create_data_type(val_type, 0, 0);

            DataTypePtr nested_map_data_type_ptr =
                    std::make_shared<DataTypeMap>(make_nullable(nested_key_data_type_ptr),
                                                  make_nullable(nested_value_data_type_ptr));

            DataTypePtr array_data_type_ptr =
                    std::make_shared<DataTypeMap>(make_nullable(std::make_shared<DataTypeString>()),
                                                  make_nullable(nested_map_data_type_ptr));

            std::cout << " ========= ========= This type is  " << array_data_type_ptr->get_name()
                      << std::endl;

            DataTypeSerDeSPtr serde = array_data_type_ptr->get_serde();
            DataTypeSerDeSPtr serde_1 = array_data_type_ptr->get_serde();
            DataTypeSerDe::FormatOptions formatOptions;

            for (int i = 0; i < std::get<2>(type_pair).size(); ++i) {
                std::string rand_str = std::get<2>(type_pair)[i];
                std::string expect_str = std::get<3>(type_pair)[i];
                std::string expect_from_string_str = std::get<4>(type_pair)[i];
                std::string expect_str_1 = std::get<5>(type_pair)[i];
                std::cout << "rand_str:" << rand_str << std::endl;
                std::cout << "expect_str:" << expect_str << std::endl;
                std::cout << "expect_from_str:" << expect_from_string_str << std::endl;
                std::cout << "expect_str_can_format_from_string:" << expect_str << std::endl;
                {
                    // serde
                    auto col = array_data_type_ptr->create_column();
                    Slice slice(rand_str.data(), rand_str.size());
                    formatOptions.converted_from_string = false;
                    Status st = serde->deserialize_one_cell_from_json(*col, slice, formatOptions);
                    if (expect_str == "") {
                        EXPECT_EQ(st.ok(), false);
                        std::cout << st.to_json() << std::endl;
                    } else {
                        EXPECT_EQ(st.ok(), true);
                        auto ser_col = ColumnString::create();
                        ser_col->reserve(1);
                        VectorBufferWriter buffer_writer(*ser_col.get());
                        serde->serialize_one_cell_to_json(*col, 0, buffer_writer, formatOptions);
                        buffer_writer.commit();
                        StringRef rand_s_d = ser_col->get_data_at(0);
                        std::cout << "test : " << rand_s_d << std::endl;
                        EXPECT_EQ(expect_str, rand_s_d.to_string());
                    }
                }
                {
                    // from_string
                    ReadBuffer rb(rand_str.data(), rand_str.size());
                    auto col2 = array_data_type_ptr->create_column();
                    Status status = array_data_type_ptr->from_string(rb, col2);
                    if (expect_from_string_str == "") {
                        EXPECT_EQ(status.ok(), false);
                        std::cout << "test from_string: " << status.to_json() << std::endl;
                    } else {
                        auto ser_col = ColumnString::create();
                        ser_col->reserve(1);
                        VectorBufferWriter buffer_writer(*ser_col.get());
                        serde->serialize_one_cell_to_json(*col2, 0, buffer_writer, formatOptions);
                        buffer_writer.commit();
                        StringRef rand_s_d = ser_col->get_data_at(0);
                        std::cout << "test from string: " << rand_s_d << std::endl;
                        EXPECT_EQ(expect_from_string_str, rand_s_d.to_string());
                    }
                }
                {
                    formatOptions.converted_from_string = true;
                    std::cout << "======== change " << formatOptions.converted_from_string
                              << " with rand_str: " << rand_str << std::endl;
                    auto col3 = array_data_type_ptr->create_column();
                    Slice slice(rand_str.data(), rand_str.size());
                    Status st =
                            serde_1->deserialize_one_cell_from_json(*col3, slice, formatOptions);
                    if (expect_str == "") {
                        EXPECT_EQ(st.ok(), false);
                        std::cout << st.to_json() << std::endl;
                    } else {
                        EXPECT_EQ(st.ok(), true);
                        auto ser_col = ColumnString::create();
                        ser_col->reserve(1);
                        VectorBufferWriter buffer_writer(*ser_col.get());
                        serde_1->serialize_one_cell_to_json(*col3, 0, buffer_writer, formatOptions);
                        buffer_writer.commit();
                        StringRef rand_s_d = ser_col->get_data_at(0);
                        EXPECT_EQ(expect_str_1, rand_s_d.to_string());
                    }
                }
            }
        }
    }
}

TEST(TextSerde, test_slice) {
    Slice slice("[\"hello\", \"world\"]");
    slice.remove_prefix(1);
    slice.remove_suffix(1);
    std::vector<Slice> slices;
    slices.emplace_back(slice);
    //    size_t slice_size = slice.size;
    bool has_quote = false;
    int nested_level = 0;

    for (int idx = 0; idx < slice.size; ++idx) {
        char c = slice[idx];
        std::cout << "c:" << c << " " << fmt::format("{}, {}", c == '[', c == ']') << std::endl;
        if (c == '"' || c == '\'') {
            has_quote = !has_quote;
        } else if (!has_quote && (c == '[' || c == '{')) {
            ++nested_level;
        } else if (!has_quote && (c == ']' || c == '}')) {
            --nested_level;
        } else if (!has_quote && nested_level == 0 && c == ',') {
            // if meet collection_delimiter and not in quote, we can make it as an item.
            slices.back().remove_suffix(slice.size - idx);
            // add next total slice.(slice data will not change, so we can use slice directly)
            // skip delimiter
            std::cout << "back: " << slices.back().to_string() << std::endl;
            std::cout << "insert: " << Slice(slice.data + idx + 1, slice.size - idx - 1).to_string()
                      << std::endl;
            Slice next(slice.data + idx + 1, slice.size - idx - 1);
            next.trim_prefix();
            slices.emplace_back(next);
        }
    }
    std::cout << "slices size: " << nested_level << std::endl;
    for (auto s : slices) {
        std::cout << s.to_string() << std::endl;
    }
}
} // namespace doris::vectorized
