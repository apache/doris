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
        std::vector<FieldType> arithmetic_scala_field_types = {
                FieldType::OLAP_FIELD_TYPE_BOOL,       FieldType::OLAP_FIELD_TYPE_TINYINT,
                FieldType::OLAP_FIELD_TYPE_SMALLINT,   FieldType::OLAP_FIELD_TYPE_INT,
                FieldType::OLAP_FIELD_TYPE_FLOAT,      FieldType::OLAP_FIELD_TYPE_DOUBLE,
                FieldType::OLAP_FIELD_TYPE_BIGINT,     FieldType::OLAP_FIELD_TYPE_LARGEINT,
                FieldType::OLAP_FIELD_TYPE_CHAR,       FieldType::OLAP_FIELD_TYPE_VARCHAR,
                FieldType::OLAP_FIELD_TYPE_STRING,     FieldType::OLAP_FIELD_TYPE_DECIMAL,
                FieldType::OLAP_FIELD_TYPE_DECIMAL32,  FieldType::OLAP_FIELD_TYPE_DECIMAL64,
                FieldType::OLAP_FIELD_TYPE_DECIMAL128I};

        for (auto type : arithmetic_scala_field_types) {
            DataTypePtr data_type_ptr;
            if (type == FieldType::OLAP_FIELD_TYPE_DECIMAL32) {
                data_type_ptr = DataTypeFactory::instance().create_data_type(type, 9, 0);
            } else if (type == FieldType::OLAP_FIELD_TYPE_DECIMAL64) {
                data_type_ptr = DataTypeFactory::instance().create_data_type(type, 18, 0);
            } else if (type == FieldType::OLAP_FIELD_TYPE_DECIMAL128I) {
                data_type_ptr = DataTypeFactory::instance().create_data_type(type, 38, 0);
            } else {
                data_type_ptr = DataTypeFactory::instance().create_data_type(type, 0, 0);
            }
            std::cout << "this type is " << data_type_ptr->get_name() << ": "
                      << fmt::format("{}", type) << std::endl;

            std::unique_ptr<WrapperField> min_wf(WrapperField::create_by_type(type));
            std::unique_ptr<WrapperField> max_wf(WrapperField::create_by_type(type));

            min_wf->set_to_min();
            if (is_string_type(type)) {
                string rand_s = generate(128);
                Status st = max_wf->from_string(rand_s, 0, 0);
                EXPECT_EQ(st.ok(), true);
            } else {
                max_wf->set_to_max();
            }

            string min_s = min_wf->to_string();
            string max_s = max_wf->to_string();

            ReadBuffer min_rb(min_s.data(), min_s.size());
            ReadBuffer max_rb(max_s.data(), max_s.size());

            auto col = data_type_ptr->create_column();
            Status st = data_type_ptr->from_string(min_rb, col);
            EXPECT_EQ(st.ok(), true);
            st = data_type_ptr->from_string(max_rb, col);
            EXPECT_EQ(st.ok(), true);

            string min_s_d = data_type_ptr->to_string(*col, 0);
            string max_s_d = data_type_ptr->to_string(*col, 1);
            std::cout << " min: " << min_s << ": " << min_s_d << std::endl;
            std::cout << "max: " << max_s << ": " << max_s_d << std::endl;
            EXPECT_EQ(min_s, min_s_d);
            EXPECT_EQ(max_s, max_s_d);
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
            DataTypePtr data_type_ptr = DataTypeFactory::instance().create_data_type(type, 0, 0);
            std::cout << "this type is " << data_type_ptr->get_name() << ": "
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
            if (FieldType::OLAP_FIELD_TYPE_DATETIMEV2 == type) {
                // field to_string : %Y-%m-%d %H:%i:%s.%f vs data type to_string %Y-%m-%d %H:%i:%s
                min_s = min_s.substr(0, min_s.find_last_of('.'));
                max_s = max_s.substr(0, max_s.find_last_of('.'));
                rand_date = rand_date.substr(0, rand_date.find_last_of('.'));
            }
            if (FieldType::OLAP_FIELD_TYPE_DATE == type ||
                FieldType::OLAP_FIELD_TYPE_DATETIME == type) {
                // field to_string : 0-01-01 vs data type to_string: 0000-01-01
                EXPECT_NE(min_s, min_s_d);
            } else {
                EXPECT_EQ(min_s, min_s_d);
            }
            EXPECT_EQ(max_s, max_s_d);
            EXPECT_EQ(rand_date, rand_s_d);
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
        rand_wf->from_string(test_str, 0, 0);
        Field string_field(test_str);
        ColumnPtr col = nullable_ptr->create_column_const(0, string_field);
        EXPECT_EQ(rand_wf->to_string(), nullable_ptr->to_string(*col, 0));
    }
}

} // namespace doris::vectorized
