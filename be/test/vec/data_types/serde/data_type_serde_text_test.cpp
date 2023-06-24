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
#include "vec/data_types/serde/data_type_serde.h"
#include "vec/data_types/serde_utils.h"
#include "vec/io/reader_buffer.h"

namespace doris::vectorized {
// This test aim to make sense for text serde of data types.
//  we use default formatOption and special formatOption to equal serde for wrapperField.
TEST(TextSerde, ScalaDataTypeSerdeTextTest) {
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

            // serde for data types with default FormatOption
            DataTypeSerDe::FormatOptions default_format_option;
            DataTypeSerDeSPtr serde = data_type_ptr->get_serde();
            Status st = serde->deserialize_one_cell_from_text(*col, min_rb, default_format_option);
            EXPECT_EQ(st.ok(), true);
            st = serde->deserialize_one_cell_from_text(*col, max_rb, default_format_option);
            EXPECT_EQ(st.ok(), true);

            auto ser_col = ColumnString::create();
            ser_col->reserve(2);
            VectorBufferWriter buffer_writer(*ser_col.get());
            serde->serialize_one_cell_to_text(*col, 0, buffer_writer, default_format_option);
            serde->serialize_one_cell_to_text(*col, 1, buffer_writer, default_format_option);
            StringRef min_s_d = ser_col->get_data_at(0);
            StringRef max_s_d = ser_col->get_data_at(1);
            std::cout << " min: " << min_s << ": " << min_s_d.to_string() << std::endl;
            std::cout << "max: " << max_s << ": " << max_s_d.to_string() << std::endl;
            EXPECT_EQ(min_s, min_s_d.to_string());
            EXPECT_EQ(max_s, max_s_d.to_string());
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
            DataTypeSerDeSPtr serde = data_type_ptr->get_serde();
            // make use c++ lib equals to wrapper field from_string behavior
            DataTypeSerDe::FormatOptions formatOptions;
            formatOptions.use_lib_format = true;

            Status st = serde->deserialize_one_cell_from_text(*col, min_rb, formatOptions);
            EXPECT_EQ(st.ok(), true);
            st = serde->deserialize_one_cell_from_text(*col, max_rb, formatOptions);
            EXPECT_EQ(st.ok(), true);
            st = serde->deserialize_one_cell_from_text(*col, rand_rb, formatOptions);
            EXPECT_EQ(st.ok(), true);

            auto ser_col = ColumnString::create();
            ser_col->reserve(3);
            VectorBufferWriter buffer_writer(*ser_col.get());
            serde->serialize_one_cell_to_text(*col, 0, buffer_writer, formatOptions);
            serde->serialize_one_cell_to_text(*col, 1, buffer_writer, formatOptions);
            serde->serialize_one_cell_to_text(*col, 2, buffer_writer, formatOptions);
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
}
} // namespace doris::vectorized