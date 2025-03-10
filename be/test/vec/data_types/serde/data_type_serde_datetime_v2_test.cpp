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

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>
#include <streamvbyte.h>

#include <cstddef>
#include <iostream>
#include <limits>
#include <type_traits>

#include "testutil/test_util.h"
#include "util/slice.h"
#include "util/string_util.h"
#include "vec/columns/column.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/core/types.h"
#include "vec/data_types/common_data_type_serder_test.h"
#include "vec/data_types/common_data_type_test.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/serde/data_type_datetimev2_serde.h"
#include "vec/data_types/serde/data_type_datev2_serde.h"
#include "vec/data_types/serde/data_type_time_serde.h"
namespace doris::vectorized {
static std::string test_data_dir;

static auto serde_date_v2 = std::make_shared<DataTypeDateV2SerDe>();
static auto serde_datetime_v2_0 = std::make_shared<DataTypeDateTimeV2SerDe>(0);
static auto serde_datetime_v2_5 = std::make_shared<DataTypeDateTimeV2SerDe>(5);
static auto serde_datetime_v2_6 = std::make_shared<DataTypeDateTimeV2SerDe>(6);

static auto serde_time_v2_6 = std::make_shared<DataTypeTimeV2SerDe>(6);
static auto serde_time_v2_5 = std::make_shared<DataTypeTimeV2SerDe>(5);
static auto serde_time_v2_0 = std::make_shared<DataTypeTimeV2SerDe>(0);

static ColumnDateTimeV2::MutablePtr column_datetime_v2_0;
static ColumnDateTimeV2::MutablePtr column_datetime_v2_5;
static ColumnDateTimeV2::MutablePtr column_datetime_v2_6;
static ColumnDateV2::MutablePtr column_date_v2;

static ColumnFloat64::MutablePtr column_time_v2_6;
static ColumnFloat64::MutablePtr column_time_v2_5;
static ColumnFloat64::MutablePtr column_time_v2_0;

class DataTypeDateTimeV2SerDeTest : public ::testing::Test {
public:
    static void SetUpTestSuite() {
        auto root_dir = std::string(getenv("ROOT"));
        test_data_dir = root_dir + "/be/test/data/vec/columns";

        column_datetime_v2_0 = ColumnDateTimeV2::create();
        column_datetime_v2_5 = ColumnDateTimeV2::create();
        column_datetime_v2_6 = ColumnDateTimeV2::create();
        column_date_v2 = ColumnDateV2::create();
        column_time_v2_6 = ColumnFloat64::create();
        column_time_v2_5 = ColumnFloat64::create();
        column_time_v2_0 = ColumnFloat64::create();

        load_columns_data();
    }
    static void load_columns_data() {
        std::cout << "loading test dataset" << std::endl;
        auto test_func = [&](const MutableColumnPtr& column, const auto& serde,
                             const std::string& data_file_name) {
            MutableColumns columns;
            columns.push_back(column->get_ptr());
            DataTypeSerDeSPtrs serdes = {serde};
            load_columns_data_from_file(columns, serdes, ';', {0},
                                        test_data_dir + "/" + data_file_name);
            EXPECT_TRUE(!column->empty());
        };
        test_func(column_datetime_v2_0->get_ptr(), serde_datetime_v2_0, "DATETIMEV2(0).csv");
        test_func(column_datetime_v2_5->get_ptr(), serde_datetime_v2_5, "DATETIMEV2(5).csv");
        test_func(column_datetime_v2_6->get_ptr(), serde_datetime_v2_6, "DATETIMEV2(6).csv");
        test_func(column_date_v2->get_ptr(), serde_date_v2, "DATEV2.csv");

        test_func(column_time_v2_6->get_ptr(), serde_time_v2_6, "TIMEV2(6).csv");
        test_func(column_time_v2_5->get_ptr(), serde_time_v2_5, "TIMEV2(6).csv");
        test_func(column_time_v2_0->get_ptr(), serde_time_v2_0, "TIMEV2(6).csv");

        std::cout << "loading test dataset done" << std::endl;
    }
};
TEST_F(DataTypeDateTimeV2SerDeTest, serdes) {
    auto test_func = [](const auto& serde, const auto& source_column) {
        using SerdeType = decltype(serde);
        using ColumnType = typename std::remove_reference<SerdeType>::type::ColumnType;

        auto row_count = source_column->size();
        auto option = DataTypeSerDe::FormatOptions();
        char field_delim = ';';
        option.field_delim = std::string(1, field_delim);

        {
            auto ser_col = ColumnString::create();
            ser_col->reserve(row_count);
            MutableColumnPtr deser_column = source_column->clone_empty();
            const auto* deser_col_with_type = assert_cast<const ColumnType*>(deser_column.get());

            VectorBufferWriter buffer_writer(*ser_col.get());
            for (size_t j = 0; j != row_count; ++j) {
                auto st =
                        serde.serialize_one_cell_to_json(*source_column, j, buffer_writer, option);
                EXPECT_TRUE(st.ok()) << "Failed to serialize column at row " << j << ": " << st;

                buffer_writer.commit();
                std::string actual_str_value = ser_col->get_data_at(j).to_string();
                Slice slice {actual_str_value.data(), actual_str_value.size()};
                st = serde.deserialize_one_cell_from_json(*deser_column, slice, option);
                EXPECT_TRUE(st.ok()) << "Failed to deserialize column at row " << j << ": " << st;
                EXPECT_EQ(deser_col_with_type->get_element(j), source_column->get_element(j));
            }
        }

        // test serialize_column_to_json
        {
            auto ser_col = ColumnString::create();
            ser_col->reserve(row_count);

            VectorBufferWriter buffer_writer(*ser_col.get());
            auto st = serde.serialize_column_to_json(*source_column, 0, source_column->size(),
                                                     buffer_writer, option);
            EXPECT_TRUE(st.ok()) << "Failed to serialize column to json: " << st;
            buffer_writer.commit();

            std::string json_data((char*)ser_col->get_chars().data(), ser_col->get_chars().size());
            std::vector<std::string> strs = doris::split(json_data, std::string(1, field_delim));
            std::vector<Slice> slices;
            for (const auto& s : strs) {
                Slice tmp_slice(s.data(), s.size());
                tmp_slice.trim_prefix();
                slices.emplace_back(tmp_slice);
            }

            MutableColumnPtr deser_column = source_column->clone_empty();
            const auto* deser_col_with_type = assert_cast<const ColumnType*>(deser_column.get());
            uint64_t num_deserialized = 0;
            st = serde.deserialize_column_from_json_vector(*deser_column, slices, &num_deserialized,
                                                           option);
            EXPECT_TRUE(st.ok()) << "Failed to deserialize column from json: " << st;
            EXPECT_EQ(num_deserialized, row_count);
            for (size_t j = 0; j != row_count; ++j) {
                EXPECT_EQ(deser_col_with_type->get_element(j), source_column->get_element(j));
            }
        }

        {
            // test write_column_to_pb/read_column_from_pb
            PValues pv = PValues();
            Status st = serde.write_column_to_pb(*source_column, pv, 0, row_count);
            EXPECT_TRUE(st.ok()) << "Failed to write column to pb: " << st;

            MutableColumnPtr deser_column = source_column->clone_empty();
            const auto* deser_col_with_type = assert_cast<const ColumnType*>(deser_column.get());
            st = serde.read_column_from_pb(*deser_column, pv);
            EXPECT_TRUE(st.ok()) << "Failed to read column from pb: " << st;
            for (size_t j = 0; j != row_count; ++j) {
                EXPECT_EQ(deser_col_with_type->get_element(j), source_column->get_element(j));
            }
        }
        {
            // test write_one_cell_to_jsonb/read_one_cell_from_jsonb
            JsonbWriterT<JsonbOutStream> jsonb_writer;
            jsonb_writer.writeStartObject();
            Arena pool;

            for (size_t j = 0; j != row_count; ++j) {
                serde.write_one_cell_to_jsonb(*source_column, jsonb_writer, &pool, 0, j);
            }
            jsonb_writer.writeEndObject();

            auto ser_col = ColumnString::create();
            ser_col->reserve(row_count);
            MutableColumnPtr deser_column = source_column->clone_empty();
            const auto* deser_col_with_type = assert_cast<const ColumnType*>(deser_column.get());
            auto* pdoc = JsonbDocument::checkAndCreateDocument(
                    jsonb_writer.getOutput()->getBuffer(), jsonb_writer.getOutput()->getSize());
            JsonbDocument& doc = *pdoc;
            for (auto it = doc->begin(); it != doc->end(); ++it) {
                serde.read_one_cell_from_jsonb(*deser_column, it->value());
            }
            for (size_t j = 0; j != row_count; ++j) {
                EXPECT_EQ(deser_col_with_type->get_element(j), source_column->get_element(j));
            }
        }
        {
            // test write_column_to_mysql
            MysqlRowBuffer<false> mysql_rb;
            for (int row_idx = 0; row_idx < row_count; ++row_idx) {
                auto st = serde.write_column_to_mysql(*source_column, mysql_rb, row_idx, false,
                                                      option);
                EXPECT_TRUE(st.ok()) << "Failed to write column to mysql: " << st;
            }
        }
    };
    test_func(*serde_datetime_v2_0, column_datetime_v2_0);
    test_func(*serde_datetime_v2_5, column_datetime_v2_5);
    test_func(*serde_datetime_v2_6, column_datetime_v2_6);
    test_func(*serde_date_v2, column_date_v2);
    test_func(*serde_time_v2_6, column_time_v2_6);
    test_func(*serde_time_v2_5, column_time_v2_5);
    test_func(*serde_time_v2_0, column_time_v2_0);
}

} // namespace doris::vectorized