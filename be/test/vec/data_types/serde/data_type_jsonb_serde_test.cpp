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

#include "vec/data_types/serde/data_type_jsonb_serde.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>
#include <lz4/lz4.h>
#include <streamvbyte.h>

#include <cstddef>
#include <iostream>
#include <limits>
#include <type_traits>

#include "olap/olap_common.h"
#include "runtime/types.h"
#include "testutil/test_util.h"
#include "vec/columns/column.h"
#include "vec/common/assert_cast.h"
#include "vec/core/types.h"
#include "vec/data_types/common_data_type_serder_test.h"
#include "vec/data_types/common_data_type_test.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/data_types/data_type_jsonb.h"
#include "vec/data_types/data_type_nullable.h"

namespace doris::vectorized {
static std::string test_data_dir;

static auto serde_jsonb = std::make_shared<DataTypeJsonbSerDe>();

static ColumnString::MutablePtr column_jsonb;
class DataTypeJsonbSerDeTest : public ::testing::Test {
protected:
    static void SetUpTestSuite() {
        auto root_dir = std::string(getenv("ROOT"));
        test_data_dir = root_dir + "/be/test/data/vec/columns";

        column_jsonb = ColumnString::create();
        load_columns_data();
    }

    static void load_columns_data() {
        std::cout << "loading test dataset" << std::endl;
        {
            MutableColumns columns;
            columns.push_back(column_jsonb->get_ptr());
            DataTypeSerDeSPtrs serde = {serde_jsonb};
            std::string data_file = test_data_dir + "/JSONB.csv";
            load_columns_data_from_file(columns, serde, ';', {0}, data_file);
            EXPECT_TRUE(!column_jsonb->empty());
            column_jsonb->insert_default();
        }
        std::cout << "column jsonb size: " << column_jsonb->size() << std::endl;
    }
};

TEST_F(DataTypeJsonbSerDeTest, serdes) {
    auto test_func = [](const auto& serde, const auto& source_column) {
        using SerdeType = decltype(serde);
        using ColumnType = typename std::remove_reference<SerdeType>::type::ColumnStrType;

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
                if (j == row_count - 1) {
                    // last row will make simdjson parse error, because we use column_string insert_default with empty string, which is not invalid json string:
                    // [INTERNAL_ERROR]simdjson parse exception: The JSON document has an improper structure: missing or superfluous commas, braces, missing keys, etc.
                    EXPECT_TRUE(!st.ok());
                } else {
                    EXPECT_TRUE(st.ok())
                            << "Failed to deserialize column at row " << j << ": " << st;
                    EXPECT_EQ(deser_col_with_type->get_data_at(j), source_column->get_data_at(j));
                }
            }
        }

        // test serialize_column_to_json
        {
            auto ser_col = ColumnString::create();
            ser_col->reserve(row_count);

            VectorBufferWriter buffer_writer(*ser_col.get());
            auto st = serde.serialize_column_to_json(*source_column, 0, source_column->size() - 1,
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
            EXPECT_EQ(num_deserialized, row_count - 1);
            for (size_t j = 0; j != row_count - 1; ++j) {
                EXPECT_EQ(deser_col_with_type->get_data_at(j), source_column->get_data_at(j));
            }
        }

        {
            // test write_column_to_pb/read_column_from_pb
            PValues pv = PValues();
            Status st = serde.write_column_to_pb(*source_column, pv, 0, row_count - 1);
            EXPECT_TRUE(st.ok()) << "Failed to write column to pb: " << st;

            MutableColumnPtr deser_column = source_column->clone_empty();
            const auto* deser_col_with_type = assert_cast<const ColumnType*>(deser_column.get());
            st = serde.read_column_from_pb(*deser_column, pv);
            EXPECT_TRUE(st.ok()) << "Failed to read column from pb: " << st;
            for (size_t j = 0; j != row_count - 1; ++j) {
                EXPECT_EQ(deser_col_with_type->get_data_at(j), source_column->get_data_at(j));
            }
        }
        {
            // test write_one_cell_to_jsonb/read_one_cell_from_jsonb
            JsonbWriterT<JsonbOutStream> jsonb_writer;
            jsonb_writer.writeStartObject();
            Arena pool;

            for (size_t j = 0; j != row_count; ++j) {
                serde.write_one_cell_to_jsonb(*source_column, jsonb_writer, pool, 0, j);
            }
            jsonb_writer.writeEndObject();

            auto ser_col = ColumnString::create();
            ser_col->reserve(row_count);
            MutableColumnPtr deser_column = source_column->clone_empty();
            const auto* deser_col_with_type = assert_cast<const ColumnType*>(deser_column.get());
            JsonbDocument* pdoc = nullptr;
            auto st = JsonbDocument::checkAndCreateDocument(jsonb_writer.getOutput()->getBuffer(),
                                                            jsonb_writer.getOutput()->getSize(),
                                                            &pdoc);
            EXPECT_TRUE(st.ok()) << "Failed to check and create jsonb document: " << st;
            JsonbDocument& doc = *pdoc;
            for (auto it = doc->begin(); it != doc->end(); ++it) {
                serde.read_one_cell_from_jsonb(*deser_column, it->value());
            }
            for (size_t j = 0; j != row_count; ++j) {
                EXPECT_EQ(deser_col_with_type->get_data_at(j), source_column->get_data_at(j));
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
        {
            // test write_column_to_mysql with binary format
            MysqlRowBuffer<true> mysql_rb;
            for (int row_idx = 0; row_idx < row_count; ++row_idx) {
                auto st = serde.write_column_to_mysql(*source_column, mysql_rb, row_idx, false,
                                                      option);
                EXPECT_TRUE(st.ok())
                        << "Failed to write column to mysql with binary format: " << st;
            }
        }
        {
            // test write_column_to_arrow
            auto arrow_builder = std::make_shared<arrow::StringBuilder>();
            cctz::time_zone ctz;
            auto st = serde.write_column_to_arrow(*source_column, nullptr, arrow_builder.get(), 0,
                                                  row_count - 1, ctz);
            EXPECT_TRUE(st.ok()) << "Failed to write column to arrow: " << st;
            auto result = arrow_builder->Finish();
            EXPECT_TRUE(result.ok());
        }
        {
            // test write_column_to_orc
            Arena arena;
            auto orc_batch =
                    std::make_unique<orc::StringVectorBatch>(row_count, *orc::getDefaultPool());
            Status st = serde.write_column_to_orc("UTC", *source_column, nullptr, orc_batch.get(),
                                                  0, row_count - 1, arena);
            EXPECT_EQ(st, Status::OK()) << "Failed to write column to orc: " << st;
            EXPECT_EQ(orc_batch->numElements, row_count - 1);
        }
        {
            // test write_one_cell_to_json/read_one_cell_from_json
            rapidjson::Document doc;
            doc.SetObject();
            Arena mem_pool;
            for (int row_idx = 0; row_idx < row_count - 1; ++row_idx) {
                auto st = serde.write_one_cell_to_json(*source_column, doc, doc.GetAllocator(),
                                                       mem_pool, row_idx);
                EXPECT_TRUE(st.ok()) << "Failed to write one cell to json: " << st;
            }
            MutableColumnPtr deser_column = source_column->clone_empty();
            for (int row_idx = 0; row_idx < row_count - 1; ++row_idx) {
                auto st = serde.read_one_cell_from_json(*deser_column, doc);
                EXPECT_TRUE(st.ok()) << "Failed to read one cell from json: " << st;
            }
        }
    };
    test_func(*serde_jsonb, column_jsonb);
}

} // namespace doris::vectorized