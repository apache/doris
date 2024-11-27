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
#include <gen_cpp/data.pb.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <iostream>

#include "olap/schema.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_map.h"
#include "vec/columns/columns_number.h"
#include "vec/core/field.h"
#include "vec/core/sort_block.h"
#include "vec/core/sort_description.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_map.h"

// this test is gonna to be a data type serialize and deserialize functions
// such as
// 1. standard hive text ser-deserialize
//  deserialize_one_cell_from_hive_text (IColumn &column, Slice &slice, const FormatOptions &options, int hive_text_complex_type_delimiter_level=1) const
//  deserialize_column_from_hive_text_vector (IColumn &column, std::vector< Slice > &slices, int *num_deserialized, const FormatOptions &options, int hive_text_complex_type_delimiter_level=1) const
//  serialize_one_cell_to_hive_text (const IColumn &column, int row_num, BufferWritable &bw, FormatOptions &options, int hive_text_complex_type_delimiter_level=1) const
// 2. json format ser-deserialize which used in table not in doris database
//  serialize_one_cell_to_json (const IColumn &column, int row_num, BufferWritable &bw, FormatOptions &options) const =0
//  serialize_column_to_json (const IColumn &column, int start_idx, int end_idx, BufferWritable &bw, FormatOptions &options) const =0
//  deserialize_one_cell_from_json (IColumn &column, Slice &slice, const FormatOptions &options) const =0
//  deserialize_column_from_json_vector (IColumn &column, std::vector< Slice > &slices, int *num_deserialized, const FormatOptions &options) const =0
//  deserialize_column_from_fixed_json (IColumn &column, Slice &slice, int rows, int *num_deserialized, const FormatOptions &options) const
//  insert_column_last_value_multiple_times (IColumn &column, int times) const
// 3. fe|be protobuffer ser-deserialize
//  write_column_to_pb (const IColumn &column, PValues &result, int start, int end) const =0
//  read_column_from_pb (IColumn &column, const PValues &arg) const =0
// 4. jsonb ser-deserialize which used in row-store situation
//  write_one_cell_to_jsonb (const IColumn &column, JsonbWriter &result, Arena *mem_pool, int32_t col_id, int row_num) const =0
//  read_one_cell_from_jsonb (IColumn &column, const JsonbValue *arg) const =0
// 5. mysql text ser-deserialize
//  write_column_to_mysql (const IColumn &column, MysqlRowBuffer< false > &row_buffer, int row_idx, bool col_const, const FormatOptions &options) const =0
//  write_column_to_mysql (const IColumn &column, MysqlRowBuffer< true > &row_buffer, int row_idx, bool col_const, const FormatOptions &options) const =0
// 6. arrow ser-deserialize which used in spark-flink connector
//  write_column_to_arrow (const IColumn &column, const NullMap *null_map, arrow::ArrayBuilder *array_builder, int start, int end, const cctz::time_zone &ctz) const =0
//  read_column_from_arrow (IColumn &column, const arrow::Array *arrow_array, int start, int end, const cctz::time_zone &ctz) const =0
// 7. rapidjson ser-deserialize
//  write_one_cell_to_json (const IColumn &column, rapidjson::Value &result, rapidjson::Document::AllocatorType &allocator, Arena &mem_pool, int row_num) const
//  read_one_cell_from_json (IColumn &column, const rapidjson::Value &result) const
//  convert_field_to_rapidjson (const vectorized::Field &field, rapidjson::Value &target, rapidjson::Document::AllocatorType &allocator)
//  convert_array_to_rapidjson (const vectorized::Array &array, rapidjson::Value &target, rapidjson::Document::AllocatorType &allocator)

namespace doris::vectorized {

class CommonDataTypeSerdeTest : public ::testing::Test {
public:
    ////==================================================================================================================
    // this is common function to check data in column against expected results according different function in assert function
    // which can be used in all column test
    // such as run regress tests
    //  step1. we can set gen_check_data_in_assert to true, then we will generate a file for check data, otherwise we will read the file to check data
    //  step2. we should write assert callback function to check data
    static void check_data(
            MutableColumns& columns, DataTypeSerDeSPtrs serders, char col_spliter,
            std::set<int> idxes, const std::string& column_data_file,
            std::function<void(MutableColumns& load_cols, DataTypeSerDeSPtrs serders)>
                    assert_callback,
            bool is_hive_format = false) {
        ASSERT_EQ(serders.size(), columns.size());
        // Step 1: Insert data from `column_data_file` into the column and check result with `check_data_file`
        // Load column data and expected data from CSV files
        std::vector<std::vector<std::string>> res;
        struct stat buff;
        if (stat(column_data_file.c_str(), &buff) == 0) {
            if (S_ISREG(buff.st_mode)) {
                // file
                if (is_hive_format) {
                    load_data_and_assert_from_csv<true>(serders, columns, column_data_file,
                                                        col_spliter, idxes);
                } else {
                    load_data_and_assert_from_csv<false>(serders, columns, column_data_file,
                                                         col_spliter, idxes);
                }
            } else if (S_ISDIR(buff.st_mode)) {
                // dir
                std::filesystem::path fs_path(column_data_file);
                for (const auto& entry : std::filesystem::directory_iterator(fs_path)) {
                    std::string file_path = entry.path().string();
                    std::cout << "load data from file: " << file_path << std::endl;
                    if (is_hive_format) {
                        load_data_and_assert_from_csv<true>(serders, columns, file_path,
                                                            col_spliter, idxes);
                    } else {
                        load_data_and_assert_from_csv<false>(serders, columns, file_path,
                                                             col_spliter, idxes);
                    }
                }
            }
        }

        // Step 2: Validate the data in `column` matches `expected_data`
        assert_callback(columns, serders);
    }

    // Helper function to load data from CSV, with index which splited by spliter and load to columns
    template <bool is_hive_format>
    static void load_data_and_assert_from_csv(const DataTypeSerDeSPtrs serders,
                                              MutableColumns& columns, const std::string& file_path,
                                              const char spliter = ';',
                                              const std::set<int> idxes = {0}) {
        ASSERT_EQ(serders.size(), columns.size())
                << "serder size: " << serders.size() << " column size: " << columns.size();
        ASSERT_EQ(serders.size(), idxes.size())
                << "serder size: " << serders.size() << " idxes size: " << idxes.size();
        std::ifstream file(file_path);
        if (!file) {
            throw doris::Exception(ErrorCode::INVALID_ARGUMENT, "can not open the file: {} ",
                                   file_path);
        }

        std::string line;
        DataTypeSerDe::FormatOptions options;
        while (std::getline(file, line)) {
            std::stringstream lineStream(line);
            //            std::cout << "whole : " << lineStream.str() << std::endl;
            std::string value;
            int l_idx = 0;
            int c_idx = 0;
            while (std::getline(lineStream, value, spliter)) {
                if (idxes.contains(l_idx)) {
                    // load csv data
                    Slice string_slice(value.data(), value.size());
                    std::cout << string_slice << std::endl;
                    Status st;
                    // deserialize data
                    if constexpr (is_hive_format) {
                        st = serders[c_idx]->deserialize_one_cell_from_hive_text(
                                *columns[c_idx], string_slice, options);
                    } else {
                        st = serders[c_idx]->deserialize_one_cell_from_json(*columns[c_idx],
                                                                            string_slice, options);
                    }
                    if (!st.ok()) {
                        std::cout << "error in deserialize but continue: " << st.to_string()
                                  << std::endl;
                    }
                    // serialize data
                    auto ser_col = ColumnString::create();
                    ser_col->reserve(1);
                    VectorBufferWriter bw(*ser_col.get());
                    size_t row_num = columns[c_idx]->size() - 1;
                    if constexpr (is_hive_format) {
                        st = serders[c_idx]->serialize_one_cell_to_hive_text(*columns[c_idx],
                                                                             row_num, bw, options);
                        EXPECT_TRUE(st.ok()) << st.to_string();
                    } else {
                        st = serders[c_idx]->serialize_one_cell_to_json(*columns[c_idx], row_num,
                                                                        bw, options);
                        EXPECT_TRUE(st.ok()) << st.to_string();
                    }
                    bw.commit();
                    // assert data : origin data and serialized data should be equal
                    EXPECT_EQ(ser_col->get_data_at(0).to_string(), string_slice.to_string());
                    ++c_idx;
                }
                ++l_idx;
            }
        }
    }

    // standard hive text ser-deserialize assert function
    static void assert_pb_format(MutableColumns& load_cols, DataTypeSerDeSPtrs serders) {
        for (size_t i = 0; i < load_cols.size(); ++i) {
            auto& col = load_cols[i];
            std::cout << " now we are testing column : " << col->get_name() << std::endl;
            // serialize to pb
            PValues pv = PValues();
            Status st = serders[i]->write_column_to_pb(*col, pv, 0, col->size());
            if (!st.ok()) {
                std::cerr << "write_column_to_pb error: " << st.msg() << std::endl;
                continue;
            }
            // deserialize from pb
            auto except_column = col->clone_empty();
            st = serders[i]->read_column_from_pb(*except_column, pv);
            EXPECT_TRUE(st.ok()) << st.to_string();
            // check pb value from expected column
            PValues as_pv = PValues();
            st = serders[i]->write_column_to_pb(*except_column, as_pv, 0, except_column->size());
            EXPECT_TRUE(st.ok()) << st.to_string();
            EXPECT_EQ(pv.bytes_value_size(), as_pv.bytes_value_size());
            // check column value
            for (size_t j = 0; j < col->size(); ++j) {
                auto cell = col->operator[](j);
                auto except_cell = except_column->operator[](j);
                EXPECT_EQ(cell, except_cell) << "column: " << col->get_name() << " row: " << j;
            }
        }
    }

    static void assert_jsonb_format(MutableColumns& load_cols, DataTypeSerDeSPtrs serders) {
        for (size_t i = 0; i < load_cols.size(); ++i) {
            auto& col = load_cols[i];
            std::cout << " now we are testing column : " << col->get_name() << std::endl;
            // serialize to jsonb
            JsonbWriterT<JsonbOutStream> jw;
            Arena pool;
            for (size_t j = 0; j < col->size(); ++j) {
                serders[i]->write_one_cell_to_jsonb(*col, jw, &pool, i, j);
            }
            // deserialize from jsonb
            auto expect_column = col->clone_empty();
            auto jsonb_column = ColumnString::create();
            for (size_t j = 0; j < col->size(); ++j) {
                jsonb_column->insert_data(jw.getOutput()->getBuffer(), jw.getOutput()->getSize());
                StringRef jsonb_data = jsonb_column->get_data_at(0);
                auto pdoc = JsonbDocument::createDocument(jsonb_data.data, jsonb_data.size);
                JsonbDocument& doc = *pdoc;
                for (auto it = doc->begin(); it != doc->end(); ++it) {
                    serders[i]->read_one_cell_from_jsonb(*expect_column, it->value());
                }
            }
            // check column value
            for (size_t j = 0; j < col->size(); ++j) {
                auto cell = col->operator[](j);
                auto expect_cell = expect_column->operator[](j);
                EXPECT_EQ(cell, expect_cell) << "column: " << col->get_name() << " row: " << j;
            }
        }
    }
};

} // namespace doris::vectorized