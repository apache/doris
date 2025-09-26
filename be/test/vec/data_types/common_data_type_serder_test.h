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
#include <arrow/record_batch.h>
#include <gen_cpp/data.pb.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>

#include "arrow/array/array_base.h"
#include "arrow/type.h"
#include "runtime/descriptors.h"
#include "util/arrow/block_convertor.h"
#include "util/arrow/row_batch.h"
#include "util/jsonb_writer.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/runtime/ipv6_value.h"
#include "vec/utils/arrow_column_to_doris_column.h"

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
//  deserialize_column_from_json_vector (IColumn &column, std::vector< Slice > &slices, uint64_t *num_deserialized, const FormatOptions &options) const =0
//  deserialize_column_from_fixed_json (IColumn &column, Slice &slice, uint64_t rows, uint64_t *num_deserialized, const FormatOptions &options) const
//  insert_column_last_value_multiple_times (IColumn &column, uint64_t times) const
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
            bool is_hive_format = false, DataTypes dataTypes = {}) {
        ASSERT_EQ(serders.size(), columns.size());
        // Step 1: Insert data from `column_data_file` into the column and check result with `check_data_file`
        // Load column data and expected data from CSV files
        std::vector<std::vector<std::string>> res;
        struct stat buff;
        if (stat(column_data_file.c_str(), &buff) == 0) {
            if (S_ISREG(buff.st_mode)) {
                // file
                if (is_hive_format) {
                    load_data_and_assert_from_csv<true, true>(serders, columns, column_data_file,
                                                              col_spliter, idxes);
                } else {
                    load_data_and_assert_from_csv<false, true>(serders, columns, column_data_file,
                                                               col_spliter, idxes);
                }
            } else if (S_ISDIR(buff.st_mode)) {
                // dir
                std::filesystem::path fs_path(column_data_file);
                for (const auto& entry : std::filesystem::directory_iterator(fs_path)) {
                    std::string file_path = entry.path().string();
                    std::cout << "load data from file: " << file_path << std::endl;
                    if (is_hive_format) {
                        load_data_and_assert_from_csv<true, true>(serders, columns, file_path,
                                                                  col_spliter, idxes);
                    } else {
                        load_data_and_assert_from_csv<false, true>(serders, columns, file_path,
                                                                   col_spliter, idxes);
                    }
                }
            }
        }

        // Step 2: Validate the data in `column` matches `expected_data`
        assert_callback(columns, serders);
    }

    // Helper function to load data from CSV, with index which splited by spliter and load to columns
    template <bool is_hive_format, bool generate_res_file>
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
        std::vector<std::vector<std::string>> res;
        MutableColumns assert_str_cols(columns.size());
        for (size_t i = 0; i < columns.size(); ++i) {
            assert_str_cols[i] = ColumnString::create();
        }

        while (std::getline(file, line)) {
            std::stringstream lineStream(line);
            std::string value;
            int l_idx = 0;
            int c_idx = 0;
            std::vector<std::string> row;
            while (std::getline(lineStream, value, spliter)) {
                if (!value.starts_with("//") && idxes.contains(l_idx)) {
                    // load csv data
                    Slice string_slice(value.data(), value.size());
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
                        // deserialize if happen error now we do not insert any value for input column
                        // so we push a default value to column for row alignment
                        columns[c_idx]->insert_default();
                        std::cout << "error in deserialize but continue: " << st.to_string()
                                  << std::endl;
                    }
                    // serialize data
                    size_t row_num = columns[c_idx]->size() - 1;
                    assert_str_cols[c_idx]->reserve(columns[c_idx]->size());
                    VectorBufferWriter bw(assert_cast<ColumnString&>(*assert_str_cols[c_idx]));
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
                    // assert data : origin data and serialized data should be equal or generated
                    // file to check data
                    size_t assert_size = assert_str_cols[c_idx]->size();
                    if constexpr (!generate_res_file) {
                        EXPECT_EQ(assert_str_cols[c_idx]->get_data_at(assert_size - 1).to_string(),
                                  string_slice.to_string())
                                << "column: " << columns[c_idx]->get_name() << " row: " << row_num
                                << " is_hive_format: " << is_hive_format;
                    }
                    ++c_idx;
                }
                res.push_back(row);
                ++l_idx;
            }
        }

        if (generate_res_file) {
            // generate res
            auto pos = file_path.find_last_of(".");
            std::string hive_format = is_hive_format ? "_hive" : "";
            std::string res_file = file_path.substr(0, pos) + hive_format + "_serde_res.csv";
            std::ofstream res_f(res_file);
            if (!res_f.is_open()) {
                throw std::ios_base::failure("Failed to open file." + res_file);
            }
            for (size_t r = 0; r < assert_str_cols[0]->size(); ++r) {
                for (size_t c = 0; c < assert_str_cols.size(); ++c) {
                    res_f << assert_str_cols[c]->get_data_at(r).to_string() << spliter;
                }
                res_f << std::endl;
            }
            res_f.close();
            std::cout << "generate res file: " << res_file << std::endl;
        }
    }

    // standard hive text ser-deserialize assert function
    // pb serde now is only used RPCFncall and fold_constant_executor which just write column data to pb value means
    // just call write_column_to_pb
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

    // actually this is block_to_jsonb and jsonb_to_block test
    static void assert_jsonb_format(MutableColumns& load_cols, DataTypeSerDeSPtrs serders) {
        Arena pool;
        auto jsonb_column = ColumnString::create(); // jsonb column
        // maybe these load_cols has different size, so we keep it same
        size_t max_row_size = load_cols[0]->size();
        for (size_t i = 1; i < load_cols.size(); ++i) {
            if (load_cols[i]->size() > max_row_size) {
                max_row_size = load_cols[i]->size();
            }
        }
        // keep same rows
        for (size_t i = 0; i < load_cols.size(); ++i) {
            if (load_cols[i]->size() < max_row_size) {
                load_cols[i]->insert_many_defaults(max_row_size - load_cols[i]->size());
            } else if (load_cols[i]->size() > max_row_size) {
                load_cols[i]->resize(max_row_size);
            }
        }
        jsonb_column->reserve(load_cols[0]->size());
        MutableColumns assert_cols;
        for (size_t i = 0; i < load_cols.size(); ++i) {
            assert_cols.push_back(load_cols[i]->assume_mutable());
        }
        for (size_t r = 0; r < load_cols[0]->size(); ++r) {
            JsonbWriterT<JsonbOutStream> jw;
            jw.writeStartObject();
            // serialize to jsonb
            for (size_t i = 0; i < load_cols.size(); ++i) {
                auto& col = load_cols[i];
                serders[i]->write_one_cell_to_jsonb(*col, jw, pool, i, r);
            }
            jw.writeEndObject();
            jsonb_column->insert_data(jw.getOutput()->getBuffer(), jw.getOutput()->getSize());
        }
        // deserialize jsonb column to assert column
        EXPECT_EQ(jsonb_column->size(), load_cols[0]->size());
        for (size_t r = 0; r < jsonb_column->size(); ++r) {
            StringRef jsonb_data = jsonb_column->get_data_at(r);
            JsonbDocument* pdoc = nullptr;
            auto st =
                    JsonbDocument::checkAndCreateDocument(jsonb_data.data, jsonb_data.size, &pdoc);
            ASSERT_TRUE(st.ok()) << "checkAndCreateDocument failed: " << st.to_string();
            JsonbDocument& doc = *pdoc;
            size_t cIdx = 0;
            for (auto it = doc->begin(); it != doc->end(); ++it) {
                serders[cIdx]->read_one_cell_from_jsonb(*assert_cols[cIdx], it->value());
                ++cIdx;
            }
        }
        // check column value
        for (size_t i = 0; i < load_cols.size(); ++i) {
            auto& col = load_cols[i];
            auto& assert_col = assert_cols[i];
            for (size_t j = 0; j < col->size(); ++j) {
                auto cell = col->operator[](j);
                auto assert_cell = assert_col->operator[](j);
                EXPECT_EQ(cell, assert_cell) << "column: " << col->get_name() << " row: " << j;
            }
        }
    }

    // assert mysql text format, now we just simple assert not to fatal or exception here
    static void assert_mysql_format(MutableColumns& load_cols, DataTypeSerDeSPtrs serders) {
        MysqlRowBuffer<false> row_buffer;
        for (size_t i = 0; i < load_cols.size(); ++i) {
            auto& col = load_cols[i];
            for (size_t j = 0; j < col->size(); ++j) {
                Status st;
                EXPECT_NO_FATAL_FAILURE(
                        st = serders[i]->write_column_to_mysql(*col, row_buffer, j, false, {}));
                EXPECT_TRUE(st.ok()) << st.to_string();
            }
        }
    }

    // assert arrow serialize
    static void assert_arrow_format(MutableColumns& load_cols, DataTypes types) {
        // make a block to write to arrow
        auto block = std::make_shared<Block>();
        build_block(block, load_cols, types);
        auto record_batch = serialize_arrow(block);
        auto assert_block = std::make_shared<Block>(block->clone_empty());
        deserialize_arrow(assert_block, record_batch);
        compare_two_blocks(block, assert_block);
    }

    static void build_block(const std::shared_ptr<Block>& block, MutableColumns& load_cols,
                            DataTypes types) {
        // maybe these load_cols has different size, so we keep it same
        size_t max_row_size = load_cols[0]->size();
        for (size_t i = 1; i < load_cols.size(); ++i) {
            if (load_cols[i]->size() > max_row_size) {
                max_row_size = load_cols[i]->size();
            }
        }
        // keep same rows
        for (auto& load_col : load_cols) {
            if (load_col->size() < max_row_size) {
                load_col->insert_many_defaults(max_row_size - load_col->size());
            } else if (load_col->size() > max_row_size) {
                load_col->resize(max_row_size);
            }
        }
        for (size_t i = 0; i < load_cols.size(); ++i) {
            auto& col = load_cols[i];
            block->insert(ColumnWithTypeAndName(std::move(col), types[i], types[i]->get_name()));
        }
        // print block
        std::cout << "build block structure: " << block->dump_structure() << std::endl;
        std::cout << "build block data: "
                  << block->dump_data(0, std::min(max_row_size, static_cast<size_t>(5)))
                  << std::endl;
        for (int i = 0; i < block->columns(); i++) {
            auto col = block->get_by_position(i);
            std::cout << "col: " << i << ", " << col.column->get_name() << ", "
                      << col.type->get_name() << ", " << col.to_string(0) << std::endl;
        }
    }

    static std::shared_ptr<arrow::RecordBatch> serialize_arrow(
            const std::shared_ptr<Block>& block) {
        std::shared_ptr<arrow::Schema> block_arrow_schema;
        EXPECT_EQ(get_arrow_schema_from_block(*block, &block_arrow_schema, "UTC"), Status::OK());
        std::cout << "schema: " << block_arrow_schema->ToString(true) << std::endl;
        // convert block to arrow
        std::shared_ptr<arrow::RecordBatch> result;
        cctz::time_zone _timezone_obj; //default UTC
        Status stt = convert_to_arrow_batch(*block, block_arrow_schema,
                                            arrow::default_memory_pool(), &result, _timezone_obj);
        EXPECT_EQ(Status::OK(), stt) << "convert block to arrow failed" << stt.to_string();
        std::cout << "arrow serialize result: " << result->num_columns() << ", "
                  << result->num_rows() << std::endl;
        return result;
    }

    static void deserialize_arrow(const std::shared_ptr<Block>& new_block,
                                  std::shared_ptr<arrow::RecordBatch> record_batch) {
        // deserialize arrow to block
        auto rows = record_batch->num_rows();
        for (size_t i = 0; i < record_batch->num_columns(); ++i) {
            auto array = record_batch->column(i);
            std::cout << "arrow record_batch pos: " << i << std::endl;
            auto& column_with_type_and_name = new_block->get_by_position(i);
            std::cout << "now we are testing column: "
                      << column_with_type_and_name.column->get_name()
                      << ", type: " << column_with_type_and_name.type->get_name() << std::endl;
            auto ret =
                    arrow_column_to_doris_column(array.get(), 0, column_with_type_and_name.column,
                                                 column_with_type_and_name.type, rows, "UTC");
            // do check data
            std::cout << "arrow_column_to_doris_column done, column data: "
                      << column_with_type_and_name.to_string(0).substr(0, 256)
                      << ", column size: " << column_with_type_and_name.column->size() << std::endl;
            EXPECT_EQ(Status::OK(), ret) << "convert arrow to block failed" << ret.to_string();
        }
        std::cout << "arrow deserialize block structure: " << new_block->dump_structure()
                  << std::endl;
        std::cout << "arrow deserialize block data: "
                  << new_block
                             ->dump_data(
                                     0, std::min(static_cast<size_t>(rows), static_cast<size_t>(5)))
                             .substr(0, 256)
                  << std::endl;
    }

    static void compare_two_blocks(const std::shared_ptr<Block>& frist_block,
                                   const std::shared_ptr<Block>& second_block) {
        for (size_t i = 0; i < frist_block->columns(); ++i) {
            EXPECT_EQ(frist_block->get_by_position(i).type, second_block->get_by_position(i).type);
            auto& col = frist_block->get_by_position(i).column;
            auto& assert_col = second_block->get_by_position(i).column;
            std::cout << "compare_two_blocks, column: " << col->get_name()
                      << ", type: " << frist_block->get_by_position(i).type->get_name()
                      << ", size: " << col->size() << ", assert size: " << assert_col->size()
                      << std::endl;
            EXPECT_EQ(assert_col->size(), col->size());
            for (size_t j = 0; j < assert_col->size(); ++j) {
                EXPECT_EQ(frist_block->get_by_position(i).to_string(j),
                          second_block->get_by_position(i).to_string(j));
                auto cell = col->operator[](j);
                auto assert_cell = assert_col->operator[](j);
                EXPECT_EQ(cell, assert_cell) << "column: " << col->get_name() << " row: " << j;
            }
        }
        EXPECT_EQ(frist_block->dump_data(), second_block->dump_data());
    }
};

} // namespace doris::vectorized
