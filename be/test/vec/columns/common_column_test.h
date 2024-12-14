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

// this test is gonna to be a column test template for all column which should make ut test to coverage the function defined in column
// for example column_array should test this function:
// size, reserve, resize, empty, byte_size, allocated_bytes, clone_resized,
// get_shrinked_column, filter, filter_by_selector, serialize_vec, deserialize_vec, get_max_row_byte_size
//
namespace doris::vectorized {

static bool gen_check_data_in_assert = true;

class CommonColumnTest : public ::testing::Test {
protected:
    // Helper function to load data from CSV, with index which splited by spliter and load to columns
    void load_data_from_csv(const DataTypeSerDeSPtrs serders, MutableColumns& columns,
                            const std::string& file_path, const char spliter = ';',
                            const std::set<int> idxes = {0}) {
        ASSERT_EQ(serders.size(), columns.size())
                << "serder size: " << serders.size() << " column size: " << columns.size();
        ASSERT_EQ(serders.size(), idxes.size())
                << "serder size: " << serders.size() << " idxes size: " << idxes.size();
        ASSERT_EQ(serders.size(), *idxes.end())
                << "serder size: " << serders.size() << " idxes size: " << *idxes.end();
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
                    Slice string_slice(value.data(), value.size());
                    std::cout << string_slice << std::endl;
                    if (auto st = serders[c_idx]->deserialize_one_cell_from_json(
                                *columns[c_idx], string_slice, options);
                        !st.ok()) {
                        //                        std::cout << "error in deserialize but continue: " << st.to_string()
                        //                                  << std::endl;
                    }
                    ++c_idx;
                }
                ++l_idx;
            }
        }
    }

    //// this is very helpful function to check data in column against expected results according different function in assert function
    //// such as run regress tests
    ////  if gen_check_data_in_assert is true, we will generate a file for check data, otherwise we will read the file to check data
    ////  so the key point is we should how we write assert callback function to check data,
    ///   and when check data is generated, we should check result to statisfy the semantic of the function
    static void check_res_file(string function_name, std::vector<std::vector<std::string>>& res) {
        string filename = "./res_" + function_name + ".csv";
        if (gen_check_data_in_assert) {
            std::ofstream res_file(filename);
            std::cout << "gen check data: " << res.size() << " with file: " << filename
                      << std::endl;
            if (!res_file.is_open()) {
                throw std::ios_base::failure("Failed to open file.");
            }

            for (const auto& row : res) {
                for (size_t i = 0; i < row.size(); ++i) {
                    auto cell = row[i];
                    res_file << cell;
                    if (i < row.size() - 1) {
                        res_file << ";"; // Add semicolon between columns
                    }
                }
                res_file << "\n"; // Newline after each row
            }

            res_file.close();
        } else {
            // we read generate file to check result
            std::cout << "check data: " << res.size() << " with file: " << filename << std::endl;
            std::ifstream file(filename);
            if (!file) {
                throw doris::Exception(ErrorCode::INVALID_ARGUMENT, "can not open the file: {} ",
                                       filename);
            }

            std::string line;
            std::vector<std::vector<std::string>> assert_res;
            while (std::getline(file, line)) {
                std::vector<std::string> row;
                std::stringstream lineStream(line);
                std::string value;
                while (std::getline(lineStream, value, ';')) {
                    row.push_back(value);
                }
                assert_res.push_back(row);
            }

            // we just do check here
            for (size_t i = 0; i < res.size(); ++i) {
                for (size_t j = 0; j < res[i].size(); ++j) {
                    EXPECT_EQ(res[i][j], assert_res[i][j]);
                }
            }
        }
    }

public:
    void SetUp() override {
        col_str = ColumnString::create();
        col_str->insert_data("aaa", 3);
        col_str->insert_data("bb", 2);
        col_str->insert_data("cccc", 4);

        col_int = ColumnInt64::create();
        col_int->insert_value(1);
        col_int->insert_value(2);
        col_int->insert_value(3);

        col_dcm = ColumnDecimal64::create(0, 3);
        col_dcm->insert_value(1.23);
        col_dcm->insert_value(4.56);
        col_dcm->insert_value(7.89);

        col_arr = ColumnArray::create(ColumnInt64::create(), ColumnArray::ColumnOffsets::create());
        Array array1 = {1, 2, 3};
        Array array2 = {4};
        col_arr->insert(array1);
        col_arr->insert(Array());
        col_arr->insert(array2);

        col_map = ColumnMap::create(ColumnString::create(), ColumnInt64::create(),
                                    ColumnArray::ColumnOffsets::create());
        Array k1 = {"a", "b", "c"};
        Array v1 = {1, 2, 3};
        Array k2 = {"d"};
        Array v2 = {4};
        Array a = Array();
        Map map1, map2, map3;
        map1.push_back(k1);
        map1.push_back(v1);
        col_map->insert(map1);
        map3.push_back(a);
        map3.push_back(a);
        col_map->insert(map3);
        map2.push_back(k2);
        map2.push_back(v2);
        col_map->insert(map2);
    }

    ColumnString::MutablePtr col_str;
    ColumnInt64::MutablePtr col_int;
    ColumnDecimal64::MutablePtr col_dcm;
    ColumnArray::MutablePtr col_arr;
    ColumnMap::MutablePtr col_map;

    ////==================================================================================================================
    // this is common function to check data in column against expected results according different function in assert function
    // which can be used in all column test
    // such as run regress tests
    //  step1. we can set gen_check_data_in_assert to true, then we will generate a file for check data, otherwise we will read the file to check data
    //  step2. we should write assert callback function to check data
    void check_data(MutableColumns& columns, DataTypeSerDeSPtrs serders, char col_spliter,
                    std::set<int> idxes, const std::string& column_data_file,
                    std::function<void(MutableColumns& load_cols, DataTypeSerDeSPtrs serders)>
                            assert_callback) {
        ASSERT_EQ(serders.size(), columns.size());
        // Step 1: Insert data from `column_data_file` into the column and check result with `check_data_file`
        // Load column data and expected data from CSV files
        std::vector<std::vector<std::string>> res;
        struct stat buff;
        if (stat(column_data_file.c_str(), &buff) == 0) {
            if (S_ISREG(buff.st_mode)) {
                // file
                load_data_from_csv(serders, columns, column_data_file, col_spliter, idxes);
            } else if (S_ISDIR(buff.st_mode)) {
                // dir
                std::filesystem::path fs_path(column_data_file);
                for (const auto& entry : std::filesystem::directory_iterator(fs_path)) {
                    std::string file_path = entry.path().string();
                    std::cout << "load data from file: " << file_path << std::endl;
                    load_data_from_csv(serders, columns, file_path, col_spliter, idxes);
                }
            }
        }

        // Step 2: Validate the data in `column` matches `expected_data`
        assert_callback(columns, serders);
    }
    ////==================================================================================================================

    void check_columns(MutableColumns& columns, DataTypeSerDeSPtrs serders, DataTypes dataTypes,
                       char col_spliter, std::set<int> idxes, const std::string& column_data_file,
                       const std::string& check_data_file, MutableColumns& check_columns,
                       DataTypeSerDeSPtrs check_serders, char check_col_spliter,
                       std::set<int> check_idxes,
                       std::function<void(MutableColumns& load_cols, MutableColumns& assert_columns,
                                          DataTypes dataTypes)>
                               assert_callback) {
        // Load column data and expected data from CSV files
        load_data_from_csv(serders, columns, column_data_file, col_spliter, idxes);
        load_data_from_csv(check_serders, check_columns, check_data_file, col_spliter, idxes);

        // Step 2: Validate the data in `column` matches `expected_data`
        assert_callback(columns, check_columns, dataTypes);
    }

    static void checkColumn(const IColumn& col1, const IColumn& col2, const IDataType& dataType,
                            size_t column_size) {
        for (size_t i = 0; i < column_size; ++i) {
            Field f1;
            Field f2;
            col1.get(i, f1);
            col2.get(i, f2);
            EXPECT_EQ(f1, f2);
        }
    }

    void printColumn(const IColumn& column, const IDataType& dataType) {
        std::cout << "colum: " << column.get_name() << " total size: " << column.size()
                  << std::endl;
        auto serde = dataType.get_serde(0);
        auto serde_col = ColumnString::create();
        auto option = DataTypeSerDe::FormatOptions();
        serde_col->reserve(column.size());
        VectorBufferWriter buffer_writer(*serde_col.get());
        for (size_t i = 0; i < column.size(); ++i) {
            if (auto st = serde->serialize_one_cell_to_json(column, i, buffer_writer, option);
                !st) {
                std::cerr << "Failed to serialize column at row " << i << std::endl;
                break;
            }
            buffer_writer.commit();
            std::cout << serde_col->get_data_at(i).to_string() << std::endl;
        }
    }

    // assert insert_from
    // Define the custom assert callback function to verify insert_from behavior
    static void assert_insert_from_callback(MutableColumns& load_cols, DataTypeSerDeSPtrs serders,
                                            std::vector<std::vector<std::string>>& assert_res) {
        // Create an empty column to verify `insert_from` functionality
        MutableColumns verify_columns;
        for (auto& col : load_cols) {
            verify_columns.push_back(col->clone_empty());
        }
        auto option = DataTypeSerDe::FormatOptions();
        // Insert data from `load_cols` to `verify_columns` using `insert_from`
        std::vector<std::vector<std::string>> res;
        for (size_t i = 0; i < load_cols.size(); ++i) {
            auto& source_column = load_cols[i];
            auto& target_column = verify_columns[i];
            std::vector<std::string> data;
            for (size_t j = 0; j < source_column->size(); ++j) {
                target_column->insert_from(*source_column, j);
            }

            // Verify the inserted data matches the expected results in `assert_res`
            auto ser_col = ColumnString::create();
            ser_col->reserve(target_column->size());
            VectorBufferWriter buffer_writer(*ser_col.get());
            for (size_t j = 0; j < target_column->size(); ++j) {
                if (auto st = serders[i]->serialize_one_cell_to_json(*target_column, j,
                                                                     buffer_writer, option);
                    !st) {
                    std::cerr << "Failed to serialize column " << i << " at row " << j << std::endl;
                    break;
                }
                buffer_writer.commit();
                std::string actual_str_value = ser_col->get_data_at(j).to_string();
                data.push_back(actual_str_value);
            }
            res.push_back(data);
        }
        check_res_file("insert_from", assert_res);
    }

    // assert insert_range_from
    // Define the custom assert callback function to verify insert_range_from behavior
    static void assert_insert_range_from_callback(MutableColumns& load_cols,
                                                  DataTypeSerDeSPtrs serders) {
        // Create an empty column to verify `insert_range_from` functionality
        MutableColumns verify_columns;
        for (auto& col : load_cols) {
            verify_columns.push_back(col->clone_empty());
        }
        auto option = DataTypeSerDe::FormatOptions();

        std::vector<size_t> check_length = {0, 10, 100, 1000, 10000, 100000, 1000000};
        // Insert data from `load_cols` to `verify_columns` using `insert_range_from`
        std::vector<std::vector<std::string>> res;
        for (auto cl = check_length.begin(); cl < check_length.end(); ++cl) {
            for (size_t i = 0; i < load_cols.size(); ++i) {
                std::vector<std::string> data;
                std::cout << "==== now test insert_range_from with col "
                          << verify_columns[i]->get_name() << std::endl;
                auto& source_column = load_cols[i];
                auto& target_column = verify_columns[i];
                std::vector<size_t> check_start_pos = {0, source_column->size(),
                                                       (source_column->size() + 1) >> 1};
                // size_t(-1) may cause overflow, but here we have compiler to check it
                std::vector<size_t> err_start_pos = {source_column->size() + 1};
                for (auto pos = err_start_pos.begin(); pos < err_start_pos.end(); ++pos) {
                    std::cout << "error insert_range_from from " << *pos << " with length " << *cl
                              << std::endl;
                    std::cout << *pos + *cl << " > " << source_column->size() << std::endl;
                    EXPECT_THROW(target_column->insert_range_from(*source_column, *pos, *cl),
                                 Exception);
                }
                for (auto pos = check_start_pos.begin(); pos < check_start_pos.end(); ++pos) {
                    target_column->clear();
                    std::cout << "now insert_range_from from " << *pos << " with length " << *cl
                              << " with source size: " << source_column->size() << std::endl;
                    if (*pos + *cl > source_column->size()) {
                        EXPECT_THROW(target_column->insert_range_from(*source_column, *pos, *cl),
                                     Exception);
                        continue;
                    } else {
                        target_column->insert_range_from(*source_column, *pos, *cl);
                    }

                    // Verify the inserted data matches the expected results in `assert_res`
                    auto ser_col = ColumnString::create();
                    ser_col->reserve(target_column->size());
                    VectorBufferWriter buffer_writer(*ser_col.get());
                    for (size_t j = 0; j < target_column->size(); ++j) {
                        if (auto st = serders[i]->serialize_one_cell_to_json(*target_column, j,
                                                                             buffer_writer, option);
                            !st) {
                            std::cerr << "Failed to serialize column " << i << " at row " << j
                                      << std::endl;
                            break;
                        }
                        buffer_writer.commit();
                        data.push_back(ser_col->get_data_at(j).to_string());
                    }
                    res.push_back(data);
                }
            }
        }
        check_res_file("insert_range_from", res);
    }

    // assert insert_range_from_ignore_overflow which happened in columnStr<UInt32> want to insert from ColumnStr<UInt64> for more column string to be inserted not just limit to the 4G
    // Define the custom assert callback function to verify insert_range_from_ignore_overflow behavior

    // assert insert_many_from (used in join situation, which handle left table col to expand for right table : such as A[1,2,3] inner join B[2,2,4,4] => A[2,2] )
    // Define the custom assert callback function to verify insert_many_from behavior
    static void assert_insert_many_from_callback(MutableColumns& load_cols,
                                                 DataTypeSerDeSPtrs serders) {
        // Create an empty column to verify `insert_many_from` functionality
        std::cout << "now we are in assert_insert_many_from_callback" << std::endl;
        MutableColumns verify_columns;
        for (auto& col : load_cols) {
            verify_columns.push_back(col->clone_empty());
        }
        auto option = DataTypeSerDe::FormatOptions();
        std::vector<std::vector<string>> actual_res;

        std::vector<size_t> check_length = {0, 10, 100, 1000, 10000, 100000, 1000000};
        // Insert data from `load_cols` to `verify_columns` using `insert_many_from`
        for (auto cl = check_length.begin(); cl < check_length.end(); ++cl) {
            for (size_t i = 0; i < load_cols.size(); ++i) {
                auto& source_column = load_cols[i];
                auto& target_column = verify_columns[i];
                // size_t(-1) may cause overflow, but here we have compiler to check it
                std::vector<size_t> check_start_pos = {0, source_column->size(),
                                                       source_column->size() + 1,
                                                       (source_column->size() + 1) >> 1};
                for (auto pos = check_start_pos.begin(); pos < check_start_pos.end(); ++pos) {
                    if (*pos > source_column->size() || *cl > source_column->size()) {
                        // insert_range_from now we have no any exception error data to handle, so here will meet crash
                        continue;
                    } else if (*pos + *cl > source_column->size()) {
                        target_column->clear();
                        // insert_range_from now we have no any exception error data to handle and also no crash
                        std::cout << "we expect exception insert_many_from from " << *pos
                                  << " with length " << *cl
                                  << "with source size: " << source_column->size() << std::endl;
                        target_column->insert_many_from(*source_column, *pos, *cl);
                    }

                    // Verify the inserted data matches the expected results in `assert_res`
                    auto ser_col = ColumnString::create();
                    ser_col->reserve(target_column->size());
                    VectorBufferWriter buffer_writer(*ser_col.get());
                    std::vector<std::string> data;
                    for (size_t j = 0; j < target_column->size(); ++j) {
                        if (auto st = serders[i]->serialize_one_cell_to_json(*target_column, j,
                                                                             buffer_writer, option);
                            !st) {
                            std::cerr << "Failed to serialize column " << i << " at row " << j
                                      << std::endl;
                            break;
                        }
                        buffer_writer.commit();
                        std::string actual_str_value = ser_col->get_data_at(j).to_string();
                        data.push_back(actual_str_value);
                    }
                    actual_res.push_back(data);
                }
            }
        }
        // Generate or check the actual result file
        check_res_file("insert_many_from", actual_res);
    }

    static void assert_insert_indices_from_callback(MutableColumns& load_cols,
                                                    DataTypeSerDeSPtrs serders) {
        // Create an empty column to verify `insert_indices_from` functionality
        MutableColumns verify_columns;
        for (auto& col : load_cols) {
            verify_columns.push_back(col->clone_empty());
        }
        auto option = DataTypeSerDe::FormatOptions();
        std::vector<std::vector<string>> res;
        // Insert data from `load_cols` to `verify_columns` using `insert_indices_from`
        for (size_t i = 0; i < load_cols.size(); ++i) {
            auto& source_column = load_cols[i];
            auto& target_column = verify_columns[i];

            // uint32_t(-1) now we have compiler to make sure it will not appear
            // and this function
            std::vector<uint32_t> check_indices = {0, uint32_t(source_column->size()),
                                                   uint32_t(source_column->size() + 1),
                                                   uint32_t((source_column->size() + 1) >> 1)};
            for (auto from_idx = check_indices.begin(); from_idx < check_indices.end();
                 ++from_idx) {
                for (auto end_idx = check_indices.begin(); end_idx < check_indices.end();
                     ++end_idx) {
                    target_column->clear();
                    // Insert data from `load_cols` to `verify_columns` using `insert_indices_from`
                    if (*from_idx > *end_idx || *from_idx >= source_column->size() ||
                        *end_idx >= source_column->size()) {
                        //                        EXPECT_ANY_THROW(target_column->insert_indices_from(*source_column, &(*from_idx), &(*end_idx)));
                        // now we do not to check it but we should make sure pass the arguments correctly, if we do not
                        // here we will meet `heap-buffer-overflow on address`
                        continue;
                    } else {
                        target_column->insert_indices_from(*source_column, &(*from_idx),
                                                           &(*end_idx));
                    }
                    std::cout << source_column->get_name() << " now insert_indices_from from "
                              << *from_idx << " to " << *end_idx << std::endl;
                    // Verify the inserted data matches the expected results in `assert_res`
                    auto ser_col = ColumnString::create();
                    ser_col->reserve(target_column->size());
                    VectorBufferWriter buffer_writer(*ser_col.get());
                    std::vector<string> data;
                    for (size_t j = 0; j < target_column->size(); ++j) {
                        if (auto st = serders[i]->serialize_one_cell_to_json(*target_column, j,
                                                                             buffer_writer, option);
                            !st) {
                            std::cerr << "Failed to serialize column " << i << " at row " << j
                                      << std::endl;
                            break;
                        }
                        buffer_writer.commit();
                        std::string actual_str_value = ser_col->get_data_at(j).to_string();
                        std::cout << "actual_str_value: " << actual_str_value << std::endl;
                        data.push_back(actual_str_value);
                    }
                    res.push_back(data);
                }
            }
        }
        check_res_file("insert_indices_from", res);
    }

    static void assert_insert_data_from_callback(MutableColumns& load_cols,
                                                 DataTypeSerDeSPtrs serders) {
        // Create an empty column to verify `insert_data` functionality
        MutableColumns verify_columns;
        for (auto& col : load_cols) {
            verify_columns.push_back(col->clone_empty());
        }
        auto option = DataTypeSerDe::FormatOptions();
        // Insert data from `load_cols` to `verify_columns` using `insert_data`
        std::vector<std::vector<string>> res;
        for (size_t i = 0; i < load_cols.size(); ++i) {
            auto& source_column = load_cols[i];
            auto& target_column = verify_columns[i];

            for (size_t j = 0; j < source_column->size(); ++j) {
                target_column->insert_data(source_column->get_data_at(j).data,
                                           source_column->get_data_at(j).size);
            }

            // Verify the inserted data matches the expected results in `assert_res`
            auto ser_col = ColumnString::create();
            ser_col->reserve(target_column->size());
            VectorBufferWriter buffer_writer(*ser_col.get());
            std::vector<string> data;
            for (size_t j = 0; j < target_column->size(); ++j) {
                if (auto st = serders[i]->serialize_one_cell_to_json(*target_column, j,
                                                                     buffer_writer, option);
                    !st) {
                    std::cerr << "Failed to serialize column " << i << " at row " << j << std::endl;
                    break;
                }
                std::string actual_str_value = ser_col->get_data_at(j).to_string();
                data.push_back(actual_str_value);
            }
            res.push_back(data);
        }
        check_res_file("insert_data", res);
    }

    static void assert_insert_many_raw_data_from_callback(MutableColumns& load_cols,
                                                          DataTypeSerDeSPtrs serders) {
        // Create an empty column to verify `insert_many_raw_data` functionality
        MutableColumns verify_columns;
        for (auto& col : load_cols) {
            verify_columns.push_back(col->clone_empty());
        }
        auto option = DataTypeSerDe::FormatOptions();
        std::vector<std::vector<string>> res;
        // Insert data from `load_cols` to `verify_columns` using `insert_many_raw_data`
        for (size_t i = 0; i < load_cols.size(); ++i) {
            auto& source_column = load_cols[i];
            auto& target_column = verify_columns[i];

            for (size_t j = 0; j < source_column->size(); ++j) {
                target_column->insert_many_raw_data(source_column->get_data_at(j).data,
                                                    source_column->get_data_at(j).size);
            }

            // Verify the inserted data matches the expected results in `assert_res`
            auto ser_col = ColumnString::create();
            ser_col->reserve(target_column->size());
            VectorBufferWriter buffer_writer(*ser_col.get());
            std::vector<string> data;
            for (size_t j = 0; j < target_column->size(); ++j) {
                if (auto st = serders[i]->serialize_one_cell_to_json(*target_column, j,
                                                                     buffer_writer, option);
                    !st) {
                    std::cerr << "Failed to serialize column " << i << " at row " << j << std::endl;
                    break;
                }
                std::string actual_str_value = ser_col->get_data_at(j).to_string();
                data.push_back(actual_str_value);
            }
            res.push_back(data);
        }
        check_res_file("insert_many_raw_data", res);
    }

    // assert insert_default
    // Define the custom assert callback function to verify insert_default behavior
    static void assert_insert_default_callback(MutableColumns& load_cols,
                                               DataTypeSerDeSPtrs serders) {
        // Create an empty column to verify `insert_default` functionality
        MutableColumns verify_columns;
        for (auto& col : load_cols) {
            verify_columns.push_back(col->clone_empty());
        }
        auto option = DataTypeSerDe::FormatOptions();
        std::vector<std::vector<string>> res;
        // Insert data from `load_cols` to `verify_columns` using `insert_default`
        for (size_t i = 0; i < load_cols.size(); ++i) {
            auto& target_column = verify_columns[i];
            target_column->insert_default();

            // Verify the inserted data matches the expected results in `assert_res`
            auto ser_col = ColumnString::create();
            ser_col->reserve(target_column->size());
            VectorBufferWriter buffer_writer(*ser_col.get());
            std::vector<string> data;
            for (size_t j = 0; j < target_column->size(); ++j) {
                if (auto st = serders[i]->serialize_one_cell_to_json(*target_column, j,
                                                                     buffer_writer, option);
                    !st) {
                    std::cerr << "Failed to serialize column " << i << " at row " << j << std::endl;
                    break;
                }
                buffer_writer.commit();
                std::string actual_str_value = ser_col->get_data_at(j).to_string();
                data.push_back(actual_str_value);
            }
            res.push_back(data);
        }
        check_res_file("insert_default", res);
    }

    // assert insert_many_default
    // Define the custom assert callback function to verify insert_many_defaults behavior
    static void assert_insert_many_defaults_callback(MutableColumns& load_cols,
                                                     DataTypeSerDeSPtrs serders) {
        // Create an empty column to verify `insert_many_defaults` functionality
        MutableColumns verify_columns;
        for (auto& col : load_cols) {
            verify_columns.push_back(col->clone_empty());
        }
        auto option = DataTypeSerDe::FormatOptions();
        std::vector<std::vector<string>> res;
        std::vector<size_t> check_length = {0, 10, 100, 1000, 10000, 100000, 1000000};
        // Insert data from `load_cols` to `verify_columns` using `insert_many_defaults`
        for (auto cl = check_length.begin(); cl < check_length.end(); ++cl) {
            for (size_t i = 0; i < load_cols.size(); ++i) {
                auto& target_column = verify_columns[i];
                target_column->insert_many_defaults(*cl);

                // Verify the inserted data matches the expected results in `assert_res`
                auto ser_col = ColumnString::create();
                ser_col->reserve(target_column->size());
                VectorBufferWriter buffer_writer(*ser_col.get());
                std::vector<string> data;
                for (size_t j = 0; j < target_column->size(); ++j) {
                    if (auto st = serders[i]->serialize_one_cell_to_json(*target_column, j,
                                                                         buffer_writer, option);
                        !st) {
                        std::cerr << "Failed to serialize column " << i << " at row " << j
                                  << std::endl;
                        break;
                    }
                    buffer_writer.commit();
                    std::string actual_str_value = ser_col->get_data_at(j).to_string();
                    data.push_back(actual_str_value);
                }
                res.push_back(data);
            }
        }
        check_res_file("insert_many_defaults", res);
    }

    //// data access interfaces:
    // virtual StringRef
    //get_data_at (size_t n) const = 0
    // if we implement the get_data_at, we should know the data is stored in the column, and we can get the data by the index
    static void assert_get_data_at_callback(MutableColumns& load_cols, DataTypeSerDeSPtrs serders) {
        std::vector<std::vector<std::string>> res;
        // just check cols get_data_at is the same as assert_res
        for (size_t i = 0; i < load_cols.size(); ++i) {
            auto& source_column = load_cols[i];
            std::vector<std::string> data;
            for (size_t j = 0; j < source_column->size(); ++j) {
                auto actual_str_value = source_column->get_data_at(j).to_string();
                data.push_back(actual_str_value);
            }
            res.push_back(data);
        }
        check_res_file("get_data_at", res);
    }

    //virtual void
    // field is memory layout of the data , maybe we can use get from base column and insert into same type column to check this behavior is right
    //get (size_t n, Field &res) const = 0
    //Like the previous one, but avoids extra copying if Field is in a container, for example.
    //virtual Field
    //operator[] (size_t n) const = 0
    static void assert_field_callback(MutableColumns& load_cols, DataTypeSerDeSPtrs serders) {
        MutableColumns assert_cols(load_cols.size());
        for (size_t i = 0; i < load_cols.size(); ++i) {
            assert_cols[i] = load_cols[i]->clone_empty();
        }
        auto option = DataTypeSerDe::FormatOptions();
        std::vector<std::vector<std::string>> res;
        // just check cols get is the same as assert_res
        for (size_t i = 0; i < load_cols.size(); ++i) {
            auto& source_column = load_cols[i];
            std::cout << " new insert field for column : " << assert_cols[i]->get_name()
                      << " with size : " << assert_cols[i]->size() << std::endl;
            for (size_t j = 0; j < source_column->size(); ++j) {
                Field f;
                source_column->get(j, f);
                std::cout << "field: " << f.get_type() << std::endl;
                assert_cols[i]->insert(f);
            }
        }
        // Verify the inserted data matches the expected results in `assert_res`
        for (size_t i = 0; i < assert_cols.size(); ++i) {
            auto ser_col = ColumnString::create();
            ser_col->reserve(load_cols[i]->size());
            VectorBufferWriter buffer_writer(*ser_col.get());
            std::vector<string> data;
            for (size_t j = 0; j < assert_cols[i]->size(); ++j) {
                if (auto st = serders[i]->serialize_one_cell_to_json(*assert_cols[i], j,
                                                                     buffer_writer, option);
                    !st) {
                    std::cerr << "Failed to serialize column " << i << " at row " << j << std::endl;
                    break;
                }
                buffer_writer.commit();
                std::string actual_str_value = ser_col->get_data_at(j).to_string();
                std::cout << "actual_str_value: " << actual_str_value << std::endl;
                data.push_back(actual_str_value);
            }
            res.push_back(data);
        }
        check_res_file("get_field", res);
        // just check cols operator [] to get field same with field
        std::vector<std::vector<std::string>> res2;
        for (size_t i = 0; i < load_cols.size(); ++i) {
            auto& source_column = load_cols[i];
            for (size_t j = 0; j < source_column->size(); ++j) {
                Field f = source_column->operator[](j);
                assert_cols[i]->insert(f);
            }
        }

        // Verify the inserted data matches the expected results in `assert_res`
        for (size_t i = 0; i < assert_cols.size(); ++i) {
            auto ser_col = ColumnString::create();
            ser_col->reserve(load_cols[i]->size());
            VectorBufferWriter buffer_writer(*ser_col.get());
            std::vector<string> data;
            for (size_t j = 0; j < assert_cols[i]->size(); ++j) {
                if (auto st = serders[i]->serialize_one_cell_to_json(*assert_cols[i], j,
                                                                     buffer_writer, option);
                    !st) {
                    std::cerr << "Failed to serialize column " << i << " at row " << j << std::endl;
                    break;
                }
                buffer_writer.commit();
                std::string actual_str_value = ser_col->get_data_at(j).to_string();
                std::cout << "actual_str_value: " << actual_str_value << std::endl;
                data.push_back(actual_str_value);
            }
            res2.push_back(data);
        }
        check_res_file("get_field_operator", res2);
    }
    //
    //virtual StringRef
    //get_raw_data () const which is continues memory layout of the data,
    // we can use this to check the data is stored in the column
    template <class T>
    static void assert_get_raw_data_callback(MutableColumns& load_cols,
                                             DataTypeSerDeSPtrs serders) {
        // just check cols get_raw_data is the same as assert_res
        std::cout << "now we are in assert_get_raw_data_callback" << std::endl;
        std::vector<std::vector<string>> res;
        MutableColumns assert_cols(load_cols.size());
        for (size_t i = 0; i < load_cols.size(); ++i) {
            assert_cols[i] = load_cols[i]->clone_empty();
        }
        auto option = DataTypeSerDe::FormatOptions();
        for (size_t i = 0; i < load_cols.size(); ++i) {
            auto& source_column = load_cols[i];
            const T* rd = (T*)source_column->get_raw_data().data;
            for (size_t j = 0; j < source_column->size(); j++) {
                Field f;
                source_column->get(j, f);
                ASSERT_EQ(f, Field(rd[j]));
                // insert field to assert column
                assert_cols[i]->insert(f);
            }
        }

        // Verify the inserted data matches the expected results in `assert_res`
        for (size_t i = 0; i < assert_cols.size(); ++i) {
            auto ser_col = ColumnString::create();
            ser_col->reserve(load_cols[i]->size());
            VectorBufferWriter buffer_writer(*ser_col.get());
            std::vector<string> data;
            for (size_t j = 0; j < assert_cols[i]->size(); ++j) {
                if (auto st = serders[i]->serialize_one_cell_to_json(*assert_cols[i], j,
                                                                     buffer_writer, option);
                    !st) {
                    std::cerr << "Failed to serialize column " << i << " at row " << j << std::endl;
                    break;
                }
                buffer_writer.commit();
                std::string actual_str_value = ser_col->get_data_at(j).to_string();
                std::cout << "actual_str_value: " << actual_str_value << std::endl;
                data.push_back(actual_str_value);
            }
            res.push_back(data);
        }
        check_res_file("get_raw_data", res);
    }

    //If returns the underlying data array, otherwise throws an exception.
    //virtual Int64
    //get_int (size_t) const
    static void assert_get_int_callback(MutableColumns& load_cols, DataTypeSerDeSPtrs serders) {
        std::vector<std::vector<string>> res;
        // just check cols get_int is the same as assert_res
        for (size_t i = 0; i < load_cols.size(); ++i) {
            auto& source_column = load_cols[i];
            std::vector<string> data;
            for (size_t j = 0; j < source_column->size(); ++j) {
                auto actual_str_value = std::to_string(source_column->get_int(j));
                data.push_back(actual_str_value);
            }
            res.push_back(data);
        }
        check_res_file("get_int", res);
    }
    //virtual bool
    //get_bool (size_t) const
    static void assert_get_bool_callback(MutableColumns& load_cols, DataTypeSerDeSPtrs serders) {
        std::vector<std::vector<string>> res;
        // just check cols get_bool is the same as assert_res
        for (size_t i = 0; i < load_cols.size(); ++i) {
            auto& source_column = load_cols[i];
            std::vector<string> data;
            for (size_t j = 0; j < source_column->size(); ++j) {
                auto actual_str_value = std::to_string(source_column->get_bool(j));
                data.push_back(actual_str_value);
            }
            res.push_back(data);
        }
        check_res_file("get_bool", res);
    }

    //// column meta interfaces:
    // virtual std::string
    //get_name () const , simple assert to make sure name
    static void getNameAssert(IColumn& column, const string expect_name) {
        ASSERT_EQ(expect_name, column.get_name());
    }

    // size related we can check from checked file to make sure the size is right
    //
    //Returns number of values in column.
    //virtual size_t
    //size_of_value_if_fixed () const
    //
    //If values_have_fixed_size, returns size of value, otherwise throw an exception.
    //virtual size_t
    //byte_size () const =0
    //
    //Size of column data in memory (may be approximate) - for profiling. Zero, if could not be determined.
    //
    //
    //virtual size_t
    //allocated_bytes () const =0
    //
    static void assert_size_callback(MutableColumns& load_cols, DataTypeSerDeSPtrs serders) {
        std::vector<std::vector<string>> res;
        // just check cols size is the same as assert_res
        for (size_t i = 0; i < load_cols.size(); ++i) {
            std::vector<string> data;
            auto& source_column = load_cols[i];
            auto actual_str_value = std::to_string(source_column->size());
            data.push_back(actual_str_value);
            res.push_back(data);
        }
        check_res_file("size", res);
    }

    // assert byte_size
    // Define the custom assert callback function to verify byte_size behavior
    static void assert_byte_size_callback(MutableColumns& load_cols, DataTypeSerDeSPtrs serders) {
        // just check cols byte_size is the same as assert_res
        std::vector<std::vector<string>> res;
        for (size_t i = 0; i < load_cols.size(); ++i) {
            std::vector<string> data;
            auto& source_column = load_cols[i];
            auto actual_str_value = std::to_string(source_column->byte_size());
            data.push_back(actual_str_value);
            res.push_back(data);
        }
        check_res_file("byte_size", res);
    }

    // assert allocated_bytes Define the custom assert callback function to verify allocated_bytes behavior
    static void assert_allocated_bytes_callback(MutableColumns& load_cols,
                                                DataTypeSerDeSPtrs serders) {
        // just check cols allocated_bytes is the same as assert_res
        std::cout << "now we are in assert_allocated_bytes_callback" << std::endl;
        std::vector<std::vector<string>> res;
        for (size_t i = 0; i < load_cols.size(); ++i) {
            std::vector<string> data;
            auto& source_column = load_cols[i];
            auto actual_str_value = std::to_string(source_column->allocated_bytes());
            data.push_back(actual_str_value);
            res.push_back(data);
        }
        check_res_file("allocated_bytes", res);
    }

    // is_exclusive assert should assert the column which has multiple columnPtr in the column

    //// data manage interfaces
    //virtual void
    // pop_back (size_t n) = 0
    // assert pop_back
    static void assert_pop_back_callback(MutableColumns& load_cols, DataTypeSerDeSPtrs serders) {
        // Create an empty column to verify `pop_back` functionality
        // check pop_back with different n
        std::vector<std::vector<string>> res;
        auto option = DataTypeSerDe::FormatOptions();
        // size_t(-1) 会导致pod_array 溢出
        std::vector<size_t> check_length = {0, 1, 10, 100, 1000, 10000, 100000};
        for (auto cl = check_length.begin(); cl < check_length.end(); ++cl) {
            for (size_t i = 0; i < load_cols.size(); ++i) {
                auto& source_column = load_cols[i];
                if (*cl > source_column->size()) {
                    // now we do not check in popback, but we should make sure the arguments are passed correctly,
                    // otherwise we should meet `Check failed: false Amount of memory requested to allocate is more than allowed`
                    std::cout << "now we are in pop_back column : " << load_cols[i]->get_name()
                              << " for column size : " << source_column->size() << std::endl;
                    source_column->pop_back(source_column->size());
                } else {
                    std::cout << "now we are in pop_back column : " << load_cols[i]->get_name()
                              << " with check length: " << *cl
                              << " for column size : " << source_column->size() << std::endl;
                    source_column->pop_back(*cl);
                }

                // Verify the pop back data matches the expected results in `assert_res`
                auto ser_col = ColumnString::create();
                ser_col->reserve(load_cols[i]->size());
                VectorBufferWriter buffer_writer(*ser_col.get());
                std::vector<string> data;
                for (size_t j = 0; j < source_column->size(); ++j) {
                    if (auto st = serders[i]->serialize_one_cell_to_json(*source_column, j,
                                                                         buffer_writer, option);
                        !st) {
                        std::cerr << "Failed to serialize column " << i << " at row " << j
                                  << std::endl;
                        break;
                    }
                    buffer_writer.commit();
                    std::string actual_str_value = ser_col->get_data_at(j).to_string();
                    data.push_back(actual_str_value);
                }
                res.push_back(data);
            }
        }
        check_res_file("pop_back", res);
    }

    //virtual MutablePtr
    // Creates empty column with the same type.
    //clone_empty () const this is clone ,we should also check if the size is 0 after clone and ptr is not the same
    void assert_clone_empty(IColumn& column) {
        auto ptr = column.clone_empty();
        EXPECT_EQ(ptr->size(), 0);
        EXPECT_NE(ptr.get(), &column);
    }

    //virtual MutablePtr
    //clone_resized (size_t s) const
    static void assert_clone_resized_callback(MutableColumns& load_cols,
                                              DataTypeSerDeSPtrs serders) {
        // Create an empty column to verify `clone_resized` functionality
        // check clone_resized with different size
        // size_t(-1) 会导致pod_array 溢出
        std::vector<size_t> check_length = {0, 1, 10, 100, 1000, 10000, 100000};
        auto option = DataTypeSerDe::FormatOptions();
        std::vector<std::vector<string>> res;
        for (auto cl = check_length.begin(); cl < check_length.end(); ++cl) {
            for (size_t i = 0; i < load_cols.size(); ++i) {
                auto& source_column = load_cols[i];
                auto ptr = source_column->clone_resized(*cl);
                // check size
                EXPECT_EQ(ptr->size(), *cl);
                // check ptr is not the same
                EXPECT_NE(ptr.get(), source_column);

                // check after clone_resized with assert_res
                auto ser_col = ColumnString::create();
                ser_col->reserve(ptr->size());
                VectorBufferWriter buffer_writer(*ser_col.get());
                std::vector<string> data;
                data.push_back("column: " + source_column->get_name() + " with check size: " +
                               std::to_string(*cl) + " with ptr: " + std::to_string(ptr->size()));
                for (size_t j = 0; j < ptr->size(); ++j) {
                    if (auto st = serders[i]->serialize_one_cell_to_json(*ptr, j, buffer_writer,
                                                                         option);
                        !st) {
                        std::cerr << "Failed to serialize column " << i << " at row " << j
                                  << std::endl;
                        break;
                    }
                    buffer_writer.commit();
                    std::string actual_str_value = ser_col->get_data_at(j).to_string();
                    data.push_back(actual_str_value);
                }
                res.push_back(data);
            }
        }
        check_res_file("clone_resized", res);
    }

    // is_exclusive() means to check the ptr is not shared with other, and we can just clean it. so it's important to check the column is exclusive or not

    //virtual Ptr
    //cut (size_t start, size_t length) const final will call clone_empty and insert_range_from
    static void assert_cut_callback(MutableColumns& load_cols, DataTypeSerDeSPtrs serders) {
        // Create an empty column to verify `cut` functionality
        // check cut with different start and length
        // size_t(-1) 会导致pod_array 溢出
        std::vector<std::vector<string>> res;
        auto option = DataTypeSerDe::FormatOptions();
        //        std::vector<size_t> cut_start = {0, 1, 10, 100, 1000, 10000, 100000};
        std::vector<size_t> check_length = {0, 1, 10, 100, 1000, 10000, 100000};
        for (auto cl = check_length.begin(); cl < check_length.end(); ++cl) {
            for (size_t i = 0; i < load_cols.size(); ++i) {
                auto& source_column = load_cols[i];
                ColumnPtr ptr;
                size_t insert_size = *cl > source_column->size() ? source_column->size() : *cl;
                // now we do not check in cut, but we should make sure the arguments are passed correctly,
                // otherwise we should meet `Check failed: false Amount of memory requested to allocate is more than allowed`
                std::cout << "now we are in cut column : " << load_cols[i]->get_name()
                          << " with check length: " << insert_size
                          << " for column size : " << source_column->size() << std::endl;
                ptr = source_column->cut(0, insert_size);
                // check size
                EXPECT_EQ(ptr->size(), insert_size);
                // check ptr is not the same
                EXPECT_NE(ptr.get(), source_column);
                // check after cut with assert_res
                auto ser_col = ColumnString::create();
                ser_col->reserve(ptr->size());
                VectorBufferWriter buffer_writer(*ser_col.get());
                std::vector<string> data;
                data.push_back("column: " + source_column->get_name() + " with check size: " +
                               std::to_string(*cl) + " with ptr: " + std::to_string(ptr->size()));
                for (size_t j = 0; j < ptr->size(); ++j) {
                    if (auto st = serders[i]->serialize_one_cell_to_json(*ptr, j, buffer_writer,
                                                                         option);
                        !st) {
                        std::cerr << "Failed to serialize column " << i << " at row " << j
                                  << std::endl;
                        break;
                    }
                    buffer_writer.commit();
                    std::string actual_str_value = ser_col->get_data_at(j).to_string();
                    data.push_back(actual_str_value);
                }
            }
        }
        check_res_file("cut", res);
    }

    //virtual Ptr
    //shrink (size_t length) const final  shrink many has different ptr for the column, because of
    // here has some improvement according the origin column which use_count is 1, we can just return the origin column to avoid the copy
    // but we should make sure the column data is absolutely the same as expand or cut with the data
    static void assert_shrink_callback(MutableColumns& load_cols, DataTypeSerDeSPtrs serders) {
        // Create an empty column to verify `cut` functionality
        // check shrink with different start and length
        // size_t(-1) 会导致pod_array 溢出
        std::vector<std::vector<string>> res;
        auto option = DataTypeSerDe::FormatOptions();
        //        std::vector<size_t> cut_start = {0, 1, 10, 100, 1000, 10000, 100000};
        // less check_length: cut , more check_length: expand
        std::vector<size_t> check_length = {0, 1, 10, 100, 1000, 10000, 100000};
        for (auto cl = check_length.begin(); cl < check_length.end(); ++cl) {
            for (size_t i = 0; i < load_cols.size(); ++i) {
                auto& source_column = load_cols[i];
                ColumnPtr ptr;
                size_t insert_size = *cl > source_column->size() ? source_column->size() : *cl;
                // now we do not check in cut, but we should make sure the arguments are passed correctly,
                // otherwise we should meet `Check failed: false Amount of memory requested to allocate is more than allowed`
                std::cout << "now we are in shrink column : " << load_cols[i]->get_name()
                          << " with check length: " << insert_size
                          << " for column size : " << source_column->size() << std::endl;
                ptr = source_column->shrink(insert_size);
                std::cout << "use_count : " << source_column->use_count() << std::endl;
                // check size
                EXPECT_EQ(ptr->size(), insert_size);
                // check ptr is not the same
                EXPECT_NE(ptr.get(), source_column);
                // check after cut with assert_res
                auto ser_col = ColumnString::create();
                ser_col->reserve(ptr->size());
                VectorBufferWriter buffer_writer(*ser_col.get());
                std::vector<string> data;
                data.push_back("column: " + source_column->get_name() + " with check size: " +
                               std::to_string(*cl) + " with ptr: " + std::to_string(ptr->size()));
                for (size_t j = 0; j < ptr->size(); ++j) {
                    if (auto st = serders[i]->serialize_one_cell_to_json(*ptr, j, buffer_writer,
                                                                         option);
                        !st) {
                        std::cerr << "Failed to serialize column " << i << " at row " << j
                                  << std::endl;
                        break;
                    }
                    buffer_writer.commit();
                    std::string actual_str_value = ser_col->get_data_at(j).to_string();
                    data.push_back(actual_str_value);
                }
            }
        }
        check_res_file("shrink", res);
    }
    //
    //
    //cut or expand inplace. this would be moved, only the return value is available.
    //virtual void
    //reserve (size_t)
    static void assert_reserve_callback(MutableColumns& load_cols, DataTypeSerDeSPtrs serders) {
        // Create an empty column to verify `reserve` functionality
        // check reserve with different size
        // size_t(-1) 会导致pod_array 溢出: Check failed: false Amount of memory requested to allocate is more than allowed
        std::vector<std::vector<string>> res;
        auto option = DataTypeSerDe::FormatOptions();
        std::vector<size_t> check_length = {0, 1, 10, 100, 1000, 10000, 100000};
        for (auto cl = check_length.begin(); cl < check_length.end(); ++cl) {
            for (size_t i = 0; i < load_cols.size(); ++i) {
                auto assert_origin_size = load_cols[i]->size();
                auto& source_column = load_cols[i];
                std::cout << "now we are in reserve column : " << load_cols[i]->get_name()
                          << " with check length: " << *cl
                          << " for column size : " << source_column->size() << std::endl;
                source_column->reserve(*cl);
                // check size no changed after reserve
                EXPECT_EQ(source_column->size(), assert_origin_size);
                // check after reserve with assert_res
                auto ser_col = ColumnString::create();
                ser_col->reserve(source_column->size());
                VectorBufferWriter buffer_writer(*ser_col.get());
                std::vector<string> data;
                data.push_back("column: " + source_column->get_name() +
                               " with check size: " + std::to_string(*cl) +
                               " with ptr: " + std::to_string(source_column->size()));
                for (size_t j = 0; j < source_column->size(); ++j) {
                    if (auto st = serders[i]->serialize_one_cell_to_json(*source_column, j,
                                                                         buffer_writer, option);
                        !st) {
                        std::cerr << "Failed to serialize column " << i << " at row " << j
                                  << std::endl;
                        break;
                    }
                    buffer_writer.commit();
                    std::string actual_str_value = ser_col->get_data_at(j).to_string();
                    data.push_back(actual_str_value);
                }
                res.push_back(data);
            }
        }
        check_res_file("reserve", res);
    }

    //virtual void
    //resize (size_t) means we should really resize the column, include the all sub columns, like data column in column array
    static void assert_resize_callback(MutableColumns& load_cols, DataTypeSerDeSPtrs serders) {
        // Create an empty column to verify `resize` functionality
        // check resize with different size
        // size_t(-1) 会导致pod_array 溢出: Check failed: false Amount of memory requested to allocate is more than allowed
        std::vector<std::vector<string>> res;
        auto option = DataTypeSerDe::FormatOptions();
        std::vector<size_t> check_length = {0, 1, 10, 100, 1000, 10000, 100000};
        for (auto cl = check_length.begin(); cl < check_length.end(); ++cl) {
            for (size_t i = 0; i < load_cols.size(); ++i) {
                auto& source_column = load_cols[i];
                std::cout << "now we are in resize column : " << load_cols[i]->get_name()
                          << " with check length: " << *cl
                          << " for column size : " << source_column->size() << std::endl;
                source_column->resize(*cl);
                // check size
                EXPECT_EQ(source_column->size(), *cl);
                // check after resize with assert_res
                auto ser_col = ColumnString::create();
                ser_col->reserve(source_column->size());
                VectorBufferWriter buffer_writer(*ser_col.get());
                std::vector<string> data;
                data.push_back("column: " + source_column->get_name() +
                               " with check size: " + std::to_string(*cl) +
                               " with ptr: " + std::to_string(source_column->size()));
                for (size_t j = 0; j < source_column->size(); ++j) {
                    if (auto st = serders[i]->serialize_one_cell_to_json(*source_column, j,
                                                                         buffer_writer, option);
                        !st) {
                        std::cerr << "Failed to serialize column " << i << " at row " << j
                                  << std::endl;
                        break;
                    }
                    buffer_writer.commit();
                    std::string actual_str_value = ser_col->get_data_at(j).to_string();
                    data.push_back(actual_str_value);
                }
                res.push_back(data);
            }
        }
        check_res_file("resize", res);
    }

    //virtual Ptr
    //replicate (const Offsets &offsets)
    // 1. used in ColumnConst.convert_to_full_column,
    //   we should make a situation that the column is not full column, and then we can use replicate to make it full column
    // 2. used in some agg calculate
    static void assert_replicate_callback(MutableColumns& load_cols, DataTypeSerDeSPtrs serders) {
        // Create an empty column to verify `replicate` functionality
        // check replicate with different offsets
        // size_t(-1) 会导致pod_array 溢出: Check failed: false Amount of memory requested to allocate is more than allowed
        std::vector<std::vector<string>> res;
        auto option = DataTypeSerDe::FormatOptions();
        std::vector<size_t> check_length = {0, 1, 10, 100, 1000, 10000, 100000};
        //       std::vector<size_t> check_length = {10, 9};
        //       size_t sum = std::reduce(check_length.begin(), check_length.end(), 0, std::plus<size_t>());
        IColumn::Offsets offsets;
        for (size_t i = 0; i < check_length.size(); i++) {
            offsets.push_back(check_length[i]);
        }
        for (size_t i = 0; i < load_cols.size(); ++i) {
            //               auto origin_size = load_cols[0]->size();
            // here will heap_use_after_free
            //               ColumnConst* const_col = ColumnConst::create(load_cols[i]->clone_resized(1), *cl);
            if (load_cols[i]->size() != check_length.size()) {
                EXPECT_ANY_THROW(load_cols[i]->replicate(offsets));
            }
            auto source_column = load_cols[i]->shrink(check_length.size());
            std::cout << "now we are in replicate column : " << load_cols[i]->get_name()
                      << " for column size : " << source_column->size() << std::endl;
            //               auto ptr = const_col->convert_to_full_column();
            // here will return different ptr
            auto ptr = source_column->replicate(offsets);
            // check ptr
            EXPECT_NE(ptr.get(), source_column.get());
            // check after replicate with assert_res
            auto ser_col = ColumnString::create();
            ser_col->reserve(ptr->size());
            VectorBufferWriter buffer_writer(*ser_col.get());
            std::vector<string> data;
            data.push_back("column: " + source_column->get_name() +
                           " with generate col size: " + std::to_string(ptr->size()));
            for (size_t j = 0; j < ptr->size(); ++j) {
                // check size
                if (auto st =
                            serders[i]->serialize_one_cell_to_json(*ptr, j, buffer_writer, option);
                    !st) {
                    std::cerr << "Failed to serialize column " << i << " at row " << j << std::endl;
                    break;
                }
                buffer_writer.commit();
                std::string actual_str_value = ser_col->get_data_at(j).to_string();
                data.push_back(actual_str_value);
            }
            res.push_back(data);
        }
        check_res_file("replicate", res);
    }
    //virtual void
    //for_each_subcolumn (ColumnCallback)
    //virtual void
    //replace_column_data (const IColumn &, size_t row, size_t self_row=0) used in BlockReader(VerticalBlockReader)::_copy_agg_data() for agg value data
    // the passed column must be non-variable length column: like columnVector...
    static void assert_replace_column_data_callback(MutableColumns& load_cols,
                                                    DataTypeSerDeSPtrs serders) {
        // Create an empty column to verify `replace_column_data` functionality
        // check replace_column_data with different row and self_row
        std::vector<std::vector<string>> res;
        auto option = DataTypeSerDe::FormatOptions();
        std::vector<size_t> check_length = {0, 1, 7, 10, 100, 1000};
        for (auto cl = check_length.begin(); cl < check_length.end(); ++cl) {
            for (size_t i = 0; i < load_cols.size(); ++i) {
                if (load_cols[i]->is_variable_length()) {
                    throw std::runtime_error(
                            "replace_column_data only support non-variable length column");
                }
                auto& source_column = load_cols[i];
                if (*cl > source_column->size()) {
                    // if replace row is bigger than the source column size here meet pod coredump
                    continue;
                }
                std::cout << "now we are in replace_column_data column : "
                          << load_cols[i]->get_name() << " with check length: " << *cl
                          << " for column size : " << source_column->size() << std::endl;
                source_column->replace_column_data(*source_column, *cl);
                // check after replace_column_data: the first data is same with the data in source column's *cl row
                auto ser_col = ColumnString::create();
                ser_col->reserve(source_column->size());
                VectorBufferWriter buffer_writer(*ser_col.get());
                std::vector<string> data;
                data.push_back("column: " + source_column->get_name() +
                               " with check size: " + std::to_string(*cl) +
                               " with ptr: " + std::to_string(source_column->size()));
                for (size_t j = 0; j < source_column->size(); ++j) {
                    if (auto st = serders[i]->serialize_one_cell_to_json(*source_column, j,
                                                                         buffer_writer, option);
                        !st) {
                        std::cerr << "Failed to serialize column " << i << " at row " << j
                                  << std::endl;
                        break;
                    }
                    buffer_writer.commit();
                    std::string actual_str_value = ser_col->get_data_at(j).to_string();
                    data.push_back(actual_str_value);
                }
                res.push_back(data);
            }
        }
        check_res_file("replace_column_data", res);
    }

    //
    //virtual void
    //replace_column_null_data (const uint8_t *__restrict null_map)
    // which is only for column-vector/decimal with null data, like columnNullable<columnDecimal>, so other column will do nothing
    // some situation: we just calculate the decimal column data, then set the null_map. when we look at null row, maybe here meet an overflow decimal-value or just a random value but nullmap[row] is true
    // so we should make nullmap[row] = 1 but data[row] is default value for this kind of column
    static void assert_replace_column_null_data_callback(MutableColumns& load_cols,
                                                         DataTypeSerDeSPtrs serders) {
        // Create an empty column to verify `replace_column_null_data` functionality
        // check replace_column_null_data with different null_map
        std::vector<std::vector<string>> res;
        auto option = DataTypeSerDe::FormatOptions();
        const NullMap null_map = {1, 1, 0, 1};
        for (size_t i = 0; i < load_cols.size(); ++i) {
            auto& source_column = load_cols[i];
            std::cout << "now we are in replace_column_null_data column : "
                      << load_cols[i]->get_name() << " for column size : " << source_column->size()
                      << "with nullmap" << null_map.data() << std::endl;
            source_column->replace_column_null_data(null_map.data());

            // check after replace_column_null_data: 1 in nullmap present the load cols data is null and data should be default value
            auto ser_col = ColumnString::create();
            ser_col->reserve(source_column->size());
            VectorBufferWriter buffer_writer(*ser_col.get());
            std::vector<string> data;
            data.push_back("column: " + source_column->get_name() +
                           " with nullmap: " + std::to_string(*null_map.data()) +
                           " with ptr: " + std::to_string(source_column->size()));
            for (size_t j = 0; j < source_column->size(); ++j) {
                if (auto st = serders[i]->serialize_one_cell_to_json(*source_column, j,
                                                                     buffer_writer, option);
                    !st) {
                    std::cerr << "Failed to serialize column " << i << " at row " << j << std::endl;
                    break;
                }
                buffer_writer.commit();
                std::string actual_str_value = ser_col->get_data_at(j).to_string();
                data.push_back(actual_str_value);
            }
            res.push_back(data);
        }
        check_res_file("replace_column_null_data", res);
    }

    //
    //virtual void
    //append_data_by_selector (MutablePtr &res, const Selector &selector, size_t begin, size_t end) const =0
    static void assert_append_data_by_selector_callback(MutableColumns& load_cols,
                                                        DataTypeSerDeSPtrs serders) {
        // Create an empty column to verify `append_data_by_selector` functionality
        // check append_data_by_selector with different selector
        std::vector<std::vector<string>> res;
        auto option = DataTypeSerDe::FormatOptions();
        for (size_t i = 0; i < load_cols.size(); ++i) {
            auto& source_column = load_cols[i];
            MutableColumnPtr res_col = source_column->clone_empty();
            // selector is range for the column, contain values from 0 to num_columns - 1.
            // selector size should bigger than begin and end ,
            // because selector[i], i in range(begin,end),  Make a DCHECK for this
            const ColumnArray::Selector selector = {1, 2, 3, 0};
            std::cout << "now we are in append_data_by_selector column : "
                      << load_cols[i]->get_name() << " for column size : " << source_column->size()
                      << " with selector size: " << selector.size() << std::endl;
            source_column->append_data_by_selector(res_col, selector, 0, 4);
            // check after append_data_by_selector: 1 in selector present the load cols data is selected and data should be default value
            auto ser_col = ColumnString::create();
            ser_col->reserve(res_col->size());
            VectorBufferWriter buffer_writer(*ser_col.get());
            std::vector<string> data;
            data.push_back("column: " + source_column->get_name() +
                           " with selector size : " + std::to_string(selector.size()) +
                           " with ptr: " + std::to_string(res_col->size()));
            for (size_t j = 0; j < res_col->size(); ++j) {
                if (auto st = serders[i]->serialize_one_cell_to_json(*res_col, j, buffer_writer,
                                                                     option);
                    !st) {
                    std::cerr << "Failed to serialize column " << i << " at row " << j << std::endl;
                    break;
                }
                buffer_writer.commit();
                std::string actual_str_value = ser_col->get_data_at(j).to_string();
                data.push_back(actual_str_value);
            }
            res.push_back(data);
        }
        check_res_file("append_data_by_selector", res);
    }

    //
    //virtual bool
    //structure_equals (const IColumn &) const
    //void
    //copy_date_types (const IColumn &col)
    //String
    //dump_structure () const
    //MutablePtr
    //mutate () const &&
    //virtual void
    //clear ()=0
    //
    //Clear data of column, just like vector clear.

    // Column Calculate Interface: filter, compare, permute, sort
    // filter (const Filter &filt, ssize_t result_size_hint) const =0 with a result_size_hint to pass, but we should make sure the result_size_hint is not bigger than the source column size
    //  Filter is a array contains 0 or 1 to present the row is selected or not, so it should be the same size with the source column
    static void assert_filter_callback(MutableColumns& load_cols, DataTypeSerDeSPtrs serders) {
        // Create an empty column to verify `filter` functionality
        // check filter with different filter
        std::vector<std::vector<string>> res;
        auto option = DataTypeSerDe::FormatOptions();

        for (size_t i = 0; i < load_cols.size(); ++i) {
            auto& source_column = load_cols[i];
            auto source_size = source_column->size();
            const ColumnArray::Filter all_filtered(source_size, 0);
            const ColumnArray::Filter no_filtered(source_size, 1);
            // invalid data -1 will also make data without be filtered ??
            ColumnArray::Filter invalid_filter(source_size - 1, 1);
            invalid_filter.emplace_back(-1);
            std::vector<string> data;
            std::cout << "now we are in filter column : " << load_cols[i]->get_name()
                      << " for column size : " << source_column->size() << std::endl;
            {
                auto ptr = source_column->filter(all_filtered, source_column->size());
                // check filter res
                auto ser_col = ColumnString::create();
                ser_col->reserve(ptr->size());
                VectorBufferWriter buffer_writer(*ser_col.get());
                data.push_back("column: " + source_column->get_name() +
                               " with all filtered with ptr: " + std::to_string(ptr->size()));
                for (size_t j = 0; j < ptr->size(); ++j) {
                    if (auto st = serders[i]->serialize_one_cell_to_json(*ptr, j, buffer_writer,
                                                                         option);
                        !st) {
                        std::cerr << "Failed to serialize column " << i << " at row " << j
                                  << std::endl;
                        break;
                    }
                    buffer_writer.commit();
                    std::string actual_str_value = ser_col->get_data_at(j).to_string();
                    data.push_back(actual_str_value);
                }
                res.push_back(data);
            }

            {
                auto ptr = source_column->filter(no_filtered, source_column->size());
                // check filter res
                auto ser_col = ColumnString::create();
                ser_col->reserve(ptr->size());
                VectorBufferWriter buffer_writer(*ser_col.get());
                data.clear();
                data.push_back("column: " + source_column->get_name() +
                               " with no filtered with ptr: " + std::to_string(ptr->size()));
                for (size_t j = 0; j < ptr->size(); ++j) {
                    if (auto st = serders[i]->serialize_one_cell_to_json(*ptr, j, buffer_writer,
                                                                         option);
                        !st) {
                        std::cerr << "Failed to serialize column " << i << " at row " << j
                                  << std::endl;
                        break;
                    }
                    buffer_writer.commit();
                    std::string actual_str_value = ser_col->get_data_at(j).to_string();
                    data.push_back(actual_str_value);
                }
                res.push_back(data);
            }
            {
                // check filter with invalid filter
                auto ptr = source_column->filter(invalid_filter, source_column->size());
                // check filter res
                auto ser_col = ColumnString::create();
                ser_col->reserve(ptr->size());
                VectorBufferWriter buffer_writer(*ser_col.get());
                data.clear();
                data.push_back("column: " + source_column->get_name() +
                               " with invalid filtered with ptr: " + std::to_string(ptr->size()));
                for (size_t j = 0; j < ptr->size(); ++j) {
                    if (auto st = serders[i]->serialize_one_cell_to_json(*ptr, j, buffer_writer,
                                                                         option);
                        !st) {
                        std::cerr << "Failed to serialize column " << i << " at row " << j
                                  << std::endl;
                        break;
                    }
                    buffer_writer.commit();
                    std::string actual_str_value = ser_col->get_data_at(j).to_string();
                    data.push_back(actual_str_value);
                }
                res.push_back(data);
            }
        }
        check_res_file("filter", res);
    }

    // Compare

    // Column Hash Interfaces:
    // update_hashes_with_value (size_t, size_t, Hashes &hashes) const : which inner just use xxhash for column data
    static void assert_update_hashes_with_value_callback(MutableColumns& load_cols,
                                                         DataTypeSerDeSPtrs serders) {
        // Create an empty column to verify `update_hashes_with_value` functionality
        // check update_hashes_with_value with different hashes
        std::vector<std::vector<string>> res;
        auto option = DataTypeSerDe::FormatOptions();

        for (size_t i = 0; i < load_cols.size(); ++i) {
            auto& source_column = load_cols[i];
            std::vector<uint64_t> xx_hash_vals(source_column->size());
            std::cout << "now we are in update_hashes_with_value column : "
                      << load_cols[i]->get_name() << " for column size : " << source_column->size()
                      << std::endl;
            auto* __restrict xx_hashes = xx_hash_vals.data();
            EXPECT_NO_FATAL_FAILURE(source_column->update_hashes_with_value(xx_hashes));
            // check after update_hashes_with_value: 1 in selector present the load cols data is selected and data should be default value
            auto ser_col = ColumnString::create();
            ser_col->reserve(source_column->size());
            VectorBufferWriter buffer_writer(*ser_col.get());
            std::vector<string> data;
            data.push_back("column: " + source_column->get_name() +
                           " with hashes: " + std::to_string(*xx_hashes) +
                           " with ptr: " + std::to_string(source_column->size()));
            res.push_back(data);
        }
        check_res_file("update_hashes_with_value", res);
    }

    //virtual void
    //update_hashes (size_t, size_t, Hashes &hashes) const
    static void assert_update_crc_hashes_callback(MutableColumns& load_cols,
                                                  DataTypeSerDeSPtrs serders,
                                                  std::vector<PrimitiveType> pts) {
        // Create an empty column to verify `update_hashes` functionality
        // check update_hashes with different hashes
        std::vector<std::vector<string>> res;
        auto option = DataTypeSerDe::FormatOptions();
        for (size_t i = 0; i < load_cols.size(); ++i) {
            auto& source_column = load_cols[i];
            std::vector<uint32_t> crc_hash_vals(source_column->size());
            std::cout << "now we are in update_hashes column : " << load_cols[i]->get_name()
                      << " for column size : " << source_column->size() << std::endl;
            EXPECT_NO_FATAL_FAILURE(source_column->update_crcs_with_value(
                    crc_hash_vals.data(), pts[i], source_column->size()));
            // check after update_hashes: 1 in selector present the load cols data is selected and data should be default value
            auto ser_col = ColumnString::create();
            ser_col->reserve(source_column->size());
            VectorBufferWriter buffer_writer(*ser_col.get());
            std::vector<string> data;
            data.push_back("column: " + source_column->get_name() +
                           " with hashes: " + std::to_string(*crc_hash_vals.data()) +
                           " with ptr: " + std::to_string(source_column->size()));
            res.push_back(data);
        }
        check_res_file("update_crcs_hashes", res);
    }

    // column size changed calculation:
    //  size, reserve, resize, empty, byte_size, allocated_bytes, clone_resized, get_shrinked_column
    //  cut(LIMIT operation), shrink

    // get_shrinked_column should only happened in char-type column or nested char-type column,
    // other column just return the origin column without any data changed, so check file content should be the same as the origin column
    //  just shrink the end zeros for char-type column which happened in segmentIterator
    //    eg. column_desc: char(6), insert into char(3), the char(3) will padding the 3 zeros at the end for writing to disk.
    //       but we select should just print the char(3) without the padding zeros
    //  limit and topN operation will trigger this function call
    void shrink_padding_chars_callback(MutableColumns& load_cols, DataTypeSerDeSPtrs serders) {
        auto option = DataTypeSerDe::FormatOptions();
        std::vector<std::vector<string>> res;
        for (size_t i = 0; i < load_cols.size(); i++) {
            auto& source_column = load_cols[i];
            std::cout << "now we are in shrink_padding_chars column : " << load_cols[i]->get_name()
                      << " for column size : " << source_column->size() << std::endl;
            source_column->shrink_padding_chars();
            // check after get_shrinked_column: 1 in selector present the load cols data is selected and data should be default value
            auto ser_col = ColumnString::create();
            ser_col->reserve(source_column->size());
            VectorBufferWriter buffer_writer(*ser_col.get());
            std::vector<string> data;
            data.push_back("column: " + source_column->get_name() +
                           " with shrinked column size: " + std::to_string(source_column->size()));
            for (size_t j = 0; j < source_column->size(); ++j) {
                if (auto st = serders[i]->serialize_one_cell_to_json(*source_column, j,
                                                                     buffer_writer, option);
                    !st) {
                    std::cerr << "Failed to serialize column " << i << " at row " << j << std::endl;
                    break;
                }
                buffer_writer.commit();
                std::string actual_str_value = ser_col->get_data_at(j).to_string();
                data.push_back(actual_str_value);
            }
            res.push_back(data);
        }
        check_res_file("shrink_padding_chars", res);
    }

    void assert_size_eq(MutableColumnPtr col, size_t expect_size) {
        EXPECT_EQ(col->size(), expect_size);
    }

    // empty just use size() == 0 to impl as default behavior
    void assert_empty(MutableColumnPtr col) { EXPECT_EQ(col->size(), 0); }

    // reserve, resize, byte_size, allocated_bytes, clone_resized, get_shrinked_column
    void assert_reserve_size(MutableColumnPtr col, size_t reserve_size, size_t expect_size) {
        col->reserve(reserve_size);
        EXPECT_EQ(col->size(), expect_size);
    }

    //  cut(LIMIT operation) will cut the column with the given from and to, and return the new column
    //  notice return column is clone from origin column
    void assert_cut(MutableColumnPtr col, size_t from, size_t to) {
        auto ori = col->size();
        auto ptr = col->cut(from, to);
        EXPECT_EQ(ptr->size(), to - from);
        EXPECT_EQ(col->size(), ori);
    }

    // shrink is cut/append the column with the given size, which called from Block::set_num_rows
    // and some Operator may call this set_num_rows to make rows satisfied, like limit operation
    // but different from cut behavior which
    // return column is mutate from origin column
    void assert_shrink(MutableColumnPtr col, size_t shrink_size) {
        auto ptr = col->shrink(shrink_size);
        EXPECT_EQ(ptr->size(), shrink_size);
        EXPECT_EQ(col->size(), shrink_size);
    }

    // resize has fixed-column implementation and variable-column implementation
    // like string column, the resize will resize the offsets column but not the data column (because it doesn't matter the size of data column, all operation for string column is based on the offsets column)
    // like vector column, the resize will resize the data column
    // like array column, the resize will resize the offsets column and the data column (which in creator we have check staff for the size of data column is the same as the size of offsets column)
    void assert_resize(MutableColumnPtr col, size_t expect_size) {
        col->resize(expect_size);
        EXPECT_EQ(col->size(), expect_size);
    }

    // replicate is clone with new column from the origin column, always from ColumnConst to expand the column
    void assert_replicate(MutableColumnPtr col, IColumn::Offsets& offsets) {
        auto new_col = col->replicate(offsets);
        EXPECT_EQ(new_col->size(), offsets.back());
    }

    // byte size is just appriximate size of the column
    //  as fixed column type, like column_vector, the byte size is sizeof(columnType) * size()
    //  as variable column type, like column_string, the byte size is sum of chars size() and offsets size * sizeof(offsetType)
    void assert_byte_size(MutableColumnPtr col, size_t expect_size) {
        EXPECT_EQ(col->byte_size(), expect_size);
    }

    // allocated bytes is the real size of the column
    void assert_allocated_bytes(MutableColumnPtr col, size_t expect_size) {
        EXPECT_EQ(col->allocated_bytes(), expect_size);
    }

    // clone_resized will clone the column and cut/append to the new column with the size of the original column
    void assert_clone_resized(MutableColumnPtr col, size_t expect_size) {
        auto new_col = col->clone_resized(expect_size);
        EXPECT_EQ(new_col->size(), expect_size);
    }

    //serialize and deserialize which usually used in AGG function:
    //  serialize_value_into_arena, deserialize_and_insert_from_arena (called by AggregateFunctionDistinctMultipleGenericData, group_array_intersect, nested-types serder like: DataTypeArraySerDe::write_one_cell_to_jsonb)
    void ser_deserialize_with_arena_impl(MutableColumns& columns, const DataTypes& data_types) {
        size_t rows = columns[0]->size();
        for (auto& column : columns) {
            if (column->size() > rows) {
                std::cerr << "Column size mismatch: " << column->size() << " vs " << rows
                          << std::endl;
                column->pop_back(rows - column->size());
            } else if (column->size() < rows) {
                std::cerr << "Column size mismatch: " << column->size() << " vs " << rows
                          << std::endl;
                column->insert_many_defaults(rows - column->size());
            }
        }
        /// check serialization is reversible.
        Arena arena;
        MutableColumns argument_columns(data_types.size());
        const char* pos = nullptr;
        StringRef key(pos, 0);
        {
            // serialize
            for (size_t r = 0; r < columns[0]->size(); ++r) {
                for (size_t i = 0; i < columns.size(); ++i) {
                    auto cur_ref = columns[i]->serialize_value_into_arena(r, arena, pos);
                    key.data = cur_ref.data - key.size;
                    key.size += cur_ref.size;
                    //                    printColumn(*columns[i], *data_types[i]);
                }
            }
        }

        {
            // deserialize
            for (size_t i = 0; i < data_types.size(); ++i) {
                argument_columns[i] = data_types[i]->create_column();
            }
            const char* begin = key.data;
            for (size_t r = 0; r < columns[0]->size(); ++r) {
                for (size_t i = 0; i < argument_columns.size(); ++i) {
                    begin = argument_columns[i]->deserialize_and_insert_from_arena(begin);
                    //                    printColumn(*argument_columns[i], *data_types[i]);
                }
            }
        }
        {
            // check column data equal
            for (size_t i = 0; i < columns.size(); ++i) {
                EXPECT_EQ(columns[i]->size(), argument_columns[i]->size());
                checkColumn(*columns[i], *argument_columns[i], *data_types[i], columns[0]->size());
            }
        }
    }

    //  serialize_vec, deserialize_vec (called by MethodSerialized.init_serialized_keys), here are some scenarios:
    //    1/ AggState: groupby key column which be serialized to hash-table key, eg.AggLocalState::_emplace_into_hash_table
    //    2/ JoinState: hash join key column which be serialized to hash-table key, or probe column which be serialized to hash-table key, eg.ProcessHashTableBuild, ProcessHashTableProbe<JoinOpType>::probe_side_output_column
    //  serialize_vec_with_null_map, deserialize_vec_with_null_map which only called by ColumnNullable serialize_vec and deserialize_vec, and derived by other columns
    //  get_max_row_byte_size used in MethodSerialized which calculating the memory size for vectorized serialization of aggregation keys.
    void ser_deser_vec(MutableColumns& columns, DataTypes dataTypes) {
        // step1. make input_keys with given rows for a block
        size_t rows = columns[0]->size();
        std::vector<StringRef> input_keys;
        input_keys.resize(rows);
        MutableColumns check_columns(columns.size());
        int c = 0;
        for (auto& column : columns) {
            if (column->size() > rows) {
                std::cerr << "Column size mismatch: " << column->size() << " vs " << rows
                          << std::endl;
                column->pop_back(rows - column->size());
            } else if (column->size() < rows) {
                std::cerr << "Column size mismatch: " << column->size() << " vs " << rows
                          << std::endl;
                column->insert_many_defaults(rows - column->size());
            }
            check_columns[c] = column->clone_empty();
            ++c;
        }

        // step2. calculate the needed memory size for vectorized serialization of aggregation keys
        size_t max_one_row_byte_size = 0;
        for (const auto& column : columns) {
            max_one_row_byte_size += column->get_max_row_byte_size();
        }
        size_t memory_size = max_one_row_byte_size * rows;
        Arena arena(memory_size);
        auto* serialized_key_buffer = reinterpret_cast<uint8_t*>(arena.alloc(memory_size));

        // serialize the keys into arena
        {
            // step3. serialize the keys into arena
            for (size_t i = 0; i < rows; ++i) {
                input_keys[i].data =
                        reinterpret_cast<char*>(serialized_key_buffer + i * max_one_row_byte_size);
                input_keys[i].size = 0;
            }
            std::cout << "max_one_row_byte_size : " << max_one_row_byte_size << std::endl;
            for (const auto& column : columns) {
                std::cout << "now serialize_vec for column:" << column->get_name()
                          << " with column size: " << column->size() << std::endl;
                column->serialize_vec(input_keys, rows, max_one_row_byte_size);
            }
        }
        // deserialize the keys from arena into columns
        {
            // step4. deserialize the keys from arena into columns
            for (auto& column : check_columns) {
                column->deserialize_vec(input_keys, rows);
            }
        }
        // check the deserialized columns
        {
            for (size_t i = 0; i < columns.size(); ++i) {
                EXPECT_EQ(columns[i]->size(), check_columns[i]->size());
                EXPECT_EQ(columns[i]->size(), check_columns[i]->size());
                checkColumn(*columns[i], *check_columns[i], *dataTypes[i], rows);
            }
        }
    }

    PaddedPODArray<UInt8> create_filter(std::vector<uint8_t> data) {
        PaddedPODArray<UInt8> filter;
        filter.insert(filter.end(), data.begin(), data.end());
        return filter;
    }

    // filter calculation:
    //  filter (called in Block::filter_block_internal to filter data with filter filled with 0or1 array, like: [0,1,0,1])
    //   used in join to filter next block by row_ids, and filter column by row_ids in first read which called in SegmentIterator
    void assert_filter(MutableColumnPtr col, std::vector<uint8_t> filter, size_t expect_size) {
        EXPECT_EQ(col->size(), filter.size());
        auto filted_col = col->filter(create_filter(filter), expect_size);
        EXPECT_EQ(filted_col->size(), expect_size);
    }

    //  filter_by_selector (called SegmentIterator::copy_column_data_by_selector,
    //  now just used in filter column, according to the selector to
    //  select size of row_ids for column by given column, which only used for predict_column and column_dictionary, column_nullable sometimes in Schema::get_predicate_column_ptr() also will return)
    void assert_filter_by_selector(vectorized::IColumn::MutablePtr col,
                                   std::vector<uint16_t> selector, DataTypeSerDeSPtr serder,
                                   MutableColumnPtr should_sel_col, size_t expect_size) {
        // for every data type should assert behavior in own UT case
        DataTypeSerDe::FormatOptions option;
        col->clear();
        col->insert_many_defaults(should_sel_col->size());
        std::cout << "col size:" << col->size() << std::endl;
        Status st = col->filter_by_selector(selector.data(), expect_size, should_sel_col.get());
        EXPECT_EQ(st, Status::OK());
        std::vector<std::vector<string>> res;
        std::vector<string> data;
        auto ser_col = ColumnString::create();
        ser_col->reserve(should_sel_col->size());
        VectorBufferWriter buffer_writer(*ser_col.get());
        data.push_back("column: " + col->get_name() +
                       " with selector: " + std::to_string(selector.size()) +
                       " with ptr: " + std::to_string(should_sel_col->size()));
        for (size_t j = 0; j < should_sel_col->size(); ++j) {
            if (auto ret = serder->serialize_one_cell_to_json(*should_sel_col, j, buffer_writer,
                                                              option);
                !ret) {
                std::cerr << "Failed to serialize column "
                          << " at row " << j << std::endl;
                break;
            }
            buffer_writer.commit();
            std::string actual_str_value = ser_col->get_data_at(j).to_string();
            data.push_back(actual_str_value);
        }
        res.push_back(data);
        check_res_file("filter_by_selector-" + col->get_name(), res);
    }

    void assert_permutations_with_limit(const IColumn::Permutation& lhs,
                                        const IColumn::Permutation& rhs, size_t limit) {
        std::cout << "lhs size: " << lhs.size() << " rhs size: " << rhs.size()
                  << " limit: " << limit << std::endl;
        if (limit == 0) {
            limit = lhs.size();
        }

        for (size_t i = 0; i < limit; ++i) {
            ASSERT_EQ(lhs[i], rhs[i]) << "i: " << i << "limit: " << limit;
        }
    }

    // this function helps to check sort permutation behavior for column
    void stable_get_column_permutation(const IColumn& column, bool ascending, size_t limit,
                                       int nan_direction_hint,
                                       IColumn::Permutation& out_permutation) {
        (void)(limit);

        size_t size = column.size();
        out_permutation.resize(size);
        std::iota(out_permutation.begin(), out_permutation.end(),
                  IColumn::Permutation::value_type(0));

        std::stable_sort(out_permutation.begin(), out_permutation.end(),
                         [&](size_t lhs, size_t rhs) {
                             int res = column.compare_at(lhs, rhs, column, nan_direction_hint);
                             // to check element in column is sorted or not
                             if (ascending)
                                 return res < 0;
                             else
                                 return res > 0;
                         });
    }

    // sort calculation: (which used in sort_block )
    //   get_permutation
    // this function helps check permutation result with sort & limit
    //  by given ColumnValueGetter which how to generate a column value
    void assert_column_permutations(vectorized::IColumn& column, DataTypePtr dataType) {
        IColumn::Permutation actual_permutation;
        IColumn::Permutation expected_permutation;

        static constexpr size_t limit_parts = 4;
        printColumn(column, *dataType);

        size_t column_size = column.size();
        size_t column_limit_part = (column_size / limit_parts) + 1;
        std::cout << "column size: " << column_size << std::endl;
        for (size_t limit = 0; limit < column_size; limit += column_limit_part) {
            assert_column_permutation(column, true, limit, -1, actual_permutation,
                                      expected_permutation);
            assert_column_permutation(column, true, limit, 1, actual_permutation,
                                      expected_permutation);

            assert_column_permutation(column, false, limit, -1, actual_permutation,
                                      expected_permutation);
            assert_column_permutation(column, false, limit, 1, actual_permutation,
                                      expected_permutation);
        }
    }

    // sort calculation: (which used in sort_block )
    //    get_permutation means sort data in Column as sort order
    //    limit should be set to limit the sort result
    //    nan_direction_hint deal with null|NaN value
    void assert_column_permutation(const IColumn& column, bool ascending, size_t limit,
                                   int nan_direction_hint, IColumn::Permutation& actual_permutation,
                                   IColumn::Permutation& expected_permutation) {
        std::cout << "assertColumnPermutation start, limit: " << limit
                  << " ascending: " << ascending << " nan_direction_hint: " << nan_direction_hint
                  << " column size: " << column.size()
                  << " actual_permutation size: " << actual_permutation.size()
                  << " expected_permutation size: " << expected_permutation.size() << std::endl;
        // step1. get expect permutation as stabled sort
        stable_get_column_permutation(column, ascending, limit, nan_direction_hint,
                                      expected_permutation);
        std::cout << "expected_permutation size: " << expected_permutation.size() << std::endl;
        for (size_t i = 0; i < expected_permutation.size(); i++) {
            std::cout << "expected_permutation: " << expected_permutation[i] << std::endl;
        }
        // step2. get permutation by column
        column.get_permutation(!ascending, limit, nan_direction_hint, actual_permutation);

        std::cout << "actual_permutation size: " << actual_permutation.size() << std::endl;
        for (size_t i = 0; i < actual_permutation.size(); i++) {
            std::cout << "actual_permutation: " << actual_permutation[i] << std::endl;
        }

        if (limit == 0) {
            limit = actual_permutation.size();
        }

        // step3. check the permutation result
        assert_permutations_with_limit(actual_permutation, expected_permutation, limit);
        std::cout << "assertColumnPermutation done" << std::endl;
    }

    //  permute()
    //   1/ Key topN set read_orderby_key_reverse = true; SegmentIterator::next_batch will permute the column by the given permutation(which reverse the rows of current segment)
    //  should check rows with the given permutation
    void assert_permute(MutableColumns& cols, IColumn::Permutation& permutation, size_t num_rows) {
        std::vector<ColumnPtr> res_permuted;
        for (auto& col : cols) {
            res_permuted.emplace_back(col->permute(permutation, num_rows));
        }
        // check the permutation result for rowsize
        size_t res_rows = res_permuted[0]->size();
        for (auto& col : res_permuted) {
            EXPECT_EQ(col->size(), res_rows);
        }
    }
};

} // namespace doris::vectorized