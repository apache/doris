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
#pragma once

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <iostream>
#include <random>
#include <string>

#include "testutil/test_util.h"
#include "util/simd/bits.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_dictionary.h"
#include "vec/columns/column_map.h"
#include "vec/common/cow.h"
#include "vec/core/field.h"
#include "vec/core/sort_block.h"
#include "vec/core/sort_description.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_map.h"

// this test is gonna to be a column test template for all column which should make ut test to coverage the function defined in column (all maybe we need 79 interfaces to be tested)
// for example column_array should test this function:
// size, reserve, resize, empty, byte_size, allocated_bytes, clone_resized,
// get_shrinked_column, filter, filter_by_selector, serialize_vec, deserialize_vec, get_max_row_byte_size
//
namespace doris::vectorized {

static bool gen_check_data_in_assert = true;

template <typename Container>
std::string join_ints(const Container& nums) {
    if (nums.empty()) {
        return "[]";
    }

    std::ostringstream oss;
    oss << "[";
    std::copy(nums.begin(), nums.end() - 1,
              std::ostream_iterator<typename Container::value_type>(oss, ","));
    oss << nums.back() << "]";
    return oss.str();
}

class CommonColumnTest : public ::testing::Test {
protected:
    //// this is very helpful function to check data in column against expected results according different function in assert function
    //// such as run regress tests
    ////  if gen_check_data_in_assert is true, we will generate a file for check data, otherwise we will read the file to check data
    ////  so the key point is we should how we write assert callback function to check data,
    ///   and when check data is generated, we should check result to statisfy the semantic of the function
    static void check_res_file(std::string function_name,
                               std::vector<std::vector<std::string>>& res) {
        std::string filename = "./res_" + function_name + ".csv";
        if (gen_check_data_in_assert) {
            std::ofstream res_file(filename);
            LOG(INFO) << "gen check data: " << res.size() << " with file: " << filename;
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
            LOG(INFO) << "check data: " << res.size() << " with file: " << filename;
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
        Array array1 = {Field::create_field<TYPE_BIGINT>(1), Field::create_field<TYPE_BIGINT>(2),
                        Field::create_field<TYPE_BIGINT>(3)};
        Array array2 = {Field::create_field<TYPE_BIGINT>(4)};
        col_arr->insert(Field::create_field<TYPE_ARRAY>(array1));
        col_arr->insert(Field::create_field<TYPE_ARRAY>(Array()));
        col_arr->insert(Field::create_field<TYPE_ARRAY>(array2));

        col_map = ColumnMap::create(ColumnString::create(), ColumnInt64::create(),
                                    ColumnArray::ColumnOffsets::create());
        Array k1 = {Field::create_field<TYPE_STRING>("a"), Field::create_field<TYPE_STRING>("b"),
                    Field::create_field<TYPE_STRING>("c")};
        Array v1 = {Field::create_field<TYPE_BIGINT>(1), Field::create_field<TYPE_BIGINT>(2),
                    Field::create_field<TYPE_BIGINT>(3)};
        Array k2 = {Field::create_field<TYPE_STRING>("d")};
        Array v2 = {Field::create_field<TYPE_BIGINT>(4)};
        Array a = Array();
        Map map1, map2, map3;
        map1.push_back(Field::create_field<TYPE_ARRAY>(k1));
        map1.push_back(Field::create_field<TYPE_ARRAY>(v1));
        col_map->insert(Field::create_field<TYPE_MAP>(map1));
        map3.push_back(Field::create_field<TYPE_ARRAY>(a));
        map3.push_back(Field::create_field<TYPE_ARRAY>(a));
        col_map->insert(Field::create_field<TYPE_MAP>(map3));
        map2.push_back(Field::create_field<TYPE_ARRAY>(k2));
        map2.push_back(Field::create_field<TYPE_ARRAY>(v2));
        col_map->insert(Field::create_field<TYPE_MAP>(map2));
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
                    LOG(INFO) << "load data from file: " << file_path;
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

    static void ALWAYS_INLINE checkField(const IColumn& col1, const IColumn& col2, size_t idx1,
                                         size_t idx2) {
        Field f1;
        Field f2;
        col1.get(idx1, f1);
        col2.get(idx2, f2);
        EXPECT_EQ(f1, f2) << "idx1: " << idx1 << " idx2: " << idx2 << " col1: " << col1.get_name()
                          << " col2: " << col2.get_name() << " f1: " << f1.get_type_name()
                          << " f2: " << f2.get_type_name();
    }
    static void checkColumn(const IColumn& col1, const IColumn& col2, size_t column_size) {
        for (size_t i = 0; i < column_size; ++i) {
            checkField(col1, col2, i, i);
        }
    }

    void printColumn(const IColumn& column, const IDataType& dataType) {
        LOG(INFO) << "colum: " << column.get_name() << " total size: " << column.size();
        auto serde = dataType.get_serde(0);
        auto serde_col = ColumnString::create();
        auto option = DataTypeSerDe::FormatOptions();
        serde_col->reserve(column.size());
        VectorBufferWriter buffer_writer(*serde_col.get());
        for (size_t i = 0; i < column.size(); ++i) {
            if (auto st = serde->serialize_one_cell_to_json(column, i, buffer_writer, option);
                !st) {
                LOG(ERROR) << "Failed to serialize column at row " << i;
                break;
            }
            buffer_writer.commit();
            LOG(INFO) << serde_col->get_data_at(i).to_string();
        }
    }

    ////////// =================== column data insert interface assert(16) =================== //////////
    // In storage layer such as segment_iterator to call these function
    // insert_many_fix_len_data (const char *pos, size_t num);
    // insert_many_dict_data (const int32_t *data_array, size_t start_index, const StringRef *dict, size_t data_num, uint32_t dict_num=0)
    // insert_many_continuous_binary_data (const char *data, const uint32_t *offsets, const size_t num)
    static void assert_insert_many_fix_len_data(
            MutableColumns& load_cols, DataTypeSerDeSPtrs serders,
            std::function<void(MutableColumns& load_cols, DataTypeSerDeSPtrs serders)>
                    assert_callback) {
        // Create a column to verify `insert_many_fix_len_data` functionality
        assert_callback(load_cols, serders);
    }

    static void assert_insert_many_dict_data(
            MutableColumns& load_cols, DataTypeSerDeSPtrs serders,
            std::function<void(MutableColumns& load_cols, DataTypeSerDeSPtrs serders)>
                    assert_callback) {
        // Create a column to verify `insert_many_dict_data` functionality
        assert_callback(load_cols, serders);
    }

    static void assert_insert_many_continuous_binary_data(
            MutableColumns& load_cols, DataTypeSerDeSPtrs serders,
            std::function<void(MutableColumns& load_cols, DataTypeSerDeSPtrs serders)>
                    assert_callback) {
        // Create a column to verify `insert_many_continuous_binary_data` functionality
        assert_callback(load_cols, serders);
    }

    // only support in column_string: insert_many_strings(const StringRef *data, size_t num) && insert_many_strings_overflow
    static void assert_insert_many_strings(
            MutableColumns& load_cols, DataTypeSerDeSPtrs serders,
            std::function<void(MutableColumns& load_cols, DataTypeSerDeSPtrs serders)>
                    assert_callback) {
        for (auto& col : load_cols) {
            // Create a column to verify `insert_many_strings` functionality
            if (!is_column<ColumnString>(*col)) {
                EXPECT_ANY_THROW(col->insert_many_strings(nullptr, 0));
            } else {
                assert_callback(load_cols, serders);
            }
        }
    }

    static void assert_insert_many_strings_overflow(
            MutableColumns& load_cols, DataTypeSerDeSPtrs serders,
            std::function<void(MutableColumns& load_cols, DataTypeSerDeSPtrs serders)>
                    assert_callback) {
        for (auto& col : load_cols) {
            // Create a column to verify `insert_many_strings_overflow` functionality
            if (!is_column<ColumnString>(*col)) {
                // just expect throw exception as not support
                EXPECT_ANY_THROW(col->insert_many_strings_overflow(nullptr, 0, 0));
            } else {
                assert_callback(load_cols, serders);
            }
        }
    }

    static void assert_insert_from_with_field_callback(const MutableColumnPtr& source_column) {
        auto target_column = source_column->clone_empty();
        for (size_t j = 0; j < source_column->size(); ++j) {
            target_column->insert_from(*source_column, j);
        }
        ASSERT_EQ(target_column->size(), source_column->size());
        checkColumn(*target_column, *source_column, source_column->size());
    };

    static void assert_insert_many_from_with_field_callback(const MutableColumnPtr& source_column) {
        auto src_size = source_column->size();
        std::vector<size_t> insert_vals_count = {0, 3, 10};
        std::vector<size_t> src_data_indices = {0, src_size, src_size - 1, (src_size + 1) >> 1};

        auto test_func = [&](size_t clone_count) {
            size_t actual_clone_count = std::min(clone_count, src_size);
            auto target_column = source_column->clone_resized(actual_clone_count);
            for (auto pos = src_data_indices.begin(); pos < src_data_indices.end(); ++pos) {
                if (*pos >= src_size) {
                    continue;
                }
                for (auto n : insert_vals_count) {
                    target_column->resize(actual_clone_count);
                    target_column->insert_many_from(*source_column, *pos, n);
                    EXPECT_EQ(target_column->size(), actual_clone_count + n);
                    size_t i = 0;
                    for (; i < actual_clone_count; ++i) {
                        // field assert
                        checkField(*target_column, *source_column, i, i);
                    }
                    for (size_t j = 0; j < n; ++j, ++i) {
                        // field assert
                        checkField(*target_column, *source_column, i, *pos);
                    }
                }
            }
        };
        test_func(0);
        test_func(10);
    };

    // assert insert_from
    // Define the custom assert callback function to verify insert_from behavior
    static void assert_insert_from_callback(MutableColumns& load_cols, DataTypeSerDeSPtrs serders) {
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
                    LOG(ERROR) << "Failed to serialize column " << i << " at row " << j;
                    break;
                }
                buffer_writer.commit();
                std::string actual_str_value = ser_col->get_data_at(j).to_string();
                data.push_back(actual_str_value);
            }
            res.push_back(data);
        }
        check_res_file("insert_from", res);
    }

    // insert_from_multi_column (const std::vector< const IColumn * > &srcs, std::vector< size_t > positions)
    // speed up for insert_from interface according to avoid virtual call
    static void assert_insert_from_multi_column_callback(MutableColumns& load_cols,
                                                         DataTypeSerDeSPtrs serders) {
        // Create an empty column to verify `insert_from_multi_column` functionality
        MutableColumns verify_columns;
        for (auto& col : load_cols) {
            verify_columns.push_back(col->clone_empty());
        }
        auto option = DataTypeSerDe::FormatOptions();
        // Insert data from `load_cols` to `verify_columns` using `insert_from_multi_column`
        std::vector<std::vector<std::string>> res;
        for (size_t i = 0; i < load_cols.size(); ++i) {
            size_t si = load_cols[i]->size();
            std::vector<size_t> positions = {0, si >> 1, si - 1};
            auto& source_column = load_cols[i];
            std::vector<const IColumn*> s = {source_column.get(), source_column.get(),
                                             source_column.get()};
            auto& target_column = verify_columns[i];
            target_column->insert_from_multi_column(s, positions);
        }

        // Verify the inserted data matches the expected results in `assert_res`
        for (size_t i = 0; i < load_cols.size(); ++i) {
            auto& target_column = verify_columns[i];
            auto ser_col = ColumnString::create();
            ser_col->reserve(target_column->size());
            VectorBufferWriter buffer_writer(*ser_col.get());
            std::vector<std::string> data;
            for (size_t j = 0; j < target_column->size(); ++j) {
                data.push_back("now assert insert_from_multi_column for column " +
                               target_column->get_name() + " at row " + std::to_string(j));
                if (auto st = serders[i]->serialize_one_cell_to_json(*target_column, j,
                                                                     buffer_writer, option);
                    !st) {
                    LOG(ERROR) << "Failed to serialize column " << i << " at row " << j;
                    break;
                }
                buffer_writer.commit();
                data.push_back(ser_col->get_data_at(j).to_string());
            }
            res.push_back(data);
        }
        check_res_file("insert_from_multi_column", res);
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
                LOG(INFO) << "==== now test insert_range_from with col "
                          << verify_columns[i]->get_name();
                auto& source_column = load_cols[i];
                auto& target_column = verify_columns[i];
                std::vector<size_t> check_start_pos = {0, source_column->size(),
                                                       (source_column->size() + 1) >> 1};
                // size_t(-1) may cause overflow, but here we have compiler to check it
                std::vector<size_t> err_start_pos = {source_column->size() + 1};
                for (auto pos = err_start_pos.begin(); pos < err_start_pos.end(); ++pos) {
                    LOG(INFO) << "error insert_range_from from " << *pos << " with length " << *cl
                              << *pos + *cl << " > " << source_column->size();
                    EXPECT_THROW(target_column->insert_range_from(*source_column, *pos, *cl),
                                 Exception);
                }
                for (auto pos = check_start_pos.begin(); pos < check_start_pos.end(); ++pos) {
                    target_column->clear();
                    LOG(INFO) << "now insert_range_from from " << *pos << " with length " << *cl
                              << " with source size: " << source_column->size();
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
                            LOG(ERROR) << "Failed to serialize column " << i << " at row " << j;
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

    static void assert_insert_indices_from_with_field_callback(
            const MutableColumnPtr& source_column) {
        auto src_size = source_column->size();
        auto target_column = source_column->clone_resized(src_size);
        {
            auto tmp_target_column = target_column->clone_empty();
            std::vector<uint32_t> indices;

            // empty indices array
            tmp_target_column->insert_indices_from(*source_column, indices.data(), indices.data());
            EXPECT_EQ(tmp_target_column->size(), 0);
        }
        auto test_func2 = [&](size_t clone_count) {
            size_t actual_clone_count = std::min(clone_count, src_size);
            {
                auto tmp_target_column = target_column->clone_resized(actual_clone_count);
                // insert all elements from source column
                std::vector<uint32_t> indices(src_size);
                std::iota(indices.begin(), indices.end(), 0);
                tmp_target_column->insert_indices_from(*source_column, indices.data(),
                                                       indices.data() + src_size);
                EXPECT_EQ(tmp_target_column->size(), actual_clone_count + indices.size());
                size_t j = 0;
                for (j = 0; j != actual_clone_count; ++j) {
                    checkField(*tmp_target_column, *source_column, j, j);
                }
                for (size_t k = 0; j < actual_clone_count + indices.size(); ++j, ++k) {
                    checkField(*tmp_target_column, *source_column, j, indices[k]);
                }
            }
            {
                // Normal case with random indices
                auto tmp_target_column = target_column->clone_resized(actual_clone_count);
                std::vector<uint32_t> indices(src_size);
                std::iota(indices.begin(), indices.end(), 0);
                std::random_device rd;
                std::mt19937 g(rd());
                std::shuffle(indices.begin(), indices.end(), g);
                tmp_target_column->insert_indices_from(*source_column, indices.data(),
                                                       indices.data() + indices.size());
                EXPECT_EQ(tmp_target_column->size(), actual_clone_count + indices.size());
                size_t j = 0;
                for (j = 0; j != actual_clone_count; ++j) {
                    checkField(*tmp_target_column, *source_column, j, j);
                }
                for (size_t k = 0; j < actual_clone_count + indices.size(); ++j, ++k) {
                    checkField(*tmp_target_column, *source_column, j, indices[k]);
                }
            }
            {
                // Normal case with duplicate indices
                auto tmp_target_column = target_column->clone_resized(actual_clone_count);
                std::vector<uint32_t> indices = {0, uint32_t(source_column->size() - 1),
                                                 uint32_t((source_column->size() + 1) >> 1),
                                                 uint32_t(source_column->size() - 1), 0};
                tmp_target_column->insert_indices_from(*source_column, indices.data(),
                                                       indices.data() + indices.size());
                EXPECT_EQ(tmp_target_column->size(), actual_clone_count + indices.size());
                size_t j = 0;
                for (j = 0; j != actual_clone_count; ++j) {
                    checkField(*tmp_target_column, *source_column, j, j);
                }
                for (size_t k = 0; j < actual_clone_count + indices.size(); ++j, ++k) {
                    checkField(*tmp_target_column, *source_column, j, indices[k]);
                }
            }
        };
        test_func2(0);
        test_func2(10);
    }

    static void assert_insert_range_from_with_field_callback(
            const MutableColumnPtr& source_column) {
        std::vector<size_t> insert_vals_count = {0, 10, 1000};
        auto src_size = source_column->size();
        std::vector<size_t> src_data_indices = {0, src_size - 1, (src_size + 1) >> 1};
        auto test_func = [&](size_t clone_count) {
            size_t actual_clone_count = std::min(clone_count, src_size);
            auto target_column = source_column->clone_resized(actual_clone_count);
            for (auto pos = src_data_indices.begin(); pos < src_data_indices.end(); ++pos) {
                if (*pos >= src_size) {
                    continue;
                }
                for (auto n : insert_vals_count) {
                    target_column->resize(actual_clone_count);
                    size_t actual_insert_count = std::min(n, src_size - *pos);
                    target_column->insert_range_from_ignore_overflow(*source_column, *pos,
                                                                     actual_insert_count);
                    auto target_size = target_column->size();
                    EXPECT_EQ(target_size, actual_clone_count + actual_insert_count);
                    size_t i = 0;
                    for (; i < actual_clone_count; ++i) {
                        checkField(*target_column, *source_column, i, i);
                    }
                    for (size_t j = *pos; i < target_size; ++i, ++j) {
                        checkField(*target_column, *source_column, i, j);
                    }
                }
            }
        };
        test_func(0);
        test_func(10);

        auto target_column = source_column->clone_empty();
        EXPECT_THROW(target_column->insert_range_from(*source_column, 0, src_size + 1), Exception);
    }

    // assert insert_range_from_ignore_overflow which happened in columnStr<UInt32> want to insert from ColumnStr<UInt64> for more column string to be inserted not just limit to the 4G
    // Define the custom assert callback function to verify insert_range_from_ignore_overflow behavior
    static void assert_insert_range_from_ignore_overflow(MutableColumns& load_cols,
                                                         DataTypes types) {
        size_t max = load_cols[0]->size();
        for (size_t i = 1; i < load_cols.size(); ++i) {
            max = std::max(max, load_cols[i]->size());
        }
        for (size_t i = 0; i < load_cols.size(); ++i) {
            if (load_cols[i]->size() < max) {
                load_cols[i]->resize(max);
            }
        }
        // step1. to construct a block for load_cols
        Block block;
        for (size_t i = 0; i < load_cols.size(); ++i) {
            ColumnWithTypeAndName columnTypeAndName;
            columnTypeAndName.column = load_cols[i]->assume_mutable();
            columnTypeAndName.type = types[i];
            block.insert(columnTypeAndName);
        }
        MutableBlock mb = MutableBlock::build_mutable_block(&block);
        // step2. to construct a block for assert_cols
        Block assert_block;
        Block empty_block;
        for (size_t i = 0; i < load_cols.size(); ++i) {
            ColumnWithTypeAndName columnTypeAndName;
            columnTypeAndName.column = load_cols[i]->clone_empty();
            columnTypeAndName.type = types[i];
            assert_block.insert(columnTypeAndName);
            empty_block.insert(columnTypeAndName);
        }
        MutableBlock assert_mb = MutableBlock::build_mutable_block(&empty_block);
        // step3. to insert data from load_cols to assert_cols
        Status st = mb.merge_impl_ignore_overflow(assert_block);
        EXPECT_TRUE(st.ok()) << "Failed to merge block: " << st.to_string();
        Status st2 = assert_mb.merge_impl_ignore_overflow(block);
        EXPECT_TRUE(st2.ok()) << "Failed to merge block1: " << st2.to_string();
        // step4. to check data in assert_cols
        for (size_t i = 0; i < load_cols.size(); ++i) {
            checkColumn(*load_cols[i], *mb.get_column_by_position(i), load_cols[i]->size());
            checkColumn(*load_cols[i], *assert_mb.get_column_by_position(i), load_cols[i]->size());
        }
    }

    // assert insert_many_from (used in join situation, which handle left table col to expand for right table : such as A[1,2,3] inner join B[2,2,4,4] => A[2,2] )
    // Define the custom assert callback function to verify insert_many_from behavior
    static void assert_insert_many_from_callback(MutableColumns& load_cols,
                                                 DataTypeSerDeSPtrs serders) {
        // Create an empty column to verify `insert_many_from` functionality
        LOG(INFO) << "now we are in assert_insert_many_from_callback";
        MutableColumns verify_columns;
        for (auto& col : load_cols) {
            verify_columns.push_back(col->clone_empty());
        }
        auto option = DataTypeSerDe::FormatOptions();
        std::vector<std::vector<std::string>> actual_res;

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
                        if (is_column<ColumnArray>(
                                    remove_nullable(source_column->assume_mutable()).get())) {
                            // insert_range_from in array has DCHECK_LG
                            continue;
                        }
                        target_column->clear();
                        // insert_range_from now we have no any exception error data to handle and also no crash
                        LOG(INFO) << "we expect exception insert_many_from from " << *pos
                                  << " with length " << *cl << " for column "
                                  << source_column->get_name()
                                  << " with source size: " << source_column->size();
                        target_column->insert_many_from(*source_column, *pos, *cl);
                    } else {
                        target_column->clear();
                        LOG(INFO) << "now insert_many_from from " << *pos << " with length " << *cl
                                  << " for column " << source_column->get_name()
                                  << " with source size: " << source_column->size();
                        target_column->insert_many_from(*source_column, *pos, *cl);
                    }

                    // Verify the inserted data matches the expected results in `assert_res`
                    auto ser_col = ColumnString::create();
                    ser_col->reserve(target_column->size());
                    VectorBufferWriter buffer_writer(*ser_col.get());
                    std::vector<std::string> data;
                    data.push_back("now assert insert_many_from for column " +
                                   target_column->get_name() + " from " + std::to_string(*pos) +
                                   " with length " + std::to_string(*cl));
                    for (size_t j = 0; j < target_column->size(); ++j) {
                        if (auto st = serders[i]->serialize_one_cell_to_json(*target_column, j,
                                                                             buffer_writer, option);
                            !st) {
                            LOG(ERROR) << "Failed to serialize column " << i << " at row " << j;
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
        std::vector<std::vector<std::string>> res;
        // Insert data from `load_cols` to `verify_columns` using `insert_indices_from`
        for (size_t i = 0; i < load_cols.size(); ++i) {
            auto& source_column = load_cols[i];
            auto& target_column = verify_columns[i];

            // uint32_t(-1) now we have compiler to make sure it will not appear
            // and this function
            std::vector<uint32_t> check_indices = {0, uint32_t((source_column->size() + 1) >> 1),
                                                   uint32_t(source_column->size()),
                                                   uint32_t(source_column->size() + 1)};
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
                        LOG(INFO) << source_column->get_name() << " now insert_indices_from from "
                                  << *from_idx << " to " << *end_idx
                                  << " with source size: " << source_column->size();
                        target_column->insert_indices_from(*source_column, &(*from_idx),
                                                           &(*end_idx));
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
                            LOG(ERROR) << "Failed to serialize column " << i << " at row " << j;
                            break;
                        }
                        buffer_writer.commit();
                        std::string actual_str_value = ser_col->get_data_at(j).to_string();
                        data.push_back(actual_str_value);
                    }
                    res.push_back(data);
                }
            }
        }
        check_res_file("insert_indices_from", res);
    }

    static void assert_insert_default_with_field_callback(const MutableColumnPtr& source_column) {
        Field default_field;
        {
            auto target_column = source_column->clone_empty();
            target_column->insert_default();
            ASSERT_EQ(target_column->size(), 1);
            target_column->get(0, default_field);
            std::cout << "default_field: " << default_field.get_type_name() << std::endl;
        }
        auto src_size = source_column->size();

        auto test_func = [&](size_t clone_count) {
            size_t actual_clone_count = std::min(clone_count, src_size);
            auto target_column = source_column->clone_resized(actual_clone_count);
            target_column->insert_default();
            auto target_size = target_column->size();
            EXPECT_EQ(target_size, actual_clone_count + 1);
            size_t i = 0;
            for (; i < actual_clone_count; ++i) {
                checkField(*target_column, *source_column, i, i);
            }
            Field last_field;
            target_column->get(i, last_field);
            EXPECT_EQ(last_field, default_field);
        };

        std::vector<size_t> insert_vals_count = {0, 10, 1000};
        src_size = source_column->size();

        auto test_func2 = [&](size_t clone_count) {
            for (auto n : insert_vals_count) {
                size_t actual_clone_count = std::min(clone_count, src_size);
                auto target_column = source_column->clone_resized(actual_clone_count);
                target_column->insert_many_defaults(n);
                auto target_size = target_column->size();
                EXPECT_EQ(target_size, actual_clone_count + n);
                size_t i = 0;
                for (; i < actual_clone_count; ++i) {
                    checkField(*target_column, *source_column, i, i);
                }
                for (; i < target_size; ++i) {
                    Field f;
                    target_column->get(i, f);
                    EXPECT_EQ(f, default_field);
                }
            }
        };
        test_func(0);
        test_func(10);
        test_func2(0);
        test_func2(10);
    };

    static void assert_insert_data_callback(MutableColumns& load_cols, DataTypeSerDeSPtrs serders) {
        // Create an empty column to verify `insert_data` functionality
        MutableColumns verify_columns;
        for (auto& col : load_cols) {
            verify_columns.push_back(col->clone_empty());
        }
        auto option = DataTypeSerDe::FormatOptions();
        // Insert data from `load_cols` to `verify_columns` using `insert_data`
        std::vector<std::vector<std::string>> res;
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
            std::vector<std::string> data;
            for (size_t j = 0; j < target_column->size(); ++j) {
                if (auto st = serders[i]->serialize_one_cell_to_json(*target_column, j,
                                                                     buffer_writer, option);
                    !st) {
                    LOG(ERROR) << "Failed to serialize column " << i << " at row " << j;
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
        std::vector<std::vector<std::string>> res;
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
            std::vector<std::string> data;
            for (size_t j = 0; j < target_column->size(); ++j) {
                if (auto st = serders[i]->serialize_one_cell_to_json(*target_column, j,
                                                                     buffer_writer, option);
                    !st) {
                    LOG(ERROR) << "Failed to serialize column " << i << " at row " << j;
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
        std::vector<std::vector<std::string>> res;
        // Insert data from `load_cols` to `verify_columns` using `insert_default`
        for (size_t i = 0; i < load_cols.size(); ++i) {
            auto& target_column = verify_columns[i];
            target_column->insert_default();

            // Verify the inserted data matches the expected results in `assert_res`
            auto ser_col = ColumnString::create();
            ser_col->reserve(target_column->size());
            VectorBufferWriter buffer_writer(*ser_col.get());
            std::vector<std::string> data;
            for (size_t j = 0; j < target_column->size(); ++j) {
                if (auto st = serders[i]->serialize_one_cell_to_json(*target_column, j,
                                                                     buffer_writer, option);
                    !st) {
                    LOG(ERROR) << "Failed to serialize column " << i << " at row " << j;
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
        std::vector<std::vector<std::string>> res;
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
                std::vector<std::string> data;
                for (size_t j = 0; j < target_column->size(); ++j) {
                    if (auto st = serders[i]->serialize_one_cell_to_json(*target_column, j,
                                                                         buffer_writer, option);
                        !st) {
                        LOG(ERROR) << "Failed to serialize column " << i << " at row " << j;
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

    ////////// =================== column data access interface assert (6)=================== //////////
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
        auto option = DataTypeSerDe::FormatOptions();
        {
            MutableColumns assert_cols(load_cols.size());
            for (size_t i = 0; i < load_cols.size(); ++i) {
                assert_cols[i] = load_cols[i]->clone_empty();
            }
            std::vector<std::vector<std::string>> res;
            // just check cols get is the same as assert_res
            for (size_t i = 0; i < load_cols.size(); ++i) {
                auto& source_column = load_cols[i];
                LOG(INFO) << " new insert field for column : " << assert_cols[i]->get_name()
                          << " with size : " << assert_cols[i]->size() << " source_coumn"
                          << source_column->size();
                for (size_t j = 0; j < source_column->size(); ++j) {
                    Field f;
                    source_column->get(j, f);
                    assert_cols[i]->insert(f);
                }
                // check with null Field
                Field null_field;
                assert_cols[i]->insert(null_field);
            }
            // Verify the inserted data matches the expected results in `assert_res`
            for (size_t i = 0; i < assert_cols.size(); ++i) {
                auto ser_col = ColumnString::create();
                ser_col->reserve(load_cols[i]->size());
                VectorBufferWriter buffer_writer(*ser_col.get());
                std::vector<std::string> data;
                for (size_t j = 0; j < assert_cols[i]->size() - 1; ++j) {
                    if (auto st = serders[i]->serialize_one_cell_to_json(*assert_cols[i], j,
                                                                         buffer_writer, option);
                        !st) {
                        LOG(ERROR) << "Failed to serialize column " << i << " at row " << j;
                        break;
                    }
                    buffer_writer.commit();
                    std::string actual_str_value = ser_col->get_data_at(j).to_string();
                    data.push_back(actual_str_value);
                    EXPECT_EQ(load_cols[i]->operator[](j), assert_cols[i]->operator[](j));
                }
                res.push_back(data);
            }
            check_res_file("get_field", res);
        }
        {
            MutableColumns assert_cols(load_cols.size());
            for (size_t i = 0; i < load_cols.size(); ++i) {
                assert_cols[i] = load_cols[i]->clone_empty();
            }
            // just check cols operator [] to get field same with field
            std::vector<std::vector<std::string>> res2;
            for (size_t i = 0; i < load_cols.size(); ++i) {
                auto& source_column = load_cols[i];
                for (size_t j = 0; j < source_column->size(); ++j) {
                    Field f = source_column->operator[](j);
                    assert_cols[i]->insert(f);
                }
                // check with null Field
                Field null_field;
                assert_cols[i]->insert(null_field);
            }

            // Verify the inserted data matches the expected results in `assert_res`
            for (size_t i = 0; i < assert_cols.size(); ++i) {
                auto ser_col = ColumnString::create();
                ser_col->reserve(load_cols[i]->size());
                VectorBufferWriter buffer_writer(*ser_col.get());
                std::vector<std::string> data;
                for (size_t j = 0; j < assert_cols[i]->size() - 1; ++j) {
                    if (auto st = serders[i]->serialize_one_cell_to_json(*assert_cols[i], j,
                                                                         buffer_writer, option);
                        !st) {
                        LOG(ERROR) << "Failed to serialize column " << i << " at row " << j;
                        break;
                    }
                    buffer_writer.commit();
                    std::string actual_str_value = ser_col->get_data_at(j).to_string();
                    data.push_back(actual_str_value);
                    EXPECT_EQ(load_cols[i]->operator[](j), assert_cols[i]->operator[](j));
                }
                res2.push_back(data);
            }
            check_res_file("get_field_operator", res2);
        }
    }
    //
    //virtual StringRef
    //get_raw_data () const which is continues memory layout of the data,
    // we can use this to check the data is stored in the column
    template <PrimitiveType T>
    static void assert_get_raw_data_callback(MutableColumns& load_cols,
                                             DataTypeSerDeSPtrs serders) {
        // just check cols get_raw_data is the same as assert_res
        LOG(INFO) << "now we are in assert_get_raw_data_callback";
        std::vector<std::vector<std::string>> res;
        MutableColumns assert_cols(load_cols.size());
        for (size_t i = 0; i < load_cols.size(); ++i) {
            assert_cols[i] = load_cols[i]->clone_empty();
        }
        auto option = DataTypeSerDe::FormatOptions();
        for (size_t i = 0; i < load_cols.size(); ++i) {
            auto& source_column = load_cols[i];
            const typename PrimitiveTypeTraits<T>::ColumnItemType* rd =
                    (typename PrimitiveTypeTraits<T>::ColumnItemType*)source_column->get_raw_data()
                            .data;
            for (size_t j = 0; j < source_column->size(); j++) {
                Field f;
                source_column->get(j, f);
                ASSERT_EQ(f, Field::create_field<T>(rd[j]));
                // insert field to assert column
                assert_cols[i]->insert(f);
            }
        }

        // Verify the inserted data matches the expected results in `assert_res`
        for (size_t i = 0; i < assert_cols.size(); ++i) {
            auto ser_col = ColumnString::create();
            ser_col->reserve(load_cols[i]->size());
            VectorBufferWriter buffer_writer(*ser_col.get());
            std::vector<std::string> data;
            for (size_t j = 0; j < assert_cols[i]->size(); ++j) {
                if (auto st = serders[i]->serialize_one_cell_to_json(*assert_cols[i], j,
                                                                     buffer_writer, option);
                    !st) {
                    LOG(ERROR) << "Failed to serialize column " << i << " at row " << j;
                    break;
                }
                buffer_writer.commit();
                std::string actual_str_value = ser_col->get_data_at(j).to_string();
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
        std::vector<std::vector<std::string>> res;
        // just check cols get_int is the same as assert_res
        for (size_t i = 0; i < load_cols.size(); ++i) {
            auto& source_column = load_cols[i];
            std::vector<std::string> data;
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
        std::vector<std::vector<std::string>> res;
        // just check cols get_bool is the same as assert_res
        for (size_t i = 0; i < load_cols.size(); ++i) {
            auto& source_column = load_cols[i];
            std::vector<std::string> data;
            for (size_t j = 0; j < source_column->size(); ++j) {
                auto actual_str_value = std::to_string(source_column->get_bool(j));
                data.push_back(actual_str_value);
            }
            res.push_back(data);
        }
        check_res_file("get_bool", res);
    }

    ////////// =================== column data meta interface assert (7)=================== //////////
    // virtual std::string
    //get_name () const , simple assert to make sure name
    static void assert_get_name(IColumn& column, const std::string expect_name) {
        ASSERT_EQ(expect_name, column.get_name());
    }

    // use in ColumnVariant for check_if_sparse_column
    static void assert_get_ratio_of_default_rows(MutableColumns& load_cols,
                                                 DataTypeSerDeSPtrs serders) {
        // just check cols get_ratio_of_default_rows is the same as assert_res
        std::vector<std::vector<std::string>> res;
        for (size_t i = 0; i < load_cols.size(); ++i) {
            auto& source_column = load_cols[i];
            std::vector<std::string> data;
            data.push_back("in column: " + source_column->get_name() + " ratio of default rows: ");
            auto actual_str_value = std::to_string(source_column->get_ratio_of_default_rows());
            data.push_back(actual_str_value);
            res.push_back(data);
        }
        check_res_file("get_ratio_of_default_rows", res);
    }

    // size related we can check from checked file to make sure the size is right
    static void assert_size_callback(MutableColumns& load_cols, DataTypeSerDeSPtrs serders) {
        std::vector<std::vector<std::string>> res;
        // just check cols size is the same as assert_res
        for (size_t i = 0; i < load_cols.size(); ++i) {
            std::vector<std::string> data;
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
        std::vector<std::vector<std::string>> res;
        for (size_t i = 0; i < load_cols.size(); ++i) {
            std::vector<std::string> data;
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
        LOG(INFO) << "now we are in assert_allocated_bytes_callback";
        std::vector<std::vector<std::string>> res;
        for (size_t i = 0; i < load_cols.size(); ++i) {
            std::vector<std::string> data;
            auto& source_column = load_cols[i];
            auto actual_str_value = std::to_string(source_column->allocated_bytes());
            data.push_back(actual_str_value);
            res.push_back(data);
        }
        check_res_file("allocated_bytes", res);
    }

    // empty just use size() == 0 to impl as default behavior
    void assert_empty(MutableColumnPtr col) { EXPECT_EQ(col->size(), 0); }

    //The is_exclusive function is implemented differently in different columns, and the correctness should be verified reasonably.
    void assert_is_exclusive(
            MutableColumns& load_cols, DataTypeSerDeSPtrs serders,
            std::function<void(MutableColumns& load_cols, DataTypeSerDeSPtrs serders)>
                    assert_callback) {
        assert_callback(load_cols, serders);
    }

    ////////// =================== column data meta manage assert (11)=================== //////////
    //virtual void
    // pop_back (size_t n) = 0
    // assert pop_back
    static void assert_pop_back_callback(MutableColumns& load_cols, DataTypeSerDeSPtrs serders) {
        // Create an empty column to verify `pop_back` functionality
        // check pop_back with different n
        std::vector<std::vector<std::string>> res;
        auto option = DataTypeSerDe::FormatOptions();
        // size_t(-1) 会导致pod_array 溢出
        std::vector<size_t> check_length = {0, 1, 10, 100, 1000, 10000, 100000};
        for (auto cl = check_length.begin(); cl < check_length.end(); ++cl) {
            for (size_t i = 0; i < load_cols.size(); ++i) {
                auto& source_column = load_cols[i];
                if (*cl > source_column->size()) {
                    // now we do not check in popback, but we should make sure the arguments are passed correctly,
                    // otherwise we should meet `Check failed: false Amount of memory requested to allocate is more than allowed`
                    LOG(INFO) << "now we are in pop_back column : " << load_cols[i]->get_name()
                              << " for column size : " << source_column->size();
                    source_column->pop_back(source_column->size());
                } else {
                    LOG(INFO) << "now we are in pop_back column : " << load_cols[i]->get_name()
                              << " with check length: " << *cl
                              << " for column size : " << source_column->size();
                    source_column->pop_back(*cl);
                }

                // Verify the pop back data matches the expected results in `assert_res`
                auto ser_col = ColumnString::create();
                ser_col->reserve(load_cols[i]->size());
                VectorBufferWriter buffer_writer(*ser_col.get());
                std::vector<std::string> data;
                for (size_t j = 0; j < source_column->size(); ++j) {
                    if (auto st = serders[i]->serialize_one_cell_to_json(*source_column, j,
                                                                         buffer_writer, option);
                        !st) {
                        LOG(ERROR) << "Failed to serialize column " << i << " at row " << j;
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

    static void assert_pop_back_with_field_callback(const MutableColumnPtr source_column) {
        auto src_size = source_column->size();
        std::vector<size_t> pop_back_count = {0, src_size - 1, (src_size + 1) >> 1};
        for (auto n : pop_back_count) {
            auto target_column = source_column->clone_resized(src_size);
            target_column->pop_back(n);
            EXPECT_EQ(target_column->size(), src_size - n);
            checkColumn(*target_column, *source_column, target_column->size());
        }
        EXPECT_ANY_THROW(source_column->pop_back(src_size + 1)); // pop_back out of range
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
        std::vector<std::vector<std::string>> res;
        for (auto cl = check_length.begin(); cl < check_length.end(); ++cl) {
            for (size_t i = 0; i < load_cols.size(); ++i) {
                auto& source_column = load_cols[i];
                auto ptr = source_column->clone_resized(*cl);
                // check size
                EXPECT_EQ(ptr->size(), *cl);
                // check ptr is not the same
                EXPECT_NE(ptr.get(), source_column.get());

                // check after clone_resized with assert_res
                auto ser_col = ColumnString::create();
                ser_col->reserve(ptr->size());
                VectorBufferWriter buffer_writer(*ser_col.get());
                std::vector<std::string> data;
                data.push_back("column: " + source_column->get_name() + " with check size: " +
                               std::to_string(*cl) + " with ptr: " + std::to_string(ptr->size()));
                for (size_t j = 0; j < ptr->size(); ++j) {
                    if (auto st = serders[i]->serialize_one_cell_to_json(*ptr, j, buffer_writer,
                                                                         option);
                        !st) {
                        LOG(ERROR) << "Failed to serialize column " << i << " at row " << j;
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

    //virtual Ptr
    //cut (size_t start, size_t length) const final will call clone_empty and insert_range_from
    static void assert_cut_callback(MutableColumns& load_cols, DataTypeSerDeSPtrs serders) {
        // Create an empty column to verify `cut` functionality
        // check cut with different start and length
        // size_t(-1) 会导致pod_array 溢出
        std::vector<std::vector<std::string>> res;
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
                LOG(INFO) << "now we are in cut column : " << load_cols[i]->get_name()
                          << " with check length: " << insert_size
                          << " for column size : " << source_column->size();
                ptr = source_column->cut(0, insert_size);
                // check size
                EXPECT_EQ(ptr->size(), insert_size);
                // check ptr is not the same
                EXPECT_NE(ptr.get(), source_column.get());
                // check after cut with assert_res
                auto ser_col = ColumnString::create();
                ser_col->reserve(ptr->size());
                VectorBufferWriter buffer_writer(*ser_col.get());
                std::vector<std::string> data;
                data.push_back("column: " + source_column->get_name() + " with check size: " +
                               std::to_string(*cl) + " with ptr: " + std::to_string(ptr->size()));
                for (size_t j = 0; j < ptr->size(); ++j) {
                    if (auto st = serders[i]->serialize_one_cell_to_json(*ptr, j, buffer_writer,
                                                                         option);
                        !st) {
                        LOG(ERROR) << "Failed to serialize column " << i << " at row " << j;
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
        std::vector<std::vector<std::string>> res;
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
                LOG(INFO) << "now we are in shrink column : " << load_cols[i]->get_name()
                          << " with check length: " << insert_size
                          << " for column size : " << source_column->size();
                size_t cnt = source_column->use_count();
                ptr = source_column->shrink(insert_size);
                LOG(INFO) << "use_count : " << source_column->use_count();
                // check size
                EXPECT_EQ(ptr->size(), insert_size);
                // check ptr is not the same
                if (cnt == 1) {
                    // just return the origin column to avoid the copy
                    EXPECT_EQ(ptr.get(), source_column.get());
                } else {
                    EXPECT_NE(ptr.get(), source_column.get());
                }
                // check after cut with assert_res
                auto ser_col = ColumnString::create();
                ser_col->reserve(ptr->size());
                VectorBufferWriter buffer_writer(*ser_col.get());
                std::vector<std::string> data;
                data.push_back("column: " + source_column->get_name() + " with check size: " +
                               std::to_string(*cl) + " with ptr: " + std::to_string(ptr->size()));
                for (size_t j = 0; j < ptr->size(); ++j) {
                    if (auto st = serders[i]->serialize_one_cell_to_json(*ptr, j, buffer_writer,
                                                                         option);
                        !st) {
                        LOG(ERROR) << "Failed to serialize column " << i << " at row " << j;
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
        std::vector<std::vector<std::string>> res;
        auto option = DataTypeSerDe::FormatOptions();
        std::vector<size_t> check_length = {0, 1, 10, 100, 1000, 10000, 100000};
        for (auto cl = check_length.begin(); cl < check_length.end(); ++cl) {
            for (size_t i = 0; i < load_cols.size(); ++i) {
                auto assert_origin_size = load_cols[i]->size();
                auto& source_column = load_cols[i];
                LOG(INFO) << "now we are in reserve column : " << load_cols[i]->get_name()
                          << " with check length: " << *cl
                          << " for column size : " << source_column->size();
                source_column->reserve(*cl);
                // check size no changed after reserve
                EXPECT_EQ(source_column->size(), assert_origin_size);
                // check after reserve with assert_res
                auto ser_col = ColumnString::create();
                ser_col->reserve(source_column->size());
                VectorBufferWriter buffer_writer(*ser_col.get());
                std::vector<std::string> data;
                data.push_back("column: " + source_column->get_name() +
                               " with check size: " + std::to_string(*cl) +
                               " with ptr: " + std::to_string(source_column->size()));
                for (size_t j = 0; j < source_column->size(); ++j) {
                    if (auto st = serders[i]->serialize_one_cell_to_json(*source_column, j,
                                                                         buffer_writer, option);
                        !st) {
                        LOG(ERROR) << "Failed to serialize column " << i << " at row " << j;
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
        std::vector<std::vector<std::string>> res;
        auto option = DataTypeSerDe::FormatOptions();
        std::vector<size_t> check_length = {0, 1, 10, 100, 1000, 10000, 100000};
        for (auto cl = check_length.begin(); cl < check_length.end(); ++cl) {
            for (size_t i = 0; i < load_cols.size(); ++i) {
                auto& source_column = load_cols[i];
                LOG(INFO) << "now we are in resize column : " << load_cols[i]->get_name()
                          << " with check length: " << *cl
                          << " for column size : " << source_column->size();
                source_column->resize(*cl);
                // check size
                EXPECT_EQ(source_column->size(), *cl);
                // check after resize with assert_res
                auto ser_col = ColumnString::create();
                ser_col->reserve(source_column->size());
                VectorBufferWriter buffer_writer(*ser_col.get());
                std::vector<std::string> data;
                data.push_back("column: " + source_column->get_name() +
                               " with check size: " + std::to_string(*cl) +
                               " with ptr: " + std::to_string(source_column->size()));
                for (size_t j = 0; j < source_column->size(); ++j) {
                    if (auto st = serders[i]->serialize_one_cell_to_json(*source_column, j,
                                                                         buffer_writer, option);
                        !st) {
                        LOG(ERROR) << "Failed to serialize column " << i << " at row " << j;
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

    //virtual void
    //for_each_subcolumn (ColumnCallback)
    //virtual void
    //replace_column_data (const IColumn &, size_t row, size_t self_row=0) used in BlockReader(VerticalBlockReader)::_copy_agg_data() for agg value data
    // the passed column must be non-variable length column: like columnVector...
    static void assert_replace_column_data_callback(MutableColumns& load_cols,
                                                    DataTypeSerDeSPtrs serders) {
        // Create an empty column to verify `replace_column_data` functionality
        // check replace_column_data with different row and self_row
        std::vector<std::vector<std::string>> res;
        auto option = DataTypeSerDe::FormatOptions();
        std::vector<size_t> check_length = {0, 1, 7, 10, 100, 1000};
        for (auto cl = check_length.begin(); cl < check_length.end(); ++cl) {
            for (size_t i = 0; i < load_cols.size(); ++i) {
                if (load_cols[i]->is_variable_length()) {
                    EXPECT_ANY_THROW(load_cols[i]->replace_column_data(*load_cols[i], *cl));
                }
                auto& source_column = load_cols[i];
                if (*cl > source_column->size()) {
                    // if replace row is bigger than the source column size here meet pod coredump
                    continue;
                }
                LOG(INFO) << "now we are in replace_column_data column : "
                          << load_cols[i]->get_name() << " with check length: " << *cl
                          << " for column size : " << source_column->size();
                source_column->replace_column_data(*source_column, *cl);
                // check after replace_column_data: the first data is same with the data in source column's *cl row
                auto ser_col = ColumnString::create();
                ser_col->reserve(source_column->size());
                VectorBufferWriter buffer_writer(*ser_col.get());
                std::vector<std::string> data;
                data.push_back("column: " + source_column->get_name() +
                               " with check size: " + std::to_string(*cl) +
                               " with ptr: " + std::to_string(source_column->size()));
                for (size_t j = 0; j < source_column->size(); ++j) {
                    if (auto st = serders[i]->serialize_one_cell_to_json(*source_column, j,
                                                                         buffer_writer, option);
                        !st) {
                        LOG(ERROR) << "Failed to serialize column " << i << " at row " << j;
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
        std::vector<std::vector<std::string>> res;
        auto option = DataTypeSerDe::FormatOptions();
        const NullMap null_map = {1, 1, 0, 1};
        for (size_t i = 0; i < load_cols.size(); ++i) {
            auto& source_column = load_cols[i];
            LOG(INFO) << "now we are in replace_column_null_data column : "
                      << load_cols[i]->get_name() << " for column size : " << source_column->size()
                      << "with nullmap" << null_map.data();
            source_column->replace_column_null_data(null_map.data());

            // check after replace_column_null_data: 1 in nullmap present the load cols data is null and data should be default value
            auto ser_col = ColumnString::create();
            ser_col->reserve(source_column->size());
            VectorBufferWriter buffer_writer(*ser_col.get());
            std::vector<std::string> data;
            data.push_back("column: " + source_column->get_name() +
                           " with nullmap: " + std::to_string(*null_map.data()) +
                           " with ptr: " + std::to_string(source_column->size()));
            for (size_t j = 0; j < source_column->size(); ++j) {
                if (auto st = serders[i]->serialize_one_cell_to_json(*source_column, j,
                                                                     buffer_writer, option);
                    !st) {
                    LOG(ERROR) << "Failed to serialize column " << i << " at row " << j;
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
        std::vector<std::vector<std::string>> res;
        auto option = DataTypeSerDe::FormatOptions();
        for (size_t i = 0; i < load_cols.size(); ++i) {
            auto& source_column = load_cols[i];
            MutableColumnPtr res_col = source_column->clone_empty();
            // selector is range for the column, contain values from 0 to num_columns - 1.
            // selector size should bigger than begin and end ,
            // because selector[i], i in range(begin,end),  Make a DCHECK for this
            const ColumnArray::Selector selector = {1, 2, 3, 0};
            LOG(INFO) << "now we are in append_data_by_selector column : "
                      << load_cols[i]->get_name() << " for column size : " << source_column->size()
                      << " with selector size: " << selector.size();
            source_column->append_data_by_selector(res_col, selector, 0, 4);
            // check after append_data_by_selector: 1 in selector present the load cols data is selected and data should be default value
            auto ser_col = ColumnString::create();
            ser_col->reserve(res_col->size());
            VectorBufferWriter buffer_writer(*ser_col.get());
            std::vector<std::string> data;
            data.push_back("column: " + source_column->get_name() +
                           " with selector size : " + std::to_string(selector.size()) +
                           " with ptr: " + std::to_string(res_col->size()));
            for (size_t j = 0; j < res_col->size(); ++j) {
                if (auto st = serders[i]->serialize_one_cell_to_json(*res_col, j, buffer_writer,
                                                                     option);
                    !st) {
                    LOG(ERROR) << "Failed to serialize column " << i << " at row " << j;
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

    ////////// =================== column calculate interface assert (8)=================== //////////
    // Column Calculate Interface: filter, compare, permute, sort
    // filter (const Filter &filt) const =0
    //  Filter is a array contains 0 or 1 to present the row is selected or not, so it should be the same size with the source column
    static void assert_filter_callback(MutableColumns& load_cols, DataTypeSerDeSPtrs serders) {
        // Create an empty column to verify `filter` functionality
        // check filter with different filter
        std::vector<std::vector<std::string>> res;
        auto option = DataTypeSerDe::FormatOptions();

        for (size_t i = 0; i < load_cols.size(); ++i) {
            auto& source_column = load_cols[i];
            auto source_size = source_column->size();
            auto cloned_col = load_cols[i]->clone_resized(source_size);
            const ColumnArray::Filter all_filtered(source_size, 0);
            const ColumnArray::Filter no_filtered(source_size, 1);
            // invalid data -1 will also make data without be filtered ??
            ColumnArray::Filter invalid_filter(source_size - 1, 1);
            invalid_filter.emplace_back(-1);
            std::vector<std::string> data;
            LOG(INFO) << "now we are in filter column : " << load_cols[i]->get_name()
                      << " for column size : " << source_column->size();
            {
                auto ret_size = source_column->filter(no_filtered);
                EXPECT_EQ(ret_size, source_size);
                // check filter res
                auto ser_col = ColumnString::create();
                ser_col->reserve(ret_size);
                VectorBufferWriter buffer_writer(*ser_col.get());
                data.clear();
                data.push_back("column: " + source_column->get_name() +
                               " with no filtered with ptr: " + std::to_string(ret_size));
                for (size_t j = 0; j < ret_size; ++j) {
                    if (auto st = serders[i]->serialize_one_cell_to_json(*source_column, j,
                                                                         buffer_writer, option);
                        !st) {
                        LOG(ERROR) << "Failed to serialize column " << i << " at row " << j;
                        break;
                    }
                    buffer_writer.commit();
                    std::string actual_str_value = ser_col->get_data_at(j).to_string();
                    data.push_back(actual_str_value);
                }
                res.push_back(data);
            }
            {
                auto ret_size = source_column->filter(all_filtered);
                EXPECT_EQ(ret_size, 0);
            }
            {
                // check filter with invalid filter
                // source_column is filterd, size=0
                EXPECT_ANY_THROW(source_column->filter(invalid_filter));
                auto ret_size = cloned_col->filter(invalid_filter);
                // check filter res
                auto ser_col = ColumnString::create();
                ser_col->reserve(ret_size);
                VectorBufferWriter buffer_writer(*ser_col.get());
                data.clear();
                data.push_back("column: " + source_column->get_name() +
                               " with invalid filtered with ptr: " + std::to_string(ret_size));
                for (size_t j = 0; j < ret_size; ++j) {
                    if (auto st = serders[i]->serialize_one_cell_to_json(*cloned_col, j,
                                                                         buffer_writer, option);
                        !st) {
                        LOG(ERROR) << "Failed to serialize column " << i << " at row " << j;
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

    static void assert_filter_with_field_callback(const MutableColumnPtr source_column) {
        auto source_size = source_column->size();
        IColumn::Filter all_filtered(source_size, 0);
        IColumn::Filter no_filtered(source_size, 1);
        IColumn::Filter normal_filter(source_size, 1);
        normal_filter[0] = 0;
        normal_filter[source_size - 1] = 0;
        normal_filter[source_size / 2] = 0;
        std::vector<IColumn::Filter*> filters = {&all_filtered, &no_filtered, &normal_filter};
        auto test_func = [&](const IColumn::Filter* filter) {
            const auto* filter_data = (const int8_t*)filter->data();
            auto expected_size = filter->size() - simd::count_zero_num(filter_data, filter->size());
            {
                // empty column
                auto target_column = source_column->clone_empty();
                IColumn::Filter tmp_filter;
                auto ptr = target_column->filter(tmp_filter, expected_size);
                EXPECT_EQ(ptr->size(), 0);
            }
            auto target_column = source_column->clone_resized(source_size);
            auto ptr = target_column->filter(*filter, expected_size);
            EXPECT_EQ(ptr->size(), expected_size);
            // check filter result is right
            for (size_t i = 0, find_pos = 0; i < expected_size; ++i, ++find_pos) {
                find_pos = simd::find_byte(filter_data, find_pos, filter->size(), (int8_t)1);
                EXPECT_TRUE(find_pos < filter->size());
                checkField(ptr.operator*(), *source_column, i, find_pos);
            }

            // filter will modify the original column
            {
                // empty filter
                auto target_column_1 = source_column->clone_empty();
                IColumn::Filter tmp_filter;
                auto res_size = target_column_1->filter(tmp_filter);
                EXPECT_EQ(res_size, 0);
                EXPECT_EQ(target_column_1->size(), 0);
            }
            auto result_size = target_column->filter(*filter);
            EXPECT_EQ(result_size, expected_size);
            for (size_t i = 0, find_pos = 0; i < expected_size; ++i, ++find_pos) {
                find_pos = simd::find_byte(filter_data, find_pos, filter->size(), (int8_t)1);
                EXPECT_TRUE(find_pos < filter->size());
                checkField(*target_column, *source_column, i, find_pos);
            }
        };
        for (const auto& filter : filters) {
            test_func(filter);
        }
    }

    // filter with result_hint_size which should return new column ptr
    // filter (const Filter &filt, ssize_t result_size_hint) const =0 with a result_size_hint to pass, but we should make sure the result_size_hint is not bigger than the source column size
    static void assert_filter_with_result_hint_callback(MutableColumns& load_cols,
                                                        DataTypeSerDeSPtrs serders) {
        // Create an empty column to verify `filter` functionality
        // check filter with different filter
        std::vector<std::vector<std::string>> res;
        auto option = DataTypeSerDe::FormatOptions();

        for (size_t i = 0; i < load_cols.size(); ++i) {
            auto& source_column = load_cols[i];
            auto source_size = source_column->size();
            const ColumnArray::Filter all_filtered(source_size, 0);
            const ColumnArray::Filter no_filtered(source_size, 1);
            // invalid data -1 will also make data without be filtered ??
            ColumnArray::Filter invalid_filter(source_size - 1, 1);
            // now  AddressSanitizer: negative-size-param: (size=-1) can be checked
            invalid_filter.emplace_back(-1);
            std::vector<std::string> data;
            LOG(INFO) << "now we are in filter column : " << load_cols[i]->get_name()
                      << " for column size : " << source_column->size();
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
                        LOG(ERROR) << "Failed to serialize column " << i << " at row " << j;
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
                        LOG(ERROR) << "Failed to serialize column " << i << " at row " << j;
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
                        LOG(ERROR) << "Failed to serialize column " << i << " at row " << j;
                        break;
                    }
                    buffer_writer.commit();
                    std::string actual_str_value = ser_col->get_data_at(j).to_string();
                    data.push_back(actual_str_value);
                }
                res.push_back(data);
            }
        }
        check_res_file("filter_hint", res);
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
        LOG(INFO) << "column size: " << column_size;
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

    // this function helps to check sort permutation behavior for column which use column::compare_at
    static void stable_get_column_permutation(const IColumn& column, bool ascending, size_t limit,
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
    //    get_permutation means sort data in Column as sort order
    //    limit should be set to limit the sort result
    //    nan_direction_hint deal with null|NaN value
    void assert_column_permutation(const IColumn& column, bool ascending, size_t limit,
                                   int nan_direction_hint, IColumn::Permutation& actual_permutation,
                                   IColumn::Permutation& expected_permutation) {
        LOG(INFO) << "assertColumnPermutation start, limit: " << limit
                  << " ascending: " << ascending << " nan_direction_hint: " << nan_direction_hint
                  << " column size: " << column.size()
                  << " actual_permutation size: " << actual_permutation.size()
                  << " expected_permutation size: " << expected_permutation.size();
        // step1. get expect permutation as stabled sort
        stable_get_column_permutation(column, ascending, limit, nan_direction_hint,
                                      expected_permutation);
        LOG(INFO) << "expected_permutation size: " << expected_permutation.size() << ", "
                  << join_ints(expected_permutation);
        // step2. get permutation by column
        column.get_permutation(!ascending, limit, nan_direction_hint, actual_permutation);
        LOG(INFO) << "actual_permutation size: " << actual_permutation.size() << ", "
                  << join_ints(actual_permutation);

        if (limit == 0 || limit > actual_permutation.size()) {
            limit = actual_permutation.size();
        }

        // step3. check the permutation result
        assert_permutations_with_limit(column, actual_permutation, expected_permutation, limit);
        LOG(INFO) << "assertColumnPermutation done";
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

    ////////// =================== column hash interface assert (6)=================== //////////
    // update_hashes_with_value (size_t, size_t, Hashes &hashes) const : which inner just use xxhash for column data
    static void assert_update_hashes_with_value_callback(MutableColumns& load_cols,
                                                         DataTypeSerDeSPtrs serders) {
        // Create an empty column to verify `update_hashes_with_value` functionality
        // check update_hashes_with_value with different hashes
        std::vector<std::vector<std::string>> res;
        auto option = DataTypeSerDe::FormatOptions();

        for (size_t i = 0; i < load_cols.size(); ++i) {
            auto& source_column = load_cols[i];
            std::vector<uint64_t> xx_hash_vals(source_column->size());
            LOG(INFO) << "now we are in update_hashes_with_value column : "
                      << load_cols[i]->get_name() << " for column size : " << source_column->size();
            auto* __restrict xx_hashes = xx_hash_vals.data();
            EXPECT_NO_FATAL_FAILURE(source_column->update_hashes_with_value(xx_hashes));
            // check after update_hashes_with_value: 1 in selector present the load cols data is selected and data should be default value
            auto ser_col = ColumnString::create();
            ser_col->reserve(source_column->size());
            VectorBufferWriter buffer_writer(*ser_col.get());
            std::vector<std::string> data;
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
        std::vector<std::vector<std::string>> res;
        auto option = DataTypeSerDe::FormatOptions();
        for (size_t i = 0; i < load_cols.size(); ++i) {
            auto& source_column = load_cols[i];
            std::vector<uint32_t> crc_hash_vals(source_column->size());
            LOG(INFO) << "now we are in update_hashes column : " << load_cols[i]->get_name()
                      << " for column size : " << source_column->size();
            EXPECT_NO_FATAL_FAILURE(source_column->update_crcs_with_value(
                    crc_hash_vals.data(), pts[i], source_column->size()));
            // check after update_hashes: 1 in selector present the load cols data is selected and data should be default value
            auto ser_col = ColumnString::create();
            ser_col->reserve(source_column->size());
            VectorBufferWriter buffer_writer(*ser_col.get());
            std::vector<std::string> data;
            data.push_back("column: " + source_column->get_name() +
                           " with hashes: " + std::to_string(*crc_hash_vals.data()) +
                           " with ptr: " + std::to_string(source_column->size()));
            res.push_back(data);
        }
        check_res_file("update_crcs_hashes", res);
    }

    // virtual void
    // update_hash_with_value(size_t n, SipHash& hash)
    // siphash we still keep siphash for storge layer because we use it in
    //     EngineChecksumTask::_compute_checksum() and can not to remove it
    static void assert_update_siphashes_with_value_callback(MutableColumns& load_cols,
                                                            DataTypeSerDeSPtrs serders) {
        // Create an empty column to verify `update_hashes` functionality
        // check update_hashes with different hashes
        std::vector<std::vector<std::string>> res;
        auto option = DataTypeSerDe::FormatOptions();
        for (size_t i = 0; i < load_cols.size(); ++i) {
            auto& source_column = load_cols[i];
            SipHash hash;
            LOG(INFO) << "now we are in update_hashes column : " << load_cols[i]->get_name()
                      << " for column size : " << source_column->size();
            for (size_t j = 0; j < source_column->size(); ++j) {
                source_column->update_hash_with_value(j, hash);
            }
            auto ser_col = ColumnString::create();
            ser_col->reserve(source_column->size());
            VectorBufferWriter buffer_writer(*ser_col.get());
            std::vector<std::string> data;
            data.push_back("column: " + source_column->get_name() +
                           " with hashes: " + std::to_string(hash.get64()) +
                           " with ptr: " + std::to_string(source_column->size()));
            res.push_back(data);
        }
        check_res_file("update_siphashes_hashes", res);
    }

    ////////// =================== column serde interface assert (7)=================== //////////
    //serialize and deserialize which usually used in AGG function:
    //  serialize_value_into_arena, deserialize_and_insert_from_arena (called by AggregateFunctionDistinctMultipleGenericData, group_array_intersect, nested-types serder like: DataTypeArraySerDe::write_one_cell_to_jsonb)
    void ser_deserialize_with_arena_impl(MutableColumns& columns, const DataTypes& data_types) {
        size_t rows = columns[0]->size();
        for (auto& column : columns) {
            if (column->size() > rows) {
                LOG(ERROR) << "Column size mismatch: " << column->size() << " vs " << rows;
                column->pop_back(column->size() - rows);
            } else if (column->size() < rows) {
                LOG(ERROR) << "Column size mismatch: " << column->size() << " vs " << rows;
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
                checkColumn(*columns[i], *argument_columns[i], columns[0]->size());
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
                LOG(ERROR) << "Column size mismatch: " << column->size() << " vs " << rows;
                column->pop_back(column->size() - rows);
            } else if (column->size() < rows) {
                LOG(ERROR) << "Column size mismatch: " << column->size() << " vs " << rows;
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
            LOG(INFO) << "max_one_row_byte_size : " << max_one_row_byte_size;
            for (const auto& column : columns) {
                LOG(INFO) << "now serialize_vec for column:" << column->get_name()
                          << " with column size: " << column->size();
                column->serialize_vec(input_keys.data(), rows);
            }
        }
        // deserialize the keys from arena into columns
        {
            // step4. deserialize the keys from arena into columns
            for (auto& column : check_columns) {
                column->deserialize_vec(input_keys.data(), rows);
            }
        }
        // check the deserialized columns
        {
            for (size_t i = 0; i < columns.size(); ++i) {
                EXPECT_EQ(columns[i]->size(), check_columns[i]->size());
                EXPECT_EQ(columns[i]->size(), check_columns[i]->size());
                checkColumn(*columns[i], *check_columns[i], rows);
            }
        }
    }

    ////////// =================== column convert interface assert (5)=================== //////////
    // convert_to_full_column_if_const in ColumnConst will expand the column, if not return itself ptr
    static void assert_convert_to_full_column_if_const_callback(
            MutableColumns& load_cols, DataTypes typs, std::function<void(ColumnPtr)> assert_func) {
        // Create an empty column to verify `convert_to_full_column_if_const` functionality
        auto option = DataTypeSerDe::FormatOptions();
        for (size_t i = 0; i < load_cols.size(); ++i) {
            auto& source_column = load_cols[i];
            auto assert_column = source_column->clone_empty();
            LOG(INFO) << "now we are in convert_to_full_column_if_const column : "
                      << load_cols[i]->get_name() << " for column size : " << source_column->size();
            auto ptr = source_column->convert_to_full_column_if_const();
            if (is_column<ColumnConst>(*source_column)) {
                // now we should check the ptr is not the same with source_column,we create a new column for the const column
                EXPECT_NE(ptr.get(), source_column.get());
                assert_func(ptr);
            } else {
                EXPECT_EQ(ptr.get(), source_column.get());
                // check the column ptr is the same as the source column
                checkColumn(*source_column, *ptr, source_column->size());
            }
        }
    }
    // convert_column_if_overflow just used in ColumnStr or nested columnStr
    static void assert_convert_column_if_overflow_callback(MutableColumns& load_cols,
                                                           DataTypeSerDeSPtrs serders) {
        // Create an empty column to verify `convert_column_if_overflow` functionality
        auto option = DataTypeSerDe::FormatOptions();
        std::vector<std::vector<std::string>> res;
        for (size_t i = 0; i < load_cols.size(); ++i) {
            auto& source_column = load_cols[i];
            auto assert_column = source_column->clone_empty();
            LOG(INFO) << "now we are in convert_column_if_overflow column : "
                      << load_cols[i]->get_name() << " for column size : " << source_column->size();
            auto ptr = source_column->convert_column_if_overflow();
            // check after convert_column_if_overflow: 1 in selector present the load cols data is selected and data should be default value
            auto ser_col = ColumnString::create();
            ser_col->reserve(ptr->size());
            VectorBufferWriter buffer_writer(*ser_col.get());
            std::vector<std::string> data;
            data.push_back(
                    "column: " + ptr->get_name() +
                    " with convert_column_if_overflow with ptr: " + std::to_string(ptr->size()));
            for (size_t j = 0; j < ptr->size(); ++j) {
                if (auto st =
                            serders[i]->serialize_one_cell_to_json(*ptr, j, buffer_writer, option);
                    !st) {
                    LOG(ERROR) << "Failed to serialize column " << i << " at row " << j;
                    break;
                }
                buffer_writer.commit();
                std::string actual_str_value = ser_col->get_data_at(j).to_string();
                data.push_back(actual_str_value);
            }
            res.push_back(data);
        }
        check_res_file("convert_column_if_overflow", res);
    }

    // column_dictionary funcs
    // is_column_dictionary
    // convert_to_predicate_column_if_dictionary
    // If column isn't ColumnDictionary, return itself. Otherwise, transforms is to predicate column.
    static void assert_convert_to_predicate_column_if_dictionary_callback(
            MutableColumns& load_cols, DataTypes typs, std::function<void(IColumn*)> assert_func) {
        // Create an empty column to verify `convert_to_predicate_column_if_dictionary` functionality
        auto option = DataTypeSerDe::FormatOptions();
        std::vector<std::vector<std::string>> res;
        for (size_t i = 0; i < load_cols.size(); ++i) {
            auto& source_column = load_cols[i];
            LOG(INFO) << "now we are in convert_to_predicate_column_if_dictionary column : "
                      << load_cols[i]->get_name() << " for column size : " << source_column->size();
            auto ptr = source_column->convert_to_predicate_column_if_dictionary();
            if (source_column->is_column_dictionary()) {
                // in dictionary column, we should do some check staff.
                EXPECT_NE(ptr.get(), source_column.get());
                assert_func(ptr.get());
            } else {
                // just check the column ptr is the same as the source column and res
                EXPECT_EQ(ptr.get(), source_column.get());
                checkColumn(*source_column, *ptr, source_column->size());
            }
        }
    }

    // convert_dict_codes_if_necessary just used in ColumnDictionary
    // ColumnDictionary and is a range comparison predicate, will convert dict encoding
    static void assert_convert_dict_codes_if_necessary_callback(
            MutableColumns& load_cols, std::function<void(IColumn*, size_t)> assert_func) {
        for (size_t i = 0; i < load_cols.size(); ++i) {
            auto& source_column = load_cols[i];
            LOG(INFO) << "now we are in convert_to_predicate_column_if_dictionary column : "
                      << load_cols[i]->get_name() << " for column size : " << source_column->size();
            EXPECT_NO_FATAL_FAILURE(source_column->convert_dict_codes_if_necessary());
            assert_func(source_column.get(), i);
        }
    }

    ////////// =================== column data other interface assert =================== //////////
    // column_nullable functions
    // only_null ; is_null_at ; is_nullable ; has_null ; has_null(size_t) ;
    static void assert_column_nullable_funcs(MutableColumns& load_cols,
                                             std::function<void(IColumn*)> assert_func) {
        for (size_t i = 0; i < load_cols.size(); ++i) {
            auto& source_column = load_cols[i];
            LOG(INFO) << "now we are in column_nullable_funcs column : " << load_cols[i]->get_name()
                      << " for column size : " << source_column->size();
            if (source_column->is_nullable() || is_column<ColumnConst>(*source_column)) {
                if (source_column->size() == 1 && source_column->is_null_at(0)) {
                    EXPECT_EQ(source_column->only_null(), true);
                    EXPECT_EQ(source_column->has_null(), true);
                    EXPECT_EQ(source_column->has_null(0), true);
                } else {
                    EXPECT_EQ(source_column->only_null(), false);
                }
            } else {
                assert_func(source_column.get());
            }
        }
    }

    // column_string functions: is_column_string ; is_column_string64
    static void assert_column_string_funcs(MutableColumns& load_cols) {
        for (size_t i = 0; i < load_cols.size(); ++i) {
            auto& source_column = load_cols[i];
            LOG(INFO) << "now we are in column_string_funcs column : " << load_cols[i]->get_name()
                      << " for column size : " << source_column->size();
            if (is_column<ColumnString>(*source_column)) {
                EXPECT_EQ(source_column->is_column_string(), true);
                EXPECT_EQ(source_column->is_column_string64(), false);
            } else if (is_column<ColumnString64>(*source_column)) {
                EXPECT_EQ(source_column->is_column_string(), false);
                EXPECT_EQ(source_column->is_column_string64(), true);
            } else {
                EXPECT_EQ(source_column->is_column_string(), false);
                EXPECT_EQ(source_column->is_column_string64(), false);
            }
        }
    }

    // get_shrinked_column should only happened in char-type column or nested char-type column,
    // other column just return the origin column without any data changed, so check file content should be the same as the origin column
    //  just shrink the end zeros for char-type column which happened in segmentIterator
    //    eg. column_desc: char(6), insert into char(3), the char(3) will padding the 3 zeros at the end for writing to disk.
    //       but we select should just print the char(3) without the padding zeros
    //  limit and topN operation will trigger this function call
    void shrink_padding_chars_callback(MutableColumns& load_cols, DataTypeSerDeSPtrs serders) {
        auto option = DataTypeSerDe::FormatOptions();
        std::vector<std::vector<std::string>> res;
        for (size_t i = 0; i < load_cols.size(); i++) {
            auto& source_column = load_cols[i];
            LOG(INFO) << "now we are in shrink_padding_chars column : " << load_cols[i]->get_name()
                      << " for column size : " << source_column->size();
            source_column->shrink_padding_chars();
            // check after get_shrinked_column: 1 in selector present the load cols data is selected and data should be default value
            auto ser_col = ColumnString::create();
            ser_col->reserve(source_column->size());
            VectorBufferWriter buffer_writer(*ser_col.get());
            std::vector<std::string> data;
            data.push_back("column: " + source_column->get_name() +
                           " with shrinked column size: " + std::to_string(source_column->size()));
            for (size_t j = 0; j < source_column->size(); ++j) {
                if (auto st = serders[i]->serialize_one_cell_to_json(*source_column, j,
                                                                     buffer_writer, option);
                    !st) {
                    LOG(ERROR) << "Failed to serialize column " << i << " at row " << j;
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

    void assert_permutations_with_limit(const IColumn& column, const IColumn::Permutation& lhs,
                                        const IColumn::Permutation& rhs, size_t limit) {
        LOG(INFO) << "lhs size: " << lhs.size() << " rhs size: " << rhs.size()
                  << " limit: " << limit;
        if (limit == 0) {
            limit = lhs.size();
        }

        for (size_t i = 0; i < limit; ++i) {
            ASSERT_EQ(column.compare_at(lhs[i], rhs[i], column, -1), 0)
                    << "i: " << i << ", limit: " << limit;
        }
    }

    void assert_column_permutations2(vectorized::IColumn& column, DataTypePtr dataType) {
        IColumn::Permutation actual_permutation;
        IColumn::Permutation expected_permutation;

        size_t column_size = column.size();
        std::cout << "column size: " << column_size << std::endl;
        std::vector<size_t> limits = {
                0, 1, 2, 3, 5, 10, column_size - 1, column_size, column_size + 1};
        for (auto limit : limits) {
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
};
auto check_permute = [](const IColumn& column, const IColumn::Permutation& permutation,
                        size_t limit, size_t expected_size) {
    auto res_col = column.permute(permutation, limit);
    EXPECT_EQ(res_col->size(), expected_size);
    try {
        for (size_t j = 0; j < expected_size; ++j) {
            EXPECT_EQ(res_col->compare_at(j, permutation[j], column, -1), 0);
        }
    } catch (doris::Exception& e) {
        LOG(ERROR) << "Exception: " << e.what();
        // using field check
        for (size_t j = 0; j < expected_size; ++j) {
            Field r;
            Field l;
            column.get(permutation[j], r);
            res_col->get(j, l);
            EXPECT_EQ(r, l);
        }
    }
};
auto assert_column_vector_permute =
        [](MutableColumns& cols, size_t num_rows,
           bool stable_test = true /*some column does not support compare_at, should set false*/) {
            for (const auto& col : cols) {
                size_t expected_size = num_rows ? std::min(col->size(), num_rows) : col->size();
                if (stable_test) {
                    IColumn::Permutation permutation;
                    CommonColumnTest::stable_get_column_permutation(*col, true, col->size(), -1,
                                                                    permutation);
                    check_permute(*col, permutation, num_rows, expected_size);
                }
                {
                    IColumn::Permutation permutation(col->size());
                    std::iota(permutation.begin(), permutation.end(),
                              IColumn::Permutation::value_type(0));
                    std::random_device rd;
                    std::mt19937 g(rd());
                    std::shuffle(permutation.begin(), permutation.end(), g);
                    check_permute(*col, permutation, num_rows, expected_size);
                }
            }
        };
template <PrimitiveType PType>
auto assert_column_vector_has_enough_capacity_callback =
        [](auto x, const MutableColumnPtr& source_column) {
            auto src_size = source_column->size();
            auto assert_col = source_column->clone_empty();
            ASSERT_FALSE(assert_col->has_enough_capacity(*source_column));
            assert_col->reserve(src_size);
            ASSERT_TRUE(assert_col->has_enough_capacity(*source_column));
        };

template <PrimitiveType PType>
auto assert_column_vector_field_callback = [](auto x, const MutableColumnPtr& source_column) {
    using T = decltype(x);
    using ColumnVecType =
            std::conditional_t<IsDecimalNumber<T>, ColumnDecimal<PType>, ColumnVector<PType>>;
    auto* col_vec_src = assert_cast<ColumnVecType*>(source_column.get());
    auto src_size = source_column->size();
    auto assert_col = source_column->clone_empty();
    for (size_t i = 0; i != src_size; ++i) {
        Field f;
        source_column->get(i, f);
        assert_col->insert(f);
    }
    for (size_t i = 0; i != src_size; ++i) {
        Field f;
        assert_col->get(i, f);
        ASSERT_EQ(f.get<T>(), col_vec_src->get_element(i)) << f.get_type_name();
    }
};

template <PrimitiveType PType>
auto assert_column_vector_get_data_at_callback = [](auto x, const MutableColumnPtr& source_column) {
    using T = decltype(x);
    using ColumnVecType =
            std::conditional_t<IsDecimalNumber<T>, ColumnDecimal<PType>, ColumnVector<PType>>;
    auto* col_vec_src = assert_cast<ColumnVecType*>(source_column.get());
    const auto& col_raw_data = col_vec_src->get_data().data();
    auto src_size = source_column->size();
    for (size_t i = 0; i != src_size; ++i) {
        auto str_value = source_column->get_data_at(i).to_string();
        ASSERT_EQ(str_value, std::string((const char*)(col_raw_data + i), sizeof(T)));
    }
};

template <PrimitiveType PType>
auto assert_column_vector_insert_many_vals_callback = [](auto x,
                                                         const MutableColumnPtr& source_column) {
    std::vector<size_t> insert_vals_count = {0, 10, 1000};
    auto* col_vec_src = assert_cast<ColumnVector<PType>*>(source_column.get());
    auto src_size = source_column->size();
    std::vector<size_t> src_data_indices = {0, src_size, src_size - 1, (src_size + 1) >> 1};

    auto test_func = [&](size_t clone_count) {
        size_t actual_clone_count = std::min(clone_count, src_size);
        auto target_column = source_column->clone_resized(actual_clone_count);
        auto* col_vec_target = assert_cast<ColumnVector<PType>*>(target_column.get());
        for (auto pos = src_data_indices.begin(); pos < src_data_indices.end(); ++pos) {
            if (*pos >= src_size) {
                continue;
            }
            for (auto n : insert_vals_count) {
                col_vec_target->resize(actual_clone_count);
                col_vec_target->insert_many_vals(col_vec_src->get_element(*pos), n);
                EXPECT_EQ(col_vec_target->size(), actual_clone_count + n);
                size_t i = 0;
                for (; i < actual_clone_count; ++i) {
                    EXPECT_EQ(col_vec_target->get_element(i), col_vec_src->get_element(i));
                }
                for (; i < col_vec_target->size(); ++i) {
                    EXPECT_EQ(col_vec_target->get_element(i), col_vec_src->get_element(*pos));
                }
            }
        }
    };
    test_func(0);
    test_func(10);
};
template <PrimitiveType PType>
auto assert_column_vector_insert_from_callback = [](auto x, const MutableColumnPtr& source_column) {
    auto target_column = source_column->clone_empty();
    for (size_t j = 0; j < source_column->size(); ++j) {
        target_column->insert_from(*source_column, j);
    }
    ASSERT_EQ(target_column->size(), source_column->size());
    for (size_t j = 0; j < target_column->size(); ++j) {
        ASSERT_EQ(target_column->get_data_at(j), source_column->get_data_at(j));
    }
};
template <PrimitiveType PType>
auto assert_column_vector_insert_data_callback = [](auto x, const MutableColumnPtr& source_column) {
    auto target_column = source_column->clone_empty();
    for (size_t j = 0; j < source_column->size(); ++j) {
        const auto data_at = source_column->get_data_at(j);
        target_column->insert_data(data_at.data, data_at.size);
    }
    ASSERT_EQ(target_column->size(), source_column->size());
    for (size_t j = 0; j < target_column->size(); ++j) {
        ASSERT_EQ(target_column->get_data_at(j), source_column->get_data_at(j));
    }
};

template <PrimitiveType PType>
auto assert_column_vector_insert_many_from_callback = [](auto x,
                                                         const MutableColumnPtr& source_column) {
    using T = decltype(x);
    using ColumnType = std::conditional_t<
            std::is_same_v<T, ColumnString>, ColumnString,
            std::conditional_t<std::is_same_v<T, ColumnString64>, ColumnString64,
                               std::conditional_t<IsDecimalNumber<T>, ColumnDecimal<PType>,
                                                  ColumnVector<PType>>>>;
    std::vector<size_t> insert_vals_count = {0, 3, 10};
    auto* col_vec_src = assert_cast<ColumnType*>(source_column.get());
    auto src_size = source_column->size();
    std::vector<size_t> src_data_indices = {0, src_size, src_size - 1, (src_size + 1) >> 1};

    auto test_func = [&](size_t clone_count) {
        size_t actual_clone_count = std::min(clone_count, src_size);
        auto target_column = source_column->clone_resized(actual_clone_count);
        auto* col_vec_target = assert_cast<ColumnType*>(target_column.get());
        for (auto pos = src_data_indices.begin(); pos < src_data_indices.end(); ++pos) {
            if (*pos >= src_size) {
                continue;
            }
            for (auto n : insert_vals_count) {
                col_vec_target->resize(actual_clone_count);
                col_vec_target->insert_many_from(*source_column, *pos, n);
                EXPECT_EQ(col_vec_target->size(), actual_clone_count + n);
                size_t i = 0;
                for (; i < actual_clone_count; ++i) {
                    if constexpr (std::is_same_v<T, ColumnString> ||
                                  std::is_same_v<T, ColumnString64>) {
                        EXPECT_EQ(col_vec_target->get_data_at(i), col_vec_src->get_data_at(i));
                    } else {
                        EXPECT_EQ(col_vec_target->get_element(i), col_vec_src->get_element(i));
                    }
                }
                for (size_t j = 0; j < n; ++j, ++i) {
                    if constexpr (std::is_same_v<T, ColumnString> ||
                                  std::is_same_v<T, ColumnString64>) {
                        auto data_res = col_vec_target->get_data_at(i);
                        auto data_expect = col_vec_src->get_data_at(*pos);
                        if (data_res != data_expect) {
                            std::cout << "index " << i << ", data_res: " << data_res
                                      << " data_expect: " << data_expect << std::endl;
                        }
                        EXPECT_TRUE(col_vec_target->get_data_at(i) ==
                                    col_vec_src->get_data_at(*pos));
                    } else {
                        EXPECT_EQ(col_vec_target->get_element(i), col_vec_src->get_element(*pos));
                    }
                }
            }
        }
    };
    test_func(0);
    test_func(10);
};
template <PrimitiveType PType>
auto assert_column_vector_insert_indices_from_callback = [](auto x,
                                                            const MutableColumnPtr& source_column) {
    // Test case 1: Empty source column
    // Test case 2: Empty indices array
    // Test case 3: Normal case with multiple indices
    // Select elements in different order
    // Test case 4: Duplicate indices

    // Insert data from `load_cols` to `verify_columns` using `insert_indices_from`
    auto src_size = source_column->size();

    // Test case 1: Empty target column
    {
        auto target_column = source_column->clone_empty();
        std::vector<uint32_t> indices;

        // empty indices array
        target_column->insert_indices_from(*source_column, indices.data(), indices.data());
        EXPECT_EQ(target_column->size(), 0);
    }
    auto test_func = [&](size_t clone_count) {
        size_t actual_clone_count = std::min(clone_count, src_size);
        {
            auto target_column = source_column->clone_resized(actual_clone_count);
            // insert all elements from source column
            std::vector<uint32_t> indices(src_size);
            std::iota(indices.begin(), indices.end(), 0);
            target_column->insert_indices_from(*source_column, indices.data(),
                                               indices.data() + src_size);
            EXPECT_EQ(target_column->size(), actual_clone_count + indices.size());
            size_t j = 0;
            for (j = 0; j != actual_clone_count; ++j) {
                EXPECT_EQ(target_column->get_data_at(j), source_column->get_data_at(j));
            }
            for (size_t k = 0; j < actual_clone_count + indices.size(); ++j, ++k) {
                EXPECT_EQ(target_column->get_data_at(j), source_column->get_data_at(indices[k]));
            }
        }
        {
            // Normal case with random indices
            auto target_column = source_column->clone_resized(actual_clone_count);
            std::vector<uint32_t> indices(src_size);
            std::iota(indices.begin(), indices.end(), 0);
            std::random_device rd;
            std::mt19937 g(rd());
            std::shuffle(indices.begin(), indices.end(), g);
            target_column->insert_indices_from(*source_column, indices.data(),
                                               indices.data() + indices.size());
            EXPECT_EQ(target_column->size(), actual_clone_count + indices.size());
            size_t j = 0;
            for (j = 0; j != actual_clone_count; ++j) {
                EXPECT_EQ(target_column->get_data_at(j), source_column->get_data_at(j));
            }
            for (size_t k = 0; j < actual_clone_count + indices.size(); ++j, ++k) {
                EXPECT_EQ(target_column->get_data_at(j), source_column->get_data_at(indices[k]));
            }
        }
        {
            // Normal case with duplicate indices
            auto target_column = source_column->clone_resized(actual_clone_count);
            std::vector<uint32_t> indices = {0, uint32_t(source_column->size() - 1),
                                             uint32_t((source_column->size() + 1) >> 1),
                                             uint32_t(source_column->size() - 1), 0};
            target_column->insert_indices_from(*source_column, indices.data(),
                                               indices.data() + indices.size());
            EXPECT_EQ(target_column->size(), actual_clone_count + indices.size());
            size_t j = 0;
            for (j = 0; j != actual_clone_count; ++j) {
                EXPECT_EQ(target_column->get_data_at(j), source_column->get_data_at(j));
            }
            for (size_t k = 0; j < actual_clone_count + indices.size(); ++j, ++k) {
                EXPECT_EQ(target_column->get_data_at(j), source_column->get_data_at(indices[k]));
            }
        }
    };
    test_func(0);
    test_func(10);
};

template <PrimitiveType PType>
auto assert_column_vector_insert_range_of_integer_callback =
        [](auto x, const MutableColumnPtr& source_column) {
            using T = decltype(x);
            auto target_column = source_column->clone();
            auto src_size = source_column->size();
            auto* col_vec_target = assert_cast<ColumnVector<PType>*>(target_column.get());
            auto* col_vec_src = assert_cast<ColumnVector<PType>*>(source_column.get());
            if constexpr (std::is_same_v<T, Float32> || std::is_same_v<T, Float64>) {
                typename PrimitiveTypeTraits<PType>::ColumnItemType begin {0};
                typename PrimitiveTypeTraits<PType>::ColumnItemType end {11};
                EXPECT_THROW(col_vec_target->insert_range_of_integer(begin, end), Exception);
            } else {
                T begin {0};
                T end {11};
                col_vec_target->insert_range_of_integer(begin, end);
                size_t j = 0;
                for (; j < src_size; ++j) {
                    EXPECT_EQ(col_vec_target->get_element(j), col_vec_src->get_element(j));
                }
                for (size_t k = 0; j < col_vec_target->size(); ++j, ++k) {
                    EXPECT_EQ(col_vec_target->get_element(j), begin + k);
                }
            }
        };
template <PrimitiveType PType>
auto assert_column_vector_insert_many_fix_len_data_callback = [](auto x, const MutableColumnPtr&
                                                                                 source_column) {
    using T = decltype(x);
    using ColumnVecType =
            std::conditional_t<IsDecimalNumber<T>, ColumnDecimal<PType>, ColumnVector<PType>>;
    std::vector<size_t> insert_vals_count = {0, 10, 1000};
    auto* col_vec_src = assert_cast<ColumnVecType*>(source_column.get());
    auto src_size = source_column->size();
    std::vector<size_t> src_data_indices = {0, src_size - 1, (src_size + 1) >> 1};

    auto test_func = [&](size_t clone_count) {
        size_t actual_clone_count = std::min(clone_count, src_size);
        auto target_column = source_column->clone_resized(actual_clone_count);
        auto* col_vec_target = assert_cast<ColumnVecType*>(target_column.get());
        for (auto pos = src_data_indices.begin(); pos < src_data_indices.end(); ++pos) {
            if (*pos >= src_size) {
                continue;
            }
            for (auto n : insert_vals_count) {
                col_vec_target->resize(actual_clone_count);
                size_t actual_insert_count = std::min(n, src_size - *pos);
                col_vec_target->insert_many_fix_len_data(source_column->get_data_at(*pos).data,
                                                         actual_insert_count);
                auto target_size = col_vec_target->size();
                EXPECT_EQ(target_size, actual_clone_count + actual_insert_count);
                size_t i = 0;
                for (; i < actual_clone_count; ++i) {
                    EXPECT_EQ(col_vec_target->get_element(i), col_vec_src->get_element(i));
                }
                for (size_t j = *pos; i < target_size; ++i, ++j) {
                    EXPECT_EQ(col_vec_target->get_element(i), col_vec_src->get_element(j))
                            << col_vec_src->get_name() << ' ' << col_vec_target->get_name();
                }
            }
        }
    };
    test_func(0);
    test_func(10);
};
template <PrimitiveType PType>
auto assert_column_vector_insert_many_raw_data_callback = [](auto x, const MutableColumnPtr&
                                                                             source_column) {
    using T = decltype(x);
    using ColumnVecType =
            std::conditional_t<IsDecimalNumber<T>, ColumnDecimal<PType>, ColumnVector<PType>>;
    std::vector<size_t> insert_vals_count = {0, 10, 1000};
    auto* col_vec_src = assert_cast<ColumnVecType*>(source_column.get());
    auto src_size = source_column->size();
    std::vector<size_t> src_data_indices = {0, src_size - 1, (src_size + 1) >> 1};

    auto test_func = [&](size_t clone_count) {
        size_t actual_clone_count = std::min(clone_count, src_size);
        auto target_column = source_column->clone_resized(actual_clone_count);
        auto* col_vec_target = assert_cast<ColumnVecType*>(target_column.get());
        for (auto pos = src_data_indices.begin(); pos < src_data_indices.end(); ++pos) {
            if (*pos >= src_size) {
                continue;
            }
            for (auto n : insert_vals_count) {
                col_vec_target->resize(actual_clone_count);
                size_t actual_insert_count = std::min(n, src_size - *pos);
                col_vec_target->insert_many_raw_data(source_column->get_data_at(*pos).data,
                                                     actual_insert_count);
                auto target_size = col_vec_target->size();
                EXPECT_EQ(target_size, actual_clone_count + actual_insert_count);
                size_t i = 0;
                for (; i < actual_clone_count; ++i) {
                    EXPECT_EQ(col_vec_target->get_element(i), col_vec_src->get_element(i));
                }
                for (size_t j = *pos; i < target_size; ++i, ++j) {
                    EXPECT_EQ(col_vec_target->get_element(i), col_vec_src->get_element(j));
                }
            }
        }
    };
    test_func(0);
    test_func(10);
};

template <PrimitiveType PType>
auto assert_column_vector_insert_default_callback = [](auto x,
                                                       const MutableColumnPtr& source_column) {
    using T = decltype(x);
    using ColumnVecType = std::conditional_t<
            std::is_same_v<T, ColumnString>, ColumnString,
            std::conditional_t<std::is_same_v<T, ColumnString64>, ColumnString64,
                               std::conditional_t<IsDecimalNumber<T>, ColumnDecimal<PType>,
                                                  ColumnVector<PType>>>>;
    auto* col_vec_src = assert_cast<ColumnVecType*>(source_column.get());
    auto src_size = source_column->size();
    auto test_func = [&](size_t clone_count) {
        size_t actual_clone_count = std::min(clone_count, src_size);
        auto target_column = source_column->clone_resized(actual_clone_count);
        auto* col_vec_target = assert_cast<ColumnVecType*>(target_column.get());
        col_vec_target->insert_default();
        auto target_size = col_vec_target->size();
        EXPECT_EQ(target_size, actual_clone_count + 1);
        size_t i = 0;
        for (; i < actual_clone_count; ++i) {
            if constexpr (std::is_same_v<T, ColumnString> || std::is_same_v<T, ColumnString64>) {
                EXPECT_EQ(col_vec_target->get_data_at(i), col_vec_src->get_data_at(i));
            } else {
                EXPECT_EQ(col_vec_target->get_element(i), col_vec_src->get_element(i));
            }
        }
        if constexpr (std::is_same_v<T, ColumnString> || std::is_same_v<T, ColumnString64>) {
            EXPECT_EQ(col_vec_target->get_data_at(i).to_string(), "");
        } else if constexpr (PType == PrimitiveType::TYPE_DATEV2 ||
                             PType == PrimitiveType::TYPE_DATETIMEV2) {
            EXPECT_EQ(col_vec_target->get_element(i),
                      T(PrimitiveTypeTraits<PType>::CppType::FIRST_DAY.to_date_int_val()));
        } else if constexpr (PType == PrimitiveType::TYPE_DATE ||
                             PType == PrimitiveType::TYPE_DATETIME) {
            EXPECT_EQ(col_vec_target->get_element(i),
                      T(PrimitiveTypeTraits<PType>::CppType::FIRST_DAY));
        } else {
            EXPECT_EQ(col_vec_target->get_element(i), T {});
        }
    };
    test_func(0);
    test_func(10);
};

template <PrimitiveType PType>
auto assert_column_vector_insert_many_defaults_callback = [](auto x, const MutableColumnPtr&
                                                                             source_column) {
    using T = decltype(x);
    using ColumnVecType = std::conditional_t<
            std::is_same_v<T, ColumnString>, ColumnString,
            std::conditional_t<std::is_same_v<T, ColumnString64>, ColumnString64,
                               std::conditional_t<IsDecimalNumber<T>, ColumnDecimal<PType>,
                                                  ColumnVector<PType>>>>;
    std::vector<size_t> insert_vals_count = {0, 10, 1000};
    auto* col_vec_src = assert_cast<ColumnVecType*>(source_column.get());
    auto src_size = source_column->size();

    auto test_func = [&](size_t clone_count) {
        for (auto n : insert_vals_count) {
            size_t actual_clone_count = std::min(clone_count, src_size);
            auto target_column = source_column->clone_resized(actual_clone_count);
            auto* col_vec_target = assert_cast<ColumnVecType*>(target_column.get());
            col_vec_target->insert_many_defaults(n);
            auto target_size = col_vec_target->size();
            EXPECT_EQ(target_size, actual_clone_count + n);
            size_t i = 0;
            for (; i < actual_clone_count; ++i) {
                if constexpr (std::is_same_v<T, ColumnString> ||
                              std::is_same_v<T, ColumnString64>) {
                    EXPECT_EQ(col_vec_target->get_data_at(i), col_vec_src->get_data_at(i));
                } else {
                    EXPECT_EQ(col_vec_target->get_element(i), col_vec_src->get_element(i));
                }
            }
            for (; i < target_size; ++i) {
                if constexpr (std::is_same_v<T, ColumnString> ||
                              std::is_same_v<T, ColumnString64>) {
                    EXPECT_EQ(col_vec_target->get_data_at(i).to_string(), "");
                } else if constexpr (PType == PrimitiveType::TYPE_DATEV2 ||
                                     PType == PrimitiveType::TYPE_DATETIMEV2) {
                    EXPECT_EQ(col_vec_target->get_element(i),
                              T(PrimitiveTypeTraits<PType>::CppType::FIRST_DAY.to_date_int_val()));
                } else if constexpr (PType == PrimitiveType::TYPE_DATE ||
                                     PType == PrimitiveType::TYPE_DATETIME) {
                    EXPECT_EQ(col_vec_target->get_element(i),
                              T(PrimitiveTypeTraits<PType>::CppType::FIRST_DAY));
                } else {
                    EXPECT_EQ(col_vec_target->get_element(i), T {});
                }
            }
        }
    };
    test_func(0);
    test_func(10);
};

template <PrimitiveType PType>
auto assert_column_vector_get_bool_callback = [](auto x, const MutableColumnPtr& source_column) {
    using T = decltype(x);
    using ColumnVecType =
            std::conditional_t<IsDecimalNumber<T>, ColumnDecimal<PType>, ColumnVector<PType>>;
    auto src_size = source_column->size();
    auto* col_vec_src = assert_cast<ColumnVecType*>(source_column.get());
    const auto& data = col_vec_src->get_data();
    for (size_t i = 0; i != src_size; ++i) {
        EXPECT_EQ(col_vec_src->get_bool(i), (bool)data[i]);
    }
};
template <PrimitiveType PType>
auto assert_column_vector_get_int64_callback = [](auto x, const MutableColumnPtr& source_column) {
    using T = decltype(x);
    using ColumnVecType =
            std::conditional_t<IsDecimalNumber<T>, ColumnDecimal<PType>, ColumnVector<PType>>;
    auto src_size = source_column->size();
    auto* col_vec_src = assert_cast<ColumnVecType*>(source_column.get());
    const auto& data = col_vec_src->get_data();
    for (size_t i = 0; i != src_size; ++i) {
        if constexpr (IsDecimalNumber<T>) {
            EXPECT_EQ(col_vec_src->get_int(i), (Int64)(data[i].value * col_vec_src->get_scale()));
        } else {
            EXPECT_EQ(col_vec_src->get_int(i), (Int64)data[i]);
        }
    }
};
template <PrimitiveType PType>
auto assert_column_vector_insert_range_from_common = [](auto x,
                                                        const MutableColumnPtr& source_column,
                                                        bool ignore_overflow = false) {
    using T = decltype(x);
    using ColumnVecType = std::conditional_t<
            std::is_same_v<T, ColumnString>, ColumnString,
            std::conditional_t<std::is_same_v<T, ColumnString64>, ColumnString64,
                               std::conditional_t<IsDecimalNumber<T>, ColumnDecimal<PType>,
                                                  ColumnVector<PType>>>>;
    std::vector<size_t> insert_vals_count = {0, 10, 1000};
    auto* col_vec_src = assert_cast<ColumnVecType*>(source_column.get());
    auto src_size = source_column->size();
    std::vector<size_t> src_data_indices = {0, src_size - 1, (src_size + 1) >> 1};

    auto test_func = [&](size_t clone_count) {
        size_t actual_clone_count = std::min(clone_count, src_size);
        auto target_column = source_column->clone_resized(actual_clone_count);
        auto* col_vec_target = assert_cast<ColumnVecType*>(target_column.get());
        for (auto pos = src_data_indices.begin(); pos < src_data_indices.end(); ++pos) {
            if (*pos >= src_size) {
                continue;
            }
            for (auto n : insert_vals_count) {
                col_vec_target->resize(actual_clone_count);
                size_t actual_insert_count = std::min(n, src_size - *pos);
                if (ignore_overflow) {
                    col_vec_target->insert_range_from_ignore_overflow(*source_column, *pos,
                                                                      actual_insert_count);
                } else {
                    col_vec_target->insert_range_from(*source_column, *pos, actual_insert_count);
                }
                auto target_size = col_vec_target->size();
                EXPECT_EQ(target_size, actual_clone_count + actual_insert_count);
                size_t i = 0;
                for (; i < actual_clone_count; ++i) {
                    if constexpr (std::is_same_v<T, ColumnString> ||
                                  std::is_same_v<T, ColumnString64>) {
                        EXPECT_EQ(col_vec_target->get_data_at(i), col_vec_src->get_data_at(i));
                    } else {
                        EXPECT_EQ(col_vec_target->get_element(i), col_vec_src->get_element(i));
                    }
                }
                for (size_t j = *pos; i < target_size; ++i, ++j) {
                    if constexpr (std::is_same_v<T, ColumnString> ||
                                  std::is_same_v<T, ColumnString64>) {
                        EXPECT_EQ(col_vec_target->get_data_at(i), col_vec_src->get_data_at(j));
                    } else {
                        EXPECT_EQ(col_vec_target->get_element(i), col_vec_src->get_element(j));
                    }
                }
            }
        }
    };
    test_func(0);
    test_func(10);

    auto target_column = source_column->clone_empty();
    if (ignore_overflow) {
        EXPECT_THROW(
                target_column->insert_range_from_ignore_overflow(*source_column, 0, src_size + 1),
                Exception);
    } else {
        EXPECT_THROW(target_column->insert_range_from(*source_column, 0, src_size + 1), Exception);
    }
};
template <PrimitiveType PType>
auto assert_column_vector_insert_range_from_callback =
        [](auto x, const MutableColumnPtr& source_column) {
            assert_column_vector_insert_range_from_common<PType>(std::forward<decltype(x)>(x),
                                                                 source_column);
        };
template <PrimitiveType PType>
auto assert_column_vector_insert_range_from_ignore_overflow_callback =
        [](auto x, const MutableColumnPtr& source_column) {
            assert_column_vector_insert_range_from_common<PType>(std::forward<decltype(x)>(x),
                                                                 source_column, true);
        };
template <PrimitiveType PType>
auto assert_column_vector_pop_back_callback = [](auto x, const MutableColumnPtr& source_column) {
    using T = decltype(x);
    using ColumnType = std::conditional_t<
            std::is_same_v<T, ColumnString>, ColumnString,
            std::conditional_t<std::is_same_v<T, ColumnString64>, ColumnString64,
                               std::conditional_t<IsDecimalNumber<T>, ColumnDecimal<PType>,
                                                  ColumnVector<PType>>>>;
    auto src_size = source_column->size();
    auto* col_vec_src = assert_cast<ColumnType*>(source_column.get());
    std::vector<size_t> pop_back_count = {0, src_size - 1, (src_size + 1) >> 1};
    for (auto n : pop_back_count) {
        auto target_column = source_column->clone();
        target_column->pop_back(n);
        EXPECT_EQ(target_column->size(), src_size - n);
        auto* col_vec_target = assert_cast<ColumnType*>(target_column.get());
        for (size_t i = 0; i < target_column->size(); ++i) {
            if constexpr (std::is_same_v<T, ColumnString> || std::is_same_v<T, ColumnString64>) {
                auto data_res = col_vec_target->get_data_at(i);
                auto data_expect = col_vec_src->get_data_at(i);
                if (data_res != data_expect) {
                    std::cout << "index " << i << ", data_res: " << data_res
                              << " data_expect: " << data_expect << std::endl;
                }
                EXPECT_TRUE(data_res == data_expect);
            } else {
                EXPECT_EQ(col_vec_target->get_element(i), col_vec_src->get_element(i));
            }
        }
    }
};
template <PrimitiveType PType>
auto assert_column_vector_filter_callback = [](auto x, const MutableColumnPtr& source_column) {
    using T = decltype(x);
    using ColumnVecType = std::conditional_t<
            std::is_same_v<T, ColumnString>, ColumnString,
            std::conditional_t<std::is_same_v<T, ColumnString64>, ColumnString64,
                               std::conditional_t<IsDecimalNumber<T>, ColumnDecimal<PType>,
                                                  ColumnVector<PType>>>>;
    auto source_size = source_column->size();
    auto* col_vec_src = assert_cast<ColumnVecType*>(source_column.get());
    IColumn::Filter all_filtered(source_size, 0);
    IColumn::Filter no_filtered(source_size, 1);
    IColumn::Filter normal_filter(source_size, 1);
    normal_filter[0] = 0;
    normal_filter[source_size - 1] = 0;
    normal_filter[source_size / 2] = 0;
    std::vector<IColumn::Filter*> filters = {&all_filtered, &no_filtered, &normal_filter};
    auto test_func = [&](const IColumn::Filter* filter) {
        const auto* filter_data = (const int8_t*)filter->data();
        auto expected_size = filter->size() - simd::count_zero_num(filter_data, filter->size());
        {
            // empty column
            auto target_column = source_column->clone_empty();
            IColumn::Filter tmp_filter;
            auto ptr = target_column->filter(tmp_filter, expected_size);
            EXPECT_EQ(ptr->size(), 0);
        }
        auto target_column = source_column->clone();
        auto ptr = target_column->filter(*filter, expected_size);
        const auto* col_vec_target_filtered = assert_cast<const ColumnVecType*>(ptr.get());
        EXPECT_EQ(ptr->size(), expected_size);
        // check filter result is right
        for (size_t i = 0, find_pos = 0; i < expected_size; ++i, ++find_pos) {
            find_pos = simd::find_byte(filter_data, find_pos, filter->size(), (int8_t)1);
            EXPECT_TRUE(find_pos < filter->size());
            if constexpr (std::is_same_v<T, ColumnString> || std::is_same_v<T, ColumnString64>) {
                EXPECT_EQ(col_vec_target_filtered->get_data_at(i),
                          col_vec_src->get_data_at(find_pos));
            } else {
                EXPECT_EQ(col_vec_target_filtered->get_element(i),
                          col_vec_src->get_element(find_pos));
            }
        }

        // filter will modify the original column
        {
            // empty filter
            auto target_column = source_column->clone_empty();
            IColumn::Filter tmp_filter;
            auto res_size = target_column->filter(tmp_filter);
            EXPECT_EQ(res_size, 0);
            EXPECT_EQ(target_column->size(), 0);
        }
        auto result_size = target_column->filter(*filter);
        EXPECT_EQ(result_size, expected_size);
        col_vec_target_filtered = assert_cast<const ColumnVecType*>(target_column.get());
        for (size_t i = 0, find_pos = 0; i < expected_size; ++i, ++find_pos) {
            find_pos = simd::find_byte(filter_data, find_pos, filter->size(), (int8_t)1);
            EXPECT_TRUE(find_pos < filter->size());
            if constexpr (std::is_same_v<T, ColumnString> || std::is_same_v<T, ColumnString64>) {
                EXPECT_EQ(col_vec_target_filtered->get_data_at(i),
                          col_vec_src->get_data_at(find_pos));
            } else {
                EXPECT_EQ(col_vec_target_filtered->get_element(i),
                          col_vec_src->get_element(find_pos));
            }
        }
    };
    for (const auto& filter : filters) {
        test_func(filter);
    }
};
template <PrimitiveType PType>
auto assert_column_vector_replace_column_data_callback = [](auto x,
                                                            const MutableColumnPtr& source_column) {
    using T = decltype(x);
    using ColumnVecType =
            std::conditional_t<IsDecimalNumber<T>, ColumnDecimal<PType>, ColumnVector<PType>>;
    auto* col_vec_src = assert_cast<ColumnVecType*>(source_column.get());
    auto src_size = source_column->size();
    std::vector<size_t> self_data_indices = {0, src_size - 1};
    std::vector<size_t> other_data_indices = {src_size - 1, 0};

    auto target_column = source_column->clone();
    auto* col_vec_target = assert_cast<ColumnVecType*>(target_column.get());
    for (size_t i = 0; i < self_data_indices.size(); ++i) {
        if (self_data_indices[i] >= src_size || other_data_indices[i] >= src_size) {
            continue;
        }
        target_column->replace_column_data(*source_column, other_data_indices[i],
                                           self_data_indices[i]);
    }
    for (size_t i = 0; i < src_size; ++i) {
        bool is_replaced = false;
        for (size_t j = 0; j < self_data_indices.size(); ++j) {
            if (self_data_indices[j] >= src_size || other_data_indices[j] >= src_size) {
                continue;
            }
            if (i == self_data_indices[j]) {
                EXPECT_EQ(col_vec_target->get_element(i),
                          col_vec_src->get_element(other_data_indices[j]));
                is_replaced = true;
                break;
            }
        }
        if (!is_replaced) {
            EXPECT_EQ(col_vec_target->get_element(i), col_vec_src->get_element(i));
        }
    }
};

template <PrimitiveType PType>
auto assert_column_vector_replace_column_null_data_callback = [](auto x, const MutableColumnPtr&
                                                                                 source_column) {
    using T = decltype(x);
    using ColumnVecType =
            std::conditional_t<IsDecimalNumber<T>, ColumnDecimal<PType>, ColumnVector<PType>>;
    auto* col_vec_src = assert_cast<ColumnVecType*>(source_column.get());
    auto src_size = source_column->size();
    // no null data
    std::vector<UInt8> null_map(src_size, 0);
    auto target_column = source_column->clone();
    target_column->replace_column_null_data(null_map.data());
    std::vector<size_t> null_val_indices = {0, src_size - 1, src_size / 2};
    for (auto n : null_val_indices) {
        null_map[n] = 1;
    }
    auto* col_vec_target = assert_cast<ColumnVecType*>(target_column.get());
    target_column->replace_column_null_data(null_map.data());
    for (size_t i = 0; i < src_size; ++i) {
        if (null_map[i] == 1) {
            if constexpr (IsDecimalNumber<T>) {
                EXPECT_EQ(col_vec_target->get_element(i), T {});
            } else {
                EXPECT_EQ(col_vec_target->get_element(i), ColumnVecType::default_value());
            }
            continue;
        }
        EXPECT_EQ(col_vec_target->get_element(i), col_vec_src->get_element(i));
    }
};

template <PrimitiveType PType>
auto assert_column_vector_compare_internal_callback = [](auto x,
                                                         const MutableColumnPtr& source_column) {
    auto col_cloned = source_column->clone();
    size_t num_rows = col_cloned->size();
    IColumn::Permutation permutation;
    col_cloned->get_permutation(false, 0, 1, permutation);
    auto col_clone_sorted = col_cloned->permute(permutation, 0);

    auto test_func = [&](int direction) {
        std::vector<uint32_t> indices(num_rows);
        std::iota(indices.begin(), indices.end(), 0);
        std::random_device rd;
        std::mt19937 g(rd());
        std::shuffle(indices.begin(), indices.end(), g);
        size_t check_count = std::min(num_rows, 10UL);
        indices.resize(check_count);
        for (auto i : indices) {
            IColumn::Filter filter(num_rows, 0);
            for (size_t i = 0; i < num_rows; ++i) {
                filter[i] = 0;
            }
            std::vector<uint8_t> cmp_res(num_rows, 0);
            // compare row i with all rows of col_clone_sorted
            col_clone_sorted->compare_internal(i, *col_clone_sorted, 1, direction, cmp_res,
                                               filter.data());
            for (size_t j = 0; j < num_rows; ++j) {
                auto row_comp_res = col_clone_sorted->compare_at(j, i, *col_clone_sorted, 1);
                if (row_comp_res != 0) { // not equal
                    EXPECT_EQ(cmp_res[j], 1);
                } else {
                    EXPECT_EQ(cmp_res[j], 0);
                }
                if (1 == direction) {
                    if (j >= i) {
                        EXPECT_EQ(filter[j], 0);
                    } else {
                        if (row_comp_res != 0) {
                            EXPECT_EQ(filter[j], 1);
                        }
                    }
                } else if (-1 == direction) {
                    if (j <= i) {
                        EXPECT_EQ(filter[j], 0);
                    } else {
                        if (row_comp_res != 0) {
                            EXPECT_EQ(filter[j], 1);
                        }
                    }
                }
            }
        }
    };
    test_func(1);
    test_func(-1);
};
template <PrimitiveType PType>
auto assert_column_vector_get_max_row_byte_size_callback =
        [](auto x, const MutableColumnPtr& source_column) {
            using T = decltype(x);
            EXPECT_EQ(source_column->get_max_row_byte_size(), sizeof(T));
        };

template <PrimitiveType PType>
auto assert_column_vector_clone_resized_callback = [](auto x,
                                                      const MutableColumnPtr& source_column) {
    using T = decltype(x);
    using ColumnVecType = std::conditional_t<
            std::is_same_v<T, ColumnString>, ColumnString,
            std::conditional_t<std::is_same_v<T, ColumnString64>, ColumnString64,
                               std::conditional_t<IsDecimalNumber<T>, ColumnDecimal<PType>,
                                                  ColumnVector<PType>>>>;
    auto* col_vec_src = assert_cast<ColumnVecType*>(source_column.get());
    auto src_size = source_column->size();
    auto test_func = [&](size_t clone_count) {
        auto target_column = source_column->clone_resized(clone_count);
        EXPECT_EQ(target_column->size(), clone_count);
        size_t same_count = std::min(clone_count, src_size);
        auto* col_vec_target = assert_cast<ColumnVecType*>(target_column.get());
        size_t i = 0;
        for (; i < same_count; ++i) {
            if constexpr (std::is_same_v<T, ColumnString> || std::is_same_v<T, ColumnString64>) {
                EXPECT_EQ(col_vec_target->get_data_at(i), col_vec_src->get_data_at(i));
            } else {
                EXPECT_EQ(col_vec_target->get_element(i), col_vec_src->get_element(i));
            }
        }
        for (; i < clone_count; ++i) {
            if constexpr (std::is_same_v<T, ColumnString> || std::is_same_v<T, ColumnString64>) {
                EXPECT_EQ(col_vec_target->get_data_at(i).to_string(), "");
            } else if constexpr (IsDecimalNumber<T>) {
                EXPECT_EQ(col_vec_target->get_element(i), T {});
            } else {
                EXPECT_EQ(col_vec_target->get_element(i), ColumnVecType::default_value());
            }
        }
    };
    test_func(0);
    test_func(3);
    test_func(src_size);
    test_func(src_size + 10);
};

template <PrimitiveType PType>
auto assert_column_vector_serialize_vec_callback = [](auto x,
                                                      const MutableColumnPtr& source_column) {
    using T = decltype(x);
    using ColumnVecType = std::conditional_t<
            std::is_same_v<T, ColumnString>, ColumnString,
            std::conditional_t<std::is_same_v<T, ColumnString64>, ColumnString64,
                               std::conditional_t<IsDecimalNumber<T>, ColumnDecimal<PType>,
                                                  ColumnVector<PType>>>>;
    size_t rows = source_column->size();
    {
        // test with null map, but no null values
        auto null_col = ColumnUInt8::create(rows, 0);
        auto cloned_target_column = source_column->clone();
        size_t max_one_row_byte_size =
                cloned_target_column->get_max_row_byte_size() + sizeof(NullMap::value_type);
        size_t memory_size = max_one_row_byte_size * rows;
        Arena arena(memory_size);
        auto* serialized_key_buffer = reinterpret_cast<uint8_t*>(arena.alloc(memory_size));

        std::vector<StringRef> input_keys;
        input_keys.resize(rows);
        for (size_t i = 0; i < rows; ++i) {
            input_keys[i].data =
                    reinterpret_cast<char*>(serialized_key_buffer + i * max_one_row_byte_size);
            input_keys[i].size = 0;
        }
        auto wrapper = ColumnNullable::create(std::move(cloned_target_column), std::move(null_col));
        auto target_column = wrapper->get_nested_column_ptr();
        wrapper->serialize_vec(input_keys.data(), rows);
        auto deser_column_wrapper = wrapper->clone_empty();
        deser_column_wrapper->deserialize_vec(input_keys.data(), rows);
        EXPECT_EQ(deser_column_wrapper->size(), rows);
        auto* col_vec_deser =
                assert_cast<ColumnVecType*>(assert_cast<ColumnNullable*>(deser_column_wrapper.get())
                                                    ->get_nested_column_ptr()
                                                    .get());
        auto* col_vec_target = assert_cast<ColumnVecType*>(target_column.get());
        for (size_t i = 0; i < rows; ++i) {
            if constexpr (std::is_same_v<T, ColumnString> || std::is_same_v<T, ColumnString64>) {
                EXPECT_EQ(col_vec_deser->get_data_at(i), col_vec_target->get_data_at(i));
            } else {
                EXPECT_EQ(col_vec_deser->get_element(i), col_vec_target->get_element(i));
            }
        }
    }

    auto test_func = [&](bool test_null_map) {
        MutableColumnPtr cloned_target_column;
        MutableColumnPtr target_column;
        ColumnVecType* col_vec_target = nullptr;

        auto null_col = ColumnUInt8::create(rows, 0);
        auto& null_map = null_col->get_data();
        if (test_null_map) {
            std::vector<size_t> null_positions(rows);
            std::iota(null_positions.begin(), null_positions.end(), 0);
            std::random_device rd;
            std::mt19937 g(rd());
            std::shuffle(null_positions.begin(), null_positions.end(), g);
            // make random half of the rows to be null
            null_positions.resize(rows / 2);

            for (const auto& pos : null_positions) {
                null_map[pos] = 1;
            }
            if constexpr (std::is_same_v<T, ColumnString> || std::is_same_v<T, ColumnString64>) {
                cloned_target_column = source_column->clone_empty();
                for (size_t i = 0; i != rows; ++i) {
                    if (null_map[i]) {
                        cloned_target_column->insert_default();
                    } else {
                        cloned_target_column->insert_from(*source_column, i);
                    }
                }
            } else {
                cloned_target_column = source_column->clone();
                cloned_target_column->replace_column_null_data(null_map.data());
            }
            cloned_target_column =
                    ColumnNullable::create(std::move(cloned_target_column), std::move(null_col));
            target_column = ((ColumnNullable*)cloned_target_column.get())->get_nested_column_ptr();
        } else {
            cloned_target_column = source_column->clone();
            if (cloned_target_column->is_nullable()) {
                target_column =
                        ((ColumnNullable*)cloned_target_column.get())->get_nested_column_ptr();
            } else {
                target_column = std::move(cloned_target_column);
            }
        }
        col_vec_target = assert_cast<ColumnVecType*>(target_column.get());

        size_t max_one_row_byte_size = target_column->get_max_row_byte_size();
        if (test_null_map) {
            max_one_row_byte_size += sizeof(NullMap::value_type);
        }
        size_t memory_size = max_one_row_byte_size * rows;
        Arena arena(memory_size);
        auto* serialized_key_buffer = reinterpret_cast<uint8_t*>(arena.alloc(memory_size));

        std::vector<StringRef> input_keys;
        input_keys.resize(rows);
        for (size_t i = 0; i < rows; ++i) {
            input_keys[i].data =
                    reinterpret_cast<char*>(serialized_key_buffer + i * max_one_row_byte_size);
            input_keys[i].size = 0;
        }
        MutableColumnPtr deser_column;
        MutableColumnPtr deser_column_wrapper;
        if (test_null_map) {
            cloned_target_column->serialize_vec(input_keys.data(), rows);
            deser_column_wrapper = cloned_target_column->clone_empty();
            deser_column = ((ColumnNullable*)deser_column_wrapper.get())->get_nested_column_ptr();
        } else {
            target_column->serialize_vec(input_keys.data(), rows);
            deser_column = source_column->clone_empty();
        }
        if (test_null_map) {
            deser_column_wrapper->deserialize_vec(input_keys.data(), rows);
        } else {
            deser_column->deserialize_vec(input_keys.data(), rows);
        }
        EXPECT_EQ(deser_column->size(), rows);
        auto* col_vec_deser = assert_cast<ColumnVecType*>(deser_column.get());
        for (size_t i = 0; i < rows; ++i) {
            if constexpr (std::is_same_v<T, ColumnString> || std::is_same_v<T, ColumnString64>) {
                EXPECT_EQ(col_vec_deser->get_data_at(i), col_vec_target->get_data_at(i));
            } else {
                EXPECT_EQ(col_vec_deser->get_element(i), col_vec_target->get_element(i));
            }
        }
    };
    test_func(true);
    test_func(false);
};

template <PrimitiveType PType>
auto assert_sort_column_callback = [](auto x, const MutableColumnPtr& source_column) {
    std::vector<UInt64> limits = {0, 10, 100, 1000, 10000, 100000};
    std::vector<MutableColumnPtr> cloned_columns;
    size_t source_size = source_column->size();
    for (size_t i = 0; i != 2; ++i) {
        std::vector<uint32_t> indices(source_size);
        std::iota(indices.begin(), indices.end(), 0);
        std::random_device rd;
        std::mt19937 g(rd());
        std::shuffle(indices.begin(), indices.end(), g);
        indices.resize(source_size / 2);
        auto target_column = source_column->clone();
        target_column->insert_indices_from(*source_column, indices.data(),
                                           indices.data() + indices.size());
        target_column->insert_indices_from(*source_column, indices.data(),
                                           indices.data() + indices.size());
        cloned_columns.push_back(std::move(target_column));
    }
    auto cloned_column_row_count = cloned_columns[0]->size();
    for (auto orig_limit : limits) {
        auto sorted_row_count = cloned_column_row_count;
        auto limit = orig_limit;
        if (limit >= cloned_column_row_count) {
            limit = 0;
        } else {
            sorted_row_count = limit;
        }
        IColumn::Permutation perm(cloned_column_row_count);
        for (size_t i = 0; i < cloned_column_row_count; ++i) {
            perm[i] = i;
        }
        EqualFlags flags(cloned_column_row_count, 1);
        EqualRange range {0, cloned_column_row_count};
        for (size_t i = 0; i != cloned_columns.size(); ++i) {
            ColumnWithSortDescription column_with_sort_desc(cloned_columns[i].get(),
                                                            SortColumnDescription(i, 1, 0));
            ColumnSorter sorter(column_with_sort_desc, limit);
            cloned_columns[i]->sort_column(&sorter, flags, perm, range,
                                           i == cloned_columns.size() - 1);
        }
        auto col_sorted0 = cloned_columns[0]->permute(perm, limit);
        auto col_sorted1 = cloned_columns[1]->permute(perm, limit);
        for (size_t i = 1; i < sorted_row_count; ++i) {
            int compare_res = col_sorted0->compare_at(i, i - 1, *col_sorted0, 1);
            EXPECT_TRUE(compare_res >= 0);
            if (0 == compare_res) {
                auto compare_res2 = col_sorted1->compare_at(i, i - 1, *col_sorted1, 1);
                EXPECT_TRUE(compare_res2 >= 0);
            }
        }
    }
};
auto assert_column_vector_update_hashes_with_value_callback = [](const MutableColumns& load_cols,
                                                                 DataTypeSerDeSPtrs serders,
                                                                 const std::string& res_file_path) {
    // Create an empty column to verify `update_hashes_with_value` functionality
    // check update_hashes_with_value with different hashes
    auto test_func = [&](bool with_nullmap) {
        std::vector<std::vector<std::string>> res;

        for (size_t i = 0; i < load_cols.size(); ++i) {
            const auto& source_column = load_cols[i];
            size_t rows = source_column->size();
            NullMap null_map(rows, 0);
            const uint8_t* null_data = nullptr;
            if (with_nullmap) {
                null_data = null_map.data();
                std::vector<size_t> null_positions {0, rows - 1, rows / 2};
                for (const auto& pos : null_positions) {
                    null_map[pos] = 1;
                }
            }

            std::vector<uint64_t> xx_hash_vals(source_column->size());
            std::cout << "now we are in update_hashes_with_value column : "
                      << load_cols[i]->get_name() << " for column size : " << source_column->size()
                      << std::endl;
            auto* __restrict xx_hashes = xx_hash_vals.data();
            EXPECT_NO_FATAL_FAILURE(source_column->update_hashes_with_value(xx_hashes, null_data));
            // check after update_hashes_with_value: 1 in selector present the load cols data is selected and data should be default value
            std::vector<std::string> data;
            std::ostringstream oss;

            data.push_back("column: " + source_column->get_name() +
                           " with hashes: " + join_ints(xx_hash_vals));
            res.push_back(data);
        }
        std::string file_name = res_file_path.empty() ? "update_hashes_with_value" : res_file_path;
        file_name += with_nullmap ? "_with_nullmap" : "";
        check_or_generate_res_file(file_name, res);
    };
    test_func(false);
    test_func(true);
};
auto assert_column_vector_update_crc_hashes_callback = [](const MutableColumns& load_cols,
                                                          DataTypeSerDeSPtrs serders,
                                                          std::vector<PrimitiveType> pts,
                                                          const std::string& res_file_path) {
    // Create an empty column to verify `update_hashes` functionality
    // check update_hashes with different hashes
    auto test_func = [&](bool with_nullmap) {
        std::vector<std::vector<std::string>> res;
        for (size_t i = 0; i < load_cols.size(); ++i) {
            const auto& source_column = load_cols[i];
            size_t rows = source_column->size();
            NullMap null_map(rows, 0);
            const uint8_t* null_data = nullptr;
            if (with_nullmap) {
                null_data = null_map.data();
                std::vector<size_t> null_positions {0, rows - 1, rows / 2};
                for (const auto& pos : null_positions) {
                    null_map[pos] = 1;
                }
            }

            std::vector<uint32_t> crc_hash_vals(source_column->size());
            std::cout << "now we are in update_hashes column : " << load_cols[i]->get_name()
                      << " for column size : " << source_column->size() << std::endl;
            EXPECT_NO_FATAL_FAILURE(source_column->update_crcs_with_value(
                    crc_hash_vals.data(), pts[i], source_column->size(), 0, null_data));
            // check after update_hashes: 1 in selector present the load cols data is selected and data should be default value
            auto ser_col = ColumnString::create();
            ser_col->reserve(source_column->size());
            std::vector<std::string> data;
            data.push_back("column: " + source_column->get_name() +
                           " with hashes: " + join_ints(crc_hash_vals));
            res.push_back(data);
        }
        std::string file_name = res_file_path.empty() ? "update_crcs_hashes" : res_file_path;
        file_name += with_nullmap ? "_with_nullmap" : "";
        check_or_generate_res_file(file_name, res);
    };
    test_func(false);
    test_func(true);
};
auto assert_column_vector_update_siphashes_with_value_callback =
        [](const MutableColumns& load_cols, DataTypeSerDeSPtrs serders,
           const std::string& res_file_path) {
            // Create an empty column to verify `update_hashes` functionality
            // check update_hashes with different hashes
            std::vector<std::vector<std::string>> res;
            auto option = DataTypeSerDe::FormatOptions();
            for (size_t i = 0; i < load_cols.size(); ++i) {
                const auto& source_column = load_cols[i];
                SipHash hash;
                LOG(INFO) << "now we are in update_hashes column : " << load_cols[i]->get_name()
                          << " for column size : " << source_column->size();
                for (size_t j = 0; j < source_column->size(); ++j) {
                    source_column->update_hash_with_value(j, hash);
                }
                auto ser_col = ColumnString::create();
                ser_col->reserve(source_column->size());
                VectorBufferWriter buffer_writer(*ser_col.get());
                std::vector<std::string> data;
                data.push_back("column: " + source_column->get_name() +
                               " with hashes: " + std::to_string(hash.get64()) +
                               " with ptr: " + std::to_string(source_column->size()));
                res.push_back(data);
            }
            check_or_generate_res_file(
                    res_file_path.empty() ? "update_siphashes_hashes" : res_file_path, res);
        };
auto assert_update_xxHash_with_value_callback = [](const MutableColumns& load_cols,
                                                   DataTypeSerDeSPtrs serders,
                                                   const std::string& res_file_path) {
    // Create an empty column to verify `update_hashes` functionality
    // check update_hashes with different hashes
    auto test_func = [&](bool with_nullmap) {
        std::vector<std::vector<std::string>> res;
        for (size_t i = 0; i < load_cols.size(); ++i) {
            const auto& source_column = load_cols[i];
            size_t rows = source_column->size();

            NullMap null_map(rows, 0);
            const uint8_t* null_data = nullptr;
            if (with_nullmap) {
                null_data = null_map.data();
                std::vector<size_t> null_positions {0, rows - 1, rows / 2};
                for (const auto& pos : null_positions) {
                    null_map[pos] = 1;
                }
            }

            uint64_t hash = 0;
            source_column->update_xxHash_with_value(0, source_column->size(), hash, null_data);
            auto ser_col = ColumnString::create();
            ser_col->reserve(source_column->size());
            VectorBufferWriter buffer_writer(*ser_col.get());
            std::vector<std::string> data;
            data.push_back("column: " + source_column->get_name() +
                           " with hashes: " + std::to_string(hash) +
                           " with ptr: " + std::to_string(source_column->size()));
            res.push_back(data);
        }
        std::string file_name = res_file_path.empty() ? "update_xxHash_with_value" : res_file_path;
        file_name += with_nullmap ? "_with_nullmap" : "";
        check_or_generate_res_file(file_name, res);
    };
    test_func(false);
    test_func(true);
};
auto assert_update_crc_with_value_callback = [](const MutableColumns& load_cols,
                                                DataTypeSerDeSPtrs serders,
                                                const std::string& res_file_path) {
    // Create an empty column to verify `update_hashes` functionality
    // check update_hashes with different hashes
    auto test_func = [&](bool with_nullmap) {
        std::vector<std::vector<std::string>> res;
        auto option = DataTypeSerDe::FormatOptions();
        for (size_t i = 0; i < load_cols.size(); ++i) {
            const auto& source_column = load_cols[i];
            size_t rows = source_column->size();

            NullMap null_map(rows, 0);
            const uint8_t* null_data = nullptr;
            if (with_nullmap) {
                null_data = null_map.data();
                std::vector<size_t> null_positions {0, rows - 1, rows / 2};
                for (const auto& pos : null_positions) {
                    null_map[pos] = 1;
                }
            }

            uint32_t hash = 0;
            source_column->update_crc_with_value(0, source_column->size(), hash, null_data);
            auto ser_col = ColumnString::create();
            ser_col->reserve(source_column->size());
            VectorBufferWriter buffer_writer(*ser_col.get());
            std::vector<std::string> data;
            data.push_back("column: " + source_column->get_name() +
                           " with hashes: " + std::to_string(hash) +
                           " with ptr: " + std::to_string(source_column->size()));
            res.push_back(data);
        }
        std::string file_name = res_file_path.empty() ? "update_crc_with_value" : res_file_path;
        file_name += with_nullmap ? "_with_nullmap" : "";
        check_or_generate_res_file(file_name, res);
    };
    test_func(false);
    test_func(true);
};

auto assert_allocated_bytes_with_file_callback = [](const MutableColumns& load_cols,
                                                    DataTypeSerDeSPtrs serders,
                                                    const std::string& res_file_path) {
    // Create an empty column to verify `update_hashes` functionality
    // check update_hashes with different hashes
    auto test_func = [&](bool with_nullmap) {
        std::vector<std::vector<std::string>> res;
        for (size_t i = 0; i < load_cols.size(); ++i) {
            const auto& source_column = load_cols[i];

            std::vector<std::string> data;
            auto actual_str_value = std::to_string(source_column->allocated_bytes());
            data.push_back("column: " + source_column->get_name() +
                           " with allocate size: " + (actual_str_value));
            data.push_back(actual_str_value);
            res.push_back(data);
        }
        std::string file_name = res_file_path.empty() ? "_allocate_size" : res_file_path;
        if (with_nullmap) {
            file_name.replace(file_name.rfind(".out"), 4, "_with_nullmap.out");
        }
        check_or_generate_res_file(file_name, res);
    };
    test_func(false);
};

auto assert_byte_size_with_file_callback = [](const MutableColumns& load_cols,
                                              DataTypeSerDeSPtrs serders,
                                              const std::string& res_file_path) {
    // Create an empty column to verify `update_hashes` functionality
    // check update_hashes with different hashes
    auto test_func = [&](bool with_nullmap) {
        std::vector<std::vector<std::string>> res;
        for (size_t i = 0; i < load_cols.size(); ++i) {
            const auto& source_column = load_cols[i];
            std::vector<std::string> data;
            auto actual_str_value = std::to_string(source_column->byte_size());
            data.push_back("column: " + source_column->get_name() +
                           " with byte_size: " + (actual_str_value));
            data.push_back(actual_str_value);
            res.push_back(data);
        }
        std::string file_name = res_file_path.empty() ? "_byte_size" : res_file_path;
        if (with_nullmap) {
            file_name.replace(file_name.rfind(".out"), 4, "_with_nullmap.out");
        }
        check_or_generate_res_file(file_name, res);
    };
    test_func(false);
};

} // namespace doris::vectorized