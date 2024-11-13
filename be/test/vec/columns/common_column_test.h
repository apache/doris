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
#include <fstream>

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

class CommonColumnTest : public ::testing::Test {
protected:
    // Helper function to load data from CSV, with index which splited by spliter and return a vector of ColumnPtr
    void load_data_from_csv(const DataTypeSerDeSPtrs serders, MutableColumns& columns,
                            const std::string& file_path, const char spliter = ';', const std::set<int> idxes = {0}, bool gen_check_data =false ) {

        ASSERT_EQ(serders.size(), columns.size()) << "serder size: " << serders.size() << " column size: " << columns.size();
        ASSERT_EQ(serders.size(), idxes.size()) << "serder size: " << serders.size() << " idxes size: " << idxes.size();
        ASSERT_EQ(serders.size(), *idxes.end()) << "serder size: " << serders.size() << " idxes size: " << *idxes.end();
        std::ifstream file(file_path);
        if (!file) {
            throw doris::Exception(ErrorCode::INVALID_ARGUMENT, "can not open the file: {} ", file_path);
        }

        std::string line;
        DataTypeSerDe::FormatOptions options;
        std::vector<std::vector<std::string>> res;
        while (std::getline(file, line)) {
            std::stringstream lineStream(line);

            std::cout << "whole : " << lineStream.str() << std::endl;
            std::string value;
            int l_idx = 0;
            int c_idx = 0;
            std::vector<std::string> data;
            while (std::getline(lineStream, value, spliter)) {
                if (idxes.contains(l_idx)) {
                    Slice string_slice(value.data(), value.size());
                    std::cout << string_slice << std::endl;
                    if (auto st = serders[c_idx]->deserialize_one_cell_from_json(*columns[c_idx], string_slice, options); !st.ok()) {
                        data.push_back("");
                        std::cout << "error in deserialize but continue: " << st.to_string() << std::endl;
                    } else {
                        data.push_back(value);
                    }
                    ++ c_idx;
                }
                ++ l_idx;
            }
            res.push_back(data);
        }
        if (gen_check_data) {
            string filename = "./res.csv";
            std::ofstream res_file(filename);
            std::cout << "gen check data: " << res.size() <<std::endl;
            if (!res_file.is_open()) {
                throw std::ios_base::failure("Failed to open file.");
            }

            for (const auto& row : res) {
                for (size_t i = 0; i < row.size(); ++i) {
                    auto cell = row[i];
                    res_file << cell;
                    if (i < row.size() - 1) {
                        res_file << ";";  // Add semicolon between columns
                    }
                }
                res_file << "\n";  // Newline after each row
            }

            res_file.close();
        }
    }


    // Helper function to load data from CSV
    void load_data_from_csv(std::vector<std::vector<std::string>>& res, const std::string& file_path, const char spliter = ';') {
        std::ifstream file(file_path);
        if (!file) {
            throw doris::Exception(ErrorCode::INVALID_ARGUMENT, "can not open the file: {} ", file_path);
        }
        std::string line;

        while (std::getline(file, line)) {
            std::vector<std::string> data;
            std::stringstream ss(line);
            std::string value;
            while (std::getline(ss, value, spliter)) {
                data.push_back(value);
            }
            res.push_back(data);
        }
    }

private:

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

    // Tool function to check data in column against expected results according different function in assert function
    void check_data(MutableColumns& columns, DataTypeSerDeSPtrs serders, char col_spliter, std::set<int> idxes,
                    const std::string& column_data_file, const std::string& check_data_file,
                    std::function<void(MutableColumns& load_cols, DataTypeSerDeSPtrs serders, std::vector<std::vector<string>>& assert_res)> assert_callback) {

        ASSERT_EQ(serders.size(), columns.size());
        MutableColumns assert_columns(columns.size());
        for (size_t i = 0; i < columns.size(); ++i) {
            assert_columns[i] = columns[i]->clone_empty();
        }
        // Step 1: Insert data from `column_data_file` into the column and check result with `check_data_file`
        // Load column data and expected data from CSV files
        std::vector<std::vector<std::string>> res;
        load_data_from_csv(serders, columns, column_data_file, col_spliter, idxes, false);
        load_data_from_csv(res, check_data_file, col_spliter);
        std::cout << "res size: " << res.size() << std::endl;

        // Step 2: Validate the data in `column` matches `expected_data`
        assert_callback(columns, serders, res);
    }


    void check_columns(MutableColumns& columns, DataTypeSerDeSPtrs serders, DataTypes dataTypes, char col_spliter, std::set<int> idxes,
                       const std::string& column_data_file, const std::string& check_data_file,
                       MutableColumns& check_columns, DataTypeSerDeSPtrs check_serders, char check_col_spliter, std::set<int> check_idxes,
                       std::function<void(MutableColumns& load_cols, MutableColumns& assert_columns, DataTypes dataTypes)> assert_callback) {

        // Load column data and expected data from CSV files
        load_data_from_csv(serders, columns, column_data_file, col_spliter, idxes);
        load_data_from_csv(check_serders, check_columns, check_data_file, col_spliter, idxes);

        // Step 2: Validate the data in `column` matches `expected_data`
        assert_callback(columns, check_columns, dataTypes);
    }

    static void checkColumn(const IColumn& col1, const IColumn& col2, const IDataType& dataType,
                            size_t column_size) {
        if (WhichDataType(dataType).is_map()) {
            auto map1 = check_and_get_column<ColumnMap>(col1);
            auto map2 = check_and_get_column<ColumnMap>(col2);
            const DataTypeMap& rhs_map = static_cast<const DataTypeMap&>(dataType);
            checkColumn(map1->get_keys(), map2->get_keys(), *rhs_map.get_key_type(),
                        map1->get_keys().size());
            checkColumn(map2->get_values(), map2->get_values(), *rhs_map.get_value_type(),
                        map1->get_values().size());
        } else {
            if (WhichDataType(dataType).is_int8()) {
                auto c1 = check_and_get_column<ColumnInt8>(col1);
                auto c2 = check_and_get_column<ColumnInt8>(col2);
                for (size_t i = 0; i < column_size; ++i) {
                    EXPECT_EQ(c1->get_element(i), c2->get_element(i));
                }
            } else if (WhichDataType(dataType).is_int16()) {
                auto c1 = check_and_get_column<ColumnInt16>(col1);
                auto c2 = check_and_get_column<ColumnInt16>(col2);
                for (size_t i = 0; i < column_size; ++i) {
                    EXPECT_EQ(c1->get_element(i), c2->get_element(i));
                }
            } else if (WhichDataType(dataType).is_int32()) {
                auto c1 = check_and_get_column<ColumnInt32>(col1);
                auto c2 = check_and_get_column<ColumnInt32>(col2);
                for (size_t i = 0; i < column_size; ++i) {
                    EXPECT_EQ(c1->get_element(i), c2->get_element(i));
                }
            } else if (WhichDataType(dataType).is_int64()) {
                auto c1 = check_and_get_column<ColumnInt64>(col1);
                auto c2 = check_and_get_column<ColumnInt64>(col2);
                for (size_t i = 0; i < column_size; ++i) {
                    EXPECT_EQ(c1->get_element(i), c2->get_element(i));
                }
            } else if (WhichDataType(dataType).is_int128()) {
                auto c1 = check_and_get_column<ColumnInt128>(col1);
                auto c2 = check_and_get_column<ColumnInt128>(col2);
                for (size_t i = 0; i < column_size; ++i) {
                    EXPECT_EQ(c1->get_element(i), c2->get_element(i));
                }
            } else if (WhichDataType(dataType).is_float32()) {
                auto c1 = check_and_get_column<ColumnFloat32>(col1);
                auto c2 = check_and_get_column<ColumnFloat32>(col2);
                for (size_t i = 0; i < column_size; ++i) {
                    EXPECT_EQ(c1->get_element(i), c2->get_element(i));
                }
            } else if (WhichDataType(dataType).is_float64()) {
                auto c1 = check_and_get_column<ColumnFloat64>(col1);
                auto c2 = check_and_get_column<ColumnFloat64>(col2);
                for (size_t i = 0; i < column_size; ++i) {
                    EXPECT_EQ(c1->get_element(i), c2->get_element(i));
                }
            } else if (WhichDataType(dataType).is_uint8()) {
                auto c1 = check_and_get_column<ColumnUInt8>(col1);
                auto c2 = check_and_get_column<ColumnUInt8>(col2);
                for (size_t i = 0; i < column_size; ++i) {
                    EXPECT_EQ(c1->get_element(i), c2->get_element(i));
                }
            } else if (WhichDataType(dataType).is_uint16()) {
                auto c1 = check_and_get_column<ColumnUInt16>(col1);
                auto c2 = check_and_get_column<ColumnUInt16>(col2);
                for (size_t i = 0; i < column_size; ++i) {
                    EXPECT_EQ(c1->get_element(i), c2->get_element(i));
                }
            } else if (WhichDataType(dataType).is_uint32()) {
                auto c1 = check_and_get_column<ColumnUInt32>(col1);
                auto c2 = check_and_get_column<ColumnUInt32>(col2);
                for (size_t i = 0; i < column_size; ++i) {
                    EXPECT_EQ(c1->get_element(i), c2->get_element(i));
                }
            } else if (WhichDataType(dataType).is_uint64()) {
                auto c1 = check_and_get_column<ColumnUInt64>(col1);
                auto c2 = check_and_get_column<ColumnUInt64>(col2);
                for (size_t i = 0; i < column_size; ++i) {
                    EXPECT_EQ(c1->get_element(i), c2->get_element(i));
                }
            } else if (WhichDataType(dataType).is_decimal()) {
                auto c1 = check_and_get_column<ColumnDecimal64>(col1);
                auto c2 = check_and_get_column<ColumnDecimal64>(col2);
                for (size_t i = 0; i < column_size; ++i) {
                    EXPECT_EQ(c1->get_element(i), c2->get_element(i));
                }
            } else if (WhichDataType(dataType).is_decimal32()) {
                auto c1 = check_and_get_column<ColumnDecimal32>(col1);
                auto c2 = check_and_get_column<ColumnDecimal32>(col2);
                for (size_t i = 0; i < column_size; ++i) {
                    EXPECT_EQ(c1->get_element(i), c2->get_element(i));
                }
            } else if (WhichDataType(dataType).is_decimal64()) {
                auto c1 = check_and_get_column<ColumnDecimal64>(col1);
                auto c2 = check_and_get_column<ColumnDecimal64>(col2);
                for (size_t i = 0; i < column_size; ++i) {
                    EXPECT_EQ(c1->get_element(i), c2->get_element(i));
                }
            } else if (WhichDataType(dataType).is_decimal128v2()) {
                auto c1 = check_and_get_column<ColumnDecimal128V2>(col1);
                auto c2 = check_and_get_column<ColumnDecimal128V2>(col2);
                for (size_t i = 0; i < column_size; ++i) {
                    EXPECT_EQ(c1->get_element(i), c2->get_element(i));
                }
            } else if (WhichDataType(dataType).is_decimal128v3()) {
                auto c1 = check_and_get_column<ColumnDecimal128V3>(col1);
                auto c2 = check_and_get_column<ColumnDecimal128V3>(col2);
                for (size_t i = 0; i < column_size; ++i) {
                    EXPECT_EQ(c1->get_element(i), c2->get_element(i));
                }
            } else if (WhichDataType(dataType).is_decimal256()) {
                auto c1 = check_and_get_column<ColumnDecimal<Decimal256>>(col1);
                auto c2 = check_and_get_column<ColumnDecimal<Decimal256>>(col1);
                for (size_t i = 0; i < column_size; ++i) {
                    EXPECT_EQ(c1->get_element(i), c2->get_element(i));
                }
            } else {
                for (size_t i = 0; i < column_size; ++i) {
                    EXPECT_EQ(col1.get_data_at(i), col2.get_data_at(i));
                }
            }
        }
    }

    void printColumn(const IColumn& column, const IDataType& dataType) {
        std::cout << "column total size: " << column.size() << std::endl;
        if (WhichDataType(dataType).is_map()) {
            auto map = check_and_get_column<ColumnMap>(column);
            std::cout << "map {keys, values}" << std::endl;
            const DataTypeMap& rhs_map = static_cast<const DataTypeMap&>(dataType);
            printColumn(map->get_keys(), *rhs_map.get_key_type());
            printColumn(map->get_values(), *rhs_map.get_value_type());
        } else if (WhichDataType(dataType).is_array()) {
            auto array = check_and_get_column<ColumnArray>(column);
            std::cout << "array: " << std::endl;
            const auto& rhs_array = static_cast<const DataTypeArray&>(dataType);
            printColumn(array->get_data(), *rhs_array.get_nested_type());
        } else {
            size_t column_size = column.size();
            std::cout << column.get_name() << ": " << std::endl;
            if (WhichDataType(dataType).is_int8()) {
                auto col = check_and_get_column<ColumnInt8>(column);
                for (size_t i = 0; i < column_size; ++i) {
                    std::cout << col->get_element(i) << " ";
                }
            } else if (WhichDataType(dataType).is_int16()) {
                auto col = check_and_get_column<ColumnInt16>(column);
                for (size_t i = 0; i < column_size; ++i) {
                    std::cout << col->get_element(i) << " ";
                }
            } else if (WhichDataType(dataType).is_int32()) {
                auto col = check_and_get_column<ColumnInt32>(column);
                for (size_t i = 0; i < column_size; ++i) {
                    std::cout << col->get_element(i) << " ";
                }
            } else if (WhichDataType(dataType).is_int64()) {
                auto col = check_and_get_column<ColumnInt64>(column);
                for (size_t i = 0; i < column_size; ++i) {
                    std::cout << col->get_element(i) << " ";
                }
            } else if (WhichDataType(dataType).is_int128()) {
                auto col = check_and_get_column<ColumnInt128>(column);
                for (size_t i = 0; i < column_size; ++i) {
                    std::cout << col->get_element(i) << " ";
                }
            } else if (WhichDataType(dataType).is_float32()) {
                auto col = check_and_get_column<ColumnFloat32>(column);
                for (size_t i = 0; i < column_size; ++i) {
                    std::cout << col->get_element(i) << " ";
                }
            } else if (WhichDataType(dataType).is_float64()) {
                auto col = check_and_get_column<ColumnFloat64>(column);
                for (size_t i = 0; i < column_size; ++i) {
                    std::cout << col->get_element(i) << " ";
                }
            } else if (WhichDataType(dataType).is_uint8()) {
                auto col = check_and_get_column<ColumnUInt8>(column);
                for (size_t i = 0; i < column_size; ++i) {
                    std::cout << col->get_element(i) << " ";
                }
            } else if (WhichDataType(dataType).is_uint16()) {
                auto col = check_and_get_column<ColumnUInt16>(column);
                for (size_t i = 0; i < column_size; ++i) {
                    std::cout << col->get_element(i) << " ";
                }
            } else if (WhichDataType(dataType).is_uint32()) {
                auto col = check_and_get_column<ColumnUInt32>(column);
                for (size_t i = 0; i < column_size; ++i) {
                    std::cout << col->get_element(i) << " ";
                }
            } else if (WhichDataType(dataType).is_uint64()) {
                auto col = check_and_get_column<ColumnUInt64>(column);
                for (size_t i = 0; i < column_size; ++i) {
                    std::cout << col->get_element(i) << " ";
                }
            } else if (WhichDataType(dataType).is_uint128()) {
                auto col = check_and_get_column<ColumnUInt128>(column);
                for (size_t i = 0; i < column_size; ++i) {
                    std::cout << col->get_data_at(i) << " ";
                }
            } else if (WhichDataType(dataType).is_decimal()) {
                auto col = check_and_get_column<ColumnDecimal64>(column);
                for (size_t i = 0; i < column_size; ++i) {
                    std::cout << col->get_element(i) << " ";
                }
            } else if (WhichDataType(dataType).is_decimal32()) {
                auto col = check_and_get_column<ColumnDecimal32>(column);
                for (size_t i = 0; i < column_size; ++i) {
                    std::cout << col->get_element(i) << " ";
                }
            } else if (WhichDataType(dataType).is_decimal64()) {
                auto col = check_and_get_column<ColumnDecimal64>(column);
                for (size_t i = 0; i < column_size; ++i) {
                    std::cout << col->get_element(i) << " ";
                }
            } else if (WhichDataType(dataType).is_decimal128v2()) {
                auto col = check_and_get_column<ColumnDecimal128V2>(column);
                for (size_t i = 0; i < column_size; ++i) {
                    std::cout << col->get_element(i) << " ";
                }
            } else if (WhichDataType(dataType).is_decimal128v3()) {
                auto col = check_and_get_column<ColumnDecimal128V3>(column);
                for (size_t i = 0; i < column_size; ++i) {
                    std::cout << col->get_element(i) << " ";
                }
            } else if (WhichDataType(dataType).is_decimal256()) {
                auto col = check_and_get_column<ColumnDecimal<Decimal256>>(column);
                for (size_t i = 0; i < column_size; ++i) {
                    std::cout << col->get_element(i) << " ";
                }
            } else if (WhichDataType(dataType).is_date()) {
                auto col = check_and_get_column<ColumnDate>(column);
                for (size_t i = 0; i < column_size; ++i) {
                    std::cout << col->get_data_at(i) << " ";
                }
            } else if (WhichDataType(dataType).is_date_time()) {
                auto col = check_and_get_column<ColumnDateTime>(column);
                for (size_t i = 0; i < column_size; ++i) {
                    std::cout << col->get_data_at(i) << " ";
                }
            } else if (WhichDataType(dataType).is_date_v2()) {
                auto col = check_and_get_column<ColumnDateV2>(column);
                for (size_t i = 0; i < column_size; ++i) {
                    std::cout << col->get_data_at(i) << " ";
                }
            } else if (WhichDataType(dataType).is_date_time_v2()) {
                auto col = check_and_get_column<ColumnDateTimeV2>(column);
                for (size_t i = 0; i < column_size; ++i) {
                    std::cout << col->get_data_at(i) << " ";
                }
            } else {
                std::cout << "data type: " << dataType.get_name() << std::endl;
                std::cout << "column type: " << column.get_name() << std::endl;
                for (size_t i = 0; i < column_size; ++i) {
                    std::cout << column.get_data_at(i).to_string() << " ";
                }
            }
            std::cout << std::endl;
        }
    }

    //// data insert interfaces:
    //insert (const Field &x)=0
    //
    //virtual void
    //insert_from_multi_column (const std::vector< const IColumn * > &srcs, std::vector< size_t > positions)
    //
    //
    //virtual void
    //insert_data (const char *pos, size_t length)=0
    //
    //virtual void
    //insert_many_fix_len_data (const char *pos, size_t num)
    //
    //virtual void
    //insert_many_data (const char *pos, size_t length, size_t data_num)
    //
    //virtual void
    //insert_many_raw_data (const char *pos, size_t num)



    //virtual void
    //insert_many_dict_data (const int32_t *data_array, size_t start_index, const StringRef *dict, size_t data_num, uint32_t dict_num=0)
    //
    //virtual void
    //insert_many_binary_data (char *data_array, uint32_t *len_array, uint32_t *start_offset_array, size_t num)
    //
    //virtual void
    //insert_many_continuous_binary_data (const char *data, const uint32_t *offsets, const size_t num)
    //
    //virtual void
    //insert_many_strings (const StringRef *strings, size_t num)
    //
    //virtual void
    //insert_many_strings_overflow (const StringRef *strings, size_t num, size_t max_length)
    //
    //

    //
    //
    //void
    //

    //
    //
    //virtual void
    //insert_default ()=0
    //
    //
    //virtual void
    //insert_many_defaults (size_t length)



    // assert insert_from
    // Define the custom assert callback function to verify insert_from behavior
    static void assert_insert_from_callback(MutableColumns& load_cols, DataTypeSerDeSPtrs serders, std::vector<std::vector<std::string>>& assert_res) {
        // Create an empty column to verify `insert_from` functionality
        MutableColumns verify_columns;
        for (auto& col : load_cols) {
            verify_columns.push_back(col->clone_empty());
        }
        auto option = DataTypeSerDe::FormatOptions();
        // Insert data from `load_cols` to `verify_columns` using `insert_from`
        for (size_t i = 0; i < load_cols.size(); ++i) {
            auto& source_column = load_cols[i];
            auto& target_column = verify_columns[i];

            for (size_t j = 0; j < source_column->size(); ++j) {
                target_column->insert_from(*source_column, j);
            }

            // Verify the inserted data matches the expected results in `assert_res`
            auto ser_col = ColumnString::create();
            std::cout << "target_column size: " << target_column->size() << std::endl;
            ser_col->reserve(target_column->size());
            VectorBufferWriter buffer_writer(*ser_col.get());
            for (size_t j = 0; j < target_column->size(); ++j) {
                if (auto st = serders[i]->serialize_one_cell_to_json(*target_column, j, buffer_writer, option); !st) {
                    std::cerr << "Failed to serialize column " << i << " at row " << j << std::endl;
                    break;
                }
                std::string actual_str_value = ser_col->get_data_at(j).to_string();
                std::string expected_value = assert_res[i][j];

                ASSERT_EQ(actual_str_value, expected_value) << "Data mismatch at row " << j << " in column " << i
                                                            << ": expected '" << expected_value
                                                            << "', got '" << actual_str_value << "'";
            }
        }
    }


    // assert insert_range_from
    // Define the custom assert callback function to verify insert_range_from behavior
    static void assert_insert_range_from_callback(MutableColumns& load_cols, DataTypeSerDeSPtrs serders, std::vector<std::vector<std::string>>& assert_res) {
        // Create an empty column to verify `insert_range_from` functionality
        MutableColumns verify_columns;
        for (auto& col : load_cols) {
            verify_columns.push_back(col->clone_empty());
        }
        auto option = DataTypeSerDe::FormatOptions();

        std::vector<size_t> check_length = {0, 10, 100, 1000, 10000, 100000, 1000000};
        // Insert data from `load_cols` to `verify_columns` using `insert_range_from`
        for (auto cl = check_length.begin(); cl < check_length.end(); ++ cl) {
            for (size_t i = 0; i < load_cols.size(); ++i) {
                std::cout << "==== now test insert_range_from with col " << verify_columns[i]->get_name() << std::endl;
                auto& source_column = load_cols[i];
                auto& target_column = verify_columns[i];
                std::vector<size_t> check_start_pos = {0, source_column->size(), (source_column->size() + 1) >> 1};
                // size_t(-1) may cause overflow, but here we have compiler to check it
                std::vector<size_t> err_start_pos = { source_column->size() + 1};
                for (auto pos = err_start_pos.begin(); pos < err_start_pos.end(); ++pos) {
                    std::cout << "error insert_range_from from " << *pos << " with length " << *cl << std::endl;
                    std::cout << *pos + *cl  <<  " > " << source_column->size() << std::endl;
                    EXPECT_THROW(target_column->insert_range_from(*source_column, *pos, *cl), Exception);
                }
                for (auto pos = check_start_pos.begin(); pos < check_start_pos.end(); ++pos) {
                    std::cout << "now insert_range_from from " << *pos << " with length " << *cl  << " with source size: " << source_column->size() << std::endl;
                    if (*pos + *cl > source_column->size()) {
                        EXPECT_THROW(target_column->insert_range_from(*source_column, *pos, *cl), Exception);
                        continue;
                    } else {
                        target_column->insert_range_from(*source_column, *pos, *cl);
                    }
                    target_column->insert_range_from(*source_column, *pos, *cl);

                    // Verify the inserted data matches the expected results in `assert_res`
                    auto ser_col = ColumnString::create();
                    ser_col->reserve(target_column->size());
                    VectorBufferWriter buffer_writer(*ser_col.get());
                    for (size_t j = 0; j < target_column->size(); ++j) {
                        if (auto st = serders[i]->serialize_one_cell_to_json(
                                    *target_column, j, buffer_writer, option);
                            !st) {
                            std::cerr << "Failed to serialize column " << i << " at row " << j
                                      << std::endl;
                            break;
                        }
                        buffer_writer.commit();
                        if (i < assert_res.size() && j < assert_res[i].size() && assert_res[i][j] != "") {
                            std::string actual_str_value = ser_col->get_data_at(j).to_string();
                            std::string expected_value = assert_res[i][j];

                            ASSERT_EQ(actual_str_value, expected_value)
                                    << "Data mismatch at row " << j << " in column " << i
                                    << ": expected '" << expected_value << "', got '"
                                    << actual_str_value << "'";
                        }
                    }
                }
            }
        }
    }

    // assert insert_range_from_ignore_overflow which happened in columnStr<UInt32> want to insert from ColumnStr<UInt64> for more column string to be inserted not just limit to the 4G
    // Define the custom assert callback function to verify insert_range_from_ignore_overflow behavior

    // assert insert_many_from (used in join situation, which handle left table col to expand for right table : such as A[1,2,3] inner join B[2,2,4,4] => A[2,2] )
    // Define the custom assert callback function to verify insert_many_from behavior
    static void assert_insert_many_from_callback(MutableColumns& load_cols, DataTypeSerDeSPtrs serders, std::vector<std::vector<std::string>>& assert_res) {
        // Create an empty column to verify `insert_many_from` functionality
        MutableColumns verify_columns;
        for (auto& col : load_cols) {
            verify_columns.push_back(col->clone_empty());
        }
        auto option = DataTypeSerDe::FormatOptions();

        std::vector<size_t> check_length = {0, 10, 100, 1000, 10000, 100000, 1000000};
        // Insert data from `load_cols` to `verify_columns` using `insert_many_from`
        for (auto cl = check_length.begin(); cl < check_length.end(); ++ cl) {
            for (size_t i = 0; i < load_cols.size(); ++i) {
                auto& source_column = load_cols[i];
                auto& target_column = verify_columns[i];
                // size_t(-1) may cause overflow, but here we have compiler to check it
                std::vector<size_t> check_start_pos = {0, source_column->size(), source_column->size() + 1,
                                                       (source_column->size() + 1) >> 1};
                for (auto pos = check_start_pos.begin(); pos < check_start_pos.end(); ++pos) {
                    if (*pos > source_column->size() || *cl > source_column->size() || *pos + *cl > source_column->size()) {
                        EXPECT_THROW(target_column->insert_many_from(*source_column, *pos, *cl), Exception);
                        continue;
                    }
                    target_column->insert_many_from(*source_column, *pos, *cl);

                    // Verify the inserted data matches the expected results in `assert_res`
                    auto ser_col = ColumnString::create();
                    ser_col->reserve(target_column->size());
                    VectorBufferWriter buffer_writer(*ser_col.get());
                    for (size_t j = 0; j < target_column->size(); ++j) {
                        if (auto st = serders[i]->serialize_one_cell_to_json(
                                    *target_column, j, buffer_writer, option);
                            !st) {
                            std::cerr << "Failed to serialize column " << i << " at row " << j
                                      << std::endl;
                            break;
                        }
                        buffer_writer.commit();
                        std::string actual_str_value = ser_col->get_data_at(j).to_string();
                        std::string expected_value = assert_res[i][j];

                        ASSERT_EQ(actual_str_value, expected_value)
                                << "Data mismatch at row " << j << " in column " << i
                                << ": expected '" << expected_value << "', got '"
                                << actual_str_value << "'";
                    }
                }
            }
        }
    }

    static void assert_insert_indices_from_callback(MutableColumns& load_cols, DataTypeSerDeSPtrs serders, std::vector<std::vector<std::string>>& assert_res) {

        // Create an empty column to verify `insert_indices_from` functionality
        MutableColumns verify_columns;
        for (auto& col : load_cols) {
            verify_columns.push_back(col->clone_empty());
        }
        auto option = DataTypeSerDe::FormatOptions();
        // Insert data from `load_cols` to `verify_columns` using `insert_indices_from`
        for (size_t i = 0; i < load_cols.size(); ++i) {
            auto& source_column = load_cols[i];
            auto& target_column = verify_columns[i];

            std::vector<uint32_t> check_indices = {uint32_t(-1), 0, uint32_t(source_column->size()),
                                                   uint32_t(source_column->size() + 1),
                                                   uint32_t((source_column->size() + 1) >> 1)};
            for (auto from_idx = check_indices.begin(); from_idx < check_indices.end(); ++from_idx) {
                for (auto end_idx = check_indices.begin(); end_idx < check_indices.end(); ++end_idx) {
                    // Insert data from `load_cols` to `verify_columns` using `insert_indices_from`
                    target_column->insert_indices_from(*source_column, &(*from_idx), &(*end_idx));

                    // Verify the inserted data matches the expected results in `assert_res`
                    auto ser_col = ColumnString::create();
                    ser_col->reserve(target_column->size());
                    VectorBufferWriter buffer_writer(*ser_col.get());
                    for (size_t j = 0; j < target_column->size(); ++j) {
                        if (auto st = serders[i]->serialize_one_cell_to_json(
                                    *target_column, j, buffer_writer, option);
                            !st) {
                            std::cerr << "Failed to serialize column " << i << " at row " << j
                                      << std::endl;
                            break;
                        }
                        std::string actual_str_value = ser_col->get_data_at(j).to_string();
                        std::string expected_value = assert_res[i][j];

                        ASSERT_EQ(actual_str_value, expected_value)
                                << "Data mismatch at row " << j << " in column " << i
                                << ": expected '" << expected_value << "', got '"
                                << actual_str_value << "'";
                    }
                }
            }
        }
    }

    static void assert_insert_data_from_callback(MutableColumns& load_cols, DataTypeSerDeSPtrs serders, std::vector<std::vector<std::string>>& assert_res) {

        // Create an empty column to verify `insert_data` functionality
        MutableColumns verify_columns;
        for (auto& col : load_cols) {
            verify_columns.push_back(col->clone_empty());
        }
        auto option = DataTypeSerDe::FormatOptions();
        // Insert data from `load_cols` to `verify_columns` using `insert_data`
        for (size_t i = 0; i < load_cols.size(); ++i) {
            auto& source_column = load_cols[i];
            auto& target_column = verify_columns[i];

            for (size_t j = 0; j < source_column->size(); ++j) {
                target_column->insert_data(source_column->get_data_at(j).data, source_column->get_data_at(j).size);
            }

            // Verify the inserted data matches the expected results in `assert_res`
            auto ser_col = ColumnString::create();
            ser_col->reserve(target_column->size());
            VectorBufferWriter buffer_writer(*ser_col.get());
            for (size_t j = 0; j < target_column->size(); ++j) {
                if (auto st = serders[i]->serialize_one_cell_to_json(*target_column, j, buffer_writer, option); !st) {
                    std::cerr << "Failed to serialize column " << i << " at row " << j << std::endl;
                    break;
                }
                std::string actual_str_value = ser_col->get_data_at(j).to_string();
                std::string expected_value = assert_res[i][j];

                ASSERT_EQ(actual_str_value, expected_value) << "Data mismatch at row " << j << " in column " << i
                                                            << ": expected '" << expected_value
                                                            << "', got '" << actual_str_value << "'";
            }
        }
    }

    static void assert_insert_many_raw_data_from_callback(MutableColumns& load_cols, DataTypeSerDeSPtrs serders, std::vector<std::vector<std::string>>& assert_res) {

        // Create an empty column to verify `insert_many_raw_data` functionality
        MutableColumns verify_columns;
        for (auto& col : load_cols) {
            verify_columns.push_back(col->clone_empty());
        }
        auto option = DataTypeSerDe::FormatOptions();
        // Insert data from `load_cols` to `verify_columns` using `insert_many_raw_data`
        for (size_t i = 0; i < load_cols.size(); ++i) {
            auto& source_column = load_cols[i];
            auto& target_column = verify_columns[i];

            for (size_t j = 0; j < source_column->size(); ++j) {
                target_column->insert_many_raw_data(source_column->get_data_at(j).data, source_column->get_data_at(j).size);
            }

            // Verify the inserted data matches the expected results in `assert_res`
            auto ser_col = ColumnString::create();
            ser_col->reserve(target_column->size());
            VectorBufferWriter buffer_writer(*ser_col.get());
            for (size_t j = 0; j < target_column->size(); ++j) {
                if (auto st = serders[i]->serialize_one_cell_to_json(*target_column, j, buffer_writer, option); !st) {
                    std::cerr << "Failed to serialize column " << i << " at row " << j << std::endl;
                    break;
                }
                std::string actual_str_value = ser_col->get_data_at(j).to_string();
                std::string expected_value = assert_res[i][j];

                ASSERT_EQ(actual_str_value, expected_value) << "Data mismatch at row " << j << " in column " << i
                                                            << ": expected '" << expected_value
                                                            << "', got '" << actual_str_value << "'";
            }
        }
    }

    // assert insert_default
    // Define the custom assert callback function to verify insert_default behavior
    static void assert_insert_default_callback(MutableColumns& load_cols, DataTypeSerDeSPtrs serders, std::vector<std::vector<std::string>>& assert_res) {

        // Create an empty column to verify `insert_default` functionality
        MutableColumns verify_columns;
        for (auto& col : load_cols) {
            verify_columns.push_back(col->clone_empty());
        }
        auto option = DataTypeSerDe::FormatOptions();
        // Insert data from `load_cols` to `verify_columns` using `insert_default`
        for (size_t i = 0; i < load_cols.size(); ++i) {
            auto& target_column = verify_columns[i];
            target_column->insert_default();

            // Verify the inserted data matches the expected results in `assert_res`
            auto ser_col = ColumnString::create();
            ser_col->reserve(target_column->size());
            VectorBufferWriter buffer_writer(*ser_col.get());
            for (size_t j = 0; j < target_column->size(); ++j) {
                if (auto st = serders[i]->serialize_one_cell_to_json(*target_column, j, buffer_writer, option); !st) {
                    std::cerr << "Failed to serialize column " << i << " at row " << j << std::endl;
                    break;
                }
                std::string actual_str_value = ser_col->get_data_at(j).to_string();
                std::string expected_value = assert_res[i][j];

                ASSERT_EQ(actual_str_value, expected_value) << "Data mismatch at row " << j << " in column " << i
                                                            << ": expected '" << expected_value
                                                            << "', got '" << actual_str_value << "'";
            }
        }
    }

    // assert insert_many_default
    // Define the custom assert callback function to verify insert_many_defaults behavior
    static void assert_insert_many_defaults_callback(MutableColumns& load_cols, DataTypeSerDeSPtrs serders, std::vector<std::vector<std::string>>& assert_res) {

        // Create an empty column to verify `insert_many_defaults` functionality
        MutableColumns verify_columns;
        for (auto& col : load_cols) {
            verify_columns.push_back(col->clone_empty());
        }
        auto option = DataTypeSerDe::FormatOptions();
        std::vector<size_t> check_length = {0, 10, 100, 1000, 10000, 100000, 1000000};
        // Insert data from `load_cols` to `verify_columns` using `insert_many_defaults`
        for (auto cl = check_length.begin(); cl < check_length.end(); ++ cl) {
            for (size_t i = 0; i < load_cols.size(); ++i) {
                auto& target_column = verify_columns[i];
                target_column->insert_many_defaults(*cl);

                // Verify the inserted data matches the expected results in `assert_res`
                auto ser_col = ColumnString::create();
                ser_col->reserve(target_column->size());
                VectorBufferWriter buffer_writer(*ser_col.get());
                for (size_t j = 0; j < target_column->size(); ++j) {
                    if (auto st = serders[i]->serialize_one_cell_to_json(*target_column, j, buffer_writer, option); !st) {
                        std::cerr << "Failed to serialize column " << i << " at row " << j << std::endl;
                        break;
                    }
                    std::string actual_str_value = ser_col->get_data_at(j).to_string();
                    std::string expected_value = assert_res[i][j];

                    ASSERT_EQ(actual_str_value, expected_value) << "Data mismatch at row " << j << " in column " << i
                                                                << ": expected '" << expected_value
                                                                << "', got '" << actual_str_value << "'";
                }
            }
        }
    }

    //// data access interfaces:
    // virtual StringRef
    //get_data_at (size_t n) const = 0
    // if we implement the get_data_at, we should know the data is stored in the column, and we can get the data by the index
    static void assert_get_data_at_callback(MutableColumns& load_cols, DataTypeSerDeSPtrs serders, std::vector<std::vector<std::string>>& assert_res) {

        // just check cols get_data_at is the same as assert_res
        for (size_t i = 0; i < load_cols.size(); ++i) {
            auto& source_column = load_cols[i];
            for (size_t j = 0; j < source_column->size(); ++j) {
                auto actual_str_value = source_column->get_data_at(j).to_string();
                auto expected_value = assert_res[i][j];
                ASSERT_EQ(actual_str_value, expected_value) << "Data mismatch at row " << j << " in column " << i
                                                            << ": expected '" << expected_value
                                                            << "', got '" << actual_str_value << "'";
            }
        }
    }

    //virtual void
    // field is memory layout of the data , maybe we can use get from base column and insert into same type column to check this behavior is right
    //get (size_t n, Field &res) const = 0
    //Like the previous one, but avoids extra copying if Field is in a container, for example.
    //virtual Field
    //operator[] (size_t n) const = 0
    static void assert_field_callback(MutableColumns& load_cols, DataTypeSerDeSPtrs serders, std::vector<std::vector<std::string>>& assert_res) {
        MutableColumns assert_cols(load_cols.size());
        for (size_t i = 0; i < load_cols.size(); ++i) {
            assert_cols[i] = load_cols[i]->clone_empty();
        }
        auto option = DataTypeSerDe::FormatOptions();

        // just check cols get is the same as assert_res
        for (size_t i = 0; i < load_cols.size(); ++i) {
            auto& source_column = load_cols[i];

            for (size_t j = 0; j < source_column->size(); ++j) {
                Field res;
                source_column->get(j, res);
                assert_cols[j]->insert(res);
            }
            // Verify the inserted data matches the expected results in `assert_res`
            auto ser_col = ColumnString::create();
            ser_col->reserve(load_cols[i]->size());
            VectorBufferWriter buffer_writer(*ser_col.get());
            for (size_t j = 0; j < assert_cols.size(); ++j) {
                if (auto st = serders[i]->serialize_one_cell_to_json(*assert_cols[i], j,
                                                                     buffer_writer, option);
                    !st) {
                    std::cerr << "Failed to serialize column " << i << " at row " << j << std::endl;
                    break;
                }
                std::string actual_str_value = ser_col->get_data_at(j).to_string();
                std::string expected_value = assert_res[i][j];
                ASSERT_EQ(actual_str_value, expected_value)
                        << "Data mismatch at row " << j << " in column " << i << ": expected '"
                        << expected_value << "', got '" << actual_str_value << "'";
            }
        }

        // just check cols operator [] to get field same with field
        for (size_t i = 0; i < load_cols.size(); ++i) {
            auto& source_column = load_cols[i];
            for (size_t j = 0; j < source_column->size(); ++j) {
                Field res = source_column->operator[](j);
                assert_cols[j]->insert(res);
            }
            // Verify the inserted data matches the expected results in `assert_res`
            auto ser_col = ColumnString::create();
            ser_col->reserve(load_cols[i]->size());
            VectorBufferWriter buffer_writer(*ser_col.get());
            for (size_t j = 0; j < assert_cols.size(); ++j) {
                if (auto st = serders[i]->serialize_one_cell_to_json(*assert_cols[i], j,
                                                                     buffer_writer, option);
                    !st) {
                    std::cerr << "Failed to serialize column " << i << " at row " << j << std::endl;
                    break;
                }
                std::string actual_str_value = ser_col->get_data_at(j).to_string();
                std::string expected_value = assert_res[i][j];
                ASSERT_EQ(actual_str_value, expected_value)
                        << "Data mismatch at row " << j << " in column " << i << ": expected '"
                        << expected_value << "', got '" << actual_str_value << "'";
            }
        }
    }
    //
    //virtual StringRef
    //get_raw_data () const
    static void assert_get_raw_data_callback(MutableColumns& load_cols, DataTypeSerDeSPtrs serders, std::vector<std::vector<std::string>>& assert_res) {

        // just check cols get_raw_data is the same as assert_res
        for (size_t i = 0; i < load_cols.size(); ++i) {
            auto& source_column = load_cols[i];
            auto actual_str_value = source_column->get_raw_data().to_string();
            auto expected_value = assert_res[i][0];
            ASSERT_EQ(actual_str_value, expected_value) << "Data mismatch in column " << i
                                                        << ": expected '" << expected_value
                                                        << "', got '" << actual_str_value << "'";
        }
    }

    //If is_fixed_and_contiguous, returns the underlying data array, otherwise throws an exception.
    //virtual Int64
    //get_int (size_t) const
    static void assert_get_int_callback(MutableColumns& load_cols, DataTypeSerDeSPtrs serders, std::vector<std::vector<std::string>>& assert_res) {
        // check load cols size and assert res size
        ASSERT_EQ(load_cols.size(), assert_res.size())
                << "Mismatch between column data and expected results";

        // just check cols get_int is the same as assert_res
        for (size_t i = 0; i < load_cols.size(); ++i) {
            auto& source_column = load_cols[i];
            for (size_t j = 0; j < source_column->size(); ++j) {
                auto actual_str_value = std::to_string(source_column->get_int(j));
                auto expected_value = assert_res[i][j];
                ASSERT_EQ(actual_str_value, expected_value) << "Data mismatch at row " << j << " in column " << i
                                                            << ": expected '" << expected_value
                                                            << "', got '" << actual_str_value << "'";
            }
        }
    }
    //virtual bool
    //get_bool (size_t) const
    static void assert_get_bool_callback(MutableColumns& load_cols, DataTypeSerDeSPtrs serders, std::vector<std::vector<std::string>>& assert_res) {

        // just check cols get_bool is the same as assert_res
        for (size_t i = 0; i < load_cols.size(); ++i) {
            auto& source_column = load_cols[i];
            for (size_t j = 0; j < source_column->size(); ++j) {
                auto actual_str_value = std::to_string(source_column->get_bool(j));
                auto expected_value = assert_res[i][j];
                ASSERT_EQ(actual_str_value, expected_value) << "Data mismatch at row " << j << " in column " << i
                                                            << ": expected '" << expected_value
                                                            << "', got '" << actual_str_value << "'";
            }
        }
    }


    //// column meta interfaces:
    // virtual std::string
    //get_name () const , simple assert to make sure name
    static void getNameAssert(IColumn& column, const string expect_name) {
        ASSERT_EQ(expect_name, column.get_name());
    }
    //virtual const char *
    //get_family_name () const =0, same with get_name
    static void getFamilyNameAssert(IColumn& column, const string expect_name) {
        ASSERT_EQ(expect_name, column.get_family_name());
    }

    static void isFixedAndContiguousAssert(IColumn& column, const bool expect_value) {
        ASSERT_EQ(expect_value, column.is_fixed_and_contiguous());
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
    static void assert_size_callback(MutableColumns& load_cols, DataTypeSerDeSPtrs serders, std::vector<std::vector<std::string>>& assert_res) {
        // check load cols size and assert res size
        ASSERT_EQ(load_cols.size(), assert_res.size())
                << "Mismatch between column data and expected results";

        // just check cols size is the same as assert_res
        for (size_t i = 0; i < load_cols.size(); ++i) {
            auto& source_column = load_cols[i];
            auto actual_str_value = std::to_string(source_column->size());
            auto expected_value = assert_res[i][0];
            ASSERT_EQ(actual_str_value, expected_value) << "Data mismatch in column " << i
                                                        << ": expected '" << expected_value
                                                        << "', got '" << actual_str_value << "'";
        }
    }

    // assert size_of_value_if_fixed
    // Define the custom assert callback function to verify size_of_value_if_fixed behavior
    static void assert_size_of_value_if_fixed_callback(MutableColumns& load_cols, DataTypeSerDeSPtrs serders, std::vector<std::vector<std::string>>& assert_res) {
        // check load cols size and assert res size
        ASSERT_EQ(load_cols.size(), assert_res.size())
                << "Mismatch between column data and expected results";

        // just check cols size_of_value_if_fixed is the same as assert_res
        for (size_t i = 0; i < load_cols.size(); ++i) {
            auto& source_column = load_cols[i];
            auto actual_str_value = std::to_string(source_column->size_of_value_if_fixed());
            auto expected_value = assert_res[i][0];
            ASSERT_EQ(actual_str_value, expected_value) << "Data mismatch in column " << i
                                                        << ": expected '" << expected_value
                                                        << "', got '" << actual_str_value << "'";
        }
    }

    // assert byte_size
    // Define the custom assert callback function to verify byte_size behavior
    static void assert_byte_size_callback(MutableColumns& load_cols, DataTypeSerDeSPtrs serders, std::vector<std::vector<std::string>>& assert_res) {
        // check load cols size and assert res size
        ASSERT_EQ(load_cols.size(), assert_res.size())
                << "Mismatch between column data and expected results";

        // just check cols byte_size is the same as assert_res
        for (size_t i = 0; i < load_cols.size(); ++i) {
            auto& source_column = load_cols[i];
            auto actual_str_value = std::to_string(source_column->byte_size());
            auto expected_value = assert_res[i][0];
            ASSERT_EQ(actual_str_value, expected_value) << "Data mismatch in column " << i
                                                        << ": expected '" << expected_value
                                                        << "', got '" << actual_str_value << "'";
        }
    }

    // assert allocated_bytes Define the custom assert callback function to verify allocated_bytes behavior
    static void assert_allocated_bytes_callback(MutableColumns& load_cols, DataTypeSerDeSPtrs serders, std::vector<std::vector<std::string>>& assert_res) {
        // check load cols size and assert res size
        ASSERT_EQ(load_cols.size(), assert_res.size())
                << "Mismatch between column data and expected results";

        // just check cols allocated_bytes is the same as assert_res
        for (size_t i = 0; i < load_cols.size(); ++i) {
            auto& source_column = load_cols[i];
            auto actual_str_value = std::to_string(source_column->allocated_bytes());
            auto expected_value = assert_res[i][0];
            ASSERT_EQ(actual_str_value, expected_value) << "Data mismatch in column " << i
                                                        << ": expected '" << expected_value
                                                        << "', got '" << actual_str_value << "'";
        }
    }

    // is_exclusive assert should assert the column which has multiple columnPtr in the column

    //// data manage interfaces
    //virtual void
    // pop_back (size_t n) = 0
    // assert pop_back
    static void assert_pop_back_callback(MutableColumns& load_cols, MutableColumns& assert_res, DataTypes dataTypes) {
        // check load cols size and assert res size
        ASSERT_EQ(load_cols.size(), assert_res.size())
                << "Mismatch between column data and expected results";
        // Create an empty column to verify `pop_back` functionality
        // check pop_back with different n
        std::vector<size_t> check_length = {size_t(-1), 0, 1, 10, 100, 1000, 10000, 100000};
        for (auto cl = check_length.begin(); cl < check_length.end(); ++ cl) {
            for (size_t i = 0; i < load_cols.size(); ++i) {
                auto& source_column = load_cols[i];
                source_column->pop_back(*cl);
            }
            // Verify the inserted data matches the expected results in `assert_res`
            for (size_t i = 0; i < assert_res.size(); ++i) {
                auto& source_column = load_cols[i];
                auto& target_column = assert_res[i];
                checkColumn(*source_column, *target_column, *dataTypes[i], target_column->size());
            }
        }
    }

    //virtual MutablePtr
    // Creates empty column with the same type.
    //clone_empty () const this is clone ,we should also check if the size is 0 after clone and ptr is not the same
    void cloneEmptyAssert(IColumn& column) {
        auto ptr = column.clone_empty();
        EXPECT_EQ(ptr->size(), 0);
        EXPECT_NE(ptr.get(), &column);
    }

    //virtual MutablePtr
    //clone_resized (size_t s) const
    static void assert_clone_resized_callback(MutableColumns& load_cols, DataTypeSerDeSPtrs serders, std::vector<std::vector<std::string>>& assert_res) {
        // check load cols size and assert res size
        ASSERT_EQ(load_cols.size(), assert_res.size())
                << "Mismatch between column data and expected results";
        // Create an empty column to verify `clone_resized` functionality
        // check clone_resized with different size
        std::vector<size_t> check_length = {size_t(-1), 0, 1, 10, 100, 1000, 10000, 100000};
        auto option = DataTypeSerDe::FormatOptions();
        for (auto cl = check_length.begin(); cl < check_length.end(); ++ cl) {
            for (size_t i = 0; i < load_cols.size(); ++i) {
                auto& source_column = load_cols[i];
                auto ptr = source_column->clone_resized(*cl);
                EXPECT_EQ(ptr->size(), *cl);
                EXPECT_NE(ptr.get(), source_column);

                // check after clone_resized with assert_res
                auto ser_col = ColumnString::create();
                ser_col->reserve(ptr->size());
                VectorBufferWriter buffer_writer(*ser_col.get());
                for (size_t j = 0; j < ptr->size(); ++j) {
                    if (auto st = serders[i]->serialize_one_cell_to_json(*ptr, j, buffer_writer, option); !st) {
                        std::cerr << "Failed to serialize column " << i << " at row " << j << std::endl;
                        break;
                    }
                    std::string actual_str_value = ser_col->get_data_at(j).to_string();
                    std::string expected_value = assert_res[i][j];
                    ASSERT_EQ(actual_str_value, expected_value) << "Data mismatch at row " << j << " in column " << i
                                                                << ": expected '" << expected_value
                                                                << "', got '" << actual_str_value << "'";
                }
            }
        }
    }

    // is_exclusive() means to check the ptr is not shared with other, and we can just clean it. so it's important to check the column is exclusive or not


    //virtual Ptr
    //cut (size_t start, size_t length) const final
    //virtual bool
    //could_shrinked_column ()
    //virtual MutablePtr
    //get_shrinked_column ()
    //virtual Ptr
    //shrink (size_t length) const final
    //
    //
    //cut or expand inplace. this would be moved, only the return value is avaliable.
    //virtual void
    //reserve (size_t)
    //
    //
    //virtual void
    //resize (size_t)
    //
    //
    //virtual Ptr
    //replicate (const Offsets &offsets) const =0
    //
    //
    //virtual void
    //for_each_subcolumn (ColumnCallback)
    //virtual void
    //replace_column_data (const IColumn &, size_t row, size_t self_row=0)=0
    //
    //
    //virtual void
    //replace_column_null_data (const uint8_t *__restrict null_map)
    //virtual void
    //append_data_by_selector (MutablePtr &res, const Selector &selector) const =0
    //
    //
    //virtual void
    //append_data_by_selector (MutablePtr &res, const Selector &selector, size_t begin, size_t end) const =0
    //
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

    // column size changed calculation:
    //  size, reserve, resize, empty, byte_size, allocated_bytes, clone_resized, get_shrinked_column
    //  cut(LIMIT operation), shrink
    void sizeAssert(MutableColumnPtr col, size_t expect_size) {
        EXPECT_EQ(col->size(), expect_size);
    }

    // empty just use size() == 0 to impl as default behavior
    void emptyAssert(MutableColumnPtr col) { EXPECT_EQ(col->size(), 0); }

    // reserve, resize, byte_size, allocated_bytes, clone_resized, get_shrinked_column
    void reserveAssert(MutableColumnPtr col, size_t expect_size) {
        col->reserve(expect_size);
        EXPECT_EQ(col->allocated_bytes(), expect_size);
    }

    //  cut(LIMIT operation) will cut the column with the given from and to, and return the new column
    //  notice return column is clone from origin column
    void cutAssert(MutableColumnPtr col, size_t from, size_t to, size_t expect_size) {
        auto ori = col->size();
        auto ptr = col->cut(from, to);
        EXPECT_EQ(ptr->size(), expect_size);
        EXPECT_EQ(col->size(), ori);
    }

    // shrink is cut/append the column with the given size, which called from Block::set_num_rows
    // and some Operator may call this set_num_rows to make rows satisfied, like limit operation
    // but different from cut behavior which
    // return column is mutate from origin column
    void shrinkAssert(MutableColumnPtr col, size_t shrink_size) {
        auto ptr = col->shrink(shrink_size);
        EXPECT_EQ(ptr->size(), shrink_size);
        EXPECT_EQ(col->size(), shrink_size);
    }

    // resize has fixed-column implementation and variable-column implementation
    // like string column, the resize will resize the offsets column but not the data column (because it doesn't matter the size of data column, all operation for string column is based on the offsets column)
    // like vector column, the resize will resize the data column
    // like array column, the resize will resize the offsets column and the data column (which in creator we have check staff for the size of data column is the same as the size of offsets column)
    void resizeAssert(MutableColumnPtr col, size_t expect_size) {
        col->resize(expect_size);
        EXPECT_EQ(col->size(), expect_size);
    }

    // replicate is clone with new column from the origin column, always from ColumnConst to expand the column
    void replicateAssert(MutableColumnPtr col, IColumn::Offsets& offsets) {
        auto new_col = col->replicate(offsets);
        EXPECT_EQ(new_col->size(), offsets.back());
    }

    // byte size is just appriximate size of the column
    //  as fixed column type, like column_vector, the byte size is sizeof(columnType) * size()
    //  as variable column type, like column_string, the byte size is sum of chars size() and offsets size * sizeof(offsetType)
    void byteSizeAssert(MutableColumnPtr col, size_t expect_size) {
        EXPECT_EQ(col->byte_size(), expect_size);
    }

    // allocated bytes is the real size of the column
    void allocatedBytesAssert(MutableColumnPtr col, size_t expect_size) {
        EXPECT_EQ(col->allocated_bytes(), expect_size);
    }

    // clone_resized will clone the column and cut/append to the new column with the size of the original column
    void cloneResizedAssert(MutableColumnPtr col, size_t expect_size) {
        auto new_col = col->clone_resized(expect_size);
        EXPECT_EQ(new_col->size(), expect_size);
    }

    // get_shrinked_column should only happened in char-type column or nested char-type column
    //  just shrink the end zeros for char-type column which happened in segmentIterator
    //    eg. column_desc: char(6), insert into char(3), the char(3) will padding the 3 zeros at the end for writing to disk.
    //       but we select should just print the char(3) without the padding zeros
    //  limit and topN operation will trigger this function call
    void getShrinkedColumnAssert(MutableColumnPtr col, size_t spcific_size_defined) {
        EXPECT_TRUE(col->could_shrinked_column());
        auto new_col = col->get_shrinked_column();
        for (size_t i = 0; i < new_col->size(); i++) {
            EXPECT_EQ(col->get_data_at(i).size, spcific_size_defined);
        }
    }

    //serialize and deserialize which usually used in AGG function:
    //  serialize_value_into_arena, deserialize_and_insert_from_arena (called by AggregateFunctionDistinctMultipleGenericData, group_array_intersect, nested-types serder like: DataTypeArraySerDe::write_one_cell_to_jsonb)
    void ser_deserialize_with_arena_impl(MutableColumns& columns, const DataTypes& data_types) {
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
                    printColumn(*columns[i], *data_types[i]);
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
                    printColumn(*argument_columns[i], *data_types[i]);
                }
            }
            //            for (size_t i = 0; i < argument_columns.size(); ++i) {
            //                auto& column = argument_columns[i];
            //                begin = column->deserialize_and_insert_from_arena(begin);
            //                printColumn(*column, *data_types[i]);
            //            }
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
                std::cout << "input:" << input_keys[i].to_string() << std::endl;
            }
            for (const auto& column : columns) {
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
    void filterAssert(MutableColumnPtr col, std::vector<uint8_t> filter, size_t expect_size) {
        EXPECT_EQ(col->size(), filter.size());
        auto filted_col = col->filter(create_filter(filter), expect_size);
        EXPECT_EQ(filted_col->size(), expect_size);
    }

    //  filter_by_selector (called SegmentIterator::copy_column_data_by_selector,
    //  now just used in filter column, according to the selector to
    //  select size of row_ids for column by given column, which only used for predict_column and column_dictionary, column_nullable sometimes in Schema::get_predicate_column_ptr() also will return)
    void filterBySelectorAssert(vectorized::IColumn::MutablePtr col, std::vector<uint16_t> selector,
                                const IDataType& dt, MutableColumnPtr should_sel_col,
                                size_t expect_size) {
        // only used in column_nullable and predict_column, column_dictionary
        EXPECT_TRUE(col->is_nullable() || col->is_column_dictionary() ||
                    col->is_predicate_column());
        // for every data type should assert behavior in own UT case
        col->clear();
        col->insert_many_defaults(should_sel_col->size());
        std::cout << "col size:" << col->size() << std::endl;
        auto sel = should_sel_col->clone_empty();
        Status st = col->filter_by_selector(selector.data(), expect_size, sel);
        EXPECT_EQ(st, Status::OK());
        EXPECT_EQ(sel->size(), expect_size);
        printColumn(*sel, dt);
    }

    void assertPermutationsWithLimit(const IColumn::Permutation& lhs,
                                     const IColumn::Permutation& rhs, size_t limit) {
        if (limit == 0) {
            limit = lhs.size();
        }

        for (size_t i = 0; i < limit; ++i) {
            ASSERT_EQ(lhs[i], rhs[i]);
        }
    }

    // this function is common function to produce column, according to ColumnValueGetter
    // this range_size can be set, and it is helpfully make common column data which can be inserted into columns.
    template <typename ColumnValueGetter>
    void generateRanges(std::vector<std::vector<Field>>& ranges, size_t range_size,
                        ColumnValueGetter getter) {
        for (auto& range : ranges) {
            range.clear();
        }

        size_t ranges_size = ranges.size();

        for (size_t range_index = 0; range_index < ranges_size; ++range_index) {
            for (size_t index_in_range = 0; index_in_range < range_size; ++index_in_range) {
                auto value = getter(range_index, index_in_range);
                ranges[range_index].emplace_back(value);
            }
        }
    }

    void insertRangesIntoColumn(std::vector<std::vector<Field>>& ranges,
                                IColumn::Permutation& ranges_permutations,
                                vectorized::IColumn& column) {
        for (const auto& range_permutation : ranges_permutations) {
            auto& range = ranges[range_permutation];
            for (auto& value : range) {
                column.insert(value);
            }
        }
    }

    // this function helps to check sort permutation behavior for column
    void stableGetColumnPermutation(const IColumn& column, bool ascending, size_t limit,
                                    int nan_direction_hint, IColumn::Permutation& out_permutation) {
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
    //  by given a ColumnCreateFunc and ColumnValueGetter which how to generate a column value
    template <typename ColumnCreateFunc, typename ColumnValueGetter>
    void assertColumnPermutations(vectorized::IColumn& column,
                                  ColumnValueGetter columnValueGetter) {
        static constexpr size_t ranges_size = 3;
        static const std::vector<size_t> range_sizes = {1, 5, 50, 500};

        std::vector<std::vector<Field>> ranges(ranges_size);
        std::vector<size_t> ranges_permutations(ranges_size);
        std::iota(ranges_permutations.begin(), ranges_permutations.end(),
                  IColumn::Permutation::value_type(0));

        IColumn::Permutation actual_permutation;
        IColumn::Permutation expected_permutation;

        for (const auto& range_size : range_sizes) {
            // step1. generate range field data for column
            generateRanges(ranges, range_size, columnValueGetter);
            std::sort(ranges_permutations.begin(), ranges_permutations.end());
            IColumn::Permutation permutation(ranges_permutations.size());
            for (size_t i = 0; i < ranges_permutations.size(); ++i) {
                permutation[i] = ranges_permutations[i];
            }

            while (true) {
                // step2. insert range field data into column
                insertRangesIntoColumn(ranges, permutation, column);
                static constexpr size_t limit_parts = 4;

                size_t column_size = column.size();
                size_t column_limit_part = (column_size / limit_parts) + 1;

                for (size_t limit = 0; limit < column_size; limit += column_limit_part) {
                    assertColumnPermutation(column, true, limit, -1, actual_permutation,
                                            expected_permutation);
                    assertColumnPermutation(column, true, limit, 1, actual_permutation,
                                            expected_permutation);

                    assertColumnPermutation(column, false, limit, -1, actual_permutation,
                                            expected_permutation);
                    assertColumnPermutation(column, false, limit, 1, actual_permutation,
                                            expected_permutation);
                }

                assertColumnPermutation(column, true, 0, -1, actual_permutation,
                                        expected_permutation);
                assertColumnPermutation(column, true, 0, 1, actual_permutation,
                                        expected_permutation);

                assertColumnPermutation(column, false, 0, -1, actual_permutation,
                                        expected_permutation);
                assertColumnPermutation(column, false, 0, 1, actual_permutation,
                                        expected_permutation);

                if (!std::next_permutation(ranges_permutations.begin(), ranges_permutations.end()))
                    break;
            }
        }
    }

    // sort calculation: (which used in sort_block )
    //    get_permutation means sort data in Column as sort order
    //    limit should be set to limit the sort result
    //    nan_direction_hint deal with null|NaN value
    void assertColumnPermutation(const IColumn& column, bool ascending, size_t limit,
                                 int nan_direction_hint, IColumn::Permutation& actual_permutation,
                                 IColumn::Permutation& expected_permutation) {
        // step1. get expect permutation as stabled sort
        stableGetColumnPermutation(column, ascending, limit, nan_direction_hint,
                                   expected_permutation);

        // step2. get permutation by column
        column.get_permutation(ascending, limit, nan_direction_hint, expected_permutation);

        if (limit == 0) {
            limit = actual_permutation.size();
        }

        // step3. check the permutation result
        assertPermutationsWithLimit(actual_permutation, expected_permutation, limit);
    }

    //  permute()
    //   1/ Key topN set read_orderby_key_reverse = true; SegmentIterator::next_batch will permute the column by the given permutation(which reverse the rows of current segment)
    //  should check rows with the given permutation
    void assertPermute(MutableColumns& cols, IColumn::Permutation& permutation, size_t num_rows) {
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

    // sort_column
    //  1/ sort_column (called in sort_block to sort the column by given permutation)
    void assertSortColumn(IColumn& column, IColumn::Permutation& permutation, size_t num_rows) {
        // just make a simple sort function to sort the column
        std::vector<ColumnPtr> res_sorted;
        // SortColumnDescription:
        //    Column number;
        //    int direction;           /// 1 - ascending, -1 - descending.
        //    int nulls_direction;     /// 1 - NULLs and NaNs are greater, -1 - less.
        std::vector<SortColumnDescription> sort_desc;
        SortColumnDescription _column_with_descend_null_greater = {1, -1, 1};
        SortColumnDescription _column_with_descend_null_less = {1, -1, -1};
        SortColumnDescription _column_with_ascend_null_greater = {1, 1, 1};
        SortColumnDescription _column_with_ascend_null_less = {1, 1, -1};
        sort_desc.emplace_back(_column_with_descend_null_greater);
        sort_desc.emplace_back(_column_with_descend_null_less);
        sort_desc.emplace_back(_column_with_ascend_null_greater);
        sort_desc.emplace_back(_column_with_ascend_null_less);
        EqualFlags flags(num_rows, 1);
        EqualRange range {0, num_rows};
        for (auto& column_with_sort_desc : sort_desc) {
            ColumnSorter sorter({&column, column_with_sort_desc}, 0);
            column.sort_column(&sorter, flags, permutation, range, num_rows);
        };
        // check the sort result for flags and ranges
        for (size_t i = 0; i < num_rows; i++) {
            std::cout << "i: " << i << " " << flags[i] << std::endl;
        }
    }
};

} // namespace doris::vectorized