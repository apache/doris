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

#include <gtest/gtest.h>

#include <memory>

#include "orc/ColumnPrinter.hh"
#include "vec/columns/column_array.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_struct.h"
#include "vec/exec/format/orc/vorc_reader.h"

namespace doris {
namespace vectorized {
class OrcReaderConvertDictTest : public ::testing::Test {
protected:
    void SetUp() override {}

    void TearDown() override {}
};

std::unique_ptr<orc::EncodedStringVectorBatch> create_encoded_string_batch(
        const std::vector<std::string>& dict_values) {
    auto batch =
            std::make_unique<orc::EncodedStringVectorBatch>(1024 * 1024, *orc::getDefaultPool());
    batch->dictionary = std::make_unique<orc::StringDictionary>(*orc::getDefaultPool());

    // Fill dictionary data
    int sz = 0;
    for (const auto& value : dict_values) {
        sz += value.length();
    }

    batch->dictionary->dictionaryBlob.resize(sz + 1024);
    batch->dictionary->dictionaryOffset.resize(dict_values.size() + 10);
    int x = 0;
    for (const auto& value : dict_values) {
        batch->dictionary->dictionaryOffset[x + 1] =
                batch->dictionary->dictionaryOffset[x] + value.size();
        int y = batch->dictionary->dictionaryOffset[x];
        for (auto ch : value) {
            batch->dictionary->dictionaryBlob[y] = ch;
            y++;
        }
        x++;
    }

    return batch;
}

TEST_F(OrcReaderConvertDictTest, ConvertDictColumnToStringColumnBasic) {
    // Prepare dictionary data
    std::vector<std::string> dict_values = {"hello", "world", "doris", "test"};
    auto string_batch = create_encoded_string_batch(dict_values);

    // Prepare dictionary index column
    auto dict_column = ColumnInt32::create();
    std::vector<int32_t> indices = {0, 1, 2,
                                    3, 1, 0}; // "hello", "world", "doris", "test", "world", "hello"
    for (auto x : indices) {
        dict_column->insert(Field::create_field<TYPE_INT>(x));
    }

    // Create ORC type
    auto orc_type_ptr = createPrimitiveType(orc::TypeKind::STRING);

    TFileScanRangeParams params;
    TFileRangeDesc range;
    auto reader = OrcReader::create_unique(params, range, "", nullptr, true);

    // Execute conversion
    auto result_column = reader->_convert_dict_column_to_string_column(
            dict_column.get(), nullptr, string_batch.get(), orc_type_ptr.get());

    // Validate results
    auto* string_column = assert_cast<const ColumnString*>(result_column.get());
    ASSERT_EQ(string_column->size(), 6);
    ASSERT_EQ(string_column->get_data_at(0).to_string(), "hello");
    ASSERT_EQ(string_column->get_data_at(1).to_string(), "world");
    ASSERT_EQ(string_column->get_data_at(2).to_string(), "doris");
    ASSERT_EQ(string_column->get_data_at(3).to_string(), "test");
    ASSERT_EQ(string_column->get_data_at(4).to_string(), "world");
    ASSERT_EQ(string_column->get_data_at(5).to_string(), "hello");
}

TEST_F(OrcReaderConvertDictTest, ConvertDictColumnToStringColumnWithNulls) {
    // Prepare dictionary data
    std::vector<std::string> dict_values = {"hello", "world", "doris"};
    auto string_batch = create_encoded_string_batch(dict_values);

    // Prepare dictionary index column
    auto dict_column = ColumnInt32::create();
    std::vector<int32_t> indices = {0, 1, 2, 1, 0};
    for (auto x : indices) {
        dict_column->insert(Field::create_field<TYPE_INT>(x));
    }

    // Prepare null map
    NullMap null_map = {0, 1, 0, 0, 1}; // 2nd and 5th elements are null

    // Create ORC type
    auto orc_type_ptr = createPrimitiveType(orc::TypeKind::STRING);

    TFileScanRangeParams params;
    TFileRangeDesc range;
    auto _reader = OrcReader::create_unique(params, range, "", nullptr, true);

    // Execute conversion
    auto result_column = _reader->_convert_dict_column_to_string_column(
            dict_column.get(), &null_map, string_batch.get(), orc_type_ptr.get());

    // Validate results
    auto* string_column = assert_cast<const ColumnString*>(result_column.get());
    ASSERT_EQ(string_column->size(), 5);
    ASSERT_EQ(string_column->get_data_at(0).to_string(), "hello");
    ASSERT_EQ(string_column->get_data_at(1).to_string(), ""); // null value
    ASSERT_EQ(string_column->get_data_at(2).to_string(), "doris");
    ASSERT_EQ(string_column->get_data_at(3).to_string(), "world");
    ASSERT_EQ(string_column->get_data_at(4).to_string(), ""); // null value
}

TEST_F(OrcReaderConvertDictTest, ConvertDictColumnToStringColumnChar) {
    // Prepare dictionary data (CHAR type with right-padded spaces)
    std::vector<std::string> dict_values = {"hello  ", "world  ", "test   "};
    auto string_batch = create_encoded_string_batch(dict_values);

    // Prepare dictionary index column
    auto dict_column = ColumnInt32::create();
    std::vector<int32_t> indices = {0, 1, 2, 1};
    for (auto x : indices) {
        dict_column->insert(Field::create_field<TYPE_INT>(x));
    }

    // Create ORC CHAR type
    auto orc_type_ptr = createPrimitiveType(orc::TypeKind::CHAR);
    TFileScanRangeParams params;
    TFileRangeDesc range;
    auto _reader = OrcReader::create_unique(params, range, "", nullptr, true);

    // Execute conversion
    auto result_column = _reader->_convert_dict_column_to_string_column(
            dict_column.get(), nullptr, string_batch.get(), orc_type_ptr.get());

    // Validate results (should remove trailing spaces)
    auto* string_column = assert_cast<const ColumnString*>(result_column.get());
    ASSERT_EQ(string_column->size(), 4);
    ASSERT_EQ(string_column->get_data_at(0).to_string(), "hello"); // spaces removed
    ASSERT_EQ(string_column->get_data_at(1).to_string(), "world"); // spaces removed
    ASSERT_EQ(string_column->get_data_at(2).to_string(), "test");  // spaces removed
    ASSERT_EQ(string_column->get_data_at(3).to_string(), "world"); // spaces removed
}

TEST_F(OrcReaderConvertDictTest, ConvertDictColumnToStringColumnEmpty) {
    // Prepare empty dictionary data
    std::vector<std::string> dict_values = {""};
    auto string_batch = create_encoded_string_batch(dict_values);

    // Prepare dictionary index column
    auto dict_column = ColumnInt32::create();
    std::vector<int32_t> indices = {0, 0, 0};
    for (auto x : indices) {
        dict_column->insert(Field::create_field<TYPE_INT>(x));
    }

    // Create ORC type
    auto orc_type_ptr = createPrimitiveType(orc::TypeKind::STRING);
    TFileScanRangeParams params;
    TFileRangeDesc range;
    auto _reader = OrcReader::create_unique(params, range, "", nullptr, true);
    // Execute conversion
    auto result_column = _reader->_convert_dict_column_to_string_column(
            dict_column.get(), nullptr, string_batch.get(), orc_type_ptr.get());

    // Validate results
    auto* string_column = assert_cast<const ColumnString*>(result_column.get());
    ASSERT_EQ(string_column->size(), 3);
    ASSERT_EQ(string_column->get_data_at(0).to_string(), "");
    ASSERT_EQ(string_column->get_data_at(1).to_string(), "");
    ASSERT_EQ(string_column->get_data_at(2).to_string(), "");
}

TEST_F(OrcReaderConvertDictTest, ConvertDictColumnToStringColumnMixed) {
    // Prepare mixed length dictionary data
    std::vector<std::string> dict_values = {"", "a", "ab", "abc", "abcd"};
    auto string_batch = create_encoded_string_batch(dict_values);

    // Prepare dictionary index column
    auto dict_column = ColumnInt32::create();
    std::vector<int32_t> indices = {0, 1, 2, 3, 4, 2, 1, 0};
    for (auto x : indices) {
        dict_column->insert(Field::create_field<TYPE_INT>(x));
    }

    // Prepare partial null values
    NullMap null_map = {0, 0, 1, 0, 0, 1, 0, 0};

    // Create ORC type
    auto orc_type_ptr = createPrimitiveType(orc::TypeKind::STRING);
    TFileScanRangeParams params;
    TFileRangeDesc range;
    auto _reader = OrcReader::create_unique(params, range, "", nullptr, true);
    // Execute conversion
    auto result_column = _reader->_convert_dict_column_to_string_column(
            dict_column.get(), &null_map, string_batch.get(), orc_type_ptr.get());

    // Validate results
    auto* string_column = assert_cast<const ColumnString*>(result_column.get());
    ASSERT_EQ(string_column->size(), 8);
    ASSERT_EQ(string_column->get_data_at(0).to_string(), "");
    ASSERT_EQ(string_column->get_data_at(1).to_string(), "a");
    ASSERT_EQ(string_column->get_data_at(2).to_string(), ""); // null
    ASSERT_EQ(string_column->get_data_at(3).to_string(), "abc");
    ASSERT_EQ(string_column->get_data_at(4).to_string(), "abcd");
    ASSERT_EQ(string_column->get_data_at(5).to_string(), ""); // null
    ASSERT_EQ(string_column->get_data_at(6).to_string(), "a");
    ASSERT_EQ(string_column->get_data_at(7).to_string(), "");
}

} // namespace vectorized

} // namespace doris
