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

#include "format/csv/csv_reader.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <memory>

#include "runtime/runtime_profile.h"
#include "testutil/mock/mock_runtime_state.h"

namespace doris {

// Test that set_batch_size stores the value correctly.
TEST(CsvReaderSetBatchSizeTest, SetBatchSizeStoresValue) {
    TFileScanRangeParams params;
    params.format_type = TFileFormatType::FORMAT_CSV_PLAIN;
    params.__isset.file_attributes = true;
    params.file_attributes.__isset.text_params = true;
    params.file_attributes.text_params.column_separator = ",";
    params.file_attributes.text_params.line_delimiter = "\n";

    TFileRangeDesc range;
    range.path = "/nonexistent/test.csv";
    range.start_offset = 0;
    range.size = 0;

    auto runtime_state = std::make_unique<MockRuntimeState>();

    std::vector<SlotDescriptor*> file_slot_descs;
    auto reader = CsvReader::create_unique(runtime_state.get(), nullptr, nullptr, params, range,
                                           file_slot_descs, runtime_state->batch_size(), nullptr);

    // Default: _batch_size should be 0 (not set)
    // After set_batch_size, it should store the value
    reader->set_batch_size(128);
    // We can only verify indirectly that it was stored; the value is used
    // inside get_next_block(). Since we can't call get_next_block without
    // a fully initialized reader, we verify the interface doesn't crash.

    reader->set_batch_size(256);
    // Calling set_batch_size multiple times should be safe.

    reader->set_batch_size(0);
    // Setting to 0 should revert to default behavior.
}

// Test that set_batch_size is callable via the GenericReader interface.
TEST(CsvReaderSetBatchSizeTest, SetBatchSizeViaGenericInterface) {
    TFileScanRangeParams params;
    params.format_type = TFileFormatType::FORMAT_CSV_PLAIN;
    params.__isset.file_attributes = true;
    params.file_attributes.__isset.text_params = true;
    params.file_attributes.text_params.column_separator = ",";
    params.file_attributes.text_params.line_delimiter = "\n";

    TFileRangeDesc range;
    range.path = "/nonexistent/test.csv";
    range.start_offset = 0;
    range.size = 0;

    auto runtime_state = std::make_unique<MockRuntimeState>();

    std::vector<SlotDescriptor*> file_slot_descs;
    auto reader = CsvReader::create_unique(runtime_state.get(), nullptr, nullptr, params, range,
                                           file_slot_descs, runtime_state->batch_size(), nullptr);

    // Access through base class pointer — this is how FileScanner calls it.
    GenericReader* base_reader = reader.get();
    base_reader->set_batch_size(128);
    base_reader->set_batch_size(4096);
}

TEST(CsvReaderSchemaTest, InitWithoutRuntimeStateUsesDefaultQueryOptions) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_csv_reader_schema_without_state_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);
    const auto file_path = test_dir / "schema.csv";
    {
        std::ofstream output(file_path, std::ios::binary);
        ASSERT_TRUE(output.is_open());
        output << "alpha,beta\n";
    }

    TFileScanRangeParams params;
    params.__set_format_type(TFileFormatType::FORMAT_CSV_PLAIN);
    params.__set_file_type(TFileType::FILE_LOCAL);
    TFileAttributes attributes;
    TFileTextScanRangeParams text_params;
    text_params.__set_column_separator(",");
    text_params.__set_line_delimiter("\n");
    attributes.__set_text_params(std::move(text_params));
    params.__set_file_attributes(std::move(attributes));

    const auto file_size = static_cast<int64_t>(std::filesystem::file_size(file_path));
    TFileRangeDesc range;
    range.__set_path(file_path.string());
    range.__set_start_offset(0);
    range.__set_size(file_size);
    range.__set_file_size(file_size);
    range.__set_file_type(TFileType::FILE_LOCAL);

    std::vector<SlotDescriptor*> file_slot_descs;
    RuntimeProfile profile("csv_schema_reader_without_state_test");
    auto reader = CsvReader::create_unique(nullptr, &profile, nullptr, params, range,
                                           file_slot_descs, 1, nullptr);
    ASSERT_TRUE(reader->init_schema_reader().ok());

    std::vector<std::string> column_names;
    std::vector<DataTypePtr> column_types;
    ASSERT_TRUE(reader->get_parsed_schema(&column_names, &column_types).ok());
    EXPECT_EQ(column_names, (std::vector<std::string> {"c1", "c2"}));
    EXPECT_EQ(column_types.size(), 2);
    EXPECT_TRUE(reader->close().ok());
    std::filesystem::remove_all(test_dir);
}

} // namespace doris
