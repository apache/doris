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

#include <filesystem>
#include <fstream>
#include <memory>

#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_file.h"
#include "core/column/column_nullable.h"
#include "core/column/column_struct.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_file.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "exec/scan/scanner.h"
#include "format/json/new_json_reader.h"
#include "testutil/mock/mock_runtime_state.h"
#include "util/uid_util.h"

namespace doris {

static constexpr size_t kDefaultBatchSize = 4064;

// Test that set_batch_size stores the value correctly.
TEST(NewJsonReaderSetBatchSizeTest, SetBatchSizeStoresValue) {
    TFileScanRangeParams params;
    params.format_type = TFileFormatType::FORMAT_JSON;
    params.__isset.file_attributes = true;
    params.file_attributes.__isset.text_params = true;
    params.file_attributes.text_params.line_delimiter = "\n";

    TFileRangeDesc range;
    range.path = "/nonexistent/test.json";
    range.start_offset = 0;
    range.size = 0;

    std::vector<SlotDescriptor*> file_slot_descs;
    // Use the second constructor (profile, params, range, file_slot_descs, io_ctx)
    // to avoid the first constructor's ADD_TIMER(_profile, ...) which crashes on nullptr.
    auto reader = NewJsonReader::create_unique(nullptr, params, range, file_slot_descs,
                                               kDefaultBatchSize, nullptr);

    // Default: _batch_size is initialized to _MIN_BATCH_SIZE.
    EXPECT_EQ(reader->get_batch_size(), 4064U);

    // After set_batch_size, it should store the value (clamped to >=_MIN_BATCH_SIZE).
    reader->set_batch_size(8192);
    EXPECT_EQ(reader->get_batch_size(), 8192U);

    // Calling set_batch_size multiple times should update the value.
    reader->set_batch_size(16384);
    EXPECT_EQ(reader->get_batch_size(), 16384U);

    // Setting below _MIN_BATCH_SIZE (or 0) clamps to 1 so the
    // reader never spins on empty blocks.
    reader->set_batch_size(0);
    EXPECT_EQ(reader->get_batch_size(), 1UL);
}

// Test that set_batch_size is callable via the GenericReader interface.
TEST(NewJsonReaderSetBatchSizeTest, SetBatchSizeViaGenericInterface) {
    TFileScanRangeParams params;
    params.format_type = TFileFormatType::FORMAT_JSON;
    params.__isset.file_attributes = true;
    params.file_attributes.__isset.text_params = true;
    params.file_attributes.text_params.line_delimiter = "\n";

    TFileRangeDesc range;
    range.path = "/nonexistent/test.json";
    range.start_offset = 0;
    range.size = 0;

    std::vector<SlotDescriptor*> file_slot_descs;
    // Use the second constructor to avoid nullptr profile crash in ADD_TIMER.
    auto reader = NewJsonReader::create_unique(nullptr, params, range, file_slot_descs,
                                               kDefaultBatchSize, nullptr);

    // Access through base class pointer — this is how FileScanner calls it.
    GenericReader* base_reader = reader.get();
    base_reader->set_batch_size(8192);
    EXPECT_EQ(base_reader->get_batch_size(), 8192U);
    base_reader->set_batch_size(4096);
    EXPECT_EQ(base_reader->get_batch_size(), 4096U);
}

TEST(NewJsonReaderCowTest, AppendNullForMalformedJsonMutatesOwnerColumn) {
    auto nested_column = ColumnInt32::create();
    nested_column->insert_value(7);
    auto null_map = ColumnUInt8::create();
    null_map->insert_value(0);
    ColumnPtr shared_column = ColumnNullable::create(std::move(nested_column), std::move(null_map));
    const auto* original_column = shared_column.get();

    Block block;
    block.insert({shared_column, make_nullable(std::make_shared<DataTypeInt32>()), "c0"});

    ASSERT_TRUE(json_reader_detail::append_null_for_malformed_json(block).ok());
    ASSERT_EQ(block.rows(), 2);
    EXPECT_NE(block.get_by_position(0).column.get(), original_column);

    const auto& result_column =
            assert_cast<const ColumnNullable&>(*block.get_by_position(0).column);
    EXPECT_FALSE(result_column.is_null_at(0));
    EXPECT_TRUE(result_column.is_null_at(1));

    const auto& original_nullable = assert_cast<const ColumnNullable&>(*shared_column);
    EXPECT_EQ(original_nullable.size(), 1);
    EXPECT_FALSE(original_nullable.is_null_at(0));
}

TEST(NewJsonReaderCowTest, TruncateBlockToRowsMutatesOwnerColumn) {
    auto nested_column = ColumnInt32::create();
    nested_column->insert_value(7);
    nested_column->insert_value(8);
    auto null_map = ColumnUInt8::create();
    null_map->insert_value(0);
    null_map->insert_value(0);
    ColumnPtr shared_column = ColumnNullable::create(std::move(nested_column), std::move(null_map));
    const auto* original_column = shared_column.get();

    Block block;
    block.insert({shared_column, make_nullable(std::make_shared<DataTypeInt32>()), "c0"});

    json_reader_detail::truncate_block_to_rows(block, 1);
    ASSERT_EQ(block.rows(), 1);
    EXPECT_NE(block.get_by_position(0).column.get(), original_column);

    const auto& result_column =
            assert_cast<const ColumnNullable&>(*block.get_by_position(0).column);
    EXPECT_EQ(result_column.size(), 1);
    EXPECT_FALSE(result_column.is_null_at(0));

    const auto& original_nullable = assert_cast<const ColumnNullable&>(*shared_column);
    EXPECT_EQ(original_nullable.size(), 2);
}

TEST(NewJsonReaderCowTest, PopBackLastInsertedValueMutatesOwnerColumn) {
    auto column = ColumnInt32::create();
    column->insert_value(7);
    column->insert_value(8);
    ColumnPtr shared_column = std::move(column);
    const auto* original_column = shared_column.get();

    Block block;
    block.insert({shared_column, std::make_shared<DataTypeInt32>(), "c0"});

    json_reader_detail::pop_back_last_inserted_value(block, 0);
    ASSERT_EQ(block.rows(), 1);
    EXPECT_NE(block.get_by_position(0).column.get(), original_column);

    const auto& result_column = assert_cast<const ColumnInt32&>(*block.get_by_position(0).column);
    EXPECT_EQ(result_column.size(), 1);
    EXPECT_EQ(result_column.get_data()[0], 7);

    const auto& original_int_column = assert_cast<const ColumnInt32&>(*shared_column);
    EXPECT_EQ(original_int_column.size(), 2);
    EXPECT_EQ(original_int_column.get_data()[0], 7);
    EXPECT_EQ(original_int_column.get_data()[1], 8);
}

class NewJsonReaderFileLoadTest : public testing::Test {
protected:
    void check_load(bool strict, bool nested, bool jsonpaths) {
        const std::string good =
                R"({"uri":"urn:good","offset":0,"size":3,"content_type":"application/octet-stream","checksum":null,"inline":"AP9B"})";
        const std::string bad =
                R"({"uri":"urn:bad","offset":0,"size":3,"content_type":null,"checksum":null,"inline":"!!!!"})";
        const std::string empty =
                R"({"uri":"urn:empty","offset":0,"size":0,"content_type":null,"checksum":null,"inline":""})";
        const auto path = std::filesystem::temp_directory_path() /
                          ("doris_file_load_" + UniqueId::gen_uid().to_string() + ".json");
        {
            std::ofstream output(path);
            ASSERT_TRUE(output.good());
            const std::vector<std::string> values {good, bad, "null", empty};
            for (size_t i = 0; i < values.size(); ++i) {
                const auto value =
                        nested && i != 2 ? "{\"f\":" + values[i] + ",\"n\":7}" : values[i];
                output << "{\"id\":" << i << ",\"p\":" << value << ",\"tail\":\"tail" << i
                       << "\"}\n";
            }
        }
        auto file_type = make_nullable(std::make_shared<DataTypeFile>());
        DataTypePtr value_type = file_type;
        if (nested) {
            value_type = make_nullable(std::make_shared<DataTypeStruct>(
                    DataTypes {file_type, make_nullable(std::make_shared<DataTypeInt32>())},
                    Strings {"f", "n"}));
        }
        const DataTypes types {make_nullable(std::make_shared<DataTypeString>()), value_type,
                               make_nullable(std::make_shared<DataTypeString>())};
        const Strings names {"id", "p", "tail"};
        std::vector<std::unique_ptr<SlotDescriptor>> slots;
        std::vector<SlotDescriptor*> slot_ptrs;
        Block block;
        for (int i = 0; i < types.size(); ++i) {
            TSlotDescriptor descriptor;
            descriptor.__set_id(i);
            descriptor.__set_parent(0);
            descriptor.__set_slotType(types[i]->to_thrift());
            descriptor.__set_columnPos(i);
            descriptor.__set_byteOffset(0);
            descriptor.__set_nullIndicatorByte(0);
            descriptor.__set_nullIndicatorBit(i);
            descriptor.__set_slotIdx(i);
            descriptor.__set_isMaterialized(true);
            descriptor.__set_colName(names[i]);
            slots.push_back(std::make_unique<SlotDescriptor>(descriptor));
            slot_ptrs.push_back(slots.back().get());
            block.insert({types[i]->create_column(), types[i], names[i]});
        }
        TFileScanRangeParams params;
        params.__set_format_type(TFileFormatType::FORMAT_JSON);
        params.__set_file_type(TFileType::FILE_LOCAL);
        params.__set_compress_type(TFileCompressType::PLAIN);
        params.__set_strict_mode(strict);
        TFileAttributes attributes;
        TFileTextScanRangeParams text;
        text.__set_line_delimiter("\n");
        attributes.__set_text_params(text);
        attributes.__set_read_json_by_line(true);
        attributes.__set_strip_outer_array(false);
        if (jsonpaths) attributes.__set_jsonpaths(R"(["$.id","$.p","$.tail"])");
        params.__set_file_attributes(attributes);
        TFileRangeDesc range;
        range.__set_path(path.string());
        range.__set_start_offset(0);
        range.__set_size(std::filesystem::file_size(path));
        range.__set_file_size(range.size);
        MockRuntimeState state;
        RuntimeProfile profile("file_text_load");
        ScannerCounter counter;
        bool scanner_eof = false;
        auto reader = NewJsonReader::create_unique(&state, &profile, &counter, params, range,
                                                   slot_ptrs, &scanner_eof, 1024, nullptr);
        const auto init_status = reader->init_reader({}, true);
        ASSERT_TRUE(init_status.ok()) << init_status;
        size_t rows = 0;
        bool eof = false;
        const auto status = reader->_do_get_next_block(&block, &rows, &eof);
        ASSERT_TRUE(status.ok()) << status;
        const bool filter_bad = strict && !nested;
        ASSERT_EQ(rows, filter_bad ? 3 : 4);
        EXPECT_EQ(counter.num_rows_filtered, filter_bad ? 1 : 0);
        for (const auto& column : block.get_columns()) ASSERT_EQ(column->size(), rows);
        EXPECT_EQ(block.get_by_position(0).column->get_data_at(rows - 1).to_string(), "3");
        EXPECT_EQ(block.get_by_position(2).column->get_data_at(rows - 1).to_string(), "tail3");
        const auto& parent = assert_cast<const ColumnNullable&>(*block.get_by_position(1).column);
        EXPECT_TRUE(parent.is_null_at(filter_bad ? 1 : 2));
        const ColumnNullable* files = &parent;
        if (nested) {
            ASSERT_FALSE(parent.is_null_at(1));
            const auto& structure = assert_cast<const ColumnStruct&>(parent.get_nested_column());
            files = &assert_cast<const ColumnNullable&>(structure.get_column(0));
            const auto& sibling = assert_cast<const ColumnNullable&>(structure.get_column(1));
            EXPECT_FALSE(sibling.is_null_at(1));
            EXPECT_EQ(sibling.get_nested_column().get_int(1), 7);
        }
        if (!filter_bad) EXPECT_TRUE(files->is_null_at(1));
        const auto& file = assert_cast<const ColumnFile&>(files->get_nested_column());
        EXPECT_EQ(file.get_column(5).get_data_at(0).to_string(), std::string("\0\xff"
                                                                             "A",
                                                                             3));
        EXPECT_FALSE(file.get_column(5).is_null_at(rows - 1));
        EXPECT_EQ(file.get_column(5).get_data_at(rows - 1).size, 0);
        std::filesystem::remove(path);
    }
};

TEST_F(NewJsonReaderFileLoadTest, TopLevelStrictAndNonStrict) {
    for (bool strict : {false, true}) {
        for (bool jsonpaths : {false, true}) {
            SCOPED_TRACE(testing::Message() << "strict=" << strict << " jsonpaths=" << jsonpaths);
            check_load(strict, false, jsonpaths);
        }
    }
}

TEST_F(NewJsonReaderFileLoadTest, NestedInvalidFilePreservesSibling) {
    for (bool strict : {false, true}) {
        for (bool jsonpaths : {false, true}) {
            SCOPED_TRACE(testing::Message() << "strict=" << strict << " jsonpaths=" << jsonpaths);
            check_load(strict, true, jsonpaths);
        }
    }
}

} // namespace doris
