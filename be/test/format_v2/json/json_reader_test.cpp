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

#include "format_v2/json/json_reader.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <vector>

#include "common/object_pool.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "format_v2/column_data.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_profile.h"
#include "testutil/mock/mock_runtime_state.h"

namespace doris::format::json {
namespace {

TFileScanRangeParams json_scan_params(bool read_json_by_line = true, bool strip_outer_array = false,
                                      std::string jsonpaths = "", std::string json_root = "",
                                      bool ignore_malformed = false) {
    TFileScanRangeParams params;
    params.__set_format_type(TFileFormatType::FORMAT_JSON);
    params.__set_file_type(TFileType::FILE_LOCAL);
    params.__set_compress_type(TFileCompressType::PLAIN);
    TFileAttributes attributes;
    TFileTextScanRangeParams text_params;
    text_params.__set_line_delimiter("\n");
    attributes.__set_text_params(std::move(text_params));
    attributes.__set_read_json_by_line(read_json_by_line);
    attributes.__set_strip_outer_array(strip_outer_array);
    attributes.__set_num_as_string(false);
    attributes.__set_fuzzy_parse(false);
    if (!jsonpaths.empty()) {
        attributes.__set_jsonpaths(std::move(jsonpaths));
    }
    if (!json_root.empty()) {
        attributes.__set_json_root(std::move(json_root));
    }
    if (ignore_malformed) {
        attributes.__set_openx_json_ignore_malformed(true);
    }
    params.__set_file_attributes(std::move(attributes));
    return params;
}

SlotDescriptor* make_test_slot(ObjectPool* pool, int slot_id, int slot_idx, DataTypePtr type,
                               const std::string& name) {
    TSlotDescriptor slot_desc;
    slot_desc.__set_id(slot_id);
    slot_desc.__set_parent(0);
    slot_desc.__set_slotType(type->to_thrift());
    slot_desc.__set_columnPos(slot_idx);
    slot_desc.__set_byteOffset(0);
    if (type->is_nullable()) {
        slot_desc.__set_nullIndicatorByte(slot_idx / 8);
        slot_desc.__set_nullIndicatorBit(slot_idx % 8);
    } else {
        slot_desc.__set_nullIndicatorByte(0);
        slot_desc.__set_nullIndicatorBit(-1);
    }
    slot_desc.__set_slotIdx(slot_idx);
    slot_desc.__set_isMaterialized(true);
    slot_desc.__set_colName(name);
    return pool->add(new SlotDescriptor(slot_desc));
}

std::vector<SlotDescriptor*> build_slots(ObjectPool* pool) {
    return {make_test_slot(pool, 0, 0, make_nullable(std::make_shared<DataTypeInt32>()), "id"),
            make_test_slot(pool, 1, 1, make_nullable(std::make_shared<DataTypeString>()), "name")};
}

std::vector<SlotDescriptor*> build_slots_with_required_name(ObjectPool* pool) {
    return {make_test_slot(pool, 0, 0, make_nullable(std::make_shared<DataTypeInt32>()), "id"),
            make_test_slot(pool, 1, 1, std::make_shared<DataTypeString>(), "name")};
}

std::unique_ptr<io::FileDescription> file_description(const std::string& path) {
    auto desc = std::make_unique<io::FileDescription>();
    desc->path = path;
    desc->file_size = static_cast<int64_t>(std::filesystem::file_size(path));
    desc->range_start_offset = 0;
    desc->range_size = desc->file_size;
    return desc;
}

std::filesystem::path write_json_file(const std::string& name, const std::string& content) {
    const auto test_dir = std::filesystem::temp_directory_path() / "doris_format_v2_json_reader";
    std::filesystem::create_directories(test_dir);
    const auto file_path = test_dir / name;
    std::ofstream out(file_path);
    out << content;
    return file_path;
}

TFileRangeDesc file_range(const std::filesystem::path& file_path) {
    TFileRangeDesc range;
    range.__set_path(file_path.string());
    range.__set_start_offset(0);
    range.__set_size(static_cast<int64_t>(std::filesystem::file_size(file_path)));
    range.__set_file_size(static_cast<int64_t>(std::filesystem::file_size(file_path)));
    return range;
}

Block make_block(const std::vector<ColumnDefinition>& schema,
                 const std::vector<int32_t>& local_ids) {
    Block block;
    for (const auto local_id : local_ids) {
        const auto it = std::ranges::find_if(
                schema, [&](const auto& column) { return column.local_id == local_id; });
        EXPECT_TRUE(it != schema.end());
        block.insert({it->type->create_column(), it->type, it->name});
    }
    return block;
}

struct ReadResult {
    Status status;
    Status second_status = Status::OK();
    Block block;
    size_t rows = 0;
    bool eof = false;
    size_t second_rows = 0;
    bool second_eof = false;
    std::vector<ColumnDefinition> schema;
};

ReadResult read_once(const std::string& file_name, const std::string& content,
                     TFileScanRangeParams params, const std::vector<SlotDescriptor*>& slots,
                     const std::vector<int32_t>& requested_local_ids, bool read_twice = false) {
    const auto file_path = write_json_file(file_name, content);
    auto range = file_range(file_path);

    auto system_properties = std::make_shared<io::FileSystemProperties>();
    system_properties->system_type = TFileType::FILE_LOCAL;
    auto desc = file_description(file_path.string());
    RuntimeProfile profile("json_v2_reader_test");
    MockRuntimeState state;
    JsonReader reader(system_properties, desc, nullptr, &profile, &params, range, slots);

    ReadResult result;
    result.status = reader.init(&state);
    if (!result.status.ok()) {
        return result;
    }
    result.status = reader.get_schema(&result.schema);
    if (!result.status.ok()) {
        return result;
    }

    auto request = std::make_shared<FileScanRequest>();
    for (size_t i = 0; i < requested_local_ids.size(); ++i) {
        request->local_positions.emplace(LocalColumnId(requested_local_ids[i]), LocalIndex(i));
    }
    result.status = reader.open(request);
    if (!result.status.ok()) {
        return result;
    }

    result.block = make_block(result.schema, requested_local_ids);
    result.status = reader.get_block(&result.block, &result.rows, &result.eof);
    if (result.status.ok() && read_twice) {
        auto eof_block = make_block(result.schema, requested_local_ids);
        result.second_status =
                reader.get_block(&eof_block, &result.second_rows, &result.second_eof);
    }
    return result;
}

std::string nullable_string_at(const IColumn& column, size_t row) {
    const auto& nullable = assert_cast<const ColumnNullable&>(column);
    const auto& nested = assert_cast<const ColumnString&>(nullable.get_nested_column());
    return nested.get_data_at(row).to_string();
}

int32_t nullable_int_at(const IColumn& column, size_t row) {
    const auto& nullable = assert_cast<const ColumnNullable&>(column);
    const auto& nested = assert_cast<const ColumnInt32&>(nullable.get_nested_column());
    return nested.get_data()[row];
}

bool nullable_is_null_at(const IColumn& column, size_t row) {
    const auto& nullable = assert_cast<const ColumnNullable&>(column);
    return nullable.is_null_at(row);
}

} // namespace

TEST(JsonReaderTest, ReadsRequestedColumnsInFileScanRequestOrder) {
    ObjectPool pool;
    auto slots = build_slots(&pool);
    auto result = read_once("order.jsonl",
                            R"({"id":1,"name":"alice"})"
                            "\n"
                            R"({"id":2,"name":"bob"})"
                            "\n",
                            json_scan_params(), slots, {1, 0}, true);

    ASSERT_TRUE(result.status.ok()) << result.status.to_string();
    ASSERT_EQ(result.schema.size(), 2);
    EXPECT_EQ(result.schema[0].name, "id");
    EXPECT_EQ(result.schema[0].local_id, 0);
    EXPECT_EQ(result.schema[1].name, "name");
    EXPECT_EQ(result.schema[1].local_id, 1);
    ASSERT_EQ(result.rows, 2);
    ASSERT_EQ(result.block.columns(), 2);
    EXPECT_EQ(nullable_string_at(*result.block.get_by_position(0).column, 0), "alice");
    EXPECT_EQ(nullable_string_at(*result.block.get_by_position(0).column, 1), "bob");
    EXPECT_EQ(nullable_int_at(*result.block.get_by_position(1).column, 0), 1);
    EXPECT_EQ(nullable_int_at(*result.block.get_by_position(1).column, 1), 2);
    ASSERT_TRUE(result.second_status.ok()) << result.second_status.to_string();
    EXPECT_EQ(result.second_rows, 0);
    EXPECT_TRUE(result.second_eof);
}

TEST(JsonReaderTest, ReadsSingleDocumentOuterArray) {
    ObjectPool pool;
    auto slots = build_slots(&pool);
    auto result =
            read_once("outer_array.json", R"([{"id":3,"name":"carol"},{"id":4,"name":"dave"}])",
                      json_scan_params(false, true), slots, {0, 1});

    ASSERT_TRUE(result.status.ok()) << result.status.to_string();
    ASSERT_EQ(result.rows, 2);
    EXPECT_EQ(nullable_int_at(*result.block.get_by_position(0).column, 0), 3);
    EXPECT_EQ(nullable_string_at(*result.block.get_by_position(1).column, 0), "carol");
    EXPECT_EQ(nullable_int_at(*result.block.get_by_position(0).column, 1), 4);
    EXPECT_EQ(nullable_string_at(*result.block.get_by_position(1).column, 1), "dave");
}

TEST(JsonReaderTest, ReadsJsonRootByLine) {
    ObjectPool pool;
    auto slots = build_slots(&pool);
    auto result = read_once("json_root.jsonl",
                            R"({"payload":{"id":5,"name":"eve"}})"
                            "\n"
                            R"({"payload":{"id":6,"name":"frank"}})"
                            "\n",
                            json_scan_params(true, false, "", "$.payload"), slots, {0, 1});

    ASSERT_TRUE(result.status.ok()) << result.status.to_string();
    ASSERT_EQ(result.rows, 2);
    EXPECT_EQ(nullable_int_at(*result.block.get_by_position(0).column, 0), 5);
    EXPECT_EQ(nullable_string_at(*result.block.get_by_position(1).column, 0), "eve");
    EXPECT_EQ(nullable_int_at(*result.block.get_by_position(0).column, 1), 6);
    EXPECT_EQ(nullable_string_at(*result.block.get_by_position(1).column, 1), "frank");
}

TEST(JsonReaderTest, ReadsJsonPathsBySourceSlotAndReturnsRequestedBlockOrder) {
    ObjectPool pool;
    auto slots = build_slots(&pool);
    auto result = read_once("jsonpaths.jsonl",
                            R"({"payload":{"id":7,"user":"grace"}})"
                            "\n"
                            R"({"payload":{"id":8,"user":"heidi"}})"
                            "\n",
                            json_scan_params(true, false, R"(["$.payload.id","$.payload.user"])"),
                            slots, {1, 0});

    ASSERT_TRUE(result.status.ok()) << result.status.to_string();
    ASSERT_EQ(result.rows, 2);
    EXPECT_EQ(nullable_string_at(*result.block.get_by_position(0).column, 0), "grace");
    EXPECT_EQ(nullable_string_at(*result.block.get_by_position(0).column, 1), "heidi");
    EXPECT_EQ(nullable_int_at(*result.block.get_by_position(1).column, 0), 7);
    EXPECT_EQ(nullable_int_at(*result.block.get_by_position(1).column, 1), 8);
}

TEST(JsonReaderTest, ReadsJsonPathsFromSingleDocumentOuterArray) {
    ObjectPool pool;
    auto slots = build_slots(&pool);
    auto result = read_once(
            "outer_array_jsonpaths.json",
            R"([{"payload":{"id":12,"user":"kate"}},{"payload":{"id":13,"user":"leo"}}])",
            json_scan_params(false, true, R"(["$.payload.id","$.payload.user"])"), slots, {0, 1});

    ASSERT_TRUE(result.status.ok()) << result.status.to_string();
    ASSERT_EQ(result.rows, 2);
    EXPECT_EQ(nullable_int_at(*result.block.get_by_position(0).column, 0), 12);
    EXPECT_EQ(nullable_string_at(*result.block.get_by_position(1).column, 0), "kate");
    EXPECT_EQ(nullable_int_at(*result.block.get_by_position(0).column, 1), 13);
    EXPECT_EQ(nullable_string_at(*result.block.get_by_position(1).column, 1), "leo");
}

TEST(JsonReaderTest, FillsMissingNullableColumnWithNull) {
    ObjectPool pool;
    auto slots = build_slots(&pool);
    auto result = read_once("missing_nullable.jsonl",
                            R"({"id":9})"
                            "\n",
                            json_scan_params(), slots, {0, 1});

    ASSERT_TRUE(result.status.ok()) << result.status.to_string();
    ASSERT_EQ(result.rows, 1);
    EXPECT_EQ(nullable_int_at(*result.block.get_by_position(0).column, 0), 9);
    EXPECT_TRUE(nullable_is_null_at(*result.block.get_by_position(1).column, 0));
}

TEST(JsonReaderTest, ReturnsErrorForMissingRequiredColumn) {
    ObjectPool pool;
    auto slots = build_slots_with_required_name(&pool);
    auto result = read_once("missing_required.jsonl",
                            R"({"id":10})"
                            "\n",
                            json_scan_params(), slots, {0, 1});

    EXPECT_FALSE(result.status.ok());
}

TEST(JsonReaderTest, ReadsPresentRequiredColumn) {
    ObjectPool pool;
    auto slots = build_slots_with_required_name(&pool);
    auto result = read_once("present_required.jsonl",
                            R"({"id":14,"name":"mallory"})"
                            "\n",
                            json_scan_params(), slots, {0, 1});

    ASSERT_TRUE(result.status.ok()) << result.status.to_string();
    ASSERT_EQ(result.rows, 1);
    EXPECT_EQ(nullable_int_at(*result.block.get_by_position(0).column, 0), 14);
    EXPECT_EQ(nullable_string_at(*result.block.get_by_position(1).column, 0), "mallory");
}

TEST(JsonReaderTest, ReturnsErrorForMalformedJsonByDefault) {
    ObjectPool pool;
    auto slots = build_slots(&pool);
    auto result = read_once("malformed_strict.jsonl",
                            "not-json\n"
                            R"({"id":11,"name":"judy"})"
                            "\n",
                            json_scan_params(), slots, {0, 1});

    EXPECT_FALSE(result.status.ok());
}

TEST(JsonReaderTest, IgnoresMalformedJsonWhenConfigured) {
    ObjectPool pool;
    auto slots = build_slots(&pool);
    auto result = read_once("ignore_malformed.jsonl",
                            "not-json\n"
                            R"({"id":11,"name":"judy"})"
                            "\n",
                            json_scan_params(true, false, "", "", true), slots, {0, 1});

    ASSERT_TRUE(result.status.ok()) << result.status.to_string();
    ASSERT_EQ(result.rows, 1);
    EXPECT_EQ(nullable_int_at(*result.block.get_by_position(0).column, 0), 11);
    EXPECT_EQ(nullable_string_at(*result.block.get_by_position(1).column, 0), "judy");
}

TEST(JsonReaderTest, SkipsEmptyJsonLine) {
    ObjectPool pool;
    auto slots = build_slots(&pool);
    auto result = read_once("empty_line.jsonl",
                            "\n"
                            R"({"id":15,"name":"nancy"})"
                            "\n",
                            json_scan_params(), slots, {0, 1});

    ASSERT_TRUE(result.status.ok()) << result.status.to_string();
    ASSERT_EQ(result.rows, 1);
    EXPECT_EQ(nullable_int_at(*result.block.get_by_position(0).column, 0), 15);
    EXPECT_EQ(nullable_string_at(*result.block.get_by_position(1).column, 0), "nancy");
}

} // namespace doris::format::json
