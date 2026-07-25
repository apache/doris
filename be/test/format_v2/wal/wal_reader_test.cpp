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

#include "format_v2/wal/wal_reader.h"

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <memory>

#include "agent/be_exec_version_manager.h"
#include "core/block/block.h"
#include "core/column/column_string.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "io/fs/local_file_system.h"
#include "load/group_commit/wal/wal_file_reader.h"
#include "load/group_commit/wal/wal_writer.h"

namespace doris::format::wal {
namespace {

std::string temporary_wal_path() {
    const auto root = std::filesystem::temp_directory_path() /
                      ("doris-wal-v2-" +
                       std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    const auto path = root / "1" / "2" / "1_1_1_test";
    std::filesystem::create_directories(path.parent_path());
    return path.string();
}

PBlock serialize_block(const Block& block) {
    PBlock pblock;
    size_t uncompressed_bytes = 0;
    size_t compressed_bytes = 0;
    int64_t compress_time = 0;
    EXPECT_TRUE(block.serialize(BeExecVersionManager::get_newest_version(), &pblock,
                                &uncompressed_bytes, &compressed_bytes, &compress_time,
                                segment_v2::CompressionTypePB::SNAPPY)
                        .ok());
    return pblock;
}

} // namespace

TEST(WalReaderV2Test, ParseColumnIdsPreservesHeaderOrder) {
    std::vector<int32_t> column_ids;
    ASSERT_TRUE(parse_wal_column_ids("17,4,99", &column_ids).ok());
    EXPECT_EQ(column_ids, (std::vector<int32_t> {17, 4, 99}));
}

TEST(WalReaderV2Test, ParseColumnIdsRejectsMalformedOrAmbiguousHeaders) {
    std::vector<int32_t> column_ids;
    EXPECT_FALSE(parse_wal_column_ids("", &column_ids).ok());
    EXPECT_FALSE(parse_wal_column_ids("17,,99", &column_ids).ok());
    EXPECT_FALSE(parse_wal_column_ids("17,nope,99", &column_ids).ok());
    EXPECT_FALSE(parse_wal_column_ids("17,4,17", &column_ids).ok());
}

TEST(WalReaderV2Test, WriterBackedReaderPreservesNestedSchemaAndMovesFirstPayload) {
    const auto wal_path = temporary_wal_path();
    const auto int_type = make_nullable(std::make_shared<DataTypeInt32>());
    const auto array_type = make_nullable(std::make_shared<DataTypeArray>(int_type));
    const auto string_type = make_nullable(std::make_shared<DataTypeString>());
    const auto map_type = make_nullable(std::make_shared<DataTypeMap>(string_type, int_type));
    const auto struct_type = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {int_type, array_type}, Strings {"id", "nested_items"}));

    Block source;
    auto array_column = array_type->create_column();
    array_column->insert_default();
    source.insert({std::move(array_column), array_type, "items"});
    auto string_column = string_type->create_column();
    string_column->insert_data("value", 5);
    source.insert({std::move(string_column), string_type, "renamed_later"});
    auto map_column = map_type->create_column();
    map_column->insert_default();
    source.insert({std::move(map_column), map_type, "properties"});
    auto struct_column = struct_type->create_column();
    struct_column->insert_default();
    source.insert({std::move(struct_column), struct_type, "record"});
    auto pblock = serialize_block(source);

    WalWriter writer(wal_path);
    ASSERT_TRUE(writer.init(io::global_local_filesystem()).ok());
    ASSERT_TRUE(writer.append_header("17,4,88,99").ok());
    ASSERT_TRUE(writer.append_blocks({&pblock}).ok());
    ASSERT_TRUE(writer.finalize().ok());

    std::shared_ptr<io::FileSystemProperties> properties;
    std::unique_ptr<io::FileDescription> description;
    WalReader reader(properties, description, nullptr, nullptr, {});
    reader._wal_reader = std::make_shared<WalFileReader>(wal_path);
    ASSERT_TRUE(reader._wal_reader->init().ok());
    std::string encoded_ids;
    ASSERT_TRUE(reader._wal_reader->read_header(reader._version, encoded_ids).ok());
    ASSERT_TRUE(parse_wal_column_ids(encoded_ids, &reader._column_ids).ok());

    std::vector<ColumnDefinition> schema;
    ASSERT_TRUE(reader.get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 4);
    ASSERT_EQ(schema[0].children.size(), 1);
    EXPECT_TRUE(schema[0].children[0].type->is_nullable());
    ASSERT_EQ(schema[2].children.size(), 2);
    EXPECT_TRUE(schema[2].children[0].type->is_nullable());
    EXPECT_TRUE(schema[2].children[1].type->is_nullable());
    ASSERT_EQ(schema[3].children.size(), 2);
    EXPECT_EQ(schema[3].children[0].name, "id");
    EXPECT_EQ(schema[3].children[1].name, "nested_items");
    ASSERT_EQ(schema[3].children[1].children.size(), 1);

    auto request = std::make_shared<FileScanRequest>();
    request->local_positions.emplace(LocalColumnId(1), LocalIndex(0));
    request->local_positions.emplace(LocalColumnId(0), LocalIndex(1));
    ASSERT_TRUE(reader.open(std::move(request)).ok());
    Block output({
            {string_type->create_column(), string_type, "renamed"},
            {array_type->create_column(), array_type, "items"},
    });
    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader.get_block(&output, &rows, &eof).ok());
    EXPECT_EQ(rows, 1);
    EXPECT_FALSE(eof);
    EXPECT_EQ(reader._first_block.ByteSizeLong(), 0);
    EXPECT_EQ(output.get_by_position(0).column->get_data_at(0).to_string(), "value");

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(
            std::filesystem::path(wal_path).parent_path().parent_path().parent_path());
}

TEST(WalReaderV2Test, MaterializationTransfersColumnOwnership) {
    std::shared_ptr<io::FileSystemProperties> properties;
    std::unique_ptr<io::FileDescription> description;
    WalReader reader(properties, description, nullptr, nullptr, {});
    reader._request = std::make_shared<FileScanRequest>();
    reader._request->local_positions.emplace(LocalColumnId(0), LocalIndex(0));

    const auto type = std::make_shared<DataTypeString>();
    auto source_column = ColumnString::create();
    source_column->insert_data("payload", 7);
    const auto* original = source_column.get();
    Block source({{std::move(source_column), type, "value"}});
    Block output({{type->create_column(), type, "value"}});

    ASSERT_TRUE(reader._materialize_requested_columns(&source, &output).ok());
    EXPECT_FALSE(static_cast<bool>(source.get_by_position(0).column));
    EXPECT_EQ(output.get_by_position(0).column.get(), original);
}

} // namespace doris::format::wal
