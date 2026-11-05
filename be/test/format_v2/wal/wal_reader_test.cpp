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
#include <tuple>
#include <vector>

#include "agent/be_exec_version_manager.h"
#include "core/block/block.h"
#include "core/column/column_string.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "format_v2/column_mapper.h"
#include "format_v2/table_reader.h"
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

void expect_wal_column(const IColumn& column, const std::string& name, size_t expected_rows,
                       size_t offset) {
    ASSERT_EQ(column.size(), expected_rows);
    for (size_t row = 0; row < expected_rows; ++row) {
        EXPECT_EQ(column.get_data_at(row).to_string(), name + std::to_string(offset + row));
    }
}

void expect_wal_batch(WalReader* reader, Block* output, size_t expected_columns,
                      size_t expected_rows, size_t offset) {
    output->clear_column_data();
    size_t rows = 0;
    bool eof = true;
    ASSERT_TRUE(reader->get_block(output, &rows, &eof).ok());
    // Projection can remove every output column while the WAL block still contributes rows.
    EXPECT_EQ(rows, expected_rows);
    EXPECT_FALSE(eof);
    ASSERT_EQ(output->columns(), expected_columns);
    for (const auto& column : output->get_columns_with_type_and_name()) {
        ASSERT_NO_FATAL_FAILURE(
                expect_wal_column(*column.column, column.name, expected_rows, offset));
    }
}

void initialize_wal_reader(WalReader* reader, const std::string& path, bool discover_schema) {
    reader->_wal_reader = std::make_shared<WalFileReader>(path);
    ASSERT_TRUE(reader->_wal_reader->init().ok());
    std::string encoded_ids;
    ASSERT_TRUE(reader->_wal_reader->read_header(reader->_version, encoded_ids).ok());
    ASSERT_TRUE(parse_wal_column_ids(encoded_ids, &reader->_column_ids).ok());
    if (discover_schema) {
        std::vector<ColumnDefinition> schema;
        ASSERT_TRUE(reader->get_schema(&schema).ok());
        ASSERT_EQ(schema.size(), 2);
    }
}

void expect_wal_eof(WalReader* reader, Block* output) {
    output->clear_column_data();
    size_t rows = 99;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(output, &rows, &eof).ok());
    EXPECT_EQ(rows, 0);
    EXPECT_TRUE(eof);
    EXPECT_EQ(output->rows(), 0);
}

class WalReaderV2ProjectionTest
        : public testing::TestWithParam<std::tuple<std::vector<int>, bool>> {
protected:
    void SetUp() override {
        _wal_path = temporary_wal_path();
        const auto type = std::make_shared<DataTypeString>();
        WalWriter writer(_wal_path);
        ASSERT_TRUE(writer.init(io::global_local_filesystem()).ok());
        ASSERT_TRUE(writer.append_header("17,4").ok());
        size_t offset = 0;
        for (const size_t batch_rows : {3, 2}) {
            Block source;
            for (const auto& name : {"left_value", "right_value"}) {
                auto column = type->create_column();
                for (size_t row = 0; row < batch_rows; ++row) {
                    const auto value = std::string(name) + std::to_string(offset + row);
                    column->insert_data(value.data(), value.size());
                }
                source.insert({std::move(column), type, name});
            }
            auto pblock = serialize_block(source);
            ASSERT_TRUE(writer.append_blocks({&pblock}).ok());
            offset += batch_rows;
        }
        ASSERT_TRUE(writer.finalize().ok());
    }

    void TearDown() override {
        std::filesystem::remove_all(
                std::filesystem::path(_wal_path).parent_path().parent_path().parent_path());
    }

    std::string _wal_path;
};

} // namespace

TEST_P(WalReaderV2ProjectionTest, PreservesRowsAcrossBlocksAndEof) {
    const auto& [projected_columns, discover_schema] = GetParam();
    std::shared_ptr<io::FileSystemProperties> properties;
    std::unique_ptr<io::FileDescription> description;
    WalReader reader(properties, description, nullptr, nullptr, {});
    ASSERT_NO_FATAL_FAILURE(initialize_wal_reader(&reader, _wal_path, discover_schema));

    auto request = std::make_shared<FileScanRequest>();
    Block output;
    const auto type = std::make_shared<DataTypeString>();
    const std::vector<std::string> names {"left_value", "right_value"};
    for (size_t position = 0; position < projected_columns.size(); ++position) {
        const auto source_position = projected_columns[position];
        request->local_positions.emplace(LocalColumnId(source_position), LocalIndex(position));
        output.insert({type->create_column(), type, names[source_position]});
    }
    ASSERT_TRUE(reader.open(std::move(request)).ok());

    size_t offset = 0;
    for (const size_t expected_rows : {3, 2}) {
        ASSERT_NO_FATAL_FAILURE(expect_wal_batch(&reader, &output, projected_columns.size(),
                                                 expected_rows, offset));
        offset += expected_rows;
    }
    for (int attempt = 0; attempt < 2; ++attempt) {
        ASSERT_NO_FATAL_FAILURE(expect_wal_eof(&reader, &output));
    }
    ASSERT_TRUE(reader.close().ok());
}

INSTANTIATE_TEST_SUITE_P(WriterBacked, WalReaderV2ProjectionTest,
                         testing::Combine(testing::Values(std::vector<int> {}, std::vector<int> {0},
                                                          std::vector<int> {1},
                                                          std::vector<int> {1, 0}),
                                          testing::Bool()));

TEST(WalReaderV2Test, ParseColumnIdsPreservesHeaderOrder) {
    std::vector<int32_t> column_ids;
    ASSERT_TRUE(parse_wal_column_ids("17,4,99", &column_ids).ok());
    EXPECT_EQ(column_ids, (std::vector<int32_t> {17, 4, 99}));
}

TEST(WalReaderV2Test, ParseColumnIdsRejectsMalformedHeaders) {
    std::vector<int32_t> column_ids;
    EXPECT_FALSE(parse_wal_column_ids("", &column_ids).ok());
    EXPECT_FALSE(parse_wal_column_ids("17,,99", &column_ids).ok());
    EXPECT_FALSE(parse_wal_column_ids("17,nope,99", &column_ids).ok());
}

TEST(WalReaderV2Test, SchemaChangeDuplicateIdsKeepBaseColumnAndPhysicalPositions) {
    const auto wal_path = temporary_wal_path();
    const auto int_type = make_nullable(std::make_shared<DataTypeInt32>());
    const auto bigint_type = make_nullable(std::make_shared<DataTypeInt64>());
    Block source;
    auto base = int_type->create_column();
    base->insert(Field::create_field<TYPE_INT>(20));
    source.insert({std::move(base), int_type, "v"});
    auto shadow = bigint_type->create_column();
    shadow->insert(Field::create_field<TYPE_BIGINT>(200));
    source.insert({std::move(shadow), bigint_type, "__doris_shadow_v"});
    auto trailing = int_type->create_column();
    trailing->insert(Field::create_field<TYPE_INT>(30));
    source.insert({std::move(trailing), int_type, "other"});
    auto pblock = serialize_block(source);

    WalWriter writer(wal_path);
    ASSERT_TRUE(writer.init(io::global_local_filesystem()).ok());
    ASSERT_TRUE(writer.append_header("15,15,17").ok());
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
    EXPECT_EQ(reader._column_ids, (std::vector<int32_t> {15, 15, 17}));

    std::vector<ColumnDefinition> schema;
    ASSERT_TRUE(reader.get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 3);
    // The ALTER has finished: the table expects BIGINT, but replay must read the base INT.
    auto changed = schema[0];
    changed.type = bigint_type;
    const std::vector<ColumnDefinition> projected {schema[2], changed};
    auto mapper = reader.create_column_mapper({});
    ASSERT_TRUE(mapper->create_mapping(projected, {}, schema).ok());
    ASSERT_EQ(mapper->mappings().size(), 2);
    EXPECT_EQ(mapper->mappings()[0].file_local_id, 2);
    EXPECT_EQ(mapper->mappings()[1].file_local_id, 0);
    EXPECT_TRUE(mapper->mappings()[1].file_type->equals(*int_type));
    EXPECT_TRUE(mapper->mappings()[1].table_type->equals(*bigint_type));

    auto request = std::make_shared<FileScanRequest>();
    ASSERT_TRUE(mapper->create_scan_request({}, projected, request.get()).ok());
    const auto other_position = request->local_positions.at(LocalColumnId(2));
    const auto base_position = request->local_positions.at(LocalColumnId(0));
    ASSERT_TRUE(reader.open(std::move(request)).ok());
    Block output({{int_type->create_column(), int_type, "first"},
                  {int_type->create_column(), int_type, "second"}});
    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader.get_block(&output, &rows, &eof).ok());
    EXPECT_EQ(rows, 1);
    EXPECT_EQ((*output.get_by_position(other_position.value()).column)[0].get<TYPE_INT>(), 30);
    EXPECT_EQ((*output.get_by_position(base_position.value()).column)[0].get<TYPE_INT>(), 20);
    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(
            std::filesystem::path(wal_path).parent_path().parent_path().parent_path());
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
