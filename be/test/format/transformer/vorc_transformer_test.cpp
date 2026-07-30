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

#include "format/transformer/vorc_transformer.h"

#include <gtest/gtest.h>

#include "core/block/block.h"
#include "core/column/column_array.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_struct.h"
#include "core/column/column_varbinary.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type/data_type_varbinary.h"
#include "format/orc/vorc_reader.h"
#include "format/table/iceberg/schema_parser.h"
#include "io/fs/local_file_system.h"
#include "runtime/runtime_state.h"
#include "testutil/mock/mock_slot_ref.h"
#include "util/uid_util.h"

namespace doris {

class VOrcTransformerTest : public testing::Test {
protected:
    void SetUp() override {
        _file_path = "./vorc_transformer_" + UniqueId::gen_uid().to_string() + ".orc";
        _fs = io::global_local_filesystem();
    }

    void TearDown() override { static_cast<void>(_fs->delete_file(_file_path)); }

    std::string _file_path;
    std::shared_ptr<io::FileSystem> _fs;
};

TEST_F(VOrcTransformerTest, CollectsBoundsForTopLevelFieldAfterStruct) {
    auto int_type = std::make_shared<DataTypeInt32>();
    auto struct_type = std::make_shared<DataTypeStruct>(DataTypes {int_type}, Strings {"a"});
    auto string_type = std::make_shared<DataTypeString>();
    VExprContextSPtrs output_exprs =
            MockSlotRef::create_mock_contexts(DataTypes {struct_type, string_type});

    const std::string schema_json = R"({
        "type": "struct",
        "fields": [
            {
                "id": 1,
                "name": "s",
                "required": true,
                "type": {
                    "type": "struct",
                    "fields": [
                        {"id": 2, "name": "a", "required": true, "type": "int"}
                    ]
                }
            },
            {"id": 3, "name": "b", "required": true, "type": "string"}
        ]
    })";
    std::unique_ptr<iceberg::Schema> schema = iceberg::SchemaParser::from_json(schema_json);

    io::FileWriterPtr file_writer;
    ASSERT_TRUE(_fs->create_file(_file_path, &file_writer).ok());
    RuntimeState state;
    VOrcTransformer transformer(&state, file_writer.get(), output_exprs, "", {"s", "b"}, false,
                                TFileCompressType::PLAIN, schema.get(), _fs);
    ASSERT_TRUE(transformer.open().ok());

    auto nested_column = ColumnInt32::create();
    nested_column->insert_value(-1);
    Columns struct_columns;
    struct_columns.emplace_back(std::move(nested_column));
    auto struct_column = ColumnStruct::create(std::move(struct_columns));
    auto string_column = ColumnString::create();
    string_column->insert_data("hello", 5);

    Block block;
    block.insert(ColumnWithTypeAndName(std::move(struct_column), struct_type, "s"));
    block.insert(ColumnWithTypeAndName(std::move(string_column), string_type, "b"));
    ASSERT_TRUE(transformer.write(block).ok());
    ASSERT_TRUE(transformer.close().ok());

    TIcebergColumnStats stats;
    ASSERT_TRUE(transformer.collect_file_statistics_after_close(&stats).ok());
    ASSERT_TRUE(stats.__isset.lower_bounds);
    ASSERT_TRUE(stats.__isset.upper_bounds);
    ASSERT_EQ(1, stats.lower_bounds.count(3));
    ASSERT_EQ(1, stats.upper_bounds.count(3));
    EXPECT_EQ("hello", stats.lower_bounds.at(3));
    EXPECT_EQ("hello", stats.upper_bounds.at(3));
}

TEST_F(VOrcTransformerTest, IcebergBinaryTypesOverrideLegacyStringCarrier) {
    const std::string schema_json = R"({
        "type": "struct",
        "fields": [
            {"id": 1, "name": "uuid_col", "required": false, "type": "uuid"},
            {"id": 2, "name": "fixed_col", "required": false, "type": "fixed[4]"},
            {"id": 3, "name": "binary_col", "required": false, "type": "binary"}
        ]
    })";
    std::unique_ptr<iceberg::Schema> schema = iceberg::SchemaParser::from_json(schema_json);
    const auto& fields = schema->root_struct().fields();

    RuntimeState state;
    VExprContextSPtrs output_exprs;
    VOrcTransformer transformer(&state, nullptr, output_exprs, "", {}, false,
                                TFileCompressType::PLAIN, schema.get(), _fs);
    auto string_type = std::make_shared<DataTypeString>();

    auto uuid_type = transformer._build_orc_type(string_type, fields.data());
    EXPECT_EQ(orc::BINARY, uuid_type->getKind());
    EXPECT_EQ("UUID", uuid_type->getAttributeValue("iceberg.binary-type"));

    auto fixed_type = transformer._build_orc_type(string_type, fields.data() + 1);
    EXPECT_EQ(orc::BINARY, fixed_type->getKind());
    EXPECT_EQ("FIXED", fixed_type->getAttributeValue("iceberg.binary-type"));
    EXPECT_EQ("4", fixed_type->getAttributeValue("iceberg.length"));

    auto binary_type = transformer._build_orc_type(string_type, fields.data() + 2);
    EXPECT_EQ(orc::BINARY, binary_type->getKind());
    EXPECT_EQ("BINARY", binary_type->getAttributeValue("iceberg.binary-type"));
}

TEST_F(VOrcTransformerTest, ConvertsNestedLegacyUuidAndValidatesFixedBeforeOrcWrite) {
    const std::string schema_json = R"({
        "type": "struct",
        "fields": [
            {
                "id": 1,
                "name": "payload",
                "required": true,
                "type": {
                    "type": "struct",
                    "fields": [
                        {"id": 2, "name": "uuid_col", "required": true, "type": "uuid"},
                        {"id": 3, "name": "fixed_col", "required": true, "type": "fixed[4]"}
                    ]
                }
            }
        ]
    })";
    std::unique_ptr<iceberg::Schema> schema = iceberg::SchemaParser::from_json(schema_json);
    auto string_type = std::make_shared<DataTypeString>();
    auto struct_type = std::make_shared<DataTypeStruct>(DataTypes {string_type, string_type},
                                                        Strings {"uuid_col", "fixed_col"});
    VExprContextSPtrs output_exprs = MockSlotRef::create_mock_contexts(DataTypes {struct_type});

    io::FileWriterPtr file_writer;
    ASSERT_TRUE(_fs->create_file(_file_path, &file_writer).ok());
    RuntimeState state;
    state.set_timezone("UTC");
    VOrcTransformer transformer(&state, file_writer.get(), output_exprs, "", {"payload"}, false,
                                TFileCompressType::PLAIN, schema.get(), _fs);
    ASSERT_TRUE(transformer.open().ok());

    auto uuid_column = ColumnString::create();
    uuid_column->insert_data("00112233-4455-6677-8899-aabbccddeeff", 36);
    auto fixed_column = ColumnString::create();
    fixed_column->insert_data("ABCD", 4);
    Columns children;
    children.emplace_back(std::move(uuid_column));
    children.emplace_back(std::move(fixed_column));
    Block block;
    block.insert({ColumnStruct::create(std::move(children)), struct_type, "payload"});

    ASSERT_TRUE(transformer.write(block).ok());
    ASSERT_TRUE(transformer.close().ok());

    io::FileReaderSPtr file_reader;
    ASSERT_TRUE(_fs->open_file(_file_path, &file_reader).ok());
    auto input_stream = std::make_unique<ORCFileInputStream>(
            _file_path, file_reader, nullptr, nullptr, 8L * 1024L * 1024L, 1L * 1024L * 1024L);
    auto reader = orc::createReader(std::move(input_stream), orc::ReaderOptions());
    auto row_reader = reader->createRowReader();
    auto row_batch = row_reader->createRowBatch(1);
    ASSERT_TRUE(row_reader->next(*row_batch));
    const auto& root = assert_cast<const orc::StructVectorBatch&>(*row_batch);
    const auto& payload = assert_cast<const orc::StructVectorBatch&>(*root.fields[0]);
    const auto& uuid_batch = assert_cast<const orc::StringVectorBatch&>(*payload.fields[0]);
    const auto& fixed_batch = assert_cast<const orc::StringVectorBatch&>(*payload.fields[1]);
    const std::array<uint8_t, 16> expected_uuid = {0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77,
                                                   0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff};
    EXPECT_EQ(uuid_batch.length[0], expected_uuid.size());
    EXPECT_EQ(0, std::memcmp(uuid_batch.data[0], expected_uuid.data(), expected_uuid.size()));
    EXPECT_EQ(fixed_batch.length[0], 4);
    EXPECT_EQ(std::string_view(fixed_batch.data[0], fixed_batch.length[0]), "ABCD");
}

TEST_F(VOrcTransformerTest, PreservesVarbinaryUuidCarrierBeforeOrcWrite) {
    const std::string schema_json = R"({
        "type": "struct",
        "fields": [
            {"id": 1, "name": "uuid_col", "required": true, "type": "uuid"}
        ]
    })";
    std::unique_ptr<iceberg::Schema> schema = iceberg::SchemaParser::from_json(schema_json);
    auto varbinary_type = std::make_shared<DataTypeVarbinary>();
    VExprContextSPtrs output_exprs = MockSlotRef::create_mock_contexts(DataTypes {varbinary_type});

    io::FileWriterPtr file_writer;
    ASSERT_TRUE(_fs->create_file(_file_path, &file_writer).ok());
    RuntimeState state;
    state.set_timezone("UTC");
    VOrcTransformer transformer(&state, file_writer.get(), output_exprs, "", {"uuid_col"}, false,
                                TFileCompressType::PLAIN, schema.get(), _fs);
    ASSERT_TRUE(transformer.open().ok());

    const std::array<uint8_t, 16> expected_uuid = {0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77,
                                                   0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff};
    auto uuid_column = ColumnVarbinary::create();
    uuid_column->insert_data(reinterpret_cast<const char*>(expected_uuid.data()),
                             expected_uuid.size());
    Block block;
    block.insert({std::move(uuid_column), varbinary_type, "uuid_col"});
    ASSERT_TRUE(transformer.write(block).ok());
    ASSERT_TRUE(transformer.close().ok());

    io::FileReaderSPtr file_reader;
    ASSERT_TRUE(_fs->open_file(_file_path, &file_reader).ok());
    auto input_stream = std::make_unique<ORCFileInputStream>(
            _file_path, file_reader, nullptr, nullptr, 8L * 1024L * 1024L, 1L * 1024L * 1024L);
    auto reader = orc::createReader(std::move(input_stream), orc::ReaderOptions());
    auto row_reader = reader->createRowReader();
    auto row_batch = row_reader->createRowBatch(1);
    ASSERT_TRUE(row_reader->next(*row_batch));
    const auto& root = assert_cast<const orc::StructVectorBatch&>(*row_batch);
    const auto& uuid_batch = assert_cast<const orc::StringVectorBatch&>(*root.fields[0]);
    EXPECT_EQ(uuid_batch.length[0], expected_uuid.size());
    EXPECT_EQ(0, std::memcmp(uuid_batch.data[0], expected_uuid.data(), expected_uuid.size()));
}

TEST_F(VOrcTransformerTest, RejectsInvalidLegacyUuidAndFixedValues) {
    const std::string schema_json = R"({
        "type": "struct",
        "fields": [
            {"id": 1, "name": "uuid_col", "required": true, "type": "uuid"},
            {"id": 2, "name": "fixed_col", "required": true, "type": "fixed[4]"}
        ]
    })";
    std::unique_ptr<iceberg::Schema> schema = iceberg::SchemaParser::from_json(schema_json);
    auto string_type = std::make_shared<DataTypeString>();
    RuntimeState state;
    VExprContextSPtrs output_exprs =
            MockSlotRef::create_mock_contexts(DataTypes {string_type, string_type});

    io::FileWriterPtr file_writer;
    ASSERT_TRUE(_fs->create_file(_file_path, &file_writer).ok());
    VOrcTransformer transformer(&state, file_writer.get(), output_exprs, "",
                                {"uuid_col", "fixed_col"}, false, TFileCompressType::PLAIN,
                                schema.get(), _fs);
    ASSERT_TRUE(transformer.open().ok());
    auto uuid_column = ColumnString::create();
    uuid_column->insert_data("not-a-uuid", 10);
    auto fixed_column = ColumnString::create();
    fixed_column->insert_data("ABC", 3);
    Block block;
    block.insert({std::move(uuid_column), string_type, "uuid_col"});
    block.insert({std::move(fixed_column), string_type, "fixed_col"});

    const auto invalid_uuid = transformer.write(block);
    ASSERT_FALSE(invalid_uuid.ok());
    EXPECT_NE(invalid_uuid.to_string().find("Invalid UUID string length"), std::string::npos);

    auto valid_uuid_column = ColumnString::create();
    valid_uuid_column->insert_data("00112233-4455-6677-8899-aabbccddeeff", 36);
    block.replace_by_position(0, std::move(valid_uuid_column));
    const auto invalid_fixed = transformer.write(block);
    ASSERT_FALSE(invalid_fixed.ok());
    EXPECT_NE(invalid_fixed.to_string().find("FIXED[4]"), std::string::npos);
    ASSERT_TRUE(transformer.close().ok());
}

TEST_F(VOrcTransformerTest, SkipsInvalidBinaryChildrenHiddenByNullableCollections) {
    const std::string schema_json = R"({
        "type": "struct",
        "fields": [
            {
                "id": 1,
                "name": "uuid_array",
                "required": false,
                "type": {
                    "type": "list",
                    "element-id": 2,
                    "element-required": true,
                    "element": "uuid"
                }
            },
            {
                "id": 3,
                "name": "binary_map",
                "required": false,
                "type": {
                    "type": "map",
                    "key-id": 4,
                    "key": "uuid",
                    "value-id": 5,
                    "value-required": true,
                    "value": "fixed[4]"
                }
            }
        ]
    })";
    std::unique_ptr<iceberg::Schema> schema = iceberg::SchemaParser::from_json(schema_json);
    auto string_type = std::make_shared<DataTypeString>();
    auto array_type = std::make_shared<DataTypeArray>(string_type);
    auto map_type = std::make_shared<DataTypeMap>(string_type, string_type);
    auto nullable_array_type = make_nullable(array_type);
    auto nullable_map_type = make_nullable(map_type);
    VExprContextSPtrs output_exprs =
            MockSlotRef::create_mock_contexts(DataTypes {nullable_array_type, nullable_map_type});

    io::FileWriterPtr file_writer;
    ASSERT_TRUE(_fs->create_file(_file_path, &file_writer).ok());
    RuntimeState state;
    state.set_timezone("UTC");
    VOrcTransformer transformer(&state, file_writer.get(), output_exprs, "",
                                {"uuid_array", "binary_map"}, false, TFileCompressType::PLAIN,
                                schema.get(), _fs);
    ASSERT_TRUE(transformer.open().ok());

    auto array_elements = ColumnString::create();
    array_elements->insert_data("invalid-hidden-uuid", 19);
    array_elements->insert_data("00112233-4455-6677-8899-aabbccddeeff", 36);
    auto array_element_nulls = ColumnUInt8::create();
    array_element_nulls->insert_value(0);
    array_element_nulls->insert_value(0);
    auto array_offsets = ColumnArray::ColumnOffsets::create();
    array_offsets->get_data().push_back(1);
    array_offsets->get_data().push_back(2);
    auto array_nulls = ColumnUInt8::create();
    array_nulls->insert_value(1);
    array_nulls->insert_value(0);
    auto nullable_array = ColumnNullable::create(
            ColumnArray::create(ColumnNullable::create(std::move(array_elements),
                                                       std::move(array_element_nulls)),
                                std::move(array_offsets)),
            std::move(array_nulls));

    auto map_keys = ColumnString::create();
    map_keys->insert_data("invalid-hidden-uuid", 19);
    map_keys->insert_data("00112233-4455-6677-8899-aabbccddeeff", 36);
    auto map_values = ColumnString::create();
    map_values->insert_data("BAD", 3);
    map_values->insert_data("ABCD", 4);
    auto map_offsets = ColumnArray::ColumnOffsets::create();
    map_offsets->get_data().push_back(1);
    map_offsets->get_data().push_back(2);
    auto map_nulls = ColumnUInt8::create();
    map_nulls->insert_value(1);
    map_nulls->insert_value(0);
    auto nullable_map = ColumnNullable::create(
            ColumnMap::create(std::move(map_keys), std::move(map_values), std::move(map_offsets)),
            std::move(map_nulls));

    Block block;
    block.insert({std::move(nullable_array), nullable_array_type, "uuid_array"});
    block.insert({std::move(nullable_map), nullable_map_type, "binary_map"});
    ASSERT_TRUE(transformer.write(block).ok());
    ASSERT_TRUE(transformer.close().ok());
}

} // namespace doris
