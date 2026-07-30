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

#include <orc/OrcFile.hh>

#include "core/block/block.h"
#include "core/column/column_array.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_struct.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type_serde/orc_serde_utils.h"
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

TEST(OrcSerdeUtilsTest, CopiesOnlyBorrowedStringData) {
    Arena arena;
    char* arena_owned = arena.alloc(5);
    std::memcpy(arena_owned, "owned", 5);
    std::string borrowed = "borrowed";

    orc::StringVectorBatch batch(2, *orc::getDefaultPool());
    batch.numElements = 2;
    batch.data[0] = arena_owned;
    batch.length[0] = 5;
    batch.data[1] = borrowed.data();
    batch.length[1] = borrowed.size();
    const size_t used_before_copy = arena.used_size();

    copy_orc_string_data_to_arena(&batch, arena);

    EXPECT_EQ(arena_owned, batch.data[0]);
    EXPECT_NE(borrowed.data(), batch.data[1]);
    EXPECT_EQ("borrowed", std::string(batch.data[1], batch.length[1]));
    EXPECT_EQ(used_before_copy + borrowed.size(), arena.used_size());
}

TEST(OrcSerdeUtilsTest, PreservesEmptyStringAsPresentValue) {
    Arena arena;
    orc::StringVectorBatch batch(1, *orc::getDefaultPool());
    batch.numElements = 1;
    batch.data[0] = const_cast<char*>("");
    batch.length[0] = 0;

    copy_orc_string_data_to_arena(&batch, arena);

    EXPECT_NE(batch.data[0], nullptr);
    EXPECT_EQ(batch.length[0], 0);
}

TEST(OrcSerdeUtilsTest, IgnoresBorrowedPayloadForNullString) {
    Arena arena;
    std::string hidden_payload(1UL << 20, 'x');

    orc::StringVectorBatch batch(2, *orc::getDefaultPool());
    batch.numElements = 2;
    batch.hasNulls = true;
    batch.notNull[0] = 0;
    batch.data[0] = hidden_payload.data();
    batch.length[0] = hidden_payload.size();
    batch.notNull[1] = 1;
    batch.data[1] = const_cast<char*>("");
    batch.length[1] = 0;
    const size_t used_before_copy = arena.used_size();

    copy_orc_string_data_to_arena(&batch, arena);

    EXPECT_EQ(used_before_copy, arena.used_size());
    EXPECT_EQ(nullptr, batch.data[0]);
    EXPECT_EQ(0, batch.length[0]);
    EXPECT_NE(nullptr, batch.data[1]);
}

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

TEST_F(VOrcTransformerTest, PreservesNullableArrayStructChildPositions) {
    const auto int_type = make_nullable(std::make_shared<DataTypeInt32>());
    const auto string_type = make_nullable(std::make_shared<DataTypeString>());
    const auto struct_type = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {int_type, string_type}, Strings {"i_info", "s_info"}));
    const auto array_type = make_nullable(std::make_shared<DataTypeArray>(struct_type));
    VExprContextSPtrs output_exprs = MockSlotRef::create_mock_contexts(DataTypes {array_type});

    io::FileWriterPtr file_writer;
    ASSERT_TRUE(_fs->create_file(_file_path, &file_writer).ok());
    RuntimeState state;
    state.set_timezone("UTC");
    VOrcTransformer transformer(&state, file_writer.get(), output_exprs, "", {"ss_info"}, false,
                                TFileCompressType::PLAIN, nullptr, _fs);
    ASSERT_TRUE(transformer.open().ok());

    auto ints = ColumnInt32::create();
    ints->get_data().assign({1, 2, 3, 4, 0, 0, 5, 6, 0, 0, 0, 0, 0, 8, 9, 10});
    auto strings = ColumnString::create();
    for (const auto& value : std::vector<std::string> {
                 "doris1", "nereids1", "doris-nereids-1", "doris-nereids-4", "", "",
                 "doris-nereids-5", "doris7", "", "", "", "", "", "doris8", "doris9", "doris10"}) {
        strings->insert_data(value.data(), value.size());
    }
    MutableColumns struct_children;
    struct_children.push_back(ColumnNullable::create(std::move(ints), ColumnUInt8::create(16, 0)));
    struct_children.push_back(
            ColumnNullable::create(std::move(strings), ColumnUInt8::create(16, 0)));
    auto element_null_map = ColumnUInt8::create();
    element_null_map->get_data().assign({0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 1, 0, 0, 0});
    auto elements = ColumnNullable::create(ColumnStruct::create(std::move(struct_children)),
                                           std::move(element_null_map));
    auto offsets = ColumnArray::ColumnOffsets::create();
    offsets->get_data().assign({3, 4, 4, 7, 10, 13, 13, 14, 16});
    auto array_null_map = ColumnUInt8::create();
    array_null_map->get_data().assign({0, 0, 0, 0, 0, 0, 1, 0, 0});
    auto array =
            ColumnNullable::create(ColumnArray::create(std::move(elements), std::move(offsets)),
                                   std::move(array_null_map));

    Block block;
    block.insert(ColumnWithTypeAndName(std::move(array), array_type, "ss_info"));
    ASSERT_TRUE(transformer.write(block).ok());
    ASSERT_TRUE(transformer.close().ok());

    ::orc::ReaderOptions reader_options;
    auto reader = ::orc::createReader(
            ::orc::readLocalFile(_file_path, reader_options.getReaderMetrics()), reader_options);
    auto row_reader = reader->createRowReader();
    auto batch = row_reader->createRowBatch(9);
    ASSERT_TRUE(row_reader->next(*batch));
    const auto& root = dynamic_cast<const ::orc::StructVectorBatch&>(*batch);
    const auto& list = dynamic_cast<const ::orc::ListVectorBatch&>(*root.fields[0]);
    const auto& element_struct = dynamic_cast<const ::orc::StructVectorBatch&>(*list.elements);
    const auto& element_ints =
            dynamic_cast<const ::orc::LongVectorBatch&>(*element_struct.fields[0]);
    const auto& element_strings =
            dynamic_cast<const ::orc::StringVectorBatch&>(*element_struct.fields[1]);
    ASSERT_EQ(element_struct.numElements, 16);
    EXPECT_FALSE(element_struct.notNull[4]);
    EXPECT_FALSE(element_struct.notNull[5]);
    EXPECT_TRUE(element_struct.notNull[6]);
    EXPECT_EQ(element_ints.data[6], 5);
    EXPECT_EQ(std::string(element_strings.data[6], element_strings.length[6]), "doris-nereids-5");
    EXPECT_EQ(element_ints.data[7], 6);
    EXPECT_EQ(element_ints.data[13], 8);
    EXPECT_EQ(element_ints.data[14], 9);
    EXPECT_EQ(element_ints.data[15], 10);
}

TEST_F(VOrcTransformerTest, CompactsCollectionsMaskedByNullableStruct) {
    const auto int_type = make_nullable(std::make_shared<DataTypeInt32>());
    const auto string_type = make_nullable(std::make_shared<DataTypeString>());
    const auto array_type = std::make_shared<DataTypeArray>(int_type);
    const auto map_type = std::make_shared<DataTypeMap>(string_type, string_type);
    const auto struct_type = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {array_type, map_type}, Strings {"items", "properties"}));
    VExprContextSPtrs output_exprs = MockSlotRef::create_mock_contexts(DataTypes {struct_type});

    io::FileWriterPtr file_writer;
    ASSERT_TRUE(_fs->create_file(_file_path, &file_writer).ok());
    RuntimeState state;
    state.set_timezone("UTC");
    VOrcTransformer transformer(&state, file_writer.get(), output_exprs, "", {"payload"}, false,
                                TFileCompressType::PLAIN, nullptr, _fs);
    ASSERT_TRUE(transformer.open().ok());

    auto array_values = ColumnInt32::create();
    array_values->get_data().assign({10, 11, 20});
    auto array_elements =
            ColumnNullable::create(std::move(array_values), ColumnUInt8::create(3, 0));
    auto array_offsets = ColumnArray::ColumnOffsets::create();
    array_offsets->get_data().assign({2, 3});
    auto array_column = ColumnArray::create(std::move(array_elements), std::move(array_offsets));

    auto map_keys = ColumnString::create();
    map_keys->insert_data("hidden-key-1", 12);
    map_keys->insert_data("hidden-key-2", 12);
    map_keys->insert_default();
    map_keys->insert_data("z", 1);
    auto map_values = ColumnString::create();
    map_values->insert_data("hidden-value-1", 14);
    map_values->insert_data("hidden-value-2", 14);
    map_values->insert_default();
    map_values->insert_data("z", 1);
    auto map_offsets = ColumnArray::ColumnOffsets::create();
    map_offsets->get_data().assign({2, 4});
    auto map_column = ColumnMap::create(
            ColumnNullable::create(std::move(map_keys), ColumnUInt8::create(4, 0)),
            ColumnNullable::create(std::move(map_values), ColumnUInt8::create(4, 0)),
            std::move(map_offsets));

    MutableColumns children;
    children.push_back(std::move(array_column));
    children.push_back(std::move(map_column));
    auto parent_null_map = ColumnUInt8::create();
    parent_null_map->get_data().assign({1, 0});
    auto payload = ColumnNullable::create(ColumnStruct::create(std::move(children)),
                                          std::move(parent_null_map));
    Block block;
    block.insert(ColumnWithTypeAndName(std::move(payload), struct_type, "payload"));
    ASSERT_TRUE(transformer.write(block).ok());
    ASSERT_TRUE(transformer.close().ok());

    ::orc::ReaderOptions options;
    auto reader = ::orc::createReader(::orc::readLocalFile(_file_path, options.getReaderMetrics()),
                                      options);
    auto row_reader = reader->createRowReader();
    auto batch = row_reader->createRowBatch(2);
    ASSERT_TRUE(row_reader->next(*batch));
    const auto& root = dynamic_cast<const ::orc::StructVectorBatch&>(*batch);
    const auto& payload_batch = dynamic_cast<const ::orc::StructVectorBatch&>(*root.fields[0]);
    const auto& list = dynamic_cast<const ::orc::ListVectorBatch&>(*payload_batch.fields[0]);
    const auto& list_values = dynamic_cast<const ::orc::LongVectorBatch&>(*list.elements);
    const auto& map = dynamic_cast<const ::orc::MapVectorBatch&>(*payload_batch.fields[1]);
    const auto& map_keys_batch = dynamic_cast<const ::orc::StringVectorBatch&>(*map.keys);
    const auto& map_values_batch = dynamic_cast<const ::orc::StringVectorBatch&>(*map.elements);

    EXPECT_EQ(list.offsets[0], 0);
    EXPECT_EQ(list.offsets[1], 0);
    EXPECT_EQ(list.offsets[2], 1);
    EXPECT_EQ(list_values.data[0], 20);
    EXPECT_EQ(map.offsets[0], 0);
    EXPECT_EQ(map.offsets[1], 0);
    EXPECT_EQ(map.offsets[2], 2);
    EXPECT_EQ(std::string(map_keys_batch.data[0], map_keys_batch.length[0]), "");
    EXPECT_EQ(std::string(map_keys_batch.data[1], map_keys_batch.length[1]), "z");
    EXPECT_EQ(std::string(map_values_batch.data[0], map_values_batch.length[0]), "");
    EXPECT_EQ(std::string(map_values_batch.data[1], map_values_batch.length[1]), "z");

    auto statistics = reader->getStatistics();
    const auto* payload_type = reader->getType().getSubtype(0);
    const auto* map_type_node = payload_type->getSubtype(1);
    const auto* key_statistics = dynamic_cast<const ::orc::StringColumnStatistics*>(
            statistics->getColumnStatistics(map_type_node->getSubtype(0)->getColumnId()));
    ASSERT_NE(key_statistics, nullptr);
    ASSERT_TRUE(key_statistics->hasMinimum());
    EXPECT_EQ(key_statistics->getMinimum(), "");
    EXPECT_EQ(key_statistics->getMaximum(), "z");
}

} // namespace doris
