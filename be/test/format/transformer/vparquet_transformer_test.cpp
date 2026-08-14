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

#include "format/transformer/vparquet_transformer.h"

#include <gtest/gtest.h>
#include <parquet/api/reader.h>
#include <parquet/schema.h>

#include <string_view>

#include "core/block/block.h"
#include "core/column/column_array.h"
#include "core/column/column_nullable.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_variant_v2.h"
#include "exprs/function/parse/variant_string_parse.h"
#include "format/table/iceberg/schema_parser.h"
#include "io/fs/local_file_system.h"
#include "runtime/runtime_state.h"
#include "testutil/mock/mock_slot_ref.h"
#include "util/uid_util.h"

namespace doris {

class VParquetTransformerTest : public testing::Test {
protected:
    void SetUp() override {
        _file_path = "./vparquet_transformer_" + UniqueId::gen_uid().to_string() + ".parquet";
        _fs = io::global_local_filesystem();
    }

    void TearDown() override { static_cast<void>(_fs->delete_file(_file_path)); }

    std::string _file_path;
    std::shared_ptr<io::FileSystem> _fs;
};

TEST_F(VParquetTransformerTest, WritesIcebergVariantAndCollectsLogicalMetrics) {
    auto variant_type = std::make_shared<DataTypeVariantV2>();
    auto nullable_variant_type = make_nullable(variant_type);
    VExprContextSPtrs output_exprs =
            MockSlotRef::create_mock_contexts(DataTypes {nullable_variant_type});

    const std::string schema_json = R"({
        "type": "struct",
        "fields": [
            {"id": 2, "name": "payload", "required": false, "type": "variant"}
        ]
    })";
    std::unique_ptr<iceberg::Schema> schema = iceberg::SchemaParser::from_json(schema_json);

    io::FileWriterPtr file_writer;
    ASSERT_TRUE(_fs->create_file(_file_path, &file_writer).ok());
    RuntimeState state;
    state.set_timezone("UTC");
    ParquetFileOptions options {.compression_type = TParquetCompressionType::UNCOMPRESSED,
                                .parquet_version = TParquetVersion::PARQUET_1_0,
                                .parquet_disable_dictionary = false,
                                .enable_int96_timestamps = false};
    VParquetTransformer transformer(&state, file_writer.get(), output_exprs, {"payload"}, false,
                                    options, &schema_json, schema.get());
    ASSERT_TRUE(transformer.open().ok());

    JsonStringToVariantEncoder encoder({.max_json_key_length = 1024,
                                        .throw_on_invalid_json = true,
                                        .check_duplicate_json_path = false});
    for (std::string_view json : {std::string_view {R"({"name":"doris","n":1})"},
                                  std::string_view {"null"}, std::string_view {R"([1,2,3])"}}) {
        encoder.add_json({json.data(), json.size()});
    }
    auto variant_column = ColumnVariantV2::create();
    variant_column->insert_encoded_batch(encoder.finish_batch());
    auto null_map = ColumnUInt8::create();
    null_map->get_data().assign({0, 0, 1});
    auto nullable_column = ColumnNullable::create(std::move(variant_column), std::move(null_map));

    Block block;
    block.insert(
            ColumnWithTypeAndName(std::move(nullable_column), nullable_variant_type, "payload"));
    ASSERT_TRUE(transformer.write(block).ok());
    ASSERT_TRUE(transformer.close().ok());

    TIcebergColumnStats stats;
    ASSERT_TRUE(transformer.collect_file_statistics_after_close(&stats).ok());
    EXPECT_EQ(0, stats.column_sizes.count(-1));
    EXPECT_EQ(0, stats.value_counts.count(-1));
    EXPECT_EQ(0, stats.null_value_counts.count(-1));
    EXPECT_EQ(0, stats.column_sizes.count(2));
    ASSERT_EQ(1, stats.value_counts.count(2));
    EXPECT_EQ(3, stats.value_counts.at(2));
    ASSERT_TRUE(stats.__isset.null_value_counts);
    ASSERT_EQ(1, stats.null_value_counts.count(2));
    EXPECT_EQ(1, stats.null_value_counts.at(2));
    if (stats.__isset.lower_bounds) {
        EXPECT_EQ(0, stats.lower_bounds.count(2));
    }
    if (stats.__isset.upper_bounds) {
        EXPECT_EQ(0, stats.upper_bounds.count(2));
    }

    auto reader = ::parquet::ParquetFileReader::OpenFile(_file_path, false);
    const auto* root = reader->metadata()->schema()->group_node();
    ASSERT_EQ(1, root->field_count());
    const auto& payload = root->field(0);
    ASSERT_NE(nullptr, payload->logical_type());
    EXPECT_TRUE(payload->logical_type()->is_variant());
    EXPECT_EQ(2, payload->field_id());
    const auto& payload_group = static_cast<const ::parquet::schema::GroupNode&>(*payload);
    ASSERT_EQ(2, payload_group.field_count());
    EXPECT_EQ(-1, payload_group.field(0)->field_id());
    EXPECT_EQ(-1, payload_group.field(1)->field_id());
}

TEST_F(VParquetTransformerTest, WritesNestedIcebergVariant) {
    auto variant_type = std::make_shared<DataTypeVariantV2>();
    auto array_type = std::make_shared<DataTypeArray>(variant_type);
    auto nullable_array_type = make_nullable(array_type);
    VExprContextSPtrs output_exprs =
            MockSlotRef::create_mock_contexts(DataTypes {nullable_array_type});

    const std::string schema_json = R"({
        "type": "struct",
        "fields": [
            {
                "id": 2,
                "name": "events",
                "required": false,
                "type": {
                    "type": "list",
                    "element-id": 3,
                    "element": "variant",
                    "element-required": false
                }
            }
        ]
    })";
    std::unique_ptr<iceberg::Schema> schema = iceberg::SchemaParser::from_json(schema_json);

    io::FileWriterPtr file_writer;
    ASSERT_TRUE(_fs->create_file(_file_path, &file_writer).ok());
    RuntimeState state;
    state.set_timezone("UTC");
    ParquetFileOptions options {.compression_type = TParquetCompressionType::UNCOMPRESSED,
                                .parquet_version = TParquetVersion::PARQUET_1_0,
                                .parquet_disable_dictionary = false,
                                .enable_int96_timestamps = false};
    VParquetTransformer transformer(&state, file_writer.get(), output_exprs, {"events"}, false,
                                    options, &schema_json, schema.get());
    ASSERT_TRUE(transformer.open().ok());

    JsonStringToVariantEncoder encoder({.max_json_key_length = 1024,
                                        .throw_on_invalid_json = true,
                                        .check_duplicate_json_path = false});
    for (std::string_view json :
         {std::string_view {R"({"kind":"nested"})"}, std::string_view {"null"},
          std::string_view {"{}"}, std::string_view {"7"}}) {
        encoder.add_json({json.data(), json.size()});
    }
    auto variant_values = ColumnVariantV2::create();
    variant_values->insert_encoded_batch(encoder.finish_batch());
    auto element_nulls = ColumnUInt8::create();
    element_nulls->get_data().assign({0, 0, 1, 0});
    auto nullable_values =
            ColumnNullable::create(std::move(variant_values), std::move(element_nulls));
    auto offsets = ColumnArray::ColumnOffsets::create();
    offsets->get_data().assign({3, 4});
    auto events = ColumnArray::create(std::move(nullable_values), std::move(offsets));
    auto outer_nulls = ColumnUInt8::create();
    outer_nulls->get_data().assign({0, 1});
    auto nullable_events = ColumnNullable::create(std::move(events), std::move(outer_nulls));

    Block block;
    block.insert(ColumnWithTypeAndName(std::move(nullable_events), nullable_array_type, "events"));
    ASSERT_TRUE(transformer.write(block).ok());
    ASSERT_TRUE(transformer.close().ok());

    TIcebergColumnStats stats;
    ASSERT_TRUE(transformer.collect_file_statistics_after_close(&stats).ok());
    EXPECT_EQ(0, stats.value_counts.count(-1));
    EXPECT_EQ(0, stats.null_value_counts.count(-1));
    ASSERT_EQ(1, stats.value_counts.count(3));
    EXPECT_EQ(0, stats.lower_bounds.count(3));
    EXPECT_EQ(0, stats.upper_bounds.count(3));

    auto reader = ::parquet::ParquetFileReader::OpenFile(_file_path, false);
    const auto* root = reader->metadata()->schema()->group_node();
    const auto& events_group = static_cast<const ::parquet::schema::GroupNode&>(*root->field(0));
    const auto& list_group =
            static_cast<const ::parquet::schema::GroupNode&>(*events_group.field(0));
    const auto& element = list_group.field(0);
    ASSERT_NE(nullptr, element->logical_type());
    EXPECT_TRUE(element->logical_type()->is_variant());
    EXPECT_EQ(3, element->field_id());
}

} // namespace doris
