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

#include <arrow/array/array_binary.h>
#include <arrow/array/array_nested.h>
#include <arrow/table.h>
#include <gtest/gtest.h>
#include <parquet/arrow/reader.h>
#include <parquet/file_reader.h>
#include <parquet/schema.h>

#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_variant_v2.h"
#include "core/value/variant/variant_canonical.h"
#include "exprs/function/parse/variant_string_parse.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "runtime/runtime_state.h"
#include "testutil/mock/mock_slot_ref.h"
#include "util/defer_op.h"
#include "util/uid_util.h"

namespace doris {
namespace {

Status write_variant_file(const std::string& path, const Block& block, bool output_object_data) {
    const auto fs = io::global_local_filesystem();
    io::FileWriterPtr file_writer;
    RETURN_IF_ERROR(fs->create_file(path, &file_writer));

    const DataTypePtr& type = block.get_by_position(0).type;
    VExprContextSPtrs output_exprs = MockSlotRef::create_mock_contexts(DataTypes {type});
    RuntimeState state;
    state.set_timezone("UTC");
    const ParquetFileOptions options = {
            .compression_type = TParquetCompressionType::UNCOMPRESSED,
            .parquet_version = TParquetVersion::PARQUET_1_0,
            .parquet_disable_dictionary = true,
            .enable_int96_timestamps = false,
    };
    VParquetTransformer transformer(&state, file_writer.get(), output_exprs,
                                    std::vector<std::string> {"v"}, output_object_data, options);
    RETURN_IF_ERROR(transformer.open());
    RETURN_IF_ERROR(transformer.write(block));
    return transformer.close();
}

std::shared_ptr<::parquet::FileMetaData> parquet_metadata(const std::string& path) {
    return ::parquet::ParquetFileReader::OpenFile(path, false)->metadata();
}

std::shared_ptr<arrow::Table> read_arrow_table(const std::string& path) {
    ::parquet::arrow::FileReaderBuilder builder;
    DORIS_CHECK(builder.OpenFile(path).ok());
    std::unique_ptr<::parquet::arrow::FileReader> reader;
    DORIS_CHECK(builder.Build(&reader).ok());
    auto table = reader->ReadTable();
    DORIS_CHECK(table.ok());
    return std::move(table).ValueUnsafe();
}

TEST(VParquetTransformerTest, VariantV2KeepsUtf8DefaultWithoutBinaryOptIn) {
    const std::string path =
            "./vparquet_transformer_legacy_" + UniqueId::gen_uid().to_string() + ".parquet";
    const auto fs = io::global_local_filesystem();
    DEFER(static_cast<void>(fs->delete_file(path)));

    JsonStringToVariantEncoder encoder({.max_json_key_length = 255,
                                        .throw_on_invalid_json = true,
                                        .check_duplicate_json_path = false});
    constexpr std::string_view JSON = R"({"a":1})";
    encoder.add_json({JSON.data(), JSON.size()});
    auto values = ColumnVariantV2::create();
    values->insert_encoded_batch(encoder.finish_batch());
    Block block;
    block.insert({std::move(values), std::make_shared<DataTypeVariantV2>(), "v"});

    ASSERT_TRUE(write_variant_file(path, block, false).ok());
    const auto metadata = parquet_metadata(path);
    const auto& root =
            assert_cast<const ::parquet::schema::GroupNode&>(*metadata->schema()->schema_root());
    ASSERT_TRUE(root.field(0)->is_primitive());
    EXPECT_TRUE(root.field(0)->logical_type()->is_string());

    const auto table = read_arrow_table(path);
    ASSERT_EQ(table->num_rows(), 1);
    const auto strings = std::static_pointer_cast<arrow::StringArray>(table->column(0)->chunk(0));
    EXPECT_EQ(strings->GetView(0), JSON);
}

TEST(VParquetTransformerTest, VariantV2DirectFileRoundTripPreservesEncodedTypesAndNulls) {
    const std::string path =
            "./vparquet_transformer_variant_" + UniqueId::gen_uid().to_string() + ".parquet";
    const auto fs = io::global_local_filesystem();
    DEFER(static_cast<void>(fs->delete_file(path)));

    JsonStringToVariantEncoder encoder({.max_json_key_length = 255,
                                        .throw_on_invalid_json = true,
                                        .check_duplicate_json_path = false});
    for (std::string_view json :
         {R"({"a":1,"nested":[true,null,"x"]})", R"([true,"x",-7,1.25,null])", R"("hello")", "-123",
          "1234567890123", "1.25", "true", "false", "null", R"("masked-by-sql-null")"}) {
        encoder.add_json({json.data(), json.size()});
    }
    auto values = ColumnVariantV2::create();
    values->insert_encoded_batch(encoder.finish_batch());
    auto outer_nulls = ColumnUInt8::create();
    outer_nulls->insert_many_defaults(values->size());
    outer_nulls->get_data().back() = 1;
    auto nullable = ColumnNullable::create(std::move(values), std::move(outer_nulls));

    Block block;
    block.insert({std::move(nullable), make_nullable(std::make_shared<DataTypeVariantV2>()), "v"});
    ASSERT_TRUE(write_variant_file(path, block, true).ok());

    const auto metadata = parquet_metadata(path);
    const auto& root =
            assert_cast<const ::parquet::schema::GroupNode&>(*metadata->schema()->schema_root());
    const auto& variant = assert_cast<const ::parquet::schema::GroupNode&>(*root.field(0));
    ASSERT_TRUE(variant.logical_type()->is_variant());
    ASSERT_EQ(variant.field_count(), 2);
    EXPECT_EQ(variant.field(0)->name(), "metadata");
    EXPECT_TRUE(variant.field(0)->is_required());
    EXPECT_EQ(variant.field(1)->name(), "value");
    EXPECT_TRUE(variant.field(1)->is_required());

    const auto table = read_arrow_table(path);
    ASSERT_EQ(table->num_rows(), 10);
    ASSERT_EQ(table->column(0)->num_chunks(), 1);
    ASSERT_EQ(table->schema()->field(0)->type()->id(), arrow::Type::STRUCT);
    const auto storage = std::static_pointer_cast<arrow::StructArray>(table->column(0)->chunk(0));
    ASSERT_TRUE(storage->IsValid(8));
    ASSERT_TRUE(storage->IsNull(9));
    const auto read_metadata = std::static_pointer_cast<arrow::BinaryArray>(storage->field(0));
    const auto read_values = std::static_pointer_cast<arrow::BinaryArray>(storage->field(1));
    const auto& source_nullable =
            assert_cast<const ColumnNullable&>(*block.get_by_position(0).column);
    const auto& source = assert_cast<const ColumnVariantV2&>(source_nullable.get_nested_column());
    for (size_t row = 0; row + 1 < source.size(); ++row) {
        int32_t metadata_size = 0;
        int32_t value_size = 0;
        const uint8_t* metadata_bytes = read_metadata->GetValue(row, &metadata_size);
        const uint8_t* value_bytes = read_values->GetValue(row, &value_size);
        const VariantRef read_back {
                .metadata = {.data = reinterpret_cast<const char*>(metadata_bytes),
                             .size = static_cast<size_t>(metadata_size)},
                .value = {reinterpret_cast<const char*>(value_bytes),
                          static_cast<size_t>(value_size)},
        };
        EXPECT_TRUE(canonical_equals(source.get_value_ref(row), read_back)) << "row=" << row;
    }
}

} // namespace
} // namespace doris
