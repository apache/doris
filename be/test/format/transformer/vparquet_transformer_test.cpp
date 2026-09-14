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
#include <gen_cpp/parquet_types.h>
#include <gtest/gtest.h>
#include <parquet/arrow/reader.h>
#include <parquet/file_reader.h>
#include <parquet/schema.h>

#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
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
#include "format/parquet/schema_desc.h"
#include "format/parquet/vparquet_reader.h"
#include "io/fs/file_meta_cache.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "runtime/runtime_state.h"
#include "testutil/mock/mock_slot_ref.h"
#include "util/defer_op.h"
#include "util/timezone_utils.h"
#include "util/uid_util.h"

namespace doris {
namespace {

Status write_variant_file(const std::string& path, const Block& block,
                          TParquetVariantEncoding::type variant_encoding) {
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
            .variant_encoding = variant_encoding,
    };
    VParquetTransformer transformer(&state, file_writer.get(), output_exprs,
                                    std::vector<std::string> {"v"}, false, options);
    RETURN_IF_ERROR(transformer.open());
    RETURN_IF_ERROR(transformer.write(block));
    return transformer.close();
}

// Ten values covering objects, arrays, every scalar kind, the Variant null, and one row that is
// masked by SQL NULL.
Block sample_block() {
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
    return block;
}

const ColumnVariantV2& variant_values(const Block& block) {
    const auto& nullable = assert_cast<const ColumnNullable&>(*block.get_by_position(0).column);
    return assert_cast<const ColumnVariantV2&>(nullable.get_nested_column());
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

// Reads the file the way broker and stream load do: the legacy ParquetReader fills a block whose
// column type comes from the file schema.
Block read_with_doris_reader(const std::string& path) {
    io::FileReaderSPtr file_reader;
    DORIS_CHECK(io::global_local_filesystem()->open_file(path, &file_reader).ok());
    cctz::time_zone ctz;
    TimezoneUtils::find_cctz_time_zone(TimezoneUtils::default_time_zone, ctz);
    TFileScanRangeParams scan_params;
    TFileRangeDesc scan_range;
    scan_range.start_offset = 0;
    scan_range.size = static_cast<int64_t>(file_reader->size());
    FileMetaCache cache(16);
    ParquetReader reader(nullptr, scan_params, scan_range, 1024, &ctz, nullptr, nullptr, &cache);
    reader.set_file_reader(file_reader);
    std::unordered_map<std::string, uint32_t> col_name_to_block_idx {{"v", 0}};
    ParquetInitContext ctx;
    ctx.column_names = {"v"};
    ctx.col_name_to_block_idx = &col_name_to_block_idx;
    ctx.params = &scan_params;
    ctx.range = &scan_range;
    DORIS_CHECK(reader.init_reader(&ctx).ok());

    std::unordered_map<std::string, DataTypePtr> file_types;
    DORIS_CHECK(reader.get_columns(&file_types).ok());
    Block block;
    const DataTypePtr type = make_nullable(file_types.at("v"));
    block.insert({type->create_column(), type, "v"});
    size_t read_rows = 0;
    bool eof = false;
    DORIS_CHECK(reader.get_next_block(&block, &read_rows, &eof).ok());
    return block;
}

// Compares the first `rows` Parquet Variant storage rows with the source column values.
void expect_storage_rows_equal(const ColumnVariantV2& source, const arrow::StructArray& storage,
                               size_t rows) {
    const auto read_metadata = std::static_pointer_cast<arrow::BinaryArray>(storage.field(0));
    const auto read_values = std::static_pointer_cast<arrow::BinaryArray>(storage.field(1));
    for (size_t row = 0; row < rows; ++row) {
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

tparquet::SchemaElement byte_array_leaf(const std::string& name) {
    tparquet::SchemaElement leaf;
    leaf.__set_name(name);
    leaf.__set_type(tparquet::Type::BYTE_ARRAY);
    leaf.__set_repetition_type(tparquet::FieldRepetitionType::REQUIRED);
    return leaf;
}

std::vector<tparquet::SchemaElement> variant_schema(std::vector<std::string> leaf_names) {
    tparquet::SchemaElement root;
    root.__set_name("schema");
    root.__set_num_children(1);
    tparquet::SchemaElement variant;
    variant.__set_name("v");
    variant.__set_num_children(static_cast<int32_t>(leaf_names.size()));
    variant.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
    variant.__set_logicalType(tparquet::LogicalType());
    variant.logicalType.__set_VARIANT(tparquet::VariantType());
    std::vector<tparquet::SchemaElement> schema {root, variant};
    for (const auto& name : leaf_names) {
        schema.push_back(byte_array_leaf(name));
    }
    return schema;
}

TEST(VParquetTransformerTest, VariantV2WritesJsonTextByDefault) {
    const std::string path =
            "./vparquet_transformer_json_" + UniqueId::gen_uid().to_string() + ".parquet";
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

    ASSERT_TRUE(write_variant_file(path, block, TParquetVariantEncoding::JSON).ok());
    const auto metadata = parquet_metadata(path);
    const auto& root =
            assert_cast<const ::parquet::schema::GroupNode&>(*metadata->schema()->schema_root());
    ASSERT_TRUE(root.field(0)->is_primitive());
    EXPECT_TRUE(root.field(0)->logical_type()->is_string());

    const auto table = read_arrow_table(path);
    ASSERT_EQ(table->num_rows(), 1);
    const auto strings = std::static_pointer_cast<arrow::StringArray>(table->column(0)->chunk(0));
    EXPECT_EQ(strings->GetView(0), JSON);

    // A JSON text column is still read back as a plain string by the load reader.
    const Block read_back = read_with_doris_reader(path);
    EXPECT_EQ(read_back.get_by_position(0).type->get_name(), "Nullable(String)");
    const auto& strings_read_back =
            assert_cast<const ColumnNullable&>(*read_back.get_by_position(0).column);
    EXPECT_EQ(strings_read_back.get_nested_column().get_data_at(0),
              StringRef(JSON.data(), JSON.size()));
}

TEST(VParquetTransformerTest, VariantV2WritesParquetVariantLogicalType) {
    const std::string path =
            "./vparquet_transformer_variant_" + UniqueId::gen_uid().to_string() + ".parquet";
    const auto fs = io::global_local_filesystem();
    DEFER(static_cast<void>(fs->delete_file(path)));

    const Block block = sample_block();
    ASSERT_TRUE(write_variant_file(path, block, TParquetVariantEncoding::VARIANT).ok());

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
    expect_storage_rows_equal(variant_values(block), *storage, 9);
}

TEST(VParquetTransformerTest, DorisLoadReaderReadsParquetVariantBack) {
    const std::string path =
            "./vparquet_transformer_reload_" + UniqueId::gen_uid().to_string() + ".parquet";
    const auto fs = io::global_local_filesystem();
    DEFER(static_cast<void>(fs->delete_file(path)));

    const Block block = sample_block();
    ASSERT_TRUE(write_variant_file(path, block, TParquetVariantEncoding::VARIANT).ok());

    const Block read_back = read_with_doris_reader(path);
    const auto& column = read_back.get_by_position(0);
    EXPECT_EQ(column.type->get_name(), "Nullable(Variant)");
    ASSERT_EQ(column.column->size(), 10);
    const auto& nullable = assert_cast<const ColumnNullable&>(*column.column);
    const auto& source = variant_values(block);
    const auto& variants = variant_values(read_back);
    for (size_t row = 0; row + 1 < source.size(); ++row) {
        EXPECT_FALSE(nullable.is_null_at(row)) << "row=" << row;
        EXPECT_TRUE(canonical_equals(source.get_value_ref(row), variants.get_value_ref(row)))
                << "row=" << row;
    }
    EXPECT_TRUE(nullable.is_null_at(9));
}

TEST(VParquetTransformerTest, LoadSchemaExposesVariantGroupAsVariant) {
    FieldDescriptor descriptor;
    ASSERT_TRUE(descriptor.parse_from_thrift(variant_schema({"metadata", "value"})).ok());
    const FieldSchema* field = descriptor.get_column("v");
    ASSERT_NE(field, nullptr);
    EXPECT_EQ(field->data_type->get_name(), "Nullable(Variant)");
    ASSERT_EQ(field->children.size(), 2);
    EXPECT_EQ(field->children[0].data_type->get_name(), "String");
    EXPECT_EQ(field->children[1].data_type->get_name(), "String");

    for (const auto& leaves : {std::vector<std::string> {"metadata"},
                               std::vector<std::string> {"metadata", "payload"}}) {
        FieldDescriptor invalid;
        const Status status = invalid.parse_from_thrift(variant_schema(leaves));
        EXPECT_TRUE(status.is<ErrorCode::INVALID_ARGUMENT>()) << status;
    }
}

} // namespace
} // namespace doris
