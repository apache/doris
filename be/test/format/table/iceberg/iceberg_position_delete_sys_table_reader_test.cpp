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

#include "format/table/iceberg_position_delete_sys_table_reader.h"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <gtest/gtest.h>
#include <parquet/arrow/writer.h>

#include <filesystem>
#include <fstream>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/object_pool.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_struct.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "exprs/vexpr.h"
#include "format/orc/orc_memory_stream_test.h"
#include "format/table/iceberg_scan_semantics.h"
#include "format/table/parquet_utils.h"
#include "format_v2/table/iceberg_position_delete_sys_table_reader.h"
#include "io/io_common.h"
#include "runtime/runtime_profile.h"
#include "runtime/runtime_state.h"

namespace doris {

namespace {

TFileRangeDesc range_with_delete_file(const TIcebergDeleteFileDesc& delete_file);

class ProfileTrackingReader final : public GenericReader {
public:
    int collect_calls = 0;

protected:
    Status _do_get_next_block(Block* /*block*/, size_t* /*read_rows*/, bool* /*eof*/) override {
        return Status::OK();
    }

    void _collect_profile_before_close() override { ++collect_calls; }
};

class RejectAllRowsPredicate final : public VExpr {
public:
    RejectAllRowsPredicate() : VExpr(std::make_shared<DataTypeUInt8>(), false) {}

    Status execute_column_impl(VExprContext*, const Block*, const Selector*, size_t count,
                               ColumnPtr& result_column) const override {
        auto result = ColumnUInt8::create();
        result->get_data().resize_fill(count, 0);
        result_column = std::move(result);
        return Status::OK();
    }

    const std::string& expr_name() const override { return _name; }
    bool is_deterministic() const override { return false; }

    Status clone_node(VExprSPtr* cloned_expr) const override {
        DORIS_CHECK(cloned_expr != nullptr);
        *cloned_expr = std::make_shared<RejectAllRowsPredicate>();
        return Status::OK();
    }

private:
    const std::string _name = "RejectAllRowsPredicate";
};

SlotDescriptor* make_slot(ObjectPool* pool, int id, std::string name, DataTypePtr type) {
    TSlotDescriptor slot_desc;
    slot_desc.__set_id(id);
    slot_desc.__set_parent(0);
    slot_desc.__set_slotType(type->to_thrift());
    slot_desc.__set_columnPos(id);
    slot_desc.__set_byteOffset(0);
    slot_desc.__set_nullIndicatorByte(id / 8);
    slot_desc.__set_nullIndicatorBit(id % 8);
    slot_desc.__set_slotIdx(id);
    slot_desc.__set_isMaterialized(true);
    slot_desc.__set_colName(std::move(name));
    return pool->add(new SlotDescriptor(slot_desc));
}

Block make_output_block(const std::vector<SlotDescriptor*>& slots) {
    Block block;
    for (const auto* slot : slots) {
        auto type = slot->get_data_type_ptr();
        block.insert(ColumnWithTypeAndName(type->create_column(), type, slot->col_name()));
    }
    return block;
}

const IColumn& nested_column(const Block& block, const std::string& name) {
    const auto position = block.get_position_by_name(name);
    DORIS_CHECK(position >= 0);
    const auto& column = *block.get_by_position(position).column;
    if (const auto* nullable = check_and_get_column<ColumnNullable>(&column)) {
        return nullable->get_nested_column();
    }
    return column;
}

bool is_null_at(const Block& block, const std::string& name, size_t row) {
    const auto position = block.get_position_by_name(name);
    DORIS_CHECK(position >= 0);
    const auto* nullable =
            check_and_get_column<ColumnNullable>(block.get_by_position(position).column.get());
    DORIS_CHECK(nullable != nullptr);
    return nullable->is_null_at(row);
}

std::string string_at(const Block& block, const std::string& name, size_t row) {
    const auto* column = check_and_get_column<ColumnString>(&nested_column(block, name));
    DORIS_CHECK(column != nullptr);
    return column->get_data_at(row).to_string();
}

Int64 int_at(const Block& block, const std::string& name, size_t row) {
    return nested_column(block, name).get_int(row);
}

Int64 struct_int_at(const Block& block, const std::string& name, size_t child_index, size_t row) {
    const auto& struct_column = assert_cast<const ColumnStruct&>(nested_column(block, name));
    const auto& child = assert_cast<const ColumnNullable&>(struct_column.get_column(child_index));
    DORIS_CHECK(!child.is_null_at(row));
    return child.get_nested_column().get_int(row);
}

bool struct_child_is_null_at(const Block& block, const std::string& name, size_t child_index,
                             size_t row) {
    const auto& struct_column = assert_cast<const ColumnStruct&>(nested_column(block, name));
    const auto& child = assert_cast<const ColumnNullable&>(struct_column.get_column(child_index));
    return child.is_null_at(row);
}

std::shared_ptr<arrow::Array> int32_array(int32_t value) {
    arrow::Int32Builder builder;
    EXPECT_TRUE(builder.Append(value).ok());
    auto result = builder.Finish();
    EXPECT_TRUE(result.ok()) << result.status();
    return result.ok() ? *result : nullptr;
}

void write_mixed_id_position_delete_parquet(const std::string& path,
                                            std::optional<int32_t> row_id = 2147483544,
                                            bool include_child_id = true) {
    auto id = [](int32_t value) {
        return arrow::key_value_metadata({"PARQUET:field_id"}, {std::to_string(value)});
    };
    auto legacy_a = arrow::field("legacy_a", arrow::int32(), false);
    if (include_child_id) {
        legacy_a = legacy_a->WithMetadata(id(1));
    }
    auto idless_b = arrow::field("b", arrow::int32(), false);
    auto row_array =
            arrow::StructArray::Make({int32_array(42), int32_array(99)}, {legacy_a, idless_b});
    ASSERT_TRUE(row_array.ok()) << row_array.status();

    arrow::StringBuilder path_builder;
    ASSERT_TRUE(path_builder.Append("s3://bucket/data.parquet").ok());
    auto path_array = path_builder.Finish();
    ASSERT_TRUE(path_array.ok()) << path_array.status();
    arrow::Int64Builder pos_builder;
    ASSERT_TRUE(pos_builder.Append(5).ok());
    auto pos_array = pos_builder.Finish();
    ASSERT_TRUE(pos_array.ok()) << pos_array.status();

    auto row_field = arrow::field("row", arrow::struct_({legacy_a, idless_b}), false);
    if (row_id.has_value()) {
        row_field = row_field->WithMetadata(id(*row_id));
    }
    auto schema = arrow::schema({
            arrow::field("file_path", arrow::utf8(), false)->WithMetadata(id(2147483546)),
            arrow::field("pos", arrow::int64(), false)->WithMetadata(id(2147483545)),
            row_field,
    });
    auto table = arrow::Table::Make(schema, {*path_array, *pos_array, *row_array});
    auto output = arrow::io::FileOutputStream::Open(path);
    ASSERT_TRUE(output.ok()) << output.status();
    ::parquet::WriterProperties::Builder properties;
    properties.compression(::parquet::Compression::UNCOMPRESSED);
    PARQUET_THROW_NOT_OK(::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), *output,
                                                      1, properties.build()));
}

void write_nested_wrapper_position_delete_parquet(const std::string& path) {
    auto id = [](int32_t value) {
        return arrow::key_value_metadata({"PARQUET:field_id"}, {std::to_string(value)});
    };
    auto legacy_a = arrow::field("legacy_a", arrow::int32(), false)->WithMetadata(id(1));
    auto s_array = arrow::StructArray::Make({int32_array(42)}, {legacy_a});
    ASSERT_TRUE(s_array.ok()) << s_array.status();
    auto idless_s = arrow::field("s", arrow::struct_({legacy_a}), false);
    auto row_array = arrow::StructArray::Make({*s_array}, {idless_s});
    ASSERT_TRUE(row_array.ok()) << row_array.status();

    arrow::StringBuilder path_builder;
    ASSERT_TRUE(path_builder.Append("s3://bucket/data.parquet").ok());
    auto path_array = path_builder.Finish();
    ASSERT_TRUE(path_array.ok()) << path_array.status();
    arrow::Int64Builder pos_builder;
    ASSERT_TRUE(pos_builder.Append(5).ok());
    auto pos_array = pos_builder.Finish();
    ASSERT_TRUE(pos_array.ok()) << pos_array.status();

    auto schema = arrow::schema({
            arrow::field("file_path", arrow::utf8(), false)->WithMetadata(id(2147483546)),
            arrow::field("pos", arrow::int64(), false)->WithMetadata(id(2147483545)),
            arrow::field("row", arrow::struct_({idless_s}), false)->WithMetadata(id(2147483544)),
    });
    auto table = arrow::Table::Make(schema, {*path_array, *pos_array, *row_array});
    auto output = arrow::io::FileOutputStream::Open(path);
    ASSERT_TRUE(output.ok()) << output.status();
    ::parquet::WriterProperties::Builder properties;
    properties.compression(::parquet::Compression::UNCOMPRESSED);
    PARQUET_THROW_NOT_OK(::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), *output,
                                                      1, properties.build()));
}

void write_mixed_id_position_delete_orc(const std::string& path,
                                        std::optional<int32_t> row_id = 2147483544,
                                        bool include_child_id = true) {
    auto type = std::unique_ptr<::orc::Type>(::orc::Type::buildTypeFromString(
            "struct<file_path:string,pos:bigint,row:struct<legacy_a:int,b:int>>"));
    type->getSubtype(0)->setAttribute("iceberg.id", "2147483546");
    type->getSubtype(1)->setAttribute("iceberg.id", "2147483545");
    if (row_id.has_value()) {
        type->getSubtype(2)->setAttribute("iceberg.id", std::to_string(*row_id));
    }
    if (include_child_id) {
        type->getSubtype(2)->getSubtype(0)->setAttribute("iceberg.id", "1");
    }

    MemoryOutputStream memory_stream(1024 * 1024);
    ::orc::WriterOptions options;
    options.setCompression(::orc::CompressionKind_NONE);
    options.setMemoryPool(::orc::getDefaultPool());
    auto writer = ::orc::createWriter(*type, &memory_stream, options);
    auto batch = writer->createRowBatch(1);
    auto& root = dynamic_cast<::orc::StructVectorBatch&>(*batch);
    auto& file_path = dynamic_cast<::orc::StringVectorBatch&>(*root.fields[0]);
    auto& pos = dynamic_cast<::orc::LongVectorBatch&>(*root.fields[1]);
    auto& row = dynamic_cast<::orc::StructVectorBatch&>(*root.fields[2]);
    auto& legacy_a = dynamic_cast<::orc::LongVectorBatch&>(*row.fields[0]);
    auto& idless_b = dynamic_cast<::orc::LongVectorBatch&>(*row.fields[1]);
    std::string data_path = "s3://bucket/data.orc";
    file_path.data[0] = data_path.data();
    file_path.length[0] = data_path.size();
    pos.data[0] = 5;
    legacy_a.data[0] = 42;
    idless_b.data[0] = 99;
    root.numElements = file_path.numElements = pos.numElements = row.numElements =
            legacy_a.numElements = idless_b.numElements = 1;
    writer->add(*batch);
    writer->close();

    std::ofstream output(path, std::ios::binary);
    output.write(memory_stream.getData(), static_cast<std::streamsize>(memory_stream.getLength()));
}

format::ColumnDefinition table_column(int32_t id, std::string name, DataTypePtr type) {
    format::ColumnDefinition column;
    column.identifier = Field::create_field<TYPE_INT>(id);
    column.name = std::move(name);
    column.type = std::move(type);
    return column;
}

schema::external::TFieldPtr external_scalar_field(const std::string& name, int32_t id,
                                                  TPrimitiveType::type type) {
    auto field = std::make_shared<schema::external::TField>();
    field->__set_name(name);
    field->__set_id(id);
    TColumnType column_type;
    column_type.__set_type(type);
    field->__set_type(column_type);
    schema::external::TFieldPtr ptr;
    ptr.__set_field_ptr(std::move(field));
    return ptr;
}

schema::external::TStructField position_delete_table_schema() {
    schema::external::TStructField row_children;
    row_children.__set_fields({external_scalar_field("a", 1, TPrimitiveType::INT),
                               external_scalar_field("b", 2, TPrimitiveType::INT)});
    auto row = std::make_shared<schema::external::TField>();
    row->__set_name("row");
    row->__set_id(2147483544);
    TColumnType row_type;
    row_type.__set_type(TPrimitiveType::STRUCT);
    row->__set_type(row_type);
    row->nestedField.__set_struct_field(std::move(row_children));
    row->__isset.nestedField = true;
    schema::external::TFieldPtr row_ptr;
    row_ptr.__set_field_ptr(std::move(row));

    schema::external::TStructField root;
    root.__set_fields({external_scalar_field("file_path", 2147483546, TPrimitiveType::STRING),
                       external_scalar_field("pos", 2147483545, TPrimitiveType::BIGINT),
                       std::move(row_ptr)});
    return root;
}

void run_v1_idless_row_position_delete_test(TFileFormatType::type file_format,
                                            const std::string& extension) {
    const auto test_dir = std::filesystem::temp_directory_path() /
                          ("doris_v1_position_delete_idless_row_" + extension);
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);
    const auto path = (test_dir / ("delete." + extension)).string();
    if (file_format == TFileFormatType::FORMAT_PARQUET) {
        write_mixed_id_position_delete_parquet(path, std::nullopt, false);
    } else {
        write_mixed_id_position_delete_orc(path, std::nullopt, false);
    }

    const auto nullable_int32 = make_nullable(std::make_shared<DataTypeInt32>());
    const auto row_type = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {nullable_int32, nullable_int32}, Strings {"a", "b"}));
    ObjectPool pool;
    std::vector<SlotDescriptor*> slots {make_slot(&pool, 0, "row", row_type)};
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    state.set_timezone("UTC");
    RuntimeProfile profile("test_profile");
    TFileScanRangeParams scan_params;
    scan_params.__set_iceberg_scan_semantics_version(ICEBERG_SCAN_SEMANTICS_VERSION_1);
    scan_params.__set_file_type(TFileType::FILE_LOCAL);
    scan_params.__set_format_type(file_format);
    scan_params.__set_current_schema_id(-1);
    schema::external::TSchema current_schema;
    current_schema.__set_schema_id(-1);
    current_schema.__set_root_field(position_delete_table_schema());
    scan_params.__set_history_schema_info({std::move(current_schema)});
    auto io_ctx = std::make_shared<io::IOContext>();
    io::FileReaderStats file_reader_stats;
    io_ctx->file_reader_stats = &file_reader_stats;

    TIcebergDeleteFileDesc delete_file;
    delete_file.__set_content(1);
    delete_file.__set_path(path);
    delete_file.__set_file_format(file_format);
    auto range = range_with_delete_file(delete_file);
    range.__set_path(path);
    range.__set_start_offset(0);
    range.__set_size(static_cast<int64_t>(std::filesystem::file_size(path)));
    range.__set_file_size(static_cast<int64_t>(std::filesystem::file_size(path)));

    IcebergPositionDeleteSysTableReader reader(slots, &state, &profile, range, &scan_params, io_ctx,
                                               nullptr);
    ReaderInitContext context;
    ASSERT_TRUE(reader.init_reader(&context).ok());
    Block block = make_output_block(slots);
    size_t read_rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader.get_next_block(&block, &read_rows, &eof).ok());
    ASSERT_EQ(1, read_rows);
    ASSERT_EQ(1, block.rows());
    // Top-level delete fields carry IDs, so an ID-less physical row must not bind by name.
    EXPECT_TRUE(is_null_at(block, "row", 0));

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

void run_mixed_id_position_delete_test(format::FileFormat file_format,
                                       TFileFormatType::type thrift_format,
                                       const std::string& extension) {
    const auto test_dir = std::filesystem::temp_directory_path() /
                          ("doris_iceberg_position_delete_mixed_id_" + extension);
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);
    const auto path = (test_dir / ("delete." + extension)).string();
    if (file_format == format::FileFormat::PARQUET) {
        write_mixed_id_position_delete_parquet(path);
    } else {
        write_mixed_id_position_delete_orc(path);
    }

    const auto nullable_int32 = make_nullable(std::make_shared<DataTypeInt32>());
    const auto row_type = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {nullable_int32, nullable_int32}, Strings {"a", "b"}));
    auto a = table_column(1, "a", nullable_int32);
    a.name_mapping = {"legacy_a"};
    a.has_name_mapping = true;
    auto b = table_column(2, "b", nullable_int32);
    auto row = table_column(2147483544, "row", row_type);
    row.children = {a, b};
    std::vector<format::ColumnDefinition> projected_columns {row};

    ObjectPool pool;
    std::vector<SlotDescriptor*> slots {make_slot(&pool, 0, "row", row_type)};
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    RuntimeProfile profile("test_profile");
    TFileScanRangeParams scan_params;
    scan_params.__set_iceberg_scan_semantics_version(ICEBERG_SCAN_SEMANTICS_VERSION_1);
    scan_params.__set_file_type(TFileType::FILE_LOCAL);
    scan_params.__set_format_type(thrift_format);
    io::FileReaderStats file_reader_stats;
    auto io_ctx = std::make_shared<io::IOContext>();
    io_ctx->file_reader_stats = &file_reader_stats;

    format::iceberg::IcebergPositionDeleteSysTableV2Reader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .conjuncts = {},
                                    .format = file_format,
                                    .scan_params = &scan_params,
                                    .io_ctx = io_ctx,
                                    .runtime_state = &state,
                                    .scanner_profile = &profile,
                                    .file_slot_descs = &slots,
                            })
                        .ok());
    TIcebergDeleteFileDesc delete_file;
    delete_file.__set_content(1);
    delete_file.__set_path(path);
    delete_file.__set_file_format(thrift_format);
    auto range = range_with_delete_file(delete_file);
    range.__set_path(path);
    range.__set_start_offset(0);
    range.__set_file_size(static_cast<int64_t>(std::filesystem::file_size(path)));
    format::SplitReadOptions split_options;
    split_options.current_range = std::move(range);
    split_options.current_split_format = file_format;
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    Block block = make_output_block(slots);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    ASSERT_FALSE(eos);
    ASSERT_EQ(1, block.rows());
    ASSERT_FALSE(struct_child_is_null_at(block, "row", 0, 0));
    EXPECT_EQ(42, struct_int_at(block, "row", 0, 0));
    // Once any file field has an Iceberg id, the id-less sibling is absent rather than name-bound.
    EXPECT_TRUE(struct_child_is_null_at(block, "row", 1, 0));

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

void run_v2_nested_wrapper_position_delete_test() {
    const auto test_dir = std::filesystem::temp_directory_path() /
                          "doris_v2_position_delete_nested_wrapper_parquet";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);
    const auto path = (test_dir / "delete.parquet").string();
    write_nested_wrapper_position_delete_parquet(path);

    const auto nullable_int32 = make_nullable(std::make_shared<DataTypeInt32>());
    const auto s_type = make_nullable(
            std::make_shared<DataTypeStruct>(DataTypes {nullable_int32}, Strings {"a"}));
    const auto row_type =
            make_nullable(std::make_shared<DataTypeStruct>(DataTypes {s_type}, Strings {"s"}));
    auto a = table_column(1, "a", nullable_int32);
    auto s = table_column(10, "s", s_type);
    s.children = {a};
    auto row = table_column(2147483544, "row", row_type);
    row.children = {s};
    std::vector<format::ColumnDefinition> projected_columns {row};

    ObjectPool pool;
    std::vector<SlotDescriptor*> slots {make_slot(&pool, 0, "row", row_type)};
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    RuntimeProfile profile("test_profile");
    TFileScanRangeParams scan_params;
    scan_params.__set_iceberg_scan_semantics_version(ICEBERG_SCAN_SEMANTICS_VERSION_1);
    scan_params.__set_file_type(TFileType::FILE_LOCAL);
    scan_params.__set_format_type(TFileFormatType::FORMAT_PARQUET);
    io::FileReaderStats file_reader_stats;
    auto io_ctx = std::make_shared<io::IOContext>();
    io_ctx->file_reader_stats = &file_reader_stats;

    format::iceberg::IcebergPositionDeleteSysTableV2Reader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .conjuncts = {},
                                    .format = format::FileFormat::PARQUET,
                                    .scan_params = &scan_params,
                                    .io_ctx = io_ctx,
                                    .runtime_state = &state,
                                    .scanner_profile = &profile,
                                    .file_slot_descs = &slots,
                            })
                        .ok());
    TIcebergDeleteFileDesc delete_file;
    delete_file.__set_content(1);
    delete_file.__set_path(path);
    delete_file.__set_file_format(TFileFormatType::FORMAT_PARQUET);
    auto range = range_with_delete_file(delete_file);
    range.__set_path(path);
    range.__set_start_offset(0);
    range.__set_file_size(static_cast<int64_t>(std::filesystem::file_size(path)));
    format::SplitReadOptions split_options;
    split_options.current_range = std::move(range);
    split_options.current_split_format = format::FileFormat::PARQUET;
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    Block block = make_output_block(slots);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    ASSERT_FALSE(eos);
    ASSERT_EQ(1, block.rows());
    EXPECT_EQ("{\"s\":{\"a\":42}}", row_type->to_string(*block.get_by_position(0).column, 0));

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TFileRangeDesc range_with_delete_file(const TIcebergDeleteFileDesc& delete_file) {
    TIcebergFileDesc iceberg_desc;
    iceberg_desc.__set_delete_files({delete_file});
    TTableFormatFileDesc table_format_desc;
    table_format_desc.__set_iceberg_params(std::move(iceberg_desc));
    TFileRangeDesc range;
    range.__set_table_format_params(std::move(table_format_desc));
    return range;
}

} // namespace

TEST(IcebergPositionDeleteSysTableReaderTest, UsesScannerIOContext) {
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    RuntimeProfile profile("test_profile");
    TFileRangeDesc range;
    TFileScanRangeParams params;
    std::vector<SlotDescriptor*> file_slot_descs;
    auto scanner_io_ctx = std::make_shared<io::IOContext>();
    io::FileReaderStats file_reader_stats;
    scanner_io_ctx->file_reader_stats = &file_reader_stats;

    IcebergPositionDeleteSysTableReader reader(file_slot_descs, &state, &profile, range, &params,
                                               scanner_io_ctx, nullptr);

    EXPECT_EQ(scanner_io_ctx.get(), reader._io_ctx.get());
    EXPECT_EQ(&file_reader_stats, reader._io_ctx->file_reader_stats);
    scanner_io_ctx->should_stop = true;
    EXPECT_TRUE(reader._io_ctx->should_stop);

    EXPECT_TRUE(reader.count_read_rows());
    reader._delete_file_kind = IcebergPositionDeleteSysTableReader::DeleteFileKind::DELETION_VECTOR;
    EXPECT_FALSE(reader.count_read_rows());
}

TEST(IcebergPositionDeleteSysTableReaderTest, ForwardsProfileCollectionToNestedReader) {
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    RuntimeProfile profile("test_profile");
    TFileRangeDesc range;
    TFileScanRangeParams params;
    std::vector<SlotDescriptor*> file_slot_descs;
    auto scanner_io_ctx = std::make_shared<io::IOContext>();

    IcebergPositionDeleteSysTableReader reader(file_slot_descs, &state, &profile, range, &params,
                                               scanner_io_ctx, nullptr);
    auto nested_reader = std::make_unique<ProfileTrackingReader>();
    auto* nested_reader_ptr = nested_reader.get();
    reader._position_reader = std::move(nested_reader);

    reader.collect_profile_before_close();
    reader.collect_profile_before_close();

    EXPECT_EQ(1, nested_reader_ptr->collect_calls);
    reader._dv_positions.add(uint64_t {1});
    reader._next_dv_position.emplace(reader._dv_positions.begin());
    reader._partition_value = std::make_shared<DataTypeInt32>()->create_column();
    ASSERT_TRUE(reader.close().ok());
    EXPECT_TRUE(reader._dv_positions.isEmpty());
    EXPECT_FALSE(reader._next_dv_position.has_value());
    EXPECT_EQ(nullptr, reader._partition_value.get());
}

TEST(IcebergPositionDeleteSysTableReaderTest, StopsBeforeExpandingDeletionVector) {
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    RuntimeProfile profile("test_profile");
    TFileRangeDesc range;
    TFileScanRangeParams params;
    std::vector<SlotDescriptor*> file_slot_descs;
    auto scanner_io_ctx = std::make_shared<io::IOContext>();
    scanner_io_ctx->should_stop = true;

    IcebergPositionDeleteSysTableReader reader(file_slot_descs, &state, &profile, range, &params,
                                               scanner_io_ctx, nullptr);
    Block block;
    size_t read_rows = 1;
    bool eof = false;

    ASSERT_TRUE(reader._do_get_next_block(&block, &read_rows, &eof).ok());
    EXPECT_EQ(0, read_rows);
    EXPECT_TRUE(eof);
}

TEST(IcebergPositionDeleteSysTableReaderTest, ValidatesRangeAndDeleteFileMetadata) {
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    RuntimeProfile profile("test_profile");
    TFileScanRangeParams params;
    std::vector<SlotDescriptor*> file_slot_descs;
    auto scanner_io_ctx = std::make_shared<io::IOContext>();

    TFileRangeDesc empty_range;
    IcebergPositionDeleteSysTableReader invalid_context_reader(
            file_slot_descs, &state, &profile, empty_range, &params, nullptr, nullptr);
    auto status = invalid_context_reader._do_init_reader(nullptr);
    EXPECT_TRUE(status.is<ErrorCode::INVALID_ARGUMENT>()) << status;

    IcebergPositionDeleteSysTableReader missing_params_reader(
            file_slot_descs, &state, &profile, empty_range, &params, scanner_io_ctx, nullptr);
    status = missing_params_reader._do_init_reader(nullptr);
    EXPECT_NE(std::string::npos, status.to_string().find("range misses params"));

    TTableFormatFileDesc table_format_desc;
    table_format_desc.__set_iceberg_params(TIcebergFileDesc());
    TFileRangeDesc no_delete_file_range;
    no_delete_file_range.__set_table_format_params(std::move(table_format_desc));
    IcebergPositionDeleteSysTableReader no_delete_file_reader(file_slot_descs, &state, &profile,
                                                              no_delete_file_range, &params,
                                                              scanner_io_ctx, nullptr);
    status = no_delete_file_reader._do_init_reader(nullptr);
    EXPECT_NE(std::string::npos, status.to_string().find("exactly one delete file"));

    TIcebergDeleteFileDesc missing_content;
    auto missing_content_range = range_with_delete_file(missing_content);
    IcebergPositionDeleteSysTableReader missing_content_reader(file_slot_descs, &state, &profile,
                                                               missing_content_range, &params,
                                                               scanner_io_ctx, nullptr);
    status = missing_content_reader._do_init_reader(nullptr);
    EXPECT_NE(std::string::npos, status.to_string().find("misses content"));

    TIcebergDeleteFileDesc equality_delete;
    equality_delete.__set_content(2);
    auto equality_delete_range = range_with_delete_file(equality_delete);
    IcebergPositionDeleteSysTableReader equality_delete_reader(file_slot_descs, &state, &profile,
                                                               equality_delete_range, &params,
                                                               scanner_io_ctx, nullptr);
    status = equality_delete_reader._do_init_reader(nullptr);
    EXPECT_NE(std::string::npos, status.to_string().find("does not support delete file content 2"));

    TIcebergDeleteFileDesc missing_format;
    missing_format.__set_content(1);
    auto missing_format_range = range_with_delete_file(missing_format);
    IcebergPositionDeleteSysTableReader missing_format_reader(file_slot_descs, &state, &profile,
                                                              missing_format_range, &params,
                                                              scanner_io_ctx, nullptr);
    status = missing_format_reader._do_init_reader(nullptr);
    EXPECT_NE(std::string::npos, status.to_string().find("misses file format"));

    TIcebergDeleteFileDesc unsupported_format;
    unsupported_format.__set_content(1);
    unsupported_format.__set_file_format(TFileFormatType::FORMAT_CSV_PLAIN);
    auto unsupported_format_range = range_with_delete_file(unsupported_format);
    IcebergPositionDeleteSysTableReader unsupported_format_reader(file_slot_descs, &state, &profile,
                                                                  unsupported_format_range, &params,
                                                                  scanner_io_ctx, nullptr);
    status = unsupported_format_reader._do_init_reader(nullptr);
    EXPECT_TRUE(status.is<ErrorCode::NOT_IMPLEMENTED_ERROR>()) << status;

    TIcebergDeleteFileDesc invalid_dv;
    invalid_dv.__set_content(3);
    auto invalid_dv_range = range_with_delete_file(invalid_dv);
    IcebergPositionDeleteSysTableReader invalid_dv_reader(
            file_slot_descs, &state, &profile, invalid_dv_range, &params, scanner_io_ctx, nullptr);
    status = invalid_dv_reader._do_init_reader(nullptr);
    EXPECT_NE(std::string::npos, status.to_string().find("misses referenced data file path"));
}

TEST(IcebergPositionDeleteSysTableReaderTest, AppendsDeletionVectorMetadataAndCachesPartition) {
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    RuntimeProfile profile("test_profile");
    TFileRangeDesc range;
    TFileScanRangeParams params;
    ObjectPool pool;
    const auto nullable_string = make_nullable(std::make_shared<DataTypeString>());
    const auto nullable_int32 = make_nullable(std::make_shared<DataTypeInt32>());
    const auto nullable_int64 = make_nullable(std::make_shared<DataTypeInt64>());
    const auto partition_type = make_nullable(
            std::make_shared<DataTypeStruct>(DataTypes {nullable_int32}, Strings {"p"}));
    std::vector<SlotDescriptor*> slots {
            make_slot(&pool, 0, "file_path", nullable_string),
            make_slot(&pool, 1, "pos", nullable_int64),
            make_slot(&pool, 2, "row", nullable_int32),
            make_slot(&pool, 3, "partition", partition_type),
            make_slot(&pool, 4, "spec_id", nullable_int32),
            make_slot(&pool, 5, "delete_file_path", nullable_string),
            make_slot(&pool, 6, "content_offset", nullable_int64),
            make_slot(&pool, 7, "content_size_in_bytes", nullable_int64),
    };
    auto scanner_io_ctx = std::make_shared<io::IOContext>();
    IcebergPositionDeleteSysTableReader reader(slots, &state, &profile, range, &params,
                                               scanner_io_ctx, nullptr);

    TIcebergFileDesc iceberg_desc;
    iceberg_desc.__set_partition_spec_id(7);
    iceberg_desc.__set_partition_data_json(R"({"p":42})");
    TIcebergDeleteFileDesc delete_file;
    delete_file.__set_path("/physical-delete.puffin");
    delete_file.__set_original_path("s3://bucket/delete.puffin");
    delete_file.__set_referenced_data_file_path("s3://bucket/data.parquet");
    delete_file.__set_content_offset(12);
    delete_file.__set_content_size_in_bytes(34);
    reader._iceberg_file_desc = &iceberg_desc;
    reader._delete_file_desc = &delete_file;
    reader._delete_file_kind = IcebergPositionDeleteSysTableReader::DeleteFileKind::DELETION_VECTOR;
    reader._batch_size = 1;
    reader._dv_positions.add(uint64_t {5});
    reader._dv_positions.add(uint64_t {9});
    reader._next_dv_position.emplace(reader._dv_positions.begin());

    Block block = make_output_block(slots);
    size_t read_rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader._append_deletion_vector_block(&block, &read_rows, &eof).ok());
    EXPECT_EQ(1, read_rows);
    EXPECT_FALSE(eof);
    ASSERT_EQ(1, block.rows());
    ColumnPtr cached_partition = reader._partition_value;
    ASSERT_NE(nullptr, cached_partition.get());
    EXPECT_EQ("s3://bucket/data.parquet", string_at(block, "file_path", 0));
    EXPECT_EQ(5, int_at(block, "pos", 0));
    EXPECT_TRUE(is_null_at(block, "row", 0));
    EXPECT_FALSE(is_null_at(block, "partition", 0));
    EXPECT_EQ(42, struct_int_at(block, "partition", 0, 0));
    EXPECT_EQ(7, int_at(block, "spec_id", 0));
    EXPECT_EQ("s3://bucket/delete.puffin", string_at(block, "delete_file_path", 0));
    EXPECT_EQ(12, int_at(block, "content_offset", 0));
    EXPECT_EQ(34, int_at(block, "content_size_in_bytes", 0));

    ASSERT_TRUE(reader._append_deletion_vector_block(&block, &read_rows, &eof).ok());
    EXPECT_EQ(1, read_rows);
    EXPECT_TRUE(eof);
    ASSERT_EQ(2, block.rows());
    EXPECT_EQ(cached_partition.get(), reader._partition_value.get());
    EXPECT_EQ(9, int_at(block, "pos", 1));
    EXPECT_FALSE(is_null_at(block, "partition", 1));
    EXPECT_EQ(42, struct_int_at(block, "partition", 0, 1));

    ASSERT_TRUE(reader._append_deletion_vector_block(&block, &read_rows, &eof).ok());
    EXPECT_EQ(0, read_rows);
    EXPECT_TRUE(eof);

    reader._dv_positions = roaring::Roaring64Map();
    reader._dv_positions.add(uint64_t {13});
    reader._next_dv_position.emplace(reader._dv_positions.begin());
    std::vector<SlotDescriptor*> partial_slots {slots[0], slots[1]};
    Block partial_block = make_output_block(partial_slots);
    ASSERT_TRUE(reader._append_deletion_vector_block(&partial_block, &read_rows, &eof).ok());
    EXPECT_EQ(1, read_rows);
    EXPECT_TRUE(eof);
    EXPECT_EQ(13, int_at(partial_block, "pos", 0));
}

TEST(IcebergPositionDeleteSysTableReaderTest, AppendsPositionDeleteRowsAndValidatesColumns) {
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    RuntimeProfile profile("test_profile");
    TFileRangeDesc range;
    TFileScanRangeParams params;
    ObjectPool pool;
    const auto nullable_string = make_nullable(std::make_shared<DataTypeString>());
    const auto nullable_int32 = make_nullable(std::make_shared<DataTypeInt32>());
    const auto nullable_int64 = make_nullable(std::make_shared<DataTypeInt64>());
    std::vector<SlotDescriptor*> slots {
            make_slot(&pool, 0, "file_path", nullable_string),
            make_slot(&pool, 1, "pos", nullable_int64),
            make_slot(&pool, 2, "row", nullable_int32),
    };
    auto scanner_io_ctx = std::make_shared<io::IOContext>();
    IcebergPositionDeleteSysTableReader reader(slots, &state, &profile, range, &params,
                                               scanner_io_ctx, nullptr);
    TIcebergFileDesc iceberg_desc;
    TIcebergDeleteFileDesc delete_file;
    delete_file.__set_path("/delete.parquet");
    reader._iceberg_file_desc = &iceberg_desc;
    reader._delete_file_desc = &delete_file;
    reader._delete_file_kind = IcebergPositionDeleteSysTableReader::DeleteFileKind::POSITION_DELETE;

    reader.set_batch_size(17);
    EXPECT_EQ(17, reader.get_batch_size());
    EXPECT_TRUE(reader._output_column_requested("row"));
    EXPECT_FALSE(reader._output_column_requested("missing"));
    reader._init_read_columns(true);
    ASSERT_EQ(3, reader._read_columns.size());
    EXPECT_EQ("row", reader._read_columns.back().name);

    std::unordered_map<std::string, DataTypePtr> name_to_type;
    ASSERT_TRUE(reader._get_columns_impl(&name_to_type).ok());
    EXPECT_EQ(3, name_to_type.size());

    Block delete_block = reader._create_delete_block();
    {
        auto columns_guard = delete_block.mutate_columns_scoped();
        auto& columns = columns_guard.mutable_columns();
        parquet_utils::insert_string(columns[0], "s3://bucket/data.parquet");
        parquet_utils::insert_int64(columns[1], 19);
        parquet_utils::insert_int32(columns[2], 23);
    }
    Block output_block = make_output_block(slots);
    size_t appended_rows = 0;
    ASSERT_TRUE(reader._append_position_delete_block(&output_block, delete_block, 1, &appended_rows)
                        .ok());
    EXPECT_EQ(1, appended_rows);
    ASSERT_EQ(1, output_block.rows());
    EXPECT_EQ("s3://bucket/data.parquet", string_at(output_block, "file_path", 0));
    EXPECT_EQ(19, int_at(output_block, "pos", 0));
    EXPECT_EQ(23, int_at(output_block, "row", 0));

    Block empty_delete_block = reader._create_delete_block();
    auto path_column = nullable_string->create_column();
    auto status = reader._append_sys_column(path_column, *slots[0], &empty_delete_block, 0, 0);
    EXPECT_NE(std::string::npos, status.to_string().find("file_path column is missing"));
    auto pos_column = nullable_int64->create_column();
    status = reader._append_sys_column(pos_column, *slots[1], &empty_delete_block, 0, 0);
    EXPECT_NE(std::string::npos, status.to_string().find("pos column is missing"));
    auto row_column = nullable_int32->create_column();
    ASSERT_TRUE(reader._append_sys_column(row_column, *slots[2], &empty_delete_block, 0, 0).ok());
    ASSERT_EQ(1, row_column->size());

    Block missing_columns;
    path_column = nullable_string->create_column();
    status = reader._append_sys_column(path_column, *slots[0], &missing_columns, 0, 0);
    EXPECT_NE(std::string::npos, status.to_string().find("file_path column is missing"));
    pos_column = nullable_int64->create_column();
    status = reader._append_sys_column(pos_column, *slots[1], &missing_columns, 0, 0);
    EXPECT_NE(std::string::npos, status.to_string().find("pos column is missing"));

    std::vector<SlotDescriptor*> partial_slots {slots[0], slots[1]};
    Block partial_output_block = make_output_block(partial_slots);
    appended_rows = 0;
    ASSERT_TRUE(reader._append_position_delete_block(&partial_output_block, delete_block, 1,
                                                     &appended_rows)
                        .ok());
    EXPECT_EQ(1, appended_rows);
    EXPECT_EQ(1, partial_output_block.rows());

    auto* unknown_slot = make_slot(&pool, 3, "unknown", nullable_string);
    auto unknown_column = nullable_string->create_column();
    status = reader._append_sys_column(unknown_column, *unknown_slot, &delete_block, 0, 0);
    EXPECT_NE(std::string::npos, status.to_string().find("Unknown Iceberg"));

    Block unused_block;
    size_t read_rows = 0;
    bool eof = false;
    status = reader._do_get_next_block(&unused_block, &read_rows, &eof);
    EXPECT_NE(std::string::npos, status.to_string().find("reader is not initialized"));
}

TEST(IcebergPositionDeleteSysTableReaderTest, AppendsNullMetadataAndUsesDeletePathFallback) {
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    RuntimeProfile profile("test_profile");
    TFileRangeDesc range;
    TFileScanRangeParams params;
    ObjectPool pool;
    const auto nullable_string = make_nullable(std::make_shared<DataTypeString>());
    const auto nullable_int32 = make_nullable(std::make_shared<DataTypeInt32>());
    const auto nullable_int64 = make_nullable(std::make_shared<DataTypeInt64>());
    const auto partition_type = make_nullable(
            std::make_shared<DataTypeStruct>(DataTypes {nullable_int32}, Strings {"p"}));
    std::vector<SlotDescriptor*> slots {
            make_slot(&pool, 0, "partition", partition_type),
            make_slot(&pool, 1, "spec_id", nullable_int32),
            make_slot(&pool, 2, "delete_file_path", nullable_string),
            make_slot(&pool, 3, "content_offset", nullable_int64),
            make_slot(&pool, 4, "content_size_in_bytes", nullable_int64),
    };
    auto scanner_io_ctx = std::make_shared<io::IOContext>();
    IcebergPositionDeleteSysTableReader reader(slots, &state, &profile, range, &params,
                                               scanner_io_ctx, nullptr);
    TIcebergFileDesc iceberg_desc;
    TIcebergDeleteFileDesc delete_file;
    delete_file.__set_path("/fallback-delete-file");
    reader._iceberg_file_desc = &iceberg_desc;
    reader._delete_file_desc = &delete_file;
    reader._delete_file_kind = IcebergPositionDeleteSysTableReader::DeleteFileKind::DELETION_VECTOR;
    reader._dv_positions.add(uint64_t {1});
    reader._next_dv_position.emplace(reader._dv_positions.begin());

    Block block = make_output_block(slots);
    size_t read_rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader._append_deletion_vector_block(&block, &read_rows, &eof).ok());
    EXPECT_EQ(1, read_rows);
    EXPECT_TRUE(eof);
    EXPECT_TRUE(is_null_at(block, "partition", 0));
    EXPECT_TRUE(is_null_at(block, "spec_id", 0));
    EXPECT_EQ("/fallback-delete-file", string_at(block, "delete_file_path", 0));
    EXPECT_TRUE(is_null_at(block, "content_offset", 0));
    EXPECT_TRUE(is_null_at(block, "content_size_in_bytes", 0));

    std::vector<SlotDescriptor*> empty_slots;
    IcebergPositionDeleteSysTableReader empty_reader(empty_slots, &state, &profile, range, &params,
                                                     scanner_io_ctx, nullptr);
    empty_reader._dv_positions.add(uint64_t {2});
    empty_reader._next_dv_position.emplace(empty_reader._dv_positions.begin());
    Block empty_block;
    ASSERT_TRUE(empty_reader._append_deletion_vector_block(&empty_block, &read_rows, &eof).ok());
    EXPECT_EQ(1, read_rows);
    EXPECT_TRUE(eof);
}

TEST(IcebergPositionDeleteSysTableReaderTest, ParquetUsesFullFileIdModeForIdlessRow) {
    run_v1_idless_row_position_delete_test(TFileFormatType::FORMAT_PARQUET, "parquet");
}

TEST(IcebergPositionDeleteSysTableReaderTest, OrcUsesFullFileIdModeForIdlessRow) {
    run_v1_idless_row_position_delete_test(TFileFormatType::FORMAT_ORC, "orc");
}

TEST(IcebergPositionDeleteSysTableV2ReaderTest, RecordsDeletionVectorRows) {
    io::FileReaderStats file_reader_stats;
    auto scanner_io_ctx = std::make_shared<io::IOContext>();
    scanner_io_ctx->file_reader_stats = &file_reader_stats;
    std::vector<SlotDescriptor*> file_slot_descs;

    format::iceberg::IcebergPositionDeleteSysTableV2Reader reader;
    reader._io_ctx = scanner_io_ctx;
    reader._file_slot_descs = &file_slot_descs;
    reader._batch_size = 2;
    reader._dv_positions.add(uint64_t {7});
    reader._dv_positions.add(uint64_t {9});
    reader._dv_positions.add(uint64_t {11});
    reader._next_dv_position.emplace(reader._dv_positions.begin());

    Block block;
    size_t read_rows = 0;
    bool eof = true;
    ASSERT_TRUE(reader._append_deletion_vector_block(&block, &read_rows, &eof).ok());
    EXPECT_EQ(2, read_rows);
    EXPECT_FALSE(eof);
    EXPECT_EQ(2, file_reader_stats.read_rows);

    ASSERT_TRUE(reader._append_deletion_vector_block(&block, &read_rows, &eof).ok());
    EXPECT_EQ(1, read_rows);
    EXPECT_FALSE(eof);
    EXPECT_EQ(3, file_reader_stats.read_rows);
}

TEST(IcebergPositionDeleteSysTableV2ReaderTest, CachesAndClearsPartitionValue) {
    ObjectPool pool;
    const auto nullable_int32 = make_nullable(std::make_shared<DataTypeInt32>());
    const auto partition_type = make_nullable(
            std::make_shared<DataTypeStruct>(DataTypes {nullable_int32}, Strings {"p"}));
    auto* partition_slot = make_slot(&pool, 0, "partition", partition_type);

    TIcebergFileDesc iceberg_desc;
    iceberg_desc.__set_partition_data_json(R"({"p":42})");

    format::iceberg::IcebergPositionDeleteSysTableV2Reader reader;
    reader._iceberg_file_desc = &iceberg_desc;
    auto partition_column = partition_type->create_column();
    ASSERT_TRUE(reader._append_partition_column(partition_column, *partition_slot).ok());
    ColumnPtr cached_partition = reader._partition_value;
    ASSERT_NE(nullptr, cached_partition.get());

    ASSERT_TRUE(reader._append_partition_column(partition_column, *partition_slot).ok());
    EXPECT_EQ(cached_partition.get(), reader._partition_value.get());

    Block block;
    block.insert(ColumnWithTypeAndName(std::move(partition_column), partition_type, "partition"));
    ASSERT_EQ(2, block.rows());
    EXPECT_FALSE(is_null_at(block, "partition", 0));
    EXPECT_EQ(42, struct_int_at(block, "partition", 0, 0));
    EXPECT_FALSE(is_null_at(block, "partition", 1));
    EXPECT_EQ(42, struct_int_at(block, "partition", 0, 1));

    reader._has_split = true;
    reader._dv_positions.add(uint64_t {1});
    reader._next_dv_position.emplace(reader._dv_positions.begin());
    ASSERT_TRUE(reader.close().ok());
    EXPECT_EQ(nullptr, reader._iceberg_file_desc);
    EXPECT_EQ(nullptr, reader._partition_value.get());
    EXPECT_TRUE(reader._dv_positions.isEmpty());
    EXPECT_FALSE(reader._next_dv_position.has_value());
    EXPECT_FALSE(reader._has_split);
}

TEST(IcebergPositionDeleteSysTableV2ReaderTest, ValidatesDeleteFileContentAfterBindingDescriptor) {
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    RuntimeProfile profile("test_profile");
    TFileScanRangeParams params;
    std::vector<SlotDescriptor*> file_slot_descs;
    TIcebergDeleteFileDesc equality_delete;
    equality_delete.__set_content(2);

    format::iceberg::IcebergPositionDeleteSysTableV2Reader reader;
    reader._runtime_state = &state;
    reader._scanner_profile = &profile;
    reader._scan_params = &params;
    reader._file_slot_descs = &file_slot_descs;
    reader._current_range = range_with_delete_file(equality_delete);

    auto status = reader._init_split();
    EXPECT_NE(std::string::npos, status.to_string().find("does not support delete file content 2"));
    ASSERT_NE(nullptr, reader._iceberg_file_desc);
    ASSERT_NE(nullptr, reader._delete_file_desc);
    EXPECT_EQ(reader._iceberg_file_desc->delete_files.data(), reader._delete_file_desc);
}

TEST(IcebergPositionDeleteSysTableV2ReaderTest, StopsBeforeExpandingDeletionVector) {
    format::iceberg::IcebergPositionDeleteSysTableV2Reader reader;
    reader._io_ctx = std::make_shared<io::IOContext>();
    reader._io_ctx->should_stop = true;

    Block block;
    bool eof = false;
    ASSERT_TRUE(reader.get_block(&block, &eof).ok());
    EXPECT_TRUE(eof);
}

TEST(IcebergPositionDeleteSysTableV2ReaderTest,
     AllFilteredDeletionVectorYieldsBeforeObservingCancellation) {
    ObjectPool pool;
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    RuntimeProfile profile("test_profile");
    const auto nullable_int64 = make_nullable(std::make_shared<DataTypeInt64>());
    std::vector<SlotDescriptor*> file_slot_descs {
            make_slot(&pool, 0, "pos", nullable_int64),
    };

    auto conjunct = VExprContext::create_shared(std::make_shared<RejectAllRowsPredicate>());
    RowDescriptor row_desc;
    ASSERT_TRUE(conjunct->prepare(&state, row_desc).ok());
    ASSERT_TRUE(conjunct->open(&state).ok());

    format::iceberg::IcebergPositionDeleteSysTableV2Reader reader;
    reader._runtime_state = &state;
    reader._scanner_profile = &profile;
    reader._io_ctx = std::make_shared<io::IOContext>();
    reader._file_slot_descs = &file_slot_descs;
    reader._projected_columns.resize(file_slot_descs.size());
    reader._remaining_conjuncts = {conjunct};
    reader._has_split = true;
    reader._delete_file_kind =
            format::iceberg::IcebergPositionDeleteSysTableV2Reader::DeleteFileKind::DELETION_VECTOR;
    reader._batch_size = 1;
    reader._dv_positions.add(uint64_t {7});
    reader._dv_positions.add(uint64_t {9});
    reader._dv_positions.add(uint64_t {11});
    reader._next_dv_position.emplace(reader._dv_positions.begin());

    Block block = make_output_block(file_slot_descs);
    bool eof = false;
    ASSERT_TRUE(reader.get_block(&block, &eof).ok());
    EXPECT_FALSE(eof);
    EXPECT_EQ(block.rows(), 0);
    ASSERT_TRUE(reader._next_dv_position.has_value());
    EXPECT_EQ(**reader._next_dv_position, 9);

    reader._io_ctx->should_stop = true;
    ASSERT_TRUE(reader.get_block(&block, &eof).ok());
    EXPECT_TRUE(eof);
    ASSERT_TRUE(reader._next_dv_position.has_value());
    EXPECT_EQ(**reader._next_dv_position, 9);
}

TEST(IcebergPositionDeleteSysTableV2ReaderTest, ParquetRowUsesAnyFieldIdMapping) {
    run_mixed_id_position_delete_test(format::FileFormat::PARQUET, TFileFormatType::FORMAT_PARQUET,
                                      "parquet");
}

TEST(IcebergPositionDeleteSysTableV2ReaderTest, OrcRowUsesAnyFieldIdMapping) {
    run_mixed_id_position_delete_test(format::FileFormat::ORC, TFileFormatType::FORMAT_ORC, "orc");
}

TEST(IcebergPositionDeleteSysTableV2ReaderTest, ParquetReadsNestedIdlessWrapper) {
    run_v2_nested_wrapper_position_delete_test();
}

} // namespace doris
