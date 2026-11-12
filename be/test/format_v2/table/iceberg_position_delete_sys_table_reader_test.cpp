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

#include "format_v2/table/iceberg_position_delete_sys_table_reader.h"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <gtest/gtest.h>
#include <parquet/arrow/writer.h>

#include <filesystem>
#include <fstream>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "common/object_pool.h"
#include "core/column/column_nullable.h"
#include "core/column/column_struct.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type/data_type_varbinary.h"
#include "format/orc/orc_memory_stream_test.h"
#include "format/table/iceberg_scan_semantics.h"
#include "format_v2/parquet/parquet_profile.h"
#include "format_v2/parquet/parquet_reader.h"
#include "io/io_common.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_profile.h"
#include "runtime/runtime_state.h"
#include "testutil/scoped_temp_dir.h"

namespace doris::format::iceberg {
namespace {

TFileRangeDesc range_with_delete_file(const TIcebergDeleteFileDesc& delete_file) {
    TIcebergFileDesc iceberg_desc;
    iceberg_desc.__set_delete_files({delete_file});
    TTableFormatFileDesc table_format_desc;
    table_format_desc.__set_iceberg_params(std::move(iceberg_desc));
    TFileRangeDesc range;
    range.__set_table_format_params(std::move(table_format_desc));
    return range;
}

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
    slot_desc.__set_colName(name);
    return pool->add(new SlotDescriptor(slot_desc));
}

Block make_output_block(const std::vector<SlotDescriptor*>& slots) {
    Block block;
    for (const auto* slot : slots) {
        auto type = slot->type();
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

void run_mixed_id_position_delete_test(format::FileFormat file_format,
                                       TFileFormatType::type thrift_format,
                                       const std::string& extension) {
    doris::test::ScopedTempDirectory temp("iceberg_position_delete_mixed_id_" + extension);
    const auto& test_dir = temp.path();
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
}

void run_v2_nested_wrapper_position_delete_test() {
    doris::test::ScopedTempDirectory temp("iceberg_position_delete_nested_wrapper");
    const auto& test_dir = temp.path();
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
}

TEST(IcebergPositionDeleteSysTableV2ProfileTest, UsesDistinctProfileForNestedPositionReader) {
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    RuntimeProfile profile("position_delete_system_table_profile");
    TFileScanRangeParams params;
    std::vector<SlotDescriptor*> file_slot_descs;
    TIcebergDeleteFileDesc delete_file;
    delete_file.__set_content(1);
    delete_file.__set_file_format(TFileFormatType::FORMAT_PARQUET);
    delete_file.__set_path("/not-opened-during-prepare.parquet");

    IcebergPositionDeleteSysTableV2Reader reader;
    reader._runtime_state = &state;
    reader._scanner_profile = &profile;
    reader._scan_params = &params;
    reader._file_slot_descs = &file_slot_descs;
    reader._current_range = range_with_delete_file(delete_file);
    ASSERT_TRUE(reader._init_split().ok());

    ASSERT_NE(reader._position_reader_profile, nullptr);
    EXPECT_NE(reader._position_reader_profile, &profile);
    EXPECT_EQ(profile.get_child("IcebergPositionDeleteFileReader"),
              reader._position_reader_profile);
}

TEST(IcebergPositionDeleteSysTableV2ProfileTest, ReusesChildCountersAcrossSplitsAndScanners) {
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    RuntimeProfile profile("position_delete_system_table_profile");
    parquet::ParquetProfileCache outer_cache(&profile);
    TFileScanRangeParams params;
    std::vector<SlotDescriptor*> file_slot_descs;
    TIcebergDeleteFileDesc delete_file;
    delete_file.__set_content(1);
    delete_file.__set_file_format(TFileFormatType::FORMAT_PARQUET);
    delete_file.__set_path("/not-opened-during-prepare.parquet");
    parquet::ParquetProfileCache* shared_child = nullptr;
    for (int scanner = 0; scanner < 2; ++scanner) {
        IcebergPositionDeleteSysTableV2Reader reader;
        reader._runtime_state = &state;
        reader._scanner_profile = &profile;
        reader._parquet_profile_cache = &outer_cache;
        reader._scan_params = &params;
        reader._file_slot_descs = &file_slot_descs;
        for (int split = 0; split < 2; ++split) {
            reader._current_range = range_with_delete_file(delete_file);
            ASSERT_TRUE(reader._init_split().ok());
            auto* cache = reader._position_reader->_parquet_profile_cache;
            ASSERT_NE(cache, nullptr);
            ASSERT_NE(cache, &outer_cache);
            if (shared_child == nullptr) {
                shared_child = cache;
            }
            EXPECT_EQ(cache, shared_child);
            std::unique_ptr<format::FileReader> file;
            ASSERT_TRUE(reader._position_reader->create_file_reader(&file).ok());
            auto* parquet_reader = dynamic_cast<parquet::ParquetReader*>(file.get());
            ASSERT_NE(parquet_reader, nullptr);
            EXPECT_EQ(parquet_reader->_profile_cache, shared_child);
            parquet_reader->_init_profile();
            EXPECT_EQ(parquet_reader->_parquet_profile.total_time, shared_child->get().total_time);
        }
    }
    EXPECT_EQ(profile.get_counter("FileFooterReadCalls"), nullptr);
    EXPECT_NE(profile.get_child("IcebergPositionDeleteFileReader")
                      ->get_counter("FileFooterReadCalls"),
              nullptr);
}

TEST(IcebergPositionDeleteSysTableV2ProfileTest, PreparesNestedRowInitialDefaults) {
    IcebergPositionDeleteSysTableV2Reader reader;
    const auto child_type = make_nullable(std::make_shared<DataTypeInt32>());
    const auto row_type = make_nullable(
            std::make_shared<DataTypeStruct>(DataTypes {child_type}, Strings {"added"}));
    ColumnDefinition row;
    row.name = "row";
    row.type = row_type;
    ColumnDefinition child;
    child.name = "added";
    child.type = child_type;
    child.initial_default_value = "7";
    row.children.push_back(std::move(child));
    reader._projected_columns = {row};
    reader._read_columns = {{"row", row_type}};

    std::vector<ColumnDefinition> columns;
    ASSERT_TRUE(reader._build_delete_file_projected_columns(&columns).ok());
    ASSERT_EQ(columns.size(), 1);
    ASSERT_EQ(columns[0].children.size(), 1);
    EXPECT_NE(columns[0].children[0].default_expr, nullptr);
}

} // namespace
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

TEST(IcebergPositionDeleteSysTableV2ReaderTest, PreservesUuidPartitionBytes) {
    ObjectPool pool;
    const auto partition_type = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {make_nullable(std::make_shared<DataTypeVarbinary>())}, Strings {"p"}));
    auto* slot = make_slot(&pool, 0, "partition", partition_type);
    TIcebergFileDesc descriptor;
    descriptor.__set_partition_data_json(R"({"p":"0x123E4567E89B12D3A456426614174000"})");
    auto verify = [&](auto& reader) {
        reader._iceberg_file_desc = &descriptor;
        auto column = partition_type->create_column();
        ASSERT_TRUE(reader._append_partition_column(column, *slot).ok());
        ASSERT_TRUE(reader._append_partition_column(column, *slot).ok());
        const auto& structure = assert_cast<const ColumnStruct&>(
                assert_cast<const ColumnNullable&>(*column).get_nested_column());
        const auto& field = assert_cast<const ColumnNullable&>(structure.get_column(0));
        for (size_t row = 0; row < 2; ++row) {
            ASSERT_FALSE(field.is_null_at(row));
            EXPECT_EQ(
                    std::string("\x12\x3e\x45\x67\xe8\x9b\x12\xd3\xa4\x56\x42\x66\x14\x17\x40\x00",
                                16),
                    field.get_nested_column().get_data_at(row).to_string());
        }
    };
    format::iceberg::IcebergPositionDeleteSysTableV2Reader reader;
    verify(reader);
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

} // namespace doris::format::iceberg
