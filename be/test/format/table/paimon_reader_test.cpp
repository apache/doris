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

#include <cctz/time_zone.h>
#include <gen_cpp/ExternalTableSchema_types.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <functional>
#include <memory>
#include <roaring/roaring.hh>
#include <string>
#include <unordered_map>
#include <vector>

#include "core/block/block.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/primitive_type.h"
#include "format/format_common.h"
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wkeyword-macro"
#define private public
#define protected public
#include "format/table/paimon_reader.h"
#undef protected
#undef private
#pragma clang diagnostic pop
#include "format/column_descriptor.h"
#include "io/fs/file_meta_cache.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "io/fs/file_system.h"
#include "io/fs/local_file_system.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "testutil/desc_tbl_builder.h"
#include "util/timezone_utils.h"

namespace doris {

static constexpr uint32_t kPaimonDeletionVectorMagicNumber = 1581511376U;

class PaimonReaderTest : public ::testing::Test {
protected:
    void SetUp() override {
        TimezoneUtils::find_cctz_time_zone(TimezoneUtils::default_time_zone, _timezone);
        _cache = std::make_unique<FileMetaCache>(1024);
    }

    void TearDown() override {
        for (const auto& path : _temp_files) {
            std::error_code ec;
            std::filesystem::remove(path, ec);
        }
    }

    struct DeletionVectorFile {
        std::string path;
        int64_t payload_length = 0;
    };

    static void append_big_endian_u32(std::vector<char>* bytes, uint32_t value) {
        bytes->push_back(static_cast<char>((value >> 24) & 0xFF));
        bytes->push_back(static_cast<char>((value >> 16) & 0xFF));
        bytes->push_back(static_cast<char>((value >> 8) & 0xFF));
        bytes->push_back(static_cast<char>(value & 0xFF));
    }

    DeletionVectorFile write_valid_deletion_vector_file(const std::vector<uint32_t>& deleted_rows) {
        roaring::Roaring bitmap;
        for (uint32_t row : deleted_rows) {
            bitmap.add(row);
        }

        const size_t bitmap_size = bitmap.getSizeInBytes(true);
        std::vector<char> bitmap_bytes(bitmap_size);
        EXPECT_EQ(bitmap.write(bitmap_bytes.data(), true), bitmap_size);

        std::vector<char> file_bytes;
        const uint32_t payload_length = static_cast<uint32_t>(4 + bitmap_size);
        append_big_endian_u32(&file_bytes, payload_length);
        append_big_endian_u32(&file_bytes, kPaimonDeletionVectorMagicNumber);
        file_bytes.insert(file_bytes.end(), bitmap_bytes.begin(), bitmap_bytes.end());

        return write_raw_deletion_vector_file(file_bytes, payload_length);
    }

    DeletionVectorFile write_invalid_magic_deletion_vector_file() {
        roaring::Roaring bitmap;
        bitmap.add(1);

        const size_t bitmap_size = bitmap.getSizeInBytes(true);
        std::vector<char> bitmap_bytes(bitmap_size);
        EXPECT_EQ(bitmap.write(bitmap_bytes.data(), true), bitmap_size);

        std::vector<char> file_bytes;
        const uint32_t payload_length = static_cast<uint32_t>(4 + bitmap_size);
        append_big_endian_u32(&file_bytes, payload_length);
        append_big_endian_u32(&file_bytes, 0x12345678U);
        file_bytes.insert(file_bytes.end(), bitmap_bytes.begin(), bitmap_bytes.end());

        return write_raw_deletion_vector_file(file_bytes, payload_length);
    }

    DeletionVectorFile write_invalid_length_deletion_vector_file() {
        roaring::Roaring bitmap;
        bitmap.add(1);

        const size_t bitmap_size = bitmap.getSizeInBytes(true);
        std::vector<char> bitmap_bytes(bitmap_size);
        EXPECT_EQ(bitmap.write(bitmap_bytes.data(), true), bitmap_size);

        std::vector<char> file_bytes;
        const uint32_t payload_length = static_cast<uint32_t>(4 + bitmap_size);
        append_big_endian_u32(&file_bytes, payload_length + 1);
        append_big_endian_u32(&file_bytes, kPaimonDeletionVectorMagicNumber);
        file_bytes.insert(file_bytes.end(), bitmap_bytes.begin(), bitmap_bytes.end());

        return write_raw_deletion_vector_file(file_bytes, payload_length);
    }

    DeletionVectorFile write_invalid_bitmap_deletion_vector_file() {
        std::vector<char> file_bytes;
        const uint32_t payload_length = 7;
        append_big_endian_u32(&file_bytes, payload_length);
        append_big_endian_u32(&file_bytes, kPaimonDeletionVectorMagicNumber);
        file_bytes.push_back('\x01');
        file_bytes.push_back('\x02');
        file_bytes.push_back('\x03');

        return write_raw_deletion_vector_file(file_bytes, payload_length);
    }

    DeletionVectorFile write_raw_deletion_vector_file(const std::vector<char>& bytes,
                                                      int64_t payload_length) {
        auto temp_dir = std::filesystem::temp_directory_path();
        std::string pattern = (temp_dir / "paimon_reader_test_XXXXXX").string();
        std::vector<char> path_buffer(pattern.begin(), pattern.end());
        path_buffer.push_back('\0');

        int fd = mkstemp(path_buffer.data());
        EXPECT_GE(fd, 0);
        if (fd >= 0) {
            close(fd);
        }

        std::string path(path_buffer.data());
        std::ofstream out(path, std::ios::binary | std::ios::trunc);
        out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
        out.close();
        EXPECT_TRUE(out.good());

        _temp_files.push_back(path);
        return {path, payload_length};
    }

    TFileRangeDesc build_range_with_deletion_file(const DeletionVectorFile& deletion_file) {
        TFileRangeDesc range;
        range.__isset.table_format_params = true;
        range.table_format_params.__isset.paimon_params = true;
        range.table_format_params.paimon_params.__isset.deletion_file = true;
        range.table_format_params.paimon_params.deletion_file.path = deletion_file.path;
        range.table_format_params.paimon_params.deletion_file.offset = 0;
        range.table_format_params.paimon_params.deletion_file.length = deletion_file.payload_length;
        return range;
    }

    TFileScanRangeParams build_local_params() {
        TFileScanRangeParams params;
        params.file_type = TFileType::FILE_LOCAL;
        return params;
    }

    std::string paimon_parquet_test_file() const {
        return "./docker/thirdparties/docker-compose/hive/scripts/paimon1/db1.db/"
               "all_table_with_parquet/bucket-0/"
               "data-3fe3fe10-7d88-4abd-be1f-6dc2bac84d88-0.parquet";
    }

    std::string paimon_orc_test_file() const {
        return "./docker/thirdparties/docker-compose/hive/scripts/paimon1/db1.db/"
               "all_table/bucket-0/data-12a1c2cb-6f0e-4692-996f-2d7a40a8c094-0.orc";
    }

    const TupleDescriptor* build_tuple_descriptor(
            ObjectPool& object_pool,
            const std::vector<std::pair<std::string, DataTypePtr>>& columns) {
        DescriptorTblBuilder builder(&object_pool);
        auto& tuple_builder = builder.declare_tuple();
        for (const auto& [name, type] : columns) {
            tuple_builder << std::make_tuple(type, name);
        }
        return builder.build()->get_tuple_descriptor(0);
    }

    std::unordered_map<std::string, uint32_t> build_col_name_to_block_idx(
            const TupleDescriptor* tuple_descriptor) {
        std::unordered_map<std::string, uint32_t> mapping;
        for (size_t i = 0; i < tuple_descriptor->slots().size(); ++i) {
            mapping.emplace(tuple_descriptor->slots()[i]->col_name(), static_cast<uint32_t>(i));
        }
        return mapping;
    }

    std::vector<ColumnDescriptor> build_column_descs(
            const TupleDescriptor* tuple_descriptor,
            const std::vector<ColumnCategory>& categories) {
        std::vector<ColumnDescriptor> column_descs;
        column_descs.reserve(tuple_descriptor->slots().size());
        for (size_t i = 0; i < tuple_descriptor->slots().size(); ++i) {
            column_descs.push_back({tuple_descriptor->slots()[i]->col_name(),
                                    tuple_descriptor->slots()[i], categories[i], nullptr});
        }
        return column_descs;
    }

    Block make_block(const std::vector<std::pair<std::string, DataTypePtr>>& columns) {
        Block block;
        for (const auto& [name, type] : columns) {
            block.insert(ColumnWithTypeAndName(type->create_column(), type, name));
        }
        return block;
    }

    void verify_nullable_string_column_has_rows(const Block& block, const std::string& column_name,
                                                size_t expected_rows) {
        const auto& column = block.get_by_position(block.get_position_by_name(column_name));
        auto* nullable = check_and_get_column<ColumnNullable>(column.column.get());
        ASSERT_NE(nullable, nullptr);
        const auto& nested = static_cast<const ColumnString&>(nullable->get_nested_column());
        ASSERT_EQ(nullable->size(), expected_rows);
        ASSERT_EQ(nested.size(), expected_rows);
        if (expected_rows > 0) {
            EXPECT_FALSE(nullable->is_null_at(0));
        }
    }

    void verify_nullable_string_column_all_null(const Block& block, const std::string& column_name,
                                                size_t expected_rows) {
        const auto& column = block.get_by_position(block.get_position_by_name(column_name));
        auto* nullable = check_and_get_column<ColumnNullable>(column.column.get());
        ASSERT_NE(nullable, nullptr);
        ASSERT_EQ(nullable->size(), expected_rows);
        for (size_t i = 0; i < expected_rows; ++i) {
            EXPECT_TRUE(nullable->is_null_at(i));
        }
    }

    schema::external::TSchema build_history_schema_with_string_field(int64_t schema_id,
                                                                     std::string field_name,
                                                                     int32_t field_id) {
        schema::external::TSchema schema;
        schema.__set_schema_id(schema_id);

        TColumnType struct_type;
        struct_type.type = TPrimitiveType::STRUCT;
        schema::external::TStructField root_field;

        TColumnType string_type;
        string_type.type = TPrimitiveType::STRING;
        auto string_field = std::make_shared<schema::external::TField>();
        string_field->name = std::move(field_name);
        string_field->id = field_id;
        string_field->type = string_type;

        schema::external::TFieldPtr string_ptr;
        string_ptr.__set_field_ptr(string_field);
        root_field.fields.emplace_back(string_ptr);

        schema.__set_root_field(root_field);
        return schema;
    }

    Status try_read_paimon_parquet_block(
            const std::string& actual_file, TFileRangeDesc scan_range,
            const TupleDescriptor* tuple_descriptor,
            const std::unordered_map<std::string, uint32_t>& col_name_to_block_idx,
            std::vector<ColumnDescriptor> column_descs,
            const std::vector<std::pair<std::string, DataTypePtr>>& block_columns,
            size_t* read_rows, Block* block,
            const std::function<void(TFileScanRangeParams*)>& customize_scan_params = {}) {
        auto local_fs = io::global_local_filesystem();
        int64_t file_size = 0;
        auto st = local_fs->file_size(actual_file, &file_size);
        if (!st.ok()) {
            return st;
        }

        RuntimeState runtime_state((TQueryOptions()), TQueryGlobals());
        auto scan_params = build_local_params();
        scan_params.format_type = TFileFormatType::FORMAT_PARQUET;
        if (customize_scan_params != nullptr) {
            customize_scan_params(&scan_params);
        }

        scan_range.start_offset = 0;
        scan_range.size = file_size;
        scan_range.path = actual_file;
        scan_range.__isset.table_format_params = true;
        scan_range.table_format_params.__isset.paimon_params = true;

        RuntimeProfile profile("paimon_parquet_reader_test");
        auto reader = std::make_unique<PaimonParquetReader>(&profile, scan_params, scan_range, 1024,
                                                            &_timezone, &_kv_cache, nullptr,
                                                            &runtime_state, _cache.get());
        if (reader == nullptr) {
            return Status::InternalError("failed to create paimon parquet reader");
        }

        ParquetInitContext pq_ctx;
        pq_ctx.column_descs = &column_descs;
        pq_ctx.col_name_to_block_idx =
                const_cast<std::unordered_map<std::string, uint32_t>*>(&col_name_to_block_idx);
        pq_ctx.tuple_descriptor = tuple_descriptor;
        pq_ctx.params = &scan_params;
        pq_ctx.range = &scan_range;
        st = reader->init_reader(&pq_ctx);
        if (!st.ok()) {
            return st;
        }

        *block = make_block(block_columns);
        bool eof = false;
        st = reader->get_next_block(block, read_rows, &eof);
        return st;
    }

    void read_paimon_parquet_block(
            const std::string& actual_file, TFileRangeDesc scan_range,
            const TupleDescriptor* tuple_descriptor,
            const std::unordered_map<std::string, uint32_t>& col_name_to_block_idx,
            std::vector<ColumnDescriptor> column_descs,
            const std::vector<std::pair<std::string, DataTypePtr>>& block_columns,
            size_t* read_rows, Block* block,
            const std::function<void(TFileScanRangeParams*)>& customize_scan_params = {}) {
        auto st = try_read_paimon_parquet_block(
                actual_file, std::move(scan_range), tuple_descriptor, col_name_to_block_idx,
                std::move(column_descs), block_columns, read_rows, block, customize_scan_params);
        ASSERT_TRUE(st.ok()) << st;
    }

    Status try_read_paimon_orc_block(
            const std::string& actual_file, TFileRangeDesc scan_range,
            const TupleDescriptor* tuple_descriptor,
            const std::unordered_map<std::string, uint32_t>& col_name_to_block_idx,
            std::vector<ColumnDescriptor> column_descs,
            const std::vector<std::pair<std::string, DataTypePtr>>& block_columns,
            size_t* read_rows, Block* block,
            const std::function<void(TFileScanRangeParams*)>& customize_scan_params = {}) {
        auto local_fs = io::global_local_filesystem();
        int64_t file_size = 0;
        auto st = local_fs->file_size(actual_file, &file_size);
        if (!st.ok()) {
            return st;
        }

        RuntimeState runtime_state((TQueryOptions()), TQueryGlobals());
        auto scan_params = build_local_params();
        scan_params.format_type = TFileFormatType::FORMAT_ORC;
        if (customize_scan_params != nullptr) {
            customize_scan_params(&scan_params);
        }

        scan_range.start_offset = 0;
        scan_range.size = file_size;
        scan_range.path = actual_file;
        scan_range.__isset.table_format_params = true;
        scan_range.table_format_params.__isset.paimon_params = true;

        RuntimeProfile profile("paimon_orc_reader_test");
        auto reader =
                std::make_unique<PaimonOrcReader>(&profile, &runtime_state, scan_params, scan_range,
                                                  1024, "CST", &_kv_cache, nullptr, _cache.get());
        if (reader == nullptr) {
            return Status::InternalError("failed to create paimon orc reader");
        }

        OrcInitContext orc_ctx;
        orc_ctx.column_descs = &column_descs;
        orc_ctx.col_name_to_block_idx =
                const_cast<std::unordered_map<std::string, uint32_t>*>(&col_name_to_block_idx);
        orc_ctx.tuple_descriptor = tuple_descriptor;
        orc_ctx.params = &scan_params;
        orc_ctx.range = &scan_range;
        st = reader->init_reader(&orc_ctx);
        if (!st.ok()) {
            return st;
        }

        *block = make_block(block_columns);
        bool eof = false;
        st = reader->get_next_block(block, read_rows, &eof);
        return st;
    }

    void read_paimon_orc_block(
            const std::string& actual_file, TFileRangeDesc scan_range,
            const TupleDescriptor* tuple_descriptor,
            const std::unordered_map<std::string, uint32_t>& col_name_to_block_idx,
            std::vector<ColumnDescriptor> column_descs,
            const std::vector<std::pair<std::string, DataTypePtr>>& block_columns,
            size_t* read_rows, Block* block,
            const std::function<void(TFileScanRangeParams*)>& customize_scan_params = {}) {
        auto st = try_read_paimon_orc_block(actual_file, std::move(scan_range), tuple_descriptor,
                                            col_name_to_block_idx, std::move(column_descs),
                                            block_columns, read_rows, block, customize_scan_params);
        ASSERT_TRUE(st.ok()) << st;
    }

    Status try_init_paimon_parquet_reader(TFileRangeDesc scan_range,
                                          TPushAggOp::type push_down_agg_type,
                                          std::unique_ptr<PaimonParquetReader>* reader) {
        auto nullable_string = make_nullable(std::make_shared<DataTypeString>());
        ObjectPool object_pool;
        const auto* tuple_descriptor =
                build_tuple_descriptor(object_pool, {{"c13", nullable_string}});
        auto col_name_to_block_idx = build_col_name_to_block_idx(tuple_descriptor);
        auto column_descs = build_column_descs(tuple_descriptor, {ColumnCategory::REGULAR});

        auto actual_file = paimon_parquet_test_file();
        auto local_fs = io::global_local_filesystem();
        int64_t file_size = 0;
        RETURN_IF_ERROR(local_fs->file_size(actual_file, &file_size));

        auto scan_params = build_local_params();
        scan_params.format_type = TFileFormatType::FORMAT_PARQUET;

        scan_range.start_offset = 0;
        scan_range.size = file_size;
        scan_range.path = actual_file;
        scan_range.__isset.table_format_params = true;
        scan_range.table_format_params.__isset.paimon_params = true;

        auto local_reader = std::make_unique<PaimonParquetReader>(
                &_profile, scan_params, scan_range, 1024, &_timezone, &_kv_cache, nullptr,
                &_runtime_state, _cache.get());
        if (local_reader == nullptr) {
            return Status::InternalError("failed to create paimon parquet reader");
        }

        ParquetInitContext pq_ctx;
        pq_ctx.column_descs = &column_descs;
        pq_ctx.col_name_to_block_idx = &col_name_to_block_idx;
        pq_ctx.tuple_descriptor = tuple_descriptor;
        pq_ctx.params = &scan_params;
        pq_ctx.range = &scan_range;
        pq_ctx.push_down_agg_type = push_down_agg_type;
        auto st = local_reader->init_reader(&pq_ctx);
        *reader = std::move(local_reader);
        return st;
    }

    Status try_init_paimon_orc_reader(TFileRangeDesc scan_range,
                                      TPushAggOp::type push_down_agg_type,
                                      std::unique_ptr<PaimonOrcReader>* reader) {
        auto nullable_string = make_nullable(std::make_shared<DataTypeString>());
        ObjectPool object_pool;
        const auto* tuple_descriptor =
                build_tuple_descriptor(object_pool, {{"c13", nullable_string}});
        auto col_name_to_block_idx = build_col_name_to_block_idx(tuple_descriptor);
        auto column_descs = build_column_descs(tuple_descriptor, {ColumnCategory::REGULAR});

        auto actual_file = paimon_orc_test_file();
        auto local_fs = io::global_local_filesystem();
        int64_t file_size = 0;
        RETURN_IF_ERROR(local_fs->file_size(actual_file, &file_size));

        auto scan_params = build_local_params();
        scan_params.format_type = TFileFormatType::FORMAT_ORC;

        scan_range.start_offset = 0;
        scan_range.size = file_size;
        scan_range.path = actual_file;
        scan_range.__isset.table_format_params = true;
        scan_range.table_format_params.__isset.paimon_params = true;

        auto local_reader = std::make_unique<PaimonOrcReader>(&_profile, &_runtime_state,
                                                              scan_params, scan_range, 1024, "CST",
                                                              &_kv_cache, nullptr, _cache.get());
        if (local_reader == nullptr) {
            return Status::InternalError("failed to create paimon orc reader");
        }

        OrcInitContext orc_ctx;
        orc_ctx.column_descs = &column_descs;
        orc_ctx.col_name_to_block_idx = &col_name_to_block_idx;
        orc_ctx.tuple_descriptor = tuple_descriptor;
        orc_ctx.params = &scan_params;
        orc_ctx.range = &scan_range;
        orc_ctx.push_down_agg_type = push_down_agg_type;
        auto st = local_reader->init_reader(&orc_ctx);
        *reader = std::move(local_reader);
        return st;
    }

    RuntimeState _runtime_state {(TQueryOptions()), TQueryGlobals()};
    RuntimeProfile _profile {"paimon_reader_test"};
    ShardedKVCache _kv_cache {8};
    cctz::time_zone _timezone;
    std::unique_ptr<FileMetaCache> _cache;
    std::vector<std::string> _temp_files;
};

TEST_F(PaimonReaderTest, read_paimon_parquet_string_column) {
    ObjectPool object_pool;
    auto nullable_string = make_nullable(std::make_shared<DataTypeString>());
    const auto* tuple_descriptor = build_tuple_descriptor(object_pool, {{"c13", nullable_string}});
    auto col_name_to_block_idx = build_col_name_to_block_idx(tuple_descriptor);
    auto column_descs = build_column_descs(tuple_descriptor, {ColumnCategory::REGULAR});

    size_t read_rows = 0;
    Block block;
    read_paimon_parquet_block(
            "./docker/thirdparties/docker-compose/hive/scripts/paimon1/db1.db/"
            "all_table_with_parquet/bucket-0/data-3fe3fe10-7d88-4abd-be1f-6dc2bac84d88-0.parquet",
            {}, tuple_descriptor, col_name_to_block_idx, column_descs, {{"c13", nullable_string}},
            &read_rows, &block);

    ASSERT_GT(read_rows, 0);
    EXPECT_EQ(block.rows(), read_rows);
    verify_nullable_string_column_has_rows(block, "c13", read_rows);
}

TEST_F(PaimonReaderTest, read_paimon_orc_string_column) {
    ObjectPool object_pool;
    auto nullable_string = make_nullable(std::make_shared<DataTypeString>());
    const auto* tuple_descriptor = build_tuple_descriptor(object_pool, {{"c13", nullable_string}});
    auto col_name_to_block_idx = build_col_name_to_block_idx(tuple_descriptor);
    auto column_descs = build_column_descs(tuple_descriptor, {ColumnCategory::REGULAR});

    size_t read_rows = 0;
    Block block;
    read_paimon_orc_block(
            "./docker/thirdparties/docker-compose/hive/scripts/paimon1/db1.db/"
            "all_table/bucket-0/data-12a1c2cb-6f0e-4692-996f-2d7a40a8c094-0.orc",
            {}, tuple_descriptor, col_name_to_block_idx, column_descs, {{"c13", nullable_string}},
            &read_rows, &block);

    ASSERT_GT(read_rows, 0);
    EXPECT_EQ(block.rows(), read_rows);
    verify_nullable_string_column_has_rows(block, "c13", read_rows);
}

TEST_F(PaimonReaderTest, fills_missing_column_with_null_for_parquet) {
    ObjectPool object_pool;
    auto nullable_string = make_nullable(std::make_shared<DataTypeString>());
    const auto* tuple_descriptor = build_tuple_descriptor(
            object_pool, {{"c13", nullable_string}, {"missing_col", nullable_string}});
    auto col_name_to_block_idx = build_col_name_to_block_idx(tuple_descriptor);
    auto column_descs = build_column_descs(tuple_descriptor,
                                           {ColumnCategory::REGULAR, ColumnCategory::REGULAR});

    size_t read_rows = 0;
    Block block;
    read_paimon_parquet_block(
            "./docker/thirdparties/docker-compose/hive/scripts/paimon1/db1.db/"
            "all_table_with_parquet/bucket-0/data-3fe3fe10-7d88-4abd-be1f-6dc2bac84d88-0.parquet",
            {}, tuple_descriptor, col_name_to_block_idx, column_descs,
            {{"c13", nullable_string}, {"missing_col", nullable_string}}, &read_rows, &block);

    ASSERT_GT(read_rows, 0);
    EXPECT_EQ(block.rows(), read_rows);
    verify_nullable_string_column_all_null(block, "missing_col", read_rows);
}

TEST_F(PaimonReaderTest, fills_missing_column_with_null_for_orc) {
    ObjectPool object_pool;
    auto nullable_string = make_nullable(std::make_shared<DataTypeString>());
    const auto* tuple_descriptor = build_tuple_descriptor(
            object_pool, {{"c13", nullable_string}, {"missing_col", nullable_string}});
    auto col_name_to_block_idx = build_col_name_to_block_idx(tuple_descriptor);
    auto column_descs = build_column_descs(tuple_descriptor,
                                           {ColumnCategory::REGULAR, ColumnCategory::REGULAR});

    size_t read_rows = 0;
    Block block;
    read_paimon_orc_block(
            "./docker/thirdparties/docker-compose/hive/scripts/paimon1/db1.db/"
            "all_table/bucket-0/data-12a1c2cb-6f0e-4692-996f-2d7a40a8c094-0.orc",
            {}, tuple_descriptor, col_name_to_block_idx, column_descs,
            {{"c13", nullable_string}, {"missing_col", nullable_string}}, &read_rows, &block);

    ASSERT_GT(read_rows, 0);
    EXPECT_EQ(block.rows(), read_rows);
    verify_nullable_string_column_all_null(block, "missing_col", read_rows);
}

TEST_F(PaimonReaderTest, reads_generated_column_from_file_for_parquet) {
    ObjectPool object_pool;
    auto nullable_string = make_nullable(std::make_shared<DataTypeString>());
    const auto* tuple_descriptor = build_tuple_descriptor(object_pool, {{"c13", nullable_string}});
    auto col_name_to_block_idx = build_col_name_to_block_idx(tuple_descriptor);
    auto column_descs = build_column_descs(tuple_descriptor, {ColumnCategory::GENERATED});

    size_t read_rows = 0;
    Block block;
    read_paimon_parquet_block(
            "./docker/thirdparties/docker-compose/hive/scripts/paimon1/db1.db/"
            "all_table_with_parquet/bucket-0/data-3fe3fe10-7d88-4abd-be1f-6dc2bac84d88-0.parquet",
            {}, tuple_descriptor, col_name_to_block_idx, column_descs, {{"c13", nullable_string}},
            &read_rows, &block);

    ASSERT_GT(read_rows, 0);
    EXPECT_EQ(block.rows(), read_rows);
    verify_nullable_string_column_has_rows(block, "c13", read_rows);
}

TEST_F(PaimonReaderTest, reads_generated_column_from_file_for_orc) {
    ObjectPool object_pool;
    auto nullable_string = make_nullable(std::make_shared<DataTypeString>());
    const auto* tuple_descriptor = build_tuple_descriptor(object_pool, {{"c13", nullable_string}});
    auto col_name_to_block_idx = build_col_name_to_block_idx(tuple_descriptor);
    auto column_descs = build_column_descs(tuple_descriptor, {ColumnCategory::GENERATED});

    size_t read_rows = 0;
    Block block;
    read_paimon_orc_block(
            "./docker/thirdparties/docker-compose/hive/scripts/paimon1/db1.db/"
            "all_table/bucket-0/data-12a1c2cb-6f0e-4692-996f-2d7a40a8c094-0.orc",
            {}, tuple_descriptor, col_name_to_block_idx, column_descs, {{"c13", nullable_string}},
            &read_rows, &block);

    ASSERT_GT(read_rows, 0);
    EXPECT_EQ(block.rows(), read_rows);
    verify_nullable_string_column_has_rows(block, "c13", read_rows);
}

TEST_F(PaimonReaderTest, matches_uppercase_orc_columns_case_insensitively) {
    ObjectPool object_pool;
    auto nullable_string = make_nullable(std::make_shared<DataTypeString>());
    const auto* tuple_descriptor = build_tuple_descriptor(object_pool, {{"val", nullable_string}});
    auto col_name_to_block_idx = build_col_name_to_block_idx(tuple_descriptor);
    auto column_descs = build_column_descs(tuple_descriptor, {ColumnCategory::REGULAR});

    size_t read_rows = 0;
    Block block;
    read_paimon_orc_block(
            "./docker/thirdparties/docker-compose/hive/scripts/paimon1/db1.db/"
            "tb_with_upper_case/bucket-0/data-3eb9d575-5340-43d6-a45d-588a72a03306-0.orc",
            {}, tuple_descriptor, col_name_to_block_idx, column_descs, {{"val", nullable_string}},
            &read_rows, &block);

    ASSERT_GT(read_rows, 0);
    EXPECT_EQ(block.rows(), read_rows);
    verify_nullable_string_column_has_rows(block, "val", read_rows);
}

TEST_F(PaimonReaderTest, reads_renamed_column_via_history_schema_for_parquet) {
    ObjectPool object_pool;
    auto nullable_string = make_nullable(std::make_shared<DataTypeString>());
    const auto* tuple_descriptor =
            build_tuple_descriptor(object_pool, {{"renamed_c13", nullable_string}});
    auto col_name_to_block_idx = build_col_name_to_block_idx(tuple_descriptor);
    auto column_descs = build_column_descs(tuple_descriptor, {ColumnCategory::REGULAR});

    TFileRangeDesc scan_range;
    scan_range.__isset.table_format_params = true;
    scan_range.table_format_params.__isset.paimon_params = true;
    scan_range.table_format_params.paimon_params.__set_schema_id(1);

    size_t read_rows = 0;
    Block block;
    read_paimon_parquet_block(
            "./docker/thirdparties/docker-compose/hive/scripts/paimon1/db1.db/"
            "all_table_with_parquet/bucket-0/data-3fe3fe10-7d88-4abd-be1f-6dc2bac84d88-0.parquet",
            scan_range, tuple_descriptor, col_name_to_block_idx, column_descs,
            {{"renamed_c13", nullable_string}}, &read_rows, &block,
            [&](TFileScanRangeParams* scan_params) {
                scan_params->__set_current_schema_id(2);
                scan_params->__set_history_schema_info(
                        {build_history_schema_with_string_field(1, "c13", 13),
                         build_history_schema_with_string_field(2, "renamed_c13", 13)});
            });

    ASSERT_GT(read_rows, 0);
    EXPECT_EQ(block.rows(), read_rows);
    verify_nullable_string_column_has_rows(block, "renamed_c13", read_rows);
}

TEST_F(PaimonReaderTest, reads_renamed_column_via_history_schema_for_orc) {
    ObjectPool object_pool;
    auto nullable_string = make_nullable(std::make_shared<DataTypeString>());
    const auto* tuple_descriptor =
            build_tuple_descriptor(object_pool, {{"renamed_c13", nullable_string}});
    auto col_name_to_block_idx = build_col_name_to_block_idx(tuple_descriptor);
    auto column_descs = build_column_descs(tuple_descriptor, {ColumnCategory::REGULAR});

    TFileRangeDesc scan_range;
    scan_range.__isset.table_format_params = true;
    scan_range.table_format_params.__isset.paimon_params = true;
    scan_range.table_format_params.paimon_params.__set_schema_id(1);

    size_t read_rows = 0;
    Block block;
    read_paimon_orc_block(
            "./docker/thirdparties/docker-compose/hive/scripts/paimon1/db1.db/"
            "all_table/bucket-0/data-12a1c2cb-6f0e-4692-996f-2d7a40a8c094-0.orc",
            scan_range, tuple_descriptor, col_name_to_block_idx, column_descs,
            {{"renamed_c13", nullable_string}}, &read_rows, &block,
            [&](TFileScanRangeParams* scan_params) {
                scan_params->__set_current_schema_id(2);
                scan_params->__set_history_schema_info(
                        {build_history_schema_with_string_field(1, "c13", 13),
                         build_history_schema_with_string_field(2, "renamed_c13", 13)});
            });

    ASSERT_GT(read_rows, 0);
    EXPECT_EQ(block.rows(), read_rows);
    verify_nullable_string_column_has_rows(block, "renamed_c13", read_rows);
}

TEST_F(PaimonReaderTest, missing_file_history_schema_returns_error_for_parquet) {
    ObjectPool object_pool;
    auto nullable_string = make_nullable(std::make_shared<DataTypeString>());
    const auto* tuple_descriptor =
            build_tuple_descriptor(object_pool, {{"renamed_c13", nullable_string}});
    auto col_name_to_block_idx = build_col_name_to_block_idx(tuple_descriptor);
    auto column_descs = build_column_descs(tuple_descriptor, {ColumnCategory::REGULAR});

    TFileRangeDesc scan_range;
    scan_range.__isset.table_format_params = true;
    scan_range.table_format_params.__isset.paimon_params = true;
    scan_range.table_format_params.paimon_params.__set_schema_id(1);

    size_t read_rows = 0;
    Block block;
    auto st = try_read_paimon_parquet_block(
            "./docker/thirdparties/docker-compose/hive/scripts/paimon1/db1.db/"
            "all_table_with_parquet/bucket-0/data-3fe3fe10-7d88-4abd-be1f-6dc2bac84d88-0.parquet",
            scan_range, tuple_descriptor, col_name_to_block_idx, column_descs,
            {{"renamed_c13", nullable_string}}, &read_rows, &block,
            [&](TFileScanRangeParams* scan_params) {
                scan_params->__set_current_schema_id(2);
                scan_params->__set_history_schema_info(
                        {build_history_schema_with_string_field(2, "renamed_c13", 13)});
            });

    ASSERT_FALSE(st.ok());
    EXPECT_NE(st.to_string().find("miss table/file schema info"), std::string::npos);
}

TEST_F(PaimonReaderTest, missing_file_history_schema_returns_error_for_orc) {
    ObjectPool object_pool;
    auto nullable_string = make_nullable(std::make_shared<DataTypeString>());
    const auto* tuple_descriptor =
            build_tuple_descriptor(object_pool, {{"renamed_c13", nullable_string}});
    auto col_name_to_block_idx = build_col_name_to_block_idx(tuple_descriptor);
    auto column_descs = build_column_descs(tuple_descriptor, {ColumnCategory::REGULAR});

    TFileRangeDesc scan_range;
    scan_range.__isset.table_format_params = true;
    scan_range.table_format_params.__isset.paimon_params = true;
    scan_range.table_format_params.paimon_params.__set_schema_id(1);

    size_t read_rows = 0;
    Block block;
    auto st = try_read_paimon_orc_block(
            "./docker/thirdparties/docker-compose/hive/scripts/paimon1/db1.db/"
            "all_table/bucket-0/data-12a1c2cb-6f0e-4692-996f-2d7a40a8c094-0.orc",
            scan_range, tuple_descriptor, col_name_to_block_idx, column_descs,
            {{"renamed_c13", nullable_string}}, &read_rows, &block,
            [&](TFileScanRangeParams* scan_params) {
                scan_params->__set_current_schema_id(2);
                scan_params->__set_history_schema_info(
                        {build_history_schema_with_string_field(2, "renamed_c13", 13)});
            });

    ASSERT_FALSE(st.ok());
    EXPECT_NE(st.to_string().find("miss table/file schema info"), std::string::npos);
}

TEST_F(PaimonReaderTest, parquet_deletion_vector_disables_count_pushdown_without_row_count) {
    auto deletion_file = write_valid_deletion_vector_file({1, 3, 7});
    auto range = build_range_with_deletion_file(deletion_file);
    std::unique_ptr<PaimonParquetReader> reader;
    auto status = try_init_paimon_parquet_reader(range, TPushAggOp::COUNT, &reader);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(reader->get_push_down_agg_type(), TPushAggOp::NONE);
    EXPECT_TRUE(reader->has_delete_operations());
}

TEST_F(PaimonReaderTest, parquet_deletion_vector_loads_expected_row_ids) {
    auto deletion_file = write_valid_deletion_vector_file({1, 3, 7});
    auto range = build_range_with_deletion_file(deletion_file);
    std::unique_ptr<PaimonParquetReader> reader;
    auto status = try_init_paimon_parquet_reader(range, TPushAggOp::NONE, &reader);

    ASSERT_TRUE(status.ok()) << status;
    ASSERT_NE(reader->_delete_rows, nullptr);
    EXPECT_EQ(*reader->_delete_rows, (std::vector<int64_t> {1, 3, 7}));
}

TEST_F(PaimonReaderTest, parquet_without_deletion_vector_keeps_count_pushdown) {
    TFileRangeDesc range;
    range.__isset.table_format_params = true;
    std::unique_ptr<PaimonParquetReader> reader;
    auto status = try_init_paimon_parquet_reader(range, TPushAggOp::COUNT, &reader);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(reader->get_push_down_agg_type(), TPushAggOp::COUNT);
    EXPECT_FALSE(reader->has_delete_operations());
}

TEST_F(PaimonReaderTest, orc_without_deletion_vector_keeps_count_pushdown) {
    TFileRangeDesc range;
    range.__isset.table_format_params = true;
    std::unique_ptr<PaimonOrcReader> reader;
    auto status = try_init_paimon_orc_reader(range, TPushAggOp::COUNT, &reader);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(reader->get_push_down_agg_type(), TPushAggOp::COUNT);
    EXPECT_FALSE(reader->has_delete_operations());
}

TEST_F(PaimonReaderTest, parquet_deletion_vector_keeps_count_pushdown_with_row_count) {
    auto deletion_file = write_valid_deletion_vector_file({2, 4});
    auto range = build_range_with_deletion_file(deletion_file);
    range.table_format_params.paimon_params.__set_row_count(10);
    std::unique_ptr<PaimonParquetReader> reader;
    auto status = try_init_paimon_parquet_reader(range, TPushAggOp::COUNT, &reader);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(reader->get_push_down_agg_type(), TPushAggOp::COUNT);
    EXPECT_TRUE(reader->has_delete_operations());
}

TEST_F(PaimonReaderTest, orc_deletion_vector_disables_count_pushdown_without_row_count) {
    auto deletion_file = write_valid_deletion_vector_file({5, 9});
    auto range = build_range_with_deletion_file(deletion_file);
    std::unique_ptr<PaimonOrcReader> reader;
    auto status = try_init_paimon_orc_reader(range, TPushAggOp::COUNT, &reader);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(reader->get_push_down_agg_type(), TPushAggOp::NONE);
    EXPECT_TRUE(reader->has_delete_operations());
}

TEST_F(PaimonReaderTest, orc_deletion_vector_loads_expected_row_ids) {
    auto deletion_file = write_valid_deletion_vector_file({5, 9});
    auto range = build_range_with_deletion_file(deletion_file);
    std::unique_ptr<PaimonOrcReader> reader;
    auto status = try_init_paimon_orc_reader(range, TPushAggOp::NONE, &reader);

    ASSERT_TRUE(status.ok()) << status;
    ASSERT_NE(reader->_delete_rows, nullptr);
    EXPECT_EQ(*reader->_delete_rows, (std::vector<int64_t> {5, 9}));
}

TEST_F(PaimonReaderTest, orc_deletion_vector_keeps_count_pushdown_with_row_count) {
    auto deletion_file = write_valid_deletion_vector_file({5, 9});
    auto range = build_range_with_deletion_file(deletion_file);
    range.table_format_params.paimon_params.__set_row_count(20);
    std::unique_ptr<PaimonOrcReader> reader;
    auto status = try_init_paimon_orc_reader(range, TPushAggOp::COUNT, &reader);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(reader->get_push_down_agg_type(), TPushAggOp::COUNT);
    EXPECT_TRUE(reader->has_delete_operations());
}

TEST_F(PaimonReaderTest, parquet_deletion_vector_reuses_cached_rows) {
    auto deletion_file = write_valid_deletion_vector_file({2, 4, 8});
    auto range = build_range_with_deletion_file(deletion_file);
    std::unique_ptr<PaimonParquetReader> first_reader;
    auto first_status = try_init_paimon_parquet_reader(range, TPushAggOp::NONE, &first_reader);
    ASSERT_TRUE(first_status.ok()) << first_status;
    ASSERT_NE(first_reader->_delete_rows, nullptr);

    std::unique_ptr<PaimonParquetReader> second_reader;
    auto second_status = try_init_paimon_parquet_reader(range, TPushAggOp::NONE, &second_reader);
    ASSERT_TRUE(second_status.ok()) << second_status;
    ASSERT_NE(second_reader->_delete_rows, nullptr);

    EXPECT_EQ(first_reader->_delete_rows, second_reader->_delete_rows);
    EXPECT_EQ(*second_reader->_delete_rows, (std::vector<int64_t> {2, 4, 8}));
}

TEST_F(PaimonReaderTest, parquet_invalid_deletion_vector_magic_returns_error) {
    auto deletion_file = write_invalid_magic_deletion_vector_file();
    auto range = build_range_with_deletion_file(deletion_file);
    std::unique_ptr<PaimonParquetReader> reader;
    auto status = try_init_paimon_parquet_reader(range, TPushAggOp::COUNT, &reader);
    ASSERT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("invalid magic number"), std::string::npos);
    ASSERT_NE(reader, nullptr);
    EXPECT_FALSE(reader->has_delete_operations());
}

TEST_F(PaimonReaderTest, parquet_invalid_deletion_vector_length_returns_error) {
    auto deletion_file = write_invalid_length_deletion_vector_file();
    auto range = build_range_with_deletion_file(deletion_file);
    std::unique_ptr<PaimonParquetReader> reader;
    auto status = try_init_paimon_parquet_reader(range, TPushAggOp::COUNT, &reader);
    ASSERT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("length not match"), std::string::npos);
    ASSERT_NE(reader, nullptr);
    EXPECT_FALSE(reader->has_delete_operations());
}

TEST_F(PaimonReaderTest, parquet_invalid_deletion_vector_bitmap_returns_error) {
    auto deletion_file = write_invalid_bitmap_deletion_vector_file();
    auto range = build_range_with_deletion_file(deletion_file);
    std::unique_ptr<PaimonParquetReader> reader;
    auto status = try_init_paimon_parquet_reader(range, TPushAggOp::COUNT, &reader);
    ASSERT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("failed to deserialize roaring bitmap"), std::string::npos);
    ASSERT_NE(reader, nullptr);
    EXPECT_FALSE(reader->has_delete_operations());
}

} // namespace doris
