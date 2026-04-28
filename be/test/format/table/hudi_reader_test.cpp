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

#include "format/table/hudi_reader.h"

#include <cctz/time_zone.h>
#include <gen_cpp/ExternalTableSchema_types.h>
#include <gtest/gtest.h>

#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "core/block/block.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_string.h"
#include "format/column_descriptor.h"
#include "format/orc/vorc_reader.h"
#include "format/parquet/vparquet_reader.h"
#include "io/fs/file_meta_cache.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "io/fs/file_system.h"
#include "io/fs/local_file_system.h"
#include "runtime/runtime_state.h"
#include "testutil/desc_tbl_builder.h"
#include "util/timezone_utils.h"

namespace doris {

class HudiReaderTest : public ::testing::Test {
protected:
    void SetUp() override {
        _cache = std::make_unique<FileMetaCache>(1024);
        TimezoneUtils::find_cctz_time_zone(TimezoneUtils::default_time_zone, _timezone);
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

    void verify_nullable_string_column_values(const Block& block, const std::string& column_name,
                                              const std::vector<std::string>& expected_values) {
        const auto& column = block.get_by_position(block.get_position_by_name(column_name));
        auto* nullable = check_and_get_column<ColumnNullable>(column.column.get());
        ASSERT_NE(nullable, nullptr);
        const auto& nested = static_cast<const ColumnString&>(nullable->get_nested_column());
        ASSERT_EQ(nullable->size(), expected_values.size());
        for (size_t i = 0; i < expected_values.size(); ++i) {
            ASSERT_FALSE(nullable->is_null_at(i));
            EXPECT_EQ(nested.get_data_at(i).to_string(), expected_values[i]);
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

    Status try_read_hudi_parquet_block(
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
        TFileScanRangeParams scan_params;
        scan_params.format_type = TFileFormatType::FORMAT_PARQUET;
        if (customize_scan_params != nullptr) {
            customize_scan_params(&scan_params);
        }

        scan_range.start_offset = 0;
        scan_range.size = file_size;
        scan_range.path = actual_file;
        scan_range.__isset.table_format_params = true;

        RuntimeProfile profile("hudi_parquet_reader_test");
        auto reader = std::make_unique<HudiParquetReader>(&profile, scan_params, scan_range, 1024,
                                                          &_timezone, nullptr, &runtime_state,
                                                          _cache.get());
        if (reader == nullptr) {
            return Status::InternalError("failed to create hudi parquet reader");
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

    void read_hudi_parquet_block(
            const std::string& actual_file, TFileRangeDesc scan_range,
            const TupleDescriptor* tuple_descriptor,
            const std::unordered_map<std::string, uint32_t>& col_name_to_block_idx,
            std::vector<ColumnDescriptor> column_descs,
            const std::vector<std::pair<std::string, DataTypePtr>>& block_columns,
            size_t* read_rows, Block* block,
            const std::function<void(TFileScanRangeParams*)>& customize_scan_params = {}) {
        auto st = try_read_hudi_parquet_block(
                actual_file, std::move(scan_range), tuple_descriptor, col_name_to_block_idx,
                std::move(column_descs), block_columns, read_rows, block, customize_scan_params);
        ASSERT_TRUE(st.ok()) << st;
    }

    Status try_read_hudi_orc_block(
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
        TFileScanRangeParams scan_params;
        scan_params.format_type = TFileFormatType::FORMAT_ORC;
        if (customize_scan_params != nullptr) {
            customize_scan_params(&scan_params);
        }

        scan_range.start_offset = 0;
        scan_range.size = file_size;
        scan_range.path = actual_file;
        scan_range.__isset.table_format_params = true;

        RuntimeProfile profile("hudi_orc_reader_test");
        auto reader =
                std::make_unique<HudiOrcReader>(&profile, &runtime_state, scan_params, scan_range,
                                                1024, "CST", nullptr, _cache.get());
        if (reader == nullptr) {
            return Status::InternalError("failed to create hudi orc reader");
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

    void read_hudi_orc_block(
            const std::string& actual_file, TFileRangeDesc scan_range,
            const TupleDescriptor* tuple_descriptor,
            const std::unordered_map<std::string, uint32_t>& col_name_to_block_idx,
            std::vector<ColumnDescriptor> column_descs,
            const std::vector<std::pair<std::string, DataTypePtr>>& block_columns,
            size_t* read_rows, Block* block,
            const std::function<void(TFileScanRangeParams*)>& customize_scan_params = {}) {
        auto st = try_read_hudi_orc_block(actual_file, std::move(scan_range), tuple_descriptor,
                                          col_name_to_block_idx, std::move(column_descs),
                                          block_columns, read_rows, block, customize_scan_params);
        ASSERT_TRUE(st.ok()) << st;
    }

    std::unique_ptr<FileMetaCache> _cache;
    cctz::time_zone _timezone;
};

TEST_F(HudiReaderTest, read_hudi_parquet_name_column) {
    ObjectPool object_pool;
    auto nullable_string = make_nullable(std::make_shared<DataTypeString>());
    const auto* tuple_descriptor = build_tuple_descriptor(object_pool, {{"name", nullable_string}});
    auto col_name_to_block_idx = build_col_name_to_block_idx(tuple_descriptor);
    auto column_descs = build_column_descs(tuple_descriptor, {ColumnCategory::REGULAR});

    size_t read_rows = 0;
    Block block;
    read_hudi_parquet_block(
            "./be/test/exec/test_data/nested_user_profiles_parquet/"
            "part-00000-64a7a390-1a03-4efc-ab51-557e9369a1f9-c000.snappy.parquet",
            {}, tuple_descriptor, col_name_to_block_idx, column_descs, {{"name", nullable_string}},
            &read_rows, &block);

    ASSERT_GT(read_rows, 0);
    EXPECT_EQ(block.rows(), read_rows);
    verify_nullable_string_column_has_rows(block, "name", read_rows);
}

TEST_F(HudiReaderTest, read_hudi_orc_name_column) {
    ObjectPool object_pool;
    auto nullable_string = make_nullable(std::make_shared<DataTypeString>());
    const auto* tuple_descriptor = build_tuple_descriptor(object_pool, {{"name", nullable_string}});
    auto col_name_to_block_idx = build_col_name_to_block_idx(tuple_descriptor);
    auto column_descs = build_column_descs(tuple_descriptor, {ColumnCategory::REGULAR});

    size_t read_rows = 0;
    Block block;
    read_hudi_orc_block(
            "./be/test/exec/test_data/nested_user_profiles_orc/"
            "part-00000-62614f23-05d1-4043-a533-b155ef52b720-c000.snappy.orc",
            {}, tuple_descriptor, col_name_to_block_idx, column_descs, {{"name", nullable_string}},
            &read_rows, &block);

    ASSERT_GT(read_rows, 0);
    EXPECT_EQ(block.rows(), read_rows);
    verify_nullable_string_column_has_rows(block, "name", read_rows);
}

TEST_F(HudiReaderTest, fills_partition_column_from_scan_range_for_parquet) {
    ObjectPool object_pool;
    auto nullable_string = make_nullable(std::make_shared<DataTypeString>());
    const auto* tuple_descriptor = build_tuple_descriptor(
            object_pool, {{"name", nullable_string}, {"year", nullable_string}});
    auto col_name_to_block_idx = build_col_name_to_block_idx(tuple_descriptor);
    auto column_descs = build_column_descs(
            tuple_descriptor, {ColumnCategory::REGULAR, ColumnCategory::PARTITION_KEY});

    TFileRangeDesc scan_range;
    scan_range.__set_columns_from_path_keys({"year"});
    scan_range.__set_columns_from_path({"2024"});

    size_t read_rows = 0;
    Block block;
    read_hudi_parquet_block(
            "./be/test/exec/test_data/nested_user_profiles_parquet/"
            "part-00000-64a7a390-1a03-4efc-ab51-557e9369a1f9-c000.snappy.parquet",
            scan_range, tuple_descriptor, col_name_to_block_idx, column_descs,
            {{"name", nullable_string}, {"year", nullable_string}}, &read_rows, &block);

    ASSERT_GT(read_rows, 0);
    EXPECT_EQ(block.rows(), read_rows);
    verify_nullable_string_column_values(block, "year",
                                         std::vector<std::string>(read_rows, "2024"));
}

TEST_F(HudiReaderTest, fills_partition_column_from_scan_range_for_orc) {
    ObjectPool object_pool;
    auto nullable_string = make_nullable(std::make_shared<DataTypeString>());
    const auto* tuple_descriptor = build_tuple_descriptor(
            object_pool, {{"name", nullable_string}, {"year", nullable_string}});
    auto col_name_to_block_idx = build_col_name_to_block_idx(tuple_descriptor);
    auto column_descs = build_column_descs(
            tuple_descriptor, {ColumnCategory::REGULAR, ColumnCategory::PARTITION_KEY});

    TFileRangeDesc scan_range;
    scan_range.__set_columns_from_path_keys({"year"});
    scan_range.__set_columns_from_path({"2024"});

    size_t read_rows = 0;
    Block block;
    read_hudi_orc_block(
            "./be/test/exec/test_data/nested_user_profiles_orc/"
            "part-00000-62614f23-05d1-4043-a533-b155ef52b720-c000.snappy.orc",
            scan_range, tuple_descriptor, col_name_to_block_idx, column_descs,
            {{"name", nullable_string}, {"year", nullable_string}}, &read_rows, &block);

    ASSERT_GT(read_rows, 0);
    EXPECT_EQ(block.rows(), read_rows);
    verify_nullable_string_column_values(block, "year",
                                         std::vector<std::string>(read_rows, "2024"));
}

TEST_F(HudiReaderTest, fills_missing_column_with_null_for_parquet) {
    ObjectPool object_pool;
    auto nullable_string = make_nullable(std::make_shared<DataTypeString>());
    const auto* tuple_descriptor = build_tuple_descriptor(
            object_pool, {{"name", nullable_string}, {"country", nullable_string}});
    auto col_name_to_block_idx = build_col_name_to_block_idx(tuple_descriptor);
    auto column_descs = build_column_descs(tuple_descriptor,
                                           {ColumnCategory::REGULAR, ColumnCategory::REGULAR});

    size_t read_rows = 0;
    Block block;
    read_hudi_parquet_block(
            "./be/test/exec/test_data/nested_user_profiles_parquet/"
            "part-00000-64a7a390-1a03-4efc-ab51-557e9369a1f9-c000.snappy.parquet",
            {}, tuple_descriptor, col_name_to_block_idx, column_descs,
            {{"name", nullable_string}, {"country", nullable_string}}, &read_rows, &block);

    ASSERT_GT(read_rows, 0);
    EXPECT_EQ(block.rows(), read_rows);
    verify_nullable_string_column_all_null(block, "country", read_rows);
}

TEST_F(HudiReaderTest, fills_missing_column_with_null_for_orc) {
    ObjectPool object_pool;
    auto nullable_string = make_nullable(std::make_shared<DataTypeString>());
    const auto* tuple_descriptor = build_tuple_descriptor(
            object_pool, {{"name", nullable_string}, {"country", nullable_string}});
    auto col_name_to_block_idx = build_col_name_to_block_idx(tuple_descriptor);
    auto column_descs = build_column_descs(tuple_descriptor,
                                           {ColumnCategory::REGULAR, ColumnCategory::REGULAR});

    size_t read_rows = 0;
    Block block;
    read_hudi_orc_block(
            "./be/test/exec/test_data/nested_user_profiles_orc/"
            "part-00000-62614f23-05d1-4043-a533-b155ef52b720-c000.snappy.orc",
            {}, tuple_descriptor, col_name_to_block_idx, column_descs,
            {{"name", nullable_string}, {"country", nullable_string}}, &read_rows, &block);

    ASSERT_GT(read_rows, 0);
    EXPECT_EQ(block.rows(), read_rows);
    verify_nullable_string_column_all_null(block, "country", read_rows);
}

TEST_F(HudiReaderTest, reads_generated_column_from_file_for_parquet) {
    ObjectPool object_pool;
    auto nullable_string = make_nullable(std::make_shared<DataTypeString>());
    const auto* tuple_descriptor = build_tuple_descriptor(object_pool, {{"name", nullable_string}});
    auto col_name_to_block_idx = build_col_name_to_block_idx(tuple_descriptor);
    auto column_descs = build_column_descs(tuple_descriptor, {ColumnCategory::GENERATED});

    size_t read_rows = 0;
    Block block;
    read_hudi_parquet_block(
            "./be/test/exec/test_data/nested_user_profiles_parquet/"
            "part-00000-64a7a390-1a03-4efc-ab51-557e9369a1f9-c000.snappy.parquet",
            {}, tuple_descriptor, col_name_to_block_idx, column_descs, {{"name", nullable_string}},
            &read_rows, &block);

    ASSERT_GT(read_rows, 0);
    EXPECT_EQ(block.rows(), read_rows);
    verify_nullable_string_column_has_rows(block, "name", read_rows);
}

TEST_F(HudiReaderTest, reads_generated_column_from_file_for_orc) {
    ObjectPool object_pool;
    auto nullable_string = make_nullable(std::make_shared<DataTypeString>());
    const auto* tuple_descriptor = build_tuple_descriptor(object_pool, {{"name", nullable_string}});
    auto col_name_to_block_idx = build_col_name_to_block_idx(tuple_descriptor);
    auto column_descs = build_column_descs(tuple_descriptor, {ColumnCategory::GENERATED});

    size_t read_rows = 0;
    Block block;
    read_hudi_orc_block(
            "./be/test/exec/test_data/nested_user_profiles_orc/"
            "part-00000-62614f23-05d1-4043-a533-b155ef52b720-c000.snappy.orc",
            {}, tuple_descriptor, col_name_to_block_idx, column_descs, {{"name", nullable_string}},
            &read_rows, &block);

    ASSERT_GT(read_rows, 0);
    EXPECT_EQ(block.rows(), read_rows);
    verify_nullable_string_column_has_rows(block, "name", read_rows);
}

TEST_F(HudiReaderTest, reads_renamed_column_via_history_schema_for_parquet) {
    ObjectPool object_pool;
    auto nullable_string = make_nullable(std::make_shared<DataTypeString>());
    const auto* tuple_descriptor =
            build_tuple_descriptor(object_pool, {{"display_name", nullable_string}});
    auto col_name_to_block_idx = build_col_name_to_block_idx(tuple_descriptor);
    auto column_descs = build_column_descs(tuple_descriptor, {ColumnCategory::REGULAR});

    TFileRangeDesc scan_range;
    scan_range.__isset.table_format_params = true;
    scan_range.table_format_params.__isset.hudi_params = true;
    scan_range.table_format_params.hudi_params.__set_schema_id(1);

    size_t read_rows = 0;
    Block block;
    read_hudi_parquet_block(
            "./be/test/exec/test_data/nested_user_profiles_parquet/"
            "part-00000-64a7a390-1a03-4efc-ab51-557e9369a1f9-c000.snappy.parquet",
            scan_range, tuple_descriptor, col_name_to_block_idx, column_descs,
            {{"display_name", nullable_string}}, &read_rows, &block,
            [&](TFileScanRangeParams* scan_params) {
                scan_params->__set_current_schema_id(2);
                scan_params->__set_history_schema_info(
                        {build_history_schema_with_string_field(1, "name", 2),
                         build_history_schema_with_string_field(2, "display_name", 2)});
            });

    ASSERT_GT(read_rows, 0);
    EXPECT_EQ(block.rows(), read_rows);
    verify_nullable_string_column_has_rows(block, "display_name", read_rows);
}

TEST_F(HudiReaderTest, reads_renamed_column_via_history_schema_for_orc) {
    ObjectPool object_pool;
    auto nullable_string = make_nullable(std::make_shared<DataTypeString>());
    const auto* tuple_descriptor =
            build_tuple_descriptor(object_pool, {{"display_name", nullable_string}});
    auto col_name_to_block_idx = build_col_name_to_block_idx(tuple_descriptor);
    auto column_descs = build_column_descs(tuple_descriptor, {ColumnCategory::REGULAR});

    TFileRangeDesc scan_range;
    scan_range.__isset.table_format_params = true;
    scan_range.table_format_params.__isset.hudi_params = true;
    scan_range.table_format_params.hudi_params.__set_schema_id(1);

    size_t read_rows = 0;
    Block block;
    read_hudi_orc_block(
            "./be/test/exec/test_data/nested_user_profiles_orc/"
            "part-00000-62614f23-05d1-4043-a533-b155ef52b720-c000.snappy.orc",
            scan_range, tuple_descriptor, col_name_to_block_idx, column_descs,
            {{"display_name", nullable_string}}, &read_rows, &block,
            [&](TFileScanRangeParams* scan_params) {
                scan_params->__set_current_schema_id(2);
                scan_params->__set_history_schema_info(
                        {build_history_schema_with_string_field(1, "name", 2),
                         build_history_schema_with_string_field(2, "display_name", 2)});
            });

    ASSERT_GT(read_rows, 0);
    EXPECT_EQ(block.rows(), read_rows);
    verify_nullable_string_column_has_rows(block, "display_name", read_rows);
}

TEST_F(HudiReaderTest, missing_file_history_schema_returns_error_for_parquet) {
    ObjectPool object_pool;
    auto nullable_string = make_nullable(std::make_shared<DataTypeString>());
    const auto* tuple_descriptor =
            build_tuple_descriptor(object_pool, {{"display_name", nullable_string}});
    auto col_name_to_block_idx = build_col_name_to_block_idx(tuple_descriptor);
    auto column_descs = build_column_descs(tuple_descriptor, {ColumnCategory::REGULAR});

    TFileRangeDesc scan_range;
    scan_range.__isset.table_format_params = true;
    scan_range.table_format_params.__isset.hudi_params = true;
    scan_range.table_format_params.hudi_params.__set_schema_id(1);

    size_t read_rows = 0;
    Block block;
    auto st = try_read_hudi_parquet_block(
            "./be/test/exec/test_data/nested_user_profiles_parquet/"
            "part-00000-64a7a390-1a03-4efc-ab51-557e9369a1f9-c000.snappy.parquet",
            scan_range, tuple_descriptor, col_name_to_block_idx, column_descs,
            {{"display_name", nullable_string}}, &read_rows, &block,
            [&](TFileScanRangeParams* scan_params) {
                scan_params->__set_current_schema_id(2);
                scan_params->__set_history_schema_info(
                        {build_history_schema_with_string_field(2, "display_name", 2)});
            });

    ASSERT_FALSE(st.ok());
    EXPECT_NE(st.to_string().find("miss table/file schema info"), std::string::npos);
}

TEST_F(HudiReaderTest, missing_file_history_schema_returns_error_for_orc) {
    ObjectPool object_pool;
    auto nullable_string = make_nullable(std::make_shared<DataTypeString>());
    const auto* tuple_descriptor =
            build_tuple_descriptor(object_pool, {{"display_name", nullable_string}});
    auto col_name_to_block_idx = build_col_name_to_block_idx(tuple_descriptor);
    auto column_descs = build_column_descs(tuple_descriptor, {ColumnCategory::REGULAR});

    TFileRangeDesc scan_range;
    scan_range.__isset.table_format_params = true;
    scan_range.table_format_params.__isset.hudi_params = true;
    scan_range.table_format_params.hudi_params.__set_schema_id(1);

    size_t read_rows = 0;
    Block block;
    auto st = try_read_hudi_orc_block(
            "./be/test/exec/test_data/nested_user_profiles_orc/"
            "part-00000-62614f23-05d1-4043-a533-b155ef52b720-c000.snappy.orc",
            scan_range, tuple_descriptor, col_name_to_block_idx, column_descs,
            {{"display_name", nullable_string}}, &read_rows, &block,
            [&](TFileScanRangeParams* scan_params) {
                scan_params->__set_current_schema_id(2);
                scan_params->__set_history_schema_info(
                        {build_history_schema_with_string_field(2, "display_name", 2)});
            });

    ASSERT_FALSE(st.ok());
    EXPECT_NE(st.to_string().find("miss table/file schema info"), std::string::npos);
}

} // namespace doris
