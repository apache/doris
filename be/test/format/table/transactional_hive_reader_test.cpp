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

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wkeyword-macro"
#define protected public
#include "format/table/transactional_hive_reader.h"
#undef protected
#pragma clang diagnostic pop

#include <gtest/gtest.h>
#include <unistd.h>

#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/consts.h"
#include "core/block/block.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_string.h"
#include "format/orc/orc_memory_stream_test.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "storage/segment/column_reader.h"
#include "storage/utils.h"
#include "testutil/desc_tbl_builder.h"

namespace doris {

class TransactionalHiveReaderTest : public ::testing::Test {
protected:
    void TearDown() override {
        for (const auto& path : _temp_paths) {
            std::error_code ec;
            std::filesystem::remove_all(path, ec);
        }
    }

    struct DeleteDeltaRow {
        int64_t original_transaction;
        int32_t bucket;
        int64_t row_id;
    };

    struct AcidDataRow {
        int32_t operation;
        int64_t original_transaction;
        int32_t bucket;
        int64_t row_id;
        int64_t current_transaction;
        std::string name;
    };

    struct DeleteDeltaFile {
        std::string directory;
        std::string file_name;
        std::string path;
    };

    Block make_name_block() {
        auto nullable_string = make_nullable(std::make_shared<DataTypeString>());
        Block block;
        block.insert(
                ColumnWithTypeAndName(nullable_string->create_column(), nullable_string, "name"));
        return block;
    }

    DeleteDeltaFile write_delete_delta_orc_file(const std::vector<DeleteDeltaRow>& rows,
                                                const std::string& file_name) {
        auto temp_dir = std::filesystem::temp_directory_path();
        std::string pattern = (temp_dir / "transactional_hive_reader_test_XXXXXX").string();
        std::vector<char> path_buffer(pattern.begin(), pattern.end());
        path_buffer.push_back('\0');

        char* dir = mkdtemp(path_buffer.data());
        EXPECT_NE(dir, nullptr);
        std::string directory = dir == nullptr ? temp_dir.string() : std::string(dir);

        using namespace orc;
        MemoryOutputStream mem_stream(1024 * 1024);
        auto type = createStructType();
        type->addStructField(TransactionalHive::ORIGINAL_TRANSACTION,
                             createPrimitiveType(orc::TypeKind::LONG));
        type->addStructField(TransactionalHive::BUCKET, createPrimitiveType(orc::TypeKind::INT));
        type->addStructField(TransactionalHive::ROW_ID, createPrimitiveType(orc::TypeKind::LONG));

        WriterOptions options;
        options.setMemoryPool(getDefaultPool());
        auto writer = createWriter(*type, &mem_stream, options);
        auto batch = writer->createRowBatch(rows.size());
        auto& root_batch = dynamic_cast<StructVectorBatch&>(*batch);
        auto& original_transaction_batch = dynamic_cast<LongVectorBatch&>(*root_batch.fields[0]);
        auto& bucket_batch = dynamic_cast<LongVectorBatch&>(*root_batch.fields[1]);
        auto& row_id_batch = dynamic_cast<LongVectorBatch&>(*root_batch.fields[2]);

        for (size_t i = 0; i < rows.size(); ++i) {
            original_transaction_batch.data[i] = rows[i].original_transaction;
            bucket_batch.data[i] = rows[i].bucket;
            row_id_batch.data[i] = rows[i].row_id;
        }
        root_batch.numElements = rows.size();
        original_transaction_batch.numElements = rows.size();
        bucket_batch.numElements = rows.size();
        row_id_batch.numElements = rows.size();
        writer->add(*batch);
        writer->close();

        std::filesystem::path file_path = std::filesystem::path(directory) / file_name;
        std::ofstream out(file_path, std::ios::binary | std::ios::trunc);
        out.write(mem_stream.getData(), static_cast<std::streamsize>(mem_stream.getLength()));
        out.close();
        EXPECT_TRUE(out.good());

        _temp_paths.push_back(directory);
        return {directory, file_name, file_path.string()};
    }

    DeleteDeltaFile write_acid_data_orc_file(const std::vector<AcidDataRow>& rows,
                                             const std::string& file_name) {
        auto temp_dir = std::filesystem::temp_directory_path();
        std::string pattern = (temp_dir / "transactional_hive_reader_test_XXXXXX").string();
        std::vector<char> path_buffer(pattern.begin(), pattern.end());
        path_buffer.push_back('\0');

        char* dir = mkdtemp(path_buffer.data());
        EXPECT_NE(dir, nullptr);
        std::string directory = dir == nullptr ? temp_dir.string() : std::string(dir);

        using namespace orc;
        MemoryOutputStream mem_stream(1024 * 1024);
        auto type = createStructType();
        type->addStructField("operation", createPrimitiveType(orc::TypeKind::INT));
        type->addStructField("originalTransaction", createPrimitiveType(orc::TypeKind::LONG));
        type->addStructField("bucket", createPrimitiveType(orc::TypeKind::INT));
        type->addStructField("rowId", createPrimitiveType(orc::TypeKind::LONG));
        type->addStructField("currentTransaction", createPrimitiveType(orc::TypeKind::LONG));
        auto row_type = createStructType();
        row_type->addStructField("name", createPrimitiveType(orc::TypeKind::STRING));
        type->addStructField("row", std::move(row_type));

        WriterOptions options;
        options.setMemoryPool(getDefaultPool());
        auto writer = createWriter(*type, &mem_stream, options);
        auto batch = writer->createRowBatch(rows.size());
        auto& root_batch = dynamic_cast<StructVectorBatch&>(*batch);
        auto& operation_batch = dynamic_cast<LongVectorBatch&>(*root_batch.fields[0]);
        auto& original_transaction_batch = dynamic_cast<LongVectorBatch&>(*root_batch.fields[1]);
        auto& bucket_batch = dynamic_cast<LongVectorBatch&>(*root_batch.fields[2]);
        auto& row_id_batch = dynamic_cast<LongVectorBatch&>(*root_batch.fields[3]);
        auto& current_transaction_batch = dynamic_cast<LongVectorBatch&>(*root_batch.fields[4]);
        auto& row_batch = dynamic_cast<StructVectorBatch&>(*root_batch.fields[5]);
        auto& name_batch = dynamic_cast<StringVectorBatch&>(*row_batch.fields[0]);

        for (size_t i = 0; i < rows.size(); ++i) {
            operation_batch.data[i] = rows[i].operation;
            original_transaction_batch.data[i] = rows[i].original_transaction;
            bucket_batch.data[i] = rows[i].bucket;
            row_id_batch.data[i] = rows[i].row_id;
            current_transaction_batch.data[i] = rows[i].current_transaction;
            name_batch.data[i] = const_cast<char*>(rows[i].name.data());
            name_batch.length[i] = static_cast<int64_t>(rows[i].name.size());
        }
        root_batch.numElements = rows.size();
        row_batch.numElements = rows.size();
        name_batch.numElements = rows.size();
        writer->add(*batch);
        writer->close();

        std::filesystem::path file_path = std::filesystem::path(directory) / file_name;
        std::ofstream out(file_path, std::ios::binary | std::ios::trunc);
        out.write(mem_stream.getData(), static_cast<std::streamsize>(mem_stream.getLength()));
        out.close();
        EXPECT_TRUE(out.good());

        _temp_paths.push_back(directory);
        return {directory, file_name, file_path.string()};
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

    std::unique_ptr<orc::Reader> make_acid_orc_reader_with_name(const std::string& name_value) {
        using namespace orc;
        MemoryOutputStream mem_stream(1024 * 1024);
        auto type = createStructType();
        type->addStructField("operation", createPrimitiveType(orc::TypeKind::INT));
        type->addStructField("originalTransaction", createPrimitiveType(orc::TypeKind::LONG));
        type->addStructField("bucket", createPrimitiveType(orc::TypeKind::INT));
        type->addStructField("rowId", createPrimitiveType(orc::TypeKind::LONG));
        type->addStructField("currentTransaction", createPrimitiveType(orc::TypeKind::LONG));
        auto row_type = createStructType();
        row_type->addStructField("name", createPrimitiveType(orc::TypeKind::STRING));
        type->addStructField("row", std::move(row_type));

        WriterOptions options;
        options.setMemoryPool(getDefaultPool());
        auto writer = createWriter(*type, &mem_stream, options);
        auto batch = writer->createRowBatch(1);
        auto& root_batch = dynamic_cast<StructVectorBatch&>(*batch);
        auto& operation_batch = dynamic_cast<LongVectorBatch&>(*root_batch.fields[0]);
        auto& original_transaction_batch = dynamic_cast<LongVectorBatch&>(*root_batch.fields[1]);
        auto& bucket_batch = dynamic_cast<LongVectorBatch&>(*root_batch.fields[2]);
        auto& row_id_batch = dynamic_cast<LongVectorBatch&>(*root_batch.fields[3]);
        auto& current_transaction_batch = dynamic_cast<LongVectorBatch&>(*root_batch.fields[4]);
        auto& row_batch = dynamic_cast<StructVectorBatch&>(*root_batch.fields[5]);
        auto& name_batch = dynamic_cast<StringVectorBatch&>(*row_batch.fields[0]);

        operation_batch.data[0] = 0;
        original_transaction_batch.data[0] = 11;
        bucket_batch.data[0] = 7;
        row_id_batch.data[0] = 99;
        current_transaction_batch.data[0] = 11;
        name_batch.data[0] = const_cast<char*>(name_value.data());
        name_batch.length[0] = static_cast<int64_t>(name_value.size());

        root_batch.numElements = 1;
        row_batch.numElements = 1;
        name_batch.numElements = 1;
        writer->add(*batch);
        writer->close();

        auto in_stream =
                std::make_unique<MemoryInputStream>(mem_stream.getData(), mem_stream.getLength());
        ReaderOptions reader_options;
        reader_options.setMemoryPool(*getDefaultPool());
        return createReader(std::move(in_stream), reader_options);
    }

    void verify_global_rowid_column(const Block& block, const std::string& column_name,
                                    uint8_t expected_version, int64_t expected_backend_id,
                                    uint32_t expected_file_id) {
        const auto& column = block.get_by_position(block.get_position_by_name(column_name));
        const auto& string_column = static_cast<const ColumnString&>(*column.column);
        for (size_t i = 0; i < string_column.size(); ++i) {
            ASSERT_EQ(string_column.get_data_at(i).size, sizeof(GlobalRowLoacationV2));
            auto location = *reinterpret_cast<const GlobalRowLoacationV2*>(
                    string_column.get_data_at(i).data);
            EXPECT_EQ(location.version, expected_version);
            EXPECT_EQ(location.backend_id, expected_backend_id);
            EXPECT_EQ(location.file_id, expected_file_id);
            EXPECT_EQ(location.row_id, static_cast<uint32_t>(i));
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

    RuntimeProfile _profile {"transactional_hive_reader_test"};
    RuntimeState _runtime_state {(TQueryOptions()), TQueryGlobals()};
    std::vector<std::filesystem::path> _temp_paths;
};

TEST_F(TransactionalHiveReaderTest, expands_acid_columns_before_read) {
    TFileScanRangeParams params;
    TFileRangeDesc range;
    TransactionalHiveReader reader(&_profile, &_runtime_state, params, range, 1024, "CST", nullptr,
                                   nullptr);

    std::unordered_map<std::string, uint32_t> col_name_to_block_idx = {{"name", 0}};
    reader.col_name_to_block_idx_ref() = &col_name_to_block_idx;

    Block block = make_name_block();
    auto status = reader.on_before_read_block(&block);
    ASSERT_TRUE(status.ok()) << status;

    EXPECT_EQ(block.columns(), 1 + TransactionalHive::READ_PARAMS.size());
    EXPECT_EQ(block.get_by_position(0).name, "name");
    for (size_t i = 0; i < TransactionalHive::READ_PARAMS.size(); ++i) {
        const auto& param = TransactionalHive::READ_PARAMS[i];
        EXPECT_EQ(block.get_by_position(i + 1).name, param.column_lower_case);
        EXPECT_EQ(col_name_to_block_idx[param.column_lower_case], i + 1);
        EXPECT_NE(block.get_by_position(i + 1).column.get(), nullptr);
    }
}

TEST_F(TransactionalHiveReaderTest, shrinks_acid_columns_after_read) {
    TFileScanRangeParams params;
    TFileRangeDesc range;
    TransactionalHiveReader reader(&_profile, &_runtime_state, params, range, 1024, "CST", nullptr,
                                   nullptr);

    std::unordered_map<std::string, uint32_t> col_name_to_block_idx = {{"name", 0}};
    reader.col_name_to_block_idx_ref() = &col_name_to_block_idx;

    Block block = make_name_block();
    ASSERT_TRUE(reader.on_before_read_block(&block).ok());

    size_t read_rows = 0;
    auto status = reader.on_after_read_block(&block, &read_rows);
    ASSERT_TRUE(status.ok()) << status;

    EXPECT_EQ(block.columns(), 1);
    EXPECT_EQ(block.get_by_position(0).name, "name");
    EXPECT_EQ(col_name_to_block_idx.size(), 1);
    EXPECT_TRUE(col_name_to_block_idx.contains("name"));
    for (const auto& param : TransactionalHive::READ_PARAMS) {
        EXPECT_FALSE(col_name_to_block_idx.contains(param.column_lower_case));
    }
}

TEST_F(TransactionalHiveReaderTest, expand_and_shrink_is_repeatable) {
    TFileScanRangeParams params;
    TFileRangeDesc range;
    TransactionalHiveReader reader(&_profile, &_runtime_state, params, range, 1024, "CST", nullptr,
                                   nullptr);

    std::unordered_map<std::string, uint32_t> col_name_to_block_idx = {{"name", 0}};
    reader.col_name_to_block_idx_ref() = &col_name_to_block_idx;

    for (int i = 0; i < 2; ++i) {
        Block block = make_name_block();
        ASSERT_TRUE(reader.on_before_read_block(&block).ok());
        EXPECT_EQ(block.columns(), 1 + TransactionalHive::READ_PARAMS.size());

        size_t read_rows = 0;
        ASSERT_TRUE(reader.on_after_read_block(&block, &read_rows).ok());
        EXPECT_EQ(block.columns(), 1);
        EXPECT_EQ(col_name_to_block_idx.size(), 1);
        EXPECT_TRUE(col_name_to_block_idx.contains("name"));
    }
}

TEST_F(TransactionalHiveReaderTest, no_delete_delta_keeps_existing_pushdown) {
    TFileScanRangeParams params;
    TFileRangeDesc range;
    range.__isset.table_format_params = true;
    TransactionalHiveReader reader(&_profile, &_runtime_state, params, range, 1024, "CST", nullptr,
                                   nullptr);
    reader.set_push_down_agg_type(TPushAggOp::COUNT);

    auto status = reader.on_after_init_reader(nullptr);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(reader.get_push_down_agg_type(), TPushAggOp::COUNT);
    EXPECT_FALSE(reader.has_delete_operations());
}

TEST_F(TransactionalHiveReaderTest, unmatched_delete_delta_keeps_existing_pushdown) {
    TFileScanRangeParams params;
    TFileRangeDesc range;
    range.path = "/warehouse/tbl/base_0000001_v0000010/bucket_00001";
    range.__isset.table_format_params = true;

    TTransactionalHiveDeleteDeltaDesc delete_delta;
    delete_delta.__set_directory_location("/warehouse/tbl/delete_delta_0000002_0000002_0000");
    delete_delta.__set_file_names({"bucket_00002"});
    range.table_format_params.transactional_hive_params.__set_delete_deltas({delete_delta});

    TransactionalHiveReader reader(&_profile, &_runtime_state, params, range, 1024, "CST", nullptr,
                                   nullptr);
    reader.set_push_down_agg_type(TPushAggOp::COUNT);

    auto status = reader.on_after_init_reader(nullptr);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(reader.get_push_down_agg_type(), TPushAggOp::COUNT);
    EXPECT_FALSE(reader.has_delete_operations());
}

TEST_F(TransactionalHiveReaderTest, matched_delete_delta_normalizes_bucket_attempt_id) {
    TFileScanRangeParams params;
    params.__isset.hdfs_params = true;
    params.hdfs_params.__set_fs_name("hdfs://warehouse");

    TFileRangeDesc range;
    range.path = "/warehouse/tbl/base_0000001_v0000010/bucket_00001";
    range.__isset.table_format_params = true;

    TTransactionalHiveDeleteDeltaDesc delete_delta;
    delete_delta.__set_directory_location(
            "hdfs://warehouse/warehouse/tbl/delete_delta_0000002_0000002_0000");
    delete_delta.__set_file_names({"bucket_00001_0"});
    range.table_format_params.transactional_hive_params.__set_delete_deltas({delete_delta});

    TransactionalHiveReader reader(&_profile, &_runtime_state, params, range, 1024, "CST", nullptr,
                                   nullptr);
    auto status = reader.on_after_init_reader(nullptr);

    ASSERT_FALSE(status.ok());
}

TEST_F(TransactionalHiveReaderTest, matched_delete_delta_loads_delete_rows_and_disables_pushdown) {
    auto delete_file = write_delete_delta_orc_file({{11, 7, 99}, {12, 8, 100}}, "bucket_00001");

    TFileScanRangeParams params;
    params.file_type = TFileType::FILE_LOCAL;

    TFileRangeDesc range;
    range.path = "/warehouse/tbl/base_0000001_v0000010/bucket_00001";
    range.__isset.table_format_params = true;

    TTransactionalHiveDeleteDeltaDesc delete_delta;
    delete_delta.__set_directory_location(delete_file.directory);
    delete_delta.__set_file_names({delete_file.file_name});
    range.table_format_params.transactional_hive_params.__set_delete_deltas({delete_delta});

    TransactionalHiveReader reader(&_profile, &_runtime_state, params, range, 1024, "CST", nullptr,
                                   nullptr);
    reader.set_push_down_agg_type(TPushAggOp::COUNT);

    auto status = reader.on_after_init_reader(nullptr);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(reader.get_push_down_agg_type(), TPushAggOp::NONE);
    EXPECT_TRUE(reader.has_delete_operations());
}

TEST_F(TransactionalHiveReaderTest,
       matched_delete_delta_attempt_id_loads_delete_rows_and_disables_pushdown) {
    auto delete_file = write_delete_delta_orc_file({{21, 9, 101}}, "bucket_00001_0");

    TFileScanRangeParams params;
    params.file_type = TFileType::FILE_LOCAL;

    TFileRangeDesc range;
    range.path = "/warehouse/tbl/base_0000001_v0000010/bucket_00001";
    range.__isset.table_format_params = true;

    TTransactionalHiveDeleteDeltaDesc delete_delta;
    delete_delta.__set_directory_location(delete_file.directory);
    delete_delta.__set_file_names({delete_file.file_name});
    range.table_format_params.transactional_hive_params.__set_delete_deltas({delete_delta});

    TransactionalHiveReader reader(&_profile, &_runtime_state, params, range, 1024, "CST", nullptr,
                                   nullptr);
    reader.set_push_down_agg_type(TPushAggOp::COUNT);

    auto status = reader.on_after_init_reader(nullptr);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(reader.get_push_down_agg_type(), TPushAggOp::NONE);
    EXPECT_TRUE(reader.has_delete_operations());
}

TEST_F(TransactionalHiveReaderTest, reads_global_rowid_synthesized_column_from_orc_batches) {
    auto acid_file = write_acid_data_orc_file(
            {{0, 11, 7, 99, 11, "alice"}, {0, 12, 7, 100, 12, "bob"}}, "bucket_00001");

    TFileScanRangeParams params;
    params.file_type = TFileType::FILE_LOCAL;
    params.format_type = TFileFormatType::FORMAT_ORC;

    TFileRangeDesc range;
    range.path = acid_file.path;
    range.start_offset = 0;
    range.size = static_cast<int64_t>(std::filesystem::file_size(acid_file.path));
    range.__isset.table_format_params = true;

    TransactionalHiveReader reader(&_profile, &_runtime_state, params, range, 1024, "CST", nullptr,
                                   nullptr);
    constexpr uint8_t kVersion = 1;
    constexpr int64_t kBackendId = 10001;
    constexpr uint32_t kFileId = 23;
    reader.set_create_row_id_column_iterator_func([&]() {
        return std::make_shared<segment_v2::RowIdColumnIteratorV2>(kVersion, kBackendId, kFileId);
    });

    ObjectPool object_pool;
    auto nullable_string = make_nullable(std::make_shared<DataTypeString>());
    auto global_rowid_type = std::make_shared<DataTypeString>();
    std::string global_rowid_col_name = BeConsts::GLOBAL_ROWID_COL + "test";
    const auto* tuple_descriptor = build_tuple_descriptor(
            object_pool, {{"name", nullable_string}, {global_rowid_col_name, global_rowid_type}});
    auto column_descs = build_column_descs(tuple_descriptor,
                                           {ColumnCategory::REGULAR, ColumnCategory::SYNTHESIZED});
    std::unordered_map<std::string, uint32_t> col_name_to_block_idx = {{"name", 0},
                                                                       {global_rowid_col_name, 1}};

    OrcInitContext ctx;
    ctx.column_descs = &column_descs;
    ctx.col_name_to_block_idx = &col_name_to_block_idx;
    ctx.tuple_descriptor = tuple_descriptor;
    ctx.params = &params;
    ctx.range = &range;

    auto status = reader.init_reader(&ctx);
    ASSERT_TRUE(status.ok()) << status;

    Block block;
    block.insert(ColumnWithTypeAndName(nullable_string->create_column(), nullable_string, "name"));
    block.insert(ColumnWithTypeAndName(global_rowid_type->create_column(), global_rowid_type,
                                       global_rowid_col_name));

    size_t read_rows = 0;
    bool eof = false;
    status = reader.get_next_block(&block, &read_rows, &eof);
    ASSERT_TRUE(status.ok()) << status;

    ASSERT_EQ(read_rows, 2);
    EXPECT_EQ(block.columns(), 2);
    EXPECT_EQ(block.rows(), 2);
    const auto& name_column = block.get_by_position(block.get_position_by_name("name"));
    auto* nullable = check_and_get_column<ColumnNullable>(name_column.column.get());
    ASSERT_NE(nullable, nullptr);
    const auto& nested = static_cast<const ColumnString&>(nullable->get_nested_column());
    EXPECT_EQ(nested.get_data_at(0).to_string(), "alice");
    EXPECT_EQ(nested.get_data_at(1).to_string(), "bob");
    verify_global_rowid_column(block, global_rowid_col_name, kVersion, kBackendId, kFileId);
    EXPECT_FALSE(
            col_name_to_block_idx.contains(TransactionalHive::ORIGINAL_TRANSACTION_LOWER_CASE));
    EXPECT_FALSE(col_name_to_block_idx.contains(TransactionalHive::BUCKET_LOWER_CASE));
    EXPECT_FALSE(col_name_to_block_idx.contains(TransactionalHive::ROW_ID_LOWER_CASE));
}

TEST_F(TransactionalHiveReaderTest, fills_partition_and_missing_columns_during_acid_orc_reads) {
    auto acid_file = write_acid_data_orc_file(
            {{0, 11, 7, 99, 11, "alice"}, {0, 12, 7, 100, 12, "bob"}}, "bucket_00001");

    TFileScanRangeParams params;
    params.file_type = TFileType::FILE_LOCAL;
    params.format_type = TFileFormatType::FORMAT_ORC;

    TFileRangeDesc range;
    range.path = acid_file.path;
    range.start_offset = 0;
    range.size = static_cast<int64_t>(std::filesystem::file_size(acid_file.path));
    range.__isset.table_format_params = true;
    range.__set_columns_from_path_keys({"year"});
    range.__set_columns_from_path({"2024"});

    TransactionalHiveReader reader(&_profile, &_runtime_state, params, range, 1024, "CST", nullptr,
                                   nullptr);

    ObjectPool object_pool;
    auto nullable_string = make_nullable(std::make_shared<DataTypeString>());
    const auto* tuple_descriptor =
            build_tuple_descriptor(object_pool, {{"name", nullable_string},
                                                 {"year", nullable_string},
                                                 {"missing_col", nullable_string}});
    auto column_descs = build_column_descs(
            tuple_descriptor,
            {ColumnCategory::REGULAR, ColumnCategory::PARTITION_KEY, ColumnCategory::REGULAR});
    std::unordered_map<std::string, uint32_t> col_name_to_block_idx = {
            {"name", 0}, {"year", 1}, {"missing_col", 2}};

    OrcInitContext ctx;
    ctx.column_descs = &column_descs;
    ctx.col_name_to_block_idx = &col_name_to_block_idx;
    ctx.tuple_descriptor = tuple_descriptor;
    ctx.params = &params;
    ctx.range = &range;

    auto status = reader.init_reader(&ctx);
    ASSERT_TRUE(status.ok()) << status;

    Block block;
    block.insert(ColumnWithTypeAndName(nullable_string->create_column(), nullable_string, "name"));
    block.insert(ColumnWithTypeAndName(nullable_string->create_column(), nullable_string, "year"));
    block.insert(ColumnWithTypeAndName(nullable_string->create_column(), nullable_string,
                                       "missing_col"));

    size_t read_rows = 0;
    bool eof = false;
    status = reader.get_next_block(&block, &read_rows, &eof);
    ASSERT_TRUE(status.ok()) << status;

    ASSERT_EQ(read_rows, 2);
    EXPECT_EQ(block.columns(), 3);
    EXPECT_EQ(block.rows(), 2);
    verify_nullable_string_column_values(block, "name", {"alice", "bob"});
    verify_nullable_string_column_values(block, "year", {"2024", "2024"});
    verify_nullable_string_column_all_null(block, "missing_col", read_rows);
    EXPECT_FALSE(
            col_name_to_block_idx.contains(TransactionalHive::ORIGINAL_TRANSACTION_LOWER_CASE));
    EXPECT_FALSE(col_name_to_block_idx.contains(TransactionalHive::BUCKET_LOWER_CASE));
    EXPECT_FALSE(col_name_to_block_idx.contains(TransactionalHive::ROW_ID_LOWER_CASE));
}

TEST_F(TransactionalHiveReaderTest, on_before_init_reader_maps_row_columns_and_missing_columns) {
    TFileScanRangeParams params;
    TFileRangeDesc range;
    range.__isset.table_format_params = true;
    TransactionalHiveReader reader(&_profile, &_runtime_state, params, range, 1024, "CST", nullptr,
                                   nullptr);
    reader._reader = make_acid_orc_reader_with_name("alice");

    ObjectPool object_pool;
    auto nullable_string = make_nullable(std::make_shared<DataTypeString>());
    const auto* tuple_descriptor = build_tuple_descriptor(
            object_pool, {{"name", nullable_string}, {"missing_col", nullable_string}});
    auto column_descs = build_column_descs(tuple_descriptor,
                                           {ColumnCategory::REGULAR, ColumnCategory::REGULAR});
    std::unordered_map<std::string, uint32_t> col_name_to_block_idx = {{"name", 0},
                                                                       {"missing_col", 1}};

    OrcInitContext ctx;
    ctx.column_descs = &column_descs;
    ctx.col_name_to_block_idx = &col_name_to_block_idx;
    ctx.tuple_descriptor = tuple_descriptor;
    ctx.range = &range;

    auto status = reader.on_before_init_reader(&ctx);
    ASSERT_TRUE(status.ok()) << status;

    EXPECT_EQ(ctx.column_names.size(), 5);
    EXPECT_EQ(ctx.column_names[0], "name");
    EXPECT_EQ(ctx.column_names[1], "missing_col");
    EXPECT_EQ(ctx.column_names[2], TransactionalHive::ORIGINAL_TRANSACTION_LOWER_CASE);
    EXPECT_EQ(ctx.column_names[3], TransactionalHive::BUCKET_LOWER_CASE);
    EXPECT_EQ(ctx.column_names[4], TransactionalHive::ROW_ID_LOWER_CASE);
    ASSERT_NE(ctx.table_info_node, nullptr);
    EXPECT_TRUE(ctx.table_info_node->children_column_exists("name"));
    EXPECT_EQ(ctx.table_info_node->children_file_column_name("name"), "row.name");
    EXPECT_FALSE(ctx.table_info_node->children_column_exists("missing_col"));
}

TEST_F(TransactionalHiveReaderTest, on_before_init_reader_rejects_acid_metadata_conflict) {
    TFileScanRangeParams params;
    TFileRangeDesc range;
    range.__isset.table_format_params = true;
    TransactionalHiveReader reader(&_profile, &_runtime_state, params, range, 1024, "CST", nullptr,
                                   nullptr);
    reader._reader = make_acid_orc_reader_with_name("alice");

    ObjectPool object_pool;
    auto nullable_string = make_nullable(std::make_shared<DataTypeString>());
    const auto* tuple_descriptor =
            build_tuple_descriptor(object_pool, {{"bucket", nullable_string}});
    auto column_descs = build_column_descs(tuple_descriptor, {ColumnCategory::REGULAR});
    std::unordered_map<std::string, uint32_t> col_name_to_block_idx = {{"bucket", 0}};

    OrcInitContext ctx;
    ctx.column_descs = &column_descs;
    ctx.col_name_to_block_idx = &col_name_to_block_idx;
    ctx.tuple_descriptor = tuple_descriptor;
    ctx.range = &range;

    auto status = reader.on_before_init_reader(&ctx);
    ASSERT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("conflicts with ACID metadata column"), std::string::npos);
}

} // namespace doris
