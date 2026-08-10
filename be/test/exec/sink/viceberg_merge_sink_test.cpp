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

#include "exec/sink/viceberg_merge_sink.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>

#include "agent/be_exec_version_manager.h"
#include "common/config.h"
#include "common/consts.h"
#include "common/object_pool.h"
#include "core/block/block.h"
#include "core/column/column_string.h"
#include "core/column/column_struct.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "exec/sink/sink_common.h"
#include "exec/sink/viceberg_delete_sink.h"
#include "exec/sink/writer/iceberg/viceberg_table_writer.h"
#include "exprs/vexpr_context.h"
#include "gen_cpp/DataSinks_types.h"
#include "gen_cpp/Types_types.h"
#include "io/fs/local_file_system.h"
#include "runtime/runtime_profile.h"
#include "testutil/mock/mock_descriptors.h"
#include "testutil/mock/mock_runtime_state.h"
#include "testutil/mock/mock_slot_ref.h"
#include "util/debug_points.h"

namespace doris {

class VIcebergMergeSinkTest : public testing::Test {
protected:
    static std::string test_schema_json() {
        return "{\"type\":\"struct\",\"schema-id\":0,\"fields\":["
               "{\"id\":1,\"name\":\"id\",\"required\":false,\"type\":\"int\"},"
               "{\"id\":2,\"name\":\"name\",\"required\":false,\"type\":\"string\"}"
               "]}";
    }

    TDataSink build_sink(bool require_cardinality_check = true, bool set_cardinality_check = true) {
        TDataSink t_sink;
        t_sink.__set_type(TDataSinkType::ICEBERG_MERGE_SINK);

        TIcebergMergeSink merge_sink;
        merge_sink.__set_db_name("test_db");
        merge_sink.__set_tb_name("test_table");
        merge_sink.__set_schema_json(test_schema_json());
        merge_sink.__set_partition_spec_id(0);
        merge_sink.__set_file_format(TFileFormatType::FORMAT_PARQUET);
        merge_sink.__set_compression_type(TFileCompressType::SNAPPYBLOCK);
        merge_sink.__set_output_path("file:///tmp/iceberg_merge_sink");
        merge_sink.__set_original_output_path("file:///tmp/iceberg_merge_sink");
        merge_sink.__set_table_location("file:///tmp/iceberg_merge_sink");
        merge_sink.__set_file_type(TFileType::FILE_LOCAL);
        merge_sink.__set_delete_type(TFileContent::POSITION_DELETES);
        merge_sink.__set_partition_spec_id_for_delete(0);
        if (set_cardinality_check) {
            merge_sink.__set_require_merge_cardinality_check(require_cardinality_check);
        }

        t_sink.__set_iceberg_merge_sink(merge_sink);
        return t_sink;
    }

    VExprContextSPtrs build_output_exprs(ObjectPool* pool, doris::RuntimeState* state,
                                         const doris::RowDescriptor& row_desc,
                                         bool include_operation = true,
                                         bool include_row_id = true) {
        VExprContextSPtrs output_exprs;

        if (include_operation) {
            auto op_expr = std::make_shared<MockSlotRef>(0, std::make_shared<DataTypeInt8>());
            op_expr->set_expr_name("operation");
            output_exprs.emplace_back(VExprContext::create_shared(op_expr));
        }

        if (include_row_id) {
            auto row_id_expr = std::make_shared<MockSlotRef>(
                    1,
                    std::make_shared<DataTypeStruct>(DataTypes {std::make_shared<DataTypeString>(),
                                                                std::make_shared<DataTypeInt64>()},
                                                     Strings {"file_path", "row_position"}));
            row_id_expr->set_expr_name(doris::BeConsts::ICEBERG_ROWID_COL);
            output_exprs.emplace_back(VExprContext::create_shared(row_id_expr));
        }

        auto id_expr = std::make_shared<MockSlotRef>(2, std::make_shared<DataTypeInt32>());
        id_expr->set_expr_name("id");
        output_exprs.emplace_back(VExprContext::create_shared(id_expr));

        auto name_expr = std::make_shared<MockSlotRef>(3, std::make_shared<DataTypeString>());
        name_expr->set_expr_name("name");
        output_exprs.emplace_back(VExprContext::create_shared(name_expr));

        for (auto& ctx : output_exprs) {
            EXPECT_TRUE(ctx->prepare(state, row_desc).ok());
            EXPECT_TRUE(ctx->open(state).ok());
        }
        return output_exprs;
    }

    Block build_block_with_ops(const std::vector<int8_t>& ops, bool distinct_files = true,
                               size_t file_index_offset = 0) {
        Block block;

        auto op_col = ColumnInt8::create();
        for (auto op : ops) {
            op_col->insert_value(op);
        }
        block.insert(ColumnWithTypeAndName(std::move(op_col), std::make_shared<DataTypeInt8>(),
                                           "operation"));

        auto file_path_col = ColumnString::create();
        auto row_pos_col = ColumnInt64::create();
        auto id_col = ColumnInt32::create();
        auto name_col = ColumnString::create();
        for (size_t i = 0; i < ops.size(); ++i) {
            std::string file_path =
                    distinct_files ? "file" + std::to_string(file_index_offset + i + 1) + ".parquet"
                                   : "shared-file.parquet";
            file_path_col->insert_data(file_path.data(), file_path.size());
            row_pos_col->insert_value(static_cast<int64_t>((file_index_offset + i + 1) * 10));
            id_col->insert_value(static_cast<int32_t>(i + 1));
            char name_value = static_cast<char>('a' + i);
            name_col->insert_data(&name_value, 1);
        }

        Columns struct_cols;
        struct_cols.push_back(std::move(file_path_col));
        struct_cols.push_back(std::move(row_pos_col));

        auto struct_col = ColumnStruct::create(std::move(struct_cols));
        DataTypes struct_types;
        struct_types.push_back(std::make_shared<DataTypeString>());
        struct_types.push_back(std::make_shared<DataTypeInt64>());
        Strings field_names = {"file_path", "row_position"};
        auto struct_type = std::make_shared<DataTypeStruct>(struct_types, field_names);

        block.insert(ColumnWithTypeAndName(std::move(struct_col), struct_type,
                                           doris::BeConsts::ICEBERG_ROWID_COL));

        block.insert(
                ColumnWithTypeAndName(std::move(id_col), std::make_shared<DataTypeInt32>(), "id"));

        block.insert(ColumnWithTypeAndName(std::move(name_col), std::make_shared<DataTypeString>(),
                                           "name"));

        return block;
    }

    Block build_block() { return build_block_with_ops({3, 2, 1}); }
};

TEST_F(VIcebergMergeSinkTest, TestUpdateProducesDeleteAndInsert) {
    ObjectPool pool;
    MockRuntimeState state;

    DataTypes types {std::make_shared<DataTypeInt8>(),
                     std::make_shared<DataTypeStruct>(DataTypes {std::make_shared<DataTypeString>(),
                                                                 std::make_shared<DataTypeInt64>()},
                                                      Strings {"file_path", "row_position"}),
                     std::make_shared<DataTypeInt32>(), std::make_shared<DataTypeString>()};
    MockRowDescriptor row_desc(types, &pool);

    auto output_exprs = build_output_exprs(&pool, &state, row_desc);
    TDataSink t_sink = build_sink();

    auto sink = std::make_shared<VIcebergMergeSink>(t_sink, output_exprs, nullptr, nullptr);
    sink->set_skip_io(true);

    ASSERT_TRUE(sink->init_properties(&pool, row_desc).ok());
    RuntimeProfile profile("iceberg_merge_sink");
    ASSERT_TRUE(sink->open(&state, &profile).ok());

    Block block = build_block();
    ASSERT_TRUE(sink->write(&state, block).ok());

    EXPECT_EQ(2, sink->_delete_row_count);
    EXPECT_EQ(2, sink->_insert_row_count);

    ASSERT_TRUE(sink->close(Status::OK()).ok());
}

TEST_F(VIcebergMergeSinkTest, TestDeleteOnlySkipsVariantDataWriter) {
    ObjectPool pool;
    MockRuntimeState state;

    DataTypes types {std::make_shared<DataTypeInt8>(),
                     std::make_shared<DataTypeStruct>(DataTypes {std::make_shared<DataTypeString>(),
                                                                 std::make_shared<DataTypeInt64>()},
                                                      Strings {"file_path", "row_position"}),
                     std::make_shared<DataTypeInt32>(), std::make_shared<DataTypeString>()};
    MockRowDescriptor row_desc(types, &pool);

    auto output_exprs = build_output_exprs(&pool, &state, row_desc);
    TDataSink t_sink = build_sink();
    t_sink.iceberg_merge_sink.__set_writes_data_files(false);
    t_sink.iceberg_merge_sink.__set_schema_json(
            "{\"type\":\"struct\",\"schema-id\":0,\"fields\":["
            "{\"id\":1,\"name\":\"payload\",\"required\":false,\"type\":\"variant\"}"
            "]}");

    auto sink = std::make_shared<VIcebergMergeSink>(t_sink, output_exprs, nullptr, nullptr);
    sink->set_skip_io(true);

    ASSERT_TRUE(sink->init_properties(&pool, row_desc).ok());
    EXPECT_EQ(nullptr, sink->_table_writer);
    RuntimeProfile profile("iceberg_merge_sink");
    ASSERT_TRUE(sink->open(&state, &profile).ok());

    // Delete-only plans must never use the insert opcode, which intentionally requires a data writer.
    Block block = build_block_with_ops({kDeleteOperation});
    Status status = sink->write(&state, block);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(1, sink->_delete_row_count);
    EXPECT_EQ(0, sink->_insert_row_count);

    ASSERT_TRUE(sink->close(Status::OK()).ok());
}

TEST_F(VIcebergMergeSinkTest, TestMissingOperationColumn) {
    ObjectPool pool;
    MockRuntimeState state;

    DataTypes types {std::make_shared<DataTypeInt8>(),
                     std::make_shared<DataTypeStruct>(DataTypes {std::make_shared<DataTypeString>(),
                                                                 std::make_shared<DataTypeInt64>()},
                                                      Strings {"file_path", "row_position"}),
                     std::make_shared<DataTypeInt32>(), std::make_shared<DataTypeString>()};
    MockRowDescriptor row_desc(types, &pool);

    auto output_exprs = build_output_exprs(&pool, &state, row_desc, false, true);
    TDataSink t_sink = build_sink();

    auto sink = std::make_shared<VIcebergMergeSink>(t_sink, output_exprs, nullptr, nullptr);

    ASSERT_TRUE(sink->init_properties(&pool, row_desc).ok());
    RuntimeProfile profile("iceberg_merge_sink");
    Status status = sink->open(&state, &profile);
    ASSERT_FALSE(status.ok());
    ASSERT_NE(std::string::npos, status.to_string().find("missing operation column"));
}

TEST_F(VIcebergMergeSinkTest, TestMissingRowIdColumn) {
    ObjectPool pool;
    MockRuntimeState state;

    DataTypes types {std::make_shared<DataTypeInt8>(),
                     std::make_shared<DataTypeStruct>(DataTypes {std::make_shared<DataTypeString>(),
                                                                 std::make_shared<DataTypeInt64>()},
                                                      Strings {"file_path", "row_position"}),
                     std::make_shared<DataTypeInt32>(), std::make_shared<DataTypeString>()};
    MockRowDescriptor row_desc(types, &pool);

    auto output_exprs = build_output_exprs(&pool, &state, row_desc, true, false);
    TDataSink t_sink = build_sink();

    auto sink = std::make_shared<VIcebergMergeSink>(t_sink, output_exprs, nullptr, nullptr);

    ASSERT_TRUE(sink->init_properties(&pool, row_desc).ok());
    RuntimeProfile profile("iceberg_merge_sink");
    Status status = sink->open(&state, &profile);
    ASSERT_FALSE(status.ok());
    ASSERT_NE(std::string::npos, status.to_string().find("missing row_id column"));
}

TEST_F(VIcebergMergeSinkTest, TestUnknownOperation) {
    ObjectPool pool;
    MockRuntimeState state;

    DataTypes types {std::make_shared<DataTypeInt8>(),
                     std::make_shared<DataTypeStruct>(DataTypes {std::make_shared<DataTypeString>(),
                                                                 std::make_shared<DataTypeInt64>()},
                                                      Strings {"file_path", "row_position"}),
                     std::make_shared<DataTypeInt32>(), std::make_shared<DataTypeString>()};
    MockRowDescriptor row_desc(types, &pool);

    auto output_exprs = build_output_exprs(&pool, &state, row_desc);
    TDataSink t_sink = build_sink();

    auto sink = std::make_shared<VIcebergMergeSink>(t_sink, output_exprs, nullptr, nullptr);
    sink->set_skip_io(true);

    ASSERT_TRUE(sink->init_properties(&pool, row_desc).ok());
    RuntimeProfile profile("iceberg_merge_sink");
    ASSERT_TRUE(sink->open(&state, &profile).ok());

    Block block = build_block_with_ops({9});
    Status status = sink->write(&state, block);
    ASSERT_FALSE(status.ok());
    ASSERT_NE(std::string::npos, status.to_string().find("Unknown Iceberg merge operation"));
}

TEST_F(VIcebergMergeSinkTest, TestUpdateInsertAndDeleteOperations) {
    ObjectPool pool;
    MockRuntimeState state;

    DataTypes types {std::make_shared<DataTypeInt8>(),
                     std::make_shared<DataTypeStruct>(DataTypes {std::make_shared<DataTypeString>(),
                                                                 std::make_shared<DataTypeInt64>()},
                                                      Strings {"file_path", "row_position"}),
                     std::make_shared<DataTypeInt32>(), std::make_shared<DataTypeString>()};
    MockRowDescriptor row_desc(types, &pool);

    auto output_exprs = build_output_exprs(&pool, &state, row_desc);
    TDataSink t_sink = build_sink();

    auto sink = std::make_shared<VIcebergMergeSink>(t_sink, output_exprs, nullptr, nullptr);
    sink->set_skip_io(true);

    ASSERT_TRUE(sink->init_properties(&pool, row_desc).ok());
    RuntimeProfile profile("iceberg_merge_sink");
    ASSERT_TRUE(sink->open(&state, &profile).ok());

    Block block = build_block_with_ops({4, 5});
    ASSERT_TRUE(sink->write(&state, block).ok());

    EXPECT_EQ(1, sink->_delete_row_count);
    EXPECT_EQ(1, sink->_insert_row_count);

    ASSERT_TRUE(sink->close(Status::OK()).ok());
}

TEST_F(VIcebergMergeSinkTest, TestSchemaMismatch) {
    ObjectPool pool;
    MockRuntimeState state;

    DataTypes types {std::make_shared<DataTypeInt8>(),
                     std::make_shared<DataTypeStruct>(DataTypes {std::make_shared<DataTypeString>(),
                                                                 std::make_shared<DataTypeInt64>()},
                                                      Strings {"file_path", "row_position"}),
                     std::make_shared<DataTypeInt32>(), std::make_shared<DataTypeString>()};
    MockRowDescriptor row_desc(types, &pool);

    auto output_exprs = build_output_exprs(&pool, &state, row_desc);
    TDataSink t_sink = build_sink();
    t_sink.iceberg_merge_sink.__set_schema_json(
            "{\"type\":\"struct\",\"schema-id\":0,\"fields\":["
            "{\"id\":1,\"name\":\"id\",\"required\":false,\"type\":\"int\"}"
            "]}");

    auto sink = std::make_shared<VIcebergMergeSink>(t_sink, output_exprs, nullptr, nullptr);

    ASSERT_TRUE(sink->init_properties(&pool, row_desc).ok());
    RuntimeProfile profile("iceberg_merge_sink");
    Status status = sink->open(&state, &profile);
    ASSERT_FALSE(status.ok());
    ASSERT_NE(std::string::npos, status.to_string().find("do not match schema columns"));
}

TEST_F(VIcebergMergeSinkTest, TestRejectsDuplicateMatchedTargetAcrossBlocks) {
    ObjectPool pool;
    MockRuntimeState state;

    DataTypes types {std::make_shared<DataTypeInt8>(),
                     std::make_shared<DataTypeStruct>(DataTypes {std::make_shared<DataTypeString>(),
                                                                 std::make_shared<DataTypeInt64>()},
                                                      Strings {"file_path", "row_position"}),
                     std::make_shared<DataTypeInt32>(), std::make_shared<DataTypeString>()};
    MockRowDescriptor row_desc(types, &pool);

    auto output_exprs = build_output_exprs(&pool, &state, row_desc);
    auto sink = std::make_shared<VIcebergMergeSink>(build_sink(), output_exprs, nullptr, nullptr);
    sink->set_skip_io(true);

    ASSERT_TRUE(sink->init_properties(&pool, row_desc).ok());
    RuntimeProfile profile("iceberg_merge_sink");
    ASSERT_TRUE(sink->open(&state, &profile).ok());

    Block first = build_block_with_ops({3});
    ASSERT_TRUE(sink->write(&state, first).ok());
    Block duplicate = build_block_with_ops({3});
    Status status = sink->write(&state, duplicate);

    ASSERT_FALSE(status.ok());
    ASSERT_NE(std::string::npos,
              status.to_string().find("multiple source rows matched the same target row"));
}

TEST_F(VIcebergMergeSinkTest, TestUpdateSkipsCardinalityState) {
    ObjectPool pool;
    MockRuntimeState state;

    DataTypes types {std::make_shared<DataTypeInt8>(),
                     std::make_shared<DataTypeStruct>(DataTypes {std::make_shared<DataTypeString>(),
                                                                 std::make_shared<DataTypeInt64>()},
                                                      Strings {"file_path", "row_position"}),
                     std::make_shared<DataTypeInt32>(), std::make_shared<DataTypeString>()};
    MockRowDescriptor row_desc(types, &pool);

    auto output_exprs = build_output_exprs(&pool, &state, row_desc);
    auto sink =
            std::make_shared<VIcebergMergeSink>(build_sink(false), output_exprs, nullptr, nullptr);
    sink->set_skip_io(true);

    ASSERT_TRUE(sink->init_properties(&pool, row_desc).ok());
    RuntimeProfile profile("iceberg_update_sink");
    ASSERT_TRUE(sink->open(&state, &profile).ok());

    Block first = build_block_with_ops({3});
    ASSERT_TRUE(sink->write(&state, first).ok());
    Block duplicate = build_block_with_ops({3});
    ASSERT_TRUE(sink->write(&state, duplicate).ok());
    EXPECT_TRUE(sink->_matched_row_positions.empty());
    EXPECT_EQ(nullptr, profile.get_counter("MatchedRowIdStateBytes"));
}

TEST_F(VIcebergMergeSinkTest, TestOldFePlanSkipsCardinalityState) {
    ObjectPool pool;
    MockRuntimeState state;

    DataTypes types {std::make_shared<DataTypeInt8>(),
                     std::make_shared<DataTypeStruct>(DataTypes {std::make_shared<DataTypeString>(),
                                                                 std::make_shared<DataTypeInt64>()},
                                                      Strings {"file_path", "row_position"}),
                     std::make_shared<DataTypeInt32>(), std::make_shared<DataTypeString>()};
    MockRowDescriptor row_desc(types, &pool);

    auto output_exprs = build_output_exprs(&pool, &state, row_desc);
    auto sink = std::make_shared<VIcebergMergeSink>(build_sink(false, false), output_exprs, nullptr,
                                                    nullptr);
    sink->set_skip_io(true);

    ASSERT_TRUE(sink->init_properties(&pool, row_desc).ok());
    RuntimeProfile profile("old_fe_iceberg_update_sink");
    ASSERT_TRUE(sink->open(&state, &profile).ok());
    Block duplicate = build_block_with_ops({3, 3}, false);
    ASSERT_TRUE(sink->write(&state, duplicate).ok());
    EXPECT_TRUE(sink->_matched_row_positions.empty());
}

TEST_F(VIcebergMergeSinkTest, TestRollingUpgradeSkipsCardinalityState) {
    ObjectPool pool;
    MockRuntimeState state;
    state.set_be_exec_version(SUPPORT_ICEBERG_MERGE_CARDINALITY_VERSION - 1);

    DataTypes types {std::make_shared<DataTypeInt8>(),
                     std::make_shared<DataTypeStruct>(DataTypes {std::make_shared<DataTypeString>(),
                                                                 std::make_shared<DataTypeInt64>()},
                                                      Strings {"file_path", "row_position"}),
                     std::make_shared<DataTypeInt32>(), std::make_shared<DataTypeString>()};
    MockRowDescriptor row_desc(types, &pool);

    auto output_exprs = build_output_exprs(&pool, &state, row_desc);
    auto sink = std::make_shared<VIcebergMergeSink>(build_sink(), output_exprs, nullptr, nullptr);
    sink->set_skip_io(true);

    ASSERT_TRUE(sink->init_properties(&pool, row_desc).ok());
    RuntimeProfile profile("rolling_upgrade_iceberg_merge_sink");
    ASSERT_TRUE(sink->open(&state, &profile).ok());
    Block duplicate = build_block_with_ops({3, 3}, false);
    ASSERT_TRUE(sink->write(&state, duplicate).ok());
    EXPECT_TRUE(sink->_matched_row_positions.empty());
}

TEST_F(VIcebergMergeSinkTest, TestErrorCloseRemovesRolledDataFiles) {
    ObjectPool pool;
    MockRuntimeState state;

    DataTypes types {std::make_shared<DataTypeInt8>(),
                     std::make_shared<DataTypeStruct>(DataTypes {std::make_shared<DataTypeString>(),
                                                                 std::make_shared<DataTypeInt64>()},
                                                      Strings {"file_path", "row_position"}),
                     std::make_shared<DataTypeInt32>(), std::make_shared<DataTypeString>()};
    MockRowDescriptor row_desc(types, &pool);
    auto output_exprs = build_output_exprs(&pool, &state, row_desc);
    auto sink = std::make_shared<VIcebergMergeSink>(build_sink(), output_exprs, nullptr, nullptr);
    sink->set_skip_io(true);

    ASSERT_TRUE(sink->init_properties(&pool, row_desc).ok());
    RuntimeProfile profile("iceberg_merge_cleanup_sink");
    ASSERT_TRUE(sink->open(&state, &profile).ok());

    std::filesystem::path path =
            std::filesystem::temp_directory_path() / "doris_iceberg_merge_rolled_file.parquet";
    {
        std::ofstream output(path);
        output << "rolled-data";
    }
    ASSERT_TRUE(std::filesystem::exists(path));
    sink->_table_writer->_closed_files.emplace_back(io::global_local_filesystem(), path.string());

    Status failure = Status::InvalidArgument("late duplicate");
    EXPECT_FALSE(sink->close(failure).ok());
    EXPECT_FALSE(std::filesystem::exists(path));
}

TEST_F(VIcebergMergeSinkTest, TestDeleteCloseFailureRemovesBothInnerSinkFiles) {
    ObjectPool pool;
    MockRuntimeState state;

    DataTypes types {std::make_shared<DataTypeInt8>(),
                     std::make_shared<DataTypeStruct>(DataTypes {std::make_shared<DataTypeString>(),
                                                                 std::make_shared<DataTypeInt64>()},
                                                      Strings {"file_path", "row_position"}),
                     std::make_shared<DataTypeInt32>(), std::make_shared<DataTypeString>()};
    MockRowDescriptor row_desc(types, &pool);
    auto output_exprs = build_output_exprs(&pool, &state, row_desc);
    auto sink = std::make_shared<VIcebergMergeSink>(build_sink(), output_exprs, nullptr, nullptr);
    sink->set_skip_io(true);

    ASSERT_TRUE(sink->init_properties(&pool, row_desc).ok());
    RuntimeProfile profile("iceberg_merge_delete_close_cleanup_sink");
    ASSERT_TRUE(sink->open(&state, &profile).ok());

    std::filesystem::path data_path = std::filesystem::temp_directory_path() /
                                      "doris_iceberg_merge_delete_close_data.parquet";
    std::filesystem::path delete_path = std::filesystem::temp_directory_path() /
                                        "doris_iceberg_merge_delete_close_position.parquet";
    {
        std::ofstream output(data_path);
        output << "closed-data";
    }
    {
        std::ofstream output(delete_path);
        output << "closed-delete";
    }
    ASSERT_TRUE(std::filesystem::exists(data_path));
    ASSERT_TRUE(std::filesystem::exists(delete_path));
    sink->_table_writer->_closed_files.emplace_back(io::global_local_filesystem(),
                                                    data_path.string());
    sink->_delete_writer->_created_files.emplace_back(io::global_local_filesystem(),
                                                      delete_path.string());

    bool previous_enable_debug_points = config::enable_debug_points;
    config::enable_debug_points = true;
    DebugPoints::instance()->add("VIcebergDeleteSink.close.inject_failure");
    Status status = sink->close(Status::OK());
    DebugPoints::instance()->clear();
    config::enable_debug_points = previous_enable_debug_points;

    EXPECT_FALSE(status.ok());
    EXPECT_NE(std::string::npos, status.to_string().find("injected Iceberg delete close failure"));
    EXPECT_FALSE(std::filesystem::exists(data_path));
    EXPECT_FALSE(std::filesystem::exists(delete_path));
}

TEST_F(VIcebergMergeSinkTest, TestMatchedRowIdsUseCompactRetainedState) {
    ObjectPool pool;
    MockRuntimeState state;

    DataTypes types {std::make_shared<DataTypeInt8>(),
                     std::make_shared<DataTypeStruct>(DataTypes {std::make_shared<DataTypeString>(),
                                                                 std::make_shared<DataTypeInt64>()},
                                                      Strings {"file_path", "row_position"}),
                     std::make_shared<DataTypeInt32>(), std::make_shared<DataTypeString>()};
    MockRowDescriptor row_desc(types, &pool);

    auto output_exprs = build_output_exprs(&pool, &state, row_desc);
    auto sink = std::make_shared<VIcebergMergeSink>(build_sink(), output_exprs, nullptr, nullptr);
    sink->set_skip_io(true);

    ASSERT_TRUE(sink->init_properties(&pool, row_desc).ok());
    RuntimeProfile profile("iceberg_merge_sink");
    ASSERT_TRUE(sink->open(&state, &profile).ok());

    constexpr size_t row_count = 100000;
    std::vector<int8_t> operations(row_count, 3);
    Block block = build_block_with_ops(operations, false);
    ASSERT_TRUE(sink->write(&state, block).ok());

    auto* retained_bytes = profile.get_counter("MatchedRowIdStateBytes");
    ASSERT_NE(nullptr, retained_bytes);
    EXPECT_LT(retained_bytes->value(), static_cast<int64_t>(row_count * sizeof(int64_t)));
}

TEST_F(VIcebergMergeSinkTest, TestMatchedRowIdStateAcrossManyFilesAndWrites) {
    ObjectPool pool;
    MockRuntimeState state;

    DataTypes types {std::make_shared<DataTypeInt8>(),
                     std::make_shared<DataTypeStruct>(DataTypes {std::make_shared<DataTypeString>(),
                                                                 std::make_shared<DataTypeInt64>()},
                                                      Strings {"file_path", "row_position"}),
                     std::make_shared<DataTypeInt32>(), std::make_shared<DataTypeString>()};
    MockRowDescriptor row_desc(types, &pool);

    auto output_exprs = build_output_exprs(&pool, &state, row_desc);
    auto sink = std::make_shared<VIcebergMergeSink>(build_sink(), output_exprs, nullptr, nullptr);
    sink->set_skip_io(true);

    ASSERT_TRUE(sink->init_properties(&pool, row_desc).ok());
    RuntimeProfile profile("iceberg_merge_sink");
    ASSERT_TRUE(sink->open(&state, &profile).ok());

    auto* retained_bytes = profile.get_counter("MatchedRowIdStateBytes");
    ASSERT_NE(nullptr, retained_bytes);
    constexpr size_t files_per_write = 32;
    constexpr size_t write_count = 64;
    std::vector<int8_t> operations(files_per_write, 3);
    int64_t previous_bytes = 0;
    for (size_t write_index = 0; write_index < write_count; ++write_index) {
        Block block = build_block_with_ops(operations, true, write_index * files_per_write);
        ASSERT_TRUE(sink->write(&state, block).ok());
        EXPECT_GT(retained_bytes->value(), previous_bytes);
        previous_bytes = retained_bytes->value();
    }

    EXPECT_EQ(files_per_write * write_count, sink->_matched_row_positions.size());
    EXPECT_EQ(static_cast<int64_t>(sink->_matched_row_id_state_size), retained_bytes->value());
}

} // namespace doris
