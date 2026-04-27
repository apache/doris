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
#define private public
#include "vec/sink/vpaimon_table_writer.h"

#include "vec/sink/writer/paimon/vpaimon_partition_writer.h"
#undef private
#pragma clang diagnostic pop

#include <gtest/gtest.h>

#include "common/object_pool.h"
#include "core/block/block.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "exprs/vexpr_context.h"
#include "runtime/runtime_profile.h"
#include "testutil/mock/mock_descriptors.h"
#include "testutil/mock/mock_runtime_state.h"
#include "testutil/mock/mock_slot_ref.h"

namespace doris::vectorized {

class VPaimonTableWriterTest : public testing::Test {
protected:
    VExprContextSPtrs build_output_exprs(ObjectPool* pool, doris::RuntimeState* state,
                                         const doris::RowDescriptor& row_desc) {
        VExprContextSPtrs output_exprs;

        auto id_expr = std::make_shared<MockSlotRef>(0, std::make_shared<DataTypeInt32>());
        id_expr->set_expr_name("id");
        output_exprs.emplace_back(VExprContext::create_shared(id_expr));

        auto pt_expr = std::make_shared<MockSlotRef>(1, std::make_shared<DataTypeString>());
        pt_expr->set_expr_name("pt");
        output_exprs.emplace_back(VExprContext::create_shared(pt_expr));

        for (auto& ctx : output_exprs) {
            EXPECT_TRUE(ctx->prepare(state, row_desc).ok());
            EXPECT_TRUE(ctx->open(state).ok());
        }
        return output_exprs;
    }

    void init_writer_context(VPaimonTableWriter* writer, RuntimeState* state,
                             RuntimeProfile* profile) {
        writer->_state = state;
        writer->_profile = profile;
        writer->_written_rows_counter = ADD_COUNTER(profile, "WrittenRows", TUnit::UNIT);
        writer->_written_bytes_counter = ADD_COUNTER(profile, "WrittenBytes", TUnit::BYTES);
        writer->_send_data_timer = ADD_TIMER(profile, "SendDataTime");
        writer->_project_timer = ADD_CHILD_TIMER(profile, "ProjectTime", "SendDataTime");
        writer->_bucket_calc_timer = ADD_CHILD_TIMER(profile, "BucketCalcTime", "SendDataTime");
        writer->_partition_writers_dispatch_timer =
                ADD_CHILD_TIMER(profile, "PartitionsDispatchTime", "SendDataTime");
        writer->_partition_writers_write_timer =
                ADD_CHILD_TIMER(profile, "PartitionsWriteTime", "SendDataTime");
        writer->_partition_writers_count =
                ADD_COUNTER(profile, "PartitionsWriteCount", TUnit::UNIT);
        writer->_partition_writer_created =
                ADD_COUNTER(profile, "PartitionWriterCreated", TUnit::UNIT);
    }

    void init_partition_writer_context(VPaimonPartitionWriter* writer, RuntimeState* state,
                                       RuntimeProfile* profile) {
        ADD_TIMER(profile, "PartitionsWriteTime");
        ASSERT_TRUE(writer->open(state, profile).ok());
    }

    VPaimonPartitionWriter build_partition_writer(const TDataSink& sink) {
#ifdef WITH_PAIMON_CPP
        return VPaimonPartitionWriter(sink, {"p1"}, 0, nullptr, nullptr);
#else
        return VPaimonPartitionWriter(sink, {"p1"}, 0);
#endif
    }

    Block build_block(bool with_partition_value = true) {
        Block block;
        auto id_col = ColumnInt32::create();
        id_col->insert_value(1);
        block.insert(
                ColumnWithTypeAndName(std::move(id_col), std::make_shared<DataTypeInt32>(), "id"));

        auto pt_col = ColumnString::create();
        if (with_partition_value) {
            pt_col->insert_data("p1", 2);
        } else {
            pt_col->insert_default();
        }
        block.insert(
                ColumnWithTypeAndName(std::move(pt_col), std::make_shared<DataTypeString>(), "pt"));
        return block;
    }

    Block build_named_block(const std::string& id_name, const std::string& pt_name) {
        Block block;
        auto id_col = ColumnInt32::create();
        id_col->insert_value(1);
        block.insert(ColumnWithTypeAndName(std::move(id_col), std::make_shared<DataTypeInt32>(),
                                           id_name));

        auto pt_col = ColumnString::create();
        pt_col->insert_data("p1", 2);
        block.insert(ColumnWithTypeAndName(std::move(pt_col), std::make_shared<DataTypeString>(),
                                           pt_name));
        return block;
    }

    Block build_nullable_partition_block() {
        Block block;

        auto id_col = ColumnInt32::create();
        id_col->insert_value(1);
        id_col->insert_value(2);
        block.insert(
                ColumnWithTypeAndName(std::move(id_col), std::make_shared<DataTypeInt32>(), "id"));

        auto nested = ColumnString::create();
        nested->insert_data("p1", 2);
        nested->insert_default();
        auto null_map = ColumnUInt8::create();
        null_map->insert_value(0);
        null_map->insert_value(1);
        auto nullable = ColumnNullable::create(std::move(nested), std::move(null_map));
        block.insert(ColumnWithTypeAndName(
                std::move(nullable),
                std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "pt"));
        return block;
    }

    Block build_three_row_block() {
        Block block;
        auto id_col = ColumnInt32::create();
        id_col->insert_value(1);
        id_col->insert_value(2);
        id_col->insert_value(3);
        block.insert(
                ColumnWithTypeAndName(std::move(id_col), std::make_shared<DataTypeInt32>(), "id"));

        auto pt_col = ColumnString::create();
        pt_col->insert_data("p1", 2);
        pt_col->insert_data("p2", 2);
        pt_col->insert_data("p3", 2);
        block.insert(
                ColumnWithTypeAndName(std::move(pt_col), std::make_shared<DataTypeString>(), "pt"));
        return block;
    }

    TDataSink build_sink() {
        TDataSink sink;
        sink.__set_type(TDataSinkType::PAIMON_TABLE_SINK);
        sink.__isset.paimon_table_sink = true;
        sink.paimon_table_sink.__set_column_names(std::vector<std::string> {"id", "pt"});
        return sink;
    }
};

TEST_F(VPaimonTableWriterTest, TestWriteReturnsOkForEmptyBlock) {
    ObjectPool pool;
    MockRuntimeState state;
    DataTypes types {std::make_shared<DataTypeInt32>(), std::make_shared<DataTypeString>()};
    MockRowDescriptor row_desc(types, &pool);
    auto output_exprs = build_output_exprs(&pool, &state, row_desc);

    TDataSink sink = build_sink();
    VPaimonTableWriter writer(sink, output_exprs);
    RuntimeProfile profile("paimon_writer");
    init_writer_context(&writer, &state, &profile);

    Block block;
    ASSERT_TRUE(writer.write(&state, block).ok());
}

TEST_F(VPaimonTableWriterTest, TestWriteFailsWhenBucketKeysNotProvided) {
    ObjectPool pool;
    MockRuntimeState state;
    DataTypes types {std::make_shared<DataTypeInt32>(), std::make_shared<DataTypeString>()};
    MockRowDescriptor row_desc(types, &pool);
    auto output_exprs = build_output_exprs(&pool, &state, row_desc);

    TDataSink sink = build_sink();
    sink.paimon_table_sink.__set_bucket_num(4);
    VPaimonTableWriter writer(sink, output_exprs);
    RuntimeProfile profile("paimon_writer");
    init_writer_context(&writer, &state, &profile);

    Block block = build_block();
    Status status = writer.write(&state, block);
    ASSERT_FALSE(status.ok());
    ASSERT_NE(std::string::npos, status.to_string().find("requires bucket_keys"));
}

TEST_F(VPaimonTableWriterTest, TestWriteFailsWhenBucketKeyMissing) {
    ObjectPool pool;
    MockRuntimeState state;
    DataTypes types {std::make_shared<DataTypeInt32>(), std::make_shared<DataTypeString>()};
    MockRowDescriptor row_desc(types, &pool);
    auto output_exprs = build_output_exprs(&pool, &state, row_desc);

    TDataSink sink = build_sink();
    sink.paimon_table_sink.__set_bucket_num(4);
    sink.paimon_table_sink.__set_bucket_keys(std::vector<std::string> {"missing_bucket_col"});
    VPaimonTableWriter writer(sink, output_exprs);
    RuntimeProfile profile("paimon_writer");
    init_writer_context(&writer, &state, &profile);

    Block block = build_block();
    Status status = writer.write(&state, block);
    ASSERT_FALSE(status.ok());
    ASSERT_NE(std::string::npos,
              status.to_string().find("bucket key missing_bucket_col not found"));
}

TEST_F(VPaimonTableWriterTest, TestWriteFailsWhenPartitionKeyMissing) {
    ObjectPool pool;
    MockRuntimeState state;
    DataTypes types {std::make_shared<DataTypeInt32>(), std::make_shared<DataTypeString>()};
    MockRowDescriptor row_desc(types, &pool);
    auto output_exprs = build_output_exprs(&pool, &state, row_desc);

    TDataSink sink = build_sink();
    sink.paimon_table_sink.__set_partition_keys(std::vector<std::string> {"missing_partition_col"});
    VPaimonTableWriter writer(sink, output_exprs);
    RuntimeProfile profile("paimon_writer");
    init_writer_context(&writer, &state, &profile);

    Block block = build_block();
    Status status = writer.write(&state, block);
    ASSERT_FALSE(status.ok());
    ASSERT_NE(std::string::npos,
              status.to_string().find("partition key missing_partition_col not found"));
}

TEST_F(VPaimonTableWriterTest, TestDefaultPartitionNameUsesCustomOption) {
    TDataSink sink = build_sink();
    sink.paimon_table_sink.__set_options({{"partition.default-name", "__CUSTOM_DEFAULT__"}});
    VPaimonTableWriter writer(sink, {});

    ASSERT_EQ("__CUSTOM_DEFAULT__", writer._default_partition_name());
}

TEST_F(VPaimonTableWriterTest, TestInitPartitionColumnIndicesFallsBackToColumnNames) {
    TDataSink sink = build_sink();
    sink.paimon_table_sink.__set_partition_keys(std::vector<std::string> {"pt"});
    sink.paimon_table_sink.__set_column_names(std::vector<std::string> {"id", "pt"});
    VPaimonTableWriter writer(sink, {});

    Block block = build_named_block("", "");
    ASSERT_TRUE(writer._init_partition_column_indices(block).ok());
    ASSERT_TRUE(writer._partition_indices_inited);
    ASSERT_EQ(1, writer._partition_column_indices.size());
    ASSERT_EQ(1, writer._partition_column_indices[0]);
}

TEST_F(VPaimonTableWriterTest, TestCollectPartitionValueColumnsUsesDefaultForNull) {
    TDataSink sink = build_sink();
    sink.paimon_table_sink.__set_partition_keys(std::vector<std::string> {"pt"});
    sink.paimon_table_sink.__set_options({{"partition.default-name", "__CUSTOM_DEFAULT__"}});
    VPaimonTableWriter writer(sink, {});

    Block block = build_nullable_partition_block();
    std::vector<std::vector<std::string>> partition_values;
    ASSERT_TRUE(writer._collect_partition_value_columns(block, &partition_values).ok());
    ASSERT_EQ(1, partition_values.size());
    ASSERT_EQ(2, partition_values[0].size());
    ASSERT_EQ("p1", partition_values[0][0]);
    ASSERT_EQ("__CUSTOM_DEFAULT__", partition_values[0][1]);
}

TEST_F(VPaimonTableWriterTest, TestFilterBlockKeepsOnlySelectedRows) {
    TDataSink sink = build_sink();
    VPaimonTableWriter writer(sink, {});

    Block block = build_three_row_block();
    IColumn::Filter filter = {1, 0, 1};
    Block output_block;
    ASSERT_TRUE(writer._filter_block(block, &filter, &output_block).ok());
    ASSERT_EQ(2, output_block.rows());
    ASSERT_EQ(2, output_block.columns());
    ASSERT_EQ(1, output_block.get_by_position(0).column->get_int(0));
    ASSERT_EQ(3, output_block.get_by_position(0).column->get_int(1));
}

TEST_F(VPaimonTableWriterTest, TestGetOrCreateWriterReusesCachedPartitionWriter) {
    ObjectPool pool;
    MockRuntimeState state;
    DataTypes types {std::make_shared<DataTypeInt32>(), std::make_shared<DataTypeString>()};
    MockRowDescriptor row_desc(types, &pool);
    auto output_exprs = build_output_exprs(&pool, &state, row_desc);

    TDataSink sink = build_sink();
    VPaimonTableWriter writer(sink, output_exprs);
    RuntimeProfile profile("paimon_writer");
    init_writer_context(&writer, &state, &profile);

    VPaimonTableWriter::WriteKey key;
    key.bucket_id = 7;
    key.partition_values = {"p1"};

    std::shared_ptr<VPaimonPartitionWriter> first_writer;
    std::shared_ptr<VPaimonPartitionWriter> second_writer;
    ASSERT_TRUE(writer._get_or_create_writer(key, &first_writer).ok());
    ASSERT_TRUE(writer._get_or_create_writer(key, &second_writer).ok());
    ASSERT_EQ(1, writer._writers.size());
    ASSERT_EQ(first_writer.get(), second_writer.get());
}

TEST_F(VPaimonTableWriterTest, TestPartitionWriterAppendToBufferTracksRowsAndBytes) {
    TDataSink sink = build_sink();
    auto writer = build_partition_writer(sink);
    Block block = build_block();

    ASSERT_TRUE(writer._append_to_buffer(block).ok());
    ASSERT_NE(nullptr, writer._buffer.get());
    ASSERT_EQ(block.rows(), writer._buffered_rows);
    ASSERT_EQ(block.bytes(), writer._buffered_bytes);
}

TEST_F(VPaimonTableWriterTest, TestPartitionWriterFlushBufferWithoutDataIsNoop) {
    TDataSink sink = build_sink();
    auto writer = build_partition_writer(sink);

    ASSERT_TRUE(writer._flush_buffer().ok());
    ASSERT_EQ(nullptr, writer._buffer.get());
    ASSERT_EQ(0, writer._buffered_rows);
    ASSERT_EQ(0, writer._buffered_bytes);
}

TEST_F(VPaimonTableWriterTest, TestPartitionWriterCloseWithErrorClearsBufferedState) {
    TDataSink sink = build_sink();
    auto writer = build_partition_writer(sink);
    Block block = build_block();
    ASSERT_TRUE(writer._append_to_buffer(block).ok());

    Status input = Status::InternalError("abort");
    Status result = writer.close(input);
    ASSERT_FALSE(result.ok());
    ASSERT_EQ(nullptr, writer._buffer.get());
    ASSERT_EQ(0, writer._buffered_rows);
    ASSERT_EQ(0, writer._buffered_bytes);
}

} // namespace doris::vectorized
