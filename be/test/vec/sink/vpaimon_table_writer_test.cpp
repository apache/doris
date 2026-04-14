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
#undef private
#pragma clang diagnostic pop

#include <gtest/gtest.h>

#include "common/object_pool.h"
#include "core/block/block.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
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

    void init_writer_context(VPaimonTableWriter* writer, RuntimeState* state, RuntimeProfile* profile) {
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
        writer->_partition_writers_count = ADD_COUNTER(profile, "PartitionsWriteCount", TUnit::UNIT);
        writer->_partition_writer_created = ADD_COUNTER(profile, "PartitionWriterCreated", TUnit::UNIT);
    }

    Block build_block(bool with_partition_value = true) {
        Block block;
        auto id_col = ColumnInt32::create();
        id_col->insert_value(1);
        block.insert(ColumnWithTypeAndName(std::move(id_col), std::make_shared<DataTypeInt32>(), "id"));

        auto pt_col = ColumnString::create();
        if (with_partition_value) {
            pt_col->insert_data("p1", 2);
        } else {
            pt_col->insert_default();
        }
        block.insert(ColumnWithTypeAndName(std::move(pt_col), std::make_shared<DataTypeString>(), "pt"));
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
    ASSERT_NE(std::string::npos, status.to_string().find("bucket key missing_bucket_col not found"));
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
    ASSERT_NE(std::string::npos, status.to_string().find("partition key missing_partition_col not found"));
}

} // namespace doris::vectorized
