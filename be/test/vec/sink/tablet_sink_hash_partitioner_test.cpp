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

#include "vec/sink/tablet_sink_hash_partitioner.h"

#include <gen_cpp/DataSinks_types.h>
#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/Exprs_types.h>
#include <gen_cpp/FrontendService_types.h>
#include <gen_cpp/Partitions_types.h>
#include <gen_cpp/Status_types.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <utility>
#include <vector>

#include "common/cast_set.h"
#include "common/config.h"
#include "exec/tablet_info.h"
#include "pipeline/exec/exchange_sink_operator.h"
#include "pipeline/operator/operator_helper.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/types.h"
#include "testutil/column_helper.h"
#include "testutil/mock/mock_descriptors.h"
#include "util/debug_points.h"
#include "util/hash_util.hpp"
#include "util/runtime_profile.h"
#include "vec/columns/column_vector.h"
#include "vec/common/assert_cast.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/sink/sink_test_utils.h"
#include "vec/sink/vtablet_finder.h"

namespace doris::vectorized {

namespace {

using doris::pipeline::ExchangeSinkLocalState;
using doris::pipeline::ExchangeSinkOperatorX;
using doris::pipeline::OperatorContext;

std::shared_ptr<ExchangeSinkOperatorX> _create_parent_operator(
        OperatorContext& ctx, const std::shared_ptr<doris::MockRowDescriptor>& row_desc_holder) {
    TDataStreamSink sink;
    sink.dest_node_id = 0;
    sink.output_partition.type = TPartitionType::UNPARTITIONED;

    return std::make_shared<ExchangeSinkOperatorX>(&ctx.state, *row_desc_holder, 0, sink,
                                                   std::vector<TPlanFragmentDestination> {},
                                                   std::vector<TUniqueId> {});
}

std::unique_ptr<TabletSinkHashPartitioner> _create_partitioner(
        OperatorContext& ctx, ExchangeSinkLocalState* local_state, size_t partition_count,
        int64_t txn_id, const TOlapTableSchemaParam& schema,
        const TOlapTablePartitionParam& partition, const TOlapTableLocationParam& location,
        TTupleId tablet_sink_tuple_id) {
    auto partitioner = std::make_unique<TabletSinkHashPartitioner>(
            partition_count, txn_id, schema, partition, location, tablet_sink_tuple_id,
            local_state);
    auto st = partitioner->open(&ctx.state);
    EXPECT_TRUE(st.ok()) << st.to_string();
    return partitioner;
}

TEST(TabletSinkHashPartitionerTest, DoPartitioningSkipsImmutablePartitionAndHashesOthers) {
    OperatorContext ctx;
    constexpr size_t partition_count = 8;
    constexpr int64_t txn_id = 1;

    TOlapTableSchemaParam tschema;
    TTupleId tablet_sink_tuple_id = 0;
    int64_t schema_index_id = 0;
    sink_test_utils::build_desc_tbl_and_schema(ctx, tschema, tablet_sink_tuple_id, schema_index_id,
                                               false);

    auto row_desc_holder = std::make_shared<doris::MockRowDescriptor>(
            std::vector<DataTypePtr> {std::make_shared<DataTypeInt32>()}, &ctx.pool);
    auto parent_op = _create_parent_operator(ctx, row_desc_holder);
    ExchangeSinkLocalState local_state(parent_op.get(), &ctx.state);

    auto tpartition = sink_test_utils::build_partition_param(schema_index_id);
    ASSERT_EQ(tpartition.partitions.size(), 2);
    // 1: [0, 10), 2: [20, 1000)
    tpartition.partitions[0].__set_is_mutable(false);
    auto tlocation = sink_test_utils::build_location_param();

    auto partitioner = _create_partitioner(ctx, &local_state, partition_count, txn_id, tschema,
                                           tpartition, tlocation, tablet_sink_tuple_id);

    // 1 -> no partition, 25 -> p1
    auto block = ColumnHelper::create_block<DataTypeInt32>({1, 25});
    auto st = partitioner->do_partitioning(&ctx.state, &block);
    ASSERT_TRUE(st.ok()) << st.to_string();

    const auto& skipped = partitioner->get_skipped();
    ASSERT_EQ(skipped.size(), block.rows());
    EXPECT_TRUE(skipped[0]);
    EXPECT_FALSE(skipped[1]);

    auto channel_ids = partitioner->get_channel_ids();
    ASSERT_EQ(channel_ids.size(), 2);
    EXPECT_EQ(channel_ids[0], partition_count); // skipped partition

    uint32_t tablet_id = 200;
    auto hash = HashUtil::zlib_crc_hash(&tablet_id, sizeof(uint32_t), 0);
    EXPECT_EQ(channel_ids[1], static_cast<uint32_t>(hash % partition_count));
}

TEST(TabletSinkHashPartitionerTest, TryCutInLineCreatesPartitionAndReturnsBatchedBlock) {
    OperatorContext ctx;
    constexpr size_t partition_count = 8;
    constexpr int64_t txn_id = 1;

    TOlapTableSchemaParam tschema;
    TTupleId tablet_sink_tuple_id = 0;
    int64_t schema_index_id = 0;
    sink_test_utils::build_desc_tbl_and_schema(ctx, tschema, tablet_sink_tuple_id, schema_index_id,
                                               false);
    TSlotId partition_slot_id = tschema.slot_descs[0].id;

    auto row_desc_holder = std::make_shared<doris::MockRowDescriptor>(
            std::vector<DataTypePtr> {std::make_shared<DataTypeInt32>()}, &ctx.pool);
    auto parent_op = _create_parent_operator(ctx, row_desc_holder);
    ExchangeSinkLocalState local_state(parent_op.get(), &ctx.state);

    auto tpartition = sink_test_utils::build_auto_partition_param(
            schema_index_id, tablet_sink_tuple_id, partition_slot_id);
    auto tlocation = sink_test_utils::build_location_param();

    auto partitioner = _create_partitioner(ctx, &local_state, partition_count, txn_id, tschema,
                                           tpartition, tlocation, tablet_sink_tuple_id);

    doris::config::enable_debug_points = true;
    doris::DebugPoints::instance()->clear();

    bool injected = false;
    std::function<void(doris::TCreatePartitionRequest*, doris::TCreatePartitionResult*)> handler =
            [&](doris::TCreatePartitionRequest* req, doris::TCreatePartitionResult* res) {
                injected = true;
                ASSERT_TRUE(req->__isset.partitionValues);
                ASSERT_EQ(req->partitionValues.size(), 1);
                ASSERT_EQ(req->partitionValues[0].size(), 1);
                ASSERT_TRUE(req->partitionValues[0][0].__isset.value);
                EXPECT_EQ(req->partitionValues[0][0].value, "15");

                doris::TStatus tstatus;
                tstatus.__set_status_code(doris::TStatusCode::OK);
                res->__set_status(tstatus);

                doris::TOlapTablePartition new_part;
                new_part.id = 3;
                new_part.num_buckets = 1;
                new_part.__set_is_mutable(true);
                {
                    doris::TOlapTableIndexTablets index_tablets;
                    index_tablets.index_id = schema_index_id;
                    index_tablets.tablets = {300};
                    new_part.indexes = {index_tablets};
                }
                new_part.__set_start_keys({sink_test_utils::make_int_literal(10)});
                new_part.__set_end_keys({sink_test_utils::make_int_literal(20)});
                res->__set_partitions({new_part});

                doris::TTabletLocation new_location;
                new_location.__set_tablet_id(300);
                new_location.__set_node_ids({1});
                res->__set_tablets({new_location});
            };
    doris::DebugPoints::instance()->add_with_callback(
            "VRowDistribution.automatic_create_partition.inject_result", handler);

    {
        auto block = ColumnHelper::create_block<DataTypeInt32>({15, 15});
        auto st = partitioner->do_partitioning(&ctx.state, &block);
        ASSERT_TRUE(st.ok()) << st.to_string();

        // Flush batching data at end-of-stream.
        partitioner->mark_last_block();
        Block batched;
        st = partitioner->try_cut_in_line(batched);
        ASSERT_TRUE(st.ok()) << st.to_string();
        EXPECT_TRUE(injected);

        ASSERT_EQ(batched.rows(), 2);
        ASSERT_EQ(batched.columns(), 1);
        const auto& col = batched.get_by_position(0).column;
        ASSERT_EQ(col->size(), 2);
        EXPECT_EQ(assert_cast<const ColumnInt32&>(*col).get_data()[0], 15);
        EXPECT_EQ(assert_cast<const ColumnInt32&>(*col).get_data()[1], 15);
    }

    doris::DebugPoints::instance()->clear();
    doris::config::enable_debug_points = false;
}

TEST(TabletSinkHashPartitionerTest, OlapTabletFinderRoundRobinEveryBatch) {
    OperatorContext ctx;

    TOlapTableSchemaParam tschema;
    TTupleId tablet_sink_tuple_id = 0;
    int64_t schema_index_id = 0;
    sink_test_utils::build_desc_tbl_and_schema(ctx, tschema, tablet_sink_tuple_id, schema_index_id,
                                               false);

    auto schema = std::make_shared<OlapTableSchemaParam>();
    auto st = schema->init(tschema);
    ASSERT_TRUE(st.ok()) << st.to_string();

    auto tpartition =
            sink_test_utils::build_partition_param_with_load_tablet_idx(schema_index_id, 0);
    auto vpartition = std::make_unique<VOlapTablePartitionParam>(schema, tpartition);
    st = vpartition->init();
    ASSERT_TRUE(st.ok()) << st.to_string();

    OlapTabletFinder finder(vpartition.get(),
                            OlapTabletFinder::FindTabletMode::FIND_TABLET_EVERY_BATCH);

    {
        auto block = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3});
        std::vector<VOlapTablePartition*> partitions(block.rows(), nullptr);
        std::vector<uint32_t> tablet_index(block.rows(), 0);
        std::vector<bool> skip(block.rows(), false);

        st = finder.find_tablets(&ctx.state, &block, cast_set<int>(block.rows()), partitions,
                                 tablet_index, skip, nullptr);
        ASSERT_TRUE(st.ok()) << st.to_string();
        EXPECT_EQ(tablet_index[0], 0);
        EXPECT_EQ(tablet_index[1], 0);
        EXPECT_EQ(tablet_index[2], 0);
    }

    {
        auto block = ColumnHelper::create_block<DataTypeInt32>({1, 2});
        std::vector<VOlapTablePartition*> partitions(block.rows(), nullptr);
        std::vector<uint32_t> tablet_index(block.rows(), 0);
        std::vector<bool> skip(block.rows(), false);

        st = finder.find_tablets(&ctx.state, &block, cast_set<int>(block.rows()), partitions,
                                 tablet_index, skip, nullptr);
        ASSERT_TRUE(st.ok()) << st.to_string();
        EXPECT_EQ(tablet_index[0], 1);
        EXPECT_EQ(tablet_index[1], 1);
    }

    {
        auto block = ColumnHelper::create_block<DataTypeInt32>({1});
        std::vector<VOlapTablePartition*> partitions(block.rows(), nullptr);
        std::vector<uint32_t> tablet_index(block.rows(), 0);
        std::vector<bool> skip(block.rows(), false);

        st = finder.find_tablets(&ctx.state, &block, cast_set<int>(block.rows()), partitions,
                                 tablet_index, skip, nullptr);
        ASSERT_TRUE(st.ok()) << st.to_string();
        EXPECT_EQ(tablet_index[0], 0);
    }
}
} // anonymous namespace
} // namespace doris::vectorized
