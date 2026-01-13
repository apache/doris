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

#include "vec/sink/vrow_distribution.h"

#include <gen_cpp/Exprs_types.h>
#include <gen_cpp/FrontendService_types.h>
#include <gen_cpp/Partitions_types.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <functional>
#include <memory>
#include <vector>

#include "common/config.h"
#include "pipeline/operator/operator_helper.h"
#include "testutil/column_helper.h"
#include "testutil/mock/mock_slot_ref.h"
#include "util/debug_points.h"
#include "vec/data_types/data_type_number.h"
#include "vec/sink/sink_test_utils.h"
#include "vec/sink/vtablet_block_convertor.h"
#include "vec/sink/vtablet_finder.h"

namespace doris::vectorized {

namespace {

using doris::pipeline::OperatorContext;

struct VRowDistributionHarness {
    std::shared_ptr<OlapTableSchemaParam> schema;
    std::unique_ptr<VOlapTablePartitionParam> vpartition;
    std::unique_ptr<OlapTableLocationParam> location;
    std::unique_ptr<OlapTabletFinder> tablet_finder;
    std::unique_ptr<OlapTableBlockConvertor> block_convertor;
    VExprContextSPtrs output_expr_ctxs;
    std::unique_ptr<RowDescriptor> output_row_desc;
    VRowDistribution row_distribution;
};

Status _noop_create_partition_callback(void*, TCreatePartitionResult*) {
    return Status::OK();
}

std::unique_ptr<VRowDistributionHarness> _build_vrow_distribution_harness(
        OperatorContext& ctx, const TOlapTableSchemaParam& tschema,
        const TOlapTablePartitionParam& tpartition, const TOlapTableLocationParam& tlocation,
        TTupleId tablet_sink_tuple_id, int64_t txn_id) {
    auto h = std::make_unique<VRowDistributionHarness>();

    h->schema = std::make_shared<OlapTableSchemaParam>();
    auto st = h->schema->init(tschema);
    EXPECT_TRUE(st.ok()) << st.to_string();

    h->vpartition = std::make_unique<VOlapTablePartitionParam>(h->schema, tpartition);
    st = h->vpartition->init();
    EXPECT_TRUE(st.ok()) << st.to_string();

    h->location = std::make_unique<OlapTableLocationParam>(tlocation);
    h->tablet_finder = std::make_unique<OlapTabletFinder>(h->vpartition.get(),
                                                          OlapTabletFinder::FIND_TABLET_EVERY_ROW);
    h->block_convertor = std::make_unique<OlapTableBlockConvertor>(h->schema->tuple_desc());

    h->output_row_desc = std::make_unique<RowDescriptor>(
            ctx.state.desc_tbl(), std::vector<TTupleId> {tablet_sink_tuple_id});

    VRowDistribution::VRowDistributionContext rctx;
    rctx.state = &ctx.state;
    rctx.block_convertor = h->block_convertor.get();
    rctx.tablet_finder = h->tablet_finder.get();
    rctx.vpartition = h->vpartition.get();
    rctx.add_partition_request_timer = nullptr;
    rctx.txn_id = txn_id;
    rctx.pool = &ctx.pool;
    rctx.location = h->location.get();
    rctx.vec_output_expr_ctxs = &h->output_expr_ctxs;
    rctx.schema = h->schema;
    rctx.caller = nullptr;
    rctx.write_single_replica = false;
    rctx.create_partition_callback = &_noop_create_partition_callback;
    h->row_distribution.init(rctx);

    st = h->row_distribution.open(h->output_row_desc.get());
    EXPECT_TRUE(st.ok()) << st.to_string();

    return h;
}

TEST(VRowDistributionTest, GenerateRowsDistributionNonAutoPartitionBasic) {
    OperatorContext ctx;
    constexpr int64_t txn_id = 1;

    TOlapTableSchemaParam tschema;
    TTupleId tablet_sink_tuple_id = 0;
    int64_t schema_index_id = 0;
    sink_test_utils::build_desc_tbl_and_schema(ctx, tschema, tablet_sink_tuple_id, schema_index_id,
                                               false);

    auto tpartition = sink_test_utils::build_partition_param(schema_index_id);
    auto tlocation = sink_test_utils::build_location_param();

    auto h = _build_vrow_distribution_harness(ctx, tschema, tpartition, tlocation,
                                              tablet_sink_tuple_id, txn_id);

    auto input_block = ColumnHelper::create_block<DataTypeInt32>({1, 25});
    std::shared_ptr<Block> converted_block;
    std::vector<RowPartTabletIds> row_part_tablet_ids;
    int64_t rows_stat_val = input_block.rows();
    auto st = h->row_distribution.generate_rows_distribution(input_block, converted_block,
                                                             row_part_tablet_ids, rows_stat_val);
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_NE(converted_block, nullptr);

    ASSERT_EQ(row_part_tablet_ids.size(), 1);
    ASSERT_EQ(row_part_tablet_ids[0].row_ids.size(), 2);
    EXPECT_EQ(row_part_tablet_ids[0].row_ids[0], 0);
    EXPECT_EQ(row_part_tablet_ids[0].row_ids[1], 1);
    EXPECT_EQ(row_part_tablet_ids[0].partition_ids[0], 1);
    EXPECT_EQ(row_part_tablet_ids[0].partition_ids[1], 2);
}

TEST(VRowDistributionTest, GenerateRowsDistributionSkipsImmutablePartition) {
    OperatorContext ctx;
    constexpr int64_t txn_id = 1;

    TOlapTableSchemaParam tschema;
    TTupleId tablet_sink_tuple_id = 0;
    int64_t schema_index_id = 0;
    sink_test_utils::build_desc_tbl_and_schema(ctx, tschema, tablet_sink_tuple_id, schema_index_id,
                                               false);

    auto tpartition = sink_test_utils::build_partition_param(schema_index_id);
    ASSERT_EQ(tpartition.partitions.size(), 2);
    tpartition.partitions[0].__set_is_mutable(false);
    auto tlocation = sink_test_utils::build_location_param();

    auto h = _build_vrow_distribution_harness(ctx, tschema, tpartition, tlocation,
                                              tablet_sink_tuple_id, txn_id);

    auto input_block = ColumnHelper::create_block<DataTypeInt32>({1});
    std::shared_ptr<Block> converted_block;
    std::vector<RowPartTabletIds> row_part_tablet_ids;
    int64_t rows_stat_val = input_block.rows();
    auto st = h->row_distribution.generate_rows_distribution(input_block, converted_block,
                                                             row_part_tablet_ids, rows_stat_val);
    ASSERT_TRUE(st.ok()) << st.to_string();

    ASSERT_EQ(row_part_tablet_ids.size(), 1);
    EXPECT_TRUE(row_part_tablet_ids[0].row_ids.empty());

    auto skipped = h->row_distribution.get_skipped();
    ASSERT_EQ(skipped.size(), 1);
    EXPECT_TRUE(skipped[0]);
}

TEST(VRowDistributionTest, GenerateRowsDistributionWhereClauseConstFalseFiltersAllRows) {
    OperatorContext ctx;
    constexpr int64_t txn_id = 1;

    TOlapTableSchemaParam tschema;
    TTupleId tablet_sink_tuple_id = 0;
    int64_t schema_index_id = 0;
    sink_test_utils::build_desc_tbl_and_schema(ctx, tschema, tablet_sink_tuple_id, schema_index_id,
                                               false);

    auto tpartition = sink_test_utils::build_partition_param(schema_index_id);
    auto tlocation = sink_test_utils::build_location_param();

    auto h = _build_vrow_distribution_harness(ctx, tschema, tpartition, tlocation,
                                              tablet_sink_tuple_id, txn_id);

    TExpr texpr;
    texpr.nodes.emplace_back(sink_test_utils::make_bool_literal(false));
    VExprContextSPtr where_ctx;
    auto st = VExpr::create_expr_tree(texpr, where_ctx);
    ASSERT_TRUE(st.ok()) << st.to_string();
    st = where_ctx->prepare(&ctx.state, *h->output_row_desc);
    ASSERT_TRUE(st.ok()) << st.to_string();
    st = where_ctx->open(&ctx.state);
    ASSERT_TRUE(st.ok()) << st.to_string();
    h->schema->indexes()[0]->where_clause = where_ctx;

    auto input_block = ColumnHelper::create_block<DataTypeInt32>({1, 25});
    std::shared_ptr<Block> converted_block;
    std::vector<RowPartTabletIds> row_part_tablet_ids;
    int64_t rows_stat_val = input_block.rows();
    st = h->row_distribution.generate_rows_distribution(input_block, converted_block,
                                                        row_part_tablet_ids, rows_stat_val);
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_EQ(row_part_tablet_ids.size(), 1);
    EXPECT_TRUE(row_part_tablet_ids[0].row_ids.empty());
    EXPECT_TRUE(row_part_tablet_ids[0].partition_ids.empty());
    EXPECT_TRUE(row_part_tablet_ids[0].tablet_ids.empty());
}

TEST(VRowDistributionTest, GenerateRowsDistributionWhereClauseUInt8ColumnFiltersSomeRows) {
    OperatorContext ctx;
    constexpr int64_t txn_id = 1;

    TOlapTableSchemaParam tschema;
    TTupleId tablet_sink_tuple_id = 0;
    int64_t schema_index_id = 0;
    sink_test_utils::build_desc_tbl_and_schema(ctx, tschema, tablet_sink_tuple_id, schema_index_id,
                                               false);

    auto tpartition = sink_test_utils::build_partition_param(schema_index_id);
    auto tlocation = sink_test_utils::build_location_param();

    auto h = _build_vrow_distribution_harness(ctx, tschema, tpartition, tlocation,
                                              tablet_sink_tuple_id, txn_id);

    auto where_ctx = VExprContext::create_shared(
            std::make_shared<MockSlotRef>(1, std::make_shared<DataTypeUInt8>()));
    auto st = where_ctx->prepare(&ctx.state, *h->output_row_desc);
    ASSERT_TRUE(st.ok()) << st.to_string();
    st = where_ctx->open(&ctx.state);
    ASSERT_TRUE(st.ok()) << st.to_string();
    h->schema->indexes()[0]->where_clause = where_ctx;

    auto input_block = ColumnHelper::create_block<DataTypeInt32>({1, 25, 2});
    auto filter_col_mut = ColumnUInt8::create();
    filter_col_mut->get_data().push_back(1);
    filter_col_mut->get_data().push_back(0);
    filter_col_mut->get_data().push_back(1);
    ColumnPtr filter_col = std::move(filter_col_mut);
    input_block.insert({filter_col, std::make_shared<DataTypeUInt8>(), "f"});

    std::shared_ptr<Block> converted_block;
    std::vector<RowPartTabletIds> row_part_tablet_ids;
    int64_t rows_stat_val = input_block.rows();
    st = h->row_distribution.generate_rows_distribution(input_block, converted_block,
                                                        row_part_tablet_ids, rows_stat_val);
    ASSERT_TRUE(st.ok()) << st.to_string();

    ASSERT_EQ(row_part_tablet_ids.size(), 1);
    ASSERT_EQ(row_part_tablet_ids[0].row_ids.size(), 2);
    EXPECT_EQ(row_part_tablet_ids[0].row_ids[0], 0);
    EXPECT_EQ(row_part_tablet_ids[0].row_ids[1], 2);
}

TEST(VRowDistributionTest, AutoPartitionMissingValuesBatchingDedupAndCreatePartition) {
    OperatorContext ctx;
    constexpr int64_t txn_id = 1;

    TOlapTableSchemaParam tschema;
    TTupleId tablet_sink_tuple_id = 0;
    int64_t schema_index_id = 0;
    sink_test_utils::build_desc_tbl_and_schema(ctx, tschema, tablet_sink_tuple_id, schema_index_id,
                                               false);

    TSlotId partition_slot_id = tschema.slot_descs[0].id;
    auto tpartition = sink_test_utils::build_auto_partition_param(
            schema_index_id, tablet_sink_tuple_id, partition_slot_id);
    auto tlocation = sink_test_utils::build_location_param();

    auto h = _build_vrow_distribution_harness(ctx, tschema, tpartition, tlocation,
                                              tablet_sink_tuple_id, txn_id);

    auto input_block = ColumnHelper::create_block<DataTypeInt32>({15, 15});
    std::shared_ptr<Block> converted_block;
    std::vector<RowPartTabletIds> row_part_tablet_ids;
    int64_t rows_stat_val = input_block.rows();
    auto st = h->row_distribution.generate_rows_distribution(input_block, converted_block,
                                                             row_part_tablet_ids, rows_stat_val);
    ASSERT_TRUE(st.ok()) << st.to_string();

    ASSERT_TRUE(h->row_distribution._batching_block);
    EXPECT_EQ(h->row_distribution._batching_block->rows(), 2);

    h->row_distribution._deal_batched = true;
    EXPECT_TRUE(h->row_distribution.need_deal_batching());

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

    st = h->row_distribution.automatic_create_partition();
    EXPECT_TRUE(st.ok()) << st.to_string();
    EXPECT_TRUE(injected);

    auto check_block = ColumnHelper::create_block<DataTypeInt32>({15});
    std::vector<VOlapTablePartition*> parts(1, nullptr);
    h->vpartition->find_partition(&check_block, 0, parts[0]);
    ASSERT_NE(parts[0], nullptr);
    EXPECT_EQ(parts[0]->id, 3);

    h->row_distribution.clear_batching_stats();
    EXPECT_FALSE(h->row_distribution.need_deal_batching());

    doris::DebugPoints::instance()->clear();
    doris::config::enable_debug_points = false;
}

TEST(VRowDistributionTest, ReplaceOverwritingPartitionInjectedRequestDedupAndReplace) {
    OperatorContext ctx;
    constexpr int64_t txn_id = 1;

    TOlapTableSchemaParam tschema;
    TTupleId tablet_sink_tuple_id = 0;
    int64_t schema_index_id = 0;
    sink_test_utils::build_desc_tbl_and_schema(ctx, tschema, tablet_sink_tuple_id, schema_index_id,
                                               false);

    auto tpartition = sink_test_utils::build_partition_param(schema_index_id);
    tpartition.__set_enable_auto_detect_overwrite(true);
    tpartition.__set_overwrite_group_id(123);
    auto tlocation = sink_test_utils::build_location_param();

    auto h = _build_vrow_distribution_harness(ctx, tschema, tpartition, tlocation,
                                              tablet_sink_tuple_id, txn_id);

    doris::config::enable_debug_points = true;
    doris::DebugPoints::instance()->clear();

    int injected_times = 0;
    std::function<void(doris::TReplacePartitionRequest*, doris::TReplacePartitionResult*)> handler =
            [&](doris::TReplacePartitionRequest* req, doris::TReplacePartitionResult* res) {
                injected_times++;
                ASSERT_TRUE(req->__isset.partition_ids);
                ASSERT_EQ(req->partition_ids.size(), 2);
                EXPECT_EQ(req->partition_ids[0], 1);
                EXPECT_EQ(req->partition_ids[1], 2);
                ASSERT_TRUE(req->__isset.overwrite_group_id);
                EXPECT_EQ(req->overwrite_group_id, 123);

                doris::TStatus tstatus;
                tstatus.__set_status_code(doris::TStatusCode::OK);
                res->__set_status(tstatus);

                doris::TOlapTablePartition new_p1;
                new_p1.id = 11;
                new_p1.num_buckets = 1;
                new_p1.__set_is_mutable(true);
                {
                    doris::TOlapTableIndexTablets index_tablets;
                    index_tablets.index_id = schema_index_id;
                    index_tablets.tablets = {1100};
                    new_p1.indexes = {index_tablets};
                }

                doris::TOlapTablePartition new_p2;
                new_p2.id = 12;
                new_p2.num_buckets = 1;
                new_p2.__set_is_mutable(true);
                {
                    doris::TOlapTableIndexTablets index_tablets;
                    index_tablets.index_id = schema_index_id;
                    index_tablets.tablets = {1200};
                    new_p2.indexes = {index_tablets};
                }

                res->__set_partitions({new_p1, new_p2});

                doris::TTabletLocation loc1;
                loc1.__set_tablet_id(1100);
                loc1.__set_node_ids({1});
                doris::TTabletLocation loc2;
                loc2.__set_tablet_id(1200);
                loc2.__set_node_ids({1});
                res->__set_tablets({loc1, loc2});
            };
    doris::DebugPoints::instance()->add_with_callback(
            "VRowDistribution.replace_overwriting_partition.inject_result", handler);

    Status st;
    {
        auto input_block = ColumnHelper::create_block<DataTypeInt32>({1, 25});
        std::shared_ptr<Block> converted_block;
        std::vector<RowPartTabletIds> row_part_tablet_ids;
        int64_t rows_stat_val = input_block.rows();
        st = h->row_distribution.generate_rows_distribution(input_block, converted_block,
                                                            row_part_tablet_ids, rows_stat_val);
        EXPECT_TRUE(st.ok()) << st.to_string();
        EXPECT_EQ(injected_times, 1);

        ASSERT_EQ(row_part_tablet_ids.size(), 1);
        ASSERT_EQ(row_part_tablet_ids[0].partition_ids.size(), 2);
        EXPECT_EQ(row_part_tablet_ids[0].partition_ids[0], 11);
        EXPECT_EQ(row_part_tablet_ids[0].partition_ids[1], 12);
        ASSERT_EQ(row_part_tablet_ids[0].tablet_ids.size(), 2);
        EXPECT_EQ(row_part_tablet_ids[0].tablet_ids[0], 1100);
        EXPECT_EQ(row_part_tablet_ids[0].tablet_ids[1], 1200);
    }

    {
        auto input_block = ColumnHelper::create_block<DataTypeInt32>({1});
        std::shared_ptr<Block> converted_block;
        std::vector<RowPartTabletIds> row_part_tablet_ids;
        int64_t rows_stat_val = input_block.rows();
        st = h->row_distribution.generate_rows_distribution(input_block, converted_block,
                                                            row_part_tablet_ids, rows_stat_val);
        EXPECT_TRUE(st.ok()) << st.to_string();
        EXPECT_EQ(injected_times, 1);
    }

    doris::DebugPoints::instance()->clear();
    doris::config::enable_debug_points = false;
}

} // namespace

} // namespace doris::vectorized
