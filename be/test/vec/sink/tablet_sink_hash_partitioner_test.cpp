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
#include "testutil/mock/mock_slot_ref.h"
#include "util/debug_points.h"
#include "util/hash_util.hpp"
#include "util/runtime_profile.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/sink/vrow_distribution.h"
#include "vec/sink/vtablet_block_convertor.h"
#include "vec/sink/vtablet_finder.h"

namespace doris::vectorized {

namespace {

using doris::pipeline::ExchangeSinkLocalState;
using doris::pipeline::ExchangeSinkOperatorX;
using doris::pipeline::OperatorContext;

TExprNode _make_int_literal(int32_t v) {
    TExprNode node;
    node.__set_node_type(TExprNodeType::INT_LITERAL);
    node.__set_num_children(0);
    node.__set_output_scale(0);

    TIntLiteral int_lit;
    int_lit.__set_value(v);
    node.__set_int_literal(int_lit);

    TTypeDesc type_desc = create_type_desc(PrimitiveType::TYPE_INT);
    type_desc.__set_is_nullable(false);
    node.__set_type(type_desc);
    node.__set_is_nullable(false);

    return node;
}

TExpr _make_slot_ref_expr(TSlotId slot_id, TTupleId tuple_id) {
    TExprNode node;
    node.__set_node_type(TExprNodeType::SLOT_REF);
    node.__set_num_children(0);

    TSlotRef slot_ref;
    slot_ref.__set_slot_id(slot_id);
    slot_ref.__set_tuple_id(tuple_id);
    node.__set_slot_ref(slot_ref);

    TTypeDesc type_desc = create_type_desc(PrimitiveType::TYPE_INT);
    type_desc.__set_is_nullable(false);
    node.__set_type(type_desc);
    node.__set_is_nullable(false);

    TExpr expr;
    expr.nodes.emplace_back(node);
    return expr;
}

[[maybe_unused]] int64_t _calc_channel_id(int64_t tablet_id, size_t partition_count) {
    auto hash = HashUtil::zlib_crc_hash(&tablet_id, sizeof(int64_t), 0);
    return static_cast<int64_t>(hash % partition_count);
}

TExprNode _make_bool_literal(bool v) {
    TExprNode node;
    node.__set_node_type(TExprNodeType::BOOL_LITERAL);
    node.__set_num_children(0);
    node.__set_output_scale(0);

    TBoolLiteral bool_lit;
    bool_lit.__set_value(v);
    node.__set_bool_literal(bool_lit);

    TTypeDesc type_desc = create_type_desc(PrimitiveType::TYPE_BOOLEAN);
    type_desc.__set_is_nullable(false);
    node.__set_type(type_desc);
    node.__set_is_nullable(false);

    return node;
}

void _build_desc_tbl_and_schema(OperatorContext& ctx, TOlapTableSchemaParam& tschema,
                               TTupleId& tablet_sink_tuple_id, int64_t& schema_index_id,
                               bool is_nullable = true) {
    TDescriptorTableBuilder dtb;
    {
        TTupleDescriptorBuilder tuple_builder;
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .type(TYPE_INT)
                                       .nullable(is_nullable)
                                       .column_name("c1")
                                       .column_pos(1)
                                       .build());
        tuple_builder.build(&dtb);
    }

    auto thrift_desc_tbl = dtb.desc_tbl();
    DescriptorTbl* desc_tbl = nullptr;
    auto st = DescriptorTbl::create(ctx.state.obj_pool(), thrift_desc_tbl, &desc_tbl);
    ASSERT_TRUE(st.ok()) << st.to_string();
    ctx.state.set_desc_tbl(desc_tbl);

    tschema.db_id = 1;
    tschema.table_id = 2;
    tschema.version = 0;
    tschema.slot_descs = thrift_desc_tbl.slotDescriptors;
    tschema.tuple_desc = thrift_desc_tbl.tupleDescriptors[0];

    TOlapTableIndexSchema index_schema;
    index_schema.id = 10;
    index_schema.columns = {"c1"};
    index_schema.schema_hash = 123;
    tschema.indexes = {index_schema};

    tablet_sink_tuple_id = tschema.tuple_desc.id;
    schema_index_id = index_schema.id;
}

TOlapTablePartitionParam _build_partition_param(int64_t schema_index_id) {
    TOlapTablePartitionParam param;
    param.db_id = 1;
    param.table_id = 2;
    param.version = 0;

    param.__set_partition_type(TPartitionType::RANGE_PARTITIONED);
    param.__set_partition_columns({"c1"});
    param.__set_distributed_columns({"c1"});

    TOlapTablePartition p1;
    p1.id = 1;
    p1.num_buckets = 1;
    p1.__set_is_mutable(true);
    {
        TOlapTableIndexTablets index_tablets;
        index_tablets.index_id = schema_index_id;
        index_tablets.tablets = {100};
        p1.indexes = {index_tablets};
    }
    p1.__set_start_keys({_make_int_literal(0)});
    p1.__set_end_keys({_make_int_literal(10)});

    TOlapTablePartition p2;
    p2.id = 2;
    p2.num_buckets = 1;
    p2.__set_is_mutable(true);
    {
        TOlapTableIndexTablets index_tablets;
        index_tablets.index_id = schema_index_id;
        index_tablets.tablets = {200};
        p2.indexes = {index_tablets};
    }
    p2.__set_start_keys({_make_int_literal(20)});
    p2.__set_end_keys({_make_int_literal(1000)});

    param.partitions = {p1, p2};
    return param;
}

TOlapTablePartitionParam _build_auto_partition_param(int64_t schema_index_id,
                                                      TTupleId tuple_id, TSlotId slot_id) {
    auto param = _build_partition_param(schema_index_id);
    param.__set_enable_automatic_partition(true);
    param.__set_partition_function_exprs({
            _make_slot_ref_expr(slot_id, tuple_id),
    });
    return param;
}

TOlapTablePartitionParam _build_partition_param_with_load_tablet_idx(int64_t schema_index_id,
                                                                       int64_t load_tablet_idx) {
    TOlapTablePartitionParam param;
    param.db_id = 1;
    param.table_id = 2;
    param.version = 0;

    param.__set_partition_type(TPartitionType::RANGE_PARTITIONED);
    param.__set_partition_columns({"c1"});

    TOlapTablePartition p1;
    p1.id = 1;
    p1.num_buckets = 2;
    p1.__set_is_mutable(true);
    p1.__set_load_tablet_idx(load_tablet_idx);
    {
        TOlapTableIndexTablets index_tablets;
        index_tablets.index_id = schema_index_id;
        index_tablets.tablets = {100, 101};
        p1.indexes = {index_tablets};
    }
    p1.__set_start_keys({_make_int_literal(0)});
    p1.__set_end_keys({_make_int_literal(1000)});

    param.partitions = {p1};
    return param;
}

TOlapTableLocationParam _build_location_param() {
    TOlapTableLocationParam location;
    location.db_id = 1;
    location.table_id = 2;
    location.version = 0;

    TTabletLocation t1;
    t1.tablet_id = 100;
    t1.node_ids = {1};

    TTabletLocation t2;
    t2.tablet_id = 200;
    t2.node_ids = {1};

    location.tablets = {t1, t2};
    return location;
}

[[maybe_unused]] std::shared_ptr<ExchangeSinkOperatorX> _create_parent_operator(
        OperatorContext& ctx, const std::shared_ptr<doris::MockRowDescriptor>& row_desc_holder) {
    TDataStreamSink sink;
    sink.dest_node_id = 0;
    sink.output_partition.type = TPartitionType::UNPARTITIONED;

    return std::make_shared<ExchangeSinkOperatorX>(&ctx.state, *row_desc_holder, 0, sink,
                                                   std::vector<TPlanFragmentDestination> {},
                                                   std::vector<TUniqueId> {});
}

[[maybe_unused]] std::unique_ptr<TabletSinkHashPartitioner> _create_partitioner(
        OperatorContext& ctx,
                                                               ExchangeSinkLocalState* local_state,
                                                               size_t partition_count,
                                                               int64_t txn_id) {
    TOlapTableSchemaParam schema;
    TTupleId tablet_sink_tuple_id = 0;
    int64_t schema_index_id = 0;
    _build_desc_tbl_and_schema(ctx, schema, tablet_sink_tuple_id, schema_index_id);

    auto partition = _build_partition_param(schema_index_id);
    auto location = _build_location_param();

    auto partitioner = std::make_unique<TabletSinkHashPartitioner>(
            partition_count, txn_id, schema, partition, location, tablet_sink_tuple_id,
            local_state);
    auto st = partitioner->open(&ctx.state);
    EXPECT_TRUE(st.ok()) << st.to_string();
    return partitioner;
}

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
    _build_desc_tbl_and_schema(ctx, tschema, tablet_sink_tuple_id, schema_index_id, false);

    auto tpartition = _build_partition_param(schema_index_id);
    auto tlocation = _build_location_param();

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

TEST(VRowDistributionTest, GenerateRowsDistributionWhereClauseConstFalseFiltersAllRows) {
    OperatorContext ctx;
    constexpr int64_t txn_id = 1;

    TOlapTableSchemaParam tschema;
    TTupleId tablet_sink_tuple_id = 0;
    int64_t schema_index_id = 0;
    _build_desc_tbl_and_schema(ctx, tschema, tablet_sink_tuple_id, schema_index_id, false);

    auto tpartition = _build_partition_param(schema_index_id);
    auto tlocation = _build_location_param();

    auto h = _build_vrow_distribution_harness(ctx, tschema, tpartition, tlocation,
                                               tablet_sink_tuple_id, txn_id);

    TExpr texpr;
    texpr.nodes.emplace_back(_make_bool_literal(false));
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
    _build_desc_tbl_and_schema(ctx, tschema, tablet_sink_tuple_id, schema_index_id, false);

    auto tpartition = _build_partition_param(schema_index_id);
    auto tlocation = _build_location_param();

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
    _build_desc_tbl_and_schema(ctx, tschema, tablet_sink_tuple_id, schema_index_id, false);

    TSlotId partition_slot_id = tschema.slot_descs[0].id;
    auto tpartition =
            _build_auto_partition_param(schema_index_id, tablet_sink_tuple_id, partition_slot_id);
    auto tlocation = _build_location_param();

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
                new_part.__set_start_keys({_make_int_literal(10)});
                new_part.__set_end_keys({_make_int_literal(20)});
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
    _build_desc_tbl_and_schema(ctx, tschema, tablet_sink_tuple_id, schema_index_id, false);

    auto tpartition = _build_partition_param(schema_index_id);
    tpartition.__set_enable_auto_detect_overwrite(true);
    tpartition.__set_overwrite_group_id(123);
    auto tlocation = _build_location_param();

    auto h = _build_vrow_distribution_harness(ctx, tschema, tpartition, tlocation,
                                               tablet_sink_tuple_id, txn_id);

    doris::config::enable_debug_points = true;
    doris::DebugPoints::instance()->clear();

    int injected_times = 0;
    std::function<void(doris::TReplacePartitionRequest*, doris::TReplacePartitionResult*)>
            handler = [&](doris::TReplacePartitionRequest* req, doris::TReplacePartitionResult* res) {
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

    // The replaced partitions are recorded as "new" inside VRowDistribution, so the second call
    // should not request replacement again.
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

TEST(TabletSinkHashPartitionerTest, OlapTabletFinderRoundRobinEveryBatch) {
    OperatorContext ctx;

    TOlapTableSchemaParam tschema;
    TTupleId tablet_sink_tuple_id = 0;
    int64_t schema_index_id = 0;
    _build_desc_tbl_and_schema(ctx, tschema, tablet_sink_tuple_id, schema_index_id, false);

    auto schema = std::make_shared<OlapTableSchemaParam>();
    auto st = schema->init(tschema);
    ASSERT_TRUE(st.ok()) << st.to_string();

    auto tpartition = _build_partition_param_with_load_tablet_idx(schema_index_id, 0);
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
