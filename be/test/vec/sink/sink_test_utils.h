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

#pragma once

#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/Exprs_types.h>
#include <gen_cpp/Partitions_types.h>
#include <gtest/gtest.h>

#include <cstdint>

#include "exec/tablet_info.h"
#include "pipeline/operator/operator_helper.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/types.h"

namespace doris::vectorized {

namespace sink_test_utils {

inline TExprNode make_int_literal(int32_t v) {
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

inline TExprNode make_bool_literal(bool v) {
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

inline TExpr make_slot_ref_expr(TSlotId slot_id, TTupleId tuple_id) {
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

inline void build_desc_tbl_and_schema(doris::pipeline::OperatorContext& ctx,
                                      TOlapTableSchemaParam& tschema,
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

inline TOlapTablePartitionParam build_partition_param(int64_t schema_index_id) {
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
    p1.__set_start_keys({make_int_literal(0)});
    p1.__set_end_keys({make_int_literal(10)});

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
    p2.__set_start_keys({make_int_literal(20)});
    p2.__set_end_keys({make_int_literal(1000)});

    param.partitions = {p1, p2};
    return param;
}

inline TOlapTablePartitionParam build_auto_partition_param(int64_t schema_index_id,
                                                           TTupleId tuple_id, TSlotId slot_id) {
    auto param = build_partition_param(schema_index_id);
    param.__set_enable_automatic_partition(true);
    param.__set_partition_function_exprs({
            make_slot_ref_expr(slot_id, tuple_id),
    });
    return param;
}

inline TOlapTablePartitionParam build_partition_param_with_load_tablet_idx(
        int64_t schema_index_id, int64_t load_tablet_idx) {
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
    p1.__set_start_keys({make_int_literal(0)});
    p1.__set_end_keys({make_int_literal(1000)});

    param.partitions = {p1};
    return param;
}

inline TOlapTableLocationParam build_location_param() {
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

} // namespace sink_test_utils

} // namespace doris::vectorized
