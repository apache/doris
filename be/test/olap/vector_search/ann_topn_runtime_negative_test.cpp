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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>

#include "common/status.h"
#include "olap/rowset/segment_v2/ann_index/ann_topn_runtime.h"
#include "vec/exprs/vectorized_fn_call.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/virtual_slot_ref.h"
#include "vector_search_utils.h"

using ::testing::HasSubstr;

namespace doris::vectorized {

// These tests target uncovered error branches in AnnTopNRuntime::prepare and
// evaluate_vector_ann_search using the existing VectorSearchTest fixture setup.

TEST_F(VectorSearchTest, AnnTopNRuntimePrepare_NoFunctionCall) {
    // Build a DescriptorTbl where the virtual column expr is a constant literal (not a function call)
    doris::ObjectPool obj_pool_local;
    TDescriptorTable thrift_tbl;
    {
        TTableDescriptor table_desc;
        table_desc.id = 1000;
        thrift_tbl.tableDescriptors.push_back(table_desc);

        TTupleDescriptor tuple_desc;
        tuple_desc.__isset.tableId = true;
        tuple_desc.id = 2000;
        tuple_desc.tableId = 1000;
        thrift_tbl.tupleDescriptors.push_back(tuple_desc);

        // Slot 0: materialized, with a virtual column expr set to FLOAT_LITERAL (not a function)
        TSlotDescriptor slot0;
        slot0.id = 3000;
        slot0.parent = 2000;
        // type: DOUBLE (matches fixture)
        TTypeNode type_node;
        type_node.type = TTypeNodeType::type::SCALAR;
        TScalarType scalar_type;
        scalar_type.__set_type(TPrimitiveType::DOUBLE);
        type_node.__set_scalar_type(scalar_type);
        slot0.slotType.types.push_back(type_node);
        // Provide a simple FLOAT_LITERAL as the virtual column expr
        doris::TExpr vexpr;
        doris::TExprNode node;
        node.node_type = TExprNodeType::FLOAT_LITERAL;
        node.type = TTypeDesc();
        node.type.types.push_back(type_node);
        doris::TFloatLiteral flit;
        flit.value = 1.0;
        node.__set_float_literal(flit);
        node.__isset.float_literal = true;
        vexpr.nodes.push_back(node);
        slot0.virtual_column_expr = vexpr;
        slot0.__isset.virtual_column_expr = true;
        thrift_tbl.slotDescriptors.push_back(slot0);

        // Slot 1: a normal slot to satisfy references
        TSlotDescriptor slot1 = slot0;
        slot1.id = 3001;
        slot1.__isset.virtual_column_expr = false;
        thrift_tbl.slotDescriptors.push_back(slot1);
        thrift_tbl.__isset.slotDescriptors = true;
    }

    doris::DescriptorTbl* desc_tbl_local = nullptr;
    ASSERT_TRUE(DescriptorTbl::create(&obj_pool_local, thrift_tbl, &desc_tbl_local).ok());
    RowDescriptor row_desc_local(*desc_tbl_local, {2000});

    // Create a VirtualSlotRef root expr that points to the local descriptor's slot id (3000)
    doris::TExpr local_virtual_slot_ref_expr = _virtual_slot_ref_expr;
    ASSERT_TRUE(local_virtual_slot_ref_expr.nodes.size() == 1);
    local_virtual_slot_ref_expr.nodes[0].slot_ref.slot_id = 3000;
    std::shared_ptr<VExprContext> vslot_ctx;
    ASSERT_TRUE(VExpr::create_expr_tree(local_virtual_slot_ref_expr, vslot_ctx).ok());

    doris::RuntimeState state_local;
    state_local.set_desc_tbl(desc_tbl_local);

    auto runtime = segment_v2::AnnTopNRuntime::create_shared(true, 10, vslot_ctx);
    Status st = runtime->prepare(&state_local, row_desc_local);
    ASSERT_FALSE(st.ok());
    EXPECT_THAT(st.to_string(), HasSubstr("expect FuncationCall"));
}

// Note: We intentionally avoid testing a non-VirtualSlotRef root since it triggers DCHECK.

// Removed additional negative prepare tests that rely on internal descriptor mutations.

TEST_F(VectorSearchTest, AnnTopNRuntimeEvaluate_DimensionMismatch) {
    // Prepare a valid runtime first.
    std::shared_ptr<VExprContext> dist_ctx;
    auto fn_thrift = read_from_json<TExpr>(_distance_function_call_thrift);
    ASSERT_TRUE(VExpr::create_expr_tree(fn_thrift, dist_ctx).ok());

    std::shared_ptr<VExprContext> vslot_ctx;
    ASSERT_TRUE(VExpr::create_expr_tree(_virtual_slot_ref_expr, vslot_ctx).ok());
    auto vir_slot = std::dynamic_pointer_cast<VirtualSlotRef>(vslot_ctx->root());
    ASSERT_TRUE(vir_slot != nullptr);
    vir_slot->set_virtual_column_expr(dist_ctx->root());

    auto runtime = segment_v2::AnnTopNRuntime::create_shared(true, 10, vslot_ctx);
    ASSERT_TRUE(runtime->prepare(&_runtime_state, _row_desc).ok());

    // Attach an ANN reader with a different dimension to trigger the mismatch branch.
    {
        std::map<std::string, std::string> props;
        props["index_type"] = "hnsw";
        props["metric_type"] = "l2_distance";
        props["dim"] = "4"; // runtime query vector dimension is 8 from fixture JSON
        auto pair = vector_search_utils::create_tmp_ann_index_reader(props);
        _ann_index_iterator->_ann_reader = pair.second;
    }

    roaring::Roaring bitmap;
    vectorized::IColumn::MutablePtr result_col = ColumnFloat32::create(0);
    std::unique_ptr<std::vector<uint64_t>> row_ids;
    doris::segment_v2::AnnIndexStats stats;
    Status st = runtime->evaluate_vector_ann_search(_ann_index_iterator.get(), &bitmap, 10,
                                                    result_col, row_ids, stats);
    ASSERT_FALSE(st.ok());
    EXPECT_THAT(st.to_string(), HasSubstr("dimension"));
}

} // namespace doris::vectorized
