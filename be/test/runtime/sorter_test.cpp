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

#include "runtime/sorter.h"

#include <gtest/gtest.h>
#include <stdio.h>
#include <stdlib.h>

#include <iostream>

#include "common/object_pool.h"
#include "exec/sort_exec_exprs.h"
#include "exprs/expr.h"
#include "gen_cpp/Descriptors_types.h"
#include "gen_cpp/Exprs_types.h"
#include "gen_cpp/PaloInternalService_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/Types_types.h"
#include "runtime/buffered_block_mgr.h"
#include "runtime/descriptors.h"
#include "runtime/primitive_type.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "runtime/tuple_row.h"
#include "util/debug_util.h"

namespace doris {

class SorterTest : public testing::Test {
public:
    RowBatch* CreateRowBatch(int num_rows);
    ObjectPool* get_object_pool() { return _object_pool; }
    RuntimeState* get_runtime_state() { return _runtime_state; }
    SorterTest() {
        _object_pool = new ObjectPool();
        _runtime_state = new RuntimeState("SorterTest");
        _runtime_state->exec_env_ = &_exec_env;
        _runtime_state->create_block_mgr();
        {
            TExpr expr;
            {
                TExprNode node;

                node.node_type = TExprNodeType::SLOT_REF;
                node.type = ToTColumnTypeThrift(TPrimitiveType::BIGINT);
                node.num_children = 0;
                TSlotRef data;
                data.slot_id = 0;
                data.tuple_id = 0;
                node.__set_slot_ref(data);
                expr.nodes.push_back(node);
            }
            _sort_tuple_slot_expr.push_back(expr);
        }
        {
            TExpr expr;
            {
                TExprNode node;

                node.node_type = TExprNodeType::SLOT_REF;
                node.type = ToTColumnTypeThrift(TPrimitiveType::BIGINT);
                node.num_children = 0;
                TSlotRef data;
                data.slot_id = 1;
                data.tuple_id = 1;
                node.__set_slot_ref(data);
                expr.nodes.push_back(node);
            }
            _ordering_exprs.push_back(expr);
        }
        _is_asc_order.push_back(true);
        _nulls_first.push_back(true);

        {
            TTupleDescriptor tuple_desc;
            TDescriptorTable thrift_desc_tbl;
            {
                tuple_desc.__set_id(0);
                tuple_desc.__set_byteSize(8);
                tuple_desc.__set_numNullBytes(1);
                thrift_desc_tbl.tupleDescriptors.push_back(tuple_desc);
            }
            {
                tuple_desc.__set_id(1);
                tuple_desc.__set_byteSize(8);
                tuple_desc.__set_numNullBytes(1);
                thrift_desc_tbl.tupleDescriptors.push_back(tuple_desc);
            }

            TSlotDescriptor slot_desc;
            {
                slot_desc.__set_id(0);
                slot_desc.__set_parent(0);
                slot_desc.__set_slotType(TPrimitiveType::BIGINT);
                slot_desc.__set_columnPos(0);
                slot_desc.__set_byteOffset(0);
                slot_desc.__set_nullIndicatorByte(0);
                slot_desc.__set_nullIndicatorBit(-1);
                slot_desc.__set_slotIdx(0);
                slot_desc.__set_isMaterialized(true);
                thrift_desc_tbl.slotDescriptors.push_back(slot_desc);
            }
            {
                slot_desc.__set_id(1);
                slot_desc.__set_parent(1);
                slot_desc.__set_slotType(TPrimitiveType::BIGINT);
                slot_desc.__set_columnPos(0);
                slot_desc.__set_byteOffset(0);
                slot_desc.__set_nullIndicatorByte(0);
                slot_desc.__set_nullIndicatorBit(-1);
                slot_desc.__set_slotIdx(0);
                slot_desc.__set_isMaterialized(true);
                thrift_desc_tbl.slotDescriptors.push_back(slot_desc);
            }
            Status status = DescriptorTbl::Create(_object_pool, thrift_desc_tbl, &_desc_tbl);
            DCHECK(status.ok());
            _runtime_state->set_desc_tbl(_desc_tbl);
        }
        {
            std::vector<TTupleId> row_tuples;
            std::vector<bool> nullable_tuples;
            nullable_tuples.push_back(0);
            row_tuples.push_back(0);
            _child_row_desc = new RowDescriptor(*_desc_tbl, row_tuples, nullable_tuples);
        }
        {
            std::vector<TTupleId> row_tuples;
            std::vector<bool> nullable_tuples;
            nullable_tuples.push_back(1);
            row_tuples.push_back(1);
            _output_row_desc = new RowDescriptor(*_desc_tbl, row_tuples, nullable_tuples);
        }
        _runtime_profile = new RuntimeProfile("sorter");
    }
    virtual ~SorterTest() {
        delete _child_row_desc;
        delete _output_row_desc;
        delete _object_pool;
        delete _runtime_state;
        delete _runtime_profile;
    }

protected:
    virtual void SetUp() {}

private:
    ExecEnv _exec_env;
    RuntimeState* _runtime_state;
    RowDescriptor* _child_row_desc;
    RowDescriptor* _output_row_desc;
    RuntimeProfile* _runtime_profile;
    std::vector<TExpr> _sort_tuple_slot_expr;
    std::vector<TExpr> _ordering_exprs;
    std::vector<bool> _is_asc_order;
    std::vector<bool> _nulls_first;
    DescriptorTbl* _desc_tbl;
    ObjectPool* _object_pool;
};

TEST_F(SorterTest, init_sort_exec_exprs) {
    // empty  sort_tuple_slot_expr
    {
        SortExecExprs exec_exprs;
        Status status = exec_exprs.init(_ordering_exprs, nullptr, get_object_pool());
        EXPECT_TRUE(status.ok());
    }
    // full sort_tuple_slot_expr
    {
        SortExecExprs exec_exprs;
        Status status = exec_exprs.init(_ordering_exprs, &_sort_tuple_slot_expr, get_object_pool());
        EXPECT_TRUE(status.ok());
    }
}

TEST_F(SorterTest, prepare_sort_exec_exprs) {
    {
        SortExecExprs exec_exprs;
        Status status = exec_exprs.init(_ordering_exprs, nullptr, get_object_pool());
        EXPECT_TRUE(status.ok());
        status = exec_exprs.prepare(_runtime_state, *_child_row_desc, *_output_row_desc);
        EXPECT_TRUE(status.ok());
    }

    {
        SortExecExprs exec_exprs;
        Status status = exec_exprs.init(_ordering_exprs, &_sort_tuple_slot_expr, get_object_pool());
        EXPECT_TRUE(status.ok());
        status = exec_exprs.prepare(_runtime_state, *_child_row_desc, *_output_row_desc);
        EXPECT_TRUE(status.ok());
    }
}

RowBatch* SorterTest::CreateRowBatch(int num_rows) {
    RowBatch* batch = _object_pool->Add(new RowBatch(*_child_row_desc, num_rows));
    int64_t* tuple_mem = reinterpret_cast<int64_t*>(
            batch->tuple_data_pool()->Allocate(sizeof(int64_t) * num_rows));

    for (int i = 0; i < num_rows; ++i) {
        int idx = batch->AddRow();
        TupleRow* row = batch->GetRow(idx);
        *tuple_mem = i;
        row->SetTuple(0, reinterpret_cast<Tuple*>(tuple_mem));

        batch->CommitLastRow();
        tuple_mem++;
    }
    return batch;
}

TEST_F(SorterTest, sorter_run_asc) {
    SortExecExprs exec_exprs;
    Status status = exec_exprs.init(_ordering_exprs, &_sort_tuple_slot_expr, _object_pool);
    EXPECT_TRUE(status.ok());
    status = exec_exprs.prepare(_runtime_state, *_child_row_desc, *_output_row_desc);
    EXPECT_TRUE(status.ok());

    TupleRowComparator less_than(exec_exprs.lhs_ordering_expr_ctxs(),
                                 exec_exprs.rhs_ordering_expr_ctxs(), _is_asc_order, _nulls_first);
    Sorter* sorter = new Sorter(less_than, exec_exprs.sort_tuple_slot_expr_ctxs(), _child_row_desc,
                                _runtime_profile, _runtime_state);

    int num_rows = 5;
    RowBatch* batch = CreateRowBatch(num_rows);
    status = sorter->add_batch(batch);
    EXPECT_TRUE(status.ok());
    status = sorter->add_batch(batch);
    EXPECT_TRUE(status.ok());
    sorter->input_done();

    RowBatch output_batch(*_child_row_desc, 2 * num_rows);
    bool eos;
    status = sorter->get_next(&output_batch, &eos);
    EXPECT_TRUE(status.ok());

    EXPECT_EQ("[(0)]", PrintRow(output_batch.GetRow(0), *_child_row_desc));
    EXPECT_EQ("[(0)]", PrintRow(output_batch.GetRow(1), *_child_row_desc));
    EXPECT_EQ("[(1)]", PrintRow(output_batch.GetRow(2), *_child_row_desc));
    EXPECT_EQ("[(1)]", PrintRow(output_batch.GetRow(3), *_child_row_desc));
    EXPECT_EQ("[(2)]", PrintRow(output_batch.GetRow(4), *_child_row_desc));
    EXPECT_EQ("[(2)]", PrintRow(output_batch.GetRow(5), *_child_row_desc));
    EXPECT_EQ("[(3)]", PrintRow(output_batch.GetRow(6), *_child_row_desc));
    EXPECT_EQ("[(3)]", PrintRow(output_batch.GetRow(7), *_child_row_desc));
    EXPECT_EQ("[(4)]", PrintRow(output_batch.GetRow(8), *_child_row_desc));
    EXPECT_EQ("[(4)]", PrintRow(output_batch.GetRow(9), *_child_row_desc));

    delete sorter;
}

/* reverse order : exceed 16 elements, we use quick sort*/
TEST_F(SorterTest, sorter_run_desc_with_quick_sort) {
    SortExecExprs exec_exprs;
    Status status = exec_exprs.init(_ordering_exprs, &_sort_tuple_slot_expr, _object_pool);
    EXPECT_TRUE(status.ok());
    status = exec_exprs.prepare(_runtime_state, *_child_row_desc, *_output_row_desc);
    EXPECT_TRUE(status.ok());

    _is_asc_order.clear();
    _is_asc_order.push_back(false);
    TupleRowComparator less_than(exec_exprs.lhs_ordering_expr_ctxs(),
                                 exec_exprs.rhs_ordering_expr_ctxs(), _is_asc_order, _nulls_first);
    Sorter* sorter = new Sorter(less_than, exec_exprs.sort_tuple_slot_expr_ctxs(), _child_row_desc,
                                _runtime_profile, _runtime_state);

    int num_rows = 5;
    RowBatch* batch = CreateRowBatch(num_rows);
    for (int i = 0; i < 5; i++) {
        status = sorter->add_batch(batch);
        EXPECT_TRUE(status.ok());
    }

    sorter->input_done();

    RowBatch output_batch(*_child_row_desc, 2 * num_rows);
    bool eos;
    status = sorter->get_next(&output_batch, &eos);
    EXPECT_TRUE(status.ok());

    EXPECT_EQ("[(4)]", PrintRow(output_batch.GetRow(0), *_child_row_desc));
    EXPECT_EQ("[(4)]", PrintRow(output_batch.GetRow(1), *_child_row_desc));
    EXPECT_EQ("[(4)]", PrintRow(output_batch.GetRow(2), *_child_row_desc));
    EXPECT_EQ("[(4)]", PrintRow(output_batch.GetRow(3), *_child_row_desc));
    EXPECT_EQ("[(4)]", PrintRow(output_batch.GetRow(4), *_child_row_desc));
    EXPECT_EQ("[(3)]", PrintRow(output_batch.GetRow(5), *_child_row_desc));

    delete sorter;
}

TEST_F(SorterTest, sorter_run_desc) {
    SortExecExprs exec_exprs;
    Status status = exec_exprs.init(_ordering_exprs, &_sort_tuple_slot_expr, _object_pool);
    EXPECT_TRUE(status.ok());
    status = exec_exprs.prepare(_runtime_state, *_child_row_desc, *_output_row_desc);
    EXPECT_TRUE(status.ok());

    _is_asc_order.clear();
    _is_asc_order.push_back(false);
    TupleRowComparator less_than(exec_exprs.lhs_ordering_expr_ctxs(),
                                 exec_exprs.rhs_ordering_expr_ctxs(), _is_asc_order, _nulls_first);
    Sorter* sorter = new Sorter(less_than, exec_exprs.sort_tuple_slot_expr_ctxs(), _child_row_desc,
                                _runtime_profile, _runtime_state);

    int num_rows = 5;
    RowBatch* batch = CreateRowBatch(num_rows);
    status = sorter->add_batch(batch);
    EXPECT_TRUE(status.ok());
    status = sorter->add_batch(batch);
    EXPECT_TRUE(status.ok());
    sorter->input_done();

    RowBatch output_batch(*_child_row_desc, 2 * num_rows);
    bool eos;
    status = sorter->get_next(&output_batch, &eos);
    EXPECT_TRUE(status.ok());

    EXPECT_EQ("[(4)]", PrintRow(output_batch.GetRow(0), *_child_row_desc));
    EXPECT_EQ("[(4)]", PrintRow(output_batch.GetRow(1), *_child_row_desc));
    EXPECT_EQ("[(3)]", PrintRow(output_batch.GetRow(2), *_child_row_desc));
    EXPECT_EQ("[(3)]", PrintRow(output_batch.GetRow(3), *_child_row_desc));
    EXPECT_EQ("[(2)]", PrintRow(output_batch.GetRow(4), *_child_row_desc));
    EXPECT_EQ("[(2)]", PrintRow(output_batch.GetRow(5), *_child_row_desc));
    EXPECT_EQ("[(1)]", PrintRow(output_batch.GetRow(6), *_child_row_desc));
    EXPECT_EQ("[(1)]", PrintRow(output_batch.GetRow(7), *_child_row_desc));
    EXPECT_EQ("[(0)]", PrintRow(output_batch.GetRow(8), *_child_row_desc));
    EXPECT_EQ("[(0)]", PrintRow(output_batch.GetRow(9), *_child_row_desc));

    delete sorter;
}
} // namespace doris

\n