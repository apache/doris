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

#include "runtime/buffered_tuple_stream.h"

#include <gtest/gtest.h>
#include <stdio.h>
#include <stdlib.h>

#include <iostream>

#include "common/object_pool.h"
#include "exec/sort_exec_exprs.h"
#include "exprs/expr.h"
#include "gen_cpp/Descriptors_types.h"
#include "gen_cpp/Exprs_types.h"
#include "gen_cpp/ImpalaInternalService_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/Types_types.h"
#include "runtime/buffered_block_mgr.h"
#include "runtime/descriptors.h"
#include "runtime/primitive_type.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "runtime/sorter.h"
#include "runtime/tuple_row.h"
#include "util/debug_util.h"

namespace doris {

class BufferedTupleStreamTest : public testing::Test {
public:
    RowBatch* create_row_batch(int num_rows);
    BufferedTupleStreamTest() {
        _object_pool = new ObjectPool();
        _profile = new RuntimeProfile("bufferedStream");
        _runtime_state = new RuntimeState("BufferedTupleStreamTest");
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
        /*
            {  
                std::vector<TTupleId> row_tuples;
                std::vector<bool> nullable_tuples;
                nullable_tuples.push_back(1);
                row_tuples.push_back(1);
                _output_row_desc = new RowDescriptor(*_desc_tbl, row_tuples, nullable_tuples);
            }
           */
    }
    virtual ~BufferedTupleStreamTest() {
        delete _child_row_desc;
        delete _runtime_state;
        delete _profile;
        delete _object_pool;
        // delete _output_row_desc;
    }

protected:
    virtual void SetUp() {}

private:
    ExecEnv _exec_env;
    RuntimeState* _runtime_state;
    RowDescriptor* _child_row_desc;
    RowDescriptor* _output_row_desc;
    DescriptorTbl* _desc_tbl;
    ObjectPool* _object_pool;
    std::vector<TExpr> _sort_tuple_slot_expr;
    std::vector<TExpr> _ordering_exprs;
    std::vector<bool> _is_asc_order;
    std::vector<bool> _nulls_first;

    RuntimeProfile* _profile;
};

RowBatch* BufferedTupleStreamTest::create_row_batch(int num_rows) {
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

TEST_F(BufferedTupleStreamTest, init_bufferStream) {
    BufferedTupleStream* input_stream =
            new BufferedTupleStream(_runtime_state, *_child_row_desc, _runtime_state->block_mgr());
    Status status = input_stream->init(_profile);
    EXPECT_TRUE(status.ok());
    input_stream->close();
    delete input_stream;
}

TEST_F(BufferedTupleStreamTest, addRow_bufferStream) {
    BufferedTupleStream* input_stream =
            new BufferedTupleStream(_runtime_state, *_child_row_desc, _runtime_state->block_mgr());
    Status status = input_stream->init(_profile);
    EXPECT_TRUE(status.ok());
    int num_rows = 5;
    RowBatch* batch = create_row_batch(num_rows);
    for (int i = 0; i < num_rows; i++) {
        TupleRow* row = batch->GetRow(i);
        input_stream->add_row(row);
        EXPECT_TRUE(status.ok());
    }
    EXPECT_EQ(input_stream->num_rows(), num_rows);
    input_stream->close();
    delete input_stream;
}

TEST_F(BufferedTupleStreamTest, getNext_bufferStream) {
    BufferedTupleStream* input_stream =
            new BufferedTupleStream(_runtime_state, *_child_row_desc, _runtime_state->block_mgr());
    Status status = input_stream->init(_profile);
    EXPECT_TRUE(status.ok());
    int num_rows = 5;
    RowBatch* batch = create_row_batch(num_rows * 2);
    for (int i = 0; i < num_rows * 2; i++) {
        TupleRow* row = batch->GetRow(i);
        input_stream->add_row(row);
        EXPECT_TRUE(status.ok());
    }

    EXPECT_EQ(input_stream->num_rows(), num_rows * 2);

    RowBatch output_batch(*_child_row_desc, num_rows);
    bool eos;

    status = input_stream->get_next(&output_batch, &eos);
    EXPECT_TRUE(status.ok());

    EXPECT_EQ("[(0)]", PrintRow(output_batch.GetRow(0), *_child_row_desc));
    EXPECT_EQ("[(1)]", PrintRow(output_batch.GetRow(1), *_child_row_desc));
    EXPECT_EQ("[(2)]", PrintRow(output_batch.GetRow(2), *_child_row_desc));
    EXPECT_EQ("[(3)]", PrintRow(output_batch.GetRow(3), *_child_row_desc));
    EXPECT_EQ("[(4)]", PrintRow(output_batch.GetRow(4), *_child_row_desc));

    output_batch.Reset();

    status = input_stream->get_next(&output_batch, &eos);

    EXPECT_TRUE(status.ok());
    EXPECT_EQ("[(5)]", PrintRow(output_batch.GetRow(0), *_child_row_desc));
    EXPECT_EQ("[(6)]", PrintRow(output_batch.GetRow(1), *_child_row_desc));
    EXPECT_EQ("[(7)]", PrintRow(output_batch.GetRow(2), *_child_row_desc));
    EXPECT_EQ("[(8)]", PrintRow(output_batch.GetRow(3), *_child_row_desc));
    EXPECT_EQ("[(9)]", PrintRow(output_batch.GetRow(4), *_child_row_desc));

    EXPECT_EQ(input_stream->rows_returned(), num_rows * 2);
    input_stream->close();
    delete input_stream;
}

} // namespace doris

\n