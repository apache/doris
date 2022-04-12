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

#include "runtime/qsorter.h"

#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "common/object_pool.h"
#include "exprs/expr.h"
#include "gen_cpp/Descriptors_types.h"
#include "runtime/descriptors.h"
#include "runtime/mem_pool.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "runtime/tuple.h"
#include "runtime/tuple_row.h"

namespace doris {

class QSorterTest : public testing::Test {
public:
    QSorterTest() {
        init_desc_tbl();
        init_row_desc();
        init_runtime_state();
        init_order_expr();
    }

    ~QSorterTest() {}

    void init_desc_tbl();

    void init_row_desc();

    void init_order_expr();

    void init_runtime_state();

protected:
    virtual void SetUp() {}

    virtual void TearDown() {}

private:
    ObjectPool _obj_pool;
    TDescriptorTable _t_desc_tbl;
    DescriptorTbl* _desc_tbl;
    RowDescriptor* _row_desc;
    std::vector<Expr*> _order_expr;
    RuntimeState* _state;
    MemPool _tuple_pool;
};

void QSorterTest::init_runtime_state() {
    _state = _obj_pool.add(new RuntimeState("2011-10-01 12:34:56"));
    _state->set_desc_tbl(_desc_tbl);
}

void QSorterTest::init_order_expr() {
    SlotRef* slot = _obj_pool.add(new SlotRef(_desc_tbl->get_slot_descriptor(0)));
    slot->prepare(_state, *_row_desc);
    _order_expr.push_back(slot);
    slot = _obj_pool.add(new SlotRef(_desc_tbl->get_slot_descriptor(1)));
    slot->prepare(_state, *_row_desc);
    _order_expr.push_back(slot);
}

void QSorterTest::init_row_desc() {
    std::vector<TTupleId> row_tuples;
    row_tuples.push_back(0);
    std::vector<bool> nullable_tuples;
    nullable_tuples.push_back(false);

    _row_desc = _obj_pool.add(new RowDescriptor(*_desc_tbl, row_tuples, nullable_tuples));
}

void QSorterTest::init_desc_tbl() {
    // slot desc
    std::vector<TSlotDescriptor> slot_descs;

    // 1 byte null, 4 byte int, 4 byte int
    // slot 0
    {
        TSlotDescriptor t_slot_desc;
        t_slot_desc.__set_id(0);
        t_slot_desc.__set_parent(0);
        t_slot_desc.__set_slotType(TPrimitiveType::INT);
        t_slot_desc.__set_columnPos(0);
        t_slot_desc.__set_byteOffset(1);
        t_slot_desc.__set_nullIndicatorByte(0);
        t_slot_desc.__set_nullIndicatorBit(1);
        t_slot_desc.__set_colName("col1");
        t_slot_desc.__set_slotIdx(0);
        t_slot_desc.__set_isMaterialized(true);

        slot_descs.push_back(t_slot_desc);
    }
    // slot 1
    {
        TSlotDescriptor t_slot_desc;
        t_slot_desc.__set_id(1);
        t_slot_desc.__set_parent(0);
        t_slot_desc.__set_slotType(TPrimitiveType::INT);
        t_slot_desc.__set_columnPos(1);
        t_slot_desc.__set_byteOffset(5);
        t_slot_desc.__set_nullIndicatorByte(0);
        t_slot_desc.__set_nullIndicatorBit(2);
        t_slot_desc.__set_colName("col2");
        t_slot_desc.__set_slotIdx(1);
        t_slot_desc.__set_isMaterialized(true);

        slot_descs.push_back(t_slot_desc);
    }

    _t_desc_tbl.__set_slotDescriptors(slot_descs);

    // tuple desc
    std::vector<TTupleDescriptor> tuple_descs;
    // tuple 0
    {
        TTupleDescriptor t_tuple_desc;

        t_tuple_desc.__set_id(0);
        t_tuple_desc.__set_byteSize(9);
        t_tuple_desc.__set_numNullBytes(1);
        t_tuple_desc.__set_tableId(0);

        tuple_descs.push_back(t_tuple_desc);
    }
    _t_desc_tbl.__set_tupleDescriptors(tuple_descs);

    // table
    std::vector<TTableDescriptor> table_descs;
    // table 0
    {
        TTableDescriptor t_table_desc;

        t_table_desc.__set_id(0);
        t_table_desc.__set_tableType(TTableType::MYSQL_TABLE);
        t_table_desc.__set_numCols(2);
        t_table_desc.__set_numClusteringCols(2);
        t_table_desc.__set_tableName("test_tbl");
        t_table_desc.__set_dbName("test_db");

        TMySQLTable mysql_table;
        t_table_desc.__set_mysqlTable(mysql_table);

        table_descs.push_back(t_table_desc);
    }
    _t_desc_tbl.__set_tableDescriptors(table_descs);

    DescriptorTbl::create(&_obj_pool, _t_desc_tbl, &_desc_tbl);
}

TEST_F(QSorterTest, normalCase) {
    RowBatch batch(*_row_desc, 1024);

    // 5, 100
    {
        batch.add_row();
        TupleRow* row = batch.get_row(batch.num_rows());
        Tuple* tuple = Tuple::create(9, &_tuple_pool);
        row->set_tuple(0, tuple);
        char* pos = (char*)tuple;
        memset(pos, 0, 9);
        *(int*)(pos + 1) = 5;
        *(int*)(pos + 5) = 100;
        batch.commit_last_row();
    }

    // 1, 10
    {
        batch.add_row();
        TupleRow* row = batch.get_row(batch.num_rows());
        Tuple* tuple = Tuple::create(9, &_tuple_pool);
        row->set_tuple(0, tuple);
        char* pos = (char*)tuple;
        memset(pos, 0, 9);
        *(int*)(pos + 1) = 1;
        *(int*)(pos + 5) = 10;
        batch.commit_last_row();
    }

    // 5, 5
    {
        batch.add_row();
        TupleRow* row = batch.get_row(batch.num_rows());
        Tuple* tuple = Tuple::create(9, &_tuple_pool);
        row->set_tuple(0, tuple);
        char* pos = (char*)tuple;
        memset(pos, 0, 9);
        *(int*)(pos + 1) = 5;
        *(int*)(pos + 5) = 5;
        batch.commit_last_row();
    }

    // 1000, 5
    {
        batch.add_row();
        TupleRow* row = batch.get_row(batch.num_rows());
        Tuple* tuple = Tuple::create(9, &_tuple_pool);
        row->set_tuple(0, tuple);
        char* pos = (char*)tuple;
        memset(pos, 0, 9);
        *(int*)(pos + 1) = 10000;
        *(int*)(pos + 5) = 5;
        batch.commit_last_row();
    }

    // 0, 195
    {
        batch.add_row();
        TupleRow* row = batch.get_row(batch.num_rows());
        Tuple* tuple = Tuple::create(9, &_tuple_pool);
        row->set_tuple(0, tuple);
        char* pos = (char*)tuple;
        memset(pos, 0, 9);
        *(int*)(pos + 1) = 0;
        *(int*)(pos + 5) = 195;
        batch.commit_last_row();
    }

    QSorter sorter(*_row_desc, _order_expr);

    EXPECT_TRUE(sorter.prepare(_state).ok());
    EXPECT_TRUE(sorter.add_batch(&batch).ok());
    EXPECT_TRUE(sorter.input_done().ok());

    RowBatch result(*_row_desc, 1024);
    bool eos = false;
    EXPECT_TRUE(sorter.get_next(&result, &eos).ok());
    EXPECT_TRUE(eos);
    EXPECT_EQ(5, result.num_rows());

    // 0, 195
    {
        EXPECT_EQ(0, *(int*)_order_expr[0]->get_value(result.get_row(0)));
        EXPECT_EQ(195, *(int*)_order_expr[1]->get_value(result.get_row(0)));
    }
    // 1, 10
    {
        EXPECT_EQ(1, *(int*)_order_expr[0]->get_value(result.get_row(1)));
        EXPECT_EQ(10, *(int*)_order_expr[1]->get_value(result.get_row(1)));
    }
    // 5, 5
    {
        EXPECT_EQ(5, *(int*)_order_expr[0]->get_value(result.get_row(2)));
        EXPECT_EQ(5, *(int*)_order_expr[1]->get_value(result.get_row(2)));
    }
    // 5, 100
    {
        EXPECT_EQ(5, *(int*)_order_expr[0]->get_value(result.get_row(3)));
        EXPECT_EQ(100, *(int*)_order_expr[1]->get_value(result.get_row(3)));
    }
    // 10000, 5
    {
        EXPECT_EQ(10000, *(int*)_order_expr[0]->get_value(result.get_row(4)));
        EXPECT_EQ(5, *(int*)_order_expr[1]->get_value(result.get_row(4)));
    }
}

} // namespace doris
