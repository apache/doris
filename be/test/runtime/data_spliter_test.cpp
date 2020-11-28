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

#include "runtime/data_spliter.h"

#include <gtest/gtest.h>

#include "common/object_pool.h"
#include "gen_cpp/DataSinks_types.h"
#include "gen_cpp/Exprs_types.h"
#include "gen_cpp/Types_types.h"
#include "olap/olap_main.cpp"
#include "runtime/descriptors.h"
#include "runtime/dpp_sink_internal.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "runtime/tuple.h"
#include "runtime/tuple_row.h"
#include "util/file_utils.h"

namespace doris {

class DataSplitTest : public testing::Test {
public:
    DataSplitTest() {
        init_desc_tbl();
        init_row_desc();
        init_runtime_state();
    }
    ~DataSplitTest() {}

    void init_desc_tbl();

    void init_row_desc();

    void init_runtime_state();

protected:
    virtual void SetUp() {}

    virtual void TearDown() {}

private:
    ObjectPool _obj_pool;
    TDescriptorTable _t_desc_tbl;
    DescriptorTbl* _desc_tbl;
    RowDescriptor* _row_desc;
    RuntimeState* _state;
    MemPool _tuple_pool;
    ExecEnv _exec_env;
};

void DataSplitTest::init_runtime_state() {
    _state = _obj_pool.add(new RuntimeState("2011-10-01 12:34:56"));
    _state->set_desc_tbl(_desc_tbl);
    _state->_exec_env = &_exec_env;
    _state->_import_label = "zc";
    _state->_db_name = "test";
}

void DataSplitTest::init_row_desc() {
    std::vector<TTupleId> row_tuples;
    row_tuples.push_back(0);
    std::vector<bool> nullable_tuples;
    nullable_tuples.push_back(false);

    _row_desc = _obj_pool.add(new RowDescriptor(*_desc_tbl, row_tuples, nullable_tuples));
}

void DataSplitTest::init_desc_tbl() {
    // slot desc
    std::vector<TSlotDescriptor> slot_descs;

    // 1 byte null, 4 byte int, 4 byte int
    // slot 0
    {
        TSlotDescriptor t_slot_desc;
        t_slot_desc.__set_id(0);
        t_slot_desc.__set_parent(0);
        t_slot_desc.__set_slotType(gen_type_desc(TPrimitiveType::INT));
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
    // slot 2
    {
        TSlotDescriptor t_slot_desc;
        t_slot_desc.__set_id(2);
        t_slot_desc.__set_parent(0);
        t_slot_desc.__set_slotType(TPrimitiveType::INT);
        t_slot_desc.__set_columnPos(2);
        t_slot_desc.__set_byteOffset(9);
        t_slot_desc.__set_nullIndicatorByte(0);
        t_slot_desc.__set_nullIndicatorBit(4);
        t_slot_desc.__set_colName("col3");
        t_slot_desc.__set_slotIdx(2);
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

TEST_F(DataSplitTest, NoData) {
    TDataSplitSink t_sink;

    TRollupSchema t_schema;
    // Two Keys
    {
        TExpr expr;
        TExprNode node;
        node.node_type = TExprNodeType::SLOT_REF;
        node.type.type = TPrimitiveType::INT;
        node.num_children = 0;
        TSlotRef ref;
        ref.tuple_id = 0;
        ref.slot_id = 0;
        node.__set_slot_ref(ref);
        expr.nodes.push_back(node);
        t_schema.keys.push_back(expr);
    }
    {
        TExpr expr;
        TExprNode node;
        node.node_type = TExprNodeType::SLOT_REF;
        node.type.type = TPrimitiveType::INT;
        node.num_children = 0;
        TSlotRef ref;
        ref.tuple_id = 0;
        ref.slot_id = 1;
        node.__set_slot_ref(ref);
        expr.nodes.push_back(node);
        t_schema.keys.push_back(expr);
    }
    // Two Values
    {
        TExpr expr;
        TExprNode node;
        node.node_type = TExprNodeType::SLOT_REF;
        node.type.type = TPrimitiveType::INT;
        node.num_children = 0;
        TSlotRef ref;
        ref.tuple_id = 0;
        ref.slot_id = 2;
        node.__set_slot_ref(ref);
        expr.nodes.push_back(node);
        t_schema.values.push_back(expr);
    }
    // Two ValueOps
    t_schema.value_ops.push_back(TAggregationType::SUM);

    t_sink.rollup_schemas.insert(std::make_pair("base", t_schema));

    TRangePartition t_partition;
    // Two Keys
    {
        TExpr expr;
        TExprNode node;
        node.node_type = TExprNodeType::SLOT_REF;
        node.type.type = TPrimitiveType::INT;
        node.num_children = 0;
        TSlotRef ref;
        ref.tuple_id = 0;
        ref.slot_id = 1;
        node.__set_slot_ref(ref);
        expr.nodes.push_back(node);
        t_partition.distributed_exprs.push_back(expr);
    }
    t_partition.range.start_key.sign = -1;
    t_partition.range.end_key.sign = 1;
    t_partition.distribute_bucket = 1;

    t_sink.partition_infos.push_back(t_partition);

    DataSpliter spliter(*_row_desc);
    ASSERT_TRUE(DataSpliter::from_thrift(&_obj_pool, t_sink, &spliter).ok());
    ASSERT_TRUE(spliter.init(_state).ok());
    RowBatch batch(*_row_desc, 1024);
    {
        int idx = batch.add_row();
        TupleRow* row = batch.get_row(idx);
        Tuple* tuple = Tuple::create(13, &_tuple_pool);
        row->set_tuple(0, tuple);
        char* pos = (char*)tuple;
        memset(pos, 0, 13);
        *(int*)(pos + 1) = 1;
        *(int*)(pos + 5) = 10;
        *(int*)(pos + 9) = 100;
        batch.commit_last_row();
    }
    ASSERT_TRUE(spliter.send(_state, &batch).ok());
    ASSERT_TRUE(spliter.close(_state, Status::OK()).ok());
}

} // namespace doris

int main(int argc, char** argv) {
    std::string conffile = std::string(getenv("DORIS_HOME")) + "/conf/be.conf";
    if (!doris::config::init(conffile.c_str(), false)) {
        fprintf(stderr, "error read config file. \n");
        return -1;
    }
    // 覆盖be.conf中的配置
    doris::config::storage_root_path = "./test_run/mini_load";
    doris::FileUtils::create_dir(doris::config::storage_root_path);
    doris::touch_all_singleton();

    doris::CpuInfo::init();
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
