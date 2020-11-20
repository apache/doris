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

#include "runtime/dpp_sink.h"

#include <gtest/gtest.h>

#include "common/object_pool.h"
#include "exprs/expr.h"
#include "gen_cpp/Descriptors_types.h"
#include "runtime/descriptors.h"
#include "runtime/dpp_sink_internal.h"
#include "runtime/mem_pool.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "runtime/tuple.h"
#include "runtime/tuple_row.h"
#include "util/file_utils.h"

namespace doris {

class DppSinkTest : public testing::Test {
public:
    DppSinkTest() {
        init_desc_tbl();
        init_row_desc();
        init_runtime_state();
        init_rollups();
    }
    ~DppSinkTest() {}

    void init_desc_tbl();

    void init_row_desc();

    void init_runtime_state();

    void init_rollups();

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
    std::map<std::string, RollupSchema*> _rollups;
    ExecEnv _exec_env;
};

void DppSinkTest::init_rollups() {
    {
        RollupSchema* schema = _obj_pool.add(new RollupSchema());
        {
            SlotRef* ref = _obj_pool.add(new SlotRef(PrimitiveType::TYPE_INT, 1));
            schema->_keys.push_back(ref);
        }
        {
            SlotRef* ref = _obj_pool.add(new SlotRef(PrimitiveType::TYPE_INT, 5));
            schema->_keys.push_back(ref);
        }
        {
            SlotRef* ref = _obj_pool.add(new SlotRef(PrimitiveType::TYPE_INT, 9));
            schema->_values.push_back(ref);
            schema->_value_ops.push_back(TAggregationType::SUM);
        }
        _rollups.insert(std::make_pair("base", schema));
    }
    {
        RollupSchema* schema = _obj_pool.add(new RollupSchema());
        {
            SlotRef* ref = _obj_pool.add(new SlotRef(PrimitiveType::TYPE_INT, 0));
            schema->_keys.push_back(ref);
        }
        {
            SlotRef* ref = _obj_pool.add(new SlotRef(PrimitiveType::TYPE_INT, 8));
            schema->_values.push_back(ref);
            schema->_value_ops.push_back(TAggregationType::SUM);
        }
        _rollups.insert(std::make_pair("rollup", schema));
    }
}

void DppSinkTest::init_runtime_state() {
    _state = _obj_pool.add(new RuntimeState("2011-10-01 12:34:56"));
    _state->set_desc_tbl(_desc_tbl);
    _state->_load_dir = "./tmp_data";
    FileUtils::create_dir(_state->_load_dir);
    _state->_exec_env = &_exec_env;
}

void DppSinkTest::init_row_desc() {
    std::vector<TTupleId> row_tuples;
    row_tuples.push_back(0);
    std::vector<bool> nullable_tuples;
    nullable_tuples.push_back(false);

    _row_desc = _obj_pool.add(new RowDescriptor(*_desc_tbl, row_tuples, nullable_tuples));
}

void DppSinkTest::init_desc_tbl() {
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

TEST_F(DppSinkTest, NoData) {
    DppSink sink(*_row_desc, _rollups);
    ASSERT_TRUE(sink.init(_state).ok());
    RowBatch batch(*_row_desc, 1024);
    TabletDesc desc;
    desc.partition_id = 1;
    desc.bucket_id = 0;
    ASSERT_TRUE(sink.add_batch(_state, desc, &batch).ok());
    ASSERT_TRUE(sink.finish(_state).ok());
}

TEST_F(DppSinkTest, WithData) {
    DppSink sink(*_row_desc, _rollups);
    ASSERT_TRUE(sink.init(_state).ok());
    RowBatch batch(*_row_desc, 1024);
    TabletDesc desc;
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
    desc.partition_id = 1;
    desc.bucket_id = 1;
    ASSERT_TRUE(sink.add_batch(_state, desc, &batch).ok());
    // Add two time
    ASSERT_TRUE(sink.add_batch(_state, desc, &batch).ok());
    desc.partition_id = 1;
    desc.bucket_id = 1;
    ASSERT_TRUE(sink.add_batch(_state, desc, &batch).ok());
    ASSERT_TRUE(sink.finish(_state).ok());
}

} // namespace doris

int main(int argc, char** argv) {
    std::string conffile = std::string(getenv("DORIS_HOME")) + "/conf/be.conf";
    if (!doris::config::init(conffile.c_str(), false)) {
        fprintf(stderr, "error read config file. \n");
        return -1;
    }
    doris::CpuInfo::init();
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
