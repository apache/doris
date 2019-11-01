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

#include <vector>

#include "gperftools/profiler.h"
#include <gtest/gtest.h>

#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/Types_types.h"
#include "gen_cpp/Descriptors_types.h"  // for TTupleId
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "runtime/descriptors.h"
#include "runtime/tuple.h"
#include "runtime/free_pool.hpp"
#include "util/logging.h"
#include "util/debug_util.h"

#include "udf/udf.h"
#include "udf/udf_internal.h"

namespace doris {

class RecordStroreTest : public testing::Test {
public:
    RecordStroreTest() : _mem_pool(MemPool(new MemTracker())) {
    }

    ~RecordStroreTest() {}

protected:
    virtual void SetUp() {
        init_tuple_desc();
    }

    virtual void TearDown() {
    }

    void init_tuple_desc();

private:
    ObjectPool _obj_pool;
    TDescriptorTable _t_desc_table;
    DescriptorTbl *_desc_tbl;
    RuntimeState *_state;
    MemPool _mem_pool;
}; // end class RecordStroreTest

void RecordStroreTest::init_tuple_desc() {
    // TTableDescriptor
    TTableDescriptor t_table_desc;
    t_table_desc.id = 0;
    t_table_desc.tableType = TTableType::OLAP_TABLE;
    t_table_desc.numCols = 0;
    t_table_desc.numClusteringCols = 0;
    t_table_desc.olapTable.tableName = "test";
    t_table_desc.tableName = "test_table_name";
    t_table_desc.dbName = "test_db_name";
    t_table_desc.__isset.olapTable = true;

    _t_desc_table.tableDescriptors.push_back(t_table_desc);
    _t_desc_table.__isset.tableDescriptors = true;

    // TSlotDescriptor
    std::vector<TSlotDescriptor> slot_descs;
    int offset = 1;
    int i = 0;
    // UserId
    {
        TSlotDescriptor t_slot_desc;
        t_slot_desc.__set_id(i);
        t_slot_desc.__set_slotType(TypeDescriptor(TYPE_INT).to_thrift());
        t_slot_desc.__set_columnPos(i);
        t_slot_desc.__set_byteOffset(offset);
        t_slot_desc.__set_nullIndicatorByte(0);
        t_slot_desc.__set_nullIndicatorBit(-1);
        t_slot_desc.__set_slotIdx(i);
        t_slot_desc.__set_isMaterialized(true);
        t_slot_desc.__set_colName("column0");

        slot_descs.push_back(t_slot_desc);
        offset += sizeof(int32_t);
    }
    ++i;
    // column 2
    {
        TSlotDescriptor t_slot_desc;
        t_slot_desc.__set_id(i);
        t_slot_desc.__set_slotType(TypeDescriptor(TYPE_INT).to_thrift());
        t_slot_desc.__set_columnPos(i);
        t_slot_desc.__set_byteOffset(offset);
        t_slot_desc.__set_nullIndicatorByte(0);
        t_slot_desc.__set_nullIndicatorBit(1);
        t_slot_desc.__set_slotIdx(i);
        t_slot_desc.__set_isMaterialized(true);
        t_slot_desc.__set_colName("column1");

        slot_descs.push_back(t_slot_desc);
        offset += sizeof(int32_t);
    }
    ++i;
    // column 3: varchar
    {
        TSlotDescriptor t_slot_desc;
        t_slot_desc.__set_id(i);
        t_slot_desc.__set_slotType(TypeDescriptor(TYPE_VARCHAR).to_thrift());
        t_slot_desc.__set_columnPos(i);
        t_slot_desc.__set_byteOffset(offset);
        t_slot_desc.__set_nullIndicatorByte(0);
        t_slot_desc.__set_nullIndicatorBit(-1);
        t_slot_desc.__set_slotIdx(i);
        t_slot_desc.__set_isMaterialized(true);
        t_slot_desc.__set_colName("column2");

        slot_descs.push_back(t_slot_desc);
        offset += sizeof(StringValue);
    }

    _t_desc_table.__set_slotDescriptors(slot_descs);

    // TTupleDescriptor
    TTupleDescriptor t_tuple_desc;
    t_tuple_desc.id = 0;
    t_tuple_desc.byteSize = offset;
    t_tuple_desc.numNullBytes = 1;
    t_tuple_desc.tableId = 0;
    t_tuple_desc.__isset.tableId = true;
    _t_desc_table.tupleDescriptors.push_back(t_tuple_desc);

    DescriptorTbl::create(&_obj_pool, _t_desc_table, &_desc_tbl);
}
TEST_F(RecordStroreTest, normal_use) {
    std::cout << _desc_tbl->get_tuple_descriptor(0)->debug_string() << std::endl;
    doris_udf::RecordStore* store = RecordStoreImpl::create_record_store(new FreePool(&_mem_pool), _desc_tbl->get_tuple_descriptor(0));

    for (int i = 0; i < 5; ++i) {
        doris_udf::Record *record = store->allocate_record();
        // set index
        record->set_int(0, i);
        if (i % 2) {
            record->set_null(1);
        } else {
            record->set_int(1, i * 2);
        }
        record->set_int(1, i * 2);

        // set value
        char *ptr = (char*)store->allocate(7);
        memcpy(ptr, std::string("testVar").c_str(), 7);

        StringValue val(ptr, 7);
        record->set_string(2, (uint8_t*)val.ptr, val.len);
        
        store->append_record(record);
    }
    EXPECT_EQ(store->size(), 5);
    
    Tuple* tuple0 = reinterpret_cast<Tuple*> (store->get(0));
    Tuple* tuple1 = reinterpret_cast<Tuple*> (store->get(1));
    Tuple* tuple2 = reinterpret_cast<Tuple*> (store->get(2));
    Tuple* tuple3 = reinterpret_cast<Tuple*> (store->get(3));
    Tuple* tuple4 = reinterpret_cast<Tuple*> (store->get(4));

    EXPECT_EQ(Tuple::to_string(tuple0, *_desc_tbl->get_tuple_descriptor(0)), "(0 0 testVar)");
    EXPECT_EQ(Tuple::to_string(tuple1, *_desc_tbl->get_tuple_descriptor(0)), "(1 null testVar)");
    EXPECT_EQ(Tuple::to_string(tuple2, *_desc_tbl->get_tuple_descriptor(0)), "(2 4 testVar)");
    EXPECT_EQ(Tuple::to_string(tuple3, *_desc_tbl->get_tuple_descriptor(0)), "(3 null testVar)");
    EXPECT_EQ(Tuple::to_string(tuple4, *_desc_tbl->get_tuple_descriptor(0)), "(4 8 testVar)");
}

}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
