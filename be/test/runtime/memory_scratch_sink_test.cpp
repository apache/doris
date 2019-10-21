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

#include <gtest/gtest.h>
#include <iostream>
#include <stdlib.h>
#include <stdio.h>

#include "common/config.h"
#include "common/logging.h"
#include "exprs/expr.h"
#include "gen_cpp/DorisExternalService_types.h"
#include "gen_cpp/Types_types.h"
#include "gen_cpp/Exprs_types.h"
#include "olap/options.h"
#include "runtime/exec_env.h"
#include "runtime/memory_scratch_sink.h"
#include "runtime/mem_tracker.h"
#include "runtime/primitive_type.h"
#include "runtime/result_queue_mgr.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "runtime/thread_resource_mgr.h"
#include "runtime/tuple_row.h"
#include "util/blocking_queue.hpp"
#include "util/logging.h"
#include "testutil/desc_tbl_builder.h"

namespace doris {

class MemoryScratchSinkTest : public testing::Test {
public:
    MemoryScratchSinkTest() {
        // all below is just only for test MemoryScratchSink
        ResultQueueMgr* result_queue_mgr = new ResultQueueMgr();
        ThreadResourceMgr* thread_mgr = new ThreadResourceMgr();
        _exec_env._result_queue_mgr = result_queue_mgr;
        _exec_env._thread_mgr = thread_mgr;
        TQueryOptions query_options;
        query_options.batch_size = 1024;
        TUniqueId query_id;
        query_id.lo = 10;
        query_id.hi = 100;
        _runtime_state = new RuntimeState(query_id, query_options, TQueryGlobals(), &_exec_env);
        _runtime_state->init_instance_mem_tracker();
        _mem_tracker = new MemTracker(-1, "MemoryScratchSinkTest", _runtime_state->instance_mem_tracker());
        create_row_desc();
    }

    void create_row_desc() {
        TTupleDescriptor tuple_desc;
        tuple_desc.__set_id(0);
        tuple_desc.__set_byteSize(8);
        tuple_desc.__set_numNullBytes(0);
        TDescriptorTable thrift_desc_tbl;
        thrift_desc_tbl.tupleDescriptors.push_back(tuple_desc);
        TSlotDescriptor slot_desc;
        slot_desc.__set_id(0);
        slot_desc.__set_parent(0);

        slot_desc.slotType.types.push_back(TTypeNode());
        slot_desc.slotType.types.back().__isset.scalar_type = true;
        slot_desc.slotType.types.back().scalar_type.type = TPrimitiveType::BIGINT;

        slot_desc.__set_columnPos(0);
        slot_desc.__set_byteOffset(0);
        slot_desc.__set_nullIndicatorByte(0);
        slot_desc.__set_nullIndicatorBit(-1);
        slot_desc.__set_slotIdx(0);
        slot_desc.__set_isMaterialized(true);
        thrift_desc_tbl.slotDescriptors.push_back(slot_desc);
        EXPECT_TRUE(DescriptorTbl::create(&_pool, thrift_desc_tbl, &_desc_tbl).ok());
        _runtime_state->set_desc_tbl(_desc_tbl);

        vector<TTupleId> row_tids;
        row_tids.push_back(0);

        vector<bool> nullable_tuples;
        nullable_tuples.push_back(false);
        _row_desc = _pool.add(new RowDescriptor(*_desc_tbl, row_tids, nullable_tuples));
    }

    virtual ~MemoryScratchSinkTest() {
        delete _runtime_state;
        delete _mem_tracker;
    }

protected:
    virtual void SetUp() {
    }

private:
    ObjectPool _pool;
    ExecEnv _exec_env;
    std::vector<TExpr> _exprs;
    RuntimeState* _runtime_state;
    RowDescriptor* _row_desc;
    TMemoryScratchSink _tsink;
    MemTracker *_mem_tracker;
    DescriptorTbl* _desc_tbl;
};

TEST_F(MemoryScratchSinkTest, work_flow_normal) {
    MemoryScratchSink sink(*_row_desc, _exprs, _tsink);
    TDataSink data_sink;
    data_sink.memory_scratch_sink = _tsink;
    ASSERT_TRUE(sink.init(data_sink).ok());
    ASSERT_TRUE(sink.prepare(_runtime_state).ok());
    RowBatch row_batch(*_row_desc, 1024, _mem_tracker);
    row_batch.add_row();
    row_batch.commit_last_row();
    ASSERT_TRUE(sink.send(_runtime_state, &row_batch).ok());
    ASSERT_TRUE(sink.close(_runtime_state, Status::OK()).ok());
}

}

int main(int argc, char** argv) {
    std::string conffile = std::string(getenv("DORIS_HOME")) + "/conf/be.conf";
    if (!doris::config::init(conffile.c_str(), false)) {
        fprintf(stderr, "error read config file. \n");
        return -1;
    }
    doris::init_glog("be-test");
    ::testing::InitGoogleTest(&argc, argv);
    doris::CpuInfo::init();
    return RUN_ALL_TESTS();
}
