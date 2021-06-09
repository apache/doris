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

#include "runtime/routine_load/routine_load_task_executor.h"

#include <gtest/gtest.h>

#include "gen_cpp/BackendService_types.h"
#include "gen_cpp/FrontendService_types.h"
#include "gen_cpp/HeartbeatService_types.h"
#include "runtime/exec_env.h"
#include "runtime/stream_load/load_stream_mgr.h"
#include "runtime/stream_load/stream_load_executor.h"
#include "util/cpu_info.h"
#include "util/logging.h"

namespace doris {

using namespace RdKafka;

extern TLoadTxnBeginResult k_stream_load_begin_result;
extern TLoadTxnCommitResult k_stream_load_commit_result;
extern TLoadTxnRollbackResult k_stream_load_rollback_result;
extern TStreamLoadPutResult k_stream_load_put_result;

class RoutineLoadTaskExecutorTest : public testing::Test {
public:
    RoutineLoadTaskExecutorTest() {}
    virtual ~RoutineLoadTaskExecutorTest() {}

    void SetUp() override {
        k_stream_load_begin_result = TLoadTxnBeginResult();
        k_stream_load_commit_result = TLoadTxnCommitResult();
        k_stream_load_rollback_result = TLoadTxnRollbackResult();
        k_stream_load_put_result = TStreamLoadPutResult();

        _env._master_info = new TMasterInfo();
        _env._load_stream_mgr = new LoadStreamMgr();
        _env._stream_load_executor = new StreamLoadExecutor(&_env);

        config::routine_load_thread_pool_size = 5;
        config::max_consumer_num_per_group = 3;
    }

    void TearDown() override {
        delete _env._master_info;
        _env._master_info = nullptr;
        delete _env._load_stream_mgr;
        _env._load_stream_mgr = nullptr;
        delete _env._stream_load_executor;
        _env._stream_load_executor = nullptr;
    }

private:
    ExecEnv _env;
};

TEST_F(RoutineLoadTaskExecutorTest, exec_task) {
    TRoutineLoadTask task;
    task.type = TLoadSourceType::KAFKA;
    task.job_id = 1L;
    task.id = TUniqueId();
    task.txn_id = 4;
    task.auth_code = 5;
    task.__set_db("db1");
    task.__set_tbl("tbl1");
    task.__set_label("l1");
    task.__set_max_interval_s(5);
    task.__set_max_batch_rows(10);
    task.__set_max_batch_size(2048);

    TKafkaLoadInfo k_info;
    k_info.brokers = "127.0.0.1:9092";
    k_info.topic = "test";

    std::map<int32_t, int64_t> part_off;
    part_off[0] = 13L;
    k_info.__set_partition_begin_offset(part_off);

    task.__set_kafka_load_info(k_info);

    RoutineLoadTaskExecutor executor(&_env);

    // submit task
    Status st;
    st = executor.submit_task(task);
    ASSERT_TRUE(st.ok());

    usleep(200);
    k_info.brokers = "127.0.0.1:9092";
    task.__set_kafka_load_info(k_info);
    st = executor.submit_task(task);
    ASSERT_TRUE(st.ok());

    usleep(200);
    k_info.brokers = "192.0.0.2:9092";
    task.__set_kafka_load_info(k_info);
    st = executor.submit_task(task);
    ASSERT_TRUE(st.ok());

    usleep(200);
    k_info.brokers = "192.0.0.2:9092";
    task.__set_kafka_load_info(k_info);
    st = executor.submit_task(task);
    ASSERT_TRUE(st.ok());
}

} // namespace doris

int main(int argc, char* argv[]) {
    doris::CpuInfo::init();
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
