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

#include "load/routine_load/routine_load_task_executor.h"

#include <gen_cpp/BackendService_types.h>
#include <gen_cpp/FrontendService_types.h>
#include <gen_cpp/HeartbeatService_types.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/internal_service.pb.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <librdkafka/rdkafkacpp.h>
#include <unistd.h>

#include <limits>
#include <map>

#include "common/config.h"
#include "common/status.h"
#include "gtest/gtest_pred_impl.h"
#include "load/routine_load/data_consumer.h"
#include "load/stream_load/new_load_stream_mgr.h"
#include "load/stream_load/stream_load_executor.h"
#include "runtime/cluster_info.h"
#include "runtime/exec_env.h"

namespace doris {

using namespace RdKafka;

extern TLoadTxnBeginResult k_stream_load_begin_result;
extern TLoadTxnCommitResult k_stream_load_commit_result;
extern TLoadTxnRollbackResult k_stream_load_rollback_result;
extern TStreamLoadPutResult k_stream_load_put_result;

class RoutineLoadTaskExecutorTest : public testing::Test {
public:
    RoutineLoadTaskExecutorTest() = default;
    ~RoutineLoadTaskExecutorTest() override = default;

    ExecEnv* _env = nullptr;

    void SetUp() override {
        _env = ExecEnv::GetInstance();
        k_stream_load_begin_result = TLoadTxnBeginResult();
        k_stream_load_commit_result = TLoadTxnCommitResult();
        k_stream_load_rollback_result = TLoadTxnRollbackResult();
        k_stream_load_put_result = TStreamLoadPutResult();

        _env->set_cluster_info(new ClusterInfo());
        _env->set_new_load_stream_mgr(NewLoadStreamMgr::create_unique());
        _env->set_stream_load_executor(StreamLoadExecutor::create_unique(_env));

        config::max_routine_load_thread_pool_size = 1024;
        config::max_consumer_num_per_group = 3;
    }

    void TearDown() override {
        delete _env->cluster_info();
        _env->clear_new_load_stream_mgr();
        _env->clear_stream_load_executor();
    }
};

TEST_F(RoutineLoadTaskExecutorTest, KafkaExpiredDeadlineCannotBecomePositive) {
    // The clock calculation is shared by both watermark calls. Supply elapsed time directly
    // to reproduce a long-running request without a real wait or Kafka connection.
    EXPECT_EQ(KafkaDataConsumer::remaining_timeout_ms(0, 2147483648LL), 0);
    EXPECT_EQ(KafkaDataConsumer::remaining_timeout_ms(INT32_MIN, 1), 0);
    EXPECT_EQ(KafkaDataConsumer::remaining_timeout_ms(INT32_MAX, 1), INT32_MAX - 1);
    EXPECT_EQ(KafkaDataConsumer::remaining_timeout_ms(1, 1), 0);
    auto context = std::make_shared<StreamLoadContext>(_env);
    context->need_rollback = false;
    context->kafka_info = std::make_unique<KafkaLoadInfo>(TKafkaLoadInfo {});
    KafkaDataConsumer consumer(context);
    std::vector<PIntegerPair> offsets;
    auto status = consumer.get_latest_offsets_for_partitions({0}, &offsets, 0);
    EXPECT_NE(status.to_string().find("latest offsets for partitions timeout"), std::string::npos);
    PIntegerPair flag;
    flag.set_key(0);
    flag.set_val(-1);
    status = consumer.get_real_offsets_for_partitions({flag}, &offsets, 0);
    EXPECT_NE(status.to_string().find("real offsets for partitions timeout"), std::string::npos);
    EXPECT_TRUE(offsets.empty());
}

TEST_F(RoutineLoadTaskExecutorTest, RejectIntervalsOutsideMillisecondRange) {
    RoutineLoadTaskExecutor executor(_env);
    ASSERT_TRUE(executor.init(1024 * 1024).ok());
    // Zero makes the consumer loop exit before it reads any records. Reject it, negative
    // intervals, and millisecond overflow before creating a consumer. No broker or row is needed.
    for (int64_t seconds :
         {int64_t(-1), int64_t(0), std::numeric_limits<int64_t>::max() / 1000 + 1}) {
        SCOPED_TRACE(seconds);
        TRoutineLoadTask task;
        task.__set_max_interval_s(seconds);
        auto status = executor.submit_task(task);
        EXPECT_TRUE(status.is<ErrorCode::INVALID_ARGUMENT>()) << status;
        EXPECT_NE(status.to_string().find("Routine load interval out of range"), std::string::npos)
                << status;
        EXPECT_TRUE(executor._task_map.empty());
    }
    executor.stop();
}

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

    RoutineLoadTaskExecutor executor(_env);
    Status st;
    st = executor.init(1024 * 1024);
    EXPECT_TRUE(st.ok());
    // submit task
    st = executor.submit_task(task);
    EXPECT_TRUE(st.ok());

    usleep(200);
    k_info.brokers = "127.0.0.1:9092";
    task.__set_kafka_load_info(k_info);
    st = executor.submit_task(task);
    EXPECT_TRUE(st.ok());

    usleep(200);
    k_info.brokers = "192.0.0.2:9092";
    task.__set_kafka_load_info(k_info);
    st = executor.submit_task(task);
    EXPECT_TRUE(st.ok());

    usleep(200);
    k_info.brokers = "192.0.0.2:9092";
    task.__set_kafka_load_info(k_info);
    st = executor.submit_task(task);
    EXPECT_TRUE(st.ok());

    executor.stop();
}

} // namespace doris
