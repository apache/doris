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
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <librdkafka/rdkafkacpp.h>
#include <unistd.h>

#include <map>
#include <tuple>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "gtest/gtest_pred_impl.h"
#include "io/fs/stream_load_pipe.h"
#include "load/routine_load/kafka_share_consumer.h"
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

class TaskShareConsumer : public KafkaShareConsumer {
public:
    bool fail_consume = false;
    bool fail_pipeline = false;
    bool fail_ack = false;
    int cancel_calls = 0;
    std::vector<std::tuple<int32_t, int64_t, bool>> acknowledgements;

    bool match(std::shared_ptr<StreamLoadContext> ctx) override {
        return ctx->kafka_info->use_share_consumer;
    }

    Status consume(std::shared_ptr<StreamLoadContext> ctx) override {
        _ctx = ctx;
        reserve_records(2);
        _records = {{0, 100}, {1, 50}};
        _processed = 1;
        if (fail_consume) {
            return Status::InternalError("share consume failed");
        }
        auto pipe = std::static_pointer_cast<io::StreamLoadPipe>(ctx->body_sink);
        RETURN_IF_ERROR(pipe->append_with_line_delimiter("row", 3));
        if (fail_pipeline) {
            pipe->cancel("pipeline failed");
            return Status::OK();
        }
        return pipe->finish();
    }

    Status cancel(std::shared_ptr<StreamLoadContext> ctx) override {
        ++cancel_calls;
        return KafkaShareConsumer::cancel(ctx);
    }

protected:
    Status acknowledge_record(int32_t partition, int64_t offset, bool accept) override {
        if (accept) {
            EXPECT_FALSE(_ctx.lock()->need_rollback);
        }
        acknowledgements.emplace_back(partition, offset, accept);
        return Status::OK();
    }

    Status commit_acknowledgements() override {
        return fail_ack ? Status::InternalError("ack response lost") : Status::OK();
    }

private:
    std::weak_ptr<StreamLoadContext> _ctx;
};

class RoutineLoadTaskExecutorTest : public testing::Test {
public:
    RoutineLoadTaskExecutorTest() = default;
    ~RoutineLoadTaskExecutorTest() override = default;

    ExecEnv* _env = nullptr;
    std::shared_ptr<TaskShareConsumer> _share_consumer;
    std::shared_ptr<StreamLoadContext> _share_ctx;
    size_t _pooled_consumers = 0;

    void run_share_task(TStatusCode::type commit_status) {
        TKafkaLoadInfo info;
        info.__set_use_share_consumer(true);
        _share_ctx = std::make_shared<StreamLoadContext>(_env);
        _share_ctx->id = UniqueId::gen_uid();
        _share_ctx->job_id = 1;
        _share_ctx->txn_id = 4;
        _share_ctx->load_type = TLoadType::ROUTINE_LOAD;
        _share_ctx->load_src_type = TLoadSourceType::KAFKA;
        _share_ctx->kafka_info = std::make_unique<KafkaLoadInfo>(info);
        _share_ctx->need_rollback = true;
        k_stream_load_commit_result.status.status_code = commit_status;
        DataConsumerPool pool;
        pool._pool.push_back(_share_consumer);
        RoutineLoadTaskExecutor executor(_env);
        int finished = 0;
        executor.exec_task(_share_ctx, &pool, [&](auto ctx) { ++finished; });
        if (_share_ctx->load_status_future.valid()) {
            _share_ctx->load_status_future.wait();
        }
        EXPECT_EQ(finished, 1);
        _pooled_consumers = pool._pool.size();
        _env->new_load_stream_mgr()->remove(_share_ctx->id);
        executor.stop();
    }

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
        _share_consumer = std::make_shared<TaskShareConsumer>();
    }

    void TearDown() override {
        delete _env->cluster_info();
        _env->clear_new_load_stream_mgr();
        _env->clear_stream_load_executor();
    }
};

TEST_F(RoutineLoadTaskExecutorTest, ShareAcceptsOnlyAfterCommit) {
    run_share_task(TStatusCode::OK);
    EXPECT_EQ(_share_consumer->acknowledgements,
              (std::vector<std::tuple<int32_t, int64_t, bool>> {{0, 100, true}, {1, 50, false}}));
    EXPECT_EQ(_share_consumer->cancel_calls, 0);
    EXPECT_EQ(_pooled_consumers, 1);
}

TEST_F(RoutineLoadTaskExecutorTest, ShareAcceptsCommittedPublishTimeout) {
    run_share_task(TStatusCode::PUBLISH_TIMEOUT);
    EXPECT_EQ(_share_consumer->acknowledgements,
              (std::vector<std::tuple<int32_t, int64_t, bool>> {{0, 100, true}, {1, 50, false}}));
    EXPECT_EQ(_share_consumer->cancel_calls, 0);
    EXPECT_EQ(_pooled_consumers, 1);
}

TEST_F(RoutineLoadTaskExecutorTest, ShareReleasesAfterCommitFailure) {
    run_share_task(TStatusCode::INTERNAL_ERROR);
    EXPECT_EQ(_share_consumer->acknowledgements,
              (std::vector<std::tuple<int32_t, int64_t, bool>> {{0, 100, false}, {1, 50, false}}));
    EXPECT_EQ(_share_consumer->cancel_calls, 1);
    EXPECT_EQ(_pooled_consumers, 0);
}

TEST_F(RoutineLoadTaskExecutorTest, ShareReleasesAfterConsumeFailure) {
    _share_consumer->fail_consume = true;
    run_share_task(TStatusCode::OK);
    EXPECT_EQ(_share_consumer->acknowledgements,
              (std::vector<std::tuple<int32_t, int64_t, bool>> {{0, 100, false}, {1, 50, false}}));
    EXPECT_EQ(_share_consumer->cancel_calls, 1);
    EXPECT_EQ(_pooled_consumers, 0);
}

TEST_F(RoutineLoadTaskExecutorTest, ShareReleasesAfterPipelineFailure) {
    _share_consumer->fail_pipeline = true;
    run_share_task(TStatusCode::OK);
    EXPECT_EQ(_share_consumer->acknowledgements,
              (std::vector<std::tuple<int32_t, int64_t, bool>> {{0, 100, false}, {1, 50, false}}));
    EXPECT_EQ(_share_consumer->cancel_calls, 1);
    EXPECT_EQ(_pooled_consumers, 0);
}

TEST_F(RoutineLoadTaskExecutorTest, ShareAckFailureDoesNotReuseConsumerOrRollback) {
    _share_consumer->fail_ack = true;
    run_share_task(TStatusCode::OK);
    EXPECT_EQ(_share_consumer->acknowledgements,
              (std::vector<std::tuple<int32_t, int64_t, bool>> {{0, 100, true}, {1, 50, false}}));
    EXPECT_FALSE(_share_ctx->need_rollback);
    EXPECT_EQ(_pooled_consumers, 0);
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
