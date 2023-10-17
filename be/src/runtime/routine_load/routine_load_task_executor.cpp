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

#include <gen_cpp/BackendService_types.h>
#include <gen_cpp/FrontendService_types.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/Status_types.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/internal_service.pb.h>
#include <librdkafka/rdkafkacpp.h>
#include <stddef.h>

#include <algorithm>
#include <future>
#include <map>
#include <ostream>
#include <thread>
#include <utility>

// IWYU pragma: no_include <opentelemetry/common/threadlocal.h>
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "common/utils.h"
#include "io/fs/kafka_consumer_pipe.h"
#include "io/fs/multi_table_pipe.h"
#include "io/fs/stream_load_pipe.h"
#include "runtime/exec_env.h"
#include "runtime/message_body_sink.h"
#include "runtime/routine_load/data_consumer.h"
#include "runtime/routine_load/data_consumer_group.h"
#include "runtime/stream_load/new_load_stream_mgr.h"
#include "runtime/stream_load/stream_load_context.h"
#include "runtime/stream_load/stream_load_executor.h"
#include "service/backend_options.h"
#include "util/defer_op.h"
#include "util/doris_metrics.h"
#include "util/metrics.h"
#include "util/slice.h"
#include "util/time.h"
#include "util/uid_util.h"

namespace doris {
using namespace ErrorCode;

DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(routine_load_task_count, MetricUnit::NOUNIT);

RoutineLoadTaskExecutor::RoutineLoadTaskExecutor(ExecEnv* exec_env)
        : _exec_env(exec_env),
          _thread_pool(config::routine_load_thread_pool_size, config::routine_load_thread_pool_size,
                       "routine_load"),
          _data_consumer_pool(config::routine_load_consumer_pool_size) {
    REGISTER_HOOK_METRIC(routine_load_task_count, [this]() {
        // std::lock_guard<std::mutex> l(_lock);
        return _task_map.size();
    });

    static_cast<void>(_data_consumer_pool.start_bg_worker());
}

RoutineLoadTaskExecutor::~RoutineLoadTaskExecutor() {
    LOG(INFO) << _task_map.size() << " not executed tasks left, cleanup";
    _task_map.clear();
}

void RoutineLoadTaskExecutor::stop() {
    DEREGISTER_HOOK_METRIC(routine_load_task_count);
    _thread_pool.shutdown();
    _thread_pool.join();
    _data_consumer_pool.stop();
}

// Create a temp StreamLoadContext and set some kafka connection info in it.
// So that we can use this ctx to get kafka data consumer instance.
Status RoutineLoadTaskExecutor::_prepare_ctx(const PKafkaMetaProxyRequest& request,
                                             std::shared_ptr<StreamLoadContext> ctx) {
    ctx->load_type = TLoadType::ROUTINE_LOAD;
    ctx->load_src_type = TLoadSourceType::KAFKA;
    ctx->label = "NaN";

    // convert PKafkaInfo to TKafkaLoadInfo
    TKafkaLoadInfo t_info;
    t_info.brokers = request.kafka_info().brokers();
    t_info.topic = request.kafka_info().topic();
    std::map<std::string, std::string> properties;
    for (int i = 0; i < request.kafka_info().properties_size(); ++i) {
        const PStringPair& pair = request.kafka_info().properties(i);
        properties.emplace(pair.key(), pair.val());
    }
    t_info.__set_properties(std::move(properties));

    ctx->kafka_info.reset(new KafkaLoadInfo(t_info));
    ctx->need_rollback = false;
    return Status::OK();
}

Status RoutineLoadTaskExecutor::get_kafka_partition_meta(const PKafkaMetaProxyRequest& request,
                                                         std::vector<int32_t>* partition_ids) {
    CHECK(request.has_kafka_info());

    // This context is meaningless, just for unifing the interface
    std::shared_ptr<StreamLoadContext> ctx = std::make_shared<StreamLoadContext>(_exec_env);
    RETURN_IF_ERROR(_prepare_ctx(request, ctx));

    std::shared_ptr<DataConsumer> consumer;
    RETURN_IF_ERROR(_data_consumer_pool.get_consumer(ctx, &consumer));

    Status st = std::static_pointer_cast<KafkaDataConsumer>(consumer)->get_partition_meta(
            partition_ids);
    if (st.ok()) {
        _data_consumer_pool.return_consumer(consumer);
    }
    return st;
}

Status RoutineLoadTaskExecutor::get_kafka_partition_offsets_for_times(
        const PKafkaMetaProxyRequest& request, std::vector<PIntegerPair>* partition_offsets) {
    CHECK(request.has_kafka_info());

    // This context is meaningless, just for unifing the interface
    std::shared_ptr<StreamLoadContext> ctx = std::make_shared<StreamLoadContext>(_exec_env);
    RETURN_IF_ERROR(_prepare_ctx(request, ctx));

    std::shared_ptr<DataConsumer> consumer;
    RETURN_IF_ERROR(_data_consumer_pool.get_consumer(ctx, &consumer));

    Status st = std::static_pointer_cast<KafkaDataConsumer>(consumer)->get_offsets_for_times(
            std::vector<PIntegerPair>(request.offset_times().begin(), request.offset_times().end()),
            partition_offsets);
    if (st.ok()) {
        _data_consumer_pool.return_consumer(consumer);
    }
    return st;
}

Status RoutineLoadTaskExecutor::get_kafka_latest_offsets_for_partitions(
        const PKafkaMetaProxyRequest& request, std::vector<PIntegerPair>* partition_offsets) {
    CHECK(request.has_kafka_info());

    // This context is meaningless, just for unifing the interface
    std::shared_ptr<StreamLoadContext> ctx = std::make_shared<StreamLoadContext>(_exec_env);
    RETURN_IF_ERROR(_prepare_ctx(request, ctx));

    std::shared_ptr<DataConsumer> consumer;
    RETURN_IF_ERROR(_data_consumer_pool.get_consumer(ctx, &consumer));

    Status st =
            std::static_pointer_cast<KafkaDataConsumer>(consumer)
                    ->get_latest_offsets_for_partitions(
                            std::vector<int32_t>(request.partition_id_for_latest_offsets().begin(),
                                                 request.partition_id_for_latest_offsets().end()),
                            partition_offsets);
    if (st.ok()) {
        _data_consumer_pool.return_consumer(consumer);
    }
    return st;
}

Status RoutineLoadTaskExecutor::submit_task(const TRoutineLoadTask& task) {
    std::unique_lock<std::mutex> l(_lock);
    if (_task_map.find(task.id) != _task_map.end()) {
        // already submitted
        LOG(INFO) << "routine load task " << UniqueId(task.id) << " has already been submitted";
        return Status::OK();
    }

    if (_task_map.size() >= config::routine_load_thread_pool_size) {
        LOG(INFO) << "too many tasks in thread pool. reject task: " << UniqueId(task.id)
                  << ", job id: " << task.job_id
                  << ", queue size: " << _thread_pool.get_queue_size()
                  << ", current tasks num: " << _task_map.size();
        return Status::TooManyTasks("{}_{}", UniqueId(task.id).to_string(),
                                    BackendOptions::get_localhost());
    }

    // create the context
    std::shared_ptr<StreamLoadContext> ctx = std::make_shared<StreamLoadContext>(_exec_env);
    ctx->load_type = TLoadType::ROUTINE_LOAD;
    ctx->load_src_type = task.type;
    ctx->job_id = task.job_id;
    ctx->id = UniqueId(task.id);
    ctx->txn_id = task.txn_id;
    ctx->db = task.db;
    ctx->table = task.tbl;
    ctx->label = task.label;
    ctx->auth.auth_code = task.auth_code;

    if (task.__isset.max_interval_s) {
        ctx->max_interval_s = task.max_interval_s;
    }
    if (task.__isset.max_batch_rows) {
        ctx->max_batch_rows = task.max_batch_rows;
    }
    if (task.__isset.max_batch_size) {
        ctx->max_batch_size = task.max_batch_size;
    }
    if (task.__isset.is_multi_table && task.is_multi_table) {
        ctx->is_multi_table = true;
    }

    // set execute plan params (only for non-single-stream-multi-table load)
    TStreamLoadPutResult put_result;
    TStatus tstatus;
    tstatus.status_code = TStatusCode::OK;
    put_result.status = tstatus;
    if (task.__isset.params) {
        put_result.params = task.params;
        put_result.__isset.params = true;
    } else {
        put_result.pipeline_params = task.pipeline_params;
        put_result.__isset.pipeline_params = true;
    }
    ctx->put_result = put_result;
    if (task.__isset.format) {
        ctx->format = task.format;
    }
    // the routine load task'txn has already began in FE.
    // so it need to rollback if encounter error.
    ctx->need_rollback = true;
    ctx->max_filter_ratio = 1.0;

    // set source related params
    switch (task.type) {
    case TLoadSourceType::KAFKA:
        ctx->kafka_info.reset(new KafkaLoadInfo(task.kafka_load_info));
        break;
    default:
        LOG(WARNING) << "unknown load source type: " << task.type;
        return Status::InternalError("unknown load source type");
    }

    VLOG_CRITICAL << "receive a new routine load task: " << ctx->brief();
    // register the task
    _task_map[ctx->id] = ctx;

    // offer the task to thread pool
    if (!_thread_pool.offer(std::bind<void>(
                &RoutineLoadTaskExecutor::exec_task, this, ctx, &_data_consumer_pool,
                [this](std::shared_ptr<StreamLoadContext> ctx) {
                    std::unique_lock<std::mutex> l(_lock);
                    ctx->exec_env()->new_load_stream_mgr()->remove(ctx->id);
                    _task_map.erase(ctx->id);
                    LOG(INFO) << "finished routine load task " << ctx->brief()
                              << ", status: " << ctx->status
                              << ", current tasks num: " << _task_map.size();
                }))) {
        // failed to submit task, clear and return
        LOG(WARNING) << "failed to submit routine load task: " << ctx->brief();
        ctx->exec_env()->new_load_stream_mgr()->remove(ctx->id);
        _task_map.erase(ctx->id);
        return Status::InternalError("failed to submit routine load task");
    } else {
        LOG(INFO) << "submit a new routine load task: " << ctx->brief()
                  << ", current tasks num: " << _task_map.size();
        return Status::OK();
    }
}

void RoutineLoadTaskExecutor::exec_task(std::shared_ptr<StreamLoadContext> ctx,
                                        DataConsumerPool* consumer_pool, ExecFinishCallback cb) {
#define HANDLE_ERROR(stmt, err_msg)                                        \
    do {                                                                   \
        Status _status_ = (stmt);                                          \
        if (UNLIKELY(!_status_.ok() && !_status_.is<PUBLISH_TIMEOUT>())) { \
            err_handler(ctx, _status_, err_msg);                           \
            cb(ctx);                                                       \
            return;                                                        \
        }                                                                  \
    } while (false);

    LOG(INFO) << "begin to execute routine load task: " << ctx->brief();

    // create data consumer group
    std::shared_ptr<DataConsumerGroup> consumer_grp;
    HANDLE_ERROR(consumer_pool->get_consumer_grp(ctx, &consumer_grp), "failed to get consumers");

    // create and set pipe
    std::shared_ptr<io::StreamLoadPipe> pipe;
    switch (ctx->load_src_type) {
    case TLoadSourceType::KAFKA: {
        if (ctx->is_multi_table) {
            LOG(INFO) << "recv single-stream-multi-table request, ctx=" << ctx->brief();
            pipe = std::make_shared<io::MultiTablePipe>(ctx);
        } else {
            pipe = std::make_shared<io::KafkaConsumerPipe>();
        }
        Status st = std::static_pointer_cast<KafkaDataConsumerGroup>(consumer_grp)
                            ->assign_topic_partitions(ctx);
        if (!st.ok()) {
            err_handler(ctx, st, st.to_string());
            cb(ctx);
            return;
        }
        break;
    }
    default: {
        std::stringstream ss;
        ss << "unknown routine load task type: " << ctx->load_type;
        err_handler(ctx, Status::Cancelled("Cancelled"), ss.str());
        cb(ctx);
        return;
    }
    }
    ctx->body_sink = pipe;
    ctx->pipe = pipe;

    // must put pipe before executing plan fragment
    HANDLE_ERROR(_exec_env->new_load_stream_mgr()->put(ctx->id, ctx), "failed to add pipe");

    if (!ctx->is_multi_table) {
        // only for normal load, single-stream-multi-table load will be planned during consuming
#ifndef BE_TEST
        // execute plan fragment, async
        HANDLE_ERROR(_exec_env->stream_load_executor()->execute_plan_fragment(ctx),
                     "failed to execute plan fragment");
#else
        // only for test
        HANDLE_ERROR(_execute_plan_for_test(ctx), "test failed");
#endif
    }

    std::shared_ptr<io::KafkaConsumerPipe> kafka_pipe =
            std::static_pointer_cast<io::KafkaConsumerPipe>(ctx->body_sink);

    // start to consume, this may block a while
    HANDLE_ERROR(consumer_grp->start_all(ctx, kafka_pipe), "consuming failed");

    if (ctx->is_multi_table) {
        // plan the rest of unplanned data
        auto multi_table_pipe = std::static_pointer_cast<io::MultiTablePipe>(ctx->body_sink);
        HANDLE_ERROR(multi_table_pipe->request_and_exec_plans(),
                     "multi tables task executes plan error");
        // need memory order
        multi_table_pipe->set_consume_finished();
        HANDLE_ERROR(kafka_pipe->finish(), "finish multi table task failed");
    }

    // wait for all consumers finished
    HANDLE_ERROR(ctx->future.get(), "consume failed");

    ctx->load_cost_millis = UnixMillis() - ctx->start_millis;

    // return the consumer back to pool
    // call this before commit txn, in case the next task can come very fast
    consumer_pool->return_consumers(consumer_grp.get());

    // commit txn
    HANDLE_ERROR(_exec_env->stream_load_executor()->commit_txn(ctx.get()), "commit failed");
    // commit kafka offset
    switch (ctx->load_src_type) {
    case TLoadSourceType::KAFKA: {
        std::shared_ptr<DataConsumer> consumer;
        Status st = _data_consumer_pool.get_consumer(ctx, &consumer);
        if (!st.ok()) {
            // Kafka Offset Commit is idempotent, Failure should not block the normal process
            // So just print a warning
            LOG(WARNING) << st;
            break;
        }

        std::vector<RdKafka::TopicPartition*> topic_partitions;
        for (auto& kv : ctx->kafka_info->cmt_offset) {
            // The offsets you commit are the offsets of the messages you want to read next
            RdKafka::TopicPartition* tp1 = RdKafka::TopicPartition::create(ctx->kafka_info->topic,
                                                                           kv.first, kv.second + 1);
            topic_partitions.push_back(tp1);
        }

        st = std::static_pointer_cast<KafkaDataConsumer>(consumer)->commit(topic_partitions);
        if (!st.ok()) {
            // Kafka Offset Commit is idempotent, Failure should not block the normal process
            // So just print a warning
            LOG(WARNING) << st;
        }
        _data_consumer_pool.return_consumer(consumer);

        // delete TopicPartition finally
        Defer delete_tp {[&topic_partitions]() {
            std::for_each(topic_partitions.begin(), topic_partitions.end(),
                          [](RdKafka::TopicPartition* tp1) { delete tp1; });
        }};
        break;
    }
    default:
        break;
    }
    cb(ctx);
}

void RoutineLoadTaskExecutor::err_handler(std::shared_ptr<StreamLoadContext> ctx, const Status& st,
                                          const std::string& err_msg) {
    LOG(WARNING) << err_msg << ", routine load task: " << ctx->brief(true);
    ctx->status = st;
    if (ctx->need_rollback) {
        _exec_env->stream_load_executor()->rollback_txn(ctx.get());
        ctx->need_rollback = false;
    }
    if (ctx->body_sink != nullptr) {
        ctx->body_sink->cancel(err_msg);
    }
}

// for test only
Status RoutineLoadTaskExecutor::_execute_plan_for_test(std::shared_ptr<StreamLoadContext> ctx) {
    auto mock_consumer = [this, ctx]() {
        std::shared_ptr<io::StreamLoadPipe> pipe = std::static_pointer_cast<io::StreamLoadPipe>(
                _exec_env->new_load_stream_mgr()->get(ctx->id)->body_sink);
        std::stringstream ss;
        while (true) {
            char one;
            int64_t len = 1;
            size_t read_bytes = 0;
            Slice result((uint8_t*)&one, len);
            Status st = pipe->read_at(0, result, &read_bytes);
            if (!st.ok()) {
                LOG(WARNING) << "read failed";
                ctx->promise.set_value(st);
                break;
            }

            if (read_bytes == 0) {
                ctx->promise.set_value(Status::OK());
                break;
            }

            if (one == '\n') {
                LOG(INFO) << "get line: " << ss.str();
                ss.str("");
                ctx->number_loaded_rows++;
            } else {
                ss << one;
            }
        }
    };

    std::thread t1(mock_consumer);
    t1.detach();
    return Status::OK();
}

} // namespace doris
