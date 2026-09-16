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

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/config.h"
#include "common/exception.h"
#include "common/logging.h"
#include "common/metrics/doris_metrics.h"
#include "common/metrics/metrics.h"
#include "common/status.h"
#include "common/utils.h"
#include "io/fs/kafka_consumer_pipe.h"
#include "io/fs/kinesis_consumer_pipe.h"
#include "io/fs/multi_table_pipe.h"
#include "io/fs/stream_load_pipe.h"
#include "load/message_body_sink.h"
#include "load/routine_load/data_consumer.h"
#include "load/routine_load/data_consumer_group.h"
#include "load/stream_load/new_load_stream_mgr.h"
#include "load/stream_load/stream_load_context.h"
#include "load/stream_load/stream_load_executor.h"
#include "runtime/cluster_info.h"
#include "runtime/exec_env.h"
#include "runtime/memory/memory_profile.h"
#include "service/backend_options.h"
#include "util/defer_op.h"
#include "util/slice.h"
#include "util/time.h"
#include "util/uid_util.h"

namespace doris {
using namespace ErrorCode;

DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(routine_load_task_count, MetricUnit::NOUNIT);

bvar::LatencyRecorder g_routine_load_commit_and_publish_latency_ms("routine_load",
                                                                   "commit_and_publish_ms");

RoutineLoadTaskExecutor::RoutineLoadTaskExecutor(ExecEnv* exec_env) : _exec_env(exec_env) {
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

Status RoutineLoadTaskExecutor::init(int64_t process_mem_limit) {
    _load_mem_limit = process_mem_limit * config::load_process_max_memory_limit_percent / 100;
    RETURN_IF_ERROR(ThreadPoolBuilder("routine_load")
                            .set_min_threads(0)
                            .set_max_threads(config::max_routine_load_thread_pool_size)
                            .set_max_queue_size(config::max_routine_load_thread_pool_size)
                            .build(&_thread_pool));
    if (config::kinesis_latest_sequence_request_timeout_ms <= 0) {
        return Status::InvalidArgument(
                "kinesis_latest_sequence_request_timeout_ms must be positive");
    }
    return ThreadPoolBuilder("kinesis_latest_scan")
            .set_min_threads(0)
            .set_max_threads(config::kinesis_latest_sequence_scan_threads)
            .set_max_queue_size(1024)
            .build(&_kinesis_scan_pool);
}

void RoutineLoadTaskExecutor::stop() {
    DEREGISTER_HOOK_METRIC(routine_load_task_count);
    _kinesis_scan_stopping = true;
    if (_kinesis_scan_pool) {
        // Drain cancelled workers so every pending RPC still runs its completion callback.
        _kinesis_scan_pool->wait();
        _kinesis_scan_pool->shutdown();
    }
    if (_thread_pool) {
        _thread_pool->shutdown();
    }
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

Status RoutineLoadTaskExecutor::_prepare_ctx(const PKinesisMetaProxyRequest& request,
                                             std::shared_ptr<StreamLoadContext> ctx) {
    ctx->load_type = TLoadType::ROUTINE_LOAD;
    ctx->load_src_type = TLoadSourceType::KINESIS;
    ctx->label = "NaN";

    // convert PKinesisLoadInfo to TKinesisLoadInfo
    TKinesisLoadInfo t_info;
    t_info.region = request.kinesis_info().region();
    t_info.stream = request.kinesis_info().stream();
    if (request.kinesis_info().has_endpoint()) {
        t_info.__set_endpoint(request.kinesis_info().endpoint());
    }
    std::map<std::string, std::string> properties;
    for (int i = 0; i < request.kinesis_info().properties_size(); ++i) {
        const PStringPair& pair = request.kinesis_info().properties(i);
        properties.emplace(pair.key(), pair.val());
    }
    t_info.__set_properties(std::move(properties));

    ctx->kinesis_info.reset(new KinesisLoadInfo(t_info));
    ctx->need_rollback = false;
    return Status::OK();
}

Status RoutineLoadTaskExecutor::get_kinesis_shard_meta(const PKinesisMetaProxyRequest& request,
                                                       std::vector<std::string>* shard_ids) {
    CHECK(request.has_kinesis_info());

    std::shared_ptr<StreamLoadContext> ctx = std::make_shared<StreamLoadContext>(_exec_env);
    RETURN_IF_ERROR(_prepare_ctx(request, ctx));

    std::shared_ptr<DataConsumer> consumer;
    RETURN_IF_ERROR(_data_consumer_pool.get_consumer(ctx, &consumer));

    Status st = std::static_pointer_cast<KinesisDataConsumer>(consumer)->get_shard_list(shard_ids);
    if (st.ok()) {
        _data_consumer_pool.return_consumer(consumer);
    }
    return st;
}

// All workers of one RPC share a deadline and publish results only after all workers exit.
struct KinesisLatestSequenceBatch {
    PKinesisMetaProxyRequest request;
    int64_t deadline_ms;
    std::function<bool()> is_cancelled;
    RoutineLoadTaskExecutor::KinesisScanCallback on_finish;
    std::atomic<int> next_shard {0};
    std::atomic<int> remaining_workers;
    std::mutex mutex;
    Status status;
    std::map<std::string, std::string> sequences;

    KinesisLatestSequenceBatch(PKinesisMetaProxyRequest req, int64_t timeout_ms,
                               std::function<bool()> cancelled,
                               RoutineLoadTaskExecutor::KinesisScanCallback finish, int workers)
            : request(std::move(req)),
              deadline_ms(timeout_ms == -1 ? -1 : MonotonicMillis() + timeout_ms),
              is_cancelled(std::move(cancelled)),
              on_finish(std::move(finish)),
              remaining_workers(workers) {}

    Status check_status() {
        std::lock_guard<std::mutex> lock(mutex);
        RETURN_IF_ERROR(status);
        if (is_cancelled()) {
            return Status::Cancelled("Kinesis latest sequence scan cancelled");
        }
        if (deadline_ms != -1 && MonotonicMillis() >= deadline_ms) {
            return Status::TimedOut("Kinesis latest sequence scan exceeded its total timeout");
        }
        return Status::OK();
    }

    void finish_worker(const Status& worker_status) {
        {
            std::lock_guard<std::mutex> lock(mutex);
            if (status.ok() && !worker_status.ok()) {
                status = worker_status;
            }
        }
        if (remaining_workers.fetch_sub(1) == 1) {
            // No worker or SDK callback may access the RPC controller after on_finish runs.
            Status final_status = check_status();
            on_finish(final_status, sequences);
        }
    }
};

Status RoutineLoadTaskExecutor::_run_kinesis_scan_worker(
        const std::shared_ptr<KinesisLatestSequenceBatch>& batch) {
    RETURN_IF_ERROR(batch->check_status());
    auto ctx = std::make_shared<StreamLoadContext>(_exec_env);
    RETURN_IF_ERROR(_prepare_ctx(batch->request, ctx));
    // Each worker owns a client. Never share or return an in-flight scan consumer.
    KinesisDataConsumer consumer(ctx, config::kinesis_latest_sequence_request_timeout_ms);
    RETURN_IF_ERROR(consumer.init(ctx));
    while (true) {
        RETURN_IF_ERROR(batch->check_status());
        int index = batch->next_shard.fetch_add(1);
        if (index >= batch->request.shard_ids_for_latest_sequences_size()) {
            return Status::OK();
        }
        const auto& shard = batch->request.shard_ids_for_latest_sequences(index);
        std::string sequence;
        RETURN_IF_ERROR(consumer.get_latest_sequence_number(
                shard, [batch] { return batch->check_status(); }, &sequence));
        std::lock_guard<std::mutex> lock(batch->mutex);
        batch->sequences.emplace(shard, std::move(sequence));
    }
}

void RoutineLoadTaskExecutor::get_kinesis_latest_sequence_numbers(
        const PKinesisMetaProxyRequest& request, int64_t timeout_ms,
        std::function<bool()> is_cancelled, KinesisScanCallback on_finish) {
    CHECK(request.has_kinesis_info());
    int workers = std::min(request.shard_ids_for_latest_sequences_size(),
                           config::kinesis_latest_sequence_scan_threads);
    DCHECK_GT(workers, 0);
    auto batch = std::make_shared<KinesisLatestSequenceBatch>(
            request, timeout_ms,
            [this, is_cancelled = std::move(is_cancelled)] {
                return _kinesis_scan_stopping.load() || is_cancelled();
            },
            std::move(on_finish), workers);
    for (int i = 0; i < workers; ++i) {
        auto st = _kinesis_scan_pool->submit_func([this, batch] {
            Status worker_status;
            try {
                worker_status = _run_kinesis_scan_worker(batch);
            } catch (const Exception& e) {
                worker_status = Status::Error<false>(e.code(), e.to_string());
            } catch (const std::exception& e) {
                worker_status =
                        Status::InternalError("Kinesis latest sequence scan failed: {}", e.what());
            }
            batch->finish_worker(worker_status);
        });
        if (!st.ok()) {
            batch->finish_worker(st);
        }
    }
}

Status RoutineLoadTaskExecutor::get_kafka_partition_offsets_for_times(
        const PKafkaMetaProxyRequest& request, std::vector<PIntegerPair>* partition_offsets,
        int timeout) {
    CHECK(request.has_kafka_info());

    // This context is meaningless, just for unifing the interface
    std::shared_ptr<StreamLoadContext> ctx = std::make_shared<StreamLoadContext>(_exec_env);
    RETURN_IF_ERROR(_prepare_ctx(request, ctx));

    std::shared_ptr<DataConsumer> consumer;
    RETURN_IF_ERROR(_data_consumer_pool.get_consumer(ctx, &consumer));

    Status st = std::static_pointer_cast<KafkaDataConsumer>(consumer)->get_offsets_for_times(
            std::vector<PIntegerPair>(request.offset_times().begin(), request.offset_times().end()),
            partition_offsets, timeout);
    if (st.ok()) {
        _data_consumer_pool.return_consumer(consumer);
    }
    return st;
}

Status RoutineLoadTaskExecutor::get_kafka_latest_offsets_for_partitions(
        const PKafkaMetaProxyRequest& request, std::vector<PIntegerPair>* partition_offsets,
        int timeout) {
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
                            partition_offsets, timeout);
    if (st.ok()) {
        _data_consumer_pool.return_consumer(consumer);
    }
    return st;
}

Status RoutineLoadTaskExecutor::get_kafka_real_offsets_for_partitions(
        const PKafkaMetaProxyRequest& request, std::vector<PIntegerPair>* partition_offsets,
        int timeout) {
    CHECK(request.has_kafka_info());

    // This context is meaningless, just for unifing the interface
    std::shared_ptr<StreamLoadContext> ctx = std::make_shared<StreamLoadContext>(_exec_env);
    RETURN_IF_ERROR(_prepare_ctx(request, ctx));

    std::shared_ptr<DataConsumer> consumer;
    RETURN_IF_ERROR(_data_consumer_pool.get_consumer(ctx, &consumer));

    Status st =
            std::static_pointer_cast<KafkaDataConsumer>(consumer)->get_real_offsets_for_partitions(
                    std::vector<PIntegerPair>(request.offset_flags().begin(),
                                              request.offset_flags().end()),
                    partition_offsets, timeout);
    if (st.ok()) {
        _data_consumer_pool.return_consumer(consumer);
    }
    return st;
}

Status RoutineLoadTaskExecutor::submit_task(const TRoutineLoadTask& task) {
    std::unique_lock<std::mutex> l(_lock);
    // check if already submitted
    if (_task_map.find(task.id) != _task_map.end()) {
        LOG(INFO) << "routine load task " << UniqueId(task.id) << " has already been submitted";
        return Status::OK();
    }

    // check task num limit
    if (_task_map.size() >= config::max_routine_load_thread_pool_size) {
        LOG(INFO) << "too many tasks in thread pool. reject task: " << UniqueId(task.id)
                  << ", job id: " << task.job_id
                  << ", queue size: " << _thread_pool->get_queue_size()
                  << ", current tasks num: " << _task_map.size();
        return Status::TooManyTasks("{}_{}", UniqueId(task.id).to_string(),
                                    BackendOptions::get_localhost());
    }

    // check memory limit
    std::string reason;
    DBUG_EXECUTE_IF("RoutineLoadTaskExecutor.submit_task.memory_limit", {
        _reach_memory_limit(reason);
        return Status::MemoryLimitExceeded("fake reason: " + reason);
    });
    if (_reach_memory_limit(reason)) {
        LOG(INFO) << "reach memory limit. reject task: " << UniqueId(task.id)
                  << ", job id: " << task.job_id << ", reason: " << reason;
        return Status::MemoryLimitExceeded(reason);
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
    // deprecated, removed in 3.1, use auth token instead.
    ctx->auth.auth_code = task.auth_code;
    ctx->auth.token = _exec_env->cluster_info()->curr_auth_token;

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
    if (task.__isset.memtable_on_sink_node) {
        ctx->memtable_on_sink_node = task.memtable_on_sink_node;
    }
    if (task.__isset.qualified_user) {
        ctx->qualified_user = task.qualified_user;
    }
    if (task.__isset.cloud_cluster) {
        ctx->cloud_cluster = task.cloud_cluster;
    }

    // set execute plan params (only for non-single-stream-multi-table load)
    TStreamLoadPutResult put_result;
    TStatus tstatus;
    tstatus.status_code = TStatusCode::OK;
    put_result.status = tstatus;

    put_result.pipeline_params = task.pipeline_params;
    put_result.__isset.pipeline_params = true;
    if (task.pipeline_params.__isset.file_scan_params &&
        task.pipeline_params.file_scan_params.size() > 0) {
        const auto& file_scan_range_param = task.pipeline_params.file_scan_params.begin()->second;
        if (file_scan_range_param.__isset.strict_mode && file_scan_range_param.strict_mode) {
            put_result.pipeline_params.query_options.__set_enable_insert_strict(true);
        }
    } else if (task.pipeline_params.local_params.size() > 0 &&
               task.pipeline_params.local_params[0].per_node_scan_ranges.size() > 0) {
        auto scan_ranges =
                task.pipeline_params.local_params[0].per_node_scan_ranges.begin()->second;
        if (scan_ranges.size() > 0 &&
            scan_ranges[0].scan_range.ext_scan_range.file_scan_range.__isset.params) {
            const auto& params = scan_ranges[0].scan_range.ext_scan_range.file_scan_range.params;
            if (params.__isset.strict_mode) {
                put_result.pipeline_params.query_options.__set_enable_insert_strict(
                        params.strict_mode);
            }
        }
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
    case TLoadSourceType::KINESIS:
        ctx->kinesis_info.reset(new KinesisLoadInfo(task.kinesis_load_info));
        break;
    default:
        LOG(WARNING) << "unknown load source type: " << task.type;
        return Status::InternalError("unknown load source type");
    }

    VLOG_CRITICAL << "receive a new routine load task: " << ctx->brief();
    // register the task
    _task_map[ctx->id] = ctx;

    // offer the task to thread pool
    if (!_thread_pool->submit_func(std::bind<void>(
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

bool RoutineLoadTaskExecutor::_reach_memory_limit(std::string& reason) {
    DBUG_EXECUTE_IF("RoutineLoadTaskExecutor.submit_task.memory_limit", {
        reason = "reach memory limit";
        return true;
    });
    bool is_exceed_soft_mem_limit = GlobalMemoryArbitrator::is_exceed_soft_mem_limit();
    auto current_load_mem_value = MemoryProfile::load_current_usage();
    if (is_exceed_soft_mem_limit || current_load_mem_value > _load_mem_limit) {
        reason = "is_exceed_soft_mem_limit: " + std::to_string(is_exceed_soft_mem_limit) +
                 " current_load_mem_value: " + std::to_string(current_load_mem_value) +
                 " _load_mem_limit: " + std::to_string(_load_mem_limit);
        return true;
    }
    return false;
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

#define HANDLE_MULTI_TABLE_ERROR(stmt, err_msg)                            \
    do {                                                                   \
        Status _status_ = (stmt);                                          \
        if (UNLIKELY(!_status_.ok() && !_status_.is<PUBLISH_TIMEOUT>())) { \
            err_handler(ctx, _status_, err_msg);                           \
            cb(ctx);                                                       \
            _status_ = ctx->load_status_future.get();                      \
            if (!_status_.ok()) {                                          \
                LOG(ERROR) << "failed to get future, " << ctx->brief();    \
            }                                                              \
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
            LOG(INFO) << "recv single-stream-multi-table request, ctx: " << ctx->brief();
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
    case TLoadSourceType::KINESIS: {
        if (ctx->is_multi_table) {
            err_handler(ctx, Status::Cancelled("Cancelled"),
                        "Kinesis doesn't support multi-table yet");
            cb(ctx);
            return;
        } else {
            pipe = std::make_shared<io::KinesisConsumerPipe>();
        }
        Status st = std::static_pointer_cast<KinesisDataConsumerGroup>(consumer_grp)
                            ->assign_stream_shards(ctx);

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
        TPipelineFragmentParamsList mocked;
        HANDLE_ERROR(_exec_env->stream_load_executor()->execute_plan_fragment(ctx, mocked),
                     "failed to execute plan fragment");
#else
        // only for test
        HANDLE_ERROR(_execute_plan_for_test(ctx), "test failed");
#endif
    }

    pipe = std::static_pointer_cast<io::StreamLoadPipe>(ctx->body_sink);

    // Multi-table currently only supported for Kafka
    if (ctx->is_multi_table) {
        Status st;
        // plan the rest of unplanned data
        auto multi_table_pipe = std::static_pointer_cast<io::MultiTablePipe>(ctx->body_sink);
        // start to consume, this may block a while
        st = consumer_grp->start_all(ctx, pipe);
        if (!st.ok()) {
            multi_table_pipe->handle_consume_finished();
            HANDLE_MULTI_TABLE_ERROR(st, "consuming failed");
        }
        st = multi_table_pipe->request_and_exec_plans();
        if (!st.ok()) {
            multi_table_pipe->handle_consume_finished();
            HANDLE_MULTI_TABLE_ERROR(st, "multi tables task executes plan error");
        }
        // need memory order
        multi_table_pipe->handle_consume_finished();
        HANDLE_MULTI_TABLE_ERROR(pipe->finish(), "finish multi table task failed");
    } else {
        // start to consume, this may block a while
        HANDLE_ERROR(consumer_grp->start_all(ctx, pipe), "consuming failed");
    }

    // wait for all consumers finished
    HANDLE_ERROR(ctx->load_status_future.get(), "consume failed");

    ctx->load_cost_millis = UnixMillis() - ctx->start_millis;

    // return the consumer back to pool
    // call this before commit txn, in case the next task can come very fast
    consumer_pool->return_consumers(consumer_grp.get());

    // commit txn
    int64_t commit_and_publish_start_time = MonotonicNanos();
    HANDLE_ERROR(_exec_env->stream_load_executor()->commit_txn(ctx.get()), "commit failed");
    g_routine_load_commit_and_publish_latency_ms
            << (MonotonicNanos() - commit_and_publish_start_time) / 1000000;
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
    case TLoadSourceType::KINESIS: {
        // kinesis metrics/progress are aggregated in KinesisDataConsumerGroup::start_all()
        LOG(INFO) << "Kinesis routine load task completed. Committed sequence numbers for "
                  << ctx->kinesis_info->cmt_sequence_number.size()
                  << " shards. Task: " << ctx->brief();
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
                ctx->load_status_promise.set_value(st);
                break;
            }

            if (read_bytes == 0) {
                ctx->load_status_promise.set_value(Status::OK());
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
