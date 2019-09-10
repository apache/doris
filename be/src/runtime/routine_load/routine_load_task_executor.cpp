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

#include "common/status.h"
#include "runtime/exec_env.h"
#include "runtime/routine_load/data_consumer_group.h"
#include "runtime/routine_load/kafka_consumer_pipe.h"
#include "runtime/stream_load/stream_load_context.h"
#include "runtime/stream_load/stream_load_executor.h"
#include "util/defer_op.h"
#include "util/uid_util.h"

#include <thread>

#include "gen_cpp/FrontendService_types.h"
#include "gen_cpp/BackendService_types.h"
#include "gen_cpp/Types_types.h"

namespace doris {

Status RoutineLoadTaskExecutor::get_kafka_partition_meta(
        const PKafkaMetaProxyRequest& request, std::vector<int32_t>* partition_ids) {
    DCHECK(request.has_kafka_info());

    // This context is meaningless, just for unifing the interface
    StreamLoadContext ctx(_exec_env);
    ctx.load_type = TLoadType::ROUTINE_LOAD;
    ctx.load_src_type = TLoadSourceType::KAFKA;
    ctx.label = "NaN";

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

    ctx.kafka_info = new KafkaLoadInfo(t_info);
    ctx.need_rollback = false;

    std::shared_ptr<DataConsumer> consumer;
    RETURN_IF_ERROR(_data_consumer_pool.get_consumer(&ctx, &consumer));

    Status st = std::static_pointer_cast<KafkaDataConsumer>(consumer)->get_partition_meta(partition_ids); 
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

    // the max queue size of thread pool is 100, here we use 80 as a very conservative limit
    if (_thread_pool.get_queue_size() >= 80) {
        LOG(INFO) << "too many tasks in queue: " << _thread_pool.get_queue_size() << ", reject task: " << UniqueId(task.id);
        return Status::InternalError("too many tasks");
    }

    // create the context
    StreamLoadContext* ctx = new StreamLoadContext(_exec_env);
    ctx->load_type = TLoadType::ROUTINE_LOAD;
    ctx->load_src_type = task.type;
    ctx->job_id = task.job_id;
    ctx->id = UniqueId(task.id);
    ctx->txn_id = task.txn_id;
    ctx->db = task.db;
    ctx->table = task.tbl;
    ctx->label = task.label;
    ctx->auth.auth_code = task.auth_code;

    if (task.__isset.max_interval_s) { ctx->max_interval_s = task.max_interval_s; }
    if (task.__isset.max_batch_rows) { ctx->max_batch_rows = task.max_batch_rows; }
    if (task.__isset.max_batch_size) { ctx->max_batch_size = task.max_batch_size; }

    // set execute plan params
    TStreamLoadPutResult put_result;
    TStatus tstatus;
    tstatus.status_code = TStatusCode::OK;
    put_result.status = tstatus;
    put_result.params = std::move(task.params);
    put_result.__isset.params = true;
    ctx->put_result = std::move(put_result);

    // the routine load task'txn has alreay began in FE.
    // so it need to rollback if encounter error.
    ctx->need_rollback = true;
    ctx->max_filter_ratio = 1.0;

    // set source related params
    switch (task.type) {
        case TLoadSourceType::KAFKA:
            ctx->kafka_info = new KafkaLoadInfo(task.kafka_load_info);
            break;
        default:
            LOG(WARNING) << "unknown load source type: " << task.type;
            delete ctx;
            return Status::InternalError("unknown load source type");
    }

    VLOG(1) << "receive a new routine load task: " << ctx->brief();
    // register the task
    ctx->ref();
    _task_map[ctx->id] = ctx;
    
    // offer the task to thread pool
    if (!_thread_pool.offer(
            boost::bind<void>(&RoutineLoadTaskExecutor::exec_task, this, ctx,
            &_data_consumer_pool,
            [this] (StreamLoadContext* ctx) {
                std::unique_lock<std::mutex> l(_lock);
                _task_map.erase(ctx->id);
                LOG(INFO) << "finished routine load task " << ctx->brief()
                          << ", status: " << ctx->status.get_error_msg()
                          << ", current tasks num: " << _task_map.size();
                if (ctx->unref()) {
                    delete ctx;
                }
            }))) {

        // failed to submit task, clear and return
        LOG(WARNING) << "failed to submit routine load task: " << ctx->brief();
        _task_map.erase(ctx->id);
        if (ctx->unref()) {
            delete ctx;
        }
        return Status::InternalError("failed to submit routine load task");
    } else {
        LOG(INFO) << "submit a new routine load task: " << ctx->brief()
                  << ", current tasks num: " << _task_map.size();
        return Status::OK();
    }
}

void RoutineLoadTaskExecutor::exec_task(
        StreamLoadContext* ctx,
        DataConsumerPool* consumer_pool,
        ExecFinishCallback cb) {

#define HANDLE_ERROR(stmt, err_msg) \
    do { \
        Status _status_ = (stmt); \
        if (UNLIKELY(!_status_.ok())) { \
            err_handler(ctx, _status_, err_msg); \
            cb(ctx); \
            return; \
        } \
    } while (false);

    LOG(INFO) << "begin to execute routine load task: " << ctx->brief();

    // create data consumer group
    std::shared_ptr<DataConsumerGroup> consumer_grp;
    HANDLE_ERROR(consumer_pool->get_consumer_grp(ctx, &consumer_grp), "failed to get consumers");    

    // create and set pipe
    std::shared_ptr<StreamLoadPipe> pipe;
    switch (ctx->load_src_type) {
        case TLoadSourceType::KAFKA: {
            pipe = std::make_shared<KafkaConsumerPipe>();
            Status st = std::static_pointer_cast<KafkaDataConsumerGroup>(consumer_grp)->assign_topic_partitions(ctx);
            if (!st.ok()) {
                err_handler(ctx, st, st.get_error_msg());
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

    // must put pipe before executing plan fragment
    HANDLE_ERROR(_exec_env->load_stream_mgr()->put(ctx->id, pipe), "failed to add pipe");

#ifndef BE_TEST
    // execute plan fragment, async
    HANDLE_ERROR(_exec_env->stream_load_executor()->execute_plan_fragment(ctx),
            "failed to execute plan fragment");
#else
    // only for test
    HANDLE_ERROR(_execute_plan_for_test(ctx), "test failed");
#endif
    
    // start to consume, this may block a while
    HANDLE_ERROR(consumer_grp->start_all(ctx), "consuming failed");

    // wait for all consumers finished
    HANDLE_ERROR(ctx->future.get(), "consume failed");

    ctx->load_cost_nanos = MonotonicNanos() - ctx->start_nanos;
    
    // return the consumer back to pool
    // call this before commit txn, in case the next task can come very fast
    consumer_pool->return_consumers(consumer_grp.get()); 

    // commit txn
    HANDLE_ERROR(_exec_env->stream_load_executor()->commit_txn(ctx), "commit failed");

    // commit kafka offset
    switch (ctx->load_src_type) {
        case TLoadSourceType::KAFKA: {
            std::shared_ptr<DataConsumer> consumer;
            Status st = _data_consumer_pool.get_consumer(ctx, &consumer);
            if (!st.ok()) {
                // Kafka Offset Commit is idempotent, Failure should not block the normal process
                // So just print a warning
                LOG(WARNING) << st.get_error_msg();
                break;
            }

            std::vector<RdKafka::TopicPartition*> topic_partitions;
            for (auto& kv : ctx->kafka_info->cmt_offset) {
                RdKafka::TopicPartition* tp1 = RdKafka::TopicPartition::create(
                        ctx->kafka_info->topic, kv.first, kv.second);
                topic_partitions.push_back(tp1);
            }
            
            st = std::static_pointer_cast<KafkaDataConsumer>(consumer)->commit(topic_partitions);
            if (!st.ok()) {
                // Kafka Offset Commit is idempotent, Failure should not block the normal process
                // So just print a warning
                LOG(WARNING) << st.get_error_msg();
            }
            _data_consumer_pool.return_consumer(consumer);

            // delete TopicPartition finally
            auto tp_deleter = [&topic_partitions] () {
                std::for_each(topic_partitions.begin(), topic_partitions.end(),
                    [](RdKafka::TopicPartition* tp1) { delete tp1; });
            };
            DeferOp delete_tp(std::bind<void>(tp_deleter));
        }
            break;
        default:
            return;
    }
    cb(ctx);
}

void RoutineLoadTaskExecutor::err_handler(
        StreamLoadContext* ctx,
        const Status& st,
        const std::string& err_msg) {

    LOG(WARNING) << err_msg;
    ctx->status = st;
    if (ctx->need_rollback) {
        _exec_env->stream_load_executor()->rollback_txn(ctx);
        ctx->need_rollback = false;
    }
    if (ctx->body_sink.get() != nullptr) {
        ctx->body_sink->cancel();
    }

    return;
}

// for test only
Status RoutineLoadTaskExecutor::_execute_plan_for_test(StreamLoadContext* ctx) {
    auto mock_consumer = [this, ctx]() {
        std::shared_ptr<StreamLoadPipe> pipe = _exec_env->load_stream_mgr()->get(ctx->id);
        bool eof = false;
        std::stringstream ss;
        while (true) { 
            char one;
            size_t len = 1;
            Status st = pipe->read((uint8_t*) &one, &len, &eof);
            if (!st.ok()) {
                LOG(WARNING) << "read failed";
                ctx->promise.set_value(st);
                break;
            }

            if (eof) {
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

} // end namespace

