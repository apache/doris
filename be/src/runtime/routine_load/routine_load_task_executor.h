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

#pragma once

#include <functional>
#include <map>
#include <mutex>

#include "gen_cpp/internal_service.pb.h"
#include "runtime/routine_load/data_consumer_pool.h"
#include "util/doris_metrics.h"
#include "util/priority_thread_pool.hpp"
#include "util/uid_util.h"

namespace doris {

class ExecEnv;
class Status;
class StreamLoadContext;
class TRoutineLoadTask;

// A routine load task executor will receive routine load
// tasks from FE, put it to a fixed thread pool.
// The thread pool will process each task and report the result
// to FE finally.
class RoutineLoadTaskExecutor {
public:
    typedef std::function<void(StreamLoadContext*)> ExecFinishCallback;

    RoutineLoadTaskExecutor(ExecEnv* exec_env);

    ~RoutineLoadTaskExecutor();

    // submit a routine load task
    Status submit_task(const TRoutineLoadTask& task);

    Status get_kafka_partition_meta(const PKafkaMetaProxyRequest& request,
                                    std::vector<int32_t>* partition_ids);

    Status get_kafka_partition_offsets_for_times(const PKafkaMetaProxyRequest& request,
                                                 std::vector<PIntegerPair>* partition_offsets);

    Status get_kafka_latest_offsets_for_partitions(const PKafkaMetaProxyRequest& request,
                                                   std::vector<PIntegerPair>* partition_offsets);

private:
    // execute the task
    void exec_task(StreamLoadContext* ctx, DataConsumerPool* pool, ExecFinishCallback cb);

    void err_handler(StreamLoadContext* ctx, const Status& st, const std::string& err_msg);

    // for test only
    Status _execute_plan_for_test(StreamLoadContext* ctx);
    // create a dummy StreamLoadContext for PKafkaMetaProxyRequest
    Status _prepare_ctx(const PKafkaMetaProxyRequest& request, StreamLoadContext* ctx);

private:
    ExecEnv* _exec_env;
    PriorityThreadPool _thread_pool;
    DataConsumerPool _data_consumer_pool;

    std::mutex _lock;
    // task id -> load context
    std::unordered_map<UniqueId, StreamLoadContext*> _task_map;
};

} // namespace doris
