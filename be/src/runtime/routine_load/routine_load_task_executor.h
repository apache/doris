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

#include <stdint.h>

#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "runtime/routine_load/data_consumer_pool.h"
#include "util/threadpool.h"
#include "util/uid_util.h"

namespace doris {

class ExecEnv;
class Status;
class StreamLoadContext;
class TRoutineLoadTask;
class PIntegerPair;
class PKafkaMetaProxyRequest;

// A routine load task executor will receive routine load
// tasks from FE, put it to a fixed thread pool.
// The thread pool will process each task and report the result
// to FE finally.
class RoutineLoadTaskExecutor {
public:
    using ExecFinishCallback = std::function<void(std::shared_ptr<StreamLoadContext>)>;

    RoutineLoadTaskExecutor(ExecEnv* exec_env);

    ~RoutineLoadTaskExecutor();

    Status init(int64_t process_mem_limit);

    void stop();

    // submit a routine load task
    Status submit_task(const TRoutineLoadTask& task);

    Status get_kafka_partition_meta(const PKafkaMetaProxyRequest& request,
                                    std::vector<int32_t>* partition_ids);

    Status get_kafka_partition_offsets_for_times(const PKafkaMetaProxyRequest& request,
                                                 std::vector<PIntegerPair>* partition_offsets,
                                                 int timeout);

    Status get_kafka_latest_offsets_for_partitions(const PKafkaMetaProxyRequest& request,
                                                   std::vector<PIntegerPair>* partition_offsets,
                                                   int timeout);

    Status get_kafka_real_offsets_for_partitions(const PKafkaMetaProxyRequest& request,
                                                 std::vector<PIntegerPair>* partition_offsets,
                                                 int timeout);

    ThreadPool& get_thread_pool() { return *_thread_pool; }

private:
    // execute the task
    void exec_task(std::shared_ptr<StreamLoadContext> ctx, DataConsumerPool* pool,
                   ExecFinishCallback cb);

    void err_handler(std::shared_ptr<StreamLoadContext> ctx, const Status& st,
                     const std::string& err_msg);

    // for test only
    Status _execute_plan_for_test(std::shared_ptr<StreamLoadContext> ctx);
    // create a dummy StreamLoadContext for PKafkaMetaProxyRequest
    Status _prepare_ctx(const PKafkaMetaProxyRequest& request,
                        std::shared_ptr<StreamLoadContext> ctx);
    bool _reach_memory_limit();

private:
    ExecEnv* _exec_env = nullptr;
    std::unique_ptr<ThreadPool> _thread_pool;
    DataConsumerPool _data_consumer_pool;

    std::mutex _lock;
    // task id -> load context
    std::unordered_map<UniqueId, std::shared_ptr<StreamLoadContext>> _task_map;

    int64_t _load_mem_limit = -1;
};

} // namespace doris
