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

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>

#include "common/status.h"

struct LanceHostExecutor;
namespace doris {
class RuntimeState;
class ThreadPool;
class ThreadPoolToken;
namespace format::lance {

// Owns the reactor and background executors as Doris ThreadPools, never SDK-created threads.
class LanceHostRuntime final : public std::enable_shared_from_this<LanceHostRuntime> {
public:
    static std::shared_ptr<LanceHostRuntime> instance();
    ~LanceHostRuntime();
    void shutdown();
    Status ensure_started();
    Status create_executor(RuntimeState* state, ::LanceHostExecutor** out);

private:
    Status start();
    struct Task;
    struct QueryExecutor;
    static void submit_service(void*, uint32_t, void (*)(void*), void (*)(void*), void*);
    static void submit_query(void*, uint32_t, void (*)(void*), void (*)(void*), void*);
    static void retain_query(void*);
    static void release_query(void*);
    void submit(uint32_t queue, std::shared_ptr<Task> task);
    std::once_flag _once;
    std::mutex _dispatch_mutex;
    Status _status = Status::OK();
    bool _installed = false;
    std::atomic<bool> _stopped {false};
    std::unique_ptr<ThreadPool> _reactor;
    std::unique_ptr<ThreadPool> _background;
    std::unique_ptr<ThreadPool> _io;
    std::unique_ptr<ThreadPoolToken> _io_token;
};
} // namespace format::lance
} // namespace doris
