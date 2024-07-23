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

#include <condition_variable>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <utility>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/status.h"
#include "runtime/load_stream.h"
#include "util/threadpool.h"

namespace doris {

class POpenStreamSinkRequest;

class LoadStreamMgr {
public:
    LoadStreamMgr(uint32_t segment_file_writer_thread_num);
    ~LoadStreamMgr();

    Status open_load_stream(const POpenLoadStreamRequest* request, LoadStream*& load_stream);
    void clear_load(UniqueId loadid);
    void create_tokens(std::vector<std::unique_ptr<ThreadPoolToken>>& tokens) {
        for (int i = 0; i < _num_threads * 2; i++) {
            tokens.push_back(
                    _file_writer_thread_pool->new_token(ThreadPool::ExecutionMode::SERIAL));
        }
    }

    std::vector<std::string> get_all_load_stream_ids() {
        std::vector<std::string> result;
        std::lock_guard<std::mutex> lock(_lock);

        for (auto& [id, _] : _load_streams_map) {
            result.push_back(id.to_string());
        }
        return result;
    }

    // only used by ut
    size_t get_load_stream_num() { return _load_streams_map.size(); }

    FifoThreadPool* heavy_work_pool() { return _heavy_work_pool; }
    FifoThreadPool* light_work_pool() { return _light_work_pool; }

    void set_heavy_work_pool(FifoThreadPool* pool) { _heavy_work_pool = pool; }
    void set_light_work_pool(FifoThreadPool* pool) { _light_work_pool = pool; }

private:
    std::mutex _lock;
    std::unordered_map<UniqueId, LoadStreamPtr> _load_streams_map;
    std::unique_ptr<ThreadPool> _file_writer_thread_pool;

    uint32_t _num_threads = 0;

    FifoThreadPool* _heavy_work_pool = nullptr;
    FifoThreadPool* _light_work_pool = nullptr;
};

} // namespace doris
