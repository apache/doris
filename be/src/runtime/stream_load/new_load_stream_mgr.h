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

#include <cstddef>
#include <future>
#include <iterator>
#include <memory>
#include <mutex>
#include <ostream>
#include <unordered_map>
#include <utility>

#include "common/factory_creator.h"
#include "common/logging.h"
#include "common/status.h"
#include "util/uid_util.h"

namespace doris {

class StreamLoadContext;

// used to register all streams in process so that other module can get this stream
class NewLoadStreamMgr {
    ENABLE_FACTORY_CREATOR(NewLoadStreamMgr);

public:
    NewLoadStreamMgr();
    ~NewLoadStreamMgr();

    Status put(const UniqueId& id, std::shared_ptr<StreamLoadContext> stream) {
        std::lock_guard<std::mutex> l(_lock);
        auto it = _stream_map.find(id);
        if (it != std::end(_stream_map)) {
            auto&& stream_pipe = _stream_map[id];
            if (stream_pipe.first && stream_pipe.second) {
                return Status::InternalError("id already exist");
            }
            if (!stream_pipe.first) {
                stream_pipe.first = stream;
            } else if (!stream_pipe.second) {
                stream_pipe.second = stream;
            }
            return Status::OK();
        }
        std::pair<std::shared_ptr<io::StreamLoadPipe>, std::shared_ptr<io::StreamLoadPipe>>
                stream_pipes;
        stream_pipes.first = stream;
        _stream_map.emplace(id, stream_pipes);
        VLOG_NOTICE << "put stream load pipe: " << id;
        return Status::OK();
    }

    std::shared_ptr<StreamLoadContext> get(const UniqueId& id) {
        {
            std::lock_guard<std::mutex> l(_lock);
            if (auto iter = _stream_map.find(id); iter != _stream_map.end()) {
                return iter->second;
            }
        }
        return nullptr;
    }

    void* get_bufer(const UniqueId& id) {
        std::lock_guard<std::mutex> l(_buffer_lock);
        auto it = _stream_schema_buffer_map.find(id);
        if (it == std::end(_stream_schema_buffer_map)) {
            return nullptr;
        }
        void* buffer = it->second;
        _stream_schema_buffer_map.erase(it);
        return buffer;
    }

    void remove(const UniqueId& id) {
        std::lock_guard<std::mutex> l(_lock);
        if (auto iter = _stream_map.find(id); iter != _stream_map.end()) {
            _stream_map.erase(iter);
            VLOG_NOTICE << "remove stream load pipe: " << id;
        }
        std::lock_guard<std::mutex> l_buffer(_buffer_lock);
        auto it_buffer = _stream_schema_buffer_map.find(id);
        if (it_buffer != std::end(_stream_schema_buffer_map)) {
            _stream_schema_buffer_map.erase(it_buffer);
            VLOG_NOTICE << "remove stream load buffer: " << id;
        }
        std::lock_guard<std::mutex> l_ctx(_promise_lock);
        auto it_promise = _stream_promise_map.find(id);
        if (it_promise != std::end(_stream_promise_map)) {
            _stream_promise_map.erase(it_promise);
            VLOG_NOTICE << "remove stream load promise " << id;
        }
    }

private:
    std::mutex _lock;
    std::unordered_map<UniqueId, std::shared_ptr<StreamLoadContext>> _stream_map;
};
} // namespace doris
