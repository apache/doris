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

#include <memory>
#include <mutex>
#include <unordered_map>

#include "io/fs/stream_load_pipe.h"
#include "util/doris_metrics.h"
#include "util/uid_util.h"

namespace doris {

// used to register all streams in process so that other module can get this stream
// TODO(ftw): should be renamed to `LoadStreamMgr` after new file reader is ready.
class NewLoadStreamMgr {
public:
    NewLoadStreamMgr();
    ~NewLoadStreamMgr();

    Status put(const UniqueId& id, std::shared_ptr<io::StreamLoadPipe> stream) {
        std::lock_guard<std::mutex> l(_lock);
        auto it = _stream_map.find(id);
        if (it != std::end(_stream_map)) {
            return Status::InternalError("id already exist");
        }
        _stream_map.emplace(id, stream);
        VLOG_NOTICE << "put stream load pipe: " << id;
        return Status::OK();
    }

    std::shared_ptr<io::StreamLoadPipe> get(const UniqueId& id) {
        std::lock_guard<std::mutex> l(_lock);
        auto it = _stream_map.find(id);
        if (it == std::end(_stream_map)) {
            return nullptr;
        }
        auto stream = it->second;
        _stream_map.erase(it);
        return stream;
    }

    void remove(const UniqueId& id) {
        std::lock_guard<std::mutex> l(_lock);
        auto it = _stream_map.find(id);
        if (it != std::end(_stream_map)) {
            _stream_map.erase(it);
            VLOG_NOTICE << "remove stream load pipe: " << id;
        }
    }

private:
    std::mutex _lock;
    std::unordered_map<UniqueId, std::shared_ptr<io::StreamLoadPipe>> _stream_map;
};
} // namespace doris
