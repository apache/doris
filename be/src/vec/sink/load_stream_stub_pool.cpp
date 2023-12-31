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

#include "vec/sink/load_stream_stub_pool.h"

#include "util/debug_points.h"
#include "vec/sink/load_stream_stub.h"

namespace doris {
class TExpr;

LoadStreams::LoadStreams(UniqueId load_id, int64_t dst_id, int num_use, LoadStreamStubPool* pool)
        : _load_id(load_id), _dst_id(dst_id), _use_cnt(num_use), _pool(pool) {}

void LoadStreams::release() {
    int num_use = --_use_cnt;
    DBUG_EXECUTE_IF("LoadStreams.release.keeping_streams", { num_use = 1; });
    if (num_use == 0) {
        LOG(INFO) << "releasing streams, load_id=" << _load_id << ", dst_id=" << _dst_id;
        for (auto& stream : _streams) {
            auto st = stream->close_stream();
            DBUG_EXECUTE_IF("LoadStreams.release.close_stream_failed",
                            { st = Status::InternalError("stream close failed"); });
            if (!st.ok()) {
                LOG(WARNING) << "close stream failed " << st;
            }
        }
        for (auto& stream : _streams) {
            auto st = stream->close_wait();
            DBUG_EXECUTE_IF("LoadStreams.release.close_wait_failed",
                            { st = Status::InternalError("stream close wait timeout"); });
            if (!st.ok()) {
                LOG(WARNING) << "close wait failed " << st;
            }
        }
        _pool->erase(_load_id, _dst_id);
    } else {
        LOG(INFO) << "keeping streams, load_id=" << _load_id << ", dst_id=" << _dst_id
                  << ", use_cnt=" << num_use;
    }
}

void LoadStreams::cancel(Status status) {
    for (auto& stream : _streams) {
        stream->cancel(status);
    }
}

LoadStreamStubPool::LoadStreamStubPool() = default;

LoadStreamStubPool::~LoadStreamStubPool() = default;

std::shared_ptr<LoadStreams> LoadStreamStubPool::get_or_create(PUniqueId load_id, int64_t src_id,
                                                               int64_t dst_id, int num_streams,
                                                               int num_sink, RuntimeState* state) {
    auto key = std::make_pair(UniqueId(load_id), dst_id);
    std::lock_guard<std::mutex> lock(_mutex);
    std::shared_ptr<LoadStreams> streams = _pool[key];
    if (streams) {
        return streams;
    }
    DCHECK(num_streams > 0) << "stream num should be greater than 0";
    DCHECK(num_sink > 0) << "sink num should be greater than 0";
    auto [it, _] =
            _template_stubs.emplace(load_id, new LoadStreamStub {load_id, src_id, num_sink, state});
    streams = std::make_shared<LoadStreams>(load_id, dst_id, num_sink, this);
    for (int32_t i = 0; i < num_streams; i++) {
        // copy construct, internal tablet schema map will be shared among all stubs
        streams->streams().emplace_back(new LoadStreamStub {*it->second, state});
    }
    _pool[key] = streams;
    return streams;
}

void LoadStreamStubPool::erase(UniqueId load_id, int64_t dst_id) {
    std::lock_guard<std::mutex> lock(_mutex);
    _pool.erase(std::make_pair(load_id, dst_id));
    _template_stubs.erase(load_id);
}

} // namespace doris
