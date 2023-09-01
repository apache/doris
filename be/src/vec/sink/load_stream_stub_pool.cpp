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

#include <mutex>
#include <string>
#include <unordered_map>

#include "vec/sink/load_stream_stub.h"

namespace doris {
class TExpr;

namespace stream_load {

LoadStreamStubPool::LoadStreamStubPool(int64_t src_id) : _src_id(src_id) {}

LoadStreamStubPool::~LoadStreamStubPool() = default;

std::shared_ptr<Streams> LoadStreamStubPool::get_or_create(PUniqueId load_id, int64_t dst_id) {
    auto key = std::make_pair(UniqueId(load_id), dst_id);
    std::lock_guard<std::mutex> lock(_mutex);
    std::shared_ptr<Streams> streams = _pool[key].lock();
    if (streams) {
        return streams;
    }
    int32_t num_streams = std::max(1, config::num_streams_per_sink);
    streams = std::make_shared<Streams>();
    auto [it, _] = _template_stub.emplace(load_id, new LoadStreamStub {load_id, _src_id});
    for (int32_t i = 0; i < num_streams; i++) {
        // copy construct, internal tablet schema map will be shared among all stubs
        streams->emplace_back(new LoadStreamStub {*it->second});
    }
    _pool[key] = streams;
    return streams;
}

} // namespace stream_load
} // namespace doris
