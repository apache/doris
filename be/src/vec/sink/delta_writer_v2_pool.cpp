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

#include "vec/sink/delta_writer_v2_pool.h"

#include "olap/delta_writer_v2.h"
#include "util/runtime_profile.h"

namespace doris {
class TExpr;

namespace vectorized {

DeltaWriterV2Map::DeltaWriterV2Map(UniqueId load_id, int num_use, DeltaWriterV2Pool* pool)
        : _load_id(load_id), _use_cnt(num_use), _pool(pool) {}

DeltaWriterV2Map::~DeltaWriterV2Map() = default;

std::shared_ptr<DeltaWriterV2> DeltaWriterV2Map::get_or_create(
        int64_t tablet_id, std::function<std::unique_ptr<DeltaWriterV2>()> creator) {
    std::lock_guard lock(_mutex);
    if (_map.contains(tablet_id)) {
        return _map.at(tablet_id);
    }
    std::shared_ptr<DeltaWriterV2> writer = creator();
    if (writer != nullptr) {
        _map[tablet_id] = writer;
    }
    return writer;
}

Status DeltaWriterV2Map::close(std::unordered_map<int64_t, int32_t>& segments_for_tablet,
                               RuntimeProfile* profile) {
    int num_use = --_use_cnt;
    if (num_use > 0) {
        LOG(INFO) << "keeping DeltaWriterV2Map, load_id=" << _load_id << " , use_cnt=" << num_use;
        return Status::OK();
    }
    if (_pool != nullptr) {
        _pool->erase(_load_id);
    }
    LOG(INFO) << "closing DeltaWriterV2Map, load_id=" << _load_id;
    std::lock_guard lock(_mutex);
    for (auto& [_, writer] : _map) {
        RETURN_IF_ERROR(writer->close());
    }
    LOG(INFO) << "close-waiting DeltaWriterV2Map, load_id=" << _load_id;
    for (auto& [tablet_id, writer] : _map) {
        int32_t num_segments;
        RETURN_IF_ERROR(writer->close_wait(num_segments, profile));
        segments_for_tablet[tablet_id] = num_segments;
    }
    return Status::OK();
}

void DeltaWriterV2Map::cancel(Status status) {
    int num_use = --_use_cnt;
    LOG(INFO) << "cancelling DeltaWriterV2Map " << _load_id << ", use_cnt=" << num_use;
    if (num_use == 0 && _pool != nullptr) {
        _pool->erase(_load_id);
    }
    std::lock_guard lock(_mutex);
    for (auto& [_, writer] : _map) {
        static_cast<void>(writer->cancel_with_status(status));
    }
}

DeltaWriterV2Pool::DeltaWriterV2Pool() = default;

DeltaWriterV2Pool::~DeltaWriterV2Pool() = default;

std::shared_ptr<DeltaWriterV2Map> DeltaWriterV2Pool::get_or_create(PUniqueId load_id,
                                                                   int num_sink) {
    UniqueId id {load_id};
    std::lock_guard<std::mutex> lock(_mutex);
    std::shared_ptr<DeltaWriterV2Map> map = _pool[id];
    if (map) {
        return map;
    }
    map = std::make_shared<DeltaWriterV2Map>(id, num_sink, this);
    _pool[id] = map;
    return map;
}

void DeltaWriterV2Pool::erase(UniqueId load_id) {
    std::lock_guard<std::mutex> lock(_mutex);
    LOG(INFO) << "erasing DeltaWriterV2Map, load_id=" << load_id;
    _pool.erase(load_id);
}

} // namespace vectorized
} // namespace doris
