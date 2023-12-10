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

DeltaWriterV2* DeltaWriterV2Map::get_or_create(
        int64_t tablet_id, std::function<std::unique_ptr<DeltaWriterV2>()> creator) {
    _map.lazy_emplace(tablet_id, [&](const TabletToDeltaWriterV2Map::constructor& ctor) {
        ctor(tablet_id, creator());
    });
    return _map.at(tablet_id).get();
}

Status DeltaWriterV2Map::close(RuntimeProfile* profile) {
    int num_use = --_use_cnt;
    if (num_use > 0) {
        LOG(INFO) << "keeping DeltaWriterV2Map, load_id=" << _load_id << " , use_cnt=" << num_use;
        return Status::OK();
    }
    if (_pool != nullptr) {
        _pool->erase(_load_id);
    }
    LOG(INFO) << "closing DeltaWriterV2Map, load_id=" << _load_id;
    Status status = Status::OK();
    _map.for_each([&status](auto& entry) {
        if (status.ok()) {
            status = entry.second->close();
        }
    });
    if (!status.ok()) {
        return status;
    }
    LOG(INFO) << "close-waiting DeltaWriterV2Map, load_id=" << _load_id;
    _map.for_each([&status, profile](auto& entry) {
        if (status.ok()) {
            status = entry.second->close_wait(profile);
        }
    });
    return status;
}

void DeltaWriterV2Map::cancel(Status status) {
    int num_use = --_use_cnt;
    LOG(INFO) << "cancelling DeltaWriterV2Map " << _load_id << ", use_cnt=" << num_use;
    if (num_use == 0 && _pool != nullptr) {
        _pool->erase(_load_id);
    }
    _map.for_each([&status](auto& entry) {
        static_cast<void>(entry.second->cancel_with_status(status));
    });
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
