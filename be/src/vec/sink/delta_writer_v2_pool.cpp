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

DeltaWriterV2Map::DeltaWriterV2Map(UniqueId load_id) : _load_id(load_id), _use_cnt(1) {}

DeltaWriterV2Map::~DeltaWriterV2Map() = default;

DeltaWriterV2* DeltaWriterV2Map::get_or_create(
        int64_t tablet_id, std::function<std::unique_ptr<DeltaWriterV2>()> creator) {
    _map.lazy_emplace(tablet_id, [&](const TabletToDeltaWriterV2Map::constructor& ctor) {
        ctor(tablet_id, creator());
    });
    return _map.at(tablet_id).get();
}

Status DeltaWriterV2Map::close(RuntimeProfile* profile) {
    if (--_use_cnt > 0) {
        return Status::OK();
    }
    Status status = Status::OK();
    _map.for_each([&status](auto& entry) {
        if (status.ok()) {
            status = entry.second->close();
        }
    });
    if (!status.ok()) {
        return status;
    }
    _map.for_each([&status, profile](auto& entry) {
        if (status.ok()) {
            status = entry.second->close_wait(profile);
        }
    });
    return status;
}

void DeltaWriterV2Map::cancel(Status status) {
    _map.for_each([&status](auto& entry) {
        static_cast<void>(entry.second->cancel_with_status(status));
    });
}

DeltaWriterV2Pool::DeltaWriterV2Pool() = default;

DeltaWriterV2Pool::~DeltaWriterV2Pool() = default;

std::shared_ptr<DeltaWriterV2Map> DeltaWriterV2Pool::get_or_create(PUniqueId load_id) {
    UniqueId id {load_id};
    std::lock_guard<std::mutex> lock(_mutex);
    std::shared_ptr<DeltaWriterV2Map> map = _pool[id].lock();
    if (map) {
        map->grab();
        return map;
    }
    auto deleter = [this](DeltaWriterV2Map* m) {
        std::lock_guard<std::mutex> lock(_mutex);
        _pool.erase(m->unique_id());
        delete m;
    };
    map = std::shared_ptr<DeltaWriterV2Map>(new DeltaWriterV2Map(id), deleter);
    _pool[id] = map;
    return map;
}

} // namespace vectorized
} // namespace doris
