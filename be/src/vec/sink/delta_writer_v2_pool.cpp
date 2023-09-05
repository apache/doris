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

namespace doris {
class TExpr;

namespace stream_load {

DeltaWriterV2Pool::DeltaWriterV2Pool() = default;

DeltaWriterV2Pool::~DeltaWriterV2Pool() = default;

std::shared_ptr<TabletToDeltaWriterV2Map> DeltaWriterV2Pool::get_or_create(PUniqueId load_id) {
    UniqueId key {load_id};
    std::lock_guard<std::mutex> lock(_mutex);
    std::shared_ptr<TabletToDeltaWriterV2Map> writer = _pool[key].lock();
    _ref_cnt[key]++;
    if (writer) {
        return writer;
    }
    writer = std::make_shared<TabletToDeltaWriterV2Map>();
    _pool[key] = writer;
    return writer;
}

bool DeltaWriterV2Pool::remove(PUniqueId load_id) {
    UniqueId key {load_id};
    std::lock_guard<std::mutex> lock(_mutex);
    bool is_last = --_ref_cnt[key] == 0;
    if (is_last) {
        _pool.erase(key);
    }
    return is_last;
}

void DeltaWriterV2Pool::reset(PUniqueId load_id) {
    UniqueId key {load_id};
    std::lock_guard<std::mutex> lock(_mutex);
    _ref_cnt[key] = 0;
    _pool.erase(key);
}

} // namespace stream_load
} // namespace doris
