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
#include <brpc/controller.h>
#include <bthread/types.h>
#include <butil/errno.h>
#include <fmt/format.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/internal_service.pb.h>
#include <gen_cpp/types.pb.h>
#include <glog/logging.h>
#include <google/protobuf/stubs/callback.h>
#include <parallel_hashmap/phmap.h>
#include <stddef.h>
#include <stdint.h>

#include <atomic>
// IWYU pragma: no_include <bits/chrono.h>

#include <chrono> // IWYU pragma: keep
#include <functional>
#include <initializer_list>
#include <map>
#include <memory>
#include <mutex>
#include <ostream>
#include <queue>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/config.h"
#include "util/uid_util.h"

namespace doris {

class DeltaWriterV2;
class RuntimeProfile;

namespace vectorized {

class DeltaWriterV2Map {
public:
    DeltaWriterV2Map(UniqueId load_id);

    ~DeltaWriterV2Map();

    void grab() { ++_use_cnt; }

    // get or create delta writer for the given tablet, memory is managed by DeltaWriterV2Map
    DeltaWriterV2* get_or_create(int64_t tablet_id, std::function<DeltaWriterV2*()> creator);

    // close all delta writers in this DeltaWriterV2Map if there is no other users
    Status close(RuntimeProfile* profile);

    // cancel all delta writers in this DeltaWriterV2Map
    void cancel(Status status);

    UniqueId unique_id() const { return _load_id; }

    size_t size() const { return _map.size(); }

private:
    using TabletToDeltaWriterV2Map = phmap::parallel_flat_hash_map<
            int64_t, std::unique_ptr<DeltaWriterV2>, std::hash<int64_t>, std::equal_to<int64_t>,
            std::allocator<phmap::Pair<const int64_t, std::unique_ptr<DeltaWriterV2>>>, 4,
            std::mutex>;

    UniqueId _load_id;
    TabletToDeltaWriterV2Map _map;
    std::atomic<int> _use_cnt;
};

class DeltaWriterV2Pool {
public:
    DeltaWriterV2Pool();

    ~DeltaWriterV2Pool();

    std::shared_ptr<DeltaWriterV2Map> get_or_create(PUniqueId load_id);

    size_t size() {
        std::lock_guard<std::mutex> lock(_mutex);
        return _pool.size();
    }

private:
    std::mutex _mutex;
    std::unordered_map<UniqueId, std::weak_ptr<DeltaWriterV2Map>> _pool;
};

} // namespace vectorized
} // namespace doris