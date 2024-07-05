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
#include "common/status.h"
#include "exec/tablet_info.h"
#include "gutil/ref_counted.h"
#include "runtime/exec_env.h"
#include "runtime/memory/mem_tracker.h"
#include "runtime/thread_context.h"
#include "runtime/types.h"
#include "util/countdown_latch.h"
#include "util/runtime_profile.h"
#include "util/stopwatch.hpp"
#include "vec/columns/column.h"
#include "vec/common/allocator.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type.h"
#include "vec/exprs/vexpr_fwd.h"
#include "vec/sink/load_stream_stub.h"

namespace doris {

class LoadStreamStub;

class LoadStreamMapPool;

using Streams = std::vector<std::shared_ptr<LoadStreamStub>>;

class LoadStreamMap {
public:
    LoadStreamMap(UniqueId load_id, int64_t src_id, int num_streams, int num_use,
                  LoadStreamMapPool* pool);

    std::shared_ptr<Streams> get_or_create(int64_t dst_id, bool incremental = false);

    std::shared_ptr<Streams> at(int64_t dst_id);

    bool contains(int64_t dst_id);

    void for_each(std::function<void(int64_t, const Streams&)> fn);

    Status for_each_st(std::function<Status(int64_t, const Streams&)> fn);

    void save_tablets_to_commit(int64_t dst_id, const std::vector<PTabletID>& tablets_to_commit);

    void save_segments_for_tablet(const std::unordered_map<int64_t, int32_t>& segments_for_tablet) {
        _segments_for_tablet.insert(segments_for_tablet.cbegin(), segments_for_tablet.cend());
    }

    // Return true if the last instance is just released.
    bool release();

    // send CLOSE_LOAD to all streams, return ERROR if any.
    // only call this method after release() returns true.
    Status close_load(bool incremental);

private:
    const UniqueId _load_id;
    const int64_t _src_id;
    const int _num_streams;
    std::atomic<int> _use_cnt;
    std::mutex _mutex;
    std::unordered_map<int64_t, std::shared_ptr<Streams>> _streams_for_node;
    LoadStreamMapPool* _pool = nullptr;
    std::shared_ptr<IndexToTabletSchema> _tablet_schema_for_index;
    std::shared_ptr<IndexToEnableMoW> _enable_unique_mow_for_index;

    std::mutex _tablets_to_commit_mutex;
    std::unordered_map<int64_t, std::unordered_map<int64_t, PTabletID>> _tablets_to_commit;
    std::unordered_map<int64_t, int32_t> _segments_for_tablet;
};

class LoadStreamMapPool {
public:
    LoadStreamMapPool();

    ~LoadStreamMapPool();

    std::shared_ptr<LoadStreamMap> get_or_create(UniqueId load_id, int64_t src_id, int num_streams,
                                                 int num_use);

    void erase(UniqueId load_id);

    size_t size() {
        std::lock_guard<std::mutex> lock(_mutex);
        return _pool.size();
    }

private:
    std::mutex _mutex;
    std::unordered_map<UniqueId, std::shared_ptr<LoadStreamMap>> _pool;
};

} // namespace doris
