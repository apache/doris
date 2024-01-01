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
#include "exec/data_sink.h"
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

namespace doris {

class LoadStreamStub;

class LoadStreamStubPool;

using Streams = std::vector<std::shared_ptr<LoadStreamStub>>;

class LoadStreams {
public:
    LoadStreams(UniqueId load_id, int64_t dst_id, int num_use, LoadStreamStubPool* pool);

    void release();

    void cancel(Status status);

    Streams& streams() { return _streams; }

private:
    Streams _streams;
    UniqueId _load_id;
    int64_t _dst_id;
    std::atomic<int> _use_cnt;
    LoadStreamStubPool* _pool = nullptr;
};

class LoadStreamStubPool {
public:
    LoadStreamStubPool();

    ~LoadStreamStubPool();

    std::shared_ptr<LoadStreams> get_or_create(PUniqueId load_id, int64_t src_id, int64_t dst_id,
                                               int num_streams, int num_sink, RuntimeState* state);

    void erase(UniqueId load_id, int64_t dst_id);

    size_t size() {
        std::lock_guard<std::mutex> lock(_mutex);
        return _pool.size();
    }

    // for UT only
    size_t templates_size() {
        std::lock_guard<std::mutex> lock(_mutex);
        return _template_stubs.size();
    }

private:
    std::mutex _mutex;
    std::unordered_map<UniqueId, std::unique_ptr<LoadStreamStub>> _template_stubs;
    std::unordered_map<std::pair<UniqueId, int64_t>, std::shared_ptr<LoadStreams>> _pool;
};

} // namespace doris