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

#include <cctz/time_zone.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/segment_v2.pb.h>

#include <ctime>
#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "util/countdown_latch.h"
#include "util/hash_util.hpp"
#include "vec/sink/varrow_flight_result_writer.h"

namespace arrow {
class RecordBatch;
class Schema;
} // namespace arrow

namespace doris {

class ResultBlockBufferBase;
class PUniqueId;
class RuntimeState;
class MemTrackerLimiter;
class Thread;
namespace vectorized {
class GetArrowResultBatchCtx;
class GetResultBatchCtx;
class Block;
} // namespace vectorized

// manage all result buffer control block in one backend. here we use uniform id `unique_id` to identify buffer slots.
// for parallel result sink, it's `instance_id`, for non-parallel sink, it's `query_id`.
class ResultBufferMgr {
public:
    ResultBufferMgr();
    ~ResultBufferMgr() = default;
    // init Result Buffer Mgr, start cancel thread
    Status init();

    void stop();

    // for non-parallel sink, use query_id. but for parallel sink, use instance id.
    Status create_sender(const TUniqueId& unique_id, int buffer_size,
                         std::shared_ptr<ResultBlockBufferBase>* sender, RuntimeState* state,
                         bool arrow_flight, std::shared_ptr<arrow::Schema> schema = nullptr);

    template <typename ResultBlockBufferType>
    Status find_buffer(const TUniqueId& unique_id, std::shared_ptr<ResultBlockBufferType>& buffer);
    // cancel
    bool cancel(const TUniqueId& unique_id, const Status& reason);

    // cancel one query at a future time.
    void cancel_at_time(time_t cancel_time, const TUniqueId& unique_id);

private:
    using BufferMap = std::unordered_map<TUniqueId, std::shared_ptr<ResultBlockBufferBase>>;
    using TimeoutMap = std::map<time_t, std::vector<TUniqueId>>;

    template <typename ResultBlockBufferType>
    std::shared_ptr<ResultBlockBufferType> _find_control_block(const TUniqueId& unique_id);

    // used to erase the buffer that fe not clears
    // when fe crush, this thread clear the buffer avoid memory leak in this backend
    void cancel_thread();

    // lock for buffer map
    std::shared_mutex _buffer_map_lock;
    // buffer block map
    BufferMap _buffer_map;

    // lock for timeout map
    std::mutex _timeout_lock;

    // map (cancel_time : query to be cancelled),
    // cancel time maybe equal, so use one list
    TimeoutMap _timeout_map;

    CountDownLatch _stop_background_threads_latch;
    std::shared_ptr<Thread> _clean_thread;
};

} // namespace doris
