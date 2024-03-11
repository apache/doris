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

#include "runtime/memory/mem_tracker_limiter.h"
#include "util/doris_metrics.h"

namespace doris {

// Base of the lru cache value.
class LRUCacheValueBase {
public:
    LRUCacheValueBase() = default;

    virtual ~LRUCacheValueBase() {
        if (bytes_with_handle != 0) { // DummyLRUCache bytes always equal to 0
            if (mem_tracker != nullptr) {
                // value not alloc use Allocator
                mem_tracker->cache_consume(-bytes_with_handle);
            }
            DorisMetrics::instance()->lru_cache_memory_bytes->increment(-bytes_with_handle);
        }
    }

    void bind_release_memory_callback(size_t bytes_with_handle,
                                      const std::shared_ptr<MemTrackerLimiter>& mem_tracker) {
        this->bytes_with_handle = bytes_with_handle;
        this->mem_tracker = mem_tracker;
    }

private:
    size_t bytes_with_handle = 0;
    std::shared_ptr<MemTrackerLimiter> mem_tracker;
};

} // namespace doris
