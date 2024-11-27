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

#include "runtime/memory/cache_manager.h"
#include "runtime/memory/mem_tracker_limiter.h"

namespace doris {

// Base of the lru cache value.
class LRUCacheValueBase {
public:
    virtual ~LRUCacheValueBase() {
        if (_tracking_bytes > 0) {
            _mem_tracker->consume(-_tracking_bytes);
        }
    }

    void set_tracking_bytes(size_t tracking_bytes, MemTracker* mem_tracker) {
        this->_tracking_bytes = tracking_bytes;
        this->_mem_tracker = mem_tracker;
    }

protected:
    size_t _tracking_bytes = 0;
    MemTracker* _mem_tracker = nullptr;
};

} // namespace doris
