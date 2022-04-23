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

#ifndef DORIS_BE_SRC_UTIL_BARRIER_H
#define DORIS_BE_SRC_UTIL_BARRIER_H

#include <condition_variable>
#include <mutex>

#include "olap/olap_define.h"

namespace doris {

// Implementation of pthread-style Barriers.
class Barrier {
public:
    // Initialize the barrier with the given initial count.
    explicit Barrier(int count) : _count(count), _initial_count(count) { DCHECK_GT(count, 0); }

    ~Barrier() {}
    Barrier(const Barrier&) = delete;
    void operator=(const Barrier&) = delete;
    // wait until all threads have reached the barrier.
    // Once all threads have reached the barrier, the barrier is reset
    // to the initial count.
    void wait() {
        std::unique_lock<std::mutex> l(_mutex);
        if (--_count == 0) {
            _count = _initial_count;
            _cycle_count++;
            _cond.notify_all();
            return;
        }

        int initial_cycle = _cycle_count;
        while (_cycle_count == initial_cycle) {
            _cond.wait(l);
        }
    }

private:
    int _count;
    const int _initial_count;
    uint32_t _cycle_count = 0;
    std::mutex _mutex;
    std::condition_variable _cond;
};

#endif //DORIS_BE_SRC_UTIL_BARRIER_H

} // namespace doris
