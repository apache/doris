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

#include <bvar/bvar.h>
#include <stdint.h>

#include <atomic>
#include <condition_variable>
#include <mutex>

namespace doris {

class IOThrottle {
public:
    IOThrottle(std::string prefix, std::string name);

    ~IOThrottle() = default;

    bool acquire(int64_t block_timeout_ms);

    // non-block acquire
    bool try_acquire();

    void update_next_io_time(int64_t bytes);

    void set_io_bytes_per_second(int64_t read_bytes_per_second);

    size_t get_bvar_io_per_second() { return _io_adder_per_second->get_value(); }

private:
    std::mutex _mutex;
    std::condition_variable wait_condition;
    int64_t _next_io_time_micros {0};
    std::atomic<int64_t> _io_bytes_per_second_limit {-1};

    // bvar monitor
    std::unique_ptr<bvar::Adder<size_t>> _io_adder;
    std::unique_ptr<bvar::PerSecond<bvar::Adder<size_t>>> _io_adder_per_second;
};
}; // namespace doris