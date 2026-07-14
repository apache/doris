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

#include <cstdint>

namespace doris::snii::io {

// Object-storage access metrics collected at FileReader boundaries.
struct IoMetrics {
    uint64_t read_at_calls = 0;       // BE-internal logical read requests issued
    uint64_t serial_rounds = 0;       // dependent serial I/O rounds
    uint64_t range_gets = 0;          // remote range GETs after cache coalescing
    uint64_t remote_bytes = 0;        // bytes fetched from remote
    uint64_t total_request_bytes = 0; // sum of requested lengths before cache
};

inline IoMetrics delta(const IoMetrics& after, const IoMetrics& before) {
    IoMetrics out;
    out.read_at_calls = after.read_at_calls - before.read_at_calls;
    out.serial_rounds = after.serial_rounds - before.serial_rounds;
    out.range_gets = after.range_gets - before.range_gets;
    out.remote_bytes = after.remote_bytes - before.remote_bytes;
    out.total_request_bytes = after.total_request_bytes - before.total_request_bytes;
    return out;
}

} // namespace doris::snii::io
