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
#include <limits>

#include "util/time.h"

namespace doris::io {

// Calc absolute expiration timestamp for file cache TTL mode.
//
// Return 0 means treat it as non-TTL cache (NORMAL/INDEX/DISPOSABLE) and avoid
// putting data into TTL queues / TTL-path directories.
inline int64_t calc_file_cache_expiration_time(int64_t base_timestamp, int64_t ttl_seconds) {
    if (ttl_seconds <= 0 || base_timestamp <= 0) {
        return 0;
    }

    // Overflow protection.
    if (base_timestamp > std::numeric_limits<int64_t>::max() - ttl_seconds) {
        return 0;
    }

    const int64_t expiration_time = base_timestamp + ttl_seconds;
    // Clamp expired TTL to 0 to keep behavior consistent across read/write/warmup.
    return expiration_time > UnixSeconds() ? expiration_time : 0;
}

} // namespace doris::io
