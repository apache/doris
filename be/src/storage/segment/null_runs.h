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

#include <cstddef>
#include <cstdint>
#include <cstring>

#include "common/status.h"

namespace doris::segment_v2 {

// Walks null_map[0, n) as maximal runs of equal flags, calling
// fn(is_null, start, len) for each in order. The flags are 0 or 1, so memchr
// finds where a run ends.
template <typename Fn>
Status for_each_null_run(const uint8_t* null_map, size_t n, Fn&& fn) {
    size_t offset = 0;
    while (offset < n) {
        const bool is_null = null_map[offset] != 0;
        const size_t remaining = n - offset;
        const auto* run_end =
                static_cast<const uint8_t*>(memchr(null_map + offset, is_null ? 0 : 1, remaining));
        const size_t len = run_end != nullptr ? run_end - (null_map + offset) : remaining;
        RETURN_IF_ERROR(fn(is_null, offset, len));
        offset += len;
    }
    return Status::OK();
}

} // namespace doris::segment_v2
