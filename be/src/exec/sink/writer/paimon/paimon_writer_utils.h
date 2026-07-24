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

#include <algorithm>
#include <cstdint>

namespace doris {

inline int64_t get_paimon_write_buffer_size(int64_t configured_buffer_size, bool enable_adaptive,
                                            int32_t bucket_num) {
    if (!enable_adaptive || bucket_num <= 0) {
        return configured_buffer_size;
    }
    if (bucket_num >= 500) {
        return std::min(configured_buffer_size, 32L * 1024L * 1024L);
    }
    if (bucket_num >= 200) {
        return std::min(configured_buffer_size, 64L * 1024L * 1024L);
    }
    if (bucket_num >= 50) {
        return std::min(configured_buffer_size, 128L * 1024L * 1024L);
    }
    return configured_buffer_size;
}

} // namespace doris
