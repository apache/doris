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

#include "util/crc32c.h"

#include <crc32c/crc32c.h>

namespace doris {
namespace crc32c {

uint32_t Extend(uint32_t crc, const char* data, size_t n) {
    return crc32c_extend(crc, (const uint8_t*)data, n);
}

uint32_t Value(const char* data, size_t n) {
    return crc32c_value((const uint8_t*)data, n);
}

uint32_t Value(const std::vector<Slice>& slices) {
    uint32_t crc = 0;
    for (const auto& slice : slices) {
        crc = crc32c_extend(crc, (const uint8_t*)slice.get_data(), slice.get_size());
    }
    return crc;
}

} // namespace crc32c
} // namespace doris
