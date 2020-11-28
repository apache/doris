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

// the following code are modified from RocksDB:
// https://github.com/facebook/rocksdb/blob/master/util/crc32c.h

#ifndef DORIS_BE_SRC_UTIL_CRC32C_H
#define DORIS_BE_SRC_UTIL_CRC32C_H

#include <stddef.h>
#include <stdint.h>

#include <vector>

#include "util/slice.h"

namespace doris {
namespace crc32c {

// Return the crc32c of concat(A, data[0,n-1]) where init_crc is the
// crc32c of some string A.  Extend() is often used to maintain the
// crc32c of a stream of data.
extern uint32_t Extend(uint32_t init_crc, const char* data, size_t n);

// Return the crc32c of data[0,n-1]
inline uint32_t Value(const char* data, size_t n) {
    return Extend(0, data, n);
}

// Return the crc32c of data content in all slices
inline uint32_t Value(const std::vector<Slice>& slices) {
    uint32_t crc = 0;
    for (auto& slice : slices) {
        crc = Extend(crc, slice.get_data(), slice.get_size());
    }
    return crc;
}

} // namespace crc32c
} // namespace doris

#endif //DORIS_BE_SRC_UTIL_CRC32C_H
