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

#include "util/hash_util.hpp"

namespace doris {
// Define the hashing functions for llvm.  They are not used by anything that is
// cross compiled and without this, would get stripped by the clang optimizer.
#ifdef IR_COMPILE
extern "C" uint32_t ir_fnv_hash(const void* data, int32_t bytes, uint32_t hash) {
    return HashUtil::fnv_hash(data, bytes, hash);
}

extern "C" uint32_t ir_crc_hash(const void* data, int32_t bytes, uint32_t hash) {
#ifdef __SSE4_2__
    return HashUtil::crc_hash(data, bytes, hash);
#else
    return HashUtil::fnv_hash(data, bytes, hash);
#endif
}
#else
#error "This file should only be compiled by clang."
#endif

} // namespace doris
