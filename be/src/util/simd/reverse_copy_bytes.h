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

#include "vec/data_types/data_type.h"

namespace doris::simd {
// Copy src_len bytes from src_void to desc in reverse order.
// The first bytes of src_void will be copied to the last bytes of desc.
inline void reverse_copy_bytes(vectorized::UInt8* __restrict desc, size_t desc_len,
                               const void* src_void, size_t src_len) {
    if (src_len == 0) {
        return;
    }

    const vectorized::UInt8* __restrict src_ui8 = static_cast<const vectorized::UInt8*>(src_void);

    for (int i = desc_len - 1, j = 0; j < src_len; --i, ++j) {
        desc[i] = src_ui8[j];
    }
}
} // namespace doris::simd
