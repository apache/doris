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
void reverse_copy_bytes(UInt8* __restrict desc, size_t desc_len, const void* src_void,
                        size_t str_len) {
    if (str_len == 0) {
        return;
    }

    const UInt8* src_ui8 = static_cast<const UInt8*>(src);

    for (int i = desc_len - 1, j = 0; j < str_len; --i, ++j) {
        desc[i] = src_ui8[j];
    }
}
} // namespace doris::simd
