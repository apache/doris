// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#pragma once

#include "unpack_def.h"
inline void extend_32u64u(const __m512i src, uint64_t* &dst_ptr) {
    __m256i tmp = _mm512_extracti64x4_epi64(src, 0);
    __m512i result = _mm512_cvtepu32_epi64(tmp);
    _mm512_storeu_si512(dst_ptr, result);
    dst_ptr += 8;

    tmp = _mm512_extracti64x4_epi64(src, 1);
    result = _mm512_cvtepu32_epi64(tmp);
    _mm512_storeu_si512(dst_ptr, result);
    dst_ptr += 8;
}
