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

#include "olap/rowset/segment_v2/bitshuffle_wrapper.h"

// Include the bitshuffle header once to get the default (non-AVX2)
// symbols.
#include <bitshuffle/bitshuffle.h>

#include "gutil/cpu.h"

// Include the bitshuffle header again, but this time importing the
// AVX2-compiled symbols by defining some macros.
#undef BITSHUFFLE_H
#define bshuf_compress_lz4_bound bshuf_compress_lz4_bound_avx2
#define bshuf_compress_lz4 bshuf_compress_lz4_avx2
#define bshuf_decompress_lz4 bshuf_decompress_lz4_avx2
#include <bitshuffle/bitshuffle.h> // NOLINT(*)
#undef bshuf_compress_lz4_bound
#undef bshuf_compress_lz4
#undef bshuf_decompress_lz4

#undef BITSHUFFLE_H
#define bshuf_compress_lz4_bound bshuf_compress_lz4_bound_neon
#define bshuf_compress_lz4 bshuf_compress_lz4_neon
#define bshuf_decompress_lz4 bshuf_decompress_lz4_neon
#include <bitshuffle/bitshuffle.h> // NOLINT(*)
#undef bshuf_compress_lz4_bound
#undef bshuf_compress_lz4
#undef bshuf_decompress_lz4

using base::CPU;

namespace doris {
namespace bitshuffle {

// Function pointers which will be assigned the correct implementation
// for the runtime architecture.
namespace {
decltype(&bshuf_compress_lz4_bound) g_bshuf_compress_lz4_bound;
decltype(&bshuf_compress_lz4) g_bshuf_compress_lz4;
decltype(&bshuf_decompress_lz4) g_bshuf_decompress_lz4;
} // anonymous namespace

// When this translation unit is initialized, figure out the current CPU and
// assign the correct function for this architecture.
//
// This avoids an expensive 'cpuid' call in the hot path, and also avoids
// the cost of a 'std::once' call.
__attribute__((constructor)) void SelectBitshuffleFunctions() {
#if (defined(__i386) || defined(__x86_64__))
    if (CPU().has_avx2()) {
        g_bshuf_compress_lz4_bound = bshuf_compress_lz4_bound_avx2;
        g_bshuf_compress_lz4 = bshuf_compress_lz4_avx2;
        g_bshuf_decompress_lz4 = bshuf_decompress_lz4_avx2;
    } else {
        g_bshuf_compress_lz4_bound = bshuf_compress_lz4_bound;
        g_bshuf_compress_lz4 = bshuf_compress_lz4;
        g_bshuf_decompress_lz4 = bshuf_decompress_lz4;
    }
#elif defined(__ARM_NEON) && defined(__aarch64__) && !defined(__APPLE__)
    g_bshuf_compress_lz4_bound = bshuf_compress_lz4_bound_neon;
    g_bshuf_compress_lz4 = bshuf_compress_lz4_neon;
    g_bshuf_decompress_lz4 = bshuf_decompress_lz4_neon;
#else
    g_bshuf_compress_lz4_bound = bshuf_compress_lz4_bound;
    g_bshuf_compress_lz4 = bshuf_compress_lz4;
    g_bshuf_decompress_lz4 = bshuf_decompress_lz4;
#endif
}

int64_t compress_lz4(void* in, void* out, size_t size, size_t elem_size, size_t block_size) {
    return g_bshuf_compress_lz4(in, out, size, elem_size, block_size);
}
int64_t decompress_lz4(void* in, void* out, size_t size, size_t elem_size, size_t block_size) {
    return g_bshuf_decompress_lz4(in, out, size, elem_size, block_size);
}
size_t compress_lz4_bound(size_t size, size_t elem_size, size_t block_size) {
    return g_bshuf_compress_lz4_bound(size, elem_size, block_size);
}

} // namespace bitshuffle
} // namespace doris
