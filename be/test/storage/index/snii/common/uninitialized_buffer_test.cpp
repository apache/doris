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

#include "snii/common/uninitialized_buffer.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

#include "common/status.h"
#include "snii/common/slice.h"
#include "snii/encoding/byte_sink.h"
#include "snii/encoding/byte_source.h"
#include "snii/encoding/zstd_codec.h"
#include "snii/format/prx_pod.h"

using snii::ByteSink;
using snii::ByteSource;
using snii::Slice;
using snii::zstd_compress;
using snii::zstd_decompress;
using snii::format::build_prx_window;
using snii::format::read_prx_window_csr;

namespace {

using PerDoc = std::vector<std::vector<uint32_t>>;

// The uninitialized_vector overload must NOT value-initialize the regrown tail:
// after a sentinel fill + clear + regrow into the SAME storage, the object
// representation still holds the sentinel bytes (proves no zero-fill pass).
TEST(SniiUninitializedBuffer, UninitVectorGrowSkipsZeroFill) {
    constexpr size_t kN = 64;
    snii::uninitialized_vector<uint32_t> v;
    snii::resize_uninitialized(v, kN);
    for (size_t i = 0; i < kN; ++i) {
        v[i] = 0xAAAAAAAAU;
    }
    const uint32_t* old_data = v.data();
    v.clear(); // trivial element destruction is a no-op; storage/capacity retained
    snii::resize_uninitialized(v, kN);
    ASSERT_EQ(v.data(), old_data) << "regrow within capacity must not reallocate";
    // Examining the object representation through unsigned char* is well-defined.
    const auto* bytes = reinterpret_cast<const unsigned char*>(v.data());
    bool all_sentinel = true;
    for (size_t i = 0; i < kN * sizeof(uint32_t); ++i) {
        if (bytes[i] != 0xAA) {
            all_sentinel = false;
            break;
        }
    }
    EXPECT_TRUE(all_sentinel) << "uninitialized_vector regrow must default-init (no zero-fill)";
}

// Contrast: a plain std::vector DOES value-initialize the regrown tail to zero.
TEST(SniiUninitializedBuffer, StdVectorGrowZeroFillsForContrast) {
    constexpr size_t kN = 64;
    std::vector<uint32_t> v;
    snii::resize_uninitialized(v, kN); // std::vector overload == plain resize
    for (size_t i = 0; i < kN; ++i) {
        v[i] = 0xAAAAAAAAU;
    }
    v.clear();
    snii::resize_uninitialized(v, kN);
    for (size_t i = 0; i < kN; ++i) {
        EXPECT_EQ(v[i], 0U) << "std::vector regrow value-initializes to 0";
    }
}

// Shrinking a warm buffer must reuse storage (no realloc).
TEST(SniiUninitializedBuffer, ResizeUninitializedShrinkKeepsCapacity) {
    std::vector<uint32_t> v;
    snii::resize_uninitialized(v, 1000);
    const size_t cap = v.capacity();
    snii::resize_uninitialized(v, 10);
    EXPECT_EQ(v.capacity(), cap) << "shrink must not reallocate";
}

// Warm-reused CSR decode (large window then small window into the SAME buffers)
// must equal a fresh decode of the small window -- no stale tail leaks past size().
TEST(SniiUninitializedBuffer, CsrWarmReuseLargeThenSmallNoStaleTail) {
    PerDoc large(64);
    for (auto& doc : large) {
        uint32_t p = 0;
        for (int i = 0; i < 256; ++i) {
            doc.push_back(p += 1);
        }
    }
    const PerDoc small = {{1, 2, 3}, {10}, {7, 9}};

    ByteSink large_sink;
    ByteSink small_sink;
    ASSERT_TRUE(build_prx_window(large, -1, &large_sink).ok());
    ASSERT_TRUE(build_prx_window(small, -1, &small_sink).ok());

    std::vector<uint32_t> pos_flat;
    std::vector<uint32_t> pos_off;
    {
        ByteSource src(large_sink.view());
        ASSERT_TRUE(read_prx_window_csr(&src, &pos_flat, &pos_off).ok());
    }
    {
        ByteSource src(small_sink.view());
        ASSERT_TRUE(read_prx_window_csr(&src, &pos_flat, &pos_off).ok());
    }
    std::vector<uint32_t> exp_flat;
    std::vector<uint32_t> exp_off;
    {
        ByteSource src(small_sink.view());
        ASSERT_TRUE(read_prx_window_csr(&src, &exp_flat, &exp_off).ok());
    }
    EXPECT_EQ(pos_flat, exp_flat) << "warm-reused decode must equal a fresh decode";
    EXPECT_EQ(pos_off, exp_off);
    EXPECT_EQ(pos_flat.size(), 6U) << "small window total_pos = 3 + 1 + 2";
}

// zstd decode into a reused buffer must not reallocate and stays byte-identical.
TEST(SniiUninitializedBuffer, ZstdReusedBufferNoReallocAndByteIdentical) {
    std::string payload(50000, '\0');
    for (size_t i = 0; i < payload.size(); ++i) {
        payload[i] = static_cast<char>(i * 7 + 3);
    }
    std::vector<uint8_t> comp;
    ASSERT_TRUE(zstd_compress(Slice(payload), 3, &comp).ok());

    std::vector<uint8_t> out;
    ASSERT_TRUE(zstd_decompress(Slice(comp), payload.size(), &out).ok());
    ASSERT_EQ(out.size(), payload.size());
    EXPECT_EQ(0, std::memcmp(out.data(), payload.data(), payload.size()));
    const size_t cap = out.capacity();

    ASSERT_TRUE(zstd_decompress(Slice(comp), payload.size(), &out).ok());
    EXPECT_EQ(out.capacity(), cap) << "warm-reused decompress must not reallocate";
    EXPECT_EQ(0, std::memcmp(out.data(), payload.data(), payload.size()));
}

} // namespace
