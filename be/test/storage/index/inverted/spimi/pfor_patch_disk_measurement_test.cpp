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

// Macro disk-win measurement for the Opt-PForDelta freq patch channel.
// Encodes realistic high-doc-freq per-doc term-frequency streams as 128-value
// PFOR blocks, both WITHOUT (allow_patch=false, legacy) and WITH
// (allow_patch=true) the patch trailer, and reports the .frq freq-region byte
// delta — raw, and after the whole-term ZSTD-1 envelope that FlushFrqBlock
// applies in production. The patch only ever touches freq blocks, so this
// isolates exactly what it affects. Deterministic (fixed seeds); prints a table
// and asserts the patch never enlarges output.

#include <gtest/gtest.h>
#include <zstd.h>

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <random>
#include <string>
#include <vector>

#include "storage/index/inverted/spimi/pfor_encoder.h"

namespace doris::segment_v2::inverted_index::spimi {

namespace {

constexpr size_t kDocFreq = 4096; // 32 full PFOR blocks per term

// Sum of per-128-block encoded bytes for a full freq run.
size_t encode_run_bytes(const std::vector<uint32_t>& freqs, bool allow_patch,
                        std::vector<uint8_t>* concat, size_t* patched_blocks) {
    size_t total = 0;
    for (size_t off = 0; off < freqs.size(); off += SpimiPforEncoder::kBlockSize) {
        const size_t n = std::min(SpimiPforEncoder::kBlockSize, freqs.size() - off);
        std::vector<uint32_t> block(freqs.begin() + static_cast<long>(off),
                                    freqs.begin() + static_cast<long>(off + n));
        const std::vector<uint8_t> bytes = SpimiPforEncoder::EncodeBlockToBytes(block, allow_patch);
        // A patched block sets 0x80 on the width byte (2nd byte, after VInt(n)
        // which is 1 byte for n<=127, 2 bytes for n==128).
        const size_t wbyte = (n < 128) ? 1 : 2;
        if (allow_patch && wbyte < bytes.size() && (bytes[wbyte] & 0x80U) != 0U) {
            ++*patched_blocks;
        }
        total += bytes.size();
        concat->insert(concat->end(), bytes.begin(), bytes.end());
    }
    return total;
}

size_t zstd1(const std::vector<uint8_t>& in) {
    if (in.empty()) {
        return 0;
    }
    const size_t bound = ZSTD_compressBound(in.size());
    std::vector<uint8_t> out(bound);
    const size_t csize = ZSTD_compress(out.data(), bound, in.data(), in.size(), /*level=*/1);
    return ZSTD_isError(csize) ? in.size() : csize;
}

double pct(size_t plain, size_t patched) {
    if (plain == 0) {
        return 0.0;
    }
    return 100.0 * (1.0 - static_cast<double>(patched) / static_cast<double>(plain));
}

void measure(const std::string& name, const std::vector<uint32_t>& freqs) {
    std::vector<uint8_t> plain_concat, patched_concat;
    size_t patched_blocks = 0, ignored = 0;
    const size_t plain = encode_run_bytes(freqs, /*allow_patch=*/false, &plain_concat, &ignored);
    const size_t patched =
            encode_run_bytes(freqs, /*allow_patch=*/true, &patched_concat, &patched_blocks);
    const size_t zplain = zstd1(plain_concat);
    const size_t zpatched = zstd1(patched_concat);
    const size_t blocks =
            (freqs.size() + SpimiPforEncoder::kBlockSize - 1) / SpimiPforEncoder::kBlockSize;

    // The patch is opt-in per block on a strict byte win, so it must never grow
    // the raw output.
    EXPECT_LE(patched, plain) << name << ": patched raw bytes exceeded plain";

    std::printf(
            "[pfor-patch][%-14s] df=%zu blocks=%zu patched=%zu/%zu | raw .frq-freq: "
            "plain=%zu patched=%zu (%.1f%%) | zstd-1: plain=%zu patched=%zu (%.1f%%)\n",
            name.c_str(), freqs.size(), blocks, patched_blocks, blocks, plain, patched,
            pct(plain, patched), zplain, zpatched, pct(zplain, zpatched));
}

// ---- realistic per-doc term-frequency distributions for one high-DF term ----

// Zipfian-ish natural-language: most docs see the term once or twice, a long
// thin tail of topical docs see it many times.
std::vector<uint32_t> zipf_text(uint64_t seed) {
    std::mt19937_64 rng(seed);
    std::uniform_real_distribution<double> u(0.0, 1.0);
    std::vector<uint32_t> v;
    v.reserve(kDocFreq);
    for (size_t i = 0; i < kDocFreq; ++i) {
        // freq = 1 + floor(C / U^0.9): heavy mass at 1, tail into the hundreds.
        const double x = u(rng);
        auto f = static_cast<uint32_t>(1.0 + 1.0 / std::pow(x + 1e-6, 0.9));
        v.push_back(std::min<uint32_t>(f, 400));
    }
    return v;
}

// Base freq 1-2 everywhere, ~3% of docs are large spikes (50..800). The classic
// PForDelta outlier case the patch targets.
std::vector<uint32_t> log_spikes(uint64_t seed) {
    std::mt19937_64 rng(seed);
    std::uniform_int_distribution<uint32_t> base(1, 2);
    std::uniform_int_distribution<uint32_t> spike(50, 800);
    std::uniform_real_distribution<double> u(0.0, 1.0);
    std::vector<uint32_t> v;
    v.reserve(kDocFreq);
    for (size_t i = 0; i < kDocFreq; ++i) {
        v.push_back(u(rng) < 0.03 ? spike(rng) : base(rng));
    }
    return v;
}

// Two clusters: half small (1-2), half large (200-260). High entropy, few-bit
// base won't cover the large half, so the patch should mostly decline.
std::vector<uint32_t> bimodal(uint64_t seed) {
    std::mt19937_64 rng(seed);
    std::uniform_int_distribution<uint32_t> lo(1, 2);
    std::uniform_int_distribution<uint32_t> hi(200, 260);
    std::uniform_real_distribution<double> u(0.0, 1.0);
    std::vector<uint32_t> v;
    v.reserve(kDocFreq);
    for (size_t i = 0; i < kDocFreq; ++i) {
        v.push_back(u(rng) < 0.5 ? hi(rng) : lo(rng));
    }
    return v;
}

// No outliers: all freqs in {1,2,3}. Patch must be a no-op (byte-identical).
std::vector<uint32_t> uniform_low(uint64_t seed) {
    std::mt19937_64 rng(seed);
    std::uniform_int_distribution<uint32_t> d(1, 3);
    std::vector<uint32_t> v(kDocFreq);
    for (auto& x : v) {
        x = d(rng);
    }
    return v;
}

} // namespace

TEST(PforPatchDiskMeasurement, FreqStreamSizeAcrossDistributions) {
    std::puts("\n===== Opt-PForDelta freq patch: .frq freq-stream size (one high-DF term) =====");
    measure("zipf_text", zipf_text(0xF00D0001));
    measure("log_spikes", log_spikes(0xF00D0002));
    measure("bimodal", bimodal(0xF00D0003));
    measure("uniform_low", uniform_low(0xF00D0004));
    std::puts(
            "(raw = pre-ZSTD PFOR freq bytes; zstd-1 = the whole-term envelope FlushFrqBlock"
            " applies. Negative % would be a regression; the patch is opt-in per block so raw"
            " is always <= plain.)");
}

} // namespace doris::segment_v2::inverted_index::spimi
