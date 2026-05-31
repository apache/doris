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

// S3-first GATING measurement: whole-term ZSTD (today) vs per-BLOCK ZSTD on a
// large high-doc-freq term, as a function of block granularity. Block-granular
// framing is required so a selective S3 query can range-GET only the blocks it
// touches; the risk is that per-block ZSTD resets its dictionary at every block
// boundary and loses the cross-block redundancy that gives repetitive data its
// ~98% whole-term-ZSTD win. This quantifies that loss for .frq (PFOR integer
// streams: doc-deltas + freqs) and .prx (VInt position deltas) across block
// sizes, so the block size can be picked on the granularity-vs-compression
// curve. Deterministic; size is exact. Per block we keep min(ZSTD, raw) — the
// real per-block RAW fallback. NOT a production change.

#include <gtest/gtest.h>
#include <zstd.h>

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <functional>
#include <random>
#include <string>
#include <vector>

#include "storage/index/inverted/spimi/pfor_encoder.h"

namespace doris::segment_v2::inverted_index::spimi {

namespace {

void append_vint(std::vector<uint8_t>& b, uint32_t v) {
    while (v & ~0x7FU) {
        b.push_back(static_cast<uint8_t>((v & 0x7FU) | 0x80U));
        v >>= 7U;
    }
    b.push_back(static_cast<uint8_t>(v));
}

// ZSTD-1 with the real per-block RAW fallback: keep compressed only if it beats
// raw past the ~10B (mode + two VInt lengths) framing header, else raw+1 mode B.
size_t framed_zstd(const uint8_t* p, size_t n) {
    if (n == 0) {
        return 0;
    }
    const size_t bound = ZSTD_compressBound(n);
    std::vector<uint8_t> out(bound);
    const size_t c = ZSTD_compress(out.data(), bound, p, n, 1);
    if (!ZSTD_isError(c) && c + 10 < n) {
        return 1 /*mode*/ + 5 /*~VInt(uncomp)*/ + 5 /*~VInt(comp)*/ + c;
    }
    return 1 /*mode*/ + n; // raw fallback
}

// PFOR-encode `values` as 128-sub-blocks, return concatenated bytes.
std::vector<uint8_t> pfor_bytes(const std::vector<uint32_t>& values, bool patch) {
    std::vector<uint8_t> out;
    for (size_t off = 0; off < values.size(); off += SpimiPforEncoder::kBlockSize) {
        const size_t n = std::min(SpimiPforEncoder::kBlockSize, values.size() - off);
        std::vector<uint32_t> blk(values.begin() + static_cast<long>(off),
                                  values.begin() + static_cast<long>(off + n));
        const auto b = SpimiPforEncoder::EncodeBlockToBytes(blk, patch);
        out.insert(out.end(), b.begin(), b.end());
    }
    return out;
}

// Sum of per-window framed-ZSTD sizes when the value stream is chopped into
// windows of `win_values`; each window's encoded bytes are framed independently.
// encode_window: values -> the window's on-wire bytes (PFOR or VInt).
size_t per_window(const std::vector<uint32_t>& values, size_t win_values,
                  const std::function<std::vector<uint8_t>(const std::vector<uint32_t>&)>& enc) {
    size_t total = 0;
    for (size_t off = 0; off < values.size(); off += win_values) {
        const size_t n = std::min(win_values, values.size() - off);
        std::vector<uint32_t> w(values.begin() + static_cast<long>(off),
                                values.begin() + static_cast<long>(off + n));
        const auto bytes = enc(w);
        total += framed_zstd(bytes.data(), bytes.size());
    }
    return total;
}

struct Streams {
    std::vector<uint32_t> doc_deltas; // .frq doc-delta stream
    std::vector<uint32_t> freqs;      // .frq freq stream
    std::vector<uint32_t> pos_deltas; // .prx position-delta stream (reset per doc)
};

Streams gen(const std::string& regime, uint64_t seed) {
    std::mt19937_64 rng(seed);
    std::uniform_real_distribution<double> u(0, 1);
    Streams s;
    const size_t df = 16384; // large term
    for (size_t d = 0; d < df; ++d) {
        uint32_t dd = 0, fq = 0, tf = 0;
        if (regime == "repetitive") { // term in ~every doc, uniform low freq
            dd = 1;
            fq = 2 + static_cast<uint32_t>(u(rng) * 2); // 2..3
            tf = fq;
        } else if (regime == "loggy") { // clustered docs, spiky freq
            dd = (u(rng) < 0.8) ? 1 : (2 + static_cast<uint32_t>(u(rng) * 8));
            fq = (u(rng) < 0.04) ? (50 + static_cast<uint32_t>(u(rng) * 400))
                                 : (1 + static_cast<uint32_t>(u(rng) * 2));
            tf = std::min<uint32_t>(fq, 20);
        } else { // natural: varied doc gaps, zipf freq
            dd = 1 + static_cast<uint32_t>(1.0 / (u(rng) + 0.03));
            fq = 1 + static_cast<uint32_t>(1.0 / (u(rng) + 0.1));
            tf = std::min<uint32_t>(fq, 30);
        }
        s.doc_deltas.push_back(dd);
        s.freqs.push_back(fq);
        // positions for this doc (reset per doc): first absolute + small deltas
        auto pos = static_cast<uint32_t>(u(rng) * 50);
        s.pos_deltas.push_back(pos);
        for (uint32_t i = 1; i < tf; ++i) {
            s.pos_deltas.push_back(1 + static_cast<uint32_t>(u(rng) * 3));
        }
    }
    return s;
}

std::vector<uint8_t> vint_bytes(const std::vector<uint32_t>& v) {
    std::vector<uint8_t> b;
    for (uint32_t x : v) {
        append_vint(b, x);
    }
    return b;
}

void report(const std::string& regime, uint64_t seed) {
    const Streams s = gen(regime, seed);

    // ---- .frq integer stream = PFOR(doc-deltas, unpatched) ++ PFOR(freqs, patched) ----
    std::vector<uint8_t> frq_all = pfor_bytes(s.doc_deltas, false);
    {
        const auto fb = pfor_bytes(s.freqs, true);
        frq_all.insert(frq_all.end(), fb.begin(), fb.end());
    }
    const size_t frq_whole = framed_zstd(frq_all.data(), frq_all.size());

    // ---- .prx stream = VInt position deltas ----
    const std::vector<uint8_t> prx_all = vint_bytes(s.pos_deltas);
    const size_t prx_whole = framed_zstd(prx_all.data(), prx_all.size());

    auto frq_enc = [](const std::vector<uint32_t>& w) { return pfor_bytes(w, true); };
    auto prx_enc = [](const std::vector<uint32_t>& w) { return vint_bytes(w); };

    std::printf("[block-zstd][%-11s] FRQ whole=%zuB | ", regime.c_str(), frq_whole);
    for (size_t W : {128UL, 256UL, 512UL, 1024UL, 2048UL}) {
        // .frq windows are over the COMBINED doc-delta+freq value count; approximate
        // by framing doc-deltas and freqs each in W-windows.
        const size_t blk = per_window(s.doc_deltas, W, frq_enc) + per_window(s.freqs, W, frq_enc);
        std::printf("W%zu=%zu(+%.1f%%) ", W, blk,
                    100.0 * (static_cast<double>(blk) / static_cast<double>(frq_whole) - 1.0));
    }
    std::printf("\n[block-zstd][%-11s] PRX whole=%zuB | ", regime.c_str(), prx_whole);
    for (size_t W : {128UL, 256UL, 512UL, 1024UL, 2048UL}) {
        const size_t blk = per_window(s.pos_deltas, W, prx_enc);
        std::printf("W%zu=%zu(+%.1f%%) ", W, blk,
                    100.0 * (static_cast<double>(blk) / static_cast<double>(prx_whole) - 1.0));
    }
    std::printf("\n");
}

} // namespace

TEST(BlockVsWholeTermZstd, GatingMeasurement) {
    std::puts("\n===== per-BLOCK ZSTD vs whole-term ZSTD (S3 granularity gate) =====");
    std::puts("(+% = per-window framed size vs whole-term; lower regression = safer to block.");
    std::puts(" W = values per window; smaller W = finer S3 range-GET but more ZSTD resets.)");
    report("repetitive", 0xC0DE0001);
    report("loggy", 0xC0DE0002);
    report("natural", 0xC0DE0003);
}

} // namespace doris::segment_v2::inverted_index::spimi
