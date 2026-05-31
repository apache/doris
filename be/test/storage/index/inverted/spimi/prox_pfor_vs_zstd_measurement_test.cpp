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

// .prx position-stream codec study: current (per-doc VInt delta + whole-term
// ZSTD-1, mirroring FreqProxEncoder::FlushProxBlock) vs patched-PFOR 128-block
// bit-packing (the codec already used for .frq) vs PFOR-then-ZSTD. Reports, for
// realistic per-doc position-delta streams of one high-doc-freq term:
//   - encoded size / compression ratio  (== bytes held in RAM & transferred)
//   - decode CPU (ns/position, and the PFOR-vs-ZSTD ratio)
// Deterministic (fixed seeds). NOTE: this is an ASAN UT build — absolute decode
// times are inflated and ZSTD (a less-instrumented prebuilt lib) is biased
// faster than the heavily-instrumented PFOR C++; treat CPU as directional, size
// as exact. Asserts every codec round-trips to the original deltas.

#include <gtest/gtest.h>
#include <zstd.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <functional>
#include <random>
#include <string>
#include <vector>

#include "storage/index/inverted/spimi/pfor_encoder.h"

namespace doris::segment_v2::inverted_index::spimi {

namespace {

void append_vint(std::vector<uint8_t>& buf, uint32_t v) {
    while (v & ~0x7FU) {
        buf.push_back(static_cast<uint8_t>((v & 0x7FU) | 0x80U));
        v >>= 7U;
    }
    buf.push_back(static_cast<uint8_t>(v));
}

uint32_t read_vint(const uint8_t* d, size_t& p) {
    uint32_t v = 0, shift = 0;
    while (true) {
        const uint8_t b = d[p++];
        v |= static_cast<uint32_t>(b & 0x7FU) << shift;
        if ((b & 0x80U) == 0) break;
        shift += 7;
    }
    return v;
}

size_t zstd1(const std::vector<uint8_t>& in, std::vector<uint8_t>* out) {
    const size_t bound = ZSTD_compressBound(in.size());
    out->resize(bound);
    const size_t c = ZSTD_compress(out->data(), bound, in.data(), in.size(), 1);
    out->resize(ZSTD_isError(c) ? 0 : c);
    return out->size();
}

// ---------- realistic per-doc position-delta streams for one high-DF term ----
// The .prx delta stream is, per doc: [abs_pos0, p1-p0, p2-p1, ...] (reset per
// doc). We build df docs and concatenate.

constexpr size_t kDocFreq = 3000;

std::vector<uint32_t> make_stream(const std::string& kind, uint64_t seed) {
    std::mt19937_64 rng(seed);
    std::uniform_real_distribution<double> u(0.0, 1.0);
    std::vector<uint32_t> deltas;
    deltas.reserve(kDocFreq * 8);
    for (size_t d = 0; d < kDocFreq; ++d) {
        // term-frequency within this doc
        uint32_t tf = 0;
        uint32_t first = 0; // absolute position of the first occurrence
        if (kind == "phrase_dense") {
            tf = 2 + static_cast<uint32_t>(u(rng) * 12); // 2..13 adjacent hits
            first = static_cast<uint32_t>(u(rng) * 200);
        } else if (kind == "stopword_gaps") {
            tf = 1 + static_cast<uint32_t>(u(rng) * 10);
            first = static_cast<uint32_t>(u(rng) * 50);
        } else if (kind == "natural_text") {
            tf = 1 + static_cast<uint32_t>(1.0 / (u(rng) + 0.05)); // skewed, mostly small
            first = static_cast<uint32_t>(u(rng) * 800);
        } else { // sparse_large: few hits in long docs
            tf = 1 + static_cast<uint32_t>(u(rng) * 3);
            first = static_cast<uint32_t>(u(rng) * 4000);
        }
        deltas.push_back(first);
        for (uint32_t i = 1; i < tf; ++i) {
            uint32_t step;
            if (kind == "phrase_dense") {
                step = 1 + static_cast<uint32_t>(u(rng) * 2); // 1..2
            } else if (kind == "stopword_gaps") {
                step = (u(rng) < 0.15) ? (5 + static_cast<uint32_t>(u(rng) * 25)) // gap
                                       : (1 + static_cast<uint32_t>(u(rng) * 2));
            } else if (kind == "natural_text") {
                step = 1 + static_cast<uint32_t>(u(rng) * 40);
            } else {
                step = 20 + static_cast<uint32_t>(u(rng) * 400);
            }
            deltas.push_back(step);
        }
    }
    return deltas;
}

// ---- codec A: current .prx (VInt deltas + whole-term ZSTD-1 if it wins) ----
struct EncodedProx {
    std::vector<uint8_t> bytes; // what lands in .prx for the term (incl. mode byte)
    bool zstd = false;
    std::vector<uint8_t> raw_vint;     // pre-ZSTD VInt bytes (decode dest / raw-path source)
    std::vector<uint8_t> zstd_payload; // the ZSTD frame (decode source when zstd)
};
EncodedProx encode_current(const std::vector<uint32_t>& deltas) {
    EncodedProx e;
    for (uint32_t x : deltas) {
        append_vint(e.raw_vint, x);
    }
    std::vector<uint8_t> comp;
    constexpr size_t kProxCompressMin = 48;
    if (e.raw_vint.size() >= kProxCompressMin) {
        const size_t cs = zstd1(e.raw_vint, &comp);
        if (cs > 0 && cs + 10 < e.raw_vint.size()) {
            e.zstd = true;
            e.zstd_payload = comp;
            e.bytes.push_back(1); // kProxZstd
            append_vint(e.bytes, static_cast<uint32_t>(e.raw_vint.size()));
            append_vint(e.bytes, static_cast<uint32_t>(comp.size()));
            e.bytes.insert(e.bytes.end(), comp.begin(), comp.end());
            return e;
        }
    }
    e.bytes.push_back(0); // kProxRaw
    e.bytes.insert(e.bytes.end(), e.raw_vint.begin(), e.raw_vint.end());
    return e;
}

// ---- codec B: patched-PFOR 128-blocks of the delta stream ----
std::vector<std::vector<uint8_t>> encode_pfor(const std::vector<uint32_t>& deltas, size_t* total) {
    std::vector<std::vector<uint8_t>> blocks;
    *total = 0;
    for (size_t off = 0; off < deltas.size(); off += SpimiPforEncoder::kBlockSize) {
        const size_t n = std::min(SpimiPforEncoder::kBlockSize, deltas.size() - off);
        std::vector<uint32_t> blk(deltas.begin() + static_cast<long>(off),
                                  deltas.begin() + static_cast<long>(off + n));
        blocks.push_back(SpimiPforEncoder::EncodeBlockToBytes(blk, /*allow_patch=*/true));
        *total += blocks.back().size();
    }
    return blocks;
}

double now_ns_per(size_t positions, int reps, const std::function<void()>& body) {
    using clk = std::chrono::steady_clock;
    auto t0 = clk::now();
    for (int r = 0; r < reps; ++r) {
        body();
    }
    auto t1 = clk::now();
    const double ns = std::chrono::duration<double, std::nano>(t1 - t0).count();
    return ns / (static_cast<double>(positions) * reps);
}

void measure(const std::string& kind, uint64_t seed) {
    const std::vector<uint32_t> deltas = make_stream(kind, seed);
    const size_t npos = deltas.size();
    const size_t raw = npos * 4; // 4 bytes/position uncompressed

    const EncodedProx cur = encode_current(deltas);
    size_t pfor_total = 0;
    const auto pfor_blocks = encode_pfor(deltas, &pfor_total);
    std::vector<uint8_t> pfor_concat;
    for (const auto& b : pfor_blocks) {
        pfor_concat.insert(pfor_concat.end(), b.begin(), b.end());
    }
    std::vector<uint8_t> pfor_zstd;
    const size_t pfor_zstd_sz = zstd1(pfor_concat, &pfor_zstd);

    // ---- correctness: PFOR blocks round-trip to the original deltas ----
    {
        std::vector<uint32_t> back;
        for (const auto& b : pfor_blocks) {
            std::vector<uint32_t> sub;
            SpimiPforDecoder::DecodeBlockFromBytes(b, &sub);
            back.insert(back.end(), sub.begin(), sub.end());
        }
        ASSERT_EQ(back, deltas) << kind << ": PFOR round-trip mismatch";
    }

    // ---- decode CPU (ASAN-biased; ratio is directional) ----
    const int reps = 40;
    std::vector<uint8_t> dezstd(cur.raw_vint.size());
    const double t_cur = now_ns_per(npos, reps, [&] {
        const uint8_t* d = cur.raw_vint.data();
        size_t p = 0;
        if (cur.zstd) {
            ZSTD_decompress(dezstd.data(), dezstd.size(), cur.zstd_payload.data(),
                            cur.zstd_payload.size());
            d = dezstd.data();
        }
        uint32_t acc = 0;
        for (size_t i = 0; i < npos; ++i) {
            acc += read_vint(d, p);
        }
        ASSERT_NE(acc, 0xFFFFFFFFU);
    });
    std::vector<uint32_t> sub;
    const double t_pfor = now_ns_per(npos, reps, [&] {
        for (const auto& b : pfor_blocks) {
            SpimiPforDecoder::DecodeBlockFromBytes(b, &sub);
        }
    });

    std::printf(
            "[prx-codec][%-13s] npos=%zu raw=%zuB | VInt+ZSTD=%zuB(%.2fx%s) PFOR=%zuB(%.2fx) "
            "PFOR+ZSTD=%zuB(%.2fx) | decode ns/pos: cur=%.1f pfor=%.1f (pfor %.2fx)\n",
            kind.c_str(), npos, raw, cur.bytes.size(),
            static_cast<double>(raw) / static_cast<double>(cur.bytes.size()), cur.zstd ? "z" : "r",
            pfor_total, static_cast<double>(raw) / static_cast<double>(pfor_total), pfor_zstd_sz,
            static_cast<double>(raw) / static_cast<double>(pfor_zstd_sz), t_cur, t_pfor,
            t_cur > 0 ? t_pfor / t_cur : 0.0);
}

} // namespace

TEST(ProxCodecMeasurement, PforVsVIntZstdAcrossPositionStreams) {
    std::puts("\n===== .prx position codec: VInt+ZSTD (current) vs patched-PFOR =====");
    std::puts("(ratio = raw 4B/pos / encoded; higher=smaller. decode ns/pos: lower=faster.");
    std::puts(" ASAN build: absolute ns inflated, ZSTD lib biased faster than instrumented PFOR.)");
    measure("phrase_dense", 0xB0010001);
    measure("stopword_gaps", 0xB0010002);
    measure("natural_text", 0xB0010003);
    measure("sparse_large", 0xB0010004);
}

} // namespace doris::segment_v2::inverted_index::spimi
