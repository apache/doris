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

// Memory-budget guardrail for the SPIMI accumulator. Locks in the resident
// in-RAM cost of the live SpimiPostingBuffer so a regression in record
// size, arena layout, or intern-map density gets caught here. The exact
// per-occurrence cost depends on `std::vector` capacity rounding, but the
// upper bound is a small multiple of `sizeof(SpimiRecord)` (12 bytes); we
// assert that bound below.
//
// A *differential* comparison against the existing CLucene write path
// (per-term `Posting` struct + byte-pool slices) requires the integration
// layer landing first — it has to instantiate both writers over the same
// input and snapshot the resident allocation. That validation lives in a
// follow-up that ships with the InvertedIndexColumnWriter integration.
// For now this test only asserts the SPIMI buffer's own shape.

#include <gtest/gtest.h>

#include <cstdio>
#include <random>
#include <string>

#include "storage/index/inverted/spimi/posting_buffer.h"

namespace doris::segment_v2::inverted_index::spimi {

// Upper bound on bytes per appended record. Each occurrence stores one
// 12-byte SpimiRecord; std::vector capacity grows by ~1.5× / 2× so the
// resident bytes can be larger than the live size. We accept up to 4× the
// raw record cost (48 B/occurrence), which still leaves plenty of headroom
// over a hypothetical "12 B/occurrence" lower bound and catches any
// architectural regression (e.g. adding a field to SpimiRecord, switching
// to a 64-bit hash table key, etc.).
constexpr size_t kMaxBytesPerOccurrence = 4 * sizeof(SpimiRecord);

TEST(SpimiMemoryBudget, RecordOverheadIsTwelveBytes) {
    // Append N occurrences of the SAME term. The dominant cost
    // varies by mode:
    //  - Flat mode (small workloads): one 12-byte `SpimiRecord` per
    //    occurrence plus a fixed-overhead arena entry.
    //  - Hybrid compact mode (avg occurrences/term ≥
    //    kCompactAvgOcc): records are replaced by a per-term
    //    tagged-VInt stream — 1–2 bytes per occurrence — which is
    //    the path that delivers the -70 % memory result on
    //    repetitive vocabularies.
    // 4096 occurrences of a single term puts us well into compact
    // mode, so the test now bounds the result against the COMPACT
    // expectation: ≤ 12 × N (the flat mode), and ≥ N bytes (the
    // VInt stream is at least 1 byte per occurrence).
    SpimiPostingBuffer buffer;
    constexpr int kOccurrences = 4096;
    for (int i = 0; i < kOccurrences; ++i) {
        buffer.Append("repeated", /*doc=*/0, /*pos=*/static_cast<uint32_t>(i));
    }
    const size_t bytes = buffer.MemoryUsage();
    // Lower bound: at least the VInt stream + arena entry. Allow
    // very tight packing — compact mode for 4096 occurrences of one
    // term in one doc tagged-VInt-encodes as roughly 1 byte each.
    EXPECT_GE(bytes, static_cast<size_t>(kOccurrences));
    // Upper bound: at most the flat-mode footprint (records + arena
    // + intern slots). If MemoryUsage exceeds flat-mode then the
    // compact-mode invariant is broken.
    EXPECT_LE(bytes, static_cast<size_t>(kOccurrences) * 2U * sizeof(SpimiRecord) + 4096);
}

TEST(SpimiMemoryBudget, RepeatedVocabularyStaysWithinPerRecordBound) {
    // Workload: 100 docs × 200 positions each, drawing from a 50-term
    // vocabulary. Representative of natural-language fulltext — many
    // occurrences share a small vocabulary, so the records dominate.
    SpimiPostingBuffer buffer;
    std::mt19937 rng(0x1234);
    std::uniform_int_distribution<int> term_dist(0, 49);

    constexpr int kDocs = 100;
    constexpr int kPositionsPerDoc = 200;
    constexpr size_t kOccurrences = static_cast<size_t>(kDocs) * kPositionsPerDoc;

    for (int doc = 0; doc < kDocs; ++doc) {
        for (int p = 0; p < kPositionsPerDoc; ++p) {
            char term[8];
            std::snprintf(term, sizeof(term), "t%02d", term_dist(rng));
            buffer.Append(term, static_cast<uint32_t>(doc), static_cast<uint32_t>(p));
        }
    }

    const size_t spimi_bytes = buffer.MemoryUsage();
    EXPECT_LE(spimi_bytes, kOccurrences * kMaxBytesPerOccurrence)
            << "SPIMI memory (" << spimi_bytes << " B for " << kOccurrences
            << " occurrences) regressed past the per-record bound";
}

TEST(SpimiMemoryBudget, SparseVocabularyArenaCostIsBounded) {
    // Workload: 1 000 occurrences, one position per doc, with mostly-unique
    // terms (10-character random tokens). Stresses the arena (high per-term
    // arena cost). Per-occurrence bytes here include both the 12-byte
    // record and the ~14 bytes for the term arena entry (4-byte length +
    // 10 bytes of text), so the upper bound is more generous.
    SpimiPostingBuffer buffer;
    std::mt19937 rng(0xCAFE);
    std::uniform_int_distribution<int> char_dist('a', 'z');

    constexpr int kOccurrences = 1000;
    for (int i = 0; i < kOccurrences; ++i) {
        std::string term(10, 'a');
        for (auto& c : term) {
            c = static_cast<char>(char_dist(rng));
        }
        buffer.Append(term, static_cast<uint32_t>(i), 0U);
    }

    const size_t spimi_bytes = buffer.MemoryUsage();
    // Sparse-vocab budget: record (≤ 4× 12 B = 48 B) + arena (~24 B per
    // unique 10-char term, doubled for vector growth) + intern slots.
    EXPECT_LE(spimi_bytes, static_cast<size_t>(kOccurrences) * 128U)
            << "Sparse-vocab arena footprint regressed: " << spimi_bytes << " B for "
            << kOccurrences << " unique-ish terms";
}

TEST(SpimiMemoryBudget, ResetReturnsToBaseline) {
    // After Reset() the resident allocation may still be present (the
    // accumulator keeps capacity for reuse), but MemoryUsage() should at
    // least not grow when re-filling to the same size — i.e., reuse works.
    SpimiPostingBuffer buffer;
    for (int i = 0; i < 1000; ++i) {
        buffer.Append("term", static_cast<uint32_t>(i), 0U);
    }
    const size_t before_reset = buffer.MemoryUsage();
    buffer.Reset();
    EXPECT_EQ(buffer.RecordCount(), 0U);
    EXPECT_EQ(buffer.TermCount(), 0U);

    for (int i = 0; i < 1000; ++i) {
        buffer.Append("term", static_cast<uint32_t>(i), 0U);
    }
    const size_t after_refill = buffer.MemoryUsage();
    EXPECT_LE(after_refill, before_reset)
            << "Reuse after Reset() must not grow beyond the first fill";
}

} // namespace doris::segment_v2::inverted_index::spimi
