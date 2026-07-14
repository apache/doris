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

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <map>
#include <string>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/writer/spimi_term_buffer.h"

// G09 run-file FD hygiene: a SPIMI buffer's spill runs are ALL (re)opened
// simultaneously -- one fd each, held for the whole k-way merge -- so an
// unbounded run count across ~100 concurrent writers exhausted the BE nofile
// rlimit in the conc=16 wikipedia field failure ('Too many open files' at run
// reopen). The cap (set_max_run_files / config
// snii_spill_max_run_files_per_buffer) merge-compacts the accumulated runs
// into ONE before a new spill would exceed it. These tests pin:
//   * the cap is HONORED: the tracked run count never exceeds it, and the
//     compaction seam records each collapse;
//   * compaction is INVISIBLE in the output: a capped buffer drains the
//     byte-identical term stream (terms, docids, freqs, positions) of an
//     uncapped control fed the same tokens;
//   * cap 0 disables compaction entirely (pre-cap behavior).
using doris::Status;
using doris::snii::writer::SpimiTermBuffer;
using doris::snii::writer::TermPostings;

namespace snii_testing = doris::snii::writer::testing;

namespace {

// Distinct short unigrams ("uaa", "uab", ...): SSO-sized, alpha-only, never
// bigram-marker-prefixed.
std::string unigram(uint32_t i) {
    std::string s = "u";
    s += static_cast<char>('a' + i % 26);
    s += static_cast<char>('a' + (i / 26) % 26);
    s += static_cast<char>('a' + (i / 676) % 26);
    return s;
}

// One feed step at docid k: the shared term "hot" gets TWO tokens in the SAME
// doc (freq 2 -- a spill lands between them in the per-token spill regime
// below, exercising boundary-doc coalescing across run seams and through a
// compaction) and unigram(k) gets one token (a distinct term per step). With
// the tests' tiny 1 KiB local threshold, resident (>= one fresh 32 KiB arena
// block) exceeds the cap on EVERY token and the arena floor (one block) is
// met as soon as a chain claims its block -- so every token cuts a run:
// N fed tokens == N run files, densely exercising the cap machinery with tiny
// step counts (an uncapped 30-step feed already holds 90 runs).
void feed_step(SpimiTermBuffer* buf, uint32_t k) {
    buf->add_token("hot", /*docid=*/k, /*pos=*/0);
    buf->add_token("hot", /*docid=*/k, /*pos=*/1);
    buf->add_token(unigram(k), /*docid=*/k, /*pos=*/2);
}

// Bounds the honored-cap search loop; far below unigram()'s 17576 distinct
// strings so every step's unigram is DISTINCT (the drained-term-count
// assertions rely on that).
constexpr uint32_t kMaxSteps = 16000;

struct DrainedTerm {
    std::vector<uint32_t> docids;
    std::vector<uint32_t> freqs;
    std::vector<uint32_t> positions;
};

std::map<std::string, DrainedTerm> drain(SpimiTermBuffer* buf) {
    std::map<std::string, DrainedTerm> out;
    Status s = buf->for_each_term_sorted([&out](TermPostings&& tp) {
        DrainedTerm& t = out[tp.term];
        t.docids = tp.docids;
        t.freqs = tp.freqs;
        if (tp.pos_pump) {
            // Wide streamed term: pull the pump synchronously (the contract).
            t.positions.resize(static_cast<size_t>(tp.pos_total));
            if (tp.pos_total != 0) {
                tp.pos_pump(t.positions.data(), t.positions.size());
            }
        } else {
            t.positions = tp.positions_flat;
        }
    });
    EXPECT_TRUE(s.ok()) << s.to_string();
    return out;
}

TEST(SniiSpimiRunCap, CapIsHonoredAndCompactionSeamFires) {
    snii_testing::reset_run_compactions();
    SpimiTermBuffer buf(/*has_positions=*/true, /*spill_threshold_bytes=*/1024);
    constexpr size_t kCap = 3;
    buf.set_max_run_files(kCap);

    // Feed until the cap has forced a compaction (bounded); the cap must hold
    // at EVERY step, so track the running maximum of the run count.
    size_t max_runs_seen = 0;
    uint32_t steps = 0;
    while (steps < kMaxSteps && snii_testing::run_compactions() < 1) {
        feed_step(&buf, steps);
        ++steps;
        max_runs_seen = std::max(max_runs_seen, buf.run_count_for_test());
    }
    ASSERT_TRUE(buf.status().ok()) << buf.status().to_string();
    ASSERT_GE(snii_testing::run_compactions(), 1U)
            << "the cap was never hit within " << steps << " steps";
    EXPECT_LE(max_runs_seen, kCap) << "run count exceeded the cap";

    // The capped buffer still drains every term exactly once with the full
    // posting content.
    std::map<std::string, DrainedTerm> got = drain(&buf);
    ASSERT_TRUE(buf.status().ok()) << buf.status().to_string();
    ASSERT_EQ(got.size(), static_cast<size_t>(steps) + 1U); // distinct unigrams + "hot"
    const DrainedTerm& hot = got.at("hot");
    ASSERT_EQ(hot.docids.size(), static_cast<size_t>(steps));
    EXPECT_EQ(hot.docids.front(), 0U);
    EXPECT_EQ(hot.docids.back(), steps - 1);
    for (uint32_t f : hot.freqs) {
        ASSERT_EQ(f, 2U) << "boundary-doc coalescing must survive compaction";
    }
    ASSERT_EQ(hot.positions.size(), 2ULL * steps);
    EXPECT_EQ(hot.positions[0], 0U);
    EXPECT_EQ(hot.positions[1], 1U);
    const DrainedTerm& first = got.at(unigram(0));
    ASSERT_EQ(first.docids.size(), 1U);
    EXPECT_EQ(first.docids[0], 0U);
    ASSERT_EQ(first.positions.size(), 1U);
    EXPECT_EQ(first.positions[0], 2U);
}

TEST(SniiSpimiRunCap, CompactedDrainMatchesUncappedControl) {
    snii_testing::reset_run_compactions();
    // 30 steps == 90 tokens == 90 runs uncapped (per-token spills, see
    // feed_step): plenty of cap-2 compactions on the capped side while the
    // control's 90-way merge stays trivial.
    constexpr uint32_t kSteps = 30;

    SpimiTermBuffer capped(/*has_positions=*/true, /*spill_threshold_bytes=*/1024);
    capped.set_max_run_files(2); // aggressive: compact on nearly every spill
    for (uint32_t k = 0; k < kSteps; ++k) {
        feed_step(&capped, k);
    }
    ASSERT_TRUE(capped.status().ok()) << capped.status().to_string();
    ASSERT_GE(snii_testing::run_compactions(), 1U);

    SpimiTermBuffer control(/*has_positions=*/true, /*spill_threshold_bytes=*/1024);
    control.set_max_run_files(0); // uncapped: the pre-cap spill pipeline
    for (uint32_t k = 0; k < kSteps; ++k) {
        feed_step(&control, k);
    }
    ASSERT_TRUE(control.status().ok()) << control.status().to_string();
    ASSERT_GT(control.run_count_for_test(), 2U) << "the control must hold many runs";

    std::map<std::string, DrainedTerm> got = drain(&capped);
    std::map<std::string, DrainedTerm> want = drain(&control);
    ASSERT_TRUE(capped.status().ok()) << capped.status().to_string();
    ASSERT_TRUE(control.status().ok()) << control.status().to_string();
    ASSERT_EQ(got.size(), want.size());
    for (const auto& [term, w] : want) {
        auto it = got.find(term);
        ASSERT_NE(it, got.end()) << "missing term: " << term;
        EXPECT_EQ(it->second.docids, w.docids) << term;
        EXPECT_EQ(it->second.freqs, w.freqs) << term;
        EXPECT_EQ(it->second.positions, w.positions) << term;
    }
}

TEST(SniiSpimiRunCap, ZeroCapDisablesCompaction) {
    snii_testing::reset_run_compactions();
    SpimiTermBuffer buf(/*has_positions=*/true, /*spill_threshold_bytes=*/1024);
    buf.set_max_run_files(0);
    constexpr uint32_t kSteps = 30; // 90 per-token spill runs, uncapped
    for (uint32_t k = 0; k < kSteps; ++k) {
        feed_step(&buf, k);
    }
    ASSERT_TRUE(buf.status().ok()) << buf.status().to_string();
    EXPECT_GT(buf.run_count_for_test(), 2U);
    EXPECT_EQ(snii_testing::run_compactions(), 0U);
    // Still drains cleanly through the plain multi-run merge.
    std::map<std::string, DrainedTerm> got = drain(&buf);
    EXPECT_EQ(got.size(), static_cast<size_t>(kSteps) + 1U);
}

} // namespace
