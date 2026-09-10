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
#include "storage/index/snii/writer/term_posting_test_utils.h"

// Ingestion retains one append-only spool regardless of logical run count.
// Final reduction limits active inputs, preserves chronological ranges and
// coalesces a document split across runs. The historical run-file knob may
// further restrict fan-in; zero still uses the workspace and fd bounds.
using doris::Status;
using doris::snii::writer::SpimiTermBuffer;
using doris::snii::writer::StreamedTermPostings;
using doris::snii::writer::TermPostings;
using doris::snii::writer::materialize_streamed_term;

namespace snii_testing = doris::snii::writer::testing;

namespace {

// Distinct short ordinary terms ("uaa", "uab", ...).
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

struct DrainedTerm {
    std::vector<uint32_t> docids;
    std::vector<uint32_t> freqs;
    std::vector<uint32_t> positions;
};

std::map<std::string, DrainedTerm> drain(SpimiTermBuffer* buf) {
    std::map<std::string, DrainedTerm> out;
    Status s = buf->for_each_term_sorted([&out](StreamedTermPostings&& source) {
        TermPostings tp;
        RETURN_IF_ERROR(materialize_streamed_term(std::move(source), &tp));
        DrainedTerm& t = out[tp.term];
        t.docids = tp.docids;
        t.freqs = tp.freqs;
        t.positions = tp.positions_flat;
        return Status::OK();
    });
    EXPECT_TRUE(s.ok()) << s.to_string();
    return out;
}

TEST(SniiSpimiRunCap, IngestionUsesOneSpoolWithoutRewritingEarlierRuns) {
    snii_testing::reset_run_compactions();
    SpimiTermBuffer buf(/*has_positions=*/true, /*spill_threshold_bytes=*/1024);
    constexpr size_t kCap = 3;
    buf.set_max_run_files(kCap);
    constexpr uint32_t steps = 30;
    for (uint32_t k = 0; k < steps; ++k) {
        feed_step(&buf, k);
        EXPECT_LE(buf.spill_file_count_for_test(), 1U);
    }
    ASSERT_TRUE(buf.status().ok()) << buf.status();
    EXPECT_GT(buf.run_count_for_test(), kCap);
    EXPECT_EQ(snii_testing::run_compactions(), 0U);

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
    // Both sides retain 90 logical ranges. Two-input and automatically sized
    // contiguous merge groups must produce the same term stream.
    constexpr uint32_t kSteps = 30;

    SpimiTermBuffer capped(/*has_positions=*/true, /*spill_threshold_bytes=*/1024);
    capped.set_max_run_files(2); // two active inputs per contiguous merge group
    for (uint32_t k = 0; k < kSteps; ++k) {
        feed_step(&capped, k);
    }
    ASSERT_TRUE(capped.status().ok()) << capped.status().to_string();
    EXPECT_EQ(capped.spill_file_count_for_test(), 1U);
    EXPECT_EQ(snii_testing::run_compactions(), 0U);

    SpimiTermBuffer control(/*has_positions=*/true, /*spill_threshold_bytes=*/1024);
    control.set_max_run_files(0); // choose fan-in from workspace and fd limits
    for (uint32_t k = 0; k < kSteps; ++k) {
        feed_step(&control, k);
    }
    ASSERT_TRUE(control.status().ok()) << control.status().to_string();
    ASSERT_GT(control.run_count_for_test(), 2U) << "the control must hold many runs";

    std::map<std::string, DrainedTerm> got = drain(&capped);
    EXPECT_GT(snii_testing::run_compactions(), 0U);
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

TEST(SniiSpimiRunCap, ZeroCapKeepsIngestionBoundedAndChoosesMergeFanInFromBudget) {
    snii_testing::reset_run_compactions();
    SpimiTermBuffer buf(/*has_positions=*/true, /*spill_threshold_bytes=*/1024);
    buf.set_max_run_files(0);
    constexpr uint32_t kSteps = 30; // 90 per-token spill runs, uncapped
    for (uint32_t k = 0; k < kSteps; ++k) {
        feed_step(&buf, k);
    }
    ASSERT_TRUE(buf.status().ok()) << buf.status().to_string();
    EXPECT_GT(buf.run_count_for_test(), 2U);
    EXPECT_EQ(buf.spill_file_count_for_test(), 1U);
    EXPECT_EQ(snii_testing::run_compactions(), 0U);
    // Still drains cleanly through the plain multi-run merge.
    std::map<std::string, DrainedTerm> got = drain(&buf);
    EXPECT_EQ(got.size(), static_cast<size_t>(kSteps) + 1U);
}

} // namespace
