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

#include <cstdint>
#include <string>
#include <vector>

#include "storage/index/snii/writer/memory_reporter.h"
#include "storage/index/snii/writer/spimi_term_buffer.h"
#include "storage/index/snii/writer/term_posting_test_utils.h"

using doris::snii::writer::MemoryReporter;
using doris::snii::writer::SpimiTermBuffer;
using doris::snii::writer::StreamedTermPostings;
using doris::snii::writer::TermPostings;
using doris::Status;

namespace {

std::string ordinary_term(uint32_t id) {
    return "ordinary-prefix-sharing-resident-term-" + std::to_string(id) + "-payload";
}

void expect_same_stream(const std::vector<TermPostings>& got, const std::vector<TermPostings>& want,
                        const char* label) {
    ASSERT_EQ(got.size(), want.size()) << label;
    for (size_t i = 0; i < got.size(); ++i) {
        EXPECT_EQ(got[i].term, want[i].term) << label << " term order diverged at " << i;
        EXPECT_EQ(got[i].docids, want[i].docids) << label << " " << got[i].term;
        EXPECT_EQ(got[i].freqs, want[i].freqs) << label << " " << got[i].term;
        EXPECT_EQ(got[i].positions_flat, want[i].positions_flat) << label << " " << got[i].term;
    }
}

TEST(SniiSpimiResidentAccounting, ResidentCoversOwnedVocabularyAndIsMonotone) {
    SpimiTermBuffer buf(/*has_positions=*/true, /*spill_threshold_bytes=*/0);

    constexpr uint32_t kTerms = 4000;
    uint64_t previous = buf.resident_bytes_for_test();
    uint64_t owned_string_payload_floor = 0;
    for (uint32_t id = 0; id < kTerms; ++id) {
        const std::string term = ordinary_term(id);
        owned_string_payload_floor += term.size() + 1;
        buf.add_token(term, /*docid=*/0, /*pos=*/id);
        const uint64_t current = buf.resident_bytes_for_test();
        ASSERT_GE(current, previous) << "resident_bytes decreased at ordinary term " << id;
        previous = current;
    }

    ASSERT_TRUE(buf.status().ok()) << buf.status().to_string();
    EXPECT_EQ(buf.unique_terms(), kTerms);
    EXPECT_GE(buf.resident_bytes_for_test(), owned_string_payload_floor);
}

TEST(SniiSpimiResidentAccounting, TinyThresholdSpillsOnOwnedVocabularyGrowth) {
    SpimiTermBuffer buf(/*has_positions=*/true, /*spill_threshold_bytes=*/64 * 1024);

    uint32_t terms_fed = 0;
    for (; terms_fed < 4000 && buf.run_count_for_test() == 0; ++terms_fed) {
        buf.add_token(ordinary_term(terms_fed), /*docid=*/0, /*pos=*/terms_fed);
    }
    ASSERT_TRUE(buf.status().ok()) << buf.status().to_string();
    ASSERT_GT(buf.run_count_for_test(), 0U);

    size_t terms_seen = 0;
    ASSERT_TRUE(buf.for_each_term_sorted([&terms_seen](StreamedTermPostings&& source) {
                       RETURN_IF_ERROR(
                               doris::snii::writer::consume_streamed_term(std::move(source)));
                       ++terms_seen;
                       return Status::OK();
                   }).ok());
    EXPECT_EQ(terms_seen, terms_fed);
    EXPECT_TRUE(buf.status().ok()) << buf.status().to_string();
}

TEST(SniiSpimiResidentAccounting, OwnedVocabularySpillEqualsNoSpillControl) {
    SpimiTermBuffer spilled(/*has_positions=*/true, /*spill_threshold_bytes=*/64 * 1024);
    SpimiTermBuffer control(/*has_positions=*/true, /*spill_threshold_bytes=*/0);

    constexpr uint32_t kTerms = 800;
    for (SpimiTermBuffer* buf : {&spilled, &control}) {
        for (uint32_t id = 0; id < kTerms; ++id) {
            buf->add_token(ordinary_term(id), /*docid=*/1, /*pos=*/id);
        }
        for (uint32_t id = 0; id < kTerms; ++id) {
            buf->add_token(ordinary_term(id), /*docid=*/2, /*pos=*/id + 1);
        }
        ASSERT_TRUE(buf->status().ok()) << buf->status().to_string();
    }
    ASSERT_GE(spilled.run_count_for_test(), 1U);
    ASSERT_EQ(control.run_count_for_test(), 0U);

    const std::vector<TermPostings> got = spilled.finalize_sorted();
    const std::vector<TermPostings> want = control.finalize_sorted();
    ASSERT_TRUE(spilled.status().ok()) << spilled.status().to_string();
    ASSERT_TRUE(control.status().ok()) << control.status().to_string();
    ASSERT_EQ(want.size(), kTerms);
    expect_same_stream(got, want, "owned-vocabulary spill");
}

TEST(SniiSpimiResidentAccounting, OwnedModeReporterNetsToZeroAfterInMemoryDrain) {
    MemoryReporter reporter;
    SpimiTermBuffer buf(/*has_positions=*/true, /*spill_threshold_bytes=*/0, &reporter);
    for (uint32_t id = 0; id < 600; ++id) {
        buf.add_token(ordinary_term(id), /*docid=*/0, /*pos=*/id);
        buf.add_token(ordinary_term(id), /*docid=*/1, /*pos=*/id + 1);
    }

    EXPECT_GT(reporter.current_bytes(), 0);
    size_t terms_seen = 0;
    ASSERT_TRUE(buf.for_each_term_sorted([&terms_seen](StreamedTermPostings&& source) {
                       RETURN_IF_ERROR(
                               doris::snii::writer::consume_streamed_term(std::move(source)));
                       ++terms_seen;
                       return Status::OK();
                   }).ok());
    EXPECT_EQ(terms_seen, 600U);
    EXPECT_EQ(reporter.current_bytes(), 0);
}

TEST(SniiSpimiResidentAccounting, OwnedModeReporterNetsToZeroAfterSpilledDrain) {
    MemoryReporter reporter(/*consume_release=*/nullptr, /*cap_bytes=*/64 * 1024,
                            MemoryReporter::CapPolicy::kSpillThreshold);
    SpimiTermBuffer buf(/*has_positions=*/true, /*spill_threshold_bytes=*/0, &reporter);

    uint32_t terms_fed = 0;
    for (; terms_fed < 4000 && buf.run_count_for_test() < 2; ++terms_fed) {
        buf.add_token(ordinary_term(terms_fed), /*docid=*/0, /*pos=*/terms_fed);
    }
    ASSERT_TRUE(buf.status().ok()) << buf.status().to_string();
    ASSERT_GE(buf.run_count_for_test(), 1U);
    EXPECT_GT(reporter.current_bytes(), 0);

    size_t terms_seen = 0;
    ASSERT_TRUE(buf.for_each_term_sorted([&terms_seen](StreamedTermPostings&& source) {
                       RETURN_IF_ERROR(
                               doris::snii::writer::consume_streamed_term(std::move(source)));
                       ++terms_seen;
                       return Status::OK();
                   }).ok());
    EXPECT_EQ(terms_seen, terms_fed);
    EXPECT_EQ(reporter.current_bytes(), 0);
}

} // namespace
