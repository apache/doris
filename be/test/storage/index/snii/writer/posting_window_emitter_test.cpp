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

#include "storage/index/snii/writer/posting_window_emitter.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <numeric>
#include <span>
#include <vector>

#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/format/frq_prelude.h"
#include "storage/index/snii_query_test_util.h"

namespace doris::snii::writer {
namespace {

using format::DictEntry;
using format::DictEntryEnc;
using format::DictEntryKind;
using format::FrqPreludeReader;
using format::WindowMeta;
using snii_test::MemoryFile;

void assert_ok(const Status& status) {
    ASSERT_TRUE(status.ok()) << status.to_string();
}

std::vector<WindowMeta> read_windows(const MemoryFile& file, const DictEntry& entry) {
    EXPECT_EQ(entry.enc, DictEntryEnc::kWindowed);
    EXPECT_LE(entry.frq_off_delta + entry.prelude_len, file.data().size());
    if (entry.frq_off_delta + entry.prelude_len > file.data().size()) {
        return {};
    }
    FrqPreludeReader prelude;
    assert_ok(FrqPreludeReader::open(
            Slice(file.data().data() + entry.frq_off_delta, entry.prelude_len), &prelude));
    std::vector<WindowMeta> windows;
    windows.reserve(prelude.n_windows());
    for (uint32_t i = 0; i < prelude.n_windows(); ++i) {
        WindowMeta window;
        assert_ok(prelude.window(i, &window));
        windows.push_back(window);
    }
    return windows;
}

TEST(PostingWindowEmitterTest, EmitsPositionedRecutAndDocsOnlyMetadata) {
    constexpr uint32_t kDocs = 512;
    constexpr uint32_t kFarPosition = 1U << 28;
    std::vector<uint32_t> docids(kDocs);
    std::iota(docids.begin(), docids.end(), 0);
    std::vector<uint32_t> freqs(kDocs, 2);
    std::vector<uint32_t> position_offsets(kDocs + 1);
    std::vector<uint32_t> positions;
    positions.reserve(kDocs * 2);
    for (uint32_t doc = 0; doc < kDocs; ++doc) {
        position_offsets[doc] = static_cast<uint32_t>(positions.size());
        positions.push_back(0);
        positions.push_back(kFarPosition);
    }
    position_offsets.back() = static_cast<uint32_t>(positions.size());

    testing::reset_window_emitter_counters();
    MemoryFile positioned_file;
    WindowEmitterOptions positioned_options;
    positioned_options.posting_out = &positioned_file;
    positioned_options.has_freq = true;
    positioned_options.has_prx = true;
    positioned_options.prx_window_limits = {
            .max_docs = 1024,
            .max_positions = 2048,
            .max_uncomp_bytes = 2048,
    };
    WindowEmitter positioned(positioned_options);
    assert_ok(positioned.emit_window(PostingRunView {
            .docids = docids,
            .freqs = freqs,
            .position_offsets = position_offsets,
            .positions_flat = positions,
    }));
    DictEntry positioned_entry;
    TermAggregateStats positioned_stats;
    assert_ok(positioned.finish_term(&positioned_entry, &positioned_stats));

    EXPECT_EQ(positioned_entry.kind, DictEntryKind::kPodRef);
    EXPECT_EQ(positioned_entry.enc, DictEntryEnc::kWindowed);
    EXPECT_EQ(positioned_stats.df, kDocs);
    EXPECT_EQ(positioned_stats.total_freq, kDocs * 2);
    EXPECT_EQ(positioned_stats.max_freq, 2);
    const std::vector<WindowMeta> positioned_windows =
            read_windows(positioned_file, positioned_entry);
    ASSERT_GT(positioned_windows.size(), 1U);
    EXPECT_LT(positioned_windows.front().doc_count, kDocs);
    EXPECT_EQ(std::accumulate(positioned_windows.begin(), positioned_windows.end(), 0U,
                              [](uint32_t total, const WindowMeta& window) {
                                  return total + window.doc_count;
                              }),
              kDocs);

    MemoryFile docs_only_file;
    WindowEmitterOptions docs_only_options;
    docs_only_options.posting_out = &docs_only_file;
    WindowEmitter docs_only(docs_only_options);
    PostingRunView first_docs_only;
    first_docs_only.docids = std::span<const uint32_t>(docids).first(256);
    assert_ok(docs_only.emit_window(first_docs_only));
    PostingRunView second_docs_only;
    second_docs_only.docids = std::span<const uint32_t>(docids).subspan(256);
    assert_ok(docs_only.emit_window(second_docs_only));
    DictEntry docs_only_entry;
    TermAggregateStats docs_only_stats;
    assert_ok(docs_only.finish_term(&docs_only_entry, &docs_only_stats));

    EXPECT_EQ(docs_only_stats.df, kDocs);
    EXPECT_EQ(docs_only_stats.total_freq, kDocs);
    EXPECT_EQ(docs_only_stats.max_freq, 0);
    const std::vector<WindowMeta> docs_only_windows = read_windows(docs_only_file, docs_only_entry);
    ASSERT_EQ(docs_only_windows.size(), 2U);
    EXPECT_EQ(docs_only_windows[0].doc_count, 256U);
    EXPECT_EQ(docs_only_windows[1].doc_count, 256U);
    EXPECT_EQ(testing::window_emitter_finished_terms(), 2U);
    EXPECT_EQ(testing::window_emitter_physical_windows(),
              positioned_windows.size() + docs_only_windows.size());
}

TEST(PostingWindowEmitterTest, FailurePoisonsTheEmitter) {
    std::vector<uint32_t> docids {1, 2};
    std::vector<uint32_t> freqs {1, 1};
    std::vector<uint32_t> malformed_offsets {0, 1};
    std::vector<uint32_t> positions {0, 0};
    MemoryFile file;
    WindowEmitterOptions options;
    options.posting_out = &file;
    options.has_freq = true;
    options.has_prx = true;
    WindowEmitter emitter(options);

    EXPECT_FALSE(emitter.emit_window(PostingRunView {
                                             .docids = docids,
                                             .freqs = freqs,
                                             .position_offsets = malformed_offsets,
                                             .positions_flat = positions,
                                     })
                         .ok());
    DictEntry entry;
    TermAggregateStats stats;
    EXPECT_FALSE(emitter.finish_term(&entry, &stats).ok());
    EXPECT_TRUE(file.data().empty());
}

TEST(PostingWindowEmitterTest, RejectsPositionStatsWithoutPrxOffsets) {
    std::vector<uint32_t> docids {1, 2};
    MemoryFile file;
    WindowEmitterOptions options;
    options.posting_out = &file;
    options.term_frequency_source = TermFrequencySource::kPositions;
    WindowEmitter emitter(options);

    PostingRunView run;
    run.docids = docids;
    const Status status = emitter.emit_window(run);
    EXPECT_TRUE(status.is<ErrorCode::INVALID_ARGUMENT>()) << status.to_string();
    EXPECT_NE(status.to_string().find("position-derived statistics require PRX offsets"),
              std::string::npos);
    DictEntry entry;
    TermAggregateStats stats;
    EXPECT_FALSE(emitter.finish_term(&entry, &stats).ok());
    EXPECT_TRUE(file.data().empty());
}

TEST(PostingWindowEmitterTest, AcceptsBaseRelativePositionRunWithNonzeroOffsets) {
    std::vector<uint32_t> docids {1, 4};
    std::vector<uint32_t> freqs {2, 1};
    std::vector<uint32_t> position_offsets {10, 12, 13};
    std::vector<uint32_t> positions {3, 7, 9};
    MemoryFile file;
    WindowEmitterOptions options;
    options.posting_out = &file;
    options.has_freq = true;
    options.has_prx = true;
    WindowEmitter emitter(options);

    assert_ok(emitter.emit_window(PostingRunView {
            .docids = docids,
            .freqs = freqs,
            .position_offsets = position_offsets,
            .positions_flat = positions,
    }));
    DictEntry entry;
    TermAggregateStats stats;
    assert_ok(emitter.finish_term(&entry, &stats));
    EXPECT_EQ(stats.df, 2U);
    EXPECT_EQ(stats.total_freq, 3U);
    EXPECT_EQ(stats.max_freq, 2U);
}

} // namespace
} // namespace doris::snii::writer
