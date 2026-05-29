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

#include "storage/index/inverted/spimi/segment_merger.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <string>
#include <vector>

#include "storage/index/inverted/spimi/byte_output.h"
#include "storage/index/inverted/spimi/field_infos_writer.h"
#include "storage/index/inverted/spimi/fulltext_writer.h"
#include "storage/index/inverted/spimi/posting_buffer.h"
#include "storage/index/inverted/spimi/posting_decoder.h"
#include "storage/index/inverted/spimi/spill_manager.h"
#include "storage/index/inverted/spimi/term_enum.h"

namespace doris::segment_v2::inverted_index::spimi {

namespace {

// Build a SegmentMerger::Input from a SpillSegment.
SegmentMerger::Input ToInput(const SpillSegment& seg) {
    SegmentMerger::Input in;
    in.tis_bytes = seg.tis_bytes;
    in.tii_bytes = seg.tii_bytes;
    in.frq_bytes = seg.frq_bytes;
    in.prx_bytes = seg.prx_bytes;
    in.doc_count = seg.doc_count;
    return in;
}

// Helper: emit a segment from buffer data and return it as a SegmentMerger::Input.
SegmentMerger::Input MakeInput(SpimiPostingBuffer& buffer, int32_t doc_count) {
    MemoryByteOutput tis, tii, frq, prx, fnm, nrm, seg_n, seg_gen;
    SpimiSegmentSink sink {.tis = &tis,
                           .tii = &tii,
                           .frq = &frq,
                           .prx = &prx,
                           .fnm = &fnm,
                           .nrm = &nrm,
                           .segments_n = &seg_n,
                           .segments_gen = &seg_gen};
    SpimiFulltextWriter::EmitSegment(buffer, sink, "_seg", "content", doc_count,
                                     FieldInfosWriter::kIndexVersionV1,
                                     /*omit_term_freq_and_positions=*/false,
                                     /*omit_norms=*/true);
    return {tis.bytes(), tii.bytes(), frq.bytes(), prx.bytes(), doc_count};
}

// Helper: run merge and return (term_count, output bytes).
struct MergeResult {
    int64_t term_count;
    std::vector<uint8_t> tis_bytes;
    std::vector<uint8_t> frq_bytes;
    std::vector<uint8_t> prx_bytes;
};

MergeResult DoMerge(const std::vector<SegmentMerger::Input>& inputs,
                    int32_t total_doc_count) {
    MemoryByteOutput tis, tii, frq, prx, fnm, nrm, seg_n, seg_gen;
    SpimiSegmentSink sink {.tis = &tis,
                           .tii = &tii,
                           .frq = &frq,
                           .prx = &prx,
                           .fnm = &fnm,
                           .nrm = &nrm,
                           .segments_n = &seg_n,
                           .segments_gen = &seg_gen};
    const int64_t tc = SegmentMerger::Merge(
            inputs, sink, "_merged", "content", total_doc_count,
            FieldInfosWriter::kIndexVersionV1,
            /*omit_term_freq_and_positions=*/false,
            /*omit_norms=*/true);
    return {tc, tis.bytes(), frq.bytes(), prx.bytes()};
}

// Read all terms from merged .tis bytes.
std::vector<TermEntry> ReadAllTerms(const std::vector<uint8_t>& tis_bytes) {
    std::vector<TermEntry> entries;
    TermEnum en(tis_bytes);
    while (en.Next()) {
        entries.push_back(en.Current());
    }
    return entries;
}

// Decode one term's posting list from merged .frq/.prx bytes.
std::vector<DecodedDoc> DecodeTerm(const TermEntry& entry,
                                   const std::vector<uint8_t>& frq_bytes,
                                   const std::vector<uint8_t>& prx_bytes,
                                   const TermEntry* next_entry) {
    const int64_t frq_start = entry.info.freq_pointer;
    const size_t frq_len = static_cast<size_t>(frq_bytes.size()) -
                           static_cast<size_t>(frq_start);
    const int64_t prx_start = entry.info.prox_pointer;
    const int64_t prx_end = next_entry ? next_entry->info.prox_pointer
                                       : static_cast<int64_t>(prx_bytes.size());
    const uint8_t* prx_ptr = (prx_start < static_cast<int64_t>(prx_bytes.size()))
                                     ? prx_bytes.data() + prx_start
                                     : nullptr;
    const size_t prx_len = (prx_ptr != nullptr)
                                   ? static_cast<size_t>(prx_end - prx_start)
                                   : 0;
    return PostingDecoder::Decode(frq_bytes.data() + frq_start, frq_len,
                                  prx_ptr, prx_len,
                                  entry.info.doc_freq, /*has_prox=*/true);
}

} // namespace

TEST(SegmentMergerTest, EmptyInputsReturnsZero) {
    MemoryByteOutput tis, tii, frq, prx, fnm, nrm, seg_n, seg_gen;
    SpimiSegmentSink sink {.tis = &tis, .tii = &tii, .frq = &frq, .prx = &prx,
                           .fnm = &fnm, .nrm = &nrm,
                           .segments_n = &seg_n, .segments_gen = &seg_gen};
    const int64_t tc = SegmentMerger::Merge({}, sink, "_merged", "f", 0,
                                            FieldInfosWriter::kIndexVersionV1,
                                            false, true);
    EXPECT_EQ(tc, 0);
}

TEST(SegmentMergerTest, SingleInputPassthrough) {
    SpimiPostingBuffer buf;
    buf.Append("alpha", 0, 0);
    buf.Append("beta", 1, 0);
    auto input = MakeInput(buf, 5);

    auto result = DoMerge({input}, 5);
    EXPECT_EQ(result.term_count, 2);

    auto terms = ReadAllTerms(result.tis_bytes);
    ASSERT_EQ(terms.size(), 2U);
    EXPECT_EQ(terms[0].term_utf8, "alpha");
    EXPECT_EQ(terms[1].term_utf8, "beta");
}

TEST(SegmentMergerTest, TwoInputsDistinctTerms) {
    SpimiPostingBuffer buf1, buf2;
    buf1.Append("apple", 0, 0);
    buf2.Append("banana", 0, 0);
    auto in1 = MakeInput(buf1, 5);
    auto in2 = MakeInput(buf2, 5);

    auto result = DoMerge({in1, in2}, 10);
    EXPECT_EQ(result.term_count, 2);

    auto terms = ReadAllTerms(result.tis_bytes);
    ASSERT_EQ(terms.size(), 2U);
    EXPECT_EQ(terms[0].term_utf8, "apple");
    EXPECT_EQ(terms[1].term_utf8, "banana");
}

TEST(SegmentMergerTest, TwoInputsSharedTermsMergePostingLists) {
    SpimiPostingBuffer buf1, buf2;
    // "hello" appears in both segments.
    buf1.Append("hello", 0, 0);
    buf1.Append("hello", 2, 3);
    buf2.Append("hello", 0, 1);
    auto in1 = MakeInput(buf1, 5);
    auto in2 = MakeInput(buf2, 5);

    auto result = DoMerge({in1, in2}, 10);
    EXPECT_EQ(result.term_count, 1);

    auto terms = ReadAllTerms(result.tis_bytes);
    ASSERT_EQ(terms.size(), 1U);
    EXPECT_EQ(terms[0].term_utf8, "hello");
    // Total doc_freq = 3 (docs 0, 2 from seg1 + doc 5 from seg2).
    EXPECT_EQ(terms[0].info.doc_freq, 3);
}

TEST(SegmentMergerTest, DocIdOffsetsAreCorrect) {
    SpimiPostingBuffer buf1, buf2;
    buf1.Append("word", 0, 0);
    buf1.Append("word", 3, 0);
    buf2.Append("word", 0, 0); // maps to global doc_id = 5 + 0 = 5
    buf2.Append("word", 2, 0); // maps to global doc_id = 5 + 2 = 7
    auto in1 = MakeInput(buf1, 5);
    auto in2 = MakeInput(buf2, 5);

    auto result = DoMerge({in1, in2}, 10);
    auto terms = ReadAllTerms(result.tis_bytes);
    ASSERT_EQ(terms.size(), 1U);

    auto docs = DecodeTerm(terms[0], result.frq_bytes, result.prx_bytes, nullptr);
    ASSERT_EQ(docs.size(), 4U);
    // Segment 1: doc_ids 0, 3 (offset=0).
    EXPECT_EQ(docs[0].doc_id, 0);
    EXPECT_EQ(docs[1].doc_id, 3);
    // Segment 2: doc_ids 0+5=5, 2+5=7 (offset=5).
    EXPECT_EQ(docs[2].doc_id, 5);
    EXPECT_EQ(docs[3].doc_id, 7);
}

TEST(SegmentMergerTest, ThreeInputMerge) {
    SpimiPostingBuffer buf1, buf2, buf3;
    buf1.Append("a", 0, 0);
    buf2.Append("b", 0, 0);
    buf3.Append("c", 0, 0);
    auto in1 = MakeInput(buf1, 3);
    auto in2 = MakeInput(buf2, 3);
    auto in3 = MakeInput(buf3, 3);

    auto result = DoMerge({in1, in2, in3}, 9);
    EXPECT_EQ(result.term_count, 3);

    auto terms = ReadAllTerms(result.tis_bytes);
    ASSERT_EQ(terms.size(), 3U);
    EXPECT_EQ(terms[0].term_utf8, "a");
    EXPECT_EQ(terms[1].term_utf8, "b");
    EXPECT_EQ(terms[2].term_utf8, "c");
}

TEST(SegmentMergerTest, PositionSurvivesMerge) {
    SpimiPostingBuffer buf1, buf2;
    buf1.Append("token", 0, 0);
    buf1.Append("token", 0, 5);
    buf1.Append("token", 0, 10);
    buf2.Append("token", 0, 2);
    buf2.Append("token", 0, 8);
    auto in1 = MakeInput(buf1, 4);
    auto in2 = MakeInput(buf2, 4);

    auto result = DoMerge({in1, in2}, 8);
    auto terms = ReadAllTerms(result.tis_bytes);
    ASSERT_EQ(terms.size(), 1U);

    auto docs = DecodeTerm(terms[0], result.frq_bytes, result.prx_bytes, nullptr);
    ASSERT_EQ(docs.size(), 2U);

    // Segment 1 doc 0: positions 0, 5, 10.
    EXPECT_EQ(docs[0].doc_id, 0);
    EXPECT_EQ(docs[0].freq, 3);
    ASSERT_EQ(docs[0].positions.size(), 3U);
    EXPECT_EQ(docs[0].positions[0], 0);
    EXPECT_EQ(docs[0].positions[1], 5);
    EXPECT_EQ(docs[0].positions[2], 10);

    // Segment 2 doc 0 → offset 4 → global doc 4.
    EXPECT_EQ(docs[1].doc_id, 4);
    EXPECT_EQ(docs[1].freq, 2);
    ASSERT_EQ(docs[1].positions.size(), 2U);
    EXPECT_EQ(docs[1].positions[0], 2);
    EXPECT_EQ(docs[1].positions[1], 8);
}

TEST(SegmentMergerTest, MixedUniqueAndSharedTerms) {
    SpimiPostingBuffer buf1, buf2;
    // buf1 has: "common", "only_in_1"
    buf1.Append("common", 0, 0);
    buf1.Append("only_in_1", 1, 0);
    // buf2 has: "common", "only_in_2"
    buf2.Append("common", 0, 0);
    buf2.Append("only_in_2", 1, 0);

    auto in1 = MakeInput(buf1, 5);
    auto in2 = MakeInput(buf2, 5);
    auto result = DoMerge({in1, in2}, 10);

    // 3 distinct terms: common, only_in_1, only_in_2.
    EXPECT_EQ(result.term_count, 3);
    auto terms = ReadAllTerms(result.tis_bytes);
    ASSERT_EQ(terms.size(), 3U);
    EXPECT_EQ(terms[0].term_utf8, "common");
    EXPECT_EQ(terms[0].info.doc_freq, 2); // 1 from each segment
    EXPECT_EQ(terms[1].term_utf8, "only_in_1");
    EXPECT_EQ(terms[1].info.doc_freq, 1);
    EXPECT_EQ(terms[2].term_utf8, "only_in_2");
    EXPECT_EQ(terms[2].info.doc_freq, 1);
}

TEST(SegmentMergerTest, DocIdMonotonicallyIncreasing) {
    // Many docs across 3 segments; verify global monotonicity.
    SpimiPostingBuffer buf1, buf2, buf3;
    for (int d = 0; d < 10; ++d) {
        buf1.Append("x", static_cast<uint32_t>(d), 0);
    }
    for (int d = 0; d < 10; ++d) {
        buf2.Append("x", static_cast<uint32_t>(d), 0);
    }
    for (int d = 0; d < 10; ++d) {
        buf3.Append("x", static_cast<uint32_t>(d), 0);
    }
    auto in1 = MakeInput(buf1, 10);
    auto in2 = MakeInput(buf2, 10);
    auto in3 = MakeInput(buf3, 10);

    auto result = DoMerge({in1, in2, in3}, 30);
    auto terms = ReadAllTerms(result.tis_bytes);
    ASSERT_EQ(terms.size(), 1U);
    EXPECT_EQ(terms[0].info.doc_freq, 30);

    auto docs = DecodeTerm(terms[0], result.frq_bytes, result.prx_bytes, nullptr);
    ASSERT_EQ(docs.size(), 30U);
    for (size_t i = 1; i < docs.size(); ++i) {
        EXPECT_GT(docs[i].doc_id, docs[i - 1].doc_id)
                << "doc_ids must be strictly monotonically increasing at index " << i;
    }
    // First doc should be 0, last should be 29.
    EXPECT_EQ(docs.front().doc_id, 0);
    EXPECT_EQ(docs.back().doc_id, 29);
}

TEST(SegmentMergerTest, SpillManagerEndToEnd) {
    // End-to-end test: create 2 spill-like inputs via SpillManager's
    // EmitSegment path, merge them, verify output.
    auto make_spill_input = [](const std::string& term, int32_t doc_count) {
        SpimiPostingBuffer buf;
        buf.Append(term, 0, 0);
        return MakeInput(buf, doc_count);
    };

    auto in1 = make_spill_input("spill_term", 3);
    auto in2 = make_spill_input("spill_term", 4);

    auto result = DoMerge({in1, in2}, 7);
    EXPECT_EQ(result.term_count, 1);

    auto terms = ReadAllTerms(result.tis_bytes);
    ASSERT_EQ(terms.size(), 1U);
    EXPECT_EQ(terms[0].term_utf8, "spill_term");
    EXPECT_EQ(terms[0].info.doc_freq, 2);

    auto docs = DecodeTerm(terms[0], result.frq_bytes, result.prx_bytes, nullptr);
    ASSERT_EQ(docs.size(), 2U);
    EXPECT_EQ(docs[0].doc_id, 0); // segment 1, doc 0 + offset 0
    EXPECT_EQ(docs[1].doc_id, 3); // segment 2, doc 0 + offset 3
}

// Helper: fill buffer with enough records to activate compact mode.
// Uses sequential doc_ids (matching Doris's monotonic guarantee)
// so _compact_streams_sorted stays true and the fast path is used.
void FillBufferForCompact(SpimiPostingBuffer& buffer, const std::vector<std::string>& vocab,
                          int64_t num_records) {
    // vocab_size must be small enough that avg_occ > kCompactAvgOcc (8)
    // so compact mode activates at the kCompactCheckEvery (512) boundary.
    uint32_t doc_id = 0;
    uint32_t pos = 0;
    for (int64_t i = 0; i < num_records; ++i) {
        const auto& term = vocab[i % vocab.size()];
        buffer.Append(term, doc_id, pos++);
        // Advance doc_id every few positions.
        if (pos >= 10) {
            ++doc_id;
            pos = 0;
        }
    }
}

TEST(SegmentMergerTest, CompactModeFlushBufferDoesNotSkipData) {
    // Regression test: FlushBuffer previously used buffer.records().empty()
    // to check for emptiness. In compact mode, _records is cleared after
    // MaybeCompact migrates data to per-term streams, so records().empty()
    // incorrectly returned true for a non-empty buffer. This caused
    // FlushBuffer to skip the buffer, silently losing all data, and
    // subsequent mgr.Spills().back() on an empty vector was UB (segfault).
    //
    // The fix: RecordCount() now returns _total_occurrences in compact
    // mode, and FlushBuffer uses RecordCount() == 0 instead of
    // records().empty().
    SpimiPostingBuffer buffer;
    // Use a small vocabulary so compact mode activates (avg_occ > 8).
    const std::vector<std::string> vocab = {"a", "b", "c", "d", "e"};
    FillBufferForCompact(buffer, vocab, 1000);

    // Verify compact mode actually activated: the core symptom of the bug
    // is records().empty() == true while RecordCount() > 0.
    EXPECT_TRUE(buffer.records().empty())
            << "Compact mode should have cleared _records vector";
    EXPECT_GT(buffer.RecordCount(), 0)
            << "RecordCount() must return > 0 even when records() is empty (compact mode)";
    EXPECT_EQ(buffer.RecordCount(), 1000U);

    // Flush through SpillManager.
    // FillBufferForCompact generates 1000/10 = 100 doc_ids (0..99).
    SpillManager mgr("content");
    const int64_t terms = mgr.FlushBuffer(buffer, 100);
    // Before the fix, FlushBuffer would skip the buffer (records().empty()
    // returned true in compact mode) and return 0.
    EXPECT_GT(terms, 0);
    EXPECT_EQ(mgr.SpillCount(), 1);

    // Build Input from the spill and merge.
    auto input = ToInput(mgr.Spills().back());
    auto result = DoMerge({input}, 100);
    EXPECT_GT(result.term_count, 0);

    // Verify the merged terms are correct.
    auto merged_terms = ReadAllTerms(result.tis_bytes);
    EXPECT_EQ(merged_terms.size(), vocab.size());
    for (const auto& t : merged_terms) {
        // Each term should appear in vocab.
        EXPECT_NE(std::find(vocab.begin(), vocab.end(), t.term_utf8), vocab.end());
        // Verify posting content: doc_ids should be monotonically increasing.
        auto docs = DecodeTerm(t, result.frq_bytes, result.prx_bytes,
                               &merged_terms[0] + merged_terms.size() > &t ? &(&t)[1] : nullptr);
        ASSERT_GT(docs.size(), 0U);
        for (size_t d = 1; d < docs.size(); ++d) {
            EXPECT_GT(docs[d].doc_id, docs[d - 1].doc_id)
                    << "doc_ids must be monotonically increasing for term " << t.term_utf8;
        }
    }
}

TEST(SegmentMergerTest, CompactModeMultiSpillMerge) {
    // End-to-end test: multiple compact-mode buffers are flushed through
    // SpillManager and then merged via SegmentMerger. This exercises the
    // full spill -> flush -> merge pipeline with compact mode data.
    const std::vector<std::string> vocab = {"x", "y", "z"};
    SpillManager mgr("content");
    std::vector<SegmentMerger::Input> inputs;

    // Create 3 spill segments, each with compact-mode data.
    // Must use >= 600 records to cross kCompactCheckEvery=512 boundary.
    // FillBufferForCompact generates 600/10 = 60 doc_ids (0..59).
    for (int s = 0; s < 3; ++s) {
        SpimiPostingBuffer buffer;
        FillBufferForCompact(buffer, vocab, 600);
        // Verify compact mode actually activated for each buffer.
        EXPECT_TRUE(buffer.records().empty())
                << "segment " << s << ": compact mode should have cleared _records";
        EXPECT_EQ(buffer.RecordCount(), 600U);
        mgr.FlushBuffer(buffer, 60);
        inputs.push_back(ToInput(mgr.Spills().back()));
    }

    EXPECT_EQ(mgr.SpillCount(), 3);

    auto result = DoMerge(inputs, 180);
    EXPECT_GT(result.term_count, 0);

    auto merged_terms = ReadAllTerms(result.tis_bytes);
    EXPECT_EQ(merged_terms.size(), vocab.size());

    // Verify each term has postings from all 3 segments.
    for (const auto& t : merged_terms) {
        EXPECT_GE(t.info.doc_freq, 3); // at least 1 doc from each segment
    }
}

} // namespace doris::segment_v2::inverted_index::spimi
