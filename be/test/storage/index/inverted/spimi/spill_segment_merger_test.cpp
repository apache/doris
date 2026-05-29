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
#include <string>
#include <vector>

#include "storage/index/inverted/spimi/byte_output.h"
#include "storage/index/inverted/spimi/field_infos_writer.h"
#include "storage/index/inverted/spimi/freq_prox_encoder.h"
#include "storage/index/inverted/spimi/fulltext_writer.h"
#include "storage/index/inverted/spimi/posting_buffer.h"
#include "storage/index/inverted/spimi/posting_decoder.h"
#include "storage/index/inverted/spimi/segment_infos_reader.h"
#include "storage/index/inverted/spimi/segment_merger.h"
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
// The emit flags default to the V4 spill format (positions present, norms
// omitted); callers exercising the slow path can override them so the input's
// on-disk encoding matches the merge output flags.
SegmentMerger::Input MakeInput(SpimiPostingBuffer& buffer, int32_t doc_count,
                               bool omit_term_freq_and_positions = false, bool omit_norms = true) {
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
                                     omit_term_freq_and_positions, omit_norms);
    return {tis.bytes(), tii.bytes(), frq.bytes(), prx.bytes(), doc_count};
}

// Helper: run merge and return (term_count, output bytes).
struct MergeResult {
    int64_t term_count;
    std::vector<uint8_t> tis_bytes;
    std::vector<uint8_t> tii_bytes;
    std::vector<uint8_t> frq_bytes;
    std::vector<uint8_t> prx_bytes;
    std::vector<uint8_t> fnm_bytes;
    std::vector<uint8_t> segments_n_bytes;
};

MergeResult DoMerge(const std::vector<SegmentMerger::Input>& inputs, int32_t total_doc_count,
                    int32_t index_version = FieldInfosWriter::kIndexVersionV0,
                    bool omit_term_freq_and_positions = false, bool omit_norms = true) {
    MemoryByteOutput tis, tii, frq, prx, fnm, nrm, seg_n, seg_gen;
    SpimiSegmentSink sink {.tis = &tis,
                           .tii = &tii,
                           .frq = &frq,
                           .prx = &prx,
                           .fnm = &fnm,
                           .nrm = &nrm,
                           .segments_n = &seg_n,
                           .segments_gen = &seg_gen};
    const int64_t tc =
            SegmentMerger::Merge(inputs, sink, "_merged", "content", total_doc_count, index_version,
                                 omit_term_freq_and_positions, omit_norms);
    return {tc, tis.bytes(), tii.bytes(), frq.bytes(), prx.bytes(), fnm.bytes(), seg_n.bytes()};
}

// Minimal .fnm parser: extract the bits byte for the first field.
// .fnm format: vint(field_count) + per-field: vint(name_len) + name + bits_byte [+ vint(version) + vint(flags)]
// Returns the bits byte of the first field.
uint8_t ParseFnmFirstFieldBits(const std::vector<uint8_t>& fnm_bytes) {
    size_t pos = 0;
    // Read (and discard) vint field_count.
    [[maybe_unused]] uint32_t field_count = 0;
    int shift = 0;
    while (pos < fnm_bytes.size()) {
        uint8_t b = fnm_bytes[pos++];
        field_count |= (b & 0x7F) << shift;
        shift += 7;
        if ((b & 0x80) == 0) {
            break;
        }
    }
    // Read vint name_length.
    uint32_t name_len = 0;
    shift = 0;
    while (pos < fnm_bytes.size()) {
        uint8_t b = fnm_bytes[pos++];
        name_len |= (b & 0x7F) << shift;
        shift += 7;
        if ((b & 0x80) == 0) break;
    }
    // Skip name bytes.
    pos += name_len;
    // The next byte is the bits byte.
    return fnm_bytes[pos];
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
std::vector<DecodedDoc> DecodeTerm(const TermEntry& entry, const std::vector<uint8_t>& frq_bytes,
                                   const std::vector<uint8_t>& prx_bytes,
                                   const TermEntry* next_entry) {
    const int64_t frq_start = entry.info.freq_pointer;
    const size_t frq_len = static_cast<size_t>(frq_bytes.size()) - static_cast<size_t>(frq_start);
    const int64_t prx_start = entry.info.prox_pointer;
    const int64_t prx_end =
            next_entry ? next_entry->info.prox_pointer : static_cast<int64_t>(prx_bytes.size());
    const uint8_t* prx_ptr = (prx_start < static_cast<int64_t>(prx_bytes.size()))
                                     ? prx_bytes.data() + prx_start
                                     : nullptr;
    const size_t prx_len = (prx_ptr != nullptr) ? static_cast<size_t>(prx_end - prx_start) : 0;
    return PostingDecoder::Decode(frq_bytes.data() + frq_start, frq_len, prx_ptr, prx_len,
                                  entry.info.doc_freq, /*has_prox=*/true);
}

} // namespace

TEST(SegmentMergerTest, EmptyInputsReturnsZero) {
    MemoryByteOutput tis, tii, frq, prx, fnm, nrm, seg_n, seg_gen;
    SpimiSegmentSink sink {.tis = &tis,
                           .tii = &tii,
                           .frq = &frq,
                           .prx = &prx,
                           .fnm = &fnm,
                           .nrm = &nrm,
                           .segments_n = &seg_n,
                           .segments_gen = &seg_gen};
    const int64_t tc = SegmentMerger::Merge({}, sink, "_merged", "f", 0,
                                            FieldInfosWriter::kIndexVersionV1, false, true);
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

    // Verify posting content: doc_ids and freq.
    auto docs0 = DecodeTerm(terms[0], result.frq_bytes, result.prx_bytes, &terms[1]);
    ASSERT_EQ(docs0.size(), 1U);
    EXPECT_EQ(docs0[0].doc_id, 0);

    auto docs1 = DecodeTerm(terms[1], result.frq_bytes, result.prx_bytes, nullptr);
    ASSERT_EQ(docs1.size(), 1U);
    EXPECT_EQ(docs1[0].doc_id, 1);

    // Byte-level verification: fast path copies .tis/.frq/.prx verbatim.
    EXPECT_EQ(result.tis_bytes, input.tis_bytes);
    EXPECT_EQ(result.frq_bytes, input.frq_bytes);
    EXPECT_EQ(result.prx_bytes, input.prx_bytes);
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
    EXPECT_TRUE(buffer.records().empty()) << "Compact mode should have cleared _records vector";
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

TEST(SegmentMergerTest, SingleInputFastPathByteCopy) {
    // Verifies the k=1 byte-copy fast path: when there is exactly
    // one input segment with has_positions=true and omit_norms=true,
    // MergeSingleInput copies .tis/.tii/.frq/.prx directly instead
    // of decode/re-encode, then rebuilds .fnm and segments metadata.
    //
    // Uses compact-mode data to exercise a realistic workload.
    const std::vector<std::string> vocab = {"alpha", "beta", "gamma", "delta"};
    SpimiPostingBuffer buffer;
    FillBufferForCompact(buffer, vocab, 600);

    // Verify compact mode activated.
    EXPECT_TRUE(buffer.records().empty());
    EXPECT_EQ(buffer.RecordCount(), 600U);

    // Flush through SpillManager to produce a single spill segment.
    SpillManager mgr("content");
    const int64_t spill_terms = mgr.FlushBuffer(buffer, 60);
    EXPECT_GT(spill_terms, 0);
    ASSERT_EQ(mgr.SpillCount(), 1U);

    auto input = ToInput(mgr.Spills()[0]);

    // Merge with a single input -- this triggers the byte-copy fast
    // path because inputs.size()==1, omit_term_freq_and_positions=false,
    // and omit_norms=true.
    auto result = DoMerge({input}, 60);
    EXPECT_EQ(result.term_count, spill_terms);

    // Verify the merged terms are correct.
    auto merged_terms = ReadAllTerms(result.tis_bytes);
    EXPECT_EQ(merged_terms.size(), vocab.size());

    for (size_t i = 0; i < merged_terms.size(); ++i) {
        const auto& t = merged_terms[i];
        // Each term should appear in vocab.
        EXPECT_NE(std::find(vocab.begin(), vocab.end(), t.term_utf8), vocab.end())
                << "Unexpected term: " << t.term_utf8;
        // Verify posting content: doc_ids should be monotonically
        // increasing.
        const TermEntry* next = (i + 1 < merged_terms.size()) ? &merged_terms[i + 1] : nullptr;
        auto docs = DecodeTerm(t, result.frq_bytes, result.prx_bytes, next);
        ASSERT_GT(docs.size(), 0U) << "Term " << t.term_utf8 << " should have postings";
        for (size_t d = 1; d < docs.size(); ++d) {
            EXPECT_GT(docs[d].doc_id, docs[d - 1].doc_id)
                    << "doc_ids not monotonic for term " << t.term_utf8 << " at index " << d;
        }
    }

    // Byte-level: the fast path copies .tis/.tii/.frq/.prx verbatim.
    EXPECT_EQ(result.tis_bytes, input.tis_bytes);
    EXPECT_EQ(result.tii_bytes, input.tii_bytes);
    EXPECT_EQ(result.frq_bytes, input.frq_bytes);
    EXPECT_EQ(result.prx_bytes, input.prx_bytes);
}

TEST(SegmentMergerTest, SingleInputSlowPathWhenOmitNormsFalse) {
    // When omit_norms=false, the fast path condition is NOT met,
    // so even a single input must go through the full decode/re-encode
    // path. This test ensures the dispatch logic is correct.
    SpimiPostingBuffer buf;
    buf.Append("alpha", 0, 0);
    buf.Append("beta", 1, 0);
    auto input = MakeInput(buf, 5);

    // omit_norms=false → fast path condition fails → slow path
    auto result = DoMerge({input}, 5, FieldInfosWriter::kIndexVersionV0,
                          /*omit_term_freq_and_positions=*/false,
                          /*omit_norms=*/false);
    EXPECT_EQ(result.term_count, 2);

    auto terms = ReadAllTerms(result.tis_bytes);
    ASSERT_EQ(terms.size(), 2U);
    EXPECT_EQ(terms[0].term_utf8, "alpha");
    EXPECT_EQ(terms[1].term_utf8, "beta");

    // Verify .fnm bits: omit_norms=false means kOmitNorms bit is NOT set.
    const uint8_t bits = ParseFnmFirstFieldBits(result.fnm_bytes);
    EXPECT_EQ(bits & FieldInfosWriter::kOmitNorms, 0)
            << "omit_norms=false should not set kOmitNorms bit in .fnm";
    // kHasVersionTag should NOT be set for V0.
    EXPECT_EQ(bits & FieldInfosWriter::kHasVersionTag, 0)
            << "V0 should not have kHasVersionTag bit in .fnm";
}

TEST(SegmentMergerTest, SingleInputFastPathFnmVersionDowngrade) {
    // Core scenario: spill produces .fnm with kIndexVersionV1 (has
    // kHasVersionTag bit), but the final output needs V0 (no tag).
    // MergeSingleInput must rebuild .fnm with V0, NOT copy the
    // input's V1 .fnm. This is the exact production path for V4 SPIMI.
    SpimiPostingBuffer buf;
    buf.Append("alpha", 0, 0);
    buf.Append("beta", 1, 0);
    auto input = MakeInput(buf, 5); // MakeInput uses V1

    // Merge with V0 — this is what production does.
    auto result = DoMerge({input}, 5, FieldInfosWriter::kIndexVersionV0,
                          /*omit_term_freq_and_positions=*/false,
                          /*omit_norms=*/true);
    EXPECT_EQ(result.term_count, 2);

    // Verify .fnm was rebuilt with V0 (no kHasVersionTag).
    const uint8_t bits = ParseFnmFirstFieldBits(result.fnm_bytes);
    EXPECT_EQ(bits & FieldInfosWriter::kHasVersionTag, 0)
            << "V0 output .fnm must NOT have kHasVersionTag bit";

    // Verify correct field flags in .fnm.
    EXPECT_NE(bits & FieldInfosWriter::kIsIndexed, 0);
    EXPECT_NE(bits & FieldInfosWriter::kTermFreqAndPositions, 0)
            << "has_prox=true should set kTermFreqAndPositions bit";
    EXPECT_NE(bits & FieldInfosWriter::kOmitNorms, 0)
            << "omit_norms=true should set kOmitNorms bit";

    // Byte-level: .tis/.tii/.frq/.prx should be identical to input (fast path).
    EXPECT_EQ(result.tis_bytes, input.tis_bytes);
    EXPECT_EQ(result.tii_bytes, input.tii_bytes);
    EXPECT_EQ(result.frq_bytes, input.frq_bytes);
    EXPECT_EQ(result.prx_bytes, input.prx_bytes);
}

TEST(SegmentMergerTest, SingleInputFastPathRebuildsSegmentsManifest) {
    // The stated reason the single-input path routes through Merge()
    // (instead of a raw byte copy of all 7 files) is that segments_N
    // must carry the caller's segment_name and total_doc_count, not the
    // spill's. Verify the rebuilt manifest by parsing it back.
    SpimiPostingBuffer buf;
    buf.Append("alpha", 0, 0);
    buf.Append("beta", 1, 0);
    auto input = MakeInput(buf, /*doc_count=*/3); // spill doc_count differs

    const int32_t kTotalDocCount = 42;
    auto result = DoMerge({input}, kTotalDocCount);
    EXPECT_EQ(result.term_count, 2);

    auto manifest = SegmentInfosReader::Read(result.segments_n_bytes);
    ASSERT_EQ(manifest.segments.size(), 1U);
    EXPECT_EQ(manifest.segments[0].name, "_merged")
            << "fast path must write the caller's segment name, not the spill's";
    EXPECT_EQ(manifest.segments[0].doc_count, kTotalDocCount)
            << "fast path must write the caller's total_doc_count, not the spill's";
}

TEST(SegmentMergerTest, SlowPathReEncodesHighFreqTermAsPfor) {
    // Two inputs sharing one term whose combined doc_freq crosses the
    // skip_interval (kDefaultSkipInterval = 512) force the slow-path
    // re-encoder to emit PFOR-coded blocks. The leading byte of the
    // term's .frq block is the self-describing code-mode byte, which
    // must be kCodeModeSpimiPfor (0x05). This locks the guarantee that
    // posting decode is driven by the mode byte, not the .fnm version.
    constexpr int32_t kDocsPerInput = 300; // 300 + 300 = 600 >= 512

    SpimiPostingBuffer buf_a;
    for (uint32_t d = 0; d < static_cast<uint32_t>(kDocsPerInput); ++d) {
        buf_a.Append("hot", d, 0);
    }
    auto input_a = MakeInput(buf_a, kDocsPerInput);

    SpimiPostingBuffer buf_b;
    for (uint32_t d = 0; d < static_cast<uint32_t>(kDocsPerInput); ++d) {
        buf_b.Append("hot", d, 0);
    }
    auto input_b = MakeInput(buf_b, kDocsPerInput);

    // Two inputs => slow path (k-way merge with re-encode).
    auto result = DoMerge({input_a, input_b}, 2 * kDocsPerInput);
    EXPECT_EQ(result.term_count, 1);

    auto terms = ReadAllTerms(result.tis_bytes);
    ASSERT_EQ(terms.size(), 1U);
    EXPECT_EQ(terms[0].term_utf8, "hot");
    EXPECT_EQ(terms[0].info.doc_freq, 2 * kDocsPerInput)
            << "input_b doc_ids must be offset past input_a's, doubling doc_freq";

    // The .frq block for this term must begin with the PFOR mode byte.
    const int64_t frq_start = terms[0].info.freq_pointer;
    ASSERT_LT(static_cast<size_t>(frq_start), result.frq_bytes.size());
    EXPECT_EQ(result.frq_bytes[frq_start], FreqProxEncoder::kCodeModeSpimiPfor)
            << "doc_freq >= skip_interval must re-encode .frq as PFOR";

    // And it must still decode to monotonically increasing doc_ids.
    auto docs = DecodeTerm(terms[0], result.frq_bytes, result.prx_bytes, nullptr);
    ASSERT_EQ(docs.size(), static_cast<size_t>(2 * kDocsPerInput));
    for (size_t d = 1; d < docs.size(); ++d) {
        EXPECT_GT(docs[d].doc_id, docs[d - 1].doc_id);
    }
}

TEST(SegmentMergerTest, SingleInputSlowPathWhenOmitTermFreqAndPositions) {
    // omit_term_freq_and_positions=true fails the fast-path guard
    // (!omit_tfap is false), so a single input routes through the slow
    // path even though there is nothing to merge. The input is emitted
    // without positions so its encoding matches the merge output flags.
    SpimiPostingBuffer buf;
    buf.Append("alpha", 0, 0);
    buf.Append("beta", 1, 0);
    auto input = MakeInput(buf, 5, /*omit_term_freq_and_positions=*/true,
                           /*omit_norms=*/true);

    auto result = DoMerge({input}, 5, FieldInfosWriter::kIndexVersionV0,
                          /*omit_term_freq_and_positions=*/true,
                          /*omit_norms=*/true);
    EXPECT_EQ(result.term_count, 2);

    auto terms = ReadAllTerms(result.tis_bytes);
    ASSERT_EQ(terms.size(), 2U);
    EXPECT_EQ(terms[0].term_utf8, "alpha");
    EXPECT_EQ(terms[1].term_utf8, "beta");

    // .fnm must NOT advertise positions when they are omitted.
    const uint8_t bits = ParseFnmFirstFieldBits(result.fnm_bytes);
    EXPECT_EQ(bits & FieldInfosWriter::kTermFreqAndPositions, 0)
            << "omit_term_freq_and_positions=true must clear kTermFreqAndPositions bit";
}

TEST(SegmentMergerTest, SingleInputFastPathZeroTerms) {
    // Edge case: a valid spill segment with zero terms (empty buffer).
    // EmitSegment still writes a 32-byte .tis (header + footer), so the
    // fast path's TermEnum construction must not throw, and term_count
    // must be 0 with the manifest still rebuilt correctly.
    SpimiPostingBuffer buf; // no Append() calls
    auto input = MakeInput(buf, /*doc_count=*/0);
    ASSERT_GE(input.tis_bytes.size(), 32U)
            << "a valid zero-term .tis must still carry header + footer";

    MergeResult result;
    ASSERT_NO_THROW({ result = DoMerge({input}, /*total_doc_count=*/7); });
    EXPECT_EQ(result.term_count, 0);
    EXPECT_EQ(result.tis_bytes, input.tis_bytes); // byte-copied verbatim

    auto manifest = SegmentInfosReader::Read(result.segments_n_bytes);
    ASSERT_EQ(manifest.segments.size(), 1U);
    EXPECT_EQ(manifest.segments[0].doc_count, 7);
}

TEST(SegmentMergerTest, TermEnumRejectsTruncatedTis) {
    // The fast path relies on TermEnum to read the term count from the
    // .tis footer. A truncated (<32-byte) .tis must be rejected rather
    // than read out of bounds. Documents the corruption guard the fast
    // path depends on.
    std::vector<uint8_t> truncated(10, 0);
    EXPECT_ANY_THROW({ TermEnum tenum(truncated); });
}

} // namespace doris::segment_v2::inverted_index::spimi
