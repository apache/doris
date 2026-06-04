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
#include <cstdlib>
#include <fstream>
#include <map>
#include <random>
#include <string>
#include <vector>

#include "gen_cpp/segment_v2.pb.h"
#include "storage/index/inverted/spimi/byte_output.h"
#include "storage/index/inverted/spimi/field_infos_writer.h"
#include "storage/index/inverted/spimi/freq_prox_encoder.h"
#include "storage/index/inverted/spimi/fulltext_writer.h"
#include "storage/index/inverted/spimi/posting_buffer.h"
#include "storage/index/inverted/spimi/posting_decoder.h"
#include "storage/index/inverted/spimi/segment_infos_reader.h"
#include "storage/index/inverted/spimi/segment_merger.h"
#include "storage/index/inverted/spimi/spill_manager.h"
#include "storage/index/inverted/spimi/term_docs_reader.h"
#include "storage/index/inverted/spimi/term_enum.h"
#include "util/block_compression.h"
#include "util/slice.h"

namespace doris::segment_v2::inverted_index::spimi {

namespace {

// Returns the effective inner .frq mode byte for the term whose block starts at
// `frq_start`, transparently unwrapping jk's whole-term ZSTD envelope
// (kCodeModeZstd: VInt(uncomp) VInt(comp) ZSTD-payload). When the block is not
// ZSTD-wrapped the first byte is returned as-is. Lets structural assertions
// check the real codec (kDefault / kSpimiPfor) regardless of whole-term
// compression.
uint8_t FrqInnerModeByte(const std::vector<uint8_t>& frq_bytes, int64_t frq_start) {
    const uint8_t* p = frq_bytes.data() + frq_start;
    if (p[0] != FreqProxEncoder::kCodeModeZstd) {
        return p[0];
    }
    size_t pos = 1;
    auto read_vint = [&]() {
        uint32_t v = 0, shift = 0;
        while (true) {
            const uint8_t b = p[pos++];
            v |= static_cast<uint32_t>(b & 0x7FU) << shift;
            if ((b & 0x80U) == 0) break;
            shift += 7;
        }
        return v;
    };
    const uint32_t uncomp = read_vint();
    const uint32_t comp = read_vint();
    std::vector<uint8_t> raw(uncomp);
    BlockCompressionCodec* codec = nullptr;
    EXPECT_TRUE(get_block_compression_codec(CompressionTypePB::ZSTD, &codec).ok());
    Slice in(reinterpret_cast<const char*>(p + pos), comp);
    Slice out(reinterpret_cast<char*>(raw.data()), uncomp);
    EXPECT_TRUE(codec->decompress(in, &out).ok());
    return raw.empty() ? 0 : raw[0];
}

// Build a SegmentMerger::Input by streaming spill `idx` back from its tmp file.
SegmentMerger::Input ToInput(const SpillManager& mgr, size_t idx) {
    SegmentMerger::Input in;
    EXPECT_TRUE(mgr.LoadSpill(idx, in).ok());
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

// Decode one term's posting list. Handles both inlined terms (posting bytes
// live in the .tis entry, TermEnum recorded spans into the input's .tis buffer)
// and external terms (.frq/.prx via freq_pointer/prox_pointer). V4 spills are
// now written inlined, so small terms that survive a single-input byte-copy
// merge land here as inlined and must be decoded from their inline spans.
std::vector<DecodedDoc> DecodeTerm(const TermEntry& entry, const std::vector<uint8_t>& frq_bytes,
                                   const std::vector<uint8_t>& prx_bytes,
                                   const TermEntry* next_entry) {
    if (entry.info.inlined) {
        return PostingDecoder::Decode(entry.info.inline_frq, entry.info.inline_frq_len,
                                      entry.info.inline_prx, entry.info.inline_prx_len,
                                      entry.info.doc_freq, /*has_prox=*/true, entry.info.is_slim);
    }
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
                                  entry.info.doc_freq, /*has_prox=*/true, entry.info.is_slim);
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
    auto input = ToInput(mgr, mgr.SpillCount() - 1);
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
        inputs.push_back(ToInput(mgr, mgr.SpillCount() - 1));
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

    auto input = ToInput(mgr, mgr.SpillCount() - 1);

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

TEST(SegmentMergerTest, V4SpillSingleInputMergeInlinesSmallTerms) {
    // PHASE B regression: small terms loaded via the spill -> single-input
    // byte-copy merge path must end up INLINED in the final segment (their
    // posting bytes relocated into .tis, zero per-term GET on read), not sitting
    // in an external .frq.
    //
    // Mechanism: a V4 SpillManager writes the spill inlined (use_windowed +
    // inline_small_terms, both true). The single-spill Finish dispatches to
    // SegmentMerger::MergeSingleInput, which byte-copies .tis/.frq/.prx verbatim,
    // so the inlined spill yields an inlined final segment. Before the fix the
    // spill's EmitSegment was called with inline_small_terms=false (the default),
    // so the byte-copy carried a NON-inlined .tis and these small terms stayed in
    // the external .frq.
    const std::vector<std::string> vocab = {"alpha", "beta", "gamma", "delta"};
    SpimiPostingBuffer buffer;
    // Small postings: each term appears in 6 docs with a single position each, so
    // df=6 (< skip_interval 512 => slim) and frq+prx is a few bytes per term, far
    // under the 256-byte inline threshold => inlinable. doc_ids are appended
    // non-decreasing (all four terms per doc) and positions are 0 within a doc.
    constexpr uint32_t kDocs = 6;
    for (uint32_t d = 0; d < kDocs; ++d) {
        for (const auto& term : vocab) {
            buffer.Append(term, d, 0);
        }
    }

    // is_v4=true so the spill is written V4 (windowed-capable + inline-capable).
    SpillManager mgr("content", /*is_v4=*/true);
    const int64_t spill_terms = mgr.FlushBuffer(buffer, /*doc_count=*/kDocs);
    ASSERT_EQ(mgr.SpillCount(), 1U);
    EXPECT_EQ(spill_terms, static_cast<int64_t>(vocab.size()));

    auto input = ToInput(mgr, 0);

    // V4 single-input merge => MergeSingleInput byte-copy fast path.
    auto result = DoMerge({input}, /*total_doc_count=*/kDocs, FieldInfosWriter::kIndexVersionV4);
    EXPECT_EQ(result.term_count, static_cast<int64_t>(vocab.size()));

    auto merged_terms = ReadAllTerms(result.tis_bytes);
    ASSERT_EQ(merged_terms.size(), vocab.size());

    // Every small term must be INLINED in the final segment's .tis.
    for (const auto& t : merged_terms) {
        EXPECT_TRUE(t.info.inlined)
                << "small term '" << t.term_utf8
                << "' must be inlined after spill -> single-input byte-copy merge";
        EXPECT_GT(t.info.inline_frq_len, 0U)
                << "inlined term '" << t.term_utf8 << "' must carry its .frq bytes in .tis";
    }

    // The external .frq / .prx must NOT contain these terms' posting bytes:
    // when every term inlines, SegmentWriter writes nothing to the external
    // streams, so they are empty. This is the concrete "external .frq does NOT
    // contain its bytes" proof.
    EXPECT_TRUE(result.frq_bytes.empty())
            << "external .frq must be empty when all small terms are inlined (got "
            << result.frq_bytes.size() << " bytes)";
    EXPECT_TRUE(result.prx_bytes.empty())
            << "external .prx must be empty when all small terms are inlined (got "
            << result.prx_bytes.size() << " bytes)";

    // The byte-copy still carries the inlined layout verbatim from the spill.
    EXPECT_EQ(result.tis_bytes, input.tis_bytes);
    EXPECT_EQ(result.frq_bytes, input.frq_bytes);
    EXPECT_EQ(result.prx_bytes, input.prx_bytes);

    // .fnm must advertise V4 (kHasVersionTag set) so the reader turns on the
    // inline decode path.
    const uint8_t bits = ParseFnmFirstFieldBits(result.fnm_bytes);
    EXPECT_NE(bits & FieldInfosWriter::kHasVersionTag, 0)
            << "V4 output .fnm must carry kHasVersionTag so the reader inline-decodes";

    // Content must still decode correctly from the inline spans (DecodeTerm
    // dispatches on info.inlined), with monotonically increasing doc_ids.
    for (size_t i = 0; i < merged_terms.size(); ++i) {
        const TermEntry* next = (i + 1 < merged_terms.size()) ? &merged_terms[i + 1] : nullptr;
        auto docs = DecodeTerm(merged_terms[i], result.frq_bytes, result.prx_bytes, next);
        ASSERT_GT(docs.size(), 0U) << "inlined term '" << merged_terms[i].term_utf8
                                   << "' must decode to non-empty postings";
        EXPECT_EQ(static_cast<int32_t>(docs.size()), merged_terms[i].info.doc_freq);
        for (size_t d = 1; d < docs.size(); ++d) {
            EXPECT_GT(docs[d].doc_id, docs[d - 1].doc_id)
                    << "doc_ids not monotonic for inlined term " << merged_terms[i].term_utf8;
        }
    }
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

    // The .frq block for this term must be PFOR-encoded. jk's freq/prox encoder
    // may additionally wrap the whole term in a ZSTD envelope (outer byte
    // kCodeModeZstd), so check the inner mode byte through that envelope rather
    // than the raw first byte.
    const int64_t frq_start = terms[0].info.freq_pointer;
    ASSERT_LT(static_cast<size_t>(frq_start), result.frq_bytes.size());
    EXPECT_EQ(FrqInnerModeByte(result.frq_bytes, frq_start), FreqProxEncoder::kCodeModeSpimiPfor)
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

TEST(SegmentMergerTest, PhraseOffSpillOmitsPositionsAndMergesCorrectly) {
    // Regression: a phrase-off field (omit_term_freq_and_positions=true) that
    // spills must write its spill segments in omit=true format too, in lockstep
    // with the final segment. The k-way merge decodes spill .frq/.prx with
    // has_prox = !omit; before lockstepping the spill omit flag, the spill was
    // ALWAYS written omit=false (positions present) while a phrase-off merge
    // decoded has_prox=false -> the .frq doc<<1|freq codes were misread as raw
    // doc deltas -> doc-id corruption. It also wasted a position encode + spill
    // IO that the final merge discarded.
    SpimiPostingBuffer buffer;
    // "alpha" in docs 0,2,5 ; "beta" in docs 1,3.
    buffer.Append("alpha", 0, 0);
    buffer.Append("beta", 1, 0);
    buffer.Append("alpha", 2, 0);
    buffer.Append("beta", 3, 0);
    buffer.Append("alpha", 5, 0);

    // 4th ctor arg = omit_term_freq_and_positions: the spill drops freq+pos.
    SpillManager mgr("content", /*is_v4=*/false, /*tmp_dir=*/"",
                     /*omit_term_freq_and_positions=*/true);
    const int64_t terms = mgr.FlushBuffer(buffer, /*doc_count=*/6);
    EXPECT_EQ(terms, 2);
    ASSERT_EQ(mgr.SpillCount(), 1U);

    // Perf win: no .prx stream is written when positions are omitted.
    EXPECT_EQ(mgr.Spills().back().prx.length, 0)
            << "phrase-off spill must not encode/spill positions";

    // Merge with omit=true to match the spill format.
    auto input = ToInput(mgr, 0);
    auto result = DoMerge({input}, /*total_doc_count=*/6, FieldInfosWriter::kIndexVersionV0,
                          /*omit_term_freq_and_positions=*/true, /*omit_norms=*/true);
    EXPECT_EQ(result.term_count, 2);

    auto merged = ReadAllTerms(result.tis_bytes);
    ASSERT_EQ(merged.size(), 2U);
    EXPECT_EQ(merged[0].term_utf8, "alpha");
    EXPECT_EQ(merged[1].term_utf8, "beta");

    // Decode in omit format (has_prox=false) and verify doc ids are intact —
    // this is the assertion that fails under the old "spill always omit=false".
    auto decode_omit = [&](const TermEntry& e) {
        const int64_t frq_start = e.info.freq_pointer;
        const size_t frq_len =
                static_cast<size_t>(result.frq_bytes.size()) - static_cast<size_t>(frq_start);
        return PostingDecoder::Decode(result.frq_bytes.data() + frq_start, frq_len,
                                      /*prx_data=*/nullptr, /*prx_length=*/0, e.info.doc_freq,
                                      /*has_prox=*/false, e.info.is_slim);
    };
    auto alpha = decode_omit(merged[0]);
    ASSERT_EQ(alpha.size(), 3U);
    EXPECT_EQ(alpha[0].doc_id, 0);
    EXPECT_EQ(alpha[1].doc_id, 2);
    EXPECT_EQ(alpha[2].doc_id, 5);
    auto beta = decode_omit(merged[1]);
    ASSERT_EQ(beta.size(), 2U);
    EXPECT_EQ(beta[0].doc_id, 1);
    EXPECT_EQ(beta[1].doc_id, 3);
}

// Diagnostic (env-driven): break down a V4 segment's .frq bytes by term df, to
// localize where the doc-id postings spend space (df=1 tail vs windowed high-df).
// Reads the segment's _0.tis via the real TermEnum and attributes per-term .frq
// bytes from consecutive freq_pointers; inlined small terms live in .tis instead.
//   SPIMI_TIS_PATH=/path/_0.tis  SPIMI_FRQ_LEN=<bytes>  ./doris_be_test \
//     --gtest_filter='*FrqByDfHistogram*'
TEST(SegmentMergerTest, FrqByDfHistogramFromEnv) {
    const char* tis_path = std::getenv("SPIMI_TIS_PATH");
    const char* frq_len_s = std::getenv("SPIMI_FRQ_LEN");
    if (tis_path == nullptr || frq_len_s == nullptr) {
        GTEST_SKIP() << "set SPIMI_TIS_PATH and SPIMI_FRQ_LEN to run";
    }
    const int64_t frq_total = std::atoll(frq_len_s);
    std::ifstream in(tis_path, std::ios::binary);
    ASSERT_TRUE(in.good()) << "cannot open " << tis_path;
    std::vector<uint8_t> tis((std::istreambuf_iterator<char>(in)),
                             std::istreambuf_iterator<char>());

    struct Row {
        int32_t df;
        int64_t fp;
        bool inlined;
        int64_t inline_frq;
        int64_t inline_prx;
    };
    std::vector<Row> rows;
    TermEnum en(tis);
    while (en.Next()) {
        const auto& e = en.Current();
        rows.push_back({e.info.doc_freq, e.info.freq_pointer, e.info.inlined,
                        e.info.inlined ? static_cast<int64_t>(e.info.inline_frq_len) : 0,
                        e.info.inlined ? static_cast<int64_t>(e.info.inline_prx_len) : 0});
    }
    ASSERT_FALSE(rows.empty());

    // Per-term .frq size = next term's freq_pointer - this one's (freq_pointer is
    // monotonic in term order; inlined terms don't advance it -> ~0).
    auto bucket_of = [](int32_t df) -> const char* {
        if (df == 1) return "df=1";
        if (df <= 4) return "df 2-4";
        if (df <= 16) return "df 5-16";
        if (df <= 127) return "df 17-127";
        if (df <= 1024) return "df 128-1K";
        if (df <= 16384) return "df 1K-16K";
        return "df 16K+";
    };
    struct Agg {
        int64_t nterms = 0, frq = 0, inl_frq = 0, inl_prx = 0, postings = 0;
    };
    std::map<std::string, Agg> agg;
    int64_t tot_frq = 0, tot_inl = 0;
    for (size_t i = 0; i < rows.size(); ++i) {
        const int64_t nfp = (i + 1 < rows.size()) ? rows[i + 1].fp : frq_total;
        const int64_t sz = rows[i].inlined ? 0 : std::max<int64_t>(0, nfp - rows[i].fp);
        auto& a = agg[bucket_of(rows[i].df)];
        a.nterms++;
        a.frq += sz;
        a.inl_frq += rows[i].inline_frq;
        a.inl_prx += rows[i].inline_prx;
        a.postings += rows[i].df;
        tot_frq += sz;
        tot_inl += rows[i].inline_frq;
    }
    printf("\n=== V4 .frq breakdown by df (tis=%s, terms=%zu) ===\n", tis_path, rows.size());
    printf("%-12s %12s %14s %10s %12s %12s\n", "bucket", "terms", "frq_bytes", "%frq", "B/term",
           "B/posting");
    const char* order[] = {"df=1",      "df 2-4",    "df 5-16", "df 17-127",
                           "df 128-1K", "df 1K-16K", "df 16K+"};
    for (const char* k : order) {
        auto it = agg.find(k);
        if (it == agg.end()) continue;
        const Agg& a = it->second;
        printf("%-12s %12ld %14ld %9.1f%% %12.2f %12.3f\n", k, a.nterms, a.frq,
               tot_frq ? 100.0 * a.frq / tot_frq : 0.0, a.nterms ? double(a.frq) / a.nterms : 0.0,
               a.postings ? double(a.frq) / a.postings : 0.0);
    }
    printf("TOTAL .frq=%ld bytes  inlined-in-.tis frq=%ld bytes\n", tot_frq, tot_inl);
    // Sanity: summed per-term frq must equal the .frq stream length.
    EXPECT_EQ(tot_frq, frq_total) << "freq_pointer deltas must tile the whole .frq";

    // Optional: dump per-term (df, freq_pointer, inlined) rows to a CSV so an
    // external parser can segment the .frq into per-term windowed blocks and do a
    // deeper skip-table/framing/PFOR-payload byte breakdown. Set SPIMI_ROWS_CSV.
    if (const char* csv = std::getenv("SPIMI_ROWS_CSV")) {
        std::ofstream out(csv, std::ios::trunc);
        ASSERT_TRUE(out.good()) << "cannot open " << csv;
        out << "df,freq_pointer,inlined\n";
        for (const auto& r : rows) {
            out << r.df << ',' << r.fp << ',' << (r.inlined ? 1 : 0) << '\n';
        }
        out << "# frq_total=" << frq_total << '\n';
        printf("wrote %zu rows to %s\n", rows.size(), csv);
    }
}

// Diagnostic (env-driven): dump the raw .frq bytes of sample df=1 terms and
// decode them, to attribute the per-term byte cost exactly.
//   SPIMI_TIS_PATH=_0.tis SPIMI_FRQ_PATH=_0.frq SPIMI_FRQ_LEN=<bytes> \
//     ./doris_be_test --gtest_filter='*FrqDecodeDf1Sample*'
TEST(SegmentMergerTest, FrqDecodeDf1SampleFromEnv) {
    const char* tis_path = std::getenv("SPIMI_TIS_PATH");
    const char* frq_path = std::getenv("SPIMI_FRQ_PATH");
    const char* frq_len_s = std::getenv("SPIMI_FRQ_LEN");
    if (tis_path == nullptr || frq_path == nullptr || frq_len_s == nullptr) {
        GTEST_SKIP() << "set SPIMI_TIS_PATH, SPIMI_FRQ_PATH, SPIMI_FRQ_LEN";
    }
    const int64_t frq_total = std::atoll(frq_len_s);
    auto slurp = [](const char* p) {
        std::ifstream in(p, std::ios::binary);
        return std::vector<uint8_t>((std::istreambuf_iterator<char>(in)),
                                    std::istreambuf_iterator<char>());
    };
    std::vector<uint8_t> tis = slurp(tis_path);
    std::vector<uint8_t> frq = slurp(frq_path);
    ASSERT_FALSE(tis.empty());
    ASSERT_EQ(static_cast<int64_t>(frq.size()), frq_total);

    struct Row {
        int32_t df;
        int64_t fp;
        std::string term;
    };
    std::vector<Row> rows;
    TermEnum en(tis);
    while (en.Next()) {
        const auto& e = en.Current();
        rows.push_back({e.info.doc_freq, e.info.freq_pointer, e.term_utf8});
    }
    auto rvint = [&](size_t& pos) -> uint32_t {
        uint32_t v = 0;
        int shift = 0;
        while (pos < frq.size()) {
            const uint8_t b = frq[pos++];
            v |= static_cast<uint32_t>(b & 0x7FU) << shift;
            if ((b & 0x80U) == 0) break;
            shift += 7;
        }
        return v;
    };
    printf("\n=== df=1 .frq byte decode (sample) ===\n");
    int shown = 0;
    for (size_t i = 0; i + 1 < rows.size() && shown < 10; ++i) {
        if (rows[i].df != 1) continue;
        const int64_t a = rows[i].fp;
        const int64_t b = rows[i + 1].fp;
        const int64_t n = b - a;
        if (n <= 0) continue; // inlined/non-advancing
        printf("term='%-18s' bytes=%ld : ", rows[i].term.substr(0, 18).c_str(), n);
        for (int64_t k = 0; k < n && k < 12; ++k) printf("%02x ", frq[a + k]);
        // SLIM layout (df < skip_interval, which includes every df=1 term): the
        // external .frq block has NO leading codec/mode byte — the first byte IS
        // the doc-delta VInt. Decode the doc directly from pos=a. The reader
        // recovers doc_count from .tis and selects slim purely from
        // df < skip_interval (see freq_prox_encoder.cpp:217-224), so there is no
        // framing byte to skip.
        size_t pos = static_cast<size_t>(a);
        const uint32_t doc = rvint(pos);
        printf("  -> doc=%u (docVInt=%zu B, no framing byte)\n", doc, pos - static_cast<size_t>(a));
        shown++;
    }
    // Aggregate: avg df=1 block size. Under the slim layout there is no
    // mode-byte framing for df < skip_interval external terms, so the whole
    // block is the doc-delta VInt (plus the freq VInt when phrase/freq is kept);
    // report the average bytes/term only.
    int64_t df1_terms = 0, df1_bytes = 0;
    for (size_t i = 0; i + 1 < rows.size(); ++i) {
        if (rows[i].df != 1) continue;
        const int64_t s = rows[i + 1].fp - rows[i].fp;
        if (s <= 0) continue;
        df1_terms++;
        df1_bytes += s;
    }
    printf("df=1: %ld terms, %ld .frq bytes, avg=%.3f B/term (slim: no framing byte)\n", df1_terms,
           df1_bytes, df1_terms ? double(df1_bytes) / df1_terms : 0.0);
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

// === Phase 3 fuzz: mixed slim-kDefault + PFOR/windowed in ONE segment ========
//
// The SLIM kDefault format change drops the codec byte + doc_count VInt from the
// df < skip_interval path while leaving the df >= skip_interval (PFOR / windowed)
// path untouched. The two paths must coexist in a single segment with NO framing
// ambiguity: the reader picks slim-vs-coded purely from TermInfo::is_slim
// (df < skip_interval), mirroring the writer's own dispatch.
//
// This fuzz builds ONE segment whose vocabulary deliberately straddles the
// skip_interval (512): many df=1..just-below-512 SLIM terms interleaved with a
// few df>=512 PFOR/windowed terms, then decodes EVERY term and compares the
// recovered (doc_id, freq, positions) tuples against the ground-truth input.
// It exercises three emit paths that all share FreqProxEncoder:
//   (a) direct emit (V1 -> high-df becomes PFOR) read straight off the segment,
//   (b) direct emit V4 (-> high-df becomes WINDOWED) read straight off,
//   (c) forced spill + multi-input k-way merge re-encode.
// Over-read safety: each slim term is decoded against a buffer trimmed to its
// own [freq_pointer, next freq_pointer) span, so a loop that reads even one VInt
// past doc_freq would run off the end and SPIMI_THROW_CORRUPT.
namespace {

// Ground-truth posting list for one term: ascending (doc_id, positions).
struct ExpectedTerm {
    std::vector<std::pair<int32_t, std::vector<int32_t>>> docs; // (doc_id, positions)
};

// Deterministic mixed vocabulary straddling skip_interval=512. Returns the
// ground truth keyed by term, and appends every occurrence into `buffer` in the
// (term, doc_id, position) order the writer expects (term order is free; the
// posting buffer sorts). `max_doc` reports the highest doc_id used + 1.
std::map<std::string, ExpectedTerm> BuildMixedVocab(SpimiPostingBuffer& buffer, uint32_t seed,
                                                    int32_t* max_doc) {
    std::mt19937 rng(seed);
    std::map<std::string, ExpectedTerm> truth;
    int32_t hi = 0;

    auto emit_term = [&](const std::string& term, int32_t df, int32_t doc_stride) {
        ExpectedTerm& et = truth[term];
        int32_t doc = static_cast<int32_t>(rng() % 4); // small random start gap
        for (int32_t i = 0; i < df; ++i) {
            const int32_t freq = 1 + static_cast<int32_t>(rng() % 3); // 1..3 positions
            std::vector<int32_t> positions;
            int32_t p = static_cast<int32_t>(rng() % 5);
            for (int32_t j = 0; j < freq; ++j) {
                buffer.Append(term, static_cast<uint32_t>(doc), static_cast<uint32_t>(p));
                positions.push_back(p);
                p += 1 + static_cast<int32_t>(rng() % 7); // strictly increasing within doc
            }
            et.docs.emplace_back(doc, std::move(positions));
            hi = std::max(hi, doc + 1);
            doc += 1 + static_cast<int32_t>(rng() % doc_stride);
        }
    };

    // SLIM terms: df spread across 1 .. skip_interval-1 (all df < 512 -> slim).
    const int32_t slim_dfs[] = {1, 1, 1, 2, 3, 5, 8, 16, 50, 200, 400, 511};
    int slim_idx = 0;
    for (int32_t df : slim_dfs) {
        emit_term("slim_" + std::to_string(slim_idx++), df, /*doc_stride=*/3);
    }
    // CODED terms: df >= skip_interval(512) -> PFOR (V0/V1) or windowed (V4).
    emit_term("coded_a", 512, /*doc_stride=*/2); // exactly at the boundary
    emit_term("coded_b", 800, /*doc_stride=*/2);

    *max_doc = hi;
    return truth;
}

// Decode every term off a single emitted/merged segment and assert it matches
// the ground truth exactly. `slim_bounded` additionally re-decodes each slim
// term against a buffer trimmed to its own .frq span to prove no over-read.
void VerifySegmentAgainstTruth(const std::vector<uint8_t>& tis_bytes,
                               const std::vector<uint8_t>& frq_bytes,
                               const std::vector<uint8_t>& prx_bytes,
                               const std::map<std::string, ExpectedTerm>& truth, bool has_prox) {
    auto terms = ReadAllTerms(tis_bytes);
    ASSERT_EQ(terms.size(), truth.size()) << "term count must match the input vocabulary";

    int slim_seen = 0;
    int coded_seen = 0;
    for (size_t t = 0; t < terms.size(); ++t) {
        const TermEntry& e = terms[t];
        const TermEntry* next = (t + 1 < terms.size()) ? &terms[t + 1] : nullptr;
        auto it = truth.find(e.term_utf8);
        ASSERT_NE(it, truth.end()) << "unexpected term in segment: " << e.term_utf8;
        const ExpectedTerm& et = it->second;
        ASSERT_EQ(static_cast<size_t>(e.info.doc_freq), et.docs.size())
                << "doc_freq mismatch for " << e.term_utf8;

        // is_slim must mirror the writer's df < skip_interval dispatch.
        const bool expect_slim = e.info.doc_freq < FreqProxEncoder::kDefaultSkipInterval;
        EXPECT_EQ(e.info.is_slim, expect_slim)
                << "is_slim hint disagrees with df<skip_interval for " << e.term_utf8;
        if (expect_slim) {
            ++slim_seen;
        } else {
            ++coded_seen;
        }

        auto docs = DecodeTerm(e, frq_bytes, prx_bytes, next);
        ASSERT_EQ(docs.size(), et.docs.size()) << "decoded doc count mismatch for " << e.term_utf8;
        for (size_t d = 0; d < docs.size(); ++d) {
            EXPECT_EQ(docs[d].doc_id, et.docs[d].first)
                    << "doc_id mismatch for " << e.term_utf8 << " at index " << d;
            if (has_prox) {
                EXPECT_EQ(static_cast<size_t>(docs[d].freq), et.docs[d].second.size())
                        << "freq mismatch for " << e.term_utf8 << " at index " << d;
                EXPECT_EQ(docs[d].positions, et.docs[d].second)
                        << "positions mismatch for " << e.term_utf8 << " at index " << d;
            }
        }

        // Over-read proof for slim terms: trim the .frq buffer to exactly this
        // term's [freq_pointer, next freq_pointer) span (external, non-inlined
        // terms only — an inlined term's freq_pointer does not advance). A slim
        // loop that read even one VInt past doc_freq would run off this trimmed
        // buffer and SPIMI_THROW_CORRUPT instead of returning cleanly.
        if (expect_slim && !e.info.inlined && next != nullptr && !next->info.inlined) {
            const auto fp = static_cast<size_t>(e.info.freq_pointer);
            const auto nfp = static_cast<size_t>(next->info.freq_pointer);
            ASSERT_LE(fp, nfp);
            ASSERT_LE(nfp, frq_bytes.size());
            const size_t span = nfp - fp;
            if (span > 0) {
                std::vector<uint8_t> trimmed(frq_bytes.begin() + fp, frq_bytes.begin() + nfp);
                ASSERT_NO_THROW({
                    auto only = SpimiTermDocsReader::ReadTerm(trimmed, e.info.doc_freq, has_prox,
                                                              /*is_slim=*/true);
                    EXPECT_EQ(only.size(), et.docs.size());
                }) << "slim decode over-ran its .frq span for "
                   << e.term_utf8;
            }
        }
    }
    EXPECT_GT(slim_seen, 0) << "fuzz must exercise at least one SLIM kDefault term";
    EXPECT_GT(coded_seen, 0) << "fuzz must exercise at least one coded (PFOR/windowed) term";
}

} // namespace

TEST(SegmentMergerTest, FuzzMixedSlimAndCodedDirectEmitV1) {
    // (a) Direct emit at V1: high-df terms become PFOR (kCodeModeSpimiPfor),
    // low-df terms become SLIM kDefault (no codec byte). Decode straight off the
    // emitted segment and compare every posting to the ground truth.
    SpimiPostingBuffer buffer;
    int32_t max_doc = 0;
    auto truth = BuildMixedVocab(buffer, /*seed=*/0xC0FFEE, &max_doc);
    auto input = MakeInput(buffer, max_doc); // V1, positions present
    VerifySegmentAgainstTruth(input.tis_bytes, input.frq_bytes, input.prx_bytes, truth,
                              /*has_prox=*/true);
}

TEST(SegmentMergerTest, FuzzMixedSlimAndCodedDirectEmitV4Windowed) {
    // (b) Direct emit at V4: high-df terms become WINDOWED (kCodeModeSpimiWindowed),
    // low-df terms stay SLIM kDefault. Proves slim coexists with the windowed
    // path in one segment with no framing ambiguity.
    SpimiPostingBuffer buffer;
    int32_t max_doc = 0;
    auto truth = BuildMixedVocab(buffer, /*seed=*/0x1234ABCD, &max_doc);

    MemoryByteOutput tis, tii, frq, prx, fnm, nrm, seg_n, seg_gen;
    SpimiSegmentSink sink {.tis = &tis,
                           .tii = &tii,
                           .frq = &frq,
                           .prx = &prx,
                           .fnm = &fnm,
                           .nrm = &nrm,
                           .segments_n = &seg_n,
                           .segments_gen = &seg_gen};
    SpimiFulltextWriter::EmitSegment(buffer, sink, "_seg", "content", max_doc,
                                     FieldInfosWriter::kIndexVersionV4,
                                     /*omit_term_freq_and_positions=*/false, /*omit_norms=*/true);
    VerifySegmentAgainstTruth(tis.bytes(), frq.bytes(), prx.bytes(), truth, /*has_prox=*/true);
}

TEST(SegmentMergerTest, FuzzMixedSlimAndCodedForcedSpillMultiInputMerge) {
    // (c) Forced spill + multi-input k-way merge re-encode. Split the SAME
    // vocabulary across TWO spill segments (so coded_* terms' combined df still
    // crosses the boundary and the merge re-encoder must emit PFOR for them while
    // re-emitting the low-df terms slim). The merged segment must round-trip
    // every posting. This is the lockstep proof: spilled low-df terms and merged
    // low-df terms both flow through the shared FreqProxEncoder slim path.
    std::mt19937 rng(0xBEEF77);
    std::map<std::string, ExpectedTerm> truth;

    // Two halves of each term's posting list go to two different spills; the
    // second spill's doc_ids are offset by the first spill's doc_count so the
    // merge concatenates them into one ascending list.
    SpimiPostingBuffer buf_a;
    SpimiPostingBuffer buf_b;
    int32_t doc_count_a = 0;
    int32_t doc_count_b = 0;

    auto emit_split = [&](const std::string& term, int32_t df_a, int32_t df_b, int32_t stride) {
        ExpectedTerm& et = truth[term];
        auto emit_half = [&](SpimiPostingBuffer& buf, int32_t df, int32_t base_doc,
                             int32_t* local_hi) {
            int32_t doc = static_cast<int32_t>(rng() % 3);
            for (int32_t i = 0; i < df; ++i) {
                const int32_t freq = 1 + static_cast<int32_t>(rng() % 3);
                std::vector<int32_t> positions;
                int32_t p = static_cast<int32_t>(rng() % 4);
                for (int32_t j = 0; j < freq; ++j) {
                    buf.Append(term, static_cast<uint32_t>(doc), static_cast<uint32_t>(p));
                    positions.push_back(p);
                    p += 1 + static_cast<int32_t>(rng() % 6);
                }
                et.docs.emplace_back(base_doc + doc, std::move(positions));
                *local_hi = std::max(*local_hi, doc + 1);
                doc += 1 + static_cast<int32_t>(rng() % stride);
            }
        };
        int32_t hi_a = 0;
        emit_half(buf_a, df_a, /*base_doc=*/0, &hi_a);
        doc_count_a = std::max(doc_count_a, hi_a);
        // base_doc for the second half is the first spill's doc_count (filled in
        // after we know it); stash df_b to replay once doc_count_a is final.
        (void)df_b; // emitted below in the second pass
    };

    // First pass: fill spill A and record df_b plans.
    struct Plan {
        std::string term;
        int32_t df_b;
        int32_t stride;
    };
    std::vector<Plan> plans;
    auto schedule = [&](const std::string& term, int32_t df_a, int32_t df_b, int32_t stride) {
        emit_split(term, df_a, df_b, stride);
        plans.push_back({term, df_b, stride});
    };

    // Low-df terms stay slim in BOTH spills and the merge. coded_* terms have
    // combined df (a+b) >= 512 so the merge re-encodes them as PFOR.
    schedule("slim_0", 1, 0, 3);
    schedule("slim_1", 2, 1, 3);
    schedule("slim_2", 8, 5, 3);
    schedule("slim_3", 100, 90, 2);
    schedule("coded_a", 300, 300, 2); // 600 >= 512 -> PFOR after merge
    schedule("coded_b", 260, 260, 2); // 520 >= 512 -> PFOR after merge

    // Second pass: fill spill B with doc_ids offset by spill A's doc_count.
    for (const Plan& pl : plans) {
        if (pl.df_b == 0) {
            continue;
        }
        ExpectedTerm& et = truth[pl.term];
        int32_t doc = static_cast<int32_t>(rng() % 3);
        int32_t hi_b = 0;
        for (int32_t i = 0; i < pl.df_b; ++i) {
            const int32_t freq = 1 + static_cast<int32_t>(rng() % 3);
            std::vector<int32_t> positions;
            int32_t p = static_cast<int32_t>(rng() % 4);
            for (int32_t j = 0; j < freq; ++j) {
                buf_b.Append(pl.term, static_cast<uint32_t>(doc), static_cast<uint32_t>(p));
                positions.push_back(p);
                p += 1 + static_cast<int32_t>(rng() % 6);
            }
            et.docs.emplace_back(doc_count_a + doc, std::move(positions));
            hi_b = std::max(hi_b, doc + 1);
            doc += 1 + static_cast<int32_t>(rng() % pl.stride);
        }
        doc_count_b = std::max(doc_count_b, hi_b);
    }

    // Force two distinct spill segments through SpillManager, then k-way merge.
    SpillManager mgr("content");
    ASSERT_EQ(mgr.FlushBuffer(buf_a, doc_count_a), static_cast<int64_t>(truth.size()));
    ASSERT_EQ(mgr.FlushBuffer(buf_b, doc_count_b),
              // spill B only has the terms with df_b > 0.
              [&]() {
                  int64_t n = 0;
                  for (const Plan& pl : plans) {
                      if (pl.df_b > 0) {
                          ++n;
                      }
                  }
                  return n;
              }());
    ASSERT_EQ(mgr.SpillCount(), 2U);

    std::vector<SegmentMerger::Input> inputs {ToInput(mgr, 0), ToInput(mgr, 1)};
    auto result = DoMerge(inputs, doc_count_a + doc_count_b);
    ASSERT_EQ(result.term_count, static_cast<int64_t>(truth.size()));

    VerifySegmentAgainstTruth(result.tis_bytes, result.frq_bytes, result.prx_bytes, truth,
                              /*has_prox=*/true);
}

} // namespace doris::segment_v2::inverted_index::spimi
