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
#include <re2/re2.h>

#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <memory>
#include <mutex>
#include <numeric>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/encoding/byte_sink.h"
#include "storage/index/snii/encoding/byte_source.h"
#include "storage/index/snii/encoding/pfor.h"
#include "storage/index/snii/format/format_constants.h"
#include "storage/index/snii/format/phrase_bigram.h"
#include "storage/index/snii/format/prx_pod.h"
#include "storage/index/snii/format/tail_pointer.h"
#include "storage/index/snii/io/file_reader.h"
#include "storage/index/snii/io/file_writer.h"
#include "storage/index/snii/query/docid_sink.h"
#include "storage/index/snii/query/internal/regex_prefix.h"
#include "storage/index/snii/query/internal/term_expansion.h"
#include "storage/index/snii/query/phrase_query.h"
#include "storage/index/snii/query/prefix_query.h"
#include "storage/index/snii/query/regexp_query.h"
#include "storage/index/snii/query/term_query.h"
#include "storage/index/snii/query/wildcard_query.h"
// T24 query op-count seam. Define the gate before the include so QueryTestCounters
// is visible in this TU; it is also auto-enabled library-wide by BE_TEST, so the
// phrase_query.cpp increments and the reads below share the same singleton.
#define SNII_QUERY_TEST_COUNTERS
#include "storage/index/snii/query/internal/query_test_counters.h"
#include "storage/index/snii/reader/dict_block_cache.h"
#include "storage/index/snii/reader/logical_index_reader.h"
#include "storage/index/snii/reader/snii_segment_reader.h"
#include "storage/index/snii/writer/snii_compound_writer.h"
#include "storage/index/snii/writer/spimi_term_buffer.h"
#include "storage/index/snii_query_test_util.h"

namespace doris::snii::query {
using doris::Status; // RETURN_IF_ERROR expands to bare Status
namespace {

// Shared reader/writer fixtures live in snii_query_test_util.h so other SNII test
// files can reuse them; pull them into this suite's scope unqualified.
using snii_test::assert_ok;
using snii_test::build_reader;
using snii_test::make_term;
using snii_test::MemoryFile;
using snii_test::PostingDoc;
using snii_test::ScopedEnv;

class RecordingDocIdSink final : public DocIdSink {
public:
    Status append_sorted(std::span<const uint32_t> docids) override {
        out.insert(out.end(), docids.begin(), docids.end());
        return Status::OK();
    }

    Status append_range(uint32_t first, uint64_t last_exclusive) override {
        ++range_calls;
        for (uint64_t docid = first; docid < last_exclusive; ++docid) {
            out.push_back(static_cast<uint32_t>(docid));
        }
        return Status::OK();
    }

    std::vector<uint32_t> out;
    size_t range_calls = 0;
};

struct PrxRange {
    uint64_t offset = 0;
    uint64_t len = 0;
};

void assert_selective_prx_matches_constant_positions(Slice window,
                                                     const std::vector<uint32_t>& selected_docs,
                                                     uint32_t expected_position) {
    std::vector<uint32_t> selected_positions;
    std::vector<uint32_t> selected_offsets;
    ByteSource selected_source(window);
    assert_ok(format::read_prx_window_csr_selective(&selected_source, selected_docs,
                                                    &selected_positions, &selected_offsets));

    ASSERT_EQ(selected_offsets.size(), selected_docs.size() + 1);
    ASSERT_EQ(selected_positions.size(), selected_docs.size());
    for (size_t i = 0; i < selected_docs.size(); ++i) {
        EXPECT_EQ(selected_offsets[i], i);
        EXPECT_EQ(selected_positions[i], expected_position);
    }
    EXPECT_EQ(selected_offsets.back(), selected_positions.size());
}

// A FileReader decorator that counts read_batch() invocations. Each BatchRangeFetcher
// fetch() barrier issues exactly one read_batch (== one batched/remote serial round),
// so this isolates the number of I/O rounds a query plan emits. Single read_at() calls
// (dict-block / BSBF loads) delegate straight through and are intentionally not counted.
class BatchRoundCountingReader final : public doris::snii::io::FileReader {
public:
    explicit BatchRoundCountingReader(doris::snii::io::FileReader* inner) : inner_(inner) {}

    Status read_at(uint64_t offset, size_t len, std::vector<uint8_t>* out) override {
        return inner_->read_at(offset, len, out);
    }
    Status read_batch(const std::vector<doris::snii::io::Range>& ranges,
                      std::vector<std::vector<uint8_t>>* outs) override {
        ++batch_rounds_;
        return inner_->read_batch(ranges, outs);
    }
    uint64_t size() const override { return inner_->size(); }

    size_t batch_rounds() const { return batch_rounds_; }
    void reset_rounds() { batch_rounds_ = 0; }

private:
    doris::snii::io::FileReader* inner_;
    size_t batch_rounds_ = 0;
};

// 480 docids with irregular gaps (deterministic LCG): the slim docs region PFOR
// then exceeds the 256B inline threshold so each term becomes a pod_ref, while
// df < 512 keeps it slim (not windowed). This is the layout that forced one PRX
// fetch() per term before T02.
std::vector<uint32_t> slim_pod_ref_docids() {
    std::vector<uint32_t> ids;
    ids.reserve(480);
    uint32_t cur = 0;
    uint32_t state = 0x9e3779b9U;
    for (int i = 0; i < 480; ++i) {
        ids.push_back(cur);
        state = state * 1664525U + 1013904223U;
        cur += 1U + (state >> 23) % 250U; // gap in [1, 250]
    }
    return ids;
}

// Builds an index with three overlapping slim pod_ref terms ("paa"/"pbb"/"pcc")
// sharing one docid set, each with a single position (5/6/7) so the phrase
// "paa pbb pcc" matches every shared doc. The index is opened through
// `read_through` (e.g. a counting decorator wrapping `file`). `shared_docids`
// returns the docid set, which equals the expected phrase result.
Status build_slim_pod_ref_phrase_reader(MemoryFile* file, doris::snii::io::FileReader* read_through,
                                        reader::SniiSegmentReader* segment_reader,
                                        reader::LogicalIndexReader* index_reader,
                                        std::vector<uint32_t>* shared_docids) {
    const std::vector<uint32_t> docids = slim_pod_ref_docids();
    *shared_docids = docids;
    auto make_pos_term = [&](std::string term, uint32_t position) {
        std::vector<PostingDoc> docs;
        docs.reserve(docids.size());
        for (uint32_t docid : docids) {
            docs.push_back({docid, {position}});
        }
        return make_term(std::move(term), std::move(docs));
    };

    writer::SniiIndexInput input;
    input.index_id = 11;
    input.index_suffix = "Body";
    input.config = format::IndexConfig::kDocsPositions;
    input.doc_count = docids.back() + 1;
    input.terms = {make_pos_term("paa", 5), make_pos_term("pbb", 6), make_pos_term("pcc", 7)};
    std::ranges::sort(input.terms,
                      [](const writer::TermPostings& lhs, const writer::TermPostings& rhs) {
                          return lhs.term < rhs.term;
                      });

    writer::SniiCompoundWriter writer(file);
    RETURN_IF_ERROR(writer.add_logical_index(input));
    RETURN_IF_ERROR(writer.finish());
    EXPECT_TRUE(file->finalized());

    RETURN_IF_ERROR(reader::SniiSegmentReader::open(read_through, segment_reader));
    return segment_reader->open_index(input.index_id, input.index_suffix, index_reader);
}

writer::SniiIndexInput make_many_term_input(uint64_t index_id, std::string suffix,
                                            uint32_t n_terms) {
    writer::SniiIndexInput input;
    input.index_id = index_id;
    input.index_suffix = std::move(suffix);
    input.config = format::IndexConfig::kDocsOnly;
    input.doc_count = n_terms + 1;
    input.target_dict_block_bytes = 128;
    input.terms.reserve(n_terms);
    for (uint32_t i = 0; i < n_terms; ++i) {
        input.terms.push_back(
                make_term("term_" + std::to_string(1000000 + i), {{.docid = i, .positions = {0}}}));
    }
    return input;
}

format::TailPointer read_tail_pointer(MemoryFile* file) {
    std::vector<uint8_t> bytes;
    assert_ok(file->read_at(file->size() - format::tail_pointer_size(), format::tail_pointer_size(),
                            &bytes));
    format::TailPointer tp;
    assert_ok(format::decode_tail_pointer(Slice(bytes), &tp));
    return tp;
}

TEST(SniiSegmentReaderTest, OpenDoesNotReadWholeTailMetaRegion) {
    MemoryFile file;
    writer::SniiCompoundWriter writer(&file);
    assert_ok(writer.add_logical_index(make_many_term_input(7, "Body", 4096)));
    assert_ok(writer.finish());

    const format::TailPointer tp = read_tail_pointer(&file);
    file.clear_reads();

    reader::SniiSegmentReader segment_reader;
    assert_ok(reader::SniiSegmentReader::open(&file, &segment_reader));

    EXPECT_LT(file.read_bytes(), tp.meta_region_length);
    for (const auto& read : file.reads()) {
        EXPECT_FALSE(read.offset == tp.meta_region_offset && read.len == tp.meta_region_length);
    }
}

TEST(SniiSegmentReaderTest, IndexExistsUsesCachedTailDirectory) {
    MemoryFile file;
    writer::SniiCompoundWriter writer(&file);
    writer::SniiIndexInput input = make_many_term_input(7, "Body", 4096);
    assert_ok(writer.add_logical_index(input));
    assert_ok(writer.finish());

    reader::SniiSegmentReader segment_reader;
    assert_ok(reader::SniiSegmentReader::open(&file, &segment_reader));

    file.clear_reads();
    bool exists = false;
    assert_ok(segment_reader.index_exists(input.index_id, input.index_suffix, &exists));
    EXPECT_TRUE(exists);
    EXPECT_EQ(file.read_bytes(), 0);

    assert_ok(segment_reader.index_exists(input.index_id + 1, input.index_suffix, &exists));
    EXPECT_FALSE(exists);
    EXPECT_EQ(file.read_bytes(), 0);
}

// F35: memory_usage() feeds the InvertedIndexSearcherCache charge and must now
// account for the resident sampled term index + DICT block directory (previously
// omitted -> under-charge -> over-commit). Exact per-field equality would need
// the reader's private members; the observable public properties asserted here
// are a charge floor (>= sizeof) and monotonic growth with the vocabulary (more
// DICT blocks -> larger sti_/dbd_ heap). The exact heap_bytes() formula the fix
// sums is pinned deterministically by the SampledTermIndex / DictBlockDirectory /
// DictBlock unit tests in snii_writer_test.cpp.
TEST(SniiSegmentReaderTest, MemoryUsageGrowsWithVocabulary) {
    auto open_many_term_reader = [](uint32_t n_terms, MemoryFile* file,
                                    reader::SniiSegmentReader* segment_reader,
                                    reader::LogicalIndexReader* index_reader) {
        writer::SniiCompoundWriter writer(file);
        // target_dict_block_bytes == 128 (make_many_term_input) -> many small DICT
        // blocks, so a larger vocabulary yields more sampled terms + block refs.
        assert_ok(writer.add_logical_index(make_many_term_input(7, "Body", n_terms)));
        assert_ok(writer.finish());
        assert_ok(reader::SniiSegmentReader::open(file, segment_reader));
        assert_ok(segment_reader->open_index(7, "Body", index_reader));
    };

    MemoryFile small_file;
    reader::SniiSegmentReader small_segment;
    reader::LogicalIndexReader small_reader;
    open_many_term_reader(64, &small_file, &small_segment, &small_reader);

    MemoryFile large_file;
    reader::SniiSegmentReader large_segment;
    reader::LogicalIndexReader large_reader;
    open_many_term_reader(4096, &large_file, &large_segment, &large_reader);

    EXPECT_GE(small_reader.memory_usage(), sizeof(reader::LogicalIndexReader));
    EXPECT_GT(large_reader.memory_usage(), small_reader.memory_usage());
}

// G13 end-to-end: a many-term segment's per-index META BLOB (the first serial
// fetch of a cold open, dominated by the sti + dbd tables) must SHRINK on disk
// once those sections are zstd-compressed, the open must therefore fetch fewer
// bytes (MemoryFile::reads()), and every lookup / prefix enumeration must stay
// equal to an uncompressed control built from the identical input.
// NOLINTNEXTLINE(readability-function-cognitive-complexity)
TEST(SniiSegmentReaderTest, MetaCompressionShrinksOpenFetchAndKeepsLookupsEqual) {
    constexpr uint32_t kTerms = 4096;
    auto build = [](MemoryFile* file) {
        writer::SniiCompoundWriter writer(file);
        assert_ok(writer.add_logical_index(make_many_term_input(7, "Body", kTerms)));
        assert_ok(writer.finish());
    };

    MemoryFile compressed_file;
    build(&compressed_file); // default: G13 compression active
    MemoryFile control_file;
    {
        ScopedEnv off("SNII_META_COMPRESS_MIN", "18446744073709551615");
        build(&control_file); // pre-G13 layout from the identical input
    }

    // The tail meta region (which embeds the per-index meta blob) shrinks.
    const format::TailPointer tp_compressed = read_tail_pointer(&compressed_file);
    const format::TailPointer tp_control = read_tail_pointer(&control_file);
    ASSERT_GT(tp_control.meta_region_length, 0U);
    EXPECT_LT(tp_compressed.meta_region_length, tp_control.meta_region_length);

    // So does the single per-index meta blob read_index_meta() fetches.
    reader::SniiSegmentReader compressed_segment;
    assert_ok(reader::SniiSegmentReader::open(&compressed_file, &compressed_segment));
    reader::SniiSegmentReader control_segment;
    assert_ok(reader::SniiSegmentReader::open(&control_file, &control_segment));
    std::vector<uint8_t> compressed_meta;
    assert_ok(compressed_segment.read_index_meta(7, "Body", &compressed_meta));
    std::vector<uint8_t> control_meta;
    assert_ok(control_segment.read_index_meta(7, "Body", &control_meta));
    EXPECT_LT(compressed_meta.size(), control_meta.size());

    // Cold index open fetches fewer bytes end to end (same reads elsewhere: the
    // dict blocks / bsbf bytes are identical; only the meta blob differs).
    compressed_file.clear_reads();
    reader::LogicalIndexReader compressed_reader;
    assert_ok(compressed_segment.open_index(7, "Body", &compressed_reader));
    const size_t compressed_open_bytes = compressed_file.read_bytes();

    control_file.clear_reads();
    reader::LogicalIndexReader control_reader;
    assert_ok(control_segment.open_index(7, "Body", &control_reader));
    const size_t control_open_bytes = control_file.read_bytes();
    EXPECT_LT(compressed_open_bytes, control_open_bytes);
    std::cout << "[G13] meta blob: raw=" << control_meta.size()
              << "B zstd=" << compressed_meta.size() << "B; open fetch: raw=" << control_open_bytes
              << "B zstd=" << compressed_open_bytes << "B\n";

    // Lookups stay equal across the two layouts: present terms resolve to the
    // same entry essentials, absent terms miss on both.
    for (uint32_t i = 0; i < kTerms; i += 97) {
        const std::string term = "term_" + std::to_string(1000000 + i);
        bool found_a = false;
        bool found_b = false;
        format::DictEntry entry_a;
        format::DictEntry entry_b;
        uint64_t frq_a = 0;
        uint64_t prx_a = 0;
        uint64_t frq_b = 0;
        uint64_t prx_b = 0;
        assert_ok(compressed_reader.lookup(term, &found_a, &entry_a, &frq_a, &prx_a));
        assert_ok(control_reader.lookup(term, &found_b, &entry_b, &frq_b, &prx_b));
        ASSERT_TRUE(found_a) << term;
        ASSERT_TRUE(found_b) << term;
        EXPECT_EQ(entry_a.term, entry_b.term);
        EXPECT_EQ(entry_a.kind, entry_b.kind);
        EXPECT_EQ(entry_a.df, entry_b.df);
        EXPECT_EQ(frq_a, frq_b);
        EXPECT_EQ(prx_a, prx_b);
    }
    bool found_absent = true;
    format::DictEntry absent_entry;
    uint64_t frq = 0;
    uint64_t prx = 0;
    assert_ok(
            compressed_reader.lookup("zzzz_not_indexed", &found_absent, &absent_entry, &frq, &prx));
    EXPECT_FALSE(found_absent);

    // Ordered prefix enumeration walks sti + dbd + dict blocks on both layouts
    // and must produce the identical term sequence.
    std::vector<reader::LogicalIndexReader::PrefixHit> hits_a;
    std::vector<reader::LogicalIndexReader::PrefixHit> hits_b;
    assert_ok(compressed_reader.prefix_terms("term_10001", &hits_a));
    assert_ok(control_reader.prefix_terms("term_10001", &hits_b));
    ASSERT_EQ(hits_a.size(), hits_b.size());
    ASSERT_EQ(hits_a.size(), 100U); // term_1000100 .. term_1000199
    for (size_t i = 0; i < hits_a.size(); ++i) {
        EXPECT_EQ(hits_a[i].term, hits_b[i].term);
        EXPECT_EQ(hits_a[i].entry.df, hits_b[i].entry.df);
    }
}

// P1 cold-read fix: a NON-resident bloom is skipped entirely. open() must not
// read the 28B header (which the old L1 path cached) and lookup() must not issue a
// 32B body probe; absent / present terms still resolve correctly via sti -> dict.
TEST(SniiSegmentReaderTest, NonResidentBsbfIsSkippedNotProbed) {
    ScopedEnv disable_resident_bsbf("SNII_BSBF_RESIDENT_MAX", "0");

    MemoryFile file;
    writer::SniiCompoundWriter writer(&file);
    writer::SniiIndexInput input = make_many_term_input(7, "Body", 1024);
    assert_ok(writer.add_logical_index(input));
    assert_ok(writer.finish());

    reader::SniiSegmentReader segment_reader;
    assert_ok(reader::SniiSegmentReader::open(&file, &segment_reader));
    format::SectionRefs refs;
    assert_ok(segment_reader.section_refs_for_index(input.index_id, input.index_suffix, &refs));
    ASSERT_GT(refs.bsbf.length, format::kBsbfHeaderSize); // a real filter exists on disk
    const uint64_t bsbf_end = refs.bsbf.offset + refs.bsbf.length;
    auto touches_bsbf = [&](uint64_t offset, size_t len) {
        return offset < bsbf_end && refs.bsbf.offset < offset + len;
    };

    // open() must NOT touch the bsbf section at all on the non-resident path.
    file.clear_reads();
    reader::LogicalIndexReader index_reader;
    assert_ok(segment_reader.open_index(input.index_id, input.index_suffix, &index_reader));
    for (const auto& read : file.reads()) {
        EXPECT_FALSE(touches_bsbf(read.offset, read.len))
                << "non-resident open must skip the bsbf header";
    }

    // An absent term still resolves to empty via sti -> dict, and the lookup must
    // NOT probe the bsbf section.
    file.clear_reads();
    std::vector<uint32_t> docids;
    assert_ok(term_query(index_reader, "absent_term", &docids));
    EXPECT_TRUE(docids.empty());
    for (const auto& read : file.reads()) {
        EXPECT_FALSE(touches_bsbf(read.offset, read.len))
                << "non-resident lookup must not probe the bsbf section";
    }

    // A present term is still found via sti -> dict (correctness with no bloom).
    std::vector<uint32_t> present;
    assert_ok(term_query(index_reader, "term_1000000", &present));
    EXPECT_FALSE(present.empty());
}

TEST(SniiSegmentReaderTest, LogicalIndexOpenCachesResidentMetadataAndSmallHeaders) {
    ScopedEnv resident_bsbf("SNII_BSBF_RESIDENT_MAX", "1048576");
    ScopedEnv resident_dict("SNII_DICT_RESIDENT_MAX", "1048576");

    MemoryFile file;
    reader::SniiSegmentReader segment_reader;
    reader::LogicalIndexReader index_reader;
    assert_ok(build_reader(&file, &segment_reader, &index_reader));

    file.clear_reads();
    bool found = false;
    format::DictEntry entry;
    uint64_t frq_base = 0;
    uint64_t prx_base = 0;
    assert_ok(index_reader.lookup("failed", &found, &entry, &frq_base, &prx_base));
    EXPECT_TRUE(found);
    EXPECT_EQ(file.read_bytes(), 0);

    std::vector<reader::LogicalIndexReader::PrefixHit> hits;
    assert_ok(index_reader.prefix_terms("ord", &hits, 10));
    ASSERT_EQ(hits.size(), 2);
    EXPECT_EQ(hits[0].term, "order");
    EXPECT_EQ(hits[1].term, "ordinal");
    EXPECT_EQ(file.read_bytes(), 0);
}

TEST(SniiPhraseQueryTest, WindowedPhraseQueryKeepsCorrectCandidateOrdinals) {
    MemoryFile file;
    reader::SniiSegmentReader segment_reader;
    reader::LogicalIndexReader index_reader;
    assert_ok(build_reader(&file, &segment_reader, &index_reader));

    std::vector<uint32_t> docids;
    assert_ok(phrase_query(index_reader, {"failed", "order"}, &docids));

    const std::vector<uint32_t> expected {5000, 7000, 8000};
    EXPECT_EQ(docids, expected);
}

TEST(SniiPhraseQueryTest, WindowedPhrasePrefixQueryKeepsCorrectCandidateOrdinals) {
    MemoryFile file;
    reader::SniiSegmentReader segment_reader;
    reader::LogicalIndexReader index_reader;
    assert_ok(build_reader(&file, &segment_reader, &index_reader));

    std::vector<uint32_t> docids;
    assert_ok(phrase_prefix_query(index_reader, {"failed", "ord"}, &docids, 10));

    const std::vector<uint32_t> expected {5000, 6000, 7000, 8000};
    EXPECT_EQ(docids, expected);
}

TEST(SniiPhraseQueryTest, SingleTailPhrasePrefixUsesStreamingPhrasePath) {
    MemoryFile file;
    reader::SniiSegmentReader segment_reader;
    reader::LogicalIndexReader index_reader;
    assert_ok(build_reader(&file, &segment_reader, &index_reader));

    std::vector<uint32_t> docids;
    assert_ok(phrase_prefix_query(index_reader, {"failed", "orde"}, &docids, 10));

    const std::vector<uint32_t> expected {5000, 7000, 8000};
    EXPECT_EQ(docids, expected);
}

// PV1 (deterministic perf): a 3-term slim pod_ref phrase issues exactly one batched
// PRX round. read_batch count = 1 (round1 docs) + 1 (shared PRX fetch) = 2; the
// docid conjunction decodes slim docs from round1 (no extra round) and dict/BSBF
// loads use uncounted read_at. Before T02 this was 4 (round1 + one PRX fetch per
// term).
TEST(SniiPhraseQueryTest, PhraseQueryIssuesSinglePrxBatchRound) {
    MemoryFile file;
    BatchRoundCountingReader counting(&file);
    reader::SniiSegmentReader segment_reader;
    reader::LogicalIndexReader index_reader;
    std::vector<uint32_t> shared_docids;
    assert_ok(build_slim_pod_ref_phrase_reader(&file, &counting, &segment_reader, &index_reader,
                                               &shared_docids));

    // Guard: every phrase term must be a slim pod_ref (not inline, not windowed),
    // otherwise the per-term PRX fetch this test isolates would not occur.
    for (std::string_view term : {"paa", "pbb", "pcc"}) {
        bool found = false;
        format::DictEntry entry;
        uint64_t frq_base = 0;
        uint64_t prx_base = 0;
        assert_ok(index_reader.lookup(term, &found, &entry, &frq_base, &prx_base));
        ASSERT_TRUE(found) << term;
        EXPECT_EQ(entry.kind, format::DictEntryKind::kPodRef) << term;
        EXPECT_EQ(entry.enc, format::DictEntryEnc::kSlim) << term;
    }

    counting.reset_rounds();
    std::vector<uint32_t> docids;
    assert_ok(phrase_query(index_reader, {"paa", "pbb", "pcc"}, &docids));

    EXPECT_EQ(docids, shared_docids);
    EXPECT_EQ(counting.batch_rounds(), 2U);
}

// FV3 (functional equivalence): the shared single-batch PRX path returns the result
// dictated by the position layout across three overlapping slim pod_ref terms. A
// backfill mistake (wrong plan/chunk/handle mapping) would misread a term's
// positions and drop docs.
TEST(SniiPhraseQueryTest, ThreeTermPhraseMatchesAcrossSharedPrxFetch) {
    MemoryFile file;
    reader::SniiSegmentReader segment_reader;
    reader::LogicalIndexReader index_reader;
    std::vector<uint32_t> shared_docids;
    assert_ok(build_slim_pod_ref_phrase_reader(&file, &file, &segment_reader, &index_reader,
                                               &shared_docids));

    std::vector<uint32_t> docids;
    assert_ok(phrase_query(index_reader, {"paa", "pbb", "pcc"}, &docids));
    EXPECT_EQ(docids, shared_docids);

    // Same terms, non-consecutive order (positions 7,6,5): the candidate set is
    // identical but no doc satisfies the phrase, so the shared PRX fetch must yield
    // an empty result.
    std::vector<uint32_t> reversed;
    assert_ok(phrase_query(index_reader, {"pcc", "pbb", "paa"}, &reversed));
    EXPECT_TRUE(reversed.empty());
}

TEST(SniiPhraseQueryTest, TwoTermPhraseUsesHiddenBigramPosting) {
    MemoryFile file;
    reader::SniiSegmentReader segment_reader;
    reader::LogicalIndexReader index_reader;
    assert_ok(build_reader(&file, &segment_reader, &index_reader,
                           /*include_phrase_bigrams=*/true));

    const auto original_prx_span = [&](std::string_view term) {
        bool found = false;
        format::DictEntry entry;
        uint64_t frq_base = 0;
        uint64_t prx_base = 0;
        assert_ok(index_reader.lookup(term, &found, &entry, &frq_base, &prx_base));
        EXPECT_TRUE(found);
        return PrxRange {.offset = index_reader.section_refs().posting_region.offset + prx_base +
                                   entry.prx_off_delta,
                         .len = entry.prx_len};
    };
    const std::vector<PrxRange> original_prx {
            original_prx_span("failed"),
            original_prx_span("order"),
    };

    file.clear_reads();
    std::vector<uint32_t> docids;
    assert_ok(phrase_query(index_reader, {"failed", "order"}, &docids));

    const std::vector<uint32_t> expected {5000, 7000, 8000};
    EXPECT_EQ(docids, expected);
    for (const PrxRange& prx : original_prx) {
        const bool original_prx_read = std::ranges::any_of(file.reads(), [&](const auto& read) {
            return read.offset < prx.offset + prx.len && prx.offset < read.offset + read.len;
        });
        EXPECT_FALSE(original_prx_read);
    }
}

TEST(SniiPhraseQueryTest, TwoTermPhraseWithNonIndexableTermFallsBackToPositions) {
    MemoryFile file;
    reader::SniiSegmentReader segment_reader;
    reader::LogicalIndexReader index_reader;
    assert_ok(build_reader(&file, &segment_reader, &index_reader,
                           /*include_phrase_bigrams=*/true));

    std::vector<uint32_t> docids;
    assert_ok(phrase_query(index_reader, {"trace", "123"}, &docids));

    const std::vector<uint32_t> expected {42};
    EXPECT_EQ(docids, expected);
}

TEST(SniiPhraseQueryTest, TwoTermPhrasePrefixWorksWithHiddenBigramFormat) {
    MemoryFile file;
    reader::SniiSegmentReader segment_reader;
    reader::LogicalIndexReader index_reader;
    assert_ok(build_reader(&file, &segment_reader, &index_reader,
                           /*include_phrase_bigrams=*/true));

    std::vector<uint32_t> docids;
    assert_ok(phrase_prefix_query(index_reader, {"failed", "ord"}, &docids, 10));

    const std::vector<uint32_t> expected {5000, 6000, 7000, 8000};
    EXPECT_EQ(docids, expected);
}

TEST(SniiPhraseQueryTest, PrefixQueryDoesNotExposeHiddenBigramTerms) {
    MemoryFile file;
    reader::SniiSegmentReader segment_reader;
    reader::LogicalIndexReader index_reader;
    assert_ok(build_reader(&file, &segment_reader, &index_reader,
                           /*include_phrase_bigrams=*/true));

    std::vector<uint32_t> docids;
    assert_ok(prefix_query(index_reader, std::string(format::kPhraseBigramTermMarker), &docids));
    EXPECT_TRUE(docids.empty());
}

TEST(SniiPhraseQueryTest, WildcardQueryDoesNotExposeHiddenBigramTerms) {
    MemoryFile file;
    reader::SniiSegmentReader segment_reader;
    reader::LogicalIndexReader index_reader;
    assert_ok(build_reader(&file, &segment_reader, &index_reader,
                           /*include_phrase_bigrams=*/true));

    std::vector<uint32_t> docids;
    assert_ok(wildcard_query(index_reader, "*failed*order*", &docids));
    EXPECT_TRUE(docids.empty());
}

TEST(SniiPhraseQueryTest, RegexpQueryDoesNotExposeHiddenBigramTerms) {
    MemoryFile file;
    reader::SniiSegmentReader segment_reader;
    reader::LogicalIndexReader index_reader;
    assert_ok(build_reader(&file, &segment_reader, &index_reader,
                           /*include_phrase_bigrams=*/true));

    std::vector<uint32_t> docids;
    assert_ok(regexp_query(index_reader, ".*failed.*order.*", &docids));
    EXPECT_TRUE(docids.empty());
}

std::vector<uint32_t> all_docids_0_to(uint32_t end_exclusive) {
    std::vector<uint32_t> docids(end_exclusive);
    std::iota(docids.begin(), docids.end(), 0U);
    return docids;
}

// RQ-01: a plain literal pattern is anchored at both ends (RE2::FullMatch), so it
// matches exactly the "order" term and returns its full docid range.
TEST(SniiRegexpQueryTest, MatchesAnchoredLiteralTerm) {
    MemoryFile file;
    reader::SniiSegmentReader segment_reader;
    reader::LogicalIndexReader index_reader;
    assert_ok(build_reader(&file, &segment_reader, &index_reader));

    std::vector<uint32_t> docids;
    assert_ok(regexp_query(index_reader, "order", &docids));
    EXPECT_EQ(docids, all_docids_0_to(9000));
}

// RQ-03: alternation under a shared prefix expands to "order" and "ordinal"; both
// span the full docid range, so the union must dedup back to [0..8999].
TEST(SniiRegexpQueryTest, CharClassMatchesMultipleTermsDeduped) {
    MemoryFile file;
    reader::SniiSegmentReader segment_reader;
    reader::LogicalIndexReader index_reader;
    assert_ok(build_reader(&file, &segment_reader, &index_reader));

    std::vector<uint32_t> docids;
    assert_ok(regexp_query(index_reader, "ord(er|inal)", &docids));
    EXPECT_EQ(docids, all_docids_0_to(9000));
}

// Alternation across non-adjacent terms with disjoint docids yields a sorted,
// deduplicated union.
TEST(SniiRegexpQueryTest, AlternationUnionsDistinctTerms) {
    MemoryFile file;
    reader::SniiSegmentReader segment_reader;
    reader::LogicalIndexReader index_reader;
    assert_ok(build_reader(&file, &segment_reader, &index_reader));

    std::vector<uint32_t> docids;
    assert_ok(regexp_query(index_reader, "(123|needle)", &docids));
    const std::vector<uint32_t> expected {42, 100, 101, 102, 6000};
    EXPECT_EQ(docids, expected);
}

// RQ-04: a non-existent prefix enumerates nothing and returns an empty result.
TEST(SniiRegexpQueryTest, NoTermMatchesEmpty) {
    MemoryFile file;
    reader::SniiSegmentReader segment_reader;
    reader::LogicalIndexReader index_reader;
    assert_ok(build_reader(&file, &segment_reader, &index_reader));

    std::vector<uint32_t> docids;
    assert_ok(regexp_query(index_reader, "zzz.*", &docids));
    EXPECT_TRUE(docids.empty());
}

// RQ-05: a character-class pattern matches only the numeric "123" term.
TEST(SniiRegexpQueryTest, NumericTermMatch) {
    MemoryFile file;
    reader::SniiSegmentReader segment_reader;
    reader::LogicalIndexReader index_reader;
    assert_ok(build_reader(&file, &segment_reader, &index_reader));

    std::vector<uint32_t> docids;
    assert_ok(regexp_query(index_reader, "[0-9]+", &docids));
    const std::vector<uint32_t> expected {42};
    EXPECT_EQ(docids, expected);
}

// RQ-06: backreferences are unsupported by RE2 (but accepted by std::regex);
// re.ok() is false so the call returns InvalidArgument without throwing.
TEST(SniiRegexpQueryTest, InvalidPatternReturnsInvalidArgument) {
    MemoryFile file;
    reader::SniiSegmentReader segment_reader;
    reader::LogicalIndexReader index_reader;
    assert_ok(build_reader(&file, &segment_reader, &index_reader));

    std::vector<uint32_t> docids;
    const Status status = regexp_query(index_reader, "(a)\\1", &docids);
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.is<doris::ErrorCode::INVALID_ARGUMENT>()) << status.to_string();
}

// RQ-07: a syntactically invalid pattern also returns InvalidArgument.
TEST(SniiRegexpQueryTest, UnbalancedParenReturnsInvalidArgument) {
    MemoryFile file;
    reader::SniiSegmentReader segment_reader;
    reader::LogicalIndexReader index_reader;
    assert_ok(build_reader(&file, &segment_reader, &index_reader));

    std::vector<uint32_t> docids;
    const Status status = regexp_query(index_reader, "(order", &docids);
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.is<doris::ErrorCode::INVALID_ARGUMENT>()) << status.to_string();
}

// RQ-08: null output / null sink return InvalidArgument (no crash, no throw).
TEST(SniiRegexpQueryTest, NullOutputReturnsInvalidArgument) {
    MemoryFile file;
    reader::SniiSegmentReader segment_reader;
    reader::LogicalIndexReader index_reader;
    assert_ok(build_reader(&file, &segment_reader, &index_reader));

    std::vector<uint32_t>* const null_docids = nullptr;
    EXPECT_TRUE(regexp_query(index_reader, "order", null_docids)
                        .is<doris::ErrorCode::INVALID_ARGUMENT>());

    DocIdSink* const null_sink = nullptr;
    EXPECT_TRUE(regexp_query(index_reader, "order", null_sink)
                        .is<doris::ErrorCode::INVALID_ARGUMENT>());
}

// RQ-09: max_expansions caps the number of expanded terms. Terms enumerate in
// sorted order, so the first non-bigram term "123" is the only expansion.
TEST(SniiRegexpQueryTest, MaxExpansionsCapsExpansion) {
    MemoryFile file;
    reader::SniiSegmentReader segment_reader;
    reader::LogicalIndexReader index_reader;
    assert_ok(build_reader(&file, &segment_reader, &index_reader));

    std::vector<uint32_t> docids;
    assert_ok(regexp_query(index_reader, ".*", &docids, /*max_expansions=*/1));
    const std::vector<uint32_t> expected {42};
    EXPECT_EQ(docids, expected);
}

// RQ-02/RQ-10: hidden phrase-bigram terms must never leak. ".*failed.*order.*"
// would FullMatch the bigram term text "<marker>failedorder" if it were visible,
// but the bigram filter hides it, so the result is empty.
TEST(SniiRegexpQueryTest, DoesNotExposeHiddenBigramTerms) {
    MemoryFile file;
    reader::SniiSegmentReader segment_reader;
    reader::LogicalIndexReader index_reader;
    assert_ok(build_reader(&file, &segment_reader, &index_reader,
                           /*include_phrase_bigrams=*/true));

    std::vector<uint32_t> bigram_leak;
    assert_ok(regexp_query(index_reader, ".*failed.*order.*", &bigram_leak));
    EXPECT_TRUE(bigram_leak.empty());

    // A match-all pattern returns only real-term docids (bigram terms filtered).
    std::vector<uint32_t> match_all;
    assert_ok(regexp_query(index_reader, ".*", &match_all));
    EXPECT_EQ(match_all, all_docids_0_to(9000));
}

// RQ-11: the RE2 path reproduces the std::regex_match golden result set exactly.
TEST(SniiRegexpQueryTest, EquivalenceMatchesStdRegexGolden) {
    MemoryFile file;
    reader::SniiSegmentReader segment_reader;
    reader::LogicalIndexReader index_reader;
    assert_ok(build_reader(&file, &segment_reader, &index_reader));

    struct GoldenCase {
        std::string_view pattern;
        std::vector<uint32_t> expected;
    };
    const std::vector<GoldenCase> cases {
            {.pattern = "order", .expected = all_docids_0_to(9000)},
            {.pattern = "ord(er|inal)", .expected = all_docids_0_to(9000)},
            {.pattern = "zzz.*", .expected = {}},
            {.pattern = "[0-9]+", .expected = {42}},
            {.pattern = "(123|needle)", .expected = {42, 100, 101, 102, 6000}},
    };
    for (const GoldenCase& c : cases) {
        std::vector<uint32_t> docids;
        assert_ok(regexp_query(index_reader, c.pattern, &docids));
        EXPECT_EQ(docids, c.expected) << "pattern=" << c.pattern;
    }
}

// Deterministic perf (golden prefix): RE2::PossibleMatchRange tightens the
// enumeration prefix for left-anchored patterns whose literal scan stops early.
TEST(SniiRegexpQueryTest, AnchoredPrefixIsTightened) {
    auto prefix_of = [](std::string_view pattern) -> std::string {
        re2::RE2::Options options;
        options.set_log_errors(false);
        const re2::RE2 re(re2::StringPiece(pattern.data(), pattern.size()), options);
        EXPECT_TRUE(re.ok()) << pattern;
        return internal::regex_enum_prefix(pattern, re);
    };

    // Tightened beyond the naive literal scan (which would yield "").
    EXPECT_EQ(prefix_of("^(order)"), "order");
    EXPECT_EQ(prefix_of("^(order|ordinal)"), "ord");
    EXPECT_EQ(prefix_of("^ord[ei]"), "ord");
    // Anchored literal already maximal.
    EXPECT_EQ(prefix_of("^order"), "order");
    // Non-anchored patterns fall back to the conservative literal scan.
    EXPECT_EQ(prefix_of("ord.*"), "ord");
    EXPECT_EQ(prefix_of(".*failed.*order.*"), "");
    EXPECT_EQ(prefix_of("[0-9]+"), "");
}

// Deterministic perf (op-count): the tightened "^(order)" prefix reaches a single
// dictionary term, so the matcher is invoked once instead of once per term.
TEST(SniiRegexpQueryTest, AnchoredPrefixEnumeratesSingleTerm) {
    MemoryFile file;
    reader::SniiSegmentReader segment_reader;
    reader::LogicalIndexReader index_reader;
    assert_ok(build_reader(&file, &segment_reader, &index_reader));

    constexpr std::string_view kPattern = "^(order)";
    re2::RE2::Options options;
    options.set_log_errors(false);
    const re2::RE2 re(re2::StringPiece(kPattern.data(), kPattern.size()), options);
    ASSERT_TRUE(re.ok());

    const std::string enum_prefix = internal::regex_enum_prefix(kPattern, re);
    EXPECT_EQ(enum_prefix, "order");

    auto count_matcher = [&](std::string_view prefix) {
        int calls = 0;
        std::vector<uint32_t> docids;
        VectorDocIdSink sink(docids);
        assert_ok(internal::emit_expanded_docid_union(
                index_reader, prefix,
                [&](std::string_view term) {
                    ++calls;
                    return re2::RE2::FullMatch(re2::StringPiece(term.data(), term.size()), re);
                },
                &sink));
        return std::pair<int, std::vector<uint32_t>> {calls, std::move(docids)};
    };

    // Tightened prefix enumerates only "order" -> exactly one matcher call.
    auto [tight_calls, tight_docids] = count_matcher(enum_prefix);
    EXPECT_EQ(tight_calls, 1);

    // The baseline empty prefix (old behavior) scans all 11 dictionary terms.
    auto [full_calls, full_docids] = count_matcher("");
    EXPECT_EQ(full_calls, 11);

    // Narrowing is a pure optimization: identical result either way.
    EXPECT_EQ(tight_docids, all_docids_0_to(9000));
    EXPECT_EQ(full_docids, all_docids_0_to(9000));
}

TEST(SniiPhraseQueryTest, MultiTailPhrasePrefixFiltersTailPrxByExpectedDocs) {
    MemoryFile file;
    reader::SniiSegmentReader segment_reader;
    reader::LogicalIndexReader index_reader;
    assert_ok(build_reader(&file, &segment_reader, &index_reader));

    std::vector<reader::LogicalIndexReader::PrefixHit> tail_hits;
    assert_ok(index_reader.prefix_terms("ord", &tail_hits, 10));
    ASSERT_EQ(tail_hits.size(), 2);

    struct PrxRange {
        uint64_t offset = 0;
        uint64_t len = 0;
    };
    std::vector<PrxRange> full_tail_prx_ranges;
    for (const auto& hit : tail_hits) {
        full_tail_prx_ranges.push_back({index_reader.section_refs().posting_region.offset +
                                                hit.prx_base + hit.entry.prx_off_delta,
                                        hit.entry.prx_len});
    }

    file.clear_reads();
    std::vector<uint32_t> docids;
    assert_ok(phrase_prefix_query(index_reader, {"needle", "ord"}, &docids, 10));

    const std::vector<uint32_t> expected {6000};
    EXPECT_EQ(docids, expected);
    for (const PrxRange& prx : full_tail_prx_ranges) {
        const bool full_tail_prx_read = std::ranges::any_of(file.reads(), [&](const auto& read) {
            return read.offset == prx.offset && read.len == prx.len;
        });
        EXPECT_FALSE(full_tail_prx_read);
    }
}

// ---------------------------------------------------------------------------
// T24: phrase-prefix micro-opt (sparsest anchor + expected_docids hoist).
//
// build_reader is shared across many SNII suites, so instead of mutating it we
// build small, self-contained kDocsPositions indexes here. Each scenario uses a
// distinct tail prefix so prefix_terms() expansion stays isolated per test, and
// the deterministic op-counters in query_test_counters.h are read directly.
// ---------------------------------------------------------------------------
Status build_positions_reader(MemoryFile* file, reader::SniiSegmentReader* segment_reader,
                              reader::LogicalIndexReader* index_reader,
                              std::vector<writer::TermPostings> terms, uint32_t doc_count) {
    writer::SniiIndexInput input;
    input.index_id = 21;
    input.index_suffix = "Body";
    input.config = format::IndexConfig::kDocsPositions;
    input.doc_count = doc_count;
    input.terms = std::move(terms);
    std::ranges::sort(input.terms,
                      [](const writer::TermPostings& lhs, const writer::TermPostings& rhs) {
                          return lhs.term < rhs.term;
                      });

    writer::SniiCompoundWriter writer(file);
    RETURN_IF_ERROR(writer.add_logical_index(input));
    RETURN_IF_ERROR(writer.finish());
    EXPECT_TRUE(file->finalized());

    RETURN_IF_ERROR(reader::SniiSegmentReader::open(file, segment_reader));
    return segment_reader->open_index(input.index_id, input.index_suffix, index_reader);
}

// "lead mid axt*": the leading exact term is high-frequency (5 positions/doc) while
// the middle exact term is a single position/doc -> "mid" is the sparsest exact
// term and becomes the anchor. doc400 is a valid "lead mid" candidate (expected
// tail@8) with NO tail term, so it must be filtered out of the final result.
// NOLINTBEGIN(modernize-use-designated-initializers): positional aggregate init of test posting data
std::vector<writer::TermPostings> anchor_scenario_terms() {
    return {make_term("lead", {{100, {0, 3, 6, 9, 12}},
                               {200, {0, 3, 6, 9, 12}},
                               {300, {0, 3, 6, 9, 12}},
                               {400, {0, 3, 6, 9, 12}}}),
            make_term("mid", {{100, {1}}, {200, {7}}, {300, {7}}, {400, {7}}}),
            make_term("axta", {{100, {2}}}),  // doc100: lead@0, mid@1 -> tail@2
            make_term("axtb", {{200, {8}}}),  // doc200: lead@6, mid@7 -> tail@8
            make_term("axtc", {{300, {8}}})}; // doc300: lead@6, mid@7 -> tail@8
}

// "dlead dmid dxt*": dlead is a single position/doc (sparsest), dmid is
// high-frequency. The anchor therefore stays at phrase position 0 (dlead), which
// is exactly the old span[0] behavior -- the degenerate no-change case.
std::vector<writer::TermPostings> leading_sparse_scenario_terms() {
    return {make_term("dlead", {{500, {3}}, {600, {3}}}),
            make_term("dmid", {{500, {0, 2, 4, 6, 8}}, {600, {0, 2, 4, 6, 8}}}),
            make_term("dxta", {{500, {5}}}), // dlead@3, dmid@4 -> tail@5
            make_term("dxtb", {{600, {5}}})};
}

// "ulead umid uxt*": umid (single position) is the anchor. In doc800 umid sits at
// position 0, which is < its phrase offset (1), so a general anchor would underflow
// `start`; the underflow guard skips it and doc800 is correctly excluded.
std::vector<writer::TermPostings> anchor_underflow_scenario_terms() {
    return {make_term("ulead", {{800, {5, 9}}, {810, {5, 9}}, {820, {5, 9}}}),
            make_term("umid", {{800, {0}}, {810, {6}}, {820, {6}}}),
            make_term("uxta", {{810, {7}}}), // ulead@5, umid@6 -> tail@7
            make_term("uxtb", {{820, {7}}})};
}
// NOLINTEND(modernize-use-designated-initializers)

// FUNC-1: the new sparsest-anchor path produces the correct result set. The anchor
// (mid) is not phrase-position 0, exercising the general anchor formula + underflow
// guard; doc400 (candidate, no tail term) is filtered out.
TEST(SniiPhraseQueryTest, MultiTermPhrasePrefixAnchorsOnSparsestTerm) {
    MemoryFile file;
    reader::SniiSegmentReader segment_reader;
    reader::LogicalIndexReader index_reader;
    assert_ok(build_positions_reader(&file, &segment_reader, &index_reader, anchor_scenario_terms(),
                                     /*doc_count=*/500));

    std::vector<uint32_t> docids;
    assert_ok(phrase_prefix_query(index_reader, {"lead", "mid", "axt"}, &docids, 10));

    const std::vector<uint32_t> expected {100, 200, 300};
    EXPECT_EQ(docids, expected);
}

// Perf (deterministic): the outer anchor enumeration size == docs x min_span. With
// mid as the anchor (1 position/doc) over 4 candidate docs the count is 4, strictly
// below the old span[0] baseline of Sum|lead| == 4 x 5 == 20.
TEST(SniiPhraseQueryTest, MultiTermPhrasePrefixAnchorIterationsMinimal) {
    MemoryFile file;
    reader::SniiSegmentReader segment_reader;
    reader::LogicalIndexReader index_reader;
    assert_ok(build_positions_reader(&file, &segment_reader, &index_reader, anchor_scenario_terms(),
                                     /*doc_count=*/500));

    internal::query_test_counters() = internal::QueryTestCounters {};
    std::vector<uint32_t> docids;
    assert_ok(phrase_prefix_query(index_reader, {"lead", "mid", "axt"}, &docids, 10));

    constexpr uint64_t kCandidateDocs = 4;                     // {100,200,300,400}
    constexpr uint64_t kMinSpan = 1;                           // mid has one position per doc
    constexpr uint64_t kOldLeadSpanTotal = kCandidateDocs * 5; // Sum|span[0]| (lead@5pos)
    EXPECT_EQ(internal::query_test_counters().anchor_iterations, kCandidateDocs * kMinSpan);
    EXPECT_LT(internal::query_test_counters().anchor_iterations, kOldLeadSpanTotal);
}

// FUNC-3: when the leading exact term is already the sparsest, the anchor stays at
// phrase-position 0, so anchor_iterations == Sum|span[0]| exactly (no regression /
// no change vs. the old hardcoded anchor). Result is still correct.
TEST(SniiPhraseQueryTest, MultiTermPhrasePrefixLeadingTermSparsestIsDegenerate) {
    MemoryFile file;
    reader::SniiSegmentReader segment_reader;
    reader::LogicalIndexReader index_reader;
    assert_ok(build_positions_reader(&file, &segment_reader, &index_reader,
                                     leading_sparse_scenario_terms(), /*doc_count=*/700));

    internal::query_test_counters() = internal::QueryTestCounters {};
    std::vector<uint32_t> docids;
    assert_ok(phrase_prefix_query(index_reader, {"dlead", "dmid", "dxt"}, &docids, 10));

    const std::vector<uint32_t> expected {500, 600};
    EXPECT_EQ(docids, expected);
    // dlead has one position/doc over 2 docs -> Sum|span[0]| == 2, unchanged.
    EXPECT_EQ(internal::query_test_counters().anchor_iterations, 2U);
}

// FUNC-4: a doc whose anchor position is smaller than the anchor's phrase offset
// (umid@0, offset 1) is skipped by the underflow guard and never false-matches; the
// two well-formed docs still match via distinct tails.
TEST(SniiPhraseQueryTest, MultiTermPhrasePrefixSkipsAnchorUnderflowDoc) {
    MemoryFile file;
    reader::SniiSegmentReader segment_reader;
    reader::LogicalIndexReader index_reader;
    assert_ok(build_positions_reader(&file, &segment_reader, &index_reader,
                                     anchor_underflow_scenario_terms(), /*doc_count=*/900));

    internal::query_test_counters() = internal::QueryTestCounters {};
    std::vector<uint32_t> docids;
    assert_ok(phrase_prefix_query(index_reader, {"ulead", "umid", "uxt"}, &docids, 10));

    const std::vector<uint32_t> expected {810, 820}; // doc800 excluded (underflow)
    EXPECT_EQ(docids, expected);
    // 3 candidates {800,810,820}, umid is the single-position anchor -> 3 x 1.
    EXPECT_EQ(internal::query_test_counters().anchor_iterations, 3U);
}

// Perf (deterministic): the multi-tail branch materializes expected_docids exactly
// once per query (hoisted out of the per-tail loop). Here prefix "axt" expands to 3
// tails; the old per-tail rebuild would have counted 3.
TEST(SniiPhraseQueryTest, MultiTailPhrasePrefixBuildsExpectedDocidsOnce) {
    MemoryFile file;
    reader::SniiSegmentReader segment_reader;
    reader::LogicalIndexReader index_reader;
    assert_ok(build_positions_reader(&file, &segment_reader, &index_reader, anchor_scenario_terms(),
                                     /*doc_count=*/500));

    std::vector<reader::LogicalIndexReader::PrefixHit> tail_hits;
    assert_ok(index_reader.prefix_terms("axt", &tail_hits, 10));
    ASSERT_EQ(tail_hits.size(), 3); // axta, axtb, axtc -> multi-tail branch

    internal::query_test_counters() = internal::QueryTestCounters {};
    std::vector<uint32_t> docids;
    assert_ok(phrase_prefix_query(index_reader, {"lead", "mid", "axt"}, &docids, 10));

    const std::vector<uint32_t> expected {100, 200, 300};
    EXPECT_EQ(docids, expected);
    EXPECT_EQ(internal::query_test_counters().expected_docids_build, 1U);
}

// FUNC-6: a single tail expansion takes the streaming ExecuteResolvedPhraseTerms
// path, never the multi-tail branch, so expected_docids_build stays 0. Result is
// unchanged from SingleTailPhrasePrefixUsesStreamingPhrasePath.
TEST(SniiPhraseQueryTest, SingleTailPhrasePrefixDoesNotBuildExpectedDocids) {
    MemoryFile file;
    reader::SniiSegmentReader segment_reader;
    reader::LogicalIndexReader index_reader;
    assert_ok(build_reader(&file, &segment_reader, &index_reader));

    internal::query_test_counters() = internal::QueryTestCounters {};
    std::vector<uint32_t> docids;
    assert_ok(phrase_prefix_query(index_reader, {"failed", "orde"}, &docids, 10));

    const std::vector<uint32_t> expected {5000, 7000, 8000};
    EXPECT_EQ(docids, expected);
    EXPECT_EQ(internal::query_test_counters().expected_docids_build, 0U);
}

// FUNC-5: an empty tail expansion returns OK with an empty result before the
// multi-tail branch, so no expected_docids vector is built.
TEST(SniiPhraseQueryTest, MultiTermPhrasePrefixEmptyTailExpansionReturnsEmpty) {
    MemoryFile file;
    reader::SniiSegmentReader segment_reader;
    reader::LogicalIndexReader index_reader;
    assert_ok(build_positions_reader(&file, &segment_reader, &index_reader, anchor_scenario_terms(),
                                     /*doc_count=*/500));

    internal::query_test_counters() = internal::QueryTestCounters {};
    std::vector<uint32_t> docids;
    assert_ok(phrase_prefix_query(index_reader, {"lead", "mid", "zzz"}, &docids, 10));

    EXPECT_TRUE(docids.empty());
    EXPECT_EQ(internal::query_test_counters().expected_docids_build, 0U);
}

// FUNC-7: a null output pointer returns InvalidArgument (no crash, no throw).
TEST(SniiPhraseQueryTest, PhrasePrefixQueryNullOutReturnsInvalidArgument) {
    MemoryFile file;
    reader::SniiSegmentReader segment_reader;
    reader::LogicalIndexReader index_reader;
    assert_ok(build_positions_reader(&file, &segment_reader, &index_reader, anchor_scenario_terms(),
                                     /*doc_count=*/500));

    std::vector<uint32_t>* const null_docids = nullptr;
    EXPECT_TRUE(phrase_prefix_query(index_reader, {"lead", "mid", "axt"}, null_docids, 10)
                        .is<doris::ErrorCode::INVALID_ARGUMENT>());
}

// FUNC-8: hidden phrase-bigram terms never leak through the multi-tail prefix path;
// result matches the non-bigram layout.
TEST(SniiPhraseQueryTest, MultiTailPhrasePrefixWithHiddenBigramsDoesNotLeak) {
    MemoryFile file;
    reader::SniiSegmentReader segment_reader;
    reader::LogicalIndexReader index_reader;
    assert_ok(build_reader(&file, &segment_reader, &index_reader, /*include_phrase_bigrams=*/true));

    std::vector<uint32_t> docids;
    assert_ok(phrase_prefix_query(index_reader, {"failed", "ord"}, &docids, 10));

    const std::vector<uint32_t> expected {5000, 6000, 7000, 8000};
    EXPECT_EQ(docids, expected);
}

// FUNC-2: byte-for-byte result equivalence on the existing fixture across the three
// canonical phrase-prefix shapes (multi-tail single-exact, single-tail, multi-tail
// single-doc). Locks the sparsest-anchor + hoist changes as pure optimizations.
TEST(SniiPhraseQueryTest, MultiTailPhrasePrefixEquivalenceRegression) {
    MemoryFile file;
    reader::SniiSegmentReader segment_reader;
    reader::LogicalIndexReader index_reader;
    assert_ok(build_reader(&file, &segment_reader, &index_reader));

    std::vector<uint32_t> failed_ord;
    assert_ok(phrase_prefix_query(index_reader, {"failed", "ord"}, &failed_ord, 10));
    EXPECT_EQ(failed_ord, (std::vector<uint32_t> {5000, 6000, 7000, 8000}));

    std::vector<uint32_t> failed_orde;
    assert_ok(phrase_prefix_query(index_reader, {"failed", "orde"}, &failed_orde, 10));
    EXPECT_EQ(failed_orde, (std::vector<uint32_t> {5000, 7000, 8000}));

    std::vector<uint32_t> needle_ord;
    assert_ok(phrase_prefix_query(index_reader, {"needle", "ord"}, &needle_ord, 10));
    EXPECT_EQ(needle_ord, (std::vector<uint32_t> {6000}));
}

// Perf (deterministic, corroborating): the CPU-only anchor reorder + expected_docids
// hoist do not change which bytes are fetched (spans/candidates are materialized
// before anchor selection). Two independently-built identical readers therefore
// issue an identical, non-zero number of physical reads for the same query.
TEST(SniiPhraseQueryTest, MultiTermPhrasePrefixDoesNotRegressReadCount) {
    MemoryFile file_a;
    reader::SniiSegmentReader segment_a;
    reader::LogicalIndexReader index_a;
    assert_ok(build_positions_reader(&file_a, &segment_a, &index_a, anchor_scenario_terms(),
                                     /*doc_count=*/500));
    file_a.clear_reads();
    std::vector<uint32_t> docids_a;
    assert_ok(phrase_prefix_query(index_a, {"lead", "mid", "axt"}, &docids_a, 10));
    const size_t reads_a = file_a.reads().size();

    MemoryFile file_b;
    reader::SniiSegmentReader segment_b;
    reader::LogicalIndexReader index_b;
    assert_ok(build_positions_reader(&file_b, &segment_b, &index_b, anchor_scenario_terms(),
                                     /*doc_count=*/500));
    file_b.clear_reads();
    std::vector<uint32_t> docids_b;
    assert_ok(phrase_prefix_query(index_b, {"lead", "mid", "axt"}, &docids_b, 10));
    const size_t reads_b = file_b.reads().size();

    EXPECT_EQ(docids_a, docids_b);
    // T24 is a CPU-only change (sparsest-term anchor + expected_docids hoist), so the
    // query's physical IO must be unchanged. The small anchor-scenario index is fully
    // resident, so the read count is a deterministic constant across identical readers.
    EXPECT_EQ(reads_a, reads_b);
}

TEST(SniiPhraseQueryTest, MultiTermPhraseUsesPairPrefilter) {
    MemoryFile file;
    reader::SniiSegmentReader segment_reader;
    reader::LogicalIndexReader index_reader;
    assert_ok(build_reader(&file, &segment_reader, &index_reader));

    std::vector<uint32_t> docids;
    assert_ok(phrase_query(index_reader, {"failed", "order", "ordinal"}, &docids));

    const std::vector<uint32_t> expected {5000, 7000};
    EXPECT_EQ(docids, expected);
}

TEST(SniiPhraseQueryTest, RepeatedTermPhraseUsesCachedPostingSpan) {
    MemoryFile file;
    reader::SniiSegmentReader segment_reader;
    reader::LogicalIndexReader index_reader;
    assert_ok(build_reader(&file, &segment_reader, &index_reader));

    std::vector<uint32_t> docids;
    assert_ok(phrase_query(index_reader, {"repeat", "repeat", "repeat"}, &docids));

    std::vector<uint32_t> expected(9000);
    std::iota(expected.begin(), expected.end(), 0);
    EXPECT_EQ(docids, expected);
}

TEST(SniiPhraseQueryTest, DenseTermWithMissingDocKeepsCandidateOrdinals) {
    MemoryFile file;
    reader::SniiSegmentReader segment_reader;
    reader::LogicalIndexReader index_reader;
    assert_ok(build_reader(&file, &segment_reader, &index_reader));

    std::vector<uint32_t> driver_docids;
    assert_ok(term_query(index_reader, "driver", &driver_docids));
    EXPECT_EQ(driver_docids.size(), 8000);

    std::vector<uint32_t> almost_docids;
    assert_ok(term_query(index_reader, "almost", &almost_docids));
    EXPECT_EQ(almost_docids.size(), 8999);
    ASSERT_GT(almost_docids.size(), 6144);
    EXPECT_EQ(almost_docids[3999], 3999);
    EXPECT_EQ(almost_docids[4000], 4001);
    EXPECT_EQ(almost_docids[6143], 6144);
    EXPECT_EQ(almost_docids[6144], 6145);

    std::vector<uint32_t> docids;
    assert_ok(phrase_query(index_reader, {"driver", "almost"}, &docids));

    std::vector<uint32_t> expected;
    expected.reserve(7999);
    for (uint32_t docid = 0; docid < 8000; ++docid) {
        if (docid != 4000) {
            expected.push_back(docid);
        }
    }
    EXPECT_EQ(docids, expected);
}

TEST(SniiPhraseQueryTest, SparseWindowBitsetKeepsCandidateOrdinals) {
    MemoryFile file;
    reader::SniiSegmentReader segment_reader;
    reader::LogicalIndexReader index_reader;
    assert_ok(build_reader(&file, &segment_reader, &index_reader));

    std::vector<uint32_t> docids;
    assert_ok(phrase_query(index_reader, {"sparse_left", "sparse_right"}, &docids));

    std::vector<uint32_t> expected;
    for (uint32_t docid = 0; docid < 9000; ++docid) {
        if (docid % 3 == 0 && docid % 4 != 1) {
            expected.push_back(docid);
        }
    }
    EXPECT_EQ(docids, expected);
}

TEST(SniiTermQueryTest, WindowedDenseTermEmitsRangesToSink) {
    MemoryFile file;
    reader::SniiSegmentReader segment_reader;
    reader::LogicalIndexReader index_reader;
    assert_ok(build_reader(&file, &segment_reader, &index_reader));

    RecordingDocIdSink sink;
    assert_ok(term_query(index_reader, "failed", &sink));

    std::vector<uint32_t> expected(9000);
    std::iota(expected.begin(), expected.end(), 0);
    EXPECT_EQ(sink.out, expected);
    EXPECT_GT(sink.range_calls, 0);
}

TEST(SniiPrxPodTest, SelectivePforCsrMatchesFullCsrAcrossRuns) {
    std::vector<uint32_t> freqs;
    std::vector<uint32_t> positions;
    freqs.reserve(320);
    for (uint32_t doc = 0; doc < 320; ++doc) {
        const uint32_t freq = (doc % 5 == 0) ? 2 : 1;
        freqs.push_back(freq);
        positions.push_back(doc * 3);
        if (freq == 2) {
            positions.push_back(doc * 3 + 2);
        }
    }

    ByteSink sink;
    assert_ok(format::build_prx_window_flat(positions, freqs, -1, &sink));

    std::vector<uint32_t> full_positions;
    std::vector<uint32_t> full_offsets;
    ByteSource full_source(sink.view());
    assert_ok(format::read_prx_window_csr(&full_source, &full_positions, &full_offsets));

    auto assert_selected_matches_full = [&](const std::vector<uint32_t>& selected_docs) {
        std::vector<uint32_t> selected_positions;
        std::vector<uint32_t> selected_offsets;
        ByteSource selected_source(sink.view());
        assert_ok(format::read_prx_window_csr_selective(&selected_source, selected_docs,
                                                        &selected_positions, &selected_offsets));

        ASSERT_EQ(selected_offsets.size(), selected_docs.size() + 1);
        for (size_t i = 0; i < selected_docs.size(); ++i) {
            const uint32_t doc = selected_docs[i];
            const std::vector<uint32_t> expected(full_positions.begin() + full_offsets[doc],
                                                 full_positions.begin() + full_offsets[doc + 1]);
            const std::vector<uint32_t> actual(
                    selected_positions.begin() + selected_offsets[i],
                    selected_positions.begin() + selected_offsets[i + 1]);
            EXPECT_EQ(actual, expected);
        }
    };

    assert_selected_matches_full({0, 1, 2});
    assert_selected_matches_full({0, 1, 127, 128, 129, 255, 256, 319});
}

TEST(SniiPrxPodTest, SelectivePforCsrHandlesDocsSpanningPforRuns) {
    const std::vector<uint32_t> freqs {300, 1, 260, 2, 1};
    std::vector<uint32_t> positions;
    positions.reserve(564);
    auto append_doc = [&](uint32_t count, uint32_t base, uint32_t seed) {
        uint32_t pos = base;
        uint32_t state = seed;
        for (uint32_t i = 0; i < count; ++i) {
            state = state * 1664525 + 1013904223;
            pos += 1 + (state & 0xFFFF);
            positions.push_back(pos);
        }
    };
    append_doc(freqs[0], 3, 11);
    append_doc(freqs[1], 11, 13);
    append_doc(freqs[2], 19, 17);
    append_doc(freqs[3], 29, 19);
    append_doc(freqs[4], 37, 23);

    ByteSink sink;
    assert_ok(format::build_prx_window_flat(positions, freqs, -1, &sink));
    ASSERT_FALSE(sink.buffer().empty());
    ASSERT_EQ(sink.buffer().front(), static_cast<uint8_t>(format::PrxCodec::kPfor));

    std::vector<uint32_t> full_positions;
    std::vector<uint32_t> full_offsets;
    ByteSource full_source(sink.view());
    assert_ok(format::read_prx_window_csr(&full_source, &full_positions, &full_offsets));

    const std::vector<uint32_t> selected_docs {0, 2, 3};
    std::vector<uint32_t> selected_positions;
    std::vector<uint32_t> selected_offsets;
    ByteSource selected_source(sink.view());
    assert_ok(format::read_prx_window_csr_selective(&selected_source, selected_docs,
                                                    &selected_positions, &selected_offsets));

    ASSERT_EQ(selected_offsets.size(), selected_docs.size() + 1);
    for (size_t i = 0; i < selected_docs.size(); ++i) {
        const uint32_t doc = selected_docs[i];
        const std::vector<uint32_t> expected(full_positions.begin() + full_offsets[doc],
                                             full_positions.begin() + full_offsets[doc + 1]);
        const std::vector<uint32_t> actual(selected_positions.begin() + selected_offsets[i],
                                           selected_positions.begin() + selected_offsets[i + 1]);
        EXPECT_EQ(actual, expected);
    }
}

TEST(SniiPrxPodTest, AutoCodecUsesZstdWhenItIsSmaller) {
    std::vector<uint32_t> freqs(1024, 1);
    std::vector<uint32_t> positions(1024, 7);

    ByteSink sink;
    assert_ok(format::build_prx_window_flat(positions, freqs, -1, &sink));

    ASSERT_FALSE(sink.buffer().empty());
    EXPECT_EQ(sink.buffer().front(), static_cast<uint8_t>(format::PrxCodec::kZstd));

    std::vector<uint32_t> decoded_positions;
    std::vector<uint32_t> decoded_offsets;
    ByteSource source(sink.view());
    assert_ok(format::read_prx_window_csr(&source, &decoded_positions, &decoded_offsets));

    ASSERT_EQ(decoded_offsets.size(), freqs.size() + 1);
    ASSERT_EQ(decoded_positions.size(), positions.size());
    for (size_t i = 0; i < freqs.size(); ++i) {
        EXPECT_EQ(decoded_offsets[i], i);
        EXPECT_EQ(decoded_positions[i], 7);
    }
    EXPECT_EQ(decoded_offsets.back(), positions.size());

    const std::vector<uint32_t> selected_docs {0, 17, 511, 1023};
    assert_selective_prx_matches_constant_positions(sink.view(), selected_docs, 7);
}

TEST(SniiPrxPodTest, AutoCodecKeepsPforForTinyWindows) {
    const std::vector<uint32_t> freqs {1, 2};
    const std::vector<uint32_t> positions {3, 5, 8};

    ByteSink sink;
    assert_ok(format::build_prx_window_flat(positions, freqs, -1, &sink));

    ASSERT_FALSE(sink.buffer().empty());
    EXPECT_EQ(sink.buffer().front(), static_cast<uint8_t>(format::PrxCodec::kPfor));
}

TEST(SniiPforTest, LowBitWidthFastPathsRoundTrip) {
    auto assert_round_trip = [](const std::vector<uint32_t>& values, uint8_t expected_width) {
        ByteSink sink;
        doris::snii::pfor_encode(values.data(), values.size(), &sink);
        ASSERT_FALSE(sink.buffer().empty());
        EXPECT_EQ(sink.buffer().front(), expected_width);

        std::vector<uint32_t> decoded(values.size(), 0xFFFFFFFF);
        ByteSource source(sink.view());
        assert_ok(doris::snii::pfor_decode(&source, values.size(), decoded.data()));
        EXPECT_TRUE(source.eof());
        EXPECT_EQ(decoded, values);
    };

    std::vector<uint32_t> one_bit(128);
    for (size_t i = 0; i < one_bit.size(); ++i) {
        one_bit[i] = static_cast<uint32_t>(i & 1);
    }
    assert_round_trip(one_bit, 1);

    one_bit[17] = 1000;
    assert_round_trip(one_bit, 1);

    std::vector<uint32_t> two_bit(128);
    for (size_t i = 0; i < two_bit.size(); ++i) {
        two_bit[i] = static_cast<uint32_t>(i & 3);
    }
    assert_round_trip(two_bit, 2);

    std::vector<uint32_t> three_bit(131);
    for (size_t i = 0; i < three_bit.size(); ++i) {
        three_bit[i] = static_cast<uint32_t>(i & 7);
    }
    assert_round_trip(three_bit, 3);

    std::vector<uint32_t> four_bit(128);
    for (size_t i = 0; i < four_bit.size(); ++i) {
        four_bit[i] = static_cast<uint32_t>(i & 15);
    }
    assert_round_trip(four_bit, 4);

    std::vector<uint32_t> five_bit(129);
    for (size_t i = 0; i < five_bit.size(); ++i) {
        five_bit[i] = static_cast<uint32_t>(i & 31);
    }
    assert_round_trip(five_bit, 5);

    std::vector<uint32_t> six_bit(130);
    for (size_t i = 0; i < six_bit.size(); ++i) {
        six_bit[i] = static_cast<uint32_t>(i & 63);
    }
    assert_round_trip(six_bit, 6);

    std::vector<uint32_t> seven_bit(131);
    for (size_t i = 0; i < seven_bit.size(); ++i) {
        seven_bit[i] = static_cast<uint32_t>(i & 127);
    }
    assert_round_trip(seven_bit, 7);

    std::vector<uint32_t> eight_bit(256);
    for (size_t i = 0; i < eight_bit.size(); ++i) {
        eight_bit[i] = static_cast<uint32_t>(i);
    }
    assert_round_trip(eight_bit, 8);
}

// ===========================================================================
// T04 -- DICT block request-scoped cache (MRU) + resident single-range read.
// ===========================================================================

namespace t04 {

std::string many_term_key(uint32_t i) {
    return "term_" + std::to_string(1000000 + i);
}

// Builds an index of `n_terms` tiny docs-only terms with a small target block
// size so the dictionary spans several DICT blocks, opened through `read_through`.
Status build_multi_block_reader(MemoryFile* file, doris::snii::io::FileReader* read_through,
                                reader::SniiSegmentReader* segment_reader,
                                reader::LogicalIndexReader* index_reader, uint32_t n_terms) {
    writer::SniiIndexInput input = make_many_term_input(21, "Body", n_terms);
    std::ranges::sort(input.terms,
                      [](const writer::TermPostings& lhs, const writer::TermPostings& rhs) {
                          return lhs.term < rhs.term;
                      });
    writer::SniiCompoundWriter writer(file);
    RETURN_IF_ERROR(writer.add_logical_index(input));
    RETURN_IF_ERROR(writer.finish());
    EXPECT_TRUE(file->finalized());
    RETURN_IF_ERROR(reader::SniiSegmentReader::open(read_through, segment_reader));
    return segment_reader->open_index(input.index_id, input.index_suffix, index_reader);
}

// Serializes read_at calls so the recording MemoryFile can be shared by the
// concurrency test without racing on its own bookkeeping. Test infra only -- the
// production FileReader (Doris IO / S3) is itself concurrent-read safe; this lock
// is NOT part of the reader under test and never wraps a decode.
class LockedFileReader final : public doris::snii::io::FileReader {
public:
    explicit LockedFileReader(doris::snii::io::FileReader* inner) : inner_(inner) {}
    Status read_at(uint64_t offset, size_t len, std::vector<uint8_t>* out) override {
        std::lock_guard<std::mutex> guard(mu_);
        return inner_->read_at(offset, len, out);
    }
    uint64_t size() const override { return inner_->size(); }

private:
    doris::snii::io::FileReader* inner_;
    std::mutex mu_;
};

// Captures everything lookup() returns so resident / on-demand / cached paths can
// be asserted byte-identical.
struct LookupResult {
    bool found = false;
    uint64_t frq_base = 0;
    uint64_t prx_base = 0;
    std::string term;
    uint64_t frq_off_delta = 0;
    uint64_t frq_len = 0;
    uint64_t df = 0;
    bool operator==(const LookupResult&) const = default;
};

LookupResult do_lookup(const reader::LogicalIndexReader& idx, std::string_view term,
                       reader::DictBlockCache* cache) {
    bool found = false;
    format::DictEntry entry;
    uint64_t frq_base = 0;
    uint64_t prx_base = 0;
    const Status st = idx.lookup(term, &found, &entry, &frq_base, &prx_base, cache);
    EXPECT_TRUE(st.ok()) << st.to_string();
    LookupResult r;
    r.found = found;
    if (found) {
        r.frq_base = frq_base;
        r.prx_base = prx_base;
        r.term = entry.term;
        r.frq_off_delta = entry.frq_off_delta;
        r.frq_len = entry.frq_len;
        r.df = static_cast<uint64_t>(entry.df);
    }
    return r;
}

size_t count_reads_in_region(const MemoryFile& file, const format::RegionRef& region,
                             bool* one_covers_region) {
    size_t count = 0;
    *one_covers_region = false;
    for (const MemoryFile::Read& r : file.reads()) {
        if (r.offset >= region.offset && r.offset < region.offset + region.length) {
            ++count;
            if (r.offset == region.offset && r.len == region.length) {
                *one_covers_region = true;
            }
        }
    }
    return count;
}

} // namespace t04

// F10: a resident dictionary that spans several blocks is loaded with ONE range
// read over dict_region (was one read_at per block -> up to ~4 serial S3 rounds).
TEST(SniiLogicalReaderTest, ResidentDictLoadIssuesSingleRangeRead) {
    ScopedEnv resident_dict("SNII_DICT_RESIDENT_MAX", "1048576");
    ScopedEnv resident_bsbf("SNII_BSBF_RESIDENT_MAX", "1048576");

    MemoryFile file;
    reader::SniiSegmentReader segment_reader;
    reader::LogicalIndexReader index_reader;
    assert_ok(t04::build_multi_block_reader(&file, &file, &segment_reader, &index_reader, 64));

    const format::RegionRef& dict_region = index_reader.section_refs().dict_region;
    ASSERT_GT(dict_region.length, 0U);
    bool one_covers_region = false;
    const size_t region_reads = t04::count_reads_in_region(file, dict_region, &one_covers_region);
    EXPECT_EQ(region_reads, 1U);
    EXPECT_TRUE(one_covers_region);
}

// F08/F20 + perf gate: forced on-demand, a block hit K times by repeated lookups
// AND a block shared by several distinct terms of one query each decode ONCE when
// a single request-scoped cache is threaded through -- dict_decode_counter() ==
// unique_blocks (1 here; build_reader's small dictionary is a single block).
TEST(SniiLogicalReaderTest, OnDemandLookupDecompressesBlockOncePerUniqueBlock) {
    ScopedEnv on_demand("SNII_DICT_RESIDENT_MAX", "0");

    MemoryFile file;
    reader::SniiSegmentReader segment_reader;
    reader::LogicalIndexReader index_reader;
    assert_ok(build_reader(&file, &segment_reader, &index_reader));

    doris::snii::testing::reset_dict_decode_counter();
    reader::DictBlockCache cache;
    for (int i = 0; i < 5; ++i) {
        const t04::LookupResult r = t04::do_lookup(index_reader, "failed", &cache);
        EXPECT_TRUE(r.found);
    }
    EXPECT_EQ(doris::snii::testing::dict_decode_counter(), 1U);
}

// The headline gate: a multi-term query whose terms fall in the same DICT block
// decodes it ONCE with a shared cache, and once-per-term without one.
TEST(SniiLogicalReaderTest, SharedDictBlockDecodesOncePerQuery) {
    ScopedEnv on_demand("SNII_DICT_RESIDENT_MAX", "0");

    MemoryFile file;
    reader::SniiSegmentReader segment_reader;
    reader::LogicalIndexReader index_reader;
    assert_ok(build_reader(&file, &segment_reader, &index_reader));

    const std::vector<std::string_view> terms = {"failed", "order", "driver"};

    // With one request-scoped cache: the shared block decodes once.
    doris::snii::testing::reset_dict_decode_counter();
    reader::DictBlockCache cache;
    for (std::string_view t : terms) {
        EXPECT_TRUE(t04::do_lookup(index_reader, t, &cache).found);
    }
    EXPECT_EQ(doris::snii::testing::dict_decode_counter(), 1U); // == unique_blocks

    // Baseline (no cache): each term re-decodes the same block.
    doris::snii::testing::reset_dict_decode_counter();
    for (std::string_view t : terms) {
        EXPECT_TRUE(t04::do_lookup(index_reader, t, nullptr).found);
    }
    EXPECT_EQ(doris::snii::testing::dict_decode_counter(), terms.size());
}

// New/old equivalence: the on-demand + cache path returns exactly what the
// resident path does -- lookups and full term_query docid sets are identical.
TEST(SniiLogicalReaderTest, OnDemandResultsMatchResidentBaseline) {
    const std::vector<std::string_view> present = {"failed", "order",  "ordinal",
                                                   "driver", "needle", "almost"};
    const std::string_view absent = "definitely_absent_term";

    std::vector<t04::LookupResult> resident_lookups;
    std::vector<std::vector<uint32_t>> resident_docids;
    {
        ScopedEnv resident_dict("SNII_DICT_RESIDENT_MAX", "1048576");
        MemoryFile file;
        reader::SniiSegmentReader segment_reader;
        reader::LogicalIndexReader index_reader;
        assert_ok(build_reader(&file, &segment_reader, &index_reader));
        for (std::string_view t : present) {
            resident_lookups.push_back(t04::do_lookup(index_reader, t, nullptr));
            std::vector<uint32_t> docids;
            assert_ok(term_query(index_reader, t, &docids));
            resident_docids.push_back(std::move(docids));
        }
        EXPECT_FALSE(t04::do_lookup(index_reader, absent, nullptr).found);
    }

    ScopedEnv on_demand("SNII_DICT_RESIDENT_MAX", "0");
    MemoryFile file;
    reader::SniiSegmentReader segment_reader;
    reader::LogicalIndexReader index_reader;
    assert_ok(build_reader(&file, &segment_reader, &index_reader));

    reader::DictBlockCache cache;
    for (size_t i = 0; i < present.size(); ++i) {
        // cached vs uncached on-demand both equal the resident baseline.
        EXPECT_EQ(t04::do_lookup(index_reader, present[i], &cache), resident_lookups[i]);
        EXPECT_EQ(t04::do_lookup(index_reader, present[i], nullptr), resident_lookups[i]);
        std::vector<uint32_t> docids;
        assert_ok(term_query(index_reader, present[i], &docids));
        EXPECT_EQ(docids, resident_docids[i]);
    }
    EXPECT_FALSE(t04::do_lookup(index_reader, absent, &cache).found);
}

// F-05: prefix enumeration across many on-demand blocks is correct and a second
// pass over the same cache adds no decodes (cross-call request-scoped reuse).
TEST(SniiLogicalReaderTest, PrefixEnumerationReusesCachedBlocks) {
    constexpr uint32_t kTerms = 64;

    // Resident baseline ordering.
    std::vector<std::string> resident_terms;
    {
        ScopedEnv resident_dict("SNII_DICT_RESIDENT_MAX", "1048576");
        MemoryFile file;
        reader::SniiSegmentReader seg;
        reader::LogicalIndexReader idx;
        assert_ok(t04::build_multi_block_reader(&file, &file, &seg, &idx, kTerms));
        std::vector<reader::LogicalIndexReader::PrefixHit> hits;
        assert_ok(idx.prefix_terms("term_", &hits, 0));
        for (auto& h : hits) {
            resident_terms.push_back(h.term);
        }
    }
    ASSERT_EQ(resident_terms.size(), kTerms);

    ScopedEnv on_demand("SNII_DICT_RESIDENT_MAX", "0");
    MemoryFile file;
    reader::SniiSegmentReader seg;
    reader::LogicalIndexReader idx;
    assert_ok(t04::build_multi_block_reader(&file, &file, &seg, &idx, kTerms));

    reader::DictBlockCache cache(/*max_entries=*/128);
    doris::snii::testing::reset_dict_decode_counter();

    std::vector<reader::LogicalIndexReader::PrefixHit> hits1;
    assert_ok(idx.prefix_terms("term_", &hits1, 0, &cache));
    const uint64_t blocks = doris::snii::testing::dict_decode_counter();
    EXPECT_GE(blocks, 2U); // genuinely multi-block (else the reuse gate is vacuous)

    std::vector<reader::LogicalIndexReader::PrefixHit> hits2;
    assert_ok(idx.prefix_terms("term_", &hits2, 0, &cache));
    EXPECT_EQ(doris::snii::testing::dict_decode_counter(), blocks); // second pass: no re-decode

    std::vector<std::string> got1;
    std::vector<std::string> got2;
    for (auto& h : hits1) {
        got1.push_back(h.term);
    }
    for (auto& h : hits2) {
        got2.push_back(h.term);
    }
    EXPECT_EQ(got1, resident_terms);
    EXPECT_EQ(got2, resident_terms);
}

// Request-scoped cache unit: MRU promotion, LRU eviction, a hard size bound, and
// pins that keep an evicted block alive -- no file IO involved.
TEST(SniiLogicalReaderTest, RequestCacheEvictsLruStaysBoundedAndPinsSurvive) {
    reader::DictBlockCache cache(/*max_entries=*/2);
    int loads = 0;
    auto loader_for = [&](uint8_t tag) {
        return [&loads, tag](std::shared_ptr<const reader::DecodedDictBlock>* out) -> Status {
            ++loads;
            auto block = std::make_shared<reader::DecodedDictBlock>();
            block->bytes.assign(4, tag);
            *out = block;
            return Status::OK();
        };
    };

    std::shared_ptr<const reader::DecodedDictBlock> pin0;
    assert_ok(cache.get_or_load(0, loader_for(0), &pin0));
    EXPECT_EQ(loads, 1);
    std::shared_ptr<const reader::DecodedDictBlock> tmp;
    assert_ok(cache.get_or_load(1, loader_for(1), &tmp)); // {1,0}
    EXPECT_EQ(loads, 2);
    assert_ok(cache.get_or_load(0, loader_for(0), &tmp)); // hit -> {0,1}
    EXPECT_EQ(loads, 2);
    assert_ok(cache.get_or_load(2, loader_for(2), &tmp)); // miss -> evict LRU(1) -> {2,0}
    EXPECT_EQ(loads, 3);
    EXPECT_EQ(cache.size(), 2U);

    assert_ok(cache.get_or_load(0, loader_for(0), &tmp)); // 0 still resident -> hit
    EXPECT_EQ(loads, 3);
    assert_ok(cache.get_or_load(1, loader_for(1), &tmp)); // 1 was evicted -> reload
    EXPECT_EQ(loads, 4);
    EXPECT_LE(cache.size(), cache.max_entries());

    // pin0 was taken before 0 ever cycled; even across evictions its bytes stay live.
    ASSERT_TRUE(pin0);
    ASSERT_EQ(pin0->bytes.size(), 4U);
    EXPECT_EQ(pin0->bytes[0], 0);
}

// F-06: with a 1-entry cache, alternating two different blocks forces reloads;
// a reloaded block still decodes correctly (Slice not corrupted by eviction).
TEST(SniiLogicalReaderTest, SmallCacheReloadsEvictedBlockCorrectly) {
    ScopedEnv on_demand("SNII_DICT_RESIDENT_MAX", "0");
    constexpr uint32_t kTerms = 64;

    MemoryFile file;
    reader::SniiSegmentReader seg;
    reader::LogicalIndexReader idx;
    assert_ok(t04::build_multi_block_reader(&file, &file, &seg, &idx, kTerms));

    const std::string first = t04::many_term_key(0);         // smallest -> block 0
    const std::string last = t04::many_term_key(kTerms - 1); // largest  -> last block

    reader::DictBlockCache cache(/*max_entries=*/1);
    doris::snii::testing::reset_dict_decode_counter();

    const t04::LookupResult first_a = t04::do_lookup(idx, first, &cache);
    const uint64_t after_first = doris::snii::testing::dict_decode_counter();
    EXPECT_TRUE(t04::do_lookup(idx, last, &cache).found);                 // evicts block 0
    const t04::LookupResult first_b = t04::do_lookup(idx, first, &cache); // reload block 0
    const uint64_t after_reload = doris::snii::testing::dict_decode_counter();

    EXPECT_TRUE(first_a.found);
    EXPECT_EQ(first_a, first_b);          // reload produced the identical entry
    EXPECT_GT(after_reload, after_first); // a reload actually happened (evicted)
    EXPECT_LE(cache.size(), cache.max_entries());
}

// Concurrency: N queries share the const reader, each with its OWN request-scoped
// cache. No shared mutable state is added to the reader, so every thread decodes
// the on-demand block once -> dict_decode_counter() == thread count, results are
// correct, and the run is TSAN-clean (BUILD_TYPE_UT=TSAN).
TEST(SniiLogicalReaderConcurrencyTest, ConcurrentQueriesUseIndependentRequestCaches) {
    ScopedEnv on_demand("SNII_DICT_RESIDENT_MAX", "0");

    MemoryFile file;
    reader::SniiSegmentReader seg0;
    reader::LogicalIndexReader idx0;
    assert_ok(build_reader(&file, &seg0, &idx0)); // populate `file`

    t04::LockedFileReader locked(&file);
    reader::SniiSegmentReader seg;
    assert_ok(reader::SniiSegmentReader::open(&locked, &seg));
    reader::LogicalIndexReader idx;
    assert_ok(seg.open_index(7, "Body", &idx));

    doris::snii::testing::reset_dict_decode_counter();
    constexpr int kThreads = 8;
    constexpr int kIters = 16;
    std::atomic<int> failures {0};
    auto worker = [&]() {
        reader::DictBlockCache cache; // request-scoped: one per query/thread
        for (int i = 0; i < kIters; ++i) {
            bool found = false;
            format::DictEntry entry;
            uint64_t frq_base = 0;
            uint64_t prx_base = 0;
            const Status st = idx.lookup("failed", &found, &entry, &frq_base, &prx_base, &cache);
            if (!st.ok() || !found) {
                failures.fetch_add(1, std::memory_order_relaxed);
            }
        }
    };

    std::vector<std::thread> threads;
    threads.reserve(kThreads);
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back(worker);
    }
    for (std::thread& t : threads) {
        t.join();
    }

    EXPECT_EQ(failures.load(), 0);
    // Each thread's cache decodes the shared block exactly once.
    EXPECT_EQ(doris::snii::testing::dict_decode_counter(), static_cast<uint64_t>(kThreads));
}

} // namespace
} // namespace doris::snii::query
