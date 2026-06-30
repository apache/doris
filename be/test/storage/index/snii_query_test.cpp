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
#include <memory>
#include <mutex>
#include <numeric>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "snii/common/slice.h"
#include "snii/encoding/byte_sink.h"
#include "snii/encoding/byte_source.h"
#include "snii/encoding/pfor.h"
#include "snii/format/format_constants.h"
#include "snii/format/phrase_bigram.h"
#include "snii/format/prx_pod.h"
#include "snii/format/tail_pointer.h"
#include "snii/io/file_reader.h"
#include "snii/io/file_writer.h"
#include "snii/query/docid_sink.h"
#include "snii/query/internal/regex_prefix.h"
#include "snii/query/internal/term_expansion.h"
#include "snii/query/phrase_query.h"
#include "snii/query/prefix_query.h"
#include "snii/query/regexp_query.h"
#include "snii/query/term_query.h"
#include "snii/query/wildcard_query.h"
#include "snii/reader/dict_block_cache.h"
#include "snii/reader/logical_index_reader.h"
#include "snii/reader/snii_segment_reader.h"
#include "snii/writer/snii_compound_writer.h"
#include "snii/writer/spimi_term_buffer.h"

namespace snii::query {
using doris::Status; // RETURN_IF_ERROR expands to bare Status
namespace {

class MemoryFile final : public snii::io::FileReader, public snii::io::FileWriter {
public:
    struct Read {
        uint64_t offset = 0;
        size_t len = 0;
    };

    Status append(Slice data) override {
        data_.insert(data_.end(), data.data(), data.data() + data.size());
        return Status::OK();
    }

    Status finalize() override {
        finalized_ = true;
        return Status::OK();
    }

    uint64_t bytes_written() const override { return data_.size(); }

    // NOLINTBEGIN(readability-non-const-parameter): FileReader interface writes into out.
    Status read_at(uint64_t offset, size_t len, std::vector<uint8_t>* out) override {
        if (offset > data_.size() || len > data_.size() - offset) {
            return Status::Corruption("memory file read past eof");
        }
        reads_.push_back({offset, len});
        read_bytes_ += len;
        out->resize(len);
        if (len != 0) {
            std::memcpy(out->data(), data_.data() + offset, len);
        }
        return Status::OK();
    }
    // NOLINTEND(readability-non-const-parameter)

    uint64_t size() const override { return data_.size(); }
    bool finalized() const { return finalized_; }
    const std::vector<Read>& reads() const { return reads_; }
    size_t read_bytes() const { return read_bytes_; }
    void clear_reads() {
        reads_.clear();
        read_bytes_ = 0;
    }

private:
    std::vector<uint8_t> data_;
    std::vector<Read> reads_;
    size_t read_bytes_ = 0;
    bool finalized_ = false;
};

class ScopedEnv {
public:
    ScopedEnv(const char* key, const char* value) : key_(key) {
        if (const char* old = std::getenv(key); old != nullptr) {
            old_value_ = old;
        }
        setenv(key, value, 1);
    }

    ~ScopedEnv() {
        if (old_value_.has_value()) {
            setenv(key_, old_value_->c_str(), 1);
        } else {
            unsetenv(key_);
        }
    }

private:
    const char* key_;
    std::optional<std::string> old_value_;
};

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

struct PostingDoc {
    uint32_t docid = 0;
    std::vector<uint32_t> positions;
};

struct PrxRange {
    uint64_t offset = 0;
    uint64_t len = 0;
};

writer::TermPostings make_term(std::string term, std::vector<PostingDoc> docs) {
    std::ranges::sort(docs, [](const PostingDoc& lhs, const PostingDoc& rhs) {
        return lhs.docid < rhs.docid;
    });

    writer::TermPostings posting;
    posting.term = std::move(term);
    posting.docids.reserve(docs.size());
    posting.freqs.reserve(docs.size());
    for (const PostingDoc& doc : docs) {
        posting.docids.push_back(doc.docid);
        posting.freqs.push_back(static_cast<uint32_t>(doc.positions.size()));
        posting.positions_flat.insert(posting.positions_flat.end(), doc.positions.begin(),
                                      doc.positions.end());
    }
    return posting;
}

std::vector<PostingDoc> docs_with_one_position(uint32_t begin, uint32_t end, uint32_t position) {
    std::vector<PostingDoc> docs;
    docs.reserve(end - begin);
    for (uint32_t docid = begin; docid < end; ++docid) {
        docs.push_back({docid, {position}});
    }
    return docs;
}

void assert_ok(const Status& status) {
    ASSERT_TRUE(status.ok()) << status.to_string();
}

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

Status build_reader(MemoryFile* file, reader::SniiSegmentReader* segment_reader,
                    reader::LogicalIndexReader* index_reader, bool include_phrase_bigrams = false) {
    constexpr uint32_t kDocCount = 9000;
    auto failed_docs = docs_with_one_position(0, kDocCount, 0);
    auto order_docs = docs_with_one_position(0, kDocCount, 2);
    auto ordinal_docs = docs_with_one_position(0, kDocCount, 2);
    auto driver_docs = docs_with_one_position(0, 8000, 0);
    auto almost_docs = docs_with_one_position(0, kDocCount, 1);
    std::vector<PostingDoc> needle_docs {{.docid = 100, .positions = {0}},
                                         {.docid = 101, .positions = {0}},
                                         {.docid = 102, .positions = {0}},
                                         {.docid = 6000, .positions = {0}}};
    std::vector<PostingDoc> numeric_tail_docs {{.docid = 42, .positions = {1}}};
    std::vector<PostingDoc> sparse_left_docs;
    std::vector<PostingDoc> sparse_right_docs;
    std::vector<PostingDoc> repeat_docs;
    std::vector<PostingDoc> trace_docs {{.docid = 42, .positions = {0}}};
    sparse_left_docs.reserve(kDocCount / 3 + 1);
    sparse_right_docs.reserve(kDocCount);
    repeat_docs.reserve(kDocCount);
    for (uint32_t docid = 0; docid < kDocCount; ++docid) {
        if (docid % 3 == 0) {
            sparse_left_docs.push_back({docid, {0}});
        }
        if (docid % 4 != 1) {
            sparse_right_docs.push_back({docid, {1}});
        }
        repeat_docs.push_back({docid, {0, 1, 2}});
    }
    almost_docs.erase(almost_docs.begin() + 4000);
    failed_docs[8000].positions = {0, 4};
    for (PostingDoc& doc : order_docs) {
        if (doc.docid == 5000 || doc.docid == 7000) {
            doc.positions = {1};
        } else if (doc.docid == 8000) {
            doc.positions = {5};
        }
    }
    for (PostingDoc& doc : ordinal_docs) {
        if (doc.docid == 6000) {
            doc.positions = {1};
        }
    }

    writer::SniiIndexInput input;
    input.index_id = 7;
    input.index_suffix = "Body";
    input.config = format::IndexConfig::kDocsPositions;
    input.doc_count = kDocCount;
    input.terms = {make_term("almost", std::move(almost_docs)),
                   make_term("123", std::move(numeric_tail_docs)),
                   make_term("driver", std::move(driver_docs)),
                   make_term("failed", std::move(failed_docs)),
                   make_term("needle", std::move(needle_docs)),
                   make_term("order", std::move(order_docs)),
                   make_term("ordinal", std::move(ordinal_docs)),
                   make_term("repeat", std::move(repeat_docs)),
                   make_term("sparse_left", std::move(sparse_left_docs)),
                   make_term("sparse_right", std::move(sparse_right_docs)),
                   make_term("trace", std::move(trace_docs))};
    if (include_phrase_bigrams) {
        input.terms.push_back(make_term(format::make_phrase_bigram_sentinel_term(),
                                        {{.docid = 0, .positions = {0}}}));
        input.terms.push_back(make_term(format::make_phrase_bigram_term("failed", "order"),
                                        {{.docid = 5000, .positions = {0}},
                                         {.docid = 7000, .positions = {0}},
                                         {.docid = 8000, .positions = {4}}}));
        input.terms.push_back(make_term(format::make_phrase_bigram_term("failed", "ordinal"),
                                        {{.docid = 6000, .positions = {0}}}));
    }
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

// A FileReader decorator that counts read_batch() invocations. Each BatchRangeFetcher
// fetch() barrier issues exactly one read_batch (== one batched/remote serial round),
// so this isolates the number of I/O rounds a query plan emits. Single read_at() calls
// (dict-block / BSBF loads) delegate straight through and are intentionally not counted.
class BatchRoundCountingReader final : public snii::io::FileReader {
public:
    explicit BatchRoundCountingReader(snii::io::FileReader* inner) : inner_(inner) {}

    Status read_at(uint64_t offset, size_t len, std::vector<uint8_t>* out) override {
        return inner_->read_at(offset, len, out);
    }
    Status read_batch(const std::vector<snii::io::Range>& ranges,
                      std::vector<std::vector<uint8_t>>* outs) override {
        ++batch_rounds_;
        return inner_->read_batch(ranges, outs);
    }
    uint64_t size() const override { return inner_->size(); }

    size_t batch_rounds() const { return batch_rounds_; }
    void reset_rounds() { batch_rounds_ = 0; }

private:
    snii::io::FileReader* inner_;
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
Status build_slim_pod_ref_phrase_reader(MemoryFile* file, snii::io::FileReader* read_through,
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

TEST(SniiSegmentReaderTest, NonResidentBsbfCachesHeaderAndProbesBodyBlock) {
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

    file.clear_reads();
    reader::LogicalIndexReader index_reader;
    assert_ok(segment_reader.open_index(input.index_id, input.index_suffix, &index_reader));
    bool header_read = false;
    for (const auto& read : file.reads()) {
        if (read.offset == refs.bsbf.offset && read.len == format::kBsbfHeaderSize) {
            header_read = true;
        }
    }
    EXPECT_TRUE(header_read);

    file.clear_reads();

    std::vector<uint32_t> docids;
    assert_ok(term_query(index_reader, "absent_term", &docids));
    EXPECT_TRUE(docids.empty());
    bool body_probe_read = false;
    for (const auto& read : file.reads()) {
        const uint64_t read_end = read.offset + read.len;
        const uint64_t bsbf_end = refs.bsbf.offset + refs.bsbf.length;
        EXPECT_FALSE(read.offset == refs.bsbf.offset && read.len == format::kBsbfHeaderSize);
        if (read.len == format::kBsbfBytesPerBlock &&
            refs.bsbf.offset + format::kBsbfHeaderSize <= read.offset && read_end <= bsbf_end) {
            body_probe_read = true;
        }
    }
    EXPECT_TRUE(body_probe_read);
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
        snii::pfor_encode(values.data(), values.size(), &sink);
        ASSERT_FALSE(sink.buffer().empty());
        EXPECT_EQ(sink.buffer().front(), expected_width);

        std::vector<uint32_t> decoded(values.size(), 0xFFFFFFFF);
        ByteSource source(sink.view());
        assert_ok(snii::pfor_decode(&source, values.size(), decoded.data()));
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
Status build_multi_block_reader(MemoryFile* file, snii::io::FileReader* read_through,
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
class LockedFileReader final : public snii::io::FileReader {
public:
    explicit LockedFileReader(snii::io::FileReader* inner) : inner_(inner) {}
    Status read_at(uint64_t offset, size_t len, std::vector<uint8_t>* out) override {
        std::lock_guard<std::mutex> guard(mu_);
        return inner_->read_at(offset, len, out);
    }
    uint64_t size() const override { return inner_->size(); }

private:
    snii::io::FileReader* inner_;
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

    snii::testing::reset_dict_decode_counter();
    reader::DictBlockCache cache;
    for (int i = 0; i < 5; ++i) {
        const t04::LookupResult r = t04::do_lookup(index_reader, "failed", &cache);
        EXPECT_TRUE(r.found);
    }
    EXPECT_EQ(snii::testing::dict_decode_counter(), 1U);
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
    snii::testing::reset_dict_decode_counter();
    reader::DictBlockCache cache;
    for (std::string_view t : terms) {
        EXPECT_TRUE(t04::do_lookup(index_reader, t, &cache).found);
    }
    EXPECT_EQ(snii::testing::dict_decode_counter(), 1U); // == unique_blocks

    // Baseline (no cache): each term re-decodes the same block.
    snii::testing::reset_dict_decode_counter();
    for (std::string_view t : terms) {
        EXPECT_TRUE(t04::do_lookup(index_reader, t, nullptr).found);
    }
    EXPECT_EQ(snii::testing::dict_decode_counter(), terms.size());
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
    snii::testing::reset_dict_decode_counter();

    std::vector<reader::LogicalIndexReader::PrefixHit> hits1;
    assert_ok(idx.prefix_terms("term_", &hits1, 0, &cache));
    const uint64_t blocks = snii::testing::dict_decode_counter();
    EXPECT_GE(blocks, 2U); // genuinely multi-block (else the reuse gate is vacuous)

    std::vector<reader::LogicalIndexReader::PrefixHit> hits2;
    assert_ok(idx.prefix_terms("term_", &hits2, 0, &cache));
    EXPECT_EQ(snii::testing::dict_decode_counter(), blocks); // second pass: no re-decode

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
    snii::testing::reset_dict_decode_counter();

    const t04::LookupResult first_a = t04::do_lookup(idx, first, &cache);
    const uint64_t after_first = snii::testing::dict_decode_counter();
    EXPECT_TRUE(t04::do_lookup(idx, last, &cache).found);                 // evicts block 0
    const t04::LookupResult first_b = t04::do_lookup(idx, first, &cache); // reload block 0
    const uint64_t after_reload = snii::testing::dict_decode_counter();

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

    snii::testing::reset_dict_decode_counter();
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
    EXPECT_EQ(snii::testing::dict_decode_counter(), static_cast<uint64_t>(kThreads));
}

} // namespace
} // namespace snii::query
