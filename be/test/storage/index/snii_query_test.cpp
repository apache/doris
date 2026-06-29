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
#include <cstdlib>
#include <cstring>
#include <numeric>
#include <optional>
#include <string>
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
#include "snii/query/phrase_query.h"
#include "snii/query/prefix_query.h"
#include "snii/query/regexp_query.h"
#include "snii/query/term_query.h"
#include "snii/query/wildcard_query.h"
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
    std::vector<PostingDoc> needle_docs {{100, {0}}, {101, {0}}, {102, {0}}, {6000, {0}}};
    std::vector<PostingDoc> numeric_tail_docs {{42, {1}}};
    std::vector<PostingDoc> sparse_left_docs;
    std::vector<PostingDoc> sparse_right_docs;
    std::vector<PostingDoc> repeat_docs;
    std::vector<PostingDoc> trace_docs {{42, {0}}};
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
        input.terms.push_back(make_term(format::make_phrase_bigram_sentinel_term(), {{0, {0}}}));
        input.terms.push_back(make_term(format::make_phrase_bigram_term("failed", "order"),
                                        {{5000, {0}}, {7000, {0}}, {8000, {4}}}));
        input.terms.push_back(
                make_term(format::make_phrase_bigram_term("failed", "ordinal"), {{6000, {0}}}));
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
        input.terms.push_back(make_term("term_" + std::to_string(1000000 + i), {{i, {0}}}));
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
        return PrxRange {
                index_reader.section_refs().posting_region.offset + prx_base + entry.prx_off_delta,
                entry.prx_len};
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

} // namespace
} // namespace snii::query
