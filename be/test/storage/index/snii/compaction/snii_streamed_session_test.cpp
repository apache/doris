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
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <numeric>
#include <span>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "common/status.h"
#include "storage/index/inverted/common_grams/common_grams_key_codec.h"
#include "storage/index/inverted/common_grams/common_grams_segment_metadata.h"
#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/format/dict_entry.h"
#include "storage/index/snii/format/format_constants.h"
#include "storage/index/snii/format/norms_pod.h"
#include "storage/index/snii/format/null_bitmap.h"
#include "storage/index/snii/format/prx_pod.h"
#include "storage/index/snii/io/file_writer.h"
#include "storage/index/snii/query/phrase_query.h"
#include "storage/index/snii/query/term_query.h"
#include "storage/index/snii/reader/logical_index_reader.h"
#include "storage/index/snii/reader/snii_segment_reader.h"
#include "storage/index/snii/writer/logical_index_writer.h"
#include "storage/index/snii/writer/memory_reporter.h"
#include "storage/index/snii/writer/posting_window_emitter.h"
#include "storage/index/snii/writer/snii_compound_writer.h"
#include "storage/index/snii/writer/spillable_byte_buffer.h"
#include "storage/index/snii/writer/spimi_term_buffer.h"
#include "storage/index/snii/writer/term_posting_source.h"
#include "storage/index/snii/writer/term_posting_test_utils.h"
#include "storage/index/snii_query_test_util.h"

namespace {

using namespace doris::snii;            // NOLINT
using namespace doris::snii::snii_test; // NOLINT
namespace ErrorCode = doris::ErrorCode;
using doris::Status;
using doris::snii::query::phrase_query;
using doris::snii::query::term_query;
using writer::SniiCompoundWriter;
using writer::SniiIndexInput;
using writer::SniiStreamedIndexSession;
using writer::SpanTermPostingSource;
using writer::SpimiTermBuffer;
using writer::SpillableByteBuffer;
using writer::TermPostings;
using writer::StreamedTermPostings;
using writer::TermPostingBuffer;
using writer::TermPostingSource;
namespace inverted_index = doris::segment_v2::inverted_index;

static_assert(!std::is_move_constructible_v<SniiCompoundWriter>);
static_assert(!std::is_move_assignable_v<SniiCompoundWriter>);
static_assert(!std::is_move_constructible_v<SniiStreamedIndexSession>);
static_assert(!std::is_move_assignable_v<SniiStreamedIndexSession>);
static_assert(!std::is_move_assignable_v<writer::TrackedNullDocids>);

// FileWriter fault used to prove that a failed append cannot be retried into a
// sealable container. The failing call writes one byte before returning an
// error, modeling append implementations that can have partial side effects.
class PartialFailOnAppendFile final : public io::FileWriter {
public:
    explicit PartialFailOnAppendFile(size_t fail_on_append) : fail_on_append_(fail_on_append) {}

    Status append(Slice data) override {
        ++append_calls_;
        if (append_calls_ == fail_on_append_) {
            if (!data.empty()) {
                RETURN_IF_ERROR(backing_.append(Slice(data.data(), 1)));
            }
            return Status::Error<ErrorCode::INTERNAL_ERROR, false>(
                    "injected partial append failure");
        }
        return backing_.append(data);
    }

    Status finalize() override {
        ++finalize_calls_;
        return backing_.finalize();
    }

    uint64_t bytes_written() const override { return backing_.bytes_written(); }

    size_t append_calls() const { return append_calls_; }
    size_t finalize_calls() const { return finalize_calls_; }
    bool finalized() const { return backing_.finalized(); }

private:
    MemoryFile backing_;
    size_t fail_on_append_ = 0;
    size_t append_calls_ = 0;
    size_t finalize_calls_ = 0;
};

SniiIndexInput empty_input(uint64_t index_id, std::string suffix, uint32_t doc_count = 8) {
    SniiIndexInput input;
    input.index_id = index_id;
    input.index_suffix = std::move(suffix);
    input.config = format::IndexConfig::kDocsPositions;
    input.doc_count = doc_count;
    input.write_freq = false;
    return input;
}

TermPostings make_sparse_slim_term(std::string term) {
    std::vector<PostingDoc> docs;
    docs.reserve(300);
    for (uint32_t i = 0; i < 300; ++i) {
        docs.push_back({.docid = i * 100003U, .positions = {0, 3, 9}});
    }
    return make_term(std::move(term), std::move(docs));
}

class VectorTermPostingSource final : public TermPostingSource {
public:
    explicit VectorTermPostingSource(const TermPostings* postings) : postings_(postings) {}

    Status fill(uint32_t target_docs, TermPostingBuffer* out, bool* exhausted) override {
        requests.push_back(target_docs);
        out_was_empty = out_was_empty && out->empty();
        const size_t remaining = postings_->docids.size() - doc_offset_;
        const size_t count = std::min<size_t>(target_docs, remaining);
        if (count == 0) {
            *exhausted = true;
            return Status::OK();
        }

        const auto docids =
                std::span<const uint32_t>(postings_->docids).subspan(doc_offset_, count);
        if (!postings_->retain_positions) {
            const auto freqs = postings_->freqs.empty()
                                       ? std::span<const uint32_t> {}
                                       : std::span<const uint32_t>(postings_->freqs)
                                                 .subspan(doc_offset_, count);
            RETURN_IF_ERROR(out->append(docids, freqs, {}));
        } else {
            const auto freqs =
                    std::span<const uint32_t>(postings_->freqs).subspan(doc_offset_, count);
            const size_t position_count =
                    std::accumulate(freqs.begin(), freqs.end(), static_cast<size_t>(0));
            const auto positions = std::span<const uint32_t>(postings_->positions_flat)
                                           .subspan(position_offset_, position_count);
            RETURN_IF_ERROR(out->append(docids, freqs, positions));
            position_offset_ += position_count;
        }
        doc_offset_ += count;
        *exhausted = doc_offset_ == postings_->docids.size();
        return Status::OK();
    }

    std::vector<uint32_t> requests;
    bool out_was_empty = true;

private:
    const TermPostings* postings_;
    size_t doc_offset_ = 0;
    size_t position_offset_ = 0;
};

class InvalidTermPostingSource final : public TermPostingSource {
public:
    enum class Mode {
        kEmptyBeforeEof,
        kShortBeforeEof,
        kOverfill,
        kUnordered,
        kOutOfRange,
        kError
    };

    explicit InvalidTermPostingSource(Mode mode) : mode_(mode) {}

    Status fill(uint32_t target_docs, TermPostingBuffer* out, bool* exhausted) override {
        switch (mode_) {
        case Mode::kEmptyBeforeEof:
            *exhausted = false;
            return Status::OK();
        case Mode::kShortBeforeEof: {
            const std::array<uint32_t, 1> docids {1};
            RETURN_IF_ERROR(out->append(docids, {}, {}));
            *exhausted = false;
            return Status::OK();
        }
        case Mode::kOverfill: {
            std::vector<uint32_t> docids(target_docs + 1);
            std::iota(docids.begin(), docids.end(), 0);
            RETURN_IF_ERROR(out->append(docids, {}, {}));
            *exhausted = true;
            return Status::OK();
        }
        case Mode::kUnordered: {
            const std::array<uint32_t, 2> docids {2, 1};
            RETURN_IF_ERROR(out->append(docids, {}, {}));
            *exhausted = true;
            return Status::OK();
        }
        case Mode::kOutOfRange: {
            const std::array<uint32_t, 1> docids {32};
            RETURN_IF_ERROR(out->append(docids, {}, {}));
            *exhausted = true;
            return Status::OK();
        }
        case Mode::kError:
            return Status::Error<ErrorCode::INTERNAL_ERROR, false>("injected source failure");
        }
        __builtin_unreachable();
    }

private:
    Mode mode_;
};

class InvalidPositionShapeSource final : public TermPostingSource {
public:
    explicit InvalidPositionShapeSource(size_t position_count) : position_count_(position_count) {}

    Status fill(uint32_t target_docs, TermPostingBuffer* out, bool* exhausted) override {
        if (target_docs == 0 || out == nullptr || exhausted == nullptr || !out->empty()) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                    "invalid position source: invalid fill arguments");
        }
        writer::MutableTermPostingSpan destination;
        RETURN_IF_ERROR(out->grow_uninitialized(/*document_count=*/1, /*has_freqs=*/true,
                                                position_count_, &destination));
        destination.docids[0] = 0;
        destination.freqs[0] = 2;
        std::iota(destination.positions_flat.begin(), destination.positions_flat.end(), 0);
        *exhausted = true;
        return Status::OK();
    }

private:
    size_t position_count_ = 0;
};

TermPostings make_uniform_term(std::string term, uint32_t doc_count) {
    TermPostings postings;
    postings.term = std::move(term);
    postings.docids.resize(doc_count);
    std::iota(postings.docids.begin(), postings.docids.end(), 0);
    postings.freqs.assign(doc_count, 1);
    postings.positions_flat.assign(doc_count, 0);
    return postings;
}

TermPostings make_adaptive_boundary_term(uint32_t tail_docs = 512) {
    constexpr uint32_t kPrefixDocs = format::kAdaptiveWindowDfThreshold;
    constexpr uint32_t kFarPosition = 1U << 28;
    const uint32_t doc_count = kPrefixDocs + tail_docs;
    TermPostings postings;
    postings.term = "adaptive-boundary";
    postings.docids.resize(doc_count);
    std::iota(postings.docids.begin(), postings.docids.end(), 0);
    postings.freqs.assign(doc_count, 1);
    postings.positions_flat.reserve(kPrefixDocs + 2 * tail_docs);
    for (uint32_t doc = 0; doc < doc_count; ++doc) {
        postings.positions_flat.push_back(0);
        if (doc >= kPrefixDocs) {
            postings.freqs[doc] = 2;
            postings.positions_flat.push_back(kFarPosition);
        }
    }
    return postings;
}

class FailAfterFirstFillSource final : public TermPostingSource {
public:
    explicit FailAfterFirstFillSource(const TermPostings* postings) : delegate_(postings) {}

    Status fill(uint32_t target_docs, TermPostingBuffer* out, bool* exhausted) override {
        if (fill_count_++ != 0) {
            return Status::Error<ErrorCode::INTERNAL_ERROR, false>(
                    "injected source failure after the first fill");
        }
        return delegate_.fill(target_docs, out, exhausted);
    }

private:
    VectorTermPostingSource delegate_;
    size_t fill_count_ = 0;
};

SniiIndexInput representative_input(bool write_freq = false) {
    SniiIndexInput input;
    input.index_id = 71;
    input.index_suffix = "body";
    input.config = format::IndexConfig::kDocsPositions;
    input.write_freq = write_freq;
    input.target_dict_block_bytes = 256;

    input.terms.push_back(make_term("aa_inline", {{.docid = 7, .positions = {1}}}));
    input.terms.push_back(make_sparse_slim_term("bb_slim"));

    std::vector<PostingDoc> wide_docs;
    wide_docs.reserve(600);
    for (uint32_t i = 0; i < 600; ++i) {
        wide_docs.push_back({.docid = i * 90001U + 1U, .positions = {2}});
    }
    const uint32_t max_docid = wide_docs.back().docid;
    input.terms.push_back(make_term("cc_windowed", std::move(wide_docs)));

    // Enough tiny vocabulary to cross multiple small DICT blocks.
    for (uint32_t i = 0; i < 24; ++i) {
        input.terms.push_back(make_term("zz_filler_" + std::to_string(1000 + i),
                                        {{.docid = i, .positions = {4}}}));
    }
    std::ranges::sort(input.terms, [](const TermPostings& lhs, const TermPostings& rhs) {
        return lhs.term < rhs.term;
    });

    input.doc_count = max_docid + 4;
    input.null_docids = {input.doc_count - 2, input.doc_count - 1};
    return input;
}

Status write_ordinary_index(SniiIndexInput input, MemoryFile* file) {
    SniiCompoundWriter compound(file);
    RETURN_IF_ERROR(compound.add_logical_index(input));
    return compound.finish();
}

Status push_materialized(SniiStreamedIndexSession* session, TermPostings postings) {
    SpanTermPostingSource source(postings.docids, postings.freqs, postings.positions_flat);
    return session->push_term(StreamedTermPostings {.term = std::move(postings.term),
                                                    .retain_positions = postings.retain_positions,
                                                    .source = &source});
}

Status write_streamed_index(SniiIndexInput input, MemoryFile* file) {
    std::vector<TermPostings> terms = std::move(input.terms);
    input.terms.clear();

    SniiCompoundWriter compound(file);
    SniiStreamedIndexSession* session = nullptr;
    RETURN_IF_ERROR(compound.begin_streamed_index(std::move(input), &session));
    for (TermPostings& term : terms) {
        RETURN_IF_ERROR(push_materialized(session, std::move(term)));
    }
    RETURN_IF_ERROR(session->finish());
    return compound.finish();
}

Status write_source_index(SniiIndexInput input, const TermPostings* postings, MemoryFile* file) {
    input.terms.clear();
    VectorTermPostingSource source(postings);
    SniiCompoundWriter compound(file);
    SniiStreamedIndexSession* session = nullptr;
    RETURN_IF_ERROR(compound.begin_streamed_index(std::move(input), &session));
    RETURN_IF_ERROR(session->push_term(StreamedTermPostings {
            .term = postings->term,
            .retain_positions = postings->retain_positions,
            .source = &source,
    }));
    RETURN_IF_ERROR(session->finish());
    return compound.finish();
}

Status begin_scoring_session_from_local_input(SniiCompoundWriter* compound,
                                              SniiStreamedIndexSession** session) {
    SniiIndexInput input;
    input.index_id = 91;
    input.index_suffix = "owned_scoring_body_with_heap_storage";
    input.config = format::IndexConfig::kDocsPositionsScoring;
    input.doc_count = 4;
    input.null_docids = {1, 3};
    input.encoded_norms = {7, 11, 13, 17};
    input.common_grams_metadata = make_plain_scoring_metadata(input.doc_count, 4);
    return compound->begin_streamed_index(std::move(input), session);
}

SniiIndexInput common_grams_input(uint64_t index_id, std::string suffix) {
    SniiIndexInput input;
    input.index_id = index_id;
    input.index_suffix = std::move(suffix);
    input.config = format::IndexConfig::kDocsPositionsScoring;
    input.doc_count = 1;
    input.encoded_norms = {9};
    inverted_index::CommonGramsQueryIdentity identity {.common_grams_dictionary_identity = "dict-a",
                                                       .base_analyzer_fingerprint = "base-a",
                                                       .common_grams_fingerprint = "grams-a"};
    input.common_grams_metadata = inverted_index::make_common_grams_segment_metadata(identity);
    input.common_grams_metadata->scoring_doc_count = input.doc_count;
    return input;
}

TEST(SniiStreamedWriterSessionTest, OrdinaryAndStreamedImagesAreByteIdentical) {
    for (bool write_freq : {false, true}) {
        MemoryFile ordinary;
        MemoryFile streamed;
        assert_ok(write_ordinary_index(representative_input(write_freq), &ordinary));
        assert_ok(write_streamed_index(representative_input(write_freq), &streamed));

        EXPECT_EQ(streamed.data(), ordinary.data());

        reader::SniiSegmentReader segment;
        reader::LogicalIndexReader index;
        assert_ok(reader::SniiSegmentReader::open(&streamed, &segment));
        assert_ok(segment.open_index(71, "body", &index));
        EXPECT_GT(index.n_dict_blocks(), 1U);
        EXPECT_EQ(index.stats().null_count, 2U);

        bool found = false;
        format::DictEntry entry;
        uint64_t frq_base = 0;
        uint64_t prx_base = 0;
        assert_ok(index.lookup("aa_inline", &found, &entry, &frq_base, &prx_base));
        ASSERT_TRUE(found);
        EXPECT_EQ(entry.kind, format::DictEntryKind::kInline);
        EXPECT_EQ(entry.term_stats_present, write_freq);

        assert_ok(index.lookup("bb_slim", &found, &entry, &frq_base, &prx_base));
        ASSERT_TRUE(found);
        EXPECT_EQ(entry.kind, format::DictEntryKind::kPodRef);
        EXPECT_EQ(entry.enc, format::DictEntryEnc::kSlim);
        EXPECT_EQ(entry.term_stats_present, write_freq);

        assert_ok(index.lookup("cc_windowed", &found, &entry, &frq_base, &prx_base));
        ASSERT_TRUE(found);
        EXPECT_EQ(entry.kind, format::DictEntryKind::kPodRef);
        EXPECT_EQ(entry.enc, format::DictEntryEnc::kWindowed);
        EXPECT_EQ(entry.term_stats_present, write_freq);
    }
}

TEST(SniiStreamedWriterSessionTest, CompleteCommonGramSkipsTermFreqStatsScan) {
    SniiIndexInput input = common_grams_input(113, "gram_freq_stats");
    input.terms.push_back(make_term("plain", {{.docid = 0, .positions = {1}}}));
    auto gram = inverted_index::encode_common_gram("of", "the");
    ASSERT_TRUE(gram.has_value());
    input.terms.push_back(make_term(std::move(gram.value()), {{.docid = 0, .positions = {0, 2}}}));
    std::ranges::sort(input.terms, [](const TermPostings& lhs, const TermPostings& rhs) {
        return lhs.term < rhs.term;
    });

    writer::testing::reset_term_freq_scans();
    MemoryFile file;
    std::vector<TermPostings> terms = std::move(input.terms);
    SniiCompoundWriter compound(&file);
    SniiStreamedIndexSession* session = nullptr;
    assert_ok(compound.begin_streamed_index(std::move(input), &session));
    assert_ok(session->set_semantic_token_count(1));
    for (TermPostings& term : terms) {
        assert_ok(push_materialized(session, std::move(term)));
    }
    assert_ok(session->finish());
    assert_ok(compound.finish());

    // The ordinary term still needs ttf/max_freq, while the statless gram can
    // derive ttf from its already-known position count and has no max consumer.
    EXPECT_EQ(writer::testing::term_freq_scans(), 1U);
}

TEST(SniiStreamedWriterSessionTest, CompleteWindowedCommonGramSkipsUnusedWandScans) {
    constexpr uint32_t kDocs = format::kSlimDfThreshold;
    SniiIndexInput input = common_grams_input(117, "gram_window_norm_stats");
    input.doc_count = kDocs;
    input.encoded_norms.resize(kDocs);
    std::iota(input.encoded_norms.begin(), input.encoded_norms.end(), uint8_t {1});
    input.common_grams_metadata->scoring_doc_count = kDocs;

    std::vector<PostingDoc> docs;
    docs.reserve(kDocs);
    for (uint32_t docid = 0; docid < kDocs; ++docid) {
        docs.push_back({.docid = docid, .positions = {docid}});
    }
    auto gram = inverted_index::encode_common_gram("of", "the");
    ASSERT_TRUE(gram.has_value());
    TermPostings term = make_term(std::move(gram.value()), docs);

    writer::testing::reset_window_norm_doc_visits();
    writer::testing::reset_window_freq_doc_visits();
    MemoryFile file;
    SniiCompoundWriter compound(&file);
    SniiStreamedIndexSession* session = nullptr;
    assert_ok(compound.begin_streamed_index(std::move(input), &session));
    assert_ok(session->set_semantic_token_count(kDocs));
    assert_ok(push_materialized(session, std::move(term)));
    assert_ok(session->finish());
    assert_ok(compound.finish());

    EXPECT_EQ(writer::testing::window_norm_doc_visits(), 0U);
    EXPECT_EQ(writer::testing::window_freq_doc_visits(), 0U);
}

TEST(SniiStreamedWriterSessionTest, CompleteDocsOnlyGramsOmitPrxInEveryEntryShape) {
    constexpr uint32_t kDocs = format::kSlimDfThreshold;
    SniiIndexInput input = common_grams_input(118, "gram_docs_only_shapes");
    input.config = format::IndexConfig::kDocsPositions;
    input.write_freq = false;
    input.encoded_norms.clear();
    input.common_grams_metadata->scoring_coverage = inverted_index::ScoringCoverage::kNone;
    input.common_grams_metadata->scoring_stats_version = 0;
    input.common_grams_metadata->norm_semantics_version = 0;
    input.common_grams_metadata->scoring_doc_count = 0;

    const auto make_docs_only_gram = [](std::string left, std::string right, uint32_t count,
                                        uint32_t stride) {
        TermPostings term;
        term.term = inverted_index::encode_common_gram(left, right).value();
        term.retain_positions = false;
        term.docids.resize(count);
        for (uint32_t i = 0; i < count; ++i) {
            term.docids[i] = i * stride;
        }
        term.freqs.assign(count, 1U);
        return term;
    };
    input.terms.push_back(make_docs_only_gram("a", "of", 1, 1));
    input.terms.push_back(make_docs_only_gram("b", "of", 300, 100003));
    input.terms.push_back(make_docs_only_gram("c", "of", kDocs, 1));
    input.doc_count = input.terms[1].docids.back() + 1;
    std::ranges::sort(input.terms, [](const TermPostings& lhs, const TermPostings& rhs) {
        return lhs.term < rhs.term;
    });

    writer::testing::reset_term_freq_scans();
    writer::testing::reset_window_norm_doc_visits();
    writer::testing::reset_window_freq_doc_visits();
    MemoryFile file;
    SniiCompoundWriter compound(&file);
    assert_ok(compound.add_logical_index(input));
    assert_ok(compound.finish());

    reader::SniiSegmentReader segment;
    reader::LogicalIndexReader index;
    assert_ok(reader::SniiSegmentReader::open(&file, &segment));
    assert_ok(segment.open_index(118, "gram_docs_only_shapes", &index));
    for (size_t i = 0; i < input.terms.size(); ++i) {
        bool found = false;
        format::DictEntry entry;
        uint64_t frq_base = 0;
        uint64_t prx_base = 0;
        assert_ok(index.lookup(input.terms[i].term, &found, &entry, &frq_base, &prx_base));
        ASSERT_TRUE(found);
        if (i == 0) {
            EXPECT_EQ(entry.kind, format::DictEntryKind::kInline);
            EXPECT_TRUE(entry.prx_bytes.empty());
        } else {
            EXPECT_EQ(entry.kind, format::DictEntryKind::kPodRef);
            EXPECT_EQ(entry.prx_len, 0U);
        }
    }
    EXPECT_EQ(writer::testing::term_freq_scans(), 0U);
    EXPECT_EQ(writer::testing::window_norm_doc_visits(), 0U);
    EXPECT_EQ(writer::testing::window_freq_doc_visits(), 0U);
}

TEST(SniiStreamedWriterSessionTest, MixedCommonGramKeepsTermFreqStatsScan) {
    SniiIndexInput input = common_grams_input(114, "mixed_gram_freq_stats");
    input.common_grams_metadata->common_grams_coverage =
            inverted_index::CommonGramsCoverage::kMixed;
    auto gram = inverted_index::encode_common_gram("of", "the");
    ASSERT_TRUE(gram.has_value());
    TermPostings term = make_term(std::move(gram.value()), {{.docid = 0, .positions = {0, 2}}});

    writer::testing::reset_term_freq_scans();
    MemoryFile file;
    SniiCompoundWriter compound(&file);
    SniiStreamedIndexSession* session = nullptr;
    assert_ok(compound.begin_streamed_index(std::move(input), &session));
    assert_ok(session->set_semantic_token_count(1));
    assert_ok(push_materialized(session, std::move(term)));
    assert_ok(session->finish());
    assert_ok(compound.finish());

    EXPECT_EQ(writer::testing::term_freq_scans(), 1U);
}

TEST(SniiStreamedWriterSessionTest, StatlessCommonGramRejectsInvalidPositionPartition) {
    SniiIndexInput input = common_grams_input(115, "invalid_gram_partition");
    auto gram = inverted_index::encode_common_gram("of", "the");
    ASSERT_TRUE(gram.has_value());
    TermPostings term = make_term(std::move(gram.value()), {{.docid = 0, .positions = {0, 2}}});
    term.freqs[0] = 1;

    MemoryFile file;
    SniiCompoundWriter compound(&file);
    SniiStreamedIndexSession* session = nullptr;
    assert_ok(compound.begin_streamed_index(std::move(input), &session));
    assert_ok(session->set_semantic_token_count(1));
    EXPECT_TRUE(push_materialized(session, std::move(term)).is<ErrorCode::INVALID_ARGUMENT>());
}

TEST(SniiStreamedWriterSessionTest, StatlessWindowedGramRejectsInvalidPositionPartition) {
    constexpr uint32_t kDocs = format::kSlimDfThreshold;
    SniiIndexInput input = common_grams_input(116, "invalid_windowed_gram_partition");
    input.doc_count = kDocs;
    input.encoded_norms.assign(kDocs, 1);
    input.common_grams_metadata->scoring_doc_count = kDocs;
    std::vector<PostingDoc> docs;
    docs.reserve(kDocs);
    for (uint32_t docid = 0; docid < kDocs; ++docid) {
        docs.push_back({.docid = docid, .positions = {0}});
    }
    auto gram = inverted_index::encode_common_gram("of", "the");
    ASSERT_TRUE(gram.has_value());
    TermPostings term = make_term(std::move(gram.value()), std::move(docs));
    term.freqs.back() = 2;

    MemoryFile file;
    SniiCompoundWriter compound(&file);
    SniiStreamedIndexSession* session = nullptr;
    assert_ok(compound.begin_streamed_index(std::move(input), &session));
    assert_ok(session->set_semantic_token_count(kDocs));
    EXPECT_TRUE(push_materialized(session, std::move(term)).is<ErrorCode::INVALID_ARGUMENT>());
}

TEST(SniiStreamedWriterSessionTest, NullDocidsStorageIsTransferredIntoStreamedWriter) {
    SniiIndexInput input = empty_input(94, "null_handoff", 4);
    input.null_docids = {1, 3};
    const uint32_t* const original_storage = input.null_docids.data();

    MemoryFile file;
    SniiCompoundWriter compound(&file);
    SniiStreamedIndexSession* session = nullptr;
    assert_ok(compound.begin_streamed_index(std::move(input), &session));
    ASSERT_NE(session, nullptr);
    EXPECT_EQ(session->writer_->null_docids_.data(), original_storage);
    assert_ok(session->finish());
    assert_ok(compound.finish());
}

TEST(SniiStreamedWriterSessionTest, NullBitmapFinalizationHonorsReporterCap) {
    SniiIndexInput input = empty_input(95, "null_memory_cap", 2);
    input.null_docids = {1};
    const uint64_t retained_null_bytes = input.null_docids.capacity() * sizeof(uint32_t);
    const uint64_t bitmap_build_bytes = format::NullBitmapWriter::build_memory_upper_bound(
            std::span<const uint32_t>(input.null_docids));
    writer::MemoryReporter reporter(nullptr, retained_null_bytes + bitmap_build_bytes - 1);
    input.mem_reporter = &reporter;

    {
        MemoryFile file;
        SniiCompoundWriter compound(&file);
        SniiStreamedIndexSession* session = nullptr;
        assert_ok(compound.begin_streamed_index(std::move(input), &session));
        ASSERT_NE(session, nullptr);
        const Status status = session->finish();
        EXPECT_TRUE(status.is<ErrorCode::MEM_LIMIT_EXCEEDED>()) << status;
        EXPECT_EQ(reporter.current_bytes(), retained_null_bytes);
    }
    EXPECT_EQ(reporter.current_bytes(), 0);
}

TEST(SniiStreamedWriterSessionTest, PublicOverloadChargesNullDocidCapacity) {
    SniiIndexInput input = empty_input(95, "null_public_capacity", 2);
    input.null_docids.reserve(8);
    input.null_docids.push_back(1);
    const uint64_t retained_null_bytes = input.null_docids.capacity() * sizeof(uint32_t);
    writer::MemoryReporter reporter(nullptr, retained_null_bytes - 1);
    input.mem_reporter = &reporter;

    MemoryFile file;
    SniiCompoundWriter compound(&file);
    SniiStreamedIndexSession* session = nullptr;
    const Status status = compound.begin_streamed_index(std::move(input), &session);
    EXPECT_TRUE(status.is<ErrorCode::MEM_LIMIT_EXCEEDED>()) << status;
    EXPECT_EQ(session, nullptr);
    EXPECT_EQ(reporter.current_bytes(), 0);
}

TEST(SniiStreamedWriterSessionTest, NullBitmapMemoryIsReleasedAfterCompoundAppend) {
    SniiIndexInput input = empty_input(96, "null_memory_release", 2);
    input.null_docids = {1};
    writer::MemoryReporter reporter(nullptr, 1U << 20);
    input.mem_reporter = &reporter;

    MemoryFile file;
    SniiCompoundWriter compound(&file);
    SniiStreamedIndexSession* session = nullptr;
    assert_ok(compound.begin_streamed_index(std::move(input), &session));
    ASSERT_NE(session, nullptr);
    assert_ok(session->finish());
    EXPECT_GT(reporter.current_bytes(), 0);
    assert_ok(compound.finish());
    EXPECT_EQ(reporter.current_bytes(), 0);
}

TEST(SniiStreamedWriterSessionTest, EmptyTrackedNullCapacityIsReleasedAtFinalize) {
    std::vector<uint32_t> null_docids;
    null_docids.reserve(4);
    writer::MemoryReporter reporter(nullptr, null_docids.capacity() * sizeof(uint32_t));
    auto reservation = reporter.make_reservation();
    assert_ok(reservation.set_bytes(null_docids.capacity() * sizeof(uint32_t)));
    writer::TrackedNullDocids tracked(std::move(reservation), std::move(null_docids));

    SniiIndexInput input = empty_input(97, "empty_null_capacity", 0);
    input.mem_reporter = &reporter;
    MemoryFile file;
    SniiCompoundWriter compound(&file);
    SniiStreamedIndexSession* session = nullptr;
    assert_ok(compound.begin_streamed_index(std::move(input), std::move(tracked), &session));
    ASSERT_NE(session, nullptr);
    assert_ok(session->finish());
    EXPECT_EQ(reporter.current_bytes(), 0);
    assert_ok(compound.finish());
}

TEST(SniiStreamedWriterSessionTest, TightPrxLimitsRecutSlimTermAtDocumentBoundaries) {
    SniiIndexInput input;
    input.index_id = 72;
    input.index_suffix = "tight_prx";
    input.config = format::IndexConfig::kDocsPositions;
    input.write_freq = true;
    input.doc_count = 3;
    input.prx_window_limits = {
            .max_docs = 8,
            .max_positions = 5,
            .max_uncomp_bytes = 64,
    };
    input.terms.push_back(make_term("dense", {{.docid = 0, .positions = {0, 1, 2}},
                                              {.docid = 1, .positions = {0, 1, 2}},
                                              {.docid = 2, .positions = {0, 1, 2}}}));
    input.terms.push_back(make_term("tail", {{.docid = 0, .positions = {3}},
                                             {.docid = 1, .positions = {3}},
                                             {.docid = 2, .positions = {3}}}));

    MemoryFile file;
    assert_ok(write_streamed_index(std::move(input), &file));

    reader::SniiSegmentReader segment;
    reader::LogicalIndexReader index;
    assert_ok(reader::SniiSegmentReader::open(&file, &segment));
    assert_ok(segment.open_index(72, "tight_prx", &index));
    bool found = false;
    format::DictEntry entry;
    uint64_t frq_base = 0;
    uint64_t prx_base = 0;
    assert_ok(index.lookup("dense", &found, &entry, &frq_base, &prx_base));
    ASSERT_TRUE(found);
    EXPECT_EQ(entry.kind, format::DictEntryKind::kPodRef);
    EXPECT_EQ(entry.enc, format::DictEntryEnc::kWindowed);

    std::vector<uint32_t> docs;
    assert_ok(phrase_query(index, {"dense", "tail"}, &docs));
    EXPECT_EQ(docs, (std::vector<uint32_t> {0, 1, 2}));
}

TEST(SniiStreamedWriterSessionTest, TightByteLimitKeepsReadableLegacySlimFrame) {
    SniiIndexInput input;
    input.index_id = 73;
    input.index_suffix = "tight_prx_readable";
    input.config = format::IndexConfig::kDocsPositions;
    input.write_freq = true;
    input.doc_count = 3;
    input.prx_window_limits = {
            .max_docs = 8,
            .max_positions = 10,
            .max_uncomp_bytes = 64,
    };
    input.terms.push_back(make_term("dense", {{.docid = 0, .positions = {0, 1, 2}},
                                              {.docid = 1, .positions = {0, 1, 2}},
                                              {.docid = 2, .positions = {0, 1, 2}}}));

    MemoryFile file;
    assert_ok(write_streamed_index(std::move(input), &file));

    reader::SniiSegmentReader segment;
    reader::LogicalIndexReader index;
    assert_ok(reader::SniiSegmentReader::open(&file, &segment));
    assert_ok(segment.open_index(73, "tight_prx_readable", &index));
    bool found = false;
    format::DictEntry entry;
    uint64_t frq_base = 0;
    uint64_t prx_base = 0;
    assert_ok(index.lookup("dense", &found, &entry, &frq_base, &prx_base));
    ASSERT_TRUE(found);
    EXPECT_EQ(entry.enc, format::DictEntryEnc::kSlim);
}

#ifdef BE_TEST
TEST(SniiStreamedWriterSessionTest, AutoCodecChoosesSmallestReadableRawFrame) {
    // All three codecs are reader-safe here. Complete frame sizes are PFOR=707,
    // ZSTD=659, and RAW=607 bytes, so RAW must win.
    EXPECT_EQ(format::testing::select_auto_prx_codec_for_test(
                      /*pfor_payload_size=*/700, /*plain_payload_size=*/600,
                      /*compressed_payload_size=*/650, /*max_uncomp_bytes=*/650),
              static_cast<uint8_t>(format::PrxCodec::kRaw));
}
#endif

TEST(SniiStreamedWriterSessionTest, PrxBuilderRejectsUnreadableFramesBeforeAppending) {
    ByteSink sink;
    sink.put_u8(0xA5);
    const std::vector<uint8_t> before = sink.buffer();
    const format::PrxWindowLimits position_limit {
            .max_docs = 4,
            .max_positions = 3,
            .max_uncomp_bytes = 64,
    };
    const std::vector<uint32_t> positions = {0, 1, 0, 1};
    const std::vector<uint32_t> freqs = {2, 2};
    format::PrxWindowBuildOutcome outcome = format::PrxWindowBuildOutcome::kBuilt;
    assert_ok(format::try_build_prx_window_flat(positions, freqs, -3, position_limit, &sink,
                                                &outcome));
    EXPECT_EQ(outcome, format::PrxWindowBuildOutcome::kNeedsSplit);
    EXPECT_EQ(sink.buffer(), before);
    EXPECT_TRUE(format::build_prx_window_flat(positions, freqs, -3, position_limit, &sink)
                        .is<ErrorCode::INVALID_ARGUMENT>());
    EXPECT_EQ(sink.buffer(), before);

    const std::vector<uint32_t> unsplittable_positions = {0, 1, 2, 3};
    const std::vector<uint32_t> unsplittable_freqs = {4};
    EXPECT_TRUE(format::try_build_prx_window_flat(unsplittable_positions, unsplittable_freqs, -3,
                                                  position_limit, &sink, &outcome)
                        .is<ErrorCode::INVALID_ARGUMENT>());
    EXPECT_EQ(sink.buffer(), before);

    const format::PrxWindowLimits byte_limit {
            .max_docs = 4,
            .max_positions = 4,
            .max_uncomp_bytes = 4,
    };
    const std::vector<uint32_t> wide_delta_positions = {0, UINT32_MAX};
    const std::vector<uint32_t> two_positions = {2};
    EXPECT_TRUE(
            format::build_prx_window_flat(wide_delta_positions, two_positions, 0, byte_limit, &sink)
                    .is<ErrorCode::INVALID_ARGUMENT>());
    EXPECT_EQ(sink.buffer(), before);
}

TEST(SniiStreamedWriterSessionTest, PrxBuilderRejectsMultiDocWindowWithUnsplittableDocument) {
    ByteSink sink;
    sink.put_u8(0xA5);
    const std::vector<uint8_t> before = sink.buffer();
    const format::PrxWindowLimits byte_limit {
            .max_docs = 4,
            .max_positions = 4,
            .max_uncomp_bytes = 4,
    };
    const std::vector<uint32_t> positions = {0, UINT32_MAX, 0};
    const std::vector<uint32_t> freqs = {2, 1};
    format::PrxWindowBuildOutcome outcome = format::PrxWindowBuildOutcome::kBuilt;

    EXPECT_TRUE(format::try_build_prx_window_flat(positions, freqs, 0, byte_limit, &sink, &outcome)
                        .is<ErrorCode::INVALID_ARGUMENT>());
    EXPECT_EQ(sink.buffer(), before);
    EXPECT_EQ(outcome, format::PrxWindowBuildOutcome::kBuilt);

    EXPECT_TRUE(format::try_build_prx_window_flat(positions, freqs, -3, byte_limit, &sink, &outcome)
                        .is<ErrorCode::INVALID_ARGUMENT>());
    EXPECT_EQ(sink.buffer(), before);
    EXPECT_EQ(outcome, format::PrxWindowBuildOutcome::kBuilt);

    const format::PrxWindowLimits shape_and_byte_limit {
            .max_docs = 4,
            .max_positions = 2,
            .max_uncomp_bytes = 4,
    };
    EXPECT_TRUE(format::try_build_prx_window_flat(positions, freqs, 0, shape_and_byte_limit, &sink,
                                                  &outcome)
                        .is<ErrorCode::INVALID_ARGUMENT>());
    EXPECT_EQ(sink.buffer(), before);
    EXPECT_EQ(outcome, format::PrxWindowBuildOutcome::kBuilt);
}

TEST(SniiStreamedWriterSessionTest, ExplicitReaderPrxLimitsPreserveDefaultBytes) {
    const std::vector<uint32_t> positions = {0, 2, 7, 1, 9};
    const std::vector<uint32_t> freqs = {3, 2};
    ByteSink legacy;
    ByteSink explicit_limits;
    assert_ok(format::build_prx_window_flat(positions, freqs, -3, &legacy));
    assert_ok(format::build_prx_window_flat(positions, freqs, -3, format::kReaderPrxWindowLimits,
                                            &explicit_limits));
    EXPECT_EQ(explicit_limits.buffer(), legacy.buffer());
}

TEST(SniiStreamedWriterSessionTest, DefaultSingletonRawFrameMatchesGolden) {
    const std::vector<uint32_t> positions = {0};
    const std::vector<uint32_t> freqs = {1};
    ByteSink frame;
    assert_ok(format::build_prx_window_flat(positions, freqs, -3, &frame));

    const std::vector<uint8_t> expected = {0x00, 0x03, 0x01, 0x01, 0x00, 0x05, 0xF5, 0xB3, 0x91};
    EXPECT_EQ(frame.buffer(), expected);
}

TEST(SniiStreamedWriterSessionTest, ByteLimitRecutReusesSourcePositionsAndFinalPlans) {
    constexpr uint32_t kDocs = 512;
    constexpr uint32_t kFarPosition = 1U << 28;

    TermPostings dense;
    dense.term = "dense";
    dense.docids.resize(kDocs);
    std::iota(dense.docids.begin(), dense.docids.end(), 0);
    dense.freqs.assign(kDocs, 2);
    dense.positions_flat.resize(2 * kDocs);
    for (size_t index = 0; index < dense.positions_flat.size(); ++index) {
        dense.positions_flat[index] = (index & 1U) == 0 ? 0 : kFarPosition;
    }

    std::vector<PostingDoc> tail_docs;
    tail_docs.reserve(kDocs);
    for (uint32_t docid = 0; docid < kDocs; ++docid) {
        tail_docs.push_back({.docid = docid, .positions = {1}});
    }

    SniiIndexInput input;
    input.index_id = 74;
    input.index_suffix = "byte_recut";
    input.config = format::IndexConfig::kDocsPositions;
    input.write_freq = true;
    input.doc_count = kDocs;
    input.prx_window_limits = {
            .max_docs = 1024,
            .max_positions = 2048,
            .max_uncomp_bytes = 8,
    };
    input.terms.push_back(std::move(dense));
    input.terms.push_back(make_term("tail", std::move(tail_docs)));

    MemoryFile file;
    assert_ok(write_streamed_index(std::move(input), &file));

    reader::SniiSegmentReader segment;
    reader::LogicalIndexReader index;
    assert_ok(reader::SniiSegmentReader::open(&file, &segment));
    assert_ok(segment.open_index(74, "byte_recut", &index));
    bool found = false;
    format::DictEntry entry;
    uint64_t frq_base = 0;
    uint64_t prx_base = 0;
    assert_ok(index.lookup("dense", &found, &entry, &frq_base, &prx_base));
    ASSERT_TRUE(found);
    EXPECT_EQ(entry.enc, format::DictEntryEnc::kWindowed);

    std::vector<uint32_t> expected_docs(kDocs);
    std::iota(expected_docs.begin(), expected_docs.end(), 0);
    std::vector<uint32_t> actual_docs;
    assert_ok(phrase_query(index, {"dense", "tail"}, &actual_docs));
    EXPECT_EQ(actual_docs, expected_docs);
}

TEST(SniiStreamedWriterSessionTest, ReleasesResidentDictAfterStreamingItIntoContainer) {
    writer::MemoryReporter reporter;
    MemoryFile file;
    SniiCompoundWriter compound(&file);
    SniiIndexInput input = representative_input(/*write_freq=*/false);
    std::vector<TermPostings> terms = std::move(input.terms);
    input.terms.clear();
    input.mem_reporter = &reporter;

    SniiStreamedIndexSession* session = nullptr;
    assert_ok(compound.begin_streamed_index(std::move(input), &session));
    for (TermPostings& term : terms) {
        assert_ok(push_materialized(session, std::move(term)));
    }
    assert_ok(session->finish());

    // finish() has already copied the complete DICT region into the compound
    // output. The only remaining tracked bytes are the BSBF that must be laid
    // out after every logical index's posting/DICT regions.
    EXPECT_GT(reporter.current_bytes(), 0);
    assert_ok(compound.finish());
    EXPECT_EQ(reporter.current_bytes(), 0);
}

TEST(SniiStreamedWriterSessionTest, TermHashGrowthIsReservedBeforeAllocation) {
    writer::MemoryReporter reporter(/*consume_release=*/nullptr, /*cap_bytes=*/7);
    MemoryFile file;
    SniiCompoundWriter compound(&file);
    SniiIndexInput input = empty_input(98, "term_hash_budget", 1);
    input.mem_reporter = &reporter;
    SniiStreamedIndexSession* session = nullptr;
    assert_ok(compound.begin_streamed_index(std::move(input), &session));

    const Status status =
            push_materialized(session, make_term("term", {{.docid = 0, .positions = {0}}}));
    EXPECT_TRUE(status.is<ErrorCode::MEM_LIMIT_EXCEEDED>()) << status;
    EXPECT_EQ(reporter.current_bytes(), 0);
}

TEST(SniiStreamedWriterSessionTest, FinalizesAfterPersistentVocabularyCrossesSpillThreshold) {
    constexpr uint64_t kReporterCap = 384U << 10;
    constexpr uint32_t kTermCount = 3000;
    for (bool force_spill : {false, true}) {
        writer::MemoryReporter reporter(
                /*consume_release=*/nullptr, kReporterCap,
                writer::MemoryReporter::CapPolicy::kSpillThreshold);
        SpimiTermBuffer terms(/*has_positions=*/true, kReporterCap, &reporter);
        terms.set_forced_spill_min_arena_bytes(1);
        for (uint32_t i = 0; i < kTermCount; ++i) {
            if (force_spill && i == kTermCount / 2) {
                terms.request_global_spill_for_test();
            }
            terms.add_token("term_" + std::to_string(100000 + i), /*docid=*/0, /*pos=*/i);
        }
        assert_ok(terms.status());
        ASSERT_GT(reporter.current_bytes(), static_cast<int64_t>(kReporterCap));
        if (force_spill) {
            ASSERT_GT(terms.run_count_for_test(), 0);
        } else {
            ASSERT_EQ(terms.run_count_for_test(), 0);
        }

        MemoryFile file;
        SniiCompoundWriter compound(&file);
        const std::string suffix =
                force_spill ? "persistent_vocab_spilled" : "persistent_vocab_in_memory";
        const uint64_t index_id = force_spill ? 100 : 99;
        SniiIndexInput input = empty_input(index_id, suffix, /*doc_count=*/1);
        input.target_dict_block_bytes = 16U << 10;
        input.term_source = &terms;
        input.mem_reporter = &reporter;
        assert_ok(compound.add_logical_index(input));
        assert_ok(compound.finish());
        EXPECT_EQ(reporter.current_bytes(), 0);

        reader::SniiSegmentReader segment;
        reader::LogicalIndexReader index;
        assert_ok(reader::SniiSegmentReader::open(&file, &segment));
        assert_ok(segment.open_index(index_id, suffix, &index));
        EXPECT_EQ(index.stats().term_count, kTermCount);
        bool found = false;
        format::DictEntry entry;
        uint64_t frq_base = 0;
        uint64_t prx_base = 0;
        assert_ok(index.lookup("term_100000", &found, &entry, &frq_base, &prx_base));
        EXPECT_TRUE(found);
        assert_ok(index.lookup("term_102999", &found, &entry, &frq_base, &prx_base));
        EXPECT_TRUE(found);
    }
}

TEST(SniiStreamedWriterSessionTest, TerminalDrainReleasesLookupStateWithoutUnderAccountingIds) {
    constexpr uint64_t kReporterCap = 384U << 10;
    constexpr uint32_t kTermCount = 3000;
    for (bool force_spill : {false, true}) {
        writer::MemoryReporter reporter(
                /*consume_release=*/nullptr, kReporterCap,
                writer::MemoryReporter::CapPolicy::kSpillThreshold);
        SpimiTermBuffer terms(/*has_positions=*/true, kReporterCap, &reporter);
        terms.set_forced_spill_min_arena_bytes(1);
        for (uint32_t i = 0; i < kTermCount; ++i) {
            if (force_spill && i == kTermCount / 2) {
                terms.request_global_spill_for_test();
            }
            terms.add_token("term_" + std::to_string(100000 + i), /*docid=*/0, /*pos=*/i);
        }
        assert_ok(terms.status());
        if (force_spill) {
            ASSERT_GT(terms.run_count_for_test(), 0);
        } else {
            ASSERT_EQ(terms.run_count_for_test(), 0);
        }
        const uint64_t before_drain = terms.resident_bytes_for_test();
        ASSERT_EQ(reporter.current_bytes(), static_cast<int64_t>(before_drain));
        ASSERT_GT(before_drain, kReporterCap);

        bool saw_first_term = false;
        assert_ok(terms.for_each_term_sorted([&](writer::StreamedTermPostings&& source) {
            if (!saw_first_term) {
                const uint64_t during_drain = terms.resident_bytes_for_test();
                if (force_spill) {
                    EXPECT_GT(reporter.current_bytes(), static_cast<int64_t>(during_drain));
                } else {
                    EXPECT_EQ(reporter.current_bytes(), static_cast<int64_t>(during_drain));
                }
                EXPECT_LT(during_drain, before_drain);
                if (!force_spill) {
                    EXPECT_GE(during_drain, static_cast<uint64_t>(kTermCount) * sizeof(uint32_t));
                }
                saw_first_term = true;
            }
            return writer::consume_streamed_term(std::move(source));
        }));
        EXPECT_TRUE(saw_first_term);
        EXPECT_EQ(reporter.current_bytes(), 0);
    }
}

TEST(SniiStreamedWriterSessionTest, MoveAppendAccountsRetainedVectorCapacity) {
    constexpr size_t kLogicalBytes = 7;
    constexpr size_t kReservedBytes = 4096;
    std::vector<uint8_t> bytes(kLogicalBytes, 0x5A);
    bytes.reserve(kReservedBytes);
    const size_t retained_capacity = bytes.capacity();
    ASSERT_GT(retained_capacity, bytes.size());
    const std::vector<uint8_t> expected = bytes;

    writer::MemoryReporter reporter;
    SpillableByteBuffer buffer(/*cap_bytes=*/1U << 20, "capacity_accounting", &reporter);
    assert_ok(buffer.append_move(std::move(bytes)));

    EXPECT_EQ(buffer.size(), kLogicalBytes);
    EXPECT_EQ(reporter.current_bytes(), static_cast<int64_t>(retained_capacity));

    MemoryFile output;
    assert_ok(buffer.seal());
    assert_ok(buffer.stream_into_and_release(&output));
    EXPECT_EQ(buffer.size(), kLogicalBytes);
    EXPECT_EQ(output.data(), expected);
    EXPECT_EQ(reporter.current_bytes(), 0);
}

TEST(SniiStreamedWriterSessionTest, ReusesTransferCapacityAcrossAdjacentTerms) {
    uint64_t positive_reservations = 0;
    writer::MemoryReporter reporter([&](int64_t delta) {
        if (delta > 0) {
            ++positive_reservations;
        }
    });

    {
        MemoryFile file;
        SniiCompoundWriter compound(&file);
        SniiIndexInput input = empty_input(109, "reused_transfer_capacity", /*doc_count=*/4);
        input.config = format::IndexConfig::kDocsPositions;
        input.mem_reporter = &reporter;

        SniiStreamedIndexSession* session = nullptr;
        assert_ok(compound.begin_streamed_index(std::move(input), &session));
        for (uint32_t i = 0; i < 12; ++i) {
            assert_ok(push_materialized(session, make_term("term_" + std::to_string(100 + i),
                                                           {{.docid = 0, .positions = {1}},
                                                            {.docid = 2, .positions = {3}}})));
        }
        const uint64_t reservations_after_warmup = positive_reservations;
        assert_ok(push_materialized(session,
                                    make_term("term_112", {{.docid = 0, .positions = {1}},
                                                           {.docid = 2, .positions = {3}}})));
        EXPECT_EQ(positive_reservations, reservations_after_warmup);

        assert_ok(session->finish());
        assert_ok(compound.finish());
    }
    EXPECT_EQ(reporter.current_bytes(), 0);
}

TEST(SniiStreamedWriterSessionTest, StreamsSpillWithSubMegabyteReporterHeadroom) {
    constexpr size_t kSpilledBytes = (1U << 20) + 4096;
    constexpr uint64_t kReadHeadroom = 64U << 10;
    constexpr uint64_t kReporterCap = 2U << 20;

    std::vector<uint8_t> bytes(kSpilledBytes);
    for (size_t i = 0; i < bytes.size(); ++i) {
        bytes[i] = static_cast<uint8_t>(i % 251);
    }

    writer::MemoryReporter reporter(/*consume_release=*/nullptr, kReporterCap);
    auto retained_reservation = reporter.make_reservation();
    assert_ok(retained_reservation.set_bytes(kReporterCap - kReadHeadroom));

    SpillableByteBuffer buffer(std::numeric_limits<uint64_t>::max(), "small_read_headroom",
                               &reporter);
    assert_ok(buffer.append(Slice(bytes)));
    ASSERT_TRUE(buffer.spilled());
    assert_ok(buffer.seal());

    MemoryFile output;
    assert_ok(buffer.stream_into_and_release(&output));
    EXPECT_EQ(output.data(), bytes);
    EXPECT_EQ(reporter.current_bytes(), static_cast<int64_t>(kReporterCap - kReadHeadroom));
}

TEST(SniiStreamedWriterSessionTest, SessionOwnsMovedInputUntilContainerFinish) {
    MemoryFile file;
    SniiCompoundWriter compound(&file);
    SniiStreamedIndexSession* session = nullptr;
    assert_ok(begin_scoring_session_from_local_input(&compound, &session));

    // Reuse heap storage after the caller-side SniiIndexInput is gone. The
    // session must still own encoded_norms and every other referenced vector.
    std::vector<std::string> heap_churn(64, std::string(4096, 'x'));
    ASSERT_EQ(heap_churn.front().size(), 4096U);

    assert_ok(push_materialized(session, make_term("alpha", {{.docid = 0, .positions = {0}},
                                                             {.docid = 2, .positions = {0}}})));
    assert_ok(push_materialized(session, make_term("beta", {{.docid = 0, .positions = {1}},
                                                            {.docid = 2, .positions = {2}}})));
    assert_ok(session->set_semantic_token_count(4));
    assert_ok(session->finish());
    assert_ok(compound.finish());

    reader::SniiSegmentReader segment;
    reader::LogicalIndexReader index;
    assert_ok(reader::SniiSegmentReader::open(&file, &segment));
    assert_ok(segment.open_index(91, "owned_scoring_body_with_heap_storage", &index));
    EXPECT_EQ(index.stats().doc_count, 4U);
    EXPECT_EQ(index.stats().indexed_doc_count, 2U);
    EXPECT_EQ(index.stats().null_count, 2U);

    format::NormsPodReader norms;
    assert_ok(index.open_norms(&norms));
    ASSERT_EQ(norms.doc_count(), 4U);
    EXPECT_EQ(norms.encoded_norm(0), 7U);
    EXPECT_EQ(norms.encoded_norm(1), 11U);
    EXPECT_EQ(norms.encoded_norm(2), 13U);
    EXPECT_EQ(norms.encoded_norm(3), 17U);

    std::vector<uint32_t> term_docs;
    assert_ok(term_query(index, "alpha", &term_docs));
    EXPECT_EQ(term_docs, (std::vector<uint32_t> {0, 2}));
    std::vector<uint32_t> phrase_docs;
    assert_ok(phrase_query(index, {"alpha", "beta"}, &phrase_docs));
    EXPECT_EQ(phrase_docs, (std::vector<uint32_t> {0}));
}

TEST(SniiStreamedWriterSessionTest, ActiveAndFinishedSessionLifecycleIsEnforced) {
    MemoryFile file;
    SniiCompoundWriter compound(&file);
    SniiStreamedIndexSession* first = nullptr;
    assert_ok(compound.begin_streamed_index(empty_input(101, "first"), &first));

    EXPECT_TRUE(compound.add_logical_index(empty_input(102, "blocked_add"))
                        .is<ErrorCode::INTERNAL_ERROR>());
    SniiStreamedIndexSession* blocked = first;
    EXPECT_TRUE(compound.begin_streamed_index(empty_input(103, "blocked_begin"), &blocked)
                        .is<ErrorCode::INTERNAL_ERROR>());
    EXPECT_EQ(blocked, nullptr);
    EXPECT_TRUE(compound.finish().is<ErrorCode::INTERNAL_ERROR>());

    assert_ok(push_materialized(first, make_term("alpha", {{.docid = 0, .positions = {0}}})));
    assert_ok(first->finish());
    EXPECT_TRUE(first->finished());
    EXPECT_TRUE(push_materialized(first, make_term("beta", {{.docid = 1, .positions = {0}}}))
                        .is<ErrorCode::INVALID_ARGUMENT>());
    EXPECT_TRUE(first->finish().is<ErrorCode::INVALID_ARGUMENT>());

    // Starting a second session may grow the owner's session vector. The first
    // raw handle remains valid and inert for the compound writer's lifetime.
    SniiStreamedIndexSession* second = nullptr;
    assert_ok(compound.begin_streamed_index(empty_input(104, "second"), &second));
    EXPECT_TRUE(first->finish().is<ErrorCode::INVALID_ARGUMENT>());
    assert_ok(push_materialized(second, make_term("bravo", {{.docid = 2, .positions = {1}}})));
    assert_ok(second->finish());
    assert_ok(compound.finish());
    EXPECT_TRUE(file.finalized());

    reader::SniiSegmentReader segment;
    assert_ok(reader::SniiSegmentReader::open(&file, &segment));
    bool exists = false;
    assert_ok(segment.index_exists(101, "first", &exists));
    EXPECT_TRUE(exists);
    assert_ok(segment.index_exists(104, "second", &exists));
    EXPECT_TRUE(exists);
    assert_ok(segment.index_exists(102, "blocked_add", &exists));
    EXPECT_FALSE(exists);
}

TEST(SniiStreamedWriterSessionTest, SemanticTokenCountIsLateBoundExactlyOnceBeforeFinish) {
    MemoryFile file;
    SniiCompoundWriter compound(&file);
    SniiStreamedIndexSession* session = nullptr;
    assert_ok(compound.begin_streamed_index(common_grams_input(105, "common_grams"), &session));
    assert_ok(push_materialized(session, make_term("alpha", {{.docid = 0, .positions = {0, 1}}})));

    assert_ok(session->set_semantic_token_count(2));
    EXPECT_TRUE(session->set_semantic_token_count(2).is<ErrorCode::INVALID_ARGUMENT>());
    assert_ok(session->finish());
    EXPECT_TRUE(session->set_semantic_token_count(2).is<ErrorCode::INVALID_ARGUMENT>());
    assert_ok(compound.finish());

    reader::SniiSegmentReader segment;
    reader::LogicalIndexReader index;
    assert_ok(reader::SniiSegmentReader::open(&file, &segment));
    assert_ok(segment.open_index(105, "common_grams", &index));
    ASSERT_NE(index.common_grams_metadata(), nullptr);
    EXPECT_EQ(index.common_grams_metadata()->scoring_doc_count, 1U);
    EXPECT_EQ(index.common_grams_metadata()->scoring_token_count, 2U);
}

TEST(SniiStreamedWriterSessionTest, CommonGramsFinishRequiresSemanticTokenCount) {
    MemoryFile file;
    SniiCompoundWriter compound(&file);
    SniiStreamedIndexSession* session = nullptr;
    assert_ok(compound.begin_streamed_index(common_grams_input(106, "missing_token_count"),
                                            &session));
    assert_ok(push_materialized(session, make_term("alpha", {{.docid = 0, .positions = {0}}})));

    EXPECT_TRUE(session->finish().is<ErrorCode::INVALID_ARGUMENT>());
    EXPECT_FALSE(session->finished());
}

TEST(SniiStreamedWriterSessionTest, TermOrderRejectionPoisonsCompoundSession) {
    MemoryFile file;
    SniiCompoundWriter compound(&file);
    SniiStreamedIndexSession* session = nullptr;
    assert_ok(compound.begin_streamed_index(empty_input(111, "ordered"), &session));

    assert_ok(push_materialized(session, make_term("bravo", {{.docid = 0, .positions = {0}}})));
    EXPECT_TRUE(push_materialized(session, make_term("alpha", {{.docid = 1, .positions = {0}}}))
                        .is<ErrorCode::INVALID_ARGUMENT>());
    EXPECT_TRUE(push_materialized(session, make_term("charlie", {{.docid = 2, .positions = {0}}}))
                        .is<ErrorCode::INVALID_ARGUMENT>());
    EXPECT_TRUE(session->finish().is<ErrorCode::INVALID_ARGUMENT>());
    EXPECT_TRUE(compound.finish().is<ErrorCode::INVALID_ARGUMENT>());
    EXPECT_FALSE(file.finalized());
}

TEST(SniiStreamedWriterSessionTest, RejectsNullDocidsOutsideTheDocumentDomain) {
    const std::vector<std::vector<uint32_t>> invalid_nulls {{0, 0}, {1}};
    for (const std::vector<uint32_t>& nulls : invalid_nulls) {
        MemoryFile file;
        SniiCompoundWriter compound(&file);
        SniiIndexInput input = empty_input(112, "invalid_nulls", /*doc_count=*/1);
        input.null_docids = nulls;
        SniiStreamedIndexSession* session = nullptr;
        EXPECT_TRUE(compound.begin_streamed_index(std::move(input), &session)
                            .is<ErrorCode::INVALID_ARGUMENT>());
        EXPECT_EQ(session, nullptr);
        EXPECT_FALSE(file.finalized());
    }
}

TEST(SniiStreamedWriterSessionTest, OutOfRangePostingPoisonsCompoundSession) {
    MemoryFile file;
    SniiCompoundWriter compound(&file);
    SniiStreamedIndexSession* session = nullptr;
    assert_ok(compound.begin_streamed_index(empty_input(113, "invalid_docid", /*doc_count=*/1),
                                            &session));

    EXPECT_TRUE(push_materialized(session, make_term("alpha", {{.docid = 1, .positions = {0}}}))
                        .is<ErrorCode::INVALID_ARGUMENT>());
    EXPECT_TRUE(session->finish().is<ErrorCode::INVALID_ARGUMENT>());
    EXPECT_TRUE(compound.finish().is<ErrorCode::INVALID_ARGUMENT>());
    EXPECT_FALSE(file.finalized());
}

TEST(SniiStreamedWriterSessionTest, BootstrapAppendFailurePoisonsCompound) {
    PartialFailOnAppendFile file(/*fail_on_append=*/1);
    SniiCompoundWriter compound(&file);
    SniiStreamedIndexSession* session = nullptr;
    EXPECT_FALSE(compound.begin_streamed_index(empty_input(121, "bootstrap"), &session).ok());
    EXPECT_EQ(session, nullptr);
    EXPECT_EQ(file.append_calls(), 1U);

    SniiStreamedIndexSession* retry = nullptr;
    EXPECT_FALSE(compound.begin_streamed_index(empty_input(122, "retry"), &retry).ok());
    EXPECT_EQ(retry, nullptr);
    EXPECT_FALSE(compound.finish().ok());
    EXPECT_EQ(file.append_calls(), 1U);
    EXPECT_EQ(file.finalize_calls(), 0U);
    EXPECT_FALSE(file.finalized());
}

TEST(SniiStreamedWriterSessionTest, PostingAppendFailurePoisonsSession) {
    PartialFailOnAppendFile file(/*fail_on_append=*/2);
    SniiCompoundWriter compound(&file);
    SniiStreamedIndexSession* session = nullptr;
    constexpr uint32_t kDocCount = 300U * 100003U + 1U;
    assert_ok(compound.begin_streamed_index(empty_input(131, "posting", kDocCount), &session));

    EXPECT_FALSE(push_materialized(session, make_sparse_slim_term("slim")).ok());
    EXPECT_TRUE(push_materialized(session, make_term("zulu", {{.docid = 0, .positions = {0}}}))
                        .is<ErrorCode::INTERNAL_ERROR>());
    EXPECT_TRUE(session->finish().is<ErrorCode::INTERNAL_ERROR>());
    EXPECT_TRUE(compound.finish().is<ErrorCode::INTERNAL_ERROR>());
    EXPECT_EQ(file.append_calls(), 2U);
    EXPECT_EQ(file.finalize_calls(), 0U);
    EXPECT_FALSE(file.finalized());
}

TEST(SniiStreamedWriterSessionTest, DictAppendFailureLeavesContainerUnsealable) {
    PartialFailOnAppendFile file(/*fail_on_append=*/2);
    SniiCompoundWriter compound(&file);
    SniiStreamedIndexSession* session = nullptr;
    assert_ok(compound.begin_streamed_index(empty_input(141, "dict"), &session));
    assert_ok(push_materialized(session, make_term("alpha", {{.docid = 0, .positions = {0}}})));

    // The inline term writes no posting bytes. Logical finalization succeeds,
    // then the first DICT-region append (append #2 after the bootstrap) fails.
    EXPECT_FALSE(session->finish().ok());
    EXPECT_TRUE(push_materialized(session, make_term("beta", {{.docid = 1, .positions = {0}}}))
                        .is<ErrorCode::INTERNAL_ERROR>());
    EXPECT_TRUE(session->finish().is<ErrorCode::INTERNAL_ERROR>());
    EXPECT_TRUE(compound.finish().is<ErrorCode::INTERNAL_ERROR>());
    EXPECT_EQ(file.append_calls(), 2U);
    EXPECT_EQ(file.finalize_calls(), 0U);
    EXPECT_FALSE(file.finalized());
}

TEST(SniiStreamedWriterSessionTest, SourcePushMatchesMaterializedShapeMatrix) {
    uint64_t index_id = 151;
    auto expect_match = [&](std::string suffix, TermPostings postings, format::IndexConfig config,
                            bool write_freq, format::PrxWindowLimits limits) {
        SCOPED_TRACE(suffix + (write_freq ? "-freq" : "-no-freq"));
        SniiIndexInput input;
        input.index_id = index_id++;
        input.index_suffix = std::move(suffix);
        input.config = config;
        input.doc_count = postings.docids.empty() ? 0 : postings.docids.back() + 1;
        input.write_freq = write_freq;
        input.prx_window_limits = limits;

        SniiIndexInput expected_input = input;
        TermPostings materialized = postings;
        if (!materialized.retain_positions && materialized.freqs.empty()) {
            materialized.freqs.assign(materialized.docids.size(), 1);
        }
        expected_input.terms.push_back(std::move(materialized));
        MemoryFile expected;
        assert_ok(write_ordinary_index(std::move(expected_input), &expected));

        MemoryFile actual;
        assert_ok(write_source_index(std::move(input), &postings, &actual));
        EXPECT_EQ(actual.data(), expected.data());
    };

    const format::PrxWindowLimits default_limits = format::kReaderPrxWindowLimits;
    const format::PrxWindowLimits recut_limits {
            .max_docs = 1024,
            .max_positions = 2048,
            .max_uncomp_bytes = 2048,
    };
    for (bool write_freq : {false, true}) {
        expect_match("empty-key", make_uniform_term("", 1), format::IndexConfig::kDocsPositions,
                     write_freq, default_limits);
        expect_match("inline", make_uniform_term("inline", 1), format::IndexConfig::kDocsPositions,
                     write_freq, default_limits);
        expect_match("slim", make_sparse_slim_term("slim"), format::IndexConfig::kDocsPositions,
                     write_freq, default_limits);
        expect_match("df-511", make_uniform_term("df-511", 511),
                     format::IndexConfig::kDocsPositions, write_freq, default_limits);
        expect_match("df-512", make_uniform_term("df-512", 512),
                     format::IndexConfig::kDocsPositions, write_freq, default_limits);
        expect_match("df-8191", make_uniform_term("df-8191", 8191),
                     format::IndexConfig::kDocsPositions, write_freq, default_limits);
        expect_match("df-8192", make_uniform_term("df-8192", 8192),
                     format::IndexConfig::kDocsPositions, write_freq, default_limits);
        expect_match("recut-full", make_adaptive_boundary_term(1024),
                     format::IndexConfig::kDocsPositions, write_freq, recut_limits);
        expect_match("recut-tail", make_adaptive_boundary_term(512),
                     format::IndexConfig::kDocsPositions, write_freq, recut_limits);
    }

    TermPostings docs_only = make_uniform_term("docs-only", 512);
    docs_only.retain_positions = false;
    docs_only.freqs.clear();
    docs_only.positions_flat.clear();
    expect_match("docs-only", std::move(docs_only), format::IndexConfig::kDocsOnly,
                 /*write_freq=*/false, default_limits);

    TermPostings docs_with_stats = make_uniform_term("docs-with-stats", 512);
    docs_with_stats.retain_positions = false;
    docs_with_stats.freqs.assign(docs_with_stats.docids.size(), 2);
    docs_with_stats.positions_flat.clear();
    expect_match("docs-with-stats", std::move(docs_with_stats), format::IndexConfig::kDocsOnly,
                 /*write_freq=*/false, default_limits);
}

TEST(SniiStreamedWriterSessionTest, SourcePushMatchesMaterializedAcrossAdaptiveBoundary) {
    TermPostings postings = make_adaptive_boundary_term();
    SniiIndexInput expected_input = empty_input(181, "source-boundary", postings.docids.back() + 1);
    expected_input.prx_window_limits = {
            .max_docs = 1024,
            .max_positions = 2048,
            .max_uncomp_bytes = 2048,
    };
    expected_input.terms.push_back(postings);
    MemoryFile expected;
    assert_ok(write_ordinary_index(std::move(expected_input), &expected));

    VectorTermPostingSource source(&postings);
    SniiIndexInput actual_input = empty_input(181, "source-boundary", postings.docids.back() + 1);
    actual_input.prx_window_limits = {
            .max_docs = 1024,
            .max_positions = 2048,
            .max_uncomp_bytes = 2048,
    };
    MemoryFile actual;
    SniiCompoundWriter compound(&actual);
    SniiStreamedIndexSession* session = nullptr;
    assert_ok(compound.begin_streamed_index(std::move(actual_input), &session));
    assert_ok(session->push_term(StreamedTermPostings {
            .term = postings.term,
            .retain_positions = true,
            .source = &source,
    }));
    assert_ok(session->finish());
    assert_ok(compound.finish());

    EXPECT_EQ(actual.data(), expected.data());
    EXPECT_TRUE(source.out_was_empty);
    EXPECT_EQ(source.requests, (std::vector<uint32_t> {format::kAdaptiveWindowDfThreshold,
                                                       format::kAdaptiveWindowDocs}));
}

TEST(SniiStreamedWriterSessionTest, InvalidSourceContractPoisonsCompoundSession) {
    const std::array<InvalidTermPostingSource::Mode, 6> modes = {
            InvalidTermPostingSource::Mode::kEmptyBeforeEof,
            InvalidTermPostingSource::Mode::kShortBeforeEof,
            InvalidTermPostingSource::Mode::kOverfill,
            InvalidTermPostingSource::Mode::kUnordered,
            InvalidTermPostingSource::Mode::kOutOfRange,
            InvalidTermPostingSource::Mode::kError,
    };
    for (size_t i = 0; i < modes.size(); ++i) {
        SCOPED_TRACE(i);
        InvalidTermPostingSource source(modes[i]);
        MemoryFile file;
        SniiCompoundWriter compound(&file);
        SniiStreamedIndexSession* session = nullptr;
        SniiIndexInput input = empty_input(191 + i, "invalid-source-" + std::to_string(i), 32);
        input.config = format::IndexConfig::kDocsOnly;
        assert_ok(compound.begin_streamed_index(std::move(input), &session));
        EXPECT_FALSE(session->push_term(StreamedTermPostings {
                                                .term = "alpha",
                                                .retain_positions = false,
                                                .source = &source,
                                        })
                             .ok());
        EXPECT_FALSE(
                push_materialized(session, make_term("bravo", {{.docid = 1, .positions = {0}}}))
                        .ok());
        EXPECT_FALSE(session->finish().ok());
        EXPECT_FALSE(compound.finish().ok());
        EXPECT_FALSE(file.finalized());
    }
}

TEST(SniiStreamedWriterSessionTest, PositionedSourceShapeIsValidatedWhenIndexDropsPrx) {
    for (size_t position_count : {1U, 3U}) {
        SCOPED_TRACE(position_count);
        InvalidPositionShapeSource source(position_count);
        MemoryFile file;
        SniiCompoundWriter compound(&file);
        SniiStreamedIndexSession* session = nullptr;
        SniiIndexInput input = empty_input(211 + position_count, "invalid-position-shape", 1);
        input.config = format::IndexConfig::kDocsOnly;
        assert_ok(compound.begin_streamed_index(std::move(input), &session));
        const Status status = session->push_term(StreamedTermPostings {
                .term = "alpha", .retain_positions = true, .source = &source});
        EXPECT_TRUE(status.is<ErrorCode::INVALID_ARGUMENT>()) << status.to_string();
        EXPECT_NE(status.to_string().find("source positions count must equal sum(freqs)"),
                  std::string::npos)
                << status.to_string();
        EXPECT_TRUE(session->finish().is<ErrorCode::INVALID_ARGUMENT>());
        EXPECT_TRUE(compound.finish().is<ErrorCode::INVALID_ARGUMENT>());
        EXPECT_FALSE(file.finalized());
    }
}

TEST(SniiStreamedWriterSessionTest, LateSourceFailurePoisonsAfterPostingBytesWereWritten) {
    TermPostings postings = make_adaptive_boundary_term();
    FailAfterFirstFillSource source(&postings);
    MemoryFile file;
    SniiCompoundWriter compound(&file);
    SniiStreamedIndexSession* session = nullptr;
    SniiIndexInput input = empty_input(201, "late-source-failure", postings.docids.back() + 1);
    assert_ok(compound.begin_streamed_index(std::move(input), &session));
    const uint64_t bootstrap_bytes = file.bytes_written();

    EXPECT_TRUE(session->push_term(StreamedTermPostings {
                                           .term = postings.term,
                                           .retain_positions = true,
                                           .source = &source,
                                   })
                        .is<ErrorCode::INTERNAL_ERROR>());
    EXPECT_GT(file.bytes_written(), bootstrap_bytes);
    EXPECT_TRUE(session->finish().is<ErrorCode::INTERNAL_ERROR>());
    EXPECT_TRUE(compound.finish().is<ErrorCode::INTERNAL_ERROR>());
    EXPECT_FALSE(file.finalized());
}

} // namespace
