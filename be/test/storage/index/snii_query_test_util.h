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

#pragma once

// Shared gtest fixtures for the SNII reader/writer test suites. Other test files
// reuse these by including this header and pulling the symbols in with
// `using namespace doris::snii::snii_test;` (see snii_query_test.cpp).

#include <gtest/gtest.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/format/format_constants.h"
#include "storage/index/snii/format/phrase_bigram.h"
#include "storage/index/snii/io/file_reader.h"
#include "storage/index/snii/io/file_writer.h"
#include "storage/index/snii/reader/logical_index_reader.h"
#include "storage/index/snii/reader/snii_segment_reader.h"
#include "storage/index/snii/writer/snii_compound_writer.h"
#include "storage/index/snii/writer/spimi_term_buffer.h"

namespace doris::snii::snii_test {
using doris::Status; // RETURN_IF_ERROR / Status::OK() expand to a bare Status below.

// An in-memory FileReader+FileWriter that records every read() (offset/len) so
// snii-layer round-trips and exact-range assertions can be made against it.
class MemoryFile final : public doris::snii::io::FileReader, public doris::snii::io::FileWriter {
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

// RAII helper that sets an environment variable for the duration of a test and
// restores (or clears) the previous value on destruction.
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

// A single document's postings (docid + its in-doc positions) used to drive the
// writer fixtures below.
struct PostingDoc {
    uint32_t docid = 0;
    std::vector<uint32_t> positions;
};

inline writer::TermPostings make_term(std::string term, std::vector<PostingDoc> docs) {
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

inline std::vector<PostingDoc> docs_with_one_position(uint32_t begin, uint32_t end,
                                                      uint32_t position) {
    std::vector<PostingDoc> docs;
    docs.reserve(end - begin);
    for (uint32_t docid = begin; docid < end; ++docid) {
        docs.push_back({docid, {position}});
    }
    return docs;
}

inline void assert_ok(const Status& status) {
    ASSERT_TRUE(status.ok()) << status.to_string();
}

// The standard reader-side fixture: a 9000-doc kDocsPositions index whose terms
// exercise dense/sparse/windowed/tail postings, optionally including the hidden
// phrase-bigram terms. Opens `segment_reader`/`index_reader` over `file`.
// `bigram_prune_min_df` != 0 builds a G01 df-PRUNED segment: bigram terms with
// df below it are dropped at materialization, survivors are written docs-only,
// and the per-index meta records the threshold (reader fallback gate). 0 (the
// default) keeps the legacy layout byte-identical to the pre-G01 fixture.
inline Status build_reader(MemoryFile* file, reader::SniiSegmentReader* segment_reader,
                           reader::LogicalIndexReader* index_reader,
                           bool include_phrase_bigrams = false, uint32_t bigram_prune_min_df = 0) {
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
    input.bigram_prune_min_df = bigram_prune_min_df;
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
        // bigram(order, ordinal): only docs 5000/7000 have "order"@1 immediately
        // followed by "ordinal"@2 (elsewhere both sit at position 2, not
        // adjacent). Drives the n>=3 {"failed","order","ordinal"} bigram path.
        input.terms.push_back(
                make_term(format::make_phrase_bigram_term("order", "ordinal"),
                          {{.docid = 5000, .positions = {1}}, {.docid = 7000, .positions = {1}}}));
        // bigram(repeat, repeat): every doc has "repeat"@{0,1,2}, so the adjacent
        // pair occurs in all docs. Exercises the ordered-pair construction for
        // repeated-term phrases (the same bigram appears for both adjacent pairs).
        std::vector<PostingDoc> repeat_repeat_docs;
        repeat_repeat_docs.reserve(kDocCount);
        for (uint32_t docid = 0; docid < kDocCount; ++docid) {
            repeat_repeat_docs.push_back({docid, {0}});
        }
        input.terms.push_back(make_term(format::make_phrase_bigram_term("repeat", "repeat"),
                                        std::move(repeat_repeat_docs)));
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

} // namespace doris::snii::snii_test
