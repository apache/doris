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
#include <cstring>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "snii/common/slice.h"
#include "snii/encoding/byte_sink.h"
#include "snii/encoding/byte_source.h"
#include "snii/format/format_constants.h"
#include "snii/format/prx_pod.h"
#include "snii/io/file_reader.h"
#include "snii/io/file_writer.h"
#include "snii/query/docid_sink.h"
#include "snii/query/phrase_query.h"
#include "snii/query/term_query.h"
#include "snii/reader/logical_index_reader.h"
#include "snii/reader/snii_segment_reader.h"
#include "snii/writer/snii_compound_writer.h"
#include "snii/writer/spimi_term_buffer.h"

namespace snii::query {
namespace {

class MemoryFile final : public snii::io::FileReader, public snii::io::FileWriter {
public:
    Status append(Slice data) override {
        data_.insert(data_.end(), data.data(), data.data() + data.size());
        return Status::OK();
    }

    Status finalize() override {
        finalized_ = true;
        return Status::OK();
    }

    uint64_t bytes_written() const override { return data_.size(); }

    Status read_at(uint64_t offset, size_t len, std::vector<uint8_t>* out) override {
        if (offset > data_.size() || len > data_.size() - offset) {
            return Status::Corruption("memory file read past eof");
        }
        out->resize(len);
        if (len != 0) {
            std::memcpy(out->data(), data_.data() + offset, len);
        }
        return Status::OK();
    }

    uint64_t size() const override { return data_.size(); }
    bool finalized() const { return finalized_; }

private:
    std::vector<uint8_t> data_;
    bool finalized_ = false;
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

Status build_reader(MemoryFile* file, reader::SniiSegmentReader* segment_reader,
                    reader::LogicalIndexReader* index_reader) {
    constexpr uint32_t kDocCount = 9000;
    auto failed_docs = docs_with_one_position(0, kDocCount, 0);
    auto order_docs = docs_with_one_position(0, kDocCount, 2);
    auto ordinal_docs = docs_with_one_position(0, kDocCount, 2);
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
    input.terms = {make_term("failed", std::move(failed_docs)),
                   make_term("order", std::move(order_docs)),
                   make_term("ordinal", std::move(ordinal_docs))};

    writer::SniiCompoundWriter writer(file);
    SNII_RETURN_IF_ERROR(writer.add_logical_index(input));
    SNII_RETURN_IF_ERROR(writer.finish());
    EXPECT_TRUE(file->finalized());

    SNII_RETURN_IF_ERROR(reader::SniiSegmentReader::open(file, segment_reader));
    return segment_reader->open_index(input.index_id, input.index_suffix, index_reader);
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

} // namespace
} // namespace snii::query
