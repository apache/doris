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

#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/io/metered_file_reader.h"
#include "storage/index/snii/query/internal/docid_conjunction.h"
#include "storage/index/snii/reader/logical_index_reader.h"
#include "storage/index/snii/reader/snii_segment_reader.h"
#include "storage/index/snii/writer/snii_compound_writer.h"
#include "storage/index/snii_query_test_util.h"

namespace doris::snii::query {
namespace {

using snii_test::MemoryFile;
using snii_test::ScopedEnv;
using snii_test::assert_ok;
using snii_test::make_term;

constexpr uint64_t kIndexId = 1;
constexpr const char* kIndexSuffix = "body";

class CountingReader final : public io::FileReader {
public:
    explicit CountingReader(io::FileReader* inner) : inner_(inner) {}

    Status read_at(uint64_t offset, size_t len, std::vector<uint8_t>* out) override {
        ++read_at_calls_;
        return inner_->read_at(offset, len, out);
    }

    Status read_batch(const std::vector<io::Range>& ranges,
                      std::vector<std::vector<uint8_t>>* outs) override {
        ++read_batch_calls_;
        batch_range_counts_.push_back(ranges.size());
        return inner_->read_batch(ranges, outs);
    }

    uint64_t size() const override { return inner_->size(); }
    const io::IoMetrics* io_metrics() const override { return inner_->io_metrics(); }

    void reset_counts() {
        read_at_calls_ = 0;
        read_batch_calls_ = 0;
        batch_range_counts_.clear();
    }

    uint64_t read_at_calls() const { return read_at_calls_; }
    uint64_t read_batch_calls() const { return read_batch_calls_; }
    const std::vector<size_t>& batch_range_counts() const { return batch_range_counts_; }

private:
    io::FileReader* inner_;
    uint64_t read_at_calls_ = 0;
    uint64_t read_batch_calls_ = 0;
    std::vector<size_t> batch_range_counts_;
};

Status write_index(MemoryFile* file, const std::vector<std::string>& terms,
                   uint32_t target_dict_block_bytes) {
    writer::SniiIndexInput input;
    input.index_id = kIndexId;
    input.index_suffix = kIndexSuffix;
    input.config = format::IndexConfig::kDocsPositions;
    input.doc_count = static_cast<uint32_t>(terms.size());
    input.target_dict_block_bytes = target_dict_block_bytes;
    input.terms.reserve(terms.size());
    for (size_t i = 0; i < terms.size(); ++i) {
        input.terms.push_back(
                make_term(terms[i], {{.docid = static_cast<uint32_t>(i), .positions = {0}}}));
    }

    writer::SniiCompoundWriter compound_writer(file);
    RETURN_IF_ERROR(compound_writer.add_logical_index(input));
    return compound_writer.finish();
}

std::vector<std::string> numbered_terms(size_t count) {
    std::vector<std::string> terms;
    terms.reserve(count);
    for (size_t i = 0; i < count; ++i) {
        std::string term = "term_";
        if (i < 10) {
            term.push_back('0');
        }
        term += std::to_string(i);
        terms.push_back(std::move(term));
    }
    return terms;
}

TEST(SniiQueryTermResolutionBatch, ResolvesColdDictBlocksInOnePhysicalBatch) {
    ScopedEnv dict_resident_max("SNII_DICT_RESIDENT_MAX", "0");

    MemoryFile file;
    writer::SniiIndexInput input;
    input.index_id = 1;
    input.index_suffix = "body";
    input.config = format::IndexConfig::kDocsPositions;
    input.doc_count = 6;
    input.target_dict_block_bytes = 1;
    input.terms = {
            make_term("alpha", {{.docid = 1, .positions = {0}}}),
            make_term("bravo", {{.docid = 2, .positions = {1}}}),
            make_term("kappa", {{.docid = 3, .positions = {2}}}),
            make_term("lambda", {{.docid = 4, .positions = {3}}}),
            make_term("omega", {{.docid = 5, .positions = {4}}}),
    };

    writer::SniiCompoundWriter compound_writer(&file);
    assert_ok(compound_writer.add_logical_index(input));
    assert_ok(compound_writer.finish());

    io::MeteredFileReader metered(&file, /*block_size=*/1);
    CountingReader counting(&metered);
    reader::SniiSegmentReader segment_reader;
    assert_ok(reader::SniiSegmentReader::open(&counting, &segment_reader));
    reader::LogicalIndexReader index_reader;
    assert_ok(segment_reader.open_index(input.index_id, input.index_suffix, &index_reader));
    ASSERT_EQ(index_reader.n_dict_blocks(), input.terms.size());
    metered.reset_metrics();
    counting.reset_counts();
    const std::vector<std::string> terms {"alpha", "kappa", "omega"};
    std::vector<internal::ResolvedQueryTerm> resolved;
    std::vector<uint8_t> found;
    assert_ok(internal::resolve_query_terms_batch(index_reader, terms, &resolved, &found));

    ASSERT_EQ(resolved.size(), terms.size());
    ASSERT_EQ(found, (std::vector<uint8_t> {1, 1, 1}));
    for (size_t i = 0; i < terms.size(); ++i) {
        EXPECT_EQ(resolved[i].entry.term, terms[i]);
        EXPECT_EQ(resolved[i].entry.df, 1U);
    }
    EXPECT_EQ(counting.read_at_calls(), 0U);
    EXPECT_EQ(counting.read_batch_calls(), 1U);
    EXPECT_EQ(counting.batch_range_counts(), (std::vector<size_t> {3}));
    EXPECT_EQ(metered.metrics().serial_rounds, 1U)
            << "independent cold DICT blocks must be fetched in one physical batch";
}

TEST(SniiQueryTermResolutionBatch, AlignsAbsentTermsAndReadsOneColdBlockSynchronously) {
    ScopedEnv dict_resident_max("SNII_DICT_RESIDENT_MAX", "0");
    ScopedEnv bsbf_resident_max("SNII_BSBF_RESIDENT_MAX", "0");

    MemoryFile file;
    assert_ok(write_index(&file, {"alpha", "kappa", "omega"},
                          /*target_dict_block_bytes=*/4096));

    io::MeteredFileReader metered(&file, /*block_size=*/1);
    CountingReader counting(&metered);
    reader::SniiSegmentReader segment_reader;
    assert_ok(reader::SniiSegmentReader::open(&counting, &segment_reader));
    reader::LogicalIndexReader index_reader;
    assert_ok(segment_reader.open_index(kIndexId, kIndexSuffix, &index_reader));
    ASSERT_EQ(index_reader.n_dict_blocks(), 1U);
    metered.reset_metrics();
    counting.reset_counts();
    const std::vector<std::string> terms {"aardvark", "alpha", "beta", "kappa",
                                          "lambda",   "omega", "zulu"};
    std::vector<internal::ResolvedQueryTerm> resolved;
    std::vector<uint8_t> found;
    assert_ok(internal::resolve_query_terms_batch(index_reader, terms, &resolved, &found));

    ASSERT_EQ(resolved.size(), terms.size());
    ASSERT_EQ(found, (std::vector<uint8_t> {0, 1, 0, 1, 0, 1, 0}));
    for (size_t i : {1U, 3U, 5U}) {
        EXPECT_EQ(resolved[i].entry.term, terms[i]);
        EXPECT_EQ(resolved[i].entry.df, 1U);
    }
    EXPECT_EQ(counting.read_at_calls(), 1U);
    EXPECT_EQ(counting.read_batch_calls(), 0U);
    EXPECT_EQ(metered.metrics().serial_rounds, 1U);
}

TEST(SniiQueryTermResolutionBatch, ResolvesResidentBlocksWithoutQueryIo) {
    ScopedEnv dict_resident_max("SNII_DICT_RESIDENT_MAX", "1048576");

    MemoryFile file;
    const std::vector<std::string> terms {"alpha", "kappa", "omega"};
    assert_ok(write_index(&file, terms, /*target_dict_block_bytes=*/1));

    io::MeteredFileReader metered(&file, /*block_size=*/1);
    CountingReader counting(&metered);
    reader::SniiSegmentReader segment_reader;
    assert_ok(reader::SniiSegmentReader::open(&counting, &segment_reader));
    reader::LogicalIndexReader index_reader;
    assert_ok(segment_reader.open_index(kIndexId, kIndexSuffix, &index_reader));
    ASSERT_EQ(index_reader.n_dict_blocks(), terms.size());

    metered.reset_metrics();
    counting.reset_counts();
    std::vector<internal::ResolvedQueryTerm> resolved;
    std::vector<uint8_t> found;
    assert_ok(internal::resolve_query_terms_batch(index_reader, terms, &resolved, &found));

    ASSERT_EQ(found, (std::vector<uint8_t> {1, 1, 1}));
    ASSERT_EQ(resolved.size(), terms.size());
    for (size_t i = 0; i < terms.size(); ++i) {
        EXPECT_EQ(resolved[i].entry.term, terms[i]);
    }
    EXPECT_EQ(counting.read_at_calls(), 0U);
    EXPECT_EQ(counting.read_batch_calls(), 0U);
    EXPECT_EQ(metered.metrics().serial_rounds, 0U);
}

TEST(SniiQueryTermResolutionBatch, ResolvesCompressedDictBlocksFromBatchBuffers) {
    ScopedEnv dict_resident_max("SNII_DICT_RESIDENT_MAX", "0");

    MemoryFile file;
    const std::vector<std::string> terms {
            "a" + std::string(4096, 'x'),
            "b" + std::string(4096, 'y'),
            "c" + std::string(4096, 'z'),
    };
    assert_ok(write_index(&file, terms, /*target_dict_block_bytes=*/1));

    io::MeteredFileReader metered(&file, /*block_size=*/1);
    reader::SniiSegmentReader segment_reader;
    assert_ok(reader::SniiSegmentReader::open(&metered, &segment_reader));
    reader::LogicalIndexReader index_reader;
    assert_ok(segment_reader.open_index(kIndexId, kIndexSuffix, &index_reader));
    ASSERT_EQ(index_reader.n_dict_blocks(), terms.size());
    ASSERT_LT(index_reader.section_refs().dict_region.length, terms.size() * terms.front().size());

    metered.reset_metrics();
    std::vector<internal::ResolvedQueryTerm> resolved;
    std::vector<uint8_t> found;
    assert_ok(internal::resolve_query_terms_batch(index_reader, terms, &resolved, &found));

    ASSERT_EQ(found, (std::vector<uint8_t> {1, 1, 1}));
    for (size_t i = 0; i < terms.size(); ++i) {
        EXPECT_EQ(resolved[i].entry.term, terms[i]);
    }
    EXPECT_EQ(metered.metrics().serial_rounds, 1U);
}

// GTest assertion macros inflate clang-tidy's branch count for this table-style I/O check.
// NOLINTNEXTLINE(readability-function-cognitive-complexity)
TEST(SniiQueryTermResolutionBatch, ResolvesSeventeenDisjointBlocksInTwoBoundedWaves) {
    ScopedEnv dict_resident_max("SNII_DICT_RESIDENT_MAX", "0");

    MemoryFile file;
    const std::vector<std::string> indexed_terms = numbered_terms(33);
    assert_ok(write_index(&file, indexed_terms, /*target_dict_block_bytes=*/1));

    io::MeteredFileReader metered(&file, /*block_size=*/1);
    CountingReader counting(&metered);
    reader::SniiSegmentReader segment_reader;
    assert_ok(reader::SniiSegmentReader::open(&counting, &segment_reader));
    reader::LogicalIndexReader index_reader;
    assert_ok(segment_reader.open_index(kIndexId, kIndexSuffix, &index_reader));
    ASSERT_EQ(index_reader.n_dict_blocks(), indexed_terms.size());

    std::vector<std::string> query_terms;
    query_terms.reserve(17);
    for (size_t i = 0; i < indexed_terms.size(); i += 2) {
        query_terms.push_back(indexed_terms[i]);
    }
    ASSERT_EQ(query_terms.size(), 17U);

    metered.reset_metrics();
    counting.reset_counts();
    std::vector<internal::ResolvedQueryTerm> resolved;
    std::vector<uint8_t> found;
    assert_ok(internal::resolve_query_terms_batch(index_reader, query_terms, &resolved, &found));

    ASSERT_EQ(found, std::vector<uint8_t>(query_terms.size(), 1));
    ASSERT_EQ(resolved.size(), query_terms.size());
    for (size_t i = 0; i < query_terms.size(); ++i) {
        EXPECT_EQ(resolved[i].entry.term, query_terms[i]);
        EXPECT_EQ(resolved[i].entry.df, 1U);
    }
    EXPECT_EQ(resolved.back().entry.term, "term_32");
    EXPECT_EQ(counting.read_at_calls(), 0U);
    EXPECT_EQ(counting.read_batch_calls(), 2U);
    EXPECT_EQ(counting.batch_range_counts(), (std::vector<size_t> {16, 1}));
    EXPECT_EQ(metered.metrics().serial_rounds, 2U);
    EXPECT_EQ(metered.metrics().range_gets, query_terms.size());
}

} // namespace
} // namespace doris::snii::query
