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
#include <cstdio>
#include <string>
#include <string_view>
#include <vector>

#include "common/status.h"
#include "storage/index/inverted/query/exec/term_pattern.h"
#include "storage/index/inverted/snii/reader/logical_index_reader.h"
#include "storage/index/inverted/snii/reader/snii_index_source.h"
#include "storage/index/inverted/snii/reader/snii_segment_reader.h"
#include "storage/index/inverted/snii/snii_query_test_util.h"
#include "storage/index/inverted/snii/writer/snii_compound_writer.h"

namespace doris::snii::reader {
namespace {

using snii_test::assert_ok;
using snii_test::make_term;
using snii_test::MemoryFile;
using snii_test::PostingDoc;

constexpr uint64_t kIndexId = 7;
constexpr const char* kIndexSuffix = "Body";

// Three thousand keys without the literal and three with it, far apart in key order, cut into
// small DICT blocks so the literal's blocks are a few among many.
std::vector<std::string> corpus() {
    std::vector<std::string> terms;
    for (uint32_t i = 0; i < 3000; ++i) {
        char key[16];
        std::snprintf(key, sizeof(key), "k%05u", i);
        terms.emplace_back(key);
    }
    terms.emplace_back("acache");
    terms.emplace_back("mcached");
    terms.emplace_back("zcache");
    std::ranges::sort(terms);
    return terms;
}

// Three thousand random base62 keys of twenty bytes: no literal structure, so nearly every gram
// is its own, and a summary would cost more than the dictionary.
std::vector<std::string> random_corpus(uint32_t count = 3000) {
    constexpr std::string_view kAlphabet =
            "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    uint64_t state = 0x9E3779B97F4A7C15ULL;
    std::vector<std::string> terms;
    for (uint32_t i = 0; i < count; ++i) {
        std::string key;
        for (int j = 0; j < 20; ++j) {
            state = state * 6364136223846793005ULL + 1442695040888963407ULL;
            key.push_back(kAlphabet[(state >> 33) % kAlphabet.size()]);
        }
        terms.push_back(std::move(key));
    }
    std::ranges::sort(terms);
    terms.erase(std::ranges::unique(terms).begin(), terms.end());
    return terms;
}

Status build_terms(const std::vector<std::string>& terms, bool with_summary, MemoryFile* file,
                   SniiSegmentReader* segment_reader, LogicalIndexReader* index_reader) {
    writer::SniiIndexInput input;
    input.index_id = kIndexId;
    input.index_suffix = kIndexSuffix;
    input.config = format::IndexConfig::kDocsPositions;
    input.doc_count = static_cast<uint32_t>(terms.size());
    for (uint32_t docid = 0; docid < terms.size(); ++docid) {
        input.terms.push_back(
                make_term(terms[docid], {PostingDoc {.docid = docid, .positions = {0}}}));
    }
    input.target_dict_block_bytes = 512;
    input.dict_gram_summary = with_summary;

    writer::SniiCompoundWriter writer(file);
    RETURN_IF_ERROR(writer.add_logical_index(input));
    RETURN_IF_ERROR(writer.finish());
    RETURN_IF_ERROR(SniiSegmentReader::open(file, segment_reader));
    return segment_reader->open_index(kIndexId, kIndexSuffix, index_reader);
}

Status build(bool with_summary, MemoryFile* file, SniiSegmentReader* segment_reader,
             LogicalIndexReader* index_reader) {
    return build_terms(corpus(), with_summary, file, segment_reader, index_reader);
}

// Expands `pattern` through `source` and counts the DICT blocks the expansion opened. The first
// expansion of a source with an empty enumeration prefix also opens one block to check for
// internal terms, so every test warms its source with a literal no key holds before counting.
Status expand(SniiIndexSource& source, index_query::TermPatternKind kind, std::string_view pattern,
              std::vector<std::string>* terms, uint64_t* blocks_visited) {
    index_query::TermPattern term_pattern;
    RETURN_IF_ERROR(index_query::TermPattern::create(kind, pattern, &term_pattern));
    const uint64_t before = prefix_walk_block_count();
    RETURN_IF_ERROR(source.expand_terms(term_pattern, 0, terms));
    *blocks_visited = prefix_walk_block_count() - before;
    return Status::OK();
}

Status warm(SniiIndexSource& source) {
    std::vector<std::string> terms;
    uint64_t visited = 0;
    RETURN_IF_ERROR(
            expand(source, index_query::TermPatternKind::kContains, "zqzqz", &terms, &visited));
    return terms.empty() ? Status::OK()
                         : Status::InternalError("the warm-up pattern matched a term");
}

// "acache" sorts first and "mcached" / "zcache" after every "k" key, so the literal sits in the
// first block and in the last one or two.
const std::vector<std::string> kLiteralTerms {"acache", "mcached", "zcache"};

} // namespace

TEST(SniiDictGramSummaryWalk, ExpansionOpensOnlyTheBlocksWithTheLiteral) {
    MemoryFile file;
    SniiSegmentReader segment_reader;
    LogicalIndexReader index_reader;
    assert_ok(build(/*with_summary=*/true, &file, &segment_reader, &index_reader));
    ASSERT_GT(index_reader.n_dict_blocks(), 20U);
    SniiIndexSource source(index_reader);
    assert_ok(warm(source));

    std::vector<std::string> terms;
    uint64_t visited = 0;
    assert_ok(expand(source, index_query::TermPatternKind::kContains, "cache", &terms, &visited));
    EXPECT_EQ(terms, kLiteralTerms);
    EXPECT_GE(visited, 2U);
    EXPECT_LE(visited, 3U);

    // A suffix (the first token of MATCH_PHRASE_EDGE) names its text the same way.
    assert_ok(expand(source, index_query::TermPatternKind::kSuffix, "cache", &terms, &visited));
    EXPECT_EQ(terms, (std::vector<std::string> {"acache", "zcache"}));
    EXPECT_GE(visited, 2U);
    EXPECT_LE(visited, 3U);

    // The literal's last gram sits in one block only.
    assert_ok(expand(source, index_query::TermPatternKind::kContains, "cached", &terms, &visited));
    EXPECT_EQ(terms, (std::vector<std::string> {"mcached"}));
    EXPECT_EQ(visited, 1U);

    // A gram no key holds opens no block.
    assert_ok(expand(source, index_query::TermPatternKind::kContains, "zqzqz", &terms, &visited));
    EXPECT_TRUE(terms.empty());
    EXPECT_EQ(visited, 0U);

    // A literal shorter than a gram cannot use the summary: the whole dictionary is walked.
    assert_ok(expand(source, index_query::TermPatternKind::kContains, "ca", &terms, &visited));
    EXPECT_EQ(terms, kLiteralTerms);
    EXPECT_EQ(visited, index_reader.n_dict_blocks());

    // A prefix glob keeps its prefix seek and never consults the summary: its ten keys span a
    // few of the small blocks, starting at the block the sampled-term index names.
    assert_ok(expand(source, index_query::TermPatternKind::kWildcard, "k0299*", &terms, &visited));
    EXPECT_EQ(terms.size(), 10U);
    EXPECT_EQ(terms.front(), "k02990");
    EXPECT_LE(visited, 4U);
}

// Random keys get no summary: it would exceed its share of the dictionary, so the whole
// dictionary is walked as before, and the expansion is still right.
TEST(SniiDictGramSummaryWalk, RandomKeysGetNoSummary) {
    MemoryFile file;
    SniiSegmentReader segment_reader;
    LogicalIndexReader index_reader;
    const std::vector<std::string> terms = random_corpus();
    assert_ok(build_terms(terms, /*with_summary=*/true, &file, &segment_reader, &index_reader));
    ASSERT_GT(index_reader.n_dict_blocks(), 20U);
    SniiIndexSource source(index_reader);
    assert_ok(warm(source));

    const std::string literal = terms[terms.size() / 2].substr(5, 6);
    std::vector<std::string> expanded;
    uint64_t visited = 0;
    assert_ok(
            expand(source, index_query::TermPatternKind::kContains, literal, &expanded, &visited));
    EXPECT_FALSE(expanded.empty());
    for (const std::string& term : expanded) {
        EXPECT_NE(term.find(literal), std::string::npos) << term;
    }
    EXPECT_EQ(visited, index_reader.n_dict_blocks());
}

// Keys spreading over more grams than kDictGramSummaryMaxGrams stop the collection: no summary,
// the whole dictionary is walked, the expansion is still right.
TEST(SniiDictGramSummaryWalk, TooManyGramsStopTheSummary) {
    MemoryFile file;
    SniiSegmentReader segment_reader;
    LogicalIndexReader index_reader;
    const std::vector<std::string> terms = random_corpus(12000);
    assert_ok(build_terms(terms, /*with_summary=*/true, &file, &segment_reader, &index_reader));
    SniiIndexSource source(index_reader);
    assert_ok(warm(source));

    const std::string literal = terms[terms.size() / 3].substr(7, 5);
    std::vector<std::string> expanded;
    uint64_t visited = 0;
    assert_ok(
            expand(source, index_query::TermPatternKind::kContains, literal, &expanded, &visited));
    EXPECT_FALSE(expanded.empty());
    EXPECT_EQ(visited, index_reader.n_dict_blocks());
}

TEST(SniiDictGramSummaryWalk, WithoutTheSummaryEveryBlockIsWalked) {
    MemoryFile file;
    SniiSegmentReader segment_reader;
    LogicalIndexReader index_reader;
    assert_ok(build(/*with_summary=*/false, &file, &segment_reader, &index_reader));
    SniiIndexSource source(index_reader);
    assert_ok(warm(source));

    std::vector<std::string> terms;
    uint64_t visited = 0;
    assert_ok(expand(source, index_query::TermPatternKind::kContains, "cache", &terms, &visited));
    EXPECT_EQ(terms, kLiteralTerms);
    EXPECT_EQ(visited, index_reader.n_dict_blocks());
}

} // namespace doris::snii::reader
