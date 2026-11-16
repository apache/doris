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

#include "storage/index/inverted/query/engine/phrase_query/phrase_set.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <map>
#include <memory>
#include <random>
#include <roaring/roaring.hh>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "storage/index/index_query_context.h"
#include "storage/index/inverted/common/query_info.h"
#include "storage/index/inverted/common/string_helper.h"
#include "storage/index/inverted/query/engine/phrase_query/phrase_query.h"
#include "storage/index/inverted/query/spi/fake_index_source.h"

namespace doris::segment_v2 {

using namespace inverted_index;
using index_query::testing::FakeIndexSource;

namespace {

const std::wstring kField = L"content";
const std::vector<std::string> kWords = {"hr",     "human", "resources", "coordinator",
                                         "intern", "co",    "ordinator", "payroll"};

// Rows of 2 to 24 words drawn mostly from the first few, so phrases share words and their words
// often meet without forming the phrase.
std::shared_ptr<FakeIndexSource> random_source(bool batches, uint32_t seed, uint32_t rows) {
    std::mt19937 random(seed);
    std::map<std::string, std::vector<FakeIndexSource::Posting>> postings;
    auto source = std::make_shared<FakeIndexSource>();
    source->batches = batches;
    source->set_doc_count(rows);
    for (uint32_t doc = 0; doc < rows; ++doc) {
        const uint32_t length = 2 + random() % 23;
        std::map<std::string, std::vector<uint32_t>> positions;
        for (uint32_t position = 0; position < length; ++position) {
            const double r = std::uniform_real_distribution<double>(0, 1)(random);
            const auto word = static_cast<size_t>(r * r * static_cast<double>(kWords.size()));
            positions[kWords[word]].push_back(position);
        }
        for (auto& [word, at] : positions) {
            postings[word].push_back({.doc = doc, .positions = std::move(at)});
        }
        source->norms[doc] = length;
    }
    for (auto& [word, list] : postings) {
        source->add(word, std::move(list));
    }
    return source;
}

std::vector<TermInfo> clauses(const std::vector<std::string>& words) {
    std::vector<TermInfo> result;
    for (size_t i = 0; i < words.size(); ++i) {
        TermInfo clause;
        clause.term = words[i];
        clause.position = static_cast<int32_t>(i);
        result.push_back(std::move(clause));
    }
    return result;
}

// Phrases sharing words, one repeating a word, one with a word no row holds, and sloppy ones.
std::vector<query_v2::PhraseSetPhrase> test_phrases() {
    return {{.clauses = clauses({"hr", "intern"})},
            {.clauses = clauses({"human", "resources", "coordinator"})},
            {.clauses = clauses({"hr", "coordinator", "intern"})},
            {.clauses = clauses({"hr", "co", "ordinator"})},
            {.clauses = clauses({"hr", "hr"})},
            {.clauses = clauses({"hr", "absent"})},
            {.clauses = clauses({"human", "resources", "coordinator"})},
            {.clauses = clauses({"intern", "hr"}), .slop = 2},
            {.clauses = clauses({"coordinator", "human"}), .slop = 3, .ordered = true}};
}

query_v2::QueryExecutionContext exec_context(const std::shared_ptr<FakeIndexSource>& source) {
    query_v2::QueryExecutionContext ctx;
    ctx.segment_num_rows = source->doc_count();
    ctx.field_sources.emplace(kField, source);
    return ctx;
}

query_v2::QueryPtr phrase_query(const IndexQueryContextPtr& context,
                                const query_v2::PhraseSetPhrase& phrase) {
    return std::make_shared<query_v2::PhraseQuery>(
            context, kField, phrase.clauses,
            index_query::PhraseQueryOptions {.slop = phrase.slop, .ordered = phrase.ordered});
}

std::vector<uint32_t> docs_of(const query_v2::ScorerPtr& scorer) {
    std::vector<uint32_t> docs;
    for (uint32_t doc = scorer->doc(); doc != query_v2::TERMINATED; doc = scorer->advance()) {
        docs.push_back(doc);
    }
    return docs;
}

// The rows any of the phrases matches, each run as its own phrase query.
roaring::Roaring expected_rows(const std::shared_ptr<FakeIndexSource>& source,
                               const std::vector<query_v2::PhraseSetPhrase>& phrases) {
    const auto context = std::make_shared<IndexQueryContext>();
    roaring::Roaring rows;
    for (const auto& phrase : phrases) {
        for (uint32_t doc :
             docs_of(phrase_query(context, phrase)->weight(false)->scorer(exec_context(source)))) {
            rows.add(doc);
        }
    }
    return rows;
}

std::vector<uint32_t> to_vector(const roaring::Roaring& rows) {
    std::vector<uint32_t> result(rows.cardinality());
    rows.toUint32Array(result.data());
    return result;
}

// The rows where the test field is NULL.
class FieldNulls final : public query_v2::NullBitmapResolver {
public:
    explicit FieldNulls(roaring::Roaring rows)
            : _rows(std::make_shared<roaring::Roaring>(std::move(rows))) {}
    std::shared_ptr<roaring::Roaring> bitmap_for(const query_v2::Scorer& /*scorer*/,
                                                 const std::string& logical_field) const override {
        return logical_field == StringHelper::to_string(kField) ? _rows : nullptr;
    }

private:
    std::shared_ptr<roaring::Roaring> _rows;
};

class PhraseSetTest : public testing::TestWithParam<bool> {};

} // namespace

TEST_P(PhraseSetTest, MatchesTheRowsOfItsPhrases) {
    const bool batches = GetParam();
    for (uint32_t seed = 1; seed <= 20; ++seed) {
        auto source = random_source(batches, seed, 400);
        const auto phrases = test_phrases();
        const roaring::Roaring expected = expected_rows(source, phrases);
        auto group = std::make_shared<query_v2::PhraseSetGroup>(kField, phrases);
        auto weight = query_v2::PhraseSetQuery(group).weight(false);

        EXPECT_EQ(docs_of(weight->scorer(exec_context(source))), to_vector(expected)) << seed;

        // Listed for a conjunction, only the candidates are checked.
        ASSERT_TRUE(weight->lists_rows(exec_context(source), ""));
        EXPECT_EQ(weight->listed_rows(exec_context(source), "", nullptr).true_rows, expected);
        roaring::Roaring candidates;
        for (uint32_t row = seed % 3; row < 400; row += 3) {
            candidates.add(row);
        }
        EXPECT_EQ(weight->listed_rows(exec_context(source), "", &candidates).true_rows,
                  expected & candidates)
                << seed;
        // A few candidates are looked up row by row in each word.
        roaring::Roaring few;
        for (uint32_t row = seed % 37; row < 400; row += 37) {
            few.add(row);
        }
        few |= roaring::Roaring::bitmapOf(1, 399);
        EXPECT_EQ(weight->listed_rows(exec_context(source), "", &few).true_rows, expected & few)
                << seed;
    }
}

TEST_P(PhraseSetTest, SeeksLandOnTheFirstMatchAtOrAfterTheTarget) {
    auto source = random_source(GetParam(), 7, 400);
    const auto phrases = test_phrases();
    const roaring::Roaring expected = expected_rows(source, phrases);
    auto group = std::make_shared<query_v2::PhraseSetGroup>(kField, phrases);
    auto weight = query_v2::PhraseSetQuery(group).weight(false);
    for (uint32_t step : {1U, 5U, 17U, 60U}) {
        auto scorer = weight->scorer(exec_context(source));
        for (uint32_t target = scorer->doc() + 1; target < 400; target += step) {
            if (target <= scorer->doc()) {
                continue;
            }
            auto it = expected.begin();
            it.equalorlarger(target);
            const uint32_t want = it == expected.end() ? query_v2::TERMINATED : *it;
            ASSERT_EQ(scorer->seek(target), want) << step << " " << target;
            if (want == query_v2::TERMINATED) {
                break;
            }
        }
    }
}

// A row the field is NULL in is UNKNOWN for the set, as for each of its phrases, whether the
// set scores rows or lists them, and on either source.
TEST_P(PhraseSetTest, ReportsTheFieldsNullRows) {
    auto source = random_source(GetParam(), 13, 300);
    const auto phrases = test_phrases();
    const roaring::Roaring expected = expected_rows(source, phrases);
    roaring::Roaring nulls;
    for (uint32_t row = 0; row < 300; row += 7) {
        if (!expected.contains(row)) {
            nulls.add(row);
        }
    }
    ASSERT_FALSE(nulls.isEmpty());
    FieldNulls resolver(nulls);
    auto ctx = exec_context(source);
    ctx.null_resolver = &resolver;
    auto weight =
            query_v2::PhraseSetQuery(std::make_shared<query_v2::PhraseSetGroup>(kField, phrases))
                    .weight(false);

    auto scorer = weight->scorer(ctx);
    ASSERT_TRUE(scorer->has_null_bitmap(&resolver));
    const auto* reported = scorer->get_null_bitmap(&resolver);
    ASSERT_NE(reported, nullptr);
    EXPECT_EQ(*reported, nulls);
    EXPECT_EQ(docs_of(scorer), to_vector(expected));

    const auto listed = weight->listed_rows(ctx, "", nullptr);
    EXPECT_EQ(listed.true_rows, expected);
    EXPECT_EQ(listed.null_rows, nulls);
}

INSTANTIATE_TEST_SUITE_P(Sources, PhraseSetTest, testing::Bool(),
                         [](const testing::TestParamInfo<bool>& info) {
                             return info.param ? "Batched" : "Streamed";
                         });

// On a source read one document at a time, the set opens each word once, where the phrases
// open their words once each.
TEST(PhraseSetStreamedTest, OpensEachWordOnce) {
    auto source = random_source(false, 5, 300);
    const auto phrases = test_phrases();
    auto group = std::make_shared<query_v2::PhraseSetGroup>(kField, phrases);
    auto scorer = query_v2::PhraseSetQuery(group).weight(false)->scorer(exec_context(source));
    EXPECT_FALSE(docs_of(scorer).empty());
    std::map<std::string, size_t> opens;
    for (const auto& term : source->opened) {
        ++opens[term];
    }
    // Once for its documents and once for its positions.
    for (const auto& [term, count] : opens) {
        EXPECT_LE(count, 2) << term;
    }
}

} // namespace doris::segment_v2
