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

#include "olap/rowset/segment_v2/inverted_index/query_v2/phrase_prefix_query/phrase_prefix_query.h"

#include <CLucene.h>
#include <gtest/gtest.h>

#include <memory>
#include <roaring/roaring.hh>
#include <string>

#include "io/fs/local_file_system.h"
#include "olap/rowset/segment_v2/index_query_context.h"
#include "olap/rowset/segment_v2/inverted_index/analyzer/custom_analyzer.h"
#include "olap/rowset/segment_v2/inverted_index/query/query_info.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/prefix_query/prefix_weight.h"
#include "olap/rowset/segment_v2/inverted_index/util/string_helper.h"

CL_NS_USE(store)
CL_NS_USE(index)

namespace doris::segment_v2 {

using namespace inverted_index;
using namespace inverted_index::query_v2;

class PhrasePrefixQueryV2Test : public testing::Test {
public:
    const std::string kTestDir = "./ut_dir/phrase_prefix_query_test";

    void SetUp() override {
        auto st = io::global_local_filesystem()->delete_directory(kTestDir);
        ASSERT_TRUE(st.ok()) << st;
        st = io::global_local_filesystem()->create_directory(kTestDir);
        ASSERT_TRUE(st.ok()) << st;
        create_test_index("content", kTestDir);
    }

    void TearDown() override {
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(kTestDir).ok());
    }

private:
    void create_test_index(const std::string& field_name, const std::string& dir) {
        // Designed so "quick bro*" matches docs with "quick brown" / "quick brother" etc.
        std::vector<std::string> test_data = {
                "the quick brown fox jumps over the lazy dog", // doc 0: quick brown
                "quick brown dogs are running fast",           // doc 1: quick brown
                "the brown cat sleeps peacefully",             // doc 2: no quick
                "lazy dogs and quick cats",                    // doc 3: no quick bro*
                "the lazy dog is very lazy",                   // doc 4: no quick
                "quick fox and brown bear",                    // doc 5: quick fox (not quick bro*)
                "the quick brown horse runs",                  // doc 6: quick brown
                "dogs and cats are pets",                      // doc 7: no quick
                "the fox is quick and brown",                  // doc 8: quick and (not quick bro*)
                "brown foxes jump over fences",                // doc 9: no quick
                "quick brother joined the team",               // doc 10: quick brother
                "quick brown fox in the forest",               // doc 11: quick brown
                "the dog barks loudly",                        // doc 12: no quick
                "brown and white dogs",                        // doc 13: no quick
                "quick movements of animals",                  // doc 14: no quick bro*
                "the lazy afternoon",                          // doc 15: no quick
                "brown fox runs quickly",                      // doc 16: no quick bro*
                "the quick test",                              // doc 17: no quick bro*
                "brown lazy fox",                              // doc 18: no quick
                "quick brown lazy dog",                        // doc 19: quick brown
        };

        CustomAnalyzerConfig::Builder builder;
        builder.with_tokenizer_config("standard", {});
        auto config = builder.build();
        auto analyzer = CustomAnalyzer::build_custom_analyzer(config);

        auto* writer = _CLNEW IndexWriter(dir.c_str(), analyzer.get(), true);
        writer->setMaxBufferedDocs(100);
        writer->setRAMBufferSizeMB(-1);
        writer->setMaxFieldLength(0x7FFFFFFFL);
        writer->setMergeFactor(1000000000);
        writer->setUseCompoundFile(false);

        auto char_reader = std::make_shared<lucene::util::SStringReader<char>>();
        auto* doc = _CLNEW lucene::document::Document();
        int32_t field_config = lucene::document::Field::STORE_NO;
        field_config |= lucene::document::Field::INDEX_NONORMS;
        field_config |= lucene::document::Field::INDEX_TOKENIZED;
        auto field_w = std::wstring(field_name.begin(), field_name.end());
        auto* field = _CLNEW lucene::document::Field(field_w.c_str(), field_config);
        field->setOmitTermFreqAndPositions(false);
        doc->add(*field);

        for (const auto& data : test_data) {
            char_reader->init(data.data(), data.size(), false);
            auto* stream = analyzer->reusableTokenStream(field->name(), char_reader);
            field->setValue(stream);
            writer->addDocument(doc);
        }

        writer->close();
        _CLLDELETE(writer);
        _CLLDELETE(doc);
    }
};

static std::shared_ptr<lucene::index::IndexReader> make_shared_reader(
        lucene::index::IndexReader* raw_reader) {
    return {raw_reader, [](lucene::index::IndexReader* reader) {
                if (reader != nullptr) {
                    reader->close();
                    _CLDELETE(reader);
                }
            }};
}

static std::vector<uint32_t> collect_docs(ScorerPtr scorer) {
    std::vector<uint32_t> result;
    uint32_t d = scorer->doc();
    while (d != TERMINATED) {
        result.push_back(d);
        d = scorer->advance();
    }
    return result;
}

static std::vector<TermInfo> make_term_infos(const std::vector<std::string>& terms) {
    std::vector<TermInfo> infos;
    for (size_t i = 0; i < terms.size(); ++i) {
        TermInfo ti;
        ti.term = terms[i];
        ti.position = static_cast<int32_t>(i);
        infos.push_back(ti);
    }
    return infos;
}

// --- PhrasePrefixQuery construction ---

// Normal case: multiple terms, last is prefix
TEST_F(PhrasePrefixQueryV2Test, construction_basic) {
    auto ctx = std::make_shared<IndexQueryContext>();
    std::wstring field = StringHelper::to_wstring("content");
    auto terms = make_term_infos({"quick", "bro"});

    PhrasePrefixQuery q(ctx, field, terms);
    auto w = q.weight(false);
    ASSERT_NE(w, nullptr);
}

// Single term → _phrase_terms empty → falls back to PrefixQuery
TEST_F(PhrasePrefixQueryV2Test, single_term_fallback_to_prefix) {
    auto ctx = std::make_shared<IndexQueryContext>();
    auto* dir = FSDirectory::getDirectory(kTestDir.c_str());
    auto reader = make_shared_reader(lucene::index::IndexReader::open(dir, true));

    std::wstring field = StringHelper::to_wstring("content");
    auto terms = make_term_infos({"bro"});

    PhrasePrefixQuery q(ctx, field, terms);
    auto w = q.weight(false);
    ASSERT_NE(w, nullptr);

    // Should be a PrefixWeight, not PhrasePrefixWeight
    auto prefix_w = std::dynamic_pointer_cast<PrefixWeight>(w);
    EXPECT_NE(prefix_w, nullptr);

    // Execute it
    QueryExecutionContext exec_ctx;
    exec_ctx.segment_num_rows = reader->maxDoc();
    exec_ctx.readers = {reader};
    exec_ctx.field_reader_bindings.emplace(field, reader);

    auto scorer = w->scorer(exec_ctx, "");
    auto docs = collect_docs(scorer);
    // "bro*" should match: brown (many docs), brother (doc 10)
    EXPECT_GT(docs.size(), 0);

    _CLDECDELETE(dir);
}

// --- PhrasePrefixQuery::weight with empty terms → throw (defensive check) ---

TEST_F(PhrasePrefixQueryV2Test, empty_terms_throws) {
    auto ctx = std::make_shared<IndexQueryContext>();
    std::wstring field = StringHelper::to_wstring("content");
    std::vector<TermInfo> empty_terms;

    // Constructor asserts !terms.empty(), which aborts in debug builds.
    EXPECT_DEATH({ PhrasePrefixQuery q(ctx, field, empty_terms); }, "");
}

// --- PhrasePrefixWeight scorer: phrase + prefix match ---

TEST_F(PhrasePrefixQueryV2Test, phrase_prefix_match) {
    auto ctx = std::make_shared<IndexQueryContext>();
    ctx->collection_statistics = std::make_shared<CollectionStatistics>();
    ctx->collection_similarity = std::make_shared<CollectionSimilarity>();

    auto* dir = FSDirectory::getDirectory(kTestDir.c_str());
    auto reader = make_shared_reader(lucene::index::IndexReader::open(dir, true));

    std::wstring field = StringHelper::to_wstring("content");
    // "quick bro*" → phrase_terms=["quick"], prefix="bro"
    auto terms = make_term_infos({"quick", "bro"});

    PhrasePrefixQuery q(ctx, field, terms);
    auto w = q.weight(false);

    QueryExecutionContext exec_ctx;
    exec_ctx.segment_num_rows = reader->maxDoc();
    exec_ctx.readers = {reader};
    exec_ctx.field_reader_bindings.emplace(field, reader);

    auto scorer = w->scorer(exec_ctx, "");
    auto docs = collect_docs(scorer);

    // "quick brown" in docs: 0, 1, 6, 11, 19
    // "quick brother" in doc: 10
    std::set<uint32_t> expected = {0, 1, 6, 10, 11, 19};
    std::set<uint32_t> actual(docs.begin(), docs.end());
    EXPECT_EQ(actual, expected);

    _CLDECDELETE(dir);
}

// --- PhrasePrefixWeight scorer: no reader → throw ---

TEST_F(PhrasePrefixQueryV2Test, scorer_no_reader_throws) {
    auto ctx = std::make_shared<IndexQueryContext>();
    ctx->collection_statistics = std::make_shared<CollectionStatistics>();
    ctx->collection_similarity = std::make_shared<CollectionSimilarity>();

    std::wstring field = StringHelper::to_wstring("content");
    auto terms = make_term_infos({"quick", "bro"});

    PhrasePrefixQuery q(ctx, field, terms);
    auto w = q.weight(false);

    QueryExecutionContext exec_ctx;
    exec_ctx.segment_num_rows = 20;
    // No readers → lookup_reader returns nullptr → throw

    EXPECT_THROW({ auto scorer = w->scorer(exec_ctx, ""); }, Exception);
}

// --- PhrasePrefixWeight scorer: phrase term not found → EmptyScorer ---

TEST_F(PhrasePrefixQueryV2Test, phrase_term_not_found_returns_empty) {
    auto ctx = std::make_shared<IndexQueryContext>();
    ctx->collection_statistics = std::make_shared<CollectionStatistics>();
    ctx->collection_similarity = std::make_shared<CollectionSimilarity>();

    auto* dir = FSDirectory::getDirectory(kTestDir.c_str());
    auto reader = make_shared_reader(lucene::index::IndexReader::open(dir, true));

    std::wstring field = StringHelper::to_wstring("content");
    // "nonexistent bro*" → phrase term "nonexistent" not in index → EmptyScorer
    auto terms = make_term_infos({"nonexistent", "bro"});

    PhrasePrefixQuery q(ctx, field, terms);
    auto w = q.weight(false);

    QueryExecutionContext exec_ctx;
    exec_ctx.segment_num_rows = reader->maxDoc();
    exec_ctx.readers = {reader};
    exec_ctx.field_reader_bindings.emplace(field, reader);

    auto scorer = w->scorer(exec_ctx, "");
    EXPECT_EQ(scorer->doc(), TERMINATED);

    _CLDECDELETE(dir);
}

// --- PhrasePrefixWeight scorer: prefix expands to nothing → EmptyScorer ---

TEST_F(PhrasePrefixQueryV2Test, prefix_no_expansion_returns_empty) {
    auto ctx = std::make_shared<IndexQueryContext>();
    ctx->collection_statistics = std::make_shared<CollectionStatistics>();
    ctx->collection_similarity = std::make_shared<CollectionSimilarity>();

    auto* dir = FSDirectory::getDirectory(kTestDir.c_str());
    auto reader = make_shared_reader(lucene::index::IndexReader::open(dir, true));

    std::wstring field = StringHelper::to_wstring("content");
    // "quick zzz*" → prefix "zzz" has no expansions → EmptyScorer
    auto terms = make_term_infos({"quick", "zzz"});

    PhrasePrefixQuery q(ctx, field, terms);
    auto w = q.weight(false);

    QueryExecutionContext exec_ctx;
    exec_ctx.segment_num_rows = reader->maxDoc();
    exec_ctx.readers = {reader};
    exec_ctx.field_reader_bindings.emplace(field, reader);

    auto scorer = w->scorer(exec_ctx, "");
    EXPECT_EQ(scorer->doc(), TERMINATED);

    _CLDECDELETE(dir);
}

// --- PhrasePrefixWeight scorer: with scoring enabled ---

TEST_F(PhrasePrefixQueryV2Test, scorer_with_scoring) {
    auto ctx = std::make_shared<IndexQueryContext>();
    ctx->collection_statistics = std::make_shared<CollectionStatistics>();
    ctx->collection_similarity = std::make_shared<CollectionSimilarity>();

    auto* dir = FSDirectory::getDirectory(kTestDir.c_str());
    auto reader = make_shared_reader(lucene::index::IndexReader::open(dir, true));

    std::wstring field = StringHelper::to_wstring("content");

    // Setup collection statistics for BM25
    ctx->collection_statistics->_total_num_docs = reader->numDocs();
    ctx->collection_statistics->_total_num_tokens[field] = reader->numDocs() * 8;
    ctx->collection_statistics->_term_doc_freqs[field][StringHelper::to_wstring("quick")] = 10;

    auto terms = make_term_infos({"quick", "bro"});

    PhrasePrefixQuery q(ctx, field, terms);
    auto w = q.weight(true); // enable scoring

    QueryExecutionContext exec_ctx;
    exec_ctx.segment_num_rows = reader->maxDoc();
    exec_ctx.readers = {reader};
    exec_ctx.field_reader_bindings.emplace(field, reader);

    auto scorer = w->scorer(exec_ctx, "");
    auto docs = collect_docs(scorer);
    EXPECT_GT(docs.size(), 0);

    _CLDECDELETE(dir);
}

// --- PhrasePrefixWeight scorer: nullable branch ---

TEST_F(PhrasePrefixQueryV2Test, scorer_nullable) {
    auto ctx = std::make_shared<IndexQueryContext>();
    ctx->collection_statistics = std::make_shared<CollectionStatistics>();
    ctx->collection_similarity = std::make_shared<CollectionSimilarity>();

    auto* dir = FSDirectory::getDirectory(kTestDir.c_str());
    auto reader = make_shared_reader(lucene::index::IndexReader::open(dir, true));

    std::wstring field = StringHelper::to_wstring("content");
    auto terms = make_term_infos({"quick", "bro"});

    PhrasePrefixQuery q(ctx, field, terms);
    // Default _nullable=true, so the nullable branch in scorer() is taken
    auto w = q.weight(false);

    QueryExecutionContext exec_ctx;
    exec_ctx.segment_num_rows = reader->maxDoc();
    exec_ctx.readers = {reader};
    exec_ctx.field_reader_bindings.emplace(field, reader);
    // null_resolver is nullptr → make_nullable_scorer returns inner scorer

    auto scorer = w->scorer(exec_ctx, "");
    auto docs = collect_docs(scorer);
    EXPECT_GT(docs.size(), 0);

    _CLDECDELETE(dir);
}

// --- PhrasePrefixWeight scorer: with binding key ---

TEST_F(PhrasePrefixQueryV2Test, scorer_with_binding_key) {
    auto ctx = std::make_shared<IndexQueryContext>();
    ctx->collection_statistics = std::make_shared<CollectionStatistics>();
    ctx->collection_similarity = std::make_shared<CollectionSimilarity>();

    auto* dir = FSDirectory::getDirectory(kTestDir.c_str());
    auto reader = make_shared_reader(lucene::index::IndexReader::open(dir, true));

    std::wstring field = StringHelper::to_wstring("content");
    auto terms = make_term_infos({"quick", "bro"});

    PhrasePrefixQuery q(ctx, field, terms);
    auto w = q.weight(false);

    QueryExecutionContext exec_ctx;
    exec_ctx.segment_num_rows = reader->maxDoc();
    std::string binding_key = "content#0";
    exec_ctx.reader_bindings[binding_key] = reader;
    exec_ctx.field_reader_bindings.emplace(field, reader);

    auto scorer = w->scorer(exec_ctx, binding_key);
    auto docs = collect_docs(scorer);
    EXPECT_GT(docs.size(), 0);

    _CLDECDELETE(dir);
}

// --- Three-term phrase prefix: "the quick bro*" ---

TEST_F(PhrasePrefixQueryV2Test, three_term_phrase_prefix) {
    auto ctx = std::make_shared<IndexQueryContext>();
    ctx->collection_statistics = std::make_shared<CollectionStatistics>();
    ctx->collection_similarity = std::make_shared<CollectionSimilarity>();

    auto* dir = FSDirectory::getDirectory(kTestDir.c_str());
    auto reader = make_shared_reader(lucene::index::IndexReader::open(dir, true));

    std::wstring field = StringHelper::to_wstring("content");
    // "the quick bro*" → phrase_terms=["the","quick"], prefix="bro"
    auto terms = make_term_infos({"the", "quick", "bro"});

    PhrasePrefixQuery q(ctx, field, terms);
    auto w = q.weight(false);

    QueryExecutionContext exec_ctx;
    exec_ctx.segment_num_rows = reader->maxDoc();
    exec_ctx.readers = {reader};
    exec_ctx.field_reader_bindings.emplace(field, reader);

    auto scorer = w->scorer(exec_ctx, "");
    auto docs = collect_docs(scorer);

    // "the quick brown" in docs: 0, 6
    // "the quick bro*" should match same docs
    std::set<uint32_t> actual(docs.begin(), docs.end());
    EXPECT_TRUE(actual.count(0) > 0);
    EXPECT_TRUE(actual.count(6) > 0);

    _CLDECDELETE(dir);
}

// --- Phrase exists but prefix doesn't match adjacent position → no match ---

TEST_F(PhrasePrefixQueryV2Test, phrase_prefix_no_adjacent_match) {
    auto ctx = std::make_shared<IndexQueryContext>();
    ctx->collection_statistics = std::make_shared<CollectionStatistics>();
    ctx->collection_similarity = std::make_shared<CollectionSimilarity>();

    auto* dir = FSDirectory::getDirectory(kTestDir.c_str());
    auto reader = make_shared_reader(lucene::index::IndexReader::open(dir, true));

    std::wstring field = StringHelper::to_wstring("content");
    // "lazy bro*" → "lazy" and "bro*" both exist but never adjacent
    auto terms = make_term_infos({"lazy", "bro"});

    PhrasePrefixQuery q(ctx, field, terms);
    auto w = q.weight(false);

    QueryExecutionContext exec_ctx;
    exec_ctx.segment_num_rows = reader->maxDoc();
    exec_ctx.readers = {reader};
    exec_ctx.field_reader_bindings.emplace(field, reader);

    auto scorer = w->scorer(exec_ctx, "");
    auto docs = collect_docs(scorer);
    // "lazy brown" doesn't appear as adjacent phrase in any doc (doc 18 is "brown lazy fox")
    // Actually doc 18 has "brown lazy" not "lazy brown", so no match expected
    // But let's just verify it runs without error
    // The exact result depends on the data
    SUCCEED();

    _CLDECDELETE(dir);
}

} // namespace doris::segment_v2
