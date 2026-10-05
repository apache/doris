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

#include "storage/index/inverted/query_v2/phrase_query/phrase_query.h"

#include <gtest/gtest.h>

#include <array>
#include <map>
#include <memory>
#include <numeric>
#include <roaring/roaring.hh>
#include <set>
#include <string>
#include <vector>

#include "common/status.h"
#include "io/fs/local_file_system.h"
#include "storage/index/index_query_context.h"
#include "storage/index/inverted/analyzer/custom_analyzer.h"
#include "storage/index/inverted/query/query_info.h"
#include "storage/index/inverted/query_v2/phrase_prefix_query/phrase_prefix_query.h"
#include "storage/index/inverted/query_v2/phrase_query/multi_phrase_query.h"
#include "storage/index/inverted/query_v2/phrase_query/phrase_scorer.h"
#include "storage/index/inverted/query_v2/phrase_query/phrase_weight.h"
#include "storage/index/inverted/similarity/bm25_similarity.h"
#include "storage/index/inverted/spi/clucene_index_source.h"
#include "storage/index/inverted/spi/clucene_postings_cursor.h"
#include "storage/index/inverted/util/string_helper.h"
#include "storage/index/query/fake_index_source.h"

CL_NS_USE(search)
CL_NS_USE(store)
CL_NS_USE(index)

namespace doris::segment_v2 {

using namespace inverted_index;

class PhraseQueryV2Test : public testing::Test {
public:
    const std::string kTestDir = "./ut_dir/phrase_query_test";

    void SetUp() override {
        auto st = io::global_local_filesystem()->delete_directory(kTestDir);
        ASSERT_TRUE(st.ok()) << st;
        st = io::global_local_filesystem()->create_directory(kTestDir);
        ASSERT_TRUE(st.ok()) << st;
        std::string field_name = "content";
        create_test_index(field_name, kTestDir);
    }

    void TearDown() override {
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(kTestDir).ok());
    }

private:
    void create_test_index(const std::string& field_name, const std::string& dir) {
        std::vector<std::string> test_data = {"the quick brown fox jumps over the lazy dog",
                                              "quick brown dogs are running fast",
                                              "the brown cat sleeps peacefully",
                                              "lazy dogs and quick cats",
                                              "the lazy dog is very lazy",
                                              "quick fox and brown bear",
                                              "the quick brown horse runs",
                                              "dogs and cats are pets",
                                              "the fox is quick and brown",
                                              "brown foxes jump over fences",
                                              "lazy cat sleeps all day",
                                              "quick brown fox in the forest",
                                              "the dog barks loudly",
                                              "brown and white dogs",
                                              "quick movements of animals",
                                              "the lazy afternoon",
                                              "brown fox runs quickly",
                                              "the quick test",
                                              "brown lazy fox",
                                              "quick brown lazy dog"};

        CustomAnalyzerConfig::Builder builder;
        builder.with_tokenizer_config("standard", {});
        auto custom_analyzer_config = builder.build();
        auto custom_analyzer = CustomAnalyzer::build_custom_analyzer(custom_analyzer_config);

        auto* indexwriter =
                _CLNEW lucene::index::IndexWriter(dir.c_str(), custom_analyzer.get(), true);
        indexwriter->setMaxBufferedDocs(100);
        indexwriter->setRAMBufferSizeMB(-1);
        indexwriter->setMaxFieldLength(0x7FFFFFFFL);
        indexwriter->setMergeFactor(1000000000);
        indexwriter->setUseCompoundFile(false);

        auto char_string_reader = std::make_shared<lucene::util::SStringReader<char>>();

        auto* doc = _CLNEW lucene::document::Document();
        int32_t field_config = lucene::document::Field::STORE_NO;
        field_config |= lucene::document::Field::INDEX_NONORMS;
        field_config |= lucene::document::Field::INDEX_TOKENIZED;
        auto field_name_w = std::wstring(field_name.begin(), field_name.end());
        auto* field = _CLNEW lucene::document::Field(field_name_w.c_str(), field_config);
        field->setOmitTermFreqAndPositions(false);
        doc->add(*field);

        for (const auto& data : test_data) {
            char_string_reader->init(data.data(), data.size(), false);
            auto* stream = custom_analyzer->reusableTokenStream(field->name(), char_string_reader);
            field->setValue(stream);
            indexwriter->addDocument(doc);
        }

        indexwriter->close();
        _CLLDELETE(indexwriter);
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

// Test basic phrase query construction
TEST_F(PhraseQueryV2Test, test_phrase_query_construction) {
    auto context = std::make_shared<IndexQueryContext>();
    context->collection_statistics = std::make_shared<CollectionStatistics>();
    context->collection_similarity = std::make_shared<CollectionSimilarity>();

    std::wstring field = StringHelper::to_wstring("content");
    std::vector<std::wstring> terms = {StringHelper::to_wstring("quick"),
                                       StringHelper::to_wstring("brown")};

    std::vector<TermInfo> term_infos;
    term_infos.reserve(terms.size());
    for (size_t i = 0; i < terms.size(); ++i) {
        TermInfo term_info;
        term_info.term = StringHelper::to_string(terms[i]);
        term_info.position = static_cast<int32_t>(i);
        term_infos.push_back(term_info);
    }

    // Test query construction
    auto query = std::make_shared<query_v2::PhraseQuery>(context, field, term_infos);
    ASSERT_NE(query, nullptr);

    // Test weight creation without scoring
    auto weight = query->weight(false);
    ASSERT_NE(weight, nullptr);

    // Verify weight is of correct type
    auto phrase_weight = std::dynamic_pointer_cast<query_v2::PhraseWeight>(weight);
    ASSERT_NE(phrase_weight, nullptr);
}

// Test phrase query with scoring enabled
// TEST_F(PhraseQueryV2Test, test_phrase_query_with_scoring) {
//     auto context = std::make_shared<IndexQueryContext>();
//     context->collection_statistics = std::make_shared<CollectionStatistics>();
//     context->collection_similarity = std::make_shared<CollectionSimilarity>();

//     std::wstring field = StringHelper::to_wstring("content");
//     std::vector<std::wstring> terms = {StringHelper::to_wstring("quick"),
//                                        StringHelper::to_wstring("brown"),
//                                        StringHelper::to_wstring("fox")};

//     auto query = std::make_shared<query_v2::PhraseQuery>(context, field, terms);
//     ASSERT_NE(query, nullptr);

//     // Test weight creation with scoring enabled
//     auto weight = query->weight(true);
//     ASSERT_NE(weight, nullptr);

//     auto phrase_weight = std::dynamic_pointer_cast<query_v2::PhraseWeight>(weight);
//     ASSERT_NE(phrase_weight, nullptr);
// }

// Test phrase query with empty terms (should throw exception)
TEST_F(PhraseQueryV2Test, test_phrase_query_empty_terms) {
    auto context = std::make_shared<IndexQueryContext>();
    context->collection_statistics = std::make_shared<CollectionStatistics>();
    context->collection_similarity = std::make_shared<CollectionSimilarity>();

    std::wstring field = StringHelper::to_wstring("content");
    std::vector<TermInfo> term_infos; // Empty term_infos

    auto query = std::make_shared<query_v2::PhraseQuery>(context, field, term_infos);
    ASSERT_NE(query, nullptr);

    // Should throw exception when creating weight with empty terms
    EXPECT_THROW({ auto weight = query->weight(false); }, Exception);
}

// Test phrase query execution with two-term phrase
TEST_F(PhraseQueryV2Test, test_phrase_query_two_terms) {
    auto context = std::make_shared<IndexQueryContext>();
    context->collection_statistics = std::make_shared<CollectionStatistics>();
    context->collection_similarity = std::make_shared<CollectionSimilarity>();

    auto* dir = FSDirectory::getDirectory(kTestDir.c_str());
    auto reader_holder = make_shared_reader(lucene::index::IndexReader::open(dir, true));
    ASSERT_TRUE(reader_holder != nullptr);

    std::wstring field = StringHelper::to_wstring("content");
    std::vector<std::wstring> terms = {StringHelper::to_wstring("quick"),
                                       StringHelper::to_wstring("brown")};

    std::vector<TermInfo> term_infos;
    term_infos.reserve(terms.size());
    for (size_t i = 0; i < terms.size(); ++i) {
        TermInfo term_info;
        term_info.term = StringHelper::to_string(terms[i]);
        term_info.position = static_cast<int32_t>(i);
        term_infos.push_back(term_info);
    }

    auto query = std::make_shared<query_v2::PhraseQuery>(context, field, term_infos);
    auto weight = query->weight(false);

    query_v2::QueryExecutionContext exec_ctx;
    exec_ctx.segment_num_rows = reader_holder->maxDoc();
    exec_ctx.sources = {clucene_index_source(reader_holder, field, nullptr)};
    exec_ctx.field_sources.emplace(field, clucene_index_source(reader_holder, field, nullptr));

    auto scorer = weight->scorer(exec_ctx);
    ASSERT_NE(scorer, nullptr);

    roaring::Roaring result;
    uint32_t doc = scorer->doc();
    while (doc != query_v2::TERMINATED) {
        result.add(doc);
        doc = scorer->advance();
    }

    // Should match documents containing "quick brown"
    EXPECT_GT(result.cardinality(), 0);

    _CLDECDELETE(dir);
}

// Test phrase query execution with three-term phrase
TEST_F(PhraseQueryV2Test, test_phrase_query_three_terms) {
    auto context = std::make_shared<IndexQueryContext>();
    context->collection_statistics = std::make_shared<CollectionStatistics>();
    context->collection_similarity = std::make_shared<CollectionSimilarity>();

    auto* dir = FSDirectory::getDirectory(kTestDir.c_str());
    auto reader_holder = make_shared_reader(lucene::index::IndexReader::open(dir, true));
    ASSERT_TRUE(reader_holder != nullptr);

    std::wstring field = StringHelper::to_wstring("content");
    std::vector<std::wstring> terms = {StringHelper::to_wstring("quick"),
                                       StringHelper::to_wstring("brown"),
                                       StringHelper::to_wstring("fox")};

    std::vector<TermInfo> term_infos;
    term_infos.reserve(terms.size());
    for (size_t i = 0; i < terms.size(); ++i) {
        TermInfo term_info;
        term_info.term = StringHelper::to_string(terms[i]);
        term_info.position = static_cast<int32_t>(i);
        term_infos.push_back(term_info);
    }

    auto query = std::make_shared<query_v2::PhraseQuery>(context, field, term_infos);
    auto weight = query->weight(false);

    query_v2::QueryExecutionContext exec_ctx;
    exec_ctx.segment_num_rows = reader_holder->maxDoc();
    exec_ctx.sources = {clucene_index_source(reader_holder, field, nullptr)};
    exec_ctx.field_sources.emplace(field, clucene_index_source(reader_holder, field, nullptr));

    auto scorer = weight->scorer(exec_ctx);
    ASSERT_NE(scorer, nullptr);

    roaring::Roaring result;
    uint32_t doc = scorer->doc();
    while (doc != query_v2::TERMINATED) {
        result.add(doc);
        doc = scorer->advance();
    }

    // Should match documents containing "quick brown fox"
    EXPECT_GT(result.cardinality(), 0);

    _CLDECDELETE(dir);
}

// Test phrase query with single term (should throw exception)
TEST_F(PhraseQueryV2Test, test_phrase_query_single_term) {
    auto context = std::make_shared<IndexQueryContext>();
    context->collection_statistics = std::make_shared<CollectionStatistics>();
    context->collection_similarity = std::make_shared<CollectionSimilarity>();

    std::wstring field = StringHelper::to_wstring("content");
    std::vector<std::wstring> terms = {StringHelper::to_wstring("fox")};

    std::vector<TermInfo> term_infos;
    term_infos.reserve(terms.size());
    for (size_t i = 0; i < terms.size(); ++i) {
        TermInfo term_info;
        term_info.term = StringHelper::to_string(terms[i]);
        term_info.position = static_cast<int32_t>(i);
        term_infos.push_back(term_info);
    }

    auto query = std::make_shared<query_v2::PhraseQuery>(context, field, term_infos);
    ASSERT_NE(query, nullptr);

    // Should throw exception when creating weight with single term (phrase requires at least 2 terms)
    EXPECT_THROW({ auto weight = query->weight(false); }, Exception);
}

// Test phrase query with non-matching phrase
TEST_F(PhraseQueryV2Test, test_phrase_query_no_matches) {
    auto context = std::make_shared<IndexQueryContext>();
    context->collection_statistics = std::make_shared<CollectionStatistics>();
    context->collection_similarity = std::make_shared<CollectionSimilarity>();

    auto* dir = FSDirectory::getDirectory(kTestDir.c_str());
    auto reader_holder = make_shared_reader(lucene::index::IndexReader::open(dir, true));
    ASSERT_TRUE(reader_holder != nullptr);

    std::wstring field = StringHelper::to_wstring("content");
    std::vector<std::wstring> terms = {StringHelper::to_wstring("purple"),
                                       StringHelper::to_wstring("elephant")};

    std::vector<TermInfo> term_infos;
    term_infos.reserve(terms.size());
    for (size_t i = 0; i < terms.size(); ++i) {
        TermInfo term_info;
        term_info.term = StringHelper::to_string(terms[i]);
        term_info.position = static_cast<int32_t>(i);
        term_infos.push_back(term_info);
    }

    auto query = std::make_shared<query_v2::PhraseQuery>(context, field, term_infos);
    auto weight = query->weight(false);

    query_v2::QueryExecutionContext exec_ctx;
    exec_ctx.segment_num_rows = reader_holder->maxDoc();
    exec_ctx.sources = {clucene_index_source(reader_holder, field, nullptr)};
    exec_ctx.field_sources.emplace(field, clucene_index_source(reader_holder, field, nullptr));

    auto scorer = weight->scorer(exec_ctx);
    ASSERT_NE(scorer, nullptr);

    roaring::Roaring result;
    uint32_t doc = scorer->doc();
    while (doc != query_v2::TERMINATED) {
        result.add(doc);
        doc = scorer->advance();
    }

    EXPECT_EQ(result.cardinality(), 0);

    _CLDECDELETE(dir);
}

// Test phrase query with scoring and verify scores
TEST_F(PhraseQueryV2Test, test_phrase_query_scoring) {
    auto context = std::make_shared<IndexQueryContext>();
    context->collection_statistics = std::make_shared<CollectionStatistics>();
    context->collection_similarity = std::make_shared<CollectionSimilarity>();

    auto* dir = FSDirectory::getDirectory(kTestDir.c_str());
    auto reader_holder = make_shared_reader(lucene::index::IndexReader::open(dir, true));
    ASSERT_TRUE(reader_holder != nullptr);

    std::wstring field = StringHelper::to_wstring("content");
    std::vector<std::wstring> terms = {StringHelper::to_wstring("quick"),
                                       StringHelper::to_wstring("brown")};

    std::vector<TermInfo> term_infos;
    term_infos.reserve(terms.size());
    for (size_t i = 0; i < terms.size(); ++i) {
        TermInfo term_info;
        term_info.term = StringHelper::to_string(terms[i]);
        term_info.position = static_cast<int32_t>(i);
        term_infos.push_back(term_info);
    }

    // Fill collection statistics for scoring
    context->collection_statistics->_total_num_docs = reader_holder->numDocs();
    context->collection_statistics->_total_num_tokens[field] = reader_holder->numDocs() * 8;
    context->collection_statistics->_term_doc_freqs[field][StringHelper::to_wstring("quick")] = 10;
    context->collection_statistics->_term_doc_freqs[field][StringHelper::to_wstring("brown")] = 10;

    auto query = std::make_shared<query_v2::PhraseQuery>(context, field, term_infos);
    auto weight = query->weight(true);

    query_v2::QueryExecutionContext exec_ctx;
    exec_ctx.segment_num_rows = reader_holder->maxDoc();
    exec_ctx.sources = {clucene_index_source(reader_holder, field, nullptr)};
    exec_ctx.field_sources.emplace(field, clucene_index_source(reader_holder, field, nullptr));

    auto scorer = weight->scorer(exec_ctx);
    ASSERT_NE(scorer, nullptr);

    roaring::Roaring result;
    uint32_t doc = scorer->doc();
    float total_score = 0.0F;
    uint32_t count = 0;
    while (doc != query_v2::TERMINATED) {
        float score = scorer->score();
        EXPECT_GT(score, 0.0F) << "Score should be positive";
        total_score += score;
        result.add(doc);
        ++count;
        doc = scorer->advance();
    }

    if (count > 0) {
        EXPECT_GT(total_score, 0.0F) << "Total score should be positive";
    }

    _CLDECDELETE(dir);
}

// Test phrase query with binding key
TEST_F(PhraseQueryV2Test, test_phrase_query_with_binding_key) {
    auto context = std::make_shared<IndexQueryContext>();
    context->collection_statistics = std::make_shared<CollectionStatistics>();
    context->collection_similarity = std::make_shared<CollectionSimilarity>();

    auto* dir = FSDirectory::getDirectory(kTestDir.c_str());
    auto reader_holder = make_shared_reader(lucene::index::IndexReader::open(dir, true));
    ASSERT_TRUE(reader_holder != nullptr);

    std::wstring field = StringHelper::to_wstring("content");
    std::vector<std::wstring> terms = {StringHelper::to_wstring("lazy"),
                                       StringHelper::to_wstring("dog")};

    std::vector<TermInfo> term_infos;
    term_infos.reserve(terms.size());
    for (size_t i = 0; i < terms.size(); ++i) {
        TermInfo term_info;
        term_info.term = StringHelper::to_string(terms[i]);
        term_info.position = static_cast<int32_t>(i);
        term_infos.push_back(term_info);
    }

    auto query = std::make_shared<query_v2::PhraseQuery>(context, field, term_infos);
    auto weight = query->weight(false);

    query_v2::QueryExecutionContext exec_ctx;
    exec_ctx.segment_num_rows = reader_holder->maxDoc();
    exec_ctx.sources = {clucene_index_source(reader_holder, field, nullptr)};

    std::string binding_key = "content#0";
    exec_ctx.source_bindings[binding_key] = clucene_index_source(reader_holder, field, nullptr);
    exec_ctx.field_sources.emplace(field, clucene_index_source(reader_holder, field, nullptr));

    auto scorer = weight->scorer(exec_ctx, binding_key);
    ASSERT_NE(scorer, nullptr);

    roaring::Roaring result;
    uint32_t doc = scorer->doc();
    while (doc != query_v2::TERMINATED) {
        result.add(doc);
        doc = scorer->advance();
    }

    EXPECT_GT(result.cardinality(), 0);

    _CLDECDELETE(dir);
}

// Test phrase query destructor (coverage)
TEST_F(PhraseQueryV2Test, test_phrase_query_destructor) {
    auto context = std::make_shared<IndexQueryContext>();
    context->collection_statistics = std::make_shared<CollectionStatistics>();
    context->collection_similarity = std::make_shared<CollectionSimilarity>();

    std::wstring field = StringHelper::to_wstring("content");
    std::vector<std::wstring> terms = {StringHelper::to_wstring("test"),
                                       StringHelper::to_wstring("phrase")};

    std::vector<TermInfo> term_infos;
    term_infos.reserve(terms.size());
    for (size_t i = 0; i < terms.size(); ++i) {
        TermInfo term_info;
        term_info.term = StringHelper::to_string(terms[i]);
        term_info.position = static_cast<int32_t>(i);
        term_infos.push_back(term_info);
    }

    {
        auto query = std::make_shared<query_v2::PhraseQuery>(context, field, term_infos);
        auto weight = query->weight(false);
        ASSERT_NE(weight, nullptr);
        // Query and weight will be destroyed at scope exit
    }
    // If we reach here without crash, destructor works correctly
    SUCCEED();
}

// Test phrase query with longer phrase (4+ terms)
TEST_F(PhraseQueryV2Test, test_phrase_query_long_phrase) {
    auto context = std::make_shared<IndexQueryContext>();
    context->collection_statistics = std::make_shared<CollectionStatistics>();
    context->collection_similarity = std::make_shared<CollectionSimilarity>();

    auto* dir = FSDirectory::getDirectory(kTestDir.c_str());
    auto reader_holder = make_shared_reader(lucene::index::IndexReader::open(dir, true));
    ASSERT_TRUE(reader_holder != nullptr);

    std::wstring field = StringHelper::to_wstring("content");
    std::vector<std::wstring> terms = {
            StringHelper::to_wstring("the"), StringHelper::to_wstring("quick"),
            StringHelper::to_wstring("brown"), StringHelper::to_wstring("fox"),
            StringHelper::to_wstring("jumps")};

    std::vector<TermInfo> term_infos;
    term_infos.reserve(terms.size());
    for (size_t i = 0; i < terms.size(); ++i) {
        TermInfo term_info;
        term_info.term = StringHelper::to_string(terms[i]);
        term_info.position = static_cast<int32_t>(i);
        term_infos.push_back(term_info);
    }

    auto query = std::make_shared<query_v2::PhraseQuery>(context, field, term_infos);
    auto weight = query->weight(false);

    query_v2::QueryExecutionContext exec_ctx;
    exec_ctx.segment_num_rows = reader_holder->maxDoc();
    exec_ctx.sources = {clucene_index_source(reader_holder, field, nullptr)};
    exec_ctx.field_sources.emplace(field, clucene_index_source(reader_holder, field, nullptr));

    auto scorer = weight->scorer(exec_ctx);
    ASSERT_NE(scorer, nullptr);

    roaring::Roaring result;
    uint32_t doc = scorer->doc();
    while (doc != query_v2::TERMINATED) {
        result.add(doc);
        doc = scorer->advance();
    }

    EXPECT_GE(result.cardinality(), 0);

    _CLDECDELETE(dir);
}

// Test phrase query with terms that exist but not in sequence
TEST_F(PhraseQueryV2Test, test_phrase_query_terms_not_in_sequence) {
    auto context = std::make_shared<IndexQueryContext>();
    context->collection_statistics = std::make_shared<CollectionStatistics>();
    context->collection_similarity = std::make_shared<CollectionSimilarity>();

    auto* dir = FSDirectory::getDirectory(kTestDir.c_str());
    auto reader_holder = make_shared_reader(lucene::index::IndexReader::open(dir, true));
    ASSERT_TRUE(reader_holder != nullptr);

    std::wstring field = StringHelper::to_wstring("content");
    // These terms exist in documents but not necessarily in this exact sequence
    std::vector<std::wstring> terms = {StringHelper::to_wstring("dog"),
                                       StringHelper::to_wstring("fox")};

    std::vector<TermInfo> term_infos;
    term_infos.reserve(terms.size());
    for (size_t i = 0; i < terms.size(); ++i) {
        TermInfo term_info;
        term_info.term = StringHelper::to_string(terms[i]);
        term_info.position = static_cast<int32_t>(i);
        term_infos.push_back(term_info);
    }

    auto query = std::make_shared<query_v2::PhraseQuery>(context, field, term_infos);
    auto weight = query->weight(false);

    query_v2::QueryExecutionContext exec_ctx;
    exec_ctx.segment_num_rows = reader_holder->maxDoc();
    exec_ctx.sources = {clucene_index_source(reader_holder, field, nullptr)};
    exec_ctx.field_sources.emplace(field, clucene_index_source(reader_holder, field, nullptr));

    auto scorer = weight->scorer(exec_ctx);
    ASSERT_NE(scorer, nullptr);

    roaring::Roaring result;
    uint32_t doc = scorer->doc();
    while (doc != query_v2::TERMINATED) {
        result.add(doc);
        doc = scorer->advance();
    }

    // May or may not match depending on the data
    EXPECT_GE(result.cardinality(), 0);

    _CLDECDELETE(dir);
}

// Test phrase query with BM25 similarity
TEST_F(PhraseQueryV2Test, test_phrase_query_bm25_similarity) {
    auto context = std::make_shared<IndexQueryContext>();
    context->collection_statistics = std::make_shared<CollectionStatistics>();
    context->collection_similarity = std::make_shared<CollectionSimilarity>();

    auto* dir = FSDirectory::getDirectory(kTestDir.c_str());
    auto reader_holder = make_shared_reader(lucene::index::IndexReader::open(dir, true));
    ASSERT_TRUE(reader_holder != nullptr);

    std::wstring field = StringHelper::to_wstring("content");
    std::vector<std::wstring> terms = {StringHelper::to_wstring("quick"),
                                       StringHelper::to_wstring("brown"),
                                       StringHelper::to_wstring("fox")};

    std::vector<TermInfo> term_infos;
    term_infos.reserve(terms.size());
    for (size_t i = 0; i < terms.size(); ++i) {
        TermInfo term_info;
        term_info.term = StringHelper::to_string(terms[i]);
        term_info.position = static_cast<int32_t>(i);
        term_infos.push_back(term_info);
    }

    // Setup statistics for BM25
    context->collection_statistics->_total_num_docs = reader_holder->numDocs();
    context->collection_statistics->_total_num_tokens[field] = reader_holder->numDocs() * 8;
    for (const auto& term : terms) {
        context->collection_statistics->_term_doc_freqs[field][term] = 5;
    }

    auto query = std::make_shared<query_v2::PhraseQuery>(context, field, term_infos);
    auto weight = query->weight(true); // Enable scoring

    query_v2::QueryExecutionContext exec_ctx;
    exec_ctx.segment_num_rows = reader_holder->maxDoc();
    exec_ctx.sources = {clucene_index_source(reader_holder, field, nullptr)};
    exec_ctx.field_sources.emplace(field, clucene_index_source(reader_holder, field, nullptr));

    auto scorer = weight->scorer(exec_ctx);
    ASSERT_NE(scorer, nullptr);

    uint32_t doc = scorer->doc();
    bool found_match = false;
    while (doc != query_v2::TERMINATED) {
        float score = scorer->score();
        EXPECT_GE(score, 0.0F) << "BM25 score should be non-negative";
        found_match = true;
        doc = scorer->advance();
    }

    if (found_match) {
        SUCCEED() << "Found matches with BM25 scoring";
    }

    _CLDECDELETE(dir);
}

TEST(PhraseScorerStreamTest, ExactMatchStopsReadingAfterAnEarlyHit) {
    using Cursor = index_query::testing::FakePostingsCursor;
    std::vector<Cursor*> observed;
    std::vector<std::pair<size_t, query_v2::SegmentPostingsPtr>> terms;
    for (uint32_t offset = 0; offset < 3; ++offset) {
        std::vector<uint32_t> positions;
        for (uint32_t i = 0; i < 512; ++i) {
            positions.push_back(4 * i + offset);
        }
        auto cursor = std::make_unique<Cursor>(
                std::vector<Cursor::Posting> {{.doc = 0, .positions = positions},
                                              {.doc = 2, .positions = positions}},
                true, true);
        observed.push_back(cursor.get());
        terms.emplace_back(offset,
                           query_v2::make_segment_postings(std::move(cursor), false, nullptr));
    }
    auto scorer =
            query_v2::PhraseScorer<query_v2::SegmentPostingsPtr>::create(terms, nullptr, {}, 3);
    ASSERT_EQ(scorer->doc(), 0);
    EXPECT_EQ(scorer->seek(0), 0);
    for (const auto* cursor : observed) {
        EXPECT_LE(cursor->positions_read, 16);
    }
    ASSERT_EQ(scorer->advance(), 2);
    for (const auto* cursor : observed) {
        EXPECT_LE(cursor->positions_read, 32);
    }
    EXPECT_EQ(scorer->advance(), query_v2::TERMINATED);
}

TEST_F(PhraseQueryV2Test, SloppyScorerPreservesForwardOnlyPositionsAcrossSeek) {
    std::unique_ptr<lucene::store::Directory, DirectoryDeleter> directory(
            FSDirectory::getDirectory(kTestDir.c_str()));
    auto reader = make_shared_reader(lucene::index::IndexReader::open(directory.get(), true));
    auto similarity = std::make_shared<BM25Similarity>(2.0F, 8.0F);
    auto quick = make_term_ptr(L"content", L"quick");
    auto brown = make_term_ptr(L"content", L"brown");
    auto quick_positions = make_term_positions_ptr(reader.get(), quick.get(), true, nullptr);
    auto brown_positions = make_term_positions_ptr(reader.get(), brown.get(), true, nullptr);
    ASSERT_NE(quick_positions, nullptr);
    ASSERT_NE(brown_positions, nullptr);
    const std::vector<std::pair<size_t, query_v2::SegmentPostingsPtr>> terms {
            {0, query_v2::make_segment_postings(
                        std::make_unique<ClucenePostingsCursor>(std::move(quick_positions)), true,
                        similarity)},
            {1, query_v2::make_segment_postings(
                        std::make_unique<ClucenePostingsCursor>(std::move(brown_positions)), true,
                        similarity)}};

    auto scorer = query_v2::PhraseScorer<query_v2::SegmentPostingsPtr>::create(
            terms, similarity, {.slop = 1}, reader->maxDoc());
    ASSERT_EQ(scorer->doc(), 0);
    const float first_score = scorer->score();
    EXPECT_EQ(scorer->seek(0), 0);
    EXPECT_FLOAT_EQ(scorer->score(), first_score);
    ASSERT_EQ(scorer->seek(6), 6);
    EXPECT_EQ(scorer->seek(6), 6);
    ASSERT_EQ(scorer->advance(), 8);
    EXPECT_EQ(scorer->norm(), 0);
    EXPECT_FLOAT_EQ(scorer->score(), similarity->score(0.5F, 0));
    EXPECT_EQ(scorer->seek(8), 8);
    EXPECT_FLOAT_EQ(scorer->score(), similarity->score(0.5F, 0));
    EXPECT_EQ(scorer->advance(), 11);
    EXPECT_EQ(scorer->advance(), 19);
    EXPECT_EQ(scorer->advance(), query_v2::TERMINATED);
}

// The documents a phrase over `terms` matches in the test index.
static std::set<uint32_t> phrase_docs(const std::string& dir, const std::vector<std::string>& terms,
                                      const index_query::PhraseQueryOptions& options) {
    std::unique_ptr<lucene::store::Directory, DirectoryDeleter> directory(
            FSDirectory::getDirectory(dir.c_str()));
    auto reader = make_shared_reader(lucene::index::IndexReader::open(directory.get(), true));
    const std::wstring field = L"content";
    std::vector<TermInfo> term_infos;
    for (size_t i = 0; i < terms.size(); ++i) {
        TermInfo term_info;
        term_info.term = terms[i];
        term_info.position = static_cast<int32_t>(i);
        term_infos.push_back(std::move(term_info));
    }
    query_v2::PhraseQuery query(std::make_shared<IndexQueryContext>(), field, term_infos, options);
    query_v2::QueryExecutionContext exec_ctx;
    exec_ctx.segment_num_rows = reader->maxDoc();
    exec_ctx.field_sources.emplace(field, clucene_index_source(reader, field, nullptr));
    auto scorer = query.weight(false)->scorer(exec_ctx);
    std::set<uint32_t> docs;
    for (uint32_t doc = scorer->doc(); doc != query_v2::TERMINATED; doc = scorer->advance()) {
        docs.insert(doc);
    }
    return docs;
}

// "quick fox" is exact in doc 5, one move apart in docs 0 and 11, and reversed in doc 8.
TEST_F(PhraseQueryV2Test, SlopLetsTheTermsMoveApart) {
    EXPECT_EQ(phrase_docs(kTestDir, {"quick", "fox"}, {}), (std::set<uint32_t> {5}));
    EXPECT_EQ(phrase_docs(kTestDir, {"quick", "fox"}, {.slop = 1}),
              (std::set<uint32_t> {0, 5, 11}));
    EXPECT_EQ(phrase_docs(kTestDir, {"quick", "fox"}, {.slop = 3}),
              (std::set<uint32_t> {0, 5, 8, 11}));
}

TEST_F(PhraseQueryV2Test, OrderedSlopKeepsTheTermsInOrder) {
    EXPECT_EQ(phrase_docs(kTestDir, {"quick", "fox"}, {.slop = 3, .ordered = true}),
              (std::set<uint32_t> {0, 5, 11}));
}

// "quick brown" matches docs 0, 1, 6, 11 and 19.
TEST_F(PhraseQueryV2Test, CandidatesRestrictThePhrase) {
    roaring::Roaring candidates;
    candidates.addMany(4, std::array<uint32_t, 4> {1, 2, 11, 18}.data());
    EXPECT_EQ(phrase_docs(kTestDir, {"quick", "brown"}, {.candidates = &candidates}),
              (std::set<uint32_t> {1, 11}));
    roaring::Roaring past_the_matches;
    past_the_matches.add(20);
    EXPECT_TRUE(
            phrase_docs(kTestDir, {"quick", "brown"}, {.candidates = &past_the_matches}).empty());
    const roaring::Roaring none;
    EXPECT_TRUE(phrase_docs(kTestDir, {"quick", "brown"}, {.candidates = &none}).empty());
}

// The phrase over in-memory postings, streamed one document at a time or, on a source that
// batches its reads, listed as a chain with the positions read in one round.
static index_query::testing::FakeIndexSource::Posting posting(uint32_t doc,
                                                              std::vector<uint32_t> positions) {
    return {.doc = doc, .positions = std::move(positions)};
}

static std::shared_ptr<index_query::testing::FakeIndexSource> fake_phrase_source(bool batches) {
    auto source = std::make_shared<index_query::testing::FakeIndexSource>();
    source->batches = batches;
    source->set_doc_count(16);
    source->add("quick", {posting(0, {1}), posting(1, {0}), posting(5, {0, 4}), posting(8, {3}),
                          posting(11, {0})});
    source->add("brown", {posting(0, {2}), posting(1, {1}), posting(5, {2}), posting(8, {4, 7}),
                          posting(12, {1})});
    return source;
}

static std::vector<TermInfo> phrase_terms(const std::vector<std::string>& terms) {
    std::vector<TermInfo> term_infos;
    for (size_t i = 0; i < terms.size(); ++i) {
        TermInfo term_info;
        term_info.term = terms[i];
        term_info.position = static_cast<int32_t>(i);
        term_infos.push_back(std::move(term_info));
    }
    return term_infos;
}

static std::set<uint32_t> fake_phrase_docs(
        const std::shared_ptr<index_query::testing::FakeIndexSource>& source,
        const std::vector<std::string>& terms, const index_query::PhraseQueryOptions& options) {
    const std::wstring field = L"content";
    query_v2::PhraseQuery query(std::make_shared<IndexQueryContext>(), field, phrase_terms(terms),
                                options);
    query_v2::QueryExecutionContext exec_ctx;
    exec_ctx.segment_num_rows = source->doc_count();
    exec_ctx.field_sources.emplace(field, source);
    auto scorer = query.weight(false)->scorer(exec_ctx);
    std::set<uint32_t> docs;
    for (uint32_t doc = scorer->doc(); doc != query_v2::TERMINATED; doc = scorer->advance()) {
        docs.insert(doc);
    }
    return docs;
}

// "quick brown" is exact in docs 0, 1 and 8, and one move apart in doc 5.
TEST_F(PhraseQueryV2Test, AListedPhraseMatchesTheStreamedOne) {
    roaring::Roaring candidates;
    candidates.addMany(3, std::array<uint32_t, 3> {1, 8, 12}.data());
    for (const bool batches : {false, true}) {
        auto source = fake_phrase_source(batches);
        EXPECT_EQ(fake_phrase_docs(source, {"quick", "brown"}, {}), (std::set<uint32_t> {0, 1, 8}))
                << batches;
        EXPECT_EQ(fake_phrase_docs(source, {"quick", "brown"}, {.slop = 1}),
                  (std::set<uint32_t> {0, 1, 5, 8}))
                << batches;
        EXPECT_EQ(fake_phrase_docs(source, {"brown", "quick"}, {.slop = 1}),
                  (std::set<uint32_t> {5}))
                << batches;
        EXPECT_EQ(fake_phrase_docs(source, {"quick", "brown"}, {.candidates = &candidates}),
                  (std::set<uint32_t> {1, 8}))
                << batches;
        EXPECT_TRUE(fake_phrase_docs(source, {"quick", "absent"}, {}).empty()) << batches;
    }
}

TEST_F(PhraseQueryV2Test, AListedPhraseReadsItsTermsTogether) {
    auto source = fake_phrase_source(true);
    EXPECT_EQ(fake_phrase_docs(source, {"quick", "brown"}, {}), (std::set<uint32_t> {0, 1, 8}));
    EXPECT_EQ(source->opened_together,
              (std::vector<std::vector<std::string>> {{"quick", "brown"}}));
    EXPECT_TRUE(source->opened.empty());
    // The chain lists the first term whole and the second on its rows; then both read the
    // positions of the rows holding every term, in one round.
    const auto& quick = source->prefetches["quick"];
    const auto& brown = source->prefetches["brown"];
    ASSERT_EQ(quick.size(), 2U);
    ASSERT_EQ(brown.size(), 2U);
    EXPECT_TRUE(quick[0].whole);
    EXPECT_FALSE(quick[0].positions);
    EXPECT_EQ(brown[0].candidates, (std::vector<uint32_t> {0, 1, 5, 8, 11}));
    EXPECT_FALSE(brown[0].positions);
    EXPECT_EQ(quick[1].candidates, (std::vector<uint32_t> {0, 1, 5, 8}));
    EXPECT_TRUE(quick[1].positions);
    EXPECT_EQ(brown[1].candidates, (std::vector<uint32_t> {0, 1, 5, 8}));
    EXPECT_EQ(source->fetches, 1U);
}

// Candidates far more than the rarest term's documents filter the phrase's rows rather than
// seed its chain.
TEST_F(PhraseQueryV2Test, AListedPhraseFiltersByManyCandidates) {
    auto source = fake_phrase_source(true);
    source->set_doc_count(64);
    roaring::Roaring candidates;
    candidates.addRange(1, 64);
    EXPECT_EQ(fake_phrase_docs(source, {"quick", "brown"}, {.candidates = &candidates}),
              (std::set<uint32_t> {1, 8}));
    EXPECT_TRUE(source->prefetches["quick"][0].whole);
    // The first term's rows among the candidates seed the second.
    EXPECT_EQ(source->prefetches["brown"][0].candidates, (std::vector<uint32_t> {1, 5, 8, 11}));
    EXPECT_EQ(source->prefetches["quick"][1].candidates, (std::vector<uint32_t> {1, 5, 8}));
}

TEST_F(PhraseQueryV2Test, AListedPhraseListsItsRowsForAConjunction) {
    const std::wstring field = L"content";
    query_v2::PhraseQuery query(std::make_shared<IndexQueryContext>(), field,
                                phrase_terms({"quick", "brown"}));
    query_v2::QueryExecutionContext exec_ctx;
    exec_ctx.segment_num_rows = 16;
    auto streamed = fake_phrase_source(false);
    exec_ctx.field_sources.emplace(field, streamed);
    EXPECT_FALSE(query.weight(false)->lists_rows(exec_ctx, ""));
    auto listed = fake_phrase_source(true);
    exec_ctx.field_sources[field] = listed;
    // A scored phrase streams even on a batching source.
    query_v2::PhraseWeight scored(field, phrase_terms({"quick", "brown"}), {}, nullptr,
                                  /*enable_scoring=*/true, /*nullable=*/true);
    EXPECT_FALSE(scored.lists_rows(exec_ctx, ""));
    auto weight = query.weight(false);
    ASSERT_TRUE(weight->lists_rows(exec_ctx, ""));
    roaring::Roaring candidates;
    candidates.addMany(3, std::array<uint32_t, 3> {1, 8, 12}.data());
    const auto rows = weight->listed_rows(exec_ctx, "", &candidates);
    EXPECT_EQ(rows.true_rows, roaring::Roaring::bitmapOf(2, 1U, 8U));
    EXPECT_TRUE(rows.null_rows.isEmpty());
    // The chain started from the candidates.
    EXPECT_EQ(listed->prefetches["quick"][0].candidates, (std::vector<uint32_t> {1, 8, 12}));
}

// "quick bro*" also matches "quick bronze" in docs 5 and 11, and "*ick bro*" also matches
// "thick bronze" in doc 12.
static std::shared_ptr<index_query::testing::FakeIndexSource> fake_prefix_source(bool batches) {
    auto source = fake_phrase_source(batches);
    source->add("bronze", {posting(5, {5}), posting(11, {1}), posting(12, {3})});
    source->add("thick", {posting(12, {2})});
    return source;
}

static std::set<uint32_t> fake_docs(
        query_v2::Query& query,
        const std::shared_ptr<index_query::testing::FakeIndexSource>& source) {
    query_v2::QueryExecutionContext exec_ctx;
    exec_ctx.segment_num_rows = source->doc_count();
    exec_ctx.field_sources.emplace(L"content", source);
    auto scorer = query.weight(false)->scorer(exec_ctx);
    std::set<uint32_t> docs;
    for (uint32_t doc = scorer->doc(); doc != query_v2::TERMINATED; doc = scorer->advance()) {
        docs.insert(doc);
    }
    return docs;
}

TEST_F(PhraseQueryV2Test, AListedPhrasePrefixMatchesTheStreamedOne) {
    for (const bool batches : {false, true}) {
        auto source = fake_prefix_source(batches);
        query_v2::PhrasePrefixQuery prefix(std::make_shared<IndexQueryContext>(), L"content",
                                           phrase_terms({"quick", "bro"}));
        EXPECT_EQ(fake_docs(prefix, source), (std::set<uint32_t> {0, 1, 5, 8, 11})) << batches;
        query_v2::PhrasePrefixQuery edge(std::make_shared<IndexQueryContext>(), L"content",
                                         phrase_terms({"ick", "bro"}), nullptr, /*suffix=*/true);
        EXPECT_EQ(fake_docs(edge, source), (std::set<uint32_t> {0, 1, 5, 8, 11, 12})) << batches;
        query_v2::PhrasePrefixQuery none(std::make_shared<IndexQueryContext>(), L"content",
                                         phrase_terms({"quick", "zz"}));
        EXPECT_TRUE(fake_docs(none, source).empty()) << batches;
    }
}

TEST_F(PhraseQueryV2Test, AListedPhrasePrefixReadsItsTermsTogether) {
    auto source = fake_prefix_source(true);
    query_v2::PhrasePrefixQuery query(std::make_shared<IndexQueryContext>(), L"content",
                                      phrase_terms({"quick", "bro"}));
    EXPECT_EQ(fake_docs(query, source), (std::set<uint32_t> {0, 1, 5, 8, 11}));
    // The exact term opens first, then the terms the prefix expands to.
    EXPECT_EQ(source->opened_together,
              (std::vector<std::vector<std::string>> {{"quick"}, {"bronze", "brown"}}));
    EXPECT_TRUE(source->opened.empty());
    // "quick" lists whole and the expansions on its rows; then every term reads the positions of
    // the rows it holds among those holding a term of every slot, in one round.
    auto& prefetches = source->prefetches;
    ASSERT_EQ(prefetches["quick"].size(), 2U);
    ASSERT_EQ(prefetches["bronze"].size(), 2U);
    ASSERT_EQ(prefetches["brown"].size(), 2U);
    EXPECT_TRUE(prefetches["quick"][0].whole);
    EXPECT_EQ(prefetches["bronze"][0].candidates, (std::vector<uint32_t> {0, 1, 5, 8, 11}));
    EXPECT_EQ(prefetches["brown"][0].candidates, (std::vector<uint32_t> {0, 1, 5, 8, 11}));
    EXPECT_EQ(prefetches["quick"][1].candidates, (std::vector<uint32_t> {0, 1, 5, 8, 11}));
    EXPECT_TRUE(prefetches["quick"][1].positions);
    EXPECT_EQ(prefetches["bronze"][1].candidates, (std::vector<uint32_t> {5, 11}));
    EXPECT_EQ(prefetches["brown"][1].candidates, (std::vector<uint32_t> {0, 1, 5, 8}));
    EXPECT_EQ(source->fetches, 1U);
}

// A phrase prefix missing one of its exact terms ends before any expansion runs.
TEST_F(PhraseQueryV2Test, AListedPhrasePrefixMissingATermExpandsNothing) {
    auto source = fake_prefix_source(true);
    query_v2::PhrasePrefixQuery edge(std::make_shared<IndexQueryContext>(), L"content",
                                     phrase_terms({"ick", "absent", "bro"}), nullptr,
                                     /*suffix=*/true);
    EXPECT_TRUE(fake_docs(edge, source).empty());
    EXPECT_TRUE(source->expanded.empty());
    EXPECT_TRUE(source->opened_together.empty());
}

// The expansions list after the exact term, on the rows it kept, even when they hold fewer
// documents.
TEST_F(PhraseQueryV2Test, AListedPhrasePrefixListsItsExpansionsLast) {
    auto source = fake_prefix_source(true);
    source->add("brief", {posting(11, {1})});
    source->add("brisk", {posting(5, {5})});
    query_v2::PhrasePrefixQuery query(std::make_shared<IndexQueryContext>(), L"content",
                                      phrase_terms({"quick", "bri"}));
    EXPECT_EQ(fake_docs(query, source), (std::set<uint32_t> {5, 11}));
    EXPECT_TRUE(source->prefetches["quick"][0].whole);
    EXPECT_EQ(source->prefetches["brief"][0].candidates, (std::vector<uint32_t> {0, 1, 5, 8, 11}));
    EXPECT_EQ(source->prefetches["brisk"][0].candidates, (std::vector<uint32_t> {0, 1, 5, 8, 11}));
}

// Expansions holding far fewer documents than every exact term list first, and the exact terms
// on their rows.
TEST_F(PhraseQueryV2Test, AListedPhrasePrefixListsRareExpansionsFirst) {
    auto source = std::make_shared<index_query::testing::FakeIndexSource>();
    source->batches = true;
    source->set_doc_count(16);
    std::vector<index_query::testing::FakeIndexSource::Posting> common;
    for (uint32_t doc = 0; doc < 16; ++doc) {
        common.push_back(posting(doc, {0}));
    }
    source->add("common", std::move(common));
    source->add("rabbit", {posting(3, {1})});
    source->add("raven", {posting(7, {2})});
    query_v2::PhrasePrefixQuery query(std::make_shared<IndexQueryContext>(), L"content",
                                      phrase_terms({"common", "ra"}));
    EXPECT_EQ(fake_docs(query, source), (std::set<uint32_t> {3}));
    EXPECT_TRUE(source->prefetches["rabbit"][0].whole);
    EXPECT_TRUE(source->prefetches["raven"][0].whole);
    EXPECT_EQ(source->prefetches["common"][0].candidates, (std::vector<uint32_t> {3, 7}));
}

// More expansions than one wave reads. "quick" holds rows 0 to 47 at position 0; "br00" to
// "br39" each hold their own row, right after "quick" when its number is a multiple of 3 and a
// position later otherwise, and row 47 far from it; row 45 holds "br05" late and "br36" right
// after "quick", and row 46 "br06" right after it and "br37" late, so their matches come from
// different waves.
static constexpr size_t kManyExpansions = 40;

static std::string many_expansion(const std::string& stem, size_t i) {
    return stem + (i < 10 ? "0" : "") + std::to_string(i);
}

static std::vector<std::vector<std::string>> expansion_waves(const std::string& stem) {
    std::vector<std::vector<std::string>> waves(2);
    for (size_t i = 0; i < kManyExpansions; ++i) {
        waves[i < 32 ? 0 : 1].push_back(many_expansion(stem, i));
    }
    return waves;
}

static std::shared_ptr<index_query::testing::FakeIndexSource> many_prefix_source(bool batches) {
    auto source = std::make_shared<index_query::testing::FakeIndexSource>();
    source->batches = batches;
    source->set_doc_count(64);
    std::vector<index_query::testing::FakeIndexSource::Posting> quick;
    for (uint32_t doc = 0; doc < 48; ++doc) {
        quick.push_back(posting(doc, {0}));
    }
    source->add("quick", std::move(quick));
    for (size_t i = 0; i < kManyExpansions; ++i) {
        std::vector<index_query::testing::FakeIndexSource::Posting> postings {
                posting(static_cast<uint32_t>(i), {i % 3 == 0 ? 1U : 2U}), posting(47, {4})};
        if (i == 5 || i == 37) {
            postings.push_back(posting(i == 5 ? 45 : 46, {3}));
        }
        if (i == 6 || i == 36) {
            postings.push_back(posting(i == 6 ? 46 : 45, {1}));
        }
        source->add(many_expansion("br", i), std::move(postings));
    }
    for (uint32_t doc = 0; doc < 64; ++doc) {
        source->norms[doc] = doc % 6 + 1;
    }
    return source;
}

// The rows "quick br*" matches in the source above.
static std::set<uint32_t> many_prefix_matches() {
    std::set<uint32_t> matches {45, 46};
    for (uint32_t doc = 0; doc < kManyExpansions; doc += 3) {
        matches.insert(doc);
    }
    return matches;
}

// Every expansion listed on the rows "quick" holds, then read the positions of the rows it held.
static void expect_tail_gathered_on_quick_rows(index_query::testing::FakeIndexSource& source) {
    std::vector<uint32_t> quick_rows(48);
    std::iota(quick_rows.begin(), quick_rows.end(), 0);
    for (size_t i = 0; i < kManyExpansions; ++i) {
        const auto& prefetches = source.prefetches[many_expansion("br", i)];
        ASSERT_EQ(prefetches.size(), 2U) << i;
        EXPECT_EQ(prefetches[0].candidates, quick_rows) << i;
        EXPECT_FALSE(prefetches[0].positions) << i;
        EXPECT_TRUE(prefetches[1].positions) << i;
    }
}

// A phrase prefix whose tail expands to more terms than one wave reads gathers the tail's
// positions a wave at a time on the rows "quick" kept, each wave released before the next, and
// answers as the streamed phrase does.
TEST_F(PhraseQueryV2Test, AListedPhrasePrefixOfManyExpansionsGathersThemAWaveAtATime) {
    const auto make_query = [] {
        return query_v2::PhrasePrefixQuery(std::make_shared<IndexQueryContext>(), L"content",
                                           phrase_terms({"quick", "br"}));
    };
    auto streamed = make_query();
    EXPECT_EQ(fake_docs(streamed, many_prefix_source(false)), many_prefix_matches());
    auto source = many_prefix_source(true);
    auto listed = make_query();
    EXPECT_EQ(fake_docs(listed, source), many_prefix_matches());
    auto waves = expansion_waves("br");
    waves.insert(waves.begin(), {"quick"});
    EXPECT_EQ(source->opened_together, waves);
    expect_tail_gathered_on_quick_rows(*source);
    EXPECT_EQ(source->prefetches["br05"][1].candidates, (std::vector<uint32_t> {5, 45, 47}));
    // A round of positions a wave, then the positions of "quick" at the rows kept.
    EXPECT_EQ(source->fetches, 3U);
    EXPECT_LE(source->live.peak, 33U);
    EXPECT_EQ(source->live.now, 0U);
}

// Expansions far rarer than the exact term list their rows first, docids only, and seed the
// exact term's listing; they read positions only at the rows both hold.
TEST_F(PhraseQueryV2Test, AListedPhrasePrefixOfManyRareExpansionsListsThemFirst) {
    auto source = std::make_shared<index_query::testing::FakeIndexSource>();
    source->batches = true;
    source->set_doc_count(512);
    std::vector<index_query::testing::FakeIndexSource::Posting> common;
    for (uint32_t doc = 0; doc < 512; ++doc) {
        common.push_back(posting(doc, {0}));
    }
    source->add("common", std::move(common));
    std::vector<uint32_t> rare_rows;
    for (size_t i = 0; i < kManyExpansions; ++i) {
        const auto doc = static_cast<uint32_t>(i * 10);
        source->add(many_expansion("ra", i), {posting(doc, {i % 2 == 0 ? 1U : 2U})});
        rare_rows.push_back(doc);
    }
    query_v2::PhrasePrefixQuery query(std::make_shared<IndexQueryContext>(), L"content",
                                      phrase_terms({"common", "ra"}));
    std::set<uint32_t> expected;
    for (size_t i = 0; i < kManyExpansions; i += 2) {
        expected.insert(static_cast<uint32_t>(i * 10));
    }
    EXPECT_EQ(fake_docs(query, source), expected);
    // Each expansion reads the positions of its own row only, without listing again.
    for (const size_t i : {size_t {0}, size_t {39}}) {
        const auto& prefetches = source->prefetches[many_expansion("ra", i)];
        ASSERT_EQ(prefetches.size(), 2U) << i;
        EXPECT_TRUE(prefetches[0].whole) << i;
        EXPECT_FALSE(prefetches[0].positions) << i;
        EXPECT_EQ(prefetches[1].candidates, (std::vector<uint32_t> {rare_rows[i]})) << i;
        EXPECT_TRUE(prefetches[1].positions) << i;
    }
    EXPECT_EQ(source->prefetches["common"][0].candidates, rare_rows);
    EXPECT_LE(source->live.peak, 33U);
}

// "a00x" to "a39x" each hold their own row at position 0 and the next row at 5, and "y00" to
// "y39" their own row right after the "x" term when even and later otherwise.
static std::shared_ptr<index_query::testing::FakeIndexSource> only_expansions_source(bool batches) {
    auto source = std::make_shared<index_query::testing::FakeIndexSource>();
    source->batches = batches;
    source->set_doc_count(64);
    for (size_t i = 0; i < kManyExpansions; ++i) {
        const auto doc = static_cast<uint32_t>(i);
        source->add(many_expansion("a", i) + "x", {posting(doc, {0}), posting(doc + 1, {5})});
        source->add(many_expansion("y", i), {posting(doc, {i % 2 == 0 ? 1U : 3U})});
    }
    return source;
}

// A phrase whose every slot expands to more terms than one wave reads, with no candidates,
// first lists its slots' rows, docids only, then gathers positions at the rows both hold.
TEST_F(PhraseQueryV2Test, AListedPhraseOfOnlyManyExpansionsListsRowsBeforeGathering) {
    std::set<uint32_t> streamed;
    for (const bool batches : {false, true}) {
        auto source = only_expansions_source(batches);
        query_v2::PhrasePrefixQuery query(std::make_shared<IndexQueryContext>(), L"content",
                                          phrase_terms({"x", "y"}), nullptr, /*suffix=*/true);
        const auto docs = fake_docs(query, source);
        if (!batches) {
            streamed = docs;
            EXPECT_EQ(streamed.size(), kManyExpansions / 2);
            continue;
        }
        EXPECT_EQ(docs, streamed);
        // Each slot lists its rows a wave at a time, the one holding fewer documents first, then
        // gathers its positions a wave at a time.
        EXPECT_EQ(source->opened_together.size(), 8U);
        EXPECT_TRUE(source->prefetches["y00"][0].whole);
        EXPECT_FALSE(source->prefetches["y00"][0].positions);
        EXPECT_FALSE(source->prefetches["a00x"][0].whole);
        EXPECT_LE(source->live.peak, 32U);
    }
}

TEST_F(PhraseQueryV2Test, AListedMultiPhraseMatchesTheStreamedOne) {
    std::vector<TermInfo> term_infos = phrase_terms({"quick", "brown"});
    term_infos[1].term = std::vector<std::string> {"brown", "bronze"};
    for (const bool batches : {false, true}) {
        auto source = fake_prefix_source(batches);
        query_v2::MultiPhraseQuery query(std::make_shared<IndexQueryContext>(), L"content",
                                         term_infos);
        EXPECT_EQ(fake_docs(query, source), (std::set<uint32_t> {0, 1, 5, 8, 11})) << batches;
    }
}

// Postings on both sides of the multi-phrase loading threshold of 100 documents: "wide" and
// "big" hold more, "rare" and "small" a few.
static std::shared_ptr<index_query::testing::FakeIndexSource> threshold_source(bool batches) {
    auto source = std::make_shared<index_query::testing::FakeIndexSource>();
    source->batches = batches;
    source->set_doc_count(300);
    std::vector<index_query::testing::FakeIndexSource::Posting> wide;
    std::vector<index_query::testing::FakeIndexSource::Posting> big;
    for (uint32_t doc = 0; doc < 150; ++doc) {
        wide.push_back(posting(doc, {0}));
        if (doc < 120) {
            big.push_back(posting(doc, {1}));
        }
    }
    source->add("wide", std::move(wide));
    source->add("big", std::move(big));
    source->add("small", {posting(140, {1}), posting(201, {4})});
    source->add("rare", {posting(200, {3}), posting(201, {3}), posting(205, {3})});
    return source;
}

// A multi-phrase finds the same rows whichever side of the loading threshold a clause's term or
// alternative falls on, streamed and listed alike.
TEST_F(PhraseQueryV2Test, AMultiPhraseMatchesOnBothSidesOfTheLoadingThreshold) {
    std::set<uint32_t> wide_rows {140};
    for (uint32_t doc = 0; doc < 120; ++doc) {
        wide_rows.insert(doc);
    }
    const std::vector<std::pair<std::string, std::set<uint32_t>>> leads {{"wide", wide_rows},
                                                                         {"rare", {201}}};
    for (const bool batches : {false, true}) {
        for (const auto& [lead, expected] : leads) {
            std::vector<TermInfo> term_infos = phrase_terms({lead, "big"});
            term_infos[1].term = std::vector<std::string> {"big", "small"};
            query_v2::MultiPhraseQuery query(std::make_shared<IndexQueryContext>(), L"content",
                                             term_infos);
            EXPECT_EQ(fake_docs(query, threshold_source(batches)), expected)
                    << lead << " " << batches;
        }
    }
}

// The rows a scored weight lists on `source`, each with its score.
static std::map<uint32_t, float> scored_docs(
        query_v2::Weight& weight,
        const std::shared_ptr<index_query::testing::FakeIndexSource>& source) {
    query_v2::QueryExecutionContext exec_ctx;
    exec_ctx.segment_num_rows = source->doc_count();
    exec_ctx.field_sources.emplace(L"content", source);
    auto scorer = weight.scorer(exec_ctx, "");
    std::map<uint32_t, float> rows;
    for (uint32_t doc = scorer->doc(); doc != query_v2::TERMINATED; doc = scorer->advance()) {
        rows[doc] = scorer->score();
    }
    return rows;
}

static void expect_scored_alike(const std::map<uint32_t, float>& listed,
                                const std::map<uint32_t, float>& streamed) {
    ASSERT_EQ(listed.size(), streamed.size());
    for (const auto& [doc, score] : streamed) {
        ASSERT_TRUE(listed.contains(doc)) << doc;
        EXPECT_FLOAT_EQ(listed.at(doc), score) << doc;
    }
}

// A scored phrase on a batching source lists its rows and scores each on the phrase frequency
// the verifier counts there and the source's norm, as the streamed phrase scores them.
TEST_F(PhraseQueryV2Test, AListedScoredPhraseScoresLikeTheStreamedOne) {
    std::map<uint32_t, float> streamed;
    for (const bool batches : {false, true}) {
        auto source = fake_phrase_source(batches);
        for (uint32_t doc = 0; doc < 16; ++doc) {
            source->norms[doc] = doc % 6 + 1;
        }
        query_v2::PhraseWeight weight(L"content", phrase_terms({"quick", "brown"}), {.slop = 1},
                                      std::make_shared<BM25Similarity>(2.0F, 8.0F),
                                      /*enable_scoring=*/true, /*nullable=*/false);
        const auto docs = scored_docs(weight, source);
        if (!batches) {
            streamed = docs;
            EXPECT_EQ(streamed.size(), 4U);
            continue;
        }
        expect_scored_alike(docs, streamed);
        EXPECT_EQ(source->opened_together,
                  (std::vector<std::vector<std::string>> {{"quick", "brown"}}));
        EXPECT_EQ(source->fetches, 1U);
    }
}

// An exact phrase whose terms hold many positions per document streams them: the block's listed
// rows are opened in turn instead of the block's positions being decoded at once.
TEST_F(PhraseQueryV2Test, AListedExactPhraseStreamsHeavyPositions) {
    auto source = fake_phrase_source(true);
    source->position_work = 128;
    EXPECT_EQ(fake_phrase_docs(source, {"quick", "brown"}, {}), (std::set<uint32_t> {0, 1, 8}));
    // The chain keeps rows 0, 1, 5 and 8, the first four documents of either term.
    const std::vector<std::vector<uint32_t>> first_four = {{0, 1, 2, 3}};
    EXPECT_EQ(source->streams["quick"], first_four);
    EXPECT_EQ(source->streams["brown"], first_four);
}

// Light positions, a sloppy or scored phrase, and a phrase repeating a term decode the block's
// positions as before.
TEST_F(PhraseQueryV2Test, AListedPhraseStreamsOnlyHeavyExactUnscoredDistinctTerms) {
    // Four rows: below 8 positions per document, or below 512 positions in all.
    for (const uint64_t work : {7, 63}) {
        auto source = fake_phrase_source(true);
        source->position_work = work;
        EXPECT_EQ(fake_phrase_docs(source, {"quick", "brown"}, {}), (std::set<uint32_t> {0, 1, 8}));
        EXPECT_TRUE(source->streams["quick"].empty()) << work;
    }
    auto sloppy = fake_phrase_source(true);
    sloppy->position_work = 128;
    EXPECT_EQ(fake_phrase_docs(sloppy, {"quick", "brown"}, {.slop = 1}),
              (std::set<uint32_t> {0, 1, 5, 8}));
    EXPECT_TRUE(sloppy->streams["quick"].empty());
    auto repeated = fake_phrase_source(true);
    repeated->position_work = 128;
    EXPECT_TRUE(fake_phrase_docs(repeated, {"quick", "brown", "quick"}, {}).empty());
    EXPECT_TRUE(repeated->streams["quick"].empty());
    auto scored = fake_phrase_source(true);
    scored->position_work = 128;
    query_v2::PhraseWeight weight(L"content", phrase_terms({"quick", "brown"}), {},
                                  std::make_shared<BM25Similarity>(2.0F, 8.0F),
                                  /*enable_scoring=*/true, /*nullable=*/false);
    EXPECT_EQ(scored_docs(weight, scored).size(), 3U);
    EXPECT_TRUE(scored->streams["quick"].empty());
}

// `count` positions from `first`, `step` apart.
static std::vector<uint32_t> stepped(uint32_t first, uint32_t step, uint32_t count) {
    std::vector<uint32_t> positions(count);
    for (uint32_t i = 0; i < count; ++i) {
        positions[i] = first + i * step;
    }
    return positions;
}

// Rows whose positions run past a 16-position chunk: row 0 matches in the third chunk of
// "quick", row 2 at the last position of its first chunk and the first of the second chunk of
// "brown", and rows 1 and 3 fill whole chunks without a match.
static std::shared_ptr<index_query::testing::FakeIndexSource> chunked_phrase_source(
        uint64_t position_work) {
    auto source = std::make_shared<index_query::testing::FakeIndexSource>();
    source->batches = true;
    source->set_doc_count(8);
    source->position_work = position_work;
    std::vector<uint32_t> edge_quick = stepped(0, 4, 16);
    edge_quick.insert(edge_quick.end(), {100, 104});
    std::vector<uint32_t> edge_brown = stepped(2, 4, 15);
    edge_brown.insert(edge_brown.end(), {59, 61});
    source->add("quick", {posting(0, stepped(0, 2, 40)), posting(1, stepped(0, 1, 16)),
                          posting(2, edge_quick), posting(3, stepped(0, 4, 32))});
    source->add("brown", {posting(0, {79}), posting(1, stepped(100, 1, 16)), posting(2, edge_brown),
                          posting(3, stepped(2, 4, 32))});
    return source;
}

// A streamed phrase finds what the whole-block decode finds when its rows' positions span
// several chunks.
TEST_F(PhraseQueryV2Test, AStreamedPhraseMatchesAcrossChunks) {
    auto streamed = chunked_phrase_source(128);
    EXPECT_EQ(fake_phrase_docs(streamed, {"quick", "brown"}, {}), (std::set<uint32_t> {0, 2}));
    EXPECT_EQ(streamed->streams["quick"], (std::vector<std::vector<uint32_t>> {{0, 1, 2, 3}}));
    auto decoded = chunked_phrase_source(7);
    EXPECT_EQ(fake_phrase_docs(decoded, {"quick", "brown"}, {}), (std::set<uint32_t> {0, 2}));
    EXPECT_TRUE(decoded->streams["quick"].empty());
}

TEST_F(PhraseQueryV2Test, AListedScoredPhrasePrefixScoresLikeTheStreamedOne) {
    std::map<uint32_t, float> streamed;
    for (const bool batches : {false, true}) {
        auto source = fake_prefix_source(batches);
        for (uint32_t doc = 0; doc < 16; ++doc) {
            source->norms[doc] = doc % 6 + 1;
        }
        query_v2::PhrasePrefixWeight weight(L"content", {{0, "quick"}}, {1, "bro"},
                                            std::make_shared<BM25Similarity>(2.0F, 8.0F),
                                            /*enable_scoring=*/true, /*max_expansions=*/50, {},
                                            /*suffix=*/false, /*nullable=*/false);
        const auto docs = scored_docs(weight, source);
        if (!batches) {
            streamed = docs;
            EXPECT_EQ(streamed.size(), 5U);
            continue;
        }
        expect_scored_alike(docs, streamed);
        EXPECT_EQ(source->opened_together,
                  (std::vector<std::vector<std::string>> {{"quick"}, {"bronze", "brown"}}));
    }
}

// Scored, the gathered tail's positions give each row the phrase frequency the streamed phrase
// counts there.
TEST_F(PhraseQueryV2Test, AListedScoredPhrasePrefixOfManyExpansionsScoresLikeTheStreamedOne) {
    std::map<uint32_t, float> streamed;
    for (const bool batches : {false, true}) {
        auto source = many_prefix_source(batches);
        query_v2::PhrasePrefixWeight weight(L"content", {{0, "quick"}}, {1, "br"},
                                            std::make_shared<BM25Similarity>(2.0F, 8.0F),
                                            /*enable_scoring=*/true, /*max_expansions=*/50, {},
                                            /*suffix=*/false, /*nullable=*/false);
        const auto docs = scored_docs(weight, source);
        if (!batches) {
            streamed = docs;
            EXPECT_EQ(streamed.size(), 16U);
            continue;
        }
        expect_scored_alike(docs, streamed);
        EXPECT_EQ(source->opened_together.size(), 3U);
    }
}

TEST_F(PhraseQueryV2Test, AListedScoredMultiPhraseScoresLikeTheStreamedOne) {
    std::vector<TermInfo> term_infos = phrase_terms({"quick", "brown"});
    term_infos[1].term = std::vector<std::string> {"brown", "bronze"};
    std::map<uint32_t, float> streamed;
    for (const bool batches : {false, true}) {
        auto source = fake_prefix_source(batches);
        for (uint32_t doc = 0; doc < 16; ++doc) {
            source->norms[doc] = doc % 6 + 1;
        }
        query_v2::MultiPhraseWeight weight(L"content", term_infos, {},
                                           std::make_shared<BM25Similarity>(2.0F, 8.0F),
                                           /*enable_scoring=*/true, /*nullable=*/false);
        const auto docs = scored_docs(weight, source);
        if (!batches) {
            streamed = docs;
            EXPECT_EQ(streamed.size(), 5U);
            continue;
        }
        expect_scored_alike(docs, streamed);
        EXPECT_EQ(source->opened_together,
                  (std::vector<std::vector<std::string>> {{"quick", "bronze", "brown"}}));
    }
}

} // namespace doris::segment_v2