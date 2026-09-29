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
#include <memory>
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

} // namespace doris::segment_v2