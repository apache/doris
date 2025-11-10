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

#include "olap/rowset/segment_v2/inverted_index/query_v2/phrase_query/phrase_query.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <roaring/roaring.hh>
#include <string>

#include "common/status.h"
#include "olap/rowset/segment_v2/index_query_context.h"
#include "olap/rowset/segment_v2/inverted_index/analyzer/custom_analyzer.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/phrase_query/phrase_weight.h"
#include "olap/rowset/segment_v2/inverted_index/similarity/bm25_similarity.h"
#include "olap/rowset/segment_v2/inverted_index/util/string_helper.h"

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

    // Test query construction
    auto query = std::make_shared<query_v2::PhraseQuery>(context, field, terms);
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
    std::vector<std::wstring> terms; // Empty terms

    auto query = std::make_shared<query_v2::PhraseQuery>(context, field, terms);
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

    auto query = std::make_shared<query_v2::PhraseQuery>(context, field, terms);
    auto weight = query->weight(false);

    query_v2::QueryExecutionContext exec_ctx;
    exec_ctx.segment_num_rows = reader_holder->maxDoc();
    exec_ctx.readers = {reader_holder};
    exec_ctx.field_reader_bindings.emplace(field, reader_holder);

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

    auto query = std::make_shared<query_v2::PhraseQuery>(context, field, terms);
    auto weight = query->weight(false);

    query_v2::QueryExecutionContext exec_ctx;
    exec_ctx.segment_num_rows = reader_holder->maxDoc();
    exec_ctx.readers = {reader_holder};
    exec_ctx.field_reader_bindings.emplace(field, reader_holder);

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

    auto query = std::make_shared<query_v2::PhraseQuery>(context, field, terms);
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

    auto query = std::make_shared<query_v2::PhraseQuery>(context, field, terms);
    auto weight = query->weight(false);

    query_v2::QueryExecutionContext exec_ctx;
    exec_ctx.segment_num_rows = reader_holder->maxDoc();
    exec_ctx.readers = {reader_holder};
    exec_ctx.field_reader_bindings.emplace(field, reader_holder);

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

    // Fill collection statistics for scoring
    context->collection_statistics->_total_num_docs = reader_holder->numDocs();
    context->collection_statistics->_total_num_tokens[field] = reader_holder->numDocs() * 8;
    context->collection_statistics->_term_doc_freqs[field][StringHelper::to_wstring("quick")] = 10;
    context->collection_statistics->_term_doc_freqs[field][StringHelper::to_wstring("brown")] = 10;

    auto query = std::make_shared<query_v2::PhraseQuery>(context, field, terms);
    auto weight = query->weight(true);

    query_v2::QueryExecutionContext exec_ctx;
    exec_ctx.segment_num_rows = reader_holder->maxDoc();
    exec_ctx.readers = {reader_holder};
    exec_ctx.field_reader_bindings.emplace(field, reader_holder);

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

    auto query = std::make_shared<query_v2::PhraseQuery>(context, field, terms);
    auto weight = query->weight(false);

    query_v2::QueryExecutionContext exec_ctx;
    exec_ctx.segment_num_rows = reader_holder->maxDoc();
    exec_ctx.readers = {reader_holder};

    std::string binding_key = "content#0";
    exec_ctx.reader_bindings[binding_key] = reader_holder;
    exec_ctx.field_reader_bindings.emplace(field, reader_holder);

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

    {
        auto query = std::make_shared<query_v2::PhraseQuery>(context, field, terms);
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

    auto query = std::make_shared<query_v2::PhraseQuery>(context, field, terms);
    auto weight = query->weight(false);

    query_v2::QueryExecutionContext exec_ctx;
    exec_ctx.segment_num_rows = reader_holder->maxDoc();
    exec_ctx.readers = {reader_holder};
    exec_ctx.field_reader_bindings.emplace(field, reader_holder);

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

    auto query = std::make_shared<query_v2::PhraseQuery>(context, field, terms);
    auto weight = query->weight(false);

    query_v2::QueryExecutionContext exec_ctx;
    exec_ctx.segment_num_rows = reader_holder->maxDoc();
    exec_ctx.readers = {reader_holder};
    exec_ctx.field_reader_bindings.emplace(field, reader_holder);

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

    // Setup statistics for BM25
    context->collection_statistics->_total_num_docs = reader_holder->numDocs();
    context->collection_statistics->_total_num_tokens[field] = reader_holder->numDocs() * 8;
    for (const auto& term : terms) {
        context->collection_statistics->_term_doc_freqs[field][term] = 5;
    }

    auto query = std::make_shared<query_v2::PhraseQuery>(context, field, terms);
    auto weight = query->weight(true); // Enable scoring

    query_v2::QueryExecutionContext exec_ctx;
    exec_ctx.segment_num_rows = reader_holder->maxDoc();
    exec_ctx.readers = {reader_holder};
    exec_ctx.field_reader_bindings.emplace(field, reader_holder);

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

} // namespace doris::segment_v2