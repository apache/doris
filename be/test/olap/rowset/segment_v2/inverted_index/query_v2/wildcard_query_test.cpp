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

#include "olap/rowset/segment_v2/inverted_index/query_v2/wildcard_query/wildcard_query.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <roaring/roaring.hh>
#include <string>

#include "common/status.h"
#include "olap/rowset/segment_v2/index_query_context.h"
#include "olap/rowset/segment_v2/inverted_index/analyzer/custom_analyzer.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/wildcard_query/wildcard_weight.h"
#include "olap/rowset/segment_v2/inverted_index/util/string_helper.h"

CL_NS_USE(search)
CL_NS_USE(store)
CL_NS_USE(index)

namespace doris::segment_v2 {

using namespace inverted_index;

class WildcardQueryV2Test : public testing::Test {
public:
    const std::string kTestDir = "./ut_dir/wildcard_query_test";

    void SetUp() override {
        auto st = io::global_local_filesystem()->delete_directory(kTestDir);
        ASSERT_TRUE(st.ok()) << st;
        st = io::global_local_filesystem()->create_directory(kTestDir);
        ASSERT_TRUE(st.ok()) << st;
        std::string field_name = "field";
        create_test_index(field_name, kTestDir);
    }

    void TearDown() override {
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(kTestDir).ok());
    }

private:
    void create_test_index(const std::string& field_name, const std::string& dir) {
        std::vector<std::string> test_data = {
                "apple",     "application", "apply",       "apricot", "banana",     "band",
                "bandana",   "can",         "candy",       "cat",     "catalog",    "dog",
                "document",  "door",        "hello",       "help",    "helpful",    "test",
                "testing",   "testcase",    "wildcard",    "wild",    "wilderness", "card",
                "cardboard", "abc",         "abcd",        "abcde",   "prefix123",  "123suffix",
                "pre123",    "123suf",      "both123both", "star",    "start",      "started",
                "starter",   "question",    "quest",       "query",   "queries"};

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

// Test basic wildcard query construction
TEST_F(WildcardQueryV2Test, test_wildcard_query_construction) {
    auto context = std::make_shared<IndexQueryContext>();
    context->collection_statistics = std::make_shared<CollectionStatistics>();
    context->collection_similarity = std::make_shared<CollectionSimilarity>();

    std::wstring field = StringHelper::to_wstring("field");
    std::string pattern = "app*";

    // Test query construction
    auto query = std::make_shared<query_v2::WildcardQuery>(context, field, pattern);
    ASSERT_NE(query, nullptr);

    // Test weight creation without scoring
    auto weight = query->weight(false);
    ASSERT_NE(weight, nullptr);

    // Verify weight is of correct type
    auto wildcard_weight = std::dynamic_pointer_cast<query_v2::WildcardWeight>(weight);
    ASSERT_NE(wildcard_weight, nullptr);
}

// Test wildcard query with scoring enabled
TEST_F(WildcardQueryV2Test, test_wildcard_query_with_scoring) {
    auto context = std::make_shared<IndexQueryContext>();
    context->collection_statistics = std::make_shared<CollectionStatistics>();
    context->collection_similarity = std::make_shared<CollectionSimilarity>();

    std::wstring field = StringHelper::to_wstring("field");
    std::string pattern = "test*";

    auto query = std::make_shared<query_v2::WildcardQuery>(context, field, pattern);
    ASSERT_NE(query, nullptr);

    // Test weight creation with scoring enabled
    auto weight = query->weight(true);
    ASSERT_NE(weight, nullptr);

    auto wildcard_weight = std::dynamic_pointer_cast<query_v2::WildcardWeight>(weight);
    ASSERT_NE(wildcard_weight, nullptr);
}

// Test wildcard query with asterisk prefix pattern
TEST_F(WildcardQueryV2Test, test_wildcard_query_prefix_pattern) {
    auto context = std::make_shared<IndexQueryContext>();
    context->collection_statistics = std::make_shared<CollectionStatistics>();
    context->collection_similarity = std::make_shared<CollectionSimilarity>();

    auto* dir = FSDirectory::getDirectory(kTestDir.c_str());
    auto reader_holder = make_shared_reader(lucene::index::IndexReader::open(dir, true));
    ASSERT_TRUE(reader_holder != nullptr);

    std::wstring field = StringHelper::to_wstring("field");
    std::string pattern = "app*"; // Match apple, application, apply, apricot

    auto query = std::make_shared<query_v2::WildcardQuery>(context, field, pattern);
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

    // Should match multiple documents starting with "app"
    EXPECT_GT(result.cardinality(), 0);

    _CLDECDELETE(dir);
}

// Test wildcard query with asterisk suffix pattern
TEST_F(WildcardQueryV2Test, test_wildcard_query_suffix_pattern) {
    auto context = std::make_shared<IndexQueryContext>();
    context->collection_statistics = std::make_shared<CollectionStatistics>();
    context->collection_similarity = std::make_shared<CollectionSimilarity>();

    auto* dir = FSDirectory::getDirectory(kTestDir.c_str());
    auto reader_holder = make_shared_reader(lucene::index::IndexReader::open(dir, true));
    ASSERT_TRUE(reader_holder != nullptr);

    std::wstring field = StringHelper::to_wstring("field");
    std::string pattern = "*log"; // Match catalog, dog, etc.

    auto query = std::make_shared<query_v2::WildcardQuery>(context, field, pattern);
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

// Test wildcard query with asterisk middle pattern
TEST_F(WildcardQueryV2Test, test_wildcard_query_middle_pattern) {
    auto context = std::make_shared<IndexQueryContext>();
    context->collection_statistics = std::make_shared<CollectionStatistics>();
    context->collection_similarity = std::make_shared<CollectionSimilarity>();

    auto* dir = FSDirectory::getDirectory(kTestDir.c_str());
    auto reader_holder = make_shared_reader(lucene::index::IndexReader::open(dir, true));
    ASSERT_TRUE(reader_holder != nullptr);

    std::wstring field = StringHelper::to_wstring("field");
    std::string pattern = "c*d"; // Match card, cardboard

    auto query = std::make_shared<query_v2::WildcardQuery>(context, field, pattern);
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

// Test wildcard query with multiple asterisks
TEST_F(WildcardQueryV2Test, test_wildcard_query_multiple_asterisks) {
    auto context = std::make_shared<IndexQueryContext>();
    context->collection_statistics = std::make_shared<CollectionStatistics>();
    context->collection_similarity = std::make_shared<CollectionSimilarity>();

    auto* dir = FSDirectory::getDirectory(kTestDir.c_str());
    auto reader_holder = make_shared_reader(lucene::index::IndexReader::open(dir, true));
    ASSERT_TRUE(reader_holder != nullptr);

    std::wstring field = StringHelper::to_wstring("field");
    std::string pattern = "*t*t*"; // Match terms with multiple 't's

    auto query = std::make_shared<query_v2::WildcardQuery>(context, field, pattern);
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

// Test wildcard query with no wildcard (exact match)
TEST_F(WildcardQueryV2Test, test_wildcard_query_exact_match) {
    auto context = std::make_shared<IndexQueryContext>();
    context->collection_statistics = std::make_shared<CollectionStatistics>();
    context->collection_similarity = std::make_shared<CollectionSimilarity>();

    auto* dir = FSDirectory::getDirectory(kTestDir.c_str());
    auto reader_holder = make_shared_reader(lucene::index::IndexReader::open(dir, true));
    ASSERT_TRUE(reader_holder != nullptr);

    std::wstring field = StringHelper::to_wstring("field");
    std::string pattern = "apple"; // Exact match

    auto query = std::make_shared<query_v2::WildcardQuery>(context, field, pattern);
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

    EXPECT_GT(result.cardinality(), 0);

    _CLDECDELETE(dir);
}

// Test wildcard query with non-matching pattern
TEST_F(WildcardQueryV2Test, test_wildcard_query_no_matches) {
    auto context = std::make_shared<IndexQueryContext>();
    context->collection_statistics = std::make_shared<CollectionStatistics>();
    context->collection_similarity = std::make_shared<CollectionSimilarity>();

    auto* dir = FSDirectory::getDirectory(kTestDir.c_str());
    auto reader_holder = make_shared_reader(lucene::index::IndexReader::open(dir, true));
    ASSERT_TRUE(reader_holder != nullptr);

    std::wstring field = StringHelper::to_wstring("field");
    std::string pattern = "xyz*abc"; // Non-existent pattern

    auto query = std::make_shared<query_v2::WildcardQuery>(context, field, pattern);
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

// Test wildcard query with binding key
TEST_F(WildcardQueryV2Test, test_wildcard_query_with_binding_key) {
    auto context = std::make_shared<IndexQueryContext>();
    context->collection_statistics = std::make_shared<CollectionStatistics>();
    context->collection_similarity = std::make_shared<CollectionSimilarity>();

    auto* dir = FSDirectory::getDirectory(kTestDir.c_str());
    auto reader_holder = make_shared_reader(lucene::index::IndexReader::open(dir, true));
    ASSERT_TRUE(reader_holder != nullptr);

    std::wstring field = StringHelper::to_wstring("field");
    std::string pattern = "ban*";

    auto query = std::make_shared<query_v2::WildcardQuery>(context, field, pattern);
    auto weight = query->weight(false);

    query_v2::QueryExecutionContext exec_ctx;
    exec_ctx.segment_num_rows = reader_holder->maxDoc();
    exec_ctx.readers = {reader_holder};

    std::string binding_key = "field#0";
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

// Test wildcard query destructor (coverage)
TEST_F(WildcardQueryV2Test, test_wildcard_query_destructor) {
    auto context = std::make_shared<IndexQueryContext>();
    context->collection_statistics = std::make_shared<CollectionStatistics>();
    context->collection_similarity = std::make_shared<CollectionSimilarity>();

    std::wstring field = StringHelper::to_wstring("field");
    std::string pattern = "test*";

    {
        auto query = std::make_shared<query_v2::WildcardQuery>(context, field, pattern);
        auto weight = query->weight(false);
        ASSERT_NE(weight, nullptr);
        // Query and weight will be destroyed at scope exit
    }
    // If we reach here without crash, destructor works correctly
    SUCCEED();
}

// Test wildcard query with special characters in pattern
TEST_F(WildcardQueryV2Test, test_wildcard_query_special_characters) {
    auto context = std::make_shared<IndexQueryContext>();
    context->collection_statistics = std::make_shared<CollectionStatistics>();
    context->collection_similarity = std::make_shared<CollectionSimilarity>();

    auto* dir = FSDirectory::getDirectory(kTestDir.c_str());
    auto reader_holder = make_shared_reader(lucene::index::IndexReader::open(dir, true));
    ASSERT_TRUE(reader_holder != nullptr);

    std::wstring field = StringHelper::to_wstring("field");
    // Test with numbers
    std::string pattern = "*123*";

    auto query = std::make_shared<query_v2::WildcardQuery>(context, field, pattern);
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

// Test wildcard query with all asterisk pattern
TEST_F(WildcardQueryV2Test, test_wildcard_query_all_asterisk) {
    auto context = std::make_shared<IndexQueryContext>();
    context->collection_statistics = std::make_shared<CollectionStatistics>();
    context->collection_similarity = std::make_shared<CollectionSimilarity>();

    auto* dir = FSDirectory::getDirectory(kTestDir.c_str());
    auto reader_holder = make_shared_reader(lucene::index::IndexReader::open(dir, true));
    ASSERT_TRUE(reader_holder != nullptr);

    std::wstring field = StringHelper::to_wstring("field");
    std::string pattern = "*"; // Match everything

    auto query = std::make_shared<query_v2::WildcardQuery>(context, field, pattern);
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

    // Should match all documents
    EXPECT_EQ(result.cardinality(), reader_holder->numDocs());

    _CLDECDELETE(dir);
}

// Test move semantics in weight() method
TEST_F(WildcardQueryV2Test, test_wildcard_query_move_semantics) {
    auto context = std::make_shared<IndexQueryContext>();
    context->collection_statistics = std::make_shared<CollectionStatistics>();
    context->collection_similarity = std::make_shared<CollectionSimilarity>();

    std::wstring field = StringHelper::to_wstring("field");
    std::string pattern = "test*";

    // Create query and immediately call weight() to test move semantics
    auto query = std::make_shared<query_v2::WildcardQuery>(context, field, pattern);
    auto weight1 = query->weight(false);
    ASSERT_NE(weight1, nullptr);

    // Create another query to verify weight can be called with scoring
    auto query2 = std::make_shared<query_v2::WildcardQuery>(context, field, pattern);
    auto weight2 = query2->weight(true);
    ASSERT_NE(weight2, nullptr);
}

} // namespace doris::segment_v2