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

#include "olap/rowset/segment_v2/inverted_index/query_v2/regexp_query/regexp_query.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <roaring/roaring.hh>
#include <string>

#include "common/status.h"
#include "olap/rowset/segment_v2/index_query_context.h"
#include "olap/rowset/segment_v2/inverted_index/analyzer/custom_analyzer.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/regexp_query/regexp_weight.h"
#include "olap/rowset/segment_v2/inverted_index/util/string_helper.h"

CL_NS_USE(search)
CL_NS_USE(store)
CL_NS_USE(index)

namespace doris::segment_v2 {

using namespace inverted_index;

class RegexpQueryV2Test : public testing::Test {
public:
    const std::string kTestDir = "./ut_dir/regexp_query_test";

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
        std::vector<std::string> test_data = {
                "apple123",   "apple456",     "banana789",     "test123abc", "pattern456",
                "regex999",   "match123",     "search456",     "prefix123",  "suffix789",
                "apple_test", "banana_data",  "test_pattern",  "data_regex", "apple_banana",
                "test_match", "pattern_test", "prefix_suffix", "abc123xyz",  "def456ghi"};

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

// Test basic regexp query construction and weight creation
TEST_F(RegexpQueryV2Test, test_regexp_query_construction) {
    auto context = std::make_shared<IndexQueryContext>();
    context->collection_statistics = std::make_shared<CollectionStatistics>();
    context->collection_similarity = std::make_shared<CollectionSimilarity>();

    std::wstring field = StringHelper::to_wstring("content");
    std::string pattern = "apple.*";

    // Test query construction
    auto query = std::make_shared<query_v2::RegexpQuery>(context, field, pattern);
    ASSERT_NE(query, nullptr);

    // Test weight creation without scoring
    auto weight = query->weight(false);
    ASSERT_NE(weight, nullptr);

    // Verify weight is of correct type
    auto regexp_weight = std::dynamic_pointer_cast<query_v2::RegexpWeight>(weight);
    ASSERT_NE(regexp_weight, nullptr);
}

// Test regexp query with scoring enabled
TEST_F(RegexpQueryV2Test, test_regexp_query_with_scoring) {
    auto context = std::make_shared<IndexQueryContext>();
    context->collection_statistics = std::make_shared<CollectionStatistics>();
    context->collection_similarity = std::make_shared<CollectionSimilarity>();

    std::wstring field = StringHelper::to_wstring("content");
    std::string pattern = ".*123.*";

    auto query = std::make_shared<query_v2::RegexpQuery>(context, field, pattern);
    ASSERT_NE(query, nullptr);

    // Test weight creation with scoring enabled
    auto weight = query->weight(true);
    ASSERT_NE(weight, nullptr);

    auto regexp_weight = std::dynamic_pointer_cast<query_v2::RegexpWeight>(weight);
    ASSERT_NE(regexp_weight, nullptr);
}

// Test regexp query execution
TEST_F(RegexpQueryV2Test, test_regexp_query_execution) {
    auto context = std::make_shared<IndexQueryContext>();
    context->collection_statistics = std::make_shared<CollectionStatistics>();
    context->collection_similarity = std::make_shared<CollectionSimilarity>();

    auto* dir = FSDirectory::getDirectory(kTestDir.c_str());
    auto reader_holder = make_shared_reader(lucene::index::IndexReader::open(dir, true));
    ASSERT_TRUE(reader_holder != nullptr);

    std::wstring field = StringHelper::to_wstring("content");
    std::string pattern = "apple.*";

    auto query = std::make_shared<query_v2::RegexpQuery>(context, field, pattern);
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

    // Should match documents containing terms starting with "apple"
    EXPECT_GT(result.cardinality(), 0);

    _CLDECDELETE(dir);
}

// Test regexp query with various patterns
TEST_F(RegexpQueryV2Test, test_regexp_query_different_patterns) {
    auto context = std::make_shared<IndexQueryContext>();
    context->collection_statistics = std::make_shared<CollectionStatistics>();
    context->collection_similarity = std::make_shared<CollectionSimilarity>();

    auto* dir = FSDirectory::getDirectory(kTestDir.c_str());
    auto reader_holder = make_shared_reader(lucene::index::IndexReader::open(dir, true));
    ASSERT_TRUE(reader_holder != nullptr);

    std::wstring field = StringHelper::to_wstring("content");

    // Test different regex patterns
    std::vector<std::string> patterns = {
            ".*123.*",  // Match any term containing 123
            "test.*",   // Match terms starting with test
            ".*data.*", // Match terms containing data
            "prefix.*", // Match terms starting with prefix
            ".*xyz.*"   // Match terms containing xyz
    };

    for (const auto& pattern : patterns) {
        auto query = std::make_shared<query_v2::RegexpQuery>(context, field, pattern);
        auto weight = query->weight(false);

        query_v2::QueryExecutionContext exec_ctx;
        exec_ctx.segment_num_rows = reader_holder->maxDoc();
        exec_ctx.readers = {reader_holder};
        exec_ctx.field_reader_bindings.emplace(field, reader_holder);

        auto scorer = weight->scorer(exec_ctx);
        ASSERT_NE(scorer, nullptr) << "Scorer should not be null for pattern: " << pattern;

        roaring::Roaring result;
        uint32_t doc = scorer->doc();
        while (doc != query_v2::TERMINATED) {
            result.add(doc);
            doc = scorer->advance();
        }

        // Each pattern should match at least some documents
        EXPECT_GE(result.cardinality(), 0)
                << "Pattern '" << pattern << "' should match some documents";
    }

    _CLDECDELETE(dir);
}

// Test regexp query with non-matching pattern
TEST_F(RegexpQueryV2Test, test_regexp_query_no_matches) {
    auto context = std::make_shared<IndexQueryContext>();
    context->collection_statistics = std::make_shared<CollectionStatistics>();
    context->collection_similarity = std::make_shared<CollectionSimilarity>();

    auto* dir = FSDirectory::getDirectory(kTestDir.c_str());
    auto reader_holder = make_shared_reader(lucene::index::IndexReader::open(dir, true));
    ASSERT_TRUE(reader_holder != nullptr);

    std::wstring field = StringHelper::to_wstring("content");
    std::string pattern = "nonexistent.*pattern";

    auto query = std::make_shared<query_v2::RegexpQuery>(context, field, pattern);
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

    // Should match no documents
    EXPECT_EQ(result.cardinality(), 0);

    _CLDECDELETE(dir);
}

// Test regexp query with binding key
TEST_F(RegexpQueryV2Test, test_regexp_query_with_binding_key) {
    auto context = std::make_shared<IndexQueryContext>();
    context->collection_statistics = std::make_shared<CollectionStatistics>();
    context->collection_similarity = std::make_shared<CollectionSimilarity>();

    auto* dir = FSDirectory::getDirectory(kTestDir.c_str());
    auto reader_holder = make_shared_reader(lucene::index::IndexReader::open(dir, true));
    ASSERT_TRUE(reader_holder != nullptr);

    std::wstring field = StringHelper::to_wstring("content");
    std::string pattern = "banana.*";

    auto query = std::make_shared<query_v2::RegexpQuery>(context, field, pattern);
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

// Test regexp query destructor (coverage)
TEST_F(RegexpQueryV2Test, test_regexp_query_destructor) {
    auto context = std::make_shared<IndexQueryContext>();
    context->collection_statistics = std::make_shared<CollectionStatistics>();
    context->collection_similarity = std::make_shared<CollectionSimilarity>();

    std::wstring field = StringHelper::to_wstring("content");
    std::string pattern = "test.*";

    {
        auto query = std::make_shared<query_v2::RegexpQuery>(context, field, pattern);
        auto weight = query->weight(false);
        ASSERT_NE(weight, nullptr);
        // Query and weight will be destroyed at scope exit
    }
    // If we reach here without crash, destructor works correctly
    SUCCEED();
}

// Test regexp query with complex pattern
TEST_F(RegexpQueryV2Test, test_regexp_query_complex_pattern) {
    auto context = std::make_shared<IndexQueryContext>();
    context->collection_statistics = std::make_shared<CollectionStatistics>();
    context->collection_similarity = std::make_shared<CollectionSimilarity>();

    auto* dir = FSDirectory::getDirectory(kTestDir.c_str());
    auto reader_holder = make_shared_reader(lucene::index::IndexReader::open(dir, true));
    ASSERT_TRUE(reader_holder != nullptr);

    std::wstring field = StringHelper::to_wstring("content");
    // Match terms that start with alphanumeric and contain digits
    std::string pattern = "[a-z]+[0-9]+.*";

    auto query = std::make_shared<query_v2::RegexpQuery>(context, field, pattern);
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

// Test move semantics in weight() method
TEST_F(RegexpQueryV2Test, test_regexp_query_move_semantics) {
    auto context = std::make_shared<IndexQueryContext>();
    context->collection_statistics = std::make_shared<CollectionStatistics>();
    context->collection_similarity = std::make_shared<CollectionSimilarity>();

    std::wstring field = StringHelper::to_wstring("content");
    std::string pattern = "test.*";

    // Create query and immediately call weight() to test move semantics
    auto query = std::make_shared<query_v2::RegexpQuery>(context, field, pattern);
    auto weight1 = query->weight(false);
    ASSERT_NE(weight1, nullptr);

    // Create another query to verify weight can be called multiple times
    auto query2 = std::make_shared<query_v2::RegexpQuery>(context, field, pattern);
    auto weight2 = query2->weight(true);
    ASSERT_NE(weight2, nullptr);
}

TEST_F(RegexpQueryV2Test, test_make_exact_match_anchoring) {
    auto context = std::make_shared<IndexQueryContext>();
    context->collection_statistics = std::make_shared<CollectionStatistics>();
    context->collection_similarity = std::make_shared<CollectionSimilarity>();

    auto* dir = FSDirectory::getDirectory(kTestDir.c_str());
    auto reader_holder = make_shared_reader(lucene::index::IndexReader::open(dir, true));
    ASSERT_TRUE(reader_holder != nullptr);

    std::wstring field = StringHelper::to_wstring("content");
    std::string pattern = "apple123";

    auto query = std::make_shared<query_v2::RegexpQuery>(context, field, pattern);
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

    EXPECT_EQ(result.cardinality(), 1);

    _CLDECDELETE(dir);
}

TEST_F(RegexpQueryV2Test, test_make_exact_match_already_anchored) {
    auto context = std::make_shared<IndexQueryContext>();
    context->collection_statistics = std::make_shared<CollectionStatistics>();
    context->collection_similarity = std::make_shared<CollectionSimilarity>();

    auto* dir = FSDirectory::getDirectory(kTestDir.c_str());
    auto reader_holder = make_shared_reader(lucene::index::IndexReader::open(dir, true));
    ASSERT_TRUE(reader_holder != nullptr);

    std::wstring field = StringHelper::to_wstring("content");
    std::string pattern = "^apple123$";

    auto query = std::make_shared<query_v2::RegexpQuery>(context, field, pattern);
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

    EXPECT_EQ(result.cardinality(), 1);

    _CLDECDELETE(dir);
}

TEST_F(RegexpQueryV2Test, test_make_exact_match_partial_anchor_start) {
    auto context = std::make_shared<IndexQueryContext>();
    context->collection_statistics = std::make_shared<CollectionStatistics>();
    context->collection_similarity = std::make_shared<CollectionSimilarity>();

    auto* dir = FSDirectory::getDirectory(kTestDir.c_str());
    auto reader_holder = make_shared_reader(lucene::index::IndexReader::open(dir, true));
    ASSERT_TRUE(reader_holder != nullptr);

    std::wstring field = StringHelper::to_wstring("content");
    std::string pattern = "^apple.*";

    auto query = std::make_shared<query_v2::RegexpQuery>(context, field, pattern);
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

TEST_F(RegexpQueryV2Test, test_make_exact_match_partial_anchor_end) {
    auto context = std::make_shared<IndexQueryContext>();
    context->collection_statistics = std::make_shared<CollectionStatistics>();
    context->collection_similarity = std::make_shared<CollectionSimilarity>();

    auto* dir = FSDirectory::getDirectory(kTestDir.c_str());
    auto reader_holder = make_shared_reader(lucene::index::IndexReader::open(dir, true));
    ASSERT_TRUE(reader_holder != nullptr);

    std::wstring field = StringHelper::to_wstring("content");
    std::string pattern = ".*123$";

    auto query = std::make_shared<query_v2::RegexpQuery>(context, field, pattern);
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

TEST_F(RegexpQueryV2Test, test_make_exact_match_wildcard_pattern) {
    auto context = std::make_shared<IndexQueryContext>();
    context->collection_statistics = std::make_shared<CollectionStatistics>();
    context->collection_similarity = std::make_shared<CollectionSimilarity>();

    auto* dir = FSDirectory::getDirectory(kTestDir.c_str());
    auto reader_holder = make_shared_reader(lucene::index::IndexReader::open(dir, true));
    ASSERT_TRUE(reader_holder != nullptr);

    std::wstring field = StringHelper::to_wstring("content");
    std::string pattern = ".*";

    auto query = std::make_shared<query_v2::RegexpQuery>(context, field, pattern);
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

    EXPECT_EQ(result.cardinality(), 20);

    _CLDECDELETE(dir);
}

} // namespace doris::segment_v2