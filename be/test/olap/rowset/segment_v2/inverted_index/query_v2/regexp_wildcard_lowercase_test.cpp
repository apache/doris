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

#include <memory>
#include <roaring/roaring.hh>
#include <string>
#include <vector>

#include "io/fs/local_file_system.h"
#include "olap/rowset/segment_v2/index_query_context.h"
#include "olap/rowset/segment_v2/inverted_index/analyzer/custom_analyzer.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/regexp_query/regexp_query.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/wildcard_query/wildcard_query.h"
#include "olap/rowset/segment_v2/inverted_index/util/string_helper.h"

CL_NS_USE(search)
CL_NS_USE(store)
CL_NS_USE(index)

namespace doris::segment_v2 {

using namespace inverted_index;
using namespace inverted_index::query_v2;

// Test that REGEXP queries match directly against the term dictionary (no lowercasing),
// while WILDCARD queries are expected to receive already-lowercased patterns from function_search.cpp.
//
// This test creates an index with lowercased terms (simulating parser=english, lower_case=true)
// and verifies:
// 1. REGEXP with uppercase pattern does NOT match lowercased terms (ES-compatible)
// 2. REGEXP with lowercase pattern DOES match lowercased terms
// 3. WILDCARD with lowercase pattern DOES match lowercased terms
class RegexpWildcardLowercaseTest : public testing::Test {
public:
    const std::string kTestDir = "./ut_dir/regexp_wildcard_lowercase_test";

    void SetUp() override {
        auto st = io::global_local_filesystem()->delete_directory(kTestDir);
        ASSERT_TRUE(st.ok()) << st;
        st = io::global_local_filesystem()->create_directory(kTestDir);
        ASSERT_TRUE(st.ok()) << st;
        // Create index with lowercased terms (simulating lower_case=true analyzer)
        create_test_index("title", kTestDir);
    }

    void TearDown() override {
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(kTestDir).ok());
    }

private:
    void create_test_index(const std::string& field_name, const std::string& dir) {
        // Simulate data that was indexed with lower_case=true:
        // Original data: "ABC DEF", "abc def", "Apple Banana", "cherry date"
        // After english analyzer with lower_case=true, terms are all lowercase
        std::vector<std::string> test_data = {"abc def", "abc def", "apple banana", "cherry date"};

        // Use standard tokenizer (which lowercases by default)
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

static std::vector<uint32_t> execute_query(const std::string& test_dir, const std::wstring& field,
                                           const std::shared_ptr<Query>& query) {
    auto* dir = FSDirectory::getDirectory(test_dir.c_str());
    auto reader_holder = make_shared_reader(lucene::index::IndexReader::open(dir, true));

    auto weight = query->weight(false);

    QueryExecutionContext exec_ctx;
    exec_ctx.segment_num_rows = reader_holder->maxDoc();
    exec_ctx.readers = {reader_holder};
    exec_ctx.field_reader_bindings.emplace(field, reader_holder);

    auto scorer = weight->scorer(exec_ctx);
    std::vector<uint32_t> matched_docs;
    if (scorer) {
        uint32_t doc = scorer->doc();
        while (doc != TERMINATED) {
            matched_docs.push_back(doc);
            doc = scorer->advance();
        }
    }

    _CLDECDELETE(dir);
    return matched_docs;
}

// REGEXP with uppercase pattern should NOT match lowercased index terms.
// This is consistent with ES query_string regex behavior.
TEST_F(RegexpWildcardLowercaseTest, RegexpUppercasePatternNoMatch) {
    auto context = std::make_shared<IndexQueryContext>();
    std::wstring field = StringHelper::to_wstring("title");

    // Pattern "AB.*" should NOT match "abc" (uppercase vs lowercase)
    auto query = std::make_shared<RegexpQuery>(context, field, "AB.*");
    auto matched = execute_query(kTestDir, field, query);

    EXPECT_EQ(matched.size(), 0)
            << "Uppercase regex 'AB.*' should not match lowercased terms 'abc'";
}

// REGEXP with lowercase pattern SHOULD match lowercased index terms.
TEST_F(RegexpWildcardLowercaseTest, RegexpLowercasePatternMatches) {
    auto context = std::make_shared<IndexQueryContext>();
    std::wstring field = StringHelper::to_wstring("title");

    // Pattern "ab.*" should match "abc" (both lowercase)
    auto query = std::make_shared<RegexpQuery>(context, field, "ab.*");
    auto matched = execute_query(kTestDir, field, query);

    // Docs 0 and 1 contain "abc", docs 2 and 3 don't
    EXPECT_EQ(matched.size(), 2) << "Lowercase regex 'ab.*' should match lowercased terms 'abc'";
}

// WILDCARD with lowercase pattern SHOULD match.
// In function_search.cpp, WILDCARD patterns are lowercased before being passed here.
TEST_F(RegexpWildcardLowercaseTest, WildcardLowercasePatternMatches) {
    auto context = std::make_shared<IndexQueryContext>();
    std::wstring field = StringHelper::to_wstring("title");

    // Pattern "ab*" (already lowercased by function_search.cpp) should match "abc"
    auto query = std::make_shared<WildcardQuery>(context, field, "ab*");
    auto matched = execute_query(kTestDir, field, query);

    EXPECT_EQ(matched.size(), 2) << "Lowercase wildcard 'ab*' should match lowercased terms 'abc'";
}

// WILDCARD with uppercase pattern should NOT match lowercased index terms
// (but in practice, function_search.cpp lowercases before passing to WildcardQuery).
TEST_F(RegexpWildcardLowercaseTest, WildcardUppercasePatternNoMatch) {
    auto context = std::make_shared<IndexQueryContext>();
    std::wstring field = StringHelper::to_wstring("title");

    // Pattern "AB*" should NOT match "abc" at the WildcardQuery level
    auto query = std::make_shared<WildcardQuery>(context, field, "AB*");
    auto matched = execute_query(kTestDir, field, query);

    EXPECT_EQ(matched.size(), 0) << "Uppercase wildcard 'AB*' should not match lowercased terms";
}

// REGEXP with a more complex pattern
TEST_F(RegexpWildcardLowercaseTest, RegexpComplexPatternMatches) {
    auto context = std::make_shared<IndexQueryContext>();
    std::wstring field = StringHelper::to_wstring("title");

    // Pattern "ch.*y" should match "cherry" (lowercased)
    auto query = std::make_shared<RegexpQuery>(context, field, "ch.*y");
    auto matched = execute_query(kTestDir, field, query);

    EXPECT_EQ(matched.size(), 1) << "Regex 'ch.*y' should match 'cherry' in doc 3";
    if (!matched.empty()) {
        EXPECT_EQ(matched[0], 3);
    }
}

// WILDCARD matching all terms with '*'
TEST_F(RegexpWildcardLowercaseTest, WildcardStarMatchesAll) {
    auto context = std::make_shared<IndexQueryContext>();
    std::wstring field = StringHelper::to_wstring("title");

    // Pattern "a*" should match "abc" and "apple"
    auto query = std::make_shared<WildcardQuery>(context, field, "a*");
    auto matched = execute_query(kTestDir, field, query);

    // Docs 0,1 have "abc", doc 2 has "apple", doc 3 has no "a*" terms
    EXPECT_EQ(matched.size(), 3) << "Wildcard 'a*' should match docs with 'abc' and 'apple'";
}

} // namespace doris::segment_v2
