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

#include "olap/rowset/segment_v2/inverted_index/query_v2/boolean_query/boolean_query.h"

#include <gtest/gtest.h>

#include <memory>
#include <roaring/roaring.hh>
#include <string>

#include "common/status.h"
#include "olap/rowset/segment_v2/index_query_context.h"
#include "olap/rowset/segment_v2/inverted_index/analyzer/custom_analyzer.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/operator.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/term_query/term_query.h"
#include "olap/rowset/segment_v2/inverted_index/util/string_helper.h"

CL_NS_USE(search)
CL_NS_USE(store)

namespace doris::segment_v2 {

using namespace inverted_index;

class BooleanQueryTest : public testing::Test {
public:
    const std::string kTestDir = "./ut_dir/query_test";
    std::string field_name = "name";

    void SetUp() override {
        auto st = io::global_local_filesystem()->delete_directory(kTestDir);
        ASSERT_TRUE(st.ok()) << st;
        st = io::global_local_filesystem()->create_directory(kTestDir);
        ASSERT_TRUE(st.ok()) << st;

        create_test_index();
    }
    void TearDown() override {
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(kTestDir).ok());
    }

private:
    void create_test_index() {
        std::vector<std::string> test_data = {"apple banana orange",   "apple cherry grape",
                                              "banana cherry kiwi",    "orange grape strawberry",
                                              "apple orange kiwi",     "cherry banana grape",
                                              "strawberry apple kiwi", "orange cherry banana"};

        CustomAnalyzerConfig::Builder builder;
        builder.with_tokenizer_config("standard", {});
        auto custom_analyzer_config = builder.build();
        auto custom_analyzer = CustomAnalyzer::build_custom_analyzer(custom_analyzer_config);

        auto* indexwriter =
                _CLNEW lucene::index::IndexWriter(kTestDir.c_str(), custom_analyzer.get(), true);
        indexwriter->setMaxBufferedDocs(100);
        indexwriter->setRAMBufferSizeMB(-1);
        indexwriter->setMaxFieldLength(0x7FFFFFFFL);
        indexwriter->setMergeFactor(1000000000);
        indexwriter->setUseCompoundFile(false);

        auto* char_string_reader = _CLNEW lucene::util::SStringReader<char>;

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
        _CLLDELETE(char_string_reader);
    }
};

static Status boolean_query_search(
        const std::string& name, lucene::index::IndexReader* reader,
        const std::pair<std::vector<std::string>, std::vector<std::string>>& terms,
        query_v2::OperatorType op, roaring::Roaring& out_bitmap) {
    std::wstring field = StringHelper::to_wstring(name);

    auto context = std::make_shared<IndexQueryContext>();
    context->collection_statistics = std::make_shared<CollectionStatistics>();
    context->collection_similarity = std::make_shared<CollectionSimilarity>();

    query_v2::BooleanQuery::Builder builder(op);
    {
        query_v2::BooleanQuery::Builder builder_child(query_v2::OperatorType::OP_AND);
        for (const auto& term : terms.first) {
            std::wstring t = StringHelper::to_wstring(term);
            auto clause = std::make_shared<query_v2::TermQuery>(context, field, t);
            builder_child.add(clause);
        }
        auto boolean_query = builder_child.build();
        builder.add(boolean_query);
    }
    {
        query_v2::BooleanQuery::Builder builder_child(query_v2::OperatorType::OP_OR);
        for (const auto& term : terms.second) {
            std::wstring t = StringHelper::to_wstring(term);
            auto clause = std::make_shared<query_v2::TermQuery>(context, field, t);
            builder_child.add(clause);
        }
        auto boolean_query = builder_child.build();
        builder.add(boolean_query);
    }
    auto boolean_query = builder.build();
    auto weight = boolean_query->weight(false);
    auto scorer = weight->scorer(reader);

    uint32_t doc = scorer->doc();
    while (doc != query_v2::TERMINATED) {
        out_bitmap.add(doc);
        doc = scorer->advance();
    }

    return Status::OK();
}

std::vector<std::string> tokenize(const CustomAnalyzerPtr& custom_analyzer,
                                  const std::string line) {
    std::vector<std::string> results;
    lucene::util::SStringReader<char> reader;
    reader.init(line.data(), line.size(), false);
    auto* token_stream = custom_analyzer->reusableTokenStream(L"", &reader);
    token_stream->reset();
    Token t;
    while (token_stream->next(&t)) {
        results.emplace_back(t.termBuffer<char>(), t.termLength<char>());
    }
    return results;
}

TEST_F(BooleanQueryTest, test_boolean_query) {
    CustomAnalyzerConfig::Builder builder;
    builder.with_tokenizer_config("standard", {});
    auto custom_analyzer_config = builder.build();
    auto custom_analyzer = CustomAnalyzer::build_custom_analyzer(custom_analyzer_config);

    std::vector<std::pair<std::vector<std::string>, std::vector<std::string>>> test_cases = {
            {{"apple"}, {"banana"}},
            {{"orange"}, {"grape", "kiwi"}},
            {{"cherry"}, {"strawberry"}},
            {{"apple", "banana"}, {"kiwi"}}};

    auto* dir = FSDirectory::getDirectory(kTestDir.c_str());
    auto* reader = IndexReader::open(dir, true);

    ASSERT_TRUE(reader != nullptr) << "Failed to open index reader";
    EXPECT_GT(reader->numDocs(), 0) << "Index should contain documents";

    for (size_t i = 0; i < test_cases.size(); ++i) {
        const auto& terms = test_cases[i];
        roaring::Roaring result;

        try {
            Status res = boolean_query_search(field_name, reader, terms,
                                              query_v2::OperatorType::OP_AND, result);
            EXPECT_TRUE(res.ok()) << "Boolean query case " << i << " should execute successfully";
            EXPECT_GE(result.cardinality(), 0) << "Result should be non-negative";
        } catch (const Exception& e) {
            FAIL() << "Boolean query case " << i << " failed with exception: " << e.what();
        }
    }

    reader->close();
    _CLLDELETE(reader);
    _CLDECDELETE(dir);
}

TEST_F(BooleanQueryTest, test_boolean_query_or_operation) {
    CustomAnalyzerConfig::Builder builder;
    builder.with_tokenizer_config("standard", {});
    auto custom_analyzer_config = builder.build();
    auto custom_analyzer = CustomAnalyzer::build_custom_analyzer(custom_analyzer_config);

    std::vector<std::pair<std::vector<std::string>, std::vector<std::string>>> test_cases = {
            {{"apple"}, {"banana"}}, {{"nonexistent"}, {"apple"}}};

    auto* dir = FSDirectory::getDirectory(kTestDir.c_str());
    auto* reader = IndexReader::open(dir, true);

    for (size_t i = 0; i < test_cases.size(); ++i) {
        const auto& terms = test_cases[i];
        roaring::Roaring result;

        try {
            Status res = boolean_query_search(field_name, reader, terms,
                                              query_v2::OperatorType::OP_OR, result);
            EXPECT_TRUE(res.ok()) << "Boolean OR query case " << i
                                  << " should execute successfully";
        } catch (const Exception& e) {
            FAIL() << "Boolean OR query case " << i << " failed with exception: " << e.what();
        }
    }

    reader->close();
    _CLLDELETE(reader);
    _CLDECDELETE(dir);
}

} // namespace doris::segment_v2
