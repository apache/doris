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

#include "common/status.h"
#include "io/fs/local_file_system.h"
#include "olap/rowset/segment_v2/index_query_context.h"
#include "olap/rowset/segment_v2/inverted_index/analyzer/custom_analyzer.h"
#include "olap/rowset/segment_v2/inverted_index/query/query_info.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/all_query/all_query.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/boolean_query/occur.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/boolean_query/occur_boolean_query.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/phrase_query/phrase_query.h"
#include "olap/rowset/segment_v2/inverted_index/util/string_helper.h"

CL_NS_USE(search)
CL_NS_USE(store)
CL_NS_USE(index)

namespace doris::segment_v2 {

using namespace inverted_index;
using namespace inverted_index::query_v2;

class OccurBooleanQueryRealIndexTest : public testing::Test {
public:
    const std::string kTestDir = "./ut_dir/occur_boolean_query_real_index_test";

    void SetUp() override {
        auto st = io::global_local_filesystem()->delete_directory(kTestDir);
        ASSERT_TRUE(st.ok()) << st;
        st = io::global_local_filesystem()->create_directory(kTestDir);
        ASSERT_TRUE(st.ok()) << st;
        std::string field_name = "title";
        create_test_index(field_name, kTestDir);
    }

    void TearDown() override {
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(kTestDir).ok());
    }

private:
    void create_test_index(const std::string& field_name, const std::string& dir) {
        std::vector<std::string> test_data = {
                "apple banana cherry", "apple banana",    "apple",     "banana cherry",
                "cherry date",         "date elderberry", "fig grape", "apple fig"};

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

TEST_F(OccurBooleanQueryRealIndexTest, NotPhraseQuery) {
    auto context = std::make_shared<IndexQueryContext>();
    context->collection_statistics = std::make_shared<CollectionStatistics>();
    context->collection_similarity = std::make_shared<CollectionSimilarity>();

    auto* dir = FSDirectory::getDirectory(kTestDir.c_str());
    auto reader_holder = make_shared_reader(lucene::index::IndexReader::open(dir, true));
    ASSERT_TRUE(reader_holder != nullptr);

    std::wstring field = StringHelper::to_wstring("title");

    std::vector<std::wstring> phrase_terms = {StringHelper::to_wstring("apple"),
                                              StringHelper::to_wstring("banana")};
    std::vector<TermInfo> term_infos;
    term_infos.reserve(phrase_terms.size());
    for (size_t i = 0; i < phrase_terms.size(); ++i) {
        TermInfo term_info;
        term_info.term = StringHelper::to_string(phrase_terms[i]);
        term_info.position = static_cast<int32_t>(i);
        term_infos.push_back(term_info);
    }

    auto phrase_query = std::make_shared<PhraseQuery>(context, field, term_infos);

    auto all_query = std::make_shared<AllQuery>();

    std::vector<std::pair<Occur, QueryPtr>> clauses;
    clauses.emplace_back(Occur::SHOULD, all_query);
    clauses.emplace_back(Occur::MUST_NOT, phrase_query);

    OccurBooleanQuery boolean_query(std::move(clauses));
    auto weight = boolean_query.weight(false);

    QueryExecutionContext exec_ctx;
    exec_ctx.segment_num_rows = reader_holder->maxDoc();
    exec_ctx.readers = {reader_holder};
    exec_ctx.field_reader_bindings.emplace(field, reader_holder);

    auto scorer = weight->scorer(exec_ctx);
    ASSERT_NE(scorer, nullptr);

    std::vector<uint32_t> matched_docs;
    uint32_t doc = scorer->doc();
    while (doc != TERMINATED) {
        matched_docs.push_back(doc);
        doc = scorer->advance();
    }

    EXPECT_EQ(matched_docs.size(), 6);

    EXPECT_TRUE(std::find(matched_docs.begin(), matched_docs.end(), 0) == matched_docs.end())
            << "Doc 0 should be excluded (contains 'apple banana')";
    EXPECT_TRUE(std::find(matched_docs.begin(), matched_docs.end(), 1) == matched_docs.end())
            << "Doc 1 should be excluded (contains 'apple banana')";

    for (uint32_t expected_id : {2, 3, 4, 5, 6, 7}) {
        EXPECT_TRUE(std::find(matched_docs.begin(), matched_docs.end(), expected_id) !=
                    matched_docs.end())
                << "Doc " << expected_id << " should be included";
    }

    _CLDECDELETE(dir);
}

TEST_F(OccurBooleanQueryRealIndexTest, PhraseQueryOnly) {
    auto context = std::make_shared<IndexQueryContext>();
    context->collection_statistics = std::make_shared<CollectionStatistics>();
    context->collection_similarity = std::make_shared<CollectionSimilarity>();

    auto* dir = FSDirectory::getDirectory(kTestDir.c_str());
    auto reader_holder = make_shared_reader(lucene::index::IndexReader::open(dir, true));
    ASSERT_TRUE(reader_holder != nullptr);

    std::wstring field = StringHelper::to_wstring("title");

    std::vector<std::wstring> phrase_terms = {StringHelper::to_wstring("apple"),
                                              StringHelper::to_wstring("banana")};
    std::vector<TermInfo> term_infos;
    term_infos.reserve(phrase_terms.size());
    for (size_t i = 0; i < phrase_terms.size(); ++i) {
        TermInfo term_info;
        term_info.term = StringHelper::to_string(phrase_terms[i]);
        term_info.position = static_cast<int32_t>(i);
        term_infos.push_back(term_info);
    }

    auto phrase_query = std::make_shared<PhraseQuery>(context, field, term_infos);
    auto weight = phrase_query->weight(false);

    QueryExecutionContext exec_ctx;
    exec_ctx.segment_num_rows = reader_holder->maxDoc();
    exec_ctx.readers = {reader_holder};
    exec_ctx.field_reader_bindings.emplace(field, reader_holder);

    auto scorer = weight->scorer(exec_ctx);
    ASSERT_NE(scorer, nullptr);

    std::vector<uint32_t> matched_docs;
    uint32_t doc = scorer->doc();
    while (doc != TERMINATED) {
        matched_docs.push_back(doc);
        std::cout << "Phrase query matched doc: " << doc << std::endl;
        doc = scorer->advance();
    }

    std::cout << "Total matched docs: " << matched_docs.size() << std::endl;

    EXPECT_EQ(matched_docs.size(), 2);
    if (matched_docs.size() >= 1) {
        EXPECT_EQ(matched_docs[0], 0);
    }
    if (matched_docs.size() >= 2) {
        EXPECT_EQ(matched_docs[1], 1);
    }

    _CLDECDELETE(dir);
}

TEST_F(OccurBooleanQueryRealIndexTest, NotPhraseQueryNonExistent) {
    auto context = std::make_shared<IndexQueryContext>();
    context->collection_statistics = std::make_shared<CollectionStatistics>();
    context->collection_similarity = std::make_shared<CollectionSimilarity>();

    auto* dir = FSDirectory::getDirectory(kTestDir.c_str());
    auto reader_holder = make_shared_reader(lucene::index::IndexReader::open(dir, true));
    ASSERT_TRUE(reader_holder != nullptr);

    std::wstring field = StringHelper::to_wstring("title");

    std::vector<std::wstring> phrase_terms = {StringHelper::to_wstring("nonexistent"),
                                              StringHelper::to_wstring("phrase")};
    std::vector<TermInfo> term_infos;
    term_infos.reserve(phrase_terms.size());
    for (size_t i = 0; i < phrase_terms.size(); ++i) {
        TermInfo term_info;
        term_info.term = StringHelper::to_string(phrase_terms[i]);
        term_info.position = static_cast<int32_t>(i);
        term_infos.push_back(term_info);
    }

    auto phrase_query = std::make_shared<PhraseQuery>(context, field, term_infos);

    auto all_query = std::make_shared<AllQuery>();

    std::vector<std::pair<Occur, QueryPtr>> clauses;
    clauses.emplace_back(Occur::SHOULD, all_query);
    clauses.emplace_back(Occur::MUST_NOT, phrase_query);

    OccurBooleanQuery boolean_query(std::move(clauses));
    auto weight = boolean_query.weight(false);

    QueryExecutionContext exec_ctx;
    exec_ctx.segment_num_rows = reader_holder->maxDoc();
    exec_ctx.readers = {reader_holder};
    exec_ctx.field_reader_bindings.emplace(field, reader_holder);

    auto scorer = weight->scorer(exec_ctx);
    ASSERT_NE(scorer, nullptr);

    std::vector<uint32_t> matched_docs;
    uint32_t doc = scorer->doc();
    while (doc != TERMINATED) {
        matched_docs.push_back(doc);
        doc = scorer->advance();
    }

    EXPECT_EQ(matched_docs.size(), 8);

    _CLDECDELETE(dir);
}

TEST_F(OccurBooleanQueryRealIndexTest, NotPhraseQueryExcludesPartial) {
    auto context = std::make_shared<IndexQueryContext>();
    context->collection_statistics = std::make_shared<CollectionStatistics>();
    context->collection_similarity = std::make_shared<CollectionSimilarity>();

    auto* dir = FSDirectory::getDirectory(kTestDir.c_str());
    auto reader_holder = make_shared_reader(lucene::index::IndexReader::open(dir, true));
    ASSERT_TRUE(reader_holder != nullptr);

    std::wstring field = StringHelper::to_wstring("title");

    std::vector<std::wstring> phrase_terms = {StringHelper::to_wstring("banana"),
                                              StringHelper::to_wstring("cherry")};
    std::vector<TermInfo> term_infos;
    term_infos.reserve(phrase_terms.size());
    for (size_t i = 0; i < phrase_terms.size(); ++i) {
        TermInfo term_info;
        term_info.term = StringHelper::to_string(phrase_terms[i]);
        term_info.position = static_cast<int32_t>(i);
        term_infos.push_back(term_info);
    }

    auto phrase_query = std::make_shared<PhraseQuery>(context, field, term_infos);

    auto all_query = std::make_shared<AllQuery>();

    std::vector<std::pair<Occur, QueryPtr>> clauses;
    clauses.emplace_back(Occur::SHOULD, all_query);
    clauses.emplace_back(Occur::MUST_NOT, phrase_query);

    OccurBooleanQuery boolean_query(std::move(clauses));
    auto weight = boolean_query.weight(false);

    QueryExecutionContext exec_ctx;
    exec_ctx.segment_num_rows = reader_holder->maxDoc();
    exec_ctx.readers = {reader_holder};
    exec_ctx.field_reader_bindings.emplace(field, reader_holder);

    auto scorer = weight->scorer(exec_ctx);
    ASSERT_NE(scorer, nullptr);

    std::vector<uint32_t> matched_docs;
    uint32_t doc = scorer->doc();
    while (doc != TERMINATED) {
        matched_docs.push_back(doc);
        doc = scorer->advance();
    }

    EXPECT_EQ(matched_docs.size(), 6);

    EXPECT_TRUE(std::find(matched_docs.begin(), matched_docs.end(), 0) == matched_docs.end())
            << "Doc 0 should be excluded (contains 'banana cherry')";
    EXPECT_TRUE(std::find(matched_docs.begin(), matched_docs.end(), 3) == matched_docs.end())
            << "Doc 3 should be excluded (contains 'banana cherry')";

    _CLDECDELETE(dir);
}

} // namespace doris::segment_v2
