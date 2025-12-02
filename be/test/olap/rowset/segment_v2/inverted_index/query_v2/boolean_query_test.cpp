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

#include <algorithm>
#include <memory>
#include <roaring/roaring.hh>
#include <string>
#include <unordered_map>

#include "common/status.h"
#include "olap/rowset/segment_v2/index_query_context.h"
#include "olap/rowset/segment_v2/inverted_index/analyzer/custom_analyzer.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/bit_set_query/bit_set_query.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/operator.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/term_query/term_query.h"
#include "olap/rowset/segment_v2/inverted_index/util/string_helper.h"

CL_NS_USE(search)
CL_NS_USE(store)
CL_NS_USE(index)

namespace doris::segment_v2 {

using namespace inverted_index;

class BooleanQueryTest : public testing::Test {
public:
    const std::string kTestDir1 = "./ut_dir/query_test1";
    const std::string kTestDir2 = "./ut_dir/query_test2";

    void SetUp() override {
        {
            auto st = io::global_local_filesystem()->delete_directory(kTestDir1);
            ASSERT_TRUE(st.ok()) << st;
            st = io::global_local_filesystem()->create_directory(kTestDir1);
            ASSERT_TRUE(st.ok()) << st;
            std::string field_name1 = "name1";
            create_test_index(field_name1, kTestDir1);
        }
        {
            auto st = io::global_local_filesystem()->delete_directory(kTestDir2);
            ASSERT_TRUE(st.ok()) << st;
            st = io::global_local_filesystem()->create_directory(kTestDir2);
            ASSERT_TRUE(st.ok()) << st;
            std::string field_name2 = "name2";
            create_test_index(field_name2, kTestDir2);
        }
    }
    void TearDown() override {
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(kTestDir1).ok());
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(kTestDir2).ok());
    }

private:
    void create_test_index(const std::string& field_name, const std::string& dir) {
        std::vector<std::string> test_data = {"apple banana orange",   "apple cherry grape",
                                              "banana cherry kiwi",    "orange grape strawberry",
                                              "apple orange kiwi",     "cherry banana grape",
                                              "strawberry apple kiwi", "orange cherry banana"};

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

        for (int r = 0; r < 10; ++r) {
            for (const auto& data : test_data) {
                char_string_reader->init(data.data(), data.size(), false);
                auto* stream =
                        custom_analyzer->reusableTokenStream(field->name(), char_string_reader);
                field->setValue(stream);
                indexwriter->addDocument(doc);
            }
        }

        indexwriter->close();

        _CLLDELETE(indexwriter);
        _CLLDELETE(doc);
    }
};

static Status boolean_query_search(
        const std::string& name, const std::shared_ptr<lucene::index::IndexReader>& reader,
        const std::pair<std::vector<std::string>, std::vector<std::string>>& terms,
        query_v2::OperatorType op, roaring::Roaring& out_bitmap) {
    std::wstring field = StringHelper::to_wstring(name);
    auto query_type = segment_v2::InvertedIndexQueryType::EQUAL_QUERY;
    std::string binding_key = name + "#" + std::to_string(static_cast<int>(query_type));

    auto context = std::make_shared<IndexQueryContext>();
    context->collection_statistics = std::make_shared<CollectionStatistics>();
    context->collection_similarity = std::make_shared<CollectionSimilarity>();

    query_v2::BooleanQuery::Builder builder(op);
    {
        query_v2::BooleanQuery::Builder builder_child(query_v2::OperatorType::OP_AND);
        for (const auto& term : terms.first) {
            std::wstring t = StringHelper::to_wstring(term);
            auto clause = std::make_shared<query_v2::TermQuery>(context, field, t);
            builder_child.add(clause, binding_key);
        }
        auto boolean_query = builder_child.build();
        builder.add(boolean_query, binding_key);
    }
    {
        query_v2::BooleanQuery::Builder builder_child(query_v2::OperatorType::OP_OR);
        for (const auto& term : terms.second) {
            std::wstring t = StringHelper::to_wstring(term);
            auto clause = std::make_shared<query_v2::TermQuery>(context, field, t);
            builder_child.add(clause, binding_key);
        }
        auto boolean_query = builder_child.build();
        builder.add(boolean_query, binding_key);
    }
    auto boolean_query = builder.build();
    auto weight = boolean_query->weight(false);
    query_v2::QueryExecutionContext exec_ctx;
    exec_ctx.segment_num_rows = reader->maxDoc();
    exec_ctx.readers = {reader};
    exec_ctx.reader_bindings.emplace(binding_key, reader);
    exec_ctx.field_reader_bindings.emplace(field, reader);

    auto scorer = weight->scorer(exec_ctx);

    uint32_t doc = scorer->doc();
    while (doc != query_v2::TERMINATED) {
        out_bitmap.add(doc);
        doc = scorer->advance();
    }

    return Status::OK();
}

static std::shared_ptr<lucene::index::IndexReader> make_shared_reader(
        lucene::index::IndexReader* raw_reader) {
    return {raw_reader, [](lucene::index::IndexReader* reader) {
                if (reader != nullptr) {
                    reader->close();
                    _CLDELETE(reader);
                }
            }};
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

    auto* dir = FSDirectory::getDirectory(kTestDir1.c_str());
    auto reader_holder = make_shared_reader(lucene::index::IndexReader::open(dir, true));

    ASSERT_TRUE(reader_holder != nullptr) << "Failed to open index reader";
    EXPECT_EQ(reader_holder->numDocs(), 80) << "Index should contain 80 documents";

    const std::vector<uint32_t> expected_cards = {10, 20, 0, 0};

    for (size_t i = 0; i < test_cases.size(); ++i) {
        const auto& terms = test_cases[i];
        roaring::Roaring result;

        try {
            Status res = boolean_query_search("name1", reader_holder, terms,
                                              query_v2::OperatorType::OP_AND, result);
            EXPECT_TRUE(res.ok()) << "Boolean query case " << i << " should execute successfully";
            EXPECT_EQ(result.cardinality(), expected_cards[i])
                    << "Unexpected result cardinality for AND case " << i;
        } catch (const Exception& e) {
            FAIL() << "Boolean query case " << i << " failed with exception: " << e.what();
        }
    }

    _CLDECDELETE(dir);
}

TEST_F(BooleanQueryTest, test_boolean_query_or_operation) {
    CustomAnalyzerConfig::Builder builder;
    builder.with_tokenizer_config("standard", {});
    auto custom_analyzer_config = builder.build();
    auto custom_analyzer = CustomAnalyzer::build_custom_analyzer(custom_analyzer_config);

    std::vector<std::pair<std::vector<std::string>, std::vector<std::string>>> test_cases = {
            {{"apple"}, {"banana"}}, {{"nonexistent"}, {"apple"}}};

    auto* dir = FSDirectory::getDirectory(kTestDir1.c_str());
    auto reader_holder = make_shared_reader(lucene::index::IndexReader::open(dir, true));

    const std::vector<uint32_t> expected_cards = {70, 40};

    for (size_t i = 0; i < test_cases.size(); ++i) {
        const auto& terms = test_cases[i];
        roaring::Roaring result;

        try {
            Status res = boolean_query_search("name1", reader_holder, terms,
                                              query_v2::OperatorType::OP_OR, result);
            EXPECT_TRUE(res.ok()) << "Boolean OR query case " << i
                                  << " should execute successfully";
            EXPECT_EQ(result.cardinality(), expected_cards[i])
                    << "Unexpected result cardinality for OR case " << i;
        } catch (const Exception& e) {
            FAIL() << "Boolean OR query case " << i << " failed with exception: " << e.what();
        }
    }

    _CLDECDELETE(dir);
}

TEST_F(BooleanQueryTest, test_boolean_query_not_operation) {
    std::wstring field = StringHelper::to_wstring("name1");

    auto context = std::make_shared<IndexQueryContext>();
    context->collection_statistics = std::make_shared<CollectionStatistics>();
    context->collection_similarity = std::make_shared<CollectionSimilarity>();

    auto* dir = FSDirectory::getDirectory(kTestDir1.c_str());
    auto reader_holder = make_shared_reader(lucene::index::IndexReader::open(dir, true));
    ASSERT_TRUE(reader_holder != nullptr);

    auto query_type = segment_v2::InvertedIndexQueryType::EQUAL_QUERY;
    std::string binding_key =
            std::string("name1") + "#" + std::to_string(static_cast<int>(query_type));

    query_v2::BooleanQuery::Builder builder(query_v2::OperatorType::OP_NOT);
    builder.add(std::make_shared<query_v2::TermQuery>(context, field,
                                                      StringHelper::to_wstring("apple")),
                binding_key);
    auto query = builder.build();

    auto weight = query->weight(false);
    query_v2::QueryExecutionContext exec_ctx;
    exec_ctx.segment_num_rows = reader_holder->maxDoc();
    exec_ctx.readers = {reader_holder};
    exec_ctx.reader_bindings.emplace(binding_key, reader_holder);
    exec_ctx.field_reader_bindings.emplace(field, reader_holder);

    auto scorer = weight->scorer(exec_ctx);

    uint32_t doc = scorer->doc();
    uint32_t count = 0;
    while (doc != query_v2::TERMINATED) {
        ++count;
        doc = scorer->advance();
    }

    EXPECT_EQ(count, 40);

    _CLDECDELETE(dir);
}

TEST_F(BooleanQueryTest, test_boolean_query_or_with_not_operation) {
    std::wstring field = StringHelper::to_wstring("name1");

    auto context = std::make_shared<IndexQueryContext>();
    context->collection_statistics = std::make_shared<CollectionStatistics>();
    context->collection_similarity = std::make_shared<CollectionSimilarity>();

    auto* dir = FSDirectory::getDirectory(kTestDir1.c_str());
    auto reader_holder = make_shared_reader(lucene::index::IndexReader::open(dir, true));
    ASSERT_TRUE(reader_holder != nullptr);

    query_v2::BooleanQuery::Builder builder(query_v2::OperatorType::OP_OR);
    auto query_type = segment_v2::InvertedIndexQueryType::EQUAL_QUERY;
    std::string include_key =
            std::string("name1") + "#" + std::to_string(static_cast<int>(query_type));
    builder.add(std::make_shared<query_v2::TermQuery>(context, field,
                                                      StringHelper::to_wstring("apple")),
                include_key);
    {
        query_v2::BooleanQuery::Builder not_builder(query_v2::OperatorType::OP_NOT);
        not_builder.add(std::make_shared<query_v2::TermQuery>(context, field,
                                                              StringHelper::to_wstring("banana")),
                        include_key);
        builder.add(not_builder.build(), include_key);
    }
    auto query = builder.build();

    auto weight = query->weight(false);
    query_v2::QueryExecutionContext exec_ctx;
    exec_ctx.segment_num_rows = reader_holder->maxDoc();
    exec_ctx.readers = {reader_holder};
    exec_ctx.reader_bindings[include_key] = reader_holder;
    exec_ctx.field_reader_bindings.emplace(field, reader_holder);

    auto scorer = weight->scorer(exec_ctx);

    uint32_t doc = scorer->doc();
    uint32_t count = 0;
    while (doc != query_v2::TERMINATED) {
        ++count;
        doc = scorer->advance();
    }

    EXPECT_EQ(count, 50);

    _CLDECDELETE(dir);
}

TEST_F(BooleanQueryTest, test_boolean_query_scoring_or) {
    std::wstring field = StringHelper::to_wstring("name1");

    auto context = std::make_shared<IndexQueryContext>();
    context->collection_statistics = std::make_shared<CollectionStatistics>();
    context->collection_similarity = std::make_shared<CollectionSimilarity>();

    std::wstring ws_field = StringHelper::to_wstring("name1");
    // 直接访问成员填充统计信息
    context->collection_statistics->_total_num_docs = 80;
    context->collection_statistics->_total_num_tokens[ws_field] = 240; // 80*3
    auto set_df = [&](const std::string& term, uint64_t df) {
        context->collection_statistics->_term_doc_freqs[ws_field][StringHelper::to_wstring(term)] =
                df;
    };
    set_df("apple", 40);
    set_df("banana", 40);
    set_df("orange", 40);
    set_df("cherry", 40);
    set_df("grape", 30);
    set_df("kiwi", 30);
    set_df("strawberry", 20);

    auto* dir = FSDirectory::getDirectory(kTestDir1.c_str());
    auto reader_holder = make_shared_reader(lucene::index::IndexReader::open(dir, true));
    ASSERT_TRUE(reader_holder != nullptr);

    query_v2::BooleanQuery::Builder builder(query_v2::OperatorType::OP_OR);
    auto query_type = segment_v2::InvertedIndexQueryType::EQUAL_QUERY;
    std::string binding_key =
            std::string("name1") + "#" + std::to_string(static_cast<int>(query_type));
    {
        query_v2::BooleanQuery::Builder builder_child(query_v2::OperatorType::OP_AND);
        auto clause = std::make_shared<query_v2::TermQuery>(context, field,
                                                            StringHelper::to_wstring("apple"));
        builder_child.add(clause, binding_key);
        builder.add(builder_child.build(), binding_key);
    }
    {
        query_v2::BooleanQuery::Builder builder_child(query_v2::OperatorType::OP_OR);
        auto clause = std::make_shared<query_v2::TermQuery>(context, field,
                                                            StringHelper::to_wstring("kiwi"));
        builder_child.add(clause, binding_key);
        builder.add(builder_child.build(), binding_key);
    }
    auto boolean_query = builder.build();

    auto weight = boolean_query->weight(true);
    query_v2::QueryExecutionContext exec_ctx;
    exec_ctx.segment_num_rows = reader_holder->maxDoc();
    exec_ctx.readers = {reader_holder};
    exec_ctx.reader_bindings[binding_key] = reader_holder;
    exec_ctx.field_reader_bindings.emplace(field, reader_holder);

    auto scorer = weight->scorer(exec_ctx);

    uint32_t doc = scorer->doc();
    uint32_t count = 0;
    float score_both = -1.0F;
    float score_single = -1.0F;

    while (doc != query_v2::TERMINATED) {
        float s = scorer->score();
        if ((doc % 8 == 4 || doc % 8 == 6) && score_both < 0.0F) {
            score_both = s;
        }
        if ((doc % 8 == 0 || doc % 8 == 1 || doc % 8 == 2) && score_single < 0.0F) {
            score_single = s;
        }
        ++count;
        doc = scorer->advance();
    }

    EXPECT_EQ(count, 50);
    EXPECT_GT(score_single, 0.0F);
    EXPECT_GT(score_both, 0.0F);
    EXPECT_GT(score_both, score_single);

    _CLDECDELETE(dir);
}

TEST_F(BooleanQueryTest, test_boolean_query_cross_fields_with_composite_reader) {
    std::string field_name1 = "name1";
    std::string field_name2 = "name2";
    std::wstring wfield1 = StringHelper::to_wstring(field_name1);
    std::wstring wfield2 = StringHelper::to_wstring(field_name2);

    auto* dir1 = FSDirectory::getDirectory(kTestDir1.c_str());
    auto* dir2 = FSDirectory::getDirectory(kTestDir2.c_str());
    auto ir1_holder = make_shared_reader(lucene::index::IndexReader::open(dir1, true));
    auto ir2_holder = make_shared_reader(lucene::index::IndexReader::open(dir2, true));
    ASSERT_TRUE(ir1_holder != nullptr);
    ASSERT_TRUE(ir2_holder != nullptr);
    EXPECT_EQ(ir1_holder->numDocs(), 80);
    EXPECT_EQ(ir2_holder->numDocs(), 80);

    query_v2::QueryExecutionContext exec_ctx;
    exec_ctx.segment_num_rows = std::max(ir1_holder->maxDoc(), ir2_holder->maxDoc());
    exec_ctx.readers = {ir1_holder, ir2_holder};
    auto query_type = segment_v2::InvertedIndexQueryType::EQUAL_QUERY;
    std::string binding1 = field_name1 + "#" + std::to_string(static_cast<int>(query_type));
    std::string binding2 = field_name2 + "#" + std::to_string(static_cast<int>(query_type));
    exec_ctx.reader_bindings[binding1] = ir1_holder;
    exec_ctx.reader_bindings[binding2] = ir2_holder;
    exec_ctx.field_reader_bindings.emplace(wfield1, ir1_holder);
    exec_ctx.field_reader_bindings.emplace(wfield2, ir2_holder);

    auto context = std::make_shared<IndexQueryContext>();
    context->collection_statistics = std::make_shared<CollectionStatistics>();
    context->collection_similarity = std::make_shared<CollectionSimilarity>();

    {
        query_v2::BooleanQuery::Builder b(query_v2::OperatorType::OP_AND);
        b.add(std::make_shared<query_v2::TermQuery>(context, wfield1,
                                                    StringHelper::to_wstring("apple")),
              binding1);
        b.add(std::make_shared<query_v2::TermQuery>(context, wfield2,
                                                    StringHelper::to_wstring("banana")),
              binding2);
        auto q = b.build();
        auto w = q->weight(false);
        auto s = w->scorer(exec_ctx);

        uint32_t doc = s->doc();
        uint32_t count = 0;
        while (doc != query_v2::TERMINATED) {
            ++count;
            doc = s->advance();
        }
        EXPECT_EQ(count, 10);
    }

    {
        query_v2::BooleanQuery::Builder b(query_v2::OperatorType::OP_OR);
        b.add(std::make_shared<query_v2::TermQuery>(context, wfield1,
                                                    StringHelper::to_wstring("apple")),
              binding1);
        b.add(std::make_shared<query_v2::TermQuery>(context, wfield2,
                                                    StringHelper::to_wstring("banana")),
              binding2);
        auto q = b.build();
        auto w = q->weight(false);
        auto s = w->scorer(exec_ctx);

        uint32_t doc = s->doc();
        uint32_t count = 0;
        while (doc != query_v2::TERMINATED) {
            ++count;
            doc = s->advance();
        }
        EXPECT_EQ(count, 70);
    }

    _CLDECDELETE(dir1);
    _CLDECDELETE(dir2);
}

TEST_F(BooleanQueryTest, test_boolean_query_bitmap_and_term) {
    std::wstring field = StringHelper::to_wstring("name1");

    auto context = std::make_shared<IndexQueryContext>();
    context->collection_statistics = std::make_shared<CollectionStatistics>();
    context->collection_similarity = std::make_shared<CollectionSimilarity>();

    auto* dir = FSDirectory::getDirectory(kTestDir1.c_str());
    auto* reader = lucene::index::IndexReader::open(dir, true);
    ASSERT_TRUE(reader != nullptr);

    auto reader_holder =
            std::shared_ptr<lucene::index::IndexReader>(reader, [](lucene::index::IndexReader* r) {
                if (r != nullptr) {
                    r->close();
                    _CLLDELETE(r);
                }
            });

    query_v2::QueryExecutionContext exec_ctx;
    exec_ctx.segment_num_rows = static_cast<uint32_t>(reader_holder->numDocs());
    exec_ctx.readers.push_back(reader_holder);
    exec_ctx.field_reader_bindings.emplace(field, reader_holder);

    roaring::Roaring bm;
    for (uint32_t d = 0; d < static_cast<uint32_t>(reader->numDocs()); ++d) {
        if (d % 8 == 0 || d % 8 == 1) {
            bm.add(d);
        }
    }

    query_v2::BooleanQuery::Builder builder(query_v2::OperatorType::OP_AND);
    builder.add(std::make_shared<query_v2::TermQuery>(context, field,
                                                      StringHelper::to_wstring("apple")));
    builder.add(std::make_shared<query_v2::BitSetQuery>(bm));
    auto q = builder.build();

    auto w = q->weight(false);
    auto s = w->scorer(exec_ctx);

    uint32_t doc = s->doc();
    uint32_t count = 0;
    while (doc != query_v2::TERMINATED) {
        EXPECT_TRUE(doc % 8 == 0 || doc % 8 == 1);
        ++count;
        doc = s->advance();
    }
    EXPECT_EQ(count, 20);
    _CLDECDELETE(dir);
}

TEST_F(BooleanQueryTest, test_boolean_query_bitmap_or_term) {
    std::wstring field = StringHelper::to_wstring("name1");

    auto context = std::make_shared<IndexQueryContext>();
    context->collection_statistics = std::make_shared<CollectionStatistics>();
    context->collection_similarity = std::make_shared<CollectionSimilarity>();

    auto* dir = FSDirectory::getDirectory(kTestDir1.c_str());
    auto* reader = lucene::index::IndexReader::open(dir, true);
    ASSERT_TRUE(reader != nullptr);

    auto reader_holder =
            std::shared_ptr<lucene::index::IndexReader>(reader, [](lucene::index::IndexReader* r) {
                if (r != nullptr) {
                    r->close();
                    _CLLDELETE(r);
                }
            });

    query_v2::QueryExecutionContext exec_ctx;
    exec_ctx.segment_num_rows = static_cast<uint32_t>(reader_holder->numDocs());
    exec_ctx.readers.push_back(reader_holder);
    exec_ctx.field_reader_bindings.emplace(field, reader_holder);

    roaring::Roaring bm;
    for (uint32_t d = 0; d < static_cast<uint32_t>(reader->numDocs()); ++d) {
        if (d % 8 == 2 || d % 8 == 3) {
            bm.add(d);
        }
    }

    query_v2::BooleanQuery::Builder builder(query_v2::OperatorType::OP_OR);
    builder.add(std::make_shared<query_v2::TermQuery>(context, field,
                                                      StringHelper::to_wstring("apple")));
    builder.add(std::make_shared<query_v2::BitSetQuery>(bm));
    auto q = builder.build();

    auto w = q->weight(false);
    auto s = w->scorer(exec_ctx);

    uint32_t doc = s->doc();
    uint32_t count = 0;
    while (doc != query_v2::TERMINATED) {
        ++count;
        doc = s->advance();
    }
    EXPECT_EQ(count, 60);
    _CLDECDELETE(dir);
}

} // namespace doris::segment_v2
