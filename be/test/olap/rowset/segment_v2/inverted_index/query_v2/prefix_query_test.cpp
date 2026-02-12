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

#include "olap/rowset/segment_v2/inverted_index/query_v2/prefix_query/prefix_query.h"

#include <CLucene.h>
#include <gtest/gtest.h>

#include <memory>
#include <roaring/roaring.hh>
#include <string>

#include "io/fs/local_file_system.h"
#include "olap/rowset/segment_v2/index_query_context.h"
#include "olap/rowset/segment_v2/inverted_index/analyzer/custom_analyzer.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/prefix_query/prefix_weight.h"
#include "olap/rowset/segment_v2/inverted_index/util/string_helper.h"

CL_NS_USE(store)
CL_NS_USE(index)

namespace doris::segment_v2 {

using namespace inverted_index;
using namespace inverted_index::query_v2;

class PrefixQueryV2Test : public testing::Test {
public:
    const std::string kTestDir = "./ut_dir/prefix_query_test";

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
        // Documents with various words sharing prefixes:
        //   "apple", "application", "apply", "banana", "band", "bank"
        //   "cat", "car", "card", "cart"
        std::vector<std::string> test_data = {
                "apple pie is delicious",     // doc 0
                "application form submitted", // doc 1
                "apply for the job today",    // doc 2
                "banana split dessert",       // doc 3
                "band plays music tonight",   // doc 4
                "bank account balance",       // doc 5
                "cat sleeps on the mat",      // doc 6
                "car drives fast on highway", // doc 7
                "card game with friends",     // doc 8
                "cart full of groceries",     // doc 9
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

// --- PrefixQuery construction ---

TEST_F(PrefixQueryV2Test, construction_and_weight) {
    auto ctx = std::make_shared<IndexQueryContext>();
    std::wstring field = StringHelper::to_wstring("content");

    PrefixQuery q(ctx, field, "app");
    auto w = q.weight(false);
    ASSERT_NE(w, nullptr);
}

// --- expand_prefix static method ---

TEST_F(PrefixQueryV2Test, expand_prefix_basic) {
    auto* dir = FSDirectory::getDirectory(kTestDir.c_str());
    auto reader = make_shared_reader(lucene::index::IndexReader::open(dir, true));
    ASSERT_NE(reader, nullptr);

    std::wstring field = StringHelper::to_wstring("content");
    auto terms = PrefixWeight::expand_prefix(reader.get(), field, "app", 50, nullptr);

    // Should find: apple, application, apply
    EXPECT_EQ(terms.size(), 3);
    // Verify all start with "app"
    for (const auto& t : terms) {
        EXPECT_TRUE(t.substr(0, 3) == "app") << "Term: " << t;
    }

    _CLDECDELETE(dir);
}

// expand_prefix with max_expansions limit
TEST_F(PrefixQueryV2Test, expand_prefix_max_expansions) {
    auto* dir = FSDirectory::getDirectory(kTestDir.c_str());
    auto reader = make_shared_reader(lucene::index::IndexReader::open(dir, true));

    std::wstring field = StringHelper::to_wstring("content");
    // "ban" matches: banana, band, bank → limit to 2
    auto terms = PrefixWeight::expand_prefix(reader.get(), field, "ban", 2, nullptr);
    EXPECT_EQ(terms.size(), 2);

    _CLDECDELETE(dir);
}

// expand_prefix with no matches
TEST_F(PrefixQueryV2Test, expand_prefix_no_match) {
    auto* dir = FSDirectory::getDirectory(kTestDir.c_str());
    auto reader = make_shared_reader(lucene::index::IndexReader::open(dir, true));

    std::wstring field = StringHelper::to_wstring("content");
    auto terms = PrefixWeight::expand_prefix(reader.get(), field, "zzz", 50, nullptr);
    EXPECT_TRUE(terms.empty());

    _CLDECDELETE(dir);
}

// expand_prefix where prefix is longer than any term → prefixLen > termLen branch
TEST_F(PrefixQueryV2Test, expand_prefix_longer_than_terms) {
    auto* dir = FSDirectory::getDirectory(kTestDir.c_str());
    auto reader = make_shared_reader(lucene::index::IndexReader::open(dir, true));

    std::wstring field = StringHelper::to_wstring("content");
    auto terms =
            PrefixWeight::expand_prefix(reader.get(), field, "applicationformxyz", 50, nullptr);
    EXPECT_TRUE(terms.empty());

    _CLDECDELETE(dir);
}

// --- PrefixWeight::scorer() ---

// Basic prefix scorer: "car" should match docs with car, card, cart
TEST_F(PrefixQueryV2Test, scorer_basic) {
    auto ctx = std::make_shared<IndexQueryContext>();
    auto* dir = FSDirectory::getDirectory(kTestDir.c_str());
    auto reader = make_shared_reader(lucene::index::IndexReader::open(dir, true));

    std::wstring field = StringHelper::to_wstring("content");
    // nullable=false to test the non-nullable branch
    PrefixWeight w(ctx, field, "car", false, 50, false);

    QueryExecutionContext exec_ctx;
    exec_ctx.segment_num_rows = reader->maxDoc();
    exec_ctx.readers = {reader};
    exec_ctx.field_reader_bindings.emplace(field, reader);

    auto scorer = w.scorer(exec_ctx, "");
    ASSERT_NE(scorer, nullptr);

    auto docs = collect_docs(scorer);
    // docs 7 (car), 8 (card), 9 (cart)
    EXPECT_EQ(docs.size(), 3);
    for (uint32_t d : docs) {
        EXPECT_TRUE(d >= 7 && d <= 9) << "Unexpected doc: " << d;
    }

    _CLDECDELETE(dir);
}

// Scorer with nullable=true (covers the nullable branch)
TEST_F(PrefixQueryV2Test, scorer_nullable) {
    auto ctx = std::make_shared<IndexQueryContext>();
    auto* dir = FSDirectory::getDirectory(kTestDir.c_str());
    auto reader = make_shared_reader(lucene::index::IndexReader::open(dir, true));

    std::wstring field = StringHelper::to_wstring("content");
    PrefixWeight w(ctx, field, "app", false, 50, true);

    QueryExecutionContext exec_ctx;
    exec_ctx.segment_num_rows = reader->maxDoc();
    exec_ctx.readers = {reader};
    exec_ctx.field_reader_bindings.emplace(field, reader);
    // null_resolver is nullptr → make_nullable_scorer will just return inner scorer

    auto scorer = w.scorer(exec_ctx, "");
    ASSERT_NE(scorer, nullptr);

    auto docs = collect_docs(scorer);
    // docs 0 (apple), 1 (application), 2 (apply)
    EXPECT_EQ(docs.size(), 3);

    _CLDECDELETE(dir);
}

// Scorer with no matching prefix → EmptyScorer (matching_terms.empty() branch)
TEST_F(PrefixQueryV2Test, scorer_no_match_returns_empty) {
    auto ctx = std::make_shared<IndexQueryContext>();
    auto* dir = FSDirectory::getDirectory(kTestDir.c_str());
    auto reader = make_shared_reader(lucene::index::IndexReader::open(dir, true));

    std::wstring field = StringHelper::to_wstring("content");
    PrefixWeight w(ctx, field, "zzz", false, 50, false);

    QueryExecutionContext exec_ctx;
    exec_ctx.segment_num_rows = reader->maxDoc();
    exec_ctx.readers = {reader};
    exec_ctx.field_reader_bindings.emplace(field, reader);

    auto scorer = w.scorer(exec_ctx, "");
    ASSERT_NE(scorer, nullptr);
    EXPECT_EQ(scorer->doc(), TERMINATED);

    _CLDECDELETE(dir);
}

// Scorer with no reader → EmptyScorer (!reader branch)
TEST_F(PrefixQueryV2Test, scorer_no_reader_returns_empty) {
    auto ctx = std::make_shared<IndexQueryContext>();
    std::wstring field = StringHelper::to_wstring("content");
    PrefixWeight w(ctx, field, "app", false, 50, false);

    QueryExecutionContext exec_ctx;
    exec_ctx.segment_num_rows = 10;
    // No readers at all

    auto scorer = w.scorer(exec_ctx, "");
    ASSERT_NE(scorer, nullptr);
    EXPECT_EQ(scorer->doc(), TERMINATED);
}

// Scorer with binding_key
TEST_F(PrefixQueryV2Test, scorer_with_binding_key) {
    auto ctx = std::make_shared<IndexQueryContext>();
    auto* dir = FSDirectory::getDirectory(kTestDir.c_str());
    auto reader = make_shared_reader(lucene::index::IndexReader::open(dir, true));

    std::wstring field = StringHelper::to_wstring("content");
    PrefixWeight w(ctx, field, "ban", false, 50, false);

    QueryExecutionContext exec_ctx;
    exec_ctx.segment_num_rows = reader->maxDoc();
    std::string binding_key = "content#0";
    exec_ctx.reader_bindings[binding_key] = reader;

    auto scorer = w.scorer(exec_ctx, binding_key);
    ASSERT_NE(scorer, nullptr);

    auto docs = collect_docs(scorer);
    // docs 3 (banana), 4 (band), 5 (bank)
    EXPECT_EQ(docs.size(), 3);

    _CLDECDELETE(dir);
}

// --- PrefixQuery end-to-end ---

TEST_F(PrefixQueryV2Test, end_to_end) {
    auto ctx = std::make_shared<IndexQueryContext>();
    auto* dir = FSDirectory::getDirectory(kTestDir.c_str());
    auto reader = make_shared_reader(lucene::index::IndexReader::open(dir, true));

    std::wstring field = StringHelper::to_wstring("content");
    PrefixQuery q(ctx, field, "cat");
    auto w = q.weight(false);

    QueryExecutionContext exec_ctx;
    exec_ctx.segment_num_rows = reader->maxDoc();
    exec_ctx.readers = {reader};
    exec_ctx.field_reader_bindings.emplace(field, reader);

    auto scorer = w->scorer(exec_ctx, "");
    auto docs = collect_docs(scorer);
    // Only doc 6 has "cat"
    EXPECT_EQ(docs.size(), 1);
    EXPECT_EQ(docs[0], 6);

    _CLDECDELETE(dir);
}

} // namespace doris::segment_v2
