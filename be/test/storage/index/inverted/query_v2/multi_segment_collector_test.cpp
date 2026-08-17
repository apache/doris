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

#include <CLucene.h>
#include <CLucene/index/MultiReader.h>
#include <gtest/gtest.h>

#include <memory>
#include <roaring/roaring.hh>
#include <string>
#include <vector>

#include "io/fs/local_file_system.h"
#include "storage/index/index_query_context.h"
#include "storage/index/inverted/analyzer/custom_analyzer.h"
#include "storage/index/inverted/query_v2/collect/doc_set_collector.h"
#include "storage/index/inverted/query_v2/collect/multi_segment_util.h"
#include "storage/index/inverted/query_v2/term_query/term_query.h"
#include "storage/index/inverted/util/string_helper.h"

CL_NS_USE(index)
CL_NS_USE(store)
CL_NS_USE(util)

namespace doris::segment_v2 {

using namespace inverted_index;
using namespace inverted_index::query_v2;

class MultiSegmentCollectorTest : public testing::Test {
public:
    const std::string kTestDir = "./ut_dir/multi_segment_collector_test";

    void SetUp() override {
        auto st = io::global_local_filesystem()->delete_directory(kTestDir);
        ASSERT_TRUE(st.ok()) << st;
        st = io::global_local_filesystem()->create_directory(kTestDir);
        ASSERT_TRUE(st.ok()) << st;
        st = io::global_local_filesystem()->create_directory(kTestDir + "/segment0");
        ASSERT_TRUE(st.ok()) << st;
        st = io::global_local_filesystem()->create_directory(kTestDir + "/segment1");
        ASSERT_TRUE(st.ok()) << st;

        create_test_index(kTestDir + "/segment0", {"fleabag premiere", "other title"});
        create_test_index(kTestDir + "/segment1", {"history text", "fleabag finale"});
    }

    void TearDown() override {
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(kTestDir).ok());
    }

private:
    static void create_test_index(const std::string& dir, const std::vector<std::string>& docs,
                                  int32_t max_buffered_docs = 100) {
        CustomAnalyzerConfig::Builder builder;
        builder.with_tokenizer_config("standard", {});
        auto custom_analyzer_config = builder.build();
        auto custom_analyzer = CustomAnalyzer::build_custom_analyzer(custom_analyzer_config);

        auto* index_writer =
                _CLNEW lucene::index::IndexWriter(dir.c_str(), custom_analyzer.get(), true);
        index_writer->setMaxBufferedDocs(max_buffered_docs);
        index_writer->setRAMBufferSizeMB(-1);
        index_writer->setMaxFieldLength(0x7FFFFFFFL);
        index_writer->setMergeFactor(1000000000);
        index_writer->setUseCompoundFile(false);

        auto char_string_reader = std::make_shared<lucene::util::SStringReader<char>>();
        auto* doc = _CLNEW lucene::document::Document();
        int32_t field_config = lucene::document::Field::STORE_NO;
        field_config |= lucene::document::Field::INDEX_NONORMS;
        field_config |= lucene::document::Field::INDEX_TOKENIZED;
        auto field_name_w = StringHelper::to_wstring("title");
        auto* field = _CLNEW lucene::document::Field(field_name_w.c_str(), field_config);
        field->setOmitTermFreqAndPositions(false);
        doc->add(*field);

        for (const auto& data : docs) {
            char_string_reader->init(data.data(), data.size(), false);
            auto* stream = custom_analyzer->reusableTokenStream(field->name(), char_string_reader);
            field->setValue(stream);
            index_writer->addDocument(doc);
        }

        index_writer->close();
        _CLLDELETE(index_writer);
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

TEST_F(MultiSegmentCollectorTest, CollectDocSetWithMultiReader) {
    auto* dir0 = FSDirectory::getDirectory((kTestDir + "/segment0").c_str());
    auto* dir1 = FSDirectory::getDirectory((kTestDir + "/segment1").c_str());

    ValueArray<lucene::index::IndexReader*> readers(2);
    readers[0] = lucene::index::IndexReader::open(dir0, true);
    readers[1] = lucene::index::IndexReader::open(dir1, true);
    auto reader = make_shared_reader(_CLNEW lucene::index::MultiReader(&readers, true));

    auto index_query_context = std::make_shared<IndexQueryContext>();
    auto field = StringHelper::to_wstring("title");
    TermQuery query(index_query_context, field, StringHelper::to_wstring("fleabag"));
    auto weight = query.weight(false);

    QueryExecutionContext exec_ctx;
    exec_ctx.segment_num_rows = reader->maxDoc();
    exec_ctx.readers = {reader};
    exec_ctx.field_reader_bindings.emplace(field, reader);

    auto roaring = std::make_shared<roaring::Roaring>();
    ASSERT_NO_THROW(collect_multi_segment_doc_set(weight, exec_ctx, "", roaring, nullptr, false));

    EXPECT_EQ(roaring->cardinality(), 2);
    EXPECT_TRUE(roaring->contains(0));
    EXPECT_TRUE(roaring->contains(3));

    _CLDECDELETE(dir0);
    _CLDECDELETE(dir1);
}

TEST_F(MultiSegmentCollectorTest, CollectDocSetWithSegmentedFieldBinding) {
    auto* dir0 = FSDirectory::getDirectory((kTestDir + "/segment0").c_str());

    auto leading_reader = make_shared_reader(lucene::index::IndexReader::open(dir0, true));

    const auto multi_segment_dir = kTestDir + "/multi_segment";
    ASSERT_TRUE(io::global_local_filesystem()->create_directory(multi_segment_dir).ok());
    create_test_index(multi_segment_dir,
                      {"fleabag premiere", "other title", "history text", "fleabag finale"}, 2);

    auto* multi_segment_directory = FSDirectory::getDirectory(multi_segment_dir.c_str());
    auto field_reader =
            make_shared_reader(lucene::index::IndexReader::open(multi_segment_directory, true));
    const auto* field_segments = sub_readers(field_reader.get());
    ASSERT_NE(field_segments, nullptr);
    ASSERT_GT(field_segments->length, 1);

    auto index_query_context = std::make_shared<IndexQueryContext>();
    auto field = StringHelper::to_wstring("title");
    TermQuery query(index_query_context, field, StringHelper::to_wstring("fleabag"));
    auto weight = query.weight(false);

    QueryExecutionContext exec_ctx;
    exec_ctx.segment_num_rows = field_reader->maxDoc();
    exec_ctx.readers = {leading_reader};
    exec_ctx.field_reader_bindings.emplace(field, field_reader);

    auto roaring = std::make_shared<roaring::Roaring>();
    ASSERT_NO_THROW(collect_multi_segment_doc_set(weight, exec_ctx, "", roaring, nullptr, false));

    EXPECT_EQ(roaring->cardinality(), 2);
    EXPECT_TRUE(roaring->contains(0));
    EXPECT_TRUE(roaring->contains(3));

    _CLDECDELETE(dir0);
    _CLDECDELETE(multi_segment_directory);
}

TEST_F(MultiSegmentCollectorTest, CollectDocSetWithSingleReaderBinding) {
    auto* dir0 = FSDirectory::getDirectory((kTestDir + "/segment0").c_str());
    auto* dir1 = FSDirectory::getDirectory((kTestDir + "/segment1").c_str());

    auto bound_reader = make_shared_reader(lucene::index::IndexReader::open(dir0, true));

    ValueArray<lucene::index::IndexReader*> readers(2);
    readers[0] = lucene::index::IndexReader::open(dir0, true);
    readers[1] = lucene::index::IndexReader::open(dir1, true);
    auto field_reader = make_shared_reader(_CLNEW lucene::index::MultiReader(&readers, true));

    auto index_query_context = std::make_shared<IndexQueryContext>();
    auto field = StringHelper::to_wstring("title");
    TermQuery query(index_query_context, field, StringHelper::to_wstring("fleabag"));
    auto weight = query.weight(false);

    QueryExecutionContext exec_ctx;
    exec_ctx.segment_num_rows = bound_reader->maxDoc();
    exec_ctx.readers = {bound_reader};
    exec_ctx.reader_bindings.emplace("bound-title", bound_reader);
    exec_ctx.field_reader_bindings.emplace(field, field_reader);

    auto roaring = std::make_shared<roaring::Roaring>();
    ASSERT_NO_THROW(collect_multi_segment_doc_set(weight, exec_ctx, "bound-title", roaring, nullptr,
                                                  false));

    EXPECT_EQ(roaring->cardinality(), 1);
    EXPECT_TRUE(roaring->contains(0));

    _CLDECDELETE(dir0);
    _CLDECDELETE(dir1);
}

} // namespace doris::segment_v2
