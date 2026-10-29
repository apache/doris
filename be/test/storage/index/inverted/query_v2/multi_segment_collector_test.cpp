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
#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wshadow-field"
#pragma clang diagnostic ignored "-Woverloaded-virtual"
#pragma clang diagnostic ignored "-Winconsistent-missing-override"
#pragma clang diagnostic ignored "-Wreorder-ctor"
#pragma clang diagnostic ignored "-Wshorten-64-to-32"
#elif defined(__GNUC__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Woverloaded-virtual"
#endif
#include <CLucene/index/_MultiSegmentReader.h>
#ifdef __clang__
#pragma clang diagnostic pop
#elif defined(__GNUC__)
#pragma GCC diagnostic pop
#endif
#include <gtest/gtest.h>

#include <memory>
#include <roaring/roaring.hh>
#include <string>
#include <vector>

#include "io/fs/local_file_system.h"
#include "storage/compaction/collection_statistics.h"
#include "storage/index/index_query_context.h"
#include "storage/index/inverted/analyzer/custom_analyzer.h"
#include "storage/index/inverted/query_v2/bit_set_query/bit_set_query.h"
#include "storage/index/inverted/query_v2/collect/doc_set_collector.h"
#include "storage/index/inverted/query_v2/collect/multi_segment_util.h"
#include "storage/index/inverted/query_v2/collect/top_k_collector.h"
#include "storage/index/inverted/query_v2/null_masked_query.h"
#include "storage/index/inverted/query_v2/term_query/term_query.h"
#include "storage/index/inverted/similarity/bm25_similarity.h"
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

protected:
    static void create_test_index(const std::string& dir, const std::vector<std::string>& docs,
                                  int32_t max_buffered_docs = 100,
                                  const std::string& field_name = "title",
                                  bool store_norms = false) {
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
        if (!store_norms) {
            field_config |= lucene::document::Field::INDEX_NONORMS;
        }
        field_config |= lucene::document::Field::INDEX_TOKENIZED;
        auto field_name_w = StringHelper::to_wstring(field_name);
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

TEST_F(MultiSegmentCollectorTest, SearchMaskPreservesReaderDocIdsAcrossCollectors) {
    auto* dir0 = FSDirectory::getDirectory((kTestDir + "/segment0").c_str());
    auto* dir1 = FSDirectory::getDirectory((kTestDir + "/segment1").c_str());

    ValueArray<lucene::index::IndexReader*> readers(2);
    readers[0] = lucene::index::IndexReader::open(dir0, true);
    readers[1] = lucene::index::IndexReader::open(dir1, true);
    auto reader = make_shared_reader(_CLNEW lucene::index::MultiReader(&readers, true));

    auto postings = std::make_shared<roaring::Roaring>();
    postings->add(0);
    postings->add(3);
    auto nulls = std::make_shared<roaring::Roaring>();
    nulls->add(0);
    NullMaskedQuery query(std::make_shared<BitSetQuery>(postings), nulls);
    QueryExecutionContext context;
    context.segment_num_rows = 4;
    context.readers = {reader};
    context.preserve_doc_ids = true;
    for (bool scoring : {false, true}) {
        auto result = std::make_shared<roaring::Roaring>();
        collect_multi_segment_doc_set(query.weight(scoring), context, "", result, nullptr, scoring);
        EXPECT_EQ(1u, result->cardinality());
        EXPECT_TRUE(result->contains(3));
    }
    for (bool wand : {false, true}) {
        auto result = std::make_shared<roaring::Roaring>();
        collect_multi_segment_top_k(query.weight(true), context, "", 1, result, nullptr, wand);
        EXPECT_EQ(1u, result->cardinality());
        EXPECT_TRUE(result->contains(3));
    }
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

// An index of several CLucene segments opens as a reader of several segments. The BM25 statistics
// read its norm length sums per segment, and they equal those of the reader's combined norms.
TEST_F(MultiSegmentCollectorTest, NormLengthSumsReadEachSegment) {
    const std::string dir = kTestDir + "/segmented_norms";
    ASSERT_TRUE(io::global_local_filesystem()->create_directory(dir).ok());
    std::string long_doc;
    for (int i = 0; i < 30; ++i) {
        long_doc += "word ";
    }
    create_test_index(dir, {"a b", "c", long_doc, "d e f", "g", "h i"}, 2, "title", true);

    auto reader = make_shared_reader(lucene::index::IndexReader::open(dir.c_str()));
    auto* multi_segment = dynamic_cast<lucene::index::MultiSegmentReader*>(reader.get());
    ASSERT_NE(multi_segment, nullptr);
    ASSERT_EQ(multi_segment->getSubReaders()->length, 3);
    uint64_t all = 0;
    ASSERT_TRUE(collection_statistics_detail::sum_norm_lengths(reader.get(), L"title", &all).ok());
    const std::vector<uint32_t> docs {0, 2, 3, 5};
    uint64_t some = 0;
    ASSERT_TRUE(
            collection_statistics_detail::sum_norm_lengths_of(reader.get(), L"title", docs, &some)
                    .ok());

    auto combined = make_shared_reader(lucene::index::IndexReader::open(dir.c_str()));
    const uint8_t* norms = combined->norms(L"title");
    ASSERT_NE(norms, nullptr);
    auto length = [](uint8_t norm) {
        return static_cast<uint64_t>(static_cast<float>(BM25Similarity::byte4_to_int(norm)));
    };
    uint64_t expected_all = 0;
    for (int32_t doc = 0; doc < combined->maxDoc(); ++doc) {
        expected_all += length(norms[doc]);
    }
    uint64_t expected_some = 0;
    for (const uint32_t doc : docs) {
        expected_some += length(norms[doc]);
    }
    EXPECT_EQ(all, expected_all);
    EXPECT_EQ(some, expected_some);
    // Lengths below 24 are exact; the 30-token row reads back approximately.
    EXPECT_EQ(expected_all - length(norms[2]), 9);
}

} // namespace doris::segment_v2
