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

#include <functional>
#include <map>
#include <memory>
#include <roaring/roaring.hh>
#include <string>
#include <vector>

#include "io/fs/local_file_system.h"
#include "storage/index/index_query_context.h"
#include "storage/index/inverted/analyzer/custom_analyzer.h"
#include "storage/index/inverted/query_v2/all_query/all_query.h"
#include "storage/index/inverted/query_v2/boolean_query/boolean_query_builder.h"
#include "storage/index/inverted/query_v2/collect/doc_set_collector.h"
#include "storage/index/inverted/query_v2/collect/multi_segment_util.h"
#include "storage/index/inverted/query_v2/collect/top_k_collector.h"
#include "storage/index/inverted/query_v2/term_query/term_query.h"
#include "storage/index/inverted/similarity/collection_statistics.h"
#include "storage/index/inverted/spi/clucene_index_source.h"
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

class SegmentDomainNullIterator final : public IndexIterator {
public:
    SegmentDomainNullIterator() : _cache(1024 * 1024, 1) { _nulls.add(2); }

    IndexReaderPtr get_reader(IndexReaderType /*type*/) const override { return nullptr; }
    Status read_from_index(const IndexParam& /*param*/) override { return Status::OK(); }
    Result<bool> has_null() override { return true; }
    Status read_null_bitmap(InvertedIndexQueryCacheHandle* handle) override {
        _cache.insert(_key, std::make_shared<roaring::Roaring>(_nulls), handle);
        return Status::OK();
    }

private:
    roaring::Roaring _nulls;
    InvertedIndexQueryCache _cache;
    InvertedIndexQueryCache::CacheKey _key {.index_path = "segment_domain_nulls",
                                            .column_name = "title",
                                            .query_type = InvertedIndexQueryType::UNKNOWN_QUERY,
                                            .value = ""};
};

class SegmentDomainNullResolver final : public NullBitmapResolver {
public:
    IndexIterator* iterator_for(const Scorer& /*scorer*/,
                                const std::string& /*logical_field*/) const override {
        return &_iterator;
    }

private:
    mutable SegmentDomainNullIterator _iterator;
};

TEST_F(MultiSegmentCollectorTest, CollectDocSetWithMultiReader) {
    auto* dir0 = FSDirectory::getDirectory((kTestDir + "/segment0").c_str());
    auto* dir1 = FSDirectory::getDirectory((kTestDir + "/segment1").c_str());

    ValueArray<lucene::index::IndexReader*> readers(2);
    readers[0] = lucene::index::IndexReader::open(dir0, true);
    readers[1] = lucene::index::IndexReader::open(dir1, true);
    auto reader = make_shared_reader(_CLNEW lucene::index::MultiReader(&readers, true));

    auto index_query_context = std::make_shared<IndexQueryContext>();
    auto field = StringHelper::to_wstring("title");
    TermQuery query(index_query_context, field, "fleabag");
    auto weight = query.weight(false);

    QueryExecutionContext exec_ctx;
    exec_ctx.segment_num_rows = reader->maxDoc();
    exec_ctx.sources = {clucene_index_source(reader, field, nullptr)};
    exec_ctx.field_sources.emplace(field, clucene_index_source(reader, field, nullptr));

    auto roaring = std::make_shared<roaring::Roaring>();
    ASSERT_NO_THROW(collect_multi_segment_doc_set(weight, exec_ctx, "", roaring, nullptr, false));

    EXPECT_EQ(roaring->cardinality(), 2);
    EXPECT_TRUE(roaring->contains(0));
    EXPECT_TRUE(roaring->contains(3));

    _CLDECDELETE(dir0);
    _CLDECDELETE(dir1);
}

// Statistics for scoring that read no collection.
class FixedCollectionStatistics final : public CollectionStatistics {
public:
    float get_or_calculate_idf(const std::wstring& /*field_name*/,
                               const std::wstring& /*term*/) override {
        return 1.5F;
    }
    float get_or_calculate_avg_dl(const std::wstring& /*field_name*/) override { return 2.0F; }
};

// Each segment's scored rows reach the similarity in the global docid domain, with the scores
// its scorer gives them one row at a time: a term's rows read a postings block at a time, a
// disjunction's as the rows it lists.
TEST_F(MultiSegmentCollectorTest, ScoredRowsReachTheSimilarityInTheGlobalDocIdDomain) {
    ValueArray<lucene::index::IndexReader*> readers(2);
    readers[0] = lucene::index::IndexReader::open((kTestDir + "/segment0").c_str());
    readers[1] = lucene::index::IndexReader::open((kTestDir + "/segment1").c_str());
    auto reader = make_shared_reader(_CLNEW lucene::index::MultiReader(&readers, true));

    const auto field = StringHelper::to_wstring("title");
    auto index_query_context = std::make_shared<IndexQueryContext>();
    index_query_context->collection_statistics = std::make_shared<FixedCollectionStatistics>();
    QueryExecutionContext context;
    context.segment_num_rows = reader->maxDoc();
    context.sources = {clucene_index_source(reader, field, nullptr)};
    context.field_sources.emplace(field, clucene_index_source(reader, field, nullptr));
    const auto term = [&](const std::string& text) -> QueryPtr {
        return std::make_shared<TermQuery>(index_query_context, field, text);
    };
    const auto disjunction = [&]() {
        OperatorBooleanQueryBuilder builder(OperatorType::OP_OR);
        builder.add(term("fleabag"));
        builder.add(term("title"));
        return builder.build();
    };
    const std::function<QueryPtr()> queries[] = {[&]() { return term("fleabag"); }, disjunction};
    const std::vector<uint32_t> expected_rows[] = {{0, 3}, {0, 1, 3}};
    for (size_t i = 0; i < std::size(queries); ++i) {
        std::map<uint32_t, float> expected;
        const auto stepped = queries[i]()->weight(true);
        for_each_index_segment(
                context, "", [&](const QueryExecutionContext& segment, uint32_t doc_base) {
                    auto scorer = stepped->scorer(segment, "");
                    for (uint32_t doc = scorer->doc(); doc != TERMINATED; doc = scorer->advance()) {
                        expected[doc + doc_base] = scorer->score();
                    }
                });
        auto rows = std::make_shared<roaring::Roaring>();
        auto similarity = std::make_shared<CollectionSimilarity>();
        collect_multi_segment_doc_set(queries[i]()->weight(true), context, "", rows, similarity,
                                      true);

        std::vector<uint32_t> actual_rows(rows->cardinality());
        rows->toUint32Array(actual_rows.data());
        EXPECT_EQ(actual_rows, expected_rows[i]) << i;
        const auto scores = similarity->release_scores();
        const std::map<uint32_t, float> actual(scores.begin(), scores.end());
        EXPECT_EQ(actual, expected) << i;
    }
}

TEST_F(MultiSegmentCollectorTest, DeletedDocumentsDoNotShrinkTheLocalDocIdDomain) {
    ValueArray<lucene::index::IndexReader*> readers(2);
    readers[0] = lucene::index::IndexReader::open((kTestDir + "/segment0").c_str());
    readers[1] = lucene::index::IndexReader::open((kTestDir + "/segment1").c_str());
    readers[0]->deleteDocument(0);
    ASSERT_EQ(readers[0]->numDocs(), 1);
    ASSERT_EQ(readers[0]->maxDoc(), 2);
    auto reader = make_shared_reader(_CLNEW lucene::index::MultiReader(&readers, true));

    const auto field = StringHelper::to_wstring("title");
    OperatorBooleanQueryBuilder builder(OperatorType::OP_AND);
    builder.add(std::make_shared<AllQuery>());
    builder.add(std::make_shared<TermQuery>(std::make_shared<IndexQueryContext>(), field, "other"));
    const auto weight = builder.build()->weight(false);
    QueryExecutionContext context;
    context.segment_num_rows = reader->maxDoc();
    context.sources = {clucene_index_source(reader, field, nullptr)};
    context.field_sources.emplace(field, clucene_index_source(reader, field, nullptr));

    auto actual = std::make_shared<roaring::Roaring>();
    collect_multi_segment_doc_set(weight, context, "", actual, nullptr, false);
    EXPECT_EQ(actual->cardinality(), 1);
    EXPECT_TRUE(actual->contains(1));
}

TEST_F(MultiSegmentCollectorTest, GlobalNullRowsAreSlicedIntoEachLocalDocIdDomain) {
    create_test_index(kTestDir + "/segment1", {"", "fleabag finale"});
    ValueArray<lucene::index::IndexReader*> readers(2);
    readers[0] = lucene::index::IndexReader::open((kTestDir + "/segment0").c_str());
    readers[1] = lucene::index::IndexReader::open((kTestDir + "/segment1").c_str());
    auto reader = make_shared_reader(_CLNEW lucene::index::MultiReader(&readers, true));
    SegmentDomainNullResolver resolver;
    QueryExecutionContext context;
    context.segment_num_rows = reader->maxDoc();
    context.sources = {clucene_index_source(reader, L"title", nullptr)};
    context.field_sources.emplace(L"title", clucene_index_source(reader, L"title", nullptr));
    context.null_resolver = &resolver;
    auto weight = std::make_shared<AllWeight>(L"title", true, false);

    auto actual = std::make_shared<roaring::Roaring>();
    collect_multi_segment_doc_set(weight, context, "", actual, nullptr, false);
    EXPECT_EQ(actual->cardinality(), 3);
    EXPECT_TRUE(actual->contains(0));
    EXPECT_TRUE(actual->contains(1));
    EXPECT_TRUE(actual->contains(3));
    EXPECT_FALSE(actual->contains(2));

    roaring::Roaring actual_nulls;
    for_each_index_segment(context, "", [&](const QueryExecutionContext& local, uint32_t base) {
        auto scorer = weight->scorer(local);
        const auto* nulls = scorer->get_null_bitmap(local.null_resolver);
        if (nulls != nullptr) {
            for (const auto row : *nulls) {
                actual_nulls.add(base + row);
            }
        }
    });
    EXPECT_EQ(actual_nulls.cardinality(), 1);
    EXPECT_TRUE(actual_nulls.contains(2));
}

// CLucene opens an empty posting for a term its dictionary lacks, so the term matches no row and
// leaves the field's NULL rows UNKNOWN, as a term it holds does.
TEST_F(MultiSegmentCollectorTest, AnAbsentTermKeepsTheFieldsNullRowsUnknown) {
    create_test_index(kTestDir + "/segment1", {"", "fleabag finale"});
    ValueArray<lucene::index::IndexReader*> readers(2);
    readers[0] = lucene::index::IndexReader::open((kTestDir + "/segment0").c_str());
    readers[1] = lucene::index::IndexReader::open((kTestDir + "/segment1").c_str());
    auto reader = make_shared_reader(_CLNEW lucene::index::MultiReader(&readers, true));
    SegmentDomainNullResolver resolver;
    QueryExecutionContext context;
    context.segment_num_rows = reader->maxDoc();
    context.sources = {clucene_index_source(reader, L"title", nullptr)};
    context.field_sources.emplace(L"title", clucene_index_source(reader, L"title", nullptr));
    context.null_resolver = &resolver;
    for (const std::string term : {"fleabag", "absentterm"}) {
        SCOPED_TRACE(term);
        TermQuery query(std::make_shared<IndexQueryContext>(), L"title", term);
        auto weight = query.weight(false);
        auto rows = std::make_shared<roaring::Roaring>();
        roaring::Roaring nulls;
        collect_multi_segment_doc_set(weight, context, "", rows, nullptr, false, &nulls);
        EXPECT_FALSE(rows->contains(2));
        EXPECT_EQ(nulls.cardinality(), 1);
        EXPECT_TRUE(nulls.contains(2));
    }
}

TEST_F(MultiSegmentCollectorTest, LocalNullRangesPreserveCompressionAndUint32Boundary) {
    SegmentDomainNullResolver resolver;
    QueryExecutionContext context;
    context.null_resolver = &resolver;
    for (const auto& [base, count] : {std::pair<uint32_t, uint32_t> {65535, 1000000},
                                      std::pair<uint32_t, uint32_t> {UINT32_MAX - 11, 12}}) {
        roaring::Roaring global_rows;
        global_rows.addRange(base, uint64_t(base) + count);
        global_rows.add(0);
        global_rows.add(UINT32_MAX);
        const auto original = global_rows;
        SegmentNullBitmapResolver local(context, base, count);
        local.localize_null_rows(global_rows);
        EXPECT_EQ(global_rows.cardinality(), count);
        EXPECT_TRUE(global_rows.containsRange(0, count));
        EXPECT_LE(global_rows.getSizeInBytes(), 256);
        EXPECT_TRUE(original.contains(base));
        EXPECT_TRUE(original.contains(UINT32_MAX));
    }
}

TEST_F(MultiSegmentCollectorTest, CollectTopKExcludesDeletedDocs) {
    auto* dir0 = FSDirectory::getDirectory((kTestDir + "/segment0").c_str());
    auto* dir1 = FSDirectory::getDirectory((kTestDir + "/segment1").c_str());

    ValueArray<lucene::index::IndexReader*> readers(2);
    readers[0] = lucene::index::IndexReader::open(dir0, true);
    readers[1] = lucene::index::IndexReader::open(dir1, true);
    auto reader = make_shared_reader(_CLNEW lucene::index::MultiReader(&readers, true));

    auto index_query_context = std::make_shared<IndexQueryContext>();
    auto field = StringHelper::to_wstring("title");
    TermQuery query(index_query_context, field, "fleabag");
    auto weight = query.weight(false);

    QueryExecutionContext exec_ctx;
    exec_ctx.segment_num_rows = reader->maxDoc();
    exec_ctx.sources = {clucene_index_source(reader, field, nullptr)};
    exec_ctx.field_sources.emplace(field, clucene_index_source(reader, field, nullptr));

    auto mutable_deleted_docs = std::make_shared<roaring::Roaring>();
    mutable_deleted_docs->add(0);
    std::shared_ptr<const roaring::Roaring> deleted_docs = std::move(mutable_deleted_docs);
    for (bool use_wand : {false, true}) {
        auto roaring = std::make_shared<roaring::Roaring>();
        ASSERT_NO_THROW(collect_multi_segment_top_k(weight, exec_ctx, "", 1, roaring, nullptr,
                                                    use_wand, deleted_docs));

        EXPECT_EQ(roaring->cardinality(), 1);
        EXPECT_TRUE(roaring->contains(3));
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
    ASSERT_GT(clucene_index_source(field_reader, L"title", nullptr)->segments().size(), 1);

    auto index_query_context = std::make_shared<IndexQueryContext>();
    auto field = StringHelper::to_wstring("title");
    TermQuery query(index_query_context, field, "fleabag");
    auto weight = query.weight(false);

    QueryExecutionContext exec_ctx;
    exec_ctx.segment_num_rows = field_reader->maxDoc();
    exec_ctx.sources = {clucene_index_source(leading_reader, field, nullptr)};
    exec_ctx.field_sources.emplace(field, clucene_index_source(field_reader, field, nullptr));

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
    TermQuery query(index_query_context, field, "fleabag");
    auto weight = query.weight(false);

    QueryExecutionContext exec_ctx;
    exec_ctx.segment_num_rows = bound_reader->maxDoc();
    exec_ctx.sources = {clucene_index_source(bound_reader, field, nullptr)};
    exec_ctx.source_bindings.emplace("bound-title",
                                     clucene_index_source(bound_reader, field, nullptr));
    exec_ctx.field_sources.emplace(field, clucene_index_source(field_reader, field, nullptr));

    auto roaring = std::make_shared<roaring::Roaring>();
    ASSERT_NO_THROW(collect_multi_segment_doc_set(weight, exec_ctx, "bound-title", roaring, nullptr,
                                                  false));

    EXPECT_EQ(roaring->cardinality(), 1);
    EXPECT_TRUE(roaring->contains(0));

    _CLDECDELETE(dir0);
    _CLDECDELETE(dir1);
}

} // namespace doris::segment_v2
