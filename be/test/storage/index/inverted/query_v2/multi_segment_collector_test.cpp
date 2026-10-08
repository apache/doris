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
#include "storage/index/inverted/query_v2/bit_set_query/bit_set_query.h"
#include "storage/index/inverted/query_v2/boolean_query/boolean_query_builder.h"
#include "storage/index/inverted/query_v2/collect/doc_set_collector.h"
#include "storage/index/inverted/query_v2/collect/top_k_collector.h"
#include "storage/index/inverted/query_v2/phrase_prefix_query/phrase_prefix_weight.h"
#include "storage/index/inverted/query_v2/phrase_query/multi_phrase_weight.h"
#include "storage/index/inverted/query_v2/phrase_query/phrase_query.h"
#include "storage/index/inverted/query_v2/term_query/term_query.h"
#include "storage/index/inverted/similarity/collection_statistics.h"
#include "storage/index/inverted/spi/clucene_index_source.h"
#include "storage/index/inverted/util/string_helper.h"
#include "storage/index/snii/reader/snii_index_source.h"
#include "storage/index/snii/writer/snii_compound_writer.h"
#include "storage/index/snii_query_test_util.h"

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
    InvertedIndexQueryCache::CacheKey _key {"segment_domain_nulls", "title",
                                            InvertedIndexQueryType::UNKNOWN_QUERY, ""};
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

class ListedRowsWeight final : public Weight {
public:
    bool lists_rows(const QueryExecutionContext& /*context*/,
                    const std::string& binding_key) const override {
        EXPECT_EQ(binding_key, "bound-title");
        return true;
    }

    index_query::TruthSet listed_rows(const QueryExecutionContext& /*context*/,
                                      const std::string& binding_key,
                                      const roaring::Roaring* candidates) override {
        EXPECT_EQ(binding_key, "bound-title");
        EXPECT_EQ(candidates, nullptr);
        ++listed_calls;
        return {.true_rows = roaring::Roaring::bitmapOf(1, 1U),
                .null_rows = roaring::Roaring::bitmapOf(1, 2U)};
    }

    ScorerPtr scorer(const QueryExecutionContext& context,
                     const std::string& binding_key) override {
        EXPECT_EQ(binding_key, "bound-title");
        ++scorer_calls;
        BitSetWeight weight(std::make_shared<roaring::Roaring>(roaring::Roaring::bitmapOf(1, 1U)),
                            std::make_shared<roaring::Roaring>(roaring::Roaring::bitmapOf(1, 2U)));
        return weight.scorer(context);
    }

    int listed_calls = 0;
    int scorer_calls = 0;
};

TEST(DocSetCollectorTest, ListedRowsAvoidScorersUnlessScoringIsEnabled) {
    QueryExecutionContext context;
    context.segment_num_rows = 8;
    for (bool enable_scoring : {false, true}) {
        SCOPED_TRACE(enable_scoring);
        auto weight = std::make_shared<ListedRowsWeight>();
        auto rows = std::make_shared<roaring::Roaring>(roaring::Roaring::bitmapOf(1, 3U));
        auto nulls = roaring::Roaring::bitmapOf(1, 5U);
        auto similarity = std::make_shared<CollectionSimilarity>();
        collect_multi_segment_doc_set(weight, context, "bound-title", rows, similarity,
                                      enable_scoring, &nulls);
        EXPECT_EQ(*rows, roaring::Roaring::bitmapOf(2, 1U, 3U));
        EXPECT_EQ(nulls, roaring::Roaring::bitmapOf(2, 2U, 5U));
        EXPECT_EQ(weight->listed_calls, enable_scoring ? 0 : 1);
        EXPECT_EQ(weight->scorer_calls, enable_scoring ? 1 : 0);
        EXPECT_EQ(similarity->_bm25_scores.size(), enable_scoring ? 1 : 0);
    }
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

static void check_native_posting_block(const index_query::PostingsBlock& block, uint32_t doc) {
    ASSERT_EQ(block.size(), 1);
    EXPECT_EQ(block.doc_at(0), doc);
    EXPECT_EQ(block.docs.front(), doc);
    EXPECT_EQ(block.freq_at(0), 1);
    EXPECT_EQ(block.norm_at(0), 0);
}

static void check_partition_position(index_query::PostingsCursor& cursor, uint32_t position) {
    std::vector<uint32_t> positions;
    ASSERT_TRUE(cursor.append_positions(0, 0, positions).ok());
    EXPECT_EQ(positions, (std::vector<uint32_t> {position}));
}

static void check_next_partition(index_query::PostingsCursor& cursor, uint32_t doc,
                                 uint32_t partition_end) {
    index_query::PostingsBlock block;
    bool eof = false;
    ASSERT_TRUE(cursor.next_block(&block, &eof).ok());
    ASSERT_FALSE(eof);
    check_native_posting_block(block, doc);
    const auto bound = cursor.current_block_bound();
    ASSERT_TRUE(bound.last_doc_known);
    EXPECT_GE(bound.last_doc, doc);
    EXPECT_LT(bound.last_doc, partition_end);
    check_partition_position(cursor, 0);
}

static void check_exhausted_postings(index_query::PostingsCursor& cursor, uint32_t doc_freq) {
    index_query::PostingsBlock block;
    bool eof = false;
    ASSERT_TRUE(cursor.next_block(&block, &eof).ok());
    EXPECT_TRUE(eof);
    EXPECT_EQ(block.size(), 0);
    EXPECT_EQ(cursor.doc_freq(), doc_freq);
}

TEST_F(MultiSegmentCollectorTest, NestedReadersPreserveGlobalPostingsAndTermFrequency) {
    ValueArray<lucene::index::IndexReader*> nested(1);
    nested[0] = lucene::index::IndexReader::open((kTestDir + "/segment0").c_str());
    ValueArray<lucene::index::IndexReader*> readers(2);
    readers[0] = _CLNEW lucene::index::MultiReader(&nested, true);
    readers[1] = lucene::index::IndexReader::open((kTestDir + "/segment1").c_str());
    auto reader = make_shared_reader(_CLNEW lucene::index::MultiReader(&readers, true));
    auto source = clucene_index_source(reader, L"title", nullptr);
    std::unique_ptr<index_query::PostingsCursor> cursor;
    ASSERT_TRUE(source->open_term("fleabag", true, true, &cursor).ok());
    ASSERT_NE(cursor, nullptr);
    EXPECT_EQ(cursor->doc_freq(), 2);
    check_next_partition(*cursor, 0, 2);
    check_next_partition(*cursor, 3, 4);
    check_exhausted_postings(*cursor, 2);

    ASSERT_TRUE(source->open_term("finale", true, false, &cursor).ok());
    EXPECT_EQ(cursor->doc_freq(), 1);
    index_query::PostingsBlock block;
    bool eof = false;
    ASSERT_TRUE(cursor->seek_block(2, &block, &eof).ok());
    ASSERT_FALSE(eof);
    EXPECT_EQ(block.doc_at(0), 3);
    check_partition_position(*cursor, 1);

    ASSERT_TRUE(source->open_term("absent", true, false, &cursor).ok());
    check_exhausted_postings(*cursor, 0);
}

TEST_F(MultiSegmentCollectorTest, PhraseCandidatesUseTheGlobalDocumentDomain) {
    ValueArray<lucene::index::IndexReader*> readers(2);
    readers[0] = lucene::index::IndexReader::open((kTestDir + "/segment0").c_str());
    readers[1] = lucene::index::IndexReader::open((kTestDir + "/segment1").c_str());
    auto reader = make_shared_reader(_CLNEW lucene::index::MultiReader(&readers, true));
    const std::wstring field = L"title";
    const std::vector<TermInfo> terms {{.term = std::string("fleabag"), .position = 0},
                                       {.term = std::string("finale"), .position = 1}};
    const auto candidates = roaring::Roaring::bitmapOf(1, 3);
    QueryExecutionContext context;
    context.segment_num_rows = reader->maxDoc();
    context.field_sources.emplace(field, clucene_index_source(reader, field, nullptr));

    for (const bool restricted : {false, true}) {
        PhraseQuery query(std::make_shared<IndexQueryContext>(), field, terms,
                          {.candidates = restricted ? &candidates : nullptr});
        auto rows = std::make_shared<roaring::Roaring>();
        collect_multi_segment_doc_set(query.weight(false), context, "", rows, nullptr, false);
        EXPECT_EQ(*rows, candidates) << "restricted=" << restricted;
    }

    for (const auto& selected : {roaring::Roaring(), roaring::Roaring::bitmapOf(1, 2),
                                 roaring::Roaring::bitmapOf(4, 0, 2, 3, 4)}) {
        PhraseQuery query(std::make_shared<IndexQueryContext>(), field, terms,
                          {.candidates = &selected});
        auto rows = std::make_shared<roaring::Roaring>();
        collect_multi_segment_doc_set(query.weight(false), context, "", rows, nullptr, false);
        EXPECT_EQ(*rows, candidates & selected);
    }
}

TEST_F(MultiSegmentCollectorTest, PhraseCandidatesRemainValidAfterWeightDestruction) {
    ValueArray<lucene::index::IndexReader*> readers(2);
    readers[0] = lucene::index::IndexReader::open((kTestDir + "/segment0").c_str());
    readers[1] = lucene::index::IndexReader::open((kTestDir + "/segment1").c_str());
    auto reader = make_shared_reader(_CLNEW lucene::index::MultiReader(&readers, true));
    const std::wstring field = L"title";
    const auto candidates = roaring::Roaring::bitmapOf(3, 2, 3, 4);
    const index_query::PhraseQueryOptions options {.candidates = &candidates};
    const std::vector<TermInfo> terms {{.term = std::string("fleabag"), .position = 0},
                                       {.term = std::string("finale"), .position = 1}};
    const std::vector<TermInfo> alternatives {
            {.term = std::string("fleabag"), .position = 0},
            {.term = std::vector<std::string> {"premiere", "finale"}, .position = 1}};
    QueryExecutionContext segment;
    segment.segment_num_rows = reader->maxDoc();
    segment.field_sources.emplace(field, clucene_index_source(reader, field, nullptr));
    for (const bool scoring : {false, true}) {
        auto similarity = scoring ? std::make_shared<BM25Similarity>(2.0F, 8.0F) : nullptr;
        std::vector<WeightPtr> weights {
                std::make_shared<PhraseWeight>(field, terms, options, similarity, scoring, false),
                std::make_shared<MultiPhraseWeight>(field, alternatives, options, similarity,
                                                    scoring, false),
                std::make_shared<PhrasePrefixWeight>(
                        field, std::vector<std::pair<size_t, std::string>> {{0, "fleabag"}},
                        std::pair<size_t, std::string> {1, "fin"}, similarity, scoring, 50, options,
                        false, false)};
        for (auto& weight : weights) {
            auto scorer = weight->scorer(segment, "");
            weight.reset();
            ASSERT_EQ(scorer->doc(), 3);
            const float expected_score = scoring ? similarity->score(1.0F, 0) : 1.0F;
            EXPECT_FLOAT_EQ(scorer->score(), expected_score);
            EXPECT_EQ(scorer->seek(3), 3);
            EXPECT_EQ(scorer->advance(), TERMINATED);
        }
    }
}

TEST_F(MultiSegmentCollectorTest, MaterializedPhrasePostingsPreserveDocumentNorms) {
    const std::vector<std::string> docs {"fleabag finale", "fleabag other",
                                         "fleabag news many more words",
                                         "fleabag finale plus many more words to extend length"};
    const std::vector<TermInfo> exact {{.term = std::string("fleabag"), .position = 0},
                                       {.term = std::string("finale"), .position = 1}};
    const std::vector<TermInfo> alternatives {
            {.term = std::string("fleabag"), .position = 0},
            {.term = std::vector<std::string> {"finale", "missing"}, .position = 1}};
    for (const int32_t max_buffered_docs : {100, 2}) {
        SCOPED_TRACE(max_buffered_docs);
        const auto directory = kTestDir + "/norms_" + std::to_string(max_buffered_docs);
        ASSERT_TRUE(io::global_local_filesystem()->create_directory(directory).ok());
        create_test_index(directory, docs, max_buffered_docs, "title", true);
        auto reader = make_shared_reader(lucene::index::IndexReader::open(directory.c_str()));
        QueryExecutionContext context;
        context.segment_num_rows = 4;
        context.field_sources.emplace(L"title", clucene_index_source(reader, L"title", nullptr));
        auto similarity = std::make_shared<BM25Similarity>(2.0F, 8.0F);
        const index_query::PhraseQueryOptions options;
        const std::vector<WeightPtr> weights {
                std::make_shared<PhraseWeight>(L"title", exact, options, similarity, true, false),
                std::make_shared<MultiPhraseWeight>(L"title", alternatives, options, similarity,
                                                    true, false)};
        for (const auto& weight : weights) {
            auto scorer = weight->scorer(context, "");
            for (const auto& [doc, length] : {std::pair<uint32_t, int32_t> {0, 2}, {3, 9}}) {
                ASSERT_EQ(scorer->doc(), doc);
                const auto norm = BM25Similarity::int_to_byte4(length);
                EXPECT_EQ(scorer->norm(), norm);
                EXPECT_FLOAT_EQ(scorer->score(), similarity->score(1.0F, norm));
                scorer->advance();
            }
            EXPECT_EQ(scorer->doc(), TERMINATED);
        }
    }
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

static void check_collected_scores(const CollectionSimilarityPtr& similarity,
                                   const roaring::Roaring& expected,
                                   const std::map<uint32_t, float>& expected_scores) {
    ASSERT_EQ(similarity->_bm25_scores.size(), expected.cardinality());
    for (uint32_t doc : expected) {
        EXPECT_FLOAT_EQ(similarity->_bm25_scores.at(doc), expected_scores.at(doc));
    }
}

static void check_boolean_top_k(const QueryExecutionContext& context, OperatorType op,
                                const std::function<WeightPtr(bool)>& make_weight,
                                const std::map<uint32_t, float>& expected_scores) {
    for (const bool use_wand : {false, true}) {
        for (const bool deleted : {false, true}) {
            auto rows = std::make_shared<roaring::Roaring>();
            auto similarity = std::make_shared<CollectionSimilarity>();
            auto deletes =
                    deleted ? std::make_shared<roaring::Roaring>(roaring::Roaring::bitmapOf(1, 3))
                            : nullptr;
            collect_multi_segment_top_k(make_weight(true), context, "", 1, rows, similarity,
                                        use_wand, deletes);
            auto selected = roaring::Roaring::bitmapOf(1, 3);
            if (deleted) {
                selected = op == OperatorType::OP_OR ? roaring::Roaring::bitmapOf(1, 0)
                                                     : roaring::Roaring();
            }
            EXPECT_EQ(*rows, selected);
            check_collected_scores(similarity, selected, expected_scores);
        }
    }
}

static void check_boolean_collection(const QueryExecutionContext& context) {
    auto query_context = std::make_shared<IndexQueryContext>();
    query_context->collection_statistics = std::make_shared<FixedCollectionStatistics>();
    const auto make_clauses = [&]() {
        return std::vector<QueryPtr> {
                std::make_shared<TermQuery>(query_context, L"title", "fleabag"),
                std::make_shared<TermQuery>(query_context, L"body", "selected")};
    };
    std::map<uint32_t, float> expected_scores;
    for (const auto& clause : make_clauses()) {
        auto scorer = clause->weight(true)->scorer(context, "");
        for (uint32_t doc = scorer->doc(); doc != TERMINATED; doc = scorer->advance()) {
            expected_scores[doc] += scorer->score();
        }
    }
    for (const auto op : {OperatorType::OP_AND, OperatorType::OP_OR}) {
        SCOPED_TRACE(static_cast<int>(op));
        const auto make_weight = [&](bool scoring) {
            OperatorBooleanQueryBuilder builder(op);
            for (const auto& clause : make_clauses()) {
                builder.add(clause);
            }
            return builder.build()->weight(scoring);
        };
        const auto expected = op == OperatorType::OP_AND ? roaring::Roaring::bitmapOf(1, 3)
                                                         : roaring::Roaring::bitmapOf(2, 0, 3);
        for (const bool scoring : {false, true}) {
            auto rows = std::make_shared<roaring::Roaring>();
            auto similarity = std::make_shared<CollectionSimilarity>();
            collect_multi_segment_doc_set(make_weight(scoring), context, "", rows, similarity,
                                          scoring);
            EXPECT_EQ(*rows, expected);
            if (scoring) {
                check_collected_scores(similarity, expected, expected_scores);
            }
        }
        check_boolean_top_k(context, op, make_weight, expected_scores);
    }
}

TEST_F(MultiSegmentCollectorTest, MixedIndexSegmentLayoutsUseGlobalDocumentIds) {
    ValueArray<lucene::index::IndexReader*> readers(2);
    readers[0] = lucene::index::IndexReader::open((kTestDir + "/segment0").c_str());
    readers[1] = lucene::index::IndexReader::open((kTestDir + "/segment1").c_str());
    auto reader = make_shared_reader(_CLNEW lucene::index::MultiReader(&readers, true));

    snii::snii_test::MemoryFile file;
    snii::writer::SniiIndexInput input;
    input.index_id = 7;
    input.index_suffix = "Body";
    input.config = snii::format::IndexConfig::kDocsPositions;
    input.doc_count = 4;
    input.terms = {snii::snii_test::make_term("selected", {{.docid = 3, .positions = {0}}})};
    snii::writer::SniiCompoundWriter writer(&file);
    ASSERT_TRUE(writer.add_logical_index(input).ok());
    ASSERT_TRUE(writer.finish().ok());
    snii::reader::SniiSegmentReader segment;
    snii::reader::LogicalIndexReader index;
    ASSERT_TRUE(snii::reader::SniiSegmentReader::open(&file, &segment).ok());
    ASSERT_TRUE(segment.open_index(input.index_id, input.index_suffix, &index).ok());

    const auto title = clucene_index_source(reader, L"title", nullptr);
    const auto body = std::make_shared<snii::reader::SniiIndexSource>(index);
    ASSERT_EQ(title->doc_count(), 4);
    ASSERT_EQ(body->doc_count(), 4);
    QueryExecutionContext context;
    context.segment_num_rows = 4;
    context.sources = {title, body};
    context.field_sources.emplace(L"title", title);
    context.field_sources.emplace(L"body", body);
    check_boolean_collection(context);
}

TEST_F(MultiSegmentCollectorTest, DifferentCluceneLayoutsUseGlobalDocumentIds) {
    ValueArray<lucene::index::IndexReader*> readers(2);
    readers[0] = lucene::index::IndexReader::open((kTestDir + "/segment0").c_str());
    readers[1] = lucene::index::IndexReader::open((kTestDir + "/segment1").c_str());
    auto reader = make_shared_reader(_CLNEW lucene::index::MultiReader(&readers, true));
    const auto title = clucene_index_source(reader, L"title", nullptr);
    for (const int32_t max_buffered_docs : {100, 3, 2}) {
        SCOPED_TRACE(max_buffered_docs);
        const auto directory = kTestDir + "/body_" + std::to_string(max_buffered_docs);
        ASSERT_TRUE(io::global_local_filesystem()->create_directory(directory).ok());
        create_test_index(directory, {"other", "other", "other", "selected"}, max_buffered_docs,
                          "body");
        auto body_reader = make_shared_reader(lucene::index::IndexReader::open(directory.c_str()));
        ASSERT_STREQ(body_reader->getObjectName(),
                     max_buffered_docs == 100 ? "SegmentReader" : "MultiSegmentReader");
        const auto body = clucene_index_source(body_reader, L"body", nullptr);
        QueryExecutionContext context;
        context.segment_num_rows = 4;
        context.sources = {title, body};
        context.field_sources.emplace(L"title", title);
        context.field_sources.emplace(L"body", body);
        check_boolean_collection(context);
    }
}

TEST_F(MultiSegmentCollectorTest, BitmapConditionsUseGlobalDocumentIds) {
    ValueArray<lucene::index::IndexReader*> readers(2);
    readers[0] = lucene::index::IndexReader::open((kTestDir + "/segment0").c_str());
    readers[1] = lucene::index::IndexReader::open((kTestDir + "/segment1").c_str());
    auto reader = make_shared_reader(_CLNEW lucene::index::MultiReader(&readers, true));
    QueryExecutionContext context;
    context.segment_num_rows = 4;
    context.field_sources.emplace(L"title", clucene_index_source(reader, L"title", nullptr));
    auto query_context = std::make_shared<IndexQueryContext>();
    query_context->collection_statistics = std::make_shared<FixedCollectionStatistics>();
    for (const bool unknown_field : {false, true}) {
        auto true_rows = std::make_shared<roaring::Roaring>();
        auto null_rows = std::make_shared<roaring::Roaring>();
        if (unknown_field) {
            null_rows->addRange(0, 4);
        } else {
            true_rows->add(3);
            null_rows->add(2);
        }
        for (const auto op : {OperatorType::OP_AND, OperatorType::OP_OR}) {
            const bool conjunction = op == OperatorType::OP_AND;
            auto expected = roaring::Roaring::bitmapOf(2, 0, 3);
            if (conjunction) {
                expected = unknown_field ? roaring::Roaring() : roaring::Roaring::bitmapOf(1, 3);
            }
            auto expected_nulls =
                    conjunction ? roaring::Roaring() : roaring::Roaring::bitmapOf(1, 2);
            if (unknown_field) {
                expected_nulls = conjunction ? roaring::Roaring::bitmapOf(2, 0, 3)
                                             : roaring::Roaring::bitmapOf(2, 1, 2);
            }
            for (const bool scoring : {false, true}) {
                OperatorBooleanQueryBuilder builder(op);
                builder.add(std::make_shared<TermQuery>(query_context, L"title", "fleabag"));
                builder.add(std::make_shared<BitSetQuery>(true_rows, null_rows));
                auto rows = std::make_shared<roaring::Roaring>();
                roaring::Roaring nulls;
                collect_multi_segment_doc_set(builder.build()->weight(scoring), context, "", rows,
                                              nullptr, scoring, &nulls);
                EXPECT_EQ(*rows, expected);
                EXPECT_EQ(nulls, expected_nulls);
            }
        }
    }
}

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
        auto scorer = stepped->scorer(context, "");
        for (uint32_t doc = scorer->doc(); doc != TERMINATED; doc = scorer->advance()) {
            expected[doc] = scorer->score();
        }
        auto rows = std::make_shared<roaring::Roaring>();
        auto similarity = std::make_shared<CollectionSimilarity>();
        collect_multi_segment_doc_set(queries[i]()->weight(true), context, "", rows, similarity,
                                      true);

        std::vector<uint32_t> actual_rows(rows->cardinality());
        rows->toUint32Array(actual_rows.data());
        EXPECT_EQ(actual_rows, expected_rows[i]) << i;
        const auto& scores = similarity->_bm25_scores;
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

TEST_F(MultiSegmentCollectorTest, NullRowsKeepTheirGlobalDocumentIds) {
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
    collect_multi_segment_doc_set(weight, context, "", actual, nullptr, false, &actual_nulls);
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
    ASSERT_STREQ(field_reader->getObjectName(), "MultiSegmentReader");

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
