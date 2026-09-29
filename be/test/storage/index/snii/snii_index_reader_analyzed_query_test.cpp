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

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/check.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "io/fs/local_file_system.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "storage/compaction/collection_similarity.h"
#include "storage/index/index_file_reader.h"
#include "storage/index/index_query_context.h"
#include "storage/index/inverted/inverted_index_cache.h"
#include "storage/index/inverted/inverted_index_desc.h"
#include "storage/index/inverted/query/query_info.h"
#include "storage/index/inverted/similarity/collection_statistics.h"
#include "storage/index/query/spi/index_source.h"
#include "storage/index/query/spi/postings_cursor.h"
#include "storage/index/snii/io/local_file.h"
#include "storage/index/snii/query/bm25_scorer.h"
#include "storage/index/snii/snii_index_reader.h"
#include "storage/index/snii/writer/snii_compound_writer.h"
#include "storage/index/snii_query_test_util.h"
#include "storage/olap_common.h"
#include "storage/tablet/tablet_schema.h"

// The analyzed query entry: the caller hands the reader terms it already
// analyzed, the reader takes them verbatim, and everything after analysis (the
// result cache, the count-only fast path, scoring) behaves as for a raw query.
namespace doris::segment_v2 {
namespace {

using namespace doris::snii::snii_test;

constexpr int64_t kIndexId = 43;
constexpr const char* kTestDir = "./ut_dir/snii_index_reader_analyzed_query_test";
constexpr uint32_t kDocCount = 64;

struct QueryExecution {
    explicit QueryExecution(bool enable_query_cache = true) {
        TQueryOptions query_options;
        query_options.query_type = TQueryType::SELECT;
        query_options.enable_inverted_index_query_cache = enable_query_cache;
        query_options.enable_inverted_index_searcher_cache = false;
        runtime_state.set_query_options(query_options);
        context->io_ctx = &io_ctx;
        context->stats = &stats;
        context->runtime_state = &runtime_state;
    }

    OlapReaderStatistics stats;
    io::IOContext io_ctx;
    RuntimeState runtime_state;
    IndexQueryContextPtr context = std::make_shared<IndexQueryContext>();
};

TabletIndex make_meta(const std::map<std::string, std::string>& properties) {
    TabletIndexPB pb;
    pb.set_index_type(IndexType::INVERTED);
    pb.set_index_id(kIndexId);
    pb.set_index_name("analyzed_idx");
    pb.add_col_unique_id(0);
    for (const auto& [key, value] : properties) {
        pb.mutable_properties()->insert({key, value});
    }
    TabletIndex meta;
    meta.init_from_pb(pb);
    return meta;
}

Status write_segment(const std::string& path_prefix, doris::snii::writer::SniiIndexInput input) {
    input.index_id = kIndexId;
    input.config = doris::snii::format::IndexConfig::kDocsPositions;
    input.target_dict_block_bytes = 64;
    MemoryFile memory_file;
    doris::snii::writer::SniiCompoundWriter compound(&memory_file);
    RETURN_IF_ERROR(compound.add_logical_index(input));
    RETURN_IF_ERROR(compound.finish());
    doris::snii::io::LocalFileWriter local_file;
    RETURN_IF_ERROR(local_file.open(InvertedIndexDescriptor::get_index_file_path_v2(path_prefix)));
    RETURN_IF_ERROR(local_file.append(
            doris::snii::Slice(memory_file.data().data(), memory_file.data().size())));
    return local_file.finalize();
}

// "alpha" opens every doc, "beta" follows it in even docs and "betamax" in odd
// docs divisible by three.
doris::snii::writer::SniiIndexInput phrase_segment_input() {
    std::vector<PostingDoc> alpha;
    std::vector<PostingDoc> beta;
    std::vector<PostingDoc> betamax;
    for (uint32_t docid = 0; docid < kDocCount; ++docid) {
        alpha.push_back({.docid = docid, .positions = {0}});
        if (docid % 2 == 0) {
            beta.push_back({.docid = docid, .positions = {1}});
        } else if (docid % 3 == 0) {
            betamax.push_back({.docid = docid, .positions = {1}});
        }
    }
    doris::snii::writer::SniiIndexInput input;
    input.doc_count = kDocCount;
    input.terms = {make_term("alpha", std::move(alpha)), make_term("beta", std::move(beta)),
                   make_term("betamax", std::move(betamax))};
    return input;
}

// The phrase segment with a norm per document, which scoring needs.
doris::snii::writer::SniiIndexInput scored_segment_input() {
    doris::snii::writer::SniiIndexInput input = phrase_segment_input();
    input.encoded_norms.assign(kDocCount, doris::snii::query::encode_norm(2));
    return input;
}

// doc 0 "alpha beta", doc 1 "alpha gamma beta", doc 2 "beta alpha", doc 3 "alpha".
doris::snii::writer::SniiIndexInput slop_segment_input() {
    doris::snii::writer::SniiIndexInput input;
    input.doc_count = 4;
    input.terms = {make_term("alpha", {{.docid = 0, .positions = {0}},
                                       {.docid = 1, .positions = {0}},
                                       {.docid = 2, .positions = {1}},
                                       {.docid = 3, .positions = {0}}}),
                   make_term("beta", {{.docid = 0, .positions = {1}},
                                      {.docid = 1, .positions = {2}},
                                      {.docid = 2, .positions = {0}}}),
                   make_term("gamma", {{.docid = 1, .positions = {1}}})};
    return input;
}

std::vector<uint32_t> docids_of(const roaring::Roaring& bitmap) {
    return {bitmap.begin(), bitmap.end()};
}

std::vector<uint32_t> docids_where(bool (*keep)(uint32_t)) {
    std::vector<uint32_t> out;
    for (uint32_t docid = 0; docid < kDocCount; ++docid) {
        if (keep(docid)) {
            out.push_back(docid);
        }
    }
    return out;
}

bool every_doc(uint32_t) {
    return true;
}

bool is_even(uint32_t docid) {
    return docid % 2 == 0;
}

bool has_beta_prefix(uint32_t docid) {
    return docid % 2 == 0 || docid % 3 == 0;
}

// Terms at consecutive positions starting from 1, as the analyzer numbers them.
InvertedIndexQueryInfo terms(std::vector<std::string> values, int32_t slop = 0,
                             bool ordered = false) {
    InvertedIndexQueryInfo info;
    int32_t position = 0;
    for (auto& value : values) {
        TermInfo term;
        term.term = std::move(value);
        term.position = ++position;
        info.term_infos.push_back(std::move(term));
    }
    info.slop = slop;
    info.ordered = ordered;
    return info;
}

// The leaf SEARCH lowers to the terms of `info` under `query_type`.
index_query::logical::Node leaf_of(InvertedIndexQueryType query_type,
                                   const InvertedIndexQueryInfo& info) {
    namespace logical = index_query::logical;
    logical::Node leaf;
    if (info.term_infos.empty()) {
        leaf.value = logical::Empty {.field = {}};
        return leaf;
    }
    switch (query_type) {
    case InvertedIndexQueryType::EQUAL_QUERY:
        leaf.value = logical::Term {.field = {}, .term = info.term_infos.front().get_single_term()};
        break;
    case InvertedIndexQueryType::MATCH_ANY_QUERY:
    case InvertedIndexQueryType::MATCH_ALL_QUERY: {
        logical::TermSet set {.field = {},
                              .terms = {},
                              .require_all = query_type == InvertedIndexQueryType::MATCH_ALL_QUERY};
        for (const auto& term_info : info.term_infos) {
            set.terms.push_back(term_info.get_single_term());
        }
        leaf.value = std::move(set);
        break;
    }
    case InvertedIndexQueryType::MATCH_PHRASE_QUERY:
    case InvertedIndexQueryType::MATCH_PHRASE_PREFIX_QUERY:
        leaf.value = logical::Phrase {
                .field = {},
                .slots = info.term_infos,
                .slop = info.slop,
                .ordered = info.ordered,
                .prefix = query_type == InvertedIndexQueryType::MATCH_PHRASE_PREFIX_QUERY};
        break;
    default:
        leaf.value =
                logical::Expand {.field = {},
                                 .kind = query_type == InvertedIndexQueryType::MATCH_REGEXP_QUERY
                                                 ? logical::ExpandKind::kRegexp
                                                 : logical::ExpandKind::kWildcard,
                                 .pattern = info.term_infos.front().get_single_term()};
        break;
    }
    return leaf;
}

class FixedCollectionStatistics final : public CollectionStatistics {
public:
    float get_or_calculate_idf(const std::wstring&, const std::wstring& term) override {
        return term == L"alpha" ? 1.0F : 2.0F;
    }
    float get_or_calculate_avg_dl(const std::wstring&) override { return 3.0F; }
};

float score_for_doc(const CollectionSimilarity& similarity, uint32_t docid) {
    roaring::Roaring bitmap;
    bitmap.add(docid);
    IColumn::MutablePtr scores;
    auto row_ids = std::make_unique<std::vector<uint64_t>>();
    similarity.get_bm25_scores(&bitmap, scores, row_ids);
    if (row_ids->empty()) {
        return 0.0F;
    }
    const auto& nullable = assert_cast<const ColumnNullable&>(*scores);
    return assert_cast<const ColumnFloat32&>(nullable.get_nested_column()).get_data()[0];
}

class SniiIndexReaderAnalyzedQueryTest : public testing::Test {
protected:
    void SetUp() override {
        assert_ok(io::global_local_filesystem()->delete_directory(kTestDir));
        assert_ok(io::global_local_filesystem()->create_directory(kTestDir));
        _previous_query_cache = ExecEnv::GetInstance()->get_inverted_index_query_cache();
        _query_cache.reset(InvertedIndexQueryCache::create_global_cache(1024 * 1024, 1));
        ExecEnv::GetInstance()->set_inverted_index_query_cache(_query_cache.get());
        _reader = open_reader(phrase_segment_input(), "phrase_segment", _text_meta,
                              InvertedIndexReaderType::FULLTEXT);
    }

    void TearDown() override {
        _reader.reset();
        _file_readers.clear();
        ExecEnv::GetInstance()->set_inverted_index_query_cache(_previous_query_cache);
        _query_cache.reset();
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(kTestDir).ok());
    }

    std::shared_ptr<SniiIndexReader> open_reader(doris::snii::writer::SniiIndexInput input,
                                                 std::string_view name, const TabletIndex& meta,
                                                 InvertedIndexReaderType reader_type) {
        const std::string path_prefix = std::string(kTestDir) + "/" + std::string(name);
        const uint32_t doc_count = input.doc_count;
        assert_ok(write_segment(path_prefix, std::move(input)));
        auto file_reader = std::make_shared<IndexFileReader>(
                io::global_local_filesystem(), path_prefix, InvertedIndexStorageFormatPB::SNII);
        assert_ok(file_reader->init());
        _file_readers.push_back(file_reader);
        return SniiIndexReader::create_shared(&meta, file_reader, reader_type, doc_count,
                                              /*column_is_array=*/false);
    }

    // Reports a failed query and returns no rows so the test can go on.
    std::vector<uint32_t> raw(SniiIndexReader& reader, QueryExecution& execution, std::string text,
                              InvertedIndexQueryType query_type) {
        std::shared_ptr<roaring::Roaring> bitmap;
        const Field value = Field::create_field<TYPE_STRING>(std::move(text));
        assert_ok(reader.query(execution.context, "content", value, query_type, bitmap));
        return bitmap != nullptr ? docids_of(*bitmap) : std::vector<uint32_t> {};
    }

    std::vector<uint32_t> raw(QueryExecution& execution, std::string text,
                              InvertedIndexQueryType query_type) {
        return raw(*_reader, execution, std::move(text), query_type);
    }

    Status analyzed_status(SniiIndexReader& reader, QueryExecution& execution,
                           InvertedIndexQueryType query_type,
                           const InvertedIndexQueryInfo& query_info,
                           std::shared_ptr<roaring::Roaring>* bitmap) {
        return reader.query_leaf(execution.context, "content", leaf_of(query_type, query_info),
                                 *bitmap);
    }

    std::vector<uint32_t> analyzed(SniiIndexReader& reader, QueryExecution& execution,
                                   InvertedIndexQueryType query_type,
                                   const InvertedIndexQueryInfo& query_info) {
        std::shared_ptr<roaring::Roaring> bitmap;
        assert_ok(analyzed_status(reader, execution, query_type, query_info, &bitmap));
        return bitmap != nullptr ? docids_of(*bitmap) : std::vector<uint32_t> {};
    }

    std::vector<uint32_t> analyzed(QueryExecution& execution, InvertedIndexQueryType query_type,
                                   const InvertedIndexQueryInfo& query_info) {
        return analyzed(*_reader, execution, query_type, query_info);
    }

    TabletIndex _text_meta =
            make_meta({{"parser", "english"}, {"lower_case", "true"}, {"support_phrase", "true"}});
    TabletIndex _keyword_meta = make_meta({{"ignore_above", "3"}});
    std::vector<std::shared_ptr<IndexFileReader>> _file_readers;
    std::shared_ptr<SniiIndexReader> _reader;
    InvertedIndexQueryCache* _previous_query_cache = nullptr;
    std::unique_ptr<InvertedIndexQueryCache> _query_cache;
};

TEST_F(SniiIndexReaderAnalyzedQueryTest, MatchAnyAndMatchAllEqualTheRawQuery) {
    QueryExecution raw_any;
    QueryExecution analyzed_any;
    EXPECT_EQ(analyzed(analyzed_any, InvertedIndexQueryType::MATCH_ANY_QUERY,
                       terms({"alpha", "beta"})),
              docids_where(every_doc));
    EXPECT_EQ(raw(raw_any, "alpha beta", InvertedIndexQueryType::MATCH_ANY_QUERY),
              docids_where(every_doc));

    QueryExecution raw_all;
    QueryExecution analyzed_all;
    EXPECT_EQ(analyzed(analyzed_all, InvertedIndexQueryType::MATCH_ALL_QUERY,
                       terms({"alpha", "beta"})),
              docids_where(is_even));
    EXPECT_EQ(raw(raw_all, "alpha beta", InvertedIndexQueryType::MATCH_ALL_QUERY),
              docids_where(is_even));
}

TEST_F(SniiIndexReaderAnalyzedQueryTest, TermsAreTakenVerbatim) {
    QueryExecution raw_run;
    EXPECT_EQ(raw(raw_run, "Alpha", InvertedIndexQueryType::MATCH_ANY_QUERY),
              docids_where(every_doc))
            << "the raw entry lowercases through the analyzer";
    QueryExecution analyzed_run;
    EXPECT_TRUE(analyzed(analyzed_run, InvertedIndexQueryType::MATCH_ANY_QUERY, terms({"Alpha"}))
                        .empty())
            << "the analyzed entry looks the term up as given";
    EXPECT_EQ(analyzed_run.stats.inverted_index_analyzer_timer, 0);
}

TEST_F(SniiIndexReaderAnalyzedQueryTest, PhraseTakesSlopFromTheQueryInfo) {
    auto reader = open_reader(slop_segment_input(), "slop_segment", _text_meta,
                              InvertedIndexReaderType::FULLTEXT);
    const auto run = [&](InvertedIndexQueryType query_type, const InvertedIndexQueryInfo& info) {
        QueryExecution execution;
        std::shared_ptr<roaring::Roaring> bitmap;
        Status status = analyzed_status(*reader, execution, query_type, info, &bitmap);
        EXPECT_TRUE(status.ok()) << status;
        return bitmap != nullptr ? docids_of(*bitmap) : std::vector<uint32_t> {};
    };
    EXPECT_EQ(run(InvertedIndexQueryType::MATCH_PHRASE_QUERY, terms({"alpha", "beta"})),
              (std::vector<uint32_t> {0}));
    EXPECT_EQ(run(InvertedIndexQueryType::MATCH_PHRASE_QUERY, terms({"alpha", "beta"}, 1)),
              (std::vector<uint32_t> {0, 1}));

    // The raw entry parses "~2" out of the text; the analyzed entry never sees such
    // a suffix and takes the same slop from the query info.
    QueryExecution raw_run;
    std::shared_ptr<roaring::Roaring> raw_bitmap;
    const Field value = Field::create_field<TYPE_STRING>(std::string("alpha beta ~2"));
    assert_ok(reader->query(raw_run.context, "content", value,
                            InvertedIndexQueryType::MATCH_PHRASE_QUERY, raw_bitmap));
    ASSERT_NE(raw_bitmap, nullptr);
    EXPECT_EQ(run(InvertedIndexQueryType::MATCH_PHRASE_QUERY, terms({"alpha", "beta"}, 2)),
              docids_of(*raw_bitmap));
}

TEST_F(SniiIndexReaderAnalyzedQueryTest, PhrasePrefixEqualsTheRawQuery) {
    QueryExecution raw_run;
    QueryExecution analyzed_run;
    EXPECT_EQ(analyzed(analyzed_run, InvertedIndexQueryType::MATCH_PHRASE_PREFIX_QUERY,
                       terms({"alpha", "bet"})),
              docids_where(has_beta_prefix));
    EXPECT_EQ(raw(raw_run, "alpha bet", InvertedIndexQueryType::MATCH_PHRASE_PREFIX_QUERY),
              docids_where(has_beta_prefix));
}

TEST_F(SniiIndexReaderAnalyzedQueryTest, WildcardAndRegexpTakeTheTermAsThePattern) {
    QueryExecution wildcard_run;
    EXPECT_EQ(analyzed(wildcard_run, InvertedIndexQueryType::WILDCARD_QUERY, terms({"beta*"})),
              docids_where(has_beta_prefix));
    QueryExecution regexp_run;
    EXPECT_EQ(analyzed(regexp_run, InvertedIndexQueryType::MATCH_REGEXP_QUERY, terms({"beta.*"})),
              docids_where(has_beta_prefix));
    QueryExecution equal_run;
    EXPECT_EQ(analyzed(equal_run, InvertedIndexQueryType::EQUAL_QUERY, terms({"alpha"})),
              docids_where(every_doc));
}

TEST_F(SniiIndexReaderAnalyzedQueryTest, RejectsNoTermsAndAnswersMultiTermSlots) {
    QueryExecution execution;
    std::shared_ptr<roaring::Roaring> bitmap;
    // A leaf without terms is not a MATCH value that analyzed to nothing, so it is an error.
    EXPECT_TRUE(analyzed_status(*_reader, execution, InvertedIndexQueryType::MATCH_ANY_QUERY,
                                terms({}), &bitmap)
                        .is<ErrorCode::INVERTED_INDEX_NO_TERMS>());

    // A slot holding several terms matches any of them at its position: "alpha" followed by
    // "beta" or "betamax".
    InvertedIndexQueryInfo synonyms = terms({"alpha"});
    TermInfo slot;
    slot.term = std::vector<std::string> {"beta", "betamax"};
    slot.position = 2;
    synonyms.term_infos.push_back(std::move(slot));
    QueryExecution synonym_run;
    EXPECT_EQ(analyzed(synonym_run, InvertedIndexQueryType::MATCH_PHRASE_QUERY, synonyms),
              docids_where(has_beta_prefix));
}

// A source bound the way SEARCH binds its leaves reports the PRX frames its cursors decode to the
// query's statistics when its opened index closes.
TEST_F(SniiIndexReaderAnalyzedQueryTest, BoundSourceReportsItsPrxDecodes) {
    QueryExecution execution;
    std::unique_ptr<OpenedIndex> opened;
    index_query::IndexSourcePtr source;
    assert_ok(_reader->open_source(execution.context, L"content", &opened, &source));
    std::unique_ptr<index_query::PostingsCursor> cursor;
    assert_ok(source->open_term("beta", /*positions=*/true, /*scoring=*/false, &cursor));
    ASSERT_NE(cursor, nullptr);
    index_query::PostingsBlock block;
    bool eof = false;
    assert_ok(cursor->next_block(&block, &eof));
    ASSERT_FALSE(eof);
    std::vector<uint32_t> positions;
    assert_ok(cursor->append_positions(0, 0, positions));
    EXPECT_EQ(positions, (std::vector<uint32_t> {1}));
    cursor.reset();
    source.reset();
    EXPECT_EQ(execution.stats.snii_stats.prx_total_docs, 0);

    opened.reset();
    const auto& stats = execution.stats.snii_stats;
    EXPECT_GT(stats.prx_raw_frames + stats.prx_zstd_frames + stats.prx_pfor_frames, 0);
    EXPECT_GT(stats.prx_plaintext_bytes, 0);
    EXPECT_GT(stats.prx_total_docs, 0);
    EXPECT_EQ(stats.prx_selected_docs, stats.prx_total_docs);
}

TEST_F(SniiIndexReaderAnalyzedQueryTest, StringTypeReaderSkipsATermLongerThanIgnoreAbove) {
    auto reader = open_reader(phrase_segment_input(), "keyword_segment", _keyword_meta,
                              InvertedIndexReaderType::STRING_TYPE);
    QueryExecution skipped;
    std::shared_ptr<roaring::Roaring> bitmap;
    EXPECT_TRUE(analyzed_status(*reader, skipped, InvertedIndexQueryType::EQUAL_QUERY,
                                terms({"alpha"}), &bitmap)
                        .is<ErrorCode::INVERTED_INDEX_EVALUATE_SKIPPED>());
    QueryExecution short_term;
    assert_ok(analyzed_status(*reader, short_term, InvertedIndexQueryType::EQUAL_QUERY,
                              terms({"ab"}), &bitmap));
    ASSERT_NE(bitmap, nullptr);
    EXPECT_TRUE(bitmap->isEmpty());
}

TEST_F(SniiIndexReaderAnalyzedQueryTest, ResultCacheIsKeyedApartFromRawQueries) {
    QueryExecution raw_run;
    raw(raw_run, "alpha", InvertedIndexQueryType::MATCH_ANY_QUERY);
    EXPECT_EQ(raw_run.stats.inverted_index_query_cache_miss, 1);
    EXPECT_EQ(raw_run.stats.inverted_index_query_cache_insert, 1);

    QueryExecution first;
    analyzed(first, InvertedIndexQueryType::MATCH_ANY_QUERY, terms({"alpha"}));
    EXPECT_EQ(first.stats.inverted_index_query_cache_hit, 0)
            << "an analyzed query never reads a raw query's entry";
    EXPECT_EQ(first.stats.inverted_index_query_cache_miss, 1);
    EXPECT_EQ(first.stats.inverted_index_query_cache_insert, 1);

    QueryExecution second;
    EXPECT_EQ(analyzed(second, InvertedIndexQueryType::MATCH_ANY_QUERY, terms({"alpha"})),
              docids_where(every_doc));
    EXPECT_EQ(second.stats.inverted_index_query_cache_hit, 1);
}

TEST_F(SniiIndexReaderAnalyzedQueryTest, CountOnlyFastPathAnswersASingleTerm) {
    QueryExecution execution;
    execution.context->count_on_index_fastpath = true;
    std::shared_ptr<roaring::Roaring> bitmap;
    assert_ok(analyzed_status(*_reader, execution, InvertedIndexQueryType::MATCH_ANY_QUERY,
                              terms({"beta"}), &bitmap));
    ASSERT_NE(bitmap, nullptr);
    EXPECT_TRUE(execution.context->count_on_index_fastpath_hit);
    EXPECT_EQ(bitmap->cardinality(), docids_where(is_even).size());
}

TEST_F(SniiIndexReaderAnalyzedQueryTest, ScoresReachTheCollectionSimilarity) {
    auto reader = open_reader(scored_segment_input(), "scored_segment", _text_meta,
                              InvertedIndexReaderType::FULLTEXT);
    const auto scored = [&](auto&& query) {
        QueryExecution execution;
        execution.context->collection_statistics = std::make_shared<FixedCollectionStatistics>();
        execution.context->collection_similarity = std::make_shared<CollectionSimilarity>();
        query(execution);
        return score_for_doc(*execution.context->collection_similarity, 0);
    };
    const float raw_score = scored([&](QueryExecution& execution) {
        EXPECT_EQ(raw(*reader, execution, "beta", InvertedIndexQueryType::MATCH_ANY_QUERY),
                  docids_where(is_even));
    });
    const float analyzed_score = scored([&](QueryExecution& execution) {
        EXPECT_EQ(analyzed(*reader, execution, InvertedIndexQueryType::MATCH_ANY_QUERY,
                           terms({"beta"})),
                  docids_where(is_even));
    });
    EXPECT_GT(analyzed_score, 0.0F);
    EXPECT_FLOAT_EQ(analyzed_score, raw_score);
}

} // namespace
} // namespace doris::segment_v2
