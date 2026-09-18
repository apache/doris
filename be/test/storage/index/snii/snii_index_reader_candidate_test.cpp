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

#include <atomic>
#include <cstdint>
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
#include "storage/index/inverted/analyzer/custom_analyzer.h"
#include "storage/index/inverted/inverted_index_cache.h"
#include "storage/index/inverted/inverted_index_desc.h"
#include "storage/index/inverted/similarity/collection_statistics.h"
#include "storage/index/snii/io/local_file.h"
#include "storage/index/snii/query/bm25_scorer.h"
#include "storage/index/snii/snii_index_reader.h"
#include "storage/index/snii/writer/snii_compound_writer.h"
#include "storage/index/snii_query_test_util.h"
#include "storage/olap_common.h"
#include "storage/tablet/tablet_schema.h"

// SniiIndexReader side of the candidate-pushdown handshake: multi-term phrase
// queries consume IndexQueryContext::candidate_rows, report it through
// candidate_rows_consumed, and keep the partial result out of the query cache
// and out of single-flight; every other query computes the full segment.
namespace doris::segment_v2 {
namespace {

using namespace doris::snii::snii_test;

constexpr int64_t kIndexId = 41;
constexpr const char* kTestDir = "./ut_dir/snii_index_reader_candidate_test";
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

void init_index_meta(TabletIndex* meta) {
    TabletIndexPB pb;
    pb.set_index_type(IndexType::INVERTED);
    pb.set_index_id(kIndexId);
    pb.set_index_name("candidate_idx");
    pb.add_col_unique_id(0);
    pb.mutable_properties()->insert({"parser", "english"});
    pb.mutable_properties()->insert({"lower_case", "true"});
    pb.mutable_properties()->insert({"support_phrase", "true"});
    meta->init_from_pb(pb);
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
// docs divisible by three, so the phrase, the phrase prefix and MATCH_ANY all
// have distinct full-segment answers.
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

bool is_even(uint32_t docid) {
    return docid % 2 == 0;
}

bool has_beta_prefix(uint32_t docid) {
    return docid % 2 == 0 || docid % 3 == 0;
}

void counting_observer(void* opaque) noexcept {
    static_cast<std::atomic<uint32_t>*>(opaque)->fetch_add(1, std::memory_order_relaxed);
}

class SniiIndexReaderCandidateTest : public testing::Test {
protected:
    void SetUp() override {
        assert_ok(io::global_local_filesystem()->delete_directory(kTestDir));
        assert_ok(io::global_local_filesystem()->create_directory(kTestDir));
        init_index_meta(&_meta);
        _previous_query_cache = ExecEnv::GetInstance()->get_inverted_index_query_cache();
        _query_cache.reset(InvertedIndexQueryCache::create_global_cache(1024 * 1024, 1));
        ExecEnv::GetInstance()->set_inverted_index_query_cache(_query_cache.get());
        _reader = open_reader(phrase_segment_input(), "phrase_segment");
        _candidates.addMany(6, std::vector<uint32_t> {2, 3, 4, 5, 9, 40}.data());
    }

    void TearDown() override {
        _reader.reset();
        _file_readers.clear();
        ExecEnv::GetInstance()->set_inverted_index_query_cache(_previous_query_cache);
        _query_cache.reset();
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(kTestDir).ok());
    }

    std::shared_ptr<SniiIndexReader> open_reader(doris::snii::writer::SniiIndexInput input,
                                                 std::string_view name) {
        const std::string path_prefix = std::string(kTestDir) + "/" + std::string(name);
        const uint32_t doc_count = input.doc_count;
        assert_ok(write_segment(path_prefix, std::move(input)));
        auto file_reader = std::make_shared<IndexFileReader>(
                io::global_local_filesystem(), path_prefix, InvertedIndexStorageFormatPB::SNII);
        assert_ok(file_reader->init());
        _file_readers.push_back(file_reader);
        return SniiIndexReader::create_shared(&_meta, file_reader,
                                              InvertedIndexReaderType::FULLTEXT, doc_count,
                                              /*column_is_array=*/false);
    }

    std::vector<uint32_t> run(QueryExecution& execution, std::string text,
                              InvertedIndexQueryType query_type,
                              const InvertedIndexAnalyzerCtx* analyzer_ctx = nullptr) {
        std::shared_ptr<roaring::Roaring> bitmap;
        const Field value = Field::create_field<TYPE_STRING>(std::move(text));
        assert_ok(_reader->query(execution.context, "content", value, query_type, bitmap,
                                 analyzer_ctx));
        DORIS_CHECK(bitmap != nullptr);
        return docids_of(*bitmap);
    }

    std::vector<uint32_t> restricted(const std::vector<uint32_t>& docids) const {
        std::vector<uint32_t> out;
        for (uint32_t docid : docids) {
            if (_candidates.contains(docid)) {
                out.push_back(docid);
            }
        }
        return out;
    }

    TabletIndex _meta;
    std::vector<std::shared_ptr<IndexFileReader>> _file_readers;
    std::shared_ptr<SniiIndexReader> _reader;
    roaring::Roaring _candidates;
    InvertedIndexQueryCache* _previous_query_cache = nullptr;
    std::unique_ptr<InvertedIndexQueryCache> _query_cache;
};

TEST_F(SniiIndexReaderCandidateTest, PhraseConsumesCandidateAndStaysOutOfCache) {
    QueryExecution restricted_run;
    restricted_run.context->candidate_rows = &_candidates;
    EXPECT_EQ(run(restricted_run, "alpha beta", InvertedIndexQueryType::MATCH_PHRASE_QUERY),
              restricted(docids_where(is_even)));
    EXPECT_TRUE(restricted_run.context->candidate_rows_consumed);
    EXPECT_EQ(restricted_run.stats.inverted_index_query_cache_miss, 1);
    EXPECT_EQ(restricted_run.stats.inverted_index_query_cache_insert, 0)
            << "a candidate-restricted bitmap is partial and must not be cached";

    QueryExecution full_run;
    EXPECT_EQ(run(full_run, "alpha beta", InvertedIndexQueryType::MATCH_PHRASE_QUERY),
              docids_where(is_even));
    EXPECT_FALSE(full_run.context->candidate_rows_consumed);
    EXPECT_EQ(full_run.stats.inverted_index_query_cache_hit, 0);
    EXPECT_EQ(full_run.stats.inverted_index_query_cache_insert, 1);
}

TEST_F(SniiIndexReaderCandidateTest, PhrasePrefixConsumesCandidate) {
    QueryExecution restricted_run;
    restricted_run.context->candidate_rows = &_candidates;
    EXPECT_EQ(run(restricted_run, "alpha bet", InvertedIndexQueryType::MATCH_PHRASE_PREFIX_QUERY),
              restricted(docids_where(has_beta_prefix)));
    EXPECT_TRUE(restricted_run.context->candidate_rows_consumed);
    EXPECT_EQ(restricted_run.stats.inverted_index_query_cache_insert, 0);

    QueryExecution full_run;
    EXPECT_EQ(run(full_run, "alpha bet", InvertedIndexQueryType::MATCH_PHRASE_PREFIX_QUERY),
              docids_where(has_beta_prefix));
    EXPECT_EQ(full_run.stats.inverted_index_query_cache_hit, 0);
}

// Queries without per-document position verification ignore the candidate:
// they compute the full segment, report no consumption and stay cacheable.
TEST_F(SniiIndexReaderCandidateTest, NonPhraseAndSingleTermQueriesIgnoreCandidate) {
    const std::vector<std::pair<std::string, InvertedIndexQueryType>> cases = {
            {"alpha beta", InvertedIndexQueryType::MATCH_ANY_QUERY},
            {"alpha beta", InvertedIndexQueryType::MATCH_ALL_QUERY},
            {"beta", InvertedIndexQueryType::MATCH_PHRASE_QUERY},
            {"bet", InvertedIndexQueryType::MATCH_PHRASE_PREFIX_QUERY}};
    for (const auto& [text, query_type] : cases) {
        QueryExecution candidate_run;
        candidate_run.context->candidate_rows = &_candidates;
        const std::vector<uint32_t> with_candidate = run(candidate_run, text, query_type);
        EXPECT_FALSE(candidate_run.context->candidate_rows_consumed) << text;
        EXPECT_EQ(candidate_run.stats.inverted_index_query_cache_insert, 1) << text;

        QueryExecution cached_run;
        EXPECT_EQ(run(cached_run, text, query_type), with_candidate) << text;
        EXPECT_EQ(cached_run.stats.inverted_index_query_cache_hit, 1) << text;
    }
}

// A cached full-segment bitmap is still a correct answer under a candidate:
// the scan intersects it afterwards. The cache hit reports no consumption.
TEST_F(SniiIndexReaderCandidateTest, CachedFullResultServesCandidateQuery) {
    QueryExecution full_run;
    const std::vector<uint32_t> full =
            run(full_run, "alpha beta", InvertedIndexQueryType::MATCH_PHRASE_QUERY);

    QueryExecution candidate_run;
    candidate_run.context->candidate_rows = &_candidates;
    EXPECT_EQ(run(candidate_run, "alpha beta", InvertedIndexQueryType::MATCH_PHRASE_QUERY), full);
    EXPECT_EQ(candidate_run.stats.inverted_index_query_cache_hit, 1);
    EXPECT_FALSE(candidate_run.context->candidate_rows_consumed);
}

TEST_F(SniiIndexReaderCandidateTest, ConsumedFlagIsResetBeforeEachQuery) {
    QueryExecution execution;
    execution.context->candidate_rows = &_candidates;
    execution.context->candidate_rows_consumed = true;
    run(execution, "alpha beta", InvertedIndexQueryType::MATCH_ANY_QUERY);
    EXPECT_FALSE(execution.context->candidate_rows_consumed)
            << "a consumed flag left by an earlier search must not leak into this one";
}

// Single-flight followers reuse the leader's bitmap for the same full-segment
// query, so a candidate-restricted computation must never lead or join a flight.
TEST_F(SniiIndexReaderCandidateTest, CandidateQueryBypassesSingleFlight) {
    std::atomic<uint32_t> leader_computes {0};
    _reader->set_single_flight_leader_before_compute_observer_for_test(counting_observer,
                                                                       &leader_computes);
    QueryExecution restricted_run(/*enable_query_cache=*/false);
    restricted_run.context->candidate_rows = &_candidates;
    run(restricted_run, "alpha beta", InvertedIndexQueryType::MATCH_PHRASE_QUERY);
    EXPECT_EQ(leader_computes.load(std::memory_order_relaxed), 0);

    QueryExecution full_run(/*enable_query_cache=*/false);
    run(full_run, "alpha beta", InvertedIndexQueryType::MATCH_PHRASE_QUERY);
    EXPECT_EQ(leader_computes.load(std::memory_order_relaxed), 1);
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
    DORIS_CHECK_EQ(row_ids->size(), 1);
    const auto& nullable = assert_cast<const ColumnNullable&>(*scores);
    return assert_cast<const ColumnFloat32&>(nullable.get_nested_column()).get_data()[0];
}

// Scoring phrases skip the cache and single-flight already; under a candidate
// they score exactly the candidate matches with their full-segment scores.
TEST_F(SniiIndexReaderCandidateTest, ScoringPhraseScoresOnlyCandidateMatches) {
    doris::snii::writer::SniiIndexInput input;
    input.doc_count = 3;
    input.encoded_norms = {doris::snii::query::encode_norm(2), doris::snii::query::encode_norm(4),
                           doris::snii::query::encode_norm(3)};
    input.terms = {make_term("alpha", {{.docid = 0, .positions = {0}},
                                       {.docid = 1, .positions = {0, 2}},
                                       {.docid = 2, .positions = {1}}}),
                   make_term("beta", {{.docid = 0, .positions = {1}},
                                      {.docid = 1, .positions = {1, 3}},
                                      {.docid = 2, .positions = {0, 2}}})};
    _reader = open_reader(std::move(input), "scoring_segment");

    inverted_index::Settings tokenizer_settings;
    tokenizer_settings.set("tokenize_on_chars", "[whitespace]");
    inverted_index::CustomAnalyzerConfig::Builder builder;
    builder.with_tokenizer_config("char_group", tokenizer_settings);
    builder.add_token_filter_config("lowercase", {});
    InvertedIndexAnalyzerCtx analyzer_ctx;
    analyzer_ctx.parser_type = InvertedIndexParserType::PARSER_ENGLISH;
    analyzer_ctx.analyzer_provider =
            std::make_shared<inverted_index::CustomAnalyzerProvider>(builder.build());

    auto scoring_execution = [](QueryExecution* execution) {
        execution->context->collection_statistics = std::make_shared<FixedCollectionStatistics>();
        execution->context->collection_similarity = std::make_shared<CollectionSimilarity>();
    };
    QueryExecution full_run;
    scoring_execution(&full_run);
    EXPECT_EQ(
            run(full_run, "alpha beta", InvertedIndexQueryType::MATCH_PHRASE_QUERY, &analyzer_ctx),
            (std::vector<uint32_t> {0, 1, 2}));

    roaring::Roaring candidates;
    candidates.add(1);
    candidates.add(2);
    QueryExecution restricted_run;
    scoring_execution(&restricted_run);
    restricted_run.context->candidate_rows = &candidates;
    EXPECT_EQ(run(restricted_run, "alpha beta", InvertedIndexQueryType::MATCH_PHRASE_QUERY,
                  &analyzer_ctx),
              (std::vector<uint32_t> {1, 2}));
    EXPECT_TRUE(restricted_run.context->candidate_rows_consumed);
    for (uint32_t docid : {1U, 2U}) {
        EXPECT_FLOAT_EQ(score_for_doc(*restricted_run.context->collection_similarity, docid),
                        score_for_doc(*full_run.context->collection_similarity, docid))
                << docid;
    }
}

} // namespace
} // namespace doris::segment_v2
