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
#include <gtest/gtest.h>

#include <atomic>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "io/fs/local_file_system.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "storage/compaction/collection_similarity.h"
#include "storage/index/index_file_reader.h"
#include "storage/index/inverted/analyzer/analyzer.h"
#include "storage/index/inverted/analyzer/analyzer_provider.h"
#include "storage/index/inverted/analyzer/custom_analyzer.h"
#include "storage/index/inverted/common_grams/common_grams_key_codec.h"
#include "storage/index/inverted/inverted_index_cache.h"
#include "storage/index/inverted/inverted_index_reader.h"
#include "storage/index/inverted/token_filter/common_grams_filter.h"
#include "storage/index/snii/snii_index_reader.h"
#include "storage/tablet/tablet_schema.h"
#include "util/defer_op.h"
#include "util/time.h"

namespace doris::segment_v2 {
namespace {

using inverted_index::AnalysisPurpose;
using inverted_index::AnalyzerProvider;

class RecordingFailingAnalyzerProvider final : public AnalyzerProvider {
public:
    std::shared_ptr<lucene::analysis::Analyzer> get_analyzer(
            AnalysisPurpose purpose) const override {
        purposes.push_back(purpose);
        throw Exception(ErrorCode::INVERTED_INDEX_ANALYZER_ERROR,
                        "forced analyzer provider failure");
    }

    mutable std::vector<AnalysisPurpose> purposes;
};

class IdentityFailingAnalyzerProvider final : public AnalyzerProvider {
public:
    explicit IdentityFailingAnalyzerProvider(inverted_index::CommonGramsQueryIdentity identity)
            : _identity(std::move(identity)) {}

    std::shared_ptr<lucene::analysis::Analyzer> get_analyzer(
            AnalysisPurpose purpose) const override {
        purposes.push_back(purpose);
        throw Exception(ErrorCode::INVERTED_INDEX_ANALYZER_ERROR,
                        "forced analyzer provider failure");
    }

    bool uses_common_grams() const override { return true; }

    const inverted_index::CommonGramsQueryIdentity* common_grams_identity() const override {
        return &_identity;
    }

    mutable std::vector<AnalysisPurpose> purposes;

private:
    inverted_index::CommonGramsQueryIdentity _identity;
};

class PartialFailureTokenStream final : public lucene::analysis::TokenStream {
public:
    explicit PartialFailureTokenStream(std::shared_ptr<std::atomic<uint32_t>> emitted_tokens)
            : _emitted_tokens(std::move(emitted_tokens)) {}

    lucene::analysis::Token* next(lucene::analysis::Token* token) override {
        if (!_emitted) {
            _emitted = true;
            _term = "partial";
            token->clear();
            token->setTextNoCopy(_term.data(), static_cast<int32_t>(_term.size()));
            token->positionIncrement = 1;
            _emitted_tokens->fetch_add(1, std::memory_order_relaxed);
            return token;
        }
        throw Exception(ErrorCode::INVERTED_INDEX_ANALYZER_ERROR,
                        "forced failure after first token");
    }

    void close() override {}
    void reset() override { _emitted = false; }

private:
    std::shared_ptr<std::atomic<uint32_t>> _emitted_tokens;
    bool _emitted = false;
    std::string _term;
};

class PartialFailureAnalyzer final : public lucene::analysis::Analyzer {
public:
    explicit PartialFailureAnalyzer(std::shared_ptr<std::atomic<uint32_t>> emitted_tokens)
            : _emitted_tokens(std::move(emitted_tokens)) {}

    bool isSDocOpt() override { return true; }

    lucene::analysis::TokenStream* tokenStream(const TCHAR*, lucene::util::Reader*) override {
        return new PartialFailureTokenStream(_emitted_tokens);
    }

    lucene::analysis::TokenStream* reusableTokenStream(const TCHAR*,
                                                       lucene::util::Reader*) override {
        _reusable = std::make_unique<PartialFailureTokenStream>(_emitted_tokens);
        return _reusable.get();
    }

    lucene::analysis::TokenStream* tokenStream(const TCHAR*,
                                               const inverted_index::ReaderPtr&) override {
        return new PartialFailureTokenStream(_emitted_tokens);
    }

    lucene::analysis::TokenStream* reusableTokenStream(const TCHAR*,
                                                       const inverted_index::ReaderPtr&) override {
        _reusable = std::make_unique<PartialFailureTokenStream>(_emitted_tokens);
        return _reusable.get();
    }

private:
    std::shared_ptr<std::atomic<uint32_t>> _emitted_tokens;
    std::unique_ptr<PartialFailureTokenStream> _reusable;
};

class RecordingPartialFailureAnalyzerProvider final : public AnalyzerProvider {
public:
    std::shared_ptr<lucene::analysis::Analyzer> get_analyzer(
            AnalysisPurpose purpose) const override {
        purposes.push_back(purpose);
        return std::make_shared<PartialFailureAnalyzer>(emitted_tokens);
    }

    mutable std::vector<AnalysisPurpose> purposes;
    std::shared_ptr<std::atomic<uint32_t>> emitted_tokens =
            std::make_shared<std::atomic<uint32_t>>(0);
};

class GeneratedTokenStream final : public lucene::analysis::TokenStream {
public:
    GeneratedTokenStream(std::string term, bool mark_common_gram)
            : _term(std::move(term)), _mark_common_gram(mark_common_gram) {}

    lucene::analysis::Token* next(lucene::analysis::Token* token) override {
        if (_emitted) {
            return nullptr;
        }
        _emitted = true;
        token->clear();
        token->setTextNoCopy(_term.data(), static_cast<int32_t>(_term.size()));
        token->setPositionIncrement(1);
        if (_mark_common_gram) {
            token->setType(inverted_index::COMMON_GRAM_TOKEN_TYPE);
        }
        return token;
    }

    void close() override {}
    void reset() override { _emitted = false; }

private:
    std::string _term;
    bool _mark_common_gram = false;
    bool _emitted = false;
};

class GeneratedTokenAnalyzer final : public lucene::analysis::Analyzer {
public:
    GeneratedTokenAnalyzer(std::string term, bool mark_common_gram)
            : _term(std::move(term)), _mark_common_gram(mark_common_gram) {}

    bool isSDocOpt() override { return true; }

    lucene::analysis::TokenStream* tokenStream(const TCHAR*, lucene::util::Reader*) override {
        return new GeneratedTokenStream(_term, _mark_common_gram);
    }

    lucene::analysis::TokenStream* reusableTokenStream(const TCHAR*,
                                                       lucene::util::Reader*) override {
        _reusable = std::make_unique<GeneratedTokenStream>(_term, _mark_common_gram);
        return _reusable.get();
    }

    lucene::analysis::TokenStream* tokenStream(const TCHAR*,
                                               const inverted_index::ReaderPtr&) override {
        return new GeneratedTokenStream(_term, _mark_common_gram);
    }

    lucene::analysis::TokenStream* reusableTokenStream(const TCHAR*,
                                                       const inverted_index::ReaderPtr&) override {
        _reusable = std::make_unique<GeneratedTokenStream>(_term, _mark_common_gram);
        return _reusable.get();
    }

private:
    std::string _term;
    bool _mark_common_gram = false;
    std::unique_ptr<GeneratedTokenStream> _reusable;
};

class GeneratedGramAnalyzerProvider final : public AnalyzerProvider {
public:
    GeneratedGramAnalyzerProvider()
            : _gram(*inverted_index::encode_common_gram("the", "history")),
              _identity {.common_grams_dictionary_identity = "builtin-stopwords:v1",
                         .base_analyzer_fingerprint = "base:v1",
                         .common_grams_fingerprint = "grams:v1"} {}

    std::shared_ptr<lucene::analysis::Analyzer> get_analyzer(
            AnalysisPurpose purpose) const override {
        purposes.push_back(purpose);
        return std::make_shared<GeneratedTokenAnalyzer>(_gram, true);
    }

    bool uses_common_grams() const override { return true; }
    const inverted_index::CommonGramsQueryIdentity* common_grams_identity() const override {
        return &_identity;
    }

    mutable std::vector<AnalysisPurpose> purposes;

private:
    std::string _gram;
    inverted_index::CommonGramsQueryIdentity _identity;
};

struct QueryExecutionContext {
    explicit QueryExecutionContext(bool scoring) {
        TQueryOptions query_options;
        query_options.enable_inverted_index_query_cache = true;
        query_options.enable_inverted_index_searcher_cache = true;
        runtime_state.set_query_options(query_options);
        context->io_ctx = &io_ctx;
        context->stats = &stats;
        context->runtime_state = &runtime_state;
        if (scoring) {
            context->collection_similarity = std::make_shared<CollectionSimilarity>();
        }
    }

    OlapReaderStatistics stats;
    io::IOContext io_ctx;
    RuntimeState runtime_state;
    IndexQueryContextPtr context = std::make_shared<IndexQueryContext>();
};

class InvertedIndexReaderAnalysisPurposeTest : public testing::Test {
protected:
    void SetUp() override {
        _previous_searcher_cache = ExecEnv::GetInstance()->get_inverted_index_searcher_cache();
        _previous_query_cache = ExecEnv::GetInstance()->get_inverted_index_query_cache();
        _searcher_cache.reset(InvertedIndexSearcherCache::create_global_instance(1024 * 1024, 1));
        _query_cache.reset(InvertedIndexQueryCache::create_global_cache(1024 * 1024, 1));
        ExecEnv::GetInstance()->set_inverted_index_searcher_cache(_searcher_cache.get());
        ExecEnv::GetInstance()->set_inverted_index_query_cache(_query_cache.get());

        TabletIndexPB pb;
        pb.set_index_type(IndexType::INVERTED);
        pb.set_index_id(73);
        pb.set_index_name("analysis_purpose_idx");
        pb.add_col_unique_id(0);
        pb.mutable_properties()->insert({"parser", "english"});
        pb.mutable_properties()->insert({"lower_case", "true"});
        pb.mutable_properties()->insert({"support_phrase", "true"});
        _meta.init_from_pb(pb);

        _snii_file_reader = std::make_shared<IndexFileReader>(
                io::global_local_filesystem(), "./ut_dir/missing_snii_analysis_purpose",
                InvertedIndexStorageFormatPB::SNII);
        _snii_reader = SniiIndexReader::create_shared(&_meta, _snii_file_reader,
                                                      InvertedIndexReaderType::FULLTEXT);
    }

    void TearDown() override {
        _snii_reader.reset();
        _snii_file_reader.reset();
        ExecEnv::GetInstance()->set_inverted_index_searcher_cache(_previous_searcher_cache);
        ExecEnv::GetInstance()->set_inverted_index_query_cache(_previous_query_cache);
        _searcher_cache.reset();
        _query_cache.reset();
    }

    // This entry is admission-only: tests must exit during analysis before dereferencing the
    // SNII postings reader.
    void preload_legacy_searcher_cache_entries() {
        const InvertedIndexSearcherCache::CacheKey snii_key(
                _snii_file_reader->get_index_file_cache_key(&_meta));
        _searcher_cache->insert(
                snii_key, new InvertedIndexSearcherCache::CacheValue(
                                  std::make_unique<doris::snii::reader::LogicalIndexReader>(), 1,
                                  UnixMillis(), _snii_file_reader));
    }

    template <typename Reader, typename Provider>
    void expect_analysis_failure_after_segment_admission(const std::shared_ptr<Reader>& reader,
                                                         InvertedIndexQueryType query_type,
                                                         std::string query, bool scoring,
                                                         AnalysisPurpose expected_purpose,
                                                         const std::shared_ptr<Provider>& provider,
                                                         int64_t expected_query_cache_lookups = 0,
                                                         int64_t expected_searcher_cache_hits = 1) {
        QueryExecutionContext execution(scoring);
        InvertedIndexAnalyzerCtx analyzer_ctx;
        analyzer_ctx.parser_type = InvertedIndexParserType::PARSER_ENGLISH;
        analyzer_ctx.analyzer_provider = provider;

        auto original_bitmap = std::make_shared<roaring::Roaring>();
        original_bitmap->add(999);
        std::shared_ptr<roaring::Roaring> bitmap = original_bitmap;
        const Field query_value = Field::create_field<TYPE_STRING>(std::move(query));

        Status status;
        EXPECT_NO_THROW(status = reader->query(execution.context, "content", query_value,
                                               query_type, bitmap, &analyzer_ctx));
        EXPECT_EQ(status.code(), ErrorCode::INVERTED_INDEX_ANALYZER_ERROR) << status;
        EXPECT_EQ(provider->purposes, (std::vector<AnalysisPurpose> {expected_purpose}));
        EXPECT_EQ(bitmap, original_bitmap);
        EXPECT_EQ(bitmap->cardinality(), 1);
        EXPECT_TRUE(bitmap->contains(999));
        EXPECT_EQ(execution.stats.inverted_index_query_cache_hit, 0);
        EXPECT_EQ(execution.stats.inverted_index_query_cache_miss, expected_query_cache_lookups);
        EXPECT_EQ(execution.stats.inverted_index_query_cache_lookup, expected_query_cache_lookups);
        EXPECT_EQ(execution.stats.inverted_index_query_cache_insert, 0);
        EXPECT_EQ(execution.stats.inverted_index_searcher_cache_hit, expected_searcher_cache_hits);
        EXPECT_EQ(execution.stats.inverted_index_searcher_cache_miss, 0);
    }

    template <typename Reader>
    void expect_provider_failure_after_segment_admission(const std::shared_ptr<Reader>& reader,
                                                         InvertedIndexQueryType query_type,
                                                         std::string query, bool scoring,
                                                         AnalysisPurpose expected_purpose,
                                                         int64_t expected_query_cache_lookups = 0,
                                                         int64_t expected_searcher_cache_hits = 1) {
        expect_analysis_failure_after_segment_admission(
                reader, query_type, std::move(query), scoring, expected_purpose,
                std::make_shared<RecordingFailingAnalyzerProvider>(), expected_query_cache_lookups,
                expected_searcher_cache_hits);
    }

    template <typename Reader>
    void expect_generated_gram_bypass_after_segment_admission(
            const std::shared_ptr<Reader>& reader) {
        QueryExecutionContext execution(/*scoring=*/false);
        auto provider = std::make_shared<GeneratedGramAnalyzerProvider>();
        InvertedIndexAnalyzerCtx analyzer_ctx;
        analyzer_ctx.parser_type = InvertedIndexParserType::PARSER_ENGLISH;
        analyzer_ctx.analyzer_provider = provider;
        auto original_bitmap = std::make_shared<roaring::Roaring>();
        original_bitmap->add(999);
        std::shared_ptr<roaring::Roaring> bitmap = original_bitmap;
        const Field query_value = Field::create_field<TYPE_STRING>("the history");

        const Status status =
                reader->query(execution.context, "content", query_value,
                              InvertedIndexQueryType::MATCH_PHRASE_QUERY, bitmap, &analyzer_ctx);
        EXPECT_EQ(status.code(), ErrorCode::INVERTED_INDEX_BYPASS) << status;
        EXPECT_EQ(provider->purposes,
                  (std::vector<AnalysisPurpose> {AnalysisPurpose::kPlainQuery}));
        EXPECT_EQ(bitmap, original_bitmap);
        EXPECT_EQ(execution.stats.inverted_index_query_cache_hit, 0);
        EXPECT_EQ(execution.stats.inverted_index_query_cache_miss, 1);
        EXPECT_EQ(execution.stats.inverted_index_query_cache_lookup, 1);
        EXPECT_EQ(execution.stats.inverted_index_query_cache_insert, 0);
        EXPECT_EQ(execution.stats.inverted_index_searcher_cache_hit, 1);
        EXPECT_EQ(execution.stats.inverted_index_searcher_cache_miss, 0);
    }

    template <typename Reader>
    void expect_raw_query_bypasses_analyzer(const std::shared_ptr<Reader>& reader,
                                            InvertedIndexQueryType query_type, std::string query) {
        QueryExecutionContext execution(/*scoring=*/false);
        auto provider = std::make_shared<RecordingFailingAnalyzerProvider>();
        InvertedIndexAnalyzerCtx analyzer_ctx;
        analyzer_ctx.parser_type = InvertedIndexParserType::PARSER_ENGLISH;
        analyzer_ctx.analyzer_provider = provider;
        std::shared_ptr<roaring::Roaring> bitmap;
        const Field query_value = Field::create_field<TYPE_STRING>(std::move(query));

        const Status status = reader->query(execution.context, "content", query_value, query_type,
                                            bitmap, &analyzer_ctx);
        EXPECT_NE(status.code(), ErrorCode::INVERTED_INDEX_ANALYZER_ERROR) << status;
        EXPECT_TRUE(provider->purposes.empty());
    }

    template <typename Reader, typename Provider>
    void expect_raw_cache_hit_before_analysis(const std::shared_ptr<Reader>& reader,
                                              const std::shared_ptr<IndexFileReader>& file_reader,
                                              const std::shared_ptr<Provider>& provider,
                                              uint64_t common_grams_cache_generation) {
        QueryExecutionContext execution(/*scoring=*/false);
        InvertedIndexAnalyzerCtx analyzer_ctx;
        analyzer_ctx.parser_type = InvertedIndexParserType::PARSER_ENGLISH;
        analyzer_ctx.analyzer_provider = provider;

        const std::string raw_query = "the history";
        const InvertedIndexRawQuerySemantic semantic {
                .raw_query_bytes = raw_query,
                .query_type = InvertedIndexQueryType::MATCH_PHRASE_QUERY,
                .slop = 0,
                .ordered = false,
                .max_expansions =
                        execution.runtime_state.query_options().inverted_index_max_expansions,
                .common_grams_cache_generation = common_grams_cache_generation};
        const InvertedIndexQueryCache::CacheKey key {
                file_reader->get_index_file_cache_key(&_meta), "content",
                InvertedIndexQueryType::MATCH_PHRASE_QUERY, semantic.encode()};
        auto cached = std::make_shared<roaring::Roaring>();
        cached->add(7);
        InvertedIndexQueryCacheHandle insert_handle;
        _query_cache->insert(key, cached, &insert_handle);

        std::shared_ptr<roaring::Roaring> bitmap;
        const Field query_value = Field::create_field<TYPE_STRING>(raw_query);
        const Status status =
                reader->query(execution.context, "content", query_value,
                              InvertedIndexQueryType::MATCH_PHRASE_QUERY, bitmap, &analyzer_ctx);
        EXPECT_TRUE(status.ok()) << status;
        EXPECT_TRUE(provider->purposes.empty());
        ASSERT_NE(bitmap, nullptr);
        EXPECT_EQ(bitmap->cardinality(), 1);
        EXPECT_TRUE(bitmap->contains(7));
        EXPECT_EQ(execution.stats.inverted_index_query_cache_hit, 1);
        EXPECT_EQ(execution.stats.inverted_index_query_cache_miss, 0);
        EXPECT_EQ(execution.stats.inverted_index_query_cache_lookup, 1);
        EXPECT_EQ(execution.stats.inverted_index_query_cache_insert, 0);
        EXPECT_EQ(execution.stats.inverted_index_searcher_cache_hit, 0);
        EXPECT_EQ(execution.stats.inverted_index_searcher_cache_miss, 0);
    }

    InvertedIndexSearcherCache* _previous_searcher_cache = nullptr;
    InvertedIndexQueryCache* _previous_query_cache = nullptr;
    std::unique_ptr<InvertedIndexSearcherCache> _searcher_cache;
    std::unique_ptr<InvertedIndexQueryCache> _query_cache;
    TabletIndex _meta;
    std::shared_ptr<IndexFileReader> _snii_file_reader;
    std::shared_ptr<SniiIndexReader> _snii_reader;
};

TEST(InvertedIndexRawQuerySemanticTest, EncodesOnlyRawSemanticDimensionsWithoutDelimiters) {
    const std::string raw_query("a/b\0c", 5);
    InvertedIndexRawQuerySemantic base {.raw_query_bytes = raw_query,
                                        .query_type = InvertedIndexQueryType::MATCH_PHRASE_QUERY,
                                        .slop = 2,
                                        .ordered = true,
                                        .max_expansions = 50,
                                        .cache_semantics_version = 3,
                                        .common_grams_cache_generation = 7};
    const std::string encoded = base.encode();
    constexpr size_t kFixedEncodedBytes = sizeof(uint32_t) + sizeof(uint64_t) + sizeof(uint32_t) +
                                          sizeof(uint32_t) + sizeof(uint8_t) + sizeof(uint32_t) +
                                          sizeof(uint64_t);
    EXPECT_EQ(encoded.size(), kFixedEncodedBytes + raw_query.size());

    auto changed = base;
    changed.raw_query_bytes = std::string_view(raw_query).substr(0, 3);
    EXPECT_NE(changed.encode(), encoded);
    changed = base;
    changed.query_type = InvertedIndexQueryType::MATCH_PHRASE_PREFIX_QUERY;
    EXPECT_NE(changed.encode(), encoded);
    changed = base;
    changed.slop = 3;
    EXPECT_NE(changed.encode(), encoded);
    changed = base;
    changed.ordered = false;
    EXPECT_NE(changed.encode(), encoded);
    changed = base;
    changed.max_expansions = 51;
    EXPECT_NE(changed.encode(), encoded);
    changed = base;
    changed.cache_semantics_version = 4;
    EXPECT_NE(changed.encode(), encoded);
    changed = base;
    changed.common_grams_cache_generation = 8;
    EXPECT_NE(changed.encode(), encoded);
}

TEST(InvertedIndexRawQuerySemanticTest, CacheEnvelopeSeparatesSlashAndNulBoundaries) {
    const InvertedIndexQueryCache::CacheKey slash_left {
            io::Path("a/b"), "c", InvertedIndexQueryType::MATCH_PHRASE_QUERY, "d"};
    const InvertedIndexQueryCache::CacheKey slash_right {
            io::Path("a"), "b/c", InvertedIndexQueryType::MATCH_PHRASE_QUERY, "d"};
    EXPECT_NE(slash_left.encode(), slash_right.encode());

    const InvertedIndexQueryCache::CacheKey nul_left {
            io::Path("a"), std::string("b\0c", 3), InvertedIndexQueryType::MATCH_PHRASE_QUERY, "d"};
    const InvertedIndexQueryCache::CacheKey nul_right {
            io::Path(std::string("a\0b", 3)), "c", InvertedIndexQueryType::MATCH_PHRASE_QUERY, "d"};
    EXPECT_NE(nul_left.encode(), nul_right.encode());
}

TEST(InvertedIndexRawQuerySemanticTest, KillSwitchTransitionsAdvanceCacheGeneration) {
    const bool original = config::enable_common_grams_query_plan;
    const auto before = config::common_grams_query_plan_config_snapshot();

    ASSERT_TRUE(config::set_config("enable_common_grams_query_plan", original ? "false" : "true",
                                   /*need_persist=*/false)
                        .ok());
    const auto disabled_or_enabled = config::common_grams_query_plan_config_snapshot();
    ASSERT_TRUE(config::set_config("enable_common_grams_query_plan", original ? "true" : "false",
                                   /*need_persist=*/false)
                        .ok());
    const auto restored = config::common_grams_query_plan_config_snapshot();

    EXPECT_EQ(before.enabled, original);
    EXPECT_EQ(disabled_or_enabled.enabled, !original);
    EXPECT_GT(disabled_or_enabled.cache_generation, before.cache_generation);
    EXPECT_EQ(restored.enabled, original);
    EXPECT_GT(restored.cache_generation, disabled_or_enabled.cache_generation);
}

TEST(InvertedIndexRawQuerySemanticTest, CommonGramsQueryPlanIsDisabledByDefault) {
    EXPECT_FALSE(config::enable_common_grams_query_plan);
    EXPECT_FALSE(config::common_grams_query_plan_config_snapshot().enabled);
}

TEST(InvertedIndexRawQuerySemanticTest, CostModelSnapshotTracksDynamicConfigAsOneVersion) {
    const int32_t original_ratio = config::common_grams_plan_cost_ratio_percent;
    const int32_t original_factor = config::common_grams_position_verify_factor;
    const int32_t changed_ratio = original_ratio == 84 ? 85 : 84;
    const int32_t changed_factor = original_factor == 7 ? 8 : 7;

    ASSERT_TRUE(config::set_config("common_grams_plan_cost_ratio_percent",
                                   std::to_string(changed_ratio))
                        .ok());
    ASSERT_TRUE(config::set_config("common_grams_position_verify_factor",
                                   std::to_string(changed_factor))
                        .ok());
    const auto changed = config::common_grams_query_plan_config_snapshot();
    EXPECT_EQ(changed.plan_cost_ratio_percent, changed_ratio);
    EXPECT_EQ(changed.position_verify_factor, changed_factor);
    EXPECT_EQ(changed.cost_model_generation,
              (static_cast<uint64_t>(static_cast<uint32_t>(changed_factor)) << 32) |
                      static_cast<uint32_t>(changed_ratio));

    ASSERT_TRUE(config::set_config("common_grams_plan_cost_ratio_percent",
                                   std::to_string(original_ratio))
                        .ok());
    ASSERT_TRUE(config::set_config("common_grams_position_verify_factor",
                                   std::to_string(original_factor))
                        .ok());
}

TEST(InvertedIndexRawQuerySemanticTest, InvalidCostModelConfigDoesNotMutatePublishedState) {
    const auto before = config::common_grams_query_plan_config_snapshot();

    for (const auto& [field, value] : std::vector<std::pair<std::string, std::string>> {
                 {"common_grams_plan_cost_ratio_percent", "-1"},
                 {"common_grams_plan_cost_ratio_percent", "101"},
                 {"common_grams_position_verify_factor", "-1"}}) {
        EXPECT_FALSE(config::set_config(field, value).ok());
        const auto after = config::common_grams_query_plan_config_snapshot();
        EXPECT_EQ(after.plan_cost_ratio_percent, before.plan_cost_ratio_percent);
        EXPECT_EQ(after.position_verify_factor, before.position_verify_factor);
        EXPECT_EQ(after.cost_model_generation, before.cost_model_generation);
    }
}

TEST(InvertedIndexAnalyzerCtxTest, UsesProviderCommonGramsIdentityWithoutCopyingIt) {
    auto provider = std::make_shared<GeneratedGramAnalyzerProvider>();
    InvertedIndexAnalyzerCtx analyzer_ctx;
    analyzer_ctx.analyzer_provider = provider;
    EXPECT_EQ(analyzer_ctx.get_common_grams_identity(), provider->common_grams_identity());
}

TEST_F(InvertedIndexReaderAnalysisPurposeTest,
       SniiRawCacheLookupIsIndependentOfRequestAnalyzerIdentity) {
    const bool original = config::enable_common_grams_query_plan;
    Defer restore([original] {
        EXPECT_TRUE(config::set_config("enable_common_grams_query_plan",
                                       original ? "true" : "false", /*need_persist=*/false)
                            .ok());
    });
    ASSERT_TRUE(config::set_config("enable_common_grams_query_plan", "true",
                                   /*need_persist=*/false)
                        .ok());
    const auto safety = config::common_grams_query_plan_config_snapshot();
    const inverted_index::CommonGramsQueryIdentity complete_identity {
            .common_grams_dictionary_identity = "dictionary:complete",
            .base_analyzer_fingerprint = "base:complete",
            .common_grams_fingerprint = "grams:complete"};
    const inverted_index::CommonGramsQueryIdentity empty_identity;
    for (const auto& identity : {complete_identity, empty_identity}) {
        expect_raw_cache_hit_before_analysis(
                _snii_reader, _snii_file_reader,
                std::make_shared<IdentityFailingAnalyzerProvider>(identity),
                safety.cache_generation);
    }
    expect_raw_cache_hit_before_analysis(_snii_reader, _snii_file_reader,
                                         std::make_shared<RecordingFailingAnalyzerProvider>(),
                                         safety.cache_generation);
}

TEST_F(InvertedIndexReaderAnalysisPurposeTest, DisabledResultCacheDoesNotLookupCountOrInsert) {
    QueryExecutionContext execution(/*scoring=*/false);
    TQueryOptions disabled_options;
    disabled_options.enable_inverted_index_query_cache = false;
    disabled_options.enable_inverted_index_searcher_cache = true;
    execution.runtime_state.set_query_options(disabled_options);

    const InvertedIndexQueryCache::CacheKey key {io::Path("disabled-cache"), "content",
                                                 InvertedIndexQueryType::MATCH_PHRASE_QUERY,
                                                 "raw-semantic"};
    auto bitmap = std::make_shared<roaring::Roaring>();
    bitmap->add(3);
    InvertedIndexQueryCacheHandle handle;
    _snii_reader->insert_query_cache(execution.context, _query_cache.get(), key, bitmap, &handle);
    std::shared_ptr<roaring::Roaring> lookup_bitmap;
    EXPECT_FALSE(_snii_reader->handle_query_cache(execution.context, _query_cache.get(), key,
                                                  &handle, lookup_bitmap));
    EXPECT_EQ(execution.stats.inverted_index_query_cache_hit, 0);
    EXPECT_EQ(execution.stats.inverted_index_query_cache_miss, 0);
    EXPECT_EQ(execution.stats.inverted_index_query_cache_lookup, 0);
    EXPECT_EQ(execution.stats.inverted_index_query_cache_insert, 0);

    TQueryOptions enabled_options;
    enabled_options.enable_inverted_index_query_cache = true;
    enabled_options.enable_inverted_index_searcher_cache = true;
    execution.runtime_state.set_query_options(enabled_options);
    EXPECT_FALSE(_snii_reader->handle_query_cache(execution.context, _query_cache.get(), key,
                                                  &handle, lookup_bitmap));
    EXPECT_EQ(execution.stats.inverted_index_query_cache_miss, 1);
    EXPECT_EQ(execution.stats.inverted_index_query_cache_lookup, 1);
}

TEST_F(InvertedIndexReaderAnalysisPurposeTest, SniiSelectsPurposeAfterSegmentAdmission) {
    preload_legacy_searcher_cache_entries();
    expect_provider_failure_after_segment_admission(
            _snii_reader, InvertedIndexQueryType::MATCH_PHRASE_QUERY, "the history", false,
            AnalysisPurpose::kPlainQuery, /*expected_query_cache_lookups=*/1);
    expect_provider_failure_after_segment_admission(
            _snii_reader, InvertedIndexQueryType::MATCH_PHRASE_QUERY, "the history ~2", false,
            AnalysisPurpose::kPlainQuery, /*expected_query_cache_lookups=*/1);
    expect_provider_failure_after_segment_admission(
            _snii_reader, InvertedIndexQueryType::MATCH_PHRASE_PREFIX_QUERY, "the hist", false,
            AnalysisPurpose::kPlainQuery, /*expected_query_cache_lookups=*/1);
    expect_provider_failure_after_segment_admission(
            _snii_reader, InvertedIndexQueryType::MATCH_PHRASE_QUERY, "the history", true,
            AnalysisPurpose::kPlainQuery);
}

TEST_F(InvertedIndexReaderAnalysisPurposeTest, PartialAnalysisFailureDoesNotPublishState) {
    preload_legacy_searcher_cache_entries();
    auto snii_provider = std::make_shared<RecordingPartialFailureAnalyzerProvider>();
    expect_analysis_failure_after_segment_admission(
            _snii_reader, InvertedIndexQueryType::MATCH_PHRASE_QUERY, "the history", false,
            AnalysisPurpose::kPlainQuery, snii_provider, /*expected_query_cache_lookups=*/1);
    EXPECT_EQ(snii_provider->emitted_tokens->load(std::memory_order_relaxed), 1);
}

TEST_F(InvertedIndexReaderAnalysisPurposeTest,
       SniiGeneratedGramBypassesAfterSegmentAdmissionBeforeSearch) {
    preload_legacy_searcher_cache_entries();
    const bool original = config::enable_common_grams_query_plan;
    Defer restore([original] {
        EXPECT_TRUE(config::set_config("enable_common_grams_query_plan",
                                       original ? "true" : "false", /*need_persist=*/false)
                            .ok());
    });
    ASSERT_TRUE(config::set_config("enable_common_grams_query_plan", "true",
                                   /*need_persist=*/false)
                        .ok());
    expect_generated_gram_bypass_after_segment_admission(_snii_reader);
}

TEST_F(InvertedIndexReaderAnalysisPurposeTest, RegexpAndWildcardBypassAnalyzer) {
    for (const auto query_type :
         {InvertedIndexQueryType::MATCH_REGEXP_QUERY, InvertedIndexQueryType::WILDCARD_QUERY}) {
        expect_raw_query_bypasses_analyzer(_snii_reader, query_type, "hist.*");
    }
}

} // namespace
} // namespace doris::segment_v2
