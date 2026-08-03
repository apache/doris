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

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <latch>
#include <memory>
#include <optional>
#include <semaphore>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>

#include "common/check.h"
#include "common/config.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_string.h"
#include "cpp/sync_point.h"
#include "exprs/function/function_multi_match.h"
#include "exprs/function/match.h"
#include "io/fs/local_file_system.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "runtime/thread_context.h"
#include "storage/index/index_file_reader.h"
#include "storage/index/inverted/analyzer/custom_analyzer.h"
#include "storage/index/inverted/common_grams/common_grams_key_codec.h"
#include "storage/index/inverted/common_grams/common_grams_segment_metadata.h"
#include "storage/index/inverted/inverted_index_cache.h"
#include "storage/index/inverted/inverted_index_reader.h"
#include "storage/index/snii/format/dict_entry.h"
#include "storage/index/snii/format/prx_pod.h"
#include "storage/index/snii/io/local_file.h"
#include "storage/index/snii/query/bm25_scorer.h"
#include "storage/index/snii/query/phrase_query.h"
#include "storage/index/snii/query/phrase_verify_timer.h"
#include "storage/index/snii/query/query_profile.h"
#include "storage/index/snii/snii_doris_adapter.h"
#include "storage/index/snii/snii_prx_profile.h"
// Exercise the reader router without acquiring process-global query-cache ownership.
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wkeyword-macro"
#define private public
#include "storage/index/snii/snii_index_reader.h"
#undef private
#pragma clang diagnostic pop
#include "storage/index/snii_query_test_util.h"
#include "storage/olap_common.h"
#include "storage/tablet/tablet_schema.h"
#include "util/debug_points.h"
#include "util/defer_op.h"

namespace doris::segment_v2 {
namespace {

using namespace doris::snii::snii_test;

constexpr int64_t kIndexId = 31;
constexpr const char* kTestDir = "./ut_dir/snii_index_reader_count_fallback_test";
constexpr const char* kIndexPathPrefix =
        "./ut_dir/snii_index_reader_count_fallback_test/positional_segment";

using PrxStatsSnapshot = std::array<int64_t, 11>;

struct SingleFlightFollowerCounts {
    std::atomic<uint32_t> count_consumers {0};
    std::atomic<uint32_t> row_consumers {0};
    std::binary_semaphore* row_follower_joined = nullptr;
};

struct SingleFlightLeaderGate {
    std::binary_semaphore leader_entered {0};
    std::binary_semaphore release_leader {0};
};

thread_local bool is_count_consumer = false;

void record_single_flight_follower(void* opaque) noexcept {
    auto* counts = static_cast<SingleFlightFollowerCounts*>(opaque);
    if (is_count_consumer) {
        counts->count_consumers.fetch_add(1, std::memory_order_relaxed);
    } else {
        counts->row_consumers.fetch_add(1, std::memory_order_relaxed);
        if (counts->row_follower_joined != nullptr) {
            counts->row_follower_joined->release();
        }
    }
}

void block_single_flight_leader_before_compute(void* opaque) noexcept {
    auto* gate = static_cast<SingleFlightLeaderGate*>(opaque);
    gate->leader_entered.release();
    gate->release_leader.acquire();
}

void record_single_flight_leader(void* opaque) noexcept {
    static_cast<std::atomic<uint32_t>*>(opaque)->fetch_add(1, std::memory_order_relaxed);
}

void record_searcher_open(void* opaque) noexcept {
    static_cast<std::atomic<uint32_t>*>(opaque)->fetch_add(1, std::memory_order_relaxed);
}

PrxStatsSnapshot prx_stats_snapshot(const OlapReaderStatistics& stats) {
    return {stats.snii_stats.prx_raw_frames,      stats.snii_stats.prx_zstd_frames,
            stats.snii_stats.prx_pfor_frames,     stats.snii_stats.prx_plaintext_bytes,
            stats.snii_stats.prx_total_docs,      stats.snii_stats.prx_selected_docs,
            stats.snii_stats.prx_total_positions, stats.snii_stats.prx_selected_positions,
            stats.snii_stats.prx_fetch_ns,        stats.snii_stats.prx_decode_ns,
            stats.snii_stats.prx_phrase_verify_ns};
}

void set_prx_stats_sentinel(OlapReaderStatistics* stats) {
    stats->snii_stats.prx_raw_frames = 101;
    stats->snii_stats.prx_zstd_frames = 102;
    stats->snii_stats.prx_pfor_frames = 103;
    stats->snii_stats.prx_plaintext_bytes = 104;
    stats->snii_stats.prx_total_docs = 105;
    stats->snii_stats.prx_selected_docs = 106;
    stats->snii_stats.prx_total_positions = 107;
    stats->snii_stats.prx_selected_positions = 108;
    stats->snii_stats.prx_fetch_ns = 109;
    stats->snii_stats.prx_decode_ns = 110;
    stats->snii_stats.prx_phrase_verify_ns = 111;
}

std::vector<uint32_t> bitmap_docids(const roaring::Roaring& bitmap) {
    return {bitmap.begin(), bitmap.end()};
}

struct QueryExecutionContext {
    explicit QueryExecutionContext(bool enable_query_cache, bool count_on_index_fastpath = false) {
        TQueryOptions query_options;
        query_options.query_type = TQueryType::SELECT;
        query_options.enable_inverted_index_query_cache = enable_query_cache;
        query_options.enable_inverted_index_searcher_cache = false;
        runtime_state.set_query_options(query_options);
        context->io_ctx = &io_ctx;
        context->stats = &stats;
        context->runtime_state = &runtime_state;
        context->count_on_index_fastpath = count_on_index_fastpath;
    }

    OlapReaderStatistics stats;
    io::IOContext io_ctx;
    RuntimeState runtime_state;
    IndexQueryContextPtr context = std::make_shared<IndexQueryContext>();
};

struct CommonGramsCounterSnapshot {
    int64_t candidate_queries = 0;
    int64_t plain_plans = 0;
    int64_t fallback_no_gram = 0;
    int64_t fallback_incompatible = 0;
    int64_t fallback_cost = 0;
};

CommonGramsCounterSnapshot common_grams_counter_snapshot(const OlapReaderStatistics& stats) {
    return {.candidate_queries = stats.snii_stats.common_grams_candidate_queries,
            .plain_plans = stats.snii_stats.common_grams_plain_plans,
            .fallback_no_gram = stats.snii_stats.common_grams_fallback_no_gram,
            .fallback_incompatible = stats.snii_stats.common_grams_fallback_incompatible,
            .fallback_cost = stats.snii_stats.common_grams_fallback_cost};
}

void init_index_meta(TabletIndex* meta, int64_t index_id = kIndexId,
                     std::string parser = "english") {
    TabletIndexPB pb;
    pb.set_index_type(IndexType::INVERTED);
    pb.set_index_id(index_id);
    pb.set_index_name("count_fallback_idx");
    pb.add_col_unique_id(0);
    pb.mutable_properties()->insert({"parser", std::move(parser)});
    pb.mutable_properties()->insert({"lower_case", "true"});
    pb.mutable_properties()->insert({"support_phrase", "true"});
    meta->init_from_pb(pb);
}

void write_positional_segment() {
    std::vector<doris::snii::writer::TermPostings> terms {
            make_term("failed", {{.docid = 0, .positions = {0}},
                                 {.docid = 1, .positions = {0}},
                                 {.docid = 2, .positions = {0}},
                                 {.docid = 3, .positions = {0}},
                                 {.docid = 4, .positions = {0}},
                                 {.docid = 5, .positions = {0}}}),
            make_term("order", {{.docid = 0, .positions = {1}},
                                {.docid = 1, .positions = {0}},
                                {.docid = 3, .positions = {1}},
                                {.docid = 4, .positions = {0}},
                                {.docid = 5, .positions = {1}}}),
            make_term("ordered", {{.docid = 2, .positions = {1}}}),
            make_term("warehouse", {{.docid = 2, .positions = {2}}}),
    };

    doris::snii::writer::SniiIndexInput input;
    input.index_id = kIndexId;
    input.index_suffix = "";
    input.config = doris::snii::format::IndexConfig::kDocsPositions;
    input.doc_count = 6;
    input.terms = std::move(terms);
    input.target_dict_block_bytes = 64;

    MemoryFile memory_file;
    doris::snii::writer::SniiCompoundWriter compound(&memory_file);
    assert_ok(compound.add_logical_index(input));
    assert_ok(compound.finish());

    const std::string path = InvertedIndexDescriptor::get_index_file_path_v2(kIndexPathPrefix);
    doris::snii::io::LocalFileWriter file;
    assert_ok(file.open(path));
    assert_ok(
            file.append(doris::snii::Slice(memory_file.data().data(), memory_file.data().size())));
    assert_ok(file.finalize());
}

std::shared_ptr<inverted_index::CustomAnalyzerProvider> make_common_grams_provider() {
    inverted_index::Settings tokenizer_settings;
    tokenizer_settings.set("tokenize_on_chars", "[whitespace]");
    inverted_index::CustomAnalyzerConfig::Builder builder;
    builder.with_tokenizer_config("char_group", tokenizer_settings);
    builder.add_token_filter_config("lowercase", {});
    builder.add_token_filter_config("common_grams", {});
    return std::make_shared<inverted_index::CustomAnalyzerProvider>(builder.build());
}

std::shared_ptr<inverted_index::CustomAnalyzerProvider> make_plain_provider() {
    inverted_index::Settings tokenizer_settings;
    tokenizer_settings.set("tokenize_on_chars", "[whitespace]");
    inverted_index::CustomAnalyzerConfig::Builder builder;
    builder.with_tokenizer_config("char_group", tokenizer_settings);
    builder.add_token_filter_config("lowercase", {});
    return std::make_shared<inverted_index::CustomAnalyzerProvider>(builder.build());
}

std::string encode_plain_test_term(std::string_view term) {
    auto encoded = inverted_index::encode_plain_term(
            term, inverted_index::PlainTermKeyVersion::kEscapedV1);
    DORIS_CHECK(encoded.has_value());
    return std::move(*encoded);
}

std::string encode_gram_test_term(std::string_view left, std::string_view right) {
    auto encoded = inverted_index::encode_common_gram(left, right);
    DORIS_CHECK(encoded.has_value());
    return std::move(*encoded);
}

Status write_common_grams_segment(std::string_view index_path_prefix,
                                  const inverted_index::CommonGramsQueryIdentity& analyzer_identity,
                                  inverted_index::CommonGramsCoverage coverage, bool include_gram,
                                  uint32_t dense_doc_count = 0, bool dense_common_pair = false) {
    std::vector<PostingDoc> alpha_docs {{.docid = 1, .positions = {0}}};
    std::vector<PostingDoc> beta_docs {{.docid = 1, .positions = {1}}};
    std::vector<PostingDoc> the_docs {{.docid = 0, .positions = {0}}};
    std::vector<PostingDoc> wolf_docs {{.docid = 0, .positions = {1}}};
    if (dense_doc_count != 0) {
        alpha_docs.clear();
        beta_docs.clear();
        alpha_docs.reserve(dense_doc_count);
        beta_docs.reserve(dense_doc_count);
        for (uint32_t docid = 0; docid < dense_doc_count; ++docid) {
            alpha_docs.push_back({.docid = docid, .positions = {0}});
            beta_docs.push_back({.docid = docid, .positions = {1}});
        }
        if (dense_common_pair) {
            // Make "the wolf" an adjacent phrase in EVERY doc with dense postings
            // for both terms, mirroring the dense alpha/beta shape whose plain
            // execution provably reads postings through the adapter. This is the
            // planning-entry variant: "the" is a common word, so the wordset
            // pre-proof cannot rule gram usage out. Callers must combine this
            // with include_gram=true: omitting the gram under kComplete coverage
            // would let the planner prove the phrase authoritatively empty from
            // the resident dictionary alone and skip posting IO entirely.
            the_docs.clear();
            wolf_docs.clear();
            the_docs.reserve(dense_doc_count);
            wolf_docs.reserve(dense_doc_count);
            for (uint32_t docid = 0; docid < dense_doc_count; ++docid) {
                the_docs.push_back({.docid = docid, .positions = {0}});
                wolf_docs.push_back({.docid = docid, .positions = {1}});
            }
        }
    }
    std::vector<doris::snii::writer::TermPostings> terms {
            make_term(encode_plain_test_term("alpha"), std::move(alpha_docs)),
            make_term(encode_plain_test_term("beta"), std::move(beta_docs)),
            make_term(encode_plain_test_term("the"), std::move(the_docs)),
            make_term(encode_plain_test_term("wolf"), std::move(wolf_docs)),
    };
    if (include_gram) {
        std::vector<PostingDoc> gram_docs {{.docid = 0, .positions = {0}}};
        if (dense_doc_count != 0 && dense_common_pair) {
            // Keep the gram postings consistent with the dense the@0/wolf@1
            // docs above: the_wolf occurs at position 0 in every doc.
            gram_docs.clear();
            gram_docs.reserve(dense_doc_count);
            for (uint32_t docid = 0; docid < dense_doc_count; ++docid) {
                gram_docs.push_back({.docid = docid, .positions = {0}});
            }
        }
        terms.push_back(make_term(encode_gram_test_term("the", "wolf"), std::move(gram_docs)));
    }
    std::ranges::sort(terms, [](const auto& lhs, const auto& rhs) { return lhs.term < rhs.term; });

    inverted_index::CommonGramsSegmentMetadata metadata;
    metadata.plain_term_key_version = inverted_index::PlainTermKeyVersion::kEscapedV1;
    metadata.common_grams_coverage = coverage;
    metadata.common_grams_semantics_version = inverted_index::COMMON_GRAMS_SEMANTICS_VERSION_V1;
    metadata.common_grams_key_version = inverted_index::COMMON_GRAMS_KEY_VERSION_V1;
    metadata.common_grams_dictionary_identity = analyzer_identity.common_grams_dictionary_identity;
    metadata.base_analyzer_fingerprint = analyzer_identity.base_analyzer_fingerprint;
    metadata.common_grams_fingerprint = analyzer_identity.common_grams_fingerprint;

    doris::snii::writer::SniiIndexInput input;
    input.index_id = kIndexId;
    input.index_suffix = "";
    input.config = doris::snii::format::IndexConfig::kDocsPositions;
    input.doc_count = dense_doc_count == 0 ? 2 : dense_doc_count * 4;
    input.terms = std::move(terms);
    input.target_dict_block_bytes = 64;
    input.common_grams_metadata = std::move(metadata);

    MemoryFile memory_file;
    doris::snii::writer::SniiCompoundWriter compound(&memory_file);
    RETURN_IF_ERROR(compound.add_logical_index(input));
    RETURN_IF_ERROR(compound.finish());

    doris::snii::io::LocalFileWriter local_file;
    RETURN_IF_ERROR(local_file.open(
            InvertedIndexDescriptor::get_index_file_path_v2(std::string(index_path_prefix))));
    RETURN_IF_ERROR(local_file.append(
            doris::snii::Slice(memory_file.data().data(), memory_file.data().size())));
    return local_file.finalize();
}

Status write_scoring_segment(std::string_view index_path_prefix,
                             const inverted_index::CommonGramsQueryIdentity& analyzer_identity,
                             bool corrupt_norms = false) {
    std::vector<doris::snii::writer::TermPostings> terms {
            make_term(encode_plain_test_term("alpha"),
                      {{.docid = 0, .positions = {0, 1, 2, 3}}, {.docid = 1, .positions = {0, 2}}}),
            make_term(encode_plain_test_term("beta"),
                      {{.docid = 1, .positions = {1, 3}}, {.docid = 2, .positions = {0, 1}}}),
    };
    std::ranges::sort(terms, [](const auto& lhs, const auto& rhs) { return lhs.term < rhs.term; });

    auto metadata = inverted_index::make_common_grams_segment_metadata(analyzer_identity);
    metadata.scoring_doc_count = 3;
    metadata.scoring_token_count = 10;

    doris::snii::writer::SniiIndexInput input;
    input.index_id = kIndexId;
    input.index_suffix = "";
    input.config = doris::snii::format::IndexConfig::kDocsPositionsScoring;
    input.doc_count = 3;
    input.encoded_norms = {doris::snii::query::encode_norm(4), doris::snii::query::encode_norm(4),
                           doris::snii::query::encode_norm(2)};
    input.terms = std::move(terms);
    input.target_dict_block_bytes = 64;
    input.common_grams_metadata = std::move(metadata);

    MemoryFile memory_file;
    doris::snii::writer::SniiCompoundWriter compound(&memory_file);
    RETURN_IF_ERROR(compound.add_logical_index(input));
    RETURN_IF_ERROR(compound.finish());

    std::vector<uint8_t> bytes = memory_file.data();
    if (corrupt_norms) {
        doris::snii::reader::SniiSegmentReader segment_reader;
        RETURN_IF_ERROR(
                doris::snii::reader::SniiSegmentReader::open(&memory_file, &segment_reader));
        doris::snii::reader::LogicalIndexReader logical_reader;
        RETURN_IF_ERROR(segment_reader.open_index(kIndexId, "", &logical_reader));
        const auto norms = logical_reader.section_refs().norms;
        DORIS_CHECK_GT(norms.length, 0);
        DORIS_CHECK_LE(norms.offset + norms.length, bytes.size());
        bytes[norms.offset + norms.length - 1] ^= 0x01;
    }

    doris::snii::io::LocalFileWriter local_file;
    RETURN_IF_ERROR(local_file.open(
            InvertedIndexDescriptor::get_index_file_path_v2(std::string(index_path_prefix))));
    RETURN_IF_ERROR(local_file.append(doris::snii::Slice(bytes.data(), bytes.size())));
    return local_file.finalize();
}

Status write_nullable_phrase_segment(std::string_view index_path_prefix, bool has_null,
                                     int64_t index_id = kIndexId, bool corrupt_null = false) {
    std::vector<doris::snii::writer::TermPostings> terms {
            make_term("alpha", {{.docid = 0, .positions = {0}},
                                {.docid = 2, .positions = {0}},
                                {.docid = 3, .positions = {1}}}),
            make_term("beta", {{.docid = 0, .positions = {1}}, {.docid = 3, .positions = {0}}}),
            make_term("betamax", {{.docid = 2, .positions = {1}}}),
    };

    doris::snii::writer::SniiIndexInput input;
    input.index_id = index_id;
    input.index_suffix = "";
    input.config = doris::snii::format::IndexConfig::kDocsPositions;
    input.doc_count = 4;
    input.terms = std::move(terms);
    input.target_dict_block_bytes = 64;
    if (has_null) {
        input.null_docids = {1};
    }

    MemoryFile memory_file;
    doris::snii::writer::SniiCompoundWriter compound(&memory_file);
    RETURN_IF_ERROR(compound.add_logical_index(input));
    RETURN_IF_ERROR(compound.finish());

    std::vector<uint8_t> bytes = memory_file.data();
    if (corrupt_null) {
        doris::snii::reader::SniiSegmentReader segment_reader;
        RETURN_IF_ERROR(
                doris::snii::reader::SniiSegmentReader::open(&memory_file, &segment_reader));
        doris::snii::reader::LogicalIndexReader logical_reader;
        RETURN_IF_ERROR(segment_reader.open_index(index_id, "", &logical_reader));
        const auto null_bitmap = logical_reader.section_refs().null_bitmap;
        DORIS_CHECK_GT(null_bitmap.length, 0);
        DORIS_CHECK_LE(null_bitmap.offset + null_bitmap.length, bytes.size());
        bytes[null_bitmap.offset + null_bitmap.length - 1] ^= 0x01;
    }

    doris::snii::io::LocalFileWriter local_file;
    RETURN_IF_ERROR(local_file.open(
            InvertedIndexDescriptor::get_index_file_path_v2(std::string(index_path_prefix))));
    RETURN_IF_ERROR(local_file.append(doris::snii::Slice(bytes.data(), bytes.size())));
    return local_file.finalize();
}

class FixedCollectionStatistics final : public CollectionStatistics {
public:
    float get_or_calculate_idf(const std::wstring& field_name, const std::wstring& term) override {
        fields.push_back(field_name);
        idf_terms.push_back(term);
        return idfs.at(term);
    }

    float get_or_calculate_avg_dl(const std::wstring& field_name) override {
        fields.push_back(field_name);
        return avgdl;
    }

    float avgdl = 3.0F;
    std::unordered_map<std::wstring, float> idfs {{L"alpha", 1.0F}, {L"beta", 2.0F}};
    std::vector<std::wstring> fields;
    std::vector<std::wstring> idf_terms;
};

std::vector<uint32_t> positive_score_docids(const CollectionSimilarity& similarity,
                                            std::initializer_list<uint32_t> candidates) {
    roaring::Roaring bitmap;
    for (uint32_t docid : candidates) {
        bitmap.add(docid);
    }
    IColumn::MutablePtr scores;
    auto row_ids = std::make_unique<std::vector<uint64_t>>();
    auto positive = std::make_shared<ScoreRangeFilter>(
            ScoreRangeFilter {.op = TExprOpcode::GT, .threshold = 0.0});
    similarity.get_bm25_scores(&bitmap, scores, row_ids, positive);
    return bitmap_docids(bitmap);
}

float score_for_doc(const CollectionSimilarity& similarity, uint32_t docid) {
    roaring::Roaring bitmap;
    bitmap.add(docid);
    IColumn::MutablePtr scores;
    auto row_ids = std::make_unique<std::vector<uint64_t>>();
    similarity.get_bm25_scores(&bitmap, scores, row_ids);
    DORIS_CHECK_EQ(row_ids->size(), 1);
    const auto& nullable = assert_cast<const ColumnNullable&>(*scores);
    const auto& values = assert_cast<const ColumnFloat32&>(nullable.get_nested_column());
    return values.get_data()[0];
}

struct OpenedSniiIndex {
    std::shared_ptr<IndexFileReader> file_reader;
    std::shared_ptr<SniiIndexReader> index_reader;
};

Status open_snii_index(const TabletIndex* meta, std::string index_path_prefix,
                       OpenedSniiIndex* opened) {
    opened->file_reader = std::make_shared<IndexFileReader>(io::global_local_filesystem(),
                                                            std::move(index_path_prefix),
                                                            InvertedIndexStorageFormatPB::SNII);
    RETURN_IF_ERROR(opened->file_reader->init());
    opened->index_reader = SniiIndexReader::create_shared(meta, opened->file_reader,
                                                          InvertedIndexReaderType::FULLTEXT);
    return Status::OK();
}

bool has_cached_null_bitmap(const TabletIndex& meta, const OpenedSniiIndex& opened) {
    const auto index_file_key = opened.file_reader->get_index_file_cache_key(&meta);
    InvertedIndexQueryCache::CacheKey cache_key {
            index_file_key, "", InvertedIndexQueryType::UNKNOWN_QUERY, "null_bitmap"};
    InvertedIndexQueryCacheHandle cache_handle;
    return InvertedIndexQueryCache::instance()->lookup(cache_key, &cache_handle);
}

uint32_t lookup_df(const doris::snii::reader::LogicalIndexReader& index, const std::string& term) {
    bool found = false;
    doris::snii::format::DictEntry entry;
    uint64_t frq_base = 0;
    uint64_t prx_base = 0;
    assert_ok(index.lookup(term, &found, &entry, &frq_base, &prx_base));
    EXPECT_TRUE(found);
    return entry.df;
}

class SniiIndexReaderCountFallback : public testing::Test {
protected:
    void SetUp() override {
        assert_ok(io::global_local_filesystem()->delete_directory(kTestDir));
        assert_ok(io::global_local_filesystem()->create_directory(kTestDir));
        init_index_meta(&_meta);
        write_positional_segment();
        _file_reader =
                std::make_shared<IndexFileReader>(io::global_local_filesystem(), kIndexPathPrefix,
                                                  InvertedIndexStorageFormatPB::SNII);
        assert_ok(_file_reader->init());
        _index_reader = SniiIndexReader::create_shared(&_meta, _file_reader,
                                                       InvertedIndexReaderType::FULLTEXT);
        _previous_query_cache = ExecEnv::GetInstance()->get_inverted_index_query_cache();
        _query_cache.reset(InvertedIndexQueryCache::create_global_cache(1024 * 1024, 1));
        ExecEnv::GetInstance()->set_inverted_index_query_cache(_query_cache.get());
    }

    void TearDown() override {
        _index_reader.reset();
        _file_reader.reset();
        ExecEnv::GetInstance()->set_inverted_index_query_cache(_previous_query_cache);
        _query_cache.reset();
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(kTestDir).ok());
    }

    TabletIndex _meta;
    std::shared_ptr<IndexFileReader> _file_reader;
    std::shared_ptr<SniiIndexReader> _index_reader;
    InvertedIndexQueryCache* _previous_query_cache = nullptr;
    std::unique_ptr<InvertedIndexQueryCache> _query_cache;
};

template <typename MatchFunction>
void expect_function_match_reuses_reader(const TabletIndex& meta, std::string_view path_prefix,
                                         std::string_view query, bool has_null,
                                         std::vector<uint32_t> expected_docids) {
    assert_ok(write_nullable_phrase_segment(path_prefix, has_null));
    OpenedSniiIndex opened;
    assert_ok(open_snii_index(&meta, std::string(path_prefix), &opened));

    std::atomic<uint32_t> searcher_opens {0};
    opened.index_reader->set_searcher_open_observer_for_test(record_searcher_open, &searcher_opens);

    QueryExecutionContext execution(/*enable_query_cache=*/false);
    std::unique_ptr<IndexIterator> iterator;
    assert_ok(opened.index_reader->new_iterator(&iterator));
    iterator->set_context(execution.context);

    auto query_column = ColumnString::create();
    query_column->insert_data(query.data(), query.size());
    ColumnsWithTypeAndName arguments {{ColumnConst::create(std::move(query_column), 1),
                                       std::make_shared<DataTypeString>(), "query"}};
    std::vector<IndexFieldNameAndTypePair> data_type_with_names {
            {"content", std::make_shared<DataTypeString>()}};
    std::vector<IndexIterator*> iterators {iterator.get()};

    MatchFunction function;
    InvertedIndexResultBitmap result;
    assert_ok(function.evaluate_inverted_index(arguments, data_type_with_names, iterators,
                                               /*num_rows=*/4, nullptr, result));

    ASSERT_NE(result.get_data_bitmap(), nullptr);
    EXPECT_EQ(bitmap_docids(*result.get_data_bitmap()), expected_docids);
    ASSERT_NE(result.get_null_bitmap(), nullptr);
    EXPECT_EQ(bitmap_docids(*result.get_null_bitmap()),
              has_null ? std::vector<uint32_t> {1} : std::vector<uint32_t> {});
    EXPECT_EQ(searcher_opens.load(std::memory_order_relaxed), 1);
}

TEST_F(SniiIndexReaderCountFallback, PhraseQueryReusesReaderForNullBitmap) {
    expect_function_match_reuses_reader<FunctionMatchPhrase>(
            _meta, std::string(kTestDir) + "/phrase_nullable", "alpha beta", /*has_null=*/true,
            {0});
    expect_function_match_reuses_reader<FunctionMatchPhrase>(
            _meta, std::string(kTestDir) + "/phrase_null_free", "alpha beta", /*has_null=*/false,
            {0});
}

TEST_F(SniiIndexReaderCountFallback, PhrasePrefixQueryReusesReaderForNullBitmap) {
    expect_function_match_reuses_reader<FunctionMatchPhrasePrefix>(
            _meta, std::string(kTestDir) + "/prefix_nullable", "alpha be", /*has_null=*/true,
            {0, 2});
    expect_function_match_reuses_reader<FunctionMatchPhrasePrefix>(
            _meta, std::string(kTestDir) + "/prefix_null_free", "alpha be", /*has_null=*/false,
            {0, 2});
}

TEST_F(SniiIndexReaderCountFallback, FunctionMatchUsesQuerySelectedReaderForNullBitmap) {
    constexpr int64_t kSelectedIndexId = kIndexId + 1;
    const std::string decoy_path = std::string(kTestDir) + "/affinity_decoy";
    const std::string selected_path = std::string(kTestDir) + "/affinity_selected";
    TabletIndex decoy_meta;
    TabletIndex selected_meta;
    init_index_meta(&decoy_meta, kIndexId, "standard");
    init_index_meta(&selected_meta, kSelectedIndexId, "english");
    assert_ok(write_nullable_phrase_segment(decoy_path, /*has_null=*/true, kIndexId));
    assert_ok(write_nullable_phrase_segment(selected_path, /*has_null=*/false, kSelectedIndexId));

    OpenedSniiIndex decoy;
    OpenedSniiIndex selected;
    assert_ok(open_snii_index(&decoy_meta, decoy_path, &decoy));
    assert_ok(open_snii_index(&selected_meta, selected_path, &selected));
    std::atomic<uint32_t> decoy_opens {0};
    std::atomic<uint32_t> selected_opens {0};
    decoy.index_reader->set_searcher_open_observer_for_test(record_searcher_open, &decoy_opens);
    selected.index_reader->set_searcher_open_observer_for_test(record_searcher_open,
                                                               &selected_opens);

    QueryExecutionContext execution(/*enable_query_cache=*/false);
    std::unique_ptr<IndexIterator> iterator;
    assert_ok(decoy.index_reader->new_iterator(&iterator));
    assert_ok(selected.index_reader->new_iterator(&iterator));
    iterator->set_context(execution.context);

    auto query_column = ColumnString::create();
    query_column->insert_data("alpha beta", 10);
    ColumnsWithTypeAndName arguments {{ColumnConst::create(std::move(query_column), 1),
                                       std::make_shared<DataTypeString>(), "query"}};
    std::vector<IndexFieldNameAndTypePair> data_type_with_names {
            {"content", std::make_shared<DataTypeString>()}};
    std::vector<IndexIterator*> iterators {iterator.get()};
    InvertedIndexAnalyzerCtx analyzer_ctx;
    analyzer_ctx.analyzer_name = "english";
    analyzer_ctx.parser_type = InvertedIndexParserType::PARSER_ENGLISH;

    FunctionMatchPhrase function;
    InvertedIndexResultBitmap result;
    assert_ok(function.evaluate_inverted_index(arguments, data_type_with_names, iterators,
                                               /*num_rows=*/4, &analyzer_ctx, result));

    ASSERT_NE(result.get_data_bitmap(), nullptr);
    EXPECT_EQ(bitmap_docids(*result.get_data_bitmap()), (std::vector<uint32_t> {0}));
    ASSERT_NE(result.get_null_bitmap(), nullptr);
    EXPECT_TRUE(result.get_null_bitmap()->isEmpty());
    EXPECT_EQ(selected_opens.load(std::memory_order_relaxed), 1);
    EXPECT_EQ(decoy_opens.load(std::memory_order_relaxed), 0);
    EXPECT_TRUE(has_cached_null_bitmap(selected_meta, selected));
    EXPECT_FALSE(has_cached_null_bitmap(decoy_meta, decoy));
}

TEST_F(SniiIndexReaderCountFallback, FunctionMultiMatchDoesNotCacheSniiNullBitmap) {
    const std::string path = std::string(kTestDir) + "/multi_match_no_null";
    assert_ok(write_nullable_phrase_segment(path, /*has_null=*/true));
    OpenedSniiIndex opened;
    assert_ok(open_snii_index(&_meta, path, &opened));
    std::atomic<uint32_t> searcher_opens {0};
    opened.index_reader->set_searcher_open_observer_for_test(record_searcher_open, &searcher_opens);

    QueryExecutionContext execution(/*enable_query_cache=*/false);
    std::unique_ptr<IndexIterator> iterator;
    assert_ok(opened.index_reader->new_iterator(&iterator));
    iterator->set_context(execution.context);

    auto type_column = ColumnString::create();
    type_column->insert_data("phrase", 6);
    auto query_column = ColumnString::create();
    query_column->insert_data("alpha beta", 10);
    ColumnsWithTypeAndName arguments {{ColumnConst::create(std::move(type_column), 1),
                                       std::make_shared<DataTypeString>(), "type"},
                                      {ColumnConst::create(std::move(query_column), 1),
                                       std::make_shared<DataTypeString>(), "query"}};
    std::vector<IndexFieldNameAndTypePair> data_type_with_names {
            {"content", std::make_shared<DataTypeString>()}};
    std::vector<IndexIterator*> iterators {iterator.get()};

    FunctionMultiMatch function;
    InvertedIndexResultBitmap result;
    assert_ok(function.evaluate_inverted_index(arguments, data_type_with_names, iterators,
                                               /*num_rows=*/4, nullptr, result));

    ASSERT_NE(result.get_data_bitmap(), nullptr);
    EXPECT_EQ(bitmap_docids(*result.get_data_bitmap()), (std::vector<uint32_t> {0}));
    ASSERT_NE(result.get_null_bitmap(), nullptr);
    EXPECT_TRUE(result.get_null_bitmap()->isEmpty());
    EXPECT_EQ(searcher_opens.load(std::memory_order_relaxed), 1);
    EXPECT_FALSE(has_cached_null_bitmap(_meta, opened));
    EXPECT_EQ(execution.stats.inverted_index_query_null_bitmap_timer, 0);
}

TEST_F(SniiIndexReaderCountFallback, FunctionMatchCacheHitLoadsNullFromSelectedReaderOnce) {
    const std::string path = std::string(kTestDir) + "/function_match_cache_hit";
    assert_ok(write_nullable_phrase_segment(path, /*has_null=*/true));
    OpenedSniiIndex opened;
    assert_ok(open_snii_index(&_meta, path, &opened));
    std::atomic<uint32_t> searcher_opens {0};
    opened.index_reader->set_searcher_open_observer_for_test(record_searcher_open, &searcher_opens);

    const Field query_value = Field::create_field<TYPE_STRING>(std::string("alpha beta"));
    QueryExecutionContext populate(/*enable_query_cache=*/true);
    std::shared_ptr<roaring::Roaring> populated_bitmap;
    assert_ok(opened.index_reader->query(populate.context, "content", query_value,
                                         InvertedIndexQueryType::MATCH_PHRASE_QUERY,
                                         populated_bitmap));
    EXPECT_FALSE(has_cached_null_bitmap(_meta, opened));
    searcher_opens.store(0, std::memory_order_relaxed);

    QueryExecutionContext cache_hit(/*enable_query_cache=*/true);
    std::unique_ptr<IndexIterator> iterator;
    assert_ok(opened.index_reader->new_iterator(&iterator));
    iterator->set_context(cache_hit.context);
    auto query_column = ColumnString::create();
    query_column->insert_data("alpha beta", 10);
    ColumnsWithTypeAndName arguments {{ColumnConst::create(std::move(query_column), 1),
                                       std::make_shared<DataTypeString>(), "query"}};
    std::vector<IndexFieldNameAndTypePair> data_type_with_names {
            {"content", std::make_shared<DataTypeString>()}};
    std::vector<IndexIterator*> iterators {iterator.get()};

    FunctionMatchPhrase function;
    InvertedIndexResultBitmap result;
    assert_ok(function.evaluate_inverted_index(arguments, data_type_with_names, iterators,
                                               /*num_rows=*/4, nullptr, result));

    ASSERT_NE(result.get_data_bitmap(), nullptr);
    EXPECT_EQ(bitmap_docids(*result.get_data_bitmap()), (std::vector<uint32_t> {0}));
    ASSERT_NE(result.get_null_bitmap(), nullptr);
    EXPECT_EQ(bitmap_docids(*result.get_null_bitmap()), (std::vector<uint32_t> {1}));
    EXPECT_EQ(cache_hit.stats.inverted_index_query_cache_hit, 1);
    EXPECT_EQ(searcher_opens.load(std::memory_order_relaxed), 1);
    EXPECT_TRUE(has_cached_null_bitmap(_meta, opened));
}

TEST_F(SniiIndexReaderCountFallback, PublicPhraseQueryLeaderRecordsPrxWork) {
    QueryExecutionContext execution(/*enable_query_cache=*/false);
    Field query_value = Field::create_field<TYPE_STRING>(std::string("failed order"));
    std::shared_ptr<roaring::Roaring> bitmap;

    assert_ok(_index_reader->query(execution.context, "content", query_value,
                                   InvertedIndexQueryType::MATCH_PHRASE_QUERY, bitmap));

    ASSERT_NE(bitmap, nullptr);
    EXPECT_EQ(bitmap_docids(*bitmap), (std::vector<uint32_t> {0, 3, 5}));
    EXPECT_GT(execution.stats.snii_stats.prx_raw_frames +
                      execution.stats.snii_stats.prx_zstd_frames +
                      execution.stats.snii_stats.prx_pfor_frames,
              0);
    EXPECT_GT(execution.stats.snii_stats.prx_plaintext_bytes, 0);
    EXPECT_GT(execution.stats.snii_stats.prx_total_docs, 0);
}

TEST_F(SniiIndexReaderCountFallback, PublicBooleanQueriesScoreOnlyFinalBitmapWithPlainTerms) {
    const auto provider = make_common_grams_provider();
    ASSERT_NE(provider->common_grams_identity(), nullptr);
    constexpr std::string_view kPathPrefix =
            "./ut_dir/snii_index_reader_count_fallback_test/scoring_segment";
    assert_ok(write_scoring_segment(kPathPrefix, *provider->common_grams_identity()));
    OpenedSniiIndex opened;
    assert_ok(open_snii_index(&_meta, std::string(kPathPrefix), &opened));

    InvertedIndexAnalyzerCtx analyzer_ctx;
    analyzer_ctx.parser_type = InvertedIndexParserType::PARSER_ENGLISH;
    analyzer_ctx.analyzer_provider = provider;
    const Field query_value = Field::create_field<TYPE_STRING>(std::string("alpha beta"));

    auto all_stats = std::make_shared<FixedCollectionStatistics>();
    QueryExecutionContext match_all(/*enable_query_cache=*/true);
    match_all.context->collection_statistics = all_stats;
    match_all.context->collection_similarity = std::make_shared<CollectionSimilarity>();
    std::shared_ptr<roaring::Roaring> all_bitmap;
    assert_ok(opened.index_reader->query(match_all.context, "content", query_value,
                                         InvertedIndexQueryType::MATCH_ALL_QUERY, all_bitmap,
                                         &analyzer_ctx));
    ASSERT_NE(all_bitmap, nullptr);
    EXPECT_EQ(bitmap_docids(*all_bitmap), (std::vector<uint32_t> {1}));
    EXPECT_EQ(positive_score_docids(*match_all.context->collection_similarity, {0, 1, 2}),
              (std::vector<uint32_t> {1}));
    EXPECT_EQ(all_stats->idf_terms, (std::vector<std::wstring> {L"alpha", L"beta"}));
    EXPECT_EQ(all_stats->fields, (std::vector<std::wstring> {L"content", L"content", L"content"}));
    EXPECT_EQ(match_all.stats.inverted_index_query_cache_lookup, 0);

    auto any_stats = std::make_shared<FixedCollectionStatistics>();
    QueryExecutionContext match_any(/*enable_query_cache=*/true);
    match_any.context->collection_statistics = any_stats;
    match_any.context->collection_similarity = std::make_shared<CollectionSimilarity>();
    std::shared_ptr<roaring::Roaring> any_bitmap;
    assert_ok(opened.index_reader->query(match_any.context, "content", query_value,
                                         InvertedIndexQueryType::MATCH_ANY_QUERY, any_bitmap,
                                         &analyzer_ctx));
    ASSERT_NE(any_bitmap, nullptr);
    EXPECT_EQ(bitmap_docids(*any_bitmap), (std::vector<uint32_t> {0, 1, 2}));
    EXPECT_EQ(positive_score_docids(*match_any.context->collection_similarity, {0, 1, 2}),
              (std::vector<uint32_t> {0, 1, 2}));
    EXPECT_EQ(any_stats->idf_terms, (std::vector<std::wstring> {L"alpha", L"beta"}));
    EXPECT_EQ(any_stats->fields, (std::vector<std::wstring> {L"content", L"content", L"content"}));
    EXPECT_EQ(match_any.stats.inverted_index_query_cache_lookup, 0);
}

TEST_F(SniiIndexReaderCountFallback, PublicScoringZeroHitDoesNotLoadNorms) {
    const auto provider = make_common_grams_provider();
    ASSERT_NE(provider->common_grams_identity(), nullptr);
    constexpr std::string_view kPathPrefix =
            "./ut_dir/snii_index_reader_count_fallback_test/scoring_zero_hit";
    assert_ok(write_scoring_segment(kPathPrefix, *provider->common_grams_identity(),
                                    /*corrupt_norms=*/true));
    OpenedSniiIndex opened;
    assert_ok(open_snii_index(&_meta, std::string(kPathPrefix), &opened));
    InvertedIndexAnalyzerCtx analyzer_ctx;
    analyzer_ctx.parser_type = InvertedIndexParserType::PARSER_ENGLISH;
    analyzer_ctx.analyzer_provider = provider;
    QueryExecutionContext execution(/*enable_query_cache=*/true);
    execution.context->collection_statistics = std::make_shared<FixedCollectionStatistics>();
    execution.context->collection_similarity = std::make_shared<CollectionSimilarity>();
    const Field query_value = Field::create_field<TYPE_STRING>(std::string("missing"));
    std::shared_ptr<roaring::Roaring> bitmap;

    const Status status = opened.index_reader->query(
            execution.context, "scoring_zero_hit_content", query_value,
            InvertedIndexQueryType::MATCH_ANY_QUERY, bitmap, &analyzer_ctx);

    ASSERT_TRUE(status.ok()) << status;
    ASSERT_NE(bitmap, nullptr);
    EXPECT_TRUE(bitmap->isEmpty());
}

TEST_F(SniiIndexReaderCountFallback, PublicPhraseQueriesScoreOccurrenceFrequencyWithPlainTerms) {
    const auto provider = make_common_grams_provider();
    ASSERT_NE(provider->common_grams_identity(), nullptr);
    constexpr std::string_view kPathPrefix =
            "./ut_dir/snii_index_reader_count_fallback_test/scoring_phrase";
    assert_ok(write_scoring_segment(kPathPrefix, *provider->common_grams_identity()));
    OpenedSniiIndex opened;
    assert_ok(open_snii_index(&_meta, std::string(kPathPrefix), &opened));

    InvertedIndexAnalyzerCtx analyzer_ctx;
    analyzer_ctx.parser_type = InvertedIndexParserType::PARSER_ENGLISH;
    analyzer_ctx.analyzer_provider = provider;

    QueryExecutionContext exact(/*enable_query_cache=*/true);
    auto exact_stats = std::make_shared<FixedCollectionStatistics>();
    exact.context->collection_statistics = exact_stats;
    exact.context->collection_similarity = std::make_shared<CollectionSimilarity>();
    const Field exact_value = Field::create_field<TYPE_STRING>(std::string("alpha beta"));
    std::shared_ptr<roaring::Roaring> exact_bitmap;
    assert_ok(opened.index_reader->query(exact.context, "scoring_phrase_content", exact_value,
                                         InvertedIndexQueryType::MATCH_PHRASE_QUERY, exact_bitmap,
                                         &analyzer_ctx));
    ASSERT_NE(exact_bitmap, nullptr);
    EXPECT_EQ(bitmap_docids(*exact_bitmap), (std::vector<uint32_t> {1}));
    EXPECT_EQ(exact_stats->idf_terms, (std::vector<std::wstring> {L"alpha", L"beta"}));
    EXPECT_EQ(exact.stats.inverted_index_query_cache_lookup, 0);
    const double expected_exact = doris::snii::query::ScorerContext::from_idf(3.0).score(
            2, doris::snii::query::encode_norm(4), 3.0, doris::snii::query::Bm25Params {});
    EXPECT_FLOAT_EQ(score_for_doc(*exact.context->collection_similarity, 1),
                    static_cast<float>(expected_exact));

    QueryExecutionContext prefix(/*enable_query_cache=*/true);
    auto prefix_stats = std::make_shared<FixedCollectionStatistics>();
    prefix.context->collection_statistics = prefix_stats;
    prefix.context->collection_similarity = std::make_shared<CollectionSimilarity>();
    const Field prefix_value = Field::create_field<TYPE_STRING>(std::string("alpha be"));
    std::shared_ptr<roaring::Roaring> prefix_bitmap;
    assert_ok(opened.index_reader->query(prefix.context, "scoring_phrase_content", prefix_value,
                                         InvertedIndexQueryType::MATCH_PHRASE_PREFIX_QUERY,
                                         prefix_bitmap, &analyzer_ctx));
    ASSERT_NE(prefix_bitmap, nullptr);
    EXPECT_EQ(bitmap_docids(*prefix_bitmap), (std::vector<uint32_t> {1}));
    EXPECT_EQ(prefix_stats->idf_terms, (std::vector<std::wstring> {L"alpha"}));
    EXPECT_EQ(prefix.stats.inverted_index_query_cache_lookup, 0);
    const double expected_prefix = doris::snii::query::ScorerContext::from_idf(1.0).score(
            2, doris::snii::query::encode_norm(4), 3.0, doris::snii::query::Bm25Params {});
    EXPECT_FLOAT_EQ(score_for_doc(*prefix.context->collection_similarity, 1),
                    static_cast<float>(expected_prefix));

    QueryExecutionContext single_prefix(/*enable_query_cache=*/true);
    single_prefix.context->collection_statistics = std::make_shared<FixedCollectionStatistics>();
    single_prefix.context->collection_similarity = std::make_shared<CollectionSimilarity>();
    const Field single_prefix_value = Field::create_field<TYPE_STRING>(std::string("be"));
    std::shared_ptr<roaring::Roaring> single_prefix_bitmap;
    const Status single_prefix_status = opened.index_reader->query(
            single_prefix.context, "scoring_phrase_content", single_prefix_value,
            InvertedIndexQueryType::MATCH_PHRASE_PREFIX_QUERY, single_prefix_bitmap, &analyzer_ctx);
    EXPECT_EQ(single_prefix_status.code(), ErrorCode::INVERTED_INDEX_NOT_SUPPORTED)
            << single_prefix_status;
}

TEST_F(SniiIndexReaderCountFallback, PublicPhraseQueryCacheHitLeavesPrxStatsUnchanged) {
    QueryExecutionContext leader(/*enable_query_cache=*/true);
    Field query_value = Field::create_field<TYPE_STRING>(std::string("failed order"));
    std::shared_ptr<roaring::Roaring> leader_bitmap;
    assert_ok(_index_reader->query(leader.context, "cache_content", query_value,
                                   InvertedIndexQueryType::MATCH_PHRASE_QUERY, leader_bitmap));
    EXPECT_GT(leader.stats.snii_stats.prx_plaintext_bytes, 0);

    QueryExecutionContext cache_hit(/*enable_query_cache=*/true);
    set_prx_stats_sentinel(&cache_hit.stats);
    const PrxStatsSnapshot before = prx_stats_snapshot(cache_hit.stats);
    std::shared_ptr<roaring::Roaring> cached_bitmap;
    assert_ok(_index_reader->query(cache_hit.context, "cache_content", query_value,
                                   InvertedIndexQueryType::MATCH_PHRASE_QUERY, cached_bitmap));

    ASSERT_NE(cached_bitmap, nullptr);
    EXPECT_EQ(bitmap_docids(*cached_bitmap), bitmap_docids(*leader_bitmap));
    EXPECT_EQ(cache_hit.stats.inverted_index_query_cache_hit, 1);
    EXPECT_EQ(prx_stats_snapshot(cache_hit.stats), before);
}

TEST_F(SniiIndexReaderCountFallback, CustomAnalyzerWithNoneParserRetainsAnalyzedTermCache) {
    inverted_index::Settings tokenizer_settings;
    tokenizer_settings.set("tokenize_on_chars", "[whitespace]");
    inverted_index::CustomAnalyzerConfig::Builder builder;
    builder.with_tokenizer_config("char_group", tokenizer_settings);
    builder.add_token_filter_config("lowercase", {});
    auto provider = std::make_shared<inverted_index::CustomAnalyzerProvider>(builder.build());
    ASSERT_FALSE(provider->uses_common_grams());

    InvertedIndexAnalyzerCtx analyzer_ctx;
    analyzer_ctx.analyzer_name = "test_custom_analyzer";
    analyzer_ctx.parser_type = InvertedIndexParserType::PARSER_NONE;
    analyzer_ctx.analyzer_provider = std::move(provider);
    const Field query_value = Field::create_field<TYPE_STRING>(std::string("FAILED ORDER"));

    QueryExecutionContext first(/*enable_query_cache=*/true);
    std::shared_ptr<roaring::Roaring> first_bitmap;
    assert_ok(_index_reader->query(first.context, "non_cg_cache_content", query_value,
                                   InvertedIndexQueryType::MATCH_PHRASE_QUERY, first_bitmap,
                                   &analyzer_ctx));
    ASSERT_NE(first_bitmap, nullptr);
    EXPECT_EQ(bitmap_docids(*first_bitmap), (std::vector<uint32_t> {0, 3, 5}));
    EXPECT_EQ(first.stats.inverted_index_query_cache_lookup, 1);
    EXPECT_EQ(first.stats.inverted_index_query_cache_miss, 1);
    EXPECT_EQ(first.stats.inverted_index_query_cache_insert, 1);

    QueryExecutionContext second(/*enable_query_cache=*/true);
    std::shared_ptr<roaring::Roaring> second_bitmap;
    assert_ok(_index_reader->query(second.context, "non_cg_cache_content", query_value,
                                   InvertedIndexQueryType::MATCH_PHRASE_QUERY, second_bitmap,
                                   &analyzer_ctx));
    ASSERT_NE(second_bitmap, nullptr);
    EXPECT_EQ(bitmap_docids(*second_bitmap), bitmap_docids(*first_bitmap));
    EXPECT_EQ(second.stats.inverted_index_query_cache_lookup, 1);
    EXPECT_EQ(second.stats.inverted_index_query_cache_hit, 1);
    EXPECT_EQ(second.stats.inverted_index_query_cache_miss, 0);
    EXPECT_EQ(second.stats.inverted_index_query_cache_insert, 0);
}

TEST_F(SniiIndexReaderCountFallback, CustomKeywordAnalyzerWithNoneParserNormalizesSingleTerm) {
    inverted_index::CustomAnalyzerConfig::Builder builder;
    builder.with_tokenizer_config("keyword", {});
    builder.add_token_filter_config("lowercase", {});

    InvertedIndexAnalyzerCtx analyzer_ctx;
    analyzer_ctx.analyzer_name = "test_custom_keyword_analyzer";
    analyzer_ctx.parser_type = InvertedIndexParserType::PARSER_NONE;
    analyzer_ctx.analyzer_provider =
            std::make_shared<inverted_index::CustomAnalyzerProvider>(builder.build());

    QueryExecutionContext execution(/*enable_query_cache=*/false);
    const Field query_value = Field::create_field<TYPE_STRING>(std::string("FAILED"));
    std::shared_ptr<roaring::Roaring> bitmap;
    assert_ok(_index_reader->query(execution.context, "content", query_value,
                                   InvertedIndexQueryType::MATCH_ALL_QUERY, bitmap, &analyzer_ctx));

    ASSERT_NE(bitmap, nullptr);
    EXPECT_EQ(bitmap_docids(*bitmap), (std::vector<uint32_t> {0, 1, 2, 3, 4, 5}));
}

TEST_F(SniiIndexReaderCountFallback, NoneParserWithoutAnalyzerUsesRawString) {
    InvertedIndexAnalyzerCtx analyzer_ctx;
    analyzer_ctx.parser_type = InvertedIndexParserType::PARSER_NONE;

    QueryExecutionContext lowercase_execution(/*enable_query_cache=*/false);
    const Field lowercase_query = Field::create_field<TYPE_STRING>(std::string("failed"));
    std::shared_ptr<roaring::Roaring> lowercase_bitmap;
    assert_ok(_index_reader->query(lowercase_execution.context, "content", lowercase_query,
                                   InvertedIndexQueryType::MATCH_ALL_QUERY, lowercase_bitmap,
                                   &analyzer_ctx));
    ASSERT_NE(lowercase_bitmap, nullptr);
    EXPECT_EQ(bitmap_docids(*lowercase_bitmap), (std::vector<uint32_t> {0, 1, 2, 3, 4, 5}));

    QueryExecutionContext uppercase_execution(/*enable_query_cache=*/false);
    const Field uppercase_query = Field::create_field<TYPE_STRING>(std::string("FAILED"));
    std::shared_ptr<roaring::Roaring> uppercase_bitmap;
    assert_ok(_index_reader->query(uppercase_execution.context, "content", uppercase_query,
                                   InvertedIndexQueryType::MATCH_ALL_QUERY, uppercase_bitmap,
                                   &analyzer_ctx));
    ASSERT_NE(uppercase_bitmap, nullptr);
    EXPECT_TRUE(uppercase_bitmap->isEmpty());
}

TEST_F(SniiIndexReaderCountFallback, PublicQueryWithCacheDisabledDoesNotLookupOrInsert) {
    Field query_value = Field::create_field<TYPE_STRING>(std::string("failed order"));

    QueryExecutionContext disabled(/*enable_query_cache=*/false);
    std::shared_ptr<roaring::Roaring> disabled_bitmap;
    assert_ok(_index_reader->query(disabled.context, "disabled_cache_content", query_value,
                                   InvertedIndexQueryType::MATCH_PHRASE_QUERY, disabled_bitmap));

    ASSERT_NE(disabled_bitmap, nullptr);
    EXPECT_EQ(disabled.stats.inverted_index_query_cache_lookup, 0);
    EXPECT_EQ(disabled.stats.inverted_index_query_cache_hit, 0);
    EXPECT_EQ(disabled.stats.inverted_index_query_cache_miss, 0);
    EXPECT_EQ(disabled.stats.inverted_index_query_cache_insert, 0);

    QueryExecutionContext enabled_miss(/*enable_query_cache=*/true);
    std::shared_ptr<roaring::Roaring> enabled_bitmap;
    assert_ok(_index_reader->query(enabled_miss.context, "disabled_cache_content", query_value,
                                   InvertedIndexQueryType::MATCH_PHRASE_QUERY, enabled_bitmap));

    ASSERT_NE(enabled_bitmap, nullptr);
    EXPECT_EQ(bitmap_docids(*enabled_bitmap), bitmap_docids(*disabled_bitmap));
    EXPECT_EQ(enabled_miss.stats.inverted_index_query_cache_lookup, 1);
    EXPECT_EQ(enabled_miss.stats.inverted_index_query_cache_hit, 0);
    EXPECT_EQ(enabled_miss.stats.inverted_index_query_cache_miss, 1);
    EXPECT_EQ(enabled_miss.stats.inverted_index_query_cache_insert, 1);

    QueryExecutionContext enabled_hit(/*enable_query_cache=*/true);
    std::shared_ptr<roaring::Roaring> cached_bitmap;
    assert_ok(_index_reader->query(enabled_hit.context, "disabled_cache_content", query_value,
                                   InvertedIndexQueryType::MATCH_PHRASE_QUERY, cached_bitmap));

    ASSERT_NE(cached_bitmap, nullptr);
    EXPECT_EQ(bitmap_docids(*cached_bitmap), bitmap_docids(*disabled_bitmap));
    EXPECT_EQ(enabled_hit.stats.inverted_index_query_cache_lookup, 1);
    EXPECT_EQ(enabled_hit.stats.inverted_index_query_cache_hit, 1);
    EXPECT_EQ(enabled_hit.stats.inverted_index_query_cache_miss, 0);
    EXPECT_EQ(enabled_hit.stats.inverted_index_query_cache_insert, 0);
}

TEST_F(SniiIndexReaderCountFallback,
       AuthoritativeEmptyCacheIsBypassedWhenKillSwitchForcesPlainPlan) {
    const bool original_enabled = config::enable_common_grams_query_plan;
    Defer restore_config([original_enabled] {
        EXPECT_TRUE(config::set_config("enable_common_grams_query_plan",
                                       original_enabled ? "true" : "false",
                                       /*need_persist=*/false)
                            .ok());
    });
    ASSERT_TRUE(config::set_config("enable_common_grams_query_plan", "true",
                                   /*need_persist=*/false)
                        .ok());

    const auto provider = make_common_grams_provider();
    ASSERT_NE(provider->common_grams_identity(), nullptr);
    constexpr std::string_view kPathPrefix =
            "./ut_dir/snii_index_reader_count_fallback_test/authoritative_empty";
    assert_ok(write_common_grams_segment(kPathPrefix, *provider->common_grams_identity(),
                                         inverted_index::CommonGramsCoverage::kComplete,
                                         /*include_gram=*/false));
    OpenedSniiIndex opened;
    assert_ok(open_snii_index(&_meta, std::string(kPathPrefix), &opened));

    InvertedIndexAnalyzerCtx analyzer_ctx;
    analyzer_ctx.parser_type = InvertedIndexParserType::PARSER_ENGLISH;
    analyzer_ctx.analyzer_provider = provider;
    const Field query_value = Field::create_field<TYPE_STRING>(std::string("the wolf"));

    QueryExecutionContext enabled_miss(/*enable_query_cache=*/true);
    std::shared_ptr<roaring::Roaring> empty_bitmap;
    assert_ok(opened.index_reader->query(enabled_miss.context, "authoritative_content", query_value,
                                         InvertedIndexQueryType::MATCH_PHRASE_QUERY, empty_bitmap,
                                         &analyzer_ctx));
    ASSERT_NE(empty_bitmap, nullptr);
    EXPECT_TRUE(empty_bitmap->isEmpty());
    EXPECT_EQ(enabled_miss.stats.snii_stats.common_grams_candidate_queries, 1);
    EXPECT_EQ(enabled_miss.stats.snii_stats.common_grams_gram_plans, 1);
    EXPECT_EQ(enabled_miss.stats.snii_stats.common_grams_authoritative_empty, 1);
    EXPECT_EQ(enabled_miss.stats.inverted_index_query_cache_insert, 1);

    QueryExecutionContext enabled_hit(/*enable_query_cache=*/true);
    std::shared_ptr<roaring::Roaring> cached_empty_bitmap;
    assert_ok(opened.index_reader->query(enabled_hit.context, "authoritative_content", query_value,
                                         InvertedIndexQueryType::MATCH_PHRASE_QUERY,
                                         cached_empty_bitmap, &analyzer_ctx));
    ASSERT_NE(cached_empty_bitmap, nullptr);
    EXPECT_TRUE(cached_empty_bitmap->isEmpty());
    EXPECT_EQ(enabled_hit.stats.inverted_index_query_cache_hit, 1);
    EXPECT_EQ(enabled_hit.stats.snii_stats.common_grams_candidate_queries, 0);

    ASSERT_TRUE(config::set_config("enable_common_grams_query_plan", "false",
                                   /*need_persist=*/false)
                        .ok());
    QueryExecutionContext disabled(/*enable_query_cache=*/true);
    std::shared_ptr<roaring::Roaring> plain_bitmap;
    assert_ok(opened.index_reader->query(disabled.context, "authoritative_content", query_value,
                                         InvertedIndexQueryType::MATCH_PHRASE_QUERY, plain_bitmap,
                                         &analyzer_ctx));
    ASSERT_NE(plain_bitmap, nullptr);
    EXPECT_EQ(bitmap_docids(*plain_bitmap), (std::vector<uint32_t> {0}));
    EXPECT_EQ(disabled.stats.inverted_index_query_cache_lookup, 0);
    EXPECT_EQ(disabled.stats.inverted_index_query_cache_hit, 0);
    EXPECT_EQ(disabled.stats.inverted_index_query_cache_miss, 0);
    EXPECT_EQ(disabled.stats.inverted_index_query_cache_insert, 0);
    EXPECT_EQ(disabled.stats.snii_stats.common_grams_candidate_queries, 1);
    EXPECT_EQ(disabled.stats.snii_stats.common_grams_plain_plans, 1);
    EXPECT_EQ(disabled.stats.snii_stats.common_grams_fallback_kill_switch, 1);
}

TEST_F(SniiIndexReaderCountFallback, KillSwitchBypassesCachedGramResultBeforeSegmentAnalysis) {
    const bool original_enabled = config::enable_common_grams_query_plan;
    Defer restore_config([original_enabled] {
        EXPECT_TRUE(config::set_config("enable_common_grams_query_plan",
                                       original_enabled ? "true" : "false",
                                       /*need_persist=*/false)
                            .ok());
    });
    ASSERT_TRUE(config::set_config("enable_common_grams_query_plan", "true",
                                   /*need_persist=*/false)
                        .ok());

    const auto provider = make_common_grams_provider();
    ASSERT_NE(provider->common_grams_identity(), nullptr);
    constexpr std::string_view kPathPrefix =
            "./ut_dir/snii_index_reader_count_fallback_test/query_safety_fence";
    assert_ok(write_common_grams_segment(kPathPrefix, *provider->common_grams_identity(),
                                         inverted_index::CommonGramsCoverage::kComplete,
                                         /*include_gram=*/false));
    OpenedSniiIndex opened;
    assert_ok(open_snii_index(&_meta, std::string(kPathPrefix), &opened));
    std::atomic<uint32_t> single_flight_leader_calls {0};
    opened.index_reader->set_single_flight_leader_before_compute_observer_for_test(
            record_single_flight_leader, &single_flight_leader_calls);

    InvertedIndexAnalyzerCtx analyzer_ctx;
    analyzer_ctx.parser_type = InvertedIndexParserType::PARSER_ENGLISH;
    analyzer_ctx.analyzer_provider = provider;
    const Field query_value = Field::create_field<TYPE_STRING>(std::string("the wolf"));

    QueryExecutionContext admitted(/*enable_query_cache=*/true);
    std::shared_ptr<roaring::Roaring> authoritative_empty;
    assert_ok(opened.index_reader->query(admitted.context, "safety_content", query_value,
                                         InvertedIndexQueryType::MATCH_PHRASE_QUERY,
                                         authoritative_empty, &analyzer_ctx));
    ASSERT_NE(authoritative_empty, nullptr);
    EXPECT_TRUE(authoritative_empty->isEmpty());
    EXPECT_EQ(admitted.stats.inverted_index_query_cache_insert, 1);
    EXPECT_EQ(admitted.stats.snii_stats.common_grams_gram_plans, 1);

    const auto plain_provider = make_plain_provider();
    ASSERT_EQ(plain_provider->base_analyzer_fingerprint(), provider->base_analyzer_fingerprint());
    ASSERT_FALSE(plain_provider->uses_common_grams());
    InvertedIndexAnalyzerCtx plain_analyzer_ctx = analyzer_ctx;
    plain_analyzer_ctx.analyzer_provider = plain_provider;
    // Disabling the BE switch selects a distinct cache-key mode and forces the plain path.
    ASSERT_TRUE(config::set_config("enable_common_grams_query_plan", "false",
                                   /*need_persist=*/false)
                        .ok());

    QueryExecutionContext analyzer_mismatch_disabled(/*enable_query_cache=*/true);
    std::shared_ptr<roaring::Roaring> analyzer_mismatch_bitmap;
    assert_ok(opened.index_reader->query(analyzer_mismatch_disabled.context, "safety_content",
                                         query_value, InvertedIndexQueryType::MATCH_PHRASE_QUERY,
                                         analyzer_mismatch_bitmap, &plain_analyzer_ctx));
    ASSERT_NE(analyzer_mismatch_bitmap, nullptr);
    EXPECT_EQ(bitmap_docids(*analyzer_mismatch_bitmap), (std::vector<uint32_t> {0}));
    EXPECT_EQ(analyzer_mismatch_disabled.stats.inverted_index_query_cache_lookup, 0);
    EXPECT_EQ(analyzer_mismatch_disabled.stats.inverted_index_query_cache_insert, 0);

    QueryExecutionContext forced_plain(/*enable_query_cache=*/true);
    std::shared_ptr<roaring::Roaring> plain_bitmap;
    assert_ok(opened.index_reader->query(forced_plain.context, "safety_content", query_value,
                                         InvertedIndexQueryType::MATCH_PHRASE_QUERY, plain_bitmap,
                                         &analyzer_ctx));
    ASSERT_NE(plain_bitmap, nullptr);
    EXPECT_EQ(bitmap_docids(*plain_bitmap), (std::vector<uint32_t> {0}));
    EXPECT_EQ(forced_plain.stats.inverted_index_query_cache_lookup, 0);
    EXPECT_EQ(forced_plain.stats.inverted_index_query_cache_insert, 0);
    EXPECT_EQ(forced_plain.stats.snii_stats.common_grams_plain_plans, 1);
    EXPECT_EQ(forced_plain.stats.snii_stats.common_grams_fallback_kill_switch, 1);
    EXPECT_EQ(single_flight_leader_calls.load(std::memory_order_relaxed), 1);

    ASSERT_TRUE(config::set_config("enable_common_grams_query_plan", "true",
                                   /*need_persist=*/false)
                        .ok());
    QueryExecutionContext readmitted(/*enable_query_cache=*/true);
    std::shared_ptr<roaring::Roaring> readmitted_bitmap;
    assert_ok(opened.index_reader->query(readmitted.context, "safety_content", query_value,
                                         InvertedIndexQueryType::MATCH_PHRASE_QUERY,
                                         readmitted_bitmap, &analyzer_ctx));
    ASSERT_NE(readmitted_bitmap, nullptr);
    EXPECT_TRUE(readmitted_bitmap->isEmpty());
    EXPECT_EQ(readmitted.stats.inverted_index_query_cache_lookup, 1);
    EXPECT_EQ(readmitted.stats.inverted_index_query_cache_hit, 1);
    EXPECT_EQ(readmitted.stats.inverted_index_query_cache_miss, 0);
    EXPECT_EQ(readmitted.stats.inverted_index_query_cache_insert, 0);
    EXPECT_EQ(single_flight_leader_calls.load(std::memory_order_relaxed), 1);
    opened.index_reader->set_single_flight_leader_before_compute_observer_for_test(nullptr,
                                                                                   nullptr);
}

TEST_F(SniiIndexReaderCountFallback, PublicPlannerRecordsEveryPlainFallbackReason) {
    const bool original_enabled = config::enable_common_grams_query_plan;
    const int32_t original_ratio = config::common_grams_plan_cost_ratio_percent;
    Defer restore_config([original_enabled, original_ratio] {
        EXPECT_TRUE(config::set_config("enable_common_grams_query_plan",
                                       original_enabled ? "true" : "false",
                                       /*need_persist=*/false)
                            .ok());
        EXPECT_TRUE(config::set_config("common_grams_plan_cost_ratio_percent",
                                       std::to_string(original_ratio),
                                       /*need_persist=*/false)
                            .ok());
    });
    ASSERT_TRUE(config::set_config("enable_common_grams_query_plan", "true",
                                   /*need_persist=*/false)
                        .ok());

    const auto provider = make_common_grams_provider();
    ASSERT_NE(provider->common_grams_identity(), nullptr);
    InvertedIndexAnalyzerCtx analyzer_ctx;
    analyzer_ctx.parser_type = InvertedIndexParserType::PARSER_ENGLISH;
    analyzer_ctx.analyzer_provider = provider;

    const auto run_query = [&](std::string_view path_suffix,
                               inverted_index::CommonGramsCoverage coverage, bool include_gram,
                               std::string query, CommonGramsCounterSnapshot* counters,
                               std::vector<uint32_t>* docids) -> Status {
        const std::string path_prefix = std::string(kTestDir) + "/" + std::string(path_suffix);
        RETURN_IF_ERROR(write_common_grams_segment(path_prefix, *provider->common_grams_identity(),
                                                   coverage, include_gram));
        OpenedSniiIndex opened;
        RETURN_IF_ERROR(open_snii_index(&_meta, path_prefix, &opened));
        QueryExecutionContext execution(/*enable_query_cache=*/false);
        std::shared_ptr<roaring::Roaring> bitmap;
        const Field query_value = Field::create_field<TYPE_STRING>(std::move(query));
        RETURN_IF_ERROR(opened.index_reader->query(
                execution.context, "fallback_content", query_value,
                InvertedIndexQueryType::MATCH_PHRASE_QUERY, bitmap, &analyzer_ctx));
        DORIS_CHECK(bitmap != nullptr);
        *counters = common_grams_counter_snapshot(execution.stats);
        *docids = bitmap_docids(*bitmap);
        return Status::OK();
    };

    CommonGramsCounterSnapshot no_gram;
    std::vector<uint32_t> no_gram_docids;
    assert_ok(run_query("no_gram", inverted_index::CommonGramsCoverage::kComplete,
                        /*include_gram=*/false, "alpha beta", &no_gram, &no_gram_docids));
    EXPECT_EQ(no_gram_docids, (std::vector<uint32_t> {1}));
    EXPECT_EQ(no_gram.candidate_queries, 1);
    EXPECT_EQ(no_gram.plain_plans, 1);
    EXPECT_EQ(no_gram.fallback_no_gram, 1);
    EXPECT_EQ(no_gram.fallback_incompatible, 0);
    EXPECT_EQ(no_gram.fallback_cost, 0);

    CommonGramsCounterSnapshot incompatible;
    std::vector<uint32_t> incompatible_docids;
    assert_ok(run_query("incompatible", inverted_index::CommonGramsCoverage::kMixed,
                        /*include_gram=*/false, "the wolf", &incompatible, &incompatible_docids));
    EXPECT_EQ(incompatible_docids, (std::vector<uint32_t> {0}));
    EXPECT_EQ(incompatible.candidate_queries, 1);
    EXPECT_EQ(incompatible.plain_plans, 1);
    EXPECT_EQ(incompatible.fallback_no_gram, 0);
    EXPECT_EQ(incompatible.fallback_incompatible, 1);
    EXPECT_EQ(incompatible.fallback_cost, 0);

    ASSERT_TRUE(config::set_config("common_grams_plan_cost_ratio_percent", "0",
                                   /*need_persist=*/false)
                        .ok());
    CommonGramsCounterSnapshot cost;
    std::vector<uint32_t> cost_docids;
    assert_ok(run_query("cost", inverted_index::CommonGramsCoverage::kComplete,
                        /*include_gram=*/true, "the wolf", &cost, &cost_docids));
    EXPECT_EQ(cost_docids, (std::vector<uint32_t> {0}));
    EXPECT_EQ(cost.candidate_queries, 1);
    EXPECT_EQ(cost.plain_plans, 1);
    EXPECT_EQ(cost.fallback_no_gram, 0);
    EXPECT_EQ(cost.fallback_incompatible, 0);
    EXPECT_EQ(cost.fallback_cost, 1);
}

TEST_F(SniiIndexReaderCountFallback, DebugForceGramCannotBypassProcessKillSwitch) {
    const bool original_enabled = config::enable_common_grams_query_plan;
    const bool original_enable_debug_points = config::enable_debug_points;
    Defer restore_state([original_enabled, original_enable_debug_points] {
        DebugPoints::instance()->remove("snii.common_grams.force_gram_plan");
        config::enable_debug_points = original_enable_debug_points;
        EXPECT_TRUE(config::set_config("enable_common_grams_query_plan",
                                       original_enabled ? "true" : "false",
                                       /*need_persist=*/false)
                            .ok());
    });
    ASSERT_TRUE(config::set_config("enable_common_grams_query_plan", "true",
                                   /*need_persist=*/false)
                        .ok());

    const auto provider = make_common_grams_provider();
    ASSERT_NE(provider->common_grams_identity(), nullptr);
    constexpr std::string_view kPathPrefix =
            "./ut_dir/snii_index_reader_count_fallback_test/debug_plan_safety";
    assert_ok(write_common_grams_segment(kPathPrefix, *provider->common_grams_identity(),
                                         inverted_index::CommonGramsCoverage::kComplete,
                                         /*include_gram=*/true));
    OpenedSniiIndex opened;
    assert_ok(open_snii_index(&_meta, std::string(kPathPrefix), &opened));
    InvertedIndexAnalyzerCtx analyzer_ctx;
    analyzer_ctx.parser_type = InvertedIndexParserType::PARSER_ENGLISH;
    analyzer_ctx.analyzer_provider = provider;

    config::enable_debug_points = true;
    DebugPoints::instance()->add("snii.common_grams.force_gram_plan");

    const Field exact_query = Field::create_field<TYPE_STRING>(std::string("the wolf"));
    const Field prefix_query = Field::create_field<TYPE_STRING>(std::string("the wo"));
    const auto assert_forced_plain = [&]() {
        QueryExecutionContext exact_execution(
                /*enable_query_cache=*/false, /*count_on_index_fastpath=*/false);
        std::shared_ptr<roaring::Roaring> exact_bitmap;
        assert_ok(opened.index_reader->query(
                exact_execution.context, "debug_safety_content", exact_query,
                InvertedIndexQueryType::MATCH_PHRASE_QUERY, exact_bitmap, &analyzer_ctx));
        ASSERT_NE(exact_bitmap, nullptr);
        EXPECT_EQ(bitmap_docids(*exact_bitmap), (std::vector<uint32_t> {0}));
        EXPECT_EQ(exact_execution.stats.snii_stats.common_grams_plain_plans, 1);
        EXPECT_EQ(exact_execution.stats.snii_stats.common_grams_gram_plans, 0);
        EXPECT_EQ(exact_execution.stats.snii_stats.common_grams_fallback_kill_switch, 1);

        QueryExecutionContext prefix_execution(
                /*enable_query_cache=*/false, /*count_on_index_fastpath=*/false);
        std::shared_ptr<roaring::Roaring> prefix_bitmap;
        assert_ok(opened.index_reader->query(
                prefix_execution.context, "debug_safety_content", prefix_query,
                InvertedIndexQueryType::MATCH_PHRASE_PREFIX_QUERY, prefix_bitmap, &analyzer_ctx));
        ASSERT_NE(prefix_bitmap, nullptr);
        EXPECT_EQ(bitmap_docids(*prefix_bitmap), (std::vector<uint32_t> {0}));
        EXPECT_EQ(prefix_execution.stats.snii_stats.common_grams_plain_plans, 1);
        EXPECT_EQ(prefix_execution.stats.snii_stats.common_grams_gram_plans, 0);
        EXPECT_EQ(prefix_execution.stats.snii_stats.common_grams_fallback_kill_switch, 1);
    };

    ASSERT_TRUE(config::set_config("enable_common_grams_query_plan", "false",
                                   /*need_persist=*/false)
                        .ok());
    assert_forced_plain();
}

// A plain-only pair is proven gram-free by the wordset pre-proof, so the plan
// decision cache is legitimately never consulted (miss stays 0) -- but the plain
// posting IO is NOT bypassed: the injected adapter failure must surface as the
// query error and nothing may be published.
// GTest assertion macros inflate clang-tidy's branch count; both scenarios below are linear.
// NOLINTNEXTLINE(readability-function-cognitive-complexity)
TEST_F(SniiIndexReaderCountFallback, PublicSingleTermCountFastPathLeavesPrxStatsUnchanged) {
    const auto verify_query = [&](std::string query) {
        QueryExecutionContext execution(/*enable_query_cache=*/false,
                                        /*count_on_index_fastpath=*/true);
        set_prx_stats_sentinel(&execution.stats);
        const PrxStatsSnapshot before = prx_stats_snapshot(execution.stats);
        Field query_value = Field::create_field<TYPE_STRING>(std::move(query));
        std::shared_ptr<roaring::Roaring> bitmap;

        assert_ok(_index_reader->query(execution.context, "count_content", query_value,
                                       InvertedIndexQueryType::MATCH_PHRASE_QUERY, bitmap));

        DORIS_CHECK(bitmap != nullptr);
        EXPECT_EQ(bitmap->cardinality(), 6U);
        EXPECT_TRUE(execution.context->count_on_index_fastpath_hit);
        EXPECT_EQ(prx_stats_snapshot(execution.stats), before);
    };

    verify_query("failed");
    verify_query("failed ~1");
}

TEST_F(SniiIndexReaderCountFallback, CountFastPathPublishesHitAfterRequestedNullBitmap) {
    const std::string path = std::string(kTestDir) + "/count_null_failure";
    assert_ok(write_nullable_phrase_segment(path, /*has_null=*/true, kIndexId,
                                            /*corrupt_null=*/true));
    OpenedSniiIndex opened;
    assert_ok(open_snii_index(&_meta, path, &opened));
    QueryExecutionContext execution(/*enable_query_cache=*/false,
                                    /*count_on_index_fastpath=*/true);
    const Field query_value = Field::create_field<TYPE_STRING>(std::string("missing"));
    std::shared_ptr<roaring::Roaring> bitmap;
    InvertedIndexQueryCacheHandle null_bitmap_cache_handle;

    const Status status = opened.index_reader->query_with_null_bitmap(
            execution.context, "count_content", query_value,
            InvertedIndexQueryType::MATCH_PHRASE_QUERY, bitmap, &null_bitmap_cache_handle);

    EXPECT_FALSE(status.ok()) << status.to_string();
    EXPECT_FALSE(execution.context->count_on_index_fastpath_hit);
}

TEST_F(SniiIndexReaderCountFallback, ConcurrentCountShapedResultsNeverJoinRowAccurateSingleFlight) {
    Field query_value = Field::create_field<TYPE_STRING>(std::string("order"));

    QueryExecutionContext baseline_row(/*enable_query_cache=*/false);
    std::shared_ptr<roaring::Roaring> row_bitmap;
    assert_ok(_index_reader->query(baseline_row.context, "g02_content", query_value,
                                   InvertedIndexQueryType::MATCH_PHRASE_QUERY, row_bitmap));
    ASSERT_NE(row_bitmap, nullptr);
    const auto expected_row_docids = bitmap_docids(*row_bitmap);

    QueryExecutionContext baseline_count(/*enable_query_cache=*/false,
                                         /*count_on_index_fastpath=*/true);
    std::shared_ptr<roaring::Roaring> count_bitmap;
    assert_ok(_index_reader->query(baseline_count.context, "g02_content", query_value,
                                   InvertedIndexQueryType::MATCH_PHRASE_QUERY, count_bitmap));
    ASSERT_NE(count_bitmap, nullptr);
    ASSERT_TRUE(baseline_count.context->count_on_index_fastpath_hit);
    const auto expected_count_docids = bitmap_docids(*count_bitmap);

    constexpr size_t kLeader = 0;
    constexpr size_t kCountConsumer = 1;
    constexpr size_t kRowFollower = 2;
    constexpr size_t kConsumerCount = 3;
    std::array<uint8_t, kConsumerCount> status_ok {};
    std::array<uint8_t, kConsumerCount> count_fastpath_hit {};
    std::array<std::vector<uint32_t>, kConsumerCount> docids;
    std::binary_semaphore row_follower_joined {0};
    SingleFlightFollowerCounts follower_counts;
    follower_counts.row_follower_joined = &row_follower_joined;
    SingleFlightLeaderGate leader_gate;
    _index_reader->set_single_flight_follower_joined_observer_for_test(
            record_single_flight_follower, &follower_counts);
    _index_reader->set_single_flight_leader_before_compute_observer_for_test(
            block_single_flight_leader_before_compute, &leader_gate);

    auto run_query = [&](size_t consumer, bool count_consumer) {
        SCOPED_INIT_THREAD_CONTEXT();
        is_count_consumer = count_consumer;
        QueryExecutionContext execution(/*enable_query_cache=*/false,
                                        /*count_on_index_fastpath=*/count_consumer);
        std::shared_ptr<roaring::Roaring> bitmap;
        const Status status =
                _index_reader->query(execution.context, "g02_content", query_value,
                                     InvertedIndexQueryType::MATCH_PHRASE_QUERY, bitmap);
        status_ok[consumer] = status.ok();
        count_fastpath_hit[consumer] = execution.context->count_on_index_fastpath_hit;
        if (bitmap != nullptr) {
            docids[consumer] = bitmap_docids(*bitmap);
        }
        is_count_consumer = false;
    };

    std::vector<std::thread> consumers;
    consumers.emplace_back(run_query, kLeader, false);
    leader_gate.leader_entered.acquire();
    consumers.emplace_back(run_query, kCountConsumer, true);
    consumers.emplace_back(run_query, kRowFollower, false);
    const bool row_joined_while_leader_blocked =
            row_follower_joined.try_acquire_for(std::chrono::seconds(5));
    leader_gate.release_leader.release();
    for (auto& consumer : consumers) {
        consumer.join();
    }
    _index_reader->set_single_flight_follower_joined_observer_for_test(nullptr, nullptr);
    _index_reader->set_single_flight_leader_before_compute_observer_for_test(nullptr, nullptr);

    EXPECT_TRUE(row_joined_while_leader_blocked);
    ASSERT_TRUE(status_ok[kLeader]);
    EXPECT_FALSE(count_fastpath_hit[kLeader]);
    EXPECT_EQ(docids[kLeader], expected_row_docids);
    ASSERT_TRUE(status_ok[kCountConsumer]);
    EXPECT_TRUE(count_fastpath_hit[kCountConsumer]);
    EXPECT_EQ(docids[kCountConsumer], expected_count_docids);
    ASSERT_TRUE(status_ok[kRowFollower]);
    EXPECT_FALSE(count_fastpath_hit[kRowFollower]);
    EXPECT_EQ(docids[kRowFollower], expected_row_docids);
    EXPECT_EQ(follower_counts.count_consumers.load(std::memory_order_relaxed), 0U);
    EXPECT_EQ(follower_counts.row_consumers.load(std::memory_order_relaxed), 1U);
}

TEST_F(SniiIndexReaderCountFallback, DistinctRawQueriesDoNotShareSingleFlight) {
    const std::string lower_query = "failed order";
    const std::string upper_query = "FAILED ORDER";
    const auto index_file_key = _file_reader->get_index_file_cache_key(&_meta);
    const InvertedIndexRawQuerySemantic lower_semantic {
            .raw_query_bytes = lower_query,
            .query_type = InvertedIndexQueryType::MATCH_PHRASE_QUERY};
    const InvertedIndexRawQuerySemantic upper_semantic {
            .raw_query_bytes = upper_query,
            .query_type = InvertedIndexQueryType::MATCH_PHRASE_QUERY};
    const InvertedIndexQueryCache::CacheKey lower_key {index_file_key, "raw_query_content",
                                                       InvertedIndexQueryType::MATCH_PHRASE_QUERY,
                                                       lower_semantic.encode()};
    const InvertedIndexQueryCache::CacheKey upper_key {index_file_key, "raw_query_content",
                                                       InvertedIndexQueryType::MATCH_PHRASE_QUERY,
                                                       upper_semantic.encode()};
    ASSERT_NE(lower_key.encode(), upper_key.encode());

    std::latch ready(2);
    std::latch start(1);
    std::array<uint8_t, 2> status_ok {};
    std::array<std::vector<uint32_t>, 2> docids;
    SingleFlightFollowerCounts follower_counts;
    _index_reader->set_single_flight_follower_joined_observer_for_test(
            record_single_flight_follower, &follower_counts);
    std::array<std::thread, 2> consumers {
            std::thread([&] {
                SCOPED_INIT_THREAD_CONTEXT();
                QueryExecutionContext execution(/*enable_query_cache=*/false);
                Field query_value = Field::create_field<TYPE_STRING>(lower_query);
                std::shared_ptr<roaring::Roaring> bitmap;
                ready.count_down();
                start.wait();
                const Status status =
                        _index_reader->query(execution.context, "raw_query_content", query_value,
                                             InvertedIndexQueryType::MATCH_PHRASE_QUERY, bitmap);
                status_ok[0] = status.ok();
                if (bitmap != nullptr) {
                    docids[0] = bitmap_docids(*bitmap);
                }
            }),
            std::thread([&] {
                SCOPED_INIT_THREAD_CONTEXT();
                QueryExecutionContext execution(/*enable_query_cache=*/false);
                Field query_value = Field::create_field<TYPE_STRING>(upper_query);
                std::shared_ptr<roaring::Roaring> bitmap;
                ready.count_down();
                start.wait();
                const Status status =
                        _index_reader->query(execution.context, "raw_query_content", query_value,
                                             InvertedIndexQueryType::MATCH_PHRASE_QUERY, bitmap);
                status_ok[1] = status.ok();
                if (bitmap != nullptr) {
                    docids[1] = bitmap_docids(*bitmap);
                }
            })};
    ready.wait();
    start.count_down();
    for (auto& consumer : consumers) {
        consumer.join();
    }
    _index_reader->set_single_flight_follower_joined_observer_for_test(nullptr, nullptr);

    ASSERT_TRUE(status_ok[0]);
    ASSERT_TRUE(status_ok[1]);
    EXPECT_EQ(docids[0], docids[1]);
    EXPECT_EQ(follower_counts.count_consumers.load(std::memory_order_relaxed), 0U);
    EXPECT_EQ(follower_counts.row_consumers.load(std::memory_order_relaxed), 0U);
}

TEST_F(SniiIndexReaderCountFallback, PrxProfileScopeOnlyExistsForMultiTermPhraseQueries) {
    struct ControlQuery {
        InvertedIndexQueryType type;
        std::string search_str;
        std::vector<std::string> terms;
    };
    const std::array<ControlQuery, 7> controls {{
            {InvertedIndexQueryType::EQUAL_QUERY, "failed", {"failed"}},
            {InvertedIndexQueryType::MATCH_ANY_QUERY, "failed order", {"failed", "order"}},
            {InvertedIndexQueryType::MATCH_ALL_QUERY, "failed order", {"failed", "order"}},
            {InvertedIndexQueryType::MATCH_PHRASE_QUERY, "failed", {"failed"}},
            {InvertedIndexQueryType::MATCH_PHRASE_PREFIX_QUERY, "fail", {"fail"}},
            {InvertedIndexQueryType::MATCH_REGEXP_QUERY, "fail.*", {}},
            {InvertedIndexQueryType::WILDCARD_QUERY, "fail*", {}},
    }};
    QueryExecutionContext execution(/*enable_query_cache=*/false);
    InvertedIndexQueryInfo query_info;
    const auto set_plain_terms = [&query_info](const std::vector<std::string>& terms) {
        query_info.term_infos.clear();
        query_info.term_infos.reserve(terms.size());
        int32_t position = 0;
        for (const auto& term : terms) {
            query_info.term_infos.emplace_back(term, position++);
        }
    };

    doris::snii::testing::reset_prx_execution_profile_scope_counters();
    doris::snii::query::testing::reset_query_profile_clock_read_count();
    doris::snii::format::testing::reset_prx_clock_read_count();
    doris::snii::query::internal::testing::reset_phrase_verify_clock_read_count();
    for (const auto& control : controls) {
        SCOPED_TRACE(query_type_to_string(control.type));
        set_plain_terms(control.terms);
        auto terms = control.terms;
        std::shared_ptr<roaring::Roaring> bitmap;
        assert_ok(_index_reader->_compute_query_bitmap(execution.context, control.type, query_info,
                                                       control.search_str, &terms, 50, &bitmap));
        ASSERT_NE(bitmap, nullptr);
    }
    EXPECT_EQ(doris::snii::testing::prx_execution_profile_scope_construction_count(), 0U);
    EXPECT_EQ(doris::snii::testing::prx_execution_profile_scope_flush_count(), 0U);
    EXPECT_EQ(doris::snii::query::testing::query_profile_clock_read_count(), 0U);
    EXPECT_EQ(doris::snii::format::testing::prx_clock_read_count(), 0U);
    EXPECT_EQ(doris::snii::query::internal::testing::phrase_verify_clock_read_count(), 0U);

    const auto run_profiled_query = [&](InvertedIndexQueryType type, std::string_view search_str,
                                        std::vector<std::string> terms) {
        set_plain_terms(terms);
        std::shared_ptr<roaring::Roaring> bitmap;
        assert_ok(_index_reader->_compute_query_bitmap(execution.context, type, query_info,
                                                       search_str, &terms, 50, &bitmap));
        ASSERT_NE(bitmap, nullptr);
    };
    run_profiled_query(InvertedIndexQueryType::MATCH_PHRASE_QUERY, "failed order",
                       {"failed", "order"});
    query_info.slop = 1;
    run_profiled_query(InvertedIndexQueryType::MATCH_PHRASE_QUERY, "failed warehouse",
                       {"failed", "warehouse"});
    query_info.slop = 0;
    run_profiled_query(InvertedIndexQueryType::MATCH_PHRASE_PREFIX_QUERY, "failed ord",
                       {"failed", "ord"});

    EXPECT_EQ(doris::snii::testing::prx_execution_profile_scope_construction_count(), 3U);
    EXPECT_EQ(doris::snii::testing::prx_execution_profile_scope_flush_count(), 3U);
    EXPECT_GT(doris::snii::query::testing::query_profile_clock_read_count(), 0U);
    EXPECT_GT(doris::snii::format::testing::prx_clock_read_count(), 0U);
    EXPECT_GT(doris::snii::query::internal::testing::phrase_verify_clock_read_count(), 0U);
}

TEST_F(SniiIndexReaderCountFallback, SloppyPhraseMatchesTermsSeparatedByOnePosition) {
    QueryExecutionContext execution(/*enable_query_cache=*/false);
    InvertedIndexQueryInfo query_info;
    query_info.term_infos.emplace_back("failed", 0);
    query_info.term_infos.emplace_back("warehouse", 1);
    query_info.slop = 1;

    std::vector<std::string> terms {"failed", "warehouse"};
    std::shared_ptr<roaring::Roaring> bitmap;
    assert_ok(_index_reader->_compute_query_bitmap(
            execution.context, InvertedIndexQueryType::MATCH_PHRASE_QUERY, query_info,
            "failed warehouse", &terms, 50, &bitmap));

    ASSERT_NE(bitmap, nullptr);
    EXPECT_EQ(bitmap_docids(*bitmap), (std::vector<uint32_t> {2}));
}

TEST_F(SniiIndexReaderCountFallback, OrderedSloppyPhraseRejectsOutOfOrderPositions) {
    QueryExecutionContext execution(/*enable_query_cache=*/false);
    InvertedIndexQueryInfo query_info;
    query_info.term_infos.emplace_back("failed", 0);
    query_info.term_infos.emplace_back("order", 1);
    query_info.slop = 1;
    query_info.ordered = true;

    std::vector<std::string> terms {"failed", "order"};
    std::shared_ptr<roaring::Roaring> bitmap;
    assert_ok(_index_reader->_compute_query_bitmap(
            execution.context, InvertedIndexQueryType::MATCH_PHRASE_QUERY, query_info,
            "failed order", &terms, 50, &bitmap));

    ASSERT_NE(bitmap, nullptr);
    EXPECT_EQ(bitmap_docids(*bitmap), (std::vector<uint32_t> {0, 3, 5}));
}

TEST_F(SniiIndexReaderCountFallback, UnorderedSloppyPhraseUsesWholePhraseWindow) {
    QueryExecutionContext execution(/*enable_query_cache=*/false);
    InvertedIndexQueryInfo query_info;
    query_info.term_infos.emplace_back("failed", 0);
    query_info.term_infos.emplace_back("warehouse", 1);
    query_info.term_infos.emplace_back("ordered", 2);
    std::vector<std::string> terms {"failed", "warehouse", "ordered"};

    const auto run_query = [&](int32_t slop) {
        query_info.slop = slop;
        std::shared_ptr<roaring::Roaring> bitmap;
        assert_ok(_index_reader->_compute_query_bitmap(
                execution.context, InvertedIndexQueryType::MATCH_PHRASE_QUERY, query_info,
                "failed warehouse ordered", &terms, 50, &bitmap));
        DORIS_CHECK(bitmap != nullptr);
        return bitmap_docids(*bitmap);
    };

    EXPECT_TRUE(run_query(1).empty());
    EXPECT_EQ(run_query(2), (std::vector<uint32_t> {2}));
}

TEST_F(SniiIndexReaderCountFallback, SloppyPhraseScoringUsesDistanceWeightedFrequency) {
    auto logical_reader = _file_reader->open_snii_index(&_meta);
    ASSERT_TRUE(logical_reader.has_value()) << logical_reader.error();

    QueryExecutionContext execution(/*enable_query_cache=*/false);
    InvertedIndexQueryInfo query_info;
    query_info.term_infos.emplace_back("failed", 0);
    query_info.term_infos.emplace_back("warehouse", 1);
    query_info.slop = 1;
    std::vector<std::string> terms {"failed", "warehouse"};
    std::shared_ptr<roaring::Roaring> bitmap;
    std::vector<doris::snii::query::PhraseMatch> matches;

    assert_ok(_index_reader->_compute_query_bitmap(
            execution.context,
            {.query_type = InvertedIndexQueryType::MATCH_PHRASE_QUERY,
             .query_info = query_info,
             .search_str = "failed warehouse",
             .max_expansions = 50,
             .logical_reader = logical_reader.value().get()},
            &terms, &bitmap, &matches));

    ASSERT_NE(bitmap, nullptr);
    EXPECT_EQ(bitmap_docids(*bitmap), (std::vector<uint32_t> {2}));
    ASSERT_EQ(matches.size(), 1);
    EXPECT_EQ(matches[0].docid, 2);
    EXPECT_FLOAT_EQ(matches[0].frequency, 0.5F);
}

TEST_F(SniiIndexReaderCountFallback, MultiTermPhraseUsesNormalPositionalQuery) {
    auto logical_reader = _file_reader->open_snii_index(&_meta);
    ASSERT_TRUE(logical_reader.has_value()) << logical_reader.error();
    const std::vector<std::string> terms {"failed", "order"};
    EXPECT_EQ(lookup_df(*logical_reader.value(), "failed"), 6U);

    OlapReaderStatistics stats;
    io::IOContext io_ctx;
    RuntimeState runtime_state;
    TQueryOptions query_options;
    query_options.enable_inverted_index_query_cache = false;
    query_options.enable_inverted_index_searcher_cache = false;
    runtime_state.set_query_options(query_options);
    auto context = std::make_shared<IndexQueryContext>();
    context->io_ctx = &io_ctx;
    context->stats = &stats;
    context->runtime_state = &runtime_state;
    context->count_on_index_fastpath = true;

    InvertedIndexQueryInfo query_info;
    bool handled = false;
    std::shared_ptr<roaring::Roaring> count_bitmap;
    assert_ok(_index_reader->_try_count_only_fastpath(context,
                                                      InvertedIndexQueryType::MATCH_PHRASE_QUERY,
                                                      query_info, terms, &handled, &count_bitmap));

    EXPECT_FALSE(handled);
    EXPECT_EQ(count_bitmap, nullptr);
    EXPECT_EQ(count_bitmap == nullptr ? 0 : count_bitmap->cardinality(), 0U);

    snii_doris::DorisSniiFileReader::ScopedIOContext io_context_scope(&io_ctx);
    std::vector<uint32_t> phrase_docids;
    assert_ok(doris::snii::query::phrase_query(*logical_reader.value(), terms, &phrase_docids));
    EXPECT_EQ(phrase_docids, (std::vector<uint32_t> {0, 3, 5}));
}

} // namespace
} // namespace doris::segment_v2
