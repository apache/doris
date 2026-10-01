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
//
// Candidate pushdown into multi-term phrase queries, measured for both inverted index storage
// formats. One synthetic log corpus is indexed through the production column writer as CLucene (V2)
// and as SNII. Every phrase then runs through the production reader with and without
// IndexQueryContext::candidate_rows, and every restricted result is checked against the full result
// intersected with the candidates. An untokenized index of the same rows serves exact and prefix
// lookups.
//
// The test is DISABLED_ so CI never runs it; it is still compiled into doris_be_test. Use a RELEASE
// UT build (BUILD_TYPE_UT=RELEASE in custom_env.sh) for representative numbers:
//
//   GTEST_ALSO_RUN_DISABLED_TESTS=1 ./run-be-ut.sh --run \
//       --filter='*PhraseCandidatePushdownBench*' -j <N>
//
// PHRASE_CANDIDATE_BENCH_DOCS sets the segment size (default 200000),
// PHRASE_CANDIDATE_BENCH_ITERATIONS the samples per measurement (default 10) and
// PHRASE_CANDIDATE_BENCH_CASES, a comma-separated list of query labels, the queries to run (default
// all), and PHRASE_CANDIDATE_BENCH_VARIANTS, a comma-separated list such as "random/0.500", the
// candidate variants to run (default all; a phrase then runs over the whole segment once, for the
// result check, unless "full" is listed; "scored" runs the phrases marked for it with their rows
// scored too; a docid query's cached, scored and count samples run only when listed). Times are
// medians of per-query thread CPU time, which moves far less than wall
// time on a shared machine.

#include <fmt/format.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <memory>
#include <random>
#include <ranges>
#include <string>
#include <string_view>
#include <vector>

#include "io/fs/local_file_system.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "storage/compaction/collection_similarity.h"
#include "storage/index/index_file_reader.h"
#include "storage/index/index_file_writer.h"
#include "storage/index/index_query_context.h"
#include "storage/index/index_writer.h"
#include "storage/index/inverted/inverted_index_cache.h"
#include "storage/index/inverted/inverted_index_desc.h"
#include "storage/index/inverted/inverted_index_reader.h"
#include "storage/index/inverted/similarity/collection_statistics.h"
#include "storage/index/snii/snii_index_reader.h"
#include "storage/olap_common.h"
#include "storage/tablet/tablet_schema.h"
#include "testutil/benchmark_control.h"
#include "util/slice.h"

namespace doris::segment_v2 {
namespace {

constexpr const char* kBenchDir = "./ut_dir/phrase_candidate_pushdown_bench";
constexpr const char* kColumnName = "1";
constexpr double kCandidateRatios[] = {0.001, 0.01, 0.05, 0.1, 0.224, 0.3, 0.5};

struct BenchQuery {
    const char* label;
    InvertedIndexQueryType type;
    const char* text;
    // One exact term, which a count-only scan answers from its document frequency.
    bool count = false;
    // Runs with its rows scored too.
    bool scored = false;
};

// How a query runs: with the result cache warm, as a count-only scan, or scoring its rows.
struct Profile {
    bool cached = false;
    bool count_only = false;
    bool scored = false;
    // Queries per sample: a microsecond query runs several times so no sample reads zero CPU
    // time.
    uint32_t repeats = 1;
};

// Frequent two- and four-term phrases, a frequent lead with an expanding numeric tail, a
// log-search shape with a long lead, and a rare phrase whose result is far below any candidate set.
constexpr BenchQuery kQueries[] = {{.label = "exact_2",
                                    .type = InvertedIndexQueryType::MATCH_PHRASE_QUERY,
                                    .text = "retry attempt",
                                    .scored = true},
                                   {.label = "exact_4",
                                    .type = InvertedIndexQueryType::MATCH_PHRASE_QUERY,
                                    .text = "retry attempt 2 job",
                                    .scored = true},
                                   {.label = "slop_2",
                                    .type = InvertedIndexQueryType::MATCH_PHRASE_QUERY,
                                    .text = "retry job ~2"},
                                   {.label = "ordered_2",
                                    .type = InvertedIndexQueryType::MATCH_PHRASE_QUERY,
                                    .text = "retry job ~2+"},
                                   {.label = "prefix_2",
                                    .type = InvertedIndexQueryType::MATCH_PHRASE_PREFIX_QUERY,
                                    .text = "order 12",
                                    .scored = true},
                                   {.label = "prefix_5",
                                    .type = InvertedIndexQueryType::MATCH_PHRASE_PREFIX_QUERY,
                                    .text = "retry attempt 1 job 88"},
                                   {.label = "rare_exact",
                                    .type = InvertedIndexQueryType::MATCH_PHRASE_QUERY,
                                    .text = "request 424242 completed",
                                    .scored = true}};

constexpr BenchQuery kDocIdQueries[] = {
        {.label = "term_dense",
         .type = InvertedIndexQueryType::MATCH_ANY_QUERY,
         .text = "latency",
         .count = true,
         .scored = true},
        {.label = "term_sparse",
         .type = InvertedIndexQueryType::MATCH_ANY_QUERY,
         .text = "424242",
         .count = true},
        {.label = "or_dense",
         .type = InvertedIndexQueryType::MATCH_ANY_QUERY,
         .text = "retry order latency",
         .scored = true},
        {.label = "or_sparse",
         .type = InvertedIndexQueryType::MATCH_ANY_QUERY,
         .text = "424242 424241",
         .scored = true},
        {.label = "and_dense",
         .type = InvertedIndexQueryType::MATCH_ALL_QUERY,
         .text = "retry attempt",
         .scored = true},
        {.label = "and_sparse",
         .type = InvertedIndexQueryType::MATCH_ALL_QUERY,
         .text = "retry 1234",
         .scored = true},
        {.label = "and_empty",
         .type = InvertedIndexQueryType::MATCH_ALL_QUERY,
         .text = "retry order"},
        // MATCH_REGEXP matches inside a term, so an unanchored pattern reads the whole
        // dictionary; an anchored one reads only the terms under its literal prefix.
        {.label = "regexp", .type = InvertedIndexQueryType::MATCH_REGEXP_QUERY, .text = "etr"},
        {.label = "regexp_anchored",
         .type = InvertedIndexQueryType::MATCH_REGEXP_QUERY,
         .text = "^ret.*"},
        // MATCH_PHRASE_EDGE reads the whole dictionary for the terms that contain one token, or
        // that end with a phrase's first token; "0 logi" starts with fifty such terms.
        {.label = "edge_one",
         .type = InvertedIndexQueryType::MATCH_PHRASE_EDGE_QUERY,
         .text = "etr"},
        {.label = "edge_two",
         .type = InvertedIndexQueryType::MATCH_PHRASE_EDGE_QUERY,
         .text = "ogin regi"},
        {.label = "edge_multi",
         .type = InvertedIndexQueryType::MATCH_PHRASE_EDGE_QUERY,
         .text = "try attempt 2 jo"},
        {.label = "edge_rare",
         .type = InvertedIndexQueryType::MATCH_PHRASE_EDGE_QUERY,
         .text = "est 424242 comp"},
        {.label = "edge_many",
         .type = InvertedIndexQueryType::MATCH_PHRASE_EDGE_QUERY,
         .text = "0 logi"}};

// Lookups on an untokenized index of the same rows: a value about nine rows hold, a value none
// holds, and a prefix that expands to many values.
// Phrases over long documents whose terms hold many positions in each: one matching early in
// almost every document, one whose terms never meet, one matching somewhere in most of them.
constexpr BenchQuery kLongQueries[] = {{.label = "long_early",
                                        .type = InvertedIndexQueryType::MATCH_PHRASE_QUERY,
                                        .text = "harbor crane"},
                                       {.label = "long_absent",
                                        .type = InvertedIndexQueryType::MATCH_PHRASE_QUERY,
                                        .text = "pier tide"},
                                       {.label = "long_mid",
                                        .type = InvertedIndexQueryType::MATCH_PHRASE_QUERY,
                                        .text = "crane tide"}};

constexpr BenchQuery kKeywordQueries[] = {
        {.label = "kw_equal",
         .type = InvertedIndexQueryType::EQUAL_QUERY,
         .text = "Retry attempt 2 job 1234 failed",
         .count = true},
        {.label = "kw_equal_missing",
         .type = InvertedIndexQueryType::EQUAL_QUERY,
         .text = "Retry attempt 9 job 1234 failed",
         .count = true},
        {.label = "kw_prefix",
         .type = InvertedIndexQueryType::MATCH_PHRASE_PREFIX_QUERY,
         .text = "Order 12"}};

uint32_t env_or(const char* name, uint32_t fallback) {
    const char* value = std::getenv(name);
    return value == nullptr ? fallback : static_cast<uint32_t>(std::stoul(value));
}

// Whether the comma-separated list in `variable` names `name`; every name is selected when the
// variable is unset.
bool selected(const char* variable, std::string_view name) {
    const char* names = std::getenv(variable);
    if (names == nullptr) {
        return true;
    }
    return std::ranges::any_of(std::views::split(std::string_view(names), ','),
                               [name](const auto& part) {
                                   return std::string_view(part.begin(), part.end()) == name;
                               });
}

std::vector<std::string> build_corpus(uint32_t doc_count) {
    std::mt19937 rng(20260918);
    std::vector<std::string> docs;
    docs.reserve(doc_count);
    for (uint32_t docid = 0; docid < doc_count; ++docid) {
        const uint32_t r = rng();
        switch (r % 4) {
        case 0:
            docs.push_back(fmt::format("Order {} processed gateway latency {}", 1000 + r % 9000,
                                       r % 1000));
            break;
        case 1:
            docs.push_back(fmt::format("Retry attempt {} job {} failed", 1 + (r >> 8) % 3,
                                       1000 + (r >> 12) % 9000));
            break;
        case 2:
            docs.push_back(fmt::format("User {} login region {}", r % 100000, (r >> 20) % 16));
            break;
        default:
            docs.push_back(
                    fmt::format("Request {} completed latency {}", r % 1000000, (r >> 10) % 1000));
            break;
        }
    }
    return docs;
}

// Documents of 200 to 300 words: "harbor crane" often, "harbor", "crane", "pier" and "tide" alone
// often, "tide" never right after "pier", and the rest from a few thousand others.
std::vector<std::string> build_long_corpus(uint32_t doc_count) {
    std::mt19937 rng(20261001);
    std::vector<std::string> docs;
    docs.reserve(doc_count);
    for (uint32_t docid = 0; docid < doc_count; ++docid) {
        const uint32_t words = 200 + rng() % 101;
        std::string doc;
        std::string_view previous;
        for (uint32_t word = 0; word < words; ++word) {
            const uint32_t r = rng() % 100;
            std::string_view next;
            if (r < 6) {
                next = "harbor crane";
            } else if (r < 11) {
                next = "harbor";
            } else if (r < 16) {
                next = "crane";
            } else if (r < 21) {
                next = "pier";
            } else if (r < 26 && previous != "pier") {
                next = "tide";
            }
            if (!next.empty()) {
                doc.append(next).push_back(' ');
                previous = next;
                continue;
            }
            doc.append(fmt::format("w{} ", rng() % 4000));
            previous = {};
        }
        docs.push_back(std::move(doc));
    }
    return docs;
}

// Random candidates model a selective non-index predicate; a clustered range models a time
// range over rows sorted by time.
roaring::Roaring make_candidates(uint32_t doc_count, double ratio, bool clustered) {
    roaring::Roaring candidates;
    if (clustered) {
        const auto count = static_cast<uint32_t>(doc_count * ratio);
        candidates.addRange(doc_count / 4, doc_count / 4 + count);
        return candidates;
    }
    std::mt19937 rng(static_cast<uint32_t>(ratio * 1000000));
    std::bernoulli_distribution keep(ratio);
    for (uint32_t docid = 0; docid < doc_count; ++docid) {
        if (keep(rng)) {
            candidates.add(docid);
        }
    }
    return candidates;
}

double thread_cpu_ms() {
    timespec ts {};
    clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts);
    return static_cast<double>(ts.tv_sec) * 1000.0 + static_cast<double>(ts.tv_nsec) / 1e6;
}

// Fixed statistics for a scored run: it measures the scoring work, not the collection's
// statistics, which a query reads once per term.
class BenchCollectionStatistics final : public CollectionStatistics {
public:
    float get_or_calculate_idf(const std::wstring& /*field*/,
                               const std::wstring& /*term*/) override {
        return 1.5F;
    }

    float get_or_calculate_avg_dl(const std::wstring& /*field*/) override { return 8.0F; }
};

struct QueryRun {
    explicit QueryRun(Profile profile = {}) {
        TQueryOptions query_options;
        query_options.query_type = TQueryType::SELECT;
        query_options.enable_inverted_index_query_cache = profile.cached;
        query_options.enable_inverted_index_searcher_cache = true;
        query_options.inverted_index_max_expansions = 50;
        runtime_state.set_query_options(query_options);
        context->io_ctx = &io_ctx;
        context->stats = &stats;
        context->runtime_state = &runtime_state;
        context->count_on_index_fastpath = profile.count_only;
        if (profile.scored) {
            context->collection_statistics = std::make_shared<BenchCollectionStatistics>();
            context->collection_similarity = std::make_shared<CollectionSimilarity>();
        }
    }

    OlapReaderStatistics stats;
    io::IOContext io_ctx;
    RuntimeState runtime_state;
    IndexQueryContextPtr context = std::make_shared<IndexQueryContext>();
};

class PhraseCandidatePushdownBench : public testing::Test {
protected:
    void SetUp() override {
        ASSERT_TRUE(io::global_local_filesystem()->delete_directory(kBenchDir).ok());
        ASSERT_TRUE(io::global_local_filesystem()->create_directory(kBenchDir).ok());
        std::vector<StorePath> paths;
        paths.emplace_back(std::string(kBenchDir), 1024L * 1024 * 1024);
        auto tmp_file_dirs = std::make_unique<TmpFileDirs>(paths);
        ASSERT_TRUE(tmp_file_dirs->init().ok());
        ExecEnv::GetInstance()->set_tmp_file_dir(std::move(tmp_file_dirs));
        constexpr int64_t kCacheLimit = 4L * 1024 * 1024 * 1024;
        _searcher_cache.reset(InvertedIndexSearcherCache::create_global_instance(kCacheLimit, 1));
        _query_cache.reset(InvertedIndexQueryCache::create_global_cache(kCacheLimit, 1));
        ExecEnv::GetInstance()->set_inverted_index_searcher_cache(_searcher_cache.get());
        ExecEnv::GetInstance()->set_inverted_index_query_cache(_query_cache.get());
        init_index_meta();
    }

    void TearDown() override {
        ExecEnv::GetInstance()->set_inverted_index_searcher_cache(nullptr);
        ExecEnv::GetInstance()->set_inverted_index_query_cache(nullptr);
        _searcher_cache.reset();
        _query_cache.reset();
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(kBenchDir).ok());
    }

    void init_index_meta() {
        TabletIndexPB pb;
        pb.set_index_type(IndexType::INVERTED);
        pb.set_index_id(1);
        pb.set_index_name("phrase_candidate_bench");
        pb.add_col_unique_id(1);
        pb.mutable_properties()->insert({"parser", "english"});
        pb.mutable_properties()->insert({"lower_case", "true"});
        pb.mutable_properties()->insert({"support_phrase", "true"});
        _meta.init_from_pb(pb);

        TabletIndexPB keyword_pb;
        keyword_pb.set_index_type(IndexType::INVERTED);
        keyword_pb.set_index_id(2);
        keyword_pb.set_index_name("phrase_candidate_bench_keyword");
        keyword_pb.add_col_unique_id(1);
        _keyword_meta.init_from_pb(keyword_pb);
    }

    static TabletSchemaSPtr create_schema() {
        TabletSchemaPB schema_pb;
        schema_pb.set_keys_type(DUP_KEYS);
        schema_pb.set_num_short_key_columns(1);
        schema_pb.set_num_rows_per_row_block(1024);
        schema_pb.set_compress_kind(COMPRESS_NONE);
        schema_pb.set_next_column_unique_id(2);
        ColumnPB* key = schema_pb.add_column();
        key->set_unique_id(0);
        key->set_name("c1");
        key->set_type("INT");
        key->set_is_key(true);
        key->set_length(4);
        key->set_index_length(4);
        key->set_is_nullable(false);
        ColumnPB* text = schema_pb.add_column();
        text->set_unique_id(1);
        text->set_name("c2");
        text->set_type("VARCHAR");
        text->set_length(255);
        text->set_index_length(255);
        text->set_is_nullable(false);
        auto schema = std::make_shared<TabletSchema>();
        schema->init_from_pb(schema_pb);
        return schema;
    }

    std::string write_index(const std::vector<std::string>& docs, const TabletIndex& meta,
                            InvertedIndexStorageFormatPB format, std::string_view name,
                            std::string_view directory = kBenchDir) {
        const std::string segment_path = fmt::format("{}/{}_0.dat", directory, name);
        const std::string prefix(InvertedIndexDescriptor::get_index_file_path_prefix(segment_path));
        io::FileWriterPtr file_writer;
        io::FileWriterOptions opts;
        auto fs = io::global_local_filesystem();
        EXPECT_TRUE(fs->create_file(InvertedIndexDescriptor::get_index_file_path_v2(prefix),
                                    &file_writer, &opts)
                            .ok());
        auto index_file_writer = std::make_unique<IndexFileWriter>(fs, prefix, std::string(name), 0,
                                                                   format, std::move(file_writer));
        const auto schema = create_schema();
        std::unique_ptr<IndexColumnWriter> column_writer;
        EXPECT_TRUE(IndexColumnWriter::create(&schema->column(1), &column_writer,
                                              index_file_writer.get(), &meta)
                            .ok());
        std::vector<Slice> values(docs.begin(), docs.end());
        EXPECT_TRUE(column_writer->add_values("c2", values.data(), values.size()).ok());
        EXPECT_TRUE(column_writer->finish().ok());
        EXPECT_TRUE(index_file_writer->begin_close().ok());
        EXPECT_TRUE(index_file_writer->finish_close().ok());
        return prefix;
    }

    std::shared_ptr<InvertedIndexReader> open_reader(const std::string& prefix,
                                                     InvertedIndexStorageFormatPB format,
                                                     uint32_t doc_count, bool keyword) {
        auto file_reader =
                std::make_shared<IndexFileReader>(io::global_local_filesystem(), prefix, format);
        EXPECT_TRUE(file_reader->init().ok());
        const TabletIndex* meta = keyword ? &_keyword_meta : &_meta;
        if (format == InvertedIndexStorageFormatPB::SNII) {
            return SniiIndexReader::create_shared(meta, file_reader,
                                                  keyword ? InvertedIndexReaderType::STRING_TYPE
                                                          : InvertedIndexReaderType::FULLTEXT,
                                                  doc_count, /*column_is_array=*/false);
        }
        if (keyword) {
            return StringTypeInvertedIndexReader::create_shared(meta, file_reader, doc_count,
                                                                /*column_is_array=*/false);
        }
        return FullTextIndexReader::create_shared(meta, file_reader, doc_count,
                                                  /*column_is_array=*/false);
    }

    TabletIndex _meta;
    TabletIndex _keyword_meta;
    std::unique_ptr<InvertedIndexSearcherCache> _searcher_cache;
    std::unique_ptr<InvertedIndexQueryCache> _query_cache;
};

// Runs one query and returns its thread CPU time. `consumed` reports whether the reader restricted
// the evaluation to the candidates.
double run_query(InvertedIndexReader* reader, const BenchQuery& query,
                 const roaring::Roaring* candidates, roaring::Roaring* result, bool* consumed,
                 Profile profile = {}) {
    QueryRun run(profile);
    run.context->candidate_rows = candidates;
    std::shared_ptr<roaring::Roaring> bitmap;
    const Field value = Field::create_field<TYPE_STRING>(std::string(query.text));
    const double start = thread_cpu_ms();
    const Status status = reader->query(run.context, kColumnName, value, query.type, bitmap);
    const double elapsed = thread_cpu_ms() - start;
    EXPECT_TRUE(status.ok()) << status;
    *result = *bitmap;
    *consumed = run.context->candidate_rows_consumed;
    return elapsed;
}

// A count-only answer is compared by its cardinality, since its ids are fabricated.
uint64_t bitmap_checksum(const roaring::Roaring& result, Profile profile) {
    if (profile.count_only) {
        return result.cardinality();
    }
    uint64_t checksum = 14695981039346656037ULL;
    for (uint32_t docid : result) {
        checksum = (checksum ^ docid) * 1099511628211ULL;
    }
    return checksum;
}

double median_query_ms(InvertedIndexReader* reader, const BenchQuery& query,
                       const roaring::Roaring* candidates, uint32_t iterations,
                       roaring::Roaring* result, std::string_view label, Profile profile = {}) {
    std::vector<double> samples;
    bool consumed = false;
    for (uint32_t i = 0; i < iterations; ++i) {
        benchmark::wait_for_turn(label, i);
        double elapsed_ms = 0;
        for (uint32_t repeat = 0; repeat < profile.repeats; ++repeat) {
            elapsed_ms += run_query(reader, query, candidates, result, &consumed, profile);
        }
        samples.push_back(elapsed_ms / profile.repeats);
        benchmark::report_sample(label, i, profile.repeats,
                                 static_cast<uint64_t>(elapsed_ms * 1000000.0),
                                 bitmap_checksum(*result, profile));
    }
    EXPECT_EQ(consumed, candidates != nullptr) << query.label;
    std::ranges::sort(samples);
    return samples[samples.size() / 2];
}

void print_row(std::string_view format, const BenchQuery& query, std::string_view shape,
               double ratio, double full_ms, double restricted_ms, uint64_t matches) {
    std::cout << std::left << std::setw(6) << format << std::setw(12) << query.label
              << std::setw(10) << shape << std::right << std::setw(8) << std::fixed
              << std::setprecision(1) << ratio * 100 << "%" << std::setw(12) << std::setprecision(3)
              << full_ms << std::setw(14) << restricted_ms << std::setw(9) << std::setprecision(2)
              << full_ms / restricted_ms << "x" << std::setw(10) << matches << '\n';
}

// Runs each query of `queries` over the whole segment, then with the result cache warm, the ones
// marked for it scoring their rows, and one exact term as a count-only scan too.
template <size_t N>
void benchmark_docid_queries(InvertedIndexReader* reader, std::string_view format_name,
                             std::string_view group, const BenchQuery (&queries)[N],
                             uint32_t iterations) {
    for (const BenchQuery& query : queries) {
        if (!selected("PHRASE_CANDIDATE_BENCH_CASES", query.label)) {
            continue;
        }
        roaring::Roaring full;
        const std::string label = fmt::format("reader/{}/{}/{}", format_name, group, query.label);
        median_query_ms(reader, query, nullptr, iterations, &full, label + "/full");
        if (selected("PHRASE_CANDIDATE_BENCH_VARIANTS", "cached")) {
            median_query_ms(reader, query, nullptr, iterations, &full, label + "/cached",
                            {.cached = true, .repeats = 16});
        }
        if (query.scored && selected("PHRASE_CANDIDATE_BENCH_VARIANTS", "scored")) {
            roaring::Roaring scored;
            median_query_ms(reader, query, nullptr, iterations, &scored, label + "/scored",
                            {.scored = true});
            EXPECT_EQ(scored, full) << query.label;
        }
        if (query.count && selected("PHRASE_CANDIDATE_BENCH_VARIANTS", "count")) {
            median_query_ms(reader, query, nullptr, iterations, &full, label + "/count",
                            {.count_only = true, .repeats = 16});
        }
    }
}

void benchmark_reader(InvertedIndexReader* reader, std::string_view format_name, uint32_t doc_count,
                      uint32_t iterations) {
    benchmark_docid_queries(reader, format_name, "docids", kDocIdQueries, iterations);
    for (const BenchQuery& query : kQueries) {
        if (!selected("PHRASE_CANDIDATE_BENCH_CASES", query.label)) {
            continue;
        }
        roaring::Roaring full;
        const std::string full_label = fmt::format("reader/{}/{}/full", format_name, query.label);
        const double full_ms = median_query_ms(
                reader, query, nullptr,
                selected("PHRASE_CANDIDATE_BENCH_VARIANTS", "full") ? iterations : 1, &full,
                full_label);
        if (query.scored && selected("PHRASE_CANDIDATE_BENCH_VARIANTS", "scored")) {
            roaring::Roaring scored;
            median_query_ms(reader, query, nullptr, iterations, &scored,
                            fmt::format("reader/{}/{}/scored", format_name, query.label),
                            {.scored = true});
            ASSERT_EQ(scored, full) << query.label;
        }
        for (const bool clustered : {false, true}) {
            const std::string_view shape_name = clustered ? "range" : "random";
            for (const double ratio : kCandidateRatios) {
                if (!selected("PHRASE_CANDIDATE_BENCH_VARIANTS",
                              fmt::format("{}/{:.3f}", shape_name, ratio))) {
                    continue;
                }
                const roaring::Roaring candidates = make_candidates(doc_count, ratio, clustered);
                roaring::Roaring restricted;
                const std::string label = fmt::format("reader/{}/{}/{}/{:.3f}", format_name,
                                                      query.label, shape_name, ratio);
                const double restricted_ms =
                        median_query_ms(reader, query, &candidates, iterations, &restricted, label);
                ASSERT_EQ(restricted, full & candidates) << query.label << " " << ratio;
                print_row(format_name, query, shape_name, ratio, full_ms, restricted_ms,
                          restricted.cardinality());
            }
        }
    }
}

void benchmark_keyword_reader(InvertedIndexReader* reader, std::string_view format_name,
                              uint32_t iterations) {
    benchmark_docid_queries(reader, format_name, "keyword", kKeywordQueries, iterations);
}

// Runs each long-document phrase over the whole segment and over random and clustered candidates.
void benchmark_long_reader(InvertedIndexReader* reader, std::string_view format_name,
                           uint32_t doc_count, uint32_t iterations) {
    for (const BenchQuery& query : kLongQueries) {
        if (!selected("PHRASE_CANDIDATE_BENCH_CASES", query.label)) {
            continue;
        }
        roaring::Roaring full;
        median_query_ms(reader, query, nullptr, iterations, &full,
                        fmt::format("reader/{}/long/{}/full", format_name, query.label));
        for (const bool clustered : {false, true}) {
            const roaring::Roaring candidates = make_candidates(doc_count, 0.05, clustered);
            roaring::Roaring restricted;
            median_query_ms(reader, query, &candidates, iterations, &restricted,
                            fmt::format("reader/{}/long/{}/{}/0.050", format_name, query.label,
                                        clustered ? "range" : "random"));
            EXPECT_EQ(restricted, full & candidates) << query.label;
        }
    }
}

TEST_F(PhraseCandidatePushdownBench, DISABLED_RestrictedVersusFullPhrase) {
    const uint32_t doc_count = env_or("PHRASE_CANDIDATE_BENCH_DOCS", 200000);
    const uint32_t iterations = env_or("PHRASE_CANDIDATE_BENCH_ITERATIONS", 10);
    const char* shared_root = std::getenv("PHRASE_CANDIDATE_BENCH_INDEX_ROOT");
    const bool prepare_shared = env_or("PHRASE_CANDIDATE_BENCH_PREPARE", 0) != 0;
    ASSERT_TRUE(!prepare_shared || shared_root != nullptr)
            << "PHRASE_CANDIDATE_BENCH_PREPARE requires PHRASE_CANDIDATE_BENCH_INDEX_ROOT";
    if (prepare_shared) {
        ASSERT_TRUE(io::global_local_filesystem()->create_directory(shared_root).ok());
    }
    const std::vector<std::string> docs = shared_root == nullptr || prepare_shared
                                                  ? build_corpus(doc_count)
                                                  : std::vector<std::string> {};
    std::cout << "docs=" << doc_count << " iterations=" << iterations
              << " (thread CPU ms, median)\n"
              << "format query       shape        ratio     full_ms  restricted_ms  speedup"
                 "   matches\n";
    for (const auto format :
         {InvertedIndexStorageFormatPB::V2, InvertedIndexStorageFormatPB::SNII}) {
        const bool is_snii = format == InvertedIndexStorageFormatPB::SNII;
        const std::string_view format_name = is_snii ? "SNII" : "V2";
        if (!selected("PHRASE_CANDIDATE_BENCH_FORMATS", format_name)) {
            continue;
        }
        const std::string name = fmt::format("{}_{}", is_snii ? "snii" : "clucene", doc_count);
        const std::string keyword_name =
                fmt::format("{}_keyword_{}", is_snii ? "snii" : "clucene", doc_count);
        if (prepare_shared) {
            write_index(docs, _meta, format, name, shared_root);
            write_index(docs, _keyword_meta, format, keyword_name, shared_root);
            continue;
        }
        // Writes the index, or finds the one prepared under the shared root.
        const auto index_prefix = [&](const TabletIndex& meta, const std::string& index_name) {
            const std::string prefix = shared_root == nullptr
                                               ? write_index(docs, meta, format, index_name)
                                               : fmt::format("{}/{}_0", shared_root, index_name);
            bool exists = false;
            EXPECT_TRUE(io::global_local_filesystem()
                                ->exists(InvertedIndexDescriptor::get_index_file_path_v2(prefix),
                                         &exists)
                                .ok());
            EXPECT_TRUE(exists) << "Missing benchmark index: " << prefix;
            return prefix;
        };
        const auto reader =
                open_reader(index_prefix(_meta, name), format, doc_count, /*keyword=*/false);
        benchmark_reader(reader.get(), format_name, doc_count, iterations);
        const auto keyword_reader = open_reader(index_prefix(_keyword_meta, keyword_name), format,
                                                doc_count, /*keyword=*/true);
        benchmark_keyword_reader(keyword_reader.get(), format_name, iterations);
    }
}

// Long documents, written per launch: PHRASE_CANDIDATE_BENCH_LONG_DOCS documents (default 20000).
TEST_F(PhraseCandidatePushdownBench, DISABLED_LongDocumentPhrases) {
    const uint32_t doc_count = env_or("PHRASE_CANDIDATE_BENCH_LONG_DOCS", 20000);
    const uint32_t iterations = env_or("PHRASE_CANDIDATE_BENCH_ITERATIONS", 10);
    const std::vector<std::string> docs = build_long_corpus(doc_count);
    for (const auto format :
         {InvertedIndexStorageFormatPB::V2, InvertedIndexStorageFormatPB::SNII}) {
        const bool is_snii = format == InvertedIndexStorageFormatPB::SNII;
        const std::string_view format_name = is_snii ? "SNII" : "V2";
        if (!selected("PHRASE_CANDIDATE_BENCH_FORMATS", format_name)) {
            continue;
        }
        const std::string name = fmt::format("{}_long_{}", is_snii ? "snii" : "clucene", doc_count);
        const auto reader = open_reader(write_index(docs, _meta, format, name), format, doc_count,
                                        /*keyword=*/false);
        benchmark_long_reader(reader.get(), format_name, doc_count, iterations);
    }
}

} // namespace
} // namespace doris::segment_v2
