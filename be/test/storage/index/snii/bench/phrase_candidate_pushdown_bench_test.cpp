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
// intersected with the candidates.
//
// The test is DISABLED_ so CI never runs it; it is still compiled into doris_be_test. Use a RELEASE
// UT build (BUILD_TYPE_UT=RELEASE in custom_env.sh) for representative numbers:
//
//   GTEST_ALSO_RUN_DISABLED_TESTS=1 ./run-be-ut.sh --run \
//       --filter='*PhraseCandidatePushdownBench*' -j <N>
//
// PHRASE_CANDIDATE_BENCH_DOCS sets the segment size (default 200000) and
// PHRASE_CANDIDATE_BENCH_ITERATIONS the samples per measurement (default 10). Times are medians of
// per-query thread CPU time, which moves far less than wall time on a shared machine.

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
#include <string>
#include <string_view>
#include <vector>

#include "io/fs/local_file_system.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "storage/index/index_file_reader.h"
#include "storage/index/index_file_writer.h"
#include "storage/index/index_query_context.h"
#include "storage/index/index_writer.h"
#include "storage/index/inverted/inverted_index_cache.h"
#include "storage/index/inverted/inverted_index_desc.h"
#include "storage/index/inverted/inverted_index_reader.h"
#include "storage/index/snii/snii_index_reader.h"
#include "storage/olap_common.h"
#include "storage/tablet/tablet_schema.h"
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
};

// Frequent two- and four-term phrases, a frequent lead with an expanding numeric tail, a
// log-search shape with a long lead, and a rare phrase whose result is far below any candidate set.
constexpr BenchQuery kQueries[] = {{.label = "exact_2",
                                    .type = InvertedIndexQueryType::MATCH_PHRASE_QUERY,
                                    .text = "retry attempt"},
                                   {.label = "exact_4",
                                    .type = InvertedIndexQueryType::MATCH_PHRASE_QUERY,
                                    .text = "retry attempt 2 job"},
                                   {.label = "prefix_2",
                                    .type = InvertedIndexQueryType::MATCH_PHRASE_PREFIX_QUERY,
                                    .text = "order 12"},
                                   {.label = "prefix_5",
                                    .type = InvertedIndexQueryType::MATCH_PHRASE_PREFIX_QUERY,
                                    .text = "retry attempt 1 job 88"},
                                   {.label = "rare_exact",
                                    .type = InvertedIndexQueryType::MATCH_PHRASE_QUERY,
                                    .text = "request 424242 completed"}};

uint32_t env_or(const char* name, uint32_t fallback) {
    const char* value = std::getenv(name);
    return value == nullptr ? fallback : static_cast<uint32_t>(std::stoul(value));
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
    if (!docs.empty()) {
        docs[doc_count / 4] = "Request 424242 completed latency 42";
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

struct QueryRun {
    QueryRun() {
        TQueryOptions query_options;
        query_options.query_type = TQueryType::SELECT;
        query_options.enable_inverted_index_query_cache = false;
        query_options.enable_inverted_index_searcher_cache = true;
        query_options.inverted_index_max_expansions = 50;
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

    std::string write_index(const std::vector<std::string>& docs,
                            InvertedIndexStorageFormatPB format, std::string_view name) {
        const std::string segment_path = fmt::format("{}/{}_0.dat", kBenchDir, name);
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
                                              index_file_writer.get(), &_meta)
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
                                                     uint32_t doc_count) {
        auto file_reader =
                std::make_shared<IndexFileReader>(io::global_local_filesystem(), prefix, format);
        EXPECT_TRUE(file_reader->init().ok());
        if (format == InvertedIndexStorageFormatPB::SNII) {
            return SniiIndexReader::create_shared(&_meta, file_reader,
                                                  InvertedIndexReaderType::FULLTEXT, doc_count,
                                                  /*column_is_array=*/false);
        }
        return FullTextIndexReader::create_shared(&_meta, file_reader);
    }

    TabletIndex _meta;
    std::unique_ptr<InvertedIndexSearcherCache> _searcher_cache;
    std::unique_ptr<InvertedIndexQueryCache> _query_cache;
};

// Runs one query and returns its thread CPU time. `consumed` reports whether the reader restricted
// the evaluation to the candidates.
double run_query(InvertedIndexReader* reader, const BenchQuery& query,
                 const roaring::Roaring* candidates, roaring::Roaring* result, bool* consumed) {
    QueryRun run;
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

double median_query_ms(InvertedIndexReader* reader, const BenchQuery& query,
                       const roaring::Roaring* candidates, uint32_t iterations,
                       roaring::Roaring* result) {
    std::vector<double> samples;
    bool consumed = false;
    for (uint32_t i = 0; i < iterations; ++i) {
        samples.push_back(run_query(reader, query, candidates, result, &consumed));
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

TEST_F(PhraseCandidatePushdownBench, DISABLED_RestrictedVersusFullPhrase) {
    const uint32_t doc_count = env_or("PHRASE_CANDIDATE_BENCH_DOCS", 200000);
    const uint32_t iterations = env_or("PHRASE_CANDIDATE_BENCH_ITERATIONS", 10);
    const std::vector<std::string> docs = build_corpus(doc_count);
    std::cout << "docs=" << doc_count << " iterations=" << iterations
              << " (thread CPU ms, median)\n"
              << "format query       shape        ratio     full_ms  restricted_ms  speedup"
                 "   matches\n";
    for (const auto format :
         {InvertedIndexStorageFormatPB::V2, InvertedIndexStorageFormatPB::SNII}) {
        const bool is_snii = format == InvertedIndexStorageFormatPB::SNII;
        const std::string prefix = write_index(docs, format, is_snii ? "snii" : "clucene");
        const auto reader = open_reader(prefix, format, doc_count);
        for (const BenchQuery& query : kQueries) {
            roaring::Roaring full;
            const double full_ms = median_query_ms(reader.get(), query, nullptr, iterations, &full);
            if (std::string_view(query.label) == "rare_exact") {
                ASSERT_FALSE(full.isEmpty());
            }
            for (const bool clustered : {false, true}) {
                for (const double ratio : kCandidateRatios) {
                    const roaring::Roaring candidates =
                            make_candidates(doc_count, ratio, clustered);
                    roaring::Roaring restricted;
                    const double restricted_ms = median_query_ms(reader.get(), query, &candidates,
                                                                 iterations, &restricted);
                    ASSERT_EQ(restricted, full & candidates) << query.label << " " << ratio;
                    print_row(is_snii ? "SNII" : "V2", query, clustered ? "range" : "random", ratio,
                              full_ms, restricted_ms, restricted.cardinality());
                }
            }
        }
    }
}

} // namespace
} // namespace doris::segment_v2
