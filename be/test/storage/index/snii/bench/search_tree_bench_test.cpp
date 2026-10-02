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
// Whole-query SEARCH over both inverted index storage formats. The benchmark reads the log corpus
// that PhraseCandidatePushdownBench writes with PHRASE_CANDIDATE_BENCH_PREPARE=1, binds the text
// column to its V2 (CLucene) or SNII index, and runs each Boolean tree through
// FunctionSearch::evaluate_inverted_index_with_search_param with every cache off. Every result is
// checked against the same tree evaluated from single-clause reader queries.
//
// The test is DISABLED_ so CI never runs it; it is still compiled into doris_be_test. Use a RELEASE
// UT build (BUILD_TYPE_UT=RELEASE in custom_env.sh) for representative numbers:
//
//   SEARCH_TREE_BENCH_INDEX_ROOT=<prepared root> SEARCH_TREE_BENCH_DOCS=<rows> \
//   GTEST_ALSO_RUN_DISABLED_TESTS=1 ./run-be-ut.sh --run --filter='*SearchTreeBench*' -j <N>
//
// SEARCH_TREE_BENCH_ITERATIONS sets the samples per case (default 10). A sample is the thread CPU
// time of one whole SEARCH evaluation. SEARCH_TREE_BENCH_CASES keeps only the listed case
// labels and SEARCH_TREE_BENCH_FORMATS only the listed formats (V2, SNII), comma-separated, so
// one case of one format can be profiled on its own. The scored cases score their rows with
// fixed statistics, and a top-k case keeps only the best rows. SEARCH_TREE_BENCH_NULL_EVERY names
// the corpus's NULL rows as PHRASE_CANDIDATE_BENCH_NULL_EVERY wrote them (default none); a clause
// is UNKNOWN on those rows, so a negation leaves them out.

#include <fmt/format.h>
#include <gen_cpp/Exprs_types.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <ctime>
#include <functional>
#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "core/data_type/data_type_string.h"
#include "exprs/function/function_search.h"
#include "io/fs/local_file_system.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "storage/compaction/collection_similarity.h"
#include "storage/index/index_file_reader.h"
#include "storage/index/index_query_context.h"
#include "storage/index/inverted/inverted_index_cache.h"
#include "storage/index/inverted/inverted_index_desc.h"
#include "storage/index/inverted/inverted_index_iterator.h"
#include "storage/index/inverted/inverted_index_reader.h"
#include "storage/index/inverted/similarity/collection_statistics.h"
#include "storage/index/snii/snii_index_reader.h"
#include "storage/olap_common.h"
#include "storage/tablet/tablet_schema.h"
#include "testutil/benchmark_control.h"

namespace doris::segment_v2 {
namespace {

// The corpus index stores the text column under its unique id; SEARCH refers to it as "body".
constexpr const char* kStoredField = "1";
constexpr const char* kField = "body";

uint32_t env_or(const char* name, uint32_t fallback) {
    const char* value = std::getenv(name);
    return value == nullptr ? fallback : static_cast<uint32_t>(std::stoul(value));
}

double thread_cpu_ms() {
    timespec ts {};
    clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts);
    return static_cast<double>(ts.tv_sec) * 1000.0 + static_cast<double>(ts.tv_nsec) / 1e6;
}

uint64_t bitmap_checksum(const roaring::Roaring& result) {
    uint64_t checksum = 14695981039346656037ULL;
    for (uint32_t docid : result) {
        checksum = (checksum ^ docid) * 1099511628211ULL;
    }
    return checksum;
}

// A context with the options a SELECT carries and the result cache off, so every sample executes.
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
    // A scored run publishes its scores, and keeps only the `top_k` best rows when set.
    explicit QueryRun(bool scored = false, uint32_t top_k = 0) {
        TQueryOptions query_options;
        query_options.query_type = TQueryType::SELECT;
        query_options.enable_inverted_index_query_cache = false;
        query_options.enable_inverted_index_searcher_cache = true;
        query_options.inverted_index_max_expansions = 50;
        runtime_state.set_query_options(query_options);
        context->io_ctx = &io_ctx;
        context->stats = &stats;
        context->runtime_state = &runtime_state;
        if (scored) {
            context->collection_statistics = std::make_shared<BenchCollectionStatistics>();
            context->collection_similarity = std::make_shared<CollectionSimilarity>();
            context->query_limit = top_k;
        }
    }

    OlapReaderStatistics stats;
    io::IOContext io_ctx;
    RuntimeState runtime_state;
    IndexQueryContextPtr context = std::make_shared<IndexQueryContext>();
};

TSearchClause leaf(std::string_view clause_type, std::string_view value) {
    TSearchClause clause;
    clause.clause_type = std::string(clause_type);
    clause.field_name = kField;
    clause.value = std::string(value);
    clause.__isset.field_name = true;
    clause.__isset.value = true;
    return clause;
}

TSearchClause compound(std::string_view clause_type, std::vector<TSearchClause> children) {
    TSearchClause clause;
    clause.clause_type = std::string(clause_type);
    clause.children = std::move(children);
    clause.__isset.children = true;
    return clause;
}

TSearchClause with_occur(TSearchClause clause, TSearchOccur::type occur) {
    clause.occur = occur;
    clause.__isset.occur = true;
    return clause;
}

// Answers single clauses through the reader, which is what each tree is checked against.
class ClauseOracle {
public:
    ClauseOracle(InvertedIndexReader* reader, uint32_t doc_count, uint32_t null_every)
            : _reader(reader), _doc_count(doc_count), _null_every(null_every) {}

    // The rows that are not NULL.
    roaring::Roaring rows() const {
        roaring::Roaring rows;
        rows.addRange(0, _doc_count);
        for (uint32_t row = 0; _null_every != 0 && row < _doc_count; row += _null_every) {
            rows.remove(row);
        }
        return rows;
    }

    roaring::Roaring term(std::string_view text) {
        return query(text, InvertedIndexQueryType::MATCH_ANY_QUERY);
    }

    roaring::Roaring phrase(std::string_view text) {
        return query(text, InvertedIndexQueryType::MATCH_PHRASE_QUERY);
    }

    roaring::Roaring prefix(std::string_view text) {
        return query(text, InvertedIndexQueryType::MATCH_PHRASE_PREFIX_QUERY);
    }

    roaring::Roaring regexp(std::string_view pattern) {
        return query(pattern, InvertedIndexQueryType::MATCH_REGEXP_QUERY);
    }

private:
    roaring::Roaring query(std::string_view text, InvertedIndexQueryType type) {
        QueryRun run;
        std::shared_ptr<roaring::Roaring> bitmap;
        const Field value = Field::create_field<TYPE_STRING>(std::string(text));
        const Status status = _reader->query(run.context, kStoredField, value, type, bitmap);
        EXPECT_TRUE(status.ok()) << status;
        return *bitmap;
    }

    InvertedIndexReader* _reader;
    uint32_t _doc_count;
    uint32_t _null_every;
};

struct SearchCase {
    const char* label;
    TSearchClause root;
    int32_t minimum_should_match = -1;
    std::function<roaring::Roaring(ClauseOracle&)> expected;
    // Scores its rows too, and keeps only the `top_k` best of them when set.
    bool scored = false;
    uint32_t top_k = 0;
};

// Expansion clauses. Each pattern matches the same terms whether or not a format anchors it.
// "12*" matches more terms than max_expansions keeps; the leading patterns scan the whole
// dictionary.
void append_expansion_cases(std::vector<SearchCase>* cases) {
    cases->push_back({.label = "prefix",
                      .root = leaf("PREFIX", "ret*"),
                      .expected = [](ClauseOracle& o) { return o.prefix("ret"); }});
    cases->push_back({.label = "prefix_capped",
                      .root = leaf("PREFIX", "12*"),
                      .expected = [](ClauseOracle& o) { return o.prefix("12"); }});
    cases->push_back({.label = "wildcard",
                      .root = leaf("WILDCARD", "re*y"),
                      .expected = [](ClauseOracle& o) { return o.regexp("^re.*y$"); }});
    cases->push_back({.label = "wildcard_leading",
                      .root = leaf("WILDCARD", "*try"),
                      .expected = [](ClauseOracle& o) { return o.regexp("^.*try$"); }});
    cases->push_back({.label = "regexp",
                      .root = leaf("REGEXP", "ret.*"),
                      .expected = [](ClauseOracle& o) { return o.regexp("^ret.*$"); }});
    cases->push_back({.label = "regexp_leading",
                      .root = leaf("REGEXP", ".*try"),
                      .expected = [](ClauseOracle& o) { return o.regexp("^.*try$"); }});
}

// Terms of the corpus: every row is one of four log lines. "retry", "attempt", "job" and "failed"
// share one quarter, "order", "processed" and "gateway" another, "latency" appears in half of the
// rows, and numbers range from common latencies to unique request ids.
std::vector<SearchCase> search_cases() {
    std::vector<SearchCase> cases;
    cases.push_back(
            {.label = "and_dense",
             .root = compound("AND", {leaf("TERM", "retry"), leaf("TERM", "failed")}),
             .expected = [](ClauseOracle& o) { return o.term("retry") & o.term("failed"); }});
    cases.push_back(
            {.label = "all_dense",
             .root = leaf("ALL", "retry failed"),
             .expected = [](ClauseOracle& o) { return o.term("retry") & o.term("failed"); }});
    cases.push_back({.label = "and_sparse",
                     .root = compound("AND", {leaf("TERM", "retry"), leaf("TERM", "1234")}),
                     .expected = [](ClauseOracle& o) { return o.term("retry") & o.term("1234"); }});
    cases.push_back(
            {.label = "and_empty",
             .root = compound("AND", {leaf("TERM", "retry"), leaf("TERM", "order")}),
             .expected = [](ClauseOracle& o) { return o.term("retry") & o.term("order"); }});
    cases.push_back({.label = "or_dense",
                     .root = compound("OR", {leaf("TERM", "retry"), leaf("TERM", "order"),
                                             leaf("TERM", "latency")}),
                     .expected = [](ClauseOracle& o) {
                         return o.term("retry") | o.term("order") | o.term("latency");
                     }});
    cases.push_back({.label = "any_dense",
                     .root = leaf("ANY", "retry order latency"),
                     .expected = [](ClauseOracle& o) {
                         return o.term("retry") | o.term("order") | o.term("latency");
                     }});
    cases.push_back({.label = "or_sparse",
                     .root = compound("OR", {leaf("TERM", "1234"), leaf("TERM", "5678")}),
                     .expected = [](ClauseOracle& o) { return o.term("1234") | o.term("5678"); }});
    cases.push_back({.label = "msm_2_of_3",
                     .root = leaf("TERM", "retry attempt latency"),
                     .minimum_should_match = 2,
                     .expected = [](ClauseOracle& o) {
                         const roaring::Roaring retry = o.term("retry");
                         const roaring::Roaring attempt = o.term("attempt");
                         const roaring::Roaring latency = o.term("latency");
                         return (retry & attempt) | (retry & latency) | (attempt & latency);
                     }});
    cases.push_back(
            {.label = "and_not",
             .root = compound("AND",
                              {leaf("TERM", "latency"), compound("NOT", {leaf("TERM", "order")})}),
             .expected = [](ClauseOracle& o) { return o.term("latency") - o.term("order"); }});
    cases.push_back({.label = "or_not",
                     .root = compound("OR", {leaf("TERM", "retry"),
                                             compound("NOT", {leaf("TERM", "order")})}),
                     .expected = [](ClauseOracle& o) {
                         return o.term("retry") | (o.rows() - o.term("order"));
                     }});
    cases.push_back(
            {.label = "phrase_and_term",
             .root = compound("AND", {leaf("PHRASE", "retry attempt"), leaf("TERM", "1234")}),
             .expected = [](ClauseOracle& o) {
                 return o.phrase("retry attempt") & o.term("1234");
             }});
    cases.push_back({.label = "phrase_2",
                     .root = leaf("PHRASE", "retry attempt"),
                     .expected = [](ClauseOracle& o) { return o.phrase("retry attempt"); }});
    cases.push_back({.label = "phrase_4",
                     .root = leaf("PHRASE", "retry attempt 2 job"),
                     .expected = [](ClauseOracle& o) { return o.phrase("retry attempt 2 job"); }});
    cases.push_back({.label = "occur_must_should",
                     .root = compound("OCCUR_BOOLEAN",
                                      {with_occur(leaf("TERM", "retry"), TSearchOccur::MUST),
                                       with_occur(leaf("TERM", "1234"), TSearchOccur::SHOULD),
                                       with_occur(leaf("TERM", "failed"), TSearchOccur::SHOULD)}),
                     .expected = [](ClauseOracle& o) { return o.term("retry"); }});
    cases.push_back({.label = "scored_or",
                     .root = compound("OR", {leaf("TERM", "retry"), leaf("TERM", "order"),
                                             leaf("TERM", "latency")}),
                     .expected =
                             [](ClauseOracle& o) {
                                 return o.term("retry") | o.term("order") | o.term("latency");
                             },
                     .scored = true});
    cases.push_back({.label = "scored_and",
                     .root = compound("AND", {leaf("TERM", "retry"), leaf("TERM", "failed")}),
                     .expected = [](ClauseOracle& o) { return o.term("retry") & o.term("failed"); },
                     .scored = true});
    cases.push_back({.label = "scored_phrase",
                     .root = leaf("PHRASE", "retry attempt"),
                     .expected = [](ClauseOracle& o) { return o.phrase("retry attempt"); },
                     .scored = true});
    cases.push_back({.label = "topk_or",
                     .root = compound("OR", {leaf("TERM", "retry"), leaf("TERM", "order"),
                                             leaf("TERM", "latency")}),
                     .expected =
                             [](ClauseOracle& o) {
                                 return o.term("retry") | o.term("order") | o.term("latency");
                             },
                     .scored = true,
                     .top_k = 10});
    append_expansion_cases(&cases);
    return cases;
}

class SearchTreeBench : public testing::Test {
protected:
    void SetUp() override {
        constexpr int64_t kCacheLimit = 4L * 1024 * 1024 * 1024;
        _searcher_cache.reset(InvertedIndexSearcherCache::create_global_instance(kCacheLimit, 1));
        _query_cache.reset(InvertedIndexQueryCache::create_global_cache(kCacheLimit, 1));
        ExecEnv::GetInstance()->set_inverted_index_searcher_cache(_searcher_cache.get());
        ExecEnv::GetInstance()->set_inverted_index_query_cache(_query_cache.get());
        // The analyzer properties the corpus index was written with.
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

    void TearDown() override {
        ExecEnv::GetInstance()->set_inverted_index_searcher_cache(nullptr);
        ExecEnv::GetInstance()->set_inverted_index_query_cache(nullptr);
        _searcher_cache.reset();
        _query_cache.reset();
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

    TSearchParam search_param(const SearchCase& search_case) const {
        TSearchParam param;
        param.original_dsl = search_case.label;
        param.root = search_case.root;
        TSearchFieldBinding binding;
        binding.field_name = kField;
        binding.slot_index = 0;
        binding.index_properties = _meta.properties();
        binding.__isset.index_properties = true;
        param.field_bindings = {binding};
        param.default_operator = "or";
        param.__isset.default_operator = true;
        if (search_case.minimum_should_match >= 0) {
            param.minimum_should_match = search_case.minimum_should_match;
            param.__isset.minimum_should_match = true;
        }
        return param;
    }

    TabletIndex _meta;
    std::unique_ptr<InvertedIndexSearcherCache> _searcher_cache;
    std::unique_ptr<InvertedIndexQueryCache> _query_cache;
};

TEST_F(SearchTreeBench, DISABLED_BooleanTrees) {
    const char* root = std::getenv("SEARCH_TREE_BENCH_INDEX_ROOT");
    ASSERT_NE(nullptr, root) << "SEARCH_TREE_BENCH_INDEX_ROOT must name a prepared corpus";
    const uint32_t doc_count = env_or("SEARCH_TREE_BENCH_DOCS", 200000);
    const uint32_t iterations = env_or("SEARCH_TREE_BENCH_ITERATIONS", 10);
    const uint32_t null_every = env_or("SEARCH_TREE_BENCH_NULL_EVERY", 0);
    std::vector<SearchCase> cases = search_cases();
    if (const char* only = std::getenv("SEARCH_TREE_BENCH_CASES"); only != nullptr) {
        const std::string listed = fmt::format(",{},", only);
        std::erase_if(cases, [&listed](const SearchCase& search_case) {
            return listed.find(fmt::format(",{},", search_case.label)) == std::string::npos;
        });
    }
    const std::unordered_map<std::string, int> no_column_ids;
    const FunctionSearch search;
    const char* formats = std::getenv("SEARCH_TREE_BENCH_FORMATS");
    const std::string listed_formats =
            fmt::format(",{},", formats == nullptr ? "V2,SNII" : formats);
    for (const auto format :
         {InvertedIndexStorageFormatPB::V2, InvertedIndexStorageFormatPB::SNII}) {
        const bool is_snii = format == InvertedIndexStorageFormatPB::SNII;
        const std::string_view format_name = is_snii ? "SNII" : "V2";
        if (listed_formats.find(fmt::format(",{},", format_name)) == std::string::npos) {
            continue;
        }
        const std::string prefix =
                fmt::format("{}/{}_{}_0", root, is_snii ? "snii" : "clucene", doc_count);
        bool exists = false;
        ASSERT_TRUE(
                io::global_local_filesystem()
                        ->exists(InvertedIndexDescriptor::get_index_file_path_v2(prefix), &exists)
                        .ok());
        ASSERT_TRUE(exists) << "Missing benchmark index: " << prefix;
        const auto reader = open_reader(prefix, format, doc_count);
        ClauseOracle oracle(reader.get(), doc_count, null_every);
        InvertedIndexIterator iterator;
        iterator.add_reader(InvertedIndexReaderType::FULLTEXT, reader);
        const std::unordered_map<std::string, IndexFieldNameAndTypePair> fields {
                {kField, {kStoredField, std::make_shared<DataTypeString>()}}};
        for (const SearchCase& search_case : cases) {
            const roaring::Roaring expected = search_case.expected(oracle);
            const TSearchParam param = search_param(search_case);
            const std::string label = fmt::format("search/{}/{}", format_name, search_case.label);
            for (uint32_t i = 0; i < iterations; ++i) {
                benchmark::wait_for_turn(label, i);
                QueryRun run(search_case.scored, search_case.top_k);
                // A segment scan gives its index iterators the query's context.
                iterator.set_context(run.context);
                InvertedIndexResultBitmap result;
                const double start = thread_cpu_ms();
                const Status status = search.evaluate_inverted_index_with_search_param(
                        param, fields, {{kField, &iterator}}, doc_count, result,
                        /*enable_cache=*/false, nullptr, no_column_ids, run.context);
                const double elapsed_ms = thread_cpu_ms() - start;
                ASSERT_TRUE(status.ok()) << label << ": " << status;
                const roaring::Roaring& rows = *result.get_data_bitmap();
                if (search_case.top_k == 0) {
                    ASSERT_EQ(expected, rows) << label;
                } else {
                    ASSERT_TRUE(rows.isSubset(expected)) << label;
                    ASSERT_EQ(rows.cardinality(),
                              std::min<uint64_t>(search_case.top_k, expected.cardinality()))
                            << label;
                }
                // Which rows a top-k answer keeps follows the scores' rounding, so it is compared
                // by its size.
                const uint64_t checksum =
                        search_case.top_k == 0 ? bitmap_checksum(rows) : rows.cardinality();
                benchmark::report_sample(label, i, 1, static_cast<uint64_t>(elapsed_ms * 1000000.0),
                                         checksum);
            }
            std::cout << label << " matches=" << expected.cardinality() << '\n';
        }
    }
}

} // namespace
} // namespace doris::segment_v2
