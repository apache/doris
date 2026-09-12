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

#include <array>
#include <atomic>
#include <barrier>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "core/field.h"
#include "gen_cpp/AgentService_types.h"
#include "io/fs/local_file_system.h"
#include "runtime/exec_env.h"
#include "runtime/index_policy/index_policy_mgr.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/runtime_state.h"
#include "runtime/thread_context.h"
#include "storage/index/index_file_reader.h"
#include "storage/index/index_file_writer.h"
#include "storage/index/index_query_context.h"
#include "storage/index/inverted/inverted_index_cache.h"
#include "storage/index/snii/reader/logical_index_reader.h"
#include "storage/index/snii/snii_index_reader.h"
#include "storage/index/snii/snii_index_writer.h"
#include "storage/olap_common.h"
#include "storage/tablet/tablet_schema.h"
#include "util/slice.h"

namespace doris::segment_v2 {
namespace {

constexpr const char* kTestDir = "./ut_dir/snii_gram_cache_test";
constexpr const char* kPathPrefix = "./ut_dir/snii_gram_cache_test/segment";
constexpr size_t kConsumers = 8;

// Hold the actual bitmap-computation branch until every expected consumer has
// joined. The timeout diagnoses a missing branch; it never chooses the schedule.
class GramFlightGate {
public:
    static void leader(void* opaque) noexcept {
        auto& gate = *static_cast<GramFlightGate*>(opaque);
        std::unique_lock lock(gate._mutex);
        ++gate._leaders;
        gate._cv.notify_all();
        gate._cv.wait(lock, [&] { return gate._released; });
    }

    static void follower(void* opaque) noexcept {
        auto& gate = *static_cast<GramFlightGate*>(opaque);
        std::lock_guard lock(gate._mutex);
        ++gate._followers;
        gate._cv.notify_all();
    }

    bool wait_for_consumers(size_t leaders, size_t followers) {
        std::unique_lock lock(_mutex);
        return _cv.wait_for(lock, std::chrono::seconds(10),
                            [&] { return _leaders == leaders && _followers == followers; });
    }

    void release() {
        std::lock_guard lock(_mutex);
        _released = true;
        _cv.notify_all();
    }

    // Read only after every worker has joined.
    size_t leaders() const { return _leaders; }
    size_t followers() const { return _followers; }

private:
    std::mutex _mutex;
    std::condition_variable _cv;
    size_t _leaders = 0;
    size_t _followers = 0;
    bool _released = false;
};

struct GramCacheRequest {
    std::shared_ptr<SniiIndexReader> reader;
    std::string pattern;
    InvertedIndexQueryType type = InvertedIndexQueryType::LIKE_GRAM_QUERY;
    bool query_cache = true;
    bool searcher_cache = true;
};

struct GramCacheResult {
    Status status;
    std::shared_ptr<roaring::Roaring> bitmap;
    OlapReaderStatistics stats;
};

class SniiGramCacheTest : public testing::Test {
protected:
    void SetUp() override {
        auto* exec_env = ExecEnv::GetInstance();
        _previous_policy_mgr = exec_env->index_policy_mgr();
        _previous_query_cache = exec_env->get_inverted_index_query_cache();
        _previous_searcher_cache = exec_env->get_inverted_index_searcher_cache();
        exec_env->_index_policy_mgr = &_policy_mgr;
        _query_cache.reset(InvertedIndexQueryCache::create_global_cache(1024 * 1024, 1));
        _searcher_cache.reset(InvertedIndexSearcherCache::create_global_instance(1024 * 1024, 1));
        exec_env->set_inverted_index_query_cache(_query_cache.get());
        exec_env->set_inverted_index_searcher_cache(_searcher_cache.get());
        _worker_tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::QUERY,
                                                           "SniiGramCacheTest");

        for (size_t i = 0; i < _indexes.size(); ++i) {
            const std::string min_gram = std::to_string(3 + i);
            TIndexPolicy tokenizer;
            tokenizer.id = 6753900 + 2 * i;
            tokenizer.name = "gram_cache_dense" + min_gram + "_tokenizer";
            tokenizer.type = TIndexPolicyType::TOKENIZER;
            tokenizer.properties = {{"type", "ngram"}, {"mode", "dense"}, {"min_gram", min_gram}};
            TIndexPolicy analyzer;
            analyzer.id = tokenizer.id + 1;
            analyzer.name = "gram_cache_dense" + min_gram + "_analyzer";
            analyzer.type = TIndexPolicyType::ANALYZER;
            analyzer.properties = {{"tokenizer", tokenizer.name}};
            _policy_mgr.apply_policy_changes({tokenizer, analyzer}, {});

            TabletIndexPB pb;
            pb.set_index_type(IndexType::INVERTED);
            pb.set_index_id(6753910 + i);
            pb.set_index_name("gram_cache_dense" + min_gram);
            pb.add_col_unique_id(0);
            pb.mutable_properties()->insert({"analyzer", analyzer.name});
            _indexes[i].init_from_pb(pb);
        }

        ASSERT_TRUE(io::global_local_filesystem()->delete_directory(kTestDir).ok());
        ASSERT_TRUE(io::global_local_filesystem()->create_directory(kTestDir).ok());
        auto status = write_indexes();
        ASSERT_TRUE(status.ok()) << status;
        _file_reader = std::make_shared<IndexFileReader>(io::global_local_filesystem(), kPathPrefix,
                                                         InvertedIndexStorageFormatPB::SNII);
        status = _file_reader->init();
        ASSERT_TRUE(status.ok()) << status;
        for (size_t i = 0; i < _readers.size(); ++i) {
            _readers[i] = SniiIndexReader::create_shared(&_indexes[i], _file_reader,
                                                         InvertedIndexReaderType::FULLTEXT,
                                                         _values.size(), /*column_is_array=*/false);
            _readers[i]->set_searcher_open_observer_for_test(
                    [](void* opaque) noexcept {
                        static_cast<std::atomic<size_t>*>(opaque)->fetch_add(1);
                    },
                    &_searcher_opens);
        }
    }

    void TearDown() override {
        _readers = {};
        auto* exec_env = ExecEnv::GetInstance();
        exec_env->set_inverted_index_query_cache(_previous_query_cache);
        exec_env->set_inverted_index_searcher_cache(_previous_searcher_cache);
        _query_cache.reset();
        _searcher_cache.reset();
        _file_reader.reset();
        exec_env->_index_policy_mgr = _previous_policy_mgr;
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(kTestDir).ok());
    }

    Status write_indexes() {
        io::FileWriterPtr file_writer;
        RETURN_IF_ERROR(io::global_local_filesystem()->create_file(
                std::string(kPathPrefix) + ".idx", &file_writer));
        IndexFileWriter index_writer(io::global_local_filesystem(), kPathPrefix,
                                     "gram_cache_rowset", 0, InvertedIndexStorageFormatPB::SNII,
                                     std::move(file_writer), true, 6753910);
        std::vector<Slice> slices;
        for (const auto& value : _values) {
            slices.emplace_back(value);
        }
        for (const auto& index : _indexes) {
            SniiIndexColumnWriter writer(&index_writer, &index, FieldType::OLAP_FIELD_TYPE_VARCHAR);
            RETURN_IF_ERROR(writer.init());
            RETURN_IF_ERROR(writer.add_values("p", slices.data(), slices.size()));
            RETURN_IF_ERROR(writer.finish());
        }
        RETURN_IF_ERROR(index_writer.begin_close());
        return index_writer.finish_close();
    }

    void observe_flights(GramFlightGate* gate) {
        for (const auto& reader : _readers) {
            reader->set_single_flight_leader_before_compute_observer_for_test(
                    &GramFlightGate::leader, gate);
            reader->set_single_flight_follower_joined_observer_for_test(&GramFlightGate::follower,
                                                                        gate);
        }
    }

    std::vector<GramCacheResult> run_queries(const std::vector<GramCacheRequest>& requests,
                                             GramFlightGate* blocked_gate = nullptr,
                                             size_t leaders = 0, size_t followers = 0) {
        std::vector<GramCacheResult> results(requests.size());
        std::barrier start(static_cast<ptrdiff_t>(requests.size() + 1));
        std::vector<std::thread> workers;
        for (size_t i = 0; i < requests.size(); ++i) {
            workers.emplace_back([&, i] {
                SCOPED_ATTACH_TASK(_worker_tracker);
                const auto& request = requests[i];
                RuntimeState runtime_state;
                TQueryOptions options;
                options.__set_query_type(TQueryType::SELECT);
                options.__set_enable_inverted_index_query_cache(request.query_cache);
                options.__set_enable_inverted_index_searcher_cache(request.searcher_cache);
                runtime_state.set_query_options(options);
                io::IOContext io_context;
                auto context = std::make_shared<IndexQueryContext>();
                context->runtime_state = &runtime_state;
                context->stats = &results[i].stats;
                context->io_ctx = &io_context;
                const auto value = Field::create_field<TYPE_STRING>(request.pattern);
                start.arrive_and_wait();
                results[i].status =
                        request.reader->query(context, "p", value, request.type, results[i].bitmap);
            });
        }
        start.arrive_and_wait();
        if (blocked_gate != nullptr) {
            EXPECT_TRUE(blocked_gate->wait_for_consumers(leaders, followers));
            // Always release before checking worker results, including timeout failures.
            blocked_gate->release();
        }
        for (auto& worker : workers) {
            worker.join();
        }
        return results;
    }

    void expect_candidates(const GramCacheResult& result, const std::vector<uint32_t>& expected) {
        ASSERT_TRUE(result.status.ok()) << result.status;
        ASSERT_NE(result.bitmap, nullptr);
        EXPECT_EQ(std::vector<uint32_t>(result.bitmap->begin(), result.bitmap->end()), expected);
    }

    // Row 1 has every dense3 gram in "abcdef", but no dense4 gram. It is a
    // permitted false positive for dense3 and distinguishes the physical schemes.
    const std::vector<std::string> _values {"abcdef",    "abc-bcd-cde-def", "%abcdef%",
                                            "unrelated", "xyzuvw",          "abc"};
    IndexPolicyMgr _policy_mgr;
    IndexPolicyMgr* _previous_policy_mgr = nullptr;
    InvertedIndexQueryCache* _previous_query_cache = nullptr;
    InvertedIndexSearcherCache* _previous_searcher_cache = nullptr;
    std::unique_ptr<InvertedIndexQueryCache> _query_cache;
    std::unique_ptr<InvertedIndexSearcherCache> _searcher_cache;
    std::shared_ptr<MemTrackerLimiter> _worker_tracker;
    std::array<TabletIndex, 2> _indexes;
    std::shared_ptr<IndexFileReader> _file_reader;
    std::array<std::shared_ptr<SniiIndexReader>, 2> _readers;
    std::atomic<size_t> _searcher_opens {0};
};

TEST_F(SniiGramCacheTest, ColdIdenticalPatternsShareOneBitmapComputation) {
    GramFlightGate gate;
    observe_flights(&gate);
    const std::vector<GramCacheRequest> requests(kConsumers, {_readers[0], "%abcdef%"});
    const auto results = run_queries(requests, &gate, 1, kConsumers - 1);

    EXPECT_EQ(gate.leaders(), 1);
    EXPECT_EQ(gate.followers(), kConsumers - 1);
    size_t searcher_misses = 0;
    size_t query_inserts = 0;
    for (const auto& result : results) {
        expect_candidates(result, {0, 1, 2});
        EXPECT_EQ(result.bitmap, results.front().bitmap);
        EXPECT_EQ(result.stats.inverted_index_query_cache_lookup, 1);
        EXPECT_EQ(result.stats.inverted_index_query_cache_miss, 1);
        EXPECT_EQ(result.stats.inverted_index_query_cache_hit, 0);
        EXPECT_EQ(result.stats.inverted_index_searcher_cache_hit +
                          result.stats.inverted_index_searcher_cache_miss,
                  1);
        searcher_misses += result.stats.inverted_index_searcher_cache_miss;
        query_inserts += result.stats.inverted_index_query_cache_insert;
    }
    EXPECT_EQ(query_inserts, 1);
    EXPECT_GE(searcher_misses, 1);
    // Logical readers are opened before joining the bitmap flight; concurrent
    // cold searcher misses may therefore open more than one logical reader.
    EXPECT_EQ(_searcher_opens.load(), searcher_misses);
}

TEST_F(SniiGramCacheTest, WarmQueryHitsAvoidOpeningOrJoiningAndNewPatternsReuseSearcher) {
    GramFlightGate cold_gate;
    observe_flights(&cold_gate);
    const std::vector<GramCacheRequest> requests(kConsumers, {_readers[0], "%abcdef%"});
    const auto cold = run_queries(requests, &cold_gate, 1, kConsumers - 1);
    for (const auto& result : cold) {
        expect_candidates(result, {0, 1, 2});
    }
    const size_t cold_opens = _searcher_opens.load();

    const auto warm = run_queries(requests);
    for (const auto& result : warm) {
        expect_candidates(result, {0, 1, 2});
        EXPECT_EQ(result.bitmap, cold.front().bitmap);
        EXPECT_EQ(result.stats.inverted_index_query_cache_hit, 1);
        EXPECT_EQ(result.stats.inverted_index_query_cache_miss, 0);
        EXPECT_EQ(result.stats.inverted_index_query_cache_insert, 0);
        EXPECT_EQ(result.stats.inverted_index_searcher_cache_hit, 0);
        EXPECT_EQ(result.stats.inverted_index_searcher_cache_miss, 0);
    }
    EXPECT_EQ(cold_gate.leaders(), 1);
    EXPECT_EQ(cold_gate.followers(), kConsumers - 1);
    EXPECT_EQ(_searcher_opens.load(), cold_opens);

    GramFlightGate new_pattern_gate;
    observe_flights(&new_pattern_gate);
    const std::vector<GramCacheRequest> new_requests(kConsumers, {_readers[0], "%xyzuvw%"});
    const auto searcher_hits = run_queries(new_requests, &new_pattern_gate, 1, kConsumers - 1);
    for (const auto& result : searcher_hits) {
        expect_candidates(result, {4});
        EXPECT_EQ(result.stats.inverted_index_query_cache_hit, 0);
        EXPECT_EQ(result.stats.inverted_index_query_cache_miss, 1);
        EXPECT_EQ(result.stats.inverted_index_searcher_cache_hit, 1);
        EXPECT_EQ(result.stats.inverted_index_searcher_cache_miss, 0);
    }
    EXPECT_EQ(new_pattern_gate.leaders(), 1);
    EXPECT_EQ(new_pattern_gate.followers(), kConsumers - 1);
    EXPECT_EQ(_searcher_opens.load(), cold_opens);
}

TEST_F(SniiGramCacheTest, PhysicalSchemesInOneContainerKeepFlightsAndBothCachesSeparate) {
    for (size_t i = 0; i < _indexes.size(); ++i) {
        auto opened = _file_reader->open_snii_index(&_indexes[i]);
        ASSERT_TRUE(opened.has_value()) << opened.error();
        const auto& scheme = opened.value()->gram_scheme();
        ASSERT_TRUE(scheme.has_value());
        ASSERT_EQ(scheme->min_len, 3 + i);
    }
    ASSERT_NE(_file_reader->get_index_file_cache_key(&_indexes[0]),
              _file_reader->get_index_file_cache_key(&_indexes[1]));

    std::vector<GramCacheRequest> requests;
    for (size_t i = 0; i < kConsumers; ++i) {
        requests.push_back({_readers[i % 2], "%abcdef%"});
    }
    GramFlightGate cold_gate;
    observe_flights(&cold_gate);
    const auto cold = run_queries(requests, &cold_gate, 2, kConsumers - 2);
    size_t inserts = 0;
    for (size_t i = 0; i < cold.size(); ++i) {
        expect_candidates(cold[i], i % 2 == 0 ? std::vector<uint32_t> {0, 1, 2}
                                              : std::vector<uint32_t> {0, 2});
        EXPECT_EQ(cold[i].bitmap, cold[i % 2].bitmap);
        EXPECT_EQ(cold[i].stats.inverted_index_query_cache_miss, 1);
        inserts += cold[i].stats.inverted_index_query_cache_insert;
    }
    EXPECT_NE(cold[0].bitmap, cold[1].bitmap);
    EXPECT_EQ(inserts, 2);
    EXPECT_EQ(cold_gate.leaders(), 2);
    EXPECT_EQ(cold_gate.followers(), kConsumers - 2);

    const size_t cold_opens = _searcher_opens.load();
    const auto warm = run_queries(requests);
    for (size_t i = 0; i < warm.size(); ++i) {
        expect_candidates(warm[i], i % 2 == 0 ? std::vector<uint32_t> {0, 1, 2}
                                              : std::vector<uint32_t> {0, 2});
        EXPECT_EQ(warm[i].bitmap, cold[i % 2].bitmap);
        EXPECT_EQ(warm[i].stats.inverted_index_query_cache_hit, 1);
        EXPECT_EQ(warm[i].stats.inverted_index_searcher_cache_hit, 0);
        EXPECT_EQ(warm[i].stats.inverted_index_searcher_cache_miss, 0);
    }
    EXPECT_EQ(_searcher_opens.load(), cold_opens);
    EXPECT_EQ(cold_gate.leaders(), 2);
    EXPECT_EQ(cold_gate.followers(), kConsumers - 2);

    // A different raw pattern misses the result cache and must retrieve the
    // correct physical scheme from the already warm searcher cache.
    for (auto& request : requests) {
        request.pattern = "%%abcdef%%";
    }
    GramFlightGate searcher_gate;
    observe_flights(&searcher_gate);
    const auto searcher_hits = run_queries(requests, &searcher_gate, 2, kConsumers - 2);
    for (size_t i = 0; i < searcher_hits.size(); ++i) {
        expect_candidates(searcher_hits[i], i % 2 == 0 ? std::vector<uint32_t> {0, 1, 2}
                                                       : std::vector<uint32_t> {0, 2});
        EXPECT_EQ(searcher_hits[i].stats.inverted_index_query_cache_miss, 1);
        EXPECT_EQ(searcher_hits[i].stats.inverted_index_searcher_cache_hit, 1);
        EXPECT_EQ(searcher_hits[i].stats.inverted_index_searcher_cache_miss, 0);
    }
    EXPECT_EQ(searcher_gate.leaders(), 2);
    EXPECT_EQ(searcher_gate.followers(), kConsumers - 2);
    EXPECT_EQ(_searcher_opens.load(), cold_opens);
}

TEST_F(SniiGramCacheTest, IdenticalRawLikeAndRegexpPatternsKeepFlightsAndResultsSeparate) {
    std::vector<GramCacheRequest> requests;
    for (size_t i = 0; i < kConsumers; ++i) {
        requests.push_back({_readers[0], "%abcdef%",
                            i % 2 == 0 ? InvertedIndexQueryType::LIKE_GRAM_QUERY
                                       : InvertedIndexQueryType::REGEXP_GRAM_QUERY});
    }
    GramFlightGate gate;
    observe_flights(&gate);
    const auto cold = run_queries(requests, &gate, 2, kConsumers - 2);
    size_t inserts = 0;
    for (size_t i = 0; i < cold.size(); ++i) {
        // '%' is a wildcard in LIKE but a literal byte in REGEXP. Row 2 is
        // the only candidate containing both the leading and trailing '%'.
        expect_candidates(cold[i],
                          i % 2 == 0 ? std::vector<uint32_t> {0, 1, 2} : std::vector<uint32_t> {2});
        EXPECT_EQ(cold[i].bitmap, cold[i % 2].bitmap);
        EXPECT_EQ(cold[i].stats.inverted_index_query_cache_miss, 1);
        inserts += cold[i].stats.inverted_index_query_cache_insert;
    }
    EXPECT_NE(cold[0].bitmap, cold[1].bitmap);
    EXPECT_EQ(inserts, 2);
    EXPECT_EQ(gate.leaders(), 2);
    EXPECT_EQ(gate.followers(), kConsumers - 2);

    const size_t cold_opens = _searcher_opens.load();
    const auto warm = run_queries(requests);
    for (size_t i = 0; i < warm.size(); ++i) {
        expect_candidates(warm[i],
                          i % 2 == 0 ? std::vector<uint32_t> {0, 1, 2} : std::vector<uint32_t> {2});
        EXPECT_EQ(warm[i].bitmap, cold[i % 2].bitmap);
        EXPECT_EQ(warm[i].stats.inverted_index_query_cache_hit, 1);
        EXPECT_EQ(warm[i].stats.inverted_index_query_cache_miss, 0);
        EXPECT_EQ(warm[i].stats.inverted_index_searcher_cache_hit, 0);
        EXPECT_EQ(warm[i].stats.inverted_index_searcher_cache_miss, 0);
    }
    EXPECT_EQ(_searcher_opens.load(), cold_opens);
    EXPECT_EQ(gate.leaders(), 2);
    EXPECT_EQ(gate.followers(), kConsumers - 2);
}

TEST_F(SniiGramCacheTest, UnprunablePatternsReleaseFollowersWithoutCachingResults) {
    for (const auto& [pattern, type] : std::vector<std::pair<std::string, InvertedIndexQueryType>> {
                 {"%", InvertedIndexQueryType::LIKE_GRAM_QUERY},
                 {".*", InvertedIndexQueryType::REGEXP_GRAM_QUERY},
                 {"[", InvertedIndexQueryType::REGEXP_GRAM_QUERY}}) {
        SCOPED_TRACE(pattern);
        const std::vector<GramCacheRequest> requests(kConsumers, {_readers[0], pattern, type});
        for (size_t attempt = 0; attempt < 2; ++attempt) {
            SCOPED_TRACE(attempt);
            GramFlightGate gate;
            observe_flights(&gate);
            const auto results = run_queries(requests, &gate, 1, kConsumers - 1);
            // These observers prove the consumers really waited on a leader
            // that returned non-OK. Followers then retry independently, and a
            // second cohort must be able to lead the same physical query again.
            EXPECT_EQ(gate.leaders(), 1);
            EXPECT_EQ(gate.followers(), kConsumers - 1);
            for (const auto& result : results) {
                EXPECT_TRUE(result.status.is<ErrorCode::INVERTED_INDEX_EVALUATE_SKIPPED>())
                        << result.status;
                EXPECT_EQ(result.bitmap, nullptr);
                EXPECT_EQ(result.stats.inverted_index_query_cache_hit, 0);
                EXPECT_EQ(result.stats.inverted_index_query_cache_miss, 1);
                EXPECT_EQ(result.stats.inverted_index_query_cache_insert, 0);
            }
        }
    }
}

class SniiGramCacheOptionsTest : public SniiGramCacheTest,
                                 public testing::WithParamInterface<std::pair<bool, bool>> {};

TEST_P(SniiGramCacheOptionsTest, RepeatedPatternsRespectBothCacheSwitches) {
    const auto [query_cache, searcher_cache] = GetParam();
    GramFlightGate gate;
    gate.release();
    observe_flights(&gate);
    const std::vector<GramCacheRequest> requests {{_readers[0], "%abcdef%",
                                                   InvertedIndexQueryType::LIKE_GRAM_QUERY,
                                                   query_cache, searcher_cache}};
    const auto cold = run_queries(requests);
    expect_candidates(cold.front(), {0, 1, 2});
    EXPECT_EQ(cold.front().stats.inverted_index_query_cache_hit, 0);
    EXPECT_EQ(cold.front().stats.inverted_index_query_cache_miss, query_cache ? 1 : 0);
    EXPECT_EQ(cold.front().stats.inverted_index_query_cache_insert, query_cache ? 1 : 0);
    EXPECT_EQ(cold.front().stats.inverted_index_searcher_cache_miss, 1);
    EXPECT_EQ(_searcher_opens.load(), 1);

    const auto repeated = run_queries(requests);
    expect_candidates(repeated.front(), {0, 1, 2});
    const auto& stats = repeated.front().stats;
    EXPECT_EQ(stats.inverted_index_query_cache_lookup, query_cache ? 1 : 0);
    EXPECT_EQ(stats.inverted_index_query_cache_hit, query_cache ? 1 : 0);
    EXPECT_EQ(stats.inverted_index_query_cache_miss, 0);
    EXPECT_EQ(stats.inverted_index_query_cache_insert, 0);
    EXPECT_EQ(stats.inverted_index_searcher_cache_hit, !query_cache && searcher_cache ? 1 : 0);
    EXPECT_EQ(stats.inverted_index_searcher_cache_miss, !query_cache && !searcher_cache ? 1 : 0);
    EXPECT_EQ(_searcher_opens.load(), !query_cache && !searcher_cache ? 2 : 1);
    // Query-cache disablement still allows independent calls to lead flights.
    EXPECT_EQ(gate.leaders(), query_cache ? 1 : 2);
    EXPECT_EQ(gate.followers(), 0);
}

INSTANTIATE_TEST_SUITE_P(CacheSwitches, SniiGramCacheOptionsTest,
                         testing::Values(std::pair {false, false}, std::pair {false, true},
                                         std::pair {true, false}, std::pair {true, true}));

} // namespace
} // namespace doris::segment_v2
