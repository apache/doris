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

#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/QueryCache_types.h>
#include <gen_cpp/RuntimeProfile_types.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <latch>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "common/metrics/doris_metrics.h"
#include "common/object_pool.h"
#include "core/data_type/data_type_number.h"
#include "exec/operator/olap_scan_operator.h"
#include "exec/pipeline/pipeline_fragment_context.h"
#include "exec/scan/olap_scanner.h"
#include "exec/scan/scan_node.h"
#include "io/cache/block_file_cache_profile.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_query_statistics_mgr.h"
#include "runtime/thread_context.h"
#include "runtime/workload_management/io_context.h"
#include "runtime/workload_management/resource_context.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/tablet/tablet_meta.h"
#include "storage/tablet/tablet_schema.h"
#include "testutil/desc_tbl_builder.h"
#include "testutil/mock/mock_runtime_state.h"

namespace doris {
namespace {

class FakeTablet final : public BaseTablet {
public:
    FakeTablet(int64_t partition_id, int64_t tablet_id)
            : BaseTablet(create_meta(partition_id, tablet_id)) {}

    std::string tablet_path() const override { return ""; }

    bool exceed_version_limit(int32_t /*limit*/) override { return false; }

    Result<std::unique_ptr<RowsetWriter>> create_rowset_writer(RowsetWriterContext& /*context*/,
                                                               bool /*vertical*/) override {
        return ResultError(Status::NotSupported("fake tablet"));
    }

    Result<std::unique_ptr<RowsetWriter>> create_transient_rowset_writer(
            const Rowset& /*rowset*/, std::shared_ptr<PartialUpdateInfo> /*partial_update_info*/,
            int64_t /*txn_expiration*/ = 0) override {
        return ResultError(Status::NotSupported("fake tablet"));
    }

    Status capture_rs_readers(const Version& /*spec_version*/,
                              std::vector<RowSetSplits>* /*rs_splits*/,
                              const CaptureRowsetOps& /*opts*/) override {
        return Status::NotSupported("fake tablet");
    }

    Status save_delete_bitmap(const TabletTxnInfo* /*txn_info*/, int64_t /*txn_id*/,
                              DeleteBitmapPtr /*delete_bitmap*/, RowsetWriter* /*rowset_writer*/,
                              const RowsetIdUnorderedSet& /*cur_rowset_ids*/,
                              int64_t /*lock_id*/ = -1,
                              int64_t /*next_visible_version*/ = -1) override {
        return Status::NotSupported("fake tablet");
    }

    CalcDeleteBitmapExecutor* calc_delete_bitmap_executor() override { return nullptr; }

    void clear_cache() override {}

    Versions calc_missed_versions(int64_t /*spec_version*/,
                                  Versions /*existing_versions*/) const override {
        return {};
    }

    size_t tablet_footprint() override { return 0; }

private:
    static TabletMetaSharedPtr create_meta(int64_t partition_id, int64_t tablet_id) {
        auto meta = std::make_shared<TabletMeta>(std::make_shared<TabletSchema>());
        meta->_partition_id = partition_id;
        meta->_tablet_id = tablet_id;
        return meta;
    }
};

class ProfileReader final : public TabletReader {
public:
    int profile_updates = 0;
    Status init(const ReaderParams&) override { return Status::OK(); }
    void update_profile(RuntimeProfile*) override { ++profile_updates; }
};

class OlapFileCacheProfileTest : public testing::Test {
protected:
    void SetUp() override {
        DescriptorTblBuilder builder(&pool);
        builder.declare_tuple() << TupleDescBuilder::SlotType {std::make_shared<DataTypeInt32>(),
                                                               "k"};
        auto* descs = builder.build();
        state.set_desc_tbl(descs);
        TOlapScanNode scan_node;
        scan_node.__set_tuple_id(0);
        scan_node.__set_keyType(TKeysType::DUP_KEYS);
        TPlanNode node;
        node.__set_node_id(0);
        node.__set_node_type(TPlanNodeType::OLAP_SCAN_NODE);
        node.__set_num_children(0);
        node.__set_limit(-1);
        node.__set_row_tuples({0});
        node.__set_olap_scan_node(scan_node);
        op = std::make_shared<OlapScanOperatorX>(&pool, node, 0, *descs, 1, TQueryCacheParam {});
        local = make_local();
    }

    std::shared_ptr<OlapScanLocalState> make_local() {
        auto result = OlapScanLocalState::create_shared(&state, op.get());
        result->_custom_profile = std::make_unique<RuntimeProfile>("scan");
        EXPECT_TRUE(result->_init_profile().ok());
        return result;
    }

    std::shared_ptr<OlapScanner> make_scanner(bool prepared = true,
                                              OlapScanLocalState* parent = nullptr) {
        OlapScanner::Params params;
        params.state = &state;
        params.profile = &scanner_profile;
        params.tablet = std::make_shared<FakeTablet>(10, next_tablet_id.fetch_add(1));
        params.version = 1;
        params.limit = -1;
        params.aggregation = true;
        auto scanner = OlapScanner::create_shared(parent ? parent : local.get(), std::move(params));
        if (prepared) {
            scanner->_has_prepared = true;
            scanner->_tablet_reader = std::make_unique<ProfileReader>();
        }
        return scanner;
    }

    int64_t value(const std::string& name) {
        auto* counter = local->_segment_profile->get_counter(name);
        EXPECT_NE(counter, nullptr) << name;
        return counter ? counter->value() : -1;
    }

    void expect_byte_counter(const TRuntimeProfileTree& tree, const std::string& name,
                             int64_t expected_value) {
        ASSERT_EQ(tree.nodes.size(), 1);
        const auto& node = tree.nodes.front();
        const auto& children = node.child_counters_map;
        ASSERT_TRUE(children.contains(RuntimeProfile::ROOT_COUNTER));
        ASSERT_TRUE(children.contains("FileCache"));
        EXPECT_EQ(children.at(RuntimeProfile::ROOT_COUNTER).count("FileCache"), 1);
        EXPECT_EQ(children.at(RuntimeProfile::ROOT_COUNTER).count(name), 0);
        EXPECT_EQ(children.at("FileCache").count(name), 1);
        auto counter = std::ranges::find(node.counters, name, &TCounter::name);
        ASSERT_NE(counter, node.counters.end());
        EXPECT_EQ(counter->type, TUnit::BYTES);
        EXPECT_EQ(counter->level, 1);
        EXPECT_EQ(counter->value, expected_value);
    }

    void expect_byte_counters(int64_t local_bytes, int64_t remote_bytes, int64_t written_bytes) {
        for (int level : {1, 2, 3}) {
            SCOPED_TRACE(level);
            TRuntimeProfileTree tree;
            local->_segment_profile->to_thrift(&tree, level);
            expect_byte_counter(tree, "BytesScannedFromCache", local_bytes);
            expect_byte_counter(tree, "BytesScannedFromRemote", remote_bytes);
            expect_byte_counter(tree, "BytesWriteIntoCache", written_bytes);
        }
    }

    std::atomic<int64_t> next_tablet_id {20};
    ObjectPool pool;
    MockRuntimeState state;
    RuntimeProfile scanner_profile {"scanner"};
    std::shared_ptr<OlapScanOperatorX> op;
    std::shared_ptr<OlapScanLocalState> local;
};

TEST_F(OlapFileCacheProfileTest, ByteCountersAreFileCacheChildrenAndReusedByReporter) {
    EXPECT_EQ(value("BytesScannedFromCache"), 0);
    EXPECT_EQ(value("BytesScannedFromRemote"), 0);
    EXPECT_EQ(value("BytesWriteIntoCache"), 0);
    expect_byte_counters(0, 0, 0);
    auto* file_cache = local->_segment_profile->get_counter("FileCache");
    ASSERT_NE(file_cache, nullptr);
    EXPECT_EQ(file_cache->type(), TUnit::TIME_NS);
    EXPECT_EQ(file_cache->level(), 2);
    io::FileCacheProfileReporter reporter(local->_segment_profile.get());
    EXPECT_EQ(reporter.total_time, file_cache);
    EXPECT_EQ(reporter.bytes_scanned_from_cache, local->_bytes_scanned_from_cache);
    EXPECT_EQ(reporter.bytes_scanned_from_remote, local->_bytes_scanned_from_remote);
    EXPECT_EQ(reporter.bytes_write_into_cache, local->_bytes_write_into_cache);
    io::FileCacheStatistics builder_stats;
    builder_stats.bytes_read_from_local = 3;
    builder_stats.bytes_read_from_remote = 5;
    builder_stats.bytes_write_into_cache = 2;
    reporter.update(&builder_stats);
    auto scanner = make_scanner();
    auto& stats = scanner->_tablet_reader->mutable_stats()->file_cache_stats;
    stats.bytes_read_from_local = 7;
    stats.bytes_read_from_remote = 13;
    stats.bytes_write_into_cache = 4;
    scanner->update_realtime_counters();
    expect_byte_counters(10, 18, 6);
}

TEST_F(OlapFileCacheProfileTest, MultiBatchRealtimeAndFinal) {
    auto scanner = make_scanner();
    scanner->_initial_file_cache_stats.bytes_read_from_local = 3;
    scanner->_initial_file_cache_stats.bytes_read_from_remote = 5;
    scanner->_initial_file_cache_stats.bytes_write_into_cache = 2;
    scanner->_initial_file_cache_stats.segment_footer_index_bytes_read_from_remote = 11;
    ASSERT_TRUE(scanner->_open_impl(&state).ok());
    auto& stats = *scanner->_tablet_reader->mutable_stats();
    auto* io = state.get_query_ctx()->resource_ctx()->io_context();
    const auto local_bytes_before = DorisMetrics::instance()->query_scan_bytes_from_local->value();
    const auto remote_bytes_before =
            DorisMetrics::instance()->query_scan_bytes_from_remote->value();
    stats.file_cache_stats.bytes_read_from_local += 7;
    stats.file_cache_stats.bytes_read_from_remote += 13;
    stats.file_cache_stats.bytes_write_into_cache += 4;
    stats.file_cache_stats.num_local_io_total = 3;
    stats.file_cache_stats.local_io_timer = 31;
    stats.compressed_bytes_read = 19;
    stats.uncompressed_bytes_read = 100;
    stats.raw_rows_read = 10;
    const int counters_before = local->_segment_profile->num_counters();
    scanner->update_realtime_counters();
    EXPECT_EQ(value("BytesScannedFromCache"), 10);
    EXPECT_EQ(value("BytesScannedFromRemote"), 18);
    EXPECT_EQ(value("BytesWriteIntoCache"), 6);
    EXPECT_EQ(local->_segment_profile->num_counters(), counters_before);
    EXPECT_EQ(stats.file_cache_stats.bytes_read_from_local, 10);
    EXPECT_EQ(stats.file_cache_stats.bytes_read_from_remote, 18);
    EXPECT_EQ(stats.file_cache_stats.bytes_write_into_cache, 6);
    EXPECT_EQ(stats.file_cache_stats.num_local_io_total, 3);
    EXPECT_EQ(stats.file_cache_stats.local_io_timer, 31);
    EXPECT_EQ(stats.file_cache_stats.segment_footer_index_bytes_read_from_remote, 11);

    // A local page-cache batch must use compressed bytes even after earlier cache reads.
    stats.file_cache_stats.bytes_read_from_peer = 9;
    stats.file_cache_stats.num_local_io_total += 5;
    stats.file_cache_stats.local_io_timer += 7;
    stats.compressed_bytes_read = 8;
    stats.uncompressed_bytes_read = 35;
    stats.raw_rows_read = 3;
    scanner->update_realtime_counters();
    scanner->update_realtime_counters();
    EXPECT_EQ(local->_segment_profile->num_counters(), counters_before);
    EXPECT_EQ(value("FileCache"), 0);
    EXPECT_EQ(value("BytesScannedFromRemote"), 18);
    EXPECT_EQ(io->scan_bytes_from_local_storage(), 18);
    EXPECT_EQ(io->scan_bytes_from_remote_storage(), 18);
    EXPECT_EQ(io->scan_rows(), 13);
    EXPECT_EQ(io->scan_bytes(), 135);
    EXPECT_EQ(io->bytes_write_into_cache(), 6);

    // Another cache-reading batch must report only its new bytes, not the cumulative totals.
    stats.file_cache_stats.bytes_read_from_local += 2;
    stats.file_cache_stats.bytes_read_from_remote += 4;
    stats.file_cache_stats.bytes_write_into_cache += 3;
    scanner->update_realtime_counters();
    scanner->update_realtime_counters();
    EXPECT_EQ(value("BytesScannedFromCache"), 12);
    EXPECT_EQ(value("BytesScannedFromRemote"), 22);
    EXPECT_EQ(value("BytesWriteIntoCache"), 9);
    EXPECT_EQ(io->scan_bytes_from_local_storage(), 20);
    EXPECT_EQ(io->scan_bytes_from_remote_storage(), 22);
    EXPECT_EQ(io->bytes_write_into_cache(), 9);
    EXPECT_EQ(DorisMetrics::instance()->query_scan_bytes_from_local->value(),
              local_bytes_before + 20);
    EXPECT_EQ(DorisMetrics::instance()->query_scan_bytes_from_remote->value(),
              remote_bytes_before + 22);
    EXPECT_EQ(local->_segment_profile->num_counters(), counters_before);

    // Terminal reporting adds only unpublished bytes and leaves the reader totals intact.
    stats.file_cache_stats.bytes_read_from_local += 5;
    stats.file_cache_stats.bytes_read_from_remote += 17;
    stats.file_cache_stats.bytes_write_into_cache += 6;
    scanner->_collect_profile_before_close();
    EXPECT_EQ(value("BytesScannedFromCache"), 17);
    EXPECT_EQ(value("BytesScannedFromRemote"), 39);
    EXPECT_EQ(value("BytesWriteIntoCache"), 15);
    EXPECT_EQ(stats.file_cache_stats.bytes_read_from_local, 17);
    EXPECT_EQ(stats.file_cache_stats.bytes_read_from_remote, 39);
    EXPECT_EQ(stats.file_cache_stats.bytes_write_into_cache, 15);
    EXPECT_EQ(value("BytesScannedFromPeer"), 9);
    EXPECT_EQ(value("NumLocalIOTotal"), 8);
    EXPECT_EQ(value("FileCache"), 38);
    EXPECT_EQ(value("SegmentFooterIndexBytesScannedFromRemote"), 11);
    EXPECT_EQ(local->_scan_rows->value(), 13);
    EXPECT_EQ(local->_scan_bytes->value(), 135);
    EXPECT_EQ(local->_read_compressed_counter->value(), 27);
    scanner->_collect_profile_before_close();
    ASSERT_TRUE(scanner->close(&state).ok());
    ASSERT_TRUE(scanner->close(&state).ok());
    EXPECT_EQ(value("BytesScannedFromRemote"), 39);
    EXPECT_EQ(value("NumLocalIOTotal"), 8);
    EXPECT_EQ(io->bytes_write_into_cache(), 15);
    EXPECT_EQ(static_cast<ProfileReader*>(scanner->_tablet_reader.get())->profile_updates, 1);
}

TEST_F(OlapFileCacheProfileTest, TerminalCollectionWithoutRealtimeUpdate) {
    auto scanner = make_scanner();
    auto& stats = scanner->_tablet_reader->mutable_stats()->file_cache_stats;
    stats.bytes_read_from_local = 7;
    stats.bytes_read_from_remote = 13;
    stats.bytes_write_into_cache = 4;
    stats.num_local_io_total = 2;
    stats.local_io_timer = 31;
    scanner->_collect_profile_before_close();
    scanner->_collect_profile_before_close();
    expect_byte_counters(7, 13, 4);
    EXPECT_EQ(value("NumLocalIOTotal"), 2);
    EXPECT_EQ(value("FileCache"), 31);
    EXPECT_EQ(stats.bytes_read_from_local, 7);
    EXPECT_EQ(stats.bytes_read_from_remote, 13);
    EXPECT_EQ(stats.bytes_write_into_cache, 4);
    EXPECT_EQ(state.get_query_ctx()->resource_ctx()->io_context()->bytes_write_into_cache(), 4);
}

TEST_F(OlapFileCacheProfileTest, TerminalCollectionAfterAllBytesPublished) {
    auto scanner = make_scanner();
    auto& stats = scanner->_tablet_reader->mutable_stats()->file_cache_stats;
    stats.bytes_read_from_local = 7;
    stats.bytes_read_from_remote = 13;
    stats.bytes_write_into_cache = 4;
    const int counters_before = local->_segment_profile->num_counters();
    scanner->update_realtime_counters();
    scanner->update_realtime_counters();
    scanner->_collect_profile_before_close();
    scanner->_collect_profile_before_close();
    expect_byte_counters(7, 13, 4);
    EXPECT_EQ(stats.bytes_read_from_local, 7);
    EXPECT_EQ(stats.bytes_read_from_remote, 13);
    EXPECT_EQ(stats.bytes_write_into_cache, 4);
    EXPECT_EQ(state.get_query_ctx()->resource_ctx()->io_context()->bytes_write_into_cache(), 4);
    EXPECT_EQ(local->_segment_profile->num_counters(), counters_before);
}

TEST_F(OlapFileCacheProfileTest, ConcurrentScannersAndIsolatedState) {
    constexpr int n = 8;
    std::latch ready(n), go(1);
    std::vector<std::thread> threads;
    for (int i = 0; i < n; ++i) {
        threads.emplace_back([&, i] {
            SCOPED_ATTACH_TASK(&state);
            auto scanner = make_scanner();
            auto& stats = scanner->_tablet_reader->mutable_stats()->file_cache_stats;
            stats.bytes_read_from_remote = i + 1;
            stats.bytes_write_into_cache = 2;
            stats.num_local_io_total = 1;
            ready.count_down();
            go.wait();
            scanner->update_realtime_counters();
            scanner->_collect_profile_before_close();
        });
    }
    ready.wait();
    go.count_down();
    for (auto& thread : threads) {
        thread.join();
    }
    EXPECT_EQ(value("BytesScannedFromRemote"), 36);
    EXPECT_EQ(value("BytesWriteIntoCache"), 16);
    EXPECT_EQ(value("NumLocalIOTotal"), n);
    auto other = make_local();
    auto scanner = make_scanner(true, other.get());
    scanner->_tablet_reader->mutable_stats()->file_cache_stats.bytes_read_from_remote = 99;
    scanner->update_realtime_counters();
    EXPECT_EQ(other->_bytes_scanned_from_remote->value(), 99);
    EXPECT_EQ(value("BytesScannedFromRemote"), 36);
}

TEST_F(OlapFileCacheProfileTest, NoCacheFallbackAndUnpreparedScanner) {
    auto unprepared = make_scanner(false);
    unprepared->update_realtime_counters();
    ASSERT_TRUE(unprepared->close(&state).ok());
    auto scanner = make_scanner();
    auto& stats = *scanner->_tablet_reader->mutable_stats();
    stats.compressed_bytes_read = 33;
    stats.uncompressed_bytes_read = 99;
    stats.raw_rows_read = 7;
    scanner->update_realtime_counters();
    stats.compressed_bytes_read = 11;
    stats.uncompressed_bytes_read = 22;
    stats.raw_rows_read = 2;
    scanner->update_realtime_counters();
    scanner->_collect_profile_before_close();
    auto* io = state.get_query_ctx()->resource_ctx()->io_context();
    EXPECT_EQ(io->scan_bytes_from_local_storage(), 44);
    EXPECT_EQ(io->scan_rows(), 9);
    EXPECT_EQ(io->scan_bytes(), 121);
    EXPECT_EQ(value("BytesScannedFromCache"), 0);
    EXPECT_EQ(value("BytesScannedFromRemote"), 0);
    EXPECT_EQ(value("BytesWriteIntoCache"), 0);
    EXPECT_EQ(value("FileCache"), 0);
}

class OlapFileCacheFinalProfileTest : public OlapFileCacheProfileTest {
protected:
    void SetUp() override {
        OlapFileCacheProfileTest::SetUp();
        _previous_reporter =
                std::exchange(ExecEnv::GetInstance()->_runtime_query_statistics_mgr, &_reporter);
        ASSERT_TRUE(_reporter.start_report_thread().ok());
    }

    void TearDown() override {
        // This fixture validates detached snapshots, not FE transport. Do not enqueue RPCs
        // when the query context is destroyed after the assertions.
        state._query_ctx_uptr->_profile_map.clear();
        state._query_ctx_uptr->_load_channel_profile_map.clear();
        auto finished = std::exchange(state._query_ctx_uptr, MockQueryContext::create());
        state._query_ctx = state._query_ctx_uptr.get();
        finished.reset();
        _reporter.stop_report_thread();
        ExecEnv::GetInstance()->_runtime_query_statistics_mgr = _previous_reporter;
    }

    std::shared_ptr<PipelineFragmentContext> context(int id) {
        state._query_options.__set_enable_profile(true);
        if (!state.get_query_ctx()->enable_profile()) {
            TNetworkAddress fe_address;
            fe_address.hostname = "127.0.0.1";
            fe_address.port = 1;
            state._query_ctx_uptr =
                    MockQueryContext::create(state.query_id(), ExecEnv::GetInstance(),
                                             state._query_options, fe_address, true, fe_address);
            state._query_ctx = state._query_ctx_uptr.get();
        }
        TPipelineFragmentParams params;
        params.__set_fragment_id(id);
        params.__set_need_notify_close(true);
        auto ctx = std::make_shared<PipelineFragmentContext>(
                state.query_id(), params, state._query_ctx_uptr, ExecEnv::GetInstance(),
                [](RuntimeState*, Status*) {});
        ctx->_prepared = true;
        ctx->_runtime_state = std::make_unique<MockRuntimeState>();
        ctx->_fragment_level_profile = std::make_unique<RuntimeProfile>("fragment");
        ctx->_fragment_level_profile->add_child(local->_segment_profile.get(), true, nullptr);
        state.set_task_execution_context(ctx);
        return ctx;
    }

    int64_t saved(int id, const std::string& name) {
        for (const auto& node : state._query_ctx_uptr->_profile_map.at(id).front()->nodes) {
            for (const auto& counter : node.counters) {
                if (counter.name == name) {
                    return counter.value;
                }
            }
        }
        return -1;
    }

    RuntimeQueryStatisticsMgr _reporter;
    RuntimeQueryStatisticsMgr* _previous_reporter = nullptr;
};

TEST_F(OlapFileCacheFinalProfileTest, LateCollectionDoesNotReplaceSavedFragmentProfile) {
    auto ctx = context(8);
    auto scanner = make_scanner();
    auto& stats = scanner->_tablet_reader->mutable_stats()->file_cache_stats;
    stats.bytes_read_from_remote = 5;
    stats.num_local_io_total = 3;
    scanner->update_realtime_counters();
    ASSERT_FALSE(ctx->_close_fragment_instance());
    auto original = state._query_ctx_uptr->_profile_map.at(8).front();
    EXPECT_EQ(saved(8, "BytesScannedFromRemote"), 5);
    EXPECT_EQ(saved(8, "NumLocalIOTotal"), -1);
    stats.bytes_read_from_remote += 12;
    scanner->_collect_profile_before_close();
    ASSERT_TRUE(scanner->close(&state).ok());
    EXPECT_EQ(value("BytesScannedFromRemote"), 17);
    EXPECT_EQ(value("NumLocalIOTotal"), 3);
    ctx.reset();
    EXPECT_EQ(state._query_ctx_uptr->_profile_map.at(8).front(), original);
    EXPECT_EQ(saved(8, "BytesScannedFromRemote"), 5);
    EXPECT_EQ(saved(8, "NumLocalIOTotal"), -1);
}

} // namespace
} // namespace doris
