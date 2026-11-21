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

#include <atomic>
#include <cstdint>
#include <latch>
#include <map>
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
    bool fail_init = false;
    int profile_updates = 0;
    Status init(const ReaderParams&) override {
        if (fail_init) {
            mutable_stats()->file_cache_stats.bytes_read_from_remote = 23;
            return Status::InternalError("reader init failure");
        }
        return Status::OK();
    }
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

    void expect_thrift_value(RuntimeProfile* profile, const std::string& name, int64_t expected) {
        TRuntimeProfileTree tree;
        profile->to_thrift(&tree, 3);
        bool found = false;
        for (const auto& node : tree.nodes) {
            for (const auto& counter : node.counters) {
                if (counter.name == name) {
                    EXPECT_EQ(counter.value, expected) << name;
                    found = true;
                }
            }
        }
        EXPECT_TRUE(found) << name;
    }

    std::atomic<int64_t> next_tablet_id {20};
    ObjectPool pool;
    MockRuntimeState state;
    RuntimeProfile scanner_profile {"scanner"};
    std::shared_ptr<OlapScanOperatorX> op;
    std::shared_ptr<OlapScanLocalState> local;
};

TEST_F(OlapFileCacheProfileTest, MultiBatchRealtimeAndFinal) {
    auto scanner = make_scanner();
    scanner->_initial_file_cache_stats.bytes_read_from_local = 3;
    scanner->_initial_file_cache_stats.bytes_read_from_remote = 5;
    scanner->_initial_file_cache_stats.bytes_write_into_cache = 2;
    scanner->_initial_file_cache_stats.segment_footer_index_bytes_read_from_remote = 11;
    ASSERT_TRUE(scanner->_open_impl(&state).ok()); // Exercise the actual preload merge entry.
    auto& stats = *scanner->_tablet_reader->mutable_stats();
    auto* io = state.get_query_ctx()->resource_ctx()->io_context();
    const auto local_metric = DorisMetrics::instance()->query_scan_bytes_from_local->value();
    const auto remote_metric = DorisMetrics::instance()->query_scan_bytes_from_remote->value();
    stats.file_cache_stats.bytes_read_from_local += 7;
    stats.file_cache_stats.bytes_read_from_remote += 13;
    stats.file_cache_stats.bytes_write_into_cache += 4;
    stats.file_cache_stats.local_io_timer = 31;
    stats.compressed_bytes_read = 19;
    stats.uncompressed_bytes_read = 100;
    stats.raw_rows_read = 10;
    scanner->update_realtime_counters();
    EXPECT_EQ(value("BytesScannedFromCache"), 10);
    EXPECT_EQ(value("BytesScannedFromRemote"), 18);
    EXPECT_EQ(value("BytesWriteIntoCache"), 6);
    EXPECT_EQ(local->_segment_profile->get_counter("NumLocalIOTotal"), nullptr);
    EXPECT_EQ(value("FileCache"), 0);
    EXPECT_EQ(stats.file_cache_stats.bytes_read_from_local, 10);

    // This batch has no main cache reads, even though the lifetime totals are nonzero.
    stats.file_cache_stats.bytes_read_from_peer = 9;
    stats.compressed_bytes_read = 8;
    stats.uncompressed_bytes_read = 35;
    stats.raw_rows_read = 3;
    scanner->update_realtime_counters();
    scanner->update_realtime_counters();
    EXPECT_EQ(value("BytesScannedFromRemote"), 18);
    EXPECT_EQ(io->scan_bytes_from_local_storage(), 18);
    EXPECT_EQ(io->bytes_write_into_cache(), 6);

    stats.file_cache_stats.bytes_read_from_remote += 17;
    stats.file_cache_stats.bytes_write_into_cache += 6;
    stats.compressed_bytes_read = 17;
    stats.uncompressed_bytes_read = 60;
    stats.raw_rows_read = 4;
    scanner->_collect_profile_before_close();
    EXPECT_EQ(value("BytesScannedFromCache"), 10);
    EXPECT_EQ(value("BytesScannedFromRemote"), 35);
    EXPECT_EQ(value("BytesWriteIntoCache"), 12);
    EXPECT_EQ(value("BytesScannedFromPeer"), 9);
    EXPECT_EQ(value("FileCache"), 31);
    EXPECT_EQ(value("SegmentFooterIndexBytesScannedFromRemote"), 11);
    EXPECT_EQ(io->scan_rows(), 17);
    EXPECT_EQ(io->scan_bytes(), 195);
    EXPECT_EQ(io->scan_bytes_from_local_storage(), 18);
    EXPECT_EQ(io->scan_bytes_from_remote_storage(), 35);
    EXPECT_EQ(io->bytes_write_into_cache(), 12);
    EXPECT_EQ(local->_scan_rows->value(), 17);
    EXPECT_EQ(local->_scan_bytes->value(), 195);
    EXPECT_EQ(local->_read_compressed_counter->value(), 44);
    EXPECT_EQ(DorisMetrics::instance()->query_scan_bytes_from_local->value() - local_metric, 18);
    EXPECT_EQ(DorisMetrics::instance()->query_scan_bytes_from_remote->value() - remote_metric, 35);
    scanner->_collect_profile_before_close();
    ASSERT_TRUE(scanner->close(&state).ok());
    ASSERT_TRUE(scanner->close(&state).ok());
    EXPECT_EQ(io->bytes_write_into_cache(), 12);
    EXPECT_EQ(value("BytesScannedFromPeer"), 9);
    EXPECT_EQ(static_cast<ProfileReader*>(scanner->_tablet_reader.get())->profile_updates, 1);
}

TEST_F(OlapFileCacheProfileTest, EmptyUnpreparedAndFailedInit) {
    auto unprepared = make_scanner(false);
    unprepared->update_realtime_counters();
    ASSERT_TRUE(unprepared->close(&state).ok());
    auto empty = make_scanner();
    ASSERT_TRUE(empty->close(&state).ok());
    EXPECT_EQ(local->_segment_profile->get_counter("FileCache"), nullptr);

    auto failed = make_scanner();
    auto* reader = static_cast<ProfileReader*>(failed->_tablet_reader.get());
    reader->fail_init = true;
    failed->_initial_file_cache_stats.bytes_read_from_local = 100; // Never merged on failure.
    ASSERT_FALSE(failed->_open_impl(&state).ok());
    failed->_collect_profile_before_close();
    ASSERT_TRUE(failed->close(&state).ok());
    EXPECT_EQ(reader->profile_updates, 0);
    EXPECT_EQ(value("BytesScannedFromRemote"), 23);
    EXPECT_EQ(value("BytesScannedFromCache"), 0);
}

TEST_F(OlapFileCacheProfileTest, ConcurrentLazyInitializationAndIsolatedState) {
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
            stats.remote_only_on_miss_triggered = i % 2;
            stats.remote_only_on_miss_threshold_bytes = (i + 1) * 1024;
            stats.num_peer_io_total = 1;
            ready.count_down();
            go.wait();
            scanner->update_realtime_counters();
            scanner->_flush_file_cache_profile_once();
        });
    }
    ready.wait();
    go.count_down();
    for (auto& thread : threads) {
        thread.join();
    }
    EXPECT_EQ(value("BytesScannedFromRemote"), 36);
    EXPECT_EQ(value("BytesWriteIntoCache"), 16);
    EXPECT_EQ(value("NumPeerIOTotal"), n);
    EXPECT_EQ(local->_file_cache_bytes->bytes_scanned_from_remote,
              local->_file_cache_details->bytes_scanned_from_remote);
    expect_thrift_value(local->_segment_profile.get(), "RemoteOnlyOnMissTriggered", 1);
    expect_thrift_value(local->_segment_profile.get(), "RemoteOnlyOnMissThresholdBytes", 8192);
    auto zero = make_scanner();
    zero->_tablet_reader->mutable_stats()->file_cache_stats.remote_only_on_miss_threshold_bytes = 1;
    zero->_flush_file_cache_profile_once();
    expect_thrift_value(local->_segment_profile.get(), "RemoteOnlyOnMissTriggered", 1);
    expect_thrift_value(local->_segment_profile.get(), "RemoteOnlyOnMissThresholdBytes", 8192);
    auto other = make_local();
    auto isolated = make_scanner(true, other.get());
    isolated->_tablet_reader->mutable_stats()->file_cache_stats.bytes_read_from_remote = 99;
    isolated->_flush_file_cache_profile_once();
    EXPECT_NE(other->_file_cache_bytes->bytes_scanned_from_remote,
              local->_file_cache_bytes->bytes_scanned_from_remote);
    EXPECT_EQ(value("BytesScannedFromRemote"), 36);
}

TEST_F(OlapFileCacheProfileTest, EveryStatisticsFieldSurvivesFinalization) {
    // Include every integer, plus host-only input: no hand-maintained nonempty predicate.
    const std::vector<int64_t io::FileCacheStatistics::*> fields = {
            &io::FileCacheStatistics::num_local_io_total,
            &io::FileCacheStatistics::num_remote_io_total,
            &io::FileCacheStatistics::num_peer_io_total,
            &io::FileCacheStatistics::local_io_timer,
            &io::FileCacheStatistics::bytes_read_from_local,
            &io::FileCacheStatistics::bytes_read_from_remote,
            &io::FileCacheStatistics::bytes_read_from_peer,
            &io::FileCacheStatistics::remote_io_timer,
            &io::FileCacheStatistics::peer_io_timer,
            &io::FileCacheStatistics::remote_wait_timer,
            &io::FileCacheStatistics::write_cache_io_timer,
            &io::FileCacheStatistics::bytes_write_into_cache,
            &io::FileCacheStatistics::num_skip_cache_io_total,
            &io::FileCacheStatistics::read_cache_file_directly_timer,
            &io::FileCacheStatistics::cache_get_or_set_timer,
            &io::FileCacheStatistics::lock_wait_timer,
            &io::FileCacheStatistics::get_timer,
            &io::FileCacheStatistics::set_timer,
            &io::FileCacheStatistics::io_adaptor_lookup_count,
            &io::FileCacheStatistics::io_adaptor_lookup_time_ns,
            &io::FileCacheStatistics::io_adaptor_local_reads,
            &io::FileCacheStatistics::io_adaptor_local_time_ns,
            &io::FileCacheStatistics::io_adaptor_bypass_count,
            &io::FileCacheStatistics::io_adaptor_bypass_bytes,
            &io::FileCacheStatistics::io_adaptor_remote_bytes,
            &io::FileCacheStatistics::io_adaptor_remote_time_ns,
            &io::FileCacheStatistics::io_adaptor_local_probes,
            &io::FileCacheStatistics::io_adaptor_remote_probes,
            &io::FileCacheStatistics::io_adaptor_remote_failures,
            &io::FileCacheStatistics::io_adaptor_fallback_local,
            &io::FileCacheStatistics::async_cache_write_submitted,
            &io::FileCacheStatistics::async_cache_write_rejected,
            &io::FileCacheStatistics::async_cache_write_buffer_alloc_fail,
            &io::FileCacheStatistics::async_cache_write_drop_stale_epoch,
            &io::FileCacheStatistics::async_copy_to_user_timer,
            &io::FileCacheStatistics::async_submit_write_timer,
            &io::FileCacheStatistics::inflight_write_buffer_index_hit,
            &io::FileCacheStatistics::inflight_write_buffer_index_miss,
            &io::FileCacheStatistics::probe_downloaded_hit,
            &io::FileCacheStatistics::probe_downloading_hit,
            &io::FileCacheStatistics::probe_miss,
            &io::FileCacheStatistics::block_wait_success,
            &io::FileCacheStatistics::block_wait_timeout,
            &io::FileCacheStatistics::inverted_index_num_local_io_total,
            &io::FileCacheStatistics::inverted_index_num_remote_io_total,
            &io::FileCacheStatistics::inverted_index_num_peer_io_total,
            &io::FileCacheStatistics::inverted_index_bytes_read_from_local,
            &io::FileCacheStatistics::inverted_index_bytes_read_from_remote,
            &io::FileCacheStatistics::inverted_index_bytes_read_from_peer,
            &io::FileCacheStatistics::inverted_index_remote_physical_read_bytes,
            &io::FileCacheStatistics::inverted_index_bytes_write_into_cache,
            &io::FileCacheStatistics::inverted_index_local_io_timer,
            &io::FileCacheStatistics::inverted_index_remote_io_timer,
            &io::FileCacheStatistics::inverted_index_peer_io_timer,
            &io::FileCacheStatistics::inverted_index_io_timer,
            &io::FileCacheStatistics::inverted_index_write_cache_io_timer,
            &io::FileCacheStatistics::inverted_index_request_bytes,
            &io::FileCacheStatistics::inverted_index_read_bytes,
            &io::FileCacheStatistics::inverted_index_range_read_count,
            &io::FileCacheStatistics::inverted_index_serial_read_rounds,
            &io::FileCacheStatistics::segment_footer_index_num_local_io_total,
            &io::FileCacheStatistics::segment_footer_index_num_remote_io_total,
            &io::FileCacheStatistics::segment_footer_index_num_peer_io_total,
            &io::FileCacheStatistics::segment_footer_index_bytes_read_from_local,
            &io::FileCacheStatistics::segment_footer_index_bytes_read_from_remote,
            &io::FileCacheStatistics::segment_footer_index_bytes_read_from_peer,
            &io::FileCacheStatistics::segment_footer_index_local_io_timer,
            &io::FileCacheStatistics::segment_footer_index_remote_io_timer,
            &io::FileCacheStatistics::segment_footer_index_peer_io_timer,
            &io::FileCacheStatistics::segment_footer_index_write_cache_io_timer,
            &io::FileCacheStatistics::segment_footer_index_bytes_write_into_cache,
            &io::FileCacheStatistics::remote_only_on_miss_triggered,
            &io::FileCacheStatistics::remote_only_on_miss_threshold_bytes,
            &io::FileCacheStatistics::num_cross_cg_peer_io_total,
            &io::FileCacheStatistics::bytes_read_from_cross_cg_peer,
            &io::FileCacheStatistics::cross_cg_peer_io_timer,
            &io::FileCacheStatistics::num_same_cg_peer_io_total,
            &io::FileCacheStatistics::bytes_read_from_same_cg_peer,
            &io::FileCacheStatistics::same_cg_peer_io_timer,
            &io::FileCacheStatistics::num_peer_race_peer_win,
            &io::FileCacheStatistics::num_peer_race_s3_win,
            &io::FileCacheStatistics::num_peer_lazy_fetch,
            &io::FileCacheStatistics::peer_lazy_fetch_timer,
    };
    for (auto field : fields) {
        auto parent = make_local();
        auto scanner = make_scanner(true, parent.get());
        io::FileCacheStatistics expected;
        expected.*field = 7;
        scanner->_tablet_reader->mutable_stats()->file_cache_stats = expected;
        scanner->update_realtime_counters();
        scanner->_flush_file_cache_profile_once();
        RuntimeProfile reference("SegmentIterator");
        io::FileCacheProfileReporter full(&reference);
        full.update(&expected);
        // Compare every FileCache counter, including the serialized HWM current value.
        TRuntimeProfileTree reference_tree, actual;
        reference.to_thrift(&reference_tree, 3);
        parent->_segment_profile->to_thrift(&actual, 3);
        std::map<std::string, int64_t> actual_values;
        for (const auto& counter : actual.nodes.front().counters) {
            actual_values.emplace(counter.name, counter.value);
        }
        for (const auto& counter : reference_tree.nodes.front().counters) {
            ASSERT_TRUE(actual_values.contains(counter.name)) << counter.name;
            EXPECT_EQ(actual_values.at(counter.name), counter.value) << counter.name;
        }
    }
    auto scanner = make_scanner();
    scanner->_tablet_reader->mutable_stats()->file_cache_stats.peer_hosts = {"peer-a", "peer-b"};
    scanner->_flush_file_cache_profile_once();
    EXPECT_EQ(*local->_segment_profile->get_info_string("PeerCacheNodes"), "peer-a, peer-b");
}

TEST_F(OlapFileCacheProfileTest, NoCacheFallbackDoesNotRegisterFileCacheCounters) {
    auto scanner = make_scanner();
    auto& stats = *scanner->_tablet_reader->mutable_stats();
    stats.compressed_bytes_read = 33;
    stats.uncompressed_bytes_read = 99;
    stats.raw_rows_read = 7;
    scanner->update_realtime_counters();
    stats.compressed_bytes_read = 11;
    stats.uncompressed_bytes_read = 22;
    stats.raw_rows_read = 2;
    ASSERT_TRUE(scanner->close(&state).ok());
    auto* io = state.get_query_ctx()->resource_ctx()->io_context();
    EXPECT_EQ(io->scan_bytes_from_local_storage(), 44);
    EXPECT_EQ(io->scan_rows(), 9);
    EXPECT_EQ(io->scan_bytes(), 121);
    EXPECT_EQ(local->_segment_profile->get_counter("FileCache"), nullptr);
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

TEST_F(OlapFileCacheFinalProfileTest, NoLateFlushDoesNotSerializeAgain) {
    auto ctx = context(8);
    auto scanner = make_scanner();
    scanner->_tablet_reader->mutable_stats()->file_cache_stats.bytes_read_from_remote = 11;
    ASSERT_TRUE(scanner->_open_impl(&state).ok());
    scanner->_collect_profile_before_close();
    ASSERT_FALSE(ctx->_close_fragment_instance());
    auto original = state._query_ctx_uptr->_profile_map.at(8).front();
    ASSERT_TRUE(scanner->close(&state).ok());
    ctx.reset();
    EXPECT_EQ(state._query_ctx_uptr->_profile_map.at(8).front(), original);
    EXPECT_EQ(saved(8, "BytesScannedFromRemote"), 11);
}

TEST_F(OlapFileCacheFinalProfileTest, LateDelegateFlushRefreshesActualSavedSnapshot) {
    for (bool cancelled : {false, true}) {
        const int id = cancelled ? 2 : 1;
        auto ctx = context(id);
        auto idle = make_scanner();
        idle->_tablet_reader->mutable_stats()->file_cache_stats.bytes_read_from_remote = 5;
        ASSERT_TRUE(idle->close(&state).ok());
        auto active = make_scanner();
        ScannerSPtr scanner_base = active;
        auto delegate = std::make_shared<ScannerDelegate>(scanner_base);
        std::latch started(1), finish(1);
        std::thread worker([&, task_lock = ctx, worker_delegate = delegate, active]() mutable {
            SCOPED_ATTACH_TASK(&state);
            started.count_down();
            finish.wait();
            active->_tablet_reader->mutable_stats()->file_cache_stats.bytes_read_from_remote = 17;
            active->update_realtime_counters();
            worker_delegate.reset();
            active.reset();
            task_lock.reset();
        });
        delegate.reset();
        started.wait();
        if (cancelled) {
            state._query_ctx_uptr->_exec_status.update(Status::Cancelled("cancelled"));
        }
        EXPECT_FALSE(ctx->_close_fragment_instance());
        EXPECT_EQ(saved(id, "BytesScannedFromRemote"), cancelled ? 27 : 5);
        auto original = state._query_ctx_uptr->_profile_map.at(id).front();
        ctx.reset();
        finish.count_down();
        worker.join();
        active.reset();
        scanner_base.reset();
        EXPECT_NE(state._query_ctx_uptr->_profile_map.at(id).front(), original);
        EXPECT_EQ(saved(id, "BytesScannedFromRemote"), cancelled ? 44 : 22);
    }
}

TEST_F(OlapFileCacheFinalProfileTest, FlushDuringSerializationIsNotMistakenForSavedEpoch) {
    RuntimeProfile after_segment("after-segment");
    auto ctx = context(9);
    auto scanner = make_scanner();
    scanner->_tablet_reader->mutable_stats()->file_cache_stats.bytes_read_from_remote = 5;
    scanner->update_realtime_counters();
    ctx->_fragment_level_profile->add_child(&after_segment, true, nullptr);
    bool flush = true;
    after_segment.add_derived_counter(
            "FlushScanner", TUnit::UNIT,
            [&] {
                if (flush) {
                    flush = false;
                    scanner->_tablet_reader->mutable_stats()
                            ->file_cache_stats.bytes_read_from_remote = 17;
                    scanner->_flush_file_cache_profile_once();
                }
                return 0;
            },
            RuntimeProfile::ROOT_COUNTER);
    ASSERT_FALSE(ctx->_close_fragment_instance());
    EXPECT_EQ(saved(9, "BytesScannedFromRemote"), 5);
    auto load_channel = std::make_shared<TRuntimeProfileTree>();
    state._query_ctx_uptr->_load_channel_profile_map[9] = load_channel;
    ctx.reset();
    EXPECT_EQ(saved(9, "BytesScannedFromRemote"), 17);
    EXPECT_EQ(state._query_ctx_uptr->_load_channel_profile_map.at(9), load_channel);
}

TEST_F(OlapFileCacheFinalProfileTest, RecursiveStageCannotReplaceFirstSnapshotOwner) {
    auto first = context(7);
    auto scanner = make_scanner();
    scanner->_tablet_reader->mutable_stats()->file_cache_stats.bytes_read_from_remote = 11;
    scanner->_flush_file_cache_profile_once();
    ASSERT_FALSE(first->_close_fragment_instance());
    auto original = state._query_ctx_uptr->_profile_map.at(7).front();
    EXPECT_EQ(saved(7, "BytesScannedFromRemote"), 11);
    auto next = context(7);
    ASSERT_FALSE(next->_close_fragment_instance());
    EXPECT_FALSE(next->_owns_saved_fragment_profile);
    next->notify_olap_file_cache_profile();
    next.reset();
    EXPECT_EQ(state._query_ctx_uptr->_profile_map.at(7).front(), original);
    state.set_task_execution_context(first);
    auto late = make_scanner();
    late->_tablet_reader->mutable_stats()->file_cache_stats.bytes_read_from_remote = 13;
    late->_flush_file_cache_profile_once();
    first.reset();
    EXPECT_NE(state._query_ctx_uptr->_profile_map.at(7).front(), original);
    EXPECT_EQ(saved(7, "BytesScannedFromRemote"), 24);
}

} // namespace
} // namespace doris
