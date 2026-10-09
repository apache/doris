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

#include <gen_cpp/AgentService_types.h>
#include <gtest/gtest.h>

#include <chrono>
#include <future>
#include <limits>
#include <memory>
#include <thread>
#include <vector>

#include "agent/heartbeat_server.h"
#include "common/config.h"
#include "runtime/cluster_info.h"
#include "runtime/exec_env.h"
#include "storage/binlog.h"
#include "storage/binlog_config.h"
#include "storage/rowset/rowset_meta.h"
#include "storage/storage_engine.h"
#include "storage/tablet/tablet.h"
#include "util/defer_op.h"
#include "util/threadpool.h"

namespace doris {
TEST(RowBinlogTtlTest, CutoffExpiresTheEntireBoundaryMillisecond) {
    constexpr int64_t reference_tso = (123456L << kTsoLogicalBits) | 7;
    const auto cutoff = row_binlog_ttl_cutoff_tso(reference_tso, 1);
    EXPECT_EQ(122456, extract_tso_physical_time(cutoff));
    EXPECT_EQ(122457L << kTsoLogicalBits, cutoff + 1);

    RowsetMeta meta;
    meta.set_num_rows(1);
    meta.set_commit_tso(122456L << kTsoLogicalBits);
    EXPECT_TRUE(row_binlog_rowset_expired(meta, cutoff));
    meta.set_commit_tso(cutoff);
    EXPECT_TRUE(row_binlog_rowset_expired(meta, cutoff));
    meta.set_commit_tso(cutoff + 1);
    EXPECT_FALSE(row_binlog_rowset_expired(meta, cutoff));
}

TEST(RowBinlogTtlTest, CutoffClampsRetentionBeforeEpochWithoutOverflow) {
    constexpr int64_t reference_tso = 123456L << kTsoLogicalBits;
    EXPECT_EQ(0, row_binlog_ttl_cutoff_tso(reference_tso, 124));
    EXPECT_EQ(0, row_binlog_ttl_cutoff_tso(reference_tso, std::numeric_limits<int64_t>::max()));
    EXPECT_EQ(457L << kTsoLogicalBits, row_binlog_ttl_cutoff_tso(reference_tso, 123) + 1);
}

TEST(RowBinlogTtlTest, ReferenceIsMonotonicAndVolatile) {
    ClusterInfo state;
    EXPECT_EQ(0, state.row_binlog_ttl_reference_tso());
    state.advance_row_binlog_ttl_reference_tso(-1);
    EXPECT_EQ(0, state.row_binlog_ttl_reference_tso());
    std::thread a([&] {
        for (int64_t tso = 1; tso <= 10000; ++tso) {
            state.advance_row_binlog_ttl_reference_tso(tso);
        }
    });
    std::thread b([&] {
        for (int64_t tso = 20000; tso > 0; --tso) {
            state.advance_row_binlog_ttl_reference_tso(tso);
        }
    });
    a.join();
    b.join();
    state.advance_row_binlog_ttl_reference_tso(0);
    EXPECT_EQ(20000, state.row_binlog_ttl_reference_tso());
    ClusterInfo restarted;
    EXPECT_EQ(0, restarted.row_binlog_ttl_reference_tso());
}

TEST(RowBinlogTtlTest, HeartbeatFencesClusterMasterAndEpoch) {
    ExecEnv::GetInstance()->set_storage_engine(std::make_unique<StorageEngine>(EngineOptions {}));
    Defer reset_engine([] { ExecEnv::GetInstance()->set_storage_engine(nullptr); });
    {
        ClusterInfo cluster;
        cluster.cluster_id = 7;
        HeartbeatServer server(&cluster);
        TMasterInfo master;
        master.__set_cluster_id(7);
        master.__set_epoch(10);
        TNetworkAddress address;
        address.__set_hostname("master-a");
        address.__set_port(9020);
        master.__set_network_address(address);
        master.__set_row_binlog_ttl_reference_tso(100);
        THeartbeatResult result;
        server.heartbeat(result, master);
        ASSERT_EQ(TStatusCode::OK, result.status.status_code);
        EXPECT_EQ(100, cluster.row_binlog_ttl_reference_tso());
        master.__set_epoch(9);
        master.__set_row_binlog_ttl_reference_tso(200);
        server.heartbeat(result, master);
        EXPECT_EQ(100, cluster.row_binlog_ttl_reference_tso());
        master.__set_epoch(11);
        master.__set_cluster_id(8);
        server.heartbeat(result, master);
        EXPECT_NE(TStatusCode::OK, result.status.status_code);
        EXPECT_EQ(100, cluster.row_binlog_ttl_reference_tso());
        master.__set_cluster_id(7);
        address.__set_hostname("master-b");
        master.__set_network_address(address);
        server.heartbeat(result, master);
        ASSERT_EQ(TStatusCode::OK, result.status.status_code);
        EXPECT_EQ(200, cluster.row_binlog_ttl_reference_tso());
        address.__set_hostname("master-a");
        master.__set_network_address(address);
        master.__set_row_binlog_ttl_reference_tso(300);
        server.heartbeat(result, master);
        EXPECT_NE(TStatusCode::OK, result.status.status_code);
        EXPECT_EQ(200, cluster.row_binlog_ttl_reference_tso());
    }
}

TEST(RowBinlogTtlTest, SubmissionDeduplicatesAndBoundsOutstandingWork) {
    ClusterInfo cluster;
    auto* previous = ExecEnv::GetInstance()->cluster_info();
    ExecEnv::GetInstance()->set_cluster_info(&cluster);
    Defer reset_cluster([&] { ExecEnv::GetInstance()->set_cluster_info(previous); });
    StorageEngine engine(EngineOptions {});
    EXPECT_FALSE(engine.submit_row_binlog_ttl(42).ok());
    cluster.advance_row_binlog_ttl_reference_tso(100);
    EXPECT_FALSE(engine.submit_row_binlog_ttl(42).ok());
    ASSERT_TRUE(ThreadPoolBuilder("TtlQueueTest")
                        .set_min_threads(1)
                        .set_max_threads(1)
                        .set_max_queue_size(1)
                        .build(&engine._row_binlog_ttl_prepare_pool)
                        .ok());
    {
        std::promise<void> started;
        std::promise<void> release;
        auto released = release.get_future();
        Defer release_worker([&] {
            release.set_value();
            engine._row_binlog_ttl_prepare_pool->wait();
        });
        ASSERT_TRUE(engine._row_binlog_ttl_prepare_pool
                            ->submit_func([&] {
                                started.set_value();
                                released.wait();
                            })
                            .ok());
        started.get_future().wait();
        EXPECT_TRUE(engine.submit_row_binlog_ttl(42).ok());
        EXPECT_TRUE(engine.submit_row_binlog_ttl(42).is<ErrorCode::ALREADY_EXIST>());
        EXPECT_FALSE(engine.submit_row_binlog_ttl(43).ok());
    }
    // The failed queue submission must release its deduplication entry for retry.
    EXPECT_TRUE(engine.submit_row_binlog_ttl(43).ok());
    engine._row_binlog_ttl_prepare_pool->wait();
}

TEST(RowBinlogTtlTest, DisabledScannerStillPrunesExpiredRegistrationsPastLiveTablets) {
    const bool original_enable = config::enable_feature_binlog;
    const bool original_disable = config::disable_auto_compaction;
    auto* previous = ExecEnv::GetInstance()->cluster_info();
    Defer restore([&] {
        config::enable_feature_binlog = original_enable;
        config::disable_auto_compaction = original_disable;
        ExecEnv::GetInstance()->set_cluster_info(previous);
    });
    for (int disabled_by = 0; disabled_by < 3; ++disabled_by) {
        ClusterInfo cluster;
        cluster.advance_row_binlog_ttl_reference_tso(disabled_by == 2 ? 0 : 100);
        ExecEnv::GetInstance()->set_cluster_info(&cluster);
        config::enable_feature_binlog = disabled_by != 0;
        config::disable_auto_compaction = disabled_by == 1;
        StorageEngine engine(EngineOptions {});
        std::vector<std::shared_ptr<Tablet>> live;
        // A full batch of live entries must not prevent reaching evicted tablets at the tail.
        for (int64_t id = 1; id <= 64; ++id) {
            auto meta = std::make_shared<TabletMeta>(1, 2, id, 4, 5, 6, TTabletSchema(), 7,
                                                     std::unordered_map<uint32_t, uint32_t> {},
                                                     UniqueId(8, id), TTabletType::TABLET_TYPE_DISK,
                                                     TCompressionType::LZ4F);
            live.push_back(
                    std::make_shared<Tablet>(engine, meta, nullptr, CUMULATIVE_SIZE_BASED_POLICY));
            engine._row_binlog_ttl_tablets[id] = live.back();
        }
        for (int64_t id = 65; id <= 130; ++id) {
            engine._row_binlog_ttl_tablets[id] = std::weak_ptr<BaseTablet>();
        }
        ASSERT_TRUE(engine._start_row_binlog_ttl_scanner().ok());
        Defer stop([&] {
            engine._stop_background_threads_latch.count_down();
            engine._stop_row_binlog_ttl_scanner();
        });
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        size_t remaining = 130;
        do {
            {
                std::lock_guard lock(engine._row_binlog_ttl_mutex);
                remaining = engine._row_binlog_ttl_tablets.size();
                EXPECT_TRUE(engine._row_binlog_ttl_pending.empty());
            }
            if (remaining == live.size()) {
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        } while (std::chrono::steady_clock::now() < deadline);
        EXPECT_EQ(live.size(), remaining) << "disabled_by=" << disabled_by;
    }
}

TEST(RowBinlogTtlTest, RowRetentionUsesTtlSeconds) {
    BinlogConfigPB pb;
    pb.set_enable(true);
    pb.set_binlog_format(BinlogFormatPB::ROW);
    pb.set_ttl_seconds(86400);
    BinlogConfig config;
    config = pb;
    EXPECT_TRUE(config.has_row_ttl());
    EXPECT_EQ(-1, config.row_ttl_cutoff_tso(0));
    for (int64_t seconds : {-1, 0}) {
        pb.set_ttl_seconds(seconds);
        config = pb;
        EXPECT_FALSE(config.has_row_ttl());
    }
    pb.set_ttl_seconds(60);
    pb.set_binlog_format(BinlogFormatPB::STATEMENT_AND_SNAPSHOT);
    config = pb;
    EXPECT_FALSE(config.has_row_ttl());
    pb.set_binlog_format(BinlogFormatPB::ROW);
    pb.set_enable(false);
    config = pb;
    EXPECT_FALSE(config.has_row_ttl());
}

TEST(RowBinlogTtlTest, PartialMetadataUpdatePreservesTtl) {
    BinlogConfigPB pb;
    pb.set_enable(true);
    pb.set_binlog_format(BinlogFormatPB::ROW);
    pb.set_ttl_seconds(86400);
    BinlogConfig config;
    config = pb;
    BinlogConfigPB partial;
    partial.set_max_bytes(123);
    config = partial;
    EXPECT_EQ(86400, config.ttl_seconds());
    partial.set_ttl_seconds(12);
    config = partial;
    EXPECT_EQ(12, config.ttl_seconds());
    EXPECT_TRUE(config.has_row_ttl());
    config.to_pb(&pb);
    EXPECT_EQ(12, pb.ttl_seconds());
    EXPECT_EQ(123, pb.max_bytes());
}

TEST(RowBinlogTtlTest, ThriftMetadataUsesTtlSeconds) {
    TBinlogConfig thrift;
    thrift.__set_enable(true);
    thrift.__set_binlog_format(TBinlogFormat::ROW);
    thrift.__set_ttl_seconds(60);
    BinlogConfig config;
    config = thrift;
    EXPECT_TRUE(config.has_row_ttl());
    TBinlogConfig partial;
    partial.__set_max_bytes(123);
    config = partial;
    EXPECT_EQ(60, config.ttl_seconds());
    partial.__set_ttl_seconds(12);
    config = partial;
    EXPECT_EQ(12, config.ttl_seconds());
    BinlogConfigPB pb;
    config.to_pb(&pb);
    EXPECT_EQ(12, pb.ttl_seconds());
    EXPECT_EQ(123, pb.max_bytes());
}

TEST(RowBinlogTtlTest, DelayedUpdatesCannotRestoreAnOlderRetentionPolicy) {
    EngineOptions options;
    StorageEngine engine(options);
    auto meta = std::make_shared<TabletMeta>(
            1, 2, 3, 4, 5, 6, TTabletSchema(), 7, std::unordered_map<uint32_t, uint32_t> {},
            UniqueId(8, 9), TTabletType::TABLET_TYPE_DISK, TCompressionType::LZ4F);
    auto tablet = std::make_shared<Tablet>(engine, meta, nullptr, CUMULATIVE_SIZE_BASED_POLICY);
    TBinlogConfig request;
    request.__set_enable(true);
    request.__set_binlog_format(TBinlogFormat::ROW);
    request.__set_ttl_seconds(3600);
    request.__set_config_version(2);
    BinlogConfig current;
    current = request;
    ASSERT_TRUE(tablet->set_binlog_config(current).ok());
    ASSERT_TRUE(tablet->set_binlog_config(current).ok()); // Idempotent retry.

    for (int64_t version : {0, 1}) {
        request.__set_config_version(version);
        request.__set_ttl_seconds(60);
        BinlogConfig delayed;
        delayed = request;
        EXPECT_FALSE(tablet->set_binlog_config(delayed).ok());
        EXPECT_EQ(meta->binlog_config(), current);
    }

    TabletMetaPB persisted;
    meta->to_meta_pb(&persisted, false);
    TabletMeta restored;
    restored.init_from_pb(persisted);
    EXPECT_EQ(restored.binlog_config(), current);
    EXPECT_EQ(restored.binlog_config().config_version(), 2);

    // A later, durably published shrink is allowed.
    request.__set_config_version(3);
    current = request;
    EXPECT_TRUE(tablet->set_binlog_config(current).ok());
    EXPECT_EQ(meta->binlog_config().ttl_seconds(), 60);
}

TEST(RowBinlogTtlTest, OnlyCompleteValidRangesExpire) {
    RowsetMeta meta;
    meta.set_num_rows(10);
    EXPECT_FALSE(row_binlog_rowset_expired(meta, 200));
    meta.set_commit_tso({100, 200});
    EXPECT_FALSE(row_binlog_rowset_expired(meta, -1));
    EXPECT_FALSE(row_binlog_rowset_expired(meta, 199));
    EXPECT_TRUE(row_binlog_rowset_expired(meta, 200));
    meta.set_commit_tso({0, 200});
    EXPECT_FALSE(row_binlog_rowset_expired(meta, 200));
    meta.set_commit_tso({201, 200});
    EXPECT_FALSE(row_binlog_rowset_expired(meta, 200));
    meta.set_commit_tso(200);
    meta.set_num_rows(0);
    EXPECT_FALSE(row_binlog_rowset_expired(meta, 200));
}
} // namespace doris
