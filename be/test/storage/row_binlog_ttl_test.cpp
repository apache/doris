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

#include <future>
#include <thread>

#include "agent/heartbeat_server.h"
#include "runtime/cluster_info.h"
#include "runtime/exec_env.h"
#include "storage/binlog_config.h"
#include "storage/rowset/rowset_meta.h"
#include "storage/storage_engine.h"
#include "util/defer_op.h"
#include "util/threadpool.h"

namespace doris {
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
