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
#include <fstream>
#include <limits>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "io/fs/local_file_system.h"
#include "util/cpu_info.h"

namespace doris {
using namespace config;

// These configs are checked when the test binary loads be.conf at startup,
// so their default values must pass the validators.
DEFINE_mInt16(cfg_validator_1, "5");
DEFINE_Validator(cfg_validator_1,
                 [](int16_t config) -> bool { return 0 <= config && config <= 10; });

DEFINE_mInt16(cfg_validator_2, "5");
DEFINE_Validator(cfg_validator_2,
                 [](int16_t config) -> bool { return 0 <= config && config <= 10; });

// Record the published value while the validator inspects the candidate.
// A rejected -1 must never be visible, even during the validation callback.
static int32_t published_during_validation = 0;
DEFINE_mInt32(cfg_candidate_visibility, "5");
DEFINE_Validator(cfg_candidate_visibility, [](int32_t value) {
    published_during_validation = cfg_candidate_visibility;
    return value >= 0;
});

static const std::string TEST_DIR = "./ut_dir/config_validator_test";

class ConfigValidatorTest : public testing::Test {
protected:
    void SetUp() override {
        auto st = io::global_local_filesystem()->delete_directory(TEST_DIR);
        ASSERT_TRUE(st.ok()) << st;
        st = io::global_local_filesystem()->create_directory(TEST_DIR);
        ASSERT_TRUE(st.ok()) << st;
    }

    void TearDown() override {
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(TEST_DIR).ok());
    }

    // Load a conf file with only `content` in it. set_to_default=false makes init()
    // touch only the configs in the file, so the other configs keep their values.
    static bool init_from_conf(const std::string& content) {
        const std::string conf_file = TEST_DIR + "/be.conf";
        std::ofstream(conf_file) << content;
        return config::init(conf_file.c_str(), false, true, false);
    }

    // Check one candidate without changing shared config values or allocating its capacity.
    static bool accepts32(const char* name, int32_t value) {
        return config::RegisterConfValidator::_s_field_validator->at(name)(&value);
    }

    static bool accepts64(const char* name, int64_t value) {
        return config::RegisterConfValidator::_s_field_validator->at(name)(&value);
    }
};

TEST_F(ConfigValidatorTest, Validator) {
    int16_t old_value = cfg_validator_1;
    EXPECT_FALSE(init_from_conf("cfg_validator_1 = 15\n"));
    EXPECT_EQ(cfg_validator_1, old_value);

    EXPECT_TRUE(init_from_conf("cfg_validator_1 = 8\n"));
    EXPECT_EQ(cfg_validator_1, 8);

    old_value = cfg_validator_2;
    Status s = config::set_config("cfg_validator_2", "15");
    EXPECT_FALSE(s.ok());
    EXPECT_TRUE(s.to_string().find("validate cfg_validator_2=15 failed") != std::string::npos)
            << s.to_string();
    EXPECT_EQ(cfg_validator_2, old_value);

    s = config::set_config("cfg_validator_2", "8");
    EXPECT_TRUE(s.ok());
    EXPECT_EQ(cfg_validator_2, 8);
}

TEST_F(ConfigValidatorTest, LookupConnectionCacheBlockPoolSize) {
    const auto old_value = config::get_lookup_connection_cache_block_pool_size();

    EXPECT_TRUE(config::set_config("lookup_connection_cache_block_pool_size", "1").ok());
    EXPECT_EQ(config::get_lookup_connection_cache_block_pool_size(), 1);
    EXPECT_FALSE(config::set_config("lookup_connection_cache_block_pool_size", "0").ok());
    EXPECT_FALSE(config::set_config("lookup_connection_cache_block_pool_size", "-1").ok());
    EXPECT_EQ(config::get_lookup_connection_cache_block_pool_size(), 1);

    EXPECT_TRUE(config::set_config("lookup_connection_cache_block_pool_size", "32").ok());
    EXPECT_EQ(config::get_lookup_connection_cache_block_pool_size(), 32);
    EXPECT_FALSE(config::set_config("lookup_connection_cache_block_pool_size", "33").ok());
    EXPECT_EQ(config::get_lookup_connection_cache_block_pool_size(), 32);

    EXPECT_TRUE(
            config::set_config("lookup_connection_cache_block_pool_size", std::to_string(old_value))
                    .ok());
}

TEST_F(ConfigValidatorTest, ValidateCandidateBeforePublishing) {
    ASSERT_TRUE(config::set_config("cfg_candidate_visibility", "5").ok());
    EXPECT_FALSE(config::set_config("cfg_candidate_visibility", "-1").ok());
    EXPECT_EQ(published_during_validation, 5);
    EXPECT_EQ(cfg_candidate_visibility, 5);
    EXPECT_FALSE(init_from_conf("cfg_candidate_visibility=-1\n"));
    EXPECT_EQ(published_during_validation, 5);
    EXPECT_EQ(cfg_candidate_visibility, 5);
}

TEST_F(ConfigValidatorTest, NegativeSizesAndCounts) {
    // Each case uses one config field. It must fail before any pool or buffer is created.
    // force=true exercises the same validator for startup-only and mutable fields.
    for (const auto& [name, value] : std::vector<std::pair<std::string, std::string>> {
                 {"num_query_ctx_map_partitions", "-1"},
                 {"doris_scanner_row_num", "-1"},
                 {"doris_scanner_row_bytes", "-1"},
                 {"doris_scanner_dynamic_interval_ms", "-1"},
                 {"memory_gc_sleep_time_ms", "-1"},
                 {"exchg_node_buffer_size_bytes", "-1"},
                 {"exchg_buffer_queue_capacity_factor", "-1"},
                 {"write_buffer_size", "-1"},
                 {"write_buffer_size_for_agg", "-1"},
                 {"min_write_buffer_size_for_partial_update", "-1"},
                 {"s3_file_system_local_upload_buffer_size", "-1"},
                 {"send_batch_thread_pool_thread_num", "-1"},
                 {"max_routine_load_thread_pool_size", "-1"},
                 {"fragment_mgr_async_work_pool_thread_num_max", "-1"},
                 {"file_cache_downloader_thread_num_max", "-1"},
                 {"max_s3_file_system_thread_num", "-1"},
                 {"report_exec_status_thread_num", "-1"},
                 {"group_commit_insert_threads", "-1"},
                 {"group_commit_relay_wal_threads", "-1"},
                 {"tablet_publish_txn_max_thread", "-1"},
                 {"segcompaction_num_threads", "-1"},
                 {"cooldown_thread_num", "-1"},
                 {"cold_data_compaction_thread_num", "-1"},
                 {"calc_tablet_delete_bitmap_task_max_thread", "-1"},
                 {"doris_remote_scanner_thread_pool_thread_num", "-1"},
                 {"doris_scanner_thread_pool_queue_size", "-1"},
                 {"doris_remote_scanner_thread_pool_queue_size", "-1"},
                 {"fragment_mgr_async_work_pool_thread_num_min", "-1"},
                 {"file_cache_downloader_thread_num_min", "-1"},
                 {"min_s3_file_system_thread_num", "-1"},
                 {"min_peer_race_s3_thread_num", "-1"},
                 {"fragment_mgr_async_work_pool_queue_size", "-1"},
                 {"send_batch_thread_pool_queue_size", "-1"},
                 {"memory_maintenance_sleep_time_ms", "-1"},
                 {"report_query_statistics_interval_ms", "-1"},
                 {"be_proc_monitor_interval_ms", "-1"},
                 {"workload_group_metrics_interval_ms", "-1"},
                 {"workload_policy_check_interval_ms", "-1"},
                 {"spill_gc_interval_ms", "-1"},
                 {"fragment_mgr_cancel_worker_interval_seconds", "-1"},
                 {"thrift_max_message_size", "-1"},
                 {"check_score_rounds_num", "-1"},
                 {"alter_tablet_worker_count", "-1"},
                 {"download_low_speed_limit_kbps", "-1"},
                 {"max_download_speed_kbps", "-1"},
                 {"download_low_speed_time", "-1"},
                 {"max_consumer_num_per_group", "-1"},
                 {"remote_split_source_batch_size", "-1"},
                 {"storage_page_cache_shard_size", "-1"},
                 {"tablet_map_shard_size", "-1"},
                 {"txn_map_shard_size", "-1"},
                 {"txn_shard_size", "-1"},
                 {"file_cache_mem_storage_shard_num", "-1"},
                 {"inverted_index_query_cache_shards", "-1"},
                 {"ann_index_result_cache_shards", "-1"},
                 {"thrift_connect_timeout_seconds", "-1"},
                 {"thrift_client_retry_interval_ms", "-1"},
                 {"snappy_compression_block_size", "-1"},
                 {"lz4_compression_block_size", "-1"},
                 {"brpc_heavy_work_pool_threads", "-2"},
                 {"brpc_light_work_pool_threads", "-2"},
                 {"brpc_peer_fetch_pool_threads", "-2"},
                 {"brpc_arrow_flight_work_pool_threads", "-2"},
                 {"brpc_heavy_work_pool_max_queue_size", "-2"},
                 {"brpc_light_work_pool_max_queue_size", "-2"},
                 {"brpc_peer_fetch_pool_max_queue_size", "-2"},
                 {"brpc_arrow_flight_work_pool_max_queue_size", "-2"},
                 {"compaction_batch_size", "-2"},
                 {"parquet_header_max_size_mb", "-1"},
                 {"parquet_rowgroup_max_buffer_mb", "-1"},
                 {"parquet_column_max_buffer_mb", "-1"},
                 {"s3_write_buffer_size", "-1"}}) {
        EXPECT_FALSE(init_from_conf(name + "=" + value + "\n")) << name;
        EXPECT_FALSE(config::set_config(name, value, false, true).ok()) << name;
    }
}

TEST_F(ConfigValidatorTest, PositiveCapacityBoundaries) {
    // Zero cannot supply a partition, message, queue entry, or split-source batch.
    for (const char* name :
         {"num_query_ctx_map_partitions", "thrift_max_message_size",
          "doris_scanner_thread_pool_queue_size", "remote_split_source_batch_size"}) {
        EXPECT_FALSE(accepts32(name, 0)) << name;
        EXPECT_TRUE(accepts32(name, 1)) << name;
    }
}

TEST_F(ConfigValidatorTest, ShardCountBoundaries) {
    // Mask-based indexes need powers of two. The page cache rounds up in int32.
    for (const char* name :
         {"tablet_map_shard_size", "txn_map_shard_size", "txn_shard_size",
          "file_cache_mem_storage_shard_num", "inverted_index_query_cache_shards",
          "ann_index_result_cache_shards"}) {
        EXPECT_FALSE(accepts32(name, 0)) << name;
        EXPECT_FALSE(accepts32(name, 3)) << name;
        EXPECT_TRUE(accepts32(name, 1)) << name;
        EXPECT_TRUE(accepts32(name, 2)) << name;
    }
    EXPECT_TRUE(accepts32("storage_page_cache_shard_size", 1 << 30));
    EXPECT_FALSE(accepts32("storage_page_cache_shard_size", (1 << 30) + 1));
}

TEST_F(ConfigValidatorTest, OptionalZeroAndTimeoutBoundaries) {
    // Zero keeps its existing meaning. Seconds must still fit after conversion to milliseconds.
    EXPECT_TRUE(accepts32("download_low_speed_time", 0));
    EXPECT_TRUE(accepts32("max_download_speed_kbps", 0));
    EXPECT_TRUE(accepts32("min_peer_race_s3_thread_num", 0));
    EXPECT_TRUE(accepts32("thrift_connect_timeout_seconds", 0));
    EXPECT_FALSE(accepts32("thrift_connect_timeout_seconds", 2147484));
    EXPECT_TRUE(accepts32("thrift_connect_timeout_seconds", 2147483));
}

TEST_F(ConfigValidatorTest, DefaultCapacitySentinels) {
    // -1 selects the existing default. Zero is not a usable capacity.
    for (const char* name :
         {"brpc_heavy_work_pool_threads", "brpc_peer_fetch_pool_threads",
          "brpc_light_work_pool_threads", "brpc_heavy_work_pool_max_queue_size",
          "brpc_peer_fetch_pool_max_queue_size", "brpc_light_work_pool_max_queue_size",
          "brpc_arrow_flight_work_pool_threads", "brpc_arrow_flight_work_pool_max_queue_size"}) {
        EXPECT_TRUE(accepts32(name, -1)) << name;
        EXPECT_FALSE(accepts32(name, 0)) << name;
        EXPECT_TRUE(accepts32(name, 1)) << name;
    }
    EXPECT_TRUE(accepts64("compaction_batch_size", -1));
    EXPECT_FALSE(accepts64("compaction_batch_size", 0));
    EXPECT_TRUE(accepts64("compaction_batch_size", 2147483647));
    EXPECT_FALSE(accepts64("compaction_batch_size", 2147483648LL));
    // The retry path doubles this value. Check both sides without sleeping.
    EXPECT_TRUE(
            accepts64("thrift_client_retry_interval_ms", std::numeric_limits<int64_t>::max() / 2));
    EXPECT_FALSE(accepts64("thrift_client_retry_interval_ms",
                           std::numeric_limits<int64_t>::max() / 2 + 1));
    EXPECT_FALSE(accepts32("storage_page_cache_shard_size", 0));
}

TEST_F(ConfigValidatorTest, CompressionAndParquetBoundaries) {
    // Compression needs room for one input byte and its header. Parquet zero disables prefetch.
    EXPECT_FALSE(accepts32("snappy_compression_block_size", 38));
    EXPECT_TRUE(accepts32("snappy_compression_block_size", 39));
    EXPECT_FALSE(accepts32("lz4_compression_block_size", 16));
    EXPECT_TRUE(accepts32("lz4_compression_block_size", 17));
    EXPECT_TRUE(accepts32("parquet_column_max_buffer_mb", 0));
    EXPECT_TRUE(accepts32("parquet_column_max_buffer_mb", 4096));
}

TEST_F(ConfigValidatorTest, ScannerExchangeAndWriteBoundaries) {
    // These zeros preserve one-block scans, immediate checks, or immediate backpressure.
    for (const char* name :
         {"doris_scanner_row_num", "doris_scanner_row_bytes", "doris_scanner_dynamic_interval_ms",
          "memory_gc_sleep_time_ms", "exchg_node_buffer_size_bytes",
          "exchg_buffer_queue_capacity_factor"}) {
        EXPECT_TRUE(accepts32(name, 0)) << name;
    }
    // Check the largest adaptive buffer without allocating it.
    for (const char* name : {"write_buffer_size", "write_buffer_size_for_agg",
                             "min_write_buffer_size_for_partial_update"}) {
        EXPECT_TRUE(accepts64(name, 0)) << name;
    }
    EXPECT_TRUE(accepts64("write_buffer_size", std::numeric_limits<int64_t>::max() / 4));
    EXPECT_FALSE(accepts64("write_buffer_size", std::numeric_limits<int64_t>::max() / 4 + 1));
}

TEST_F(ConfigValidatorTest, WriterSizeWithoutFileCache) {
    // Disabling the cache must not disable the writer's positive-size requirement.
    bool original_cache = config::enable_file_cache;
    config::enable_file_cache = false;
    EXPECT_FALSE(accepts64("s3_write_buffer_size", 0));
    EXPECT_FALSE(accepts64("s3_write_buffer_size", -1));
    EXPECT_TRUE(accepts64("s3_write_buffer_size", 1));
    config::enable_file_cache = original_cache;
}

} // namespace doris
