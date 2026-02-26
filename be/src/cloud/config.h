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

#pragma once

#include "common/config.h"

namespace doris::config {
#include "common/compile_check_begin.h"

DECLARE_String(deploy_mode);
// deprecated do not configure directly
DECLARE_mString(cloud_unique_id);

static inline bool is_cloud_mode() {
    return deploy_mode == "cloud" || !cloud_unique_id.empty();
}

// Set the endpoint of meta service.
//
// If meta services are deployed behind a load balancer, set this config to "host:port" of the load balancer.
// Here is a set of configs to configure the connection behaviors:
// - meta_service_connection_pooled: distribute the long connections to different RS of the VIP.
// - meta_service_connection_age_base_seconds: expire the connection after a random time during [base, 2*base],
//      so that the BE has a chance to connect to a new RS. (When you add a new RS, the BE will connect to it)
// - meta_service_idle_connection_timeout_ms: rebuild the idle connections after the timeout exceeds. Some LB
// vendors will reset the connection if it is idle for a long time.
//
// If you want to access a group of meta services directly, set the addresses of meta services to this config,
// separated by a comma, like "host:port,host:port,host:port", then BE will choose a server to connect in randomly.
// In this mode, The config meta_service_connection_pooled is still useful, but the other two configs will be ignored.
DECLARE_mString(meta_service_endpoint);
// Whether check config::meta_service_endpoint is identical to the ms endpoint from FE master heartbeat
// This may help in some cases that we intend to change the config only FE side or BE side
DECLARE_mBool(enable_meta_service_endpoint_consistency_check);
// Set the underlying connection type to pooled.
DECLARE_Bool(meta_service_connection_pooled);
DECLARE_mInt64(meta_service_connection_pool_size);
// A connection will expire after a random time during [base, 2*base], so that the BE
// has a chance to connect to a new RS. Set zero to disable it.
//
// Only works when meta_service_endpoint is set to a single host.
DECLARE_mInt32(meta_service_connection_age_base_seconds);
// Rebuild the idle connections after the timeout exceeds. Set zero to disable it.
//
// Only works when meta_service_endpoint is set to a single host.
DECLARE_mInt32(meta_service_idle_connection_timeout_ms);
DECLARE_mInt32(meta_service_rpc_timeout_ms);
DECLARE_mInt32(meta_service_rpc_retry_times);
// default brpc timeout
DECLARE_mInt32(meta_service_brpc_timeout_ms);
DECLARE_mInt32(meta_service_rpc_timeout_retry_times);

// CloudTabletMgr config
DECLARE_Int64(tablet_cache_capacity);
DECLARE_Int64(tablet_cache_shards);
DECLARE_mInt32(tablet_sync_interval_s);
// parallelism for scanner init where may issue RPCs to sync rowset meta from MS
DECLARE_mInt32(init_scanner_sync_rowsets_parallelism);
DECLARE_mInt32(sync_rowsets_slow_threshold_ms);

// Cloud compaction config
DECLARE_mInt64(min_compaction_failure_interval_ms);
DECLARE_mBool(enable_new_tablet_do_compaction);
// Enable empty rowset compaction strategy
DECLARE_mBool(enable_empty_rowset_compaction);
// Minimum number of consecutive empty rowsets to trigger compaction
DECLARE_mInt32(empty_rowset_compaction_min_count);
// Minimum percentage of empty rowsets to trigger compaction
DECLARE_mDouble(empty_rowset_compaction_min_ratio);
// For cloud read/write separate mode
DECLARE_mInt64(base_compaction_freeze_interval_s);
DECLARE_mInt64(compaction_load_max_freeze_interval_s);
DECLARE_mInt64(cumu_compaction_interval_s);

DECLARE_mInt32(compaction_timeout_seconds);
DECLARE_mInt32(lease_compaction_interval_seconds);
DECLARE_mBool(enable_parallel_cumu_compaction);
DECLARE_mDouble(base_compaction_thread_num_factor);
DECLARE_mDouble(cumu_compaction_thread_num_factor);
DECLARE_mInt32(check_auto_compaction_interval_seconds);
DECLARE_mInt32(max_base_compaction_task_num_per_disk);
DECLARE_mBool(prioritize_query_perf_in_compaction);
DECLARE_mInt32(compaction_max_rowset_count);
DECLARE_mInt64(compaction_txn_max_size_bytes);

// CloudStorageEngine config
DECLARE_mInt32(refresh_s3_info_interval_s);
DECLARE_mInt32(vacuum_stale_rowsets_interval_s);
DECLARE_mInt32(schedule_sync_tablets_interval_s);

// Cloud mow
DECLARE_mInt32(mow_stream_load_commit_retry_times);

DECLARE_mBool(save_load_error_log_to_s3);

// Whether to use public endpoint for error log presigned URL
DECLARE_mBool(use_public_endpoint_for_error_log);

// the theads which sync the datas which loaded in other clusters
DECLARE_mInt32(sync_load_for_tablets_thread);

DECLARE_Int32(warmup_cache_async_thread);

DECLARE_mInt32(delete_bitmap_lock_expiration_seconds);

DECLARE_mInt32(get_delete_bitmap_lock_max_retry_times);

DECLARE_mBool(enable_sync_tablet_delete_bitmap_by_cache);
// 1: write v1, 2: write v2, 3: double write v1 and v2
DECLARE_Int32(delete_bitmap_store_write_version);
// 1: read v1, 2: read v2, 3: double read v1 and v2
DECLARE_Int32(delete_bitmap_store_read_version);
// for store v2
DECLARE_mBool(enable_agg_delta_delete_bitmap_for_store_v2);
DECLARE_mInt64(delete_bitmap_store_v2_max_bytes_in_fdb);
DECLARE_Int32(sync_delete_bitmap_task_max_thread);
DECLARE_mBool(enable_delete_bitmap_store_v2_check_correctness);

DECLARE_mBool(enable_batch_get_delete_bitmap);
DECLARE_mInt64(get_delete_bitmap_bytes_threshold);

// Skip writing empty rowset metadata to meta service
DECLARE_mBool(skip_writing_empty_rowset_metadata);

// enable large txn lazy commit in meta-service `commit_txn`
DECLARE_mBool(enable_cloud_txn_lazy_commit);

DECLARE_mInt32(remove_expired_tablet_txn_info_interval_seconds);

DECLARE_mInt32(tablet_txn_info_min_expired_seconds);

DECLARE_mBool(enable_use_cloud_unique_id_from_fe);

DECLARE_Bool(enable_cloud_tablet_report);

DECLARE_mInt32(delete_bitmap_rpc_retry_times);

DECLARE_mInt64(meta_service_rpc_reconnect_interval_ms);

DECLARE_mInt32(meta_service_conflict_error_retry_times);

DECLARE_Bool(enable_check_storage_vault);

DECLARE_mInt64(cloud_index_change_task_timeout_second);

DECLARE_mInt64(warmup_tablet_replica_info_cache_ttl_sec);

DECLARE_mInt64(warm_up_rowset_slow_log_ms);

DECLARE_mInt32(warm_up_manager_thread_pool_size);

// When event driven warm-up is enabled by the user, turning on this option can help
// avoid file cache misses in the read cluster caused by compaction.
// If enabled, compaction will wait for the warm-up to complete before committing.
//
// ATTN: Enabling this option may slow down compaction due to the added wait.
DECLARE_mBool(enable_compaction_delay_commit_for_warm_up);

DECLARE_mInt64(warm_up_rowset_sync_wait_min_timeout_ms);

DECLARE_mInt64(warm_up_rowset_sync_wait_max_timeout_ms);

DECLARE_mBool(enable_warmup_immediately_on_new_rowset);

// Packed file manager config
DECLARE_mBool(enable_packed_file);
DECLARE_mInt64(packed_file_size_threshold_bytes);
DECLARE_mInt64(packed_file_time_threshold_ms);
DECLARE_mInt64(packed_file_try_lock_timeout_ms);
DECLARE_mInt64(packed_file_small_file_count_threshold);
DECLARE_mInt64(small_file_threshold_bytes);
DECLARE_mInt64(uploaded_file_retention_seconds);
DECLARE_mInt64(packed_file_cleanup_interval_seconds);

DECLARE_mBool(enable_standby_passive_compaction);

DECLARE_mDouble(standby_compaction_version_ratio);

// Compaction read-write separation: only the "last active" cluster (the one that most recently
// performed load) is allowed to compact a tablet
DECLARE_mBool(enable_compaction_rw_separation);
// Timeout in ms for takeover when last active cluster becomes unavailable (default 5 min)
DECLARE_mInt64(compaction_cluster_takeover_timeout_ms);
// Interval in seconds to refresh cluster status cache for compaction read-write separation
DECLARE_mInt64(cluster_status_cache_refresh_interval_sec);
// When version count exceeds this ratio of max_tablet_version_num, force compaction
// even on read-only clusters (safety valve to prevent unbounded version growth)
DECLARE_mDouble(compaction_rw_separation_version_threshold_ratio);

DECLARE_mBool(enable_cache_read_from_peer);

// Rate limit for warmup download in bytes per second, default 100MB/s
// <= 0 means no limit
DECLARE_mInt64(file_cache_warmup_download_rate_limit_bytes_per_second);

DECLARE_mInt64(cache_read_from_peer_expired_seconds);

// Base compaction output: only write index files to file cache, not data files
DECLARE_mBool(enable_file_cache_write_base_compaction_index_only);

// Cumulative compaction output: only write index files to file cache, not data files
DECLARE_mBool(enable_file_cache_write_cumu_compaction_index_only);

#include "common/compile_check_end.h"
} // namespace doris::config
