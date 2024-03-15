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

DECLARE_String(cloud_unique_id);

static inline bool is_cloud_mode() {
    return !cloud_unique_id.empty();
}

DECLARE_String(meta_service_endpoint);
// Set the underlying connection type to pooled.
DECLARE_Bool(meta_service_connection_pooled);
DECLARE_mInt64(meta_service_connection_pool_size);
// A connection will expire after a random time during [base, 2*base], so that the BE
// has a chance to connect to a new RS. Set zero to disable it.
DECLARE_mInt32(meta_service_connection_age_base_minutes);
// Rebuild the idle connections after the timeout exceeds. Set zero to disable it.
DECLARE_mInt32(meta_service_idle_connection_timeout_ms);
DECLARE_mInt32(meta_service_rpc_timeout_ms);
DECLARE_mInt32(meta_service_rpc_retry_times);
// default brpc timeout
DECLARE_mInt32(meta_service_brpc_timeout_ms);

// CloudTabletMgr config
DECLARE_Int64(tablet_cache_capacity);
DECLARE_Int64(tablet_cache_shards);
DECLARE_mInt32(tablet_sync_interval_s);

// Cloud compaction config
DECLARE_mInt64(min_compaction_failure_interval_ms);
// For cloud read/write separate mode
DECLARE_mInt64(base_compaction_freeze_interval_s);
DECLARE_mInt64(cu_compaction_freeze_interval_s);
DECLARE_mInt64(cumu_compaction_interval_s);

DECLARE_mInt32(compaction_timeout_seconds);
DECLARE_mInt32(lease_compaction_interval_seconds);
DECLARE_mInt64(base_compaction_interval_seconds_since_last_operation);
DECLARE_mBool(enable_parallel_cumu_compaction);
DECLARE_mDouble(base_compaction_thread_num_factor);
DECLARE_mDouble(cumu_compaction_thread_num_factor);
DECLARE_mInt32(check_auto_compaction_interval_seconds);
DECLARE_mInt32(max_base_compaction_task_num_per_disk);
DECLARE_mBool(prioritize_query_perf_in_compaction);

// CloudStorageEngine config
DECLARE_mInt32(refresh_s3_info_interval_s);
DECLARE_mInt32(vacuum_stale_rowsets_interval_s);
DECLARE_mInt32(schedule_sync_tablets_interval_s);

} // namespace doris::config
