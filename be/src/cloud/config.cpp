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

#include "cloud/config.h"

namespace doris::config {

DEFINE_String(cloud_unique_id, "");
DEFINE_String(meta_service_endpoint, "");
DEFINE_Bool(meta_service_use_load_balancer, "false");
DEFINE_mInt32(meta_service_rpc_timeout_ms, "10000");
DEFINE_Bool(meta_service_connection_pooled, "true");
DEFINE_mInt64(meta_service_connection_pool_size, "20");
DEFINE_mInt32(meta_service_connection_age_base_minutes, "5");
DEFINE_mInt32(meta_service_idle_connection_timeout_ms, "0");
DEFINE_mInt32(meta_service_rpc_retry_times, "200");
DEFINE_mInt32(meta_service_brpc_timeout_ms, "10000");

DEFINE_Int64(tablet_cache_capacity, "100000");
DEFINE_Int64(tablet_cache_shards, "16");
DEFINE_mInt64(tablet_sync_interval_seconds, "1800");

DEFINE_mInt64(min_compaction_failure_interval_ms, "5000");
DEFINE_mInt64(base_compaction_freeze_interval_s, "86400");
DEFINE_mInt64(cu_compaction_freeze_interval_s, "1200");
DEFINE_mInt64(cumu_compaction_interval_s, "1800");

} // namespace doris::config
