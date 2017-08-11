// Modifications copyright (C) 2017, Baidu.com, Inc.
// Copyright 2017 The Apache Software Foundation

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

#ifndef BDG_PALO_BE_SRC_COMMON_UTIL_PALO_METRICS_H
#define BDG_PALO_BE_SRC_COMMON_UTIL_PALO_METRICS_H

#include "util/metrics.h"

namespace palo {

// Global impalad-wide metrics.  This is useful for objects that want to update metrics
// without having to do frequent metrics lookups.
// These get created by impala-server from the Metrics ob<ject in ExecEnv right when the
// ImpaladServer starts up.
class PaloMetrics {
public:
    // Creates and initializes all metrics above in 'm'.
    static void create_metrics(MetricGroup* m);

    static StringProperty* palo_be_start_time() {
        return _s_palo_be_start_time;
    }
    static StringProperty* palo_be_version() {
        return _s_palo_be_version;
    }
    static BooleanProperty* palo_be_ready() {
        return _s_palo_be_ready;
    }
    static IntCounter* palo_be_num_fragments() {
        return _s_palo_be_num_fragments;
    }
    static IntCounter* num_ranges_ranges_processed() {
        return _s_num_ranges_processed;
    }
    static IntCounter* num_ranges_missing_volume_id() {
        return _s_num_ranges_missing_volume_id;
    }
    static IntGauge* mem_pool_total_bytes() {
        return _s_mem_pool_total_bytes;
    }
    static IntGauge* hash_table_total_bytes() {
        return _s_hash_table_total_bytes;
    }
    static IntCounter* olap_lru_cache_lookup_count() {
        return _s_olap_lru_cache_lookup_count;
    }
    static IntCounter* olap_lru_cache_hit_count() {
        return _s_olap_lru_cache_hit_count;
    }
    static IntCounter* palo_push_count() {
        return _s_palo_push_count;
    }
    static IntCounter* palo_fetch_count() {
        return _s_palo_fetch_count;
    }
    static IntCounter* palo_request_count() {
        return _s_palo_request_count;
    }
    static IntCounter* be_merge_delta_num() {
        return _s_be_merge_delta_num;
    }
    static IntCounter* be_merge_size() {
        return _s_be_merge_size;
    }
    static IntCounter* ce_merge_delta_num() {
        return _s_ce_merge_delta_num;
    }
    static IntCounter* ce_merge_size() {
        return _s_ce_merge_size;
    }

    // static IntGauge* io_mgr_bytes_read() {
    //     return _s_io_mgr_bytes_read;
    // }
    // static IntGauge* io_mgr_local_bytes_read() {
    //     return _s_io_mgr_local_bytes_read;
    // }
    // static IntGauge* io_mgr_cached_bytes_read() {
    //     return _s_io_mgr_cached_bytes_read;
    // }
    // static IntGauge* io_mgr_short_circuit_bytes_read() {
    //     return _s_io_mgr_short_circuit_bytes_read;
    // }
    static IntCounter* io_mgr_bytes_written() {
        return _s_io_mgr_bytes_written;
    }

    static IntGauge* io_mgr_num_buffers() {
        return _s_io_mgr_num_buffers;
    }
    static IntGauge* io_mgr_num_open_files() {
        return _s_io_mgr_num_open_files;
    }
    static IntGauge* io_mgr_num_unused_buffers() {
        return _s_io_mgr_num_unused_buffers;
    }
    static IntGauge* io_mgr_num_file_handles_outstanding() {
        return _s_io_mgr_num_file_handles_outstanding;
    }
    static IntGauge* io_mgr_total_bytes() {
        return _s_io_mgr_total_bytes;
    }

    static IntCounter* num_queries_spilled() {
        return _s_num_queries_spilled;
    }

private:
    static StringProperty* _s_palo_be_start_time;
    static StringProperty* _s_palo_be_version;
    static BooleanProperty* _s_palo_be_ready;
    static IntCounter* _s_palo_be_num_fragments;
    static IntCounter* _s_num_ranges_processed;
    static IntCounter* _s_num_ranges_missing_volume_id;
    static IntGauge* _s_mem_pool_total_bytes;
    static IntGauge* _s_hash_table_total_bytes;
    static IntCounter* _s_olap_lru_cache_lookup_count;
    static IntCounter* _s_olap_lru_cache_hit_count;
    static IntCounter* _s_palo_push_count;
    static IntCounter* _s_palo_fetch_count;
    static IntCounter* _s_palo_request_count;
    static IntCounter* _s_be_merge_delta_num;
    static IntCounter* _s_be_merge_size;
    static IntCounter* _s_ce_merge_delta_num;
    static IntCounter* _s_ce_merge_size;

    // static IntGauge* _s_io_mgr_bytes_read;
    // static IntGauge* _s_io_mgr_local_bytes_read;
    // static IntGauge* _s_io_mgr_cached_bytes_read;
    // static IntGauge* _s_io_mgr_short_circuit_bytes_read;
    static IntCounter* _s_io_mgr_bytes_written;

    static IntGauge* _s_io_mgr_num_buffers;
    static IntGauge* _s_io_mgr_num_open_files;
    static IntGauge* _s_io_mgr_num_unused_buffers;
    // static IntGauge* _s_io_mgr_num_cached_file_handles;
    static IntGauge* _s_io_mgr_num_file_handles_outstanding;
    // static IntGauge* _s_io_mgr_cached_file_handles_hit_count;
    // static IntGauge* _s_io_mgr_cached_file_handles_miss_count;
    static IntGauge* _s_io_mgr_total_bytes;

    static IntCounter* _s_num_queries_spilled;

};

};

#endif
