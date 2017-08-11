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

#include "util/palo_metrics.h"

#include "util/debug_util.h"

namespace palo {

// Naming convention: Components should be separated by '.' and words should
// be separated by '-'.
const char* PALO_BE_START_TIME = "palo_be.start_time";
const char* PALO_BE_VERSION = "palo_be.version";
const char* PALO_BE_READY = "palo_be.ready";
const char* PALO_BE_NUM_FRAGMENTS = "palo_be.num_fragments";
const char* TOTAL_SCAN_RANGES_PROCESSED = "palo_be.scan_ranges.total";
const char* NUM_SCAN_RANGES_MISSING_VOLUME_ID = "palo_be.scan_ranges.num_missing_volume_id";
const char* MEM_POOL_TOTAL_BYTES = "palo_be.mem_pool.total_bytes";
const char* HASH_TABLE_TOTAL_BYTES = "palo_be.hash_table.total_bytes";
const char* OLAP_LRU_CACHE_LOOKUP_COUNT = "palo_be.olap.lru_cache.lookup_count";
const char* OLAP_LRU_CACHE_HIT_COUNT = "palo_be.olap.lru_cache.hit_count";
const char* PALO_PUSH_COUNT = "palo_be.olap.push_count";
const char* PALO_FETCH_COUNT = "palo_be.olap.fetch_count";
const char* PALO_REQUEST_COUNT = "palo_be.olap.request_count";
const char* BE_MERGE_DELTA_NUM = "palo_be.olap.be_merge.delta_num";
const char* BE_MERGE_SIZE = "palo_be.olap.be_merge_size";
const char* CE_MERGE_DELTA_NUM = "palo_be.olap.ce_merge.delta_num";
const char* CE_MERGE_SIZE = "palo_be.olap.ce_merge_size";

const char* IO_MGR_NUM_BUFFERS = "palo_be.io_mgr.num_buffers";
const char* IO_MGR_NUM_OPEN_FILES = "palo_be.io_mgr.num_open_files";
const char* IO_MGR_NUM_UNUSED_BUFFERS = "palo_be.io_mgr.num_unused_buffers";
// const char* IO_MGR_NUM_CACHED_FILE_HANDLES = "palo_be.io_mgr_num_cached_file_handles";
const char* IO_MGR_NUM_FILE_HANDLES_OUTSTANDING = "palo_be.io_mgr.num_file_handles_outstanding";
// const char* IO_MGR_CACHED_FILE_HANDLES_HIT_COUNT = "palo_be.io_mgr_cached_file_handles_hit_count";
// const char* IO_MGR_CACHED_FILE_HANDLES_MISS_COUNT = "palo_be.io_mgr_cached_file_handles_miss_count";
const char* IO_MGR_TOTAL_BYTES = "palo_be.io_mgr.total_bytes";

// const char* IO_MGR_BYTES_READ = "palo_be.io_mgr_bytes_read";
// const char* IO_MGR_LOCAL_BYTES_READ = "palo_be.io_mgr_local_bytes_read";
// const char* IO_MGR_CACHED_BYTES_READ = "palo_be.io_mgr_cached_bytes_read";
// const char* IO_MGR_SHORT_CIRCUIT_BYTES_READ = "palo_be.io_mgr_short_circuit_bytes_read";
const char* IO_MGR_BYTES_WRITTEN = "palo_be.io_mgr.bytes_written";

const char* NUM_QUERIES_SPILLED = "palo_be.num_queries_spilled";


// These are created by palo_be during startup.
StringProperty* PaloMetrics::_s_palo_be_start_time = NULL;
StringProperty* PaloMetrics::_s_palo_be_version = NULL;
BooleanProperty* PaloMetrics::_s_palo_be_ready = NULL;
IntCounter* PaloMetrics::_s_palo_be_num_fragments = NULL;
IntCounter* PaloMetrics::_s_num_ranges_processed = NULL;
IntCounter* PaloMetrics::_s_num_ranges_missing_volume_id = NULL;
IntGauge* PaloMetrics::_s_mem_pool_total_bytes = NULL;
IntGauge* PaloMetrics::_s_hash_table_total_bytes = NULL;
IntCounter* PaloMetrics::_s_olap_lru_cache_lookup_count = NULL;
IntCounter* PaloMetrics::_s_olap_lru_cache_hit_count = NULL;
IntCounter* PaloMetrics::_s_palo_push_count = NULL;
IntCounter* PaloMetrics::_s_palo_fetch_count = NULL;
IntCounter* PaloMetrics::_s_palo_request_count = NULL;
IntCounter* PaloMetrics::_s_be_merge_delta_num = NULL;
IntCounter* PaloMetrics::_s_be_merge_size = NULL;
IntCounter* PaloMetrics::_s_ce_merge_delta_num = NULL;
IntCounter* PaloMetrics::_s_ce_merge_size = NULL;

IntGauge* PaloMetrics::_s_io_mgr_num_buffers = NULL;
IntGauge* PaloMetrics::_s_io_mgr_num_open_files = NULL;
IntGauge* PaloMetrics::_s_io_mgr_num_unused_buffers = NULL;
// IntGauge* PaloMetrics::_s_io_mgr_num_cached_file_handles = NULL;
IntGauge* PaloMetrics::_s_io_mgr_num_file_handles_outstanding = NULL;
// IntGauge* PaloMetrics::_s_io_mgr_cached_file_handles_hit_count = NULL;
// IntGauge* PaloMetrics::_s_io_mgr_cached_file_handles_miss_count = NULL;
IntGauge* PaloMetrics::_s_io_mgr_total_bytes = NULL;

// IntGauge* PaloMetrics::_s_io_mgr_bytes_read = NULL;
// IntGauge* PaloMetrics::_s_io_mgr_local_bytes_read = NULL;
// IntGauge* PaloMetrics::_s_io_mgr_cached_bytes_read = NULL;
// IntGauge* PaloMetrics::_s_io_mgr_short_circuit_bytes_read = NULL;
IntCounter* PaloMetrics::_s_io_mgr_bytes_written = NULL;

IntCounter* PaloMetrics::_s_num_queries_spilled = NULL;

void PaloMetrics::create_metrics(MetricGroup* m) {
    // Initialize impalad metrics
    _s_palo_be_start_time = m->AddProperty<std::string>(
                                   PALO_BE_START_TIME, "");
    _s_palo_be_version = m->AddProperty<std::string>(
                                PALO_BE_VERSION, get_version_string(true));
    _s_palo_be_ready = m->AddProperty(PALO_BE_READY, false);
    _s_palo_be_num_fragments = m->AddCounter(PALO_BE_NUM_FRAGMENTS, 0L);

    // Initialize scan node metrics
    _s_num_ranges_processed = m->AddCounter(TOTAL_SCAN_RANGES_PROCESSED, 0L);
    _s_num_ranges_missing_volume_id = m->AddCounter(NUM_SCAN_RANGES_MISSING_VOLUME_ID, 0L);

    // Initialize memory usage metrics
    _s_mem_pool_total_bytes = m->AddGauge(MEM_POOL_TOTAL_BYTES, 0L);
    _s_hash_table_total_bytes = m->AddGauge(HASH_TABLE_TOTAL_BYTES, 0L);

    // Initialize olap metrics
    _s_olap_lru_cache_lookup_count = m->AddCounter(OLAP_LRU_CACHE_LOOKUP_COUNT, 0L);
    _s_olap_lru_cache_hit_count = m->AddCounter(OLAP_LRU_CACHE_HIT_COUNT, 0L);

    // Initialize push_count, fetch_count, request_count metrics
    _s_palo_push_count = m->AddCounter(PALO_PUSH_COUNT, 0L);
    _s_palo_fetch_count = m->AddCounter(PALO_FETCH_COUNT, 0L);
    _s_palo_request_count = m->AddCounter(PALO_REQUEST_COUNT, 0L);

    // Initialize be/ce merge metrics
    _s_be_merge_delta_num = m->AddCounter(BE_MERGE_DELTA_NUM, 0L);
    _s_be_merge_size = m->AddCounter(BE_MERGE_SIZE, 0L);
    _s_ce_merge_delta_num = m->AddCounter(CE_MERGE_DELTA_NUM, 0L);
    _s_ce_merge_size = m->AddCounter(CE_MERGE_SIZE, 0L);

    // Initialize metrics relate to spilling to disk
    // _s_io_mgr_bytes_read
    //         = m->AddGauge(IO_MGR_BYTES_READ, 0L);
    // _s_io_mgr_local_bytes_read
    //         = m->AddGauge(IO_MGR_LOCAL_BYTES_READ, 0L);
    // _s_io_mgr_cached_bytes_read
    //         = m->AddGauge(IO_MGR_CACHED_BYTES_READ, 0L);
    // _s_io_mgr_short_circuit_bytes_read
    //         = m->AddGauge(IO_MGR_SHORT_CIRCUIT_BYTES_READ, 0L);
    _s_io_mgr_bytes_written = m->AddCounter(IO_MGR_BYTES_WRITTEN, 0L);

    _s_io_mgr_num_buffers
            = m->AddGauge(IO_MGR_NUM_BUFFERS, 0L);
    _s_io_mgr_num_open_files
            = m->AddGauge(IO_MGR_NUM_OPEN_FILES, 0L);
    _s_io_mgr_num_unused_buffers
            = m->AddGauge(IO_MGR_NUM_UNUSED_BUFFERS, 0L);
    _s_io_mgr_num_file_handles_outstanding
            = m->AddGauge(IO_MGR_NUM_FILE_HANDLES_OUTSTANDING, 0L);
    _s_io_mgr_total_bytes
            = m->AddGauge(IO_MGR_TOTAL_BYTES, 0L);

    _s_num_queries_spilled = m->AddCounter(NUM_QUERIES_SPILLED, 0L);
}

}
