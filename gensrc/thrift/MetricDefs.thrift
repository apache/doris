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

namespace cpp doris
namespace java org.apache.doris.thrift

include "Metrics.thrift"

// All metadata associated with a metric. Used to instanciate metrics.
struct TMetricDef {
  1: optional string key
  2: optional Metrics.TMetricKind kind
  3: optional Metrics.TUnit units
  4: optional list<string> contexts
  5: optional string label
  6: optional string description
}

const map<string,TMetricDef> TMetricDefs =
{
  "admission_controller.agg_mem_reserved.$0": {
    "contexts": [
      "RESOURCE_POOL"
    ], 
    "description": "Resource Pool $0 Aggregate Mem Reserved", 
    "key": "admission_controller.agg_mem_reserved.$0", 
    "kind": Metrics.TMetricKind.GAUGE, 
    "label": "Resource Pool $0 Aggregate Mem Reserved", 
    "units": Metrics.TUnit.NONE
  }, 
  "admission_controller.agg_num_queued.$0": {
    "contexts": [
      "RESOURCE_POOL"
    ], 
    "description": "Resource Pool $0 Aggregate Queue Size", 
    "key": "admission_controller.agg_num_queued.$0", 
    "kind": Metrics.TMetricKind.GAUGE, 
    "label": "Resource Pool $0 Aggregate Queue Size", 
    "units": Metrics.TUnit.NONE
  }, 
  "admission_controller.agg_num_running.$0": {
    "contexts": [
      "RESOURCE_POOL"
    ], 
    "description": "Resource Pool $0 Aggregate Num Running", 
    "key": "admission_controller.agg_num_running.$0", 
    "kind": Metrics.TMetricKind.GAUGE, 
    "label": "Resource Pool $0 Aggregate Num Running", 
    "units": Metrics.TUnit.NONE
  }, 
  "admission_controller.local_backend_mem_reserved.$0": {
    "contexts": [
      "RESOURCE_POOL"
    ], 
    "description": "Resource Pool $0 Mem Reserved by the backend coordinator", 
    "key": "admission_controller.local_backend_mem_reserved.$0", 
    "kind": Metrics.TMetricKind.GAUGE, 
    "label": "Resource Pool $0 Coordinator Backend Mem Reserved", 
    "units": Metrics.TUnit.NONE
  }, 
  "admission_controller.local_backend_mem_usage.$0": {
    "contexts": [
      "RESOURCE_POOL"
    ], 
    "description": "Resource Pool $0 Coordinator Backend Mem Usage", 
    "key": "admission_controller.local_backend_mem_usage.$0", 
    "kind": Metrics.TMetricKind.GAUGE, 
    "label": "Resource Pool $0 Coordinator Backend Mem Usage", 
    "units": Metrics.TUnit.NONE
  }, 
  "admission_controller.local_mem_admitted.$0": {
    "contexts": [
      "RESOURCE_POOL"
    ], 
    "description": "Resource Pool $0 Local Mem Admitted", 
    "key": "admission_controller.local_mem_admitted.$0", 
    "kind": Metrics.TMetricKind.GAUGE, 
    "label": "Resource Pool $0 Local Mem Admitted", 
    "units": Metrics.TUnit.NONE
  }, 
  "admission_controller.local_num_admitted_running.$0": {
    "contexts": [
      "RESOURCE_POOL"
    ], 
    "description": "Resource Pool $0 Coordinator Backend Num Running", 
    "key": "admission_controller.local_num_admitted_running.$0", 
    "kind": Metrics.TMetricKind.GAUGE, 
    "label": "Resource Pool $0 Coordinator Backend Num Running", 
    "units": Metrics.TUnit.NONE
  }, 
  "admission_controller.local_num_queued.$0": {
    "contexts": [
      "RESOURCE_POOL"
    ], 
    "description": "Resource Pool $0 Queue Size on the coordinator", 
    "key": "admission_controller.local_num_queued.$0", 
    "kind": Metrics.TMetricKind.GAUGE, 
    "label": "Resource Pool $0 Coordinator Backend Queue Size", 
    "units": Metrics.TUnit.NONE
  }, 
  "admission_controller.pool_max_mem_resources.$0": {
    "contexts": [
      "RESOURCE_POOL"
    ], 
    "description": "Resource Pool $0 Configured Max Mem Resources", 
    "key": "admission_controller.pool_max_mem_resources.$0", 
    "kind": Metrics.TMetricKind.GAUGE, 
    "label": "Resource Pool $0 Configured Max Mem Resources", 
    "units": Metrics.TUnit.NONE
  }, 
  "admission_controller.pool_max_queued.$0": {
    "contexts": [
      "RESOURCE_POOL"
    ], 
    "description": "Resource Pool $0 Configured Max Queued", 
    "key": "admission_controller.pool_max_queued.$0", 
    "kind": Metrics.TMetricKind.GAUGE, 
    "label": "Resource Pool $0 Configured Max Queued", 
    "units": Metrics.TUnit.NONE
  }, 
  "admission_controller.pool_max_requests.$0": {
    "contexts": [
      "RESOURCE_POOL"
    ], 
    "description": "Resource Pool $0 Configured Max Requests", 
    "key": "admission_controller.pool_max_requests.$0", 
    "kind": Metrics.TMetricKind.GAUGE, 
    "label": "Resource Pool $0 Configured Max Requests", 
    "units": Metrics.TUnit.NONE
  }, 
  "admission_controller.time_in_queue_ms.$0": {
    "contexts": [
      "RESOURCE_POOL"
    ], 
    "description": "Resource Pool $0 Time in Queue", 
    "key": "admission_controller.time_in_queue_ms.$0", 
    "kind": Metrics.TMetricKind.COUNTER, 
    "label": "Resource Pool $0 Time in Queue", 
    "units": Metrics.TUnit.TIME_MS
  }, 
  "admission_controller.total_admitted.$0": {
    "contexts": [
      "RESOURCE_POOL"
    ], 
    "description": "Total number of requests admitted to pool $0", 
    "key": "admission_controller.total_admitted.$0", 
    "kind": Metrics.TMetricKind.COUNTER, 
    "label": "Resource Pool $0 Total Admitted", 
    "units": Metrics.TUnit.UNIT
  }, 
  "admission_controller.total_dequeued.$0": {
    "contexts": [
      "RESOURCE_POOL"
    ], 
    "description": "Total number of requests dequeued in pool $0", 
    "key": "admission_controller.total_dequeued.$0", 
    "kind": Metrics.TMetricKind.COUNTER, 
    "label": "Resource Pool $0 Total Dequeued", 
    "units": Metrics.TUnit.UNIT
  }, 
  "admission_controller.total_queued.$0": {
    "contexts": [
      "RESOURCE_POOL"
    ], 
    "description": "Total number of requests queued in pool $0", 
    "key": "admission_controller.total_queued.$0", 
    "kind": Metrics.TMetricKind.COUNTER, 
    "label": "Resource Pool $0 Total Queued", 
    "units": Metrics.TUnit.UNIT
  }, 
  "admission_controller.total_rejected.$0": {
    "contexts": [
      "RESOURCE_POOL"
    ], 
    "description": "Total number of requests rejected in pool $0", 
    "key": "admission_controller.total_rejected.$0", 
    "kind": Metrics.TMetricKind.COUNTER, 
    "label": "Resource Pool $0 Total Rejected", 
    "units": Metrics.TUnit.UNIT
  }, 
  "admission_controller.total_released.$0": {
    "contexts": [
      "RESOURCE_POOL"
    ], 
    "description": "Total number of requests that have completed and released resources in pool $0", 
    "key": "admission_controller.total_released.$0", 
    "kind": Metrics.TMetricKind.COUNTER, 
    "label": "Resource Pool $0 Total Released", 
    "units": Metrics.TUnit.UNIT
  }, 
  "admission_controller.total_timed_out.$0": {
    "contexts": [
      "RESOURCE_POOL"
    ], 
    "description": "Total number of requests timed out waiting while queued in pool $0", 
    "key": "admission_controller.total_timed_out.$0", 
    "kind": Metrics.TMetricKind.COUNTER, 
    "label": "Resource Pool $0 Total Timed Out", 
    "units": Metrics.TUnit.UNIT
  }, 
  "buffer_pool.limit": {
    "contexts": [
      "PALO BE"
    ], 
    "description": "Maximum allowed bytes allocated by the buffer pool.", 
    "key": "buffer_pool.limit", 
    "kind": Metrics.TMetricKind.GAUGE, 
    "label": "Buffer Pool Allocated Memory Limit.", 
    "units": Metrics.TUnit.BYTES
  }, 
  "buffer_pool.reserved": {
    "contexts": [
      "PALO BE"
    ], 
    "description": "Total memory currently reserved for buffers.", 
    "key": "buffer_pool.reserved", 
    "kind": Metrics.TMetricKind.GAUGE, 
    "label": "Buffer Pool Total Reserved Memory.", 
    "units": Metrics.TUnit.BYTES
  }, 
  "buffer_pool.system_allocated": {
    "contexts": [
      "PALO BE"
    ], 
    "description": "Total buffer memory currently allocated by the buffer pool.", 
    "key": "buffer_pool.system_allocated", 
    "kind": Metrics.TMetricKind.GAUGE, 
    "label": "Buffer Pool Total Allocated Memory.", 
    "units": Metrics.TUnit.BYTES
  }, 
  "catalog_server.topic_processing_time_s": {
    "contexts": [
      "CATALOGSERVER"
    ], 
    "description": "Catalog Server Topic Processing Time", 
    "key": "catalog_server.topic_processing_time_s", 
    "kind": Metrics.TMetricKind.STATS, 
    "label": "Catalog Server Topic Processing Time", 
    "units": Metrics.TUnit.TIME_S
  }, 
  "catalog.num_databases": {
    "contexts": [
      "PALO BE"
    ], 
    "description": "The number of databases in the catalog.", 
    "key": "catalog.num_databases", 
    "kind": Metrics.TMetricKind.GAUGE, 
    "label": "Databases", 
    "units": Metrics.TUnit.NONE
  }, 
  "catalog.num_tables": {
    "contexts": [
      "PALO BE"
    ], 
    "description": "The number of tables in the catalog.", 
    "key": "catalog.num_tables", 
    "kind": Metrics.TMetricKind.GAUGE, 
    "label": "Tables", 
    "units": Metrics.TUnit.NONE
  }, 
  "catalog.ready": {
    "contexts": [
      "PALO BE"
    ], 
    "description": "Indicates if the catalog is ready.", 
    "key": "catalog.ready", 
    "kind": Metrics.TMetricKind.PROPERTY, 
    "label": "Catalog Ready", 
    "units": Metrics.TUnit.NONE
  }, 
  "catalog.server.client_cache.clients_in_use": {
    "contexts": [
      "PALO BE"
    ], 
    "description": "The number of clients currently in use by the Catalog Server client cache.", 
    "key": "catalog.server.client_cache.clients_in_use", 
    "kind": Metrics.TMetricKind.GAUGE, 
    "label": "Catalog Server Client Cache Clients In Use", 
    "units": Metrics.TUnit.NONE
  }, 
  "catalog.server.client_cache.total_clients": {
    "contexts": [
      "PALO BE"
    ], 
    "description": "The total number of clients in the Catalog Server client cache.", 
    "key": "catalog.server.client_cache.total_clients", 
    "kind": Metrics.TMetricKind.GAUGE, 
    "label": "Catalog Server Client Cache Total Clients", 
    "units": Metrics.TUnit.NONE
  }, 
  "catalog.version": {
    "contexts": [
      "CATALOGSERVER"
    ], 
    "description": "The full version string of the Catalog Server.", 
    "key": "catalog.version", 
    "kind": Metrics.TMetricKind.PROPERTY, 
    "label": "Catalog Version", 
    "units": Metrics.TUnit.NONE
  }, 
  "cgroups_mgr.active_cgroups": {
    "contexts": [
      "PALO BE"
    ], 
    "description": "The number of cgroups currently registered with the Cgroups Manager", 
    "key": "cgroups_mgr.active_cgroups", 
    "kind": Metrics.TMetricKind.GAUGE, 
    "label": "Cgroups Manager Active Cgroups", 
    "units": Metrics.TUnit.UNIT
  }, 
  "external_data_source.class_cache.hits": {
    "contexts": [
      "PALO BE"
    ], 
    "description": "Number of cache hits in the External Data Source Class Cache", 
    "key": "external_data_source.class_cache.hits", 
    "kind": Metrics.TMetricKind.COUNTER, 
    "label": "External Data Source Class Cache Hits", 
    "units": Metrics.TUnit.UNIT
  }, 
  "external_data_source.class_cache.misses": {
    "contexts": [
      "PALO BE"
    ], 
    "description": "Number of cache misses in the External Data Source Class Cache", 
    "key": "external_data_source.class_cache.misses", 
    "kind": Metrics.TMetricKind.COUNTER, 
    "label": "External Data Source Class Cache Misses", 
    "units": Metrics.TUnit.UNIT
  }, 
  "palo.backends.client_cache.clients_in_use": {
    "contexts": [
      "PALO_BE"
    ], 
    "description": "The number of active Palo Backend clients. These clients are for communication with other Palo Be.", 
    "key": "palo.backends.client_cache.clients_in_use", 
    "kind": Metrics.TMetricKind.GAUGE, 
    "label": "Palo Backend Active Clients", 
    "units": Metrics.TUnit.NONE
  }, 
  "palo.backends.client_cache.total_clients": {
    "contexts": [
      "PALO_BE"
    ], 
    "description": "The total number of Palo Backend clients in this Palo Be's client cache. These clients are for communication with other Palo Be.", 
    "key": "palo.backends.client_cache.total_clients", 
    "kind": Metrics.TMetricKind.GAUGE, 
    "label": "Palo Backend Total Clients", 
    "units": Metrics.TUnit.NONE
  }, 
  "palo_be.ddl_durations_ms": {
    "contexts": [
      "PALO BE"
    ], 
    "description": "Distribution of DDL operation latencies", 
    "key": "palo_be.ddl_durations_ms", 
    "kind": Metrics.TMetricKind.HISTOGRAM, 
    "label": "DDL latency distribution", 
    "units": Metrics.TUnit.TIME_MS
  }, 
  "palo_be.hash_table.total_bytes": {
    "contexts": [
      "PALO BE"
    ], 
    "description": "The current size of all allocated hash tables.", 
    "key": "palo_be.hash_table.total_bytes", 
    "kind": Metrics.TMetricKind.GAUGE, 
    "label": "Hash Tables Size", 
    "units": Metrics.TUnit.BYTES
  }, 
  "palo_be.io_mgr.bytes_read": {
    "contexts": [
      "PALO BE"
    ], 
    "description": "The total number of bytes read by the IO manager.", 
    "key": "palo_be.io_mgr.bytes_read", 
    "kind": Metrics.TMetricKind.COUNTER, 
    "label": "Palo Be Io Mgr Bytes Read", 
    "units": Metrics.TUnit.BYTES
  }, 
  "palo_be.io_mgr.bytes_written": {
    "contexts": [
      "PALO BE"
    ], 
    "description": "Total number of bytes written to disk by the IO manager.", 
    "key": "palo_be.io_mgr.bytes_written", 
    "kind": Metrics.TMetricKind.COUNTER, 
    "label": "Palo Be Io Mgr Bytes Written", 
    "units": Metrics.TUnit.BYTES
  }, 
  "palo_be.io_mgr.cached_bytes_read": {
    "contexts": [
      "PALO BE"
    ], 
    "description": "Total number of cached bytes read by the IO manager.", 
    "key": "palo_be.io_mgr.cached_bytes_read", 
    "kind": Metrics.TMetricKind.COUNTER, 
    "label": "Palo Be Io Mgr Cached Bytes Read", 
    "units": Metrics.TUnit.BYTES
  }, 
  "palo_be.io_mgr.local_bytes_read": {
    "contexts": [
      "PALO BE"
    ], 
    "description": "Total number of local bytes read by the IO manager.", 
    "key": "palo_be.io_mgr.local_bytes_read", 
    "kind": Metrics.TMetricKind.COUNTER, 
    "label": "Palo Be Io Mgr Local Bytes Read", 
    "units": Metrics.TUnit.BYTES
  }, 
  "palo_be.io_mgr.num_buffers": {
    "contexts": [
      "PALO BE"
    ], 
    "description": "The number of allocated IO buffers. IO buffers are shared by all queries.", 
    "key": "palo_be.io_mgr.num_buffers", 
    "kind": Metrics.TMetricKind.GAUGE, 
    "label": "IO Buffers", 
    "units": Metrics.TUnit.NONE
  }, 
  "palo_be.io_mgr.num_open_files": {
    "contexts": [
      "PALO BE"
    ], 
    "description": "The current number of files opened by the IO Manager", 
    "key": "palo_be.io_mgr.num_open_files", 
    "kind": Metrics.TMetricKind.GAUGE, 
    "label": "Open Files", 
    "units": Metrics.TUnit.NONE
  }, 
  "palo_be.io_mgr.num_unused_buffers": {
    "contexts": [
      "PALO BE"
    ], 
    "description": "The number of unused IO buffers. IO buffers are shared by all queries.", 
    "key": "palo_be.io_mgr.num_unused_buffers", 
    "kind": Metrics.TMetricKind.GAUGE, 
    "label": "Unused IO Buffers", 
    "units": Metrics.TUnit.NONE
  }, 
  "palo_be.io_mgr.short_circuit_bytes_read": {
    "contexts": [
      "PALO BE"
    ], 
    "description": "Total number of short_circuit bytes read by the IO manager.", 
    "key": "palo_be.io_mgr.short_circuit_bytes_read", 
    "kind": Metrics.TMetricKind.COUNTER, 
    "label": "Palo Be Io Mgr Short Circuit Bytes Read", 
    "units": Metrics.TUnit.BYTES
  }, 
  "palo_be.io_mgr.total_bytes": {
    "contexts": [
      "PALO BE"
    ], 
    "description": "Number of bytes used by IO buffers (used and unused).", 
    "key": "palo_be.io_mgr.total_bytes", 
    "kind": Metrics.TMetricKind.GAUGE, 
    "label": "IO Buffers Total Size", 
    "units": Metrics.TUnit.BYTES
  }, 
  "palo_be.io_mgr.cached_file_handles_hit_count": {
    "contexts": [
      "PALO BE"
    ], 
    "description": "Number of cache hits for cached HDFS file handles", 
    "key": "palo_be.io_mgr.cached_file_handles_hit_count", 
    "kind": Metrics.TMetricKind.GAUGE, 
    "label": "HDFS cached file handles hit count", 
    "units": Metrics.TUnit.NONE
  }, 
  "palo_be.io_mgr.cached_file_handles_hit_ratio": {
    "contexts": [
      "PALO BE"
    ], 
    "description": "HDFS file handle cache hit ratio, between 0 and 1, where 1 means all reads were served from cached file handles.", 
    "key": "palo_be.io_mgr.cached_file_handles_hit_ratio", 
    "kind": Metrics.TMetricKind.STATS, 
    "label": "HDFS file handle cache hit ratio", 
    "units": Metrics.TUnit.NONE
  }, 
  "palo_be.io_mgr.cached_file_handles_miss_count": {
    "contexts": [
      "PALO BE"
    ], 
    "description": "Number of cache misses for cached HDFS file handles", 
    "key": "palo_be.io_mgr.cached_file_handles_miss_count", 
    "kind": Metrics.TMetricKind.GAUGE, 
    "label": "HDFS cached file handles miss count", 
    "units": Metrics.TUnit.NONE
  }, 
  "palo_be.io_mgr.num_cached_file_handles": {
    "contexts": [
      "PALO BE"
    ], 
    "description": "Number of currently cached HDFS file handles in the IO manager.", 
    "key": "palo_be.io_mgr.num_cached_file_handles", 
    "kind": Metrics.TMetricKind.GAUGE, 
    "label": "Number of cached HDFS file handles", 
    "units": Metrics.TUnit.NONE
  }, 
  "palo_be.io_mgr.num_file_handles_outstanding": {
    "contexts": [
      "PALO BE"
    ], 
    "description": "Number of HDFS file handles that are currently in use by readers.", 
    "key": "palo_be.io_mgr.num_file_handles_outstanding", 
    "kind": Metrics.TMetricKind.GAUGE, 
    "label": "Number of outstanding HDFS file handles", 
    "units": Metrics.TUnit.NONE
  }, 
  "palo_be.mem_pool.total_bytes": {
    "contexts": [
      "PALO BE"
    ], 
    "description": "The current size of the memory pool shared by all queries", 
    "key": "palo_be.mem_pool.total_bytes", 
    "kind": Metrics.TMetricKind.GAUGE, 
    "label": "Memory Pool Size", 
    "units": Metrics.TUnit.BYTES
  }, 
  "palo_be.num_files_open_for_insert": {
    "contexts": [
      "PALO BE"
    ], 
    "description": "The number of HDFS files currently open for writing.", 
    "key": "palo_be.num_files_open_for_insert", 
    "kind": Metrics.TMetricKind.GAUGE, 
    "label": "Files Open For Insert", 
    "units": Metrics.TUnit.NONE
  }, 
  "palo_be.num_fragments": {
    "contexts": [
      "PALO BE"
    ], 
    "description": "The total number of query fragments processed over the life of the process.", 
    "key": "palo_be.num_fragments", 
    "kind": Metrics.TMetricKind.COUNTER, 
    "label": "Query Fragments", 
    "units": Metrics.TUnit.UNIT
  }, 
  "palo_be.num_fragments_in_flight": {
    "contexts": [
      "PALO BE"
    ], 
    "description": "The number of query fragments currently executing.", 
    "key": "palo_be.num_fragments_in_flight", 
    "kind": Metrics.TMetricKind.GAUGE, 
    "label": "Query Fragments", 
    "units": Metrics.TUnit.UNIT
  }, 
  "palo_be.num_open_beeswax_sessions": {
    "contexts": [
      "PALO BE"
    ], 
    "description": "The number of open Beeswax sessions.", 
    "key": "palo_be.num_open_beeswax_sessions", 
    "kind": Metrics.TMetricKind.GAUGE, 
    "label": "Beeswax Sessions", 
    "units": Metrics.TUnit.NONE
  }, 
  "palo_be.num_open_hiveserver2_sessions": {
    "contexts": [
      "PALO BE"
    ], 
    "description": "The number of open HiveServer2 sessions.", 
    "key": "palo_be.num_open_hiveserver2_sessions", 
    "kind": Metrics.TMetricKind.GAUGE, 
    "label": "HiveServer2 Sessions", 
    "units": Metrics.TUnit.NONE
  }, 
  "palo_be.num_queries": {
    "contexts": [
      "PALO BE"
    ], 
    "description": "The total number of queries processed over the life of the process", 
    "key": "palo_be.num_queries", 
    "kind": Metrics.TMetricKind.COUNTER, 
    "label": "Queries", 
    "units": Metrics.TUnit.UNIT
  }, 
  "palo_be.num_queries_expired": {
    "contexts": [
      "PALO BE"
    ], 
    "description": "Number of queries expired due to inactivity.", 
    "key": "palo_be.num_queries_expired", 
    "kind": Metrics.TMetricKind.COUNTER, 
    "label": "Queries Expired", 
    "units": Metrics.TUnit.UNIT
  }, 
  "palo_be.num_queries_spilled": {
    "contexts": [
      "PALO BE"
    ], 
    "description": "Number of queries for which any operator spilled.", 
    "key": "palo_be.num_queries_spilled", 
    "kind": Metrics.TMetricKind.COUNTER, 
    "label": "Palo Be Num Queries Spilled", 
    "units": Metrics.TUnit.UNIT
  }, 
  "palo_be.num_sessions_expired": {
    "contexts": [
      "PALO BE"
    ], 
    "description": "Number of sessions expired due to inactivity.", 
    "key": "palo_be.num_sessions_expired", 
    "kind": Metrics.TMetricKind.COUNTER, 
    "label": "Sessions Expired", 
    "units": Metrics.TUnit.UNIT
  }, 
  "palo_be.query_durations_ms": {
    "contexts": [
      "PALO BE"
    ], 
    "description": "Distribution of query latencies", 
    "key": "palo_be.query_durations_ms", 
    "kind": Metrics.TMetricKind.HISTOGRAM, 
    "label": "Query latency distribution", 
    "units": Metrics.TUnit.TIME_MS
  }, 
  "palo_be.ready": {
    "contexts": [
      "PALO BE"
    ], 
    "description": "Indicates if the Palo Be is ready.", 
    "key": "palo_be.ready", 
    "kind": Metrics.TMetricKind.PROPERTY, 
    "label": "Palo Be Ready", 
    "units": Metrics.TUnit.NONE
  }, 
  "palo_be.resultset_cache.total_bytes": {
    "contexts": [
      "PALO BE"
    ], 
    "description": "Total number of bytes consumed for rows cached to support HS2 FETCH_FIRST.", 
    "key": "palo_be.resultset_cache.total_bytes", 
    "kind": Metrics.TMetricKind.GAUGE, 
    "label": "Palo Be Resultset Cache Total Bytes", 
    "units": Metrics.TUnit.NONE
  }, 
  "palo_be.resultset_cache.total_num_rows": {
    "contexts": [
      "PALO BE"
    ], 
    "description": "Total number of rows cached to support HS2 FETCH_FIRST.", 
    "key": "palo_be.resultset_cache.total_num_rows", 
    "kind": Metrics.TMetricKind.GAUGE, 
    "label": "Palo Be Resultset Cache Total Num Rows", 
    "units": Metrics.TUnit.NONE
  }, 
  "palo_be.scan_ranges.num_missing_volume_id": {
    "contexts": [
      "PALO BE"
    ], 
    "description": "The total number of scan ranges read over the life of the process that did not have volume metadata", 
    "key": "palo_be.scan_ranges.num_missing_volume_id", 
    "kind": Metrics.TMetricKind.COUNTER, 
    "label": "Scan Ranges Missing Volume Information", 
    "units": Metrics.TUnit.UNIT
  }, 
  "palo_be.scan_ranges.total": {
    "contexts": [
      "PALO BE"
    ], 
    "description": "The total number of scan ranges read over the life of the process", 
    "key": "palo_be.scan_ranges.total", 
    "kind": Metrics.TMetricKind.COUNTER, 
    "label": "Scan Ranges", 
    "units": Metrics.TUnit.UNIT
  }, 
  "palo_be.start_time": {
    "contexts": [
      "PALO_BE"
    ], 
    "description": "The local start time of the Palo Be.", 
    "key": "palo_be.start_time", 
    "kind": Metrics.TMetricKind.PROPERTY, 
    "label": "Palo Be Start Time", 
    "units": Metrics.TUnit.NONE
  }, 
  "palo_be.version": {
    "contexts": [
      "PALO_BE"
    ], 
    "description": "The full version string of the Palo Be.", 
    "key": "palo_be.version", 
    "kind": Metrics.TMetricKind.PROPERTY, 
    "label": "Palo Be Version", 
    "units": Metrics.TUnit.NONE
  }, 
  "palo_be.olap.lru_cache.lookup_count": {
    "contexts": [
      "PALO_BE"
    ], 
    "description": "Looking count of StorageEngine's lru cache.", 
    "key": "palo_be.olap.lru_cache.lookup_count", 
    "kind": Metrics.TMetricKind.COUNTER, 
    "label": "StorageEngine Lru Cache Lookup Count", 
    "units": Metrics.TUnit.NONE
  }, 
  "palo_be.olap.lru_cache.hit_count": {
    "contexts": [
      "PALO_BE"
    ], 
    "description": "Hit count of StorageEngine's lru cache.", 
    "key": "palo_be.olap.lru_cache.hit_count",
    "kind": Metrics.TMetricKind.COUNTER, 
    "label": "StorageEngine Lru Cache Hit Count", 
    "units": Metrics.TUnit.NONE
  }, 
  "palo_be.olap.push_count": {
    "contexts": [
      "PALO_BE"
    ], 
    "description": "Pushing count over the life of the Palo Be process.", 
    "key": "palo_be.olap.push_count",
    "kind": Metrics.TMetricKind.COUNTER, 
    "label": "StorageEngine Pushing Count", 
    "units": Metrics.TUnit.NONE
  }, 
  "palo_be.olap.fetch_count": {
    "contexts": [
      "PALO_BE"
    ], 
    "description": "Fetch count over the life of the Palo Be process.", 
    "key": "palo_be.olap.fetch_count",
    "kind": Metrics.TMetricKind.COUNTER, 
    "label": "StorageEngine Fetch Count", 
    "units": Metrics.TUnit.NONE
  }, 
  "palo_be.olap.request_count": {
    "contexts": [
      "PALO_BE"
    ], 
    "description": "Request count over the life of the Palo Be process.", 
    "key": "palo_be.olap.request_count",
    "kind": Metrics.TMetricKind.COUNTER, 
    "label": "StorageEngine Request Count", 
    "units": Metrics.TUnit.NONE
  }, 
  "palo_be.olap.be_merge.delta_num": {
    "contexts": [
      "PALO_BE"
    ], 
    "description": "Base compaction num over the life of the Palo Be process.", 
    "key": "palo_be.olap.be_merge.delta_num",
    "kind": Metrics.TMetricKind.COUNTER, 
    "label": "StorageEngine base compatcion num", 
    "units": Metrics.TUnit.NONE
  }, 
  "palo_be.olap.be_merge_size": {
    "contexts": [
      "PALO_BE"
    ], 
    "description": "Base compaction size over the life of the Palo Be process.", 
    "key": "palo_be.olap.be_merge_size",
    "kind": Metrics.TMetricKind.COUNTER, 
    "label": "StorageEngine base compatcion size", 
    "units": Metrics.TUnit.NONE
  }, 
  "palo_be.olap.ce_merge.delta_num": {
    "contexts": [
      "PALO_BE"
    ], 
    "description": "Cumulative compaction num over the life of the Palo Be process.", 
    "key": "palo_be.olap.ce_merge.delta_num",
    "kind": Metrics.TMetricKind.COUNTER, 
    "label": "StorageEngine cumulative compatcion num", 
    "units": Metrics.TUnit.NONE
  }, 
  "palo_be.olap.ce_merge_size": {
    "contexts": [
      "PALO_BE"
    ], 
    "description": "Cumulative compaction size over the life of the Palo Be process.", 
    "key": "palo_be.olap.ce_merge_size",
    "kind": Metrics.TMetricKind.COUNTER, 
    "label": "StorageEngine cumulative compatcion size", 
    "units": Metrics.TUnit.NONE
  }, 
  "palo_be.thrift_server.PaloBackend.connections_in_use": {
    "contexts": [
      "PALO BE"
    ], 
    "description": "The number of active Palo Backend client connections to this Palo Be.", 
    "key": "palo_be.thrift_server.PaloBackend.connections_in_use", 
    "kind": Metrics.TMetricKind.GAUGE, 
    "label": "Palo Backend Active Connections", 
    "units": Metrics.TUnit.NONE
  }, 
  "palo_be.thrift_server.PaloBackend.total_connections": {
    "contexts": [
      "PALO BE"
    ], 
    "description": "The total number of Palo Backend client connections made to this Palo Be over its lifetime.", 
    "key": "palo_be.thrift_server.PaloBackend.total_connections", 
    "kind": Metrics.TMetricKind.COUNTER, 
    "label": "Palo Backend Server Total Connections", 
    "units": Metrics.TUnit.UNIT
  }, 
  "palo_be.thrift_server.heartbeat.connections_in_use": {
    "contexts": [
      "PALO BE"
    ], 
    "description": "The number of active Palo Backend heartbeat connections to this Palo Be.", 
    "key": "palo_be.thrift_server.heartbeat.connections_in_use", 
    "kind": Metrics.TMetricKind.GAUGE, 
    "label": "Palo Backend HeartBeat Active Connections", 
    "units": Metrics.TUnit.NONE
  }, 
  "palo_be.thrift_server.heartbeat.total_connections": {
    "contexts": [
      "PALO BE"
    ], 
    "description": "The total number of Palo Backend heartbeat connections made to this Palo Be over its lifetime.", 
    "key": "palo_be.thrift_server.heartbeat.total_connections", 
    "kind": Metrics.TMetricKind.COUNTER, 
    "label": "Palo Backend HeartBeat Total Connections", 
    "units": Metrics.TUnit.UNIT
  }, 
  "impala.thrift_server.CatalogService.connection_setup_queue_size": {
    "contexts": [
      "PALO BE"
    ], 
    "description": "The number of connections to the Catalog Service that have been accepted and are waiting to be setup.", 
    "key": "impala.thrift_server.CatalogService.connection_setup_queue_size", 
    "kind": Metrics.TMetricKind.GAUGE, 
    "label": "Catalog Service Connections Queued for Setup", 
    "units": Metrics.TUnit.NONE
  }, 
  "impala.thrift_server.CatalogService.connections_in_use": {
    "contexts": [
      "CATALOGSERVER"
    ], 
    "description": "The number of active catalog service connections to this Catalog Server.", 
    "key": "impala.thrift_server.CatalogService.connections_in_use", 
    "kind": Metrics.TMetricKind.GAUGE, 
    "label": "Catalog Service Active Connections", 
    "units": Metrics.TUnit.NONE
  }, 
  "impala.thrift_server.CatalogService.total_connections": {
    "contexts": [
      "CATALOGSERVER"
    ], 
    "description": "The total number of connections made to this Catalog Server's catalog service  over its lifetime.", 
    "key": "impala.thrift_server.CatalogService.total_connections", 
    "kind": Metrics.TMetricKind.COUNTER, 
    "label": "Catalog Service Total Connections", 
    "units": Metrics.TUnit.UNIT
  }, 
  "impala.thrift_server.StatestoreService.connection_setup_queue_size": {
    "contexts": [
      "PALO BE"
    ], 
    "description": "The number of connections to the Statestore Service that have been accepted and are waiting to be setup.", 
    "key": "impala.thrift_server.StatestoreService.connection_setup_queue_size", 
    "kind": Metrics.TMetricKind.GAUGE, 
    "label": "Statestore Service Connections Queued for Setup", 
    "units": Metrics.TUnit.NONE
  }, 
  "impala.thrift_server.StatestoreService.connections_in_use": {
    "contexts": [
      "STATESTORE"
    ], 
    "description": "The number of active connections to this StateStore's StateStore service.", 
    "key": "impala.thrift_server.StatestoreService.connections_in_use", 
    "kind": Metrics.TMetricKind.GAUGE, 
    "label": "StateStore Service Active Connections", 
    "units": Metrics.TUnit.NONE
  }, 
  "impala.thrift_server.StatestoreService.total_connections": {
    "contexts": [
      "STATESTORE"
    ], 
    "description": "The total number of connections made to this StateStore's StateStore service over its lifetime.", 
    "key": "impala.thrift_server.StatestoreService.total_connections", 
    "kind": Metrics.TMetricKind.COUNTER, 
    "label": "StateStore Service Connections", 
    "units": Metrics.TUnit.UNIT
  }, 
  "impala.thrift_server.backend.connection_setup_queue_size": {
    "contexts": [
      "PALO BE"
    ], 
    "description": "The number of connections to the Impala Backend Server that have been accepted and are waiting to be setup.", 
    "key": "impala.thrift_server.backend.connection_setup_queue_size", 
    "kind": Metrics.TMetricKind.GAUGE, 
    "label": "Impala Backend Server Connections Queued for Setup", 
    "units": Metrics.TUnit.NONE
  }, 
  "impala.thrift_server.backend.connections_in_use": {
    "contexts": [
      "PALO BE"
    ], 
    "description": "The number of active Impala Backend client connections to this Impala Daemon.", 
    "key": "impala.thrift_server.backend.connections_in_use", 
    "kind": Metrics.TMetricKind.GAUGE, 
    "label": "Impala Backend Server Active Connections", 
    "units": Metrics.TUnit.NONE
  }, 
  "impala.thrift_server.backend.total_connections": {
    "contexts": [
      "PALO BE"
    ], 
    "description": "The total number of Impala Backend client connections made to this Impala Daemon over its lifetime.", 
    "key": "impala.thrift_server.backend.total_connections", 
    "kind": Metrics.TMetricKind.COUNTER, 
    "label": "Impala Backend Server Connections", 
    "units": Metrics.TUnit.UNIT
  }, 
  "impala.thrift_server.beeswax_frontend.connections_in_use": {
    "contexts": [
      "PALO BE"
    ], 
    "description": "The number of active Beeswax API connections to this Impala Daemon.", 
    "key": "impala.thrift_server.beeswax_frontend.connections_in_use", 
    "kind": Metrics.TMetricKind.GAUGE, 
    "label": "Beeswax API Active Connections", 
    "units": Metrics.TUnit.NONE
  }, 
  "impala.thrift_server.beeswax_frontend.total_connections": {
    "contexts": [
      "PALO BE"
    ], 
    "description": "The total number of Beeswax API connections made to this Impala Daemon over its lifetime.", 
    "key": "impala.thrift_server.beeswax_frontend.total_connections", 
    "kind": Metrics.TMetricKind.COUNTER, 
    "label": "Beeswax API Total Connections", 
    "units": Metrics.TUnit.UNIT
  }, 
  "impala.thrift_server.hiveserver2_frontend.connections_in_use": {
    "contexts": [
      "PALO BE"
    ], 
    "description": "The number of active HiveServer2 API connections to this Impala Daemon.", 
    "key": "impala.thrift_server.hiveserver2_frontend.connections_in_use", 
    "kind": Metrics.TMetricKind.GAUGE, 
    "label": "HiveServer2 API Active Connections", 
    "units": Metrics.TUnit.NONE
  }, 
  "impala.thrift_server.hiveserver2_frontend.total_connections": {
    "contexts": [
      "PALO BE"
    ], 
    "description": "The total number of HiveServer2 API connections made to this Impala Daemon over its lifetime.", 
    "key": "impala.thrift_server.hiveserver2_frontend.total_connections", 
    "kind": Metrics.TMetricKind.COUNTER, 
    "label": "HiveServer2 API Total Connections", 
    "units": Metrics.TUnit.UNIT
  }, 
  "jvm.$0.committed_usage_bytes": {
    "contexts": [
      "CATALOGSERVER", 
      "PALO BE"
    ], 
    "description": "Jvm $0 Committed Usage Bytes", 
    "key": "jvm.$0.committed_usage_bytes", 
    "kind": Metrics.TMetricKind.GAUGE, 
    "label": "Jvm $0 Committed Usage Bytes", 
    "units": Metrics.TUnit.BYTES
  }, 
  "jvm.$0.current_usage_bytes": {
    "contexts": [
      "CATALOGSERVER", 
      "PALO BE"
    ], 
    "description": "Jvm $0 Current Usage Bytes", 
    "key": "jvm.$0.current_usage_bytes", 
    "kind": Metrics.TMetricKind.GAUGE, 
    "label": "Jvm $0 Current Usage Bytes", 
    "units": Metrics.TUnit.BYTES
  }, 
  "jvm.$0.init_usage_bytes": {
    "contexts": [
      "CATALOGSERVER", 
      "PALO BE"
    ], 
    "description": "Jvm $0 Init Usage Bytes", 
    "key": "jvm.$0.init_usage_bytes", 
    "kind": Metrics.TMetricKind.GAUGE, 
    "label": "Jvm $0 Init Usage Bytes", 
    "units": Metrics.TUnit.BYTES
  }, 
  "jvm.$0.max_usage_bytes": {
    "contexts": [
      "CATALOGSERVER", 
      "PALO BE"
    ], 
    "description": "Jvm $0 Max Usage Bytes", 
    "key": "jvm.$0.max_usage_bytes", 
    "kind": Metrics.TMetricKind.GAUGE, 
    "label": "Jvm $0 Max Usage Bytes", 
    "units": Metrics.TUnit.BYTES
  }, 
  "jvm.$0.peak_committed_usage_bytes": {
    "contexts": [
      "CATALOGSERVER", 
      "PALO BE"
    ], 
    "description": "Jvm $0 Peak Committed Usage Bytes", 
    "key": "jvm.$0.peak_committed_usage_bytes", 
    "kind": Metrics.TMetricKind.GAUGE, 
    "label": "Jvm $0 Peak Committed Usage Bytes", 
    "units": Metrics.TUnit.BYTES
  }, 
  "jvm.$0.peak_current_usage_bytes": {
    "contexts": [
      "CATALOGSERVER", 
      "PALO BE"
    ], 
    "description": "Jvm $0 Peak Current Usage Bytes", 
    "key": "jvm.$0.peak_current_usage_bytes", 
    "kind": Metrics.TMetricKind.GAUGE, 
    "label": "Jvm $0 Peak Current Usage Bytes", 
    "units": Metrics.TUnit.BYTES
  }, 
  "jvm.$0.peak_init_usage_bytes": {
    "contexts": [
      "CATALOGSERVER", 
      "PALO BE"
    ], 
    "description": "Jvm $0 Peak Init Usage Bytes", 
    "key": "jvm.$0.peak_init_usage_bytes", 
    "kind": Metrics.TMetricKind.GAUGE, 
    "label": "Jvm $0 Peak Init Usage Bytes", 
    "units": Metrics.TUnit.BYTES
  }, 
  "jvm.$0.peak_max_usage_bytes": {
    "contexts": [
      "CATALOGSERVER", 
      "PALO BE"
    ], 
    "description": "Jvm $0 Peak Max Usage Bytes", 
    "key": "jvm.$0.peak_max_usage_bytes", 
    "kind": Metrics.TMetricKind.GAUGE, 
    "label": "Jvm $0 Peak Max Usage Bytes", 
    "units": Metrics.TUnit.BYTES
  }, 
  "mem_tracker.process.bytes_freed_by_last_gc": {
    "contexts": [
      "PALO BE"
    ], 
    "description": "The amount of memory freed by the last memory tracker garbage collection.", 
    "key": "mem_tracker.process.bytes_freed_by_last_gc", 
    "kind": Metrics.TMetricKind.GAUGE, 
    "label": "MemTracker Bytes Freed By Last Garbage Collection", 
    "units": Metrics.TUnit.BYTES
  }, 
  "mem_tracker.process.bytes_over_limit": {
    "contexts": [
      "PALO BE"
    ], 
    "description": "The amount of memory by which the process was over its memory limit the last time the memory limit was encountered.", 
    "key": "mem_tracker.process.bytes_over_limit", 
    "kind": Metrics.TMetricKind.GAUGE, 
    "label": "MemTracker Bytes Over Limit", 
    "units": Metrics.TUnit.BYTES
  }, 
  "mem_tracker.process.limit": {
    "contexts": [
      "PALO BE"
    ], 
    "description": "The process memory tracker limit.", 
    "key": "mem_tracker.process.limit", 
    "kind": Metrics.TMetricKind.GAUGE, 
    "label": "Process Tracker Limit", 
    "units": Metrics.TUnit.BYTES
  }, 
  "mem_tracker.process.num_gcs": {
    "contexts": [
      "PALO BE"
    ], 
    "description": "The total number of garbage collections performed by the memory tracker over the life of the process.", 
    "key": "mem_tracker.process.num_gcs", 
    "kind": Metrics.TMetricKind.COUNTER, 
    "label": "MemTracker Garbage Collections", 
    "units": Metrics.TUnit.UNIT
  }, 
  "memory.total_used": {
    "contexts": [
      "STATESTORE", 
      "CATALOGSERVER", 
      "PALO BE"
    ], 
    "description": "Total memory currently used by TCMalloc and buffer pool.", 
    "key": "memory.total_used", 
    "kind": Metrics.TMetricKind.GAUGE, 
    "label": "Total Used Memory.", 
    "units": Metrics.TUnit.BYTES
  }, 
  "request_pool_service.resolve_pool_duration_ms": {
    "contexts": [
      "PALO BE"
    ], 
    "description": "Time (ms) spent resolving request request pools.", 
    "key": "request_pool_service.resolve_pool_duration_ms", 
    "kind": Metrics.TMetricKind.STATS, 
    "label": "Request Pool Service Resolve Pool Duration Ms", 
    "units": Metrics.TUnit.TIME_MS
  }, 
  "rpc_method.$0.call_duration": {
    "contexts": [
      "CATALOGSERVER", 
      "STATESTORE", 
      "PALO BE"
    ], 
    "description": "Duration (ms) of RPC calls to $0", 
    "key": "rpc_method.$0.call_duration", 
    "kind": Metrics.TMetricKind.HISTOGRAM, 
    "label": "$0 RPC Call Duration", 
    "units": Metrics.TUnit.TIME_MS
  }, 
  "senders_blocked_on_recvr_creation": {
    "contexts": [
      "PALO BE"
    ], 
    "description": "Number of senders waiting for receiving fragment to initialize", 
    "key": "senders_blocked_on_recvr_creation", 
    "kind": Metrics.TMetricKind.GAUGE, 
    "label": "Number of senders waiting for receiving fragment to initialize.", 
    "units": Metrics.TUnit.NONE
  }, 
  "simple_scheduler.assignments.total": {
    "contexts": [
      "PALO BE"
    ], 
    "description": "The number of assignments", 
    "key": "simple_scheduler.assignments.total", 
    "kind": Metrics.TMetricKind.COUNTER, 
    "label": "Assignments", 
    "units": Metrics.TUnit.UNIT
  }, 
  "simple_scheduler.initialized": {
    "contexts": [
      "PALO BE"
    ], 
    "description": "Indicates whether the scheduler has been initialized.", 
    "key": "simple_scheduler.initialized", 
    "kind": Metrics.TMetricKind.PROPERTY, 
    "label": "Simple Scheduler Initialized", 
    "units": Metrics.TUnit.NONE
  }, 
  "simple_scheduler.local_assignments.total": {
    "contexts": [
      "PALO BE"
    ], 
    "description": "Number of assignments operating on local data", 
    "key": "simple_scheduler.local_assignments.total", 
    "kind": Metrics.TMetricKind.COUNTER, 
    "label": "Local Assignments", 
    "units": Metrics.TUnit.UNIT
  }, 
  "simple_scheduler.num_backends": {
    "contexts": [
      "PALO BE"
    ], 
    "description": "The number of backend connections from this Impala Daemon to other Impala Daemons.", 
    "key": "simple_scheduler.num_backends", 
    "kind": Metrics.TMetricKind.GAUGE, 
    "label": "Backend Connections", 
    "units": Metrics.TUnit.NONE
  }, 
  "statestore_subscriber.connected": {
    "contexts": [
      "CATALOGSERVER", 
      "PALO BE"
    ], 
    "description": "Whether the Impala Daemon considers itself connected to the StateStore.", 
    "key": "statestore_subscriber.connected", 
    "kind": Metrics.TMetricKind.PROPERTY, 
    "label": "StateStore Connectivity", 
    "units": Metrics.TUnit.NONE
  }, 
  "statestore_subscriber.heartbeat_interval_time": {
    "contexts": [
      "CATALOGSERVER", 
      "PALO BE"
    ], 
    "description": "The time (sec) between Statestore heartbeats.", 
    "key": "statestore_subscriber.heartbeat_interval_time", 
    "kind": Metrics.TMetricKind.STATS, 
    "label": "Statestore Subscriber Heartbeat Interval Time", 
    "units": Metrics.TUnit.TIME_S
  }, 
  "statestore_subscriber.last_recovery_duration": {
    "contexts": [
      "CATALOGSERVER", 
      "PALO BE"
    ], 
    "description": "The amount of time the StateStore subscriber took to recover the connection the last time it was lost.", 
    "key": "statestore_subscriber.last_recovery_duration", 
    "kind": Metrics.TMetricKind.GAUGE, 
    "label": "StateStore Subscriber Last Recovery Duration", 
    "units": Metrics.TUnit.NONE
  }, 
  "statestore_subscriber.last_recovery_time": {
    "contexts": [
      "CATALOGSERVER", 
      "PALO BE"
    ], 
    "description": "The local time that the last statestore recovery happened.", 
    "key": "statestore_subscriber.last_recovery_time", 
    "kind": Metrics.TMetricKind.PROPERTY, 
    "label": "Statestore Subscriber Last Recovery Time", 
    "units": Metrics.TUnit.NONE
  }, 
  "statestore_subscriber.registration_id": {
    "contexts": [
      "CATALOGSERVER", 
      "PALO BE"
    ], 
    "description": "The most recent registration ID for this subscriber with the statestore. Set to 'N/A' if no registration has been completed", 
    "key": "statestore_subscriber.registration_id", 
    "kind": Metrics.TMetricKind.PROPERTY, 
    "label": "Statestore Subscriber Registration Id", 
    "units": Metrics.TUnit.NONE
  }, 
  "statestore_subscriber.statestore.client_cache.clients_in_use": {
    "contexts": [
      "CATALOGSERVER", 
      "PALO BE"
    ], 
    "description": "The number of active StateStore subscriber clients in this Impala Daemon's client cache. These clients are for communication from this role to the StateStore.", 
    "key": "statestore_subscriber.statestore.client_cache.clients_in_use", 
    "kind": Metrics.TMetricKind.GAUGE, 
    "label": "StateStore Subscriber Active Clients", 
    "units": Metrics.TUnit.NONE
  }, 
  "statestore_subscriber.statestore.client_cache.total_clients": {
    "contexts": [
      "CATALOGSERVER", 
      "PALO BE"
    ], 
    "description": "The total number of StateStore subscriber clients in this Impala Daemon's client cache. These clients are for communication from this role to the StateStore.", 
    "key": "statestore_subscriber.statestore.client_cache.total_clients", 
    "kind": Metrics.TMetricKind.GAUGE, 
    "label": "StateStore Subscriber Total Clients", 
    "units": Metrics.TUnit.NONE
  }, 
  "statestore_subscriber.topic_$0.processing_time_s": {
    "contexts": [
      "CATALOGSERVER", 
      "PALO BE"
    ], 
    "description": "Statestore Subscriber Topic $0 Processing Time", 
    "key": "statestore_subscriber.topic_$0.processing_time_s", 
    "kind": Metrics.TMetricKind.STATS, 
    "label": "Statestore Subscriber Topic $0 Processing Time", 
    "units": Metrics.TUnit.TIME_S
  }, 
  "statestore_subscriber.topic_update_duration": {
    "contexts": [
      "CATALOGSERVER", 
      "PALO BE"
    ], 
    "description": "The time (sec) taken to process Statestore subcriber topic updates.", 
    "key": "statestore_subscriber.topic_update_duration", 
    "kind": Metrics.TMetricKind.STATS, 
    "label": "Statestore Subscriber Topic Update Duration", 
    "units": Metrics.TUnit.TIME_S
  }, 
  "statestore_subscriber.topic_update_interval_time": {
    "contexts": [
      "CATALOGSERVER", 
      "PALO BE"
    ], 
    "description": "The time (sec) between Statestore subscriber topic updates.", 
    "key": "statestore_subscriber.topic_update_interval_time", 
    "kind": Metrics.TMetricKind.STATS, 
    "label": "Statestore Subscriber Topic Update Interval Time", 
    "units": Metrics.TUnit.TIME_S
  }, 
  "statestore.heartbeat_durations": {
    "contexts": [
      "STATESTORE"
    ], 
    "description": "The time (sec) spent sending heartbeat RPCs. Includes subscriber_side processing time and network transmission time.", 
    "key": "statestore.heartbeat_durations", 
    "kind": Metrics.TMetricKind.STATS, 
    "label": "Statestore Heartbeat Durations", 
    "units": Metrics.TUnit.TIME_S
  }, 
  "statestore.live_backends": {
    "contexts": [
      "STATESTORE"
    ], 
    "description": "The number of registered Statestore subscribers.", 
    "key": "statestore.live_backends", 
    "kind": Metrics.TMetricKind.GAUGE, 
    "label": "Statestore Live Backends", 
    "units": Metrics.TUnit.NONE
  }, 
  "statestore.live_backends.list": {
    "contexts": [
      "STATESTORE"
    ], 
    "description": "The set of all live Statestore subscribers.", 
    "key": "statestore.live_backends.list", 
    "kind": Metrics.TMetricKind.SET, 
    "label": "Statestore Live Backends List", 
    "units": Metrics.TUnit.NONE
  }, 
  "statestore.topic_update_durations": {
    "contexts": [
      "STATESTORE"
    ], 
    "description": "The time (sec) spent sending topic update RPCs. Includes subscriber_side processing time and network transmission time.", 
    "key": "statestore.topic_update_durations", 
    "kind": Metrics.TMetricKind.STATS, 
    "label": "Statestore Topic Update Durations", 
    "units": Metrics.TUnit.TIME_S
  }, 
  "statestore.total_key_size_bytes": {
    "contexts": [
      "STATESTORE"
    ], 
    "description": "The sum of the size of all keys for all topics tracked by the StateStore.", 
    "key": "statestore.total_key_size_bytes", 
    "kind": Metrics.TMetricKind.GAUGE, 
    "label": "Topic Key Size", 
    "units": Metrics.TUnit.NONE
  }, 
  "statestore.total_topic_size_bytes": {
    "contexts": [
      "STATESTORE"
    ], 
    "description": "The sum of the size of all keys and all values for all topics tracked by the StateStore.", 
    "key": "statestore.total_topic_size_bytes", 
    "kind": Metrics.TMetricKind.GAUGE, 
    "label": "Topic Size", 
    "units": Metrics.TUnit.NONE
  }, 
  "statestore.total_value_size_bytes": {
    "contexts": [
      "STATESTORE"
    ], 
    "description": "The sum of the size of all values for all topics tracked by the StateStore.", 
    "key": "statestore.total_value_size_bytes", 
    "kind": Metrics.TMetricKind.GAUGE, 
    "label": "Topic Value Size", 
    "units": Metrics.TUnit.NONE
  }, 
  "statestore.version": {
    "contexts": [
      "STATESTORE"
    ], 
    "description": "The full version string of the Statestore Server.", 
    "key": "statestore.version", 
    "kind": Metrics.TMetricKind.PROPERTY, 
    "label": "Statestore Version", 
    "units": Metrics.TUnit.NONE
  }, 
  "subscriber_heartbeat.client_cache.clients_in_use": {
    "contexts": [
      "STATESTORE"
    ], 
    "description": "The number of clients in use by the Statestore heartbeat client cache.", 
    "key": "subscriber_heartbeat.client_cache.clients_in_use", 
    "kind": Metrics.TMetricKind.GAUGE, 
    "label": "Subscriber Heartbeat Client Cache Clients In Use", 
    "units": Metrics.TUnit.NONE
  }, 
  "subscriber_heartbeat.client_cache.total_clients": {
    "contexts": [
      "STATESTORE"
    ], 
    "description": "The total number of clients in the Statestore heartbeat client cache.", 
    "key": "subscriber_heartbeat.client_cache.total_clients", 
    "kind": Metrics.TMetricKind.GAUGE, 
    "label": "Subscriber Heartbeat Client Cache Total Clients", 
    "units": Metrics.TUnit.NONE
  }, 
  "subscriber_update_state.client_cache.clients_in_use": {
    "contexts": [
      "STATESTORE"
    ], 
    "description": "The number of clients in use by the Statestore update state client cache.", 
    "key": "subscriber_update_state.client_cache.clients_in_use", 
    "kind": Metrics.TMetricKind.GAUGE, 
    "label": "Subscriber Update State Client Cache Clients In Use", 
    "units": Metrics.TUnit.NONE
  }, 
  "subscriber_update_state.client_cache.total_clients": {
    "contexts": [
      "STATESTORE"
    ], 
    "description": "The total number of clients in the Statestore update state client cache.", 
    "key": "subscriber_update_state.client_cache.total_clients", 
    "kind": Metrics.TMetricKind.GAUGE, 
    "label": "Subscriber Update State Client Cache Total Clients", 
    "units": Metrics.TUnit.NONE
  }, 
  "tcmalloc.bytes_in_use": {
    "contexts": [
      "STATESTORE", 
      "CATALOGSERVER", 
      "PALO BE"
    ], 
    "description": "Number of bytes used by the application. This will not typically match the memory use reported by the OS, because it does not include TCMalloc overhead or memory fragmentation.", 
    "key": "tcmalloc.bytes_in_use", 
    "kind": Metrics.TMetricKind.GAUGE, 
    "label": "TCMalloc Bytes in Use", 
    "units": Metrics.TUnit.BYTES
  }, 
  "tcmalloc.pageheap_free_bytes": {
    "contexts": [
      "STATESTORE", 
      "CATALOGSERVER", 
      "PALO BE"
    ], 
    "description": "Number of bytes in free, mapped pages in page heap. These bytes can be used to fulfill allocation requests. They always count towards virtual memory usage, and unless the underlying memory is swapped out by the OS, they also count towards physical memory usage.", 
    "key": "tcmalloc.pageheap_free_bytes", 
    "kind": Metrics.TMetricKind.GAUGE, 
    "label": "TCMalloc Pageheap Free", 
    "units": Metrics.TUnit.BYTES
  }, 
  "tcmalloc.pageheap_unmapped_bytes": {
    "contexts": [
      "STATESTORE", 
      "CATALOGSERVER", 
      "PALO BE"
    ], 
    "description": "Number of bytes in free, unmapped pages in page heap. These are bytes that have been released back to the OS, possibly by one of the MallocExtension \"Release\" calls. They can be used to fulfill allocation requests, but typically incur a page fault. They always count towards virtual memory usage, and depending on the OS, typically do not count towards physical memory usage.", 
    "key": "tcmalloc.pageheap_unmapped_bytes", 
    "kind": Metrics.TMetricKind.GAUGE, 
    "label": "TCMalloc Pageheap Unmapped", 
    "units": Metrics.TUnit.BYTES
  }, 
  "tcmalloc.physical_bytes_reserved": {
    "contexts": [
      "STATESTORE", 
      "CATALOGSERVER", 
      "PALO BE"
    ], 
    "description": "Derived metric computing the amount of physical memory (in bytes) used by the process, including that actually in use and free bytes reserved by tcmalloc. Does not include the tcmalloc metadata.", 
    "key": "tcmalloc.physical_bytes_reserved", 
    "kind": Metrics.TMetricKind.GAUGE, 
    "label": "TCMalloc Physical Bytes Reserved", 
    "units": Metrics.TUnit.BYTES
  }, 
  "tcmalloc.total_bytes_reserved": {
    "contexts": [
      "STATESTORE", 
      "CATALOGSERVER", 
      "PALO BE"
    ], 
    "description": "Bytes of system memory reserved by TCMalloc.", 
    "key": "tcmalloc.total_bytes_reserved", 
    "kind": Metrics.TMetricKind.GAUGE, 
    "label": "TCMalloc Total Bytes Reserved", 
    "units": Metrics.TUnit.BYTES
  }, 
  "thread_manager.running_threads": {
    "contexts": [
      "STATESTORE", 
      "CATALOGSERVER", 
      "PALO BE"
    ], 
    "description": "The number of running threads in this process.", 
    "key": "thread_manager.running_threads", 
    "kind": Metrics.TMetricKind.GAUGE, 
    "label": "Running Threads", 
    "units": Metrics.TUnit.NONE
  }, 
  "thread_manager.total_threads_created": {
    "contexts": [
      "STATESTORE", 
      "CATALOGSERVER", 
      "PALO BE"
    ], 
    "description": "Threads created over the lifetime of the process.", 
    "key": "thread_manager.total_threads_created", 
    "kind": Metrics.TMetricKind.GAUGE, 
    "label": "Threads Created", 
    "units": Metrics.TUnit.NONE
  }, 
  "tmp_file_mgr.active_scratch_dirs": {
    "contexts": [
      "PALO BE"
    ], 
    "description": "The number of active scratch directories for spilling to disk.", 
    "key": "tmp_file_mgr.active_scratch_dirs", 
    "kind": Metrics.TMetricKind.GAUGE, 
    "label": "Active scratch directories", 
    "units": Metrics.TUnit.NONE
  }, 
  "tmp_file_mgr.active_scratch_dirs.list": {
    "contexts": [
      "PALO BE"
    ], 
    "description": "The set of all active scratch directories for spilling to disk.", 
    "key": "tmp_file_mgr.active_scratch_dirs.list", 
    "kind": Metrics.TMetricKind.SET, 
    "label": "Active scratch directories list", 
    "units": Metrics.TUnit.NONE
  }, 
  "total_senders_blocked_on_recvr_creation": {
    "contexts": [
      "PALO BE"
    ], 
    "description": "Total number of senders that have been blocked waiting for receiving fragment to initialize.", 
    "key": "total_senders_blocked_on_recvr_creation", 
    "kind": Metrics.TMetricKind.COUNTER, 
    "label": "Total senders waiting for receiving fragment to initialize", 
    "units": Metrics.TUnit.NONE
  }, 
  "total_senders_timedout_waiting_for_recvr_creation": {
    "contexts": [
      "PALO BE"
    ], 
    "description": "Total number of senders that timed_out waiting for receiving fragment to initialize.", 
    "key": "total_senders_timedout_waiting_for_recvr_creation", 
    "kind": Metrics.TMetricKind.COUNTER, 
    "label": "Total senders timed_out waiting for receiving fragment to initialize", 
    "units": Metrics.TUnit.NONE
  }
}
