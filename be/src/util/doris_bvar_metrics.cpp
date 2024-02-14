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

#include "doris_bvar_metrics.h"

#include <sstream>

namespace doris {

#define INIT_INT64_BVAR_METRIC(name, type, unit, description, group_name, labels, core)           \
    name = std::make_shared<BvarAdderMetric<int64_t>>(type, unit, #name, description, group_name, \
                                                      labels, core);

#define INIT_UINT64_BVAR_METRIC(name, type, unit, description, group_name, labels, core)           \
    name = std::make_shared<BvarAdderMetric<uint64_t>>(type, unit, #name, description, group_name, \
                                                       labels, core);

#define ENTITY_REGISTER_METRICS(name, type)                            \
    auto name##_ptr = std::make_shared<BvarMetricEntity>(#name, type); \
    entities_map_[#name].push_back(name##_ptr);                        \
    name##_ptr->register_metric(#name, *name);

// #define INIT_DOUBLE_BVAR_METRIC(name, type, unit, description, group_name, labels, core) \
//     name = std::make_shared<BvarAdderMetric<double>>(type, unit, #name, description, group_name, labels, core);

const std::string DorisBvarMetrics::s_registry_name_ = "doris_be";

DorisBvarMetrics::DorisBvarMetrics() {
    INIT_INT64_BVAR_METRIC(fragment_requests_total, BvarMetricType::COUNTER,
                           BvarMetricUnit::REQUESTS, "Total fragment requests received.", "",
                           BvarMetric::Labels(), false)
    INIT_INT64_BVAR_METRIC(fragment_request_duration_us, BvarMetricType::COUNTER,
                           BvarMetricUnit::MICROSECONDS, "", "", BvarMetric::Labels(), false)
    INIT_INT64_BVAR_METRIC(query_scan_bytes, BvarMetricType::COUNTER, BvarMetricUnit::BYTES, "", "",
                           BvarMetric::Labels(), false)
    INIT_INT64_BVAR_METRIC(query_scan_rows, BvarMetricType::COUNTER, BvarMetricUnit::ROWS, "", "",
                           BvarMetric::Labels(), false)
    INIT_INT64_BVAR_METRIC(push_requests_success_total, BvarMetricType::COUNTER,
                           BvarMetricUnit::REQUESTS, "", "push_requests_total",
                           BvarMetric::Labels({{"status", "SUCCESS"}}), false);
    INIT_INT64_BVAR_METRIC(push_requests_fail_total, BvarMetricType::COUNTER,
                           BvarMetricUnit::REQUESTS, "", "push_requests_total",
                           BvarMetric::Labels({{"status", "FAIL"}}), false);
    INIT_INT64_BVAR_METRIC(push_request_duration_us, BvarMetricType::COUNTER,
                           BvarMetricUnit::MICROSECONDS, "", "", BvarMetric::Labels(), false)
    INIT_INT64_BVAR_METRIC(push_request_write_bytes, BvarMetricType::COUNTER, BvarMetricUnit::BYTES,
                           "", "", BvarMetric::Labels(), false)
    INIT_INT64_BVAR_METRIC(push_request_write_rows, BvarMetricType::COUNTER, BvarMetricUnit::ROWS,
                           "", "", BvarMetric::Labels(), false)
    INIT_INT64_BVAR_METRIC(create_tablet_requests_total, BvarMetricType::COUNTER,
                           BvarMetricUnit::REQUESTS, "", "engine_requests_total",
                           BvarMetric::Labels({{"type", "create_tablet"}, {"status", "total"}}), false);
    INIT_INT64_BVAR_METRIC(create_tablet_requests_failed, BvarMetricType::COUNTER,
                           BvarMetricUnit::REQUESTS, "", "engine_requests_total",
                           BvarMetric::Labels({{"type", "create_tablet"}, {"status", "failed"}}), false);
    INIT_INT64_BVAR_METRIC(drop_tablet_requests_total, BvarMetricType::COUNTER,
                           BvarMetricUnit::REQUESTS, "", "engine_requests_total",
                           BvarMetric::Labels({{"type", "drop_tablet"}, {"status", "total"}}), false);
    INIT_INT64_BVAR_METRIC(report_all_tablets_requests_skip, BvarMetricType::COUNTER,
                           BvarMetricUnit::REQUESTS, "", "engine_requests_total",
                           BvarMetric::Labels({{"type", "report_all_tablets"}, {"status", "skip"}}), false);
    INIT_INT64_BVAR_METRIC(schema_change_requests_total, BvarMetricType::COUNTER,
                           BvarMetricUnit::REQUESTS, "", "engine_requests_total",
                           BvarMetric::Labels({{"type", "schema_change"}, {"status", "total"}}), false);
    INIT_INT64_BVAR_METRIC(schema_change_requests_failed, BvarMetricType::COUNTER,
                           BvarMetricUnit::REQUESTS, "", "engine_requests_total",
                           BvarMetric::Labels({{"type", "schema_change"}, {"status", "failed"}}), false);
    INIT_INT64_BVAR_METRIC(create_rollup_requests_total, BvarMetricType::COUNTER,
                           BvarMetricUnit::REQUESTS, "", "engine_requests_total",
                           BvarMetric::Labels({{"type", "create_rollup"}, {"status", "total"}}), false);
    INIT_INT64_BVAR_METRIC(create_rollup_requests_failed, BvarMetricType::COUNTER,
                           BvarMetricUnit::REQUESTS, "", "engine_requests_total",
                           BvarMetric::Labels({{"type", "create_rollup"}, {"status", "failed"}}), false);
    INIT_INT64_BVAR_METRIC(storage_migrate_requests_total, BvarMetricType::COUNTER,
                           BvarMetricUnit::REQUESTS, "", "engine_requests_total",
                           BvarMetric::Labels({{"type", "storage_migrate"}, {"status", "total"}}), false);
    INIT_INT64_BVAR_METRIC(storage_migrate_v2_requests_total, BvarMetricType::COUNTER,
                           BvarMetricUnit::REQUESTS, "", "engine_requests_total",
                           BvarMetric::Labels({{"type", "storage_migrate_v2"}, {"status", "total"}}), false);
    INIT_INT64_BVAR_METRIC(storage_migrate_v2_requests_failed, BvarMetricType::COUNTER,
                           BvarMetricUnit::REQUESTS, "", "engine_requests_total",
                           BvarMetric::Labels({{"type", "storage_migrate_v2"}, {"status", "failed"}}), false);
    INIT_INT64_BVAR_METRIC(delete_requests_total, BvarMetricType::COUNTER, BvarMetricUnit::REQUESTS,
                           "", "engine_requests_total",
                           BvarMetric::Labels({{"type", "delete"}, {"status", "total"}}), false);
    INIT_INT64_BVAR_METRIC(delete_requests_failed, BvarMetricType::COUNTER,
                           BvarMetricUnit::REQUESTS, "", "engine_requests_total",
                           BvarMetric::Labels({{"type", "delete"}, {"status", "failed"}}), false);
    INIT_INT64_BVAR_METRIC(clone_requests_total, BvarMetricType::COUNTER, BvarMetricUnit::REQUESTS,
                           "", "engine_requests_total",
                           BvarMetric::Labels({{"type", "clone"}, {"status", "total"}}), false);
    INIT_INT64_BVAR_METRIC(clone_requests_failed, BvarMetricType::COUNTER, BvarMetricUnit::REQUESTS,
                           "", "engine_requests_total",
                           BvarMetric::Labels({{"type", "clone"}, {"status", "failed"}}), false);
    INIT_INT64_BVAR_METRIC(alter_inverted_index_requests_total, BvarMetricType::COUNTER,
                           BvarMetricUnit::REQUESTS, "", "engine_requests_total",
                           BvarMetric::Labels({{"type", "alter_inverted_index"}, {"status", "total"}}), false);
    INIT_INT64_BVAR_METRIC(alter_inverted_index_requests_failed, BvarMetricType::COUNTER,
                           BvarMetricUnit::REQUESTS, "", "engine_requests_total",
                           BvarMetric::Labels({{"type", "alter_inverted_index"}, {"status", "failed"}}), false);
    INIT_INT64_BVAR_METRIC(finish_task_requests_total, BvarMetricType::COUNTER,
                           BvarMetricUnit::REQUESTS, "", "engine_requests_total",
                           BvarMetric::Labels({{"type", "finish_task"}, {"status", "total"}}), false);
    INIT_INT64_BVAR_METRIC(finish_task_requests_failed, BvarMetricType::COUNTER,
                           BvarMetricUnit::REQUESTS, "", "engine_requests_total",
                           BvarMetric::Labels({{"type", "finish_task"}, {"status", "failed"}}), false);
    INIT_INT64_BVAR_METRIC(base_compaction_request_total, BvarMetricType::COUNTER,
                           BvarMetricUnit::REQUESTS, "", "engine_requests_total",
                           BvarMetric::Labels({{"type", "base_compaction"}, {"status", "total"}}), false);
    INIT_INT64_BVAR_METRIC(base_compaction_request_failed, BvarMetricType::COUNTER,
                           BvarMetricUnit::REQUESTS, "", "engine_requests_total",
                           BvarMetric::Labels({{"type", "base_compaction"}, {"status", "failed"}}), false);
    INIT_INT64_BVAR_METRIC(cumulative_compaction_request_total, BvarMetricType::COUNTER,
                           BvarMetricUnit::REQUESTS, "", "engine_requests_total",
                           BvarMetric::Labels({{"type", "cumulative_compaction"}, {"status", "total"}}), false);
    INIT_INT64_BVAR_METRIC(cumulative_compaction_request_failed, BvarMetricType::COUNTER,
                           BvarMetricUnit::REQUESTS, "", "engine_requests_total",
                           BvarMetric::Labels({{"type", "cumulative_compaction"}, {"status", "failed"}}),
                           false);
    INIT_INT64_BVAR_METRIC(base_compaction_deltas_total, BvarMetricType::COUNTER,
                           BvarMetricUnit::ROWSETS, "", "compaction_deltas_total",
                           BvarMetric::Labels({{"type", "base"}}), false);
    INIT_INT64_BVAR_METRIC(base_compaction_bytes_total, BvarMetricType::COUNTER,
                           BvarMetricUnit::BYTES, "", "compaction_bytes_total",
                           BvarMetric::Labels({{"type", "base"}}), false);
    INIT_INT64_BVAR_METRIC(cumulative_compaction_deltas_total, BvarMetricType::COUNTER,
                           BvarMetricUnit::ROWSETS, "", "compaction_deltas_total",
                           BvarMetric::Labels({{"type", "cumulative"}}), false);
    INIT_INT64_BVAR_METRIC(cumulative_compaction_bytes_total, BvarMetricType::COUNTER,
                           BvarMetricUnit::BYTES, "", "compaction_bytes_total",
                           BvarMetric::Labels({{"type", "cumulative"}}), false);
    INIT_INT64_BVAR_METRIC(publish_task_request_total, BvarMetricType::COUNTER,
                           BvarMetricUnit::REQUESTS, "", "engine_requests_total",
                           BvarMetric::Labels({{"type", "publish"}, {"status", "total"}}), false);
    INIT_INT64_BVAR_METRIC(publish_task_failed_total, BvarMetricType::COUNTER,
                           BvarMetricUnit::REQUESTS, "", "engine_requests_total",
                           BvarMetric::Labels({{"type", "publish"}, {"status", "failed"}}), false);
    INIT_INT64_BVAR_METRIC(segment_read_total, BvarMetricType::COUNTER, BvarMetricUnit::OPERATIONS,
                           "(segment_v2) total number of segments read", "segment_read",
                           BvarMetric::Labels({{"type", "segment_read_total"}}), false)
    INIT_INT64_BVAR_METRIC(
            segment_row_total, BvarMetricType::COUNTER, BvarMetricUnit::ROWS,
            "(segment_v2) total number of rows in queried segments (before index pruning)",
            "segment_read", BvarMetric::Labels({{"type", "segment_row_total"}}), false)
    INIT_INT64_BVAR_METRIC(stream_load_txn_begin_request_total, BvarMetricType::COUNTER,
                           BvarMetricUnit::OPERATIONS, "", "stream_load_txn_request",
                           BvarMetric::Labels({{"type", "begin"}}), false)
    INIT_INT64_BVAR_METRIC(stream_load_txn_commit_request_total, BvarMetricType::COUNTER,
                           BvarMetricUnit::OPERATIONS, "", "stream_load_txn_request",
                           BvarMetric::Labels({{"type", "commit"}}), false)
    INIT_INT64_BVAR_METRIC(stream_load_txn_rollback_request_total, BvarMetricType::COUNTER,
                           BvarMetricUnit::OPERATIONS, "", "stream_load_txn_request",
                           BvarMetric::Labels({{"type", "rollback"}}), false)
    INIT_INT64_BVAR_METRIC(stream_receive_bytes_total, BvarMetricType::COUNTER,
                           BvarMetricUnit::BYTES, "", "stream_load",
                           BvarMetric::Labels({{"type", "receive_bytes"}}), false)
    INIT_INT64_BVAR_METRIC(stream_load_rows_total, BvarMetricType::COUNTER, BvarMetricUnit::ROWS,
                           "", "stream_load", BvarMetric::Labels({{"type", "load_rows"}}), false)
    INIT_INT64_BVAR_METRIC(load_rows, BvarMetricType::COUNTER, BvarMetricUnit::ROWS, "", "",
                           BvarMetric::Labels(), false)
    INIT_INT64_BVAR_METRIC(load_bytes, BvarMetricType::COUNTER, BvarMetricUnit::BYTES, "", "",
                           BvarMetric::Labels(), false)
    INIT_INT64_BVAR_METRIC(memtable_flush_total, BvarMetricType::COUNTER,
                           BvarMetricUnit::OPERATIONS, "", "", BvarMetric::Labels(), false)
    INIT_INT64_BVAR_METRIC(memtable_flush_duration_us, BvarMetricType::COUNTER,
                           BvarMetricUnit::MICROSECONDS, "", "", BvarMetric::Labels(), false)
    INIT_INT64_BVAR_METRIC(memory_pool_bytes_total, BvarMetricType::GAUGE, BvarMetricUnit::BYTES,
                           "", "", BvarMetric::Labels(), false)
    INIT_INT64_BVAR_METRIC(process_thread_num, BvarMetricType::GAUGE, BvarMetricUnit::NOUNIT, "",
                           "", BvarMetric::Labels(), true)
    INIT_INT64_BVAR_METRIC(process_fd_num_used, BvarMetricType::GAUGE, BvarMetricUnit::NOUNIT, "",
                           "", BvarMetric::Labels(), true)
    INIT_INT64_BVAR_METRIC(process_fd_num_limit_soft, BvarMetricType::GAUGE, BvarMetricUnit::NOUNIT,
                           "", "", BvarMetric::Labels(), false)
    INIT_INT64_BVAR_METRIC(process_fd_num_limit_hard, BvarMetricType::GAUGE, BvarMetricUnit::NOUNIT,
                           "", "", BvarMetric::Labels(), false)
    INIT_INT64_BVAR_METRIC(tablet_cumulative_max_compaction_score, BvarMetricType::GAUGE,
                           BvarMetricUnit::NOUNIT, "", "", BvarMetric::Labels(), false)
    INIT_INT64_BVAR_METRIC(tablet_base_max_compaction_score, BvarMetricType::GAUGE,
                           BvarMetricUnit::NOUNIT, "", "", BvarMetric::Labels(), false)
    INIT_INT64_BVAR_METRIC(all_rowsets_num, BvarMetricType::GAUGE, BvarMetricUnit::NOUNIT, "", "",
                           BvarMetric::Labels(), false)
    INIT_INT64_BVAR_METRIC(all_segments_num, BvarMetricType::GAUGE, BvarMetricUnit::NOUNIT, "", "",
                           BvarMetric::Labels(), false)
    INIT_INT64_BVAR_METRIC(compaction_used_permits, BvarMetricType::GAUGE, BvarMetricUnit::NOUNIT,
                           "", "", BvarMetric::Labels(), false)
    INIT_INT64_BVAR_METRIC(compaction_waitting_permits, BvarMetricType::GAUGE,
                           BvarMetricUnit::NOUNIT, "", "", BvarMetric::Labels(), false)
    // INIT_INT64_BVAR_METRIC(tablet_version_num_distribution, BvarMetricType::HISTOGRAM, BvarMetricUnit::NOUNIT,
    //                        "", "", BvarMetric::Labels(), false)
    INIT_INT64_BVAR_METRIC(query_scan_bytes_per_second, BvarMetricType::GAUGE,
                           BvarMetricUnit::BYTES, "", "", BvarMetric::Labels(), true)
    INIT_INT64_BVAR_METRIC(local_file_reader_total, BvarMetricType::COUNTER,
                           BvarMetricUnit::FILESYSTEM, "", "", BvarMetric::Labels(), false)
    INIT_INT64_BVAR_METRIC(s3_file_reader_total, BvarMetricType::COUNTER,
                           BvarMetricUnit::FILESYSTEM, "", "", BvarMetric::Labels(), false)
    INIT_INT64_BVAR_METRIC(hdfs_file_reader_total, BvarMetricType::COUNTER,
                           BvarMetricUnit::FILESYSTEM, "", "", BvarMetric::Labels(), false)
    INIT_INT64_BVAR_METRIC(broker_file_reader_total, BvarMetricType::COUNTER,
                           BvarMetricUnit::FILESYSTEM, "", "", BvarMetric::Labels(), false)
    INIT_INT64_BVAR_METRIC(local_file_writer_total, BvarMetricType::COUNTER,
                           BvarMetricUnit::FILESYSTEM, "", "", BvarMetric::Labels(), false)
    INIT_INT64_BVAR_METRIC(s3_file_writer_total, BvarMetricType::COUNTER,
                           BvarMetricUnit::FILESYSTEM, "", "", BvarMetric::Labels(), false)
    INIT_INT64_BVAR_METRIC(file_created_total, BvarMetricType::COUNTER, BvarMetricUnit::FILESYSTEM,
                           "", "", BvarMetric::Labels(), false)
    INIT_INT64_BVAR_METRIC(s3_file_created_total, BvarMetricType::COUNTER,
                           BvarMetricUnit::FILESYSTEM, "", "", BvarMetric::Labels(), false)
    INIT_INT64_BVAR_METRIC(local_bytes_read_total, BvarMetricType::COUNTER,
                           BvarMetricUnit::FILESYSTEM, "", "", BvarMetric::Labels(), false)
    INIT_INT64_BVAR_METRIC(s3_bytes_read_total, BvarMetricType::COUNTER, BvarMetricUnit::FILESYSTEM,
                           "", "", BvarMetric::Labels(), false)
    INIT_INT64_BVAR_METRIC(local_bytes_written_total, BvarMetricType::COUNTER,
                           BvarMetricUnit::FILESYSTEM, "", "", BvarMetric::Labels(), false)
    INIT_INT64_BVAR_METRIC(s3_bytes_written_total, BvarMetricType::COUNTER,
                           BvarMetricUnit::FILESYSTEM, "", "", BvarMetric::Labels(), false)
    INIT_INT64_BVAR_METRIC(local_file_open_reading, BvarMetricType::GAUGE,
                           BvarMetricUnit::FILESYSTEM, "", "", BvarMetric::Labels(), false)
    INIT_INT64_BVAR_METRIC(s3_file_open_reading, BvarMetricType::GAUGE, BvarMetricUnit::FILESYSTEM,
                           "", "", BvarMetric::Labels(), false)
    INIT_INT64_BVAR_METRIC(hdfs_file_open_reading, BvarMetricType::GAUGE,
                           BvarMetricUnit::FILESYSTEM, "", "", BvarMetric::Labels(), false)
    INIT_INT64_BVAR_METRIC(broker_file_open_reading, BvarMetricType::GAUGE,
                           BvarMetricUnit::FILESYSTEM, "", "", BvarMetric::Labels(), false)
    INIT_INT64_BVAR_METRIC(local_file_open_writing, BvarMetricType::GAUGE,
                           BvarMetricUnit::FILESYSTEM, "", "", BvarMetric::Labels(), false)
    INIT_INT64_BVAR_METRIC(s3_file_open_writing, BvarMetricType::GAUGE, BvarMetricUnit::FILESYSTEM,
                           "", "", BvarMetric::Labels(), false)
    INIT_UINT64_BVAR_METRIC(query_cache_memory_total_byte, BvarMetricType::GAUGE,
                            BvarMetricUnit::BYTES, "", "", BvarMetric::Labels(), true)
    INIT_UINT64_BVAR_METRIC(query_cache_sql_total_count, BvarMetricType::GAUGE,
                            BvarMetricUnit::NOUNIT, "", "", BvarMetric::Labels(), true)
    INIT_UINT64_BVAR_METRIC(query_cache_partition_total_count, BvarMetricType::GAUGE,
                            BvarMetricUnit::NOUNIT, "", "", BvarMetric::Labels(), true)
    INIT_INT64_BVAR_METRIC(lru_cache_memory_bytes, BvarMetricType::GAUGE, BvarMetricUnit::BYTES, "",
                           "", BvarMetric::Labels(), true)
    INIT_UINT64_BVAR_METRIC(upload_total_byte, BvarMetricType::GAUGE, BvarMetricUnit::BYTES, "", "",
                            BvarMetric::Labels(), true)
    INIT_INT64_BVAR_METRIC(upload_rowset_count, BvarMetricType::COUNTER, BvarMetricUnit::ROWSETS,
                           "", "", BvarMetric::Labels(), false)
    INIT_INT64_BVAR_METRIC(upload_fail_count, BvarMetricType::COUNTER, BvarMetricUnit::ROWSETS, "",
                           "", BvarMetric::Labels(), false)

    ENTITY_REGISTER_METRICS(fragment_requests_total, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(fragment_request_duration_us, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(query_scan_bytes, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(query_scan_rows, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(push_requests_success_total, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(push_requests_fail_total, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(push_request_duration_us, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(push_request_write_bytes, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(push_request_write_rows, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(create_tablet_requests_total, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(create_tablet_requests_failed, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(drop_tablet_requests_total, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(report_all_tablets_requests_skip, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(schema_change_requests_total, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(schema_change_requests_failed, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(create_rollup_requests_total, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(create_rollup_requests_failed, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(storage_migrate_requests_total, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(storage_migrate_v2_requests_total, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(storage_migrate_v2_requests_failed, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(delete_requests_total, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(delete_requests_failed, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(clone_requests_total, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(clone_requests_failed, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(alter_inverted_index_requests_total, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(alter_inverted_index_requests_failed, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(finish_task_requests_total, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(finish_task_requests_failed, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(base_compaction_request_total, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(base_compaction_request_failed, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(cumulative_compaction_request_total, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(cumulative_compaction_request_failed, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(base_compaction_deltas_total, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(base_compaction_bytes_total, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(cumulative_compaction_deltas_total, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(cumulative_compaction_bytes_total, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(publish_task_request_total, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(publish_task_failed_total, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(segment_read_total, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(segment_row_total, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(stream_load_txn_begin_request_total, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(stream_load_txn_commit_request_total, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(stream_load_txn_rollback_request_total, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(stream_receive_bytes_total, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(stream_load_rows_total, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(load_rows, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(load_bytes, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(memtable_flush_total, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(memtable_flush_duration_us, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(memory_pool_bytes_total, BvarMetricType::GAUGE)
    ENTITY_REGISTER_METRICS(process_thread_num, BvarMetricType::GAUGE)
    ENTITY_REGISTER_METRICS(process_fd_num_used, BvarMetricType::GAUGE)
    ENTITY_REGISTER_METRICS(process_fd_num_limit_soft, BvarMetricType::GAUGE)
    ENTITY_REGISTER_METRICS(process_fd_num_limit_hard, BvarMetricType::GAUGE)
    ENTITY_REGISTER_METRICS(tablet_cumulative_max_compaction_score, BvarMetricType::GAUGE)
    ENTITY_REGISTER_METRICS(tablet_base_max_compaction_score, BvarMetricType::GAUGE)
    ENTITY_REGISTER_METRICS(all_rowsets_num, BvarMetricType::GAUGE)
    ENTITY_REGISTER_METRICS(all_segments_num, BvarMetricType::GAUGE)
    ENTITY_REGISTER_METRICS(compaction_used_permits, BvarMetricType::GAUGE)
    ENTITY_REGISTER_METRICS(compaction_waitting_permits, BvarMetricType::GAUGE)
    ENTITY_REGISTER_METRICS(local_file_reader_total, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(query_scan_bytes_per_second, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(s3_file_reader_total, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(hdfs_file_reader_total, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(broker_file_reader_total, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(local_file_writer_total, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(s3_file_writer_total, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(file_created_total, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(s3_file_created_total, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(local_bytes_read_total, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(s3_bytes_read_total, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(local_bytes_written_total, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(s3_bytes_written_total, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(local_file_open_reading, BvarMetricType::GAUGE)
    ENTITY_REGISTER_METRICS(s3_file_open_reading, BvarMetricType::GAUGE)
    ENTITY_REGISTER_METRICS(hdfs_file_open_reading, BvarMetricType::GAUGE)
    ENTITY_REGISTER_METRICS(broker_file_open_reading, BvarMetricType::GAUGE)
    ENTITY_REGISTER_METRICS(local_file_open_writing, BvarMetricType::GAUGE)
    ENTITY_REGISTER_METRICS(s3_file_open_writing, BvarMetricType::GAUGE)
    ENTITY_REGISTER_METRICS(query_cache_memory_total_byte, BvarMetricType::GAUGE)
    ENTITY_REGISTER_METRICS(query_cache_sql_total_count, BvarMetricType::GAUGE)
    ENTITY_REGISTER_METRICS(query_cache_partition_total_count, BvarMetricType::GAUGE)
    ENTITY_REGISTER_METRICS(lru_cache_memory_bytes, BvarMetricType::GAUGE)
    ENTITY_REGISTER_METRICS(upload_total_byte, BvarMetricType::GAUGE)
    ENTITY_REGISTER_METRICS(upload_rowset_count, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(upload_fail_count, BvarMetricType::COUNTER)
}

void DorisBvarMetrics::initialize(bool init_system_metrics,
                                  const std::set<std::string>& disk_devices,
                                  const std::vector<std::string>& network_interfaces) {
    if (init_system_metrics) {
        system_metrics_ = std::make_unique<SystemBvarMetrics>(disk_devices, network_interfaces);
    }
}

void DorisBvarMetrics::register_entity(BvarMetricEntity entity) {}

const std::string DorisBvarMetrics::to_prometheus() {
    std::lock_guard<bthread::Mutex> l(mutex_);
    std::stringstream ss;
    for (auto& entities : entities_map_) {
        if (entities.second.empty()) {
            continue;
        }
        int count = 0;
        for (auto& entity : entities.second) {
            if (!count) {
                ss << "# TYPE " << s_registry_name_ << "_" << entity->get_name() << " "
                   << entity->get_type() << "\n";
                count++;
            }
            ss << entity->to_prometheus(s_registry_name_);
        }
    }
    if (system_metrics_) ss << system_metrics_->to_prometheus(s_registry_name_);
    return ss.str();
}

const std::string DorisBvarMetrics::to_core_string(){
    std::stringstream ss;
    std::lock_guard<bthread::Mutex> l(mutex_);
    for (auto& entities : entities_map_) {
        if (entities.second.empty()) {
            continue;
        }
        for (auto& entity : entities.second) {
            ss << entity->to_core_string(s_registry_name_);
        }   
    }
    ss << system_metrics_->to_core_string(s_registry_name_);
    return ss.str();
}

const std::string DorisBvarMetrics::to_json(bool with_tablet_metrics) {
    rj::Document doc {rj::kArrayType};
    rj::Document::AllocatorType& allocator = doc.GetAllocator();
    std::lock_guard<bthread::Mutex> l(mutex_);
    for (const auto& entities : entities_map_) {
        for(const auto& entity : entities.second) {
            // if (entity.first->_type == MetricEntityType::kTablet && !with_tablet_metrics) {
            //     continue;
            // }
            std::lock_guard<bthread::Mutex> l(entity->mutex_);
            //entity.first->trigger_hook_unlocked(false);
            for (const auto& metric : entity->metrics_) {
                rj::Value metric_obj(rj::kObjectType);
                // tags
                rj::Value tag_obj(rj::kObjectType);
                tag_obj.AddMember("metric", rj::Value(metric.second->name_.c_str(), allocator),
                                allocator);
                // MetricPrototype's labels
                for (auto& label : metric.second->labels_) {
                    tag_obj.AddMember(rj::Value(label.first.c_str(), allocator),
                                    rj::Value(label.second.c_str(), allocator), allocator);
                }
                // MetricEntity's labels
                // for (auto& label : entity->labels_) {
                //     tag_obj.AddMember(rj::Value(label.first.c_str(), allocator),
                //                     rj::Value(label.second.c_str(), allocator), allocator);
                // }
                metric_obj.AddMember("tags", tag_obj, allocator);
                // unit
                rj::Value unit_val(unit_name(metric.second->unit_), allocator);
                metric_obj.AddMember("unit", unit_val, allocator);
                // value
                metric_obj.AddMember("value", metric.second->to_json_value(allocator), allocator);
                doc.PushBack(metric_obj, allocator);
            }
        }
    }
    system_metrics_->to_json(doc, with_tablet_metrics);
    rj::StringBuffer strBuf;
    rj::Writer<rj::StringBuffer> writer(strBuf);
    doc.Accept(writer);
    return strBuf.GetString();
}

} // namespace doris