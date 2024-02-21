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

#define REGISTER_INT64_BVAR_METRIC(entity, name, type, unit, description, group_name, labels,     \
                                   core)                                                          \
    name = std::make_shared<BvarAdderMetric<int64_t>>(type, unit, #name, description, group_name, \
                                                      labels, core);                              \
    entity->register_metric(#name, *name);

#define REGISTER_UINT64_BVAR_METRIC(entity, name, type, unit, description, group_name, labels,     \
                                    core)                                                          \
    name = std::make_shared<BvarAdderMetric<uint64_t>>(type, unit, #name, description, group_name, \
                                                       labels, core);                              \
    entity->register_metric(#name, *name);
// #define INIT_DOUBLE_BVAR_METRIC(name, type, unit, description, group_name, labels, core) \
//     name = std::make_shared<BvarAdderMetric<double>>(type, unit, #name, description, group_name, labels, core);

const std::string DorisBvarMetrics::s_registry_name_ = "doris_be";

DorisBvarMetrics::DorisBvarMetrics() : metric_registry_(s_registry_name_) {
    server_metric_entity_ = metric_registry_.register_entity("server");
    REGISTER_INT64_BVAR_METRIC(server_metric_entity_, fragment_requests_total,
                               BvarMetricType::COUNTER, BvarMetricUnit::REQUESTS,
                               "Total fragment requests received.", "", BvarMetric::Labels(), false)
    REGISTER_INT64_BVAR_METRIC(server_metric_entity_, fragment_request_duration_us,
                               BvarMetricType::COUNTER, BvarMetricUnit::MICROSECONDS, "", "",
                               BvarMetric::Labels(), false)
    REGISTER_INT64_BVAR_METRIC(server_metric_entity_, query_scan_bytes, BvarMetricType::COUNTER,
                               BvarMetricUnit::BYTES, "", "", BvarMetric::Labels(), false)
    REGISTER_INT64_BVAR_METRIC(server_metric_entity_, query_scan_rows, BvarMetricType::COUNTER,
                               BvarMetricUnit::ROWS, "", "", BvarMetric::Labels(), false)
    REGISTER_INT64_BVAR_METRIC(server_metric_entity_, push_requests_success_total,
                               BvarMetricType::COUNTER, BvarMetricUnit::REQUESTS, "",
                               "push_requests_total", BvarMetric::Labels({{"status", "SUCCESS"}}),
                               false);
    REGISTER_INT64_BVAR_METRIC(server_metric_entity_, push_requests_fail_total,
                               BvarMetricType::COUNTER, BvarMetricUnit::REQUESTS, "",
                               "push_requests_total", BvarMetric::Labels({{"status", "FAIL"}}),
                               false);
    REGISTER_INT64_BVAR_METRIC(server_metric_entity_, push_request_duration_us,
                               BvarMetricType::COUNTER, BvarMetricUnit::MICROSECONDS, "", "",
                               BvarMetric::Labels(), false)
    REGISTER_INT64_BVAR_METRIC(server_metric_entity_, push_request_write_bytes,
                               BvarMetricType::COUNTER, BvarMetricUnit::BYTES, "", "",
                               BvarMetric::Labels(), false)
    REGISTER_INT64_BVAR_METRIC(server_metric_entity_, push_request_write_rows,
                               BvarMetricType::COUNTER, BvarMetricUnit::ROWS, "", "",
                               BvarMetric::Labels(), false)
    REGISTER_INT64_BVAR_METRIC(
            server_metric_entity_, create_tablet_requests_total, BvarMetricType::COUNTER,
            BvarMetricUnit::REQUESTS, "", "engine_requests_total",
            BvarMetric::Labels({{"type", "create_tablet"}, {"status", "total"}}), false);
    REGISTER_INT64_BVAR_METRIC(
            server_metric_entity_, create_tablet_requests_failed, BvarMetricType::COUNTER,
            BvarMetricUnit::REQUESTS, "", "engine_requests_total",
            BvarMetric::Labels({{"type", "create_tablet"}, {"status", "failed"}}), false);
    REGISTER_INT64_BVAR_METRIC(
            server_metric_entity_, drop_tablet_requests_total, BvarMetricType::COUNTER,
            BvarMetricUnit::REQUESTS, "", "engine_requests_total",
            BvarMetric::Labels({{"type", "drop_tablet"}, {"status", "total"}}), false);
    REGISTER_INT64_BVAR_METRIC(
            server_metric_entity_, report_all_tablets_requests_skip, BvarMetricType::COUNTER,
            BvarMetricUnit::REQUESTS, "", "engine_requests_total",
            BvarMetric::Labels({{"type", "report_all_tablets"}, {"status", "skip"}}), false);
    REGISTER_INT64_BVAR_METRIC(
            server_metric_entity_, schema_change_requests_total, BvarMetricType::COUNTER,
            BvarMetricUnit::REQUESTS, "", "engine_requests_total",
            BvarMetric::Labels({{"type", "schema_change"}, {"status", "total"}}), false);
    REGISTER_INT64_BVAR_METRIC(
            server_metric_entity_, schema_change_requests_failed, BvarMetricType::COUNTER,
            BvarMetricUnit::REQUESTS, "", "engine_requests_total",
            BvarMetric::Labels({{"type", "schema_change"}, {"status", "failed"}}), false);
    REGISTER_INT64_BVAR_METRIC(
            server_metric_entity_, create_rollup_requests_total, BvarMetricType::COUNTER,
            BvarMetricUnit::REQUESTS, "", "engine_requests_total",
            BvarMetric::Labels({{"type", "create_rollup"}, {"status", "total"}}), false);
    REGISTER_INT64_BVAR_METRIC(
            server_metric_entity_, create_rollup_requests_failed, BvarMetricType::COUNTER,
            BvarMetricUnit::REQUESTS, "", "engine_requests_total",
            BvarMetric::Labels({{"type", "create_rollup"}, {"status", "failed"}}), false);
    REGISTER_INT64_BVAR_METRIC(
            server_metric_entity_, storage_migrate_requests_total, BvarMetricType::COUNTER,
            BvarMetricUnit::REQUESTS, "", "engine_requests_total",
            BvarMetric::Labels({{"type", "storage_migrate"}, {"status", "total"}}), false);
    REGISTER_INT64_BVAR_METRIC(
            server_metric_entity_, storage_migrate_v2_requests_total, BvarMetricType::COUNTER,
            BvarMetricUnit::REQUESTS, "", "engine_requests_total",
            BvarMetric::Labels({{"type", "storage_migrate_v2"}, {"status", "total"}}), false);
    REGISTER_INT64_BVAR_METRIC(
            server_metric_entity_, storage_migrate_v2_requests_failed, BvarMetricType::COUNTER,
            BvarMetricUnit::REQUESTS, "", "engine_requests_total",
            BvarMetric::Labels({{"type", "storage_migrate_v2"}, {"status", "failed"}}), false);
    REGISTER_INT64_BVAR_METRIC(
            server_metric_entity_, delete_requests_total, BvarMetricType::COUNTER,
            BvarMetricUnit::REQUESTS, "", "engine_requests_total",
            BvarMetric::Labels({{"type", "delete"}, {"status", "total"}}), false);
    REGISTER_INT64_BVAR_METRIC(
            server_metric_entity_, delete_requests_failed, BvarMetricType::COUNTER,
            BvarMetricUnit::REQUESTS, "", "engine_requests_total",
            BvarMetric::Labels({{"type", "delete"}, {"status", "failed"}}), false);
    REGISTER_INT64_BVAR_METRIC(server_metric_entity_, clone_requests_total, BvarMetricType::COUNTER,
                               BvarMetricUnit::REQUESTS, "", "engine_requests_total",
                               BvarMetric::Labels({{"type", "clone"}, {"status", "total"}}), false);
    REGISTER_INT64_BVAR_METRIC(
            server_metric_entity_, clone_requests_failed, BvarMetricType::COUNTER,
            BvarMetricUnit::REQUESTS, "", "engine_requests_total",
            BvarMetric::Labels({{"type", "clone"}, {"status", "failed"}}), false);
    REGISTER_INT64_BVAR_METRIC(
            server_metric_entity_, alter_inverted_index_requests_total, BvarMetricType::COUNTER,
            BvarMetricUnit::REQUESTS, "", "engine_requests_total",
            BvarMetric::Labels({{"type", "alter_inverted_index"}, {"status", "total"}}), false);
    REGISTER_INT64_BVAR_METRIC(
            server_metric_entity_, alter_inverted_index_requests_failed, BvarMetricType::COUNTER,
            BvarMetricUnit::REQUESTS, "", "engine_requests_total",
            BvarMetric::Labels({{"type", "alter_inverted_index"}, {"status", "failed"}}), false);
    REGISTER_INT64_BVAR_METRIC(
            server_metric_entity_, finish_task_requests_total, BvarMetricType::COUNTER,
            BvarMetricUnit::REQUESTS, "", "engine_requests_total",
            BvarMetric::Labels({{"type", "finish_task"}, {"status", "total"}}), false);
    REGISTER_INT64_BVAR_METRIC(
            server_metric_entity_, finish_task_requests_failed, BvarMetricType::COUNTER,
            BvarMetricUnit::REQUESTS, "", "engine_requests_total",
            BvarMetric::Labels({{"type", "finish_task"}, {"status", "failed"}}), false);
    REGISTER_INT64_BVAR_METRIC(
            server_metric_entity_, base_compaction_request_total, BvarMetricType::COUNTER,
            BvarMetricUnit::REQUESTS, "", "engine_requests_total",
            BvarMetric::Labels({{"type", "base_compaction"}, {"status", "total"}}), false);
    REGISTER_INT64_BVAR_METRIC(
            server_metric_entity_, base_compaction_request_failed, BvarMetricType::COUNTER,
            BvarMetricUnit::REQUESTS, "", "engine_requests_total",
            BvarMetric::Labels({{"type", "base_compaction"}, {"status", "failed"}}), false);
    REGISTER_INT64_BVAR_METRIC(
            server_metric_entity_, cumulative_compaction_request_total, BvarMetricType::COUNTER,
            BvarMetricUnit::REQUESTS, "", "engine_requests_total",
            BvarMetric::Labels({{"type", "cumulative_compaction"}, {"status", "total"}}), false);
    REGISTER_INT64_BVAR_METRIC(
            server_metric_entity_, cumulative_compaction_request_failed, BvarMetricType::COUNTER,
            BvarMetricUnit::REQUESTS, "", "engine_requests_total",
            BvarMetric::Labels({{"type", "cumulative_compaction"}, {"status", "failed"}}), false);
    REGISTER_INT64_BVAR_METRIC(server_metric_entity_, base_compaction_deltas_total,
                               BvarMetricType::COUNTER, BvarMetricUnit::ROWSETS, "",
                               "compaction_deltas_total", BvarMetric::Labels({{"type", "base"}}),
                               false);
    REGISTER_INT64_BVAR_METRIC(server_metric_entity_, base_compaction_bytes_total,
                               BvarMetricType::COUNTER, BvarMetricUnit::BYTES, "",
                               "compaction_bytes_total", BvarMetric::Labels({{"type", "base"}}),
                               false);
    REGISTER_INT64_BVAR_METRIC(server_metric_entity_, cumulative_compaction_deltas_total,
                               BvarMetricType::COUNTER, BvarMetricUnit::ROWSETS, "",
                               "compaction_deltas_total",
                               BvarMetric::Labels({{"type", "cumulative"}}), false);
    REGISTER_INT64_BVAR_METRIC(server_metric_entity_, cumulative_compaction_bytes_total,
                               BvarMetricType::COUNTER, BvarMetricUnit::BYTES, "",
                               "compaction_bytes_total",
                               BvarMetric::Labels({{"type", "cumulative"}}), false);
    REGISTER_INT64_BVAR_METRIC(
            server_metric_entity_, publish_task_request_total, BvarMetricType::COUNTER,
            BvarMetricUnit::REQUESTS, "", "engine_requests_total",
            BvarMetric::Labels({{"type", "publish"}, {"status", "total"}}), false);
    REGISTER_INT64_BVAR_METRIC(
            server_metric_entity_, publish_task_failed_total, BvarMetricType::COUNTER,
            BvarMetricUnit::REQUESTS, "", "engine_requests_total",
            BvarMetric::Labels({{"type", "publish"}, {"status", "failed"}}), false);
    REGISTER_INT64_BVAR_METRIC(server_metric_entity_, segment_read_total, BvarMetricType::COUNTER,
                               BvarMetricUnit::OPERATIONS,
                               "(segment_v2) total number of segments read", "segment_read",
                               BvarMetric::Labels({{"type", "segment_read_total"}}), false)
    REGISTER_INT64_BVAR_METRIC(
            server_metric_entity_, segment_row_total, BvarMetricType::COUNTER, BvarMetricUnit::ROWS,
            "(segment_v2) total number of rows in queried segments (before index pruning)",
            "segment_read", BvarMetric::Labels({{"type", "segment_row_total"}}), false)
    REGISTER_INT64_BVAR_METRIC(server_metric_entity_, stream_load_txn_begin_request_total,
                               BvarMetricType::COUNTER, BvarMetricUnit::OPERATIONS, "",
                               "stream_load_txn_request", BvarMetric::Labels({{"type", "begin"}}),
                               false)
    REGISTER_INT64_BVAR_METRIC(server_metric_entity_, stream_load_txn_commit_request_total,
                               BvarMetricType::COUNTER, BvarMetricUnit::OPERATIONS, "",
                               "stream_load_txn_request", BvarMetric::Labels({{"type", "commit"}}),
                               false)
    REGISTER_INT64_BVAR_METRIC(server_metric_entity_, stream_load_txn_rollback_request_total,
                               BvarMetricType::COUNTER, BvarMetricUnit::OPERATIONS, "",
                               "stream_load_txn_request",
                               BvarMetric::Labels({{"type", "rollback"}}), false)
    REGISTER_INT64_BVAR_METRIC(server_metric_entity_, stream_receive_bytes_total,
                               BvarMetricType::COUNTER, BvarMetricUnit::BYTES, "", "stream_load",
                               BvarMetric::Labels({{"type", "receive_bytes"}}), false)
    REGISTER_INT64_BVAR_METRIC(server_metric_entity_, stream_load_rows_total,
                               BvarMetricType::COUNTER, BvarMetricUnit::ROWS, "", "stream_load",
                               BvarMetric::Labels({{"type", "load_rows"}}), false)
    REGISTER_INT64_BVAR_METRIC(server_metric_entity_, load_rows, BvarMetricType::COUNTER,
                               BvarMetricUnit::ROWS, "", "", BvarMetric::Labels(), false)
    REGISTER_INT64_BVAR_METRIC(server_metric_entity_, load_bytes, BvarMetricType::COUNTER,
                               BvarMetricUnit::BYTES, "", "", BvarMetric::Labels(), false)
    REGISTER_INT64_BVAR_METRIC(server_metric_entity_, memtable_flush_total, BvarMetricType::COUNTER,
                               BvarMetricUnit::OPERATIONS, "", "", BvarMetric::Labels(), false)
    REGISTER_INT64_BVAR_METRIC(server_metric_entity_, memtable_flush_duration_us,
                               BvarMetricType::COUNTER, BvarMetricUnit::MICROSECONDS, "", "",
                               BvarMetric::Labels(), false)
    REGISTER_INT64_BVAR_METRIC(server_metric_entity_, memory_pool_bytes_total,
                               BvarMetricType::GAUGE, BvarMetricUnit::BYTES, "", "",
                               BvarMetric::Labels(), false)
    REGISTER_INT64_BVAR_METRIC(server_metric_entity_, process_thread_num, BvarMetricType::GAUGE,
                               BvarMetricUnit::NOUNIT, "", "", BvarMetric::Labels(), true)
    REGISTER_INT64_BVAR_METRIC(server_metric_entity_, process_fd_num_used, BvarMetricType::GAUGE,
                               BvarMetricUnit::NOUNIT, "", "", BvarMetric::Labels(), true)
    REGISTER_INT64_BVAR_METRIC(server_metric_entity_, process_fd_num_limit_soft,
                               BvarMetricType::GAUGE, BvarMetricUnit::NOUNIT, "", "",
                               BvarMetric::Labels(), false)
    REGISTER_INT64_BVAR_METRIC(server_metric_entity_, process_fd_num_limit_hard,
                               BvarMetricType::GAUGE, BvarMetricUnit::NOUNIT, "", "",
                               BvarMetric::Labels(), false)
    REGISTER_INT64_BVAR_METRIC(server_metric_entity_, tablet_cumulative_max_compaction_score,
                               BvarMetricType::GAUGE, BvarMetricUnit::NOUNIT, "", "",
                               BvarMetric::Labels(), false)
    REGISTER_INT64_BVAR_METRIC(server_metric_entity_, tablet_base_max_compaction_score,
                               BvarMetricType::GAUGE, BvarMetricUnit::NOUNIT, "", "",
                               BvarMetric::Labels(), false)
    REGISTER_INT64_BVAR_METRIC(server_metric_entity_, all_rowsets_num, BvarMetricType::GAUGE,
                               BvarMetricUnit::NOUNIT, "", "", BvarMetric::Labels(), false)
    REGISTER_INT64_BVAR_METRIC(server_metric_entity_, all_segments_num, BvarMetricType::GAUGE,
                               BvarMetricUnit::NOUNIT, "", "", BvarMetric::Labels(), false)
    REGISTER_INT64_BVAR_METRIC(server_metric_entity_, compaction_used_permits,
                               BvarMetricType::GAUGE, BvarMetricUnit::NOUNIT, "", "",
                               BvarMetric::Labels(), false)
    REGISTER_INT64_BVAR_METRIC(server_metric_entity_, compaction_waitting_permits,
                               BvarMetricType::GAUGE, BvarMetricUnit::NOUNIT, "", "",
                               BvarMetric::Labels(), false)
    // REGISTER_INT64_BVAR_METRIC(server_metric_entity_, tablet_version_num_distribution, BvarMetricType::HISTOGRAM, BvarMetricUnit::NOUNIT,
    //                        "", "", BvarMetric::Labels(), false)
    REGISTER_INT64_BVAR_METRIC(server_metric_entity_, query_scan_bytes_per_second,
                               BvarMetricType::GAUGE, BvarMetricUnit::BYTES, "", "",
                               BvarMetric::Labels(), true)
    REGISTER_INT64_BVAR_METRIC(server_metric_entity_, local_file_reader_total,
                               BvarMetricType::COUNTER, BvarMetricUnit::FILESYSTEM, "", "",
                               BvarMetric::Labels(), false)
    REGISTER_INT64_BVAR_METRIC(server_metric_entity_, s3_file_reader_total, BvarMetricType::COUNTER,
                               BvarMetricUnit::FILESYSTEM, "", "", BvarMetric::Labels(), false)
    REGISTER_INT64_BVAR_METRIC(server_metric_entity_, hdfs_file_reader_total,
                               BvarMetricType::COUNTER, BvarMetricUnit::FILESYSTEM, "", "",
                               BvarMetric::Labels(), false)
    REGISTER_INT64_BVAR_METRIC(server_metric_entity_, broker_file_reader_total,
                               BvarMetricType::COUNTER, BvarMetricUnit::FILESYSTEM, "", "",
                               BvarMetric::Labels(), false)
    REGISTER_INT64_BVAR_METRIC(server_metric_entity_, local_file_writer_total,
                               BvarMetricType::COUNTER, BvarMetricUnit::FILESYSTEM, "", "",
                               BvarMetric::Labels(), false)
    REGISTER_INT64_BVAR_METRIC(server_metric_entity_, s3_file_writer_total, BvarMetricType::COUNTER,
                               BvarMetricUnit::FILESYSTEM, "", "", BvarMetric::Labels(), false)
    REGISTER_INT64_BVAR_METRIC(server_metric_entity_, file_created_total, BvarMetricType::COUNTER,
                               BvarMetricUnit::FILESYSTEM, "", "", BvarMetric::Labels(), false)
    REGISTER_INT64_BVAR_METRIC(server_metric_entity_, s3_file_created_total,
                               BvarMetricType::COUNTER, BvarMetricUnit::FILESYSTEM, "", "",
                               BvarMetric::Labels(), false)
    REGISTER_INT64_BVAR_METRIC(server_metric_entity_, local_bytes_read_total,
                               BvarMetricType::COUNTER, BvarMetricUnit::FILESYSTEM, "", "",
                               BvarMetric::Labels(), false)
    REGISTER_INT64_BVAR_METRIC(server_metric_entity_, s3_bytes_read_total, BvarMetricType::COUNTER,
                               BvarMetricUnit::FILESYSTEM, "", "", BvarMetric::Labels(), false)
    REGISTER_INT64_BVAR_METRIC(server_metric_entity_, local_bytes_written_total,
                               BvarMetricType::COUNTER, BvarMetricUnit::FILESYSTEM, "", "",
                               BvarMetric::Labels(), false)
    REGISTER_INT64_BVAR_METRIC(server_metric_entity_, s3_bytes_written_total,
                               BvarMetricType::COUNTER, BvarMetricUnit::FILESYSTEM, "", "",
                               BvarMetric::Labels(), false)
    REGISTER_INT64_BVAR_METRIC(server_metric_entity_, local_file_open_reading,
                               BvarMetricType::GAUGE, BvarMetricUnit::FILESYSTEM, "", "",
                               BvarMetric::Labels(), false)
    REGISTER_INT64_BVAR_METRIC(server_metric_entity_, s3_file_open_reading, BvarMetricType::GAUGE,
                               BvarMetricUnit::FILESYSTEM, "", "", BvarMetric::Labels(), false)
    REGISTER_INT64_BVAR_METRIC(server_metric_entity_, hdfs_file_open_reading, BvarMetricType::GAUGE,
                               BvarMetricUnit::FILESYSTEM, "", "", BvarMetric::Labels(), false)
    REGISTER_INT64_BVAR_METRIC(server_metric_entity_, broker_file_open_reading,
                               BvarMetricType::GAUGE, BvarMetricUnit::FILESYSTEM, "", "",
                               BvarMetric::Labels(), false)
    REGISTER_INT64_BVAR_METRIC(server_metric_entity_, local_file_open_writing,
                               BvarMetricType::GAUGE, BvarMetricUnit::FILESYSTEM, "", "",
                               BvarMetric::Labels(), false)
    REGISTER_INT64_BVAR_METRIC(server_metric_entity_, s3_file_open_writing, BvarMetricType::GAUGE,
                               BvarMetricUnit::FILESYSTEM, "", "", BvarMetric::Labels(), false)

    // server_metric_entity_->register_hook(s_hook_name_, std::bind(&DorisMetrics::update, this));

    REGISTER_UINT64_BVAR_METRIC(server_metric_entity_, query_cache_memory_total_byte,
                                BvarMetricType::GAUGE, BvarMetricUnit::BYTES, "", "",
                                BvarMetric::Labels(), true)
    REGISTER_UINT64_BVAR_METRIC(server_metric_entity_, query_cache_sql_total_count,
                                BvarMetricType::GAUGE, BvarMetricUnit::NOUNIT, "", "",
                                BvarMetric::Labels(), true)
    REGISTER_UINT64_BVAR_METRIC(server_metric_entity_, query_cache_partition_total_count,
                                BvarMetricType::GAUGE, BvarMetricUnit::NOUNIT, "", "",
                                BvarMetric::Labels(), true)
    REGISTER_INT64_BVAR_METRIC(server_metric_entity_, lru_cache_memory_bytes, BvarMetricType::GAUGE,
                               BvarMetricUnit::BYTES, "", "", BvarMetric::Labels(), true)
    REGISTER_UINT64_BVAR_METRIC(server_metric_entity_, upload_total_byte, BvarMetricType::GAUGE,
                                BvarMetricUnit::BYTES, "", "", BvarMetric::Labels(), true)
    REGISTER_INT64_BVAR_METRIC(server_metric_entity_, upload_rowset_count, BvarMetricType::COUNTER,
                               BvarMetricUnit::ROWSETS, "", "", BvarMetric::Labels(), false)
    REGISTER_INT64_BVAR_METRIC(server_metric_entity_, upload_fail_count, BvarMetricType::COUNTER,
                               BvarMetricUnit::ROWSETS, "", "", BvarMetric::Labels(), false)
}

void DorisBvarMetrics::initialize(bool init_system_metrics,
                                  const std::set<std::string>& disk_devices,
                                  const std::vector<std::string>& network_interfaces) {
    if (init_system_metrics) {
        system_metrics_.reset(
                new SystemBvarMetrics(&metric_registry_, disk_devices, network_interfaces));
    }
}

// void BvarDorisMetrics::update() {
//     update_process_thread_num();
//     update_process_fd_num();
// }

} // namespace doris