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

#include "util/doris_bvar_metrics.h"

// IWYU pragma: no_include <bthread/errno.h>
#include <glog/logging.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno> // IWYU pragma: keep
#include <cinttypes>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <ostream>

#include "common/status.h"
#include "io/fs/local_file_system.h"
#include "util/system_bvar_metrics.h"

namespace doris {

const std::string DorisBvarMetrics::s_registry_name_ = "doris_be";
const std::string DorisBvarMetrics::s_hook_name_ = "doris_be";

DorisBvarMetrics::DorisBvarMetrics() : metric_registry_(s_registry_name_) {
    server_metric_entity_ = metric_registry_.register_entity("server");
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_fragment_requests_total)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_fragment_request_duration_us)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_query_scan_bytes)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_query_scan_rows)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_push_requests_success_total)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_push_requests_fail_total)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_push_request_duration_us)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_push_request_write_bytes)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_push_request_write_rows)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_create_tablet_requests_total)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_create_tablet_requests_failed)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_drop_tablet_requests_total)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_report_all_tablets_requests_skip)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_schema_change_requests_total)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_schema_change_requests_failed)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_create_rollup_requests_total)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_create_rollup_requests_failed)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_storage_migrate_requests_total)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_storage_migrate_v2_requests_total)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_storage_migrate_v2_requests_failed)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_delete_requests_total)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_delete_requests_failed)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_clone_requests_total)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_clone_requests_failed)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_alter_inverted_index_requests_total)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_alter_inverted_index_requests_failed)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_finish_task_requests_total)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_finish_task_requests_failed)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_base_compaction_request_total)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_base_compaction_request_failed)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_cumulative_compaction_request_total)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_cumulative_compaction_request_failed)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_base_compaction_deltas_total)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_base_compaction_bytes_total)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_cumulative_compaction_deltas_total)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_cumulative_compaction_bytes_total)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_publish_task_request_total)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_publish_task_failed_total)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_segment_read_total)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_segment_row_total)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_stream_load_txn_begin_request_total)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_stream_load_txn_commit_request_total)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_stream_load_txn_rollback_request_total)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_stream_receive_bytes_total)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_stream_load_rows_total)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_load_rows)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_load_bytes)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_memtable_flush_total)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_memtable_flush_duration_us)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_memory_pool_bytes_total)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_process_thread_num)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_process_fd_num_used)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_process_fd_num_limit_soft)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_process_fd_num_limit_hard)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_tablet_cumulative_max_compaction_score)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_tablet_base_max_compaction_score)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_all_rowsets_num)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_all_segments_num)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_compaction_used_permits)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_compaction_waitting_permits)

    // REGISTER_BVAR_METRIC(server_metric_entity_,  g_adder_tablet_version_num_distribution)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_query_scan_bytes_per_second)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_local_file_reader_total)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_s3_file_reader_total)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_hdfs_file_reader_total)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_broker_file_reader_total)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_local_file_writer_total)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_s3_file_writer_total)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_file_created_total)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_s3_file_created_total)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_local_bytes_read_total)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_s3_bytes_read_total)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_local_bytes_written_total)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_s3_bytes_written_total)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_local_file_open_reading)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_s3_file_open_reading)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_hdfs_file_open_reading)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_broker_file_open_reading)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_local_file_open_writing)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_s3_file_open_writing)
    server_metric_entity_->register_hook(s_hook_name_, std::bind(&DorisBvarMetrics::update, this));
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_query_cache_memory_total_byte)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_query_cache_sql_total_count)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_query_cache_partition_total_count)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_lru_cache_memory_bytes)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_upload_total_byte)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_upload_rowset_count)
    REGISTER_BVAR_METRIC(server_metric_entity_, g_adder_upload_fail_count)

    // g_adder_upload_fail_count.set_value(1000); for test
}

void DorisBvarMetrics::initialize(bool init_system_metrics,
                                  const std::set<std::string>& disk_devices,
                                  const std::vector<std::string>& network_interfaces) {
    if (init_system_metrics) {
        system_metrics_.reset(
                new SystemBvarMetrics(&metric_registry_, disk_devices, network_interfaces));
    }
}

void DorisBvarMetrics::update() {
    update_process_thread_num();
    update_process_fd_num();
}

// get num of thread of doris_be process
// from /proc/self/task
void DorisBvarMetrics::update_process_thread_num() {
    std::error_code ec;
    std::filesystem::directory_iterator dict_iter("/proc/self/task/", ec);
    if (ec) {
        LOG(WARNING) << "failed to count thread num: " << ec.message();
        g_adder_process_fd_num_used.set_value(0);
        return;
    }
    int64_t count =
            std::count_if(dict_iter, std::filesystem::end(dict_iter), [](const auto& entry) {
                std::error_code error_code;
                return entry.is_regular_file(error_code) && !error_code;
            });

    g_adder_process_thread_num.set_value(count);
}

// get num of file descriptor of doris_be process
void DorisBvarMetrics::update_process_fd_num() {
    // fd used
    std::error_code ec;
    std::filesystem::directory_iterator dict_iter("/proc/self/fd/", ec);
    if (ec) {
        LOG(WARNING) << "failed to count fd: " << ec.message();
        g_adder_process_fd_num_used.set_value(0);
        return;
    }
    int64_t count =
            std::count_if(dict_iter, std::filesystem::end(dict_iter), [](const auto& entry) {
                std::error_code error_code;
                return entry.is_regular_file(error_code) && !error_code;
            });

    g_adder_process_fd_num_used.set_value(count);

    // fd limits
    FILE* fp = fopen("/proc/self/limits", "r");
    if (fp == nullptr) {
        char buf[64];
        LOG(WARNING) << "open /proc/self/limits failed, errno=" << errno
                     << ", message=" << strerror_r(errno, buf, 64);
        return;
    }

    // /proc/self/limits
    // Max open files            65536                65536                files
    int64_t values[2];
    size_t line_buf_size = 0;
    char* line_ptr = nullptr;
    while (getline(&line_ptr, &line_buf_size, fp) > 0) {
        memset(values, 0, sizeof(values));
        int num = sscanf(line_ptr, "Max open files %" PRId64 " %" PRId64, &values[0], &values[1]);
        if (num == 2) {
            g_adder_process_fd_num_limit_soft.set_value(values[0]);
            g_adder_process_fd_num_limit_hard.set_value(values[1]);
            break;
        }
    }

    if (line_ptr != nullptr) {
        free(line_ptr);
    }

    if (ferror(fp) != 0) {
        char buf[64];
        LOG(WARNING) << "getline failed, errno=" << errno
                     << ", message=" << strerror_r(errno, buf, 64);
    }
    fclose(fp);
}

INIT_INT64_G_ADDER_BVAR_METRIC(fragment_requests_total, BvarMetricType::COUNTER,
                               BvarMetricUnit::REQUESTS, "Total fragment requests received.", "",
                               BvarMetric::Labels(), false)
INIT_INT64_G_ADDER_BVAR_METRIC(fragment_request_duration_us, BvarMetricType::COUNTER,
                               BvarMetricUnit::MICROSECONDS, "", "", BvarMetric::Labels(), false)
INIT_INT64_G_ADDER_BVAR_METRIC(query_scan_bytes, BvarMetricType::COUNTER, BvarMetricUnit::BYTES, "",
                               "", BvarMetric::Labels(), false)
INIT_INT64_G_ADDER_BVAR_METRIC(query_scan_rows, BvarMetricType::COUNTER, BvarMetricUnit::ROWS, "",
                               "", BvarMetric::Labels(), false)
INIT_INT64_G_ADDER_BVAR_METRIC(push_requests_success_total, BvarMetricType::COUNTER,
                               BvarMetricUnit::REQUESTS, "", "push_requests_total",
                               BvarMetric::Labels({{"status", "SUCCESS"}}), false);
INIT_INT64_G_ADDER_BVAR_METRIC(push_requests_fail_total, BvarMetricType::COUNTER,
                               BvarMetricUnit::REQUESTS, "", "push_requests_total",
                               BvarMetric::Labels({{"status", "FAIL"}}), false);
INIT_INT64_G_ADDER_BVAR_METRIC(push_request_duration_us, BvarMetricType::COUNTER,
                               BvarMetricUnit::MICROSECONDS, "", "", BvarMetric::Labels(), false)
INIT_INT64_G_ADDER_BVAR_METRIC(push_request_write_bytes, BvarMetricType::COUNTER,
                               BvarMetricUnit::BYTES, "", "", BvarMetric::Labels(), false)
INIT_INT64_G_ADDER_BVAR_METRIC(push_request_write_rows, BvarMetricType::COUNTER,
                               BvarMetricUnit::ROWS, "", "", BvarMetric::Labels(), false)
INIT_INT64_G_ADDER_BVAR_METRIC(create_tablet_requests_total, BvarMetricType::COUNTER,
                               BvarMetricUnit::REQUESTS, "", "engine_requests_total",
                               BvarMetric::Labels({{"type", "create_tablet"}, {"status", "total"}}),
                               false);
INIT_INT64_G_ADDER_BVAR_METRIC(create_tablet_requests_failed, BvarMetricType::COUNTER,
                               BvarMetricUnit::REQUESTS, "", "engine_requests_total",
                               BvarMetric::Labels({{"type", "create_tablet"},
                                                   {"status", "failed"}}),
                               false);
INIT_INT64_G_ADDER_BVAR_METRIC(drop_tablet_requests_total, BvarMetricType::COUNTER,
                               BvarMetricUnit::REQUESTS, "", "engine_requests_total",
                               BvarMetric::Labels({{"type", "drop_tablet"}, {"status", "total"}}),
                               false);
INIT_INT64_G_ADDER_BVAR_METRIC(report_all_tablets_requests_skip, BvarMetricType::COUNTER,
                               BvarMetricUnit::REQUESTS, "", "engine_requests_total",
                               BvarMetric::Labels({{"type", "report_all_tablets"},
                                                   {"status", "skip"}}),
                               false);
INIT_INT64_G_ADDER_BVAR_METRIC(schema_change_requests_total, BvarMetricType::COUNTER,
                               BvarMetricUnit::REQUESTS, "", "engine_requests_total",
                               BvarMetric::Labels({{"type", "schema_change"}, {"status", "total"}}),
                               false);
INIT_INT64_G_ADDER_BVAR_METRIC(schema_change_requests_failed, BvarMetricType::COUNTER,
                               BvarMetricUnit::REQUESTS, "", "engine_requests_total",
                               BvarMetric::Labels({{"type", "schema_change"},
                                                   {"status", "failed"}}),
                               false);
INIT_INT64_G_ADDER_BVAR_METRIC(create_rollup_requests_total, BvarMetricType::COUNTER,
                               BvarMetricUnit::REQUESTS, "", "engine_requests_total",
                               BvarMetric::Labels({{"type", "create_rollup"}, {"status", "total"}}),
                               false);
INIT_INT64_G_ADDER_BVAR_METRIC(create_rollup_requests_failed, BvarMetricType::COUNTER,
                               BvarMetricUnit::REQUESTS, "", "engine_requests_total",
                               BvarMetric::Labels({{"type", "create_rollup"},
                                                   {"status", "failed"}}),
                               false);
INIT_INT64_G_ADDER_BVAR_METRIC(storage_migrate_requests_total, BvarMetricType::COUNTER,
                               BvarMetricUnit::REQUESTS, "", "engine_requests_total",
                               BvarMetric::Labels({{"type", "storage_migrate"},
                                                   {"status", "total"}}),
                               false);
INIT_INT64_G_ADDER_BVAR_METRIC(storage_migrate_v2_requests_total, BvarMetricType::COUNTER,
                               BvarMetricUnit::REQUESTS, "", "engine_requests_total",
                               BvarMetric::Labels({{"type", "storage_migrate_v2"},
                                                   {"status", "total"}}),
                               false);
INIT_INT64_G_ADDER_BVAR_METRIC(storage_migrate_v2_requests_failed, BvarMetricType::COUNTER,
                               BvarMetricUnit::REQUESTS, "", "engine_requests_total",
                               BvarMetric::Labels({{"type", "storage_migrate_v2"},
                                                   {"status", "failed"}}),
                               false);
INIT_INT64_G_ADDER_BVAR_METRIC(delete_requests_total, BvarMetricType::COUNTER,
                               BvarMetricUnit::REQUESTS, "", "engine_requests_total",
                               BvarMetric::Labels({{"type", "delete"}, {"status", "total"}}),
                               false);
INIT_INT64_G_ADDER_BVAR_METRIC(delete_requests_failed, BvarMetricType::COUNTER,
                               BvarMetricUnit::REQUESTS, "", "engine_requests_total",
                               BvarMetric::Labels({{"type", "delete"}, {"status", "failed"}}),
                               false);
INIT_INT64_G_ADDER_BVAR_METRIC(clone_requests_total, BvarMetricType::COUNTER,
                               BvarMetricUnit::REQUESTS, "", "engine_requests_total",
                               BvarMetric::Labels({{"type", "clone"}, {"status", "total"}}), false);
INIT_INT64_G_ADDER_BVAR_METRIC(clone_requests_failed, BvarMetricType::COUNTER,
                               BvarMetricUnit::REQUESTS, "", "engine_requests_total",
                               BvarMetric::Labels({{"type", "clone"}, {"status", "failed"}}),
                               false);
INIT_INT64_G_ADDER_BVAR_METRIC(alter_inverted_index_requests_total, BvarMetricType::COUNTER,
                               BvarMetricUnit::REQUESTS, "", "engine_requests_total",
                               BvarMetric::Labels({{"type", "alter_inverted_index"},
                                                   {"status", "total"}}),
                               false);
INIT_INT64_G_ADDER_BVAR_METRIC(alter_inverted_index_requests_failed, BvarMetricType::COUNTER,
                               BvarMetricUnit::REQUESTS, "", "engine_requests_total",
                               BvarMetric::Labels({{"type", "alter_inverted_index"},
                                                   {"status", "failed"}}),
                               false);
INIT_INT64_G_ADDER_BVAR_METRIC(finish_task_requests_total, BvarMetricType::COUNTER,
                               BvarMetricUnit::REQUESTS, "", "engine_requests_total",
                               BvarMetric::Labels({{"type", "finish_task"}, {"status", "total"}}),
                               false);
INIT_INT64_G_ADDER_BVAR_METRIC(finish_task_requests_failed, BvarMetricType::COUNTER,
                               BvarMetricUnit::REQUESTS, "", "engine_requests_total",
                               BvarMetric::Labels({{"type", "finish_task"}, {"status", "failed"}}),
                               false);
INIT_INT64_G_ADDER_BVAR_METRIC(base_compaction_request_total, BvarMetricType::COUNTER,
                               BvarMetricUnit::REQUESTS, "", "engine_requests_total",
                               BvarMetric::Labels({{"type", "base_compaction"},
                                                   {"status", "total"}}),
                               false);
INIT_INT64_G_ADDER_BVAR_METRIC(base_compaction_request_failed, BvarMetricType::COUNTER,
                               BvarMetricUnit::REQUESTS, "", "engine_requests_total",
                               BvarMetric::Labels({{"type", "base_compaction"},
                                                   {"status", "failed"}}),
                               false);
INIT_INT64_G_ADDER_BVAR_METRIC(cumulative_compaction_request_total, BvarMetricType::COUNTER,
                               BvarMetricUnit::REQUESTS, "", "engine_requests_total",
                               BvarMetric::Labels({{"type", "cumulative_compaction"},
                                                   {"status", "total"}}),
                               false);
INIT_INT64_G_ADDER_BVAR_METRIC(cumulative_compaction_request_failed, BvarMetricType::COUNTER,
                               BvarMetricUnit::REQUESTS, "", "engine_requests_total",
                               BvarMetric::Labels({{"type", "cumulative_compaction"},
                                                   {"status", "failed"}}),
                               false);
INIT_INT64_G_ADDER_BVAR_METRIC(base_compaction_deltas_total, BvarMetricType::COUNTER,
                               BvarMetricUnit::ROWSETS, "", "compaction_deltas_total",
                               BvarMetric::Labels({{"type", "base"}}), false);
INIT_INT64_G_ADDER_BVAR_METRIC(base_compaction_bytes_total, BvarMetricType::COUNTER,
                               BvarMetricUnit::BYTES, "", "compaction_bytes_total",
                               BvarMetric::Labels({{"type", "base"}}), false);
INIT_INT64_G_ADDER_BVAR_METRIC(cumulative_compaction_deltas_total, BvarMetricType::COUNTER,
                               BvarMetricUnit::ROWSETS, "", "compaction_deltas_total",
                               BvarMetric::Labels({{"type", "cumulative"}}), false);
INIT_INT64_G_ADDER_BVAR_METRIC(cumulative_compaction_bytes_total, BvarMetricType::COUNTER,
                               BvarMetricUnit::BYTES, "", "compaction_bytes_total",
                               BvarMetric::Labels({{"type", "cumulative"}}), false);
INIT_INT64_G_ADDER_BVAR_METRIC(full_compaction_deltas_total, BvarMetricType::COUNTER,
                               BvarMetricUnit::ROWSETS, "", "compaction_deltas_total",
                               BvarMetric::Labels({{"type", "base"}}), false);
INIT_INT64_G_ADDER_BVAR_METRIC(full_compaction_bytes_total, BvarMetricType::COUNTER,
                               BvarMetricUnit::BYTES, "", "compaction_bytes_total",
                               BvarMetric::Labels({{"type", "base"}}), false);

INIT_INT64_G_ADDER_BVAR_METRIC(publish_task_request_total, BvarMetricType::COUNTER,
                               BvarMetricUnit::REQUESTS, "", "engine_requests_total",
                               BvarMetric::Labels({{"type", "publish"}, {"status", "total"}}),
                               false);
INIT_INT64_G_ADDER_BVAR_METRIC(publish_task_failed_total, BvarMetricType::COUNTER,
                               BvarMetricUnit::REQUESTS, "", "engine_requests_total",
                               BvarMetric::Labels({{"type", "publish"}, {"status", "failed"}}),
                               false);
INIT_INT64_G_ADDER_BVAR_METRIC(segment_read_total, BvarMetricType::COUNTER,
                               BvarMetricUnit::OPERATIONS,
                               "(segment_v2) total number of segments read", "segment_read",
                               BvarMetric::Labels({{"type", "segment_read_total"}}), false)
INIT_INT64_G_ADDER_BVAR_METRIC(
        segment_row_total, BvarMetricType::COUNTER, BvarMetricUnit::ROWS,
        "(segment_v2) total number of rows in queried segments (before index pruning)",
        "segment_read", BvarMetric::Labels({{"type", "segment_row_total"}}), false)
INIT_INT64_G_ADDER_BVAR_METRIC(stream_load_txn_begin_request_total, BvarMetricType::COUNTER,
                               BvarMetricUnit::OPERATIONS, "", "stream_load_txn_request",
                               BvarMetric::Labels({{"type", "begin"}}), false)
INIT_INT64_G_ADDER_BVAR_METRIC(stream_load_txn_commit_request_total, BvarMetricType::COUNTER,
                               BvarMetricUnit::OPERATIONS, "", "stream_load_txn_request",
                               BvarMetric::Labels({{"type", "commit"}}), false)
INIT_INT64_G_ADDER_BVAR_METRIC(stream_load_txn_rollback_request_total, BvarMetricType::COUNTER,
                               BvarMetricUnit::OPERATIONS, "", "stream_load_txn_request",
                               BvarMetric::Labels({{"type", "rollback"}}), false)
INIT_INT64_G_ADDER_BVAR_METRIC(stream_receive_bytes_total, BvarMetricType::COUNTER,
                               BvarMetricUnit::BYTES, "", "stream_load",
                               BvarMetric::Labels({{"type", "receive_bytes"}}), false)
INIT_INT64_G_ADDER_BVAR_METRIC(stream_load_rows_total, BvarMetricType::COUNTER,
                               BvarMetricUnit::ROWS, "", "stream_load",
                               BvarMetric::Labels({{"type", "load_rows"}}), false)
INIT_INT64_G_ADDER_BVAR_METRIC(load_rows, BvarMetricType::COUNTER, BvarMetricUnit::ROWS, "", "",
                               BvarMetric::Labels(), false)
INIT_INT64_G_ADDER_BVAR_METRIC(load_bytes, BvarMetricType::COUNTER, BvarMetricUnit::BYTES, "", "",
                               BvarMetric::Labels(), false)
INIT_INT64_G_ADDER_BVAR_METRIC(memtable_flush_total, BvarMetricType::COUNTER,
                               BvarMetricUnit::OPERATIONS, "", "", BvarMetric::Labels(), false)
INIT_INT64_G_ADDER_BVAR_METRIC(memtable_flush_duration_us, BvarMetricType::COUNTER,
                               BvarMetricUnit::MICROSECONDS, "", "", BvarMetric::Labels(), false)
INIT_INT64_G_ADDER_BVAR_METRIC(memory_pool_bytes_total, BvarMetricType::GAUGE,
                               BvarMetricUnit::BYTES, "", "", BvarMetric::Labels(), false)
INIT_INT64_G_ADDER_BVAR_METRIC(process_thread_num, BvarMetricType::GAUGE, BvarMetricUnit::NOUNIT,
                               "", "", BvarMetric::Labels(), true)
INIT_INT64_G_ADDER_BVAR_METRIC(process_fd_num_used, BvarMetricType::GAUGE, BvarMetricUnit::NOUNIT,
                               "", "", BvarMetric::Labels(), true)
INIT_INT64_G_ADDER_BVAR_METRIC(process_fd_num_limit_soft, BvarMetricType::GAUGE,
                               BvarMetricUnit::NOUNIT, "", "", BvarMetric::Labels(), false)
INIT_INT64_G_ADDER_BVAR_METRIC(process_fd_num_limit_hard, BvarMetricType::GAUGE,
                               BvarMetricUnit::NOUNIT, "", "", BvarMetric::Labels(), false)
INIT_INT64_G_ADDER_BVAR_METRIC(tablet_cumulative_max_compaction_score, BvarMetricType::GAUGE,
                               BvarMetricUnit::NOUNIT, "", "", BvarMetric::Labels(), false)
INIT_INT64_G_ADDER_BVAR_METRIC(tablet_base_max_compaction_score, BvarMetricType::GAUGE,
                               BvarMetricUnit::NOUNIT, "", "", BvarMetric::Labels(), false)
INIT_INT64_G_ADDER_BVAR_METRIC(all_rowsets_num, BvarMetricType::GAUGE, BvarMetricUnit::NOUNIT, "",
                               "", BvarMetric::Labels(), false)
INIT_INT64_G_ADDER_BVAR_METRIC(all_segments_num, BvarMetricType::GAUGE, BvarMetricUnit::NOUNIT, "",
                               "", BvarMetric::Labels(), false)
INIT_INT64_G_ADDER_BVAR_METRIC(compaction_used_permits, BvarMetricType::GAUGE,
                               BvarMetricUnit::NOUNIT, "", "", BvarMetric::Labels(), false)
INIT_INT64_G_ADDER_BVAR_METRIC(compaction_waitting_permits, BvarMetricType::GAUGE,
                               BvarMetricUnit::NOUNIT, "", "", BvarMetric::Labels(), false)
// INIT_INT64_G_ADDER_BVAR_METRIC(tablet_version_num_distribution, BvarMetricType::HISTOGRAM, BvarMetricUnit::NOUNIT,
//                        "", "", BvarMetric::Labels(), false)
INIT_INT64_G_ADDER_BVAR_METRIC(query_scan_bytes_per_second, BvarMetricType::GAUGE,
                               BvarMetricUnit::BYTES, "", "", BvarMetric::Labels(), true)
INIT_INT64_G_ADDER_BVAR_METRIC(local_file_reader_total, BvarMetricType::COUNTER,
                               BvarMetricUnit::FILESYSTEM, "", "", BvarMetric::Labels(), false)
INIT_INT64_G_ADDER_BVAR_METRIC(s3_file_reader_total, BvarMetricType::COUNTER,
                               BvarMetricUnit::FILESYSTEM, "", "", BvarMetric::Labels(), false)
INIT_INT64_G_ADDER_BVAR_METRIC(hdfs_file_reader_total, BvarMetricType::COUNTER,
                               BvarMetricUnit::FILESYSTEM, "", "", BvarMetric::Labels(), false)
INIT_INT64_G_ADDER_BVAR_METRIC(broker_file_reader_total, BvarMetricType::COUNTER,
                               BvarMetricUnit::FILESYSTEM, "", "", BvarMetric::Labels(), false)
INIT_INT64_G_ADDER_BVAR_METRIC(local_file_writer_total, BvarMetricType::COUNTER,
                               BvarMetricUnit::FILESYSTEM, "", "", BvarMetric::Labels(), false)
INIT_INT64_G_ADDER_BVAR_METRIC(s3_file_writer_total, BvarMetricType::COUNTER,
                               BvarMetricUnit::FILESYSTEM, "", "", BvarMetric::Labels(), false)
INIT_INT64_G_ADDER_BVAR_METRIC(file_created_total, BvarMetricType::COUNTER,
                               BvarMetricUnit::FILESYSTEM, "", "", BvarMetric::Labels(), false)
INIT_INT64_G_ADDER_BVAR_METRIC(s3_file_created_total, BvarMetricType::COUNTER,
                               BvarMetricUnit::FILESYSTEM, "", "", BvarMetric::Labels(), false)
INIT_INT64_G_ADDER_BVAR_METRIC(local_bytes_read_total, BvarMetricType::COUNTER,
                               BvarMetricUnit::FILESYSTEM, "", "", BvarMetric::Labels(), false)
INIT_INT64_G_ADDER_BVAR_METRIC(s3_bytes_read_total, BvarMetricType::COUNTER,
                               BvarMetricUnit::FILESYSTEM, "", "", BvarMetric::Labels(), false)
INIT_INT64_G_ADDER_BVAR_METRIC(local_bytes_written_total, BvarMetricType::COUNTER,
                               BvarMetricUnit::FILESYSTEM, "", "", BvarMetric::Labels(), false)
INIT_INT64_G_ADDER_BVAR_METRIC(s3_bytes_written_total, BvarMetricType::COUNTER,
                               BvarMetricUnit::FILESYSTEM, "", "", BvarMetric::Labels(), false)
INIT_INT64_G_ADDER_BVAR_METRIC(local_file_open_reading, BvarMetricType::GAUGE,
                               BvarMetricUnit::FILESYSTEM, "", "", BvarMetric::Labels(), false)
INIT_INT64_G_ADDER_BVAR_METRIC(s3_file_open_reading, BvarMetricType::GAUGE,
                               BvarMetricUnit::FILESYSTEM, "", "", BvarMetric::Labels(), false)
INIT_INT64_G_ADDER_BVAR_METRIC(hdfs_file_open_reading, BvarMetricType::GAUGE,
                               BvarMetricUnit::FILESYSTEM, "", "", BvarMetric::Labels(), false)
INIT_INT64_G_ADDER_BVAR_METRIC(broker_file_open_reading, BvarMetricType::GAUGE,
                               BvarMetricUnit::FILESYSTEM, "", "", BvarMetric::Labels(), false)
INIT_INT64_G_ADDER_BVAR_METRIC(local_file_open_writing, BvarMetricType::GAUGE,
                               BvarMetricUnit::FILESYSTEM, "", "", BvarMetric::Labels(), false)
INIT_INT64_G_ADDER_BVAR_METRIC(s3_file_open_writing, BvarMetricType::GAUGE,
                               BvarMetricUnit::FILESYSTEM, "", "", BvarMetric::Labels(), false)

INIT_UINT64_G_ADDER_BVAR_METRIC(query_cache_memory_total_byte, BvarMetricType::GAUGE,
                                BvarMetricUnit::BYTES, "", "", BvarMetric::Labels(), true)
INIT_UINT64_G_ADDER_BVAR_METRIC(query_cache_sql_total_count, BvarMetricType::GAUGE,
                                BvarMetricUnit::NOUNIT, "", "", BvarMetric::Labels(), true)
INIT_UINT64_G_ADDER_BVAR_METRIC(query_cache_partition_total_count, BvarMetricType::GAUGE,
                                BvarMetricUnit::NOUNIT, "", "", BvarMetric::Labels(), true)
INIT_INT64_G_ADDER_BVAR_METRIC(lru_cache_memory_bytes, BvarMetricType::GAUGE, BvarMetricUnit::BYTES,
                               "", "", BvarMetric::Labels(), true)
INIT_UINT64_G_ADDER_BVAR_METRIC(upload_total_byte, BvarMetricType::GAUGE, BvarMetricUnit::BYTES, "",
                                "", BvarMetric::Labels(), true)
INIT_INT64_G_ADDER_BVAR_METRIC(upload_rowset_count, BvarMetricType::COUNTER,
                               BvarMetricUnit::ROWSETS, "", "", BvarMetric::Labels(), false)
INIT_INT64_G_ADDER_BVAR_METRIC(upload_fail_count, BvarMetricType::COUNTER, BvarMetricUnit::ROWSETS,
                               "", "", BvarMetric::Labels(), false)
INIT_UINT64_G_ADDER_BVAR_METRIC(flush_thread_pool_queue_size, BvarMetricType::GAUGE,
                                BvarMetricUnit::NOUNIT, "", "", BvarMetric::Labels(), false)
INIT_UINT64_G_ADDER_BVAR_METRIC(flush_thread_pool_thread_num, BvarMetricType::GAUGE,
                                BvarMetricUnit::NOUNIT, "", "", BvarMetric::Labels(), false)
INIT_UINT64_G_ADDER_BVAR_METRIC(memtable_memory_limiter_mem_consumption, BvarMetricType::GAUGE,
                                BvarMetricUnit::BYTES, "",
                                "memtable_memory_limiter_mem_consumption",
                                BvarMetric::Labels({{"type", "load"}}), false)

// Size of some global containers
INIT_UINT64_G_ADDER_BVAR_METRIC(unused_rowsets_count, BvarMetricType::GAUGE,
                                BvarMetricUnit::ROWSETS, "", "", BvarMetric::Labels(), false)
INIT_UINT64_G_ADDER_BVAR_METRIC(tablet_meta_mem_consumption, BvarMetricType::GAUGE,
                                BvarMetricUnit::BYTES, "", "mem_consumption",
                                BvarMetric::Labels({{"type", "tablet_meta"}}), false)
INIT_UINT64_G_ADDER_BVAR_METRIC(broker_count, BvarMetricType::GAUGE, BvarMetricUnit::NOUNIT, "", "",
                                BvarMetric::Labels(), false)
INIT_UINT64_G_ADDER_BVAR_METRIC(active_scan_context_count, BvarMetricType::GAUGE,
                                BvarMetricUnit::NOUNIT, "", "", BvarMetric::Labels(), false)
INIT_UINT64_G_ADDER_BVAR_METRIC(fragment_instance_count, BvarMetricType::GAUGE,
                                BvarMetricUnit::NOUNIT, "", "", BvarMetric::Labels(), false)
INIT_UINT64_G_ADDER_BVAR_METRIC(fragment_thread_pool_queue_size, BvarMetricType::GAUGE,
                                BvarMetricUnit::NOUNIT, "", "", BvarMetric::Labels(), false)
INIT_UINT64_G_ADDER_BVAR_METRIC(load_channel_count, BvarMetricType::GAUGE, BvarMetricUnit::NOUNIT,
                                "", "", BvarMetric::Labels(), false)
INIT_UINT64_G_ADDER_BVAR_METRIC(result_buffer_block_count, BvarMetricType::GAUGE,
                                BvarMetricUnit::NOUNIT, "", "", BvarMetric::Labels(), false)
INIT_UINT64_G_ADDER_BVAR_METRIC(result_block_queue_count, BvarMetricType::GAUGE,
                                BvarMetricUnit::NOUNIT, "", "", BvarMetric::Labels(), false)
INIT_UINT64_G_ADDER_BVAR_METRIC(small_file_cache_count, BvarMetricType::GAUGE,
                                BvarMetricUnit::NOUNIT, "", "", BvarMetric::Labels(), false)
INIT_UINT64_G_ADDER_BVAR_METRIC(tablet_writer_count, BvarMetricType::GAUGE, BvarMetricUnit::NOUNIT,
                                "", "", BvarMetric::Labels(), false)
INIT_UINT64_G_ADDER_BVAR_METRIC(new_stream_load_pipe_count, BvarMetricType::GAUGE,
                                BvarMetricUnit::NOUNIT, "", "", BvarMetric::Labels(), false)
INIT_UINT64_G_ADDER_BVAR_METRIC(brpc_endpoint_stub_count, BvarMetricType::GAUGE,
                                BvarMetricUnit::NOUNIT, "", "", BvarMetric::Labels(), false)
INIT_UINT64_G_ADDER_BVAR_METRIC(brpc_function_endpoint_stub_count, BvarMetricType::GAUGE,
                                BvarMetricUnit::NOUNIT, "", "", BvarMetric::Labels(), false)
INIT_UINT64_G_ADDER_BVAR_METRIC(routine_load_task_count, BvarMetricType::GAUGE,
                                BvarMetricUnit::NOUNIT, "", "", BvarMetric::Labels(), false)

INIT_UINT64_G_ADDER_BVAR_METRIC(local_scan_thread_pool_queue_size, BvarMetricType::GAUGE,
                                BvarMetricUnit::NOUNIT, "", "", BvarMetric::Labels(), false)
INIT_UINT64_G_ADDER_BVAR_METRIC(local_scan_thread_pool_thread_num, BvarMetricType::GAUGE,
                                BvarMetricUnit::NOUNIT, "", "", BvarMetric::Labels(), false)
INIT_UINT64_G_ADDER_BVAR_METRIC(remote_scan_thread_pool_queue_size, BvarMetricType::GAUGE,
                                BvarMetricUnit::NOUNIT, "", "", BvarMetric::Labels(), false)
INIT_UINT64_G_ADDER_BVAR_METRIC(remote_scan_thread_pool_thread_num, BvarMetricType::GAUGE,
                                BvarMetricUnit::NOUNIT, "", "", BvarMetric::Labels(), false)
INIT_UINT64_G_ADDER_BVAR_METRIC(limited_scan_thread_pool_queue_size, BvarMetricType::GAUGE,
                                BvarMetricUnit::NOUNIT, "", "", BvarMetric::Labels(), false)
INIT_UINT64_G_ADDER_BVAR_METRIC(limited_scan_thread_pool_thread_num, BvarMetricType::GAUGE,
                                BvarMetricUnit::NOUNIT, "", "", BvarMetric::Labels(), false)

INIT_UINT64_G_ADDER_BVAR_METRIC(heavy_work_pool_queue_size, BvarMetricType::GAUGE,
                                BvarMetricUnit::NOUNIT, "", "", BvarMetric::Labels(), false)
INIT_UINT64_G_ADDER_BVAR_METRIC(light_work_pool_queue_size, BvarMetricType::GAUGE,
                                BvarMetricUnit::NOUNIT, "", "", BvarMetric::Labels(), false)
INIT_UINT64_G_ADDER_BVAR_METRIC(heavy_work_active_threads, BvarMetricType::GAUGE,
                                BvarMetricUnit::NOUNIT, "", "", BvarMetric::Labels(), false)
INIT_UINT64_G_ADDER_BVAR_METRIC(light_work_active_threads, BvarMetricType::GAUGE,
                                BvarMetricUnit::NOUNIT, "", "", BvarMetric::Labels(), false)

INIT_UINT64_G_ADDER_BVAR_METRIC(heavy_work_pool_max_queue_size, BvarMetricType::GAUGE,
                                BvarMetricUnit::NOUNIT, "", "", BvarMetric::Labels(), false)
INIT_UINT64_G_ADDER_BVAR_METRIC(light_work_pool_max_queue_size, BvarMetricType::GAUGE,
                                BvarMetricUnit::NOUNIT, "", "", BvarMetric::Labels(), false)
INIT_UINT64_G_ADDER_BVAR_METRIC(heavy_work_max_threads, BvarMetricType::GAUGE,
                                BvarMetricUnit::NOUNIT, "", "", BvarMetric::Labels(), false)
INIT_UINT64_G_ADDER_BVAR_METRIC(light_work_max_threads, BvarMetricType::GAUGE,
                                BvarMetricUnit::NOUNIT, "", "", BvarMetric::Labels(), false)

INIT_UINT64_G_ADDER_BVAR_METRIC(load_channel_mem_consumption, BvarMetricType::GAUGE,
                                BvarMetricUnit::BYTES, "", "mem_consumption",
                                BvarMetric::Labels({{"type", "load"}}), false)

INIT_UINT64_G_ADDER_BVAR_METRIC(scanner_thread_pool_queue_size, BvarMetricType::GAUGE,
                                BvarMetricUnit::NOUNIT, "", "", BvarMetric::Labels(), false)
INIT_UINT64_G_ADDER_BVAR_METRIC(send_batch_thread_pool_thread_num, BvarMetricType::GAUGE,
                                BvarMetricUnit::NOUNIT, "", "", BvarMetric::Labels(), false)
INIT_UINT64_G_ADDER_BVAR_METRIC(send_batch_thread_pool_queue_size, BvarMetricType::GAUGE,
                                BvarMetricUnit::NOUNIT, "", "", BvarMetric::Labels(), false)
} // namespace doris