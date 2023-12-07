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

#include "util/doris_metrics.h"

// IWYU pragma: no_include <bthread/errno.h>
#include <errno.h> // IWYU pragma: keep
#include <glog/logging.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <algorithm>
#include <functional>
#include <ostream>

#include "common/status.h"
#include "io/fs/local_file_system.h"
#include "util/system_metrics.h"

namespace doris {
namespace io {
struct FileInfo;
} // namespace io

DEFINE_COUNTER_METRIC_PROTOTYPE_3ARG(fragment_requests_total, MetricUnit::REQUESTS,
                                     "Total fragment requests received.");
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(fragment_request_duration_us, MetricUnit::MICROSECONDS);
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(query_scan_bytes, MetricUnit::BYTES);
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(query_scan_rows, MetricUnit::ROWS);
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(query_scan_count, MetricUnit::NOUNIT);
DEFINE_COUNTER_METRIC_PROTOTYPE_5ARG(push_requests_success_total, MetricUnit::REQUESTS, "",
                                     push_requests_total, Labels({{"status", "SUCCESS"}}));
DEFINE_COUNTER_METRIC_PROTOTYPE_5ARG(push_requests_fail_total, MetricUnit::REQUESTS, "",
                                     push_requests_total, Labels({{"status", "FAIL"}}));
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(push_request_duration_us, MetricUnit::MICROSECONDS);
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(push_request_write_bytes, MetricUnit::BYTES);
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(push_request_write_rows, MetricUnit::ROWS);

#define DEFINE_ENGINE_COUNTER_METRIC(name, type, status)                                        \
    DEFINE_COUNTER_METRIC_PROTOTYPE_5ARG(name, MetricUnit::REQUESTS, "", engine_requests_total, \
                                         Labels({{"type", #type}, {"status", #status}}));

DEFINE_ENGINE_COUNTER_METRIC(create_tablet_requests_total, create_tablet, total);
DEFINE_ENGINE_COUNTER_METRIC(create_tablet_requests_failed, create_tablet, failed);
DEFINE_ENGINE_COUNTER_METRIC(drop_tablet_requests_total, drop_tablet, total);
DEFINE_ENGINE_COUNTER_METRIC(report_all_tablets_requests_skip, report_all_tablets, skip)
DEFINE_ENGINE_COUNTER_METRIC(schema_change_requests_total, schema_change, total);
DEFINE_ENGINE_COUNTER_METRIC(schema_change_requests_failed, schema_change, failed);
DEFINE_ENGINE_COUNTER_METRIC(create_rollup_requests_total, create_rollup, total);
DEFINE_ENGINE_COUNTER_METRIC(create_rollup_requests_failed, create_rollup, failed);
DEFINE_ENGINE_COUNTER_METRIC(storage_migrate_requests_total, storage_migrate, total);
DEFINE_ENGINE_COUNTER_METRIC(storage_migrate_v2_requests_total, storage_migrate_v2, total);
DEFINE_ENGINE_COUNTER_METRIC(storage_migrate_v2_requests_failed, storage_migrate_v2, failed);
DEFINE_ENGINE_COUNTER_METRIC(delete_requests_total, delete, total);
DEFINE_ENGINE_COUNTER_METRIC(delete_requests_failed, delete, failed);
DEFINE_ENGINE_COUNTER_METRIC(clone_requests_total, clone, total);
DEFINE_ENGINE_COUNTER_METRIC(clone_requests_failed, clone, failed);
DEFINE_ENGINE_COUNTER_METRIC(finish_task_requests_total, finish_task, total);
DEFINE_ENGINE_COUNTER_METRIC(finish_task_requests_failed, finish_task, failed);
DEFINE_ENGINE_COUNTER_METRIC(base_compaction_request_total, base_compaction, total);
DEFINE_ENGINE_COUNTER_METRIC(base_compaction_request_failed, base_compaction, failed);
DEFINE_ENGINE_COUNTER_METRIC(cumulative_compaction_request_total, cumulative_compaction, total);
DEFINE_ENGINE_COUNTER_METRIC(cumulative_compaction_request_failed, cumulative_compaction, failed);
DEFINE_ENGINE_COUNTER_METRIC(publish_task_request_total, publish, total);
DEFINE_ENGINE_COUNTER_METRIC(publish_task_failed_total, publish, failed);
DEFINE_ENGINE_COUNTER_METRIC(alter_inverted_index_requests_total, alter_inverted_index, total);
DEFINE_ENGINE_COUNTER_METRIC(alter_inverted_index_requests_failed, alter_inverted_index, failed);

DEFINE_COUNTER_METRIC_PROTOTYPE_5ARG(base_compaction_deltas_total, MetricUnit::ROWSETS, "",
                                     compaction_deltas_total, Labels({{"type", "base"}}));
DEFINE_COUNTER_METRIC_PROTOTYPE_5ARG(cumulative_compaction_deltas_total, MetricUnit::ROWSETS, "",
                                     compaction_deltas_total, Labels({{"type", "cumulative"}}));
DEFINE_COUNTER_METRIC_PROTOTYPE_5ARG(base_compaction_bytes_total, MetricUnit::BYTES, "",
                                     compaction_bytes_total, Labels({{"type", "base"}}));
DEFINE_COUNTER_METRIC_PROTOTYPE_5ARG(cumulative_compaction_bytes_total, MetricUnit::BYTES, "",
                                     compaction_bytes_total, Labels({{"type", "cumulative"}}));

DEFINE_COUNTER_METRIC_PROTOTYPE_5ARG(meta_write_request_total, MetricUnit::REQUESTS, "",
                                     meta_request_total, Labels({{"type", "write"}}));
DEFINE_COUNTER_METRIC_PROTOTYPE_5ARG(meta_read_request_total, MetricUnit::REQUESTS, "",
                                     meta_request_total, Labels({{"type", "read"}}));

DEFINE_COUNTER_METRIC_PROTOTYPE_5ARG(meta_write_request_duration_us, MetricUnit::MICROSECONDS, "",
                                     meta_request_duration, Labels({{"type", "write"}}));
DEFINE_COUNTER_METRIC_PROTOTYPE_5ARG(meta_read_request_duration_us, MetricUnit::MICROSECONDS, "",
                                     meta_request_duration, Labels({{"type", "read"}}));

DEFINE_COUNTER_METRIC_PROTOTYPE_5ARG(segment_read_total, MetricUnit::OPERATIONS,
                                     "(segment_v2) total number of segments read", segment_read,
                                     Labels({{"type", "segment_read_total"}}));
DEFINE_COUNTER_METRIC_PROTOTYPE_5ARG(
        segment_row_total, MetricUnit::ROWS,
        "(segment_v2) total number of rows in queried segments (before index pruning)",
        segment_read, Labels({{"type", "segment_row_total"}}));

DEFINE_COUNTER_METRIC_PROTOTYPE_5ARG(stream_load_txn_begin_request_total, MetricUnit::OPERATIONS,
                                     "", stream_load_txn_request, Labels({{"type", "begin"}}));
DEFINE_COUNTER_METRIC_PROTOTYPE_5ARG(stream_load_txn_commit_request_total, MetricUnit::OPERATIONS,
                                     "", stream_load_txn_request, Labels({{"type", "commit"}}));
DEFINE_COUNTER_METRIC_PROTOTYPE_5ARG(stream_load_txn_rollback_request_total, MetricUnit::OPERATIONS,
                                     "", stream_load_txn_request, Labels({{"type", "rollback"}}));

DEFINE_COUNTER_METRIC_PROTOTYPE_5ARG(stream_receive_bytes_total, MetricUnit::BYTES, "", stream_load,
                                     Labels({{"type", "receive_bytes"}}));
DEFINE_COUNTER_METRIC_PROTOTYPE_5ARG(stream_load_rows_total, MetricUnit::ROWS, "", stream_load,
                                     Labels({{"type", "load_rows"}}));

DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(load_rows, MetricUnit::ROWS);
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(load_bytes, MetricUnit::BYTES);

DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(memtable_flush_total, MetricUnit::OPERATIONS);
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(memtable_flush_duration_us, MetricUnit::MICROSECONDS);

DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(memory_pool_bytes_total, MetricUnit::BYTES);
DEFINE_GAUGE_CORE_METRIC_PROTOTYPE_2ARG(process_thread_num, MetricUnit::NOUNIT);
DEFINE_GAUGE_CORE_METRIC_PROTOTYPE_2ARG(process_fd_num_used, MetricUnit::NOUNIT);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(process_fd_num_limit_soft, MetricUnit::NOUNIT);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(process_fd_num_limit_hard, MetricUnit::NOUNIT);

DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(tablet_cumulative_max_compaction_score, MetricUnit::NOUNIT);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(tablet_base_max_compaction_score, MetricUnit::NOUNIT);

DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(all_rowsets_num, MetricUnit::NOUNIT);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(all_segments_num, MetricUnit::NOUNIT);

DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(compaction_used_permits, MetricUnit::NOUNIT);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(compaction_waitting_permits, MetricUnit::NOUNIT);

DEFINE_HISTOGRAM_METRIC_PROTOTYPE_2ARG(tablet_version_num_distribution, MetricUnit::NOUNIT);

DEFINE_GAUGE_CORE_METRIC_PROTOTYPE_2ARG(query_scan_bytes_per_second, MetricUnit::BYTES);

DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(readable_blocks_total, MetricUnit::BLOCKS);
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(writable_blocks_total, MetricUnit::BLOCKS);
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(blocks_created_total, MetricUnit::OPERATIONS);
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(blocks_deleted_total, MetricUnit::OPERATIONS);
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(bytes_read_total, MetricUnit::BYTES);
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(bytes_written_total, MetricUnit::BYTES);
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(disk_sync_total, MetricUnit::OPERATIONS);

DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(blocks_open_reading, MetricUnit::BLOCKS);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(blocks_open_writing, MetricUnit::BLOCKS);

DEFINE_GAUGE_CORE_METRIC_PROTOTYPE_2ARG(query_cache_memory_total_byte, MetricUnit::BYTES);
DEFINE_GAUGE_CORE_METRIC_PROTOTYPE_2ARG(query_cache_sql_total_count, MetricUnit::NOUNIT);
DEFINE_GAUGE_CORE_METRIC_PROTOTYPE_2ARG(query_cache_partition_total_count, MetricUnit::NOUNIT);

DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(tablet_schema_cache_count, MetricUnit::NOUNIT);
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(tablet_schema_cache_columns_count, MetricUnit::NOUNIT);
DEFINE_GAUGE_CORE_METRIC_PROTOTYPE_2ARG(lru_cache_memory_bytes, MetricUnit::BYTES);

DEFINE_GAUGE_CORE_METRIC_PROTOTYPE_2ARG(upload_total_byte, MetricUnit::BYTES);
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(upload_rowset_count, MetricUnit::ROWSETS);
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(upload_fail_count, MetricUnit::ROWSETS);

DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(local_file_reader_total, MetricUnit::FILESYSTEM);
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(s3_file_reader_total, MetricUnit::FILESYSTEM);
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(hdfs_file_reader_total, MetricUnit::FILESYSTEM);
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(broker_file_reader_total, MetricUnit::FILESYSTEM);
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(local_file_writer_total, MetricUnit::FILESYSTEM);
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(s3_file_writer_total, MetricUnit::FILESYSTEM);
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(file_created_total, MetricUnit::FILESYSTEM);
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(s3_file_created_total, MetricUnit::FILESYSTEM);
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(local_bytes_read_total, MetricUnit::FILESYSTEM);
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(s3_bytes_read_total, MetricUnit::FILESYSTEM);
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(local_bytes_written_total, MetricUnit::FILESYSTEM);
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(s3_bytes_written_total, MetricUnit::FILESYSTEM);

DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(local_file_open_reading, MetricUnit::FILESYSTEM);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(s3_file_open_reading, MetricUnit::FILESYSTEM);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(hdfs_file_open_reading, MetricUnit::FILESYSTEM);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(broker_file_open_reading, MetricUnit::FILESYSTEM);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(local_file_open_writing, MetricUnit::FILESYSTEM);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(s3_file_open_writing, MetricUnit::FILESYSTEM);

const std::string DorisMetrics::_s_registry_name = "doris_be";
const std::string DorisMetrics::_s_hook_name = "doris_metrics";

DorisMetrics::DorisMetrics() : _metric_registry(_s_registry_name) {
    _server_metric_entity = _metric_registry.register_entity("server");

    INT_COUNTER_METRIC_REGISTER(_server_metric_entity, fragment_requests_total);
    INT_COUNTER_METRIC_REGISTER(_server_metric_entity, fragment_request_duration_us);
    INT_COUNTER_METRIC_REGISTER(_server_metric_entity, query_scan_bytes);
    INT_COUNTER_METRIC_REGISTER(_server_metric_entity, query_scan_rows);

    INT_COUNTER_METRIC_REGISTER(_server_metric_entity, push_requests_success_total);
    INT_COUNTER_METRIC_REGISTER(_server_metric_entity, push_requests_fail_total);
    INT_COUNTER_METRIC_REGISTER(_server_metric_entity, push_request_duration_us);
    INT_COUNTER_METRIC_REGISTER(_server_metric_entity, push_request_write_bytes);
    INT_COUNTER_METRIC_REGISTER(_server_metric_entity, push_request_write_rows);

    // engine_requests_total
    INT_COUNTER_METRIC_REGISTER(_server_metric_entity, create_tablet_requests_total);
    INT_COUNTER_METRIC_REGISTER(_server_metric_entity, create_tablet_requests_failed);
    INT_COUNTER_METRIC_REGISTER(_server_metric_entity, drop_tablet_requests_total);
    INT_COUNTER_METRIC_REGISTER(_server_metric_entity, report_all_tablets_requests_skip);
    INT_COUNTER_METRIC_REGISTER(_server_metric_entity, schema_change_requests_total);
    INT_COUNTER_METRIC_REGISTER(_server_metric_entity, schema_change_requests_failed);
    INT_COUNTER_METRIC_REGISTER(_server_metric_entity, create_rollup_requests_total);
    INT_COUNTER_METRIC_REGISTER(_server_metric_entity, create_rollup_requests_failed);
    INT_COUNTER_METRIC_REGISTER(_server_metric_entity, storage_migrate_requests_total);
    INT_COUNTER_METRIC_REGISTER(_server_metric_entity, storage_migrate_v2_requests_total);
    INT_COUNTER_METRIC_REGISTER(_server_metric_entity, storage_migrate_v2_requests_failed);
    INT_COUNTER_METRIC_REGISTER(_server_metric_entity, delete_requests_total);
    INT_COUNTER_METRIC_REGISTER(_server_metric_entity, delete_requests_failed);
    INT_COUNTER_METRIC_REGISTER(_server_metric_entity, clone_requests_total);
    INT_COUNTER_METRIC_REGISTER(_server_metric_entity, clone_requests_failed);
    INT_COUNTER_METRIC_REGISTER(_server_metric_entity, finish_task_requests_total);
    INT_COUNTER_METRIC_REGISTER(_server_metric_entity, finish_task_requests_failed);
    INT_COUNTER_METRIC_REGISTER(_server_metric_entity, base_compaction_request_total);
    INT_COUNTER_METRIC_REGISTER(_server_metric_entity, base_compaction_request_failed);
    INT_COUNTER_METRIC_REGISTER(_server_metric_entity, cumulative_compaction_request_total);
    INT_COUNTER_METRIC_REGISTER(_server_metric_entity, cumulative_compaction_request_failed);
    INT_COUNTER_METRIC_REGISTER(_server_metric_entity, publish_task_request_total);
    INT_COUNTER_METRIC_REGISTER(_server_metric_entity, publish_task_failed_total);
    INT_COUNTER_METRIC_REGISTER(_server_metric_entity, alter_inverted_index_requests_total);
    INT_COUNTER_METRIC_REGISTER(_server_metric_entity, alter_inverted_index_requests_failed);

    INT_COUNTER_METRIC_REGISTER(_server_metric_entity, base_compaction_deltas_total);
    INT_COUNTER_METRIC_REGISTER(_server_metric_entity, base_compaction_bytes_total);
    INT_COUNTER_METRIC_REGISTER(_server_metric_entity, cumulative_compaction_deltas_total);
    INT_COUNTER_METRIC_REGISTER(_server_metric_entity, cumulative_compaction_bytes_total);

    INT_COUNTER_METRIC_REGISTER(_server_metric_entity, meta_write_request_total);
    INT_COUNTER_METRIC_REGISTER(_server_metric_entity, meta_write_request_duration_us);
    INT_COUNTER_METRIC_REGISTER(_server_metric_entity, meta_read_request_total);
    INT_COUNTER_METRIC_REGISTER(_server_metric_entity, meta_read_request_duration_us);

    INT_COUNTER_METRIC_REGISTER(_server_metric_entity, segment_read_total);
    INT_COUNTER_METRIC_REGISTER(_server_metric_entity, segment_row_total);

    INT_COUNTER_METRIC_REGISTER(_server_metric_entity, stream_load_txn_begin_request_total);
    INT_COUNTER_METRIC_REGISTER(_server_metric_entity, stream_load_txn_commit_request_total);
    INT_COUNTER_METRIC_REGISTER(_server_metric_entity, stream_load_txn_rollback_request_total);
    INT_COUNTER_METRIC_REGISTER(_server_metric_entity, stream_receive_bytes_total);
    INT_COUNTER_METRIC_REGISTER(_server_metric_entity, stream_load_rows_total);

    INT_COUNTER_METRIC_REGISTER(_server_metric_entity, memtable_flush_total);
    INT_COUNTER_METRIC_REGISTER(_server_metric_entity, memtable_flush_duration_us);

    INT_GAUGE_METRIC_REGISTER(_server_metric_entity, memory_pool_bytes_total);
    INT_GAUGE_METRIC_REGISTER(_server_metric_entity, process_thread_num);
    INT_GAUGE_METRIC_REGISTER(_server_metric_entity, process_fd_num_used);
    INT_GAUGE_METRIC_REGISTER(_server_metric_entity, process_fd_num_limit_soft);
    INT_GAUGE_METRIC_REGISTER(_server_metric_entity, process_fd_num_limit_hard);

    INT_GAUGE_METRIC_REGISTER(_server_metric_entity, tablet_cumulative_max_compaction_score);
    INT_GAUGE_METRIC_REGISTER(_server_metric_entity, tablet_base_max_compaction_score);

    INT_GAUGE_METRIC_REGISTER(_server_metric_entity, all_rowsets_num);
    INT_GAUGE_METRIC_REGISTER(_server_metric_entity, all_segments_num);

    INT_GAUGE_METRIC_REGISTER(_server_metric_entity, compaction_used_permits);
    INT_GAUGE_METRIC_REGISTER(_server_metric_entity, compaction_waitting_permits);

    HISTOGRAM_METRIC_REGISTER(_server_metric_entity, tablet_version_num_distribution);

    INT_GAUGE_METRIC_REGISTER(_server_metric_entity, query_scan_bytes_per_second);

    INT_COUNTER_METRIC_REGISTER(_server_metric_entity, load_rows);
    INT_COUNTER_METRIC_REGISTER(_server_metric_entity, load_bytes);

    INT_UGAUGE_METRIC_REGISTER(_server_metric_entity, upload_total_byte);
    INT_COUNTER_METRIC_REGISTER(_server_metric_entity, upload_rowset_count);
    INT_COUNTER_METRIC_REGISTER(_server_metric_entity, upload_fail_count);

    _server_metric_entity->register_hook(_s_hook_name, std::bind(&DorisMetrics::_update, this));

    INT_UGAUGE_METRIC_REGISTER(_server_metric_entity, query_cache_memory_total_byte);
    INT_UGAUGE_METRIC_REGISTER(_server_metric_entity, query_cache_sql_total_count);
    INT_UGAUGE_METRIC_REGISTER(_server_metric_entity, query_cache_partition_total_count);

    INT_COUNTER_METRIC_REGISTER(_server_metric_entity, tablet_schema_cache_count);
    INT_COUNTER_METRIC_REGISTER(_server_metric_entity, tablet_schema_cache_columns_count);
    INT_GAUGE_METRIC_REGISTER(_server_metric_entity, lru_cache_memory_bytes);

    INT_COUNTER_METRIC_REGISTER(_server_metric_entity, local_file_reader_total);
    INT_COUNTER_METRIC_REGISTER(_server_metric_entity, s3_file_reader_total);
    INT_COUNTER_METRIC_REGISTER(_server_metric_entity, hdfs_file_reader_total);
    INT_COUNTER_METRIC_REGISTER(_server_metric_entity, broker_file_reader_total);
    INT_COUNTER_METRIC_REGISTER(_server_metric_entity, local_file_writer_total);
    INT_COUNTER_METRIC_REGISTER(_server_metric_entity, s3_file_writer_total);
    INT_COUNTER_METRIC_REGISTER(_server_metric_entity, file_created_total);
    INT_COUNTER_METRIC_REGISTER(_server_metric_entity, s3_file_created_total);
    INT_COUNTER_METRIC_REGISTER(_server_metric_entity, local_bytes_read_total);
    INT_COUNTER_METRIC_REGISTER(_server_metric_entity, s3_bytes_read_total);
    INT_COUNTER_METRIC_REGISTER(_server_metric_entity, local_bytes_written_total);
    INT_COUNTER_METRIC_REGISTER(_server_metric_entity, s3_bytes_written_total);
    INT_GAUGE_METRIC_REGISTER(_server_metric_entity, local_file_open_reading);
    INT_GAUGE_METRIC_REGISTER(_server_metric_entity, s3_file_open_reading);
    INT_GAUGE_METRIC_REGISTER(_server_metric_entity, hdfs_file_open_reading);
    INT_GAUGE_METRIC_REGISTER(_server_metric_entity, broker_file_open_reading);
    INT_GAUGE_METRIC_REGISTER(_server_metric_entity, local_file_open_writing);
    INT_GAUGE_METRIC_REGISTER(_server_metric_entity, s3_file_open_writing);
}

void DorisMetrics::initialize(bool init_system_metrics, const std::set<std::string>& disk_devices,
                              const std::vector<std::string>& network_interfaces) {
    if (init_system_metrics) {
        _system_metrics.reset(
                new SystemMetrics(&_metric_registry, disk_devices, network_interfaces));
    }
}

void DorisMetrics::_update() {
    _update_process_thread_num();
    _update_process_fd_num();
}

// get num of thread of doris_be process
// from /proc/self/task
void DorisMetrics::_update_process_thread_num() {
    std::error_code ec;
    std::filesystem::directory_iterator dict_iter("/proc/self/task/", ec);
    if (ec) {
        LOG(WARNING) << "failed to count thread num: " << ec.message();
        process_fd_num_used->set_value(0);
        return;
    }
    int64_t count =
            std::count_if(dict_iter, std::filesystem::end(dict_iter), [](const auto& entry) {
                std::error_code error_code;
                return entry.is_regular_file(error_code) && !error_code;
            });

    process_thread_num->set_value(count);
}

// get num of file descriptor of doris_be process
void DorisMetrics::_update_process_fd_num() {
    // fd used
    std::error_code ec;
    std::filesystem::directory_iterator dict_iter("/proc/self/fd/", ec);
    if (ec) {
        LOG(WARNING) << "failed to count fd: " << ec.message();
        process_fd_num_used->set_value(0);
        return;
    }
    int64_t count =
            std::count_if(dict_iter, std::filesystem::end(dict_iter), [](const auto& entry) {
                std::error_code error_code;
                return entry.is_regular_file(error_code) && !error_code;
            });

    process_fd_num_used->set_value(count);

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
            process_fd_num_limit_soft->set_value(values[0]);
            process_fd_num_limit_hard->set_value(values[1]);
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

} // namespace doris
