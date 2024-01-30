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

#include <memory>
#include <set>
#include <string>
#include <vector>

#include "util/bvar_metrics.h"
#include "util/system_bvar_metrics.h"
namespace doris {

class DorisBvarMetrics {
public:
    std::shared_ptr<BvarAdderMetric<int64_t>> fragment_requests_total;
    std::shared_ptr<BvarAdderMetric<int64_t>> fragment_request_duration_us;
    std::shared_ptr<BvarAdderMetric<int64_t>> query_scan_bytes;
    std::shared_ptr<BvarAdderMetric<int64_t>> query_scan_rows;
    
    std::shared_ptr<BvarAdderMetric<int64_t>> push_requests_success_total;
    std::shared_ptr<BvarAdderMetric<int64_t>> push_requests_fail_total;
    std::shared_ptr<BvarAdderMetric<int64_t>> push_request_duration_us;
    std::shared_ptr<BvarAdderMetric<int64_t>> push_request_write_bytes;
    std::shared_ptr<BvarAdderMetric<int64_t>> push_request_write_rows;

    std::shared_ptr<BvarAdderMetric<int64_t>> create_tablet_requests_total;
    std::shared_ptr<BvarAdderMetric<int64_t>> create_tablet_requests_failed;
    std::shared_ptr<BvarAdderMetric<int64_t>> drop_tablet_requests_total;

    std::shared_ptr<BvarAdderMetric<int64_t>> report_all_tablets_requests_skip;

    std::shared_ptr<BvarAdderMetric<int64_t>> schema_change_requests_total;
    std::shared_ptr<BvarAdderMetric<int64_t>> schema_change_requests_failed;
    std::shared_ptr<BvarAdderMetric<int64_t>> create_rollup_requests_total;
    std::shared_ptr<BvarAdderMetric<int64_t>> create_rollup_requests_failed;
    std::shared_ptr<BvarAdderMetric<int64_t>> storage_migrate_requests_total;
    std::shared_ptr<BvarAdderMetric<int64_t>> storage_migrate_v2_requests_total;
    std::shared_ptr<BvarAdderMetric<int64_t>> storage_migrate_v2_requests_failed;
    std::shared_ptr<BvarAdderMetric<int64_t>> delete_requests_total;
    std::shared_ptr<BvarAdderMetric<int64_t>> delete_requests_failed;
    std::shared_ptr<BvarAdderMetric<int64_t>> clone_requests_total;
    std::shared_ptr<BvarAdderMetric<int64_t>> clone_requests_failed;
    std::shared_ptr<BvarAdderMetric<int64_t>> alter_inverted_index_requests_total;
    std::shared_ptr<BvarAdderMetric<int64_t>> alter_inverted_index_requests_failed;

    std::shared_ptr<BvarAdderMetric<int64_t>> finish_task_requests_total;
    std::shared_ptr<BvarAdderMetric<int64_t>> finish_task_requests_failed;

    std::shared_ptr<BvarAdderMetric<int64_t>> base_compaction_request_total;
    std::shared_ptr<BvarAdderMetric<int64_t>> base_compaction_request_failed;
    std::shared_ptr<BvarAdderMetric<int64_t>> cumulative_compaction_request_total;
    std::shared_ptr<BvarAdderMetric<int64_t>> cumulative_compaction_request_failed;

    std::shared_ptr<BvarAdderMetric<int64_t>> base_compaction_deltas_total;
    std::shared_ptr<BvarAdderMetric<int64_t>> base_compaction_bytes_total;
    std::shared_ptr<BvarAdderMetric<int64_t>> cumulative_compaction_deltas_total;
    std::shared_ptr<BvarAdderMetric<int64_t>> cumulative_compaction_bytes_total;

    std::shared_ptr<BvarAdderMetric<int64_t>> publish_task_request_total;
    std::shared_ptr<BvarAdderMetric<int64_t>> publish_task_failed_total;

    // Counters for segment_v2
    // -----------------------
    // total number of segments read
    std::shared_ptr<BvarAdderMetric<int64_t>> segment_read_total;
    // total number of rows in queried segments (before index pruning)
    std::shared_ptr<BvarAdderMetric<int64_t>> segment_row_total;

    std::shared_ptr<BvarAdderMetric<int64_t>> stream_load_txn_begin_request_total;
    std::shared_ptr<BvarAdderMetric<int64_t>> stream_load_txn_commit_request_total;
    std::shared_ptr<BvarAdderMetric<int64_t>> stream_load_txn_rollback_request_total;
    std::shared_ptr<BvarAdderMetric<int64_t>> stream_receive_bytes_total;
    std::shared_ptr<BvarAdderMetric<int64_t>> stream_load_rows_total;
    std::shared_ptr<BvarAdderMetric<int64_t>> load_rows;
    std::shared_ptr<BvarAdderMetric<int64_t>> load_bytes;

    std::shared_ptr<BvarAdderMetric<int64_t>> memtable_flush_total;
    std::shared_ptr<BvarAdderMetric<int64_t>> memtable_flush_duration_us;
    //Gauge  line 95 to 120
    std::shared_ptr<BvarAdderMetric<int64_t>> memory_pool_bytes_total;
    std::shared_ptr<BvarAdderMetric<int64_t>> process_thread_num;
    std::shared_ptr<BvarAdderMetric<int64_t>> process_fd_num_used;
    std::shared_ptr<BvarAdderMetric<int64_t>> process_fd_num_limit_soft;
    std::shared_ptr<BvarAdderMetric<int64_t>> process_fd_num_limit_hard;

    // the max compaction score of all tablets.
    // Record base and cumulative scores separately, because
    // we need to get the larger of the two.
    std::shared_ptr<BvarAdderMetric<int64_t>> tablet_cumulative_max_compaction_score;
    std::shared_ptr<BvarAdderMetric<int64_t>> tablet_base_max_compaction_score;

    std::shared_ptr<BvarAdderMetric<int64_t>> all_rowsets_num;
    std::shared_ptr<BvarAdderMetric<int64_t>> all_segments_num;

    // permits have been used for all compaction tasks
    std::shared_ptr<BvarAdderMetric<int64_t>> compaction_used_permits;
    // permits required by the compaction task which is waiting for permits
    std::shared_ptr<BvarAdderMetric<int64_t>> compaction_waitting_permits;

    // HistogramMetric* tablet_version_num_distribution;
    
    // The following metrics will be calculated
    // by metric calculator
    std::shared_ptr<BvarAdderMetric<int64_t>> query_scan_bytes_per_second;
    
    //Counter
    // Metrics related with file reader/writer
    std::shared_ptr<BvarAdderMetric<int64_t>> local_file_reader_total;
    std::shared_ptr<BvarAdderMetric<int64_t>> s3_file_reader_total;
    std::shared_ptr<BvarAdderMetric<int64_t>> hdfs_file_reader_total;
    std::shared_ptr<BvarAdderMetric<int64_t>> broker_file_reader_total;
    std::shared_ptr<BvarAdderMetric<int64_t>> local_file_writer_total;
    std::shared_ptr<BvarAdderMetric<int64_t>> s3_file_writer_total;
    std::shared_ptr<BvarAdderMetric<int64_t>> file_created_total;
    std::shared_ptr<BvarAdderMetric<int64_t>> s3_file_created_total;
    std::shared_ptr<BvarAdderMetric<int64_t>> local_bytes_read_total;
    std::shared_ptr<BvarAdderMetric<int64_t>> s3_bytes_read_total;
    std::shared_ptr<BvarAdderMetric<int64_t>> local_bytes_written_total;
    std::shared_ptr<BvarAdderMetric<int64_t>> s3_bytes_written_total;
    //Gauge
    std::shared_ptr<BvarAdderMetric<int64_t>> local_file_open_reading;
    std::shared_ptr<BvarAdderMetric<int64_t>> s3_file_open_reading;
    std::shared_ptr<BvarAdderMetric<int64_t>> hdfs_file_open_reading;
    std::shared_ptr<BvarAdderMetric<int64_t>> broker_file_open_reading;
    std::shared_ptr<BvarAdderMetric<int64_t>> local_file_open_writing;
    std::shared_ptr<BvarAdderMetric<int64_t>> s3_file_open_writing;
    
    static DorisBvarMetrics* instance() {
        static DorisBvarMetrics metrics;
        return &metrics;
    }

    void initialize(
        bool init_system_metrics = false,
        const std::set<std::string>& disk_devices = std::set<std::string>(),
        const std::vector<std::string>& network_interfaces = std::vector<std::string>());
    
    void register_entity(BvarMetricEntity entity);
    // SystemBvarMetrics* get_system_bvar_metrics() { return system_metrics_.get(); }

    std::string to_prometheus() const;

private:
    DorisBvarMetrics();

private:
    static const std::string s_registry_name_;
    
    std::unique_ptr<SystemBvarMetrics> system_metrics_;

    std::unordered_map<std::string, 
                       std::vector<std::shared_ptr<BvarMetricEntity>>> entities_map_;

};

extern BvarAdderMetric<int64_t> g_adder_timeout_canceled_fragment_count;
extern BvarAdderMetric<int64_t> g_adder_file_created_total;

extern BvarAdderMetric<int64_t> g_adder_fragment_request_duration_us;
extern BvarAdderMetric<int64_t> g_adder_query_scan_bytes;
extern BvarAdderMetric<int64_t> g_adder_segment_read_total;

} // namespace doris