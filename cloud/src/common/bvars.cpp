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

#include "common/bvars.h"

#include <cstdint>
#include <stdexcept>

// meta-service's bvars
BvarLatencyRecorderWithTag g_bvar_ms_begin_txn("ms", "begin_txn");
BvarLatencyRecorderWithTag g_bvar_ms_precommit_txn("ms", "precommit_txn");
BvarLatencyRecorderWithTag g_bvar_ms_commit_txn("ms", "commit_txn");
BvarLatencyRecorderWithTag g_bvar_ms_abort_txn("ms", "abort_txn");
BvarLatencyRecorderWithTag g_bvar_ms_get_txn("ms", "get_txn");
BvarLatencyRecorderWithTag g_bvar_ms_get_current_max_txn_id("ms", "get_current_max_txn_id");
BvarLatencyRecorderWithTag g_bvar_ms_begin_sub_txn("ms", "begin_sub_txn");
BvarLatencyRecorderWithTag g_bvar_ms_abort_sub_txn("ms", "abort_sub_txn");
BvarLatencyRecorderWithTag g_bvar_ms_check_txn_conflict("ms", "check_txn_conflict");
BvarLatencyRecorderWithTag g_bvar_ms_abort_txn_with_coordinator("ms", "abort_txn_with_coordinator");
BvarLatencyRecorderWithTag g_bvar_ms_clean_txn_label("ms", "clean_txn_label");
BvarLatencyRecorderWithTag g_bvar_ms_get_version("ms", "get_version");
BvarLatencyRecorderWithTag g_bvar_ms_batch_get_version("ms", "batch_get_version");
BvarLatencyRecorderWithTag g_bvar_ms_create_tablets("ms", "create_tablets");
BvarLatencyRecorderWithTag g_bvar_ms_update_tablet("ms", "update_tablet");
BvarLatencyRecorderWithTag g_bvar_ms_update_tablet_schema("ms", "update_tablet_schema");
BvarLatencyRecorderWithTag g_bvar_ms_get_tablet("ms", "get_tablet");
BvarLatencyRecorderWithTag g_bvar_ms_prepare_rowset("ms", "prepare_rowset");
BvarLatencyRecorderWithTag g_bvar_ms_commit_rowset("ms", "commit_rowset");
BvarLatencyRecorderWithTag g_bvar_ms_update_tmp_rowset("ms", "update_tmp_rowset");
BvarLatencyRecorderWithTag g_bvar_ms_get_rowset("ms", "get_rowset");
BvarLatencyRecorderWithTag g_bvar_ms_drop_index("ms", "drop_index");
BvarLatencyRecorderWithTag g_bvar_ms_prepare_index("ms", "prepare_index");
BvarLatencyRecorderWithTag g_bvar_ms_commit_index("ms", "commit_index");
BvarLatencyRecorderWithTag g_bvar_ms_prepare_partition("ms", "prepare_partition");
BvarLatencyRecorderWithTag g_bvar_ms_commit_partition("ms", "commit_partition");
BvarLatencyRecorderWithTag g_bvar_ms_drop_partition("ms", "drop_partition");
BvarLatencyRecorderWithTag g_bvar_ms_get_tablet_stats("ms", "get_tablet_stats");
BvarLatencyRecorderWithTag g_bvar_ms_get_obj_store_info("ms", "get_obj_store_info");
BvarLatencyRecorderWithTag g_bvar_ms_alter_obj_store_info("ms", "alter_obj_store_info");
BvarLatencyRecorderWithTag g_bvar_ms_alter_storage_vault("ms", "alter_storage_vault");
BvarLatencyRecorderWithTag g_bvar_ms_create_instance("ms", "create_instance");
BvarLatencyRecorderWithTag g_bvar_ms_alter_instance("ms", "alter_instance");
BvarLatencyRecorderWithTag g_bvar_ms_alter_cluster("ms", "alter_cluster");
BvarLatencyRecorderWithTag g_bvar_ms_get_cluster("ms", "get_cluster");
BvarLatencyRecorderWithTag g_bvar_ms_create_stage("ms", "create_stage");
BvarLatencyRecorderWithTag g_bvar_ms_get_stage("ms", "get_stage");
BvarLatencyRecorderWithTag g_bvar_ms_drop_stage("ms", "drop_stage");
BvarLatencyRecorderWithTag g_bvar_ms_get_iam("ms", "get_iam");
BvarLatencyRecorderWithTag g_bvar_ms_alter_iam("ms", "alter_iam");
BvarLatencyRecorderWithTag g_bvar_ms_update_ak_sk("ms", "update_ak_sk");
BvarLatencyRecorderWithTag g_bvar_ms_alter_ram_user("ms", "alter_ram_user");
BvarLatencyRecorderWithTag g_bvar_ms_begin_copy("ms", "begin_copy");
BvarLatencyRecorderWithTag g_bvar_ms_finish_copy("ms", "finish_copy");
BvarLatencyRecorderWithTag g_bvar_ms_get_copy_job("ms", "get_copy_job");
BvarLatencyRecorderWithTag g_bvar_ms_get_copy_files("ms", "get_copy_files");
BvarLatencyRecorderWithTag g_bvar_ms_filter_copy_files("ms", "filter_copy_files");
BvarLatencyRecorderWithTag g_bvar_ms_update_delete_bitmap("ms", "update_delete_bitmap");
BvarLatencyRecorderWithTag g_bvar_ms_get_delete_bitmap("ms", "get_delete_bitmap");
BvarLatencyRecorderWithTag g_bvar_ms_get_delete_bitmap_update_lock("ms",
                                                                   "get_delete_bitmap_update_lock");
BvarLatencyRecorderWithTag g_bvar_ms_get_instance("ms", "get_instance");
BvarLatencyRecorderWithTag g_bvar_ms_get_rl_task_commit_attach("ms", "get_rl_task_commit_attach");
BvarLatencyRecorderWithTag g_bvar_ms_get_txn_id("ms", "get_txn_id");

BvarLatencyRecorderWithTag g_bvar_ms_start_tablet_job("ms", "start_tablet_job");
BvarLatencyRecorderWithTag g_bvar_ms_finish_tablet_job("ms", "finish_tablet_job");
BvarLatencyRecorderWithTag g_bvar_ms_get_cluster_status("ms", "get_cluster_status");
BvarLatencyRecorderWithTag g_bvar_ms_set_cluster_status("ms", "set_cluster_status");

BvarLatencyRecorderWithTag g_bvar_ms_check_kv("ms", "check_kv");

// txn_kv's bvars
bvar::LatencyRecorder g_bvar_txn_kv_get("txn_kv", "get");
bvar::LatencyRecorder g_bvar_txn_kv_range_get("txn_kv", "range_get");
bvar::LatencyRecorder g_bvar_txn_kv_put("txn_kv", "put");
bvar::LatencyRecorder g_bvar_txn_kv_commit("txn_kv", "commit");
bvar::LatencyRecorder g_bvar_txn_kv_atomic_set_ver_key("txn_kv", "atomic_set_ver_key");
bvar::LatencyRecorder g_bvar_txn_kv_atomic_set_ver_value("txn_kv", "atomic_set_ver_value");
bvar::LatencyRecorder g_bvar_txn_kv_atomic_add("txn_kv", "atomic_add");
bvar::LatencyRecorder g_bvar_txn_kv_remove("txn_kv", "remove");
bvar::LatencyRecorder g_bvar_txn_kv_range_remove("txn_kv", "range_remove");
bvar::LatencyRecorder g_bvar_txn_kv_get_read_version("txn_kv", "get_read_version");
bvar::LatencyRecorder g_bvar_txn_kv_get_committed_version("txn_kv", "get_committed_version");
bvar::LatencyRecorder g_bvar_txn_kv_batch_get("txn_kv", "batch_get");

bvar::Adder<int64_t> g_bvar_txn_kv_commit_error_counter;
bvar::Window<bvar::Adder<int64_t> > g_bvar_txn_kv_commit_error_counter_minute(
        "txn_kv", "commit_error", &g_bvar_txn_kv_commit_error_counter, 60);

bvar::Adder<int64_t> g_bvar_txn_kv_commit_conflict_counter;
bvar::Window<bvar::Adder<int64_t> > g_bvar_txn_kv_commit_conflict_counter_minute(
        "txn_kv", "commit_conflict", &g_bvar_txn_kv_commit_conflict_counter, 60);

const int64_t BVAR_FDB_INVALID_VALUE = -99999999L;
bvar::Status<int64_t> g_bvar_fdb_client_count("fdb_client_count", BVAR_FDB_INVALID_VALUE);
bvar::Status<int64_t> g_bvar_fdb_configuration_coordinators_count(
        "fdb_configuration_coordinators_count", BVAR_FDB_INVALID_VALUE);
bvar::Status<int64_t> g_bvar_fdb_configuration_usable_regions("fdb_configuration_usable_regions",
                                                              BVAR_FDB_INVALID_VALUE);
bvar::Status<int64_t> g_bvar_fdb_coordinators_unreachable_count(
        "fdb_coordinators_unreachable_count", BVAR_FDB_INVALID_VALUE);
bvar::Status<int64_t> g_bvar_fdb_fault_tolerance_count("fdb_fault_tolerance_count",
                                                       BVAR_FDB_INVALID_VALUE);
bvar::Status<int64_t> g_bvar_fdb_data_average_partition_size_bytes(
        "fdb_data_average_partition_size_bytes", BVAR_FDB_INVALID_VALUE);
bvar::Status<int64_t> g_bvar_fdb_data_log_server_space_bytes("fdb_data_log_server_space_bytes",
                                                             BVAR_FDB_INVALID_VALUE);
bvar::Status<int64_t> g_bvar_fdb_data_moving_data_highest_priority(
        "fdb_data_moving_data_highest_priority", BVAR_FDB_INVALID_VALUE);
bvar::Status<int64_t> g_bvar_fdb_data_moving_data_in_flight_bytes(
        "fdb_data_moving_data_in_flight_bytes", BVAR_FDB_INVALID_VALUE);
bvar::Status<int64_t> g_bvar_fdb_data_moving_data_in_queue_bytes(
        "fdb_data_moving_data_in_queue_bytes", BVAR_FDB_INVALID_VALUE);
bvar::Status<int64_t> g_bvar_fdb_data_moving_total_written_bytes(
        "fdb_data_moving_total_written_bytes", BVAR_FDB_INVALID_VALUE);
bvar::Status<int64_t> g_bvar_fdb_data_partition_count("fdb_data_partition_count",
                                                      BVAR_FDB_INVALID_VALUE);
bvar::Status<int64_t> g_bvar_fdb_data_storage_server_space_bytes(
        "fdb_data_storage_server_space_bytes", BVAR_FDB_INVALID_VALUE);
bvar::Status<int64_t> g_bvar_fdb_data_state_min_replicas_remaining(
        "fdb_data_state_min_replicas_remaining", BVAR_FDB_INVALID_VALUE);
bvar::Status<int64_t> g_bvar_fdb_data_total_kv_size_bytes("fdb_data_total_kv_size_bytes",
                                                          BVAR_FDB_INVALID_VALUE);
bvar::Status<int64_t> g_bvar_fdb_data_total_disk_used_bytes("fdb_data_total_disk_used_bytes",
                                                            BVAR_FDB_INVALID_VALUE);
bvar::Status<int64_t> g_bvar_fdb_generation("fdb_generation", BVAR_FDB_INVALID_VALUE);
bvar::Status<int64_t> g_bvar_fdb_incompatible_connections("fdb_incompatible_connections",
                                                          BVAR_FDB_INVALID_VALUE);
bvar::Status<int64_t> g_bvar_fdb_latency_probe_transaction_start_ns(
        "fdb_latency_probe_transaction_start_ns", BVAR_FDB_INVALID_VALUE);
bvar::Status<int64_t> g_bvar_fdb_latency_probe_commit_ns("fdb_latency_probe_commit_ns",
                                                         BVAR_FDB_INVALID_VALUE);
bvar::Status<int64_t> g_bvar_fdb_latency_probe_read_ns("fdb_latency_probe_read_ns",
                                                       BVAR_FDB_INVALID_VALUE);
bvar::Status<int64_t> g_bvar_fdb_machines_count("fdb_machines_count", BVAR_FDB_INVALID_VALUE);
bvar::Status<int64_t> g_bvar_fdb_process_count("fdb_process_count", BVAR_FDB_INVALID_VALUE);
bvar::Status<int64_t> g_bvar_fdb_qos_worst_data_lag_storage_server_ns(
        "fdb_qos_worst_data_lag_storage_server_ns", BVAR_FDB_INVALID_VALUE);
bvar::Status<int64_t> g_bvar_fdb_qos_worst_durability_lag_storage_server_ns(
        "fdb_qos_worst_durability_lag_storage_server_ns", BVAR_FDB_INVALID_VALUE);
bvar::Status<int64_t> g_bvar_fdb_qos_worst_log_server_queue_bytes(
        "fdb_qos_worst_log_server_queue_bytes", BVAR_FDB_INVALID_VALUE);
bvar::Status<int64_t> g_bvar_fdb_qos_worst_storage_server_queue_bytes(
        "fdb_qos_worst_storage_server_queue_bytes", BVAR_FDB_INVALID_VALUE);
bvar::Status<int64_t> g_bvar_fdb_workload_conflict_rate_hz("fdb_workload_conflict_rate_hz",
                                                           BVAR_FDB_INVALID_VALUE);
bvar::Status<int64_t> g_bvar_fdb_workload_location_rate_hz("fdb_workload_location_rate_hz",
                                                           BVAR_FDB_INVALID_VALUE);
bvar::Status<int64_t> g_bvar_fdb_workload_keys_read_hz("fdb_workload_keys_read_hz",
                                                       BVAR_FDB_INVALID_VALUE);
bvar::Status<int64_t> g_bvar_fdb_workload_read_bytes_hz("fdb_workload_read_bytes_hz",
                                                        BVAR_FDB_INVALID_VALUE);
bvar::Status<int64_t> g_bvar_fdb_workload_read_rate_hz("fdb_workload_read_rate_hz",
                                                       BVAR_FDB_INVALID_VALUE);
bvar::Status<int64_t> g_bvar_fdb_workload_write_rate_hz("fdb_workload_write_rate_hz",
                                                        BVAR_FDB_INVALID_VALUE);
bvar::Status<int64_t> g_bvar_fdb_workload_written_bytes_hz("fdb_workload_written_bytes_hz",
                                                           BVAR_FDB_INVALID_VALUE);
bvar::Status<int64_t> g_bvar_fdb_workload_transactions_started_hz(
        "fdb_workload_transactions_started_hz", BVAR_FDB_INVALID_VALUE);
bvar::Status<int64_t> g_bvar_fdb_workload_transactions_committed_hz(
        "fdb_workload_transactions_committed_hz", BVAR_FDB_INVALID_VALUE);
bvar::Status<int64_t> g_bvar_fdb_workload_transactions_rejected_hz(
        "fdb_workload_transactions_rejected_hz", BVAR_FDB_INVALID_VALUE);

// checker's bvars
BvarStatusWithTag<long> g_bvar_checker_num_scanned("checker", "num_scanned");
BvarStatusWithTag<long> g_bvar_checker_num_scanned_with_segment("checker",
                                                                "num_scanned_with_segment");
BvarStatusWithTag<long> g_bvar_checker_num_check_failed("checker", "num_check_failed");
BvarStatusWithTag<long> g_bvar_checker_check_cost_s("checker", "check_cost_seconds");
BvarStatusWithTag<long> g_bvar_checker_enqueue_cost_s("checker", "enqueue_cost_seconds");
BvarStatusWithTag<long> g_bvar_checker_last_success_time_ms("checker", "last_success_time_ms");
BvarStatusWithTag<long> g_bvar_checker_instance_volume("checker", "instance_volume");
