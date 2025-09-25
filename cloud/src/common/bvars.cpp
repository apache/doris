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

#include <bvar/multi_dimension.h>
#include <bvar/reducer.h>
#include <bvar/status.h>
#include <bvar/window.h>

#include <cstdint>
#include <stdexcept>

// clang-format off

// meta-service's bvars
BvarLatencyRecorderWithTag g_bvar_ms_begin_txn("ms", "begin_txn");
BvarLatencyRecorderWithTag g_bvar_ms_precommit_txn("ms", "precommit_txn");
BvarLatencyRecorderWithTag g_bvar_ms_commit_txn("ms", "commit_txn");
BvarLatencyRecorderWithTag g_bvar_ms_commit_txn_eventually("ms", "commit_txn_eventually");
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
BvarLatencyRecorderWithTag g_bvar_ms_prepare_restore_job("ms", "prepare_restore_job");
BvarLatencyRecorderWithTag g_bvar_ms_commit_restore_job("ms", "commit_restore_job");
BvarLatencyRecorderWithTag g_bvar_ms_finish_restore_job("ms", "finish_restore_job");
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
BvarLatencyRecorderWithTag g_bvar_ms_get_delete_bitmap_update_lock("ms", "get_delete_bitmap_update_lock");
BvarLatencyRecorderWithTag g_bvar_ms_remove_delete_bitmap("ms", "remove_delete_bitmap");
BvarLatencyRecorderWithTag g_bvar_ms_remove_delete_bitmap_update_lock("ms", "remove_delete_bitmap_update_lock");
BvarLatencyRecorderWithTag g_bvar_ms_get_instance("ms", "get_instance");
BvarLatencyRecorderWithTag g_bvar_ms_get_rl_task_commit_attach("ms", "get_rl_task_commit_attach");
BvarLatencyRecorderWithTag g_bvar_ms_reset_rl_progress("ms", "reset_rl_progress");
BvarLatencyRecorderWithTag g_bvar_ms_get_txn_id("ms", "get_txn_id");
BvarLatencyRecorderWithTag g_bvar_ms_start_tablet_job("ms", "start_tablet_job");
BvarLatencyRecorderWithTag g_bvar_ms_finish_tablet_job("ms", "finish_tablet_job");
BvarLatencyRecorderWithTag g_bvar_ms_get_cluster_status("ms", "get_cluster_status");
BvarLatencyRecorderWithTag g_bvar_ms_set_cluster_status("ms", "set_cluster_status");
BvarLatencyRecorderWithTag g_bvar_ms_check_kv("ms", "check_kv");
BvarLatencyRecorderWithTag g_bvar_ms_get_schema_dict("ms", "get_schema_dict");
bvar::Adder<int64_t> g_bvar_update_delete_bitmap_fail_counter;
bvar::Window<bvar::Adder<int64_t> > g_bvar_update_delete_bitmap_fail_counter_minute("ms", "update_delete_bitmap_fail", &g_bvar_update_delete_bitmap_fail_counter, 60);
bvar::Adder<int64_t> g_bvar_get_delete_bitmap_fail_counter;
bvar::Window<bvar::Adder<int64_t> > g_bvar_get_delete_bitmap_fail_counter_minute("ms", "get_delete_bitmap_fail", &g_bvar_get_delete_bitmap_fail_counter, 60);

// recycler's bvars
// TODO: use mbvar for per instance, https://github.com/apache/brpc/blob/master/docs/cn/mbvar_c++.md
BvarStatusWithTag<int64_t> g_bvar_recycler_recycle_index_earlest_ts("recycler", "recycle_index_earlest_ts");
BvarStatusWithTag<int64_t> g_bvar_recycler_recycle_partition_earlest_ts("recycler", "recycle_partition_earlest_ts");
BvarStatusWithTag<int64_t> g_bvar_recycler_recycle_rowset_earlest_ts("recycler", "recycle_rowset_earlest_ts");
BvarStatusWithTag<int64_t> g_bvar_recycler_recycle_tmp_rowset_earlest_ts("recycler", "recycle_tmp_rowset_earlest_ts");
BvarStatusWithTag<int64_t> g_bvar_recycler_recycle_expired_txn_label_earlest_ts("recycler", "recycle_expired_txn_label_earlest_ts");
BvarStatusWithTag<int64_t> g_bvar_recycler_recycle_restore_job_earlest_ts("recycler", "recycle_restore_job_earlest_ts");
bvar::Status<int64_t> g_bvar_recycler_task_max_concurrency("recycler_task_max_concurrency_num",0);
// current status of recycle task (submitted, completed, error)
mBvarIntAdder g_bvar_recycler_instance_recycle_task_status("recycler_instance_recycle_task_status", { "status"});
// recycler's mbvars
// cost time of the last whole recycle process
mBvarStatus<int64_t> g_bvar_recycler_instance_last_round_recycle_duration("recycler_instance_last_round_recycle_duration",{"instance_id"});
mBvarStatus<int64_t> g_bvar_recycler_instance_next_ts("recycler_instance_next_ts",{"instance_id"});
// start and end timestamps of the recycle process
mBvarStatus<int64_t> g_bvar_recycler_instance_recycle_start_ts("recycler_instance_recycle_start_ts",{"instance_id"});
mBvarStatus<int64_t> g_bvar_recycler_instance_recycle_end_ts("recycler_instance_recycle_end_ts",{"instance_id"});
mBvarStatus<int64_t> g_bvar_recycler_instance_recycle_last_success_ts("recycler_instance_recycle_last_success_ts",{"instance_id"});

// recycler's mbvars
// instance_id: unique identifier for the instance
// resource_id: unique identifier for the repository
// status: status of the recycle task (submitted, completed, error)
mBvarIntAdder g_bvar_recycler_vault_recycle_task_status("recycler_vault_recycle_task_status", {"instance_id", "resource_id", "status"});
// current concurrency of vault delete task
mBvarStatus<int64_t> g_bvar_recycler_instance_last_round_recycled_num("recycler_instance_last_round_recycled_num", {"instance_id", "resource_type"});
mBvarStatus<int64_t> g_bvar_recycler_instance_last_round_to_recycle_num("recycler_instance_last_round_to_recycle_num", {"instance_id", "resource_type"});
mBvarStatus<int64_t> g_bvar_recycler_instance_last_round_recycled_bytes("recycler_instance_last_round_recycled_bytes", {"instance_id", "resource_type"});
mBvarStatus<int64_t> g_bvar_recycler_instance_last_round_to_recycle_bytes("recycler_instance_last_round_to_recycle_bytes", {"instance_id", "resource_type"});
mBvarStatus<double> g_bvar_recycler_instance_last_round_recycle_elpased_ts("recycler_instance_last_round_recycle_elpased_ts", {"instance_id", "resource_type"});
// total recycled num and bytes of resources since recycler started
mBvarInt64Adder g_bvar_recycler_instance_recycle_total_num_since_started("recycler_instance_recycle_total_num_since_started", {"instance_id", "resource_type"});
mBvarInt64Adder g_bvar_recycler_instance_recycle_total_bytes_since_started("recycler_instance_recycle_total_bytes_since_started", {"instance_id", "resource_type"});
mBvarIntAdder g_bvar_recycler_instance_recycle_round("recycler_instance_recycle_round", {"instance_id", "resource_type"});
// represents the ms required per resource to be recycled
// value of -1 means no resource recycled
mBvarStatus<double> g_bvar_recycler_instance_recycle_time_per_resource("recycler_instance_recycle_time_per_resource", {"instance_id", "resource_type"});
// represents the bytes of resources that can be recycled per ms
mBvarStatus<double> g_bvar_recycler_instance_recycle_bytes_per_ms("recycler_instance_recycle_bytes_per_ms", {"instance_id", "resource_type"});

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
bvar::Adder<int64_t> g_bvar_txn_kv_get_count_normalized("txn_kv", "get_count_normalized");
bvar::Adder<int64_t> g_bvar_txn_kv_commit_error_counter;
bvar::Window<bvar::Adder<int64_t> > g_bvar_txn_kv_commit_error_counter_minute("txn_kv", "commit_error", &g_bvar_txn_kv_commit_error_counter, 60);
bvar::Adder<int64_t> g_bvar_txn_kv_commit_conflict_counter;
bvar::Window<bvar::Adder<int64_t> > g_bvar_txn_kv_commit_conflict_counter_minute("txn_kv", "commit_conflict", &g_bvar_txn_kv_commit_conflict_counter, 60);
bvar::Adder<int64_t> g_bvar_delete_bitmap_lock_txn_put_conflict_counter;
bvar::Window<bvar::Adder<int64_t> > g_bvar_delete_bitmap_lock_txn_put_conflict_counter_minute("delete_bitmap_lock", "txn_put_conflict", &g_bvar_delete_bitmap_lock_txn_put_conflict_counter, 60);
bvar::Adder<int64_t> g_bvar_delete_bitmap_lock_txn_remove_conflict_by_fail_counter;
bvar::Window<bvar::Adder<int64_t> > g_bvar_delete_bitmap_lock_txn_remove_conflict_by_fail_counter_minute("delete_bitmap_lock", "txn_remove_conflict_by_fail", &g_bvar_delete_bitmap_lock_txn_remove_conflict_by_fail_counter, 60);
bvar::Adder<int64_t> g_bvar_delete_bitmap_lock_txn_remove_conflict_by_load_counter;
bvar::Window<bvar::Adder<int64_t> > g_bvar_delete_bitmap_lock_txn_remove_conflict_by_load_counter_minute("delete_bitmap_lock", "txn_remove_conflict_by_load", &g_bvar_delete_bitmap_lock_txn_remove_conflict_by_load_counter, 60);
bvar::Adder<int64_t> g_bvar_delete_bitmap_lock_txn_remove_conflict_by_compaction_commit_counter;
bvar::Window<bvar::Adder<int64_t> > g_bvar_delete_bitmap_lock_txn_remove_conflict_by_compaction_commit_counter_minute("delete_bitmap_lock", "txn_remove_conflict_by_compaction_commit", &g_bvar_delete_bitmap_lock_txn_remove_conflict_by_compaction_commit_counter, 60);
bvar::Adder<int64_t> g_bvar_delete_bitmap_lock_txn_remove_conflict_by_compaction_lease_counter;
bvar::Window<bvar::Adder<int64_t> > g_bvar_delete_bitmap_lock_txn_remove_conflict_by_compaction_lease_counter_minute("delete_bitmap_lock", "txn_remove_conflict_by_compaction_lease", &g_bvar_delete_bitmap_lock_txn_remove_conflict_by_compaction_lease_counter, 60);
bvar::Adder<int64_t> g_bvar_delete_bitmap_lock_txn_remove_conflict_by_compaction_abort_counter;
bvar::Window<bvar::Adder<int64_t> > g_bvar_delete_bitmap_lock_txn_remove_conflict_by_compaction_abort_counter_minute("delete_bitmap_lock", "txn_remove_conflict_by_compaction_abort", &g_bvar_delete_bitmap_lock_txn_remove_conflict_by_compaction_abort_counter, 60);

// fdb's bvars
const int64_t BVAR_FDB_INVALID_VALUE = -99999999L;
bvar::Status<int64_t> g_bvar_fdb_client_count("fdb_client_count", BVAR_FDB_INVALID_VALUE);
bvar::Status<int64_t> g_bvar_fdb_configuration_coordinators_count("fdb_configuration_coordinators_count", BVAR_FDB_INVALID_VALUE);
bvar::Status<int64_t> g_bvar_fdb_configuration_usable_regions("fdb_configuration_usable_regions", BVAR_FDB_INVALID_VALUE);
bvar::Status<int64_t> g_bvar_fdb_coordinators_unreachable_count("fdb_coordinators_unreachable_count", BVAR_FDB_INVALID_VALUE);
bvar::Status<int64_t> g_bvar_fdb_fault_tolerance_count("fdb_fault_tolerance_count", BVAR_FDB_INVALID_VALUE);
bvar::Status<int64_t> g_bvar_fdb_data_average_partition_size_bytes("fdb_data_average_partition_size_bytes", BVAR_FDB_INVALID_VALUE);
bvar::Status<int64_t> g_bvar_fdb_data_log_server_space_bytes("fdb_data_log_server_space_bytes", BVAR_FDB_INVALID_VALUE);
bvar::Status<int64_t> g_bvar_fdb_data_moving_data_highest_priority("fdb_data_moving_data_highest_priority", BVAR_FDB_INVALID_VALUE);
bvar::Status<int64_t> g_bvar_fdb_data_moving_data_in_flight_bytes("fdb_data_moving_data_in_flight_bytes", BVAR_FDB_INVALID_VALUE);
bvar::Status<int64_t> g_bvar_fdb_data_moving_data_in_queue_bytes("fdb_data_moving_data_in_queue_bytes", BVAR_FDB_INVALID_VALUE);
bvar::Status<int64_t> g_bvar_fdb_data_moving_total_written_bytes("fdb_data_moving_total_written_bytes", BVAR_FDB_INVALID_VALUE);
bvar::Status<int64_t> g_bvar_fdb_data_partition_count("fdb_data_partition_count", BVAR_FDB_INVALID_VALUE);
bvar::Status<int64_t> g_bvar_fdb_data_storage_server_space_bytes("fdb_data_storage_server_space_bytes", BVAR_FDB_INVALID_VALUE);
bvar::Status<int64_t> g_bvar_fdb_data_state_min_replicas_remaining("fdb_data_state_min_replicas_remaining", BVAR_FDB_INVALID_VALUE);
bvar::Status<int64_t> g_bvar_fdb_data_total_kv_size_bytes("fdb_data_total_kv_size_bytes", BVAR_FDB_INVALID_VALUE);
bvar::Status<int64_t> g_bvar_fdb_data_total_disk_used_bytes("fdb_data_total_disk_used_bytes", BVAR_FDB_INVALID_VALUE);
bvar::Status<int64_t> g_bvar_fdb_generation("fdb_generation", BVAR_FDB_INVALID_VALUE);
bvar::Status<int64_t> g_bvar_fdb_incompatible_connections("fdb_incompatible_connections", BVAR_FDB_INVALID_VALUE);
bvar::Status<int64_t> g_bvar_fdb_latency_probe_transaction_start_ns("fdb_latency_probe_transaction_start_ns", BVAR_FDB_INVALID_VALUE);
bvar::Status<int64_t> g_bvar_fdb_latency_probe_commit_ns("fdb_latency_probe_commit_ns", BVAR_FDB_INVALID_VALUE);
bvar::Status<int64_t> g_bvar_fdb_latency_probe_read_ns("fdb_latency_probe_read_ns", BVAR_FDB_INVALID_VALUE);
bvar::Status<int64_t> g_bvar_fdb_machines_count("fdb_machines_count", BVAR_FDB_INVALID_VALUE);
bvar::Status<int64_t> g_bvar_fdb_process_count("fdb_process_count", BVAR_FDB_INVALID_VALUE);
bvar::Status<int64_t> g_bvar_fdb_qos_worst_data_lag_storage_server_ns("fdb_qos_worst_data_lag_storage_server_ns", BVAR_FDB_INVALID_VALUE);
bvar::Status<int64_t> g_bvar_fdb_qos_worst_durability_lag_storage_server_ns("fdb_qos_worst_durability_lag_storage_server_ns", BVAR_FDB_INVALID_VALUE);
bvar::Status<int64_t> g_bvar_fdb_qos_worst_log_server_queue_bytes("fdb_qos_worst_log_server_queue_bytes", BVAR_FDB_INVALID_VALUE);
bvar::Status<int64_t> g_bvar_fdb_qos_worst_storage_server_queue_bytes("fdb_qos_worst_storage_server_queue_bytes", BVAR_FDB_INVALID_VALUE);
bvar::Status<int64_t> g_bvar_fdb_workload_conflict_rate_hz("fdb_workload_conflict_rate_hz", BVAR_FDB_INVALID_VALUE);
bvar::Status<int64_t> g_bvar_fdb_workload_location_rate_hz("fdb_workload_location_rate_hz", BVAR_FDB_INVALID_VALUE);
bvar::Status<int64_t> g_bvar_fdb_workload_keys_read_hz("fdb_workload_keys_read_hz", BVAR_FDB_INVALID_VALUE);
bvar::Status<int64_t> g_bvar_fdb_workload_read_bytes_hz("fdb_workload_read_bytes_hz", BVAR_FDB_INVALID_VALUE);
bvar::Status<int64_t> g_bvar_fdb_workload_read_rate_hz("fdb_workload_read_rate_hz", BVAR_FDB_INVALID_VALUE);
bvar::Status<int64_t> g_bvar_fdb_workload_write_rate_hz("fdb_workload_write_rate_hz", BVAR_FDB_INVALID_VALUE);
bvar::Status<int64_t> g_bvar_fdb_workload_written_bytes_hz("fdb_workload_written_bytes_hz", BVAR_FDB_INVALID_VALUE);
bvar::Status<int64_t> g_bvar_fdb_workload_transactions_started_hz("fdb_workload_transactions_started_hz", BVAR_FDB_INVALID_VALUE);
bvar::Status<int64_t> g_bvar_fdb_workload_transactions_committed_hz("fdb_workload_transactions_committed_hz", BVAR_FDB_INVALID_VALUE);
bvar::Status<int64_t> g_bvar_fdb_workload_transactions_rejected_hz("fdb_workload_transactions_rejected_hz", BVAR_FDB_INVALID_VALUE);
bvar::Status<int64_t> g_bvar_fdb_client_thread_busyness_percent("fdb_client_thread_busyness_percent", BVAR_FDB_INVALID_VALUE);
mBvarStatus<int64_t> g_bvar_fdb_process_status_int("fdb_process_status_int", {"process_id", "component", "metric"});
mBvarStatus<double> g_bvar_fdb_process_status_float("fdb_process_status_float", {"process_id", "component", "metric"});

// checker's bvars
BvarStatusWithTag<int64_t> g_bvar_checker_num_scanned("checker", "num_scanned");
BvarStatusWithTag<int64_t> g_bvar_checker_num_scanned_with_segment("checker", "num_scanned_with_segment");
BvarStatusWithTag<int64_t> g_bvar_checker_num_check_failed("checker", "num_check_failed");
BvarStatusWithTag<int64_t> g_bvar_checker_check_cost_s("checker", "check_cost_seconds");
BvarStatusWithTag<int64_t> g_bvar_checker_enqueue_cost_s("checker", "enqueue_cost_seconds");
BvarStatusWithTag<int64_t> g_bvar_checker_last_success_time_ms("checker", "last_success_time_ms");
BvarStatusWithTag<int64_t> g_bvar_checker_instance_volume("checker", "instance_volume");
BvarStatusWithTag<int64_t> g_bvar_inverted_checker_num_scanned("checker", "num_inverted_scanned");
BvarStatusWithTag<int64_t> g_bvar_inverted_checker_num_check_failed("checker", "num_inverted_check_failed");
BvarStatusWithTag<int64_t> g_bvar_inverted_checker_leaked_delete_bitmaps("checker", "leaked_delete_bitmaps");
BvarStatusWithTag<int64_t> g_bvar_inverted_checker_abnormal_delete_bitmaps("checker", "abnormal_delete_bitmaps");
BvarStatusWithTag<int64_t> g_bvar_inverted_checker_delete_bitmaps_scanned("checker", "delete_bitmap_keys_scanned");
BvarStatusWithTag<int64_t> g_bvar_max_rowsets_with_useless_delete_bitmap_version("checker", "max_rowsets_with_useless_delete_bitmap_version");
BvarStatusWithTag<int64_t> g_bvar_checker_restore_job_prepared_state("checker", "restore_job_prepared_state");
BvarStatusWithTag<int64_t> g_bvar_checker_restore_job_committed_state("checker", "restore_job_committed_state");
BvarStatusWithTag<int64_t> g_bvar_checker_restore_job_dropped_state("checker", "restore_job_dropped_state");
BvarStatusWithTag<int64_t> g_bvar_checker_restore_job_completed_state("checker", "restore_job_completed_state");
BvarStatusWithTag<int64_t> g_bvar_checker_restore_job_recycling_state("checker", "restore_job_recycling_state");
BvarStatusWithTag<int64_t> g_bvar_checker_restore_job_cost_many_time("checker", "restore_job_cost_many_time");

// rpc kv rw count
// get_rowset
mBvarInt64Adder g_bvar_rpc_kv_get_rowset_get_counter("rpc_kv_get_rowset_get_counter",{"instance_id"});
// get_version
mBvarInt64Adder g_bvar_rpc_kv_get_version_get_counter("rpc_kv_get_version_get_counter",{"instance_id"});
// get_schema_dict
mBvarInt64Adder g_bvar_rpc_kv_get_schema_dict_get_counter("rpc_kv_get_schema_dict_get_counter",{"instance_id"});
// create_tablets
mBvarInt64Adder g_bvar_rpc_kv_create_tablets_get_counter("rpc_kv_create_tablets_get_counter",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_create_tablets_put_counter("rpc_kv_create_tablets_put_counter",{"instance_id"});
// update_tablet
mBvarInt64Adder g_bvar_rpc_kv_update_tablet_get_counter("rpc_kv_update_tablet_get_counter",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_update_tablet_put_counter("rpc_kv_update_tablet_put_counter",{"instance_id"});
// get_tablet
mBvarInt64Adder g_bvar_rpc_kv_get_tablet_get_counter("rpc_kv_get_tablet_get_counter",{"instance_id"});
// prepare_rowset
mBvarInt64Adder g_bvar_rpc_kv_prepare_rowset_get_counter("rpc_kv_prepare_rowset_get_counter",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_prepare_rowset_put_counter("rpc_kv_prepare_rowset_put_counter",{"instance_id"});
// commit_rowset
mBvarInt64Adder g_bvar_rpc_kv_commit_rowset_get_counter("rpc_kv_commit_rowset_get_counter",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_commit_rowset_put_counter("rpc_kv_commit_rowset_put_counter",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_commit_rowset_del_counter("rpc_kv_commit_rowset_del_counter",{"instance_id"});
// update_tmp_rowset
mBvarInt64Adder g_bvar_rpc_kv_update_tmp_rowset_get_counter("rpc_kv_update_tmp_rowset_get_counter",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_update_tmp_rowset_put_counter("rpc_kv_update_tmp_rowset_put_counter",{"instance_id"});
// get_tablet_stats
mBvarInt64Adder g_bvar_rpc_kv_get_tablet_stats_get_counter("rpc_kv_get_tablet_stats_get_counter",{"instance_id"});
// update_delete_bitmap
mBvarInt64Adder g_bvar_rpc_kv_update_delete_bitmap_get_counter("rpc_kv_update_delete_bitmap_get_counter",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_update_delete_bitmap_put_counter("rpc_kv_update_delete_bitmap_put_counter",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_update_delete_bitmap_del_counter("rpc_kv_update_delete_bitmap_del_counter",{"instance_id"});
// get_delete_bitmap
mBvarInt64Adder g_bvar_rpc_kv_get_delete_bitmap_get_counter("rpc_kv_get_delete_bitmap_get_counter",{"instance_id"});
// get_delete_bitmap_update_lock
mBvarInt64Adder g_bvar_rpc_kv_get_delete_bitmap_update_lock_get_counter("rpc_kv_get_delete_bitmap_update_lock_get_counter",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_get_delete_bitmap_update_lock_put_counter("rpc_kv_get_delete_bitmap_update_lock_put_counter",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_get_delete_bitmap_update_lock_del_counter("rpc_kv_get_delete_bitmap_update_lock_del_counter",{"instance_id"});
// remove_delete_bitmap_update_lock
mBvarInt64Adder g_bvar_rpc_kv_remove_delete_bitmap_update_lock_get_counter("rpc_kv_remove_delete_bitmap_update_lock_get_counter",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_remove_delete_bitmap_update_lock_put_counter("rpc_kv_remove_delete_bitmap_update_lock_put_counter",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_remove_delete_bitmap_update_lock_del_counter("rpc_kv_remove_delete_bitmap_update_lock_del_counter",{"instance_id"});
// remove_delete_bitmap
mBvarInt64Adder g_bvar_rpc_kv_remove_delete_bitmap_del_counter("rpc_kv_remove_delete_bitmap_del_counter",{"instance_id"});
// start_tablet_job
mBvarInt64Adder g_bvar_rpc_kv_start_tablet_job_get_counter("rpc_kv_start_tablet_job_get_counter",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_start_tablet_job_put_counter("rpc_kv_start_tablet_job_put_counter",{"instance_id"});
// finish_tablet_job
mBvarInt64Adder g_bvar_rpc_kv_finish_tablet_job_get_counter("rpc_kv_finish_tablet_job_get_counter",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_finish_tablet_job_put_counter("rpc_kv_finish_tablet_job_put_counter",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_finish_tablet_job_del_counter("rpc_kv_finish_tablet_job_del_counter",{"instance_id"});
// prepare_index
mBvarInt64Adder g_bvar_rpc_kv_prepare_index_get_counter("rpc_kv_prepare_index_get_counter",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_prepare_index_put_counter("rpc_kv_prepare_index_put_counter",{"instance_id"});
// commit_index
mBvarInt64Adder g_bvar_rpc_kv_commit_index_get_counter("rpc_kv_commit_index_get_counter",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_commit_index_put_counter("rpc_kv_commit_index_put_counter",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_commit_index_del_counter("rpc_kv_commit_index_del_counter",{"instance_id"});
// drop_index
mBvarInt64Adder g_bvar_rpc_kv_drop_index_get_counter("rpc_kv_drop_index_get_counter",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_drop_index_put_counter("rpc_kv_drop_index_put_counter",{"instance_id"});
// prepare_partition
mBvarInt64Adder g_bvar_rpc_kv_prepare_partition_get_counter("rpc_kv_prepare_partition_get_counter",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_prepare_partition_put_counter("rpc_kv_prepare_partition_put_counter",{"instance_id"});
// commit_partition
mBvarInt64Adder g_bvar_rpc_kv_commit_partition_get_counter("rpc_kv_commit_partition_get_counter",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_commit_partition_put_counter("rpc_kv_commit_partition_put_counter",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_commit_partition_del_counter("rpc_kv_commit_partition_del_counter",{"instance_id"});
// drop_partition
mBvarInt64Adder g_bvar_rpc_kv_drop_partition_get_counter("rpc_kv_drop_partition_get_counter",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_drop_partition_put_counter("rpc_kv_drop_partition_put_counter",{"instance_id"});
// check_kv
mBvarInt64Adder g_bvar_rpc_kv_check_kv_get_counter("rpc_kv_check_kv_get_counter",{"instance_id"});
// get_obj_store_info
mBvarInt64Adder g_bvar_rpc_kv_get_obj_store_info_get_counter("rpc_kv_get_obj_store_info_get_counter",{"instance_id"});
// alter_storage_vault
mBvarInt64Adder g_bvar_rpc_kv_alter_storage_vault_get_counter("rpc_kv_alter_storage_vault_get_counter",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_alter_storage_vault_put_counter("rpc_kv_alter_storage_vault_put_counter",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_alter_storage_vault_del_counter("rpc_kv_alter_storage_vault_del_counter",{"instance_id"});
// alter_obj_store_info
mBvarInt64Adder g_bvar_rpc_kv_alter_obj_store_info_get_counter("rpc_kv_alter_obj_store_info_get_counter",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_alter_obj_store_info_put_counter("rpc_kv_alter_obj_store_info_put_counter",{"instance_id"});
// update_ak_sk
mBvarInt64Adder g_bvar_rpc_kv_update_ak_sk_get_counter("rpc_kv_update_ak_sk_get_counter",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_update_ak_sk_put_counter("rpc_kv_update_ak_sk_put_counter",{"instance_id"});
// create_instance
mBvarInt64Adder g_bvar_rpc_kv_create_instance_get_counter("rpc_kv_create_instance_get_counter",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_create_instance_put_counter("rpc_kv_create_instance_put_counter",{"instance_id"});
// get_instance
mBvarInt64Adder g_bvar_rpc_kv_get_instance_get_counter("rpc_kv_get_instance_get_counter",{"instance_id"});
// alter_cluster
mBvarInt64Adder g_bvar_rpc_kv_alter_cluster_get_counter("rpc_kv_alter_cluster_get_counter",{"instance_id"});
// get_cluster
mBvarInt64Adder g_bvar_rpc_kv_get_cluster_get_counter("rpc_kv_get_cluster_get_counter",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_get_cluster_put_counter("rpc_kv_get_cluster_put_counter",{"instance_id"});
// create_stage
mBvarInt64Adder g_bvar_rpc_kv_create_stage_get_counter("rpc_kv_create_stage_get_counter",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_create_stage_put_counter("rpc_kv_create_stage_put_counter",{"instance_id"});
// get_stage
mBvarInt64Adder g_bvar_rpc_kv_get_stage_get_counter("rpc_kv_get_stage_get_counter",{"instance_id"});
// get_iam
mBvarInt64Adder g_bvar_rpc_kv_get_iam_get_counter("rpc_kv_get_iam_get_counter",{"instance_id"});
// alter_iam
mBvarInt64Adder g_bvar_rpc_kv_alter_iam_get_counter("rpc_kv_alter_iam_get_counter",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_alter_iam_put_counter("rpc_kv_alter_iam_put_counter",{"instance_id"});
// alter_ram_user
mBvarInt64Adder g_bvar_rpc_kv_alter_ram_user_get_counter("rpc_kv_alter_ram_user_get_counter",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_alter_ram_user_put_counter("rpc_kv_alter_ram_user_put_counter",{"instance_id"});
// begin_copy
mBvarInt64Adder g_bvar_rpc_kv_begin_copy_get_counter("rpc_kv_begin_copy_get_counter",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_begin_copy_put_counter("rpc_kv_begin_copy_put_counter",{"instance_id"});
// finish_copy
mBvarInt64Adder g_bvar_rpc_kv_finish_copy_get_counter("rpc_kv_finish_copy_get_counter",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_finish_copy_put_counter("rpc_kv_finish_copy_put_counter",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_finish_copy_del_counter("rpc_kv_finish_copy_del_counter",{"instance_id"});
// get_copy_job
mBvarInt64Adder g_bvar_rpc_kv_get_copy_job_get_counter("rpc_kv_get_copy_job_get_counter",{"instance_id"});
// get_copy_files
mBvarInt64Adder g_bvar_rpc_kv_get_copy_files_get_counter("rpc_kv_get_copy_files_get_counter",{"instance_id"});
// filter_copy_files
mBvarInt64Adder g_bvar_rpc_kv_filter_copy_files_get_counter("rpc_kv_filter_copy_files_get_counter",{"instance_id"});
// get_cluster_status
mBvarInt64Adder g_bvar_rpc_kv_get_cluster_status_get_counter("rpc_kv_get_cluster_status_get_counter",{"instance_id"});
// begin_txn
mBvarInt64Adder g_bvar_rpc_kv_begin_txn_get_counter("rpc_kv_begin_txn_get_counter",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_begin_txn_put_counter("rpc_kv_begin_txn_put_counter",{"instance_id"});
// precommit_txn
mBvarInt64Adder g_bvar_rpc_kv_precommit_txn_get_counter("rpc_kv_precommit_txn_get_counter",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_precommit_txn_put_counter("rpc_kv_precommit_txn_put_counter",{"instance_id"});
// get_rl_task_commit_attach
mBvarInt64Adder g_bvar_rpc_kv_get_rl_task_commit_attach_get_counter("rpc_kv_get_rl_task_commit_attach_get_counter",{"instance_id"});
// reset_rl_progress
mBvarInt64Adder g_bvar_rpc_kv_reset_rl_progress_get_counter("rpc_kv_reset_rl_progress_get_counter",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_reset_rl_progress_put_counter("rpc_kv_reset_rl_progress_put_counter",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_reset_rl_progress_del_counter("rpc_kv_reset_rl_progress_del_counter",{"instance_id"});
// commit_txn
mBvarInt64Adder g_bvar_rpc_kv_commit_txn_get_counter("rpc_kv_commit_txn_get_counter",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_commit_txn_put_counter("rpc_kv_commit_txn_put_counter",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_commit_txn_del_counter("rpc_kv_commit_txn_del_counter",{"instance_id"});
// abort_txn
mBvarInt64Adder g_bvar_rpc_kv_abort_txn_get_counter("rpc_kv_abort_txn_get_counter",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_abort_txn_put_counter("rpc_kv_abort_txn_put_counter",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_abort_txn_del_counter("rpc_kv_abort_txn_del_counter",{"instance_id"});
// get_txn
mBvarInt64Adder g_bvar_rpc_kv_get_txn_get_counter("rpc_kv_get_txn_get_counter",{"instance_id"});
// get_current_max_txn_id
mBvarInt64Adder g_bvar_rpc_kv_get_current_max_txn_id_get_counter("rpc_kv_get_current_max_txn_id_get_counter",{"instance_id"});
// begin_sub_txn
mBvarInt64Adder g_bvar_rpc_kv_begin_sub_txn_get_counter("rpc_kv_begin_sub_txn_get_counter",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_begin_sub_txn_put_counter("rpc_kv_begin_sub_txn_put_counter",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_begin_sub_txn_del_counter("rpc_kv_begin_sub_txn_del_counter",{"instance_id"});
// abort_sub_txn
mBvarInt64Adder g_bvar_rpc_kv_abort_sub_txn_get_counter("rpc_kv_abort_sub_txn_get_counter",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_abort_sub_txn_put_counter("rpc_kv_abort_sub_txn_put_counter",{"instance_id"});
// abort_txn_with_coordinator
mBvarInt64Adder g_bvar_rpc_kv_abort_txn_with_coordinator_get_counter("rpc_kv_abort_txn_with_coordinator_get_counter",{"instance_id"});
// check_txn_conflict
mBvarInt64Adder g_bvar_rpc_kv_check_txn_conflict_get_counter("rpc_kv_check_txn_conflict_get_counter",{"instance_id"});
// clean_txn_label
mBvarInt64Adder g_bvar_rpc_kv_clean_txn_label_get_counter("rpc_kv_clean_txn_label_get_counter",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_clean_txn_label_put_counter("rpc_kv_clean_txn_label_put_counter",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_clean_txn_label_del_counter("rpc_kv_clean_txn_label_del_counter",{"instance_id"});
// get_txn_id
mBvarInt64Adder g_bvar_rpc_kv_get_txn_id_get_counter("rpc_kv_get_txn_id_get_counter",{"instance_id"});
// bytes
// get_rowset
mBvarInt64Adder g_bvar_rpc_kv_get_rowset_get_bytes("rpc_kv_get_rowset_get_bytes",{"instance_id"});
// get_version
mBvarInt64Adder g_bvar_rpc_kv_get_version_get_bytes("rpc_kv_get_version_get_bytes",{"instance_id"});
// get_schema_dict
mBvarInt64Adder g_bvar_rpc_kv_get_schema_dict_get_bytes("rpc_kv_get_schema_dict_get_bytes",{"instance_id"});
// create_tablets
mBvarInt64Adder g_bvar_rpc_kv_create_tablets_get_bytes("rpc_kv_create_tablets_get_bytes",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_create_tablets_put_bytes("rpc_kv_create_tablets_put_bytes",{"instance_id"});
// update_tablet
mBvarInt64Adder g_bvar_rpc_kv_update_tablet_get_bytes("rpc_kv_update_tablet_get_bytes",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_update_tablet_put_bytes("rpc_kv_update_tablet_put_bytes",{"instance_id"});
// get_tablet
mBvarInt64Adder g_bvar_rpc_kv_get_tablet_get_bytes("rpc_kv_get_tablet_get_bytes",{"instance_id"});
// prepare_rowset
mBvarInt64Adder g_bvar_rpc_kv_prepare_rowset_get_bytes("rpc_kv_prepare_rowset_get_bytes",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_prepare_rowset_put_bytes("rpc_kv_prepare_rowset_put_bytes",{"instance_id"});
// commit_rowset
mBvarInt64Adder g_bvar_rpc_kv_commit_rowset_get_bytes("rpc_kv_commit_rowset_get_bytes",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_commit_rowset_put_bytes("rpc_kv_commit_rowset_put_bytes",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_commit_rowset_del_bytes("rpc_kv_commit_rowset_del_bytes",{"instance_id"});
// update_tmp_rowset
mBvarInt64Adder g_bvar_rpc_kv_update_tmp_rowset_get_bytes("rpc_kv_update_tmp_rowset_get_bytes",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_update_tmp_rowset_put_bytes("rpc_kv_update_tmp_rowset_put_bytes",{"instance_id"});
// get_tablet_stats
mBvarInt64Adder g_bvar_rpc_kv_get_tablet_stats_get_bytes("rpc_kv_get_tablet_stats_get_bytes",{"instance_id"});
// update_delete_bitmap
mBvarInt64Adder g_bvar_rpc_kv_update_delete_bitmap_get_bytes("rpc_kv_update_delete_bitmap_get_bytes",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_update_delete_bitmap_put_bytes("rpc_kv_update_delete_bitmap_put_bytes",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_update_delete_bitmap_del_bytes("rpc_kv_update_delete_bitmap_del_bytes",{"instance_id"});
// get_delete_bitmap
mBvarInt64Adder g_bvar_rpc_kv_get_delete_bitmap_get_bytes("rpc_kv_get_delete_bitmap_get_bytes",{"instance_id"});
// get_delete_bitmap_update_lock
mBvarInt64Adder g_bvar_rpc_kv_get_delete_bitmap_update_lock_get_bytes("rpc_kv_get_delete_bitmap_update_lock_get_bytes",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_get_delete_bitmap_update_lock_put_bytes("rpc_kv_get_delete_bitmap_update_lock_put_bytes",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_get_delete_bitmap_update_lock_del_bytes("rpc_kv_get_delete_bitmap_update_lock_del_bytes",{"instance_id"});
// remove_delete_bitmap_update_lock
mBvarInt64Adder g_bvar_rpc_kv_remove_delete_bitmap_update_lock_get_bytes("rpc_kv_remove_delete_bitmap_update_lock_get_bytes",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_remove_delete_bitmap_update_lock_put_bytes("rpc_kv_remove_delete_bitmap_update_lock_put_bytes",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_remove_delete_bitmap_update_lock_del_bytes("rpc_kv_remove_delete_bitmap_update_lock_del_bytes",{"instance_id"});
// remove_delete_bitmap
mBvarInt64Adder g_bvar_rpc_kv_remove_delete_bitmap_del_bytes("rpc_kv_remove_delete_bitmap_del_bytes",{"instance_id"});
// start_tablet_job
mBvarInt64Adder g_bvar_rpc_kv_start_tablet_job_get_bytes("rpc_kv_start_tablet_job_get_bytes",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_start_tablet_job_put_bytes("rpc_kv_start_tablet_job_put_bytes",{"instance_id"});
// finish_tablet_job
mBvarInt64Adder g_bvar_rpc_kv_finish_tablet_job_get_bytes("rpc_kv_finish_tablet_job_get_bytes",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_finish_tablet_job_put_bytes("rpc_kv_finish_tablet_job_put_bytes",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_finish_tablet_job_del_bytes("rpc_kv_finish_tablet_job_del_bytes",{"instance_id"});
// prepare_index
mBvarInt64Adder g_bvar_rpc_kv_prepare_index_get_bytes("rpc_kv_prepare_index_get_bytes",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_prepare_index_put_bytes("rpc_kv_prepare_index_put_bytes",{"instance_id"});
// commit_index
mBvarInt64Adder g_bvar_rpc_kv_commit_index_get_bytes("rpc_kv_commit_index_get_bytes",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_commit_index_put_bytes("rpc_kv_commit_index_put_bytes",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_commit_index_del_bytes("rpc_kv_commit_index_del_bytes",{"instance_id"});
// drop_index
mBvarInt64Adder g_bvar_rpc_kv_drop_index_get_bytes("rpc_kv_drop_index_get_bytes",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_drop_index_put_bytes("rpc_kv_drop_index_put_bytes",{"instance_id"});
// prepare_partition
mBvarInt64Adder g_bvar_rpc_kv_prepare_partition_get_bytes("rpc_kv_prepare_partition_get_bytes",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_prepare_partition_put_bytes("rpc_kv_prepare_partition_put_bytes",{"instance_id"});
// commit_partition
mBvarInt64Adder g_bvar_rpc_kv_commit_partition_get_bytes("rpc_kv_commit_partition_get_bytes",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_commit_partition_put_bytes("rpc_kv_commit_partition_put_bytes",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_commit_partition_del_bytes("rpc_kv_commit_partition_del_bytes",{"instance_id"});
// drop_partition
mBvarInt64Adder g_bvar_rpc_kv_drop_partition_get_bytes("rpc_kv_drop_partition_get_bytes",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_drop_partition_put_bytes("rpc_kv_drop_partition_put_bytes",{"instance_id"});
// check_kv
mBvarInt64Adder g_bvar_rpc_kv_check_kv_get_bytes("rpc_kv_check_kv_get_bytes",{"instance_id"});
// get_obj_store_info
mBvarInt64Adder g_bvar_rpc_kv_get_obj_store_info_get_bytes("rpc_kv_get_obj_store_info_get_bytes",{"instance_id"});
// alter_storage_vault
mBvarInt64Adder g_bvar_rpc_kv_alter_storage_vault_get_bytes("rpc_kv_alter_storage_vault_get_bytes",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_alter_storage_vault_put_bytes("rpc_kv_alter_storage_vault_put_bytes",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_alter_storage_vault_del_bytes("rpc_kv_alter_storage_vault_del_bytes",{"instance_id"});
// alter_obj_store_info
mBvarInt64Adder g_bvar_rpc_kv_alter_obj_store_info_get_bytes("rpc_kv_alter_obj_store_info_get_bytes",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_alter_obj_store_info_put_bytes("rpc_kv_alter_obj_store_info_put_bytes",{"instance_id"});
// update_ak_sk
mBvarInt64Adder g_bvar_rpc_kv_update_ak_sk_get_bytes("rpc_kv_update_ak_sk_get_bytes",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_update_ak_sk_put_bytes("rpc_kv_update_ak_sk_put_bytes",{"instance_id"});
// create_instance
mBvarInt64Adder g_bvar_rpc_kv_create_instance_get_bytes("rpc_kv_create_instance_get_bytes",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_create_instance_put_bytes("rpc_kv_create_instance_put_bytes",{"instance_id"});
// get_instance
mBvarInt64Adder g_bvar_rpc_kv_get_instance_get_bytes("rpc_kv_get_instance_get_bytes",{"instance_id"});
// alter_cluster
mBvarInt64Adder g_bvar_rpc_kv_alter_cluster_get_bytes("rpc_kv_alter_cluster_get_bytes",{"instance_id"});
// get_cluster
mBvarInt64Adder g_bvar_rpc_kv_get_cluster_get_bytes("rpc_kv_get_cluster_get_bytes",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_get_cluster_put_bytes("rpc_kv_get_cluster_put_bytes",{"instance_id"});
// create_stage
mBvarInt64Adder g_bvar_rpc_kv_create_stage_get_bytes("rpc_kv_create_stage_get_bytes",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_create_stage_put_bytes("rpc_kv_create_stage_put_bytes",{"instance_id"});
// get_stage
mBvarInt64Adder g_bvar_rpc_kv_get_stage_get_bytes("rpc_kv_get_stage_get_bytes",{"instance_id"});
// get_iam
mBvarInt64Adder g_bvar_rpc_kv_get_iam_get_bytes("rpc_kv_get_iam_get_bytes",{"instance_id"});
// alter_iam
mBvarInt64Adder g_bvar_rpc_kv_alter_iam_get_bytes("rpc_kv_alter_iam_get_bytes",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_alter_iam_put_bytes("rpc_kv_alter_iam_put_bytes",{"instance_id"});
// alter_ram_user
mBvarInt64Adder g_bvar_rpc_kv_alter_ram_user_get_bytes("rpc_kv_alter_ram_user_get_bytes",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_alter_ram_user_put_bytes("rpc_kv_alter_ram_user_put_bytes",{"instance_id"});
// begin_copy
mBvarInt64Adder g_bvar_rpc_kv_begin_copy_get_bytes("rpc_kv_begin_copy_get_bytes",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_begin_copy_put_bytes("rpc_kv_begin_copy_put_bytes",{"instance_id"});
// finish_copy
mBvarInt64Adder g_bvar_rpc_kv_finish_copy_get_bytes("rpc_kv_finish_copy_get_bytes",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_finish_copy_put_bytes("rpc_kv_finish_copy_put_bytes",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_finish_copy_del_bytes("rpc_kv_finish_copy_del_bytes",{"instance_id"});
// get_copy_job
mBvarInt64Adder g_bvar_rpc_kv_get_copy_job_get_bytes("rpc_kv_get_copy_job_get_bytes",{"instance_id"});
// get_copy_files
mBvarInt64Adder g_bvar_rpc_kv_get_copy_files_get_bytes("rpc_kv_get_copy_files_get_bytes",{"instance_id"});
// filter_copy_files
mBvarInt64Adder g_bvar_rpc_kv_filter_copy_files_get_bytes("rpc_kv_filter_copy_files_get_bytes",{"instance_id"});
// get_cluster_status
mBvarInt64Adder g_bvar_rpc_kv_get_cluster_status_get_bytes("rpc_kv_get_cluster_status_get_bytes",{"instance_id"});
// begin_txn
mBvarInt64Adder g_bvar_rpc_kv_begin_txn_get_bytes("rpc_kv_begin_txn_get_bytes",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_begin_txn_put_bytes("rpc_kv_begin_txn_put_bytes",{"instance_id"});
// precommit_txn
mBvarInt64Adder g_bvar_rpc_kv_precommit_txn_get_bytes("rpc_kv_precommit_txn_get_bytes",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_precommit_txn_put_bytes("rpc_kv_precommit_txn_put_bytes",{"instance_id"});
// get_rl_task_commit_attach
mBvarInt64Adder g_bvar_rpc_kv_get_rl_task_commit_attach_get_bytes("rpc_kv_get_rl_task_commit_attach_get_bytes",{"instance_id"});
// reset_rl_progress
mBvarInt64Adder g_bvar_rpc_kv_reset_rl_progress_get_bytes("rpc_kv_reset_rl_progress_get_bytes",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_reset_rl_progress_put_bytes("rpc_kv_reset_rl_progress_put_bytes",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_reset_rl_progress_del_bytes("rpc_kv_reset_rl_progress_del_bytes",{"instance_id"});
// commit_txn
mBvarInt64Adder g_bvar_rpc_kv_commit_txn_get_bytes("rpc_kv_commit_txn_get_bytes",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_commit_txn_put_bytes("rpc_kv_commit_txn_put_bytes",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_commit_txn_del_bytes("rpc_kv_commit_txn_del_bytes",{"instance_id"});
// abort_txn
mBvarInt64Adder g_bvar_rpc_kv_abort_txn_get_bytes("rpc_kv_abort_txn_get_bytes",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_abort_txn_put_bytes("rpc_kv_abort_txn_put_bytes",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_abort_txn_del_bytes("rpc_kv_abort_txn_del_bytes",{"instance_id"});
// get_txn
mBvarInt64Adder g_bvar_rpc_kv_get_txn_get_bytes("rpc_kv_get_txn_get_bytes",{"instance_id"});
// get_current_max_txn_id
mBvarInt64Adder g_bvar_rpc_kv_get_current_max_txn_id_get_bytes("rpc_kv_get_current_max_txn_id_get_bytes",{"instance_id"});
// begin_sub_txn
mBvarInt64Adder g_bvar_rpc_kv_begin_sub_txn_get_bytes("rpc_kv_begin_sub_txn_get_bytes",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_begin_sub_txn_put_bytes("rpc_kv_begin_sub_txn_put_bytes",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_begin_sub_txn_del_bytes("rpc_kv_begin_sub_txn_del_bytes",{"instance_id"});
// abort_sub_txn
mBvarInt64Adder g_bvar_rpc_kv_abort_sub_txn_get_bytes("rpc_kv_abort_sub_txn_get_bytes",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_abort_sub_txn_put_bytes("rpc_kv_abort_sub_txn_put_bytes",{"instance_id"});
// abort_txn_with_coordinator
mBvarInt64Adder g_bvar_rpc_kv_abort_txn_with_coordinator_get_bytes("rpc_kv_abort_txn_with_coordinator_get_bytes",{"instance_id"});
// check_txn_conflict
mBvarInt64Adder g_bvar_rpc_kv_check_txn_conflict_get_bytes("rpc_kv_check_txn_conflict_get_bytes",{"instance_id"});
// clean_txn_label
mBvarInt64Adder g_bvar_rpc_kv_clean_txn_label_get_bytes("rpc_kv_clean_txn_label_get_bytes",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_clean_txn_label_put_bytes("rpc_kv_clean_txn_label_put_bytes",{"instance_id"});
mBvarInt64Adder g_bvar_rpc_kv_clean_txn_label_del_bytes("rpc_kv_clean_txn_label_del_bytes",{"instance_id"});
// get_txn_id
mBvarInt64Adder g_bvar_rpc_kv_get_txn_id_get_bytes("rpc_kv_get_txn_id_get_bytes",{"instance_id"});

// meta ranges
mBvarStatus<int64_t> g_bvar_fdb_kv_ranges_count("fdb_kv_ranges_count", {"category","instance_id", "sub_category"});

// clang-format on
