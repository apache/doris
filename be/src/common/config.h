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

#ifndef DORIS_BE_SRC_COMMON_CONFIG_H
#define DORIS_BE_SRC_COMMON_CONFIG_H

#include "configbase.h"

namespace doris {
namespace config {
    // cluster id
    CONF_Int32(cluster_id, "-1");
    // port on which ImpalaInternalService is exported
    CONF_Int32(be_port, "9060");

    // port for brpc
    CONF_Int32(brpc_port, "8060");

    // Declare a selection strategy for those servers have many ips.
    // Note that there should at most one ip match this list.
    // this is a list in semicolon-delimited format, in CIDR notation, e.g. 10.10.10.0/24
    // If no ip match this rule, will choose one randomly.
    CONF_String(priority_networks, "")

    ////
    //// tcmalloc gc parameter
    ////
    // min memory for TCmalloc, when used memory is smaller than this, do not returned to OS
    CONF_Int64(tc_use_memory_min, "10737418240");
    // free memory rate.[0-100]
    CONF_Int64(tc_free_memory_rate, "20");

    // process memory limit specified as number of bytes
    // ('<int>[bB]?'), megabytes ('<float>[mM]'), gigabytes ('<float>[gG]'),
    // or percentage of the physical memory ('<int>%').
    // defaults to bytes if no unit is given"
    CONF_String(mem_limit, "80%");

    // the port heartbeat service used
    CONF_Int32(heartbeat_service_port, "9050");
    // the count of heart beat service
    CONF_Int32(heartbeat_service_thread_count, "1");
    // the count of thread to create table
    CONF_Int32(create_table_worker_count, "3");
    // the count of thread to drop table
    CONF_Int32(drop_table_worker_count, "3");
    // the count of thread to batch load
    CONF_Int32(push_worker_count_normal_priority, "3");
    // the count of thread to high priority batch load
    CONF_Int32(push_worker_count_high_priority, "3");
    // the count of thread to publish version
    CONF_Int32(publish_version_worker_count, "2");
    // the count of thread to clear alter task
    CONF_Int32(clear_alter_task_worker_count, "1");
    // the count of thread to clear transaction task
    CONF_Int32(clear_transaction_task_worker_count, "1");
    // the count of thread to delete
    CONF_Int32(delete_worker_count, "3");
    // the count of thread to alter table
    CONF_Int32(alter_table_worker_count, "3");
    // the count of thread to clone
    CONF_Int32(clone_worker_count, "3");
    // the count of thread to clone
    CONF_Int32(storage_medium_migrate_count, "1");
    // the count of thread to cancel delete data
    CONF_Int32(cancel_delete_data_worker_count, "3");
    // the count of thread to check consistency
    CONF_Int32(check_consistency_worker_count, "1");
    // the count of thread to upload
    CONF_Int32(upload_worker_count, "3");
    // the count of thread to download
    CONF_Int32(download_worker_count, "3");
    // the count of thread to make snapshot
    CONF_Int32(make_snapshot_worker_count, "5");
    // the count of thread to release snapshot
    CONF_Int32(release_snapshot_worker_count, "5");
    // the interval time(seconds) for agent report tasks signatrue to FE
    CONF_Int32(report_task_interval_seconds, "10");
    // the interval time(seconds) for agent report disk state to FE
    CONF_Int32(report_disk_state_interval_seconds, "60");
    // the interval time(seconds) for agent report olap table to FE
    CONF_Int32(report_olap_table_interval_seconds, "60");
    // the timeout(seconds) for alter table
    CONF_Int32(alter_table_timeout_seconds, "86400");
    // the timeout(seconds) for make snapshot
    CONF_Int32(make_snapshot_timeout_seconds, "600");
    // the timeout(seconds) for release snapshot
    CONF_Int32(release_snapshot_timeout_seconds, "600");
    // the max download speed(KB/s)
    CONF_Int32(max_download_speed_kbps, "50000");
    // download low speed limit(KB/s)
    CONF_Int32(download_low_speed_limit_kbps, "50");
    // download low speed time(seconds)
    CONF_Int32(download_low_speed_time, "300");
    // curl verbose mode
    CONF_Int64(curl_verbose_mode, "1");
    // seconds to sleep for each time check table status
    CONF_Int32(check_status_sleep_time_seconds, "10");
    // sleep time for one second
    CONF_Int32(sleep_one_second, "1");
    // sleep time for five seconds
    CONF_Int32(sleep_five_seconds, "5");
    // trans file tools dir
    CONF_String(trans_file_tool_path, "${DORIS_HOME}/tools/trans_file_tool/trans_files.sh");
    // agent tmp dir
    CONF_String(agent_tmp_dir, "${DORIS_HOME}/tmp");

    // log dir
    CONF_String(sys_log_dir, "${DORIS_HOME}/log");
    // INFO, WARNING, ERROR, FATAL
    CONF_String(sys_log_level, "INFO");
    // TIME-DAY, TIME-HOUR, SIZE-MB-nnn
    CONF_String(sys_log_roll_mode, "SIZE-MB-1024");
    // log roll num
    CONF_Int32(sys_log_roll_num, "10");
    // verbose log
    CONF_Strings(sys_log_verbose_modules, "");
    // log buffer level
    CONF_String(log_buffer_level, "");

    // Pull load task dir
    CONF_String(pull_load_task_dir, "${DORIS_HOME}/var/pull_load");

    // the maximum number of bytes to display on the debug webserver's log page
    CONF_Int64(web_log_bytes, "1048576");
    // number of threads available to serve backend execution requests
    CONF_Int32(be_service_threads, "64");
    // key=value pair of default query options for Doris, separated by ','
    CONF_String(default_query_options, "");

    // If non-zero, Doris will output memory usage every log_mem_usage_interval'th fragment completion.
    CONF_Int32(log_mem_usage_interval, "0");
    // if non-empty, enable heap profiling and output to specified directory.
    CONF_String(heap_profile_dir, "");

    // cgroups allocated for doris
    CONF_String(doris_cgroups, "");

    // Controls the number of threads to run work per core.  It's common to pick 2x
    // or 3x the number of cores.  This keeps the cores busy without causing excessive
    // thrashing.
    CONF_Int32(num_threads_per_core, "3");
    // if true, compresses tuple data in Serialize
    CONF_Bool(compress_rowbatches, "true");
    // serialize and deserialize each returned row batch
    CONF_Bool(serialize_batch, "false");
    // interval between profile reports; in seconds
    CONF_Int32(status_report_interval, "5");
    // Local directory to copy UDF libraries from HDFS into
    CONF_String(local_library_dir, "${UDF_RUNTIME_DIR}");
    // number of olap scanner thread pool size
    CONF_Int32(doris_scanner_thread_pool_thread_num, "48");
    // number of olap scanner thread pool size
    CONF_Int32(doris_scanner_thread_pool_queue_size, "102400");
    // number of etl thread pool size
    CONF_Int32(etl_thread_pool_size, "8");
    // number of etl thread pool size
    CONF_Int32(etl_thread_pool_queue_size, "256");
    // port on which to run Doris test backend
    CONF_Int32(port, "20001");
    // default thrift client connect timeout(in seconds)
    CONF_Int32(thrift_connect_timeout_seconds, "3");
    // max row count number for single scan range
    CONF_Int32(doris_scan_range_row_count, "524288");
    // size of scanner queue between scanner thread and compute thread
    CONF_Int32(doris_scanner_queue_size, "1024");
    // single read execute fragment row size
    CONF_Int32(doris_scanner_row_num, "16384");
    // number of max scan keys
    CONF_Int32(doris_max_scan_key_num, "1024");
    // return_row / total_row
    CONF_Int32(doris_max_pushdown_conjuncts_return_rate, "90");
    // (Advanced) Maximum size of per-query receive-side buffer
    CONF_Int32(exchg_node_buffer_size_bytes, "10485760");
    // insert sort threadhold for sorter
    CONF_Int32(insertion_threadhold, "16");
    // the block_size every block allocate for sorter
    CONF_Int32(sorter_block_size, "8388608");
    // push_write_mbytes_per_sec
    CONF_Int32(push_write_mbytes_per_sec, "10");

    CONF_Int64(column_dictionary_key_ration_threshold, "0");
    CONF_Int64(column_dictionary_key_size_threshold, "0");
    // if true, output IR after optimization passes
    CONF_Bool(dump_ir, "false");
    // if set, saves the generated IR to the output file.
    CONF_String(module_output, "");
    // memory_limitation_per_thread_for_schema_change unit GB
    CONF_Int32(memory_limitation_per_thread_for_schema_change, "2");

    CONF_Int64(max_unpacked_row_block_size, "104857600");

    CONF_Int32(file_descriptor_cache_clean_interval, "3600");
    CONF_Int32(disk_stat_monitor_interval, "5");
    CONF_Int32(unused_index_monitor_interval, "30");
    CONF_String(storage_root_path, "${DORIS_HOME}/storage");
    CONF_Int32(min_percentage_of_error_disk, "50");
    CONF_Int32(default_num_rows_per_data_block, "1024");
    CONF_Int32(default_num_rows_per_column_file_block, "1024");
    CONF_Int32(max_tablet_num_per_shard, "1024");
    // pending data policy
    CONF_Int32(pending_data_expire_time_sec, "1800");
    // incremental delta policy
    CONF_Int32(incremental_delta_expire_time_sec, "1800");
    // garbage sweep policy
    CONF_Int32(max_garbage_sweep_interval, "86400");
    CONF_Int32(min_garbage_sweep_interval, "200");
    CONF_Int32(snapshot_expire_time_sec, "864000");
    // 仅仅是建议值，当磁盘空间不足时，trash下的文件保存期可不遵守这个参数
    CONF_Int32(trash_file_expire_time_sec, "259200");
    CONF_Int32(disk_capacity_insufficient_percentage, "90");
    // check row nums for BE/CE and schema change. true is open, false is closed.
    CONF_Bool(row_nums_check, "true")
    //file descriptors cache, by default, cache 30720 descriptors
    CONF_Int32(file_descriptor_cache_capacity, "30720");
    CONF_Int64(index_stream_cache_capacity, "10737418240");
    CONF_Int64(max_packed_row_block_size, "20971520");

    // be policy
    CONF_Int64(base_compaction_start_hour, "20");
    CONF_Int64(base_compaction_end_hour, "7");
    CONF_Int32(base_compaction_check_interval_seconds, "60");
    CONF_Int64(base_compaction_num_cumulative_deltas, "5");
    CONF_Int32(base_compaction_num_threads, "1");
    CONF_Double(base_cumulative_delta_ratio, "0.3");
    CONF_Int64(base_compaction_interval_seconds_since_last_operation, "604800");
    CONF_Int32(base_compaction_write_mbytes_per_sec, "5");

    // cumulative compaction policy: max delta file's size unit:B
    CONF_Int32(cumulative_compaction_check_interval_seconds, "10");
    CONF_Int64(cumulative_compaction_num_singleton_deltas, "5");
    CONF_Int32(cumulative_compaction_num_threads, "1");
    CONF_Int64(cumulative_compaction_budgeted_bytes, "104857600");
    CONF_Int32(cumulative_compaction_write_mbytes_per_sec, "100");

    // Port to start debug webserver on
    CONF_Int32(webserver_port, "8040");
    // Interface to start debug webserver on. If blank, webserver binds to 0.0.0.0
    CONF_String(webserver_interface, "");
    CONF_String(webserver_doc_root, "${DORIS_HOME}");
    CONF_Int32(webserver_num_workers, "5");
    // If true, webserver may serve static files from the webserver_doc_root
    CONF_Bool(enable_webserver_doc_root, "true");
    // Period to update rate counters and sampling counters in ms.
    CONF_Int32(periodic_counter_update_period_ms, "500");

    // Used for mini Load
    CONF_Int64(load_data_reserve_hours, "24");
    CONF_Int64(mini_load_max_mb, "2048");
    CONF_Int32(number_tablet_writer_threads, "16");

    CONF_Int64(streaming_load_max_mb, "10240");

    // Fragment thread pool
    CONF_Int32(fragment_pool_thread_num, "64");
    CONF_Int32(fragment_pool_queue_size, "1024");

    //for cast
    CONF_Bool(cast, "true");

    // Spill to disk when query
    // Writable scratch directories, splitted by ";"
    CONF_String(query_scratch_dirs, "${DORIS_HOME}");

    // Control the number of disks on the machine.  If 0, this comes from the system settings.
    CONF_Int32(num_disks, "0");
    // The maximum number of the threads per disk is also the max queue depth per disk.
    CONF_Int32(num_threads_per_disk, "0");
    // The read size is the size of the reads sent to os.
    // There is a trade off of latency and throughout, trying to keep disks busy but
    // not introduce seeks.  The literature seems to agree that with 8 MB reads, random
    // io and sequential io perform similarly.
    CONF_Int32(read_size, "8388608"); // 8 * 1024 * 1024, Read Size (in bytes)
    CONF_Int32(min_buffer_size, "1024"); // 1024, The minimum read buffer size (in bytes)

    // For each io buffer size, the maximum number of buffers the IoMgr will hold onto
    // With 1024B through 8MB buffers, this is up to ~2GB of buffers.
    CONF_Int32(max_free_io_buffers, "128");

    CONF_Bool(disable_mem_pools, "false");

    // The probing algorithm of partitioned hash table.
    // Enable quadratic probing hash table
    CONF_Bool(enable_quadratic_probing, "false");

    // for pprof
    CONF_String(pprof_profile_dir, "${DORIS_HOME}/log")

    // for partition
    CONF_Bool(enable_partitioned_hash_join, "false")
    CONF_Bool(enable_partitioned_aggregation, "false")
    CONF_Bool(enable_new_partitioned_aggregation, "true")
    
    // for kudu
    // "The maximum size of the row batch queue, for Kudu scanners."
    CONF_Int32(kudu_max_row_batches, "0")
    // "The period at which Kudu Scanners should send keep-alive requests to the tablet "
    // "server to ensure that scanners do not time out.")
    // 150 * 1000 * 1000
    CONF_Int32(kudu_scanner_keep_alive_period_us, "15000000")

    // "(Advanced) Sets the Kudu scan ReadMode. "
    // "Supported Kudu read modes are READ_LATEST and READ_AT_SNAPSHOT. Invalid values "
    // "result in using READ_LATEST."
    CONF_String(kudu_read_mode, "READ_LATEST")
    // "Whether to pick only leader replicas, for tests purposes only.")
    CONF_Bool(pick_only_leaders_for_tests, "false")
    // "The period at which Kudu Scanners should send keep-alive requests to the tablet "
    // "server to ensure that scanners do not time out."
    CONF_Int32(kudu_scanner_keep_alive_period_sec, "15")
    CONF_Int32(kudu_operation_timeout_ms, "5000")
    // "If true, Kudu features will be disabled."
    CONF_Bool(disable_kudu, "false")

    // to forward compatibility, will be removed later
    CONF_Bool(enable_token_check, "true");

    // to open/close system metrics
    CONF_Bool(enable_system_metrics, "true");
    
    CONF_Bool(enable_prefetch, "true");

    // cpu count
    CONF_Int32(flags_num_cores, "32");

    CONF_Bool(thread_creation_fault_injection, "false");

    // Set this to encrypt and perform an integrity
    // check on all data spilled to disk during a query
    CONF_Bool(disk_spill_encryption, "false");

    // Writable scratch directories
    CONF_String(scratch_dirs, "/tmp");

    // If false and --scratch_dirs contains multiple directories on the same device,
    // then only the first writable directory is used
    CONF_Bool(allow_multiple_scratch_dirs_per_device, "false");

    // linux transparent huge page
    CONF_Bool(madvise_huge_pages, "false");

    // whether use mmap to allocate memory
    CONF_Bool(mmap_buffers, "false");

    // max memory can be allocated by buffer pool
    CONF_String(buffer_pool_limit, "80G");

    // clean page can be hold by buffer pool
    CONF_String(buffer_pool_clean_pages_limit, "20G");

    // Sleep time in seconds between memory maintenance iterations
    CONF_Int64(memory_maintenance_sleep_time_s, "10");

    // Aligement
    CONF_Int32(memory_max_alignment, "16");

    // write buffer size before flush
    CONF_Int32(write_buffer_size, "104857600");

    // update interval of tablet stat cache
    CONF_Int32(tablet_stat_cache_update_interval_second, "300");

    // result buffer cancelled time (unit: second)
    CONF_Int32(result_buffer_cancelled_interval_time, "5");

    // can perform recovering tablet
    CONF_Bool(force_recovery, "false");
} // namespace config

} // namespace doris

#endif // DORIS_BE_SRC_COMMON_CONFIG_H
