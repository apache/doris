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
    CONF_Int32(create_tablet_worker_count, "3");
    // the count of thread to drop table
    CONF_Int32(drop_tablet_worker_count, "3");
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
    CONF_Int32(alter_tablet_worker_count, "3");
    // the count of thread to clone
    CONF_Int32(clone_worker_count, "3");
    // the count of thread to clone
    CONF_Int32(storage_medium_migrate_count, "1");
    // the count of thread to check consistency
    CONF_Int32(check_consistency_worker_count, "1");
    // the count of thread to upload
    CONF_Int32(upload_worker_count, "1");
    // the count of thread to download
    CONF_Int32(download_worker_count, "1");
    // the count of thread to make snapshot
    CONF_Int32(make_snapshot_worker_count, "5");
    // the count of thread to release snapshot
    CONF_Int32(release_snapshot_worker_count, "5");
    // the interval time(seconds) for agent report tasks signatrue to FE
    CONF_Int32(report_task_interval_seconds, "10");
    // the interval time(seconds) for agent report disk state to FE
    CONF_Int32(report_disk_state_interval_seconds, "60");
    // the interval time(seconds) for agent report olap table to FE
    CONF_Int32(report_tablet_interval_seconds, "60");
    // the timeout(seconds) for alter table
    CONF_Int32(alter_tablet_timeout_seconds, "86400");
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

    // log dir
    CONF_String(sys_log_dir, "${DORIS_HOME}/log");
    CONF_String(user_function_dir, "${DORIS_HOME}/lib/udf");
    // INFO, WARNING, ERROR, FATAL
    CONF_String(sys_log_level, "INFO");
    // TIME-DAY, TIME-HOUR, SIZE-MB-nnn
    CONF_String(sys_log_roll_mode, "SIZE-MB-1024");
    // log roll num
    CONF_Int32(sys_log_roll_num, "10");
    // verbose log
    CONF_Strings(sys_log_verbose_modules, "");
    // verbose log level
    CONF_Int32(sys_log_verbose_level, "10");
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
    CONF_Int32(unused_rowset_monitor_interval, "30");
    CONF_String(storage_root_path, "${DORIS_HOME}/storage");
    CONF_Int32(min_percentage_of_error_disk, "50");
    CONF_Int32(default_num_rows_per_data_block, "1024");
    CONF_Int32(default_num_rows_per_column_file_block, "1024");
    CONF_Int32(max_tablet_num_per_shard, "1024");
    // pending data policy
    CONF_Int32(pending_data_expire_time_sec, "1800");
    // inc_rowset expired interval
    CONF_Int32(inc_rowset_expired_sec, "1800");
    // garbage sweep policy
    CONF_Int32(max_garbage_sweep_interval, "3600");
    CONF_Int32(min_garbage_sweep_interval, "180");
    CONF_Int32(snapshot_expire_time_sec, "172800");
    // 仅仅是建议值，当磁盘空间不足时，trash下的文件保存期可不遵守这个参数
    CONF_Int32(trash_file_expire_time_sec, "259200");
    // check row nums for BE/CE and schema change. true is open, false is closed.
    CONF_Bool(row_nums_check, "true")
    //file descriptors cache, by default, cache 32768 descriptors
    CONF_Int32(file_descriptor_cache_capacity, "32768");
    // minimum file descriptor number
    // modify them upon necessity
    CONF_Int32(min_file_descriptor_number, "60000");
    CONF_Int64(index_stream_cache_capacity, "10737418240");
    CONF_Int64(max_packed_row_block_size, "20971520");

    // Cache for stoage page size
    CONF_String(storage_page_cache_limit, "20G");

    // be policy
    CONF_Int64(base_compaction_start_hour, "20");
    CONF_Int64(base_compaction_end_hour, "7");
    CONF_Int32(base_compaction_check_interval_seconds, "60");
    CONF_Int64(base_compaction_num_cumulative_deltas, "5");
    CONF_Int32(base_compaction_num_threads, "1");
    CONF_Int32(base_compaction_num_threads_per_disk, "1");
    CONF_Double(base_cumulative_delta_ratio, "0.3");
    CONF_Int64(base_compaction_interval_seconds_since_last_operation, "604800");
    CONF_Int32(base_compaction_write_mbytes_per_sec, "5");

    // cumulative compaction policy: max delta file's size unit:B
    CONF_Int32(cumulative_compaction_check_interval_seconds, "10");
    CONF_Int64(min_cumulative_compaction_num_singleton_deltas, "5");
    CONF_Int64(max_cumulative_compaction_num_singleton_deltas, "1000");
    CONF_Int32(cumulative_compaction_num_threads, "1");
    CONF_Int32(cumulative_compaction_num_threads_per_disk, "1");
    CONF_Int64(cumulative_compaction_budgeted_bytes, "104857600");
    CONF_Int32(cumulative_compaction_write_mbytes_per_sec, "100");

    // if compaction of a tablet failed, this tablet should not be chosen to
    // compaction until this interval passes.
    CONF_Int64(min_compaction_failure_interval_sec, "600") // 10 min

    // Port to start debug webserver on
    CONF_Int32(webserver_port, "8040");
    // Number of webserver workers
    CONF_Int32(webserver_num_workers, "5");
    // Period to update rate counters and sampling counters in ms.
    CONF_Int32(periodic_counter_update_period_ms, "500");

    // Used for mini Load. mini load data file will be removed after this time.
    CONF_Int64(load_data_reserve_hours, "4");
    // log error log will be removed after this time
    CONF_Int64(load_error_log_reserve_hours, "48");
    // Deprecated, use streaming_load_max_mb instead
    CONF_Int64(mini_load_max_mb, "2048");
    CONF_Int32(number_tablet_writer_threads, "16");

    CONF_Int64(streaming_load_max_mb, "10240");
    // the alive time of a TabletsChannel.
    // If the channel does not receive any data till this time,
    // the channel will be removed.
    CONF_Int32(streaming_load_rpc_max_alive_time_sec, "1200");
    // the timeout of a rpc to process one batch in tablet writer.
    // you may need to increase this timeout if using larger 'streaming_load_max_mb',
    // or encounter 'tablet writer write failed' error when loading.
    CONF_Int32(tablet_writer_rpc_timeout_sec, "600");

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

    // Whether to allocate chunk using mmap. If you enable this, you'd better to 
    // increase vm.max_map_count's value whose default value is 65530.
    // you can do it as root via "sysctl -w vm.max_map_count=262144" or
    // "echo 262144 > /proc/sys/vm/max_map_count"
    // NOTE: When this is set to true, you must set chunk_reserved_bytes_limit
    // to a relative large number or the performace is very very bad.
    CONF_Bool(use_mmap_allocate_chunk, "false");

    // Chunk Allocator's reserved bytes limit,
    // Default value is 2GB, increase this variable can improve performance, but will
    // aquire more free memory which can not be used by other modules
    CONF_Int64(chunk_reserved_bytes_limit, "2147483648");

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

    // Number of cores Doris will used, this will effect only when it's greater than 0.
    // Otherwise, Doris will use all cores returned from "/proc/cpuinfo".
    CONF_Int32(num_cores, "0");

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
    CONF_Int64(write_buffer_size, "104857600");

    // following 2 configs limit the memory consumption of load process on a Backend.
    // eg: memory limit to 80% of mem limit config but up to 100GB(default)
    // NOTICE(cmy): set these default values very large because we don't want to
    // impact the load performace when user upgrading Doris.
    // user should set these configs properly if necessary.
    CONF_Int64(load_process_max_memory_limit_bytes, "107374182400"); // 100GB
    CONF_Int32(load_process_max_memory_limit_percent, "80");    // 80%

    // update interval of tablet stat cache
    CONF_Int32(tablet_stat_cache_update_interval_second, "300");

    // result buffer cancelled time (unit: second)
    CONF_Int32(result_buffer_cancelled_interval_time, "300");

    // can perform recovering tablet
    CONF_Bool(force_recovery, "false");

    // the increased frequency of priority for remaining tasks in BlockingPriorityQueue
    CONF_Int32(priority_queue_remaining_tasks_increased_frequency, "512");

    // sync tablet_meta when modifing meta
    CONF_Bool(sync_tablet_meta, "false");

    // default thrift rpc timeout ms
    CONF_Int32(thrift_rpc_timeout_ms, "5000");

    // txn commit rpc timeout
    CONF_Int32(txn_commit_rpc_timeout_ms, "10000");

    // If set to true, metric calculator will run
    CONF_Bool(enable_metric_calculator, "true");

    // max consumer num in one data consumer group, for routine load
    CONF_Int32(max_consumer_num_per_group, "3");

    // the size of thread pool for routine load task.
    // this should be larger than FE config 'max_concurrent_task_num_per_be' (default 5)
    CONF_Int32(routine_load_thread_pool_size, "10");

    // Is set to true, index loading failure will not causing BE exit,
    // and the tablet will be marked as bad, so that FE will try to repair it.
    CONF_Bool(auto_recover_index_loading_failure, "false");

    // max external scan cache batch count, means cache max_memory_cache_batch_count * batch_size row
    // default is 10, batch_size's defualt value is 1024 means 10 * 1024 rows will be cached
    CONF_Int32(max_memory_sink_batch_count, "20");
    
    // This configuration is used for the context gc thread schedule period
    // note: unit is minute, default is 5min
    CONF_Int32(scan_context_gc_interval_min, "5");

    // es scroll keep-alive
    CONF_String(es_scroll_keepalive, "5m");

    // HTTP connection timeout for es
    CONF_Int32(es_http_timeout_ms, "5000");

    // the max client cache number per each host
    // There are variety of client cache in BE, but currently we use the
    // same cache size configuration.
    // TODO(cmy): use different config to set different client cache if necessary.
    CONF_Int32(max_client_cache_size_per_host, "10");

    // Dir to save files downloaded by SmallFileMgr
    CONF_String(small_file_dir, "${DORIS_HOME}/lib/small_file/");
    // path gc
    CONF_Bool(path_gc_check, "true");
    CONF_Int32(path_gc_check_interval_second, "86400");
    CONF_Int32(path_gc_check_step, "1000");
    CONF_Int32(path_gc_check_step_interval_ms, "10");
    CONF_Int32(path_scan_interval_second, "86400");

    // The following 2 configs limit the max usage of disk capacity of a data dir.
    // If both of these 2 threshold reached, no more data can be writen into that data dir.
    // The percent of max used capacity of a data dir
    CONF_Int32(storage_flood_stage_usage_percent, "95");    // 95%
    // The min bytes that should be left of a data dir
    CONF_Int64(storage_flood_stage_left_capacity_bytes, "1073741824")   // 1GB
    // number of thread for flushing memtable per store
    CONF_Int32(flush_thread_num_per_store, "2");

    // config for tablet meta checkpoint
    CONF_Int32(tablet_meta_checkpoint_min_new_rowsets_num, "10");
    CONF_Int32(tablet_meta_checkpoint_min_interval_secs, "600");

    // brpc config
    CONF_Int64(brpc_max_body_size, "67108864")

    // max number of txns in txn manager
    // this is a self protection to avoid too many txns saving in manager
    CONF_Int64(max_runnings_transactions, "200");

} // namespace config

} // namespace doris

#endif // DORIS_BE_SRC_COMMON_CONFIG_H
