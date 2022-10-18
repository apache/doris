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

#include "configbase.h"

namespace doris {
namespace config {
// Dir of custom config file
CONF_String(custom_config_dir, "${DORIS_HOME}/conf");

// cluster id
CONF_Int32(cluster_id, "-1");
// port on which BackendService is exported
CONF_Int32(be_port, "9060");

// port for brpc
CONF_Int32(brpc_port, "8060");

// the number of bthreads for brpc, the default value is set to -1, which means the number of bthreads is #cpu-cores
CONF_Int32(brpc_num_threads, "-1");

// port to brpc server for single replica load
CONF_Int32(single_replica_load_brpc_port, "8070");
// the number of bthreads to brpc server for single replica load
CONF_Int32(single_replica_load_brpc_num_threads, "64");

// Declare a selection strategy for those servers have many ips.
// Note that there should at most one ip match this list.
// this is a list in semicolon-delimited format, in CIDR notation, e.g. 10.10.10.0/24
// If no ip match this rule, will choose one randomly.
CONF_String(priority_networks, "");

////
//// tcmalloc gc parameter
////
// min memory for TCmalloc, when used memory is smaller than this, do not returned to OS
CONF_mInt64(tc_use_memory_min, "10737418240");
// free memory rate.[0-100]
CONF_mInt64(tc_free_memory_rate, "20");
// tcmallc aggressive_memory_decommit
CONF_mBool(tc_enable_aggressive_memory_decommit, "false");

// Bound on the total amount of bytes allocated to thread caches.
// This bound is not strict, so it is possible for the cache to go over this bound
// in certain circumstances. This value defaults to 1GB
// If you suspect your application is not scaling to many threads due to lock contention in TCMalloc,
// you can try increasing this value. This may improve performance, at a cost of extra memory
// use by TCMalloc.
// reference: https://gperftools.github.io/gperftools/tcmalloc.html: TCMALLOC_MAX_TOTAL_THREAD_CACHE_BYTES
//            https://github.com/gperftools/gperftools/issues/1111
CONF_Int64(tc_max_total_thread_cache_bytes, "1073741824");

// process memory limit specified as number of bytes
// ('<int>[bB]?'), megabytes ('<float>[mM]'), gigabytes ('<float>[gG]'),
// or percentage of the physical memory ('<int>%').
// defaults to bytes if no unit is given"
// must larger than 0. and if larger than physical memory size,
// it will be set to physical memory size.
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
CONF_Int32(publish_version_worker_count, "8");
// the count of tablet thread to publish version
CONF_Int32(tablet_publish_txn_max_thread, "32");
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
CONF_mInt32(report_task_interval_seconds, "10");
// the interval time(seconds) for refresh storage policy from FE
CONF_mInt32(storage_refresh_storage_policy_task_interval_seconds, "5");
// the interval time(seconds) for agent report disk state to FE
CONF_mInt32(report_disk_state_interval_seconds, "60");
// the interval time(seconds) for agent report olap table to FE
CONF_mInt32(report_tablet_interval_seconds, "60");
// the max download speed(KB/s)
CONF_mInt32(max_download_speed_kbps, "50000");
// download low speed limit(KB/s)
CONF_mInt32(download_low_speed_limit_kbps, "50");
// download low speed time(seconds)
CONF_mInt32(download_low_speed_time, "300");
// sleep time for one second
CONF_Int32(sleep_one_second, "1");

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

// number of threads available to serve backend execution requests
CONF_Int32(be_service_threads, "64");

// cgroups allocated for doris
CONF_String(doris_cgroups, "");

// Controls the number of threads to run work per core.  It's common to pick 2x
// or 3x the number of cores.  This keeps the cores busy without causing excessive
// thrashing.
CONF_Int32(num_threads_per_core, "3");
// if true, compresses tuple data in Serialize
CONF_mBool(compress_rowbatches, "true");
CONF_mBool(rowbatch_align_tuple_offset, "false");
// interval between profile reports; in seconds
CONF_mInt32(status_report_interval, "5");
// if true, each disk will have a separate thread pool for scanner
CONF_Bool(doris_enable_scanner_thread_pool_per_disk, "true");
// the timeout of a work thread to wait the blocking priority queue to get a task
CONF_mInt64(doris_blocking_priority_queue_wait_timeout_ms, "500");
// number of olap scanner thread pool size
CONF_Int32(doris_scanner_thread_pool_thread_num, "48");
// number of olap scanner thread pool queue size
CONF_Int32(doris_scanner_thread_pool_queue_size, "102400");
// default thrift client connect timeout(in seconds)
CONF_mInt32(thrift_connect_timeout_seconds, "3");
// default thrift client retry interval (in milliseconds)
CONF_mInt64(thrift_client_retry_interval_ms, "1000");
// max row count number for single scan range, used in segmentv1
CONF_mInt32(doris_scan_range_row_count, "524288");
// max bytes number for single scan range, used in segmentv2
CONF_mInt32(doris_scan_range_max_mb, "1024");
// max bytes number for single scan block, used in segmentv2
CONF_mInt32(doris_scan_block_max_mb, "67108864");
// size of scanner queue between scanner thread and compute thread
CONF_mInt32(doris_scanner_queue_size, "1024");
// single read execute fragment row number
CONF_mInt32(doris_scanner_row_num, "16384");
// single read execute fragment row bytes
CONF_mInt32(doris_scanner_row_bytes, "10485760");
// number of max scan keys
CONF_mInt32(doris_max_scan_key_num, "1024");
// the max number of push down values of a single column.
// if exceed, no conditions will be pushed down for that column.
CONF_mInt32(max_pushdown_conditions_per_column, "1024");
// return_row / total_row
CONF_mInt32(doris_max_pushdown_conjuncts_return_rate, "90");
// (Advanced) Maximum size of per-query receive-side buffer
CONF_mInt32(exchg_node_buffer_size_bytes, "10485760");

CONF_mInt64(column_dictionary_key_ratio_threshold, "0");
CONF_mInt64(column_dictionary_key_size_threshold, "0");
// memory_limitation_per_thread_for_schema_change_bytes unit bytes
CONF_mInt64(memory_limitation_per_thread_for_schema_change_bytes, "2147483648");
CONF_mInt64(memory_limitation_per_thread_for_storage_migration_bytes, "100000000");

// the clean interval of file descriptor cache and segment cache
CONF_mInt32(cache_clean_interval, "1800");
CONF_mInt32(disk_stat_monitor_interval, "5");
CONF_mInt32(unused_rowset_monitor_interval, "30");
CONF_String(storage_root_path, "${DORIS_HOME}/storage");

// Config is used to check incompatible old format hdr_ format
// whether doris uses strict way. When config is true, process will log fatal
// and exit. When config is false, process will only log warning.
CONF_Bool(storage_strict_check_incompatible_old_format, "true");

// BE process will exit if the percentage of error disk reach this value.
CONF_mInt32(max_percentage_of_error_disk, "0");
CONF_mInt32(default_num_rows_per_column_file_block, "1024");
// pending data policy
CONF_mInt32(pending_data_expire_time_sec, "1800");
// inc_rowset snapshot rs sweep time interval
CONF_mInt32(tablet_rowset_stale_sweep_time_sec, "1800");
// garbage sweep policy
CONF_Int32(max_garbage_sweep_interval, "3600");
CONF_Int32(min_garbage_sweep_interval, "180");
CONF_mInt32(snapshot_expire_time_sec, "172800");
// It is only a recommended value. When the disk space is insufficient,
// the file storage period under trash dose not have to comply with this parameter.
CONF_mInt32(trash_file_expire_time_sec, "259200");
// check row nums for BE/CE and schema change. true is open, false is closed.
CONF_mBool(row_nums_check, "true");
// minimum file descriptor number
// modify them upon necessity
CONF_Int32(min_file_descriptor_number, "60000");
CONF_Int64(index_stream_cache_capacity, "10737418240");

// Cache for storage page size
CONF_String(storage_page_cache_limit, "20%");
// Shard size for page cache, the value must be power of two.
// It's recommended to set it to a value close to the number of BE cores in order to reduce lock contentions.
CONF_Int32(storage_page_cache_shard_size, "16");
// Percentage for index page cache
// all storage page cache will be divided into data_page_cache and index_page_cache
CONF_Int32(index_page_cache_percentage, "10");
// whether to disable page cache feature in storage
CONF_Bool(disable_storage_page_cache, "true");

CONF_Bool(enable_storage_vectorization, "true");

CONF_Bool(enable_low_cardinality_optimize, "true");

// be policy
// whether disable automatic compaction task
CONF_mBool(disable_auto_compaction, "false");
// whether enable vectorized compaction
CONF_Bool(enable_vectorized_compaction, "true");
// whether enable vectorized schema change/material-view/rollup task.
CONF_Bool(enable_vectorized_alter_table, "true");

// check the configuration of auto compaction in seconds when auto compaction disabled
CONF_mInt32(check_auto_compaction_interval_seconds, "5");

CONF_mInt64(base_compaction_num_cumulative_deltas, "5");
CONF_mDouble(base_cumulative_delta_ratio, "0.3");
CONF_mInt64(base_compaction_interval_seconds_since_last_operation, "86400");
CONF_mInt32(base_compaction_write_mbytes_per_sec, "5");
CONF_Bool(enable_base_compaction_idle_sched, "true");

// dup key not compaction big files
CONF_Bool(enable_dup_key_base_compaction_skip_big_file, "true");
CONF_mInt64(base_compaction_dup_key_max_file_size_mbytes, "1024");

// In size_based policy, output rowset of cumulative compaction total disk size exceed this config size,
// this rowset will be given to base compaction, unit is m byte.
CONF_mInt64(cumulative_size_based_promotion_size_mbytes, "1024");

// In size_based policy, output rowset of cumulative compaction total disk size exceed this config ratio of
// base rowset's total disk size, this rowset will be given to base compaction. The value must be between
// 0 and 1.
CONF_mDouble(cumulative_size_based_promotion_ratio, "0.05");

// In size_based policy, the smallest size of rowset promotion. When the rowset is less than this config, this
// rowset will be not given to base compaction. The unit is m byte.
CONF_mInt64(cumulative_size_based_promotion_min_size_mbytes, "64");

// The lower bound size to do cumulative compaction. When total disk size of candidate rowsets is less than
// this size, size_based policy may not do to cumulative compaction. The unit is m byte.
CONF_mInt64(cumulative_size_based_compaction_lower_size_mbytes, "64");

// cumulative compaction policy: min and max delta file's number
CONF_mInt64(min_cumulative_compaction_num_singleton_deltas, "5");
CONF_mInt64(max_cumulative_compaction_num_singleton_deltas, "1000");

// if compaction of a tablet failed, this tablet should not be chosen to
// compaction until this interval passes.
CONF_mInt64(min_compaction_failure_interval_sec, "5"); // 5 seconds

// This config can be set to limit thread number in compaction thread pool.
CONF_mInt32(max_base_compaction_threads, "4");
CONF_mInt32(max_cumu_compaction_threads, "10");

// This config can be set to limit thread number in  smallcompaction thread pool.
CONF_mInt32(quick_compaction_max_threads, "10");

// Thread count to do tablet meta checkpoint, -1 means use the data directories count.
CONF_Int32(max_meta_checkpoint_threads, "-1");

// The upper limit of "permits" held by all compaction tasks. This config can be set to limit memory consumption for compaction.
CONF_mInt64(total_permits_for_compaction_score, "10000");

// sleep interval in ms after generated compaction tasks
CONF_mInt32(generate_compaction_tasks_min_interval_ms, "10");

// Compaction task number per disk.
// Must be greater than 2, because Base compaction and Cumulative compaction have at least one thread each.
CONF_mInt32(compaction_task_num_per_disk, "2");
// compaction thread num for fast disk(typically .SSD), must be greater than 2.
CONF_mInt32(compaction_task_num_per_fast_disk, "4");
CONF_Validator(compaction_task_num_per_disk, [](const int config) -> bool { return config >= 2; });
CONF_Validator(compaction_task_num_per_fast_disk,
               [](const int config) -> bool { return config >= 2; });

// How many rounds of cumulative compaction for each round of base compaction when compaction tasks generation.
CONF_mInt32(cumulative_compaction_rounds_for_each_base_compaction_round, "9");

// Merge log will be printed for each "row_step_for_compaction_merge_log" rows merged during compaction
CONF_mInt64(row_step_for_compaction_merge_log, "0");

// Threshold to logging compaction trace, in seconds.
CONF_mInt32(base_compaction_trace_threshold, "60");
CONF_mInt32(cumulative_compaction_trace_threshold, "10");
CONF_mBool(disable_compaction_trace_log, "true");

// Threshold to logging agent task trace, in seconds.
CONF_mInt32(agent_task_trace_threshold_sec, "2");

// time interval to record tablet scan count in second for the purpose of calculating tablet scan frequency
CONF_mInt64(tablet_scan_frequency_time_node_interval_second, "300");
// coefficient for tablet scan frequency and compaction score when finding a tablet for compaction
CONF_mInt32(compaction_tablet_scan_frequency_factor, "0");
CONF_mInt32(compaction_tablet_compaction_score_factor, "1");

// This config can be set to limit thread number in tablet migration thread pool.
CONF_Int32(min_tablet_migration_threads, "1");
CONF_Int32(max_tablet_migration_threads, "1");

CONF_mInt32(finished_migration_tasks_size, "10000");
// If size less than this, the remaining rowsets will be force to complete
CONF_mInt32(migration_remaining_size_threshold_mb, "10");
// If the task runs longer than this time, the task will be terminated, in seconds.
// tablet max size / migration min speed * factor = 10GB / 1MBps * 2 = 20480 seconds
CONF_mInt32(migration_task_timeout_secs, "20480");

// Port to start debug webserver on
CONF_Int32(webserver_port, "8040");
// Number of webserver workers
CONF_Int32(webserver_num_workers, "48");
// Period to update rate counters and sampling counters in ms.
CONF_mInt32(periodic_counter_update_period_ms, "500");

CONF_Bool(enable_single_replica_load, "false");

// Port to download server for single replica load
CONF_Int32(single_replica_load_download_port, "8050");
// Number of download workers for single replica load
CONF_Int32(single_replica_load_download_num_workers, "64");

// Used for mini Load. mini load data file will be removed after this time.
CONF_Int64(load_data_reserve_hours, "4");
// log error log will be removed after this time
CONF_mInt64(load_error_log_reserve_hours, "48");
CONF_Int32(number_tablet_writer_threads, "16");
CONF_Int32(number_slave_replica_download_threads, "64");

// The maximum amount of data that can be processed by a stream load
CONF_mInt64(streaming_load_max_mb, "10240");
// Some data formats, such as JSON, cannot be streamed.
// Therefore, it is necessary to limit the maximum number of
// such data when using stream load to prevent excessive memory consumption.
CONF_mInt64(streaming_load_json_max_mb, "100");
// the alive time of a TabletsChannel.
// If the channel does not receive any data till this time,
// the channel will be removed.
CONF_mInt32(streaming_load_rpc_max_alive_time_sec, "1200");
// the timeout of a rpc to open the tablet writer in remote BE.
// short operation time, can set a short timeout
CONF_Int32(tablet_writer_open_rpc_timeout_sec, "60");
// You can ignore brpc error '[E1011]The server is overcrowded' when writing data.
CONF_mBool(tablet_writer_ignore_eovercrowded, "false");
CONF_mInt32(slave_replica_writer_rpc_timeout_sec, "60");
// Whether to enable stream load record function, the default is false.
// False: disable stream load record
CONF_mBool(enable_stream_load_record, "false");
// batch size of stream load record reported to FE
CONF_mInt32(stream_load_record_batch_size, "50");
// expire time of stream load record in rocksdb.
CONF_Int32(stream_load_record_expire_time_secs, "28800");
// time interval to clean expired stream load records
CONF_mInt64(clean_stream_load_record_interval_secs, "1800");
CONF_mBool(disable_stream_load_2pc, "false");

// OlapTableSink sender's send interval, should be less than the real response time of a tablet writer rpc.
// You may need to lower the speed when the sink receiver bes are too busy.
CONF_mInt32(olap_table_sink_send_interval_ms, "1");

// Fragment thread pool
CONF_Int32(fragment_pool_thread_num_min, "64");
CONF_Int32(fragment_pool_thread_num_max, "512");
CONF_Int32(fragment_pool_queue_size, "2048");

// Control the number of disks on the machine.  If 0, this comes from the system settings.
CONF_Int32(num_disks, "0");
// The maximum number of the threads per disk is also the max queue depth per disk.
CONF_Int32(num_threads_per_disk, "0");
// The read size is the size of the reads sent to os.
// There is a trade off of latency and throughout, trying to keep disks busy but
// not introduce seeks.  The literature seems to agree that with 8 MB reads, random
// io and sequential io perform similarly.
CONF_Int32(read_size, "8388608");    // 8 * 1024 * 1024, Read Size (in bytes)
CONF_Int32(min_buffer_size, "1024"); // 1024, The minimum read buffer size (in bytes)

// For each io buffer size, the maximum number of buffers the IoMgr will hold onto
// With 1024B through 8MB buffers, this is up to ~2GB of buffers.
CONF_Int32(max_free_io_buffers, "128");

// Whether to disable the memory cache pool,
// including MemPool, ChunkAllocator, BufferPool, DiskIO free buffer.
CONF_Bool(disable_mem_pools, "false");

// The reserved bytes limit of Chunk Allocator, usually set as a percentage of mem_limit.
// defaults to bytes if no unit is given, the number of bytes must be a multiple of 2.
// must larger than 0. and if larger than physical memory size, it will be set to physical memory size.
// increase this variable can improve performance,
// but will acquire more free memory which can not be used by other modules.
CONF_mString(chunk_reserved_bytes_limit, "10%");

// Whether using chunk allocator to cache memory chunk
CONF_Bool(disable_chunk_allocator, "true");
// Disable Chunk Allocator in Vectorized Allocator, this will reduce memory cache.
// For high concurrent queries, using Chunk Allocator with vectorized Allocator can reduce the impact
// of gperftools tcmalloc central lock.
// Jemalloc or google tcmalloc have core cache, Chunk Allocator may no longer be needed after replacing
// gperftools tcmalloc.
CONF_mBool(disable_chunk_allocator_in_vec, "true");

// Both MemPool and vectorized engine's podarray allocator, vectorized engine's arena will try to allocate memory as power of two.
// But if the memory is very large then power of two is also very large. This config means if the allocated memory's size is larger
// than this limit then all allocators will not use RoundUpToPowerOfTwo to allocate memory.
CONF_mInt64(memory_linear_growth_threshold, "134217728"); // 128Mb

// The probing algorithm of partitioned hash table.
// Enable quadratic probing hash table
CONF_Bool(enable_quadratic_probing, "false");

// for pprof
CONF_String(pprof_profile_dir, "${DORIS_HOME}/log");

// to forward compatibility, will be removed later
CONF_mBool(enable_token_check, "true");

// to open/close system metrics
CONF_Bool(enable_system_metrics, "true");

CONF_mBool(enable_prefetch, "true");

// Number of cores Doris will used, this will effect only when it's greater than 0.
// Otherwise, Doris will use all cores returned from "/proc/cpuinfo".
CONF_Int32(num_cores, "0");

// When BE start, If there is a broken disk, BE process will exit by default.
// Otherwise, we will ignore the broken disk,
CONF_Bool(ignore_broken_disk, "false");

// linux transparent huge page
CONF_Bool(madvise_huge_pages, "false");

// whether use mmap to allocate memory
CONF_Bool(mmap_buffers, "false");

// max memory can be allocated by buffer pool
// This is the percentage of mem_limit
CONF_String(buffer_pool_limit, "20%");

// clean page can be hold by buffer pool
// This is the percentage of buffer_pool_limit
CONF_String(buffer_pool_clean_pages_limit, "50%");

// Sleep time in seconds between memory maintenance iterations
CONF_mInt64(memory_maintenance_sleep_time_s, "10");

// Alignment
CONF_Int32(memory_max_alignment, "16");

// write buffer size before flush
CONF_mInt64(write_buffer_size, "209715200");

// max buffer size used in memtable for the aggregated table
CONF_mInt64(memtable_max_buffer_size, "419430400");

// following 2 configs limit the memory consumption of load process on a Backend.
// eg: memory limit to 80% of mem limit config but up to 100GB(default)
// NOTICE(cmy): set these default values very large because we don't want to
// impact the load performance when user upgrading Doris.
// user should set these configs properly if necessary.
CONF_Int64(load_process_max_memory_limit_bytes, "107374182400"); // 100GB
CONF_Int32(load_process_max_memory_limit_percent, "50");         // 50%

// If the memory consumption of load jobs exceed load_process_max_memory_limit,
// all load jobs will hang there to wait for memtable flush. We should have a
// soft limit which can trigger the memtable flush for the load channel who
// consumes lagest memory size before we reach the hard limit. The soft limit
// might avoid all load jobs hang at the same time.
CONF_Int32(load_process_soft_mem_limit_percent, "50");

// result buffer cancelled time (unit: second)
CONF_mInt32(result_buffer_cancelled_interval_time, "300");

// the increased frequency of priority for remaining tasks in BlockingPriorityQueue
CONF_mInt32(priority_queue_remaining_tasks_increased_frequency, "512");

// sync tablet_meta when modifying meta
CONF_mBool(sync_tablet_meta, "false");

// default thrift rpc timeout ms
CONF_mInt32(thrift_rpc_timeout_ms, "10000");

// txn commit rpc timeout
CONF_mInt32(txn_commit_rpc_timeout_ms, "10000");

// If set to true, metric calculator will run
CONF_Bool(enable_metric_calculator, "true");

// max consumer num in one data consumer group, for routine load
CONF_mInt32(max_consumer_num_per_group, "3");

// the size of thread pool for routine load task.
// this should be larger than FE config 'max_routine_load_task_num_per_be' (default 5)
CONF_Int32(routine_load_thread_pool_size, "10");

// max external scan cache batch count, means cache max_memory_cache_batch_count * batch_size row
// default is 20, batch_size's default value is 1024 means 20 * 1024 rows will be cached
CONF_mInt32(max_memory_sink_batch_count, "20");

// This configuration is used for the context gc thread schedule period
// note: unit is minute, default is 5min
CONF_mInt32(scan_context_gc_interval_min, "5");

// es scroll keep-alive
CONF_String(es_scroll_keepalive, "5m");

// HTTP connection timeout for es
CONF_mInt32(es_http_timeout_ms, "5000");

// the max client cache number per each host
// There are variety of client cache in BE, but currently we use the
// same cache size configuration.
// TODO(cmy): use different config to set different client cache if necessary.
CONF_Int32(max_client_cache_size_per_host, "10");

// Dir to save files downloaded by SmallFileMgr
CONF_String(small_file_dir, "${DORIS_HOME}/lib/small_file/");
// path gc
CONF_Bool(path_gc_check, "true");
CONF_mInt32(path_gc_check_interval_second, "86400");
CONF_mInt32(path_gc_check_step, "1000");
CONF_mInt32(path_gc_check_step_interval_ms, "10");
CONF_mInt32(path_scan_interval_second, "86400");

// The following 2 configs limit the max usage of disk capacity of a data dir.
// If both of these 2 threshold reached, no more data can be writen into that data dir.
// The percent of max used capacity of a data dir
CONF_mInt32(storage_flood_stage_usage_percent, "90"); // 90%
// The min bytes that should be left of a data dir
CONF_mInt64(storage_flood_stage_left_capacity_bytes, "1073741824"); // 1GB
// number of thread for flushing memtable per store
CONF_Int32(flush_thread_num_per_store, "6");
// number of thread for flushing memtable per store, for high priority load task
CONF_Int32(high_priority_flush_thread_num_per_store, "1");

// config for tablet meta checkpoint
CONF_mInt32(tablet_meta_checkpoint_min_new_rowsets_num, "10");
CONF_mInt32(tablet_meta_checkpoint_min_interval_secs, "600");
CONF_Int32(generate_tablet_meta_checkpoint_tasks_interval_secs, "600");

// config for default rowset type
// Valid configs: ALPHA, BETA
CONF_String(default_rowset_type, "BETA");

// Maximum size of a single message body in all protocols
CONF_Int64(brpc_max_body_size, "3147483648");
// Max unwritten bytes in each socket, if the limit is reached, Socket.Write fails with EOVERCROWDED
CONF_Int64(brpc_socket_max_unwritten_bytes, "1073741824");
// TODO(zxy): expect to be true in v1.3
// Whether to embed the ProtoBuf Request serialized string together with Tuple/Block data into
// Controller Attachment and send it through http brpc when the length of the Tuple/Block data
// is greater than 1.8G. This is to avoid the error of Request length overflow (2G).
CONF_mBool(transfer_large_data_by_brpc, "false");

// max number of txns for every txn_partition_map in txn manager
// this is a self protection to avoid too many txns saving in manager
CONF_mInt64(max_runnings_transactions_per_txn_map, "100");

// tablet_map_lock shard size, the value is 2^n, n=0,1,2,3,4
// this is a an enhancement for better performance to manage tablet
CONF_Int32(tablet_map_shard_size, "1");

// txn_map_lock shard size, the value is 2^n, n=0,1,2,3,4
// this is a an enhancement for better performance to manage txn
CONF_Int32(txn_map_shard_size, "128");

// txn_lock shard size, the value is 2^n, n=0,1,2,3,4
// this is a an enhancement for better performance to commit and publish txn
CONF_Int32(txn_shard_size, "1024");

// Whether to continue to start be when load tablet from header failed.
CONF_Bool(ignore_load_tablet_failure, "false");

// Whether to continue to start be when load tablet from header failed.
CONF_mBool(ignore_rowset_stale_unconsistent_delete, "false");

// Soft memory limit as a fraction of hard memory limit.
CONF_Double(soft_mem_limit_frac, "0.9");

// Set max cache's size of query results, the unit is M byte
CONF_Int32(query_cache_max_size_mb, "256");

// Cache memory is pruned when reach query_cache_max_size_mb + query_cache_elasticity_size_mb
CONF_Int32(query_cache_elasticity_size_mb, "128");

// Maximum number of cache partitions corresponding to a SQL
CONF_Int32(query_cache_max_partition_count, "1024");

// Maximum number of version of a tablet. If the version num of a tablet exceed limit,
// the load process will reject new incoming load job of this tablet.
// This is to avoid too many version num.
CONF_mInt32(max_tablet_version_num, "500");

// Frontend mainly use two thrift sever type: THREAD_POOL, THREADED_SELECTOR. if fe use THREADED_SELECTOR model for thrift server,
// the thrift_server_type_of_fe should be set THREADED_SELECTOR to make be thrift client to fe constructed with TFramedTransport
CONF_String(thrift_server_type_of_fe, "THREAD_POOL");

// disable zone map index when page row is too few
CONF_mInt32(zone_map_row_num_threshold, "20");

// aws sdk log level
//    Off = 0,
//    Fatal = 1,
//    Error = 2,
//    Warn = 3,
//    Info = 4,
//    Debug = 5,
//    Trace = 6
CONF_Int32(aws_log_level, "3");

// the buffer size when read data from remote storage like s3
CONF_mInt32(remote_storage_read_buffer_mb, "16");

// Whether Hook TCmalloc new/delete, currently consume/release tls mem tracker in Hook.
CONF_Bool(enable_tcmalloc_hook, "true");

// If true, switch TLS MemTracker to count more detailed memory,
// including caches such as ExecNode operators and TabletManager.
//
// At present, there is a performance problem in the frequent switch thread mem tracker.
// This is because the mem tracker exists as a shared_ptr in the thread local. Each time it is switched,
// the atomic variable use_count in the shared_ptr of the current tracker will be -1, and the tracker to be
// replaced use_count +1, multi-threading Frequent changes to the same tracker shared_ptr are slow.
// TODO: 1. Reduce unnecessary thread mem tracker switches,
//       2. Consider using raw pointers for mem tracker in thread local
CONF_Bool(memory_verbose_track, "false");

// The minimum length when TCMalloc Hook consumes/releases MemTracker, consume size
// smaller than this value will continue to accumulate. specified as number of bytes.
// Decreasing this value will increase the frequency of consume/release.
// Increasing this value will cause MemTracker statistics to be inaccurate.
CONF_mInt32(mem_tracker_consume_min_size_bytes, "1048576");

// The version information of the tablet will be stored in the memory
// in an adjacency graph data structure.
// And as the new version is written and the old version is deleted,
// the data structure will begin to have empty vertex with no edge associations(orphan vertex).
// This config is used to control that when the proportion of orphan vertex is greater than the threshold,
// the adjacency graph will be rebuilt to ensure that the data structure will not expand indefinitely.
// This config usually only needs to be modified during testing.
// In most cases, it does not need to be modified.
CONF_mDouble(tablet_version_graph_orphan_vertex_ratio, "0.1");

// if set runtime_filter_use_async_rpc true, publish runtime filter will be a async method
// else we will call sync method
CONF_mBool(runtime_filter_use_async_rpc, "true");

// max send batch parallelism for OlapTableSink
// The value set by the user for send_batch_parallelism is not allowed to exceed max_send_batch_parallelism_per_job,
// if exceed, the value of send_batch_parallelism would be max_send_batch_parallelism_per_job
CONF_mInt32(max_send_batch_parallelism_per_job, "5");
CONF_Validator(max_send_batch_parallelism_per_job,
               [](const int config) -> bool { return config >= 1; });

// number of send batch thread pool size
CONF_Int32(send_batch_thread_pool_thread_num, "64");
// number of send batch thread pool queue size
CONF_Int32(send_batch_thread_pool_queue_size, "102400");
// number of download cache thread pool size
CONF_Int32(download_cache_thread_pool_thread_num, "48");
// number of download cache thread pool queue size
CONF_Int32(download_cache_thread_pool_queue_size, "102400");
// download cache buffer size
CONF_Int64(download_cache_buffer_size, "10485760");

// Limit the number of segment of a newly created rowset.
// The newly created rowset may to be compacted after loading,
// so if there are too many segment in a rowset, the compaction process
// will run out of memory.
// When doing compaction, each segment may take at least 1MB buffer.
CONF_mInt32(max_segment_num_per_rowset, "200");

// The connection timeout when connecting to external table such as odbc table.
CONF_mInt32(external_table_connect_timeout_sec, "30");

// Global bitmap cache capacity for aggregation cache, size in bytes
CONF_Int64(delete_bitmap_agg_cache_capacity, "104857600");

// s3 config
CONF_mInt32(max_remote_storage_count, "10");

// reference https://github.com/edenhill/librdkafka/blob/master/INTRODUCTION.md#broker-version-compatibility
// If the dependent kafka broker version older than 0.10.0.0,
// the value of kafka_api_version_request should be false, and the
// value set by the fallback version kafka_broker_version_fallback will be used,
// and the valid values are: 0.9.0.x, 0.8.x.y.
CONF_String(kafka_api_version_request, "true");
CONF_String(kafka_broker_version_fallback, "0.10.0");

// The number of pool siz of routine load consumer.
// If you meet the error describe in https://github.com/edenhill/librdkafka/issues/3608
// Change this size to 0 to fix it temporarily.
CONF_Int32(routine_load_consumer_pool_size, "10");

// When the timeout of a load task is less than this threshold,
// Doris treats it as a high priority task.
// high priority tasks use a separate thread pool for flush and do not block rpc by memory cleanup logic.
// this threshold is mainly used to identify routine load tasks and should not be modified if not necessary.
CONF_mInt32(load_task_high_priority_threshold_second, "120");

// The min timeout of load rpc (add batch, close, etc.)
// Because a load rpc may be blocked for a while.
// Increase this config may avoid rpc timeout.
CONF_mInt32(min_load_rpc_timeout_ms, "20000");

// use which protocol to access function service, candicate is baidu_std/h2:grpc
CONF_String(function_service_protocol, "h2:grpc");

// use which load balancer to select server to connect
CONF_String(rpc_load_balancer, "rr");

// Enable tracing
// If this configuration is enabled, you should also specify the trace_export_url.
CONF_Bool(enable_tracing, "false");

// Enable opentelemtry collector
CONF_Bool(enable_otel_collector, "false");

// Current support for exporting traces:
// zipkin: Export traces directly to zipkin, which is used to enable the tracing feature quickly.
// collector: The collector can be used to receive and process traces and support export to a variety of
//   third-party systems.
CONF_mString(trace_exporter, "zipkin");
CONF_Validator(trace_exporter, [](const std::string& config) -> bool {
    return config == "zipkin" || config == "collector";
});

// The endpoint to export spans to.
// export to zipkin like: http://127.0.0.1:9411/api/v2/spans
// export to collector like: http://127.0.0.1:4318/v1/traces
CONF_String(trace_export_url, "http://127.0.0.1:9411/api/v2/spans");

// The maximum buffer/queue size to collect span. After the size is reached, spans are dropped.
// An export will be triggered when the number of spans in the queue reaches half of the maximum.
CONF_Int32(max_span_queue_size, "2048");

// The maximum batch size of every export spans. It must be smaller or equal to max_queue_size.
CONF_Int32(max_span_export_batch_size, "512");

// The time interval between two consecutive export spans.
CONF_Int32(export_span_schedule_delay_millis, "500");

// a soft limit of string type length, the hard limit is 2GB - 4, but if too long will cause very low performance,
// so we set a soft limit, default is 1MB
CONF_mInt32(string_type_length_soft_limit_bytes, "1048576");

CONF_Validator(string_type_length_soft_limit_bytes,
               [](const int config) -> bool { return config > 0 && config <= 2147483643; });

CONF_mInt32(jsonb_type_length_soft_limit_bytes, "1048576");

CONF_Validator(jsonb_type_length_soft_limit_bytes,
               [](const int config) -> bool { return config > 0 && config <= 2147483643; });

// used for olap scanner to save memory, when the size of unused_object_pool
// is greater than object_pool_buffer_size, release the object in the unused_object_pool.
CONF_Int32(object_pool_buffer_size, "100");

// ParquetReaderWrap prefetch buffer size
CONF_Int32(parquet_reader_max_buffer_size, "50");
// Max size of parquet page header in bytes
CONF_mInt32(parquet_header_max_size_mb, "1");
// Max buffer size for parquet row group
CONF_mInt32(parquet_rowgroup_max_buffer_mb, "128");
// Max buffer size for parquet chunk column
CONF_mInt32(parquet_column_max_buffer_mb, "8");

// OrcReader
CONF_mInt32(orc_natural_read_size_mb, "8");

// When the rows number reached this limit, will check the filter rate the of bloomfilter
// if it is lower than a specific threshold, the predicate will be disabled.
CONF_mInt32(bloom_filter_predicate_check_row_num, "20480");

CONF_Bool(enable_decimalv3, "false");

//whether turn on quick compaction feature
CONF_Bool(enable_quick_compaction, "false");
// For continuous versions that rows less than quick_compaction_max_rows will  trigger compaction quickly
CONF_Int32(quick_compaction_max_rows, "1000");
// min compaction versions
CONF_Int32(quick_compaction_batch_size, "10");
// do compaction min rowsets
CONF_Int32(quick_compaction_min_rowsets, "10");

// cooldown task configs
CONF_Int32(cooldown_thread_num, "5");
CONF_mInt64(generate_cooldown_task_interval_sec, "20");
CONF_mInt64(generate_cache_cleaner_task_interval_sec, "43200"); // 12 h
CONF_Int32(concurrency_per_dir, "2");
CONF_mInt64(cooldown_lag_time_sec, "10800");       // 3h
CONF_mInt64(max_sub_cache_file_size, "104857600"); // 100MB
CONF_mInt64(file_cache_alive_time_sec, "604800");  // 1 week
// file_cache_type is used to set the type of file cache for remote files.
// "": no cache, "sub_file_cache": split sub files from remote file.
// "whole_file_cache": the whole file.
CONF_mString(file_cache_type, "");
CONF_Validator(file_cache_type, [](const std::string config) -> bool {
    return config == "sub_file_cache" || config == "whole_file_cache" || config == "";
});
CONF_mInt64(file_cache_max_size_per_disk, "0"); // zero for no limit

CONF_Int32(s3_transfer_executor_pool_size, "2");

CONF_Bool(enable_time_lut, "true");
CONF_Bool(enable_simdjson_reader, "false");

// number of s3 scanner thread pool size
CONF_Int32(doris_remote_scanner_thread_pool_thread_num, "16");
// number of s3 scanner thread pool queue size
CONF_Int32(doris_remote_scanner_thread_pool_queue_size, "10240");

// If set to true, the new scan node framework will be used.
// This config should be removed when the new scan node is ready.
CONF_Bool(enable_new_scan_node, "true");

// limit the queue of pending batches which will be sent by a single nodechannel
CONF_mInt64(nodechannel_pending_queue_max_bytes, "67108864");

// Max waiting time to wait the "plan fragment start" rpc.
// If timeout, the fragment will be cancelled.
// This parameter is usually only used when the FE loses connection,
// and the BE can automatically cancel the relevant fragment after the timeout,
// so as to avoid occupying the execution thread for a long time.
CONF_mInt32(max_fragment_start_wait_time_seconds, "30");

// Temp config. True to use new file scan node to do load job. Will remove after fully test.
CONF_mBool(enable_new_load_scan_node, "false");

// Temp config. True to use new file scanner. Will remove after fully test.
CONF_mBool(enable_new_file_scanner, "false");

// Hide webserver page for safety.
// Hide the be config page for webserver.
CONF_Bool(hide_webserver_config_page, "false");

#ifdef BE_TEST
// test s3
CONF_String(test_s3_resource, "resource");
CONF_String(test_s3_ak, "ak");
CONF_String(test_s3_sk, "sk");
CONF_String(test_s3_endpoint, "endpoint");
CONF_String(test_s3_region, "region");
CONF_String(test_s3_bucket, "bucket");
CONF_String(test_s3_prefix, "prefix");
#endif
} // namespace config

} // namespace doris
