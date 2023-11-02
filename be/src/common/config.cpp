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

#include <fmt/core.h>
#include <stdint.h>

#include <algorithm>
#include <cctype>
// IWYU pragma: no_include <bthread/errno.h>
#include <lz4/lz4hc.h>

#include <cerrno> // IWYU pragma: keep
#include <cstdlib>
#include <cstring>
#include <fstream> // IWYU pragma: keep
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"

namespace doris {
namespace config {

// Dir of custom config file
DEFINE_String(custom_config_dir, "${DORIS_HOME}/conf");

// Dir of jdbc drivers
DEFINE_String(jdbc_drivers_dir, "${DORIS_HOME}/jdbc_drivers");

// cluster id
DEFINE_Int32(cluster_id, "-1");
// port on which BackendService is exported
DEFINE_Int32(be_port, "9060");

// port for brpc
DEFINE_Int32(brpc_port, "8060");

DEFINE_Int32(arrow_flight_sql_port, "-1");

// the number of bthreads for brpc, the default value is set to -1,
// which means the number of bthreads is #cpu-cores
DEFINE_Int32(brpc_num_threads, "256");

// Declare a selection strategy for those servers have many ips.
// Note that there should at most one ip match this list.
// this is a list in semicolon-delimited format, in CIDR notation, e.g. 10.10.10.0/24
// If no ip match this rule, will choose one randomly.
DEFINE_String(priority_networks, "");

// memory mode
// performance or compact
DEFINE_String(memory_mode, "moderate");

// process memory limit specified as number of bytes
// ('<int>[bB]?'), megabytes ('<float>[mM]'), gigabytes ('<float>[gG]'),
// or percentage of the physical memory ('<int>%').
// defaults to bytes if no unit is given"
// must larger than 0. and if larger than physical memory size,
// it will be set to physical memory size.
DEFINE_String(mem_limit, "80%");

// Soft memory limit as a fraction of hard memory limit.
DEFINE_Double(soft_mem_limit_frac, "0.9");

// Many modern allocators (for example, tcmalloc) do not do a mremap for
// realloc, even in case of large enough chunks of memory. Although this allows
// you to increase performance and reduce memory consumption during realloc.
// To fix this, we do mremap manually if the chunk of memory is large enough.
//
// The threshold (128 MB, 128 * (1ULL << 20)) is chosen quite large, since changing the address
// space is very slow, especially in the case of a large number of threads. We
// expect that the set of operations mmap/something to do/mremap can only be
// performed about 1000 times per second.
//
// P.S. This is also required, because tcmalloc can not allocate a chunk of
// memory greater than 16 GB.
DEFINE_mInt64(mmap_threshold, "134217728"); // bytes

// When hash table capacity is greater than 2^double_grow_degree(default 2G), grow when 75% of the capacity is satisfied.
// Increase can reduce the number of hash table resize, but may waste more memory.
DEFINE_mInt32(hash_table_double_grow_degree, "31");

DEFINE_mInt32(max_fill_rate, "2");

DEFINE_mInt32(double_resize_threshold, "23");
// Expand the hash table before inserting data, the maximum expansion size.
// There are fewer duplicate keys, reducing the number of resize hash tables
// There are many duplicate keys, and the hash table filled bucket is far less than the hash table build bucket.
DEFINE_mInt64(hash_table_pre_expanse_max_rows, "65535");

// The maximum low water mark of the system `/proc/meminfo/MemAvailable`, Unit byte, default 1.6G,
// actual low water mark=min(1.6G, MemTotal * 10%), avoid wasting too much memory on machines
// with large memory larger than 16G.
// Turn up max. On machines with more than 16G memory, more memory buffers will be reserved for Full GC.
// Turn down max. will use as much memory as possible.
DEFINE_Int64(max_sys_mem_available_low_water_mark_bytes, "1717986918");

// The size of the memory that gc wants to release each time, as a percentage of the mem limit.
DEFINE_mString(process_minor_gc_size, "10%");
DEFINE_mString(process_full_gc_size, "20%");

// If true, when the process does not exceed the soft mem limit, the query memory will not be limited;
// when the process memory exceeds the soft mem limit, the query with the largest ratio between the currently
// used memory and the exec_mem_limit will be canceled.
// If false, cancel query when the memory used exceeds exec_mem_limit, same as before.
DEFINE_mBool(enable_query_memory_overcommit, "true");

DEFINE_mBool(disable_memory_gc, "false");

DEFINE_mInt64(large_memory_check_bytes, "2147483648");

// The maximum time a thread waits for full GC. Currently only query will wait for full gc.
DEFINE_mInt32(thread_wait_gc_max_milliseconds, "1000");

DEFINE_mInt64(pre_serialize_keys_limit_bytes, "16777216");

// the port heartbeat service used
DEFINE_Int32(heartbeat_service_port, "9050");
// the count of heart beat service
DEFINE_Int32(heartbeat_service_thread_count, "1");
// the count of thread to create table
DEFINE_Int32(create_tablet_worker_count, "3");
// the count of thread to drop table
DEFINE_Int32(drop_tablet_worker_count, "3");
// the count of thread to batch load
DEFINE_Int32(push_worker_count_normal_priority, "3");
// the count of thread to high priority batch load
DEFINE_Int32(push_worker_count_high_priority, "3");
// the count of thread to publish version
DEFINE_Int32(publish_version_worker_count, "8");
// the count of tablet thread to publish version
DEFINE_Int32(tablet_publish_txn_max_thread, "32");
// the timeout of EnginPublishVersionTask
DEFINE_Int32(publish_version_task_timeout_s, "8");
// the count of thread to calc delete bitmap
DEFINE_Int32(calc_delete_bitmap_max_thread, "32");
// the count of thread to clear transaction task
DEFINE_Int32(clear_transaction_task_worker_count, "1");
// the count of thread to delete
DEFINE_Int32(delete_worker_count, "3");
// the count of thread to alter table
DEFINE_Int32(alter_tablet_worker_count, "3");
// the count of thread to alter index
DEFINE_Int32(alter_index_worker_count, "3");
// the count of thread to clone
DEFINE_Int32(clone_worker_count, "3");
// the count of thread to clone
DEFINE_Int32(storage_medium_migrate_count, "1");
// the count of thread to check consistency
DEFINE_Int32(check_consistency_worker_count, "1");
// the count of thread to upload
DEFINE_Int32(upload_worker_count, "1");
// the count of thread to download
DEFINE_Int32(download_worker_count, "1");
// the count of thread to make snapshot
DEFINE_Int32(make_snapshot_worker_count, "5");
// the count of thread to release snapshot
DEFINE_Int32(release_snapshot_worker_count, "5");
// the interval time(seconds) for agent report tasks signature to FE
DEFINE_mInt32(report_task_interval_seconds, "10");
// the interval time(seconds) for refresh storage policy from FE
DEFINE_mInt32(storage_refresh_storage_policy_task_interval_seconds, "5");
// the interval time(seconds) for agent report disk state to FE
DEFINE_mInt32(report_disk_state_interval_seconds, "60");
// the interval time(seconds) for agent report olap table to FE
DEFINE_mInt32(report_tablet_interval_seconds, "60");
// the max download speed(KB/s)
DEFINE_mInt32(max_download_speed_kbps, "50000");
// download low speed limit(KB/s)
DEFINE_mInt32(download_low_speed_limit_kbps, "50");
// download low speed time(seconds)
DEFINE_mInt32(download_low_speed_time, "300");
// sleep time for one second
DEFINE_Int32(sleep_one_second, "1");

// log dir
DEFINE_String(sys_log_dir, "${DORIS_HOME}/log");
DEFINE_String(user_function_dir, "${DORIS_HOME}/lib/udf");
// INFO, WARNING, ERROR, FATAL
DEFINE_String(sys_log_level, "INFO");
// TIME-DAY, TIME-HOUR, SIZE-MB-nnn
DEFINE_String(sys_log_roll_mode, "SIZE-MB-1024");
// log roll num
DEFINE_Int32(sys_log_roll_num, "10");
// verbose log
DEFINE_Strings(sys_log_verbose_modules, "");
// verbose log level
DEFINE_Int32(sys_log_verbose_level, "10");
// log buffer level
DEFINE_String(log_buffer_level, "");

// number of threads available to serve backend execution requests
DEFINE_Int32(be_service_threads, "64");

// Controls the number of threads to run work per core.  It's common to pick 2x
// or 3x the number of cores.  This keeps the cores busy without causing excessive
// thrashing.
DEFINE_Int32(num_threads_per_core, "3");
// if true, compresses tuple data in Serialize
DEFINE_mBool(compress_rowbatches, "true");
DEFINE_mBool(rowbatch_align_tuple_offset, "false");
// interval between profile reports; in seconds
DEFINE_mInt32(status_report_interval, "5");
// The pipeline task has a high concurrency, therefore reducing its report frequency
DEFINE_mInt32(pipeline_status_report_interval, "10");
// if true, each disk will have a separate thread pool for scanner
DEFINE_Bool(doris_enable_scanner_thread_pool_per_disk, "true");
// the timeout of a work thread to wait the blocking priority queue to get a task
DEFINE_mInt64(doris_blocking_priority_queue_wait_timeout_ms, "500");
// number of scanner thread pool size for olap table
// and the min thread num of remote scanner thread pool
DEFINE_Int32(doris_scanner_thread_pool_thread_num, "48");
DEFINE_Int32(doris_max_remote_scanner_thread_pool_thread_num, "-1");
// number of olap scanner thread pool queue size
DEFINE_Int32(doris_scanner_thread_pool_queue_size, "102400");
// default thrift client connect timeout(in seconds)
DEFINE_mInt32(thrift_connect_timeout_seconds, "3");
DEFINE_mInt32(fetch_rpc_timeout_seconds, "30");
// default thrift client retry interval (in milliseconds)
DEFINE_mInt64(thrift_client_retry_interval_ms, "1000");
// max row count number for single scan range, used in segmentv1
DEFINE_mInt32(doris_scan_range_row_count, "524288");
// max bytes number for single scan range, used in segmentv2
DEFINE_mInt32(doris_scan_range_max_mb, "1024");
// max bytes number for single scan block, used in segmentv2
DEFINE_mInt32(doris_scan_block_max_mb, "67108864");
// size of scanner queue between scanner thread and compute thread
DEFINE_mInt32(doris_scanner_queue_size, "1024");
// single read execute fragment row number
DEFINE_mInt32(doris_scanner_row_num, "16384");
// single read execute fragment row bytes
DEFINE_mInt32(doris_scanner_row_bytes, "10485760");
// number of max scan keys
DEFINE_mInt32(doris_max_scan_key_num, "48");
// the max number of push down values of a single column.
// if exceed, no conditions will be pushed down for that column.
DEFINE_mInt32(max_pushdown_conditions_per_column, "1024");
// return_row / total_row
DEFINE_mInt32(doris_max_pushdown_conjuncts_return_rate, "90");
// (Advanced) Maximum size of per-query receive-side buffer
DEFINE_mInt32(exchg_node_buffer_size_bytes, "20485760");

DEFINE_mInt64(column_dictionary_key_ratio_threshold, "0");
DEFINE_mInt64(column_dictionary_key_size_threshold, "0");
// memory_limitation_per_thread_for_schema_change_bytes unit bytes
DEFINE_mInt64(memory_limitation_per_thread_for_schema_change_bytes, "2147483648");
DEFINE_mInt64(memory_limitation_per_thread_for_storage_migration_bytes, "100000000");

DEFINE_mInt32(cache_prune_stale_interval, "10");
// the clean interval of tablet lookup cache
DEFINE_mInt32(tablet_lookup_cache_clean_interval, "30");
DEFINE_mInt32(disk_stat_monitor_interval, "5");
DEFINE_mInt32(unused_rowset_monitor_interval, "30");
DEFINE_String(storage_root_path, "${DORIS_HOME}/storage");
DEFINE_mString(broken_storage_path, "");

// Config is used to check incompatible old format hdr_ format
// whether doris uses strict way. When config is true, process will log fatal
// and exit. When config is false, process will only log warning.
DEFINE_Bool(storage_strict_check_incompatible_old_format, "true");

// BE process will exit if the percentage of error disk reach this value.
DEFINE_mInt32(max_percentage_of_error_disk, "100");
DEFINE_mInt32(default_num_rows_per_column_file_block, "1024");
// pending data policy
DEFINE_mInt32(pending_data_expire_time_sec, "1800");
// inc_rowset snapshot rs sweep time interval
DEFINE_mInt32(tablet_rowset_stale_sweep_time_sec, "300");
// tablet stale rowset sweep by threshold size
DEFINE_Bool(tablet_rowset_stale_sweep_by_size, "false");
DEFINE_mInt32(tablet_rowset_stale_sweep_threshold_size, "100");
// garbage sweep policy
DEFINE_Int32(max_garbage_sweep_interval, "3600");
DEFINE_Int32(min_garbage_sweep_interval, "180");
DEFINE_mInt32(garbage_sweep_batch_size, "100");
DEFINE_mInt32(snapshot_expire_time_sec, "172800");
// It is only a recommended value. When the disk space is insufficient,
// the file storage period under trash dose not have to comply with this parameter.
DEFINE_mInt32(trash_file_expire_time_sec, "259200");
// minimum file descriptor number
// modify them upon necessity
DEFINE_Int32(min_file_descriptor_number, "60000");
DEFINE_mBool(disable_segment_cache, "false");
DEFINE_Int64(index_stream_cache_capacity, "10737418240");
DEFINE_String(row_cache_mem_limit, "20%");

// Cache for storage page size
DEFINE_String(storage_page_cache_limit, "20%");
// Shard size for page cache, the value must be power of two.
// It's recommended to set it to a value close to the number of BE cores in order to reduce lock contentions.
DEFINE_Int32(storage_page_cache_shard_size, "256");
// Percentage for index page cache
// all storage page cache will be divided into data_page_cache and index_page_cache
DEFINE_Int32(index_page_cache_percentage, "10");
// whether to disable page cache feature in storage
DEFINE_Bool(disable_storage_page_cache, "false");
// whether to disable row cache feature in storage
DEFINE_Bool(disable_storage_row_cache, "true");
// whether to disable pk page cache feature in storage
DEFINE_Bool(disable_pk_storage_page_cache, "false");

// Cache for mow primary key storage page size
DEFINE_String(pk_storage_page_cache_limit, "10%");
// data page size for primary key index
DEFINE_Int32(primary_key_data_page_size, "32768");

DEFINE_mInt32(data_page_cache_stale_sweep_time_sec, "300");
DEFINE_mInt32(index_page_cache_stale_sweep_time_sec, "600");
DEFINE_mInt32(pk_index_page_cache_stale_sweep_time_sec, "600");

DEFINE_Bool(enable_low_cardinality_optimize, "true");
DEFINE_Bool(enable_low_cardinality_cache_code, "true");

// be policy
// whether check compaction checksum
DEFINE_mBool(enable_compaction_checksum, "false");
// whether disable automatic compaction task
DEFINE_mBool(disable_auto_compaction, "false");
// whether enable vertical compaction
DEFINE_mBool(enable_vertical_compaction, "true");
// whether enable ordered data compaction
DEFINE_mBool(enable_ordered_data_compaction, "true");
// In vertical compaction, column number for every group
DEFINE_mInt32(vertical_compaction_num_columns_per_group, "5");
// In vertical compaction, max memory usage for row_source_buffer
DEFINE_Int32(vertical_compaction_max_row_source_memory_mb, "200");
// In vertical compaction, max dest segment file size
DEFINE_mInt64(vertical_compaction_max_segment_size, "268435456");

// In ordered data compaction, min segment size for input rowset
DEFINE_mInt32(ordered_data_compaction_min_segment_size, "10485760");

// This config can be set to limit thread number in compaction thread pool.
DEFINE_mInt32(max_base_compaction_threads, "4");
DEFINE_mInt32(max_cumu_compaction_threads, "10");
DEFINE_mInt32(max_single_replica_compaction_threads, "10");

DEFINE_Bool(enable_base_compaction_idle_sched, "true");
DEFINE_mInt64(base_compaction_min_rowset_num, "5");
DEFINE_mDouble(base_compaction_min_data_ratio, "0.3");
DEFINE_mInt64(base_compaction_dup_key_max_file_size_mbytes, "1024");

DEFINE_Bool(enable_skip_tablet_compaction, "true");
// output rowset of cumulative compaction total disk size exceed this config size,
// this rowset will be given to base compaction, unit is m byte.
DEFINE_mInt64(compaction_promotion_size_mbytes, "1024");

// output rowset of cumulative compaction total disk size exceed this config ratio of
// base rowset's total disk size, this rowset will be given to base compaction. The value must be between
// 0 and 1.
DEFINE_mDouble(compaction_promotion_ratio, "0.05");

// the smallest size of rowset promotion. When the rowset is less than this config, this
// rowset will be not given to base compaction. The unit is m byte.
DEFINE_mInt64(compaction_promotion_min_size_mbytes, "128");

// When output rowset of cumulative compaction total version count (end_version - start_version)
// exceed this config count, the rowset will be moved to base compaction
// NOTE: this config will work for unique key merge-on-write table only, to reduce version count
// related cost on delete bitmap more effectively.
DEFINE_mInt64(compaction_promotion_version_count, "1000");

// The lower bound size to do cumulative compaction. When total disk size of candidate rowsets is less than
// this size, size_based policy may not do to cumulative compaction. The unit is m byte.
DEFINE_mInt64(compaction_min_size_mbytes, "64");

// cumulative compaction policy: min and max delta file's number
DEFINE_mInt64(cumulative_compaction_min_deltas, "5");
DEFINE_mInt64(cumulative_compaction_max_deltas, "1000");

// This config can be set to limit thread number in  multiget thread pool.
DEFINE_mInt32(multi_get_max_threads, "10");

// The upper limit of "permits" held by all compaction tasks. This config can be set to limit memory consumption for compaction.
DEFINE_mInt64(total_permits_for_compaction_score, "10000");

// sleep interval in ms after generated compaction tasks
DEFINE_mInt32(generate_compaction_tasks_interval_ms, "10");

// sleep interval in second after update replica infos
DEFINE_mInt32(update_replica_infos_interval_seconds, "60");

// Compaction task number per disk.
// Must be greater than 2, because Base compaction and Cumulative compaction have at least one thread each.
DEFINE_mInt32(compaction_task_num_per_disk, "4");
// compaction thread num for fast disk(typically .SSD), must be greater than 2.
DEFINE_mInt32(compaction_task_num_per_fast_disk, "8");
DEFINE_Validator(compaction_task_num_per_disk,
                 [](const int config) -> bool { return config >= 2; });
DEFINE_Validator(compaction_task_num_per_fast_disk,
                 [](const int config) -> bool { return config >= 2; });

// How many rounds of cumulative compaction for each round of base compaction when compaction tasks generation.
DEFINE_mInt32(cumulative_compaction_rounds_for_each_base_compaction_round, "9");

// Threshold to logging compaction trace, in seconds.
DEFINE_mInt32(base_compaction_trace_threshold, "60");
DEFINE_mInt32(cumulative_compaction_trace_threshold, "10");
DEFINE_mBool(disable_compaction_trace_log, "true");

// Interval to picking rowset to compact, in seconds
DEFINE_mInt64(pick_rowset_to_compact_interval_sec, "86400");

// Thread count to do tablet meta checkpoint, -1 means use the data directories count.
DEFINE_Int32(max_meta_checkpoint_threads, "-1");

// Threshold to logging agent task trace, in seconds.
DEFINE_mInt32(agent_task_trace_threshold_sec, "2");

// This config can be set to limit thread number in tablet migration thread pool.
DEFINE_Int32(min_tablet_migration_threads, "1");
DEFINE_Int32(max_tablet_migration_threads, "1");

DEFINE_mInt32(finished_migration_tasks_size, "10000");
// If size less than this, the remaining rowsets will be force to complete
DEFINE_mInt32(migration_remaining_size_threshold_mb, "10");
// If the task runs longer than this time, the task will be terminated, in seconds.
// tablet max size / migration min speed * factor = 10GB / 1MBps * 2 = 20480 seconds
DEFINE_mInt32(migration_task_timeout_secs, "20480");

// Port to start debug webserver on
DEFINE_Int32(webserver_port, "8040");
// Https enable flag
DEFINE_Bool(enable_https, "false");
// Path of certificate
DEFINE_String(ssl_certificate_path, "");
// Path of private key
DEFINE_String(ssl_private_key_path, "");
// Whether to check authorization
DEFINE_Bool(enable_all_http_auth, "false");
// Number of webserver workers
DEFINE_Int32(webserver_num_workers, "48");
// Period to update rate counters and sampling counters in ms.
DEFINE_mInt32(periodic_counter_update_period_ms, "500");

DEFINE_Bool(enable_single_replica_load, "false");
// Number of download workers for single replica load
DEFINE_Int32(single_replica_load_download_num_workers, "64");

// Used for mini Load. mini load data file will be removed after this time.
DEFINE_Int64(load_data_reserve_hours, "4");
// log error log will be removed after this time
DEFINE_mInt64(load_error_log_reserve_hours, "48");

DEFINE_Int32(brpc_heavy_work_pool_threads, "-1");
DEFINE_Int32(brpc_light_work_pool_threads, "-1");
DEFINE_Int32(brpc_heavy_work_pool_max_queue_size, "-1");
DEFINE_Int32(brpc_light_work_pool_max_queue_size, "-1");

// The maximum amount of data that can be processed by a stream load
DEFINE_mInt64(streaming_load_max_mb, "10240");
// Some data formats, such as JSON, cannot be streamed.
// Therefore, it is necessary to limit the maximum number of
// such data when using stream load to prevent excessive memory consumption.
DEFINE_mInt64(streaming_load_json_max_mb, "100");
// the alive time of a TabletsChannel.
// If the channel does not receive any data till this time,
// the channel will be removed.
DEFINE_mInt32(streaming_load_rpc_max_alive_time_sec, "1200");
// the timeout of a rpc to open the tablet writer in remote BE.
// short operation time, can set a short timeout
DEFINE_Int32(tablet_writer_open_rpc_timeout_sec, "60");
// You can ignore brpc error '[E1011]The server is overcrowded' when writing data.
DEFINE_mBool(tablet_writer_ignore_eovercrowded, "true");
DEFINE_mBool(exchange_sink_ignore_eovercrowded, "true");
DEFINE_mInt32(slave_replica_writer_rpc_timeout_sec, "60");
// Whether to enable stream load record function, the default is false.
// False: disable stream load record
DEFINE_mBool(enable_stream_load_record, "false");
// batch size of stream load record reported to FE
DEFINE_mInt32(stream_load_record_batch_size, "50");
// expire time of stream load record in rocksdb.
DEFINE_Int32(stream_load_record_expire_time_secs, "28800");
// time interval to clean expired stream load records
DEFINE_mInt64(clean_stream_load_record_interval_secs, "1800");
// The buffer size to store stream table function schema info
DEFINE_Int64(stream_tvf_buffer_size, "1048576"); // 1MB

// OlapTableSink sender's send interval, should be less than the real response time of a tablet writer rpc.
// You may need to lower the speed when the sink receiver bes are too busy.
DEFINE_mInt32(olap_table_sink_send_interval_ms, "1");

// Fragment thread pool
DEFINE_Int32(fragment_pool_thread_num_min, "64");
DEFINE_Int32(fragment_pool_thread_num_max, "2048");
DEFINE_Int32(fragment_pool_queue_size, "4096");

// Control the number of disks on the machine.  If 0, this comes from the system settings.
DEFINE_Int32(num_disks, "0");
// The maximum number of the threads per disk is also the max queue depth per disk.
DEFINE_Int32(num_threads_per_disk, "0");
// The read size is the size of the reads sent to os.
// There is a trade off of latency and throughout, trying to keep disks busy but
// not introduce seeks.  The literature seems to agree that with 8 MB reads, random
// io and sequential io perform similarly.
DEFINE_Int32(read_size, "8388608");    // 8 * 1024 * 1024, Read Size (in bytes)
DEFINE_Int32(min_buffer_size, "1024"); // 1024, The minimum read buffer size (in bytes)

// For each io buffer size, the maximum number of buffers the IoMgr will hold onto
// With 1024B through 8MB buffers, this is up to ~2GB of buffers.
DEFINE_Int32(max_free_io_buffers, "128");

// The probing algorithm of partitioned hash table.
// Enable quadratic probing hash table
DEFINE_Bool(enable_quadratic_probing, "false");

// for pprof
DEFINE_String(pprof_profile_dir, "${DORIS_HOME}/log");
// for jeprofile in jemalloc
DEFINE_mString(jeprofile_dir, "${DORIS_HOME}/log");
DEFINE_mBool(enable_je_purge_dirty_pages, "true");

// to forward compatibility, will be removed later
DEFINE_mBool(enable_token_check, "true");

// to open/close system metrics
DEFINE_Bool(enable_system_metrics, "true");

DEFINE_mBool(enable_prefetch, "true");

// Number of cores Doris will used, this will effect only when it's greater than 0.
// Otherwise, Doris will use all cores returned from "/proc/cpuinfo".
DEFINE_Int32(num_cores, "0");

// When BE start, If there is a broken disk, BE process will exit by default.
// Otherwise, we will ignore the broken disk,
DEFINE_Bool(ignore_broken_disk, "false");

// linux transparent huge page
DEFINE_Bool(madvise_huge_pages, "false");

// whether use mmap to allocate memory
DEFINE_Bool(mmap_buffers, "false");

// Sleep time in milliseconds between memory maintenance iterations
DEFINE_mInt32(memory_maintenance_sleep_time_ms, "100");

// After full gc, no longer full gc and minor gc during sleep.
// After minor gc, no minor gc during sleep, but full gc is possible.
DEFINE_mInt32(memory_gc_sleep_time_ms, "1000");

// Sleep time in milliseconds between memtbale flush mgr refresh iterations
DEFINE_mInt64(memtable_mem_tracker_refresh_interval_ms, "100");

// Alignment
DEFINE_Int32(memory_max_alignment, "16");

// max write buffer size before flush, default 200MB
DEFINE_mInt64(write_buffer_size, "209715200");
// max buffer size used in memtable for the aggregated table, default 400MB
DEFINE_mInt64(write_buffer_size_for_agg, "419430400");

DEFINE_Int32(load_process_max_memory_limit_percent, "50"); // 50%

// If the memory consumption of load jobs exceed load_process_max_memory_limit,
// all load jobs will hang there to wait for memtable flush. We should have a
// soft limit which can trigger the memtable flush for the load channel who
// consumes lagest memory size before we reach the hard limit. The soft limit
// might avoid all load jobs hang at the same time.
DEFINE_Int32(load_process_soft_mem_limit_percent, "80");

// result buffer cancelled time (unit: second)
DEFINE_mInt32(result_buffer_cancelled_interval_time, "300");

// the increased frequency of priority for remaining tasks in BlockingPriorityQueue
DEFINE_mInt32(priority_queue_remaining_tasks_increased_frequency, "512");

// sync tablet_meta when modifying meta
DEFINE_mBool(sync_tablet_meta, "true");

// default thrift rpc timeout ms
DEFINE_mInt32(thrift_rpc_timeout_ms, "60000");

// txn commit rpc timeout
DEFINE_mInt32(txn_commit_rpc_timeout_ms, "60000");

// If set to true, metric calculator will run
DEFINE_Bool(enable_metric_calculator, "true");

// max consumer num in one data consumer group, for routine load
DEFINE_mInt32(max_consumer_num_per_group, "3");

// the size of thread pool for routine load task.
// this should be larger than FE config 'max_routine_load_task_num_per_be' (default 5)
DEFINE_Int32(routine_load_thread_pool_size, "10");

// max external scan cache batch count, means cache max_memory_cache_batch_count * batch_size row
// default is 20, batch_size's default value is 1024 means 20 * 1024 rows will be cached
DEFINE_mInt32(max_memory_sink_batch_count, "20");

// This configuration is used for the context gc thread schedule period
// note: unit is minute, default is 5min
DEFINE_mInt32(scan_context_gc_interval_min, "5");

// es scroll keep-alive
DEFINE_String(es_scroll_keepalive, "5m");

// HTTP connection timeout for es
DEFINE_mInt32(es_http_timeout_ms, "5000");

// the max client cache number per each host
// There are variety of client cache in BE, but currently we use the
// same cache size configuration.
// TODO(cmy): use different config to set different client cache if necessary.
DEFINE_Int32(max_client_cache_size_per_host, "10");

// Dir to save files downloaded by SmallFileMgr
DEFINE_String(small_file_dir, "${DORIS_HOME}/lib/small_file/");
// path gc
DEFINE_Bool(path_gc_check, "true");
DEFINE_mInt32(path_gc_check_interval_second, "86400");
DEFINE_mInt32(path_gc_check_step, "1000");
DEFINE_mInt32(path_gc_check_step_interval_ms, "10");
DEFINE_mInt32(path_scan_interval_second, "86400");
DEFINE_mInt32(path_scan_step_interval_ms, "70");

// The following 2 configs limit the max usage of disk capacity of a data dir.
// If both of these 2 threshold reached, no more data can be writen into that data dir.
// The percent of max used capacity of a data dir
DEFINE_mInt32(storage_flood_stage_usage_percent, "90"); // 90%
// The min bytes that should be left of a data dir
DEFINE_mInt64(storage_flood_stage_left_capacity_bytes, "1073741824"); // 1GB
// number of thread for flushing memtable per store
DEFINE_Int32(flush_thread_num_per_store, "6");
// number of thread for flushing memtable per store, for high priority load task
DEFINE_Int32(high_priority_flush_thread_num_per_store, "6");

// config for tablet meta checkpoint
DEFINE_mInt32(tablet_meta_checkpoint_min_new_rowsets_num, "10");
DEFINE_mInt32(tablet_meta_checkpoint_min_interval_secs, "600");
DEFINE_Int32(generate_tablet_meta_checkpoint_tasks_interval_secs, "600");

// config for default rowset type
// Valid configs: ALPHA, BETA
DEFINE_String(default_rowset_type, "BETA");

// Maximum size of a single message body in all protocols
DEFINE_Int64(brpc_max_body_size, "3147483648");
DEFINE_Int64(brpc_socket_max_unwritten_bytes, "-1");
// TODO(zxy): expect to be true in v1.3
// Whether to embed the ProtoBuf Request serialized string together with Tuple/Block data into
// Controller Attachment and send it through http brpc when the length of the Tuple/Block data
// is greater than 1.8G. This is to avoid the error of Request length overflow (2G).
DEFINE_mBool(transfer_large_data_by_brpc, "false");

// max number of txns for every txn_partition_map in txn manager
// this is a self protection to avoid too many txns saving in manager
DEFINE_mInt64(max_runnings_transactions_per_txn_map, "2000");

// tablet_map_lock shard size, the value is 2^n, n=0,1,2,3,4
// this is a an enhancement for better performance to manage tablet
DEFINE_Int32(tablet_map_shard_size, "256");

// txn_map_lock shard size, the value is 2^n, n=0,1,2,3,4
// this is a an enhancement for better performance to manage txn
DEFINE_Int32(txn_map_shard_size, "1024");

// txn_lock shard size, the value is 2^n, n=0,1,2,3,4
// this is a an enhancement for better performance to commit and publish txn
DEFINE_Int32(txn_shard_size, "1024");

// Whether to continue to start be when load tablet from header failed.
DEFINE_Bool(ignore_load_tablet_failure, "false");

// Whether to continue to start be when load tablet from header failed.
DEFINE_mBool(ignore_rowset_stale_unconsistent_delete, "false");

// Set max cache's size of query results, the unit is M byte
DEFINE_Int32(query_cache_max_size_mb, "256");

// Cache memory is pruned when reach query_cache_max_size_mb + query_cache_elasticity_size_mb
DEFINE_Int32(query_cache_elasticity_size_mb, "128");

// Maximum number of cache partitions corresponding to a SQL
DEFINE_Int32(query_cache_max_partition_count, "1024");

// Maximum number of version of a tablet. If the version num of a tablet exceed limit,
// the load process will reject new incoming load job of this tablet.
// This is to avoid too many version num.
DEFINE_mInt32(max_tablet_version_num, "2000");

// Frontend mainly use two thrift sever type: THREAD_POOL, THREADED_SELECTOR. if fe use THREADED_SELECTOR model for thrift server,
// the thrift_server_type_of_fe should be set THREADED_SELECTOR to make be thrift client to fe constructed with TFramedTransport
DEFINE_String(thrift_server_type_of_fe, "THREAD_POOL");

// disable zone map index when page row is too few
DEFINE_mInt32(zone_map_row_num_threshold, "20");

// aws sdk log level
//    Off = 0,
//    Fatal = 1,
//    Error = 2,
//    Warn = 3,
//    Info = 4,
//    Debug = 5,
//    Trace = 6
// Default to turn off aws sdk log, because aws sdk errors that need to be cared will be output through Doris logs
DEFINE_Int32(aws_log_level, "0");

// the buffer size when read data from remote storage like s3
DEFINE_mInt32(remote_storage_read_buffer_mb, "16");

// The minimum length when TCMalloc Hook consumes/releases MemTracker, consume size
// smaller than this value will continue to accumulate. specified as number of bytes.
// Decreasing this value will increase the frequency of consume/release.
// Increasing this value will cause MemTracker statistics to be inaccurate.
DEFINE_mInt32(mem_tracker_consume_min_size_bytes, "1048576");

// The version information of the tablet will be stored in the memory
// in an adjacency graph data structure.
// And as the new version is written and the old version is deleted,
// the data structure will begin to have empty vertex with no edge associations(orphan vertex).
// This config is used to control that when the proportion of orphan vertex is greater than the threshold,
// the adjacency graph will be rebuilt to ensure that the data structure will not expand indefinitely.
// This config usually only needs to be modified during testing.
// In most cases, it does not need to be modified.
DEFINE_mDouble(tablet_version_graph_orphan_vertex_ratio, "0.1");

// share delta writers when memtable_on_sink_node = true
DEFINE_Bool(share_delta_writers, "true");
// number of brpc stream per load
DEFINE_Int32(num_streams_per_load, "5");
// timeout for open load stream rpc in ms
DEFINE_Int64(open_load_stream_timeout_ms, "500");

// max send batch parallelism for OlapTableSink
// The value set by the user for send_batch_parallelism is not allowed to exceed max_send_batch_parallelism_per_job,
// if exceed, the value of send_batch_parallelism would be max_send_batch_parallelism_per_job
DEFINE_mInt32(max_send_batch_parallelism_per_job, "5");
DEFINE_Validator(max_send_batch_parallelism_per_job,
                 [](const int config) -> bool { return config >= 1; });

// number of send batch thread pool size
DEFINE_Int32(send_batch_thread_pool_thread_num, "64");
// number of send batch thread pool queue size
DEFINE_Int32(send_batch_thread_pool_queue_size, "102400");
// number of download cache thread pool size
DEFINE_Int32(download_cache_thread_pool_thread_num, "48");
// number of download cache thread pool queue size
DEFINE_Int32(download_cache_thread_pool_queue_size, "102400");
// download cache buffer size
DEFINE_Int64(download_cache_buffer_size, "10485760");

// Limit the number of segment of a newly created rowset.
// The newly created rowset may to be compacted after loading,
// so if there are too many segment in a rowset, the compaction process
// will run out of memory.
// When doing compaction, each segment may take at least 1MB buffer.
DEFINE_mInt32(max_segment_num_per_rowset, "1000");
DEFINE_mInt32(segment_compression_threshold_kb, "256");

// The connection timeout when connecting to external table such as odbc table.
DEFINE_mInt32(external_table_connect_timeout_sec, "30");

// Global bitmap cache capacity for aggregation cache, size in bytes
DEFINE_Int64(delete_bitmap_agg_cache_capacity, "104857600");

// s3 config
DEFINE_mInt32(max_remote_storage_count, "10");

// reference https://github.com/edenhill/librdkafka/blob/master/INTRODUCTION.md#broker-version-compatibility
// If the dependent kafka broker version older than 0.10.0.0,
// the value of kafka_api_version_request should be false, and the
// value set by the fallback version kafka_broker_version_fallback will be used,
// and the valid values are: 0.9.0.x, 0.8.x.y.
DEFINE_String(kafka_api_version_request, "true");
DEFINE_String(kafka_broker_version_fallback, "0.10.0");
DEFINE_String(kafka_debug, "disable");

// The number of pool siz of routine load consumer.
// If you meet the error describe in https://github.com/edenhill/librdkafka/issues/3608
// Change this size to 0 to fix it temporarily.
DEFINE_Int32(routine_load_consumer_pool_size, "10");

// Used in single-stream-multi-table load. When receive a batch of messages from kafka,
// if the size of batch is more than this threshold, we will request plans for all related tables.
DEFINE_Int32(multi_table_batch_plan_threshold, "200");

// When the timeout of a load task is less than this threshold,
// Doris treats it as a high priority task.
// high priority tasks use a separate thread pool for flush and do not block rpc by memory cleanup logic.
// this threshold is mainly used to identify routine load tasks and should not be modified if not necessary.
DEFINE_mInt32(load_task_high_priority_threshold_second, "120");

// The min timeout of load rpc (add batch, close, etc.)
// Because a load rpc may be blocked for a while.
// Increase this config may avoid rpc timeout.
DEFINE_mInt32(min_load_rpc_timeout_ms, "20000");

// use which protocol to access function service, candicate is baidu_std/h2:grpc
DEFINE_String(function_service_protocol, "h2:grpc");

// use which load balancer to select server to connect
DEFINE_String(rpc_load_balancer, "rr");

// Enable tracing
// If this configuration is enabled, you should also specify the trace_export_url.
DEFINE_Bool(enable_tracing, "false");

// Enable opentelemtry collector
DEFINE_Bool(enable_otel_collector, "false");

// Current support for exporting traces:
// zipkin: Export traces directly to zipkin, which is used to enable the tracing feature quickly.
// collector: The collector can be used to receive and process traces and support export to a variety of
//   third-party systems.
DEFINE_mString(trace_exporter, "zipkin");
DEFINE_Validator(trace_exporter, [](const std::string& config) -> bool {
    return config == "zipkin" || config == "collector";
});

// The endpoint to export spans to.
// export to zipkin like: http://127.0.0.1:9411/api/v2/spans
// export to collector like: http://127.0.0.1:4318/v1/traces
DEFINE_String(trace_export_url, "http://127.0.0.1:9411/api/v2/spans");

// The maximum buffer/queue size to collect span. After the size is reached, spans are dropped.
// An export will be triggered when the number of spans in the queue reaches half of the maximum.
DEFINE_Int32(max_span_queue_size, "2048");

// The maximum batch size of every export spans. It must be smaller or equal to max_queue_size.
DEFINE_Int32(max_span_export_batch_size, "512");

// The time interval between two consecutive export spans.
DEFINE_Int32(export_span_schedule_delay_millis, "500");

// a soft limit of string type length, the hard limit is 2GB - 4, but if too long will cause very low performance,
// so we set a soft limit, default is 1MB
DEFINE_mInt32(string_type_length_soft_limit_bytes, "1048576");

DEFINE_Validator(string_type_length_soft_limit_bytes,
                 [](const int config) -> bool { return config > 0 && config <= 2147483643; });

DEFINE_mInt32(jsonb_type_length_soft_limit_bytes, "1048576");

DEFINE_Validator(jsonb_type_length_soft_limit_bytes,
                 [](const int config) -> bool { return config > 0 && config <= 2147483643; });

// used for olap scanner to save memory, when the size of unused_object_pool
// is greater than object_pool_buffer_size, release the object in the unused_object_pool.
DEFINE_Int32(object_pool_buffer_size, "100");

// Threshold of reading a small file into memory
DEFINE_mInt32(in_memory_file_size, "1048576"); // 1MB

// ParquetReaderWrap prefetch buffer size
DEFINE_Int32(parquet_reader_max_buffer_size, "50");
// Max size of parquet page header in bytes
DEFINE_mInt32(parquet_header_max_size_mb, "1");
// Max buffer size for parquet row group
DEFINE_mInt32(parquet_rowgroup_max_buffer_mb, "128");
// Max buffer size for parquet chunk column
DEFINE_mInt32(parquet_column_max_buffer_mb, "8");
DEFINE_mDouble(max_amplified_read_ratio, "0.8");

// OrcReader
DEFINE_mInt32(orc_natural_read_size_mb, "8");
DEFINE_mInt64(big_column_size_buffer, "65535");
DEFINE_mInt64(small_column_size_buffer, "100");

// When the rows number reached this limit, will check the filter rate the of bloomfilter
// if it is lower than a specific threshold, the predicate will be disabled.
DEFINE_mInt32(bloom_filter_predicate_check_row_num, "204800");

// cooldown task configs
DEFINE_Int32(cooldown_thread_num, "5");
DEFINE_mInt64(generate_cooldown_task_interval_sec, "20");
DEFINE_mInt32(remove_unused_remote_files_interval_sec, "21600"); // 6h
DEFINE_mInt32(confirm_unused_remote_files_interval_sec, "60");
DEFINE_Int32(cold_data_compaction_thread_num, "2");
DEFINE_mInt32(cold_data_compaction_interval_sec, "1800");
DEFINE_Int32(concurrency_per_dir, "2");
// file_cache_type is used to set the type of file cache for remote files.
// "": no cache, "sub_file_cache": split sub files from remote file.
// "whole_file_cache": the whole file.
DEFINE_mString(file_cache_type, "file_block_cache");
DEFINE_Validator(file_cache_type, [](std::string_view config) -> bool {
    return config == "" || config == "file_block_cache";
});

DEFINE_Int32(s3_transfer_executor_pool_size, "2");

DEFINE_Bool(enable_time_lut, "true");
DEFINE_Bool(enable_simdjson_reader, "true");

DEFINE_mBool(enable_query_like_bloom_filter, "true");
// number of s3 scanner thread pool size
DEFINE_Int32(doris_remote_scanner_thread_pool_thread_num, "48");
// number of s3 scanner thread pool queue size
DEFINE_Int32(doris_remote_scanner_thread_pool_queue_size, "102400");

// limit the queue of pending batches which will be sent by a single nodechannel
DEFINE_mInt64(nodechannel_pending_queue_max_bytes, "67108864");

// The batch size for sending data by brpc streaming client
DEFINE_mInt64(brpc_streaming_client_batch_bytes, "262144");

// Max waiting time to wait the "plan fragment start" rpc.
// If timeout, the fragment will be cancelled.
// This parameter is usually only used when the FE loses connection,
// and the BE can automatically cancel the relevant fragment after the timeout,
// so as to avoid occupying the execution thread for a long time.
DEFINE_mInt32(max_fragment_start_wait_time_seconds, "30");

// Node role tag for backend. Mix role is the default role, and computation role have no
// any tablet.
DEFINE_String(be_node_role, "mix");

// Hide webserver page for safety.
// Hide the be config page for webserver.
DEFINE_Bool(hide_webserver_config_page, "false");

DEFINE_Bool(enable_segcompaction, "true");

// Max number of segments allowed in a single segcompaction task.
DEFINE_Int32(segcompaction_batch_size, "10");

// Max row count allowed in a single source segment, bigger segments will be skipped.
DEFINE_Int32(segcompaction_candidate_max_rows, "1048576");

// Max file size allowed in a single source segment, bigger segments will be skipped.
DEFINE_Int64(segcompaction_candidate_max_bytes, "104857600");

// Max total row count allowed in a single segcompaction task.
DEFINE_Int32(segcompaction_task_max_rows, "1572864");

// Max total file size allowed in a single segcompaction task.
DEFINE_Int64(segcompaction_task_max_bytes, "157286400");

// Global segcompaction thread pool size.
DEFINE_mInt32(segcompaction_num_threads, "5");

// enable java udf and jdbc scannode
DEFINE_Bool(enable_java_support, "true");

// Set config randomly to check more issues in github workflow
DEFINE_Bool(enable_fuzzy_mode, "false");

DEFINE_Bool(enable_debug_points, "false");

DEFINE_Int32(pipeline_executor_size, "0");
DEFINE_Bool(enable_workload_group_for_scan, "false");

// Temp config. True to use optimization for bitmap_index apply predicate except leaf node of the and node.
// Will remove after fully test.
DEFINE_Bool(enable_index_apply_preds_except_leafnode_of_andnode, "true");

// block file cache
DEFINE_Bool(enable_file_cache, "false");
// format: [{"path":"/path/to/file_cache","total_size":21474836480,"query_limit":10737418240}]
// format: [{"path":"/path/to/file_cache","total_size":21474836480,"query_limit":10737418240},{"path":"/path/to/file_cache2","total_size":21474836480,"query_limit":10737418240}]
DEFINE_String(file_cache_path, "");
DEFINE_Int64(file_cache_max_file_segment_size, "4194304"); // 4MB
// 4KB <= file_cache_max_file_segment_size <= 256MB
DEFINE_Validator(file_cache_max_file_segment_size, [](const int64_t config) -> bool {
    return config >= 4096 && config <= 268435456;
});
DEFINE_Int64(file_cache_min_file_segment_size, "1048576"); // 1MB
// 4KB <= file_cache_min_file_segment_size <= 256MB
DEFINE_Validator(file_cache_min_file_segment_size, [](const int64_t config) -> bool {
    return config >= 4096 && config <= 268435456 &&
           config <= config::file_cache_max_file_segment_size;
});
DEFINE_Bool(clear_file_cache, "false");
DEFINE_Bool(enable_file_cache_query_limit, "false");
DEFINE_mInt32(file_cache_wait_sec_after_fail, "0"); // // zero for no waiting and retrying

DEFINE_mInt32(index_cache_entry_stay_time_after_lookup_s, "1800");
DEFINE_mInt32(inverted_index_cache_stale_sweep_time_sec, "600");
// inverted index searcher cache size
DEFINE_String(inverted_index_searcher_cache_limit, "10%");
// set `true` to enable insert searcher into cache when write inverted index data
DEFINE_Bool(enable_write_index_searcher_cache, "true");
DEFINE_Bool(enable_inverted_index_cache_check_timestamp, "true");
DEFINE_Int32(inverted_index_fd_number_limit_percent, "40"); // 40%

// inverted index match bitmap cache size
DEFINE_String(inverted_index_query_cache_limit, "10%");

// inverted index
DEFINE_mDouble(inverted_index_ram_buffer_size, "512");
DEFINE_Int32(query_bkd_inverted_index_limit_percent, "5"); // 5%
// dict path for chinese analyzer
DEFINE_String(inverted_index_dict_path, "${DORIS_HOME}/dict");
DEFINE_Int32(inverted_index_read_buffer_size, "4096");
// tree depth for bkd index
DEFINE_Int32(max_depth_in_bkd_tree, "32");
// index compaction
DEFINE_Bool(inverted_index_compaction_enable, "false");
// use num_broadcast_buffer blocks as buffer to do broadcast
DEFINE_Int32(num_broadcast_buffer, "32");
// semi-structure configs
DEFINE_Bool(enable_parse_multi_dimession_array, "false");

// max depth of expression tree allowed.
DEFINE_Int32(max_depth_of_expr_tree, "600");

// Report a tablet as bad when io errors occurs more than this value.
DEFINE_mInt64(max_tablet_io_errors, "-1");

// Report a tablet as bad when its path not found
DEFINE_Int32(tablet_path_check_interval_seconds, "-1");
DEFINE_mInt32(tablet_path_check_batch_size, "1000");

// Page size of row column, default 4KB
DEFINE_mInt64(row_column_page_size, "4096");
// it must be larger than or equal to 5MB
DEFINE_mInt32(s3_write_buffer_size, "5242880");
// the size of the whole s3 buffer pool, which indicates the s3 file writer
// can at most buffer 50MB data. And the num of multi part upload task is
// s3_write_buffer_whole_size / s3_write_buffer_size
DEFINE_mInt32(s3_write_buffer_whole_size, "524288000");
// The timeout config for S3 buffer allocation
DEFINE_mInt32(s3_writer_buffer_allocation_timeout, "300");
DEFINE_mInt64(file_cache_max_file_reader_cache_size, "1000000");

//disable shrink memory by default
DEFINE_Bool(enable_shrink_memory, "false");
DEFINE_mInt32(schema_cache_capacity, "1024");
DEFINE_mInt32(schema_cache_sweep_time_sec, "100");

// max number of segment cache, default -1 for backward compatibility fd_number*2/5
DEFINE_mInt32(segment_cache_capacity, "-1");

// enable feature binlog, default false
DEFINE_Bool(enable_feature_binlog, "false");

// enable set in BitmapValue
DEFINE_Bool(enable_set_in_bitmap_value, "false");

DEFINE_Int64(max_hdfs_file_handle_cache_num, "20000");
DEFINE_Int64(max_external_file_meta_cache_num, "20000");
// Apply delete pred in cumu compaction
DEFINE_mBool(enable_delete_when_cumu_compaction, "false");

// max_write_buffer_number for rocksdb
DEFINE_Int32(rocksdb_max_write_buffer_number, "5");

DEFINE_Bool(allow_invalid_decimalv2_literal, "false");
DEFINE_mInt64(kerberos_expiration_time_seconds, "43200");

DEFINE_mString(get_stack_trace_tool, "libunwind");
DEFINE_mString(dwarf_location_info_mode, "FAST");

// the ratio of _prefetch_size/_batch_size in AutoIncIDBuffer
DEFINE_mInt64(auto_inc_prefetch_size_ratio, "10");

// the ratio of _low_level_water_level_mark/_batch_size in AutoIncIDBuffer
DEFINE_mInt64(auto_inc_low_water_level_mark_size_ratio, "3");

// number of threads that fetch auto-inc ranges from FE
DEFINE_mInt64(auto_inc_fetch_thread_num, "3");
// default 4GB
DEFINE_mInt64(lookup_connection_cache_bytes_limit, "4294967296");

// level of compression when using LZ4_HC, whose defalut value is LZ4HC_CLEVEL_DEFAULT
DEFINE_mInt64(LZ4_HC_compression_level, "9");

DEFINE_mBool(enable_merge_on_write_correctness_check, "true");

// The secure path with user files, used in the `local` table function.
DEFINE_mString(user_files_secure_path, "${DORIS_HOME}");

DEFINE_Int32(partition_topn_partition_threshold, "1024");

DEFINE_Int32(fe_expire_duration_seconds, "60");

DEFINE_Int32(grace_shutdown_wait_seconds, "120");

DEFINE_Int16(bitmap_serialize_version, "1");

// group commit insert config
DEFINE_String(group_commit_replay_wal_dir, "./wal");
DEFINE_Int32(group_commit_replay_wal_retry_num, "10");
DEFINE_Int32(group_commit_replay_wal_retry_interval_seconds, "5");
DEFINE_Int32(group_commit_sync_wal_batch, "10");

// the count of thread to group commit insert
DEFINE_Int32(group_commit_insert_threads, "10");
DEFINE_mInt32(group_commit_interval_ms, "10000");

DEFINE_mInt32(scan_thread_nice_value, "0");
DEFINE_mInt32(tablet_schema_cache_recycle_interval, "86400");

DEFINE_Bool(exit_on_exception, "false");
// This config controls whether the s3 file writer would flush cache asynchronously
DEFINE_Bool(enable_flush_file_cache_async, "true");

// cgroup
DEFINE_String(doris_cgroup_cpu_path, "");
DEFINE_Bool(enable_cgroup_cpu_soft_limit, "false");

DEFINE_Bool(ignore_always_true_predicate_for_segment, "true");

// Dir of default timezone files
DEFINE_String(default_tzfiles_path, "${DORIS_HOME}/zoneinfo");

// Max size(bytes) of group commit queues, used for mem back pressure.
DEFINE_Int32(group_commit_max_queue_size, "65536");

// Ingest binlog work pool size, -1 is disable, 0 is hardware concurrency
DEFINE_Int32(ingest_binlog_work_pool_size, "-1");

// Download binlog rate limit, unit is KB/s, 0 means no limit
DEFINE_Int32(download_binlog_rate_limit_kbs, "0");

// clang-format off
#ifdef BE_TEST
// test s3
DEFINE_String(test_s3_resource, "resource");
DEFINE_String(test_s3_ak, "ak");
DEFINE_String(test_s3_sk, "sk");
DEFINE_String(test_s3_endpoint, "endpoint");
DEFINE_String(test_s3_region, "region");
DEFINE_String(test_s3_bucket, "bucket");
DEFINE_String(test_s3_prefix, "prefix");
#endif
// clang-format on

std::map<std::string, Register::Field>* Register::_s_field_map = nullptr;
std::map<std::string, std::function<bool()>>* RegisterConfValidator::_s_field_validator = nullptr;
std::map<std::string, std::string>* full_conf_map = nullptr;

std::mutex custom_conf_lock;

std::mutex mutable_string_config_lock;

// trim string
std::string& trim(std::string& s) {
    // rtrim
    s.erase(std::find_if(s.rbegin(), s.rend(), [](unsigned char c) { return !std::isspace(c); })
                    .base(),
            s.end());
    // ltrim
    s.erase(s.begin(),
            std::find_if(s.begin(), s.end(), [](unsigned char c) { return !std::isspace(c); }));
    return s;
}

// split string by '='
void splitkv(const std::string& s, std::string& k, std::string& v) {
    const char sep = '=';
    int start = 0;
    int end = 0;
    if ((end = s.find(sep, start)) != std::string::npos) {
        k = s.substr(start, end - start);
        v = s.substr(end + 1);
    } else {
        k = s;
        v = "";
    }
}

// replace env variables
bool replaceenv(std::string& s) {
    std::size_t pos = 0;
    std::size_t start = 0;
    while ((start = s.find("${", pos)) != std::string::npos) {
        std::size_t end = s.find("}", start + 2);
        if (end == std::string::npos) {
            return false;
        }
        std::string envkey = s.substr(start + 2, end - start - 2);
        const char* envval = std::getenv(envkey.c_str());
        if (envval == nullptr) {
            return false;
        }
        s.erase(start, end - start + 1);
        s.insert(start, envval);
        pos = start + strlen(envval);
    }
    return true;
}

bool strtox(const std::string& valstr, bool& retval);
bool strtox(const std::string& valstr, int16_t& retval);
bool strtox(const std::string& valstr, int32_t& retval);
bool strtox(const std::string& valstr, int64_t& retval);
bool strtox(const std::string& valstr, double& retval);
bool strtox(const std::string& valstr, std::string& retval);

template <typename T>
bool strtox(const std::string& valstr, std::vector<T>& retval) {
    std::stringstream ss(valstr);
    std::string item;
    T t;
    while (std::getline(ss, item, ',')) {
        if (!strtox(trim(item), t)) {
            return false;
        }
        retval.push_back(t);
    }
    return true;
}

bool strtox(const std::string& valstr, bool& retval) {
    if (valstr.compare("true") == 0) {
        retval = true;
    } else if (valstr.compare("false") == 0) {
        retval = false;
    } else {
        return false;
    }
    return true;
}

template <typename T>
bool strtointeger(const std::string& valstr, T& retval) {
    if (valstr.length() == 0) {
        return false; // empty-string is only allowed for string type.
    }
    char* end;
    errno = 0;
    const char* valcstr = valstr.c_str();
    int64_t ret64 = strtoll(valcstr, &end, 10);
    if (errno || end != valcstr + strlen(valcstr)) {
        return false; // bad parse
    }
    T tmp = retval;
    retval = static_cast<T>(ret64);
    if (retval != ret64) {
        retval = tmp;
        return false;
    }
    return true;
}

bool strtox(const std::string& valstr, int16_t& retval) {
    return strtointeger(valstr, retval);
}

bool strtox(const std::string& valstr, int32_t& retval) {
    return strtointeger(valstr, retval);
}

bool strtox(const std::string& valstr, int64_t& retval) {
    return strtointeger(valstr, retval);
}

bool strtox(const std::string& valstr, double& retval) {
    if (valstr.length() == 0) {
        return false; // empty-string is only allowed for string type.
    }
    char* end = nullptr;
    errno = 0;
    const char* valcstr = valstr.c_str();
    retval = strtod(valcstr, &end);
    if (errno || end != valcstr + strlen(valcstr)) {
        return false; // bad parse
    }
    return true;
}

bool strtox(const std::string& valstr, std::string& retval) {
    retval = valstr;
    return true;
}

template <typename T>
bool convert(const std::string& value, T& retval) {
    std::string valstr(value);
    trim(valstr);
    if (!replaceenv(valstr)) {
        return false;
    }
    return strtox(valstr, retval);
}

// load conf file
bool Properties::load(const char* conf_file, bool must_exist) {
    // if conf_file is null, use the empty props
    if (conf_file == nullptr) {
        return true;
    }

    // open the conf file
    std::ifstream input(conf_file);
    if (!input.is_open()) {
        if (must_exist) {
            std::cerr << "config::load() failed to open the file:" << conf_file << std::endl;
            return false;
        }
        return true;
    }

    // load properties
    std::string line;
    std::string key;
    std::string value;
    line.reserve(512);
    while (input) {
        // read one line at a time
        std::getline(input, line);

        // remove left and right spaces
        trim(line);

        // ignore comments
        if (line.empty() || line[0] == '#') {
            continue;
        }

        // read key and value
        splitkv(line, key, value);
        trim(key);
        trim(value);

        // insert into file_conf_map
        file_conf_map[key] = value;
    }

    // close the conf file
    input.close();

    return true;
}

template <typename T>
bool Properties::get_or_default(const char* key, const char* defstr, T& retval,
                                bool* is_retval_set) const {
    const auto& it = file_conf_map.find(std::string(key));
    std::string valstr;
    if (it == file_conf_map.end()) {
        if (defstr == nullptr) {
            // Not found in conf map, and no default value need to be set, just return
            *is_retval_set = false;
            return true;
        } else {
            valstr = std::string(defstr);
        }
    } else {
        valstr = it->second;
    }
    *is_retval_set = true;
    return convert(valstr, retval);
}

void Properties::set(const std::string& key, const std::string& val) {
    file_conf_map.emplace(key, val);
}

void Properties::set_force(const std::string& key, const std::string& val) {
    file_conf_map[key] = val;
}

Status Properties::dump(const std::string& conffile) {
    std::string conffile_tmp = conffile + ".tmp";
    io::FileWriterPtr file_writer;
    RETURN_IF_ERROR(io::global_local_filesystem()->create_file(conffile_tmp, &file_writer));
    RETURN_IF_ERROR(file_writer->append("# THIS IS AN AUTO GENERATED CONFIG FILE.\n"));
    RETURN_IF_ERROR(file_writer->append(
            "# You can modify this file manually, and the configurations in this file\n"));
    RETURN_IF_ERROR(file_writer->append("# will overwrite the configurations in be.conf\n\n"));

    for (auto const& iter : file_conf_map) {
        RETURN_IF_ERROR(file_writer->append(iter.first));
        RETURN_IF_ERROR(file_writer->append(" = "));
        RETURN_IF_ERROR(file_writer->append(iter.second));
        RETURN_IF_ERROR(file_writer->append("\n"));
    }

    RETURN_IF_ERROR(file_writer->close());

    return io::global_local_filesystem()->rename(conffile_tmp, conffile);
}

template <typename T>
std::ostream& operator<<(std::ostream& out, const std::vector<T>& v) {
    size_t last = v.size() - 1;
    for (size_t i = 0; i < v.size(); ++i) {
        out << v[i];
        if (i != last) {
            out << ", ";
        }
    }
    return out;
}

#define SET_FIELD(FIELD, TYPE, FILL_CONF_MAP, SET_TO_DEFAULT)                                  \
    if (strcmp((FIELD).type, #TYPE) == 0) {                                                    \
        TYPE new_value = TYPE();                                                               \
        bool is_newval_set = false;                                                            \
        if (!props.get_or_default((FIELD).name, ((SET_TO_DEFAULT) ? (FIELD).defval : nullptr), \
                                  new_value, &is_newval_set)) {                                \
            std::cerr << "config field error: " << (FIELD).name << std::endl;                  \
            return false;                                                                      \
        }                                                                                      \
        if (!is_newval_set) {                                                                  \
            continue;                                                                          \
        }                                                                                      \
        TYPE& ref_conf_value = *reinterpret_cast<TYPE*>((FIELD).storage);                      \
        TYPE old_value = ref_conf_value;                                                       \
        ref_conf_value = new_value;                                                            \
        if (RegisterConfValidator::_s_field_validator != nullptr) {                            \
            auto validator = RegisterConfValidator::_s_field_validator->find((FIELD).name);    \
            if (validator != RegisterConfValidator::_s_field_validator->end() &&               \
                !(validator->second)()) {                                                      \
                ref_conf_value = old_value;                                                    \
                std::cerr << "validate " << (FIELD).name << "=" << new_value << " failed"      \
                          << std::endl;                                                        \
                return false;                                                                  \
            }                                                                                  \
        }                                                                                      \
        if (FILL_CONF_MAP) {                                                                   \
            std::ostringstream oss;                                                            \
            oss << ref_conf_value;                                                             \
            (*full_conf_map)[(FIELD).name] = oss.str();                                        \
        }                                                                                      \
        continue;                                                                              \
    }

// init conf fields
bool init(const char* conf_file, bool fill_conf_map, bool must_exist, bool set_to_default) {
    Properties props;
    // load properties file
    if (!props.load(conf_file, must_exist)) {
        return false;
    }
    // fill full_conf_map ?
    if (fill_conf_map && full_conf_map == nullptr) {
        full_conf_map = new std::map<std::string, std::string>();
    }

    // set conf fields
    for (const auto& it : *Register::_s_field_map) {
        SET_FIELD(it.second, bool, fill_conf_map, set_to_default);
        SET_FIELD(it.second, int16_t, fill_conf_map, set_to_default);
        SET_FIELD(it.second, int32_t, fill_conf_map, set_to_default);
        SET_FIELD(it.second, int64_t, fill_conf_map, set_to_default);
        SET_FIELD(it.second, double, fill_conf_map, set_to_default);
        SET_FIELD(it.second, std::string, fill_conf_map, set_to_default);
        SET_FIELD(it.second, std::vector<bool>, fill_conf_map, set_to_default);
        SET_FIELD(it.second, std::vector<int16_t>, fill_conf_map, set_to_default);
        SET_FIELD(it.second, std::vector<int32_t>, fill_conf_map, set_to_default);
        SET_FIELD(it.second, std::vector<int64_t>, fill_conf_map, set_to_default);
        SET_FIELD(it.second, std::vector<double>, fill_conf_map, set_to_default);
        SET_FIELD(it.second, std::vector<std::string>, fill_conf_map, set_to_default);
    }

    return true;
}

#define UPDATE_FIELD(FIELD, VALUE, TYPE, PERSIST)                                                  \
    if (strcmp((FIELD).type, #TYPE) == 0) {                                                        \
        TYPE new_value;                                                                            \
        if (!convert((VALUE), new_value)) {                                                        \
            return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("convert '{}' as {} failed",  \
                                                                     VALUE, #TYPE);                \
        }                                                                                          \
        TYPE& ref_conf_value = *reinterpret_cast<TYPE*>((FIELD).storage);                          \
        TYPE old_value = ref_conf_value;                                                           \
        if (RegisterConfValidator::_s_field_validator != nullptr) {                                \
            auto validator = RegisterConfValidator::_s_field_validator->find((FIELD).name);        \
            if (validator != RegisterConfValidator::_s_field_validator->end() &&                   \
                !(validator->second)()) {                                                          \
                ref_conf_value = old_value;                                                        \
                return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("validate {}={} failed",  \
                                                                         (FIELD).name, new_value); \
            }                                                                                      \
        }                                                                                          \
        ref_conf_value = new_value;                                                                \
        if (full_conf_map != nullptr) {                                                            \
            std::ostringstream oss;                                                                \
            oss << new_value;                                                                      \
            (*full_conf_map)[(FIELD).name] = oss.str();                                            \
        }                                                                                          \
        if (PERSIST) {                                                                             \
            RETURN_IF_ERROR(persist_config(std::string((FIELD).name), VALUE));                     \
        }                                                                                          \
        return Status::OK();                                                                       \
    }

// write config to be_custom.conf
// the caller need to make sure that the given config is valid
Status persist_config(const std::string& field, const std::string& value) {
    // lock to make sure only one thread can modify the be_custom.conf
    std::lock_guard<std::mutex> l(custom_conf_lock);

    static const std::string conffile = config::custom_config_dir + "/be_custom.conf";

    Properties tmp_props;
    if (!tmp_props.load(conffile.c_str(), false)) {
        LOG(WARNING) << "failed to load " << conffile;
        return Status::InternalError("failed to load conf file: {}", conffile);
    }

    tmp_props.set_force(field, value);
    return tmp_props.dump(conffile);
}

Status set_config(const std::string& field, const std::string& value, bool need_persist,
                  bool force) {
    auto it = Register::_s_field_map->find(field);
    if (it == Register::_s_field_map->end()) {
        return Status::Error<ErrorCode::NOT_FOUND, false>("'{}' is not found", field);
    }

    if (!force && !it->second.valmutable) {
        return Status::Error<ErrorCode::NOT_IMPLEMENTED_ERROR, false>(
                "'{}' is not support to modify", field);
    }

    UPDATE_FIELD(it->second, value, bool, need_persist);
    UPDATE_FIELD(it->second, value, int16_t, need_persist);
    UPDATE_FIELD(it->second, value, int32_t, need_persist);
    UPDATE_FIELD(it->second, value, int64_t, need_persist);
    UPDATE_FIELD(it->second, value, double, need_persist);
    {
        // add lock to ensure thread safe
        std::lock_guard<std::mutex> lock(mutable_string_config_lock);
        UPDATE_FIELD(it->second, value, std::string, need_persist);
    }

    // The other types are not thread safe to change dynamically.
    return Status::Error<ErrorCode::NOT_IMPLEMENTED_ERROR, false>(
            "'{}' is type of '{}' which is not support to modify", field, it->second.type);
}

Status set_fuzzy_config(const std::string& field, const std::string& value) {
    LOG(INFO) << fmt::format("FUZZY MODE: {} has been set to {}", field, value);
    return set_config(field, value, false, true);
}

void set_fuzzy_configs() {
    // random value true or false
    static_cast<void>(
            set_fuzzy_config("disable_storage_page_cache", ((rand() % 2) == 0) ? "true" : "false"));
    static_cast<void>(
            set_fuzzy_config("enable_system_metrics", ((rand() % 2) == 0) ? "true" : "false"));
    // random value from 8 to 48
    // s = set_fuzzy_config("doris_scanner_thread_pool_thread_num", std::to_string((rand() % 41) + 8));
    // LOG(INFO) << s.to_string();
}

std::mutex* get_mutable_string_config_lock() {
    return &mutable_string_config_lock;
}

std::vector<std::vector<std::string>> get_config_info() {
    std::vector<std::vector<std::string>> configs;
    std::lock_guard<std::mutex> lock(mutable_string_config_lock);
    for (const auto& it : *full_conf_map) {
        auto field_it = Register::_s_field_map->find(it.first);
        if (field_it == Register::_s_field_map->end()) {
            continue;
        }

        std::vector<std::string> _config;
        _config.push_back(it.first);

        _config.push_back(field_it->second.type);
        if (0 == strcmp(field_it->second.type, "bool")) {
            _config.push_back(it.second == "1" ? "true" : "false");
        } else {
            _config.push_back(it.second);
        }
        _config.push_back(field_it->second.valmutable ? "true" : "false");

        configs.push_back(_config);
    }
    return configs;
}

} // namespace config
} // namespace doris
