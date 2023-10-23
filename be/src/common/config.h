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

#include <cstdint>
#include <functional>
#include <map>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#define DECLARE_FIELD(FIELD_TYPE, FIELD_NAME) extern FIELD_TYPE FIELD_NAME

#define DECLARE_Bool(name) DECLARE_FIELD(bool, name)
#define DECLARE_Int16(name) DECLARE_FIELD(int16_t, name)
#define DECLARE_Int32(name) DECLARE_FIELD(int32_t, name)
#define DECLARE_Int64(name) DECLARE_FIELD(int64_t, name)
#define DECLARE_Double(name) DECLARE_FIELD(double, name)
#define DECLARE_String(name) DECLARE_FIELD(std::string, name)
#define DECLARE_Bools(name) DECLARE_FIELD(std::vector<bool>, name)
#define DECLARE_Int16s(name) DECLARE_FIELD(std::vector<int16_t>, name)
#define DECLARE_Int32s(name) DECLARE_FIELD(std::vector<int32_t>, name)
#define DECLARE_Int64s(name) DECLARE_FIELD(std::vector<int64_t>, name)
#define DECLARE_Doubles(name) DECLARE_FIELD(std::vector<double>, name)
#define DECLARE_Strings(name) DECLARE_FIELD(std::vector<std::string>, name)
#define DECLARE_mBool(name) DECLARE_FIELD(bool, name)
#define DECLARE_mInt16(name) DECLARE_FIELD(int16_t, name)
#define DECLARE_mInt32(name) DECLARE_FIELD(int32_t, name)
#define DECLARE_mInt64(name) DECLARE_FIELD(int64_t, name)
#define DECLARE_mDouble(name) DECLARE_FIELD(double, name)
#define DECLARE_mString(name) DECLARE_FIELD(std::string, name)

#define DEFINE_FIELD(FIELD_TYPE, FIELD_NAME, FIELD_DEFAULT, VALMUTABLE)                      \
    FIELD_TYPE FIELD_NAME;                                                                   \
    static Register reg_##FIELD_NAME(#FIELD_TYPE, #FIELD_NAME, &(FIELD_NAME), FIELD_DEFAULT, \
                                     VALMUTABLE);

#define DEFINE_VALIDATOR(FIELD_NAME, VALIDATOR)              \
    static auto validator_##FIELD_NAME = VALIDATOR;          \
    static RegisterConfValidator reg_validator_##FIELD_NAME( \
            #FIELD_NAME, []() -> bool { return validator_##FIELD_NAME(FIELD_NAME); });

#define DEFINE_Int16(name, defaultstr) DEFINE_FIELD(int16_t, name, defaultstr, false)
#define DEFINE_Bools(name, defaultstr) DEFINE_FIELD(std::vector<bool>, name, defaultstr, false)
#define DEFINE_Doubles(name, defaultstr) DEFINE_FIELD(std::vector<double>, name, defaultstr, false)
#define DEFINE_Int16s(name, defaultstr) DEFINE_FIELD(std::vector<int16_t>, name, defaultstr, false)
#define DEFINE_Int32s(name, defaultstr) DEFINE_FIELD(std::vector<int32_t>, name, defaultstr, false)
#define DEFINE_Int64s(name, defaultstr) DEFINE_FIELD(std::vector<int64_t>, name, defaultstr, false)
#define DEFINE_Bool(name, defaultstr) DEFINE_FIELD(bool, name, defaultstr, false)
#define DEFINE_Double(name, defaultstr) DEFINE_FIELD(double, name, defaultstr, false)
#define DEFINE_Int32(name, defaultstr) DEFINE_FIELD(int32_t, name, defaultstr, false)
#define DEFINE_Int64(name, defaultstr) DEFINE_FIELD(int64_t, name, defaultstr, false)
#define DEFINE_String(name, defaultstr) DEFINE_FIELD(std::string, name, defaultstr, false)
#define DEFINE_Strings(name, defaultstr) \
    DEFINE_FIELD(std::vector<std::string>, name, defaultstr, false)
#define DEFINE_mBool(name, defaultstr) DEFINE_FIELD(bool, name, defaultstr, true)
#define DEFINE_mInt16(name, defaultstr) DEFINE_FIELD(int16_t, name, defaultstr, true)
#define DEFINE_mInt32(name, defaultstr) DEFINE_FIELD(int32_t, name, defaultstr, true)
#define DEFINE_mInt64(name, defaultstr) DEFINE_FIELD(int64_t, name, defaultstr, true)
#define DEFINE_mDouble(name, defaultstr) DEFINE_FIELD(double, name, defaultstr, true)
#define DEFINE_mString(name, defaultstr) DEFINE_FIELD(std::string, name, defaultstr, true)
#define DEFINE_Validator(name, validator) DEFINE_VALIDATOR(name, validator)

namespace doris {
class Status;

// If you want to modify the value of config, please go to common/config.cpp to modify.
namespace config {
// Dir of custom config file
DECLARE_String(custom_config_dir);

// Dir of jdbc drivers
DECLARE_String(jdbc_drivers_dir);

// cluster id
DECLARE_Int32(cluster_id);
// port on which BackendService is exported
DECLARE_Int32(be_port);

// port for brpc
DECLARE_Int32(brpc_port);

// port for arrow flight sql
// Default -1, do not start arrow flight sql server.
DECLARE_Int32(arrow_flight_sql_port);

// the number of bthreads for brpc, the default value is set to -1,
// which means the number of bthreads is #cpu-cores
DECLARE_Int32(brpc_num_threads);

// Declare a selection strategy for those servers have many ips.
// Note that there should at most one ip match this list.
// this is a list in semicolon-delimited format, in CIDR notation, e.g. 10.10.10.0/24
// If no ip match this rule, will choose one randomly.
DECLARE_String(priority_networks);

// performance moderate or or compact, only tcmalloc compile
DECLARE_String(memory_mode);

// process memory limit specified as number of bytes
// ('<int>[bB]?'), megabytes ('<float>[mM]'), gigabytes ('<float>[gG]'),
// or percentage of the physical memory ('<int>%').
// defaults to bytes if no unit is given"
// must larger than 0. and if larger than physical memory size,
// it will be set to physical memory size.
DECLARE_String(mem_limit);

// Soft memory limit as a fraction of hard memory limit.
DECLARE_Double(soft_mem_limit_frac);

// Many modern allocators (for example) do not do a mremap for
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
DECLARE_mInt64(mmap_threshold); // bytes

// When hash table capacity is greater than 2^double_grow_degree(default 2G), grow when 75% of the capacity is satisfied.
// Increase can reduce the number of hash table resize, but may waste more memory.
DECLARE_mInt32(hash_table_double_grow_degree);

// The max fill rate for hash table
DECLARE_mInt32(max_fill_rate);

DECLARE_mInt32(double_resize_threshold);

// Expand the hash table before inserting data, the maximum expansion size.
// There are fewer duplicate keys, reducing the number of resize hash tables
// There are many duplicate keys, and the hash table filled bucket is far less than the hash table build bucket.
DECLARE_mInt64(hash_table_pre_expanse_max_rows);

// The maximum low water mark of the system `/proc/meminfo/MemAvailable`, Unit byte, default 1.6G,
// actual low water mark=min(1.6G, MemTotal * 10%), avoid wasting too much memory on machines
// with large memory larger than 16G.
// Turn up max. On machines with more than 16G memory, more memory buffers will be reserved for Full GC.
// Turn down max. will use as much memory as possible.
DECLARE_Int64(max_sys_mem_available_low_water_mark_bytes);

// The size of the memory that gc wants to release each time, as a percentage of the mem limit.
DECLARE_mString(process_minor_gc_size);
DECLARE_mString(process_full_gc_size);

// If true, when the process does not exceed the soft mem limit, the query memory will not be limited;
// when the process memory exceeds the soft mem limit, the query with the largest ratio between the currently
// used memory and the exec_mem_limit will be canceled.
// If false, cancel query when the memory used exceeds exec_mem_limit, same as before.
DECLARE_mBool(enable_query_memory_overcommit);
//waibibabu
// gc will release cache, cancel task, and task will wait for gc to release memory,
// default gc strategy is conservative, if you want to exclude the interference of gc, let it be true
DECLARE_mBool(disable_memory_gc);

// malloc or new large memory larger than large_memory_check_bytes, default 2G,
// will print a warning containing the stacktrace, but not prevent memory alloc.
// If is -1, disable large memory check.
DECLARE_mInt64(large_memory_check_bytes);

// The maximum time a thread waits for a full GC. Currently only query will wait for full gc.
DECLARE_mInt32(thread_wait_gc_max_milliseconds);

// reach mem limit, don't serialize in batch
DECLARE_mInt64(pre_serialize_keys_limit_bytes);

// the port heartbeat service used
DECLARE_Int32(heartbeat_service_port);
// the count of heart beat service
DECLARE_Int32(heartbeat_service_thread_count);
// the count of thread to create table
DECLARE_Int32(create_tablet_worker_count);
// the count of thread to drop table
DECLARE_Int32(drop_tablet_worker_count);
// the count of thread to batch load
DECLARE_Int32(push_worker_count_normal_priority);
// the count of thread to high priority batch load
DECLARE_Int32(push_worker_count_high_priority);
// the count of thread to publish version
DECLARE_Int32(publish_version_worker_count);
// the count of tablet thread to publish version
DECLARE_Int32(tablet_publish_txn_max_thread);
// the timeout of EnginPublishVersionTask
DECLARE_Int32(publish_version_task_timeout_s);
// the count of thread to calc delete bitmap
DECLARE_Int32(calc_delete_bitmap_max_thread);
// the count of thread to clear transaction task
DECLARE_Int32(clear_transaction_task_worker_count);
// the count of thread to delete
DECLARE_Int32(delete_worker_count);
// the count of thread to alter table
DECLARE_Int32(alter_tablet_worker_count);
// the count of thread to alter index
DECLARE_Int32(alter_index_worker_count);
// the count of thread to clone
DECLARE_Int32(clone_worker_count);
// the count of thread to clone
DECLARE_Int32(storage_medium_migrate_count);
// the count of thread to check consistency
DECLARE_Int32(check_consistency_worker_count);
// the count of thread to upload
DECLARE_Int32(upload_worker_count);
// the count of thread to download
DECLARE_Int32(download_worker_count);
// the count of thread to make snapshot
DECLARE_Int32(make_snapshot_worker_count);
// the count of thread to release snapshot
DECLARE_Int32(release_snapshot_worker_count);
// the interval time(seconds) for agent report tasks signature to FE
DECLARE_mInt32(report_task_interval_seconds);
// the interval time(seconds) for refresh storage policy from FE
DECLARE_mInt32(storage_refresh_storage_policy_task_interval_seconds);
// the interval time(seconds) for agent report disk state to FE
DECLARE_mInt32(report_disk_state_interval_seconds);
// the interval time(seconds) for agent report olap table to FE
DECLARE_mInt32(report_tablet_interval_seconds);
// the max download speed(KB/s)
DECLARE_mInt32(max_download_speed_kbps);
// download low speed limit(KB/s)
DECLARE_mInt32(download_low_speed_limit_kbps);
// download low speed time(seconds)
DECLARE_mInt32(download_low_speed_time);
// sleep time for one second
DECLARE_Int32(sleep_one_second);

// log dir
DECLARE_String(sys_log_dir);
DECLARE_String(user_function_dir);
// INFO, WARNING, ERROR, FATAL
DECLARE_String(sys_log_level);
// TIME-DAY, TIME-HOUR, SIZE-MB-nnn
DECLARE_String(sys_log_roll_mode);
// log roll num
DECLARE_Int32(sys_log_roll_num);
// verbose log
DECLARE_Strings(sys_log_verbose_modules);
// verbose log level
DECLARE_Int32(sys_log_verbose_level);
// log buffer level
DECLARE_String(log_buffer_level);

// number of threads available to serve backend execution requests
DECLARE_Int32(be_service_threads);

// Controls the number of threads to run work per core.  It's common to pick 2x
// or 3x the number of cores.  This keeps the cores busy without causing excessive
// thrashing.
DECLARE_Int32(num_threads_per_core);
// if true, compresses tuple data in Serialize
DECLARE_mBool(compress_rowbatches);
DECLARE_mBool(rowbatch_align_tuple_offset);
// interval between profile reports; in seconds
DECLARE_mInt32(status_report_interval);
DECLARE_mInt32(pipeline_status_report_interval);
// if true, each disk will have a separate thread pool for scanner
DECLARE_Bool(doris_enable_scanner_thread_pool_per_disk);
// the timeout of a work thread to wait the blocking priority queue to get a task
DECLARE_mInt64(doris_blocking_priority_queue_wait_timeout_ms);
// number of scanner thread pool size for olap table
// and the min thread num of remote scanner thread pool
DECLARE_Int32(doris_scanner_thread_pool_thread_num);
// max number of remote scanner thread pool size
// if equal to -1, value is std::max(512, CpuInfo::num_cores() * 10)
DECLARE_Int32(doris_max_remote_scanner_thread_pool_thread_num);
// number of olap scanner thread pool queue size
DECLARE_Int32(doris_scanner_thread_pool_queue_size);
// default thrift client connect timeout(in seconds)
DECLARE_mInt32(thrift_connect_timeout_seconds);
DECLARE_mInt32(fetch_rpc_timeout_seconds);
// default thrift client retry interval (in milliseconds)
DECLARE_mInt64(thrift_client_retry_interval_ms);
// max row count number for single scan range, used in segmentv1
DECLARE_mInt32(doris_scan_range_row_count);
// max bytes number for single scan range, used in segmentv2
DECLARE_mInt32(doris_scan_range_max_mb);
// max bytes number for single scan block, used in segmentv2
DECLARE_mInt32(doris_scan_block_max_mb);
// size of scanner queue between scanner thread and compute thread
DECLARE_mInt32(doris_scanner_queue_size);
// single read execute fragment row number
DECLARE_mInt32(doris_scanner_row_num);
// single read execute fragment row bytes
DECLARE_mInt32(doris_scanner_row_bytes);
// number of max scan keys
DECLARE_mInt32(doris_max_scan_key_num);
// the max number of push down values of a single column.
// if exceed, no conditions will be pushed down for that column.
DECLARE_mInt32(max_pushdown_conditions_per_column);
// return_row / total_row
DECLARE_mInt32(doris_max_pushdown_conjuncts_return_rate);
// (Advanced) Maximum size of per-query receive-side buffer
DECLARE_mInt32(exchg_node_buffer_size_bytes);

DECLARE_mInt64(column_dictionary_key_ratio_threshold);
DECLARE_mInt64(column_dictionary_key_size_threshold);
// memory_limitation_per_thread_for_schema_change_bytes unit bytes
DECLARE_mInt64(memory_limitation_per_thread_for_schema_change_bytes);
DECLARE_mInt64(memory_limitation_per_thread_for_storage_migration_bytes);

// the prune stale interval of all cache
DECLARE_mInt32(cache_prune_stale_interval);
// the clean interval of tablet lookup cache
DECLARE_mInt32(tablet_lookup_cache_clean_interval);
DECLARE_mInt32(disk_stat_monitor_interval);
DECLARE_mInt32(unused_rowset_monitor_interval);
DECLARE_String(storage_root_path);
DECLARE_mString(broken_storage_path);

// Config is used to check incompatible old format hdr_ format
// whether doris uses strict way. When config is true, process will log fatal
// and exit. When config is false, process will only log warning.
DECLARE_Bool(storage_strict_check_incompatible_old_format);

// BE process will exit if the percentage of error disk reach this value.
DECLARE_mInt32(max_percentage_of_error_disk);
DECLARE_mInt32(default_num_rows_per_column_file_block);
// pending data policy
DECLARE_mInt32(pending_data_expire_time_sec);
// inc_rowset snapshot rs sweep time interval
DECLARE_mInt32(tablet_rowset_stale_sweep_time_sec);
// tablet stale rowset sweep by threshold size
DECLARE_Bool(tablet_rowset_stale_sweep_by_size);
DECLARE_mInt32(tablet_rowset_stale_sweep_threshold_size);
// garbage sweep policy
DECLARE_Int32(max_garbage_sweep_interval);
DECLARE_Int32(min_garbage_sweep_interval);
// garbage sweep every batch will sleep 1ms
DECLARE_mInt32(garbage_sweep_batch_size);
DECLARE_mInt32(snapshot_expire_time_sec);
// It is only a recommended value. When the disk space is insufficient,
// the file storage period under trash dose not have to comply with this parameter.
DECLARE_mInt32(trash_file_expire_time_sec);
// minimum file descriptor number
// modify them upon necessity
DECLARE_Int32(min_file_descriptor_number);
DECLARE_mBool(disable_segment_cache);
DECLARE_Int64(index_stream_cache_capacity);
DECLARE_String(row_cache_mem_limit);

// Cache for storage page size
DECLARE_String(storage_page_cache_limit);
// Shard size for page cache, the value must be power of two.
// It's recommended to set it to a value close to the number of BE cores in order to reduce lock contentions.
DECLARE_Int32(storage_page_cache_shard_size);
// Percentage for index page cache
// all storage page cache will be divided into data_page_cache and index_page_cache
DECLARE_Int32(index_page_cache_percentage);
// whether to disable page cache feature in storage
// TODO delete it. Divided into Data page, Index page, pk index page
DECLARE_Bool(disable_storage_page_cache);
// whether to disable row cache feature in storage
DECLARE_Bool(disable_storage_row_cache);
// whether to disable pk page cache feature in storage
DECLARE_Bool(disable_pk_storage_page_cache);

// Cache for mow primary key storage page size, it's seperated from
// storage_page_cache_limit
DECLARE_String(pk_storage_page_cache_limit);
// data page size for primary key index
DECLARE_Int32(primary_key_data_page_size);

// inc_rowset snapshot rs sweep time interval
DECLARE_mInt32(data_page_cache_stale_sweep_time_sec);
DECLARE_mInt32(index_page_cache_stale_sweep_time_sec);
// great impact on the performance of MOW, so it can be longer.
DECLARE_mInt32(pk_index_page_cache_stale_sweep_time_sec);

DECLARE_Bool(enable_low_cardinality_optimize);
DECLARE_Bool(enable_low_cardinality_cache_code);

// be policy
// whether check compaction checksum
DECLARE_mBool(enable_compaction_checksum);
// whether disable automatic compaction task
DECLARE_mBool(disable_auto_compaction);
// whether enable vertical compaction
DECLARE_mBool(enable_vertical_compaction);
// whether enable ordered data compaction
DECLARE_mBool(enable_ordered_data_compaction);
// In vertical compaction, column number for every group
DECLARE_mInt32(vertical_compaction_num_columns_per_group);
// In vertical compaction, max memory usage for row_source_buffer
DECLARE_Int32(vertical_compaction_max_row_source_memory_mb);
// In vertical compaction, max dest segment file size
DECLARE_mInt64(vertical_compaction_max_segment_size);

// In ordered data compaction, min segment size for input rowset
DECLARE_mInt32(ordered_data_compaction_min_segment_size);

// This config can be set to limit thread number in compaction thread pool.
DECLARE_mInt32(max_base_compaction_threads);
DECLARE_mInt32(max_cumu_compaction_threads);
DECLARE_mInt32(max_single_replica_compaction_threads);

DECLARE_Bool(enable_base_compaction_idle_sched);
DECLARE_mInt64(base_compaction_min_rowset_num);
DECLARE_mDouble(base_compaction_min_data_ratio);
DECLARE_mInt64(base_compaction_dup_key_max_file_size_mbytes);

DECLARE_Bool(enable_skip_tablet_compaction);
// output rowset of cumulative compaction total disk size exceed this config size,
// this rowset will be given to base compaction, unit is m byte.
DECLARE_mInt64(compaction_promotion_size_mbytes);

// output rowset of cumulative compaction total disk size exceed this config ratio of
// base rowset's total disk size, this rowset will be given to base compaction. The value must be between
// 0 and 1.
DECLARE_mDouble(compaction_promotion_ratio);

// the smallest size of rowset promotion. When the rowset is less than this config, this
// rowset will be not given to base compaction. The unit is m byte.
DECLARE_mInt64(compaction_promotion_min_size_mbytes);

// When output rowset of cumulative compaction total version count (end_version - start_version)
// exceed this config count, the rowset will be moved to base compaction
// NOTE: this config will work for unique key merge-on-write table only, to reduce version count
// related cost on delete bitmap more effectively.
DECLARE_mInt64(compaction_promotion_version_count);

// The lower bound size to do cumulative compaction. When total disk size of candidate rowsets is less than
// this size, size_based policy may not do to cumulative compaction. The unit is m byte.
DECLARE_mInt64(compaction_min_size_mbytes);

// cumulative compaction policy: min and max delta file's number
DECLARE_mInt64(cumulative_compaction_min_deltas);
DECLARE_mInt64(cumulative_compaction_max_deltas);

// This config can be set to limit thread number in  multiget thread pool.
DECLARE_mInt32(multi_get_max_threads);

// The upper limit of "permits" held by all compaction tasks. This config can be set to limit memory consumption for compaction.
DECLARE_mInt64(total_permits_for_compaction_score);

// sleep interval in ms after generated compaction tasks
DECLARE_mInt32(generate_compaction_tasks_interval_ms);
// sleep interval in second after update replica infos
DECLARE_mInt32(update_replica_infos_interval_seconds);

// Compaction task number per disk.
// Must be greater than 2, because Base compaction and Cumulative compaction have at least one thread each.
DECLARE_mInt32(compaction_task_num_per_disk);
// compaction thread num for fast disk(typically .SSD), must be greater than 2.
DECLARE_mInt32(compaction_task_num_per_fast_disk);

// How many rounds of cumulative compaction for each round of base compaction when compaction tasks generation.
DECLARE_mInt32(cumulative_compaction_rounds_for_each_base_compaction_round);

// Threshold to logging compaction trace, in seconds.
DECLARE_mInt32(base_compaction_trace_threshold);
DECLARE_mInt32(cumulative_compaction_trace_threshold);
DECLARE_mBool(disable_compaction_trace_log);

// Interval to picking rowset to compact, in seconds
DECLARE_mInt64(pick_rowset_to_compact_interval_sec);

// Thread count to do tablet meta checkpoint, -1 means use the data directories count.
DECLARE_Int32(max_meta_checkpoint_threads);

// Threshold to logging agent task trace, in seconds.
DECLARE_mInt32(agent_task_trace_threshold_sec);

// This config can be set to limit thread number in tablet migration thread pool.
DECLARE_Int32(min_tablet_migration_threads);
DECLARE_Int32(max_tablet_migration_threads);

DECLARE_mInt32(finished_migration_tasks_size);
// If size less than this, the remaining rowsets will be force to complete
DECLARE_mInt32(migration_remaining_size_threshold_mb);
// If the task runs longer than this time, the task will be terminated, in seconds.
// tablet max size / migration min speed * factor = 10GB / 1MBps * 2 = 20480 seconds
DECLARE_mInt32(migration_task_timeout_secs);

// Port to start debug webserver on
DECLARE_Int32(webserver_port);
// Https enable flag
DECLARE_Bool(enable_https);
// Path of certificate
DECLARE_String(ssl_certificate_path);
// Path of private key
DECLARE_String(ssl_private_key_path);
// Whether to check authorization
DECLARE_Bool(enable_all_http_auth);
// Number of webserver workers
DECLARE_Int32(webserver_num_workers);
// Period to update rate counters and sampling counters in ms.
DECLARE_mInt32(periodic_counter_update_period_ms);

DECLARE_Bool(enable_single_replica_load);
// Number of download workers for single replica load
DECLARE_Int32(single_replica_load_download_num_workers);

// Used for mini Load. mini load data file will be removed after this time.
DECLARE_Int64(load_data_reserve_hours);
// log error log will be removed after this time
DECLARE_mInt64(load_error_log_reserve_hours);

// be brpc interface is classified into two categories: light and heavy
// each category has diffrent thread number
// threads to handle heavy api interface, such as transmit_data/transmit_block etc
// Default, if less than or equal 32 core, the following are 128, 128, 10240, 10240 in turn.
//          if greater than 32 core, the following are core num * 4, core num * 4, core num * 320, core num * 320 in turn
DECLARE_Int32(brpc_heavy_work_pool_threads);
// threads to handle light api interface, such as exec_plan_fragment_prepare/exec_plan_fragment_start
DECLARE_Int32(brpc_light_work_pool_threads);
DECLARE_Int32(brpc_heavy_work_pool_max_queue_size);
DECLARE_Int32(brpc_light_work_pool_max_queue_size);

// The maximum amount of data that can be processed by a stream load
DECLARE_mInt64(streaming_load_max_mb);
// Some data formats, such as JSON, cannot be streamed.
// Therefore, it is necessary to limit the maximum number of
// such data when using stream load to prevent excessive memory consumption.
DECLARE_mInt64(streaming_load_json_max_mb);
// the alive time of a TabletsChannel.
// If the channel does not receive any data till this time,
// the channel will be removed.
DECLARE_mInt32(streaming_load_rpc_max_alive_time_sec);
// the timeout of a rpc to open the tablet writer in remote BE.
// short operation time, can set a short timeout
DECLARE_Int32(tablet_writer_open_rpc_timeout_sec);
// You can ignore brpc error '[E1011]The server is overcrowded' when writing data.
DECLARE_mBool(tablet_writer_ignore_eovercrowded);
DECLARE_mBool(exchange_sink_ignore_eovercrowded);
DECLARE_mInt32(slave_replica_writer_rpc_timeout_sec);
// Whether to enable stream load record function, the default is false.
// False: disable stream load record
DECLARE_mBool(enable_stream_load_record);
// batch size of stream load record reported to FE
DECLARE_mInt32(stream_load_record_batch_size);
// expire time of stream load record in rocksdb.
DECLARE_Int32(stream_load_record_expire_time_secs);
// time interval to clean expired stream load records
DECLARE_mInt64(clean_stream_load_record_interval_secs);
// The buffer size to store stream table function schema info
DECLARE_Int64(stream_tvf_buffer_size);

// OlapTableSink sender's send interval, should be less than the real response time of a tablet writer rpc.
// You may need to lower the speed when the sink receiver bes are too busy.
DECLARE_mInt32(olap_table_sink_send_interval_ms);

// Fragment thread pool
DECLARE_Int32(fragment_pool_thread_num_min);
DECLARE_Int32(fragment_pool_thread_num_max);
DECLARE_Int32(fragment_pool_queue_size);

// Control the number of disks on the machine.  If 0, this comes from the system settings.
DECLARE_Int32(num_disks);
// The maximum number of the threads per disk is also the max queue depth per disk.
DECLARE_Int32(num_threads_per_disk);
// The read size is the size of the reads sent to os.
// There is a trade off of latency and throughout, trying to keep disks busy but
// not introduce seeks.  The literature seems to agree that with 8 MB reads, random
// io and sequential io perform similarly.
DECLARE_Int32(read_size);       // 8 * 1024 * 1024, Read Size (in bytes)
DECLARE_Int32(min_buffer_size); // 1024, The minimum read buffer size (in bytes)

// For each io buffer size, the maximum number of buffers the IoMgr will hold onto
// With 1024B through 8MB buffers, this is up to ~2GB of buffers.
DECLARE_Int32(max_free_io_buffers);

// The probing algorithm of partitioned hash table.
// Enable quadratic probing hash table
DECLARE_Bool(enable_quadratic_probing);

// for pprof
DECLARE_String(pprof_profile_dir);
// for jeprofile in jemalloc
DECLARE_mString(jeprofile_dir);
// Purge all unused dirty pages for all arenas.
DECLARE_mBool(enable_je_purge_dirty_pages);

// to forward compatibility, will be removed later
DECLARE_mBool(enable_token_check);

// to open/close system metrics
DECLARE_Bool(enable_system_metrics);

DECLARE_mBool(enable_prefetch);

// Number of cores Doris will used, this will effect only when it's greater than 0.
// Otherwise, Doris will use all cores returned from "/proc/cpuinfo".
DECLARE_Int32(num_cores);

// When BE start, If there is a broken disk, BE process will exit by default.
// Otherwise, we will ignore the broken disk,
DECLARE_Bool(ignore_broken_disk);

// linux transparent huge page
DECLARE_Bool(madvise_huge_pages);

// whether use mmap to allocate memory
DECLARE_Bool(mmap_buffers);

// Sleep time in milliseconds between memory maintenance iterations
DECLARE_mInt32(memory_maintenance_sleep_time_ms);

// After full gc, no longer full gc and minor gc during sleep.
// After minor gc, no minor gc during sleep, but full gc is possible.
DECLARE_mInt32(memory_gc_sleep_time_ms);

// Sleep time in milliseconds between memtbale flush mgr memory refresh iterations
DECLARE_mInt64(memtable_mem_tracker_refresh_interval_ms);

// Alignment
DECLARE_Int32(memory_max_alignment);

// max write buffer size before flush, default 200MB
DECLARE_mInt64(write_buffer_size);
// max buffer size used in memtable for the aggregated table, default 400MB
DECLARE_mInt64(write_buffer_size_for_agg);

DECLARE_Int32(load_process_max_memory_limit_percent); // 50%

// If the memory consumption of load jobs exceed load_process_max_memory_limit,
// all load jobs will hang there to wait for memtable flush. We should have a
// soft limit which can trigger the memtable flush for the load channel who
// consumes lagest memory size before we reach the hard limit. The soft limit
// might avoid all load jobs hang at the same time.
DECLARE_Int32(load_process_soft_mem_limit_percent);

// result buffer cancelled time (unit: second)
DECLARE_mInt32(result_buffer_cancelled_interval_time);

// the increased frequency of priority for remaining tasks in BlockingPriorityQueue
DECLARE_mInt32(priority_queue_remaining_tasks_increased_frequency);

// sync tablet_meta when modifying meta
DECLARE_mBool(sync_tablet_meta);

// default thrift rpc timeout ms
DECLARE_mInt32(thrift_rpc_timeout_ms);

// txn commit rpc timeout
DECLARE_mInt32(txn_commit_rpc_timeout_ms);

// If set to true, metric calculator will run
DECLARE_Bool(enable_metric_calculator);

// max consumer num in one data consumer group, for routine load
DECLARE_mInt32(max_consumer_num_per_group);

// the size of thread pool for routine load task.
// this should be larger than FE config 'max_routine_load_task_num_per_be' (default 5)
DECLARE_Int32(routine_load_thread_pool_size);

// max external scan cache batch count, means cache max_memory_cache_batch_count * batch_size row
// default is 20, batch_size's default value is 1024 means 20 * 1024 rows will be cached
DECLARE_mInt32(max_memory_sink_batch_count);

// This configuration is used for the context gc thread schedule period
// note: unit is minute, default is 5min
DECLARE_mInt32(scan_context_gc_interval_min);

// es scroll keep-alive
DECLARE_String(es_scroll_keepalive);

// HTTP connection timeout for es
DECLARE_mInt32(es_http_timeout_ms);

// the max client cache number per each host
// There are variety of client cache in BE, but currently we use the
// same cache size configuration.
// TODO(cmy): use different config to set different client cache if necessary.
DECLARE_Int32(max_client_cache_size_per_host);

// Dir to save files downloaded by SmallFileMgr
DECLARE_String(small_file_dir);
// path gc
DECLARE_Bool(path_gc_check);
DECLARE_mInt32(path_gc_check_interval_second);
DECLARE_mInt32(path_gc_check_step);
DECLARE_mInt32(path_gc_check_step_interval_ms);
DECLARE_mInt32(path_scan_interval_second);
DECLARE_mInt32(path_scan_step_interval_ms);

// The following 2 configs limit the max usage of disk capacity of a data dir.
// If both of these 2 threshold reached, no more data can be writen into that data dir.
// The percent of max used capacity of a data dir
DECLARE_mInt32(storage_flood_stage_usage_percent); // 90%
// The min bytes that should be left of a data dir
DECLARE_mInt64(storage_flood_stage_left_capacity_bytes); // 1GB
// number of thread for flushing memtable per store
DECLARE_Int32(flush_thread_num_per_store);
// number of thread for flushing memtable per store, for high priority load task
DECLARE_Int32(high_priority_flush_thread_num_per_store);

// config for tablet meta checkpoint
DECLARE_mInt32(tablet_meta_checkpoint_min_new_rowsets_num);
DECLARE_mInt32(tablet_meta_checkpoint_min_interval_secs);
DECLARE_Int32(generate_tablet_meta_checkpoint_tasks_interval_secs);

// config for default rowset type
// Valid configs: ALPHA, BETA
DECLARE_String(default_rowset_type);

// Maximum size of a single message body in all protocols
DECLARE_Int64(brpc_max_body_size);
// Max unwritten bytes in each socket, if the limit is reached, Socket.Write fails with EOVERCROWDED
// Default, if the physical memory is less than or equal to 64G, the value is 1G
//          if the physical memory is greater than 64G, the value is physical memory * mem_limit(0.8) / 1024 * 20
DECLARE_Int64(brpc_socket_max_unwritten_bytes);
// TODO(zxy): expect to be true in v1.3
// Whether to embed the ProtoBuf Request serialized string together with Tuple/Block data into
// Controller Attachment and send it through http brpc when the length of the Tuple/Block data
// is greater than 1.8G. This is to avoid the error of Request length overflow (2G).
DECLARE_mBool(transfer_large_data_by_brpc);

// max number of txns for every txn_partition_map in txn manager
// this is a self protection to avoid too many txns saving in manager
DECLARE_mInt64(max_runnings_transactions_per_txn_map);

// tablet_map_lock shard size, the value is 2^n, n=0,1,2,3,4
// this is a an enhancement for better performance to manage tablet
DECLARE_Int32(tablet_map_shard_size);

// txn_map_lock shard size, the value is 2^n, n=0,1,2,3,4
// this is a an enhancement for better performance to manage txn
DECLARE_Int32(txn_map_shard_size);

// txn_lock shard size, the value is 2^n, n=0,1,2,3,4
// this is a an enhancement for better performance to commit and publish txn
DECLARE_Int32(txn_shard_size);

// Whether to continue to start be when load tablet from header failed.
DECLARE_Bool(ignore_load_tablet_failure);

// Whether to continue to start be when load tablet from header failed.
DECLARE_mBool(ignore_rowset_stale_unconsistent_delete);

// Set max cache's size of query results, the unit is M byte
DECLARE_Int32(query_cache_max_size_mb);

// Cache memory is pruned when reach query_cache_max_size_mb + query_cache_elasticity_size_mb
DECLARE_Int32(query_cache_elasticity_size_mb);

// Maximum number of cache partitions corresponding to a SQL
DECLARE_Int32(query_cache_max_partition_count);

// Maximum number of version of a tablet. If the version num of a tablet exceed limit,
// the load process will reject new incoming load job of this tablet.
// This is to avoid too many version num.
DECLARE_mInt32(max_tablet_version_num);

// Frontend mainly use two thrift sever type: THREAD_POOL, THREADED_SELECTOR. if fe use THREADED_SELECTOR model for thrift server,
// the thrift_server_type_of_fe should be set THREADED_SELECTOR to make be thrift client to fe constructed with TFramedTransport
DECLARE_String(thrift_server_type_of_fe);

// disable zone map index when page row is too few
DECLARE_mInt32(zone_map_row_num_threshold);

// aws sdk log level
//    Off = 0,
//    Fatal = 1,
//    Error = 2,
//    Warn = 3,
//    Info = 4,
//    Debug = 5,
//    Trace = 6
DECLARE_Int32(aws_log_level);

// the buffer size when read data from remote storage like s3
DECLARE_mInt32(remote_storage_read_buffer_mb);

// The minimum length when TCMalloc Hook consumes/releases MemTracker, consume size
// smaller than this value will continue to accumulate. specified as number of bytes.
// Decreasing this value will increase the frequency of consume/release.
// Increasing this value will cause MemTracker statistics to be inaccurate.
DECLARE_mInt32(mem_tracker_consume_min_size_bytes);

// The version information of the tablet will be stored in the memory
// in an adjacency graph data structure.
// And as the new version is written and the old version is deleted,
// the data structure will begin to have empty vertex with no edge associations(orphan vertex).
// This config is used to control that when the proportion of orphan vertex is greater than the threshold,
// the adjacency graph will be rebuilt to ensure that the data structure will not expand indefinitely.
// This config usually only needs to be modified during testing.
// In most cases, it does not need to be modified.
DECLARE_mDouble(tablet_version_graph_orphan_vertex_ratio);

// share delta writers when memtable_on_sink_node = true
DECLARE_Bool(share_delta_writers);
// number of brpc stream per load
DECLARE_Int32(num_streams_per_load);
// timeout for open stream sink rpc in ms
DECLARE_Int64(open_stream_sink_timeout_ms);

// max send batch parallelism for OlapTableSink
// The value set by the user for send_batch_parallelism is not allowed to exceed max_send_batch_parallelism_per_job,
// if exceed, the value of send_batch_parallelism would be max_send_batch_parallelism_per_job
DECLARE_mInt32(max_send_batch_parallelism_per_job);

// number of send batch thread pool size
DECLARE_Int32(send_batch_thread_pool_thread_num);
// number of send batch thread pool queue size
DECLARE_Int32(send_batch_thread_pool_queue_size);
// number of download cache thread pool size
DECLARE_Int32(download_cache_thread_pool_thread_num);
// number of download cache thread pool queue size
DECLARE_Int32(download_cache_thread_pool_queue_size);
// download cache buffer size
DECLARE_Int64(download_cache_buffer_size);

// Limit the number of segment of a newly created rowset.
// The newly created rowset may to be compacted after loading,
// so if there are too many segment in a rowset, the compaction process
// will run out of memory.
// When doing compaction, each segment may take at least 1MB buffer.
DECLARE_mInt32(max_segment_num_per_rowset);

// Store segment without compression if a segment is smaller than
// segment_compression_threshold_kb.
DECLARE_mInt32(segment_compression_threshold_kb);

// The connection timeout when connecting to external table such as odbc table.
DECLARE_mInt32(external_table_connect_timeout_sec);

// Global bitmap cache capacity for aggregation cache, size in bytes
DECLARE_Int64(delete_bitmap_agg_cache_capacity);

// s3 config
DECLARE_mInt32(max_remote_storage_count);

// reference https://github.com/edenhill/librdkafka/blob/master/INTRODUCTION.md#broker-version-compatibility
// If the dependent kafka broker version older than 0.10.0.0,
// the value of kafka_api_version_request should be false, and the
// value set by the fallback version kafka_broker_version_fallback will be used,
// and the valid values are: 0.9.0.x, 0.8.x.y.
DECLARE_String(kafka_api_version_request);
DECLARE_String(kafka_broker_version_fallback);
DECLARE_mString(kafka_debug);

// The number of pool siz of routine load consumer.
// If you meet the error describe in https://github.com/edenhill/librdkafka/issues/3608
// Change this size to 0 to fix it temporarily.
DECLARE_Int32(routine_load_consumer_pool_size);

// Used in single-stream-multi-table load. When receive a batch of messages from kafka,
// if the size of batch is more than this threshold, we will request plans for all related tables.
DECLARE_Int32(multi_table_batch_plan_threshold);

// When the timeout of a load task is less than this threshold,
// Doris treats it as a high priority task.
// high priority tasks use a separate thread pool for flush and do not block rpc by memory cleanup logic.
// this threshold is mainly used to identify routine load tasks and should not be modified if not necessary.
DECLARE_mInt32(load_task_high_priority_threshold_second);

// The min timeout of load rpc (add batch, close)
// Because a load rpc may be blocked for a while.
// Increase this config may avoid rpc timeout.
DECLARE_mInt32(min_load_rpc_timeout_ms);

// use which protocol to access function service, candicate is baidu_std/h2:grpc
DECLARE_String(function_service_protocol);

// use which load balancer to select server to connect
DECLARE_String(rpc_load_balancer);

// Enable tracing
// If this configuration is enabled, you should also specify the trace_export_url.
DECLARE_Bool(enable_tracing);

// Enable opentelemtry collector
DECLARE_Bool(enable_otel_collector);

// Current support for exporting traces:
// zipkin: Export traces directly to zipkin, which is used to enable the tracing feature quickly.
// collector: The collector can be used to receive and process traces and support export to a variety of
//   third-party systems.
DECLARE_mString(trace_exporter);

// The endpoint to export spans to.
// export to zipkin like: http://127.0.0.1:9411/api/v2/spans
// export to collector like: http://127.0.0.1:4318/v1/traces
DECLARE_String(trace_export_url);

// The maximum buffer/queue size to collect span. After the size is reached, spans are dropped.
// An export will be triggered when the number of spans in the queue reaches half of the maximum.
DECLARE_Int32(max_span_queue_size);

// The maximum batch size of every export spans. It must be smaller or equal to max_queue_size.
DECLARE_Int32(max_span_export_batch_size);

// The time interval between two consecutive export spans.
DECLARE_Int32(export_span_schedule_delay_millis);

// a soft limit of string type length, the hard limit is 2GB - 4, but if too long will cause very low performance,
// so we set a soft limit, default is 1MB
DECLARE_mInt32(string_type_length_soft_limit_bytes);

DECLARE_mInt32(jsonb_type_length_soft_limit_bytes);

// used for olap scanner to save memory, when the size of unused_object_pool
// is greater than object_pool_buffer_size, release the object in the unused_object_pool.
DECLARE_Int32(object_pool_buffer_size);

// Threshold fo reading a small file into memory
DECLARE_mInt32(in_memory_file_size);

// ParquetReaderWrap prefetch buffer size
DECLARE_Int32(parquet_reader_max_buffer_size);
// Max size of parquet page header in bytes
DECLARE_mInt32(parquet_header_max_size_mb);
// Max buffer size for parquet row group
DECLARE_mInt32(parquet_rowgroup_max_buffer_mb);
// Max buffer size for parquet chunk column
DECLARE_mInt32(parquet_column_max_buffer_mb);
// Merge small IO, the max amplified read ratio
DECLARE_mDouble(max_amplified_read_ratio);

// OrcReader
DECLARE_mInt32(orc_natural_read_size_mb);
DECLARE_mInt64(big_column_size_buffer);
DECLARE_mInt64(small_column_size_buffer);

// When the rows number reached this limit, will check the filter rate the of bloomfilter
// if it is lower than a specific threshold, the predicate will be disabled.
DECLARE_mInt32(bloom_filter_predicate_check_row_num);

// cooldown task configs
DECLARE_Int32(cooldown_thread_num);
DECLARE_mInt64(generate_cooldown_task_interval_sec);
DECLARE_mInt32(remove_unused_remote_files_interval_sec); // 6h
DECLARE_mInt32(confirm_unused_remote_files_interval_sec);
DECLARE_Int32(cold_data_compaction_thread_num);
DECLARE_mInt32(cold_data_compaction_interval_sec);
DECLARE_Int32(concurrency_per_dir);
// file_cache_type is used to set the type of file cache for remote files.
// "": no cache, "sub_file_cache": split sub files from remote file.
// "whole_file_cache": the whole file.
DECLARE_mString(file_cache_type);

DECLARE_Int32(s3_transfer_executor_pool_size);

DECLARE_Bool(enable_time_lut);
DECLARE_Bool(enable_simdjson_reader);

DECLARE_mBool(enable_query_like_bloom_filter);
// number of s3 scanner thread pool size
DECLARE_Int32(doris_remote_scanner_thread_pool_thread_num);
// number of s3 scanner thread pool queue size
DECLARE_Int32(doris_remote_scanner_thread_pool_queue_size);

// limit the queue of pending batches which will be sent by a single nodechannel
DECLARE_mInt64(nodechannel_pending_queue_max_bytes);

// The batch size for sending data by brpc streaming client
DECLARE_mInt64(brpc_streaming_client_batch_bytes);

// Max waiting time to wait the "plan fragment start" rpc.
// If timeout, the fragment will be cancelled.
// This parameter is usually only used when the FE loses connection,
// and the BE can automatically cancel the relevant fragment after the timeout,
// so as to avoid occupying the execution thread for a long time.
DECLARE_mInt32(max_fragment_start_wait_time_seconds);

// Node role tag for backend. Mix role is the default role, and computation role have no
// any tablet.
DECLARE_String(be_node_role);

// Hide webserver page for safety.
// Hide the be config page for webserver.
DECLARE_Bool(hide_webserver_config_page);

DECLARE_Bool(enable_segcompaction);

// Max number of segments allowed in a single segcompaction task.
DECLARE_Int32(segcompaction_batch_size);

// Max row count allowed in a single source segment, bigger segments will be skipped.
DECLARE_Int32(segcompaction_candidate_max_rows);

// Max file size allowed in a single source segment, bigger segments will be skipped.
DECLARE_Int64(segcompaction_candidate_max_bytes);

// Max total row count allowed in a single segcompaction task.
DECLARE_Int32(segcompaction_task_max_rows);

// Max total file size allowed in a single segcompaction task.
DECLARE_Int64(segcompaction_task_max_bytes);

// Global segcompaction thread pool size.
DECLARE_mInt32(segcompaction_num_threads);

// enable java udf and jdbc scannode
DECLARE_Bool(enable_java_support);

// Set config randomly to check more issues in github workflow
DECLARE_Bool(enable_fuzzy_mode);

DECLARE_Bool(enable_debug_points);

DECLARE_Int32(pipeline_executor_size);
DECLARE_Bool(enable_workload_group_for_scan);

// Temp config. True to use optimization for bitmap_index apply predicate except leaf node of the and node.
// Will remove after fully test.
DECLARE_Bool(enable_index_apply_preds_except_leafnode_of_andnode);

// block file cache
DECLARE_Bool(enable_file_cache);
// format: [{"path":"/path/to/file_cache","total_size":21474836480,"query_limit":10737418240}]
// format: [{"path":"/path/to/file_cache","total_size":21474836480,"query_limit":10737418240},{"path":"/path/to/file_cache2","total_size":21474836480,"query_limit":10737418240}]
DECLARE_String(file_cache_path);
DECLARE_Int64(file_cache_min_file_segment_size);
DECLARE_Int64(file_cache_max_file_segment_size);
DECLARE_Bool(clear_file_cache);
DECLARE_Bool(enable_file_cache_query_limit);
// only for debug, will be removed after finding out the root cause
DECLARE_mInt32(file_cache_wait_sec_after_fail); // zero for no waiting and retrying

// inverted index searcher cache
// cache entry stay time after lookup
DECLARE_mInt32(index_cache_entry_stay_time_after_lookup_s);
// cache entry that have not been visited for a certain period of time can be cleaned up by GC thread
DECLARE_mInt32(inverted_index_cache_stale_sweep_time_sec);
// inverted index searcher cache size
DECLARE_String(inverted_index_searcher_cache_limit);
// set `true` to enable insert searcher into cache when write inverted index data
DECLARE_Bool(enable_write_index_searcher_cache);
DECLARE_Bool(enable_inverted_index_cache_check_timestamp);
DECLARE_Int32(inverted_index_fd_number_limit_percent); // 50%

// inverted index match bitmap cache size
DECLARE_String(inverted_index_query_cache_limit);

// inverted index
DECLARE_mDouble(inverted_index_ram_buffer_size);
DECLARE_Int32(query_bkd_inverted_index_limit_percent); // 5%
// dict path for chinese analyzer
DECLARE_String(inverted_index_dict_path);
DECLARE_Int32(inverted_index_read_buffer_size);
// tree depth for bkd index
DECLARE_Int32(max_depth_in_bkd_tree);
// index compaction
DECLARE_Bool(inverted_index_compaction_enable);
// use num_broadcast_buffer blocks as buffer to do broadcast
DECLARE_Int32(num_broadcast_buffer);
// semi-structure configs
DECLARE_Bool(enable_parse_multi_dimession_array);

// max depth of expression tree allowed.
DECLARE_Int32(max_depth_of_expr_tree);

// Report a tablet as bad when io errors occurs more than this value.
DECLARE_mInt64(max_tablet_io_errors);

// Report a tablet as bad when its path not found
DECLARE_Int32(tablet_path_check_interval_seconds);
DECLARE_mInt32(tablet_path_check_batch_size);

// Page size of row column, default 4KB
DECLARE_mInt64(row_column_page_size);
// it must be larger than or equal to 5MB
DECLARE_mInt32(s3_write_buffer_size);
// the size of the whole s3 buffer pool, which indicates the s3 file writer
// can at most buffer 50MB data. And the num of multi part upload task is
// s3_write_buffer_whole_size / s3_write_buffer_size
DECLARE_mInt32(s3_write_buffer_whole_size);
// the max number of cached file handle for block segemnt
DECLARE_mInt64(file_cache_max_file_reader_cache_size);
//enable shrink memory
DECLARE_Bool(enable_shrink_memory);
// enable cache for high concurrent point query work load
DECLARE_mInt32(schema_cache_capacity);
DECLARE_mInt32(schema_cache_sweep_time_sec);

// max number of segment cache
DECLARE_mInt32(segment_cache_capacity);

// enable binlog
DECLARE_Bool(enable_feature_binlog);

// enable set in BitmapValue
DECLARE_Bool(enable_set_in_bitmap_value);

// max number of hdfs file handle in cache
DECLARE_Int64(max_hdfs_file_handle_cache_num);
// max number of meta info of external files, such as parquet footer
DECLARE_Int64(max_external_file_meta_cache_num);
// Apply delete pred in cumu compaction
DECLARE_mBool(enable_delete_when_cumu_compaction);

// max_write_buffer_number for rocksdb
DECLARE_Int32(rocksdb_max_write_buffer_number);

// Allow invalid decimalv2 literal for compatible with old version. Recommend set it false strongly.
DECLARE_mBool(allow_invalid_decimalv2_literal);
// the max expiration time of kerberos ticket.
// If a hdfs filesytem with kerberos authentication live longer
// than this time, it will be expired.
DECLARE_mInt64(kerberos_expiration_time_seconds);

// Values include `none`, `glog`, `boost`, `glibc`, `libunwind`
DECLARE_mString(get_stack_trace_tool);

// DISABLED: Don't resolve location info.
// FAST: Perform CU lookup using .debug_aranges (might be incomplete).
// FULL: Scan all CU in .debug_info (slow!) on .debug_aranges lookup failure.
// FULL_WITH_INLINE: Scan .debug_info (super slower, use with caution) for inline functions in addition to FULL.
DECLARE_mString(dwarf_location_info_mode);

// the ratio of _prefetch_size/_batch_size in AutoIncIDBuffer
DECLARE_mInt64(auto_inc_prefetch_size_ratio);

// the ratio of _low_level_water_level_mark/_batch_size in AutoIncIDBuffer
DECLARE_mInt64(auto_inc_low_water_level_mark_size_ratio);

// number of threads that fetch auto-inc ranges from FE
DECLARE_mInt64(auto_inc_fetch_thread_num);
// Max connection cache num for point lookup queries
DECLARE_mInt64(lookup_connection_cache_bytes_limit);

// level of compression when using LZ4_HC, whose defalut value is LZ4HC_CLEVEL_DEFAULT
DECLARE_mInt64(LZ4_HC_compression_level);

DECLARE_mBool(enable_merge_on_write_correctness_check);

// The secure path with user files, used in the `local` table function.
DECLARE_mString(user_files_secure_path);

// This threshold determines how many partitions will be allocated for window function get topn.
// and if this threshold is exceeded, the remaining data will be pass through to other node directly.
DECLARE_Int32(partition_topn_partition_threshold);

// If fe's frontend info has not been updated for more than fe_expire_duration_seconds, it will be regarded
// as an abnormal fe, this will cause be to cancel this fe's related query.
DECLARE_Int32(fe_expire_duration_seconds);

// If use stop_be.sh --grace, then BE has to wait all running queries to stop to avoiding running query failure
// , but if the waiting time exceed the limit, then be will exit directly.
// During this period, FE will not send any queries to BE and waiting for all running queries to stop.
DECLARE_Int32(grace_shutdown_wait_seconds);

// BitmapValue serialize version.
DECLARE_Int16(bitmap_serialize_version);

// group commit insert config
DECLARE_String(group_commit_replay_wal_dir);
DECLARE_Int32(group_commit_replay_wal_retry_num);
DECLARE_Int32(group_commit_replay_wal_retry_interval_seconds);
DECLARE_Int32(group_commit_sync_wal_batch);

// This config can be set to limit thread number in group commit insert thread pool.
DECLARE_mInt32(group_commit_insert_threads);
DECLARE_mInt32(group_commit_interval_ms);

// The configuration item is used to lower the priority of the scanner thread,
// typically employed to ensure CPU scheduling for write operations.
// Default is 0, which is default value of thread nice value, increase this value
// to lower the priority of scan threads
DECLARE_Int32(scan_thread_nice_value);
// Used to modify the recycle interval of tablet schema cache
DECLARE_mInt32(tablet_schema_cache_recycle_interval);

// Use `LOG(FATAL)` to replace `throw` when true
DECLARE_mBool(exit_on_exception);

// cgroup
DECLARE_String(doris_cgroup_cpu_path);
DECLARE_Bool(enable_cpu_hard_limit);
// This config controls whether the s3 file writer would flush cache asynchronously
DECLARE_Bool(enable_flush_file_cache_async);

// Remove predicate that is always true for a segment.
DECLARE_Bool(ignore_always_true_predicate_for_segment);

// Dir of default timezone files
DECLARE_String(default_tzfiles_path);

#ifdef BE_TEST
// test s3
DECLARE_String(test_s3_resource);
DECLARE_String(test_s3_ak);
DECLARE_String(test_s3_sk);
DECLARE_String(test_s3_endpoint);
DECLARE_String(test_s3_region);
DECLARE_String(test_s3_bucket);
DECLARE_String(test_s3_prefix);
#endif

class Register {
public:
    struct Field {
        const char* type = nullptr;
        const char* name = nullptr;
        void* storage = nullptr;
        const char* defval = nullptr;
        bool valmutable = false;
        Field(const char* ftype, const char* fname, void* fstorage, const char* fdefval,
              bool fvalmutable)
                : type(ftype),
                  name(fname),
                  storage(fstorage),
                  defval(fdefval),
                  valmutable(fvalmutable) {}
    };

public:
    static std::map<std::string, Field>* _s_field_map;

public:
    Register(const char* ftype, const char* fname, void* fstorage, const char* fdefval,
             bool fvalmutable) {
        if (_s_field_map == nullptr) {
            _s_field_map = new std::map<std::string, Field>();
        }
        Field field(ftype, fname, fstorage, fdefval, fvalmutable);
        _s_field_map->insert(std::make_pair(std::string(fname), field));
    }
};

// RegisterConfValidator class is used to store validator function of registered config fields in
// Register::_s_field_map.
// If any validator return false when BE bootstart, the bootstart will be terminated.
// If validator return false when use http API to update some config, the config will not
// be modified and the API will return failure.
class RegisterConfValidator {
public:
    // Validator for each config name.
    static std::map<std::string, std::function<bool()>>* _s_field_validator;

public:
    RegisterConfValidator(const char* fname, const std::function<bool()>& validator) {
        if (_s_field_validator == nullptr) {
            _s_field_validator = new std::map<std::string, std::function<bool()>>();
        }
        // register validator to _s_field_validator
        _s_field_validator->insert(std::make_pair(std::string(fname), validator));
    }
};

// configuration properties load from config file.
class Properties {
public:
    // load conf from file, if must_exist is true and file does not exist, return false
    bool load(const char* conf_file, bool must_exist = true);

    // Find the config value by key from `file_conf_map`.
    // If found, set `retval` to the config value,
    // or set `retval` to `defstr`
    // if retval is not set(in case defstr is nullptr), set is_retval_set to false
    template <typename T>
    bool get_or_default(const char* key, const char* defstr, T& retval, bool* is_retval_set) const;

    void set(const std::string& key, const std::string& val);

    void set_force(const std::string& key, const std::string& val);

    // dump props to conf file
    void dump(const std::string& conffile);

private:
    std::map<std::string, std::string> file_conf_map;
};

// full configurations.
extern std::map<std::string, std::string>* full_conf_map;

extern std::mutex custom_conf_lock;

// Init the config from `conf_file`.
// If fill_conf_map is true, the updated config will also update the `full_conf_map`.
// If must_exist is true and `conf_file` does not exist, this function will return false.
// If set_to_default is true, the config value will be set to default value if not found in `conf_file`.
bool init(const char* conf_file, bool fill_conf_map = false, bool must_exist = true,
          bool set_to_default = true);

void set_config(const std::string& field, const std::string& value, bool need_persist = false,
                bool force = false);

void persist_config(const std::string& field, const std::string& value);

std::mutex* get_mutable_string_config_lock();

std::vector<std::vector<std::string>> get_config_info();

void set_fuzzy_config(const std::string& field, const std::string& value);

void set_fuzzy_configs();

} // namespace config
} // namespace doris
