// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

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

package com.baidu.palo.common;

public class Config extends ConfigBase {

    @ConfField public static String sys_log_dir = System.getenv("PALO_HOME") + "/log";
    @ConfField public static String sys_log_level = "INFO"; // INFO, WARNING, ERROR, FATAL
    @ConfField public static String sys_log_roll_mode = "SIZE-MB-1024"; // TIME-DAY， TIME-HOUR， SIZE-MB-nnn
    @ConfField public static int sys_log_roll_num = 10; // the config doesn't work if rollmode is TIME-*

    // verbose modules. VERBOSE level is implemented by log4j DEBUG level.
    @ConfField public static String[] sys_log_verbose_modules = {};

    @ConfField public static String audit_log_dir = System.getenv("PALO_HOME") + "/log";
    @ConfField public static String[] audit_log_modules = {"slow_query", "query"};
    @ConfField public static String audit_log_roll_mode = "TIME-DAY"; // TIME-DAY， TIME-HOUR， SIZE-MB-nnn
    @ConfField public static int audit_log_roll_num = 10; // the config doesn't work if rollmode is TIME-*

    @ConfField public static int label_keep_max_second = 7 * 24 * 3600; // 7 days
    @ConfField public static int label_clean_interval_second = 4 * 3600; // 4 hours
    @ConfField public static int quorum_load_job_max_second = 24 * 3600; // 1 days

    // Configurations for meta data durability
    @ConfField public static String meta_dir = System.getenv("PALO_HOME") + "/palo-meta";
    @ConfField public static String edit_log_type = "BDB";    // BDB, LOCAL
    @ConfField public static int edit_log_port = 9010;        // Only used when edit_log_type = "BDB
    @ConfField public static int edit_log_roll_num = 100000;
    @ConfField public static int meta_delay_toleration_second = 300;    // 5 min
    @ConfField public static String master_sync_policy = "WRITE_NO_SYNC"; // SYNC, NO_SYNC, WRITE_NO_SYNC
    @ConfField public static String replica_sync_policy = "WRITE_NO_SYNC"; // SYNC, NO_SYNC, WRITE_NO_SYNC
    @ConfField public static String replica_ack_policy = "SIMPLE_MAJORITY"; // ALL, NONE, SIMPLE_MAJORITY

    // kudu master address
    @ConfField public static String kudu_master_addresses = "127.0.0.1:8030";
    @ConfField public static int kudu_client_timeout_ms = 500;

    /*
     * true means reset replication group. If all the electable nodes can not start, we can
     * copy the meta data to another node and set this item to true to recover the metadata.
     * In this scenario, we can get the newest image file contains all the meta data, then
     * use the image file to restart the failed cluster.
     */
    @ConfField public static String metadata_failure_recovery = "false";

    /*
     * false means non-master node need to check its own journal is out of date or not in every replay loop.
     * true means to ignore this meta check.
     * this should only be set by rest api and only be set when master is truly out of services.
     */
    @ConfField public static boolean ignore_meta_check = false;

    @ConfField public static int http_port = 8030;
    @ConfField public static int rpc_port = 9020;
    @ConfField public static int query_port = 9030;

    // Config cluster name and id
    @ConfField public static String cluster_name = "Baidu Palo";
    @ConfField public static int cluster_id = -1;

    // Configurations for load, clone, create table, alter table etc. We will rarely change them
    @ConfField public static int tablet_create_timeout_second = 1;
    @ConfField public static int table_create_default_keys_num = 5;
    @ConfField public static int table_create_default_distribute_num = 5;
    @ConfField public static int load_checker_interval_second = 5;
    @ConfField public static int load_pending_thread_num_high_priority = 3;
    @ConfField public static int load_pending_thread_num_normal_priority = 10;
    @ConfField public static int load_etl_thread_num_high_priority = 3;
    @ConfField public static int load_etl_thread_num_normal_priority = 10;
    @ConfField public static int load_input_size_limit_gb = 0; // GB, 0 is no limit
    @ConfField public static int load_running_job_num_limit = 0; // 0 is no limit
    @ConfField public static int tablet_delete_timeout_second = 2;
    @ConfField public static int clone_checker_interval_second = 300;
    @ConfField public static int clone_job_timeout_second = 7200; // 2h
    @ConfField public static int clone_max_job_num = 100;
    @ConfField public static int clone_low_priority_delay_second = 600;
    @ConfField public static int clone_normal_priority_delay_second = 300;
    @ConfField public static int clone_high_priority_delay_second = 0;
    @ConfField public static double clone_capacity_balance_threshold = 0.2;
    @ConfField public static double clone_distribution_balance_threshold = 0.2;
    @ConfField public static int alter_table_timeout_second = 86400; // 1day
    @ConfField public static int alter_delete_base_delay_second = 600; // 10min
    @ConfField public static int max_backend_down_time_second = 3600; // 1h
    @ConfField public static long storage_cooldown_second = 30 * 24 * 3600L; // 30 days
    @ConfField public static long catalog_trash_expire_second = 86400L; // 1day
    @ConfField public static int pull_load_task_default_timeout_second = 3600; // 1hour
    @ConfField public static long min_bytes_per_broker_scanner = 67108864L; // 64MB
    @ConfField public static int max_broker_concurrency = 10;
    @ConfField public static int export_checker_interval_second = 5;
    @ConfField public static int export_pending_thread_num = 5;
    @ConfField public static int export_exporting_thread_num = 10;
    @ConfField public static int export_running_job_num_limit = 0; // 0 is no limit
    @ConfField public static int export_task_default_timeout_second = 24 * 3600;
    @ConfField public static int export_parallel_tablet_num = 5;
    @ConfField public static int export_keep_max_second = 7 * 24 * 3600; // 7 days

    // Configurations for consistency check
    @ConfField public static String consistency_check_start_time = "23";
    @ConfField public static String consistency_check_end_time = "4";
    @ConfField public static long check_consistency_default_timeout_second = 600; // 10 min

    // Configurations for query engine
    @ConfField public static int qe_max_connection = 1024;
    @ConfField public static int max_conn_per_user = 100;
    @ConfField public static int qe_query_timeout_second = 300;
    @ConfField public static long qe_slow_log_ms = 5000;
    @ConfField public static int blacklist_backends_max_times = 6;
    @ConfField public static int meta_resource_publish_interval_ms = 60000; // 1m
    @ConfField public static int meta_publish_timeout_ms = 1000;
    @ConfField public static boolean proxy_auth_enable = false;
    @ConfField public static String proxy_auth_magic_prefix = "x@8";
    // Limits on the number of expr children and the depth of an expr tree.
    // exceed this limit may cause long analysis time while holding db read lock.
    @ConfField public static int expr_children_limit = 10000;
    // The expr depth limit is mostly due to our recursive implementation of toSql().
    @ConfField public static int expr_depth_limit = 3000;

    // Configurations for backup and restore
    @ConfField public static String backup_plugin_path = "/tools/trans_file_tool/trans_files.sh";

    // Configurations for hadoop dpp
    @ConfField public static String dpp_hadoop_client_path = "/lib/hadoop-client/hadoop/bin/hadoop";
    @ConfField public static long dpp_bytes_per_reduce = 100 * 1024 * 1024L; // 100M
    @ConfField public static String dpp_default_cluster = "palo-dpp";
    @ConfField public static String dpp_default_config_str = ""
            + "{"
            + "hadoop_configs : '"
            + "mapred.job.priority=NORMAL;"
            + "mapred.job.map.capacity=50;"
            + "mapred.job.reduce.capacity=50;"
            + "mapred.hce.replace.streaming=false;"
            + "abaci.long.stored.job=true;"
            + "dce.shuffle.enable=false;"
            + "dfs.client.authserver.force_stop=true;"
            + "dfs.client.auth.method=0"
            + "'}";
    @ConfField public static String dpp_config_str = ""
            + "{palo-dpp : {"
            + "hadoop_palo_path : '/dir',"
            + "hadoop_configs : '"
            + "fs.default.name=hdfs://host:port;"
            + "mapred.job.tracker=host:port;"
            + "hadoop.job.ugi=user,password"
            + "'}"
            + "}";
}
