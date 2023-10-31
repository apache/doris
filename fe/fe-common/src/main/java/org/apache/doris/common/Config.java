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

package org.apache.doris.common;

public class Config extends ConfigBase {

    /**
     * Dir of custom config file
     */
    @ConfField
    public static String custom_config_dir = System.getenv("DORIS_HOME") + "/conf";

    /**
     * The max size of one sys log and audit log
     */
    @ConfField public static int log_roll_size_mb = 1024; // 1 GB

    /**
     * sys_log_dir:
     *      This specifies FE log dir. FE will produces 2 log files:
     *      fe.log:      all logs of FE process.
     *      fe.warn.log  all WARNING and ERROR log of FE process.
     *
     * sys_log_level:
     *      INFO, WARNING, ERROR, FATAL
     *
     * sys_log_roll_num:
     *      Maximal FE log files to be kept within an sys_log_roll_interval.
     *      default is 10, which means there will be at most 10 log files in a day
     *
     * sys_log_verbose_modules:
     *      Verbose modules. VERBOSE level is implemented by log4j DEBUG level.
     *      eg:
     *          sys_log_verbose_modules = org.apache.doris.catalog
     *      This will only print debug log of files in package org.apache.doris.catalog and all its sub packages.
     *
     * sys_log_roll_interval:
     *      DAY:  log suffix is yyyyMMdd
     *      HOUR: log suffix is yyyyMMddHH
     *
     * sys_log_delete_age:
     *      default is 7 days, if log's last modify time is 7 days ago, it will be deleted.
     *      support format:
     *          7d      7 days
     *          10h     10 hours
     *          60m     60 mins
     *          120s    120 seconds
     */
    @ConfField
    public static String sys_log_dir = System.getenv("DORIS_HOME") + "/log";
    @ConfField
    public static String sys_log_level = "INFO";
    @ConfField public static int sys_log_roll_num = 10;
    @ConfField
    public static String[] sys_log_verbose_modules = {};
    @ConfField public static String sys_log_roll_interval = "DAY";
    @ConfField public static String sys_log_delete_age = "7d";
    @Deprecated
    @ConfField public static String sys_log_roll_mode = "SIZE-MB-1024";
    @ConfField
    public static boolean sys_log_enable_compress = false;

    /**
     * audit_log_dir:
     *      This specifies FE audit log dir.
     *      Audit log fe.audit.log contains all requests with related infos such as user, host, cost, status, etc.
     *
     * audit_log_roll_num:
     *      Maximal FE audit log files to be kept within an audit_log_roll_interval.
     *
     * audit_log_modules:
     *       Slow query contains all queries which cost exceed *qe_slow_log_ms*
     *
     * qe_slow_log_ms:
     *      If the response time of a query exceed this threshold, it will be recorded in audit log as slow_query.
     *
     * audit_log_roll_interval:
     *      DAY:  log suffix is yyyyMMdd
     *      HOUR: log suffix is yyyyMMddHH
     *
     * audit_log_delete_age:
     *      default is 30 days, if log's last modify time is 30 days ago, it will be deleted.
     *      support format:
     *          7d      7 days
     *          10h     10 hours
     *          60m     60 mins
     *          120s    120 seconds
     */
    @ConfField
    public static String audit_log_dir = System.getenv("DORIS_HOME") + "/log";
    @ConfField
    public static int audit_log_roll_num = 90;
    @ConfField
    public static String[] audit_log_modules = {"slow_query", "query", "load", "stream_load"};
    @ConfField(mutable = true)
    public static long qe_slow_log_ms = 5000;
    @ConfField
    public static String audit_log_roll_interval = "DAY";
    @ConfField
    public static String audit_log_delete_age = "30d";
    @Deprecated
    @ConfField
    public static String audit_log_roll_mode = "TIME-DAY";
    @ConfField
    public static boolean audit_log_enable_compress = false;

    /**
     * plugin_dir:
     * plugin install directory
     */
    @ConfField
    public static String plugin_dir = System.getenv("DORIS_HOME") + "/plugins";

    @ConfField(mutable = true, masterOnly = true)
    public static boolean plugin_enable = true;

    /**
     * The default path to save jdbc drivers.
     * You can put all jdbc drivers in this path, and when creating jdbc resource with only jdbc driver file name,
     * Doris will find jars from this path.
     */
    @ConfField
    public static String jdbc_drivers_dir = System.getenv("DORIS_HOME") + "/jdbc_drivers";

    /**
     * The default parallelism of the load execution plan
     * on a single node when the broker load is submitted
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int default_load_parallelism = 1;

    /**
     * Labels of finished or cancelled load jobs will be removed after *label_keep_max_second*
     * The removed labels can be reused.
     * Set a short time will lower the FE memory usage.
     * (Because all load jobs' info is kept in memory before being removed)
     */
    @ConfField(mutable = true)
    public static int label_keep_max_second = 3 * 24 * 3600; // 3 days

    // For some high frequency load job such as
    // INSERT, STREAMING LOAD, ROUTINE_LOAD_TASK, DELETE
    // Remove the finished job or task if expired.
    @ConfField(mutable = true, masterOnly = true)
    public static int streaming_label_keep_max_second = 43200; // 12 hour

    /**
     * The max keep time of some kind of jobs.
     * like alter job or export job.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int history_job_keep_max_second = 7 * 24 * 3600; // 7 days

    /**
     * the transaction will be cleaned after transaction_clean_interval_second seconds
     * if the transaction is visible or aborted
     * we should make this interval as short as possible and each clean cycle as soon as possible
     */
    @ConfField
    public static int transaction_clean_interval_second = 30;

    /**
     * Load label cleaner will run every *label_clean_interval_second* to clean the outdated jobs.
     */
    @ConfField
    public static int label_clean_interval_second = 1 * 3600; // 1 hours

    // Configurations for meta data durability
    /**
     * Doris meta data will be saved here.
     * The storage of this dir is highly recommended as to be:
     * 1. High write performance (SSD)
     * 2. Safe (RAID)
     */
    @ConfField
    public static String meta_dir = System.getenv("DORIS_HOME") + "/doris-meta";

    /**
     * temp dir is used to save intermediate results of some process, such as backup and restore process.
     * file in this dir will be cleaned after these process is finished.
     */
    @ConfField
    public static String tmp_dir = System.getenv("DORIS_HOME") + "/temp_dir";

    /**
     * Edit log type.
     * BDB: write log to bdbje
     * LOCAL: use local file to save edit log, only used for unit test
     */
    @ConfField
    public static String edit_log_type = "bdb";

    /**
     * bdbje port
     */
    @ConfField
    public static int edit_log_port = 9010;

    /**
     * Master FE will save image every *edit_log_roll_num* meta journals.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int edit_log_roll_num = 50000;

    /**
     * Non-master FE will stop offering service
     * if meta data delay gap exceeds *meta_delay_toleration_second*
     */
    @ConfField public static int meta_delay_toleration_second = 300;    // 5 min

    /**
     * Master FE sync policy of bdbje.
     * If you only deploy one Follower FE, set this to 'SYNC'. If you deploy more than 3 Follower FE,
     * you can set this and the following 'replica_sync_policy' to WRITE_NO_SYNC.
     * more info, see: http://docs.oracle.com/cd/E17277_02/html/java/com/sleepycat/je/Durability.SyncPolicy.html
     */
    @ConfField public static String master_sync_policy = "SYNC"; // SYNC, NO_SYNC, WRITE_NO_SYNC

    /**
     * Follower FE sync policy of bdbje.
     */
    @ConfField public static String replica_sync_policy = "SYNC"; // SYNC, NO_SYNC, WRITE_NO_SYNC

    /**
     * Replica ack policy of bdbje.
     * more info, see: http://docs.oracle.com/cd/E17277_02/html/java/com/sleepycat/je/Durability.ReplicaAckPolicy.html
     */
    @ConfField public static String replica_ack_policy = "SIMPLE_MAJORITY"; // ALL, NONE, SIMPLE_MAJORITY

    /**
     * The heartbeat timeout of bdbje between master and follower.
     * the default is 30 seconds, which is same as default value in bdbje.
     * If the network is experiencing transient problems, of some unexpected long java GC annoying you,
     * you can try to increase this value to decrease the chances of false timeouts
     */
    @ConfField
    public static int bdbje_heartbeat_timeout_second = 30;

    /**
     * The lock timeout of bdbje operation
     * If there are many LockTimeoutException in FE WARN log, you can try to increase this value
     */
    @ConfField
    public static int bdbje_lock_timeout_second = 1;

    /**
     * The replica ack timeout when writing to bdbje
     * When writing some relatively large logs, the ack time may time out, resulting in log writing failure.
     * At this time, you can increase this value appropriately.
     */
    @ConfField
    public static int bdbje_replica_ack_timeout_second = 10;

    /**
     * The desired upper limit on the number of bytes of reserved space to
     * retain in a replicated JE Environment.
     * You only need to decrease this value if your FE meta disk is really small.
     * And don't need to increase this value.
     */
    @ConfField
    public static int bdbje_reserved_disk_bytes = 1 * 1024 * 1024 * 1024; // 1G

    /**
     * num of thread to handle heartbeat events in heartbeat_mgr.
     */
    @ConfField(masterOnly = true)
    public static int heartbeat_mgr_threads_num = 8;

    /**
     * blocking queue size to store heartbeat task in heartbeat_mgr.
     */
    @ConfField(masterOnly = true)
    public static int heartbeat_mgr_blocking_queue_size = 1024;

    /**
     * max num of thread to handle agent task in agent task thread-pool.
     */
    @ConfField(masterOnly = true)
    public static int max_agent_task_threads_num = 4096;

    /**
     * the max txn number which bdbje can rollback when trying to rejoin the group
     */
    @ConfField public static int txn_rollback_limit = 100;

    /**
     * Specified an IP for frontend, instead of the ip get by *InetAddress.getByName*.
     * This can be used when *InetAddress.getByName* get an unexpected IP address.
     * Default is "0.0.0.0", which means not set.
     * CAN NOT set this as a hostname, only IP.
     */
    @Deprecated
    @ConfField
    public static String frontend_address = "0.0.0.0";

    /**
     * Declare a selection strategy for those servers have many ips.
     * Note that there should at most one ip match this list.
     * this is a list in semicolon-delimited format, in CIDR notation, e.g. 10.10.10.0/24
     * If no ip match this rule, will choose one randomly.
     */
    @ConfField public static String priority_networks = "";

    /**
     * If true, FE will reset bdbje replication group(that is, to remove all electable nodes info)
     * and is supposed to start as Master.
     * If all the electable nodes can not start, we can copy the meta data
     * to another node and set this config to true to try to restart the FE.
     */
    @ConfField public static String metadata_failure_recovery = "false";

    /**
     * If true, non-master FE will ignore the meta data delay gap between Master FE and its self,
     * even if the metadata delay gap exceeds *meta_delay_toleration_second*.
     * Non-master FE will still offer read service.
     *
     * This is helpful when you try to stop the Master FE for a relatively long time for some reason,
     * but still wish the non-master FE can offer read service.
     */
    @ConfField(mutable = true)
    public static boolean ignore_meta_check = false;

    /**
     * Set the maximum acceptable clock skew between non-master FE to Master FE host.
     * This value is checked whenever a non-master FE establishes a connection to master FE via BDBJE.
     * The connection is abandoned if the clock skew is larger than this value.
     */
    @ConfField public static long max_bdbje_clock_delta_ms = 5000; // 5s

    /**
     * Fe http port
     * Currently, all FEs' http port must be same.
     */
    @ConfField public static int http_port = 8030;

    /**
     * Jetty container default configuration
     * Jetty's thread architecture model is very simple, divided into three thread pools:
     * acceptors,selectors and workers. Acceptors are responsible for accepting new connections,
     * and then hand over to selectors to process the unpacking of the HTTP message protocol,
     * and finally workers process the request. The first two thread pools adopt a non-blocking model,
     * and one thread can handle the read and write of many sockets, so the number of thread pools is small.
     *
     * For most projects, only 1-2 acceptors threads are needed, and 2 to 4 selectors threads are sufficient.
     * Workers are obstructive business logic, often have more database operations, and require a large number of
     * threads. The specific number depends on the proportion of QPS and IO events of the application. The higher the
     * QPS, the more threads are required, the higher the proportion of IO, the more threads waiting, and the more
     * total threads required.
     */
    @ConfField public static int jetty_server_acceptors = 2;
    @ConfField public static int jetty_server_selectors = 4;
    @ConfField public static int jetty_server_workers = 0;

    /**
     * Configure the default minimum and maximum number of threads for jetty.
     * The default minimum and maximum number of threads for jetty is 10 and the maximum is 200.
     * If this is relatively small in a high-concurrency import scenario,
     * users can adjust it according to their own conditions.
     */
    @ConfField public static int jetty_threadPool_minThreads = 20;
    @ConfField public static int jetty_threadPool_maxThreads = 400;

    /**
     * Jetty maximum number of bytes in put or post method,default:100MB
     */
    @ConfField public static int jetty_server_max_http_post_size = 100 * 1024 * 1024;

    /**
     * http header size configuration parameter, the default value is 10K
     */
    @ConfField public static int jetty_server_max_http_header_size = 10240;

    /**
     * Mini load disabled by default
     */
    @ConfField public static boolean disable_mini_load = true;

    /**
     * The backlog_num for mysql nio server
     * When you enlarge this backlog_num, you should enlarge the value in
     * the linux /proc/sys/net/core/somaxconn file at the same time
     */
    @ConfField public static int mysql_nio_backlog_num = 1024;

    /**
     * The connection timeout and socket timeout config for thrift server
     * The default value for thrift_client_timeout_ms is set to be zero to prevent readtimeout
     *
     */
    @ConfField public static int thrift_client_timeout_ms = 0;

    /**
     * The backlog_num for thrift server
     * When you enlarge this backlog_num, you should ensure it's value larger than
     * the linux /proc/sys/net/core/somaxconn config
     */
    @ConfField public static int thrift_backlog_num = 1024;

    /**
     * FE thrift server port
     */
    @ConfField public static int rpc_port = 9020;

    /**
     * FE mysql server port
     */
    @ConfField public static int query_port = 9030;

    /**
     * num of thread to handle io events in mysql.
     */
    @ConfField public static int mysql_service_io_threads_num = 4;

    /**
     * max num of thread to handle task in mysql.
     */
    @ConfField public static int max_mysql_service_task_threads_num = 4096;

    /**
     * Cluster name will be shown as the title of web page
     */
    @ConfField public static String cluster_name = "Baidu Palo";

    /**
     * node(FE or BE) will be considered belonging to the same Palo cluster if they have same cluster id.
     * Cluster id is usually a random integer generated when master FE start at first time.
     * You can also specify one.
     */
    @ConfField public static int cluster_id = -1;

    /**
     * Cluster token used for internal authentication.
     */
    @ConfField public static String auth_token = "";

    // Configurations for load, clone, create table, alter table etc. We will rarely change them
    /**
     * Maximal waiting time for creating a single replica.
     * eg.
     *      if you create a table with #m tablets and #n replicas for each tablet,
     *      the create table request will run at most (m * n * tablet_create_timeout_second) before timeout.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int tablet_create_timeout_second = 1;

    /**
     * In order not to wait too long for create table(index), set a max timeout.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int max_create_table_timeout_second = 3600;

    /**
     * Maximal waiting time for all publish version tasks of one transaction to be finished
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int publish_version_timeout_second = 30; // 30 seconds

    /**
     * Maximal waiting time for all data inserted before one transaction to be committed
     * This is the timeout second for the command "commit"
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int commit_timeout_second = 30; // 30 seconds

    /**
     * minimal intervals between two publish version action
     */
    @ConfField public static int publish_version_interval_ms = 10;

    /**
     * The thrift server max worker threads
     */
    @ConfField public static int thrift_server_max_worker_threads = 4096;

    /**
     * Maximal wait seconds for straggler node in load
     * eg.
     *      there are 3 replicas A, B, C
     *      load is already quorum finished(A,B) at t1 and C is not finished
     *      if (current_time - t1) > 300s, then palo will treat C as a failure node
     *      will call transaction manager to commit the transaction and tell transaction manager
     *      that C is failed
     *
     * This is also used when waiting for publish tasks
     *
     * TODO this parameter is the default value for all job and the DBA could specify it for separate job
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int load_straggler_wait_second = 300;

    /**
     * The load scheduler running interval.
     * A load job will transfer its state from PENDING to LOADING to FINISHED.
     * The load scheduler will transfer load job from PENDING to LOADING
     *      while the txn callback will transfer load job from LOADING to FINISHED.
     * So a load job will cost at most one interval to finish when the concurrency has not reached the upper limit.
     */
    @ConfField public static int load_checker_interval_second = 5;

    /**
     * The spark load scheduler running interval.
     * Default 60 seconds, because spark load job is heavy and yarn client returns slowly.
     */
    @ConfField public static int spark_load_checker_interval_second = 60;

    /**
     * Concurrency of HIGH priority pending load jobs.
     * Load job priority is defined as HIGH or NORMAL.
     * All mini batch load jobs are HIGH priority, other types of load jobs are NORMAL priority.
     * Priority is set to avoid that a slow load job occupies a thread for a long time.
     * This is just a internal optimized scheduling policy.
     * Currently, you can not specified the job priority manually,
     * and do not change this if you know what you are doing.
     */
    @ConfField public static int load_pending_thread_num_high_priority = 3;
    /**
     * Concurrency of NORMAL priority pending load jobs.
     * Do not change this if you know what you are doing.
     */
    @ConfField public static int load_pending_thread_num_normal_priority = 10;
    /**
     * Concurrency of HIGH priority etl load jobs.
     * Do not change this if you know what you are doing.
     */
    @ConfField public static int load_etl_thread_num_high_priority = 3;
    /**
     * Concurrency of NORMAL priority etl load jobs.
     * Do not change this if you know what you are doing.
     */
    @ConfField public static int load_etl_thread_num_normal_priority = 10;
    /**
     * Concurrency of delete jobs.
     */
    @ConfField public static int delete_thread_num = 10;
    /**
     * Not available.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int load_input_size_limit_gb = 0; // GB, 0 is no limit
    /**
     * Not available.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int load_running_job_num_limit = 0; // 0 is no limit
    /**
     * Default broker load timeout
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int broker_load_default_timeout_second = 14400; // 4 hour

    /**
     * Broker rpc timeout
     */
    @ConfField public static int broker_timeout_ms = 10000; // 10s
    /**
     * Default non-streaming mini load timeout
     */
    @Deprecated
    @ConfField(mutable = true, masterOnly = true)
    public static int mini_load_default_timeout_second = 3600; // 1 hour

    /**
     * Default insert load timeout
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int insert_load_default_timeout_second = 3600; // 1 hour

    /**
     * Default stream load and streaming mini load timeout
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int stream_load_default_timeout_second = 86400 * 3; // 3days

    /**
     * Default stream load pre-commit status timeout
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int stream_load_default_precommit_timeout_second = 3600; // 3600s

    /**
     * Max load timeout applicable to all type of load except for stream load
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int max_load_timeout_second = 259200; // 3days

    /**
     * Max stream load and streaming mini load timeout
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int max_stream_load_timeout_second = 259200; // 3days

    /**
     * Min stream load timeout applicable to all type of load
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int min_load_timeout_second = 1; // 1s

    /**
     * Default hadoop load timeout
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int hadoop_load_default_timeout_second = 86400 * 3; // 3 day

    // Configurations for spark load
    /**
     * Default spark dpp version
     */
    @ConfField
    public static String spark_dpp_version = "1.0.0";
    /**
     * Default spark load timeout
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int spark_load_default_timeout_second = 86400; // 1 day

    /**
     * Default spark home dir
     */
    @ConfField(mutable = true, masterOnly = true)
    public static String spark_home_default_dir = System.getenv("DORIS_HOME") + "/lib/spark2x";

    /**
     * Default spark dependencies path
     */
    @ConfField
    public static String spark_resource_path = "";

    /**
     * The specified spark launcher log dir
     */
    @ConfField
    public static String spark_launcher_log_dir = sys_log_dir + "/spark_launcher_log";

    /**
     * Default yarn client path
     */
    @ConfField
    public static String yarn_client_path = System.getenv("DORIS_HOME") + "/lib/yarn-client/hadoop/bin/yarn";

    /**
     * Default yarn config file directory
     * Each time before running the yarn command, we need to check that the
     * config file exists under this path, and if not, create them.
     */
    @ConfField
    public static String yarn_config_dir = System.getenv("DORIS_HOME") + "/lib/yarn-config";

    /**
     * Maximal intervals between two syncJob's commits.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static long sync_commit_interval_second = 10;

    /**
     * Sync checker's running interval.
     */
    @ConfField public static int sync_checker_interval_second = 5;

    /**
     * max num of thread to handle sync task in sync task thread-pool.
     */
    @ConfField public static int max_sync_task_threads_num = 10;


    /**
     * Min event size that a sync job will commit.
     * When receiving events less than it, SyncJob will continue
     * to wait for the next batch of data until the time exceeds
     * `sync_commit_interval_second`.
     * The default value is 10000 (canal default event buffer size is 16384).
     * You should set it smaller than canal buffer size.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static long min_sync_commit_size = 10000;

    /**
     * Min bytes that a sync job will commit.
     * When receiving bytes less than it, SyncJob will continue
     * to wait for the next batch of data until the time exceeds
     * `sync_commit_interval_second`.
     * The default value is 15 MB (canal default memory is 16 MB).
     * You should set it slightly smaller than canal memory.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static long min_bytes_sync_commit = 15 * 1024 * 1024; // 15 MB

    /**
     * Max bytes that a sync job will commit.
     * When receiving bytes less than it, SyncJob will commit
     * all data immediately.
     * The default value is 64 MB (canal default memory is 16 MB).
     * You should set it larger than canal memory and
     * `min_bytes_sync_commit`.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static long max_bytes_sync_commit = 64 * 1024 * 1024; // 64 MB

    /**
     * Default number of waiting jobs for routine load and version 2 of load
     * This is a desired number.
     * In some situation, such as switch the master, the current number is maybe more than desired_max_waiting_jobs
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int desired_max_waiting_jobs = 100;

    /**
     * fetch stream load record interval.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int fetch_stream_load_record_interval_second = 120;

    /**
     * Default max number of recent stream load record that can be stored in memory.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int max_stream_load_record_size = 5000;

    /**
     * Default max number of recent iceberg database table creation record that can be stored in memory.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int max_iceberg_table_creation_record_size = 2000;

    /**
     * Whether to disable show stream load and clear stream load records in memory.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static boolean disable_show_stream_load = false;

    /**
     * Whether to enable to write single replica for stream load and broker load.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static boolean enable_single_replica_load = false;

    /**
     * maximum concurrent running txn num including prepare, commit txns under a single db
     * txn manager will reject coming txns
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int max_running_txn_num_per_db = 100;

    /**
     * This configuration is just for compatible with old version,
     * this config has been replaced by async_loading_load_task_pool_size,
     * it will be removed in the future.
     */
    @Deprecated
    @ConfField(mutable = false, masterOnly = true)
    public static int async_load_task_pool_size = 10;

    /**
     * The pending_load task executor pool size. This pool size limits the max running pending_load tasks.
     * Currently, it only limits the pending_load task of broker load and spark load.
     * It should be less than 'max_running_txn_num_per_db'
     */
    @ConfField(mutable = false, masterOnly = true)
    public static int async_pending_load_task_pool_size = 10;

    /**
     * The loading_load task executor pool size. This pool size limits the max running loading_load tasks.
     * Currently, it only limits the loading_load task of broker load.
     */
    @ConfField(mutable = false, masterOnly = true)
    public static int async_loading_load_task_pool_size = async_load_task_pool_size;

    /**
     * Same meaning as *tablet_create_timeout_second*, but used when delete a tablet.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int tablet_delete_timeout_second = 2;
    /**
     * the minimal delay seconds between a replica is failed and fe try to recovery it using clone.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int replica_delay_recovery_second = 0;
    /**
     * Balance threshold of data size in BE.
     * The balance algorithm is:
     * 1. Calculate the average used capacity(AUC) of the entire cluster. (total data size / total backends num)
     * 2. The high water level is (AUC * (1 + clone_capacity_balance_threshold))
     * 3. The low water level is (AUC * (1 - clone_capacity_balance_threshold))
     * The Clone checker will try to move replica from high water level BE to low water level BE.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static double clone_capacity_balance_threshold = 0.2;
    /**
     * Balance threshold of num of replicas in Backends.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static double clone_distribution_balance_threshold = 0.2;
    /**
     * The high water of disk capacity used percent.
     * This is used for calculating load score of a backend.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static double capacity_used_percent_high_water = 0.75;
    /**
     * Maximal timeout of ALTER TABLE request. Set long enough to fit your table data size.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int alter_table_timeout_second = 86400 * 30; // 1month
    /**
     * If a backend is down for *max_backend_down_time_second*, a BACKEND_DOWN event will be triggered.
     * Do not set this if you know what you are doing.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int max_backend_down_time_second = 3600; // 1h

    /**
     * If disable_storage_medium_check is true, ReportHandler would not check tablet's storage medium
     * and disable storage cool down function, the default value is false.
     * You can set the value true when you don't care what the storage medium of the tablet is.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static boolean disable_storage_medium_check = false;
    /**
     * When create a table(or partition), you can specify its storage medium(HDD or SSD).
     * If not set, this specifies the default medium when created.
     */
    @ConfField public static String default_storage_medium = "HDD";
    /**
     * After dropping database(table/partition), you can recover it by using RECOVER stmt.
     * And this specifies the maximal data retention time. After time, the data will be deleted permanently.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static long catalog_trash_expire_second = 86400L; // 1day
    /**
     * Maximal bytes that a single broker scanner will read.
     * Do not set this if you know what you are doing.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static long min_bytes_per_broker_scanner = 67108864L; // 64MB
    /**
     * Maximal concurrency of broker scanners.
     * Do not set this if you know what you are doing.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int max_broker_concurrency = 10;

    /**
     * Export checker's running interval.
     */
    @ConfField public static int export_checker_interval_second = 5;
    /**
     * Limitation of the concurrency of running export jobs.
     * Default is 5.
     * 0 is unlimited
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int export_running_job_num_limit = 5;
    /**
     * Default timeout of export jobs.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int export_task_default_timeout_second = 2 * 3600; // 2h
    /**
     * Number of tablets per export query plan
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int export_tablet_num_per_task = 5;

    // Configurations for consistency check
    /**
     * Consistency checker will run from *consistency_check_start_time* to *consistency_check_end_time*.
     * If start time == end time, the checker will stop scheduling.
     * And default is disabled.
     * TODO(cmy): Disable by default because current checksum logic has some bugs.
     * And it will also bring some overhead.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static String consistency_check_start_time = "23";
    @ConfField(mutable = true, masterOnly = true)
    public static String consistency_check_end_time = "23";
    /**
     * Default timeout of a single consistency check task. Set long enough to fit your tablet size.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static long check_consistency_default_timeout_second = 600; // 10 min

    // Configurations for query engine
    /**
     * Maximal number of connections per FE.
     */
    @ConfField public static int qe_max_connection = 1024;

    /**
     * Maximal number of thread in connection-scheduler-pool.
     */
    @ConfField public static int max_connection_scheduler_threads_num = 4096;

    /**
     * The memory_limit for colocote join PlanFragment instance =
     * exec_mem_limit / min (query_colocate_join_memory_limit_penalty_factor, instance_num)
     */
    @ConfField(mutable = true)
    public static int query_colocate_join_memory_limit_penalty_factor = 1;

    /**
     * This configs can set to true to disable the automatic colocate tables's relocate and balance.
     * If 'disable_colocate_balance' is set to true,
     *   ColocateTableBalancer will not relocate and balance colocate tables.
     * Attention:
     *   Under normal circumstances, there is no need to turn off balance at all.
     *   Because once the balance is turned off, the unstable colocate table may not be restored
     *   Eventually the colocate plan cannot be used when querying.
     */
    @ConfField(mutable = true, masterOnly = true) public static boolean disable_colocate_balance = false;

    /**
     * The default user resource publishing timeout.
     */
    @ConfField public static int meta_publish_timeout_ms = 1000;
    @ConfField public static boolean proxy_auth_enable = false;
    @ConfField public static String proxy_auth_magic_prefix = "x@8";
    /**
     * Limit on the number of expr children of an expr tree.
     * Exceed this limit may cause long analysis time while holding database read lock.
     * Do not set this if you know what you are doing.
     */
    @ConfField(mutable = true)
    public static int expr_children_limit = 10000;
    /**
     * Limit on the depth of an expr tree.
     * Exceed this limit may cause long analysis time while holding db read lock.
     * Do not set this if you know what you are doing.
     */
    @ConfField(mutable = true)
    public static int expr_depth_limit = 3000;

    // Configurations for backup and restore
    /**
     * Plugins' path for BACKUP and RESTORE operations. Currently deprecated.
     */
    @Deprecated
    @ConfField public static String backup_plugin_path = "/tools/trans_file_tool/trans_files.sh";

    // Configurations for hadoop dpp
    /**
     * The following configurations are not available.
     */
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

    // For forward compatibility, will be removed later.
    // check token when download image file.
    @ConfField public static boolean enable_token_check = true;

    /**
     * Set to true if you deploy Palo using thirdparty deploy manager
     * Valid options are:
     *      disable:    no deploy manager
     *      k8s:        Kubernetes
     *      ambari:     Ambari
     *      local:      Local File (for test or Boxer2 BCC version)
     */
    @ConfField public static String enable_deploy_manager = "disable";

    // If use k8s deploy manager locally, set this to true and prepare the certs files
    @ConfField public static boolean with_k8s_certs = false;

    // Set runtime locale when exec some cmds
    @ConfField public static String locale = "zh_CN.UTF-8";

    // default timeout of backup job
    @ConfField(mutable = true, masterOnly = true)
    public static int backup_job_default_timeout_ms = 86400 * 1000; // 1 day

    /**
     * 'storage_high_watermark_usage_percent' limit the max capacity usage percent of a Backend storage path.
     * 'storage_min_left_capacity_bytes' limit the minimum left capacity of a Backend storage path.
     * If both limitations are reached, this storage path can not be chose as tablet balance destination.
     * But for tablet recovery, we may exceed these limit for keeping data integrity as much as possible.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int storage_high_watermark_usage_percent = 85;
    @ConfField(mutable = true, masterOnly = true)
    public static long storage_min_left_capacity_bytes = 2 * 1024 * 1024 * 1024L; // 2G

    /**
     * If capacity of disk reach the 'storage_flood_stage_usage_percent' and 'storage_flood_stage_left_capacity_bytes',
     * the following operation will be rejected:
     * 1. load job
     * 2. restore job
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int storage_flood_stage_usage_percent = 95;
    @ConfField(mutable = true, masterOnly = true)
    public static long storage_flood_stage_left_capacity_bytes = 1 * 1024 * 1024 * 1024; // 100MB

    // update interval of tablet stat
    // All frontends will get tablet stat from all backends at each interval
    @ConfField public static int tablet_stat_update_interval_second = 60;  // 1 min

    /**
     * Max bytes a broker scanner can process in one broker load job.
     * Commonly, each Backends has one broker scanner.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static long max_bytes_per_broker_scanner = 3 * 1024 * 1024 * 1024L; // 3G

    /**
     * Max number of load jobs, include PENDING、ETL、LOADING、QUORUM_FINISHED.
     * If exceed this number, load job is not allowed to be submitted.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static long max_unfinished_load_job = 1000;

    /**
     * If set to true, Planner will try to select replica of tablet on same host as this Frontend.
     * This may reduce network transmission in following case:
     * 1. N hosts with N Backends and N Frontends deployed.
     * 2. The data has N replicas.
     * 3. High concurrency queries are sent to all Frontends evenly
     * In this case, all Frontends can only use local replicas to do the query.
     * If you want to allow fallback to nonlocal replicas when no local replicas available,
     * set enable_local_replica_selection_fallback to true.
     */
    @ConfField(mutable = true)
    public static boolean enable_local_replica_selection = false;

    /**
     * Used with enable_local_replica_selection.
     * If the local replicas is not available, fallback to the nonlocal replicas.
     * */
    @ConfField(mutable = true)
    public static boolean enable_local_replica_selection_fallback = false;

    /**
     * The number of query retries.
     * A query may retry if we encounter RPC exception and no result has been sent to user.
     * You may reduce this number to avoid Avalanche disaster.
     */
    @ConfField(mutable = true)
    public static int max_query_retry_time = 1;

    /**
     * The tryLock timeout configuration of catalog lock.
     * Normally it does not need to change, unless you need to test something.
     */
    @ConfField(mutable = true)
    public static long catalog_try_lock_timeout_ms = 5000; // 5 sec

    /**
     * if this is set to true
     *    all pending load job will failed when call begin txn api
     *    all prepare load job will failed when call commit txn api
     *    all committed load job will waiting to be published
     */
    @ConfField(mutable = true, masterOnly = true)
    public static boolean disable_load_job = false;

    /*
     * One master daemon thread will update database used data quota for db txn manager
     * every db_used_data_quota_update_interval_secs
     */
    @ConfField(mutable = false, masterOnly = true)
    public static int db_used_data_quota_update_interval_secs = 300;

    /**
     * Load using hadoop cluster will be deprecated in future.
     * Set to true to disable this kind of load.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static boolean disable_hadoop_load = false;

    /**
     * fe will call es api to get es index shard info every es_state_sync_interval_secs
     */
    @ConfField
    public static long es_state_sync_interval_second = 10;

    /**
     * fe will create iceberg table every iceberg_table_creation_interval_second
     */
    @ConfField(mutable = true, masterOnly = true)
    public static long iceberg_table_creation_interval_second = 10;

    /**
     * the factor of delay time before deciding to repair tablet.
     * if priority is VERY_HIGH, repair it immediately.
     * HIGH, delay tablet_repair_delay_factor_second * 1;
     * NORMAL: delay tablet_repair_delay_factor_second * 2;
     * LOW: delay tablet_repair_delay_factor_second * 3;
     */
    @ConfField(mutable = true, masterOnly = true)
    public static long tablet_repair_delay_factor_second = 60;

    /**
     * the default slot number per path in tablet scheduler
     * TODO(cmy): remove this config and dynamically adjust it by clone task statistic
     */
    @ConfField public static int schedule_slot_num_per_path = 2;

    /**
     * Deprecated after 0.10
     */
    @ConfField public static boolean use_new_tablet_scheduler = true;

    /**
     * the threshold of cluster balance score, if a backend's load score is 10% lower than average score,
     * this backend will be marked as LOW load, if load score is 10% higher than average score, HIGH load
     * will be marked.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static double balance_load_score_threshold = 0.1; // 10%

    /**
     * if set to true, TabletScheduler will not do balance.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static boolean disable_balance = false;

    /**
     * if set to true, TabletScheduler will not do disk balance.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static boolean disable_disk_balance = false;

    // if the number of scheduled tablets in TabletScheduler exceed max_scheduling_tablets
    // skip checking.
    @ConfField(mutable = true, masterOnly = true)
    public static int max_scheduling_tablets = 2000;

    // if the number of balancing tablets in TabletScheduler exceed max_balancing_tablets,
    // no more balance check
    @ConfField(mutable = true, masterOnly = true)
    public static int max_balancing_tablets = 100;

    // Rebalancer type(ignore case): BeLoad, Partition. If type parse failed, use BeLoad as default.
    @ConfField(masterOnly = true)
    public static String tablet_rebalancer_type = "BeLoad";

    // Valid only if use PartitionRebalancer. If this changed, cached moves will be cleared.
    @ConfField(mutable = true, masterOnly = true)
    public static long partition_rebalance_move_expire_after_access = 600; // 600s

    // Valid only if use PartitionRebalancer
    @ConfField(mutable = true, masterOnly = true)
    public static int partition_rebalance_max_moves_num_per_selection = 10;

    // 1 slot for reduce unnecessary balance task, provided a more accurate estimate of capacity
    @ConfField(masterOnly = true, mutable = true)
    public static int balance_slot_num_per_path = 1;

    // This threshold is to avoid piling up too many report task in FE, which may cause OOM exception.
    // In some large Doris cluster, eg: 100 Backends with ten million replicas, a tablet report may cost
    // several seconds after some modification of metadata(drop partition, etc..).
    // And one Backend will report tablets info every 1 min, so unlimited receiving reports is unacceptable.
    // TODO(cmy): we will optimize the processing speed of tablet report in future, but now, just discard
    // the report if queue size exceeding limit.
    // Some online time cost:
    // 1. disk report: 0-1 ms
    // 2. task report: 0-1 ms
    // 3. tablet report
    //      10000 replicas: 200ms
    @ConfField(mutable = true, masterOnly = true)
    public static int report_queue_size = 100;

    /**
     * If set to true, metric collector will be run as a daemon timer to collect metrics at fix interval
     */
    @ConfField public static boolean enable_metric_calculator = true;

    /**
     * the max routine load job num, including NEED_SCHEDULED, RUNNING, PAUSE
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int max_routine_load_job_num = 100;

    /**
     * the max concurrent routine load task num of a single routine load job
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int max_routine_load_task_concurrent_num = 5;

    /**
     * the max concurrent routine load task num per BE.
     * This is to limit the num of routine load tasks sending to a BE, and it should also less
     * than BE config 'routine_load_thread_pool_size'(default 10),
     * which is the routine load task thread pool size on BE.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int max_routine_load_task_num_per_be = 5;

    /**
     * The max number of files store in SmallFileMgr
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int max_small_file_number = 100;

    /**
     * The max size of a single file store in SmallFileMgr
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int max_small_file_size_bytes = 1024 * 1024; // 1MB

    /**
     * Save small files
     */
    @ConfField
    public static String small_file_dir = System.getenv("DORIS_HOME") + "/small_files";

    /**
     * If set to true, the insert stmt with processing error will still return a label to user.
     * And user can use this label to check the load job's status.
     * The default value is false, which means if insert operation encounter errors,
     * exception will be thrown to user client directly without load label.
     */
    @ConfField(mutable = true, masterOnly = true) public static boolean using_old_load_usage_pattern = false;

    /**
     * This will limit the max recursion depth of hash distribution pruner.
     * eg: where a in (5 elements) and b in (4 elements) and c in (3 elements) and d in (2 elements).
     * a/b/c/d are distribution columns, so the recursion depth will be 5 * 4 * 3 * 2 = 120, larger than 100,
     * So that distribution pruner will no work and just return all buckets.
     *
     * Increase the depth can support distribution pruning for more elements, but may cost more CPU.
     */
    @ConfField(mutable = true, masterOnly = false)
    public static int max_distribution_pruner_recursion_depth = 100;

    /**
     * If the jvm memory used percent(heap or old mem pool) exceed this threshold, checkpoint thread will
     * not work to avoid OOM.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static long metadata_checkpoint_memory_threshold = 70;

    /**
     * If set to true, the checkpoint thread will make the checkpoint regardless of the jvm memory used percent.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static boolean force_do_metadata_checkpoint = false;

    /**
     * The multi cluster feature will be deprecated in version 0.12
     * set this config to true will disable all operations related to cluster feature, include:
     *   create/drop cluster
     *   add free backend/add backend to cluster/decommission cluster balance
     *   change the backends num of cluster
     *   link/migration db
     */
    @ConfField(mutable = true)
    public static boolean disable_cluster_feature = true;

    /**
     * Decide how often to check dynamic partition
     */
    @ConfField(mutable = true, masterOnly = true)
    public static long dynamic_partition_check_interval_seconds = 600;

    /**
     * If set to true, dynamic partition feature will open
     */
    @ConfField(mutable = true, masterOnly = true)
    public static boolean dynamic_partition_enable = true;

    /**
     * control rollup job concurrent limit
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int max_running_rollup_job_num_per_table = 1;

    /**
     * If set to true, Doris will check if the compiled and running versions of Java are compatible
     */
    @ConfField
    public static boolean check_java_version = true;

    /**
     * it can't auto-resume routine load job as long as one of the backends is down
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int max_tolerable_backend_down_num = 0;

    /**
     * a period for auto resume routine load
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int period_of_auto_resume_min = 5;

    /**
     * If set to true, the backend will be automatically dropped after finishing decommission.
     * If set to false, the backend will not be dropped and remaining in DECOMMISSION state.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static boolean drop_backend_after_decommission = true;

    /**
     * When tablet size of decommissioned backend is lower than this threshold,
     * SystemHandler will start to check if all tablets of this backend are in recycled status,
     * this backend will be dropped immediately if the check result is true.
     * For performance based considerations, better not set a very high value for this.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int decommission_tablet_check_threshold = 5000;

    /**
     * Define thrift server's server model, default is TThreadPoolServer model
     */
    @ConfField
    public static String thrift_server_type = "THREAD_POOL";

    /**
     * This config will decide whether to resend agent task when create_time for agent_task is set,
     * only when current_time - create_time > agent_task_resend_wait_time_ms can ReportHandler do resend agent task
     */
    @ConfField (mutable = true, masterOnly = true)
    public static long agent_task_resend_wait_time_ms = 5000;

    /**
     * min_clone_task_timeout_sec and max_clone_task_timeout_sec is to limit the
     * min and max timeout of a clone task.
     * Under normal circumstances, the timeout of a clone task is estimated by
     * the amount of data and the minimum transmission speed(5MB/s).
     * But in special cases, you may need to manually set these two configs
     * to ensure that the clone task will not fail due to timeout.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static long min_clone_task_timeout_sec = 3 * 60; // 3min
    @ConfField(mutable = true, masterOnly = true)
    public static long max_clone_task_timeout_sec = 2 * 60 * 60; // 2h

    /**
     * If set to true, fe will enable sql result cache
     * This option is suitable for offline data update scenarios
     *                              case1   case2   case3   case4
     * enable_sql_cache             false   true    true    false
     * enable_partition_cache       false   false   true    true
     */
    @ConfField(mutable = true, masterOnly = false)
    public static boolean cache_enable_sql_mode = true;

    /**
     * If set to true, fe will get data from be cache,
     * This option is suitable for real-time updating of partial partitions.
     */
    @ConfField(mutable = true, masterOnly = false)
    public static boolean cache_enable_partition_mode = true;

    /**
     *  Minimum interval between last version when caching results,
     *  This parameter distinguishes between offline and real-time updates
     */
    @ConfField(mutable = true, masterOnly = false)
    public static int cache_last_version_interval_second = 900;

    /**
     * Set the maximum number of rows that can be cached
     */
    @ConfField(mutable = true, masterOnly = false)
    public static int cache_result_max_row_count = 3000;

    /**
     * Used to limit element num of InPredicate in delete statement.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int max_allowed_in_element_num_of_delete = 1024;

    /**
     * In some cases, some tablets may have all replicas damaged or lost.
     * At this time, the data has been lost, and the damaged tablets
     * will cause the entire query to fail, and the remaining healthy tablets cannot be queried.
     * In this case, you can set this configuration to true.
     * The system will replace damaged tablets with empty tablets to ensure that the query
     * can be executed. (but at this time the data has been lost, so the query results may be inaccurate)
     */
    @ConfField(mutable = true, masterOnly = true)
    public static boolean recover_with_empty_tablet = false;

    /**
     * In some scenarios, there is an unrecoverable metadata problem in the cluster,
     * and the visibleVersion of the data does not match be. In this case, it is still
     * necessary to restore the remaining data (which may cause problems with the correctness of the data).
     * This configuration is the same as` recover_with_empty_tablet` should only be used in emergency situations
     * This configuration has three values:
     *   disable : If an exception occurs, an error will be reported normally.
     *   ignore_version: ignore the visibleVersion information recorded in fe partition, use replica version
     *   ignore_all: In addition to ignore_version, when encountering no queryable replica,
     *   skip it directly instead of throwing an exception
     */
    @ConfField(mutable = true, masterOnly = true)
    public static String recover_with_skip_missing_version = "disable";

    /**
     * Whether to add a delete sign column when create unique table
     */
    @ConfField(mutable = true, masterOnly = true)
    public static boolean enable_batch_delete_by_default = true;

    /**
     * Whether to add a version column when create unique table
     */
    @ConfField(mutable = true, masterOnly = true)
    public static boolean enable_hidden_version_column_by_default = true;

    /**
     * Used to set default db data quota bytes.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static long default_db_data_quota_bytes = 1024L * 1024 * 1024 * 1024 * 1024L; // 1PB

    /**
     * Used to set default db replica quota num.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static long default_db_replica_quota_size = 1024 * 1024 * 1024;

    /*
     * Maximum percentage of data that can be filtered (due to reasons such as data is irregularly)
     * The default value is 0.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static double default_max_filter_ratio = 0;

    /**
     * HTTP Server V2 is implemented by SpringBoot.
     * It uses an architecture that separates front and back ends.
     * Only enable httpv2 can user to use the new Frontend UI interface
     */
    @ConfField
    public static boolean enable_http_server_v2 = true;

    /*
     * Base path is the URL prefix for all API paths.
     * Some deployment environments need to configure additional base path to match resources.
     * This Api will return the path configured in Config.http_api_extra_base_path.
     * Default is empty, which means not set.
     */
    @ConfField
    public static String http_api_extra_base_path = "";

    /**
     * If set to true, FE will be started in BDBJE debug mode
     */
    @ConfField
    public static boolean enable_bdbje_debug_mode = false;

    /**
     * This config is used to try skip broker when access bos or other cloud storage via broker
     */
    @ConfField(mutable = true, masterOnly = true)
    public static boolean enable_access_file_without_broker = false;

    /**
     * Whether to allow the outfile function to export the results to the local disk.
     */
    @ConfField
    public static boolean enable_outfile_to_local = false;

    /**
     * Used to set the initial flow window size of the GRPC client channel, and also used to max message size.
     * When the result set is large, you may need to increase this value.
     */
    @ConfField
    public static int grpc_max_message_size_bytes = 2147483647; // 2GB

    /**
     * Used to set minimal number of replication per tablet.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static short min_replication_num_per_tablet = 1;

    /**
     * Used to set maximal number of replication per tablet.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static short max_replication_num_per_tablet = Short.MAX_VALUE;

    /**
     * Used to limit the maximum number of partitions that can be created when creating a dynamic partition table,
     * to avoid creating too many partitions at one time.
     * The number is determined by "start" and "end" in the dynamic partition parameters.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int max_dynamic_partition_num = 500;

    /**
     * Used to limit the maximum number of partitions that can be created when creating multi partition,
     * to avoid creating too many partitions at one time.
     * The number is determined by "start" and "end" in the multi partition parameters.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int max_multi_partition_num = 4096;

    /**
     * Control the max num of backup/restore job per db
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int max_backup_restore_job_num_per_db = 10;

    /**
     * Control the default max num of the instance for a user.
     */
    @ConfField(mutable = true)
    public static int default_max_query_instances = -1;

    /*
     * One master daemon thread will update global partition in memory
     * info every partition_in_memory_update_interval_secs
     */
    @ConfField(mutable = false, masterOnly = true)
    public static int partition_in_memory_update_interval_secs = 300;

    @ConfField(masterOnly = true)
    public static boolean enable_concurrent_update = false;

    /**
     * This configuration can only be configured during cluster initialization and cannot be modified during cluster
     * restart and upgrade after initialization is complete.
     *
     * 0: table names are stored as specified and comparisons are case sensitive.
     * 1: table names are stored in lowercase and comparisons are not case sensitive.
     * 2: table names are stored as given but compared in lowercase.
     */
    @ConfField(masterOnly = true)
    public static int lower_case_table_names = 0;

    @ConfField(mutable = true, masterOnly = true)
    public static int table_name_length_limit = 64;

    /*
     * The job scheduling interval of the schema change handler.
     * The user should not set this parameter.
     * This parameter is currently only used in the regression test environment to appropriately
     * reduce the running speed of the schema change job to test the correctness of the system
     * in the case of multiple tasks in parallel.
     */
    @ConfField(mutable = false, masterOnly = true)
    public static int default_schema_change_scheduler_interval_millisecond = 500;

    /*
     * If set to true, the thrift structure of query plan will be sent to BE in compact mode.
     * This will significantly reduce the size of rpc data, which can reduce the chance of rpc timeout.
     * But this may slightly decrease the concurrency of queries, because compress and decompress cost more CPU.
     */
    @ConfField(mutable = true, masterOnly = false)
    public static boolean use_compact_thrift_rpc = true;

    /*
     * If set to true, the tablet scheduler will not work, so that all tablet repair/balance task will not work.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static boolean disable_tablet_scheduler = false;

    /*
     * When doing clone or repair tablet task, there may be replica is REDUNDANT state, which
     * should be dropped later. But there are be loading task on these replicas, so the default strategy
     * is to wait until the loading task finished before dropping them.
     * But the default strategy may takes very long time to handle these redundant replicas.
     * So we can set this config to true to not wait any loading task.
     * Set this config to true may cause loading task failed, but will
     * speed up the process of tablet balance and repair.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static boolean enable_force_drop_redundant_replica = false;

    /*
     * auto set the slowest compaction replica's status to bad
     */
    @ConfField(mutable = true, masterOnly = true)
    public static boolean repair_slow_replica = false;

    /*
     * The relocation of a colocation group may involve a large number of tablets moving within the cluster.
     * Therefore, we should use a more conservative strategy to avoid relocation
     * of colocation groups as much as possible.
     * Reloaction usually occurs after a BE node goes offline or goes down.
     * This parameter is used to delay the determination of BE node unavailability.
     * The default is 30 minutes, i.e., if a BE node recovers within 30 minutes, relocation of the colocation group
     * will not be triggered.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static long colocate_group_relocate_delay_second = 1800; // 30 min

    /*
     * If set to true, when creating table, Doris will allow to locate replicas of a tablet
     * on same host.
     * This is only for local test, so that we can deploy multi BE on same host and create table
     * with multi replicas.
     * DO NOT use it for production env.
     */
    @ConfField
    public static boolean allow_replica_on_same_host = false;

    /**
     *  The version count threshold used to judge whether replica compaction is too slow
     */
    @ConfField(mutable = true)
    public static int min_version_count_indicate_replica_compaction_too_slow = 200;

    /**
     * The valid ratio threshold of the difference between the version count of the slowest replica and the fastest
     * replica. If repair_slow_replica is set to true, it is used to determine whether to repair the slowest replica
     */
    @ConfField(mutable = true, masterOnly = true)
    public static double valid_version_count_delta_ratio_between_replicas = 0.5;

    /**
     * The data size threshold used to judge whether replica is too large
     */
    @ConfField(mutable = true, masterOnly = true)
    public static long min_bytes_indicate_replica_too_large = 2 * 1024 * 1024 * 1024L;

    /**
     * If set to TRUE, the column definitions of iceberg table and the doris table must be consistent
     * If set to FALSE, Doris only creates columns of supported data types.
     * Default is true.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static boolean iceberg_table_creation_strict_mode = true;

    // statistics
    /*
     * the max unfinished statistics job number
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int cbo_max_statistics_job_num = 20;
    /*
     * the max timeout of a statistics task
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int max_cbo_statistics_task_timeout_sec = 300;
    /*
     * the concurrency of statistics task
     */
    // TODO change it to mutable true
    @ConfField(mutable = false, masterOnly = true)
    public static int cbo_concurrency_statistics_task_num = 10;
    /*
     * default sample percentage
     * The value from 0 ~ 100. The 100 means no sampling and fetch all data.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int cbo_default_sample_percentage = 10;

    /**
     * If this configuration is enabled, you should also specify the trace_export_url.
     */
    @ConfField(mutable = false, masterOnly = false)
    public static boolean enable_tracing = false;

    /**
     * Current support for exporting traces:
     *   zipkin: Export traces directly to zipkin, which is used to enable the tracing feature quickly.
     *   collector: The collector can be used to receive and process traces and support export to a variety of
     *     third-party systems.
     * If this configuration is enabled, you should also specify the enable_tracing=true and trace_export_url.
     */
    @ConfField(mutable = false, masterOnly = false)
    public static String trace_exporter = "zipkin";

    /**
     * The endpoint to export spans to.
     * export to zipkin like: http://127.0.0.1:9411/api/v2/spans
     * export to collector like: http://127.0.0.1:4318/v1/traces
     */
    @ConfField(mutable = false, masterOnly = false)
    public static String trace_export_url = "http://127.0.0.1:9411/api/v2/spans";

    /**
     * If set to TRUE, the compaction slower replica will be skipped when select get queryable replicas
     * Default is true.
     */
    @ConfField(mutable = true)
    public static boolean skip_compaction_slower_replica = true;

    /**
     * Enable quantile_state type column
     * Default is false.
     * */
    @ConfField(mutable = true, masterOnly = true)
    public static boolean enable_quantile_state_type = false;

    @ConfField
    public static boolean enable_vectorized_load = true;

    @ConfField(mutable = false, masterOnly = true)
    public static int backend_rpc_timeout_ms = 60000; // 1 min

    @ConfField(mutable = true, masterOnly = false)
    public static long file_scan_node_split_size = 256 * 1024 * 1024; // 256mb

    @ConfField(mutable = true, masterOnly = false)
    public static long file_scan_node_split_num = 128;

    /**
     * If set to TRUE, FE will:
     * 1. divide BE into high load and low load(no mid load) to force triggering tablet scheduling;
     * 2. ignore whether the cluster can be more balanced during tablet scheduling;
     *
     * It's used to test the reliability in single replica case when tablet scheduling are frequent.
     * Default is false.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static boolean be_rebalancer_fuzzy_test = false;

    /**
     * If set to TRUE, FE will convert date/datetime to datev2/datetimev2(0) automatically.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static boolean enable_date_conversion = false;

    @ConfField(mutable = false, masterOnly = true)
    public static boolean enable_multi_tags = false;

    /**
     * If set to TRUE, FE will convert DecimalV2 to DecimalV3 automatically.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static boolean enable_decimal_conversion = false;

    /**
     * List of S3 API compatible object storage systems.
     */
    @ConfField
    public static String s3_compatible_object_storages = "s3,oss,cos,bos";

    /**
     * Support complex data type ARRAY.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static boolean enable_array_type = false;

    /**
     * Use new fe generate es dsl.
     */
    @ConfField(mutable = true)
    public static boolean enable_new_es_dsl = true;

    /**
     * The timeout of executing async remote fragment.
     * In normal case, the async remote fragment will be executed in a short time. If system are under high load
     * condition，try to set this timeout longer.
     */
    @ConfField(mutable = true)
    public static long remote_fragment_exec_timeout_ms = 5000; // 5 sec

    /**
     * Temp config, should be removed when new file scan node is ready.
     */
    @ConfField(mutable = true)
    public static boolean enable_new_load_scan_node = true;

    /**
     * Max data version of backends serialize block.
     */
    @ConfField(mutable = false)
    public static int max_be_exec_version = 1;

    /**
     * Min data version of backends serialize block.
     */
    @ConfField(mutable = false)
    public static int min_be_exec_version = 0;

    /**
     * Data version of backends serialize block.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int be_exec_version = max_be_exec_version;

    @ConfField(mutable = false)
    public static int statistic_job_scheduler_execution_interval_ms = 1000;

    @ConfField(mutable = false)
    public static int statistic_task_scheduler_execution_interval_ms = 1000;

    /*
     * mtmv scheduler framework is still under dev, remove this config when it is graduate.
     */
    @ConfField(mutable = true)
    public static boolean enable_mtmv_scheduler_framework = false;

    /* Max running task num at the same time, otherwise the submitted task will still be keep in pending poll*/
    @ConfField(mutable = true, masterOnly = true)
    public static int max_running_mtmv_scheduler_task_num = 100;

    /* Max pending task num keep in pending poll, otherwise it reject the task submit*/
    @ConfField(mutable = true, masterOnly = true)
    public static int max_pending_mtmv_scheduler_task_num = 100;

    /* Remove the completed mtmv job after this expired time. */
    @ConfField(mutable = true, masterOnly = true)
    public static long scheduler_mtmv_job_expired = 24 * 60 * 60L; // 1day

    /* Remove the finished mtmv task after this expired time. */
    @ConfField(mutable = true, masterOnly = true)
    public static long scheduler_mtmv_task_expired = 24 * 60 * 60L; // 1day

    /**
     * If set to true, query on external table will prefer to assign to compute node.
     * And the max number of compute node is controlled by min_backend_num_for_external_table.
     * If set to false, query on external table will assign to any node.
     */
    @ConfField(mutable = true, masterOnly = false)
    public static boolean prefer_compute_node_for_external_table = false;

    /**
     * Only take effect when prefer_compute_node_for_external_table is true.
     * If the compute node number is less than this value, query on external table will try to get some mix node
     * to assign, to let the total number of node reach this value.
     * If the compute node number is larger than this value, query on external table will assign to compute node only.
     */
    @ConfField(mutable = true, masterOnly = false)
    public static int min_backend_num_for_external_table = 3;

    /**
     * Max query profile num.
     */
    @ConfField(mutable = true, masterOnly = false)
    public static int max_query_profile_num = 100;

    /**
     * Set to true to disable backend black list, so that even if we failed to send task to a backend,
     * that backend won't be added to black list.
     * This should only be set when running tests, such as regression test.
     * Highly recommended NOT disable it in product environment.
     */
    @ConfField(mutable = true, masterOnly = false)
    public static boolean disable_backend_black_list = false;

    /**
     * Maximum backend heartbeat failure tolerance count.
     * Default is 1, which means if 1 heart failed, the backend will be marked as dead.
     * A larger value can improve the tolerance of the cluster to occasional heartbeat failures.
     * For example, when running regression tests, this value can be increased.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static long max_backend_heartbeat_failure_tolerance_count = 1;

    /**
     * The iceberg and hudi table will be removed in v1.3
     * Use multi catalog instead.
     */
    @ConfField(mutable = true, masterOnly = false)
    public static boolean disable_iceberg_hudi_table = true;

    /**
     * The default connection timeout for hive metastore.
     * hive.metastore.client.socket.timeout
     */
    @ConfField(mutable = true, masterOnly = false)
    public static long hive_metastore_client_timeout_second = 10;

    /**
     * Whether to load default config files when creating hive metastore client.
     */
    @ConfField(mutable = true, masterOnly = false)
    public static boolean load_default_conf_for_hms_client = true;

    /**
     * Used to determined how many statistics collection SQL could run simultaneously.
     */
    @ConfField
    public static int statistics_simultaneously_running_task_num = 10;

    /**
     * Internal table replica num, once set, user should promise the avaible BE is greater than this value,
     * otherwise the statistics related internal table creation would be failed.
     */
    @ConfField
    public static int statistic_internal_table_replica_num = 1;

    /**
     * if table has too many replicas, Fe occur oom when schema change.
     * 10W replicas is a reasonable value for testing.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static long max_replica_count_when_schema_change = 100000;

    /**
     * Max cache num of hive partition.
     * Decrease this value if FE's memory is small
     */
    @ConfField(mutable = false, masterOnly = false)
    public static long max_hive_partition_cache_num = 100000;

    @ConfField(mutable = false, masterOnly = false)
    public static long max_hive_table_catch_num = 1000;

    @ConfField(mutable = false, masterOnly = false)
    public static short max_hive_list_partition_num = -1;

    /**
     * Max cache loader thread-pool size.
     * Max thread pool size for loading external meta cache
     */
    @ConfField(mutable = false, masterOnly = false)
    public static int max_external_cache_loader_thread_pool_size = 10;

    /**
     * Max cache num of external catalog's file
     * Decrease this value if FE's memory is small
     */
    @ConfField(mutable = false, masterOnly = false)
    public static long max_external_file_cache_num = 100000;

    /**
     * Max cache num of external table's schema
     * Decrease this value if FE's memory is small
     */
    @ConfField(mutable = false, masterOnly = false)
    public static long max_external_schema_cache_num = 10000;

    /**
     * The expiration time of a cache object after last access of it.
     * For external schema cache and hive meta cache.
     */
    @ConfField(mutable = false, masterOnly = false)
    public static long external_cache_expire_time_minutes_after_access = 24 * 60; // 1 day

    /**
     * Set session variables randomly to check more issues in github workflow
     */
    @ConfField(mutable = true, masterOnly = false)
    public static boolean use_fuzzy_session_variable = false;

    /**
     * Collect external table statistic info by running sql when set to true.
     * Otherwise, use external catalog metadata.
     */
    @ConfField(mutable = true)
    public static boolean collect_external_table_stats_by_sql = false;

    /**
     * Max num of same name meta informatntion in catalog recycle bin.
     * Default is 3.
     * 0 means do not keep any meta obj with same name.
     * < 0 means no limit
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int max_same_name_catalog_trash_num = 3;

    /**
     * The storage policy is still under developement.
     * Disable it by default.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static boolean enable_storage_policy = false;

    /**
     * Only for branch-1.2
     * Set to true to disable the session variable: enable_vectorized_engine.
     * And the vec engine will be used by default, no matter the value of enable_vectorized_engine.
     */
    @ConfField(mutable = true, masterOnly = false)
    public static boolean disable_enable_vectorized_engine = true;

    /**
     * This is used whether to push down function to MYSQL in external Table with query sql
     * like odbc, jdbc for mysql table
     */
    @ConfField(mutable = true)
    public static boolean enable_func_pushdown = true;

    /**
     * If set to true, doris will try to parse the ddl of a hive view and try to execute the query
     * otherwise it will throw an AnalysisException.
     */
    @ConfField(mutable = true)
    public static boolean enable_query_hive_views = false;

    /**
     * If set to true, doris will automatically synchronize hms metadata to the cache in fe.
     */
    @ConfField(masterOnly = true)
    public static boolean enable_hms_events_incremental_sync = false;

    /**
     * Maximum number of events to poll in each RPC.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int hms_events_batch_size_per_rpc = 500;

    /**
     * HMS polling interval in milliseconds.
     */
    @ConfField(masterOnly = true)
    public static int hms_events_polling_interval_ms = 10000;

    /**
     * Maximum number of error tablets showed in broker load
     */
    @ConfField(masterOnly = true, mutable = true)
    public static int max_error_tablet_of_broker_load = 3;

    /**
     * If set to ture, doris will establish an encrypted channel based on the SSL protocol with mysql.
     */
    @ConfField(mutable = false, masterOnly = false)
    public static boolean enable_ssl = false;

    /**
     * Default certificate file location for mysql ssl connection.
     */
    @ConfField(mutable = false, masterOnly = false)
    public static String mysql_ssl_default_certificate = System.getenv("DORIS_HOME")
            + "/mysql_ssl_default_certificate/certificate.p12";

    /**
     * Password for default certificate file.
     */
    @ConfField(mutable = false, masterOnly = false)
    public static String mysql_ssl_default_certificate_password = "doris";

    /**
     * If false, when select from tables in information_schema database,
     * the result will not contain the information of the table in external catalog.
     * This is to avoid query time when external catalog is not reachable.
     * TODO: this is a temp solution, we should support external catalog in the future.
     */
    @ConfField(mutable = true)
    public static boolean infodb_support_ext_catalog = false;

    @ConfField(mutable = true)
    public static boolean use_mysql_bigint_for_largeint = false;

    /** 
    * the max package size fe thrift server can receive,avoid accepting error or too large package causing OOM,default 20M
    */
    @ConfField
    public static int fe_thrift_max_pkg_bytes = 20000000;
}

