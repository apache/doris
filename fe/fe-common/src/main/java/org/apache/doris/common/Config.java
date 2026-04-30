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

import java.io.File;

public class Config extends ConfigBase {

    @ConfField(description = {"The path of the user-defined configuration file, used to store fe_custom.conf. "
            + "Configurations in this file will override those in fe.conf"})
    public static String custom_config_dir = EnvUtils.getDorisHome() + "/conf";

    @ConfField(description = {
            "The maximum file size of fe.log and fe.audit.log. Once this size is exceeded, the log file will be split"})
    public static int log_roll_size_mb = 1024; // 1 GB

    /**
     * sys_log_dir:
     *      This specifies FE log dir. FE will produces 2 log files:
     *      fe.log:      all logs of FE process.
     *      fe.warn.log  all WARN and ERROR log of FE process.
     *
     * sys_log_level:
     *      INFO, WARN, ERROR, FATAL
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
     *
     * sys_log_enable_compress:
     *      default is false. if true, will compress fe.log & fe.warn.log by gzip
     */
    @Deprecated // use env var LOG_DIR instead
    @ConfField(description = {"The path of the FE log file, used to store fe.log"})
    public static String sys_log_dir = "";

    @ConfField(description = {"The level of FE log"}, options = {"INFO", "WARN", "ERROR", "FATAL"})
    public static String sys_log_level = "INFO";

    @ConfField(description = {
            "The output mode of the FE log. "
                    + "NORMAL mode is synchronous output with location information; "
            + "ASYNC mode is the default mode, asynchronous output with location information; "
            + "BRIEF mode is asynchronous output without location information. "
            + "Performance improves in the order: NORMAL, ASYNC, BRIEF"},
            options = {"NORMAL", "ASYNC", "BRIEF"})
    public static String sys_log_mode = "ASYNC";

    @ConfField(description = {
            "The maximum number of FE log files that can be retained within the "
                    + "sys_log_roll_interval (log roll interval). The default value is 10, which means the system "
                    + "will keep up to 10 log files during each log roll interval."})
    public static int sys_log_roll_num = 10;

    @ConfField(description = {"Verbose modules. VERBOSE level logging is implemented by the DEBUG level of log4j. "
            + "If set to `org.apache.doris.catalog`, "
            + "DEBUG logs of classes under this package will be printed."})
    public static String[] sys_log_verbose_modules = {};
    @ConfField(description = {"The split cycle of the FE log file"}, options = {"DAY", "HOUR"})
    public static String sys_log_roll_interval = "DAY";
    @ConfField(description = {
            "The maximum retention time of the FE log file. After this time, the log file will be deleted. "
                    + "Supported formats include: 7d, 10h, 60m, 120s"})
    public static String sys_log_delete_age = "7d";
    @ConfField(description = {"Whether to enable compression for FE log files"})
    public static boolean sys_log_enable_compress = false;

    @ConfField(description = {"The path of the FE audit log file, used to store fe.audit.log"})
    public static String audit_log_dir = System.getenv("LOG_DIR");
    @ConfField(description = {"The maximum number of FE audit log files. "
            + "After exceeding this number, the oldest log file will be deleted"})
    public static int audit_log_roll_num = 90;
    @ConfField(description = {"The type of FE audit log file"},
            options = {"slow_query", "query", "load", "stream_load"})
    public static String[] audit_log_modules = {"slow_query", "query", "load", "stream_load"};
    @ConfField(mutable = true, description = {"The threshold of slow query, in milliseconds. "
            + "If the response time of a query exceeds this threshold, it will be recorded in audit log."})
    public static long qe_slow_log_ms = 5000;
    @ConfField(mutable = true, description = {"The threshold of sql_digest generation, in milliseconds. "
            + "If the response time of a query exceeds this threshold, "
            + "sql_digest will be generated for it."})
    public static long sql_digest_generation_threshold_ms = 5000;
    @ConfField(description = {"The split cycle of the FE audit log file"},
            options = {"DAY", "HOUR"})
    public static String audit_log_roll_interval = "DAY";
    @ConfField(description = {"The maximum retention time of the FE audit log file. "
            + "After this time, the log file will be deleted. "
            + "Supported formats include: 7d, 10h, 60m, 120s"})
    public static String audit_log_delete_age = "30d";
    @ConfField(description = {"Whether to enable compression for FE audit log files"})
    public static boolean audit_log_enable_compress = false;

    @ConfField(description = {"Active lineage plugins. Specify the name returned by LineagePlugin.name()"})
    public static String[] activate_lineage_plugin = {};

    @ConfField(description = {"Whether to use a file to record logs. When starting FE with --console, "
            + "all logs will be written to both standard output and file. "
            + "Disabling this option will stop writing logs to files."})
    public static boolean enable_file_logger = true;

    @ConfField(mutable = false, masterOnly = false,
            description = {"Whether to check for table lock leaks"})
    public static boolean check_table_lock_leaky = false;

    @ConfField(mutable = true, masterOnly = false,
            description = {"PreparedStatement stmtId starting position, used for testing only"})
    public static long prepared_stmt_start_id = -1;

    @ConfField(description = {"The installation directory of the plugin"})
    public static String plugin_dir =  EnvUtils.getDorisHome() + "/plugins";

    @ConfField(mutable = true, masterOnly = true, description = {"Whether to enable the plugin"})
    public static boolean plugin_enable = true;

    @ConfField(description = {"The path to save JDBC drivers. When creating a JDBC Catalog, "
            + "if the specified driver file path is not an absolute path, Doris will look for jars in this path"})
    public static String jdbc_drivers_dir = EnvUtils.getDorisHome() + "/plugins/jdbc_drivers";

    @ConfField(description = {"The safe path of the JDBC driver. When creating a JDBC Catalog, "
            + "you can configure multiple files or network paths that are allowed to be used, "
            + "separated by semicolons. "
            + "The default is * to allow all; if set to empty, it also means to allow all"})
    public static String jdbc_driver_secure_path = "*";

    @ConfField(description = {"Functions that MySQL JDBC Catalog does not support pushing down"})
    public static String[] jdbc_mysql_unsupported_pushdown_functions = {"date_trunc", "money_format", "negative"};

    @ConfField(mutable = true, description = {
            "MySQL compatibility variable whitelist. These variables will be silently ignored in SET statements "
                    + "instead of throwing an error. This is mainly used for compatibility with MySQL client tools "
                    + "(such as phpMyAdmin, mysqldump). Doris does not need to understand the specific meaning of "
                    + "these variables, it just needs to accept them without error."})
    public static String[] mysql_compat_var_whitelist = {};

    @ConfField(mutable = true, masterOnly = true, description = {"Force SQLServer Jdbc Catalog encrypt to false"})
    public static boolean force_sqlserver_jdbc_encrypt_false = false;

    @ConfField(mutable = true, masterOnly = true, description = {
            "The default parallelism of the load execution plan on a single node when the broker load is submitted"})
    public static int default_load_parallelism = 8;

    @ConfField(mutable = true, masterOnly = true, description = {
            "Labels of finished or cancelled load jobs will be removed after this time. "
                    + "The removed labels can be reused."})
    public static int label_keep_max_second = 3 * 24 * 3600; // 3 days

    @ConfField(mutable = true, masterOnly = true, description = {
            "For some high-frequency load jobs such as INSERT, STREAMING LOAD, ROUTINE_LOAD_TASK, and DELETE, "
                    + "remove the finished job or task if expired. The removed labels can be reused."})
    public static int streaming_label_keep_max_second = 43200; // 12 hour

    @ConfField(mutable = true, masterOnly = true, description = {
            "For ALTER and EXPORT jobs, remove the finished job if expired."})
    public static int history_job_keep_max_second = 7 * 24 * 3600; // 7 days

    @ConfField(mutable = true, masterOnly = true, description = {
            "For EXPORT jobs, if the number of EXPORT jobs in the system exceeds this value, "
                    + "the oldest records will be deleted."})
    public static int max_export_history_job_num = 1000;

    @ConfField(description = {"The cleanup interval for transactions, in seconds. "
            + "In each cycle, expired historical transactions will be cleaned up"})
    public static int transaction_clean_interval_second = 30;

    @ConfField(description = {"The cleanup interval for load jobs, in seconds. "
            + "In each cycle, expired historical load jobs will be cleaned up"})
    public static int label_clean_interval_second = 1 * 3600; // 1 hours

    @ConfField(mutable = true, masterOnly = true, description = {"Time interval for cleaning up discarded temporary "
            + "partitions after an Insert Overwrite task fails, in milliseconds"})
    public static int overwrite_clean_interval_ms = 10000;

    @ConfField(description = {"The directory to save Doris meta data"})
    public static String meta_dir =  EnvUtils.getDorisHome() + "/doris-meta";

    @ConfField(description = {"The directory to save Doris temp data"})
    public static String tmp_dir =  EnvUtils.getDorisHome() + "/temp_dir";

    @ConfField(description = {"The storage type of the metadata log. BDB: Logs are stored in BDBJE. "
            + "LOCAL: logs are stored in a local file (for testing only)"}, options = {"BDB", "LOCAL"})
    public static String edit_log_type = "bdb";

    @ConfField(description = {"The port of BDBJE"})
    public static int edit_log_port = 9010;

    @ConfField(mutable = true, masterOnly = true, description = {
            "The log roll size of BDBJE. When the number of log entries exceeds this value, the log will be rolled"})
    public static int edit_log_roll_num = 50000;

    @ConfField(mutable = true, masterOnly = true, description = {"The max number of log entries for batching BDBJE"})
    public static int batch_edit_log_max_item_num = 100;

    @ConfField(mutable = true, masterOnly = true, description = {"The max size for batching BDBJE"})
    public static long batch_edit_log_max_byte_size = 640 * 1024L;

    @ConfField(mutable = true, masterOnly = true, description = {
            "The sleep time after writing multiple batching BDBJE entries continuously"})
    public static long batch_edit_log_rest_time_ms = 10;

    @ConfField(mutable = true, masterOnly = true, description = {
            "After writing multiple batching BDBJE entries continuously, a short rest is needed. "
                    + "This indicates the write count before a rest"})
    public static long batch_edit_log_continuous_count_for_rest = 1000;

    @ConfField(description = {"Batch EditLog writing"})
    public static boolean enable_batch_editlog = true;

    @ConfField(description = {"The tolerated delay time of metadata synchronization, in seconds. "
            + "If the metadata delay exceeds this value, non-master FE will stop offering service"})
    public static int meta_delay_toleration_second = 300;    // 5 min

    @ConfField(description = {"The sync policy of meta data log. If you only deploy one Follower FE, "
            + "set this to `SYNC`. If you deploy more than 3 Follower FE, "
            + "you can set this and the following `replica_sync_policy` to `WRITE_NO_SYNC`. "
            + "See: http://docs.oracle.com/cd/E17277_02/html/java/com/sleepycat/je/Durability.SyncPolicy.html"},
            options = {"SYNC", "NO_SYNC", "WRITE_NO_SYNC"})
    public static String master_sync_policy = "SYNC"; // SYNC, NO_SYNC, WRITE_NO_SYNC

    @ConfField(description = {"Same as `master_sync_policy`"},
            options = {"SYNC", "NO_SYNC", "WRITE_NO_SYNC"})
    public static String replica_sync_policy = "SYNC"; // SYNC, NO_SYNC, WRITE_NO_SYNC

    @ConfField(description = {"The replica ack policy of bdbje. "
            + "See: http://docs.oracle.com/cd/E17277_02/html/java/com/sleepycat/je/Durability.ReplicaAckPolicy.html"},
            options = {"ALL", "NONE", "SIMPLE_MAJORITY"})
    public static String replica_ack_policy = "SIMPLE_MAJORITY"; // ALL, NONE, SIMPLE_MAJORITY

    @ConfField(description = {"The heartbeat timeout of BDBJE between master and follower, in seconds. "
            + "The default is 30 seconds, which is the same as the default value in BDBJE. "
            + "If the network is experiencing transient problems, "
            + "or some unexpected long Java GC is bothering you, "
            + "you can try to increase this value to decrease the chances of false timeouts"})
    public static int bdbje_heartbeat_timeout_second = 30;

    @ConfField(description = {"The lock timeout of bdbje operation, in seconds. "
            + "If there are many LockTimeoutException in FE WARN log, you can try to increase this value"})
    public static int bdbje_lock_timeout_second = 5;

    @ConfField(description = {"The replica ack timeout of bdbje between master and follower, in seconds. "
            + "If there are many ReplicaWriteException in FE WARN log, you can try to increase this value"})
    public static int bdbje_replica_ack_timeout_second = 10;

    @ConfField(description = {"The desired upper limit on the number of bytes of reserved space to retain "
            + "in a replicated JE Environment. "
            + "This parameter is ignored in a non-replicated JE Environment."})
    public static long bdbje_reserved_disk_bytes = 1 * 1024 * 1024 * 1024; // 1G

    @ConfField(description = {"Amount of free disk space required by BDBJE. "
            + "If the free disk space is less than this value, BDBJE will not be able to write."})
    public static long bdbje_free_disk_bytes = 1 * 1024 * 1024 * 1024; // 1G

    @ConfField(description = {"Amount of memory used by BDBJE as cache."})
    public static long bdbje_cache_size_bytes = 10 * 1024 * 1024; // 10 MB

    @ConfField(description = {"Maximum message size of BDBJE."})
    public static long bdbje_max_message_size_bytes = Integer.MAX_VALUE; // 2 GB

    @ConfField(masterOnly = true, description = {"Number of threads to handle heartbeat events"})
    public static int heartbeat_mgr_threads_num = 8;

    @ConfField(masterOnly = true, description = {"Queue size to store heartbeat tasks in heartbeat_mgr"})
    public static int heartbeat_mgr_blocking_queue_size = 1024;

    @ConfField(masterOnly = true, description = {"Number of threads to update tablet statistics"})
    public static int tablet_stat_mgr_threads_num = -1;

    @ConfField(masterOnly = true, description = {
            "Number of threads to handle agent tasks in the agent task thread pool."})
    public static int max_agent_task_threads_num = 4096;

    @ConfField(description = {
            "The maximum number of transactions that BDBJE can roll back when trying to rejoin the group. "
            + "If the number of transactions to roll back is larger than this value, "
            + "BDBJE will not be able to rejoin the group, and you need to clean up BDBJE data manually."})
    public static int txn_rollback_limit = 100;

    @ConfField(description = {"The preferred network address. If FE has multiple network addresses, "
            + "this configuration can be used to specify the preferred network address. "
            + "This is a semicolon-separated list, "
            + "each element is a CIDR representation of the network address"})
    public static String priority_networks = "";

    @ConfField(mutable = true, description = {
            "If true, non-master FE will ignore the metadata delay gap between Master FE and itself, "
                    + "even if the metadata delay gap exceeds the threshold. "
                    + "Non-master FE will still offer read service. "
                    + "This is helpful when you need to stop the Master FE for a relatively long time for some reason, "
                    + "but still want the non-master FE to offer read service."})
    public static boolean ignore_meta_check = false;

    @ConfField(description = {"The maximum clock skew between non-master FE to Master FE host, in milliseconds. "
            + "This value is checked whenever a non-master FE establishes a connection to master FE via BDBJE. "
            + "The connection is abandoned if the clock skew is larger than this value."})
    public static long max_bdbje_clock_delta_ms = 5000; // 5s

    @ConfField(mutable = true, description = {
            "Whether to enable authentication for all HTTP interfaces"}, varType = VariableAnnotation.EXPERIMENTAL)
    public static boolean enable_all_http_auth = false;

    @ConfField(description = {"FE HTTP port. Currently, all FEs' HTTP port must be the same"})
    public static int http_port = 8030;

    @ConfField(description = {"FE HTTPS port. Currently, all FEs' HTTPS port must be the same"})
    public static int https_port = 8050;

    @ConfField(description = {"The key store path of FE https service"})
    public static String key_store_path =  EnvUtils.getDorisHome()
            + "/conf/ssl/doris_ssl_certificate.keystore";

    @ConfField(description = {"The key store password of FE https service"})
    public static String key_store_password = "";

    @ConfField(description = {"The key store type of FE https service"})
    public static String key_store_type = "JKS";

    @ConfField(description = {"The key store alias of FE https service"})
    public static String key_store_alias = "doris_ssl_certificate";

    @ConfField(description = {"Whether to enable https, if enabled, http port will not be available"},
            varType = VariableAnnotation.EXPERIMENTAL)
    public static boolean enable_https = false;

    @ConfField(description = {
            "The number of acceptor threads for Jetty. Jetty's thread architecture model is very simple, "
                    + "divided into three thread pools: acceptor, selector and worker. "
                    + "The acceptor is responsible for accepting new connections, "
                    + "and then handing it over to the selector to process the unpacking of the HTTP message protocol, "
                    + "and finally the worker processes the request. "
                    + "The first two thread pools adopt a non-blocking model, "
                    + "and one thread can handle many socket reads and writes, "
                    + "so the number of thread pools is small. For most projects, "
                    + "only 1-2 acceptor threads are needed, 2 to 4 should be enough. "
                    + "The number of workers depends on the ratio of QPS and IO events of the application. "
                    + "The higher the QPS, or the higher the IO ratio, the more threads are waiting, "
                    + "and the more threads are required."})
    public static int jetty_server_acceptors = 2;
    @ConfField(description = {"The number of selector threads for Jetty."})
    public static int jetty_server_selectors = 4;
    @ConfField(description = {"The number of worker threads for Jetty. 0 means using the default thread pool."})
    public static int jetty_server_workers = 0;

    @ConfField(description = {"The default minimum number of threads for jetty."})
    public static int jetty_threadPool_minThreads = 20;
    @ConfField(description = {"The default maximum number of threads for jetty."})
    public static int jetty_threadPool_maxThreads = 400;

    @ConfField(description = {"The maximum HTTP POST size of Jetty, in bytes, the default value is 100MB."})
    public static int jetty_server_max_http_post_size = 100 * 1024 * 1024;

    @ConfField(description = {"The maximum HTTP header size of Jetty, in bytes, the default value is 1MB."})
    public static int jetty_server_max_http_header_size = 1048576;

    @ConfField(description = {"Whether to disable mini load, disabled by default"})
    public static boolean disable_mini_load = true;

    @ConfField(description = {"The backlog number of the MySQL NIO server. "
            + "If you increase this value, you should also increase the value in "
            + "`/proc/sys/net/core/somaxconn` at the same time"})
    public static int mysql_nio_backlog_num = 1024;

    @ConfField(description = {"Whether to enable TCP Keep-Alive for MySQL connections, disabled by default"})
    public static boolean mysql_nio_enable_keep_alive = false;

    @ConfField(description = {"The connection timeout of thrift client, in milliseconds. 0 means no timeout."})
    public static int thrift_client_timeout_ms = 0;

    @ConfField(mutable = true, masterOnly = false,
            description = {"Thrift RPC 连接阶段的超时时间（毫秒），包括 TCP connect 和可能的 TLS 握手。"
                    + "用于防止 reopen() 时因网络异常长时间阻塞。0 表示不设置。",
                    "Timeout in milliseconds for the connect phase of Thrift RPC connections, "
                    + "including TCP connect and potential TLS handshake. "
                    + "Prevents long blocking during reopen() when network is unreachable. "
                    + "0 means no timeout."})
    public static int thrift_rpc_connect_timeout_ms = 10000;

    // The default value is inherited from org.apache.thrift.TConfiguration
    @ConfField(description = {"The maximum size of a received message of the Thrift server, in bytes"})
    public static int thrift_max_message_size = 100 * 1024 * 1024;

    // The default value is inherited from org.apache.thrift.TConfiguration
    @ConfField(description = {"The size limit of one frame for the Thrift server transport"})
    public static int thrift_max_frame_size = 16384000;

    @ConfField(description = {"The backlog number of the Thrift server. "
            + "If you increase this value, you should also increase the value in "
            + "`/proc/sys/net/core/somaxconn` at the same time"})
    public static int thrift_backlog_num = 1024;

    @ConfField(description = {"The port of FE thrift server"})
    public static int rpc_port = 9020;

    @ConfField(description = {"The port of FE MySQL server"})
    public static int query_port = 9030;

    @ConfField(description = {"The port of FE Arrow-Flight-SQL server"})
    public static int arrow_flight_sql_port = 8070;

    @ConfField(description = {"The number of IO threads in the MySQL service"})
    public static int mysql_service_io_threads_num = 4;

    @ConfField(description = {"The maximum number of task threads in the MySQL service"})
    public static int max_mysql_service_task_threads_num = 4096;

    @ConfField(description = {"BackendServiceProxy pool size for pooling GRPC channels."})
    public static int backend_proxy_num = 48;

    @ConfField(description = {
            "Cluster ID used for internal authentication. Usually a random integer generated when the master FE "
                    + "starts for the first time. You can also specify one."})
    public static int cluster_id = -1;

    @ConfField(description = {"Cluster token used for internal authentication."})
    public static String auth_token = "";

    @ConfField(mutable = true, masterOnly = true,
            description = {"Maximal waiting time for creating a single replica, in seconds. "
                    + "eg. if you create a table with #m tablets and #n replicas for each tablet, "
                    + "the create table request will run at most "
                    + "(m * n * tablet_create_timeout_second) before timeout"})
    public static int tablet_create_timeout_second = 2;

    @ConfField(mutable = true, masterOnly = true, description = {
            "Minimal waiting time for creating a table, in seconds."})
    public static int min_create_table_timeout_second = 30;

    @ConfField(mutable = true, masterOnly = true, description = {
            "Maximal waiting time for creating a table, in seconds."})
    public static int max_create_table_timeout_second = 3600;

    @ConfField(mutable = true, masterOnly = true, description = {
            "Maximal waiting time for all publish version tasks of one transaction to be finished, in seconds."})
    public static int publish_version_timeout_second = 30; // 30 seconds

    @ConfField(mutable = true, masterOnly = true, description = {
            "Waiting time for a transaction to reach \"at least one replica success\", in seconds. "
                    + "If time exceeds this and each tablet has at least one replica published successfully, "
                    + "then the load task will be considered successful."})
    public static int publish_wait_time_second = 300;

    @ConfField(mutable = true, masterOnly = true, description = {
            "Check the replicas that are undergoing schema change when publishing a transaction. "
                    + "Do not turn off this check "
                    + "under normal circumstances. It only temporarily skips the check if "
                    + "publish version and schema change encounter a deadlock"})
    public static boolean publish_version_check_alter_replica = true;

    @ConfField(mutable = true, masterOnly = true, description = {
            "Log printing interval for failed publish transactions, in seconds"})
    public static long publish_fail_log_interval_second = 5 * 60;

    @ConfField(mutable = true, masterOnly = true, description = {
            "The upper limit of failure logs for PUBLISH_VERSION tasks"})
    public static long publish_version_task_failed_log_threshold = 80;

    @ConfField(masterOnly = true, description = {"Number of threads to handle publish tasks"})
    public static int publish_thread_pool_num = 128;

    @ConfField(masterOnly = true, description = {"Queue size to store publish tasks in the publish thread pool"})
    public static int publish_queue_size = 128;

    @ConfField(mutable = true, description = {"Whether to enable parallel publish version"})
    public static boolean enable_parallel_publish_version = true;


    @ConfField(masterOnly = true, description = {"Number of threads to handle tablet report tasks"})
    public static int tablet_report_thread_pool_num = 10;

    @ConfField(masterOnly = true, description = {
            "Queue size to store tablet report tasks in the tablet report thread pool."})
    public static int tablet_report_queue_size = 1024;

    @ConfField(mutable = true, masterOnly = true, description = {
            "Maximal waiting time for all data inserted before one transaction to be committed, in seconds. "
                    + "This parameter is only used for transactional insert operation"})
    public static int commit_timeout_second = 30; // 30 seconds

    @ConfField(masterOnly = true, description = {"The interval of the publish task trigger thread, in milliseconds"})
    public static int publish_version_interval_ms = 10;

    @ConfField(mutable = true, masterOnly = true, description = {
            "If the number of publishing transactions of a table exceeds this value, new transactions will "
                    + "be rejected. Set to -1 to disable this limit."})
    public static long max_publishing_txn_num_per_table = 500;

    @ConfField(description = {"The maximum number of worker threads of the Thrift server"})
    public static int thrift_server_max_worker_threads = 4096;

    @ConfField(mutable = true, masterOnly = true, description = {"Maximal timeout for delete job, in seconds."})
    public static int delete_job_max_timeout_second = 300;

    @ConfField(mutable = true, masterOnly = true, description = {
            "Minimum number of successfully written replicas for a load job."})
    public static short min_load_replica_num = -1;

    @ConfField(description = {"The interval of the load job scheduler, in seconds."})
    public static int load_checker_interval_second = 5;

    @ConfField(description = {"The interval of the ingestion load job scheduler, in seconds."})
    public static int ingestion_load_checker_interval_second = 60;

    @ConfField(mutable = true, masterOnly = true, description = {"Default timeout for broker load jobs, in seconds."})
    public static int broker_load_default_timeout_second = 14400; // 4 hour

    @ConfField(description = {"The timeout of RPC between FE and Broker, in milliseconds"})
    public static int broker_timeout_ms = 10000; // 10s

    @ConfField(description = {"The timeout of RPC for high-concurrency short-circuit queries"})
    public static int point_query_timeout_ms = 10000; // 10s

    @ConfField(mutable = true, masterOnly = true, description = {"Default timeout for insert load jobs, in seconds."})
    public static int insert_load_default_timeout_second = 14400; // 4 hour

    @ConfField(mutable = true, masterOnly = true, description = {
            "Randomly set ORDER BY keys for MOW tables for testing."})
    public static boolean random_add_order_by_keys_for_mow = false;

    @ConfField(mutable = true, masterOnly = true, description = {
            "Randomly use V3 storage_format (ext_meta) for some tables in fuzzy tests to increase coverage"})
    public static boolean random_use_v3_storage_format = true;

    @ConfField(mutable = true, masterOnly = true, description = {
            "The stale threshold of checkpoint image file in cloud mode (in seconds). "
                    + "If the image file is older than this threshold, a new checkpoint will be triggered "
                    + "even if there are no new journals. This helps keep table version, partition version, "
                    + "and tablet stats in the image up-to-date. If the value is less than or equal to 0, "
                    + "this feature is disabled."})
    public static long cloud_checkpoint_image_stale_threshold_seconds = 3600;

    @ConfField(mutable = true, masterOnly = true, description = {
            "Wait for the internal batch to be written before returning; "
                    + "insert into and stream load use group commit by default."})
    public static boolean wait_internal_group_commit_finish = false;

    @ConfField(mutable = false, masterOnly = true, description = {"Default commit interval in ms for group commit"})
    public static int group_commit_interval_ms_default_value = 10000;

    @ConfField(mutable = false, masterOnly = true, description = {"Default commit data bytes for group commit"})
    public static int group_commit_data_bytes_default_value = 134217728;

    @ConfField(mutable = true, masterOnly = true, description = {
            "The internal group commit timeout is a multiple of the table's group_commit_interval_ms"})
    public static int group_commit_timeout_multipler = 10;

    @ConfField(mutable = true, masterOnly = true, description = {"Default timeout for stream load jobs, in seconds."})
    public static int stream_load_default_timeout_second = 86400 * 3; // 3days

    @ConfField(mutable = true, masterOnly = true, description = {
            "Default pre-commit timeout for stream load jobs, in seconds."})
    public static int stream_load_default_precommit_timeout_second = 3600; // 3600s

    @ConfField(mutable = true, masterOnly = true, description = {
            "Whether to enable memtable on sink node by default in stream load"})
    public static boolean stream_load_default_memtable_on_sink_node = false;

    @ConfField(mutable = true, masterOnly = true, description = {"Maximum timeout for load jobs, in seconds."})
    public static int max_load_timeout_second = 259200; // 3days

    @ConfField(mutable = true, masterOnly = true, description = {"Maximum timeout for stream load jobs, in seconds."})
    public static int max_stream_load_timeout_second = 259200; // 3days

    @ConfField(mutable = true, masterOnly = true, description = {"Minimum timeout for load jobs, in seconds."})
    public static int min_load_timeout_second = 1; // 1s

    @ConfField(mutable = true, masterOnly = true, description = {
            "Default timeout for ingestion load jobs, in seconds."})
    public static int ingestion_load_default_timeout_second = 86400; // 1 day

    @ConfField(mutable = true, masterOnly = true, description = {
            "Maximum number of waiting jobs for Broker Load. This is a desired number. "
                    + "In some situations, such as switching the master, "
                    + "the current number may exceed this value."})
    public static int desired_max_waiting_jobs = 100;

    @ConfField(mutable = true, masterOnly = true, description = {
            "The interval at which FE fetches stream load records from BE."})
    public static int fetch_stream_load_record_interval_second = 120;

    @ConfField(mutable = true, masterOnly = true, description = {
            "Default maximum number of recent stream load records that can be stored in memory."})
    public static int max_stream_load_record_size = 5000;

    @ConfField(mutable = true, masterOnly = true, description = {
            "Whether to disable show stream load and clear stream load records in memory."})
    public static boolean disable_show_stream_load = false;

    @ConfField(mutable = true, description = {"Whether to enable stream load profile"})
    public static boolean enable_stream_load_profile = false;

    @ConfField(mutable = true, masterOnly = true, description = {
            "Whether to enable writing to a single replica for stream load and broker load."},
            varType = VariableAnnotation.EXPERIMENTAL)
    public static boolean enable_single_replica_load = false;

    @ConfField(mutable = true, masterOnly = true, description = {
            "Shuffle will not be enabled for DUPLICATE KEY tables if their tablet count is lower than this number"},
            varType = VariableAnnotation.EXPERIMENTAL)
    public static int min_tablets_for_dup_table_shuffle = 64;

    @ConfField(mutable = true, masterOnly = true, description = {
            "Maximum number of concurrently running transactions, including prepare and commit transactions, "
                    + "under a single database.",
            "The transaction manager will reject incoming transactions once this limit is reached."})
    public static int max_running_txn_num_per_db = 10000;

    @ConfField(mutable = true, masterOnly = true, description = {
            "Whether to move transaction edit log writes outside the write lock to reduce lock contention. "
                    + "When enabled, edit log entries are enqueued inside the write lock (FIFO preserves ordering) "
                    + "and awaited outside the lock, reducing write lock hold time "
                    + "and improving concurrent transaction throughput. "
                    + "Default is true. Set to false to use the traditional in-lock synchronous write mode."})
    public static boolean enable_txn_log_outside_lock = true;

    @ConfField(mutable = true, description = {
            "Whether to enable per-transaction parallel publish. When enabled, different transactions "
                    + "in the same database can finish publishing in parallel across executor threads, "
                    + "instead of being serialized per database. "
                    + "When disabled, falls back to per-database routing (old behavior) "
                    + "where transactions within a DB are published sequentially."})
    public static boolean enable_per_txn_publish = true;

    @ConfField(masterOnly = true, description = {"The pending load task executor pool size. "
            + "This pool size limits the maximum number of running pending load tasks.",
            "Currently, it only limits the pending load tasks of broker load and ingestion load.",
            "It should be less than `max_running_txn_num_per_db`"})
    public static int async_pending_load_task_pool_size = 10;

    @ConfField(masterOnly = true, description = {"The loading load task executor pool size. "
            + "This pool size limits the maximum number of running loading load tasks.",
            "Currently, it only limits the loading load tasks of broker load."})
    public static int async_loading_load_task_pool_size = 10;

    @ConfField(mutable = true, masterOnly = true, description = {
            "The same meaning as `tablet_create_timeout_second`, but used when deleting a tablet."})
    public static int tablet_delete_timeout_second = 2;

    @ConfField(mutable = true, masterOnly = true, description = {
            "The high watermark of disk capacity usage percent. "
                    + "This is used for calculating the load score of a backend."})
    public static double capacity_used_percent_high_water = 0.75;

    @ConfField(mutable = true, masterOnly = true, description = {
            "The maximum difference in disk capacity usage percent between BEs. "
                    + "It is used for calculating the load score of a backend."})
    public static double used_capacity_percent_max_diff = 0.30;

    @ConfField(mutable = true, masterOnly = true, description = {
            "Sets a fixed disk usage factor in the BE load fraction. The BE load score is a combination of disk usage "
                    + "and replica count. The valid value range is [0, 1]. When it is out of this range, other "
                    + "methods are used to automatically calculate this coefficient."})
    public static double backend_load_capacity_coeficient = -1.0;

    @ConfField(mutable = true, masterOnly = true, description = {
            "Maximum timeout for ALTER TABLE requests. Set this long enough to accommodate your table data size."})
    public static int alter_table_timeout_second = 86400 * 30; // 1month

    @ConfField(mutable = true, masterOnly = true, description = {
            "When disable_storage_medium_check is true, ReportHandler will not check the tablet's storage medium "
                    + "and will disable the storage cooldown function."})
    public static boolean disable_storage_medium_check = false;

    @ConfField(description = {"When creating a table (or partition), you can specify its storage medium (HDD or SSD)."})
    public static String default_storage_medium = "HDD";

    @ConfField(mutable = true, masterOnly = true, description = {
            "After dropping a database (table/partition), you can recover it by using the RECOVER statement.",
            "This specifies the maximum data retention time. After this time, the data will be deleted permanently."})
    public static long catalog_trash_expire_second = 86400L; // 1day

    @ConfField
    public static boolean catalog_trash_ignore_min_erase_latency = false;

    @ConfField(mutable = true, masterOnly = true, description = {
            "Minimum bytes that a single broker scanner will read. When splitting files in broker load, "
                    + "if the size of a split file is less than this value, it will not be split."})
    public static long min_bytes_per_broker_scanner = 67108864L; // 64MB

    @ConfField(mutable = true, masterOnly = true, description = {"Maximal concurrency of broker scanners."})
    public static int max_broker_concurrency = 100;

    // TODO(cmy): Disable by default because current checksum logic has some bugs.
    @ConfField(mutable = true, masterOnly = true, description = {
            "Start time of consistency check. Used with `consistency_check_end_time` "
                    + "to decide the start and end time of consistency check. "
                    + "If set to the same value, consistency check will not be scheduled."})
    public static String consistency_check_start_time = "23";
    @ConfField(mutable = true, masterOnly = true, description = {
            "End time of consistency check. Used with `consistency_check_start_time` "
                    + "to decide the start and end time of consistency check. "
                    + "If set to the same value, consistency check will not be scheduled."})
    public static String consistency_check_end_time = "23";

    @ConfField(mutable = true, masterOnly = true, description = {
            "Default timeout of a single consistency check task. Set long enough to fit your tablet size."})
    public static long check_consistency_default_timeout_second = 600; // 10 min

    @ConfField(description = {"Maximum number of MySQL server connections per FE."})
    public static int qe_max_connection = 1024;

    @ConfField(mutable = true, description = {"Colocate join PlanFragment instance memory limit penalty factor.",
            "The memory_limit for colocate join PlanFragment instance = "
                    + "`exec_mem_limit / min (query_colocate_join_memory_limit_penalty_factor, instance_num)`"})
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

    @ConfField(mutable = true, masterOnly = true, description = {
            "Whether to allow colocate balance between all groups."})
    public static boolean disable_colocate_balance_between_groups = false;

    /**
     * The default user resource publishing timeout.
     */
    @Deprecated
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

    // For forward compatibility, will be removed later.
    // check token when download image file.
    @ConfField public static boolean enable_token_check = true;

    /**
     * Set to true if you deploy Palo using thirdparty deploy manager
     * Valid options are:
     *      disable:    no deploy manager
     *      k8s:        Kubernetes NB:Support removed starting from version 3.1.X
     *      ambari:     Ambari NB: Support removed starting from version 3.1.X
     *      local:      Local File (for test or Boxer2 BCC version)
     */
    @ConfField public static String enable_deploy_manager = "disable";

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
    public static long storage_flood_stage_left_capacity_bytes = 1 * 1024 * 1024 * 1024; // 1GB

    // update interval of tablet stat
    // All frontends will get tablet stat from all backends at each interval
    @ConfField(mutable = true)
    public static int tablet_stat_update_interval_second = 60;  // 1 min

    // update interval of alive session
    // Only master FE collect this info from all frontends at each interval
    @ConfField public static int alive_session_update_interval_second = 5;

    @ConfField public static int fe_session_mgr_threads_num = 1;

    @ConfField public static int fe_session_mgr_blocking_queue_size = 1024;

    @ConfField(mutable = true, masterOnly = true)
    public static int loss_conn_fe_temp_table_keep_second = 60;

    /**
     * Max bytes a broker scanner can process in one broker load job.
     * Commonly, each Backends has one broker scanner.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static long max_bytes_per_broker_scanner = 500 * 1024 * 1024 * 1024L; // 500G

    /**
     * Max number of load jobs, include PENDING、ETL、LOADING、QUORUM_FINISHED.
     * If exceed this number, load job is not allowed to be submitted.
     */
    @Deprecated
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
    public static int max_query_retry_time = 3;

    /**
     * The number of point query retries in executor.
     * A query may retry if we encounter RPC exception and no result has been sent to user.
     * You may reduce this number to avoid Avalanche disaster.
     */
    @ConfField(mutable = true)
    public static int max_point_query_retry_time = 2;

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
     * fe will call es api to get es index shard info every es_state_sync_interval_secs
     */
    @ConfField
    public static long es_state_sync_interval_second = 10;

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
     * clone a tablet, further repair timeout.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static long tablet_further_repair_timeout_second = 20 * 60;

    /**
     * clone a tablet, further repair max times.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int tablet_further_repair_max_times = 5;

    /**
     * if tablet loaded txn failed recently, it will get higher priority to repair.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static long tablet_recent_load_failed_second = 30 * 60;

    /**
     * base time for higher tablet scheduler task,
     * set this config value bigger if want the high priority effect last longer.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static long tablet_schedule_high_priority_second = 30 * 60;

    /**
     * publish version queue's size in be, report it to fe,
     * if publish task in be exceed direct_publish_limit_number,
     * fe will direct publish task
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int publish_version_queued_limit_number = 1000;

    /**
     * the default slot number per path for hdd in tablet scheduler
     * TODO(cmy): remove this config and dynamically adjust it by clone task statistic
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int schedule_slot_num_per_hdd_path = 4;


    /**
     * the default slot number per path for ssd in tablet scheduler
     * TODO(cmy): remove this config and dynamically adjust it by clone task statistic
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int schedule_slot_num_per_ssd_path = 8;

    /**
     * the default batch size in tablet scheduler for a single schedule.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int schedule_batch_size = 50;

    /**
     * tablet health check interval. Do not modify it in production environment.
     */
    @ConfField(mutable = false, masterOnly = true)
    public static long tablet_checker_interval_ms = 20 * 1000;

    /**
     * tablet scheduled interval. Do not modify it in production environment.
     */
    @ConfField(mutable = false, masterOnly = true)
    public static long tablet_schedule_interval_ms = 1000;

    /**
     * Deprecated after 0.10
     */
    @Deprecated
    @ConfField public static boolean use_new_tablet_scheduler = true;

    /**
     * the threshold of cluster balance score, if a backend's load score is 10% lower than average score,
     * this backend will be marked as LOW load, if load score is 10% higher than average score, HIGH load
     * will be marked.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static double balance_load_score_threshold = 0.1; // 10%

    // if disk usage > balance_load_score_threshold + urgent_disk_usage_extra_threshold
    // then this disk need schedule quickly
    // this value could less than 0.
    @ConfField(mutable = true, masterOnly = true)
    public static double urgent_balance_disk_usage_extra_threshold = 0.05;

    // when run urgent disk balance, shuffle the top large tablets
    // range: [ 0 ~ 100 ]
    @ConfField(mutable = true, masterOnly = true)
    public static int urgent_balance_shuffle_large_tablet_percentage = 1;

    @ConfField(mutable = true, masterOnly = true)
    public static double urgent_balance_pick_large_tablet_num_threshold = 1000;

    // range: 0 ~ 100
    @ConfField(mutable = true, masterOnly = true)
    public static int urgent_balance_pick_large_disk_usage_percentage = 80;

    // there's a case, all backend has a high disk, by default, it will not run urgent disk balance.
    // if set this value to true, urgent disk balance will always run,
    // the backends will exchange tablets among themselves.
    @ConfField(mutable = true, masterOnly = true)
    public static boolean enable_urgent_balance_no_low_backend = true;

    /**
     * if set to true, TabletScheduler will not do balance.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static boolean disable_balance = false;

    /**
     * when be rebalancer idle, then disk balance will occurs.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int be_rebalancer_idle_seconds = 0;

    /**
     * if set to true, TabletScheduler will not do disk balance.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static boolean disable_disk_balance = false;

    // balance order
    // ATTN: a temporary config, may delete later.
    @ConfField(mutable = true, masterOnly = true)
    public static boolean balance_be_then_disk = true;

    /**
     * if set to false, TabletScheduler will not do disk balance for replica num = 1.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static boolean enable_disk_balance_for_single_replica = false;

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

    // when execute admin set replica status = 'drop', the replica will marked as user drop.
    // will try to drop this replica within time not exceeds manual_drop_replica_valid_second
    @ConfField(masterOnly = true, mutable = true)
    public static long manual_drop_replica_valid_second = 24 * 3600L;

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

    // if the number of report task in FE exceed max_report_task_num_per_rpc, then split it to multiple rpc
    @ConfField(mutable = true, masterOnly = true, description = {
            "The maximum number of batched tasks per RPC assigned to each BE when resending agent tasks, "
                    + "the default value is 10000."})
    public static int report_resend_batch_task_num_per_rpc = 10000;

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
    public static int max_routine_load_task_concurrent_num = 256;

    /**
     * the max concurrent routine load task num per BE.
     * This is to limit the num of routine load tasks sending to a BE, and it should also less
     * than BE config 'max_routine_load_thread_pool_size'(default 1024),
     * which is the routine load task thread pool max size on BE.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int max_routine_load_task_num_per_be = 1024;

    /**
     * routine load timeout is equal to maxBatchIntervalS * routine_load_task_timeout_multiplier.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int routine_load_task_timeout_multiplier = 10;

    /**
     * routine load task min timeout second.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int routine_load_task_min_timeout_sec = 60;

    /**
     * streaming task load timeout is equal to maxIntervalS * streaming_task_timeout_multiplier.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int streaming_task_timeout_multiplier = 10;

    /**
     * the max timeout of get kafka meta.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int max_get_kafka_meta_timeout_second = 60;


    /**
     * the expire time of routine load blacklist.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int routine_load_blacklist_expire_time_second = 300;

    /**
     * Minimum batch interval for adaptive routine load tasks when not at EOF.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int routine_load_adaptive_min_batch_interval_sec = 360;

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
    public static String small_file_dir =  EnvUtils.getDorisHome() + "/small_files";

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
     * If some joural is wrong, and FE can't start, we can use this to skip it.
     */
    @ConfField(mutable = false, masterOnly = false)
    public static String[] force_skip_journal_ids = {};

    @ConfField(description = {
            "When replaying editlog encounters exceptions with specific operation types that prevent FE from starting, "
                    + "you can configure the editlog operation type enum values to be ignored, "
                    + "thereby skipping these exceptions and allowing the replay thread to continue "
                    + "replaying other logs."})
    public static short[] skip_operation_types_on_replay_exception =  {-1, -1};

    /**
     * Decide how often to check dynamic partition
     */
    @ConfField(mutable = true, masterOnly = true)
    public static long dynamic_partition_check_interval_seconds = 600;

    /**
     * When scheduling dynamic partition tables,
     * the execution interval of each table to prevent excessive consumption of FE CPU at the same time
     * default is 0
     */
    @ConfField(mutable = true, masterOnly = true)
    public static long dynamic_partition_step_interval_ms = 0;

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
     * a period for auto resume routine load
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int period_of_auto_resume_min = 10;

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
    public static int decommission_tablet_check_threshold = 50000;

    /**
     * When decommission a backend, need to migrate all its tablets to other backends.
     * But there maybe some leaky tablets due to forgetting to delete them from TabletInvertIndex.
     * They are not in use. Decommission can skip migrating them.
     * For safety, decommission wait for a period after founding leaky tablets.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int decommission_skip_leaky_tablet_second = 3600 * 5;

    /**
     * Decommission a tablet need to wait all the previous txns finished.
     * If wait timeout, decommission will fail.
     * Need to increase this wait time if the txn take a long time.
     *
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int decommission_tablet_wait_time_seconds = 3600;

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
     *  Minimum interval between last version when caching results,
     *  This parameter distinguishes between offline and real-time updates
     */
    @ConfField(mutable = true, masterOnly = false)
    public static int cache_last_version_interval_second = 30;

    /**
     *  Expire sql sql in frontend time
     */
    @ConfField(
            mutable = true,
            masterOnly = false,
            callbackClassString = "org.apache.doris.common.cache.NereidsSqlCacheManager$UpdateConfig",
            description = {
                    "The current default setting is 300, which is used to control the expiration time of SQL cache "
                            + "in NereidsSqlCacheManager. If the cache is not accessed for a period of time, "
                            + "it will be reclaimed."}
    )
    public static int expire_sql_cache_in_fe_second = 300;

    /**
     *  Expire hbo plan stats. cache in frontend time.
     */
    @ConfField(
            mutable = true,
            masterOnly = false,
            callbackClassString = "org.apache.doris.nereids.stats.MemoryHboPlanStatisticsProvider$UpdateConfig",
            description = {
                    "The default setting is 86400, which is used to control the expiration time of plan stats cache "
                            + "in MemoryHboPlanStatisticsProvider. If the cache is not accessed for a period of time, "
                            + "it will be reclaimed."}
    )
    public static int expire_hbo_plan_stats_cache_in_fe_second = 86400;

    /**
     *  Expire hbo plan info cache in frontend time.
     */
    @ConfField(
            mutable = true,
            masterOnly = false,
            callbackClassString = "org.apache.doris.nereids.stats.HboPlanInfoProvider$UpdateConfig",
            description = {
                    "The default setting is 100, which is used to control the expiration time of HBO plan info cache "
                            + "in HboPlanInfoProvider. If the cache is not accessed for a period of time, "
                            + "it will be reclaimed."}
    )
    public static int expire_hbo_plan_info_cache_in_fe_second = 1000;

    /**
     *  Expire sql sql in frontend time
     */
    @ConfField(
            mutable = true,
            masterOnly = false,
            callbackClassString = "org.apache.doris.common.cache.NereidsSortedPartitionsCacheManager$UpdateConfig",
            description = {"The current default setting is 300, which is used to control the expiration time of "
                    + "the partition metadata cache in NereidsSortedPartitionsCacheManager. "
                    + "If the cache is not accessed for a period of time, it will be reclaimed."}
    )
    public static int expire_cache_partition_meta_table_in_fe_second = 300;

    /**
     * Set the maximum number of rows that can be cached
     */
    @ConfField(mutable = true, masterOnly = false, description = {
            "Maximum number of rows that can be cached in SQL/Partition Cache, is 3000 by default."})
    public static int cache_result_max_row_count = 3000;

    /**
     * Set the maximum data size that can be cached
     */
    @ConfField(mutable = true, masterOnly = false, description = {
            "Maximum data size of rows that can be cached in SQL/Partition Cache. The default is 30MB."})
    public static int cache_result_max_data_size = 31457280; // 30M

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
     * Whether to add a version column when create unique table
     */
    @ConfField(mutable = true, masterOnly = true)
    public static boolean enable_hidden_version_column_by_default = true;

    /**
     * Whether to add a skip bitmap column when create merge-on-write unique table
     */
    @ConfField(mutable = true, masterOnly = true)
    public static boolean enable_skip_bitmap_column_by_default = false;

    /**
     * Used to set default db data quota bytes.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static long default_db_data_quota_bytes = Long.MAX_VALUE; // 8192 PB

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

    @ConfField(mutable = false, masterOnly = true, description = {"Whether to enable debug points, used in testing."})
    public static boolean enable_debug_points = false;

    /**
     * This config is used to try skip broker when access bos or other cloud storage via broker
     */
    @ConfField(mutable = true, masterOnly = true)
    public static boolean enable_access_file_without_broker = false;

    /**
     * Whether to allow the outfile function to export the results to the local disk.
     * If set to true, there's risk to run out of FE disk capacity.
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
     * num of thread to handle grpc events in grpc_threadmgr
     */
    @ConfField
    public static int grpc_threadmgr_threads_nums = 4096;

    /**
     * sets the time without read activity before sending a keepalive ping
     * the smaller the value, the sooner the channel is unavailable, but it will increase network io
     */
    @ConfField(description = {"The time without GRPC read activity before sending a keepalive ping"})
    public static int grpc_keep_alive_second = 10;

    /**
     * Whether to use gRPC directExecutor() for BackendServiceClient.
     *
     * WARNING: When enabled, gRPC client call listeners (including protobuf parsing/completion) may run on
     * Netty EventLoop threads. If response messages are large, this can block transport threads and delay
     * unrelated RPCs on the same channel.
     *
     * This option should only be enabled when you are sure responses are small and the risk is acceptable.
     * Takes effect after FE restart.
     */
    @ConfField(description = {"是否为 BackendServiceClient 使用 gRPC directExecutor",
            "Whether to use gRPC directExecutor for BackendServiceClient"})
    public static boolean grpc_backend_client_use_direct_executor = false;

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
    public static int max_dynamic_partition_num = 20000;

    /**
     * Used to limit the maximum number of partitions that can be created when creating multi partition,
     * to avoid creating too many partitions at one time.
     * The number is determined by "start" and "end" in the multi partition parameters.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int max_multi_partition_num = 4096;

    /**
     * Use this parameter to set the partition name prefix for multi partition,
     * Only multi partition takes effect, not dynamic partitions.
     * The default prefix is "p_".
     */
    @ConfField(mutable = true, masterOnly = true)
    public static String multi_partition_name_prefix = "p_";

    /**
     * Control the max num of backup/restore job per db
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int max_backup_restore_job_num_per_db = 10;

    /**
     * A internal config, to reduce the restore job size during serialization by compress.
     *
     * WARNING: Once this option is enabled and a restore is performed, the FE version cannot be rolled back.
     */
    @ConfField(mutable = false)
    public static boolean restore_job_compressed_serialization = false;

    /**
     * A internal config, to reduce the backup job size during serialization by compress.
     *
     * WARNING: Once this option is enabled and a backup is performed, the FE version cannot be rolled back.
     */
    @ConfField(mutable = false)
    public static boolean backup_job_compressed_serialization = false;

    /**
     * A internal config, to indicate whether to enable the restore snapshot rpc compression.
     *
     * The ccr syncer will depends this config to decide whether to compress the meta and job
     * info of the restore snapshot request.
     */
    @ConfField(mutable = false)
    public static boolean enable_restore_snapshot_rpc_compression = true;

    /**
     * A internal config, to indicate whether to reset the index id when restore olap table.
     *
     * The inverted index saves the index id in the file path/header, so the index id between
     * two clusters must be the same.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static boolean restore_reset_index_id = false;

    /**
     * Control the max num of tablets per backup job involved.
     */
    @ConfField(mutable = true, masterOnly = true, description = {
            "Control the maximum number of tablets per backup job, to avoid OOM."})
    public static int max_backup_tablets_per_job = 300000;

    /**
     * whether to ignore table that not support type when backup, and not report exception.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static boolean ignore_backup_not_support_table_type = false;

    /**
     * whether to ignore temp partitions when backup, and not report exception.
     */
    @ConfField(mutable = true, masterOnly = true, description = {
            "Whether to ignore temporary partitions during backup without reporting an exception."})
    public static boolean ignore_backup_tmp_partitions = false;

    /**
     * A internal config, to control the update interval of backup handler. Only used to speed up tests.
     */
    @ConfField(mutable = false)
    public static long backup_handler_update_interval_millis = 3000;


    /**
     * Whether to enable cloud restore job.
     */
    @ConfField(mutable = true, masterOnly = true, description = {
            "Whether to enable cloud restore job."}, varType = VariableAnnotation.EXPERIMENTAL)
    public static boolean enable_cloud_restore_job = false;

    @ConfField(mutable = true, masterOnly = true, description = {
            "During the cloud restore job, the maximum number of tablets created per "
                    + "create-tablets RPC. Default is 256."})
    public static int cloud_restore_create_tablet_batch_size = 256;

    /**
     * Control the default max num of the instance for a user.
     */
    @ConfField(mutable = true)
    public static int default_max_query_instances = -1;

    /*
     * One master daemon thread will update global partition info, include in memory and visible version
     * info every partition_info_update_interval_secs
     */
    @ConfField(mutable = false, masterOnly = true)
    public static int partition_info_update_interval_secs = 60;

    @Deprecated
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

    /**
     * Used to limit the length of table name.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int table_name_length_limit = 64;

    @ConfField(mutable = true, description = {"Used to limit the length of column comments. "
            + "If the existing column comment is too long, it will be truncated when displayed."})
    public static int column_comment_length_limit = -1;

    @ConfField(mutable = true, description = {
            "Default compression type for internal tables. Supported values: LZ4, LZ4F, LZ4HC, ZLIB, ZSTD, "
                    + "SNAPPY, NONE."})
    public static String default_compression_type = "ZSTD";

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

    // statistics
    /*
     * the max unfinished statistics job number
     */
    @Deprecated
    @ConfField(mutable = true, masterOnly = true)
    public static int cbo_max_statistics_job_num = 20;
    /*
     * the max timeout of a statistics task
     */
    @Deprecated
    @ConfField(mutable = true, masterOnly = true)
    public static int max_cbo_statistics_task_timeout_sec = 300;
    /*
     * the concurrency of statistics task
     */
    @Deprecated
    @ConfField(mutable = false, masterOnly = true)
    public static int cbo_concurrency_statistics_task_num = 10;
    /*
     * default sample percentage
     * The value from 0 ~ 100. The 100 means no sampling and fetch all data.
     */
    @Deprecated
    @ConfField(mutable = true, masterOnly = true)
    public static int cbo_default_sample_percentage = 10;

    /*
     * the system automatically checks the time interval for statistics
     */
    @ConfField(mutable = true, masterOnly = true, description = {
            "This parameter controls the time interval for automatic collection jobs to check the health of table "
                    + "statistics and trigger automatic collection."})
    public static int auto_check_statistics_in_minutes = 1;

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
    public static boolean enable_quantile_state_type = true;

    /*---------------------- JOB CONFIG START------------------------*/
    /**
     * The number of threads used to dispatch timer job.
     * If we have a lot of timer jobs, we need more threads to dispatch them.
     * All timer job will be dispatched to a thread pool, and they will be dispatched to the thread queue of the
     * corresponding type of job
     * The value should be greater than 0, if it is 0 or <=0, set it to 5
     */
    @ConfField(masterOnly = true, description = {"The number of threads used to dispatch timer jobs."})
    public static int job_dispatch_timer_job_thread_num = 2;

    /**
     * The number of timer jobs that can be queued.
     * if consumer is slow, the queue will be full, and the producer will be blocked.
     * if you have a lot of timer jobs, you need to increase this value or increase the number of
     * {@code @dispatch_timer_job_thread_num}
     * The value should be greater than 0, if it is 0 or <=0, set it to 1024
     */
    @ConfField(masterOnly = true, description = {"The number of timer jobs that can be queued."})
    public static int job_dispatch_timer_job_queue_size = 1024;
    @ConfField(masterOnly = true, description = {
            "Maximum number of persisted tasks allowed per job. Tasks exceeding this limit will be discarded. "
                    + "If the value is less than 1, tasks will not be persisted."})
    public static int max_persistence_task_count = 100;

    @ConfField(masterOnly = true, description = {
            "The size of the MTMV task's waiting queue. If the size is negative, 1024 will be used. If "
                    + "the size is not a power of two, the nearest power of two will be"
                    + " automatically selected."})
    public static int mtmv_task_queue_size = 1024;
    @ConfField(masterOnly = true, description = {
            "The size of the Insert task's waiting queue. If the size is negative, 1024 will be used."
                    + " If the size is not a power of two, the nearest power of two will "
                    + "be automatically selected."})
    public static int insert_task_queue_size = 1024;
    @ConfField(masterOnly = true, description = {
            "The size of the Dictionary loading task's waiting queue. If the size is negative, 1024 will be used."
                    + " If the size is not a power of two, the nearest power of two will "
                    + "be automatically selected."})
    public static int dictionary_task_queue_size = 1024;

    @ConfField(masterOnly = true, description = {
            "The maximum time to retain a finished job before it is deleted. Unit: hour."})
    public static int finished_job_cleanup_threshold_time_hour = 24;

    @ConfField(masterOnly = true, description = {"The number of threads used to consume Insert tasks, "
            + "the value should be greater than 0, if it is <=0, default is 10."})
    public static int job_insert_task_consumer_thread_num = 10;

    @ConfField(masterOnly = true, description = {"The number of threads used to consume MTMV tasks, "
            + "the value should be greater than 0, if it is <=0, default is 10."})
    public static int job_mtmv_task_consumer_thread_num = 10;

    @ConfField(masterOnly = true, description = {
            "The number of threads used to perform dictionary import and delete tasks. The value should be"
                    + " greater than 0; otherwise it defaults to 3."})
    public static int job_dictionary_task_consumer_thread_num = 3;

    @ConfField(masterOnly = true, description = {"The number of threads used to execute streaming tasks. "
            + "The value should be greater than 0; if it is <=0, the default is 100."})
    public static int job_streaming_task_exec_thread_num = 100;

    @ConfField(masterOnly = true, description = {"The maximum number of streaming jobs. "
            + "The value should be greater than 0; if it is <=0, the default is 1024."})
    public static int max_streaming_job_num = 1024;

    @ConfField(masterOnly = true, description = {
            "The maximum number of tasks a streaming job can keep in memory. If the number exceeds the limit, "
                    + "old records will be discarded."})
    public static int max_streaming_task_show_count = 100;

    @ConfField(masterOnly = true, mutable = true, description = {
            "Max auto resume retry count for streaming jobs. "
                    + "After exceeding, the failure reason is rewritten to CANNOT_RESUME_ERR "
                    + "and the job requires manual intervention."})
    public static int streaming_job_max_auto_resume_count = 10;

    /* job test config */
    /**
     * If set to true, we will allow the interval unit to be set to second, when creating a recurring job.
     */
    @ConfField
    public static boolean enable_job_schedule_second_for_test = false;

    /*---------------------- JOB CONFIG END------------------------*/
    /**
     * The number of async tasks that can be queued. @See TaskDisruptor
     * if consumer is slow, the queue will be full, and the producer will be blocked.
     */
    @ConfField
    public static int async_task_queen_size = 1024;

    /**
     * The number of threads used to consume async tasks. @See TaskDisruptor
     * if we have a lot of async tasks, we need more threads to consume them. Sure, it's depends on the cpu cores.
     */
    @ConfField
    public static int async_task_consumer_thread_num = 64;

    /**
     * When job is finished, it will be saved in job manager for a while.
     * This configuration is used to control the max saved time.
     * Default is 3 days.
     */
    @Deprecated
    @ConfField
    public static int finish_job_max_saved_second = 60 * 60 * 24 * 3;

    // enable_workload_group should be immutable and temporarily set to mutable during the development test phase
    @ConfField(mutable = true, varType = VariableAnnotation.EXPERIMENTAL)
    public static boolean enable_workload_group = true;

    @ConfField(mutable = true)
    public static boolean enable_query_queue = true;

    @ConfField(mutable = true)
    public static long query_queue_update_interval_ms = 5000;

    @ConfField(mutable = true, description = {"When BE memory usage is higher than this value, queries may be queued. "
            + "Default value is -1, meaning this feature is disabled. Decimal value range is from 0 to 1."})
    public static double query_queue_by_be_used_memory = -1;

    @ConfField(mutable = true, description = {"In the scenario of memory back-pressure, "
            + "the time interval for periodically obtaining BE memory usage."})
    public static long get_be_resource_usage_interval_ms = 10000;

    @ConfField(mutable = false, masterOnly = true)
    public static int backend_rpc_timeout_ms = 60000; // 1 min

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
    @ConfField(mutable = true)
    public static boolean enable_date_conversion = true;

    @ConfField(mutable = false, masterOnly = true)
    public static boolean enable_multi_tags = true;

    /**
     * If set to TRUE, FE will convert DecimalV2 to DecimalV3 automatically.
     */
    @ConfField(mutable = true)
    public static boolean enable_decimal_conversion = true;

    /**
     * Support complex data type ARRAY.
     */
    @Deprecated
    @ConfField(mutable = true, masterOnly = true)
    public static boolean enable_array_type = false;

    /**
     * The timeout of executing async remote fragment.
     * In normal case, the async remote fragment will be executed in a short time. If system are under high load
     * condition，try to set this timeout longer.
     */
    @ConfField(mutable = true)
    public static long remote_fragment_exec_timeout_ms = 30000; // 30 sec

    /**
     * Max data version of backends serialize block.
     */
    @ConfField(mutable = false)
    public static int max_be_exec_version = 10;

    /**
     * Min data version of backends serialize block.
     */
    @ConfField(mutable = false)
    public static int min_be_exec_version = 8;

    /**
     * Data version of backends serialize block.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int be_exec_version = max_be_exec_version;

    /**
     * If set to true, query on external table will prefer to assign to compute node.
     * And the max number of compute node is controlled by min_backend_num_for_external_table.
     * If set to false, query on external table will assign to any node.
     */
    @ConfField(mutable = true, description = {
            "If set to true, queries on external tables will prefer to be assigned to compute nodes. "
                    + "The maximum number of compute nodes is controlled by min_backend_num_for_external_table. "
                    + "If set to false, queries on external tables will be assigned to any node. "
                    + "If there are no compute nodes in the cluster, this config has no effect."})
    public static boolean prefer_compute_node_for_external_table = false;

    @ConfField(mutable = true, description = {"Only takes effect when prefer_compute_node_for_external_table is true. "
            + "If the compute node count is less than this value, "
            + "queries on external tables will try to use some mix nodes as well, "
            + "to let the total number of nodes reach this value. "
            + "If the compute node count is larger than this value, "
            + "queries on external tables will be assigned to compute nodes only. "
            + "-1 means only use current compute nodes."})
    public static int min_backend_num_for_external_table = -1;

    /**
     * Max query profile num.
     */
    @ConfField(mutable = true, masterOnly = false)
    public static int max_query_profile_num = 500;

    /**
     * Set to true to disable backend black list, so that even if we failed to send task to a backend,
     * that backend won't be added to black list.
     * This should only be set when running tests, such as regression test.
     * Highly recommended NOT disable it in product environment.
     */
    @ConfField(mutable = true, masterOnly = false)
    public static boolean disable_backend_black_list = false;

    @ConfField(mutable = true, masterOnly = false, description = {
            "If a backend is attempted to be added to the blacklist do_add_backend_black_list_threshold_count times "
                    + "within do_add_backend_black_list_threshold_seconds, it will be added to the blacklist."})
    public static long do_add_backend_black_list_threshold_count = 10;

    @ConfField(mutable = true, masterOnly = false, description = {
            "If a backend is attempted to be added to the blacklist do_add_backend_black_list_threshold_count times "
                    + "within do_add_backend_black_list_threshold_seconds, it will be added to the blacklist."})
    public static long do_add_backend_black_list_threshold_seconds = 30;

    @ConfField(mutable = true, masterOnly = false, description = {
            "A backend will stay in the blacklist for this duration after being added."})
    public static long stay_in_backend_black_list_threshold_seconds = 60;

    /**
     * Maximum backend heartbeat failure tolerance count.
     * Default is 1, which means if 1 heart failed, the backend will be marked as dead.
     * A larger value can improve the tolerance of the cluster to occasional heartbeat failures.
     * For example, when running regression tests, this value can be increased.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static long max_backend_heartbeat_failure_tolerance_count = 1;

    /**
     * Even if a backend is healthy, still write a heartbeat editlog to update backend's lastUpdateMs of bdb image.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int editlog_healthy_heartbeat_seconds = 300;

    /**
     * Abort transaction time after lost heartbeat.
     * The default value is 300s, which means transactions of be will be aborted after lost heartbeat 300s.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int abort_txn_after_lost_heartbeat_time_second = 300;

    /**
     * Heartbeat interval in seconds.
     * Default is 10, which means every 10 seconds, the master will send a heartbeat to all backends.
     */
    @ConfField(mutable = false, masterOnly = false)
    public static int heartbeat_interval_second = 10;

    /**
     * After a backend is marked as unavailable, it will be added to blacklist.
     * Default is 120.
     */
    @ConfField(mutable = true, masterOnly = false)
    public static int blacklist_duration_second = 120;

    /**
     * The default connection timeout for hive metastore.
     * hive.metastore.client.socket.timeout
     */
    @ConfField(mutable = false, masterOnly = false)
    public static long hive_metastore_client_timeout_second = 10;

    /**
     * Used to determined how many statistics collection SQL could run simultaneously.
     */
    @ConfField
    public static int statistics_simultaneously_running_task_num = 3;

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
    @ConfField(description = {"Maximum cache number of partitions at table level in Hive Metastore."})
    public static long max_hive_partition_cache_num = 10000;

    @ConfField(description = {"Maximum cache number of Hudi/Iceberg tables."})
    public static long max_external_table_cache_num = 1000;

    @ConfField(description = {"Maximum cache number of database and table instances in external catalogs."})
    public static long max_meta_object_cache_num = 1000;

    @ConfField(description = {"Maximum cache number of Hive partitioned tables."})
    public static long max_hive_partition_table_cache_num = 1000;

    @ConfField(mutable = false, masterOnly = false, description = {
            "Max number of hive partition values to return while list partitions, -1 means no limitation."})
    public static short max_hive_list_partition_num = -1;

    @ConfField(mutable = false, masterOnly = false, description = {"Max cache number of remote file system."})
    public static long max_remote_file_system_cache_num = 100;

    @ConfField(mutable = false, masterOnly = false, description = {
            "Maximum cache number of external table row counts."})
    public static long max_external_table_row_count_cache_num = 100000;

    @ConfField(description = {"Maximum cached file number for external table split file meta cache at query level."})
    public static long max_external_table_split_file_meta_cache_num = 100000;

    /**
     * Max cache loader thread-pool size.
     * Max thread pool size for loading external meta cache
     */
    @ConfField(mutable = false, masterOnly = false)
    public static int max_external_cache_loader_thread_pool_size = 64;

    /**
     * Max cache num of external catalog's file
     * Decrease this value if FE's memory is small
     */
    @ConfField(mutable = false, masterOnly = false)
    public static long max_external_file_cache_num = 10000;

    /**
     * Max cache num of external table's schema
     * Decrease this value if FE's memory is small
     */
    @ConfField(mutable = false, masterOnly = false)
    public static long max_external_schema_cache_num = 10000;

    @ConfField(description = {
            "The expiration time of a cache object after its last access. Used for external meta cache."})
    public static long external_cache_expire_time_seconds_after_access = 86400L; // 24 hours

    @ConfField(description = {"The auto-refresh interval of the external meta cache."})
    public static long external_cache_refresh_time_minutes = 10; // 10 mins

    /**
     * Github workflow test type, for setting some session variables
     * only for certain test type. E.g. only settting batch_size to small
     * value for p0.
     */
    @ConfField(mutable = true, masterOnly = false, options = {"p0", "daily", "rqg", "external"})
    public static String fuzzy_test_type = "";

    /**
     * Set session variables randomly to check more issues in github workflow
     */
    @ConfField(mutable = true, masterOnly = false)
    public static boolean use_fuzzy_session_variable = false;

    /**
     * Set config variables randomly to check more issues in github workflow
     */
    @ConfField(mutable = true, masterOnly = false)
    public static boolean use_fuzzy_conf = false;

    /**
     * Max num of same name meta informatntion in catalog recycle bin.
     * Default is 3.
     * 0 means do not keep any meta obj with same name.
     * < 0 means no limit
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int max_same_name_catalog_trash_num = 3;

    /**
     * NOTE: The storage policy is still under developement.
     */
    @ConfField(mutable = false, masterOnly = true)
    public static boolean enable_storage_policy = true;

    /**
     * This config is mainly used in the k8s cluster environment.
     * When enable_fqdn_mode is true, the name of the pod where be is located will remain unchanged
     * after reconstruction, while the ip can be changed.
     */
    @ConfField(mutable = false, varType = VariableAnnotation.EXPERIMENTAL)
    public static boolean enable_fqdn_mode = false;

    /**
     * If set to true, doris will try to parse the ddl of a hive view and try to execute the query
     * otherwise it will throw an AnalysisException.
     */
    @ConfField(mutable = true)
    public static boolean enable_query_hive_views = true;

    @ConfField(mutable = true)
    public static boolean enable_query_iceberg_views = true;

    /**
     * If set to true, doris will automatically synchronize hms metadata to the cache in fe.
     */
    @ConfField(masterOnly = true)
    public static boolean enable_hms_events_incremental_sync = false;

    /**
     * the plan cache num which can be reused for the next query
     */
    @ConfField(
            mutable = true,
            callbackClassString = "org.apache.doris.common.cache.NereidsSqlCacheManager$UpdateConfig",
            description = {"Currently defaults to 100. This config is used to control the number of "
                    + "SQL caches managed by NereidsSqlCacheManager."}
    )
    public static int sql_cache_manage_num = 100;

    @ConfField(
            mutable = true,
            callbackClassString = "org.apache.doris.common.cache.NereidsSortedPartitionsCacheManager$UpdateConfig",
            description = {"Currently defaults to 100. This is used to control the number of ordered "
                    + "partition metadata caches in NereidsSortedPartitionsCacheManager, "
                    + "and to accelerate partition pruning."}
    )
    public static int cache_partition_meta_table_manage_num = 100;

    /**
     * HBO plan stats. cache number which can be reused for the next query.
     */
    @ConfField(
            mutable = true,
            callbackClassString = "org.apache.doris.nereids.stats.MemoryHboPlanStatisticsProvider$UpdateConfig",
            description = {"Currently defaults to 100000. This config is used to control the number of "
                    + "HBO plan stats cache entries."}
    )
    public static int hbo_plan_stats_cache_num = 100000;

    /**
     * HBO plan recent runs entry number.
     */
    @ConfField(
            mutable = true,
            description = {"Currently defaults to 10. This config is used to control the number of "
                    + "recent runs entries in the HBO plan stats cache."}
    )
    public static int hbo_plan_stats_cache_recent_runs_entry_num = 10;

    /**
     * Plan info cache number which is used for HboPlanInfoProvider.
     */
    @ConfField(
            mutable = true,
            callbackClassString = "org.apache.doris.nereids.stats.HboPlanInfoProvider$UpdateConfig",
            description = {"Currently defaults to 1000. This config is used to control the number of "
                    + "HBO plan info cache entries."}
    )
    public static int hbo_plan_info_cache_num = 1000;

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
    @ConfField(mutable = false, masterOnly = false, varType = VariableAnnotation.EXPERIMENTAL)
    public static boolean enable_ssl = false;

    /**
     * If set to ture, ssl connection needs to authenticate client's certificate.
     */
    @ConfField(mutable = false, masterOnly = false)
    public static boolean ssl_force_client_auth = false;

    /**
     * ssl connection needs to authenticate client's certificate store type.
     */
    @ConfField(mutable = false, masterOnly = false)
    public static String ssl_trust_store_type = "PKCS12";

    /**
     * Default CA certificate file location for mysql ssl connection.
     */
    @ConfField(mutable = false, masterOnly = false)
    public static String mysql_ssl_default_ca_certificate =  EnvUtils.getDorisHome()
            + "/mysql_ssl_default_certificate/ca_certificate.p12";

    /**
     * Default server certificate file location for mysql ssl connection.
     */
    @ConfField(mutable = false, masterOnly = false)
    public static String mysql_ssl_default_server_certificate =  EnvUtils.getDorisHome()
            + "/mysql_ssl_default_certificate/server_certificate.p12";

    /**
     * Password for default CA certificate file.
     */
    @ConfField(mutable = false, masterOnly = false)
    public static String mysql_ssl_default_ca_certificate_password = "doris";

    /**
     * Password for default CA certificate file.
     */
    @ConfField(mutable = false, masterOnly = false)
    public static String mysql_ssl_default_server_certificate_password = "doris";

    /**
     * Used to set session variables randomly to check more issues in github workflow
     */
    @ConfField(mutable = true)
    public static int pull_request_id = 0;

    /**
     * Used to set default db transaction quota num.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static long default_db_max_running_txn_num = -1;

    /**
     * Used by TokenManager to control the number of tokens keep in memory.
     * One token will keep alive for {token_queue_size * token_generate_period_hour} hours.
     * By defaults, one token will keep for 3 days.
     */
    @ConfField(mutable = false, masterOnly = true)
    public static int token_queue_size = 6;

    /**
     * TokenManager will generate token every token_generate_period_hour.
     */
    @ConfField(mutable = false, masterOnly = true)
    public static int token_generate_period_hour = 12;

    /**
     * The secure local path of the FE node the place the data which will be loaded in doris.
     * The default value is empty for this config which means this feature is not allowed.
     * User who want to load fe server local file should config the value to a right local path.
     */
    @ConfField(mutable = false, masterOnly = false)
    public static String mysql_load_server_secure_path = "";

    @ConfField(mutable = false, masterOnly = false)
    public static int mysql_load_in_memory_record = 20;

    @ConfField(mutable = false, masterOnly = false)
    public static int mysql_load_thread_pool = 4;

    /**
     * BDBJE file logging level
     * OFF, SEVERE, WARNING, INFO, CONFIG, FINE, FINER, FINEST, ALL
     */
    @ConfField
    public static String bdbje_file_logging_level = "INFO";

    /**
     * When holding lock time exceeds the threshold, need to report it.
     */
    @ConfField
    public static long lock_reporting_threshold_ms = 500L;

    /**
     * If true, auth check will be disabled. The default value is false.
     * This is to solve the case that user forgot the password.
     */
    @ConfField(mutable = false)
    public static boolean skip_localhost_auth_check  = true;

    @ConfField(mutable = true)
    public static boolean enable_round_robin_create_tablet = true;

    @ConfField(mutable = true, masterOnly = true, description = {
            "When creating tablets for a partition, always start from the first BE. "
                    + "Note: This method may cause BE imbalance."})
    public static boolean create_tablet_round_robin_from_start = false;

    /**
     * To prevent different types (V1, V2, V3) of behavioral inconsistencies,
     * we may delete the DecimalV2 and DateV1 types in the future.
     * At this stage, we use 'disable_decimalv2' and 'disable_datev1'
     * to determine whether these two types take effect.
     */
    @ConfField(mutable = true)
    public static boolean disable_decimalv2  = true;

    @ConfField(mutable = true)
    public static boolean disable_datev1  = true;

    /*
     * This variable indicates the number of digits by which to increase the scale
     * of the result of division operations performed with the `/` operator. The
     * default value is 4, and it is currently only used for the DECIMALV3 type.
     */
    @ConfField(mutable = true)
    public static int div_precision_increment = 4;

    /**
     * This config used for export/outfile.
     * Whether delete all files in the directory specified by export/outfile.
     * It is a very dangerous operation, should only be used in test env.
     */

    @ConfField(mutable = false)
    public static boolean enable_delete_existing_files  = false;
    /*
     * The actual memory size taken by stats cache highly depends on characteristics of data, since on the different
     * dataset and scenarios the max/min literal's average size and buckets count of histogram would be highly
     * different. Besides, JVM version etc. also has influence on it, though not much as data itself.
     * Here I would give the mem size taken by stats cache with 10_0000 items.Each item's avg length of max/min literal
     * is 32, and the avg column name length is 16, and each column has a histogram with 128 buckets
     * In this case, stats cache takes total 911.954833984MiB mem.
     * If without histogram, stats cache takes total 61.2777404785MiB mem.
     * It's strongly discourage analyzing a column with a very large STRING value in the column, since it would cause
     * FE OOM.
     */
    @ConfField
    public static long stats_cache_size = 50_0000;

    /**
     * This config used for ranger cache data mask/row policy
     */
    @ConfField
    public static long ranger_cache_size = 10000;

    /**
     * This configuration is used to enable the statistics of query information, which will record
     * the access status of databases, tables, and columns, and can be used to guide the
     * optimization of table structures
     *
     */
    @ConfField(mutable = true)
    public static boolean enable_query_hit_stats = false;

    @ConfField(mutable = true, description = {"When set to true, if a query is unable to select a healthy replica, "
            + "the detailed information of all replicas of the tablet, "
            + "including the specific reason why they are unqueryable, will be printed out."})
    public static boolean show_details_for_unaccessible_tablet = true;

    @ConfField(mutable = false, masterOnly = false, varType = VariableAnnotation.EXPERIMENTAL, description = {
            "Whether to enable the binlog feature"})
    public static boolean enable_feature_binlog = false;

    @ConfField(mutable = false, description = {"Whether to enable the binlog feature for databases/tables by default"})
    public static boolean force_enable_feature_binlog = false;

    @ConfField(mutable = false, masterOnly = false, varType = VariableAnnotation.EXPERIMENTAL, description = {
            "Set the maximum byte length of a binlog message"})
    public static int max_binlog_messsage_size = 1024 * 1024 * 1024;

    @ConfField(mutable = true, masterOnly = true, description = {
            "Whether to disable creating catalog with WITH RESOURCE statement."})
    public static boolean disallow_create_catalog_with_resource = true;

    @ConfField(mutable = true, masterOnly = false, description = {"Sample size for hive row count estimation."})
    public static int hive_stats_partition_sample_size = 30;

    @ConfField(mutable = true, masterOnly = true, description = {"Whether to enable external Hive bucket tables"})
    public static boolean enable_create_hive_bucket_table = false;

    @ConfField(mutable = true, masterOnly = true, description = {"Default Hive file format when creating tables."})
    public static String hive_default_file_format = "orc";

    @ConfField
    public static int statistics_sql_parallel_exec_instance_num = 1;

    @ConfField
    public static long statistics_sql_mem_limit_in_bytes = 2L * 1024 * 1024 * 1024;

    @ConfField(mutable = true, masterOnly = true, description = {
            "Used to force the number of replicas of internal tables. If this config is greater than zero, "
                    + "the number of replicas specified by the user when creating the table will be ignored, "
                    + "and the value set by this parameter will be used. At the same time, the replica tags "
                    + "and other parameters specified in the CREATE TABLE statement will be ignored. "
                    + "This config does not affect operations including creating partitions "
                    + "and modifying table properties. "
                    + "This config is recommended to be used only in the test environment."})
    public static int force_olap_table_replication_num = 0;

    @ConfField(mutable = true, description = {
            "Used to force set the replica allocation of internal tables. If this config is not empty, "
                    + "the replication_num and replication_allocation specified by the user when creating the table "
                    + "or partitions will be ignored, and the value set by this parameter will be used. "
                    + "This config affects operations including creating tables, creating partitions, and creating "
                    + "dynamic partitions. This config is recommended to be used only in the test environment."})
    public static String force_olap_table_replication_allocation = "";

    @ConfField
    public static int auto_analyze_simultaneously_running_task_num = 1;

    @ConfField(mutable = true, masterOnly = true, description = {
            "统计信息收集时 string 列允许的最大字节长度。若列中存在长度超过该值的行，"
                    + "该列的统计信息将被跳过收集（task 仍标记为 FINISHED，在 SHOW ANALYZE 中显示跳过原因）。"
                    + "≤ 0 表示关闭此保护。默认 1024 (1KB)。"
                    + "注意：此保护只覆盖 FULL / LINEAR / DUJ1 统计收集路径（即 analyze 全表和 sample 的主 SQL）。"
                    + "当 enable_partition_analyze=true 时的 per-partition 路径（PARTITION_ANALYZE_TEMPLATE）"
                    + "出于正确性考虑不启用该保护，详见 BaseAnalysisTask 中的 NOTE。",
            "Max byte length allowed for a string column when collecting statistics. "
                    + "If any row in a string column is longer than this value, the column's stats "
                    + "collection is skipped (the task is still marked FINISHED, with the skip reason "
                    + "shown in SHOW ANALYZE). A value <= 0 disables this protection. Default: 1024 (1KB). "
                    + "Note: this protection applies to the FULL / LINEAR / DUJ1 collection paths "
                    + "(i.e. the main SQL used by full-table and sample analyze). The per-partition path "
                    + "(PARTITION_ANALYZE_TEMPLATE, used when enable_partition_analyze=true) is intentionally "
                    + "not guarded for correctness reasons; see the NOTE in BaseAnalysisTask."})
    public static long statistics_max_string_column_length = 1024;

    @Deprecated
    @ConfField
    public static final int period_analyze_simultaneously_running_task_num = 1;

    @ConfField(mutable = false)
    public static boolean allow_analyze_statistics_info_polluting_file_cache = true;

    @ConfField
    public static int cpu_resource_limit_per_analyze_task = 1;

    @ConfField(mutable = true)
    public static boolean force_sample_analyze = false; // avoid full analyze for performance reason

    @ConfField(mutable = true, description = {"The maximum number of partitions allowed for an Export job"})
    public static int maximum_number_of_export_partitions = 2000;

    @Deprecated
    @ConfField(mutable = true, description = {"The maximum parallelism allowed for an Export job"})
    public static int maximum_parallelism_of_export_job = 50;

    @ConfField(mutable = true, description = {"Whether to use MySQL's BIGINT type to return Doris's LARGEINT type"})
    public static boolean use_mysql_bigint_for_largeint = false;

    @ConfField
    public static boolean forbid_running_alter_job = false;

    @ConfField(description = {"Temporary config field. Will make all OLAP tables enable light schema change."})
    public static boolean enable_convert_light_weight_schema_change = false;

    @ConfField(mutable = true, masterOnly = false, description = {
            "When querying the information_schema.metadata_name_ids table, "
                    + "the timeout for obtaining all tables in one database."})
    public static long query_metadata_name_ids_timeout = 3;

    @ConfField(mutable = true, masterOnly = true, description = {"Whether to disable LocalDeployManager drop node."})
    public static boolean disable_local_deploy_manager_drop_node = true;

    @ConfField(mutable = true, description = {
            "When file cache is enabled, the number of virtual nodes of each node in the consistent hash algorithm. "
                    + "The larger the value, the more uniform the distribution of the hash algorithm, "
                    + "but it will increase the memory overhead."})
    public static int split_assigner_virtual_node_number = 256;

    @ConfField(mutable = true, description = {"Local node soft affinity optimization. Prefer local replication node."})
    public static boolean split_assigner_optimized_local_scheduling = true;

    @ConfField(mutable = true, description = {
            "The random algorithm has the smallest number of candidates and will select the most idle node."})
    public static int split_assigner_min_random_candidate_num = 2;

    @ConfField(mutable = true, description = {
            "The consistent hash algorithm has the smallest number of candidates and will select the most idle node."})
    public static int split_assigner_min_consistent_hash_candidate_num = 2;

    @ConfField(mutable = true, description = {"The maximum difference in the number of splits between nodes. "
            + "If this number is exceeded, the splits will be redistributed."})
    public static int split_assigner_max_split_num_variance = 1;

    @ConfField(description = {"Determines the number of persisted automatic analyze job execution status records."})
    public static long analyze_record_limit = 20000;

    @ConfField(mutable = true, masterOnly = true, description = {"Minimum number of buckets for auto bucketing."})
    public static int autobucket_min_buckets = 1;

    @ConfField(mutable = true, masterOnly = true, description = {"Maximum number of buckets for auto bucketing."})
    public static int autobucket_max_buckets = 128;

    @ConfField(mutable = true, masterOnly = true, description = {
            "Maximum number of buckets allowed when creating a table or adding a partition. "
                    + "This config shares the same default value with autobucket_max_buckets for consistency. "
                    + "Behavior: "
                    + "1. For user-specified buckets (CREATE TABLE / ALTER TABLE ADD PARTITION): "
                    + "if bucket number exceeds this limit, the operation will be rejected with an error message. "
                    + "2. For auto-bucket feature (Dynamic Partition): "
                    + "bucket number will be capped at autobucket_max_buckets automatically. "
                    + "Set to 0 or negative value to disable this limit for user-specified buckets."})
    public static int max_bucket_num_per_partition = 768;

    @ConfField(description = {"Maximum number of connections for the Arrow Flight Server per FE."})
    public static int arrow_flight_max_connections = 4096;

    @ConfField(mutable = true, masterOnly = true, description = {
            "In auto bucketing, the number of buckets is estimated based on the partition size. "
                    + "For storage and computing integration, a partition size of 5GB is estimated as one bucket, "
                    + "but for cloud, a partition size of 10GB is estimated as one bucket. "
                    + "If the configuration is less than 0, the code will adaptively use a default of 5GB "
                    + "in non-cloud mode, and 10GB in cloud mode."})
    public static int autobucket_partition_size_per_bucket_gb = -1;

    @ConfField(mutable = true, masterOnly = true, description = {
            "If the new partition bucket number calculated by auto bucketing exceeds this percentage "
                    + "of the previous partition's bucket number, "
                    + "it is considered an abnormal case and triggers an alert."})
    public static double autobucket_out_of_bounds_percent_threshold = 0.5;

    @ConfField(description = {
            "(Deprecated, replaced by arrow_flight_max_connection) The cache limit of all user tokens in "
                    + "Arrow Flight Server, which will be eliminated by LRU rules after exceeding the limit. "
                    + "Arrow Flight SQL is a stateless protocol; the connection is usually not actively disconnected. "
                    + "A bearer token evicted from the cache will unregister its ConnectContext."})
    public static int arrow_flight_token_cache_size = 4096;

    @ConfField(description = {
            "The alive time of the user token in Arrow Flight Server (expire after write), in seconds. "
                    + "The default value is 86400, which is 1 day."})
    public static int arrow_flight_token_alive_time_second = 86400;

    @ConfField(mutable = true, description = {
            "To ensure compatibility with the MySQL ecosystem, Doris includes a built-in database called mysql. "
                    + "If this database conflicts with a user's own database, please modify this field to replace "
                    + "the name of the Doris built-in MySQL database with a different name."})
    public static String mysqldb_replace_name = "mysql";

    @ConfField(description = {"Set the specific domain name that allows cross-domain access. "
            + "By default, any domain name is allowed cross-domain access."})
    public static String access_control_allowed_origin_domain = "*";

    @ConfField(description = {
            "Used to enable Java UDF. Default is true. If this configuration is false, creation and use of Java UDF is "
                    + "disabled. In some scenarios it may be necessary to disable this configuration to prevent "
                    + "command injection attacks."})
    public static boolean enable_java_udf = true;

    @ConfField(mutable = true, masterOnly = true, description = {
            "When enabled, data can be processed using the globally created Java UDF function during import. "
                    + "The default setting is false."})
    public static boolean enable_udf_in_load = false;

    @ConfField(description = {
            "Used to enable Python UDF. Default is true. If this configuration is false, "
                    + "creation and use of Python UDF is disabled. "
                    + "In some scenarios it may be necessary to disable this configuration to prevent "
                    + "command injection attacks."})
    public static boolean enable_python_udf = true;

    @ConfField(description = {"Whether to ignore unknown modules in Image file. "
            + "If true, metadata modules not in PersistMetaModules.MODULE_NAMES "
            + "will be ignored and skipped. Default is false, if Image file contains unknown modules, "
            + "Doris will throw exception. "
            + "This parameter is mainly used in downgrade operation, "
            + "old version can be compatible with new version Image file."})
    public static boolean ignore_unknown_metadata_module = false;

    @ConfField(mutable = true, description = {
            "The timeout for FE Follower/Observer synchronizing an image file from the FE Master. Can be adjusted "
                    + "based on the size of the image file in ${meta_dir}/image and the network environment between "
                    + "nodes. The default value is 300."})
    public static int sync_image_timeout_second = 300;

    @ConfField(mutable = true, description = {
            "The batch size (in bytes) when loading the binary content of a module from the "
                    + "image file into a byte array and deserializing it into a UTF-8 encoded string "
                    + "when FE starts. A value of -1 means reading the entire byte array at once and "
                    + "then deserializing it into a UTF-8 encoded string; any other value means reading "
                    + "a certain size (at least 16MB) of byte array in batches, deserializing each into a "
                    + "UTF-8 encoded string, and then merging them into a complete string. The default value is -1."})
    public static int metadata_text_read_max_batch_bytes = -1;

    @ConfField(mutable = true, masterOnly = true)
    public static int publish_topic_info_interval_ms = 30000; // 30s

    @ConfField(mutable = true)
    public static int workload_sched_policy_interval_ms = 10000; // 10s

    @ConfField(mutable = true, masterOnly = true)
    public static int workload_group_check_interval_ms = 2000; // 2s

    @ConfField(mutable = true, masterOnly = true)
    public static int workload_max_policy_num = 25;

    @ConfField(mutable = true, masterOnly = true)
    public static int workload_max_condition_num_in_policy = 5;

    @ConfField(mutable = true, masterOnly = true)
    public static int workload_max_action_num_in_policy = 5; // mainly used to limit set session var action

    @ConfField(mutable = true)
    public static int workload_runtime_status_thread_interval_ms = 2000;

    // NOTE: it should bigger than be config report_query_statistics_interval_ms
    @ConfField(mutable = true)
    public static int query_audit_log_timeout_ms = 5000;

    @ConfField(description = {"The operations of the users in this list will not be recorded in the audit log. "
            + "Multiple users are separated by commas."})
    public static String skip_audit_user_list = "";

    @ConfField(mutable = true)
    public static int be_report_query_statistics_timeout_ms = 60000;

    @ConfField(mutable = true, masterOnly = true)
    public static int workload_group_max_num = 15;

    @ConfField(description = {"The timeout threshold for checking the WAL queue on BE, in milliseconds."})
    public static int check_wal_queue_timeout_threshold = 180000;   // 3 min

    @ConfField(mutable = true, masterOnly = true, description = {
            "For auto-partitioned tables to prevent users from accidentally creating a large number of partitions, "
                    + "the number of partitions allowed per OLAP table is `max_auto_partition_num`. Default 20000."})
    public static int max_auto_partition_num = 20000;

    @ConfField(mutable = true, masterOnly = true, description = {
            "The maximum difference in the number of tablets of each BE in partition rebalance mode. "
                    + "If it is less than this value, it will be diagnosed as balanced."})
    public static int diagnose_balance_max_tablet_num_diff = 50;

    @ConfField(mutable = true, masterOnly = true, description = {
            "The maximum ratio of the number of tablets in each BE in partition rebalance mode. "
                    + "If it is less than this value, it will be diagnosed as balanced."})
    public static double diagnose_balance_max_tablet_num_ratio = 1.1;

    @ConfField(masterOnly = true, description = {
            "Set root user initial 2-staged SHA-1 encrypted password, default as '', means no root password. "
                    + "Subsequent `set password` operations for root user will overwrite the initial root password. "
                    + "Example: If you want to configure a plaintext password `root@123`."
                    + "You can execute Doris SQL `select password('root@123')` to generate encrypted "
                    + "password `*A00C34073A26B40AB4307650BFB9309D6BFA6999`"})
    public static String initial_root_password = "";

    @ConfField(description = {"The path of the nereids trace file."})
    public static String nereids_trace_log_dir = System.getenv("LOG_DIR") + "/nereids_trace";

    @ConfField(mutable = true, masterOnly = true, description = {
            "The maximum number of snapshots assigned to an upload task during the backup process. "
                    + "The default value is 10."})
    public static int backup_upload_snapshot_batch_size = 10;

    @ConfField(mutable = true, masterOnly = true, description = {
            "The maximum number of snapshots assigned to a download task during the restore process. "
                    + "The default value is 10."})
    public static int restore_download_snapshot_batch_size = 10;

    @ConfField(mutable = true, masterOnly = true, description = {
            "The maximum number of batched tasks per RPC assigned to each BE during the backup/restore process. "
                    + "The default value is 10000."})
    public static int backup_restore_batch_task_num_per_rpc = 10000;

    @ConfField(mutable = true, masterOnly = true, description = {"The number of concurrent restore tasks per BE."})
    public static int restore_task_concurrency_per_be = 5000;

    @ConfField(mutable = true, description = {
            "The time after which a BE is considered unavailable if no heartbeat is received."})
    public static int agent_task_be_unavailable_heartbeat_timeout_second = 300;

    @ConfField(description = {"Whether to enable the function of getting log files through the HTTP interface."})
    public static boolean enable_get_log_file_api = false;

    @ConfField(mutable = true)
    public static boolean enable_profile_when_analyze = false;
    @ConfField(mutable = true)
    public static boolean enable_collect_internal_query_profile = false;

    @ConfField(mutable = false, masterOnly = false, description = {
            "The maximum number of worker threads for the HTTP SQL submitter."})
    public static int http_sql_submitter_max_worker_threads = 2;

    @ConfField(mutable = false, masterOnly = false, description = {
            "The maximum number of worker threads for the HTTP upload submitter."})
    public static int http_load_submitter_max_worker_threads = 2;

    @ConfField(mutable = true, masterOnly = true, description = {
            "The threshold of load labels' number. After this number is exceeded, "
                    + "the labels of the completed import jobs or tasks will be deleted, "
                    + "and the deleted labels can be reused. "
                    + "When the value is -1, it indicates no threshold."})
    public static int label_num_threshold = 2000;

    @ConfField(description = {"Specify the default authentication class of internal catalog"},
            options = {"default", "ranger-doris"})
    public static String access_controller_type = "default";

    /* https://forums.oracle.com/ords/apexds/post/je-log-checksumexception-2812
      when meeting disk damage or other reason described in the oracle forums
      and fe cannot start due to `com.sleepycat.je.log.ChecksumException`, we
      add a param `ignore_bdbje_log_checksum_read` to ignore the exception, but
      there is no guarantee of correctness for bdbje kv data
    */
    @ConfField
    public static boolean ignore_bdbje_log_checksum_read = false;

    @ConfField(description = {
            "Specifies the primary MySQL authenticator name, either a built-in authenticator "
                    + "or an authentication plugin name"},
            options = {"default", "password", "ldap", "<plugin_name>"})
    public static String authentication_type = "default";

    @ConfField(mutable = true, description = {
            "Specifies the authentication chain used after primary authentication failure, "
                    + "multiple integration names are comma-separated"})
    public static String authentication_chain = "";

    @ConfField(mutable = true, masterOnly = false, description = {
            "Specify the default plugins loading path for the trino-connector catalog"})
    public static String trino_connector_plugin_dir = EnvUtils.getDorisHome() + "/plugins/connectors";

    @ConfField(mutable = true)
    public static boolean fix_tablet_partition_id_eq_0 = false;

    @ConfField(mutable = true, masterOnly = true, description = {
            "Default storage format of inverted index, the default value is V3."})
    public static String inverted_index_storage_format = "V3";

    @ConfField(mutable = true, masterOnly = true, description = {
            "Enable the 'delete predicate' for DELETE statements. If enabled, it will enhance the performance of "
                    + "DELETE statements, but partial column updates after a DELETE may result in erroneous data. "
                    + "If disabled, it will reduce the performance of DELETE statements to ensure accuracy."})
    public static boolean enable_mow_light_delete = false;

    @ConfField(description = {"Whether to enable proxy protocol"})
    public static boolean enable_proxy_protocol = false;

    @ConfField(description = {
            "Profile async collect expire time. After the query is completed, if the profile is not collected within "
                    + "the time specified by this parameter, the uncompleted profile will be abandoned."})
    public static int profile_async_collect_expire_time_secs = 5;

    @ConfField(description = {"Used to control the interval time of ProfileManager for profile garbage collection."})
    public static int profile_manager_gc_interval_seconds = 1;
    // Used to check compatibility when upgrading.
    @ConfField
    public static boolean enable_check_compatibility_mode = false;

    // Do checkpoint after replaying edit logs.
    @ConfField
    public static boolean checkpoint_after_check_compatibility = false;

    // Advance the next id before transferring to the master.
    @ConfField(description = {"Whether to advance the ID generator after becoming Master to ensure that the id "
            + "generator will not be rolled back even when metadata is rolled back."})
    public static boolean enable_advance_next_id = true;

    // The count threshold to do manual GC when doing checkpoint but not enough memory.
    // Set zero to disable it.
    @ConfField(description = {"The threshold to do manual GC when doing checkpoint but not enough memory"})
    public static int checkpoint_manual_gc_threshold = 0;

    @ConfField(mutable = true, description = {
            "Whether to log the request content before each request starts, specifically the query statements."})
    public static boolean enable_print_request_before_execution = false;

    @ConfField
    public static String spilled_profile_storage_path = System.getenv("LOG_DIR") + File.separator + "profile";

    @ConfField
    public static String spilled_minidump_storage_path = System.getenv("LOG_DIR") + File.separator + "minidump";

    // The max number of profiles that can be stored to storage.
    @ConfField
    public static int max_spilled_profile_num = 500;

    // The total size of profiles that can be stored to storage.
    @ConfField
    public static long spilled_profile_storage_limit_bytes = 1 * 1024 * 1024 * 1024; // 1GB

    // Profile will be spilled to storage after query has finished for this time.
    @ConfField(mutable = true, description = {
            "Profile will be spilled to storage after the query has been finished for this duration."})
    public static int profile_waiting_time_for_spill_seconds = 10;

    // Enable profile archive feature. When enabled, profiles exceeding storage limits
    // will be archived to compressed ZIP files instead of being directly deleted.
    @ConfField(mutable = true, description = {
            "Enable profile archive feature. When enabled, profiles exceeding storage limits "
                    + "will be archived to compressed ZIP files instead of being directly deleted."})
    public static boolean enable_profile_archive = true;

    // Number of profiles to include in each archive ZIP file.
    // Recommended value: 1000
    @ConfField(mutable = true, description = {"Number of profiles per archive ZIP file. Recommended: 1000"})
    public static int profile_archive_batch_size = 1000;

    // Storage path for archived profiles.
    // If empty, defaults to ${spilled_profile_storage_path}/archive
    @ConfField(description = {
            "Storage path for archived profiles. Defaults to ${spilled_profile_storage_path}/archive if empty."})
    public static String profile_archive_path = "";

    // Retention period for archive files in seconds.
    // -1: keep forever
    // 0: disable archiving (equivalent to enable_profile_archive = false)
    // >0: delete archives older than specified seconds (e.g., 604800 = 30 days)
    @ConfField(mutable = true, description = {
            "Retention period for archive files in seconds. -1 for unlimited, 0 to disable archiving."})
    public static int profile_archive_retention_seconds = 28800; // 8 hours

    // Maximum waiting time for pending archive files in seconds.
    // If the oldest file in pending directory exceeds this time, archive will be forced
    // even if the batch size is not reached.
    @ConfField(mutable = true, description = {"Maximum waiting time for pending archive files in seconds. "
            + "Forces archive even if the batch is not full."})
    public static int profile_archive_pending_timeout_seconds = 3600; // 1 hours

    @ConfField(mutable = true, description = {"Whether to abort transactions by checking coordinator BE heartbeat."})
    public static boolean enable_abort_txn_by_checking_coordinator_be = true;

    @ConfField(mutable = true, description = {
            "Whether to abort transactions by checking conflict transactions in schema change."})
    public static boolean enable_abort_txn_by_checking_conflict_txn = true;

    @ConfField(mutable = true, description = {
            "Columns that have not been collected within the specified interval will trigger automatic analyze. "
                    + "0 means not trigger."})
    public static long auto_analyze_interval_seconds = 86400; // 24 hours.

    // A internal config to control whether to enable the checkpoint.
    //
    // ATTN: it only used in test environment.
    @ConfField(mutable = true, masterOnly = true)
    public static boolean enable_checkpoint = true;

    @ConfField(description = {"The default directory for storing hadoop conf configuration files."})
    public static String hadoop_config_dir = EnvUtils.getDorisHome() + "/plugins/hadoop_conf/";

    @ConfField(mutable = true, masterOnly = true, description = {"Timeout for dictionary-related RPCs."})
    public static int dictionary_rpc_timeout_seconds = 5;

    @ConfField(mutable = true, masterOnly = true, description = {
            "Interval at which the dictionary triggers a data expiration check, in seconds."})
    public static int dictionary_auto_refresh_interval_seconds = 5;

    @ConfField(mutable = false, masterOnly = false, description = {
            "Whether to enable the experimental Table Stream functionality" },
            varType = VariableAnnotation.EXPERIMENTAL)
    public static boolean enable_table_stream = false;

    //==========================================================================
    //                    begin of cloud config
    //==========================================================================
    @ConfField(description = {"Whether to enable the FE log file deletion policy based on size, "
            + "where logs exceeding the specified size are deleted. "
            + "It is disabled by default and follows a time-based deletion policy."},
            options = {"age", "size"})
    public static String log_rollover_strategy = "age";

    @ConfField public static int info_sys_accumulated_file_size = 4;
    @ConfField public static int warn_sys_accumulated_file_size = 2;
    @ConfField public static int audit_sys_accumulated_file_size = 4;

    @ConfField
    public static String deploy_mode = "";

    // compatibily with elder version.
    // cloud_unique_id has higher priority than cluster_id.
    @ConfField
    public static String cloud_unique_id = "";

    public static boolean isCloudMode() {
        return deploy_mode.equals("cloud") || !cloud_unique_id.isEmpty();
    }

    public static boolean isNotCloudMode() {
        return !isCloudMode();
    }

    /**
     * MetaService endpoint, ip:port, such as meta_service_endpoint = "192.0.0.10:8866"
     *
     * If you want to access a group of meta services, separated the endpoints by comma,
     * like "host-1:port,host-2:port".
     */
    @ConfField(mutable = true, callback = CommaSeparatedIntersectConfHandler.class)
    public static String meta_service_endpoint = "";

    @ConfField(mutable = true)
    public static boolean meta_service_connection_pooled = true;

    @ConfField(mutable = true)
    public static int meta_service_connection_pool_size = 20;

    @ConfField(mutable = true)
    public static int meta_service_rpc_retry_times = 200;

    public static int metaServiceRpcRetryTimes() {
        if (isCloudMode() && enable_check_compatibility_mode) {
            return 1;
        }
        return meta_service_rpc_retry_times;
    }

    // A connection will expire after a random time during [base, 2*base), so that the FE
    // has a chance to connect to a new RS. Set zero to disable it.
    @ConfField(mutable = true)
    public static int meta_service_connection_age_base_minutes = 5;

    @ConfField(mutable = false)
    public static boolean enable_sts_vpc = true;

    @ConfField(mutable = true)
    public static int sts_duration = 3600;

    @ConfField(mutable = true)
    public static int drop_rpc_retry_num = 200;

    @ConfField
    public static int default_get_version_from_ms_timeout_second = 3;

    @ConfField(mutable = true)
    public static boolean enable_cloud_multi_replica = false;

    @ConfField(mutable = true)
    public static int cloud_replica_num = 3;

    @ConfField(mutable = true)
    public static int cloud_cold_read_percent = 10; // 10%

    @ConfField(mutable = true)
    public static int get_tablet_stat_batch_size = 1000;

    @ConfField(mutable = true, masterOnly = true)
    public static boolean enable_light_index_change = true;

    // The original meta read lock is not enough to keep a snapshot of partition versions,
    // so the execution of `createScanRangeLocations` are delayed to `Coordinator::exec`,
    // to help to acquire a snapshot of partition versions.
    @ConfField
    public static boolean enable_cloud_snapshot_version = true;

    // Interval in seconds for checking the status of compute groups (cloud clusters).
    // Compute groups and cloud clusters refer to the same concept.
    @ConfField
    public static int cloud_cluster_check_interval_second = 10;

    @ConfField
    public static String cloud_sql_server_cluster_name = "RESERVED_CLUSTER_NAME_FOR_SQL_SERVER";

    @ConfField
    public static String cloud_sql_server_cluster_id = "RESERVED_CLUSTER_ID_FOR_SQL_SERVER";

    @ConfField
    public static int cloud_txn_tablet_batch_size = 50;

    /**
     * Default number of waiting copy jobs for the whole cluster
     */
    @ConfField(mutable = true)
    public static int cluster_max_waiting_copy_jobs = 100;

    /**
     * Default number of max file num for per copy into job
     */
    @ConfField(mutable = true)
    public static int max_file_num_per_copy_into_job = 50;

    /**
     * Default number of max meta size for per copy into job
     */
    @ConfField(mutable = true)
    public static int max_meta_size_per_copy_into_job = 51200;

    // 0 means no limit
    @ConfField(mutable = true)
    public static int cloud_max_copy_job_per_table = 10000;

    @ConfField(mutable = true)
    public static int cloud_filter_copy_file_num_limit = 100;

    @ConfField(mutable = true, masterOnly = true)
    public static boolean cloud_delete_loaded_internal_stage_files = false;

    @ConfField(mutable = false)
    public static int cloud_copy_txn_conflict_error_retry_num = 5;

    @ConfField(mutable = false)
    public static int cloud_copy_into_statement_submitter_threads_num = 64;

    @ConfField
    public static int drop_user_notify_ms_max_times = 86400;

    @ConfField(mutable = true, masterOnly = true)
    public static long cloud_tablet_rebalancer_interval_second = 1;

    @ConfField(mutable = true, masterOnly = true)
    public static boolean enable_cloud_partition_balance = true;

    @ConfField(mutable = true, masterOnly = true)
    public static boolean enable_cloud_table_balance = true;

    @ConfField(mutable = true, masterOnly = true)
    public static boolean enable_cloud_global_balance = true;

    @ConfField(mutable = true, masterOnly = true)
    public static int cloud_pre_heating_time_limit_sec = 300;

    @ConfField(mutable = true, masterOnly = true)
    public static double cloud_rebalance_percent_threshold = 0.05;

    @ConfField(mutable = true, masterOnly = true)
    public static long cloud_rebalance_number_threshold = 2;

    @ConfField(mutable = true, masterOnly = true)
    public static double cloud_balance_tablet_percent_per_run = 0.05;

    @ConfField(mutable = true, masterOnly = true, description = {
            "Specify the scaling and warming methods for all compute groups in cloud mode. "
                    + "without_warmup: Directly modify shard mapping, first read from S3, "
                    + "fastest rebalance but largest fluctuation; "
                    + "async_warmup: Asynchronous warmup, best-effort cache pulling, "
                    + "faster rebalance but possible cache miss; "
                    + "sync_warmup: Synchronous warmup, ensure cache migration completion, "
                    + "slower rebalance but no cache miss; "
                    + "peer_read_async_warmup: Directly modify shard mapping, first read from peer BE, "
                    + "fastest rebalance but may affect other BEs in the same compute group's performance. "
                    + "Note: This is a global FE configuration. "
                    + "You can also use SQL (ALTER COMPUTE GROUP cg PROPERTIES) "
                    + "to set balance type at compute group level. "
                    + "Compute group level configuration has higher priority."},
            options = {"without_warmup", "async_warmup", "sync_warmup", "peer_read_async_warmup"})
    public static String cloud_warm_up_for_rebalance_type = "async_warmup";

    @ConfField(mutable = true, masterOnly = true, description = {"The maximum number of tablets per host "
            + "when batching warm-up requests during tablet rebalancing in "
            + "compute-storage separation mode. Default is 10."})
    public static int cloud_warm_up_batch_size = 10;

    @ConfField(mutable = true, masterOnly = true, description = {"Maximum wait time in milliseconds before a "
            + "pending warm-up batch is flushed. Default is 50ms."})
    public static int cloud_warm_up_batch_flush_interval_ms = 50;

    @ConfField(mutable = true, masterOnly = true, description = {
            "Thread pool size for asynchronous warm-up RPC dispatch during tablet "
                    + "rebalancing in compute-storage separation mode. Default is 4."})
    public static int cloud_warm_up_rpc_async_pool_size = 4;

    @ConfField(masterOnly = true, description = {"When tablets are being balanced in compute-storage separation mode, "
            + "whether to enable the active tablet priority scheduling strategy. Default is true."})
    public static boolean enable_cloud_active_tablet_priority_scheduling = true;

    @ConfField(masterOnly = true, description = {
            "Whether to enable active tablet sliding window access statistics feature. Default is true."})
    public static boolean enable_active_tablet_sliding_window_access_stats = true;

    @ConfField(mutable = true, masterOnly = true, description = {
            "Time window size in seconds for active tablet sliding window access statistics. "
                    + "Default is 3600 seconds (1 hour)."})
    public static long active_tablet_sliding_window_time_window_second = 3600L;

    @ConfField(mutable = true, masterOnly = true, description = {
            "When active tablet priority scheduling is enabled: partition-level scheduling processes TopN active "
                    + "partitions first, then other active partitions, "
                    + "then inactive partitions, and internal databases last. "
                    + "Default is 10000. <=0 disables TopN segmentation."})
    public static int cloud_active_partition_scheduling_topn = 10000;

    @ConfField(mutable = true, masterOnly = true, description = {
            "Refresh interval in seconds for the active-tablet snapshot when active priority scheduling is enabled. "
                    + "Default 60 seconds. Reuses the same active-tablet set within the interval."})
    public static long cloud_active_tablet_ids_refresh_interval_second = 60L;

    @ConfField(mutable = true, masterOnly = true, description = {
            "When active priority scheduling is enabled and the active phase remains unbalanced for N consecutive "
                    + "rounds, force one inactive phase round to avoid long-term starvation. "
                    + "Default 10. <=0 disables this forced mechanism."})
    public static int cloud_active_unbalanced_force_inactive_after_rounds = 10;

    @ConfField(mutable = true, masterOnly = false)
    public static String security_checker_class_name = "";

    @ConfField(mutable = true)
    public static int mow_calculate_delete_bitmap_retry_times = 10;

    @ConfField(mutable = true, description = {
            "The allowlist for S3 load endpoints. If it is empty, no allowlist will be set. "
                    + "For example: s3_load_endpoint_white_list=a,b,c"})
    public static String[] s3_load_endpoint_white_list = {};

    @ConfField(mutable = true, description = {
            "For deterministic S3 paths (without wildcards like *, ?), use HEAD requests instead of "
                    + "ListObjects to avoid requiring ListBucket permission. Brace patterns {1,2,3} and "
                    + "non-negated bracket patterns [abc] are expanded to concrete paths. This is useful when only "
                    + "GetObject permission is granted. Set to false to fall back to the original listing behavior."})
    public static boolean s3_skip_list_for_deterministic_path = true;

    @ConfField(mutable = true, description = {
            "Maximum number of expanded paths when using HEAD requests instead of ListObjects. "
                    + "If the expanded path count exceeds this limit, falls back to ListObjects. "
                    + "This prevents patterns like {1..100}/{1..100} from triggering too many HEAD requests."})
    public static int s3_head_request_max_paths = 100;
    @ConfField(mutable = true, description = {
            "The host suffix whitelist for Azure endpoints (both blob and dfs), separated by commas. "
                    + "The default value is .blob.core.windows.net,.dfs.core.windows.net,"
                    + ".blob.core.chinacloudapi.cn,.dfs.core.chinacloudapi.cn,"
                    + ".blob.core.usgovcloudapi.net,.dfs.core.usgovcloudapi.net,"
                    + ".blob.core.cloudapi.de,.dfs.core.cloudapi.de."})
    public static String[] azure_blob_host_suffixes = {
            ".blob.core.windows.net",
            ".dfs.core.windows.net",
            ".blob.core.chinacloudapi.cn",
            ".dfs.core.chinacloudapi.cn",
            ".blob.core.usgovcloudapi.net",
            ".dfs.core.usgovcloudapi.net",
            ".blob.core.cloudapi.de",
            ".dfs.core.cloudapi.de"
    };

    @ConfField(mutable = true, description = {
            "The allowlist for JDBC driver URLs. If it is empty, no allowlist will be set. "
                    + "For example: jdbc_driver_url_white_list=a,b,c"})
    public static String[] jdbc_driver_url_white_list = {};

    @ConfField(description = {"The maximum length of label in Stream Load is limited."})
    public static int label_regex_length = 128;

    @ConfField(mutable = true, masterOnly = true)
    public static int history_cloud_warm_up_job_keep_max_second = 7 * 24 * 3600;

    @ConfField(mutable = true, masterOnly = true)
    public static int max_active_cloud_warm_up_job = 10;

    @ConfField(mutable = true, masterOnly = true)
    public static int cloud_warm_up_timeout_second = 86400 * 30; // 30 days

    @ConfField(mutable = true, masterOnly = true)
    public static int cloud_warm_up_job_scheduler_interval_millisecond = 1000; // 1 seconds

    @ConfField(mutable = true, masterOnly = true)
    public static long cloud_warm_up_job_max_bytes_per_batch = 21474836480L; // 20GB

    @ConfField(mutable = true, masterOnly = true)
    public static boolean cloud_warm_up_force_all_partitions = false;

    @ConfField(mutable = true, masterOnly = true)
    public static boolean enable_fetch_cluster_cache_hotspot = true;

    @ConfField(mutable = true)
    public static long fetch_cluster_cache_hotspot_interval_ms = 3600000;
    // to control the max num of values inserted into cache hotspot internal table
    // insert into cache table when the size of batch values reaches this limit
    @ConfField(mutable = true)
    public static long batch_insert_cluster_cache_hotspot_num = 5000;

    /**
     * intervals between be status checks for CloudUpgradeMgr
     */
    @ConfField(mutable = true)
    public static int cloud_upgrade_mgr_interval_second = 15;

    @ConfField(mutable = true)
    public static boolean enable_cloud_running_txn_check = true;

    //* audit_event_log_queue_size = qps * query_audit_log_timeout_ms
    @ConfField(mutable = true)
    public static int audit_event_log_queue_size = 250000;

    @ConfField(description = {"Maximum size of the lineage event queue. Events will be discarded when exceeded."})
    public static int lineage_event_queue_size = 50000;

    @ConfField(mutable = true, description = {"Stream load route policy. Available options are "
            + "public-private/public/private/direct/random-be and empty string."})
    public static String streamload_redirect_policy = "";

    @ConfField(mutable = true, description = {
            "Whether to enable group commit streamload BE forward feature in cloud mode. "
                    + "Solves the issue where LB random forwarding breaks group commit batching "
                    + "by implementing BE-level forwarding to ensure same-table requests reach the same BE node."})
    public static boolean enable_group_commit_streamload_be_forward = false;

    @ConfField(description = {"When creating a table in cloud mode, check if recycler keys remain. Default is true."})
    public static boolean check_create_table_recycle_key_remained = true;

    @ConfField(mutable = true, description = {
            "Lock expiration time for FE requesting a lock from meta service in cloud mode. Default is 60s."})
    public static int delete_bitmap_lock_expiration_seconds = 60;

    @ConfField(mutable = true, description = {
            "Timeout for calculate delete bitmap task in cloud mode. Default is 60s."})
    public static int calculate_delete_bitmap_task_timeout_seconds = 60;

    @ConfField(mutable = true, description = {
            "Timeout for calculate delete bitmap task during transaction load in cloud mode. Default is 300s."})
    public static int calculate_delete_bitmap_task_timeout_seconds_for_transaction_load = 300;

    @ConfField(mutable = true, description = {"Lock wait timeout during commit phase in cloud mode. Default is 5s."})
    public static int try_commit_lock_timeout_seconds = 5;

    @ConfField(mutable = true, description = {"Whether to enable commit lock for all tables during transaction commit. "
            + "If true, commit lock will be applied to all tables. "
            + "If false, commit lock will only be applied to Merge-On-Write tables. "
            + "Default value is true."})
    public static boolean enable_commit_lock_for_all_tables = true;

    @ConfField(mutable = true, description = {
            "Whether to enable lazy commit for large transactions in cloud mode. Default is true."})
    public static boolean enable_cloud_txn_lazy_commit = true;

    @ConfField(mutable = true, masterOnly = true,
            description = {
                    "Whether to immediately reassign tablets to a new BE when the assigned BE is abnormal "
                            + "in cloud mode. Default is false."})
    public static boolean enable_immediate_be_assign = false;

    @ConfField(mutable = true, masterOnly = false,
            description = {
                    "Time in seconds after a BE goes down before its tablets are permanently reassigned "
                            + "to other BEs in cloud mode."})
    public static int rehash_tablet_after_be_dead_seconds = 3600;

    @ConfField(mutable = true, description = {
            "Whether to enable the automatic start-stop feature in cloud model, default is true."})
    public static boolean enable_auto_start_for_cloud_cluster = true;

    @ConfField(mutable = true, description = {
            "The automatic start-stop wait time for cluster wake-up backoff retry count in the cloud "
                    + "model is set to 300 times, which is approximately 5 minutes by default."})
    public static int auto_start_wait_to_resume_times = 300;

    @ConfField(description = {
            "Maximal concurrent num of master FE sync tablet stats task to observers and followers in cloud mode."})
    public static int cloud_sync_tablet_stats_task_threads_num = 4;

    @ConfField(mutable = true, description = {"Version of getting tablet stats in cloud mode. "
            + "Version 1: get all tablets; Version 2: get active and interval expired tablets"})
    public static int cloud_get_tablet_stats_version = 2;

    @ConfField(description = {"Maximum concurrent number of get tablet stat jobs."})
    public static int max_get_tablet_stat_task_threads_num = 4;

    @ConfField(description = {
            "Cloud table and partition version syncer interval. All frontends will perform the checking."})
    public static int cloud_version_syncer_interval_second = 20;

    @ConfField(mutable = true, description = {
            "Whether to enable the function of syncing table and partition version in cloud mode."})
    public static boolean cloud_enable_version_syncer = true;

    @ConfField(description = {"Concurrent number of get version tasks."})
    public static int cloud_get_version_task_threads_num = 4;

    @ConfField(description = {"Maximum concurrent number of sync version tasks between Master FE and other FEs."})
    public static int cloud_sync_version_task_threads_num = 4;

    @ConfField(mutable = true, description = {"Maximum table or partition batch size for get version tasks."})
    public static int cloud_get_version_task_batch_size = 2000;

    @ConfField(mutable = true, description = {
            "Whether to enable retry when a schema change job fails, default is true."})
    public static boolean enable_schema_change_retry = true;

    @ConfField(mutable = true, description = {"Max retry times when a schema change job fails, default is 3."})
    public static int schema_change_max_retry_time = 3;

    @ConfField(mutable = true, description = {"Whether to enable the use of ShowCacheHotSpotStmt, default is false."})
    public static boolean enable_show_file_cache_hotspot_stmt = false;

    @ConfField(mutable = true, description = {
            "Request timeout for FE connecting to meta service in cloud mode, default is 30000ms."})
    public static int meta_service_brpc_timeout_ms = 30000;

    @ConfField(mutable = true, description = {
            "Connection timeout for FE connecting to meta service in cloud mode. Default is 500ms."})
    public static int meta_service_brpc_connect_timeout_ms = 500;

    @ConfField(mutable = true, description = {
            "In cloud mode, the retry count when the FE request to meta service times out. Default is 1."})
    public static int meta_service_rpc_timeout_retry_times = 1;

    @ConfField(mutable = true, description = {
            "In cloud mode, the auto start and stop ignores the databases used by internal jobs, "
                    + "such as those used for statistics. "
                    + "For example: auto_start_ignore_db_names=__internal_schema, information_schema"})
    public static String[] auto_start_ignore_resume_db_names = {"__internal_schema", "information_schema"};

    @ConfField(mutable = true, masterOnly = true)
    public static boolean enable_mow_load_force_take_ms_lock = true;

    @ConfField(mutable = true, masterOnly = true)
    public static long mow_load_force_take_ms_lock_threshold_ms = 500;

    @ConfField(mutable = true, masterOnly = true)
    public static long mow_get_ms_lock_retry_backoff_base = 20;

    @ConfField(mutable = true, masterOnly = true)
    public static long mow_get_ms_lock_retry_backoff_interval = 80;

    @ConfField(mutable = true, masterOnly = true, description = {
            "Whether to enable TSO."}, varType = VariableAnnotation.EXPERIMENTAL)
    public static boolean enable_tso_feature = false;

    @ConfField(mutable = false, masterOnly = true, description = {
            "TSO service update interval in milliseconds. Default is 50, which means the TSO service "
                    + "will perform timestamp update checks every 50 milliseconds."})
    public static int tso_service_update_interval_ms = 50;

    @ConfField(mutable = true, masterOnly = true, description = {
            "TSO service max retry count. Default is 3, which means the TSO service will retry 3 times "
                    + "to update the global timestamp."})
    public static int tso_max_update_retry_count = 3;

    @ConfField(mutable = true, masterOnly = true, description = {
            "TSO get max retry count. Default is 10, which means the TSO service will retry 10 times "
                    + "to generate TSO."})
    public static int tso_max_get_retry_count = 10;

    @ConfField(mutable = true, masterOnly = true, description = {
            "TSO service time window in milliseconds. Default is 5000, which means the TSO service "
                    + "will apply for a TSO time window of 5000ms from BDBJE once."})
    public static int tso_service_window_duration_ms = 5000;

    @ConfField(mutable = true, masterOnly = true, description = {
            "Max tolerated clock backward threshold during TSO calibration in milliseconds. "
                    + "Exceeding this threshold will fail enabling TSO. Default is 30 minutes."})
    public static long tso_clock_backward_startup_threshold_ms = 30L * 60 * 1000;

    @ConfField(mutable = true, description = {
            "TSO service time offset in milliseconds. Only for test. Default is 0, which means the TSO service "
                    + "timestamp offset is 0 milliseconds."})
    public static int tso_time_offset_debug_mode = 0;

    @ConfField(mutable = true, masterOnly = true, description = {
            "Whether to enable persisting TSO window end into edit log. Enabling emits new op code, "
                    + "which may break rollback to older versions."})
    public static boolean enable_tso_persist_journal = false;

    @ConfField(mutable = true, masterOnly = true, description = {
            "Whether to include TSO info as an image module in checkpoint. Older versions may need to ignore "
                    + "unknown modules when reading new images."})
    public static boolean enable_tso_checkpoint_module = false;

    @ConfField(mutable = true, masterOnly = true, description = {
            "Whether to forward TSO 1ms when logical counter is nearly full. Default is true."})
    public static boolean enable_tso_forward_when_counter_full = true;

    @ConfField(mutable = true, masterOnly = true)
    public static boolean enable_notify_be_after_load_txn_commit = false;

    // ATTN: DONOT add any config not related to cloud mode here
    // ATTN: DONOT add any config not related to cloud mode here
    // ATTN: DONOT add any config not related to cloud mode here
    //==========================================================================
    //                      end of cloud config
    //==========================================================================
    //==========================================================================
    //                      start of lock config
    @ConfField(description = {"Whether to enable deadlock detection."})
    public static boolean enable_deadlock_detection = true;

    @ConfField(description = {"Deadlock detection interval time, in minutes."})
    public static long deadlock_detection_interval_minute = 5;

    @ConfField(mutable = true, description = {"Maximum lock hold time. Logs a warning if exceeded."})
    public static long max_lock_hold_threshold_seconds = 10;

    @ConfField(mutable = true, description = {"Whether metadata synchronization is enabled in safe mode."})
    public static boolean meta_helper_security_mode = false;

    @ConfField(description = {"Interval for checking if a resource is ready."})
    public static long resource_not_ready_sleep_seconds = 5;

    @ConfField(mutable = true, description = {
            "When set to true, if a query cannot select a healthy replica, "
                    + "detailed information of all replicas of the tablet will be printed."})
    public static boolean sql_block_rule_ignore_admin = false;

    @ConfField(description = {"Authentication plugin root directories. Use a comma-separated list to configure "
            + "multiple roots."})
    public static String authentication_plugins_dir = EnvUtils.getDorisHome() + "/plugins/authentication";

    @ConfField(description = {"Authorization plugin root directories. Use a comma-separated list to configure "
            + "multiple roots."})
    public static String authorization_plugins_dir = EnvUtils.getDorisHome() + "/plugins/authorization";

    @ConfField(description = {"Security plugin directory."})
    public static String security_plugins_dir = EnvUtils.getDorisHome() + "/plugins/security";

    @ConfField(description = {"Directory containing filesystem provider plugin subdirectories. "
            + "Each subdirectory is one storage backend (e.g., s3/, hdfs/, azure/). "
            + "If empty, only classpath-based built-in providers are used (test/dev mode)."})
    public static String filesystem_plugin_root = EnvUtils.getDorisHome() + "/plugins/filesystem";

    @ConfField(description = {"Directory containing connector provider plugin subdirectories. "
            + "Each subdirectory is one connector (e.g., es/, jdbc/, iceberg/). "
            + "If empty, only classpath-based built-in providers are used (test/dev mode)."})
    public static String connector_plugin_root = EnvUtils.getDorisHome() + "/plugins/connector";

    @ConfField(description = {"Authorization plugin configuration file path. Must be under DORIS_HOME. "
            + "Default is conf/authorization.conf."})
    public static String authorization_config_file_path = "/conf/authorization.conf";

    @ConfField(description = {"Authentication plugin configuration file path. Must be under DORIS_HOME. "
            + "Default is conf/authentication.conf."})
    public static String authentication_config_file_path = "/conf/authentication.conf";

    @ConfField(description = {"For testing purposes, all queries are forcibly forwarded to the master to verify "
            + "the behavior of forwarding queries."})
    public static boolean force_forward_all_queries = false;

    @ConfField(description = {
            "For disabling certain SQL queries, the configuration item is a list of simple class names of AST "
                    + "(for example CreateRepositoryStmt, CreatePolicyCommand), separated by commas."})
    public static String block_sql_ast_names = "";

    public static long meta_service_rpc_reconnect_interval_ms = 100;

    public static long meta_service_rpc_retry_cnt = 10;

    @ConfField(mutable = true, masterOnly = true, description = {
            "Whether to allow the use of inverted index v1 for variant."})
    public static boolean enable_inverted_index_v1_for_variant = false;

    @ConfField(mutable = true, description = {"Prometheus output table dimension metric count limit."})
    public static int prom_output_table_metrics_limit = 10000;


    @ConfField(mutable = true, masterOnly = true)
    public static long create_partition_wait_seconds = 300;

    @ConfField(mutable = true, description = {
            "The ID of the master key in KMS, used for generating and encrypting data keys"})
    public static String doris_tde_key_id = "";

    @ConfField(mutable = true, description = {"The endpoint of the KMS service, should match the region of the key"})
    public static String doris_tde_key_endpoint = "";

    @ConfField(mutable = true, description = {"The region where the KMS key is located, used for SDK configuration"})
    public static String doris_tde_key_region = "";

    @ConfField(mutable = true, description = {
            "The key provider for TDE (Transparent Data Encryption), currently supports aws_kms"})
    public static String doris_tde_key_provider = "";

    @ConfField(mutable = true, description = {
            "The encryption algorithm used for data. Default is AES256; may be set to empty later for KMS to decide."})
    public static String doris_tde_algorithm = "PLAINTEXT";

    @ConfField(mutable = true, description = {
            "The time interval for automatic rotation of the master key in data encryption, in milliseconds."
                    + "The default interval is one month."})
    public static long doris_tde_rotate_master_key_interval_ms = 30 * 24 * 3600 * 1000L;

    @ConfField(mutable = true, description = {
            "The interval at which data encryption checks whether to rotate the master key, in milliseconds. "
                    + "The default interval is five minutes."})
    public static long doris_tde_check_rotate_master_key_interval_ms = 5 * 60 * 1000L;

    @ConfField(mutable = true, description = {
            "The maximum length of the first row error message when data quality error occurs, default is 256 bytes"})
    public static int first_error_msg_max_length = 256;

    @ConfField(mutable = false, description = {
        "Whether to enable file cache admission control(Blocklist and Allowlist)"
    })
    public static boolean enable_file_cache_admission_control = false;

    @ConfField(mutable = false, description = {
        "Directory path for storing admission rules JSON files"
    })
    public static String file_cache_admission_control_json_dir = "";

    @ConfField
    public static String cloud_snapshot_handler_class = "org.apache.doris.cloud.snapshot.CloudSnapshotHandler";
    @ConfField
    public static int cloud_snapshot_handler_interval_second = 3600;
    @ConfField(mutable = true)
    public static long cloud_snapshot_timeout_seconds = 600;
    @ConfField(mutable = true)
    public static long cloud_auto_snapshot_max_reversed_num = 35;
    @ConfField(mutable = true)
    public static long cloud_auto_snapshot_min_interval_seconds = 3600;

    @ConfField(mutable = true, description = {"The minimum privilege required for cluster snapshot operations. "
            + "Valid values: 'root' (only root user can execute)"
            + " or 'admin' (users with ADMIN privilege can execute). "
            + "Default is 'root'."})
    public static String cluster_snapshot_min_privilege = "root";

    @ConfField(mutable = true)
    public static long multi_part_upload_part_size_in_bytes = 256 * 1024 * 1024L; // 256MB
    @ConfField(mutable = true)
    public static int multi_part_upload_max_seconds = 3600; // 1 hour
    @ConfField(mutable = true)
    public static int multi_part_upload_pool_size = 10;

    @ConfField(mutable = true)
    public static String aws_credentials_provider_version = "v2";

    @ConfField(mutable = true, description = {
            "The soft upper limit of FILE_CACHE percent that a single query of a user can use (range: 1 to 100).",
            "100 indicates that the full FILE_CACHE capacity can be used."})
    public static int file_cache_query_limit_max_percent = 100;
    @ConfField(description = {
            "The thread pool size used by the AWS SDK to schedule asynchronous retries, timeout tasks, "
                    + "and other background operations. Shared globally."})
    public static int aws_sdk_async_scheduler_thread_pool_size = 20;

    @ConfField(description = {
            "Agent tasks health check interval. Default is five minutes. "
                    + "No health check when less than or equal to 0."})
    public static long agent_task_health_check_intervals_ms = 5 * 60 * 1000L; // 5 min
    @ConfField(description = {
            "Whether to skip the FE internal catalog privilege check in catalog-level privilege validation. "
                    + "This only applies to SHOW/SELECT on external catalogs with a custom access controller. "
                    + "Internal catalogs, catalogs without a custom access controller, and other privileges such "
                    + "as CREATE/LOAD/ALTER are still validated by the default logic."})
    public static boolean skip_catalog_priv_check = false;

    @ConfField(mutable = true, description = {
            "In compute-storage separation mode, whether to obtain partition version information in batches when "
                    + "calculating the delete bitmap. Enabled by default."})
    public static boolean calc_delete_bitmap_get_versions_in_batch = true;

    @ConfField(mutable = true, description = {
            "In compute-storage separation mode, whether to wait for pending transactions to complete before "
                    + "obtaining partition version information when calculating the delete bitmap. Enabled "
                    + "by default."})
    public static boolean calc_delete_bitmap_get_versions_waiting_for_pending_txns = true;
}
