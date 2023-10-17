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

import org.apache.doris.common.ExperimentalUtil.ExperimentalType;

import java.util.concurrent.TimeUnit;

public class Config extends ConfigBase {

    @ConfField(description = {"用户自定义配置文件的路径，用于存放 fe_custom.conf。该文件中的配置会覆盖 fe.conf 中的配置",
            "The path of the user-defined configuration file, used to store fe_custom.conf. "
                    + "The configuration in this file will override the configuration in fe.conf"})
    public static String custom_config_dir = EnvUtils.getDorisHome() + "/conf";

    @ConfField(description = {"fe.log 和 fe.audit.log 的最大文件大小。超过这个大小后，日志文件会被切分",
            "The maximum file size of fe.log and fe.audit.log. After exceeding this size, the log file will be split"})
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
    @ConfField(description = {"FE 日志文件的存放路径，用于存放 fe.log。",
            "The path of the FE log file, used to store fe.log"})
    public static String sys_log_dir = System.getenv("DORIS_HOME") + "/log";

    @ConfField(description = {"FE 日志的级别", "The level of FE log"}, options = {"INFO", "WARN", "ERROR", "FATAL"})
    public static String sys_log_level = "INFO";

    @ConfField(description = {"FE 日志的输出模式，其中 NORMAL 为默认的输出模式，日志同步输出且包含位置信息，"
            + "BRIEF 模式是日志同步输出但不包含位置信息，ASYNC 模式是日志异步输出且不包含位置信息，三种日志输出模式的性能依次递增",
            "The output mode of FE log, and NORMAL mode is the default output mode, which means the logs are "
                    + "synchronized and contain location information. BRIEF mode is synchronized and does not contain"
                    + " location information. ASYNC mode is asynchronous and does not contain location information."
                    + " The performance of the three log output modes increases in sequence"},
            options = {"NORMAL", "BRIEF", "ASYNC"})
    public static String sys_log_mode = "NORMAL";

    @ConfField(description = {"FE 日志文件的最大数量。超过这个数量后，最老的日志文件会被删除",
            "The maximum number of FE log files. After exceeding this number, the oldest log file will be deleted"})
    public static int sys_log_roll_num = 10;

    @ConfField(description = {
            "Verbose 模块。VERBOSE 级别的日志是通过 log4j 的 DEBUG 级别实现的。"
                    + "如设置为 `org.apache.doris.catalog`，则会打印这个 package 下的类的 DEBUG 日志。",
            "Verbose module. The VERBOSE level log is implemented by the DEBUG level of log4j. "
                    + "If set to `org.apache.doris.catalog`, "
                    + "the DEBUG log of the class under this package will be printed."})
    public static String[] sys_log_verbose_modules = {};
    @ConfField(description = {"FE 日志文件的切分周期", "The split cycle of the FE log file"}, options = {"DAY", "HOUR"})
    public static String sys_log_roll_interval = "DAY";
    @ConfField(description = {
            "FE 日志文件的最大存活时间。超过这个时间后，日志文件会被删除。支持的格式包括：7d, 10h, 60m, 120s",
            "The maximum survival time of the FE log file. After exceeding this time, the log file will be deleted. "
                    + "Supported formats include: 7d, 10h, 60m, 120s"})
    public static String sys_log_delete_age = "7d";
    @ConfField(description = {"是否压缩 FE 的历史日志", "enable compression for FE log file"})
    public static boolean sys_log_enable_compress = false;

    @ConfField(description = {"FE 审计日志文件的存放路径，用于存放 fe.audit.log。",
            "The path of the FE audit log file, used to store fe.audit.log"})
    public static String audit_log_dir = System.getenv("DORIS_HOME") + "/log";
    @ConfField(description = {"FE 审计日志文件的最大数量。超过这个数量后，最老的日志文件会被删除",
            "The maximum number of FE audit log files. "
                    + "After exceeding this number, the oldest log file will be deleted"})
    public static int audit_log_roll_num = 90;
    @ConfField(description = {"FE 审计日志文件的种类", "The type of FE audit log file"},
            options = {"slow_query", "query", "load", "stream_load"})
    public static String[] audit_log_modules = {"slow_query", "query", "load", "stream_load"};
    @ConfField(mutable = true, description = {"慢查询的阈值，单位为毫秒。如果一个查询的响应时间超过这个阈值，"
            + "则会被记录在 audit log 中。",
            "The threshold of slow query, in milliseconds. "
                    + "If the response time of a query exceeds this threshold, it will be recorded in audit log."})
    public static long qe_slow_log_ms = 5000;
    @ConfField(description = {"FE 审计日志文件的切分周期", "The split cycle of the FE audit log file"},
            options = {"DAY", "HOUR"})
    public static String audit_log_roll_interval = "DAY";
    @ConfField(description = {
            "FE 审计日志文件的最大存活时间。超过这个时间后，日志文件会被删除。支持的格式包括：7d, 10h, 60m, 120s",
            "The maximum survival time of the FE audit log file. "
                    + "After exceeding this time, the log file will be deleted. "
                    + "Supported formats include: 7d, 10h, 60m, 120s"})
    public static String audit_log_delete_age = "30d";
    @ConfField(description = {"是否压缩 FE 的 Audit 日志", "enable compression for FE audit log file"})
    public static boolean audit_log_enable_compress = false;

    @ConfField(description = {"插件的安装目录", "The installation directory of the plugin"})
    public static String plugin_dir = System.getenv("DORIS_HOME") + "/plugins";

    @ConfField(mutable = true, masterOnly = true, description = {"是否启用插件", "Whether to enable the plugin"})
    public static boolean plugin_enable = true;

    @ConfField(description = {
            "JDBC 驱动的存放路径。在创建 JDBC Catalog 时，如果指定的驱动文件路径不是绝对路径，则会在这个目录下寻找",
            "The path to save jdbc drivers. When creating JDBC Catalog,"
                    + "if the specified driver file path is not an absolute path, Doris will find jars from this path"})
    public static String jdbc_drivers_dir = System.getenv("DORIS_HOME") + "/jdbc_drivers";

    @ConfField(mutable = true, masterOnly = true, description = {"broker load 时，单个节点上 load 执行计划的默认并行度",
            "The default parallelism of the load execution plan on a single node when the broker load is submitted"})
    public static int default_load_parallelism = 1;

    @ConfField(mutable = true, masterOnly = true, description = {
            "已完成或取消的导入作业信息的 label 会在这个时间后被删除。被删除的 label 可以被重用。",
            "Labels of finished or cancelled load jobs will be removed after this time"
                    + "The removed labels can be reused."})
    public static int label_keep_max_second = 3 * 24 * 3600; // 3 days

    @ConfField(mutable = true, masterOnly = true, description = {
            "针对一些高频的导入作业，比如 INSERT, STREAMING LOAD, ROUTINE_LOAD_TASK, DELETE"
                    + "如果导入作业或者任务已经完成，且超过这个时间后，会被删除。被删除的作业或者任务可以被重用。",
            "For some high frequency load jobs such as INSERT, STREAMING LOAD, ROUTINE_LOAD_TASK, DELETE"
                    + "Remove the finished job or task if expired. The removed job or task can be reused."})
    public static int streaming_label_keep_max_second = 43200; // 12 hour

    @ConfField(mutable = true, masterOnly = true, description = {
            "针对 ALTER, EXPORT 作业，如果作业已经完成，且超过这个时间后，会被删除。",
            "For ALTER, EXPORT jobs, remove the finished job if expired."})
    public static int history_job_keep_max_second = 7 * 24 * 3600; // 7 days

    @ConfField(description = {"事务的清理周期，单位为秒。每个周期内，将会清理已经结束的并且过期的历史事务信息",
            "The clean interval of transaction, in seconds. "
                    + "In each cycle, the expired history transaction will be cleaned"})
    public static int transaction_clean_interval_second = 30;

    @ConfField(description = {"导入作业的清理周期，单位为秒。每个周期内，将会清理已经结束的并且过期的导入作业",
            "The clean interval of load job, in seconds. "
                    + "In each cycle, the expired history load job will be cleaned"})
    public static int label_clean_interval_second = 1 * 3600; // 1 hours

    @ConfField(description = {"元数据的存储目录", "The directory to save Doris meta data"})
    public static String meta_dir = System.getenv("DORIS_HOME") + "/doris-meta";

    @ConfField(description = {"临时文件的存储目录", "The directory to save Doris temp data"})
    public static String tmp_dir = System.getenv("DORIS_HOME") + "/temp_dir";

    @ConfField(description = {"元数据日志的存储类型。BDB: 日志存储在 BDBJE 中。LOCAL：日志存储在本地文件中（仅用于测试）",
            "The storage type of the metadata log. BDB: Logs are stored in BDBJE. "
                    + "LOCAL: logs are stored in a local file (for testing only)"}, options = {"BDB", "LOCAL"})
    public static String edit_log_type = "bdb";

    @ConfField(description = {"BDBJE 的端口号", "The port of BDBJE"})
    public static int edit_log_port = 9010;

    @ConfField(mutable = true, masterOnly = true, description = {
            "BDBJE 的日志滚动大小。当日志条目数超过这个值后，会触发日志滚动",
            "The log roll size of BDBJE. When the number of log entries exceeds this value, the log will be rolled"})
    public static int edit_log_roll_num = 50000;

    @ConfField(description = {"元数据同步的容忍延迟时间，单位为秒。如果元数据的延迟超过这个值，非主 FE 会停止提供服务",
            "The toleration delay time of meta data synchronization, in seconds. "
                    + "If the delay of meta data exceeds this value, non-master FE will stop offering service"})
    public static int meta_delay_toleration_second = 300;    // 5 min

    @ConfField(description = {"元数据日志的写同步策略。如果仅部署一个 Follower FE，"
            + "则推荐设置为 `SYNC`，如果有多个 Follower FE，则可以设置为 `WRITE_NO_SYNC`。"
            + "可参阅：http://docs.oracle.com/cd/E17277_02/html/java/com/sleepycat/je/Durability.SyncPolicy.html",
            "The sync policy of meta data log. If you only deploy one Follower FE, "
                    + "set this to `SYNC`. If you deploy more than 3 Follower FE, "
                    + "you can set this and the following `replica_sync_policy` to `WRITE_NO_SYNC`. "
                    + "See: http://docs.oracle.com/cd/E17277_02/html/java/com/sleepycat/je/Durability.SyncPolicy.html"},
            options = {"SYNC", "NO_SYNC", "WRITE_NO_SYNC"})
    public static String master_sync_policy = "SYNC"; // SYNC, NO_SYNC, WRITE_NO_SYNC

    @ConfField(description = {"同 `master_sync_policy`", "Same as `master_sync_policy`"},
            options = {"SYNC", "NO_SYNC", "WRITE_NO_SYNC"})
    public static String replica_sync_policy = "SYNC"; // SYNC, NO_SYNC, WRITE_NO_SYNC

    @ConfField(description = {"BDBJE 节点间同步策略，"
            + "可参阅：http://docs.oracle.com/cd/E17277_02/html/java/com/sleepycat/je/Durability.ReplicaAckPolicy.html",
            "The replica ack policy of bdbje. "
                    + "See: http://docs.oracle.com/cd/E17277_02/html/java/com/sleepycat/je/Durability.ReplicaAckPolicy.html"},
            options = {"ALL", "NONE", "SIMPLE_MAJORITY"})
    public static String replica_ack_policy = "SIMPLE_MAJORITY"; // ALL, NONE, SIMPLE_MAJORITY

    @ConfField(description = {"BDBJE 主从节点间心跳超时时间，单位为秒。默认值为 30 秒，与 BDBJE 的默认值相同。"
            + "如果网络不稳定，或者 Java GC 经常导致长时间的暂停，可以适当增大这个值，减少误报超时的概率",
            "The heartbeat timeout of bdbje between master and follower, in seconds. "
                    + "The default is 30 seconds, which is same as default value in bdbje. "
                    + "If the network is experiencing transient problems, "
                    + "of some unexpected long java GC annoying you, "
                    + "you can try to increase this value to decrease the chances of false timeouts"})
    public static int bdbje_heartbeat_timeout_second = 30;

    @ConfField(description = {"BDBJE 操作的锁超时时间，单位为秒。如果 FE 的 WARN 日志中出现大量的 LockTimeoutException，"
            + "可以适当增大这个值",
            "The lock timeout of bdbje operation, in seconds. "
                    + "If there are many LockTimeoutException in FE WARN log, you can try to increase this value"})
    public static int bdbje_lock_timeout_second = 5;

    @ConfField(description = {"BDBJE 主从节点间同步的超时时间，单位为秒。如果出现大量的 ReplicaWriteException，"
            + "可以适当增大这个值",
            "The replica ack timeout of bdbje between master and follower, in seconds. "
                    + "If there are many ReplicaWriteException in FE WARN log, you can try to increase this value"})
    public static int bdbje_replica_ack_timeout_second = 10;

    @ConfField(description = {"BDBJE 所需的空闲磁盘空间大小。如果空闲磁盘空间小于这个值，则BDBJE将无法写入。",
            "Amount of free disk space required by BDBJE. "
                    + "If the free disk space is less than this value, BDBJE will not be able to write."})
    public static int bdbje_reserved_disk_bytes = 1 * 1024 * 1024 * 1024; // 1G

    @ConfField(masterOnly = true, description = {"心跳线程池的线程数",
            "Num of thread to handle heartbeat events"})
    public static int heartbeat_mgr_threads_num = 8;

    @ConfField(masterOnly = true, description = {"心跳线程池的队列大小",
            "Queue size to store heartbeat task in heartbeat_mgr"})
    public static int heartbeat_mgr_blocking_queue_size = 1024;

    @ConfField(masterOnly = true, description = {"Agent任务线程池的线程数",
            "Num of thread to handle agent task in agent task thread-pool"})
    public static int max_agent_task_threads_num = 4096;

    @ConfField(description = {"BDBJE 重加入集群时，最多回滚的事务数。如果回滚的事务数超过这个值，"
            + "则 BDBJE 将无法重加入集群，需要手动清理 BDBJE 的数据。",
            "The max txn number which bdbje can rollback when trying to rejoin the group. "
                    + "If the number of rollback txn is larger than this value, "
                    + "bdbje will not be able to rejoin the group, and you need to clean up bdbje data manually."})
    public static int txn_rollback_limit = 100;

    @ConfField(description = {"优先使用的网络地址，如果 FE 有多个网络地址，"
            + "可以通过这个配置来指定优先使用的网络地址。"
            + "这是一个分号分隔的列表，每个元素是一个 CIDR 表示的网络地址",
            "The preferred network address. If FE has multiple network addresses, "
                    + "this configuration can be used to specify the preferred network address. "
                    + "This is a semicolon-separated list, "
                    + "each element is a CIDR representation of the network address"})
    public static String priority_networks = "";

    @ConfField(mutable = true, description = {"是否忽略元数据延迟，如果 FE 的元数据延迟超过这个阈值，"
            + "则非 Master FE 仍然提供读服务。这个配置可以用于当 Master FE 因为某些原因停止了较长时间，"
            + "但是仍然希望非 Master FE 可以提供读服务。",
            "If true, non-master FE will ignore the meta data delay gap between Master FE and its self, "
                    + "even if the metadata delay gap exceeds this threshold. "
                    + "Non-master FE will still offer read service. "
                    + "This is helpful when you try to stop the Master FE for a relatively long time for some reason, "
                    + "but still wish the non-master FE can offer read service."})
    public static boolean ignore_meta_check = false;

    @ConfField(description = {"非 Master FE 与 Master FE 的最大时钟偏差，单位为毫秒。"
            + "这个配置用于在非 Master FE 与 Master FE 之间建立 BDBJE 连接时检查时钟偏差，"
            + "如果时钟偏差超过这个阈值，则 BDBJE 连接会被放弃。",
            "The maximum clock skew between non-master FE to Master FE host, in milliseconds. "
                    + "This value is checked whenever a non-master FE establishes a connection to master FE via BDBJE. "
                    + "The connection is abandoned if the clock skew is larger than this value."})
    public static long max_bdbje_clock_delta_ms = 5000; // 5s

    @ConfField(description = {"是否启用所有 http 接口的认证",
            "Whether to enable all http interface authentication"}, expType = ExperimentalType.EXPERIMENTAL)
    public static boolean enable_all_http_auth = false;

    @ConfField(description = {"FE http 端口，目前所有 FE 的 http 端口必须相同",
            "Fe http port, currently all FE's http port must be same"})
    public static int http_port = 8030;

    @ConfField(description = {"FE https 端口，目前所有 FE 的 https 端口必须相同",
            "Fe https port, currently all FE's https port must be same"})
    public static int https_port = 8050;

    @ConfField(description = {"FE https 服务的 key store 路径",
            "The key store path of FE https service"})
    public static String key_store_path = System.getenv("DORIS_HOME")
            + "/conf/ssl/doris_ssl_certificate.keystore";

    @ConfField(description = {"FE https 服务的 key store 密码",
            "The key store password of FE https service"})
    public static String key_store_password = "";

    @ConfField(description = {"FE https 服务的 key store 类型",
            "The key store type of FE https service"})
    public static String key_store_type = "JKS";

    @ConfField(description = {"FE https 服务的 key store 别名",
            "The key store alias of FE https service"})
    public static String key_store_alias = "doris_ssl_certificate";

    @ConfField(description = {"是否启用 https，如果启用，http 端口将不可用",
            "Whether to enable https, if enabled, http port will not be available"},
            expType = ExperimentalType.EXPERIMENTAL)
    public static boolean enable_https = false;

    @ConfField(description = {"Jetty 的 acceptor 线程数。Jetty的线程架构模型很简单，分为三个线程池：acceptor、selector 和 worker。"
            + "acceptor 负责接受新的连接，然后交给 selector 处理HTTP报文协议的解包，最后由 worker 处理请求。"
            + "前两个线程池采用非阻塞模型，并且一个线程可以处理很多socket的读写，所以线程池的数量少。"
            + "对于大多数项目，只需要 1-2 个 acceptor 线程，2 到 4 个就足够了。Worker 的数量取决于应用的QPS和IO事件的比例。"
            + "越高QPS，或者IO占比越高，等待的线程越多，需要的线程总数越多。",
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
    @ConfField(description = {"Jetty 的 selector 线程数。", "The number of selector threads for Jetty."})
    public static int jetty_server_selectors = 4;
    @ConfField(description = {"Jetty 的 worker 线程数。0 表示使用默认线程池。",
            "The number of worker threads for Jetty. 0 means using the default thread pool."})
    public static int jetty_server_workers = 0;

    @ConfField(description = {"Jetty 的线程池的默认最小线程数。",
            "The default minimum number of threads for jetty."})
    public static int jetty_threadPool_minThreads = 20;
    @ConfField(description = {"Jetty 的线程池的默认最大线程数。",
            "The default maximum number of threads for jetty."})
    public static int jetty_threadPool_maxThreads = 400;

    @ConfField(description = {"Jetty 的最大 HTTP POST 大小，单位是字节，默认值是 100MB。",
            "The maximum HTTP POST size of Jetty, in bytes, the default value is 100MB."})
    public static int jetty_server_max_http_post_size = 100 * 1024 * 1024;

    @ConfField(description = {"Jetty 的最大 HTTP header 大小，单位是字节，默认值是 1MB。",
            "The maximum HTTP header size of Jetty, in bytes, the default value is 1MB."})
    public static int jetty_server_max_http_header_size = 1048576;

    @ConfField(description = {"是否禁用 mini load，默认禁用",
            "Whether to disable mini load, disabled by default"})
    public static boolean disable_mini_load = true;

    @ConfField(description = {"mysql nio server 的 backlog 数量。"
            + "如果调大这个值，则需同时调整 /proc/sys/net/core/somaxconn 的值",
            "The backlog number of mysql nio server. "
                    + "If you enlarge this value, you should enlarge the value in "
                    + "`/proc/sys/net/core/somaxconn` at the same time"})
    public static int mysql_nio_backlog_num = 1024;

    @ConfField(description = {"thrift client 的连接超时时间，单位是毫秒。0 表示不设置超时时间。",
            "The connection timeout of thrift client, in milliseconds. 0 means no timeout."})
    public static int thrift_client_timeout_ms = 0;

    @ConfField(description = {"thrift server 的 backlog 数量。"
            + "如果调大这个值，则需同时调整 /proc/sys/net/core/somaxconn 的值",
            "The backlog number of thrift server. "
                    + "If you enlarge this value, you should enlarge the value in "
                    + "`/proc/sys/net/core/somaxconn` at the same time"})
    public static int thrift_backlog_num = 1024;

    @ConfField(description = {"FE thrift server 的端口号", "The port of FE thrift server"})
    public static int rpc_port = 9020;

    @ConfField(description = {"FE MySQL server 的端口号", "The port of FE MySQL server"})
    public static int query_port = 9030;

    @ConfField(description = {"MySQL 服务的 IO 线程数", "The number of IO threads in MySQL service"})
    public static int mysql_service_io_threads_num = 4;

    @ConfField(description = {"MySQL 服务的最大任务线程数", "The max number of task threads in MySQL service"})
    public static int max_mysql_service_task_threads_num = 4096;

    @ConfField(description = {"BackendServiceProxy数量, 用于池化GRPC channel",
            "BackendServiceProxy pool size for pooling GRPC channels."})
    public static int backend_proxy_num = 48;

    @ConfField(description = {
            "集群 ID，用于内部认证。通常在集群第一次启动时，会随机生成一个 cluster id. 用户也可以手动指定。",
            "Cluster id used for internal authentication. Usually a random integer generated when master FE "
                    + "start at first time. You can also specify one."})
    public static int cluster_id = -1;

    @ConfField(description = {"集群 token，用于内部认证。",
            "Cluster token used for internal authentication."})
    public static String auth_token = "";

    @ConfField(mutable = true, masterOnly = true,
            description = {"创建单个 Replica 的最大超时时间，单位是秒。如果你要创建 m 个 tablet，每个 tablet 有 n 个 replica。"
                    + "则总的超时时间为 `m * n * tablet_create_timeout_second`",
                    "Maximal waiting time for creating a single replica, in seconds. "
                            + "eg. if you create a table with #m tablets and #n replicas for each tablet, "
                            + "the create table request will run at most "
                            + "(m * n * tablet_create_timeout_second) before timeout"})
    public static int tablet_create_timeout_second = 2;

    @ConfField(mutable = true, masterOnly = true, description = {"创建表的最小超时时间，单位是秒。",
            "Minimal waiting time for creating a table, in seconds."})
    public static int min_create_table_timeout_second = 30;

    @ConfField(mutable = true, masterOnly = true, description = {"创建表的最大超时时间，单位是秒。",
            "Maximal waiting time for creating a table, in seconds."})
    public static int max_create_table_timeout_second = 3600;

    @ConfField(mutable = true, masterOnly = true, description = {"导入 Publish 阶段的最大超时时间，单位是秒。",
            "Maximal waiting time for all publish version tasks of one transaction to be finished, in seconds."})
    public static int publish_version_timeout_second = 30; // 30 seconds

    @ConfField(mutable = true, masterOnly = true, description = {"导入 Publish 阶段的等待时间，单位是秒。超过此时间，"
            + "则只需每个tablet包含一个成功副本，则导入成功。值为 -1 时，表示无限等待。",
            "Waiting time for one transaction changing to \"at least one replica success\", in seconds."
            + "If time exceeds this, and for each tablet it has at least one replica publish successful, "
            + "then the load task will be successful." })
    public static int publish_wait_time_second = 300;

    @ConfField(mutable = true, masterOnly = true, description = {"提交事务的最大超时时间，单位是秒。"
            + "该参数仅用于事务型 insert 操作中。",
            "Maximal waiting time for all data inserted before one transaction to be committed, in seconds. "
                    + "This parameter is only used for transactional insert operation"})
    public static int commit_timeout_second = 30; // 30 seconds

    @ConfField(masterOnly = true, description = {"Publish 任务触发线程的执行间隔，单位是毫秒。",
            "The interval of publish task trigger thread, in milliseconds"})
    public static int publish_version_interval_ms = 10;

    @ConfField(description = {"thrift server 的最大 worker 线程数", "The max worker threads of thrift server"})
    public static int thrift_server_max_worker_threads = 4096;

    @ConfField(mutable = true, masterOnly = true, description = {"Delete 操作的最大超时时间，单位是秒。",
            "Maximal timeout for delete job, in seconds."})
    public static int delete_job_max_timeout_second = 300;

    @ConfField(description = {"load job 调度器的执行间隔，单位是秒。",
            "The interval of load job scheduler, in seconds."})
    public static int load_checker_interval_second = 5;

    @ConfField(description = {"spark load job 调度器的执行间隔，单位是秒。",
            "The interval of spark load job scheduler, in seconds."})
    public static int spark_load_checker_interval_second = 60;

    @ConfField(mutable = true, masterOnly = true, description = {"Broker load 的默认超时时间，单位是秒。",
            "Default timeout for broker load job, in seconds."})
    public static int broker_load_default_timeout_second = 14400; // 4 hour

    @ConfField(description = {"和 Broker 进程交互的 RPC 的超时时间，单位是毫秒。",
            "The timeout of RPC between FE and Broker, in milliseconds"})
    public static int broker_timeout_ms = 10000; // 10s

    @ConfField(description = {"主键高并发点查短路径超时时间。",
            "The timeout of RPC for high concurrenty short circuit query"})
    public static int point_query_timeout_ms = 10000; // 10s

    @ConfField(mutable = true, masterOnly = true, description = {"Insert load 的默认超时时间，单位是秒。",
            "Default timeout for insert load job, in seconds."})
    public static int insert_load_default_timeout_second = 14400; // 4 hour

    @ConfField(mutable = true, masterOnly = true, description = {"Stream load 的默认超时时间，单位是秒。",
            "Default timeout for stream load job, in seconds."})
    public static int stream_load_default_timeout_second = 86400 * 3; // 3days

    @ConfField(mutable = true, masterOnly = true, description = {"Stream load 的默认预提交超时时间，单位是秒。",
            "Default pre-commit timeout for stream load job, in seconds."})
    public static int stream_load_default_precommit_timeout_second = 3600; // 3600s

    @ConfField(mutable = true, masterOnly = true, description = {"Load 的最大超时时间，单位是秒。",
            "Maximal timeout for load job, in seconds."})
    public static int max_load_timeout_second = 259200; // 3days

    @ConfField(mutable = true, masterOnly = true, description = {"Stream load 的最大超时时间，单位是秒。",
            "Maximal timeout for stream load job, in seconds."})
    public static int max_stream_load_timeout_second = 259200; // 3days

    @ConfField(mutable = true, masterOnly = true, description = {"Load 的最小超时时间，单位是秒。",
            "Minimal timeout for load job, in seconds."})
    public static int min_load_timeout_second = 1; // 1s

    @ConfField(mutable = true, masterOnly = true, description = {"Hadoop load 的默认超时时间，单位是秒。",
            "Default timeout for hadoop load job, in seconds."})
    public static int hadoop_load_default_timeout_second = 86400 * 3; // 3 day

    @ConfField(description = {"Spark DPP 程序的版本", "Default spark dpp version"})
    public static String spark_dpp_version = "1.2-SNAPSHOT";

    @ConfField(mutable = true, masterOnly = true, description = {"Spark load 的默认超时时间，单位是秒。",
            "Default timeout for spark load job, in seconds."})
    public static int spark_load_default_timeout_second = 86400; // 1 day

    @ConfField(mutable = true, masterOnly = true, description = {"Spark Load 所使用的 Spark 程序目录",
            "Spark dir for Spark Load"})
    public static String spark_home_default_dir = System.getenv("DORIS_HOME") + "/lib/spark2x";

    @ConfField(description = {"Spark load 所使用的依赖项目录", "Spark dependencies dir for Spark Load"})
    public static String spark_resource_path = "";

    @ConfField(description = {"Spark launcher 日志路径", "Spark launcher log dir"})
    public static String spark_launcher_log_dir = sys_log_dir + "/spark_launcher_log";

    @ConfField(description = {"Yarn client 的路径", "Yarn client path"})
    public static String yarn_client_path = System.getenv("DORIS_HOME") + "/lib/yarn-client/hadoop/bin/yarn";

    @ConfField(description = {"Yarn 配置文件的路径", "Yarn config path"})
    public static String yarn_config_dir = System.getenv("DORIS_HOME") + "/lib/yarn-config";

    @ConfField(mutable = true, masterOnly = true, description = {"Sync job 的最大提交间隔，单位是秒。",
            "Maximal intervals between two sync job's commits."})
    public static long sync_commit_interval_second = 10;

    @ConfField(description = {"Sync job 调度器的执行间隔，单位是秒。",
            "The interval of sync job scheduler, in seconds."})
    public static int sync_checker_interval_second = 5;

    @ConfField(description = {"Sync job 的最大并发数。",
            "Maximal concurrent num of sync job."})
    public static int max_sync_task_threads_num = 10;

    @ConfField(mutable = true, masterOnly = true, description = {"Sync job 的最小提交事件数。如果收到的事件数小于该值，"
            + "Sync Job 会继续等待下一批数据，直到时间超过 `sync_commit_interval_second`。这个值应小于 canal 的缓冲区大小。",
            "Min events that a sync job will commit. When receiving events less than it, SyncJob will continue "
                    + "to wait for the next batch of data until the time exceeds `sync_commit_interval_second`."})
    public static long min_sync_commit_size = 10000;

    @ConfField(mutable = true, masterOnly = true, description = {"Sync job 的最小提交字节数。如果收到的字节数小于该值，"
            + "Sync Job 会继续等待下一批数据，直到时间超过 `sync_commit_interval_second`。这个值应小于 canal 的缓冲区大小。",
            "Min bytes that a sync job will commit. When receiving bytes less than it, SyncJob will continue "
                    + "to wait for the next batch of data until the time exceeds `sync_commit_interval_second`."})
    public static long min_bytes_sync_commit = 15 * 1024 * 1024; // 15 MB

    @ConfField(mutable = true, masterOnly = true, description = {"Sync job 的最大提交字节数。如果收到的字节数大于该值，"
            + "Sync Job 会立即提交所有数据。这个值应大于 canal 的缓冲区大小和 `min_bytes_sync_commit`。",
            "Max bytes that a sync job will commit. When receiving bytes larger than it, SyncJob will commit "
                    + "all data immediately. You should set it larger than canal memory and "
                    + "`min_bytes_sync_commit`."})
    public static long max_bytes_sync_commit = 64 * 1024 * 1024; // 64 MB

    @ConfField(mutable = true, masterOnly = true, description = {"Broker Load 的最大等待 job 数量。"
            + "这个值是一个期望值。在某些情况下，比如切换 master，当前等待的 job 数量可能会超过这个值。",
            "Maximal number of waiting jobs for Broker Load. This is a desired number. "
                    + "In some situation, such as switch the master, "
                    + "the current number is maybe more than this value."})
    public static int desired_max_waiting_jobs = 100;

    @ConfField(mutable = true, masterOnly = true, description = {"FE 从 BE 获取 Stream Load 作业信息的间隔。",
            "The interval of FE fetch stream load record from BE."})
    public static int fetch_stream_load_record_interval_second = 120;

    @ConfField(mutable = true, masterOnly = true, description = {"Stream load 的默认最大记录数。",
            "Default max number of recent stream load record that can be stored in memory."})
    public static int max_stream_load_record_size = 5000;

    @ConfField(mutable = true, masterOnly = true, description = {
            "是否禁用 show stream load 和 clear stream load 命令，以及是否清理内存中的 stream load 记录。",
            "Whether to disable show stream load and clear stream load records in memory."})
    public static boolean disable_show_stream_load = false;

    @ConfField(mutable = true, masterOnly = true, description = {
            "是否启用 stream load 和 broker load 的单副本写入。",
            "Whether to enable to write single replica for stream load and broker load."},
            expType = ExperimentalType.EXPERIMENTAL)
    public static boolean enable_single_replica_load = false;

    @ConfField(mutable = true, masterOnly = true, description = {
            "单个数据库最大并发运行的事务数，包括 prepare 和 commit 事务。",
            "Maximum concurrent running txn num including prepare, commit txns under a single db.",
            "Txn manager will reject coming txns."})
    public static int max_running_txn_num_per_db = 1000;

    @ConfField(masterOnly = true, description = {"pending load task 执行线程数。这个配置可以限制当前等待的导入作业数。"
            + "并且应小于 `max_running_txn_num_per_db`。",
            "The pending load task executor pool size. "
                    + "This pool size limits the max running pending load tasks.",
            "Currently, it only limits the pending load task of broker load and spark load.",
            "It should be less than `max_running_txn_num_per_db`"})
    public static int async_pending_load_task_pool_size = 10;

    @ConfField(masterOnly = true, description = {"loading load task 执行线程数。这个配置可以限制当前正在导入的作业数。",
            "The loading load task executor pool size. "
                    + "This pool size limits the max running loading load tasks.",
            "Currently, it only limits the loading load task of broker load."})
    public static int async_loading_load_task_pool_size = 10;

    @ConfField(mutable = true, masterOnly = true, description = {
            "和 `tablet_create_timeout_second` 含义相同，但是是用于 Delete 操作的。",
            "The same meaning as `tablet_create_timeout_second`, but used when delete a tablet."})
    public static int tablet_delete_timeout_second = 2;

    @ConfField(mutable = true, masterOnly = true, description = {
            "磁盘使用率的高水位线。用于计算 BE 的负载分数。",
            "The high water of disk capacity used percent. This is used for calculating load score of a backend."})
    public static double capacity_used_percent_high_water = 0.75;

    @ConfField(mutable = true, masterOnly = true, description = {
            "负载均衡时，磁盘使用率最大差值。",
            "The max diff of disk capacity used percent between BE. "
                    + "It is used for calculating load score of a backend."})
    public static double used_capacity_percent_max_diff = 0.30;

    @ConfField(mutable = true, masterOnly = true, description = {
            "设置固定的 BE 负载分数中磁盘使用率系数。BE 负载分数会综合磁盘使用率和副本数而得。有效值范围为[0, 1]，"
                    + "当超出此范围时，则使用其他方法自动计算此系数。",
            "Sets a fixed disk usage factor in the BE load fraction. The BE load score is a combination of disk usage "
                    + "and replica count. The valid value range is [0, 1]. When it is out of this range, other "
                    + "methods are used to automatically calculate this coefficient."})
    public static double backend_load_capacity_coeficient = -1.0;

    @ConfField(mutable = true, masterOnly = true, description = {
            "ALTER TABLE 请求的最大超时时间。设置的足够长以适应表的数据量。",
            "Maximal timeout of ALTER TABLE request. Set long enough to fit your table data size."})
    public static int alter_table_timeout_second = 86400 * 30; // 1month

    @ConfField(mutable = true, masterOnly = true, description = {
            "是否禁用存储介质检查。如果禁用，ReportHandler 将不会检查 tablet 的存储介质，"
                    + "并且禁用存储介质冷却功能。默认值为 false。",
            "When disable_storage_medium_check is true, ReportHandler would not check tablet's storage medium "
                    + "and disable storage cool down function."})
    public static boolean disable_storage_medium_check = false;

    @ConfField(description = {"创建表或分区时，可以指定存储介质(HDD 或 SSD)。如果未指定，"
            + "则使用此配置指定的默认介质。",
            "When create a table(or partition), you can specify its storage medium(HDD or SSD)."})
    public static String default_storage_medium = "HDD";

    @ConfField(mutable = true, masterOnly = true, description = {
            "删除数据库(表/分区)后，可以使用 RECOVER 语句恢复。此配置指定了数据的最大保留时间。"
                    + "超过此时间，数据将被永久删除。",
            "After dropping database(table/partition), you can recover it by using RECOVER stmt.",
            "And this specifies the maximal data retention time. After time, the data will be deleted permanently."})
    public static long catalog_trash_expire_second = 86400L; // 1day

    @ConfField(mutable = true, masterOnly = true, description = {
            "单个 broker scanner 读取的最小字节数。Broker Load 切分文件时，"
                    + "如果切分后的文件大小小于此值，将不会切分。",
            "Minimal bytes that a single broker scanner will read. When splitting file in broker load, "
                    + "if the size of split file is less than this value, it will not be split."})
    public static long min_bytes_per_broker_scanner = 67108864L; // 64MB

    @ConfField(mutable = true, masterOnly = true, description = {
            "单个 broker scanner 的最大并发数。", "Maximal concurrency of broker scanners."})
    public static int max_broker_concurrency = 10;

    @ConfField(mutable = true, masterOnly = true, description = {
            "导出作业的最大并发数。", "Limitation of the concurrency of running export jobs."})
    public static int export_running_job_num_limit = 5;

    @ConfField(mutable = true, masterOnly = true, description = {
            "导出作业的默认超时时间。", "Default timeout of export jobs."})
    public static int export_task_default_timeout_second = 2 * 3600; // 2h

    @ConfField(mutable = true, masterOnly = true, description = {
            "每个导出作业的需要处理的 tablet 数量。", "Number of tablets need to be handled per export job."})
    public static int export_tablet_num_per_task = 5;

    // TODO(cmy): Disable by default because current checksum logic has some bugs.
    @ConfField(mutable = true, masterOnly = true, description = {
            "一致性检查的开始时间。与 `consistency_check_end_time` 配合使用，决定一致性检查的起止时间。"
                    + "如果将两个参数设置为相同的值，则一致性检查将不会被调度。",
            "Start time of consistency check. Used with `consistency_check_end_time` "
                    + "to decide the start and end time of consistency check. "
                    + "If set to the same value, consistency check will not be scheduled."})
    public static String consistency_check_start_time = "23";
    @ConfField(mutable = true, masterOnly = true, description = {
            "一致性检查的结束时间。与 `consistency_check_start_time` 配合使用，决定一致性检查的起止时间。"
                    + "如果将两个参数设置为相同的值，则一致性检查将不会被调度。",
            "End time of consistency check. Used with `consistency_check_start_time` "
                    + "to decide the start and end time of consistency check. "
                    + "If set to the same value, consistency check will not be scheduled."})
    public static String consistency_check_end_time = "23";

    @ConfField(mutable = true, masterOnly = true, description = {
            "单个一致性检查任务的默认超时时间。设置的足够长以适应表的数据量。",
            "Default timeout of a single consistency check task. Set long enough to fit your tablet size."})
    public static long check_consistency_default_timeout_second = 600; // 10 min

    @ConfField(description = {"单个 FE 的 MySQL Server 的最大连接数。",
            "Maximal number of connections of MySQL server per FE."})
    public static int qe_max_connection = 1024;

    @ConfField(description = {"MySQL 连接调度线程池的最大线程数。",
            "Maximal number of thread in MySQL connection-scheduler-pool."})
    public static int max_connection_scheduler_threads_num = 4096;

    @ConfField(mutable = true, description = {"Colocate join 每个 instance 的内存 penalty 系数。"
            + "计算方式：`exec_mem_limit / min (query_colocate_join_memory_limit_penalty_factor, instance_num)`",
            "Colocate join PlanFragment instance memory limit penalty factor.",
            "The memory_limit for colocote join PlanFragment instance = "
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
    public static long storage_flood_stage_left_capacity_bytes = 1 * 1024 * 1024 * 1024; // 1GB

    // update interval of tablet stat
    // All frontends will get tablet stat from all backends at each interval
    @ConfField public static int tablet_stat_update_interval_second = 60;  // 1 min

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
    @ConfField(mutable = true, masterOnly = false, description = {"SQL/Partition Cache可以缓存的最大行数。",
        "Maximum number of rows that can be cached in SQL/Partition Cache, is 3000 by default."})
    public static int cache_result_max_row_count = 3000;

    /**
     * Set the maximum data size that can be cached
     */
    @ConfField(mutable = true, masterOnly = false, description = {"SQL/Partition Cache可以缓存的最大数据大小。",
        "Maximum data size of rows that can be cached in SQL/Partition Cache, is 3000 by default."})
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
     * num of thread to handle grpc events in grpc_threadmgr
     */
    @ConfField
    public static int grpc_threadmgr_threads_nums = 4096;

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

    /*
     * if true, will allow the system to collect statistics automatically
     */
    @ConfField(mutable = true, masterOnly = true)
    public static boolean enable_auto_collect_statistics = true;

    /*
     * the system automatically checks the time interval for statistics
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int auto_check_statistics_in_minutes = 5;

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
    public static boolean enable_quantile_state_type = true;

    @ConfField
    public static boolean enable_pipeline_load = false;

    // enable_workload_group should be immutable and temporarily set to mutable during the development test phase
    @ConfField(mutable = true, masterOnly = true, expType = ExperimentalType.EXPERIMENTAL)
    public static boolean enable_workload_group = false;

    @ConfField(mutable = true)
    public static boolean enable_query_queue = true;

    @ConfField(mutable = true)
    public static boolean disable_shared_scan = false;

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
    public static boolean enable_multi_tags = false;

    /**
     * If set to TRUE, FE will convert DecimalV2 to DecimalV3 automatically.
     */
    @ConfField(mutable = true)
    public static boolean enable_decimal_conversion = true;

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
    public static int max_be_exec_version = 3;

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

    /*
     * mtmv is still under dev, remove this config when it is graduate.
     */
    @ConfField(mutable = true, masterOnly = true, expType = ExperimentalType.EXPERIMENTAL)
    public static boolean enable_mtmv = false;

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

    @ConfField(mutable = true, masterOnly = true)
    public static boolean keep_scheduler_mtmv_task_when_job_deleted = false;

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
     * Used to determined how many statistics collection SQL could run simultaneously.
     */
    @ConfField
    public static int statistics_simultaneously_running_task_num = 10;

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

    @ConfField(mutable = false, masterOnly = false, description = {"Hive表到分区名列表缓存的最大数量。",
        "Max cache number of hive table to partition names list."})
    public static long max_hive_table_cache_num = 1000;

    @ConfField(mutable = false, masterOnly = false, description = {"获取Hive分区值时候的最大返回数量，-1代表没有限制。",
        "Max number of hive partition values to return while list partitions, -1 means no limitation."})
    public static short max_hive_list_partition_num = -1;

    @ConfField(mutable = false, masterOnly = false, description = {"远程文件系统缓存的最大数量",
        "Max cache number of remote file system."})
    public static long max_remote_file_system_cache_num = 100;

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
    public static long external_cache_expire_time_minutes_after_access = 10; // 10 mins

    /**
     * Github workflow test type, for setting some session variables
     * only for certain test type. E.g. only settting batch_size to small
     * value for p0.
     */
    @ConfField(mutable = true, masterOnly = false)
    public static String fuzzy_test_type = "";

    /**
     * Set session variables randomly to check more issues in github workflow
     */
    @ConfField(mutable = true, masterOnly = false)
    public static boolean use_fuzzy_session_variable = false;

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
    @ConfField(mutable = false, expType = ExperimentalType.EXPERIMENTAL)
    public static boolean enable_fqdn_mode = false;

    /**
     * enable use odbc table
     */
    @ConfField(mutable = true, masterOnly = true, description = {
        "是否开启 ODBC 外表功能，默认关闭，ODBC 外表是淘汰的功能，请使用 JDBC Catalog",
        "Whether to enable the ODBC appearance function, it is disabled by default,"
            + " and the ODBC appearance is an obsolete feature. Please use the JDBC Catalog"})
    public static boolean enable_odbc_table = false;

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
    @ConfField(mutable = true, expType = ExperimentalType.EXPERIMENTAL)
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
    @ConfField(mutable = false, masterOnly = false, expType = ExperimentalType.EXPERIMENTAL)
    public static boolean enable_ssl = false;

    /**
     * If set to ture, ssl connection needs to authenticate client's certificate.
     */
    @ConfField(mutable = false, masterOnly = false)
    public static boolean ssl_force_client_auth = false;

    /**
     * Default CA certificate file location for mysql ssl connection.
     */
    @ConfField(mutable = false, masterOnly = false)
    public static String mysql_ssl_default_ca_certificate = System.getenv("DORIS_HOME")
            + "/mysql_ssl_default_certificate/ca_certificate.p12";

    /**
     * Default server certificate file location for mysql ssl connection.
     */
    @ConfField(mutable = false, masterOnly = false)
    public static String mysql_ssl_default_server_certificate = System.getenv("DORIS_HOME")
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
    public static String bdbje_file_logging_level = "ALL";

    /**
     * When holding lock time exceeds the threshold, need to report it.
     */
    @ConfField
    public static long lock_reporting_threshold_ms = 500L;

    /**
     * If false, when select from tables in information_schema database,
     * the result will not contain the information of the table in external catalog.
     * This is to avoid query time when external catalog is not reachable.
     * TODO: this is a temp solution, we should support external catalog in the future.
     */
    @ConfField(mutable = true)
    public static boolean infodb_support_ext_catalog = false;

    /**
     * If true, auth check will be disabled. The default value is false.
     * This is to solve the case that user forgot the password.
     */
    @ConfField(mutable = false)
    public static boolean skip_localhost_auth_check  = true;

    @ConfField(mutable = true)
    public static boolean enable_round_robin_create_tablet = false;

    /**
     * If set false, user couldn't submit analyze SQL and FE won't allocate any related resources.
     */
    @ConfField
    public static boolean enable_stats = true;

    /**
     * To prevent different types (V1, V2, V3) of behavioral inconsistencies,
     * we may delete the DecimalV2 and DateV1 types in the future.
     * At this stage, we use ‘disable_decimalv2’ and ‘disable_datev1’
     * to determine whether these two types take effect.
     */
    @ConfField(mutable = true)
    public static boolean disable_decimalv2  = true;

    @ConfField(mutable = true)
    public static boolean disable_datev1  = true;

    /**
     * Now we not fully support array/struct/map nesting complex type in many situation,
     * so just disable creating nesting complex data type when create table.
     * We can make it able after we fully support
     */
    @ConfField(mutable = true, masterOnly = true, description = {
            "当前默认设置为 true，不支持建表时创建复杂类型(array/struct/map)嵌套复杂类型, 仅支持array类型自身嵌套。",
            "Now default set to true, not support create complex type(array/struct/map) nested complex type "
                    + "when we create table, only support array type nested array"})
    public static boolean disable_nested_complex_type  = true;

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
    public static long stats_cache_size = 10_0000;

    /**
     * This configuration is used to enable the statistics of query information, which will record
     * the access status of databases, tables, and columns, and can be used to guide the
     * optimization of table structures
     *
     */
    @ConfField(mutable = true)
    public static boolean enable_query_hit_stats = false;

    @ConfField(mutable = true, description = {
            "设置为 true，如果查询无法选择到健康副本时，会打印出该tablet所有副本的详细信息，" + "以及不可查询的具体原因。",
            "When set to true, if a query is unable to select a healthy replica, "
                    + "the detailed information of all the replicas of the tablet,"
                    + " including the specific reason why they are unqueryable, will be printed out."})
    public static boolean show_details_for_unaccessible_tablet = false;

    @ConfField(mutable = false, masterOnly = false, expType = ExperimentalType.EXPERIMENTAL, description = {
            "是否启用binlog特性",
            "Whether to enable binlog feature"})
    public static boolean enable_feature_binlog = false;

    @ConfField
    public static int analyze_task_timeout_in_hours = 12;

    @ConfField(mutable = true, masterOnly = true, description = {
            "是否禁止使用 WITH REOSOURCE 语句创建 Catalog。",
            "Whether to disable creating catalog with WITH RESOURCE statement."})
    public static boolean disallow_create_catalog_with_resource = true;

    @ConfField(mutable = true, masterOnly = false, description = {
        "Hive行数估算分区采样数",
        "Sample size for hive row count estimation."})
    public static int hive_stats_partition_sample_size = 3000;

    @ConfField(mutable = true, masterOnly = true, description = {
            "用于强制设定内表的副本数，如果该参数大于零，则用户在建表时指定的副本数将被忽略，而使用本参数设置的值。"
                    + "同时，建表语句中指定的副本标签等参数会被忽略。该参数不影响包括创建分区、修改表属性的操作。该参数建议仅用于测试环境",
            "Used to force the number of replicas of the internal table. If the config is greater than zero, "
                    + "the number of replicas specified by the user when creating the table will be ignored, "
                    + "and the value set by this parameter will be used. At the same time, the replica tags "
                    + "and other parameters specified in the create table statement will be ignored. "
                    + "This config does not effect the operations including creating partitions "
                    + "and modifying table properties. "
                    + "This config is recommended to be used only in the test environment"})
    public static int force_olap_table_replication_num = 0;

    @ConfField
    public static long statistics_sql_mem_limit_in_bytes = 2L * 1024 * 1024 * 1024;

    @ConfField
    public static int statistics_sql_parallel_exec_instance_num = 1;

    @ConfField
    public static int cpu_resource_limit_per_analyze_task = 1;

    @ConfField(mutable = true, description = {
            "Export任务允许的最大分区数量",
            "The maximum number of partitions allowed by Export job"})
    public static int maximum_number_of_export_partitions = 2000;

    @ConfField(mutable = true, description = {
            "Export任务允许的最大并行数",
            "The maximum parallelism allowed by Export job"})
    public static int maximum_parallelism_of_export_job = 50;

    @ConfField(mutable = true, description = {
            "是否用 mysql 的 bigint 类型来返回 Doris 的 largeint 类型",
            "Whether to use mysql's bigint type to return Doris's largeint type"})
    public static boolean use_mysql_bigint_for_largeint = false;

    @ConfField(description = {
            "是否开启列权限",
            "Whether to enable col auth"})
    public static boolean enable_col_auth = false;

    @ConfField
    public static boolean forbid_running_alter_job = false;

    @ConfField
    public static int table_stats_health_threshold = 80;

    @ConfField(description = {
            "暂时性配置项，开启后会自动将所有的olap表修改为可light schema change",
            "temporary config filed, will make all olap tables enable light schema change"
    })
    public static boolean enable_convert_light_weight_schema_change = true;
    @ConfField(mutable = true, masterOnly = false, description = {
            "查询information_schema.metadata_name_ids表时,获取一个数据库中所有表用的时间",
            "When querying the information_schema.metadata_name_ids table,"
                    + " the time used to obtain all tables in one database"
    })
    public static long query_metadata_name_ids_timeout = 3;

    @ConfField(mutable = true, masterOnly = true, description = {
            "是否禁止LocalDeployManager删除节点",
            "Whether to disable LocalDeployManager drop node"})
    public static boolean disable_local_deploy_manager_drop_node = true;

    @ConfField(mutable = true, description = {
            "开启 file cache 后，一致性哈希算法中，每个节点的虚拟节点数。"
                    + "该值越大，哈希算法的分布越均匀，但是会增加内存开销。",
            "When file cache is enabled, the number of virtual nodes of each node in the consistent hash algorithm. "
                    + "The larger the value, the more uniform the distribution of the hash algorithm, "
                    + "but it will increase the memory overhead."})
    public static int virtual_node_number = 2048;

    @ConfField(description = {"控制对大表的自动ANALYZE的最小时间间隔，"
            + "在该时间间隔内大小超过huge_table_lower_bound_size_in_bytes的表仅ANALYZE一次",
            "This controls the minimum time interval for automatic ANALYZE on large tables. Within this interval,"
                    + "tables larger than huge_table_lower_bound_size_in_bytes are analyzed only once."})
    public static long huge_table_auto_analyze_interval_in_millis = TimeUnit.HOURS.toMillis(12);

    @ConfField(description = {"定义大表的大小下界，在开启enable_auto_sample的情况下，"
            + "大小超过该值的表将会自动通过采样收集统计信息", "This defines the lower size bound for large tables. "
            + "When enable_auto_sample is enabled, tables larger than this value will automatically collect "
            + "statistics through sampling"})
    public static long huge_table_lower_bound_size_in_bytes = 5L * 1024 * 1024 * 1024;

    @ConfField(description = {"定义开启开启大表自动sample后，对大表的采样比例",
            "This defines the number of sample percent for large tables when automatic sampling for"
                    + "large tables is enabled"})
    public static int huge_table_default_sample_rows = 4194304;

    @ConfField(description = {"是否开启大表自动sample，开启后对于大小超过huge_table_lower_bound_size_in_bytes会自动通过采样收集"
            + "统计信息", "Whether to enable automatic sampling for large tables, which, when enabled, automatically"
            + "collects statistics through sampling for tables larger than 'huge_table_lower_bound_size_in_bytes'"})
    public static boolean enable_auto_sample = false;

    @ConfField(description = {
            "控制统计信息的自动触发作业执行记录的持久化行数",
            "Determine the persist number of automatic triggered analyze job execution status"
    })
    public static long auto_analyze_job_record_count = 20000;

    @ConfField(description = {
            "Auto Buckets中最小的buckets数目",
            "min buckets of auto bucket"
    })
    public static int autobucket_min_buckets = 1;

    @ConfField(description = {
            "是否忽略 Image 文件中未知的模块。如果为 true，不在 PersistMetaModules.MODULE_NAMES 中的元数据模块将被忽略并跳过。"
                    + "默认为 false，如果 Image 文件中包含未知的模块，Doris 将会抛出异常。"
                    + "该参数主要用于降级操作中，老版本可以兼容新版本的 Image 文件。",
            "Whether to ignore unknown modules in Image file. "
                    + "If true, metadata modules not in PersistMetaModules.MODULE_NAMES "
                    + "will be ignored and skipped. Default is false, if Image file contains unknown modules, "
                    + "Doris will throw exception. "
                    + "This parameter is mainly used in downgrade operation, "
                    + "old version can be compatible with new version Image file."
    })
    public static boolean ignore_unknown_metadata_module = false;

}
