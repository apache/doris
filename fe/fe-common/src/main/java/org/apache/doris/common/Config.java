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
    @ConfField(description = {"FE 日志文件的存放路径，用于存放 fe.log。",
            "The path of the FE log file, used to store fe.log"})
    public static String sys_log_dir = System.getenv("DORIS_HOME") + "/log";

    @ConfField(description = {"FE 日志的级别", "The level of FE log"}, options = {"INFO", "WARNING", "ERROR", "FATAL"})
    public static String sys_log_level = "INFO";

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
    public static int bdbje_lock_timeout_second = 1;

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

    @ConfField(description = {"是否重置 BDBJE 的复制组，如果所有的可选节点都无法启动，"
            + "可以将元数据拷贝到另一个节点，并将这个配置设置为 true，尝试重启 FE。更多信息请参阅官网的元数据故障恢复文档。",
            "If true, FE will reset bdbje replication group(that is, to remove all electable nodes info) "
                    + "and is supposed to start as Master. "
                    + "If all the electable nodes can not start, we can copy the meta data "
                    + "to another node and set this config to true to try to restart the FE. "
                    + "For more information, please refer to the metadata failure recovery document "
                    + "on the official website."})
    public static String metadata_failure_recovery = "false";

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

    @ConfField(description = {"Jetty 的最大 HTTP header 大小，单位是字节，默认值是 10KB。",
            "The maximum HTTP header size of Jetty, in bytes, the default value is 10KB."})
    public static int jetty_server_max_http_header_size = 10240;

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
    public static int tablet_create_timeout_second = 1;

    @ConfField(mutable = true, masterOnly = true, description = {"创建表的最大超时时间，单位是秒。",
            "Maximal waiting time for creating a table, in seconds."})
    public static int max_create_table_timeout_second = 3600;

    @ConfField(mutable = true, masterOnly = true, description = {"导入 Publish 阶段的最大超时时间，单位是秒。",
            "Maximal waiting time for all publish version tasks of one transaction to be finished, in seconds."})
    public static int publish_version_timeout_second = 30; // 30 seconds

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

    @ConfField(mutable = true, masterOnly = true, description = {"Insert load 的默认超时时间，单位是秒。",
            "Default timeout for insert load job, in seconds."})
    public static int insert_load_default_timeout_second = 3600; // 1 hour

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
    public static String spark_dpp_version = "1.0.0";

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
    public static int max_running_txn_num_per_db = 100;

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

    @ConfField(mutable = true, masterOnly = true, description = {"设置为 true，则会禁止 Colocate 表的重分布和均衡操作。\n"
            + "注意：如果禁止，则 Colocate table 将无法自动修复。",
            "If set to true, the redistribution and balance operations of the Colocate table will be disabled.\n"
                    + "Note: If disabled, the Colocate table will not be automatically repaired."})
    public static boolean disable_colocate_balance = false;

    @ConfField(description = {"待补充", "TODO"})
    public static boolean proxy_auth_enable = false;
    @ConfField(description = {"待补充", "TODO"})
    public static String proxy_auth_magic_prefix = "x@8";

    @ConfField(mutable = true, description = {"限制表达式最大孩子节点的数量（比如 IN 表达式里的元素个数）。如果孩子节点数量多过，"
            + "则会导致SQL解析阶段耗时较长，并长时间持有元数据锁",
            "Limit on the number of expr children of an expr tree(such as elements num in InPredicate). "
                    + "Exceed this limit may cause long analysis time while holding database read lock."})
    public static int expr_children_limit = 10000;

    @ConfField(mutable = true, description = {
            "限制表达式的深度。如果深度过深，则会导致SQL解析阶段耗时较长，并长时间持有元数据锁。",
            "Limit on the depth of an expr tree. "
                    + "If too deep, it may cause long analysis time while holding metadata read lock."})
    public static int expr_depth_limit = 3000;

    // Configurations for hadoop dpp
    /**
     * The following configurations are not available.
     */
    @ConfField(description = {"已废弃", "Desprecated"})
    public static String dpp_hadoop_client_path = "/lib/hadoop-client/hadoop/bin/hadoop";
    @ConfField(description = {"已废弃", "Desprecated"})
    public static long dpp_bytes_per_reduce = 100 * 1024 * 1024L; // 100M
    @ConfField(description = {"已废弃", "Desprecated"})
    public static String dpp_default_cluster = "palo-dpp";
    @ConfField(description = {"已废弃", "Desprecated"})
    public static String dpp_default_config_str = ""
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
    @ConfField(description = {"已废弃", "Desprecated"})
    public static String dpp_config_str = ""
            + "{palo-dpp : {"
            + "hadoop_palo_path : '/dir',"
            + "hadoop_configs : '"
            + "fs.default.name=hdfs://host:port;"
            + "mapred.job.tracker=host:port;"
            + "hadoop.job.ugi=user,password"
            + "'}"
            + "}";

    @ConfField(description = {"是否使用内置的部署管理器。可选项包括：disable, k8s, ambari, local",
            "If use built-in deploy manager. Valid options are: disable, k8s, ambari, local"})
    public static String enable_deploy_manager = "disable";

    @ConfField(description = {"是否使用 k8s 部署管理器。如果使用，需要准备好 k8s 的证书文件。",
            "If use k8s deploy manager. If use, prepare the certs files."})
    public static boolean with_k8s_certs = false;

    @ConfField(description = {"设置执行命令时的语言环境。目前仅用于 Spark Load。",
            "Set runtime locale when exec some cmds. Only used for Spark Load now."})
    public static String locale = "zh_CN.UTF-8";

    @ConfField(mutable = true, masterOnly = true, description = {"备份任务的默认超时时间，单位为毫秒。",
            "Default timeout of backup job, in milliseconds."})
    public static int backup_job_default_timeout_ms = 86400 * 1000; // 1 day

    @ConfField(mutable = true, masterOnly = true, description = {"`storage_high_watermark_usage_percent` "
            + "和 `storage_min_left_capacity_bytes` 配合使用。分别表示最大磁盘使用率和最小磁盘剩余空间限制。\n"
            + "当其中一个达到阈值后，则对应的存储路径将不能作为副本均衡的目标路径。但对于副本修复操作，我们可能会超过这些限制，"
            + "以尽可能保证数据完整性。",
            "`storage_high_watermark_usage_percent` and `storage_min_left_capacity_bytes` work together. "
                    + "They represent the maximum disk usage and the minimum disk remaining space limit respectively. "
                    + "When one of them reaches the threshold, the corresponding storage path will not "
                    + "be used as the target path for replica balance. "
                    + "However, for replica repair operations, we may exceed these limits to ensure data integrity."})
    public static int storage_high_watermark_usage_percent = 85;
    @ConfField(mutable = true, masterOnly = true, description = {"`storage_high_watermark_usage_percent` "
            + "和 `storage_min_left_capacity_bytes` 配合使用。分别表示最大磁盘使用率和最小磁盘剩余空间限制。\n"
            + "当其中一个达到阈值后，则对应的存储路径将不能作为副本均衡的目标路径。但对于副本修复操作，我们可能会超过这些限制，"
            + "以尽可能保证数据完整性。",
            "`storage_high_watermark_usage_percent` and `storage_min_left_capacity_bytes` work together. "
                    + "They represent the maximum disk usage and the minimum disk remaining space limit respectively. "
                    + "When one of them reaches the threshold, the corresponding storage path will not "
                    + "be used as the target path for replica balance. "
                    + "However, for replica repair operations, we may exceed these limits to ensure data integrity."})
    public static long storage_min_left_capacity_bytes = 2 * 1024 * 1024 * 1024; // 2G

    @ConfField(mutable = true, masterOnly = true, description = {
            "`storage_flood_stage_usage_percent` 和 `storage_flood_stage_left_capacity_bytes` 配合使用。"
                    + "分别表示最大磁盘使用率上限和最小磁盘剩余空间下限。当其中一个达到阈值后，则导入和数据恢复操作将被禁止。",
            "`storage_flood_stage_usage_percent` and `storage_flood_stage_left_capacity_bytes` work together. "
                    + "They represent the maximum disk usage and the minimum disk remaining space limit respectively. "
                    + "When one of them reaches the threshold, "
                    + "the load and restore operations will be prohibited."})
    public static int storage_flood_stage_usage_percent = 95;
    @ConfField(mutable = true, masterOnly = true, description = {
            "`storage_flood_stage_usage_percent` 和 `storage_flood_stage_left_capacity_bytes` 配合使用。"
                    + "分别表示最大磁盘使用率上限和最小磁盘剩余空间下限。当其中一个达到阈值后，则导入和数据恢复操作将被禁止。",
            "`storage_flood_stage_usage_percent` and `storage_flood_stage_left_capacity_bytes` work together. "
                    + "They represent the maximum disk usage and the minimum disk remaining space limit respectively. "
                    + "When one of them reaches the threshold, "
                    + "the load and restore operations will be prohibited."})
    public static long storage_flood_stage_left_capacity_bytes = 1 * 1024 * 1024 * 1024; // 100MB

    @ConfField(description = {"FE 从 BE 获取 Tablet 统计信息的间隔时间。所有 FE 都会定期向所有 BE 获取 Tablet 信息。"
            + "主要用于行数和数据量统计", "Interval of FE get tablet stat from BE. All FE will get tablet stat from "
            + "all BE at each interval. Mainly used for row count and data size statistics"})
    public static int tablet_stat_update_interval_second = 60;  // 1 min

    @ConfField(mutable = true, masterOnly = true, description = {"Broker Load 任务中，单个扫描任务所允许的最大数据扫描量。"
            + "此参数用于避免一次导入任务导入过多的数据而增加失败重试的成本。",
            "Max bytes a broker scanner can process in one broker load job. This parameter is used to avoid "
                    + "loading too much data at one time and increase the cost of failure retry."})
    public static long max_bytes_per_broker_scanner = 3 * 1024 * 1024 * 1024L; // 3G

    @ConfField(mutable = true, description = {"如果设置为True，优化器会选择和 FE 节点同机的本地副本进行读取。这可能会降低网络传输的开销并优化读取效率。适用以下场景：\n"
            + "1. N 个节点，每个节点都部署了 FE 和 BE。\n"
            + "2. 数据有 N 副本，即每个节点都有一份完整的数据。\n"
            + "3. 高并发查询场景，并且查询会均匀的路由到所有 FE 节点。",
            "If set to True, the optimizer will choose the local copy on the same machine as the FE node for reading. "
                    + "This may reduce the overhead of network transfers and optimize read efficiency. "
                    + "Applies to the following scenarios:\n"
                    + "1. N nodes, each node is deployed with FE and BE.\n"
                    + "2. There are N copies of the data, that is, each node has a complete copy of the data.\n"
                    + "3. High concurrent query scenario, and the query will be evenly routed to all FE nodes."})
    public static boolean enable_local_replica_selection = false;

    @ConfField(mutable = true, description = {"当 `enable_local_replica_selection` 为 true 但无法访问到本地数据副本时，"
            + "是否允许读取远端副本。", "When `enable_local_replica_selection` is true "
            + "but the local data replica cannot be accessed, whether to allow reading the remote replica."})
    public static boolean enable_local_replica_selection_fallback = false;

    @ConfField(mutable = true, description = {
            "查询自动重试次数。仅当查询失败，且没有数据返回给客户端时，查询才会自动重试。调大这个值可能引起雪崩",
            "The number of query retries. A query may retry if no result has been sent to user. "
                    + "You may reduce this number to avoid Avalanche disaster."})
    public static int max_query_retry_time = 1;

    @ConfField(mutable = true, description = {
            "点查询场景下，查询自动重试次数。仅当查询失败，且没有数据返回给客户端时，查询才会自动重试。调大这个值可能引起雪崩",
            "For point query, the number of query retries. A query may retry if no result has been sent to user. "
                    + "You may reduce this number to avoid Avalanche disaster."})
    public static int max_point_query_retry_time = 2;

    @ConfField(mutable = true, description = {"元数据锁的 try lock 超时时长。",
            " The tryLock timeout configuration of catalog lock."})
    public static long catalog_try_lock_timeout_ms = 5000; // 5 sec

    @ConfField(mutable = true, masterOnly = true, description = {"是否禁止加载任务。如果设置为 true，"
            + "所有 pending 状态的加载任务在调用 begin txn api 时会失败，所有 prepare 状态的加载任务在调用 commit txn api 时会失败，"
            + "所有 committed 状态的加载任务会等待被发布。", "Whether to disable load job. If set to true, "
            + "all pending load jobs will fail when calling begin txn api, "
            + "all prepare load jobs will fail when calling commit txn api, "
            + "and all committed load jobs will wait to be published."})
    public static boolean disable_load_job = false;

    @ConfField(masterOnly = true, description = {"用于更新每个数据库已使用的数据量配额的线程的调度间隔。",
            "The scheduling interval for the thread used to update the amount of data quota used by each database."})
    public static int db_used_data_quota_update_interval_secs = 300;

    @ConfField(description = {"FE 从 ES 获取 ES 索引分片信息的间隔时间。所有 FE 都会定期向所有 ES 获取 ES 索引分片信息。"
            + "主要用于 ES 索引分片信息的同步",
            "Interval of FE get es index shard info from ES. All FE will get es index shard info from "
                    + "all ES at each interval. Mainly used for es index shard info sync"})
    public static long es_state_sync_interval_second = 10;

    /**
     * fe will create iceberg table every iceberg_table_creation_interval_second
     */
    @ConfField(mutable = true, masterOnly = true, description = {"已废弃", "Deprecated"})
    public static long iceberg_table_creation_interval_second = 10;

    @ConfField(mutable = true, masterOnly = true, description = {"修复 tablet 之前的延迟因子。"
            + "如果优先级是 VERY_HIGH，立即修复。"
            + "HIGH，延迟 `tablet_repair_delay_factor_second` * 1；"
            + "NORMAL: 延迟 `tablet_repair_delay_factor_second` * 2；"
            + "LOW: 延迟 `tablet_repair_delay_factor_second` * 3。增加这个数值，可以避免不必要的副本修复操作",
            "The delay factor before repairing the tablet. "
                    + "If the priority is VERY_HIGH, repair it immediately. "
                    + "HIGH, delay `tablet_repair_delay_factor_second` * 1; "
                    + "NORMAL: delay `tablet_repair_delay_factor_second` * 2; "
                    + "LOW: delay `tablet_repair_delay_factor_second` * 3. "
                    + "Increasing this value can avoid unnecessary replica repair operations"})
    public static long tablet_repair_delay_factor_second = 60;

    @ConfField(description = {"副本修复操作中，每个存储路径对应的slot数量。每个副本修复任务，会占用源端和目的端各一个slot。"
            + "如果没有空闲的slot，则任务会排队。一个BE上的slot数量等于这个数值乘以存储路径数量。调大该数值可以提高副本修复的速度，"
            + "但会增加集群的资源开销。",
            " The number of slots per storage path in the replica repair operation. "
                    + "Each replica repair task occupies one slot on the source and destination. "
                    + "If there is no free slot, the task will be queued. "
                    + "The number of slots on a BE is equal to this value multiplied by the number of storage paths. "
                    + "Increasing this value can improve the speed of replica repair, "
                    + "but it will increase the resource cost of the cluster."})
    public static int schedule_slot_num_per_path = 2;

    @ConfField(mutable = true, masterOnly = true, description = {"计算集群节点间负载分数的差值阈值，只有在分数差距大于这个阈值时，"
            + "才会触发副本均衡任务。同时，以默认值10%为例，如果一个节点的负载分数高于平均分数的10%，则会被标记为高负载，如果低于10%，"
            + "则会标记为低负载，否则标记位均衡。副本会自动从高负载节点迁移到低负载节点。",
            " The threshold of the difference between the load scores of BE nodes. "
                    + "Only when the score difference is greater than this threshold, "
                    + "the replica balance task will be triggered. "
                    + "At the same time, taking the default value of 10% as an example, "
                    + "if the load score of a node is 10% higher than the average score, "
                    + "it will be marked as high load, if it is lower than 10%, "
                    + "it will be marked as low load, otherwise it will be marked as balanced. "
                    + "Replicas will automatically migrate from high load nodes to low load nodes."})
    public static double balance_load_score_threshold = 0.1; // 10%

    @ConfField(mutable = true, masterOnly = true, description = {
            "如果设置为true，TabletScheduler将不会进行BE间的负载均衡。",
            "If set to true, TabletScheduler will not do balance between BE nodes"})
    public static boolean disable_balance = false;

    @ConfField(mutable = true, masterOnly = true, description = {
            "如果设置为true，TabletScheduler将不会进行BE内的磁盘均衡。",
            "If set to true, TabletScheduler will not do disk balance in BE node"})
    public static boolean disable_disk_balance = false;

    @ConfField(mutable = true, masterOnly = true, description = {"如果TabletScheduler中的调度任务数超过这个值，"
            + "则不再进行调度。这个值越大，可调度的任务数越多，但是会增加调度的延迟。",
            "If the number of scheduled tasks in TabletScheduler exceeds this value, "
                    + "the scheduling will no longer be performed. "
                    + "The larger this value is, the more tasks can be scheduled, "
                    + "but it will increase the delay of scheduling."})
    public static int max_scheduling_tablets = 2000;

    @ConfField(mutable = true, masterOnly = true, description = {"如果TabletScheduler中的负载均衡任务数超过这个值，"
            + "则不再进行负载均衡。这个值越大，可调度的任务数越多，但是会增加调度的延迟。",
            "If the number of balancing tasks in TabletScheduler exceeds this value, "
                    + "the balancing will no longer be performed. "
                    + "The larger this value is, the more tasks can be scheduled, "
                    + "but it will increase the delay of scheduling."})
    public static int max_balancing_tablets = 100;

    @ConfField(masterOnly = true, description = {
            "副本均衡器类型，可选值：BeLoad, Partition。BeLoad：通过BE上的磁盘空间利用率和Tablet数量计算负载分数并进行均衡。"
                    + "Partition：针对每个表的分区进行均衡，使得一个分区内的Tablet均衡分布在不同的BE上。",
            " The type of replica rebalancer, optional values: BeLoad, Partition. "
                    + "BeLoad: Calculate the load score based on the disk space utilization "
                    + "and the number of tablets on the BE and balance them. "
                    + "Partition: Balance the tablets of each partition of the table. "
                    + "Make tablet distribution of a partition balanced on different BEs."},
            options = {"BeLoad", "Partition"})
    public static String tablet_rebalancer_type = "BeLoad";

    @ConfField(mutable = true, masterOnly = true, description = {"当使用Partition均衡方式时生效。单位是秒。",
            "Valid only if use Partition Rebalancer. Unit is second."})
    public static long partition_rebalance_move_expire_after_access = 600; // 600s

    @ConfField(mutable = true, masterOnly = true, description = {"当使用Partition均衡方式时生效。每次调度尝试的步数。",
            "Valid only if use Partition Rebalancer. The number of steps per scheduling attempt."})
    public static int partition_rebalance_max_moves_num_per_selection = 10;

    // Some online time cost:
    // 1. disk report: 0-1 ms
    // 2. task report: 0-1 ms
    // 3. tablet report
    //      10000 replicas: 200ms
    @ConfField(mutable = true, masterOnly = true, description = {"FE接收到的上报队列的最大长度。"
            + "上报任务包括：磁盘上报、任务上报、副本上报。在某些大集群上，上报一个副本可能需要几十秒，从而导致任务排队。"
            + "可以调大这个数值以确保上报任务完成，但是会增加FE的内存占用。队列满后，FE会丢弃上报任务。",
            "The max length of report queue in FE. "
                    + "Report tasks include: disk report, task report, replica report. "
                    + "In some large clusters, it may take tens of seconds to report a replica, "
                    + "which will cause the task to queue. "
                    + "You can increase this value to ensure that the report task is completed, "
                    + "but it will increase the memory usage of FE. "
                    + "After the queue is full, FE will discard the report task."})
    public static int report_queue_size = 100;

    @ConfField(description = {"是否启用指标计算器。如果设置为true，指标计算器将作为一个守护定时器运行，"
            + "以固定的时间间隔收集需要计算的指标。如QPS。",
            "Whether to enable metric calculator. If set to true, "
                    + "the metric calculator will run as a daemon timer to collect metrics that need to be calculated. "
                    + "Such as QPS."})
    public static boolean enable_metric_calculator = true;

    @ConfField(mutable = true, masterOnly = true, description = {
            "Routine load job的最大数量，包括 `NEED_SCHEDULED`、`RUNNING`、`PAUSE` 状态的job。",
            "The max routine load job num, including `NEED_SCHEDULED`, `RUNNING`, `PAUSE`."})
    public static int max_routine_load_job_num = 100;

    /**
     * the max concurrent routine load task num of a single routine load job
     */
    @ConfField(mutable = true, masterOnly = true, description = {
            "单个routine load job的最大子任务并发数。",
            "The max concurrent routine load task num of a single routine load job."})
    public static int max_routine_load_task_concurrent_num = 5;

    @ConfField(mutable = true, masterOnly = true, description = {
            "单个BE节点所能运行的Routine Load 子任务的最大任务数。"
                    + "这个值应该小于BE的配置 `routine_load_thread_pool_size`（默认为10）。",
            "The max concurrent routine load task num per BE. "
                    + "This value should be less than BE config `routine_load_thread_pool_size`(default 10)."})
    public static int max_routine_load_task_num_per_be = 5;

    @ConfField(mutable = true, masterOnly = true, description = {
            "SmallFileMgr 中可以存储的最大的文件数量。",
            "The max number of files store in SmallFileMgr."})
    public static int max_small_file_number = 100;

    @ConfField(mutable = true, masterOnly = true, description = {
            "SmallFileMgr 中可以存储的单个文件的最大大小。",
            "The max size of a single file store in SmallFileMgr."})
    public static int max_small_file_size_bytes = 1024 * 1024; // 1MB

    @ConfField(description = {
            "用于 create file 功能存放小文件的存储目录。",
            "The directory used to store small files for create file function."})
    public static String small_file_dir = System.getenv("DORIS_HOME") + "/small_files";

    @ConfField(mutable = true, description = {
            "Hash分桶裁剪的最大递归深度。"
                    + "例如：where a in (5 elements) and b in (4 elements) and c in (3 elements) and d in (2 elements)。"
                    + "a/b/c/d是分桶列，所以递归深度为 5 * 4 * 3 * 2 = 120，大于100，"
                    + "因此分布式剪枝将不起作用，只会返回所有的分桶。"
                    + "增加深度可以支持更多元素的分布式剪枝，但可能会消耗更多的CPU。",
            " The max recursion depth of hash distribution pruner. "
                    + "eg: where a in (5 elements) and b in (4 elements) and c in (3 elements) and d in (2 elements). "
                    + "a/b/c/d are distribution columns, so the recursion depth will be 5 * 4 * 3 * 2 = 120, "
                    + "larger than 100, So that distribution pruner will no work and just return all buckets. "
                    + "Increase the depth can support distribution pruning for more elements, but may cost more CPU."})
    public static int max_distribution_pruner_recursion_depth = 100;

    @ConfField(mutable = true, masterOnly = true, description = {
            "如果JVM内存使用率（堆内存或老年代内存池）超过这个阈值，checkpoint线程将不会工作，以避免OOM。",
            "If the jvm memory used percent(heap or old mem pool) exceed this threshold, "
                    + "checkpoint thread will not work to avoid OOM."})
    public static long metadata_checkpoint_memory_threshold = 70;

    @ConfField(mutable = true, masterOnly = true, description = {
            "如果设置为true，checkpoint线程将忽略内存使用率，强制进行checkpoint。",
            "If set to true, the checkpoint thread will make the checkpoint "
                    + "regardless of the jvm memory used percent."})
    public static boolean force_do_metadata_checkpoint = false;

    @ConfField(mutable = true, masterOnly = true, description = {
            "动态分区创建线程的调度时间间隔，单位为秒。",
            "The scheduling time interval of the dynamic partition creation thread, in seconds."})
    public static long dynamic_partition_check_interval_seconds = 600;

    @ConfField(mutable = true, masterOnly = true, description = {
            "单个表可以运行的创建Rollup 作业的最大数量。",
            "The max number of rollup job that can be run for a single table."})
    public static int max_running_rollup_job_num_per_table = 1;

    @ConfField(mutable = true, masterOnly = true, description = {
            "用于在 Routine Load 作业中。如果一个作业因BE宕机而进入暂停状态，系统会尝试重启这个作业。但必须保证集群内宕机的BE节点数量"
                    + "小于这个阈值。默认为0，即表示必须所有BE节点存活，才会尝试重启被暂停的作业。",
            " In the routine load job, if a job is paused because of BE down, the system will try to resume this job. "
                    + "But it must be ensured that the number of BE nodes down in the cluster "
                    + "is less than this threshold. "
                    + "The default is 0, which means that all BE nodes must be alive "
                    + "before trying to resume the paused job."})
    public static int max_tolerable_backend_down_num = 0;

    @ConfField(mutable = true, masterOnly = true, description = {"在Routine Load 作业中，"
            + "当系统尝试重启一个因BE宕机而进入暂停状态的作业时，会以这个时间粒度为周期。如果在这个周期内尝试重启超过三次都失败，"
            + "则不再重试。每个周期都会刷新一次重启失败的次数累计计数。",
            "In the routine load job, when the system tries to restart a job that is paused due to BE down, "
                    + "it will take this time granularity as the period. "
                    + "If more than three attempts to restart fail within this period, it will not be retried. "
                    + "The cumulative number of restart failures will be refreshed every period."})
    public static int period_of_auto_resume_min = 5;

    @ConfField(mutable = true, masterOnly = true, description = {
            "如果设置为true，BE会在Decommission操作完成后自动被删除。如果设置为false，BE将不会被自动删除，仍然处于DECOMMISSION状态。",
            " If set to true, the backend will be automatically dropped after finishing decommission. "
                    + "If set to false, the backend will not be dropped and remaining in DECOMMISSION state."})
    public static boolean drop_backend_after_decommission = true;

    @ConfField(mutable = true, masterOnly = true, description = {"Decommonssion 不会处理处于回收站中的 Tablet。"
            + "这个参数用于在Decommission操作中，当一个BE节点的剩余tablet数量小于这个参数，则会检查是否剩余的tablet都在回收站中。"
            + "如果是，则会认为Decommission已经完成",
            " Decommission will not process tablets in the recycle bin. "
                    + "This parameter is used in the Decommission operation. "
                    + "When the number of remaining tablets of a BE node is less than this parameter, "
                    + "it will be checked whether the remaining tablets are in the recycle bin. "
                    + "If so, it will be considered that Decommission has been completed."})
    public static int decommission_tablet_check_threshold = 5000;

    @ConfField(description = {"FE Thrift Server 类型。", " FE Thrift Server type."},
            options = {"SIMPLE", "THREADED_SELECTOR", "THREAD_POOL"})
    public static String thrift_server_type = "THREAD_POOL";

    @ConfField(mutable = true, masterOnly = true, description = {
            "BE 向 FE 汇报 Task 信息后，FE 会向 BE 重新发送所有丢失的 Task。只有当当前时间和 Task 的创建时间差值超过这个阈值时，"
                    + "才会重新发送任务。以避免频繁的发送重复的任务。",
            " After BE reports Task information to FE, FE will resend all lost Tasks to BE. "
                    + "Only when the difference between the current time and the creation time of the Task "
                    + "exceeds this threshold, will the Task be resent. "
                    + "To avoid frequent resending of duplicate tasks."})
    public static long agent_task_resend_wait_time_ms = 5000;

    @ConfField(mutable = true, masterOnly = true, description = {"最小的 clone 任务的超时间。",
            " The minimum timeout of a clone task."})
    public static long min_clone_task_timeout_sec = 3 * 60; // 3min
    @ConfField(mutable = true, masterOnly = true, description = {"最大的 clone 任务的超时间。",
            " The maximal timeout of a clone task."})
    public static long max_clone_task_timeout_sec = 2 * 60 * 60; // 2h

    /**
     * Minimum interval between last version when caching results,
     * This parameter distinguishes between offline and real-time updates
     */
    @ConfField(mutable = true)
    public static int cache_last_version_interval_second = 900;

    @ConfField(mutable = true, description = {"设置可以缓存的最大行数。",
            "Set the maximum number of rows that can be cached."})
    public static int cache_result_max_row_count = 3000;

    @ConfField(mutable = true, masterOnly = true, description = {"用于限制delete语句中InPredicate的元素个数。",
            " Used to limit the number of elements in InPredicate in delete statement."})
    public static int max_allowed_in_element_num_of_delete = 1024;

    @ConfField(mutable = true, masterOnly = true, description = {
            "在某些情况下，某些tablet可能所有副本都损坏或丢失，此时数据已经丢失，"
                    + "损坏的tablet会导致整个查询失败，剩余的健康tablet也无法查询。"
                    + "此时可以将这个配置设置为true，系统会用空tablet替换损坏的tablet，"
                    + "保证查询可以执行。（但此时数据已经丢失，所以查询结果可能不准确）",
            " In some cases, some tablets may have all replicas damaged or lost. "
                    + "At this time, the data has been lost, "
                    + "and the damaged tablets will cause the entire query to fail, "
                    + "and the remaining healthy tablets cannot be queried. "
                    + "In this case, you can set this configuration to true. "
                    + "The system will replace damaged tablets with empty tablets "
                    + "to ensure that the query can be executed. "
                    + "(but at this time the data has been lost, so the query results may be inaccurate"})
    public static boolean recover_with_empty_tablet = false;

    @ConfField(mutable = true, masterOnly = true, description = {
            "在某些场景下，集群中存在不可恢复的元数据问题，元数据的visibleVersion与BE上的replica version不匹配。"
                    + "此时仍然需要恢复剩余的数据（可能会导致数据正确性问题）。"
                    + "这个配置与`recover_with_empty_tablet`一样，应该只在紧急情况下使用。"
                    + "这个配置有三个值："
                    + "`disable`：如果出现异常，会正常报错。"
                    + "`ignore_version`：忽略fe partition中记录的visibleVersion信息，使用replica version"
                    + "`ignore_all`：除了ignore_version，当遇到没有可查询的replica时，直接跳过，而不是抛出异常",
            " In some scenarios, there is an unrecoverable metadata problem in the cluster, "
                    + "and the visibleVersion of the metadata does not match replica version on BE. "
                    + "In this case, it is still necessary to restore the remaining data "
                    + "(which may cause problems with the correctness of the data). "
                    + "This configuration is the same as` recover_with_empty_tablet` "
                    + "should only be used in emergency situations. "
                    + "This configuration has three values: "
                    + "`disable`: If an exception occurs, an error will be reported normally. "
                    + "`ignore_version`: ignore the visibleVersion information recorded in fe partition, "
                    + "use replica version "
                    + "`ignore_all`: In addition to ignore_version, when encountering no queryable replica, "
                    + "skip it directly instead of throwing an exception"},
            options = {"disable", "ignore_version", "ignore_all"})
    public static String recover_with_skip_missing_version = "disable";

    @ConfField(mutable = true, masterOnly = true, description = {
            "是否在创建 Unqiue Key 表时添加隐藏的 `version` 列",
            "Whether to add a invisible `version` column when create Unique Key table"})
    public static boolean enable_hidden_version_column_by_default = true;

    @ConfField(mutable = true, masterOnly = true, description = {"默认的单个数据库的数据配额",
            " Default database data quota"})
    public static long default_db_data_quota_bytes = 1024L * 1024 * 1024 * 1024 * 1024L; // 1PB

    @ConfField(mutable = true, masterOnly = true, description = {"默认的单个数据库的副本数量配额",
            " Default database replica number quota"})
    public static long default_db_replica_quota_size = 1024 * 1024 * 1024;

    @ConfField(description = {"HTTP API 的路径前缀。有些部署环境需要配置额外的路径前缀来匹配资源。",
            "This Api will return the path configured in Config.http_api_extra_base_path.",
            "Default is empty, which means not set."})
    public static String http_api_extra_base_path = "";

    @ConfField(description = {"是否以BDBJE debug模式启动FE。该模式仅用于调试 BDB JE 中记录的信息，FE 不会提供正常服务。",
            "If set to true, FE will be started in BDBJE debug mode. "
                    + "This mode is only used to debug the information recorded in BDB JE, "
                    + "and FE will not provide normal services."})
    public static boolean enable_bdbje_debug_mode = false;

    @ConfField(mutable = true, masterOnly = true, description = {"是否允许在没有Broker的情况下访问对象存储文件。"
            + "该参数用于在访问某些对象存储时，自动使用S3 SDK 代替 Broker 进程访问数据。",
            "Whether to allow access to object storage files without Broker. "
                    + "This parameter is used to access certain object storage, "
                    + "and automatically use the S3 SDK to access data instead of the Broker process."})
    public static boolean enable_access_file_without_broker = false;

    @ConfField(description = {"是否允许数据导出功能将结果导出到BE所在节点的本地磁盘。"
            + "仅用于测试，因为将数据写到本地节点存在安全风险。",
            "Whether to allow the outfile function to export the results to the local disk of the BE node. "
                    + "Only for testing, because writing data to the local node has security risks."})
    public static boolean enable_outfile_to_local = false;

    @ConfField(description = {"GRPC客户端通道的初始流窗口大小，也用于最大消息大小。"
            + "当结果集较大时，可能需要增加此值。",
            "Used to set the initial flow window size of the GRPC client channel, and also used to max message size. "
                    + "When the result set is large, you may need to increase this value."})
    public static int grpc_max_message_size_bytes = 2147483647; // 2GB

    @ConfField(description = {"GRPC线程池的线程数。", " Number of threads in the GRPC thread pool."})
    public static int grpc_threadmgr_threads_nums = 4096;

    @ConfField(mutable = true, masterOnly = true, description = {"用于限制tablet的最小副本数",
            "Used to limit the minimum number of replicas of tablet"})
    public static short min_replication_num_per_tablet = 1;

    @ConfField(mutable = true, masterOnly = true, description = {"用于限制tablet的最大副本数",
            "Used to limit the maximum number of replicas of tablet"})
    public static short max_replication_num_per_tablet = Short.MAX_VALUE;

    @ConfField(mutable = true, masterOnly = true, description = {"用于限制动态分区表，或者手动创建爱分区时，最大的分区数，"
            + "避免一次性创建太多分区。在动态分区表中，该数值由动态分区参数中的start和end决定。",
            " Used to limit the maximum number of partitions in dynamic partition tables, "
                    + "or when manually creating partitions, "
                    + "to avoid creating too many partitions at one time. "
                    + "In dynamic partition tables, "
                    + "this value is determined by the start and end in the dynamic partition parameters."})
    public static int max_dynamic_partition_num = 4096;

    @ConfField(mutable = true, masterOnly = true, description = {"用于限制单个数据库的备份/恢复任务数",
            "Used to limit the number of backup/restore jobs per database"})
    public static int max_backup_restore_job_num_per_db = 10;

    @ConfField(mutable = true, description = {"一个用户能够在单个FE同时执行的查询分片数量。-1 表示不限制。"
            + "可以通过 `set property` 语句为指定用户单独配置。",
            "The number of query shards that a user can execute at the same time on a single FE. -1 means no limit. "
                    + "Can be configured separately for the specified user through the `set property` statement."})
    public static int default_max_query_instances = -1;

    @ConfField(masterOnly = true, description = {"用于设置表名的大小写敏感性。该参数只能在初始化集群时配置，且不可修改。"
            + "0: 表名按照指定的大小写存储，比较时区分大小写。"
            + "1: 表名存储为小写，比较时不区分大小写。"
            + "2: 表名按照指定的大小写存储，比较时不区分大小写。",
            "Used to set the case sensitivity of table names. "
                    + "This parameter can only be configured during cluster initialization and cannot be modified. "
                    + "0: Table names are stored as specified and comparisons are case sensitive. "
                    + "1: Table names are stored in lowercase and comparisons are not case sensitive. "
                    + "2: Table names are stored as given but compared in lowercase."})
    public static int lower_case_table_names = 0;

    @ConfField(mutable = true, masterOnly = true, description = {"表名的最大长度限制。超过这个长度的表名不被允许使用。",
            "The maximum length limit of the table name. "
                    + "Table names longer than this length are not allowed to be used."})
    public static int table_name_length_limit = 64;

    @ConfField(mutable = false, masterOnly = true, description = {"schema change handler的job调度间隔。"
            + "用户不应该设置该参数。该参数目前仅在回归测试环境中使用，适当降低schema change job的运行速度，"
            + "以测试系统在多个任务并行的情况下的正确性。",
            "The job scheduling interval of the schema change handler. "
                    + "The user should not set this parameter. "
                    + "This parameter is currently only used in the regression test environment to appropriately "
                    + "reduce the running speed of the schema change job to test the correctness of the system "
                    + "in the case of multiple tasks in parallel."})
    public static int default_schema_change_scheduler_interval_millisecond = 500;

    @ConfField(mutable = true, description = {"如果设置为true，查询计划的thrift结构将被压缩后发送到BE。"
            + "这将显着减少rpc数据的大小，从而减少rpc超时的机会。"
            + "但这可能会稍微降低查询的并发性，因为压缩和解压缩会消耗更多的CPU。",
            "If set to true, the thrift structure of query plan will be sent to BE in compact format. "
                    + "This will significantly reduce the size of rpc data, "
                    + "which can reduce the chance of rpc timeout. "
                    + "But this may slightly decrease the concurrency of queries, "
                    + "because compress and decompress cost more CPU."})
    public static boolean use_compact_thrift_rpc = true;

    @ConfField(mutable = true, masterOnly = true, description = {"如果设置为true，tablet scheduler将不工作，"
            + "这样所有的tablet repair/balance任务都将不工作。",
            "If set to true, the tablet scheduler will not work, "
                    + "so that all tablet repair/balance task will not work."})
    public static boolean disable_tablet_scheduler = false;

    @ConfField(mutable = true, masterOnly = true, description = {"当执行clone或者repair tablet任务时，"
            + "可能会有一些replica处于REDUNDANT状态，这些replica应该在后续被删除。"
            + "但是这些replica上可能有loading任务，所以默认策略是等待loading任务完成后再删除。"
            + "但是默认策略可能会花费很长时间来处理这些redundant replica。"
            + "所以我们可以将这个配置设置为true来不等待任何loading任务。"
            + "设置这个配置为true可能会导致loading任务失败，但是会加快tablet balance和repair的过程。",
            "When doing clone or repair tablet task, there may be replica is REDUNDANT state, which "
                    + "should be dropped later. But there are be loading task on these replicas, "
                    + "so the default strategy is to wait until the loading task finished before dropping them. "
                    + "But the default strategy may takes very long time to handle these redundant replicas. "
                    + "So we can set this config to true to not wait any loading task. "
                    + "Set this config to true may cause loading task failed, but will "
                    + "speed up the process of tablet balance and repair."})
    public static boolean enable_force_drop_redundant_replica = false;

    @ConfField(mutable = true, masterOnly = true, description = {"如果设置为 true，则会自动检测 compaction "
            + "较慢从而版本数较高的副本，并将这些副本标记为 bad 以触发副本修复。改参数仅用于特殊情况下，自动处理 compaction 不正常的副本。",
            "If set to true, the replica with slow compaction and high version will be detected automatically, "
                    + "and the replica will be marked as bad to trigger replica repair. This parameter is only used "
                    + "in special cases to automatically handle replicas with abnormal compaction."})
    public static boolean repair_slow_replica = false;

    @ConfField(mutable = true, masterOnly = true, description = {"colocation group迁移可能会涉及大量的tablet在集群内的移动。"
            + "因此，我们应该使用更保守的策略，尽可能避免colocation group的迁移。"
            + "迁移通常发生在BE节点下线或者宕机后。"
            + "该参数用于延迟判断BE节点不可用。"
            + "默认为30分钟，即如果BE节点在30分钟内恢复，则不会触发colocation group的迁移。",
            "The relocation of a colocation group may involve a large number of tablets moving within the cluster. "
                    + "Therefore, we should use a more conservative strategy to avoid relocation "
                    + "of colocation groups as much as possible. "
                    + "Reloaction usually occurs after a BE node goes offline or goes down. "
                    + "This parameter is used to delay the determination of BE node unavailability. "
                    + "The default is 30 minutes, i.e., if a BE node recovers within 30 minutes, relocation of the "
                    + "colocation group will not be triggered."})
    public static long colocate_group_relocate_delay_second = 1800; // 30 min

    @ConfField(description = {"如果设置为true，则在创建表时，Doris将允许在同一主机上定位tablet的副本。"
            + "这仅用于本地测试，以便我们可以在同一主机上部署多个BE并创建具有多个副本的表。"
            + "不要在生产环境中使用它。",
            "If set to true, Doris will allow replicas of tablets to be located on the same host when creating tables. "
                    + "This is only used for local testing so that we can deploy multiple BEs "
                    + "on the same host and create tables with multiple replicas. "
                    + "Do not use it in production environment."})
    public static boolean allow_replica_on_same_host = false;

    @ConfField(mutable = true, description = {"当副本的版本数量超过这个阈值，则可能会被标记为 compaction 慢的副本。"
            + "如果 `repair_slow_replica` 为 true，则可能触发副本修复。",
            " If the version count of the replica exceeds this threshold, "
                    + "it may be marked as a replica with slow compaction. "
                    + "If `repair_slow_replica` is true, it may trigger replica repair."})
    public static int min_version_count_indicate_replica_compaction_too_slow = 200;

    @ConfField(mutable = true, masterOnly = true, description = {
            "当副本的版本数量与最快副本的版本数量的差值超过这个阈值的比例，则可能会被标记为 compaction 慢的副本。"
                    + "如果 `repair_slow_replica` 为 true，则可能触发副本修复。",
            "If the ratio of the difference between the version count of the replica and the version count of the "
                    + "fastest replica exceeds this threshold, it may be marked as a replica with slow compaction. "
                    + "If `repair_slow_replica` is set to true, it may trigger replica repair."})
    public static double valid_version_count_delta_ratio_between_replicas = 0.5;

    @ConfField(mutable = true, masterOnly = true, description = {
            "当副本的数据量超过这个阈值，则可能会被标记为数据量过大的副本。",
            "If the data size of the replica exceeds this threshold, "
                    + "it may be marked as a replica with too large data. "})
    public static long min_bytes_indicate_replica_too_large = 2 * 1024 * 1024 * 1024L;

    // statistics
    @ConfField(mutable = true, masterOnly = true, description = {"统计信息收集任务的最大超时时间",
            "The maximum timeout time of the statistics collection task"})
    public static int max_cbo_statistics_task_timeout_sec = 300;

    @ConfField(mutable = true, masterOnly = true, description = {"是否允许系统自动收集统计信息",
            "If true, will allow the system to collect statistics automatically"})
    public static boolean enable_auto_collect_statistics = true;

    @ConfField(mutable = true, masterOnly = true, description = {"系统自动检查统计信息的时间间隔",
            "The system automatically checks the time interval for statistics"})
    public static int auto_check_statistics_in_sec = 300;

    @ConfField(description = {"是否开启tracing功能，如果开启，需要指定 `trace_export_url`",
            "Whether to enable the tracing feature, if enabled, you need to specify `trace_export_url`"})
    public static boolean enable_tracing = false;

    @ConfField(description = {"Tracing 结果导出方式。zipkin：直接导出到zipkin，用于快速启用tracing功能。"
            + "collector：collector 可以用于接收和处理 Tracing 结果并导出到自定义的第三方系统。",
            "Tracing result export method. zipkin: Export directly to zipkin, which is used to enable the tracing "
                    + "feature quickly. collector: The collector can be used to receive and process Tracing results "
                    + "and export to custom third-party systems."},
            options = {"zipkin", "collector"})
    public static String trace_exporter = "zipkin";

    @ConfField(description = {"Tracing 结果导出的地址，如果 `trace_exporter` 为 zipkin，则导出到 zipkin 的地址。"
            + "如：http://127.0.0.1:9411/api/v2/spans"
            + "如果 `trace_exporter` 为 collector，则导出到 collector 的地址。"
            + "如：http://127.0.0.1:4318/v1/traces",
            "The address to export the Tracing result. If `trace_exporter` is zipkin, the address to export to zipkin. "
                    + "Such as: http://127.0.0.1:9411/api/v2/spans. "
                    + "If `trace_exporter` is collector, the address to export to collector. "
                    + "Such as: http://127.0.0.1:4318/v1/traces"})
    public static String trace_export_url = "http://127.0.0.1:9411/api/v2/spans";

    @ConfField(mutable = true, description = {"查询选择副本时，是否跳过 compaction 慢的副本。"
            + "可以避免查询 compaction 慢的副本导致的查询效率变差。",
            "Whether to skip the replica with slow compaction when selecting the queryable replica. "
                    + "It can avoid the query efficiency becoming worse due to the replica with slow compaction."})
    public static boolean skip_compaction_slower_replica = true;

    @ConfField(expType = ExperimentalType.EXPERIMENTAL, description = {"是否使用 pipeline 执行引擎执行导入任务。",
            "Whether to use the pipeline execution engine to execute the import task."})
    public static boolean enable_pipeline_load = false;

    // enable_resource_group should be immutable and temporarily set to mutable during the development test phase
    @ConfField(mutable = true, masterOnly = true, expType = ExperimentalType.EXPERIMENTAL,
            description = {"是否启用资源组功能",
                    "Whether to enable the resource group function."})
    public static boolean enable_resource_group = false;

    @ConfField(description = {"FE 访问其他 FE 或 BE 的 thrift server 的超时时间",
            "The timeout time for FE to access the thrift server of other FE or BE"})
    public static int backend_rpc_timeout_ms = 60000; // 1 min

    @ConfField(mutable = true, masterOnly = true, description = {
            "用于开启副本均衡的 fuzzy 测试。如果设置为 TRUE，FE 将："
                    + "1. 将 BE 分为高负载和低负载（没有中负载）以强制触发 tablet 调度；"
                    + "2. 在 tablet 调度期间忽略集群是否可以更加平衡；"
                    + "用于测试单副本情况下 tablet 调度频繁时的可靠性。",
            " Used to enable fuzzy test of replica balancing. If set to TRUE, FE will:"
                    + " 1. Divide BE into high load and low load (no mid load) to force triggering tablet scheduling;"
                    + " 2. Ignore whether the cluster can be more balanced during tablet scheduling;"
                    + " Used to test the reliability in single replica case when tablet scheduling are frequent."})
    public static boolean be_rebalancer_fuzzy_test = false;

    @ConfField(mutable = true, masterOnly = true, description = {
            "如果设置为 TRUE，FE 将自动将 date/datetime 转换为 datev2/datetimev2(0)",
            "If set to TRUE, FE will convert date/datetime to datev2/datetimev2(0) automatically."})
    public static boolean enable_date_conversion = false;

    @ConfField(masterOnly = true, description = {"是否允许为一个 BE 设置多个 Tag。",
            "Whether to allow multiple tags to be set for a BE."})
    public static boolean enable_multi_tags = false;

    @ConfField(mutable = true, masterOnly = true, description = {
            "如果设置为 TRUE，FE 将自动将 DecimalV2 转换为 DecimalV3",
            "If set to TRUE, FE will convert DecimalV2 to DecimalV3 automatically."})
    public static boolean enable_decimal_conversion = false;

    @ConfField(mutable = true, masterOnly = true, description = {
            "是否支持复杂数据类型 MAP。",
            "Whether to support complex data type MAP."},
            expType = ExperimentalType.EXPERIMENTAL)
    public static boolean enable_map_type = false;

    @ConfField(mutable = true, masterOnly = true, description = {
            "是否支持复杂数据类型 STRUCT。",
            "Whether to support complex data type STRUCT."},
            expType = ExperimentalType.EXPERIMENTAL)
    public static boolean enable_struct_type = false;

    /**
     * The timeout of executing async remote fragment.
     * In normal case, the async remote fragment will be executed in a short time. If system are under high load
     * condition，try to set this timeout longer.
     */
    @ConfField(mutable = true)
    public static long remote_fragment_exec_timeout_ms = 5000; // 5 sec

    /**
     * Max data version of backends serialize block.
     */
    @ConfField(mutable = false)
    public static int max_be_exec_version = 2;

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

    @ConfField(mutable = false, masterOnly = false, description = {"Hive表到分区名列表缓存的最大数量。",
        "Max cache number of hive table to partition names list."})
    public static long max_hive_table_catch_num = 1000;

    @ConfField(mutable = false, masterOnly = false, description = {"获取Hive分区值时候的最大返回数量，-1代表没有限制。",
        "Max number of hive partition values to return while list partitions, -1 means no limitation."})
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
     * NOTE: The storage policy is still under developement.
     */
    @ConfField(mutable = false, masterOnly = true)
    public static boolean enable_storage_policy = true;

    /**
     * This config is mainly used in the k8s cluster environment.
     * When enable_fqdn_mode is true, the name of the pod where be is located will remain unchanged
     * after reconstruction, while the ip can be changed.
     */
    @ConfField(mutable = false, masterOnly = true, expType = ExperimentalType.EXPERIMENTAL)
    public static boolean enable_fqdn_mode = false;

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
    public static boolean enable_ssl = true;

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
     * Whether create a duplicate table without keys by default
     * when creating a table which not set key type and key columns
     */
    @ConfField(mutable = true, masterOnly = true)
    public static boolean experimental_enable_duplicate_without_keys_by_default = false;

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

    /*
     * "max_instance_num" is used to set the maximum concurrency. When the value set
     * by "parallel_fragment_exec_instance_num" is greater than "max_instance_num",
     * an error will be reported.
     */
    @ConfField(mutable = true)
    public static int max_instance_num = 128;

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
    @ConfField(mutable = true, masterOnly = false)
    public static boolean enable_query_hit_stats = false;
}
