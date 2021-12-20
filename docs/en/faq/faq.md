---
{
    "title": "FAQ",
    "language": "en"
}
---

<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# FAQ

This document is mainly used to record common problems in the use of Doris. Will be updated from time to time.

### Q1. Use Stream Load to access the public network address of FE to import data, and it is redirected to the internal network IP?


When the connection target of stream load is the http port of FE, FE will only randomly select a BE node for http 307 redirect operation, so the user's request is actually sent to a BE designated by FE. The redirect returns the ip of BE, which is the intranet IP. So if you send the request through the public IP of FE, it is very likely that you will not be able to connect because you are redirected to the intranet address.

The usual approach is to ensure that you can access the intranet IP address, or assume a load balance for all BE upper layers, and then directly send the stream load request to the load balancer, and the load balancer transparently transmits the request to the BE node .

### Q2. Query error: Failed to get scan range, no queryable replica found in tablet: xxxx

This situation is because the corresponding tablet does not find a copy that can be queried, usually because the BE is down, the copy is missing, and so on. You can use the `show tablet tablet_id` statement first, and then execute the following `show proc` statement to view the copy information corresponding to this tablet, and check whether the copy is complete. At the same time, you can use the `show proc "/cluster_balance"` information to query the progress of replica scheduling and repair in the cluster.

For commands related to data copy management, please refer to [Data Copy Management](../administrator-guide/operation/tablet-repair-and-balance.md).

### Q3. FE failed to start, fe.log keeps scrolling "wait catalog to be ready. FE type UNKNOWN"

There are usually two reasons for this problem:

1. The local IP obtained when the FE is started this time is inconsistent with the last time, usually because the `priority_network` is not set correctly, the wrong IP address is matched when the FE is started. Need to modify `priority_network` and restart FE.

2. Most Follower FE nodes in the cluster are not started. For example, there are 3 Followers and only one is started. At this time, at least one other FE needs to be also activated, and the FE electable group can elect the Master to provide services.

If none of the above conditions can be resolved, you can follow the [Metadata Operation and Maintenance Document] (../administrator-guide/operation/metadata-operation.md) in the Doris official website to restore.

### Q4. When the BE node is offline through DECOMMISSION, why is there always some tablet remaining?

During the offline process, check the tabletNum of the offline node through show backends, and you will observe that the number of tabletNum is decreasing, indicating that the data fragments are migrating from this node. When the number is reduced to 0, the system will automatically delete this node. But in some cases, tabletNum does not change after it drops to a certain value. This can usually have the following two reasons:

1. These tablets belong to the table, partition, or materialized view that has just been deleted. The objects that have just been deleted will remain in the recycle bin. The offline logic will not process these fragments. You can modify the resident time of the object in the recycle bin by modifying the configuration parameter catalog_trash_expire_second of FE. When the object is deleted from the recycle bin, these tablets will be processed.

2. There is a problem with the migration task of these tablets. At this time, you need to check the error of the specific task through show proc "/cluster_balance".

For the above situation, you can first check whether the cluster still has unhealthy shards through show proc "/statistic". If it is 0, you can delete the BE directly through the drop backend statement. Otherwise, you need to check the copy status of unhealthy shards.


### Q5. How should priorty_network be set?

Priorty_network is a configuration parameter for both FE and BE. This parameter is mainly used to help the system choose the correct network card IP as its own IP. It is recommended to set this parameter explicitly under any circumstances to prevent the problem of incorrect IP selection caused by the addition of a new network card to the subsequent machine.

The value of priorty_network is expressed in CIDR format. It is divided into two parts, the first part is a dotted decimal IP address, and the second part is a prefix length. For example, 10.168.1.0/8 will match all 10.xx.xx.xx IP addresses, and 10.168.1.0/16 will match all 10.168.xx.xx IP addresses.

The reason for using the CIDR format instead of directly specifying a specific IP is to ensure that all nodes can use uniform configuration values. For example, there are two nodes: 10.168.10.1 and 10.168.10.2, then we can use 10.168.10.0/24 as the value of priorty_network.

### Q6. What are FE's Master, Follower and Observer?

First of all, make it clear that FE has only two roles: Follower and Observer. The Master is just an FE selected from a group of Follower nodes. Master can be regarded as a special kind of Follower. So when we were asked how many FEs in a cluster and what roles are they, the correct answer should be the number of all FE nodes, the number of Follower roles, and the number of Observer roles.

All FE nodes in the Follower role will form a selectable group, similar to the group concept in the Poxas consensus protocol. A Follower will be elected as the Master in the group. When the Master hangs up, the new Follower will be automatically selected as the Master. Observer will not participate in the election, so Observer will not be called Master.

A metadata log needs to be successfully written in most Follower nodes to be considered successful. For example, if 3 FEs are written, 2 writes are successful. This is why the number of Follower roles needs to be an odd number.

The role of Observer is the same as the meaning of this word. It only acts as an observer to synchronize the metadata logs that have been successfully written, and provides metadata reading services. He will not participate in the logic of majority writing.

Normally, you can deploy 1 Follower + 2 Observer or 3 Follower + N Observer. The former is simple to operate and maintain, and there will be almost no consensus agreement between Followers to cause this complicated error situation (most of Baidu's internal clusters use this method). The latter can ensure the high availability of metadata writing. If it is a high-concurrency query scenario, you can appropriately increase the Observer.

### Q7. tablet writer write failed, tablet_id=27306172, txn_id=28573520, err=-235 or -215 or -238

This error usually occurs during data import operations. The error code of the new version is -235, and the error code of the old version may be -215. The meaning of this error is that the data version of the corresponding tablet exceeds the maximum limit (default 500, controlled by the BE parameter `max_tablet_version_num`), and subsequent writes will be rejected. For example, the error in the question means that the data version of the tablet 27306172 exceeds the limit.

This error is usually because the import frequency is too high, which is greater than the compaction speed of the background data, which causes the version to accumulate and eventually exceeds the limit. At this point, we can first use the show tablet 27306172 statement, and then execute the show proc statement in the result to view the status of each copy of the tablet. The versionCount in the result represents the number of versions. If you find that there are too many versions of a copy, you need to reduce the import frequency or stop importing, and observe whether the number of versions drops. If the version number still does not decrease after the import is stopped, you need to go to the corresponding BE node to check the be.INFO log, search for the tablet id and compaction keywords, and check whether the compaction is running normally. For compaction tuning related, you can refer to the ApacheDoris public account article: Doris Best Practice-Compaction Tuning (3)

The -238 error usually occurs when the amount of imported data in the same batch is too large, which leads to too many Segment files for a certain tablet (the default is 200, which is controlled by the BE parameter `max_segment_num_per_rowset`). At this time, it is recommended to reduce the amount of data imported in one batch, or to appropriately increase the value of the BE configuration parameter to solve the problem.

### Q8. tablet 110309738 has few replicas: 1, alive backends: [10003]

This error may occur during query or import operation. It usually means that the copy of the tablet is abnormal.

At this point, you can first check whether the BE node is down by using the show backends command, such as the isAlive field is false, or LastStartTime is the most recent time (indicating that it has been restarted recently). If the BE is down, you need to go to the node corresponding to the BE and check the be.out log. If the BE is down due to an exception, usually the exception stack will be printed in be.out to help troubleshoot the problem. If there is no error stack in be.out. You can use the linux command dmesg -T to check whether the process is killed by the system because of OOM.

If no BE node is down, you need to use the show tablet 110309738 statement, and then execute the show proc statement in the result to check the status of each copy of the tablet for further investigation.

### Q9. disk xxxxx on backend xxx exceed limit usage

It usually appears in operations such as import and Alter. This error means that the usage of the corresponding disk corresponding to the BE exceeds the threshold (95% by default). At this time, you can use the show backends command first, where MaxDiskUsedPct shows the usage of the disk with the highest usage on the corresponding BE. If If it exceeds 95%, this error will be reported.

At this time, you need to go to the corresponding BE node to check the usage in the data directory. The trash directory and snapshot directory can be manually cleaned up to free up space. If the data directory occupies a lot, you need to consider deleting some data to free up space. For details, please refer to [Disk Space Management](../administrator-guide/operation/disk-capacity.md).

### Q10. invalid cluster id: xxxx

This error may appear in the results of the show backends or show frontends commands. It usually appears in the error message column of a certain FE or BE node. The meaning of this error is that after Master FE sends heartbeat information to this node, the node finds that the cluster id carried in the heartbeat information is different from the cluster id stored locally, so it refuses to respond to the heartbeat.

Doris' Master FE node will actively send a heartbeat to each FE or BE node, and will carry a cluster_id in the heartbeat information. The cluster_id is the unique cluster ID generated by the Master FE when a cluster is initialized. When the FE or BE receives the heartbeat information for the first time, it will save the cluster_id locally in the form of a file. The FE file is in the image/ directory of the metadata directory, and BE has a cluster_id file in all data directories. After that, every time a node receives a heartbeat, it will compare the content of the local cluster_id with the content in the heartbeat. If it is inconsistent, it will refuse to respond to the heartbeat.

This mechanism is a node authentication mechanism to prevent receiving wrong heartbeat information from nodes outside the cluster.

If you need to recover from this error. First, confirm whether all nodes are the correct nodes in the cluster. After that, for the FE node, you can try to modify the cluster_id value in the image/VERSION file in the metadata directory and restart the FE. For BE nodes, you can delete cluster_id files in all data directories and restart BE.

### Q11. Does Doris support modifying column names?

Does not support modifying column names.

Doris supports modifying database names, table names, partition names, materialized view (Rollup) names, and column types, comments, default values, and so on. But unfortunately, currently does not support modifying the column name.

For some historical reasons, the column names are currently written directly into the data file. When Doris searches, he also finds the corresponding column by the class name. Therefore, modifying column names is not only a simple metadata modification, but also involves data rewriting, which is a very heavy operation.

We do not rule out the subsequent use of some compatible means to support lightweight column name modification operations.

### Q12. Does the table of the Unique Key model support the creation of materialized views?

not support.

The table of the Unique Key model is a business-friendly table. Because of its unique function of de-duplication according to the primary key, it can easily synchronize business databases with frequent data changes. Therefore, many users will first consider using the Unique Key model when accessing data to Doris.

Unfortunately, the table of the Unique Key model cannot create a materialized view. The reason is that the nature of the materialized view is to "pre-calculate" the data through pre-calculation, so that the calculated data is directly returned during the query to speed up the query. In the materialized view, the "pre-calculated" data is usually some aggregated indicators, such as summation and count. At this time, if the data changes, such as udpate or delete, because the pre-calculated data has lost the detailed information, it cannot be updated synchronously. For example, a sum of 5 may be 1+4 or 2+3. Because of the loss of detailed information, we cannot distinguish how the sum is calculated, and therefore cannot meet the update requirements.

### Q13. show backends/frontends Viewed information is incomplete

After executing certain statements such as `show backends/frontends`, some columns may be incomplete in the results. For example, the disk capacity information cannot be seen in the show backends results.

This problem usually occurs when there are multiple FEs in the cluster. If users connect to non-Master FE nodes to execute these statements, they will see incomplete information. This is because part of the information only exists in the Master FE node. Such as BE's disk usage information. Therefore, the complete information can be obtained only after the Master FE is directly connected.

Of course, the user can also execute `set forward_to_master=true;` before executing these statements. After the session variable is set to true, some information viewing statements executed later will be automatically forwarded to the Master FE to obtain the results. In this way, no matter which FE the user is connected to, the complete result can be obtained.

### Q14. Import data by calling stream load through a Java program. When a batch of data is large, a Broken Pipe error may be reported

In addition to Broken Pipe, there may be other strange errors.

This situation usually occurs after opening httpv2. Because httpv2 is an http service implemented using spring boot, and uses tomcat as the default built-in container. But jetty's handling of 307 forwarding seems to have some problems, so the built-in container will be modified to jetty later. In addition, the version of apache http client in the java program needs to use a version later than 4.5.13. In the previous version, there were also some problems with the processing of forwarding.

So this problem can be solved in two ways:

1. Turn off httpv2

    Add enable_http_server_v2=false in fe.conf and restart FE. However, the new UI interface can no longer be used in this way, and some new interfaces based on httpv2 cannot be used later. (Normal import queries are not affected).

2. Upgrade

    You can upgrade to Doris 0.15 and later versions, this problem has been fixed.

### Q15. A new disk is added to the node. Why is the data not balanced on the new disk?

The current balance strategy of Doris is based on nodes. In other words, the cluster load is judged according to the overall load index of the node (the number of shards and the total disk utilization). And migrate data fragments from high-load nodes to low-load nodes. If each node adds a disk, from the perspective of the node as a whole, the load has not changed, so the balancing logic cannot be triggered.

In addition, Doris currently does not support balanced operations within a single node and between various disks. Therefore, after adding a new disk, the data will not be balanced to the new disk.

However, when data is migrated between nodes, Doris will consider the disk factor. For example, if a slice is migrated from node A to node B, the disk with lower disk space utilization among node B will be selected first.

Here we provide 3 ways to solve this problem:

1. Rebuild the new table

    Create a new table through the create table like statement, and then use insert into select to synchronize the data from the old table to the new table. Because when a new table is created, the data fragments of the new table will be distributed on the new disk, and the data will also be written to the new disk. This method is suitable for situations where the amount of data is small (within tens of GB).

2. Through the Decommission command

    The decommission command is used to safely decommission a BE node. This command will first migrate the data fragments on the node to other nodes, and then delete the node. As mentioned earlier, when data is migrated, disks with low disk utilization will be given priority, so this method can "force" the data to be migrated to the disks of other nodes. When the data migration is completed, we cancel the decommission operation, so that the data will be rebalanced back to this node. When we perform the above steps for all BE nodes, the data will be evenly distributed on all disks of all nodes.

    Note that before executing the decommission command, execute the following command first to avoid the node being deleted after it is offline.

    `admin set frontend config("drop_backend_after_decommission" = "false");`

3. Manually migrate data using API

    Doris provides [HTTP API](../administrator-guide/http-actions/tablet-migration-action.md), which allows you to manually specify data fragments on one disk to migrate to another disk.
    
### Q16. How to read FE/BE log correctly?

In many cases, we need to troubleshoot problems through logs. Here is an explanation of the format and viewing method of the FE/BE log.

1. FE

    FE logs mainly include:
    
    * fe.log: main log. Including everything except fe.out.
    * fe.warn.log: A subset of the main log, which only records WARN and ERROR level logs.
    * fe.out: Standard/error output log (stdout and stderr).
    * fe.audit.log: Audit log, which records all SQL requests received by this FE.

    A typical FE log is as follows:

    ```
    2021-09-16 23:13:22,502 INFO (tablet scheduler|43) [BeLoadRebalancer.selectAlternativeTabletsForCluster():85] cluster is balance: default_cluster with medium: HDD. skip
    ```
    
    * `2021-09-16 23:13:22,502`: log time.
    * `INFO: log level, the default is INFO`.
    * `(tablet scheduler|43)`: thread name and thread id. Through the thread id, you can view the thread context information and troubleshoot what happened in this thread.
    * `BeLoadRebalancer.selectAlternativeTabletsForCluster():85`: class name, method name and code line number.
    * `cluster is balance xxx`: log content.

    Normally, we mainly check the fe.log log. Under special circumstances, some logs may be output to fe.out.

2. BE

    The BE logs mainly include:

    * be.INFO: Main log. This is actually a soft connection, connected to the latest be.INFO.xxxx.
    * be.WARNING: A subset of the main log, only logs of WARN and FATAL levels are recorded. This is actually a soft connection, connected to the latest be.WARN.xxxx.
    * be.out: standard/error output log (stdout and stderr).

    A typical BE log is as follows:

    ```
    I0916 23:21:22.038795 28087 task_worker_pool.cpp:1594] finish report TASK. master host: 10.10.10.10, port: 9222
    ```

    * `I0916 23:21:22.038795`: Log level and date and time. The capital letter I means INFO, W means WARN, and F means FATAL.
    * `28087`: thread id. Through the thread id, you can view the thread context information and troubleshoot what happened in this thread.
    * `task_worker_pool.cpp:1594`: code file and line number.
    * `finish report TASK xxx`: log content.

    Normally, we mainly check the be.INFO log. Under special circumstances, such as BE downtime, you need to check be.out.

### Q17. How to troubleshoot the cause of FE/BE node down?

1. BE

    The BE process is a C/C++ process, and the process may hang due to some program bugs (memory out of bounds, illegal address access, etc.) or Out Of Memory (OOM). At this point, we can check the cause of the error through the following steps:
    
    1. View be.out
    
        The BE process realizes that when the program exits due to an abnormal condition, it will print the current error stack to be.out (note that it is be.out, not be.INFO or be.WARNING). Through the error stack, you can usually get a rough idea of ​​where the program went wrong.
    
        Note that if an error stack appears in be.out, it is usually due to a program bug, and ordinary users may not be able to solve it by themselves. Welcome to the WeChat group, github discussion or dev mail group for help, and post the corresponding error stack for quick Troubleshoot the problem.
    
    2. dmesg
    
        If be.out has no stack information, it is likely that OOM was forcibly killed by the system. At this point, you can use the dmesg -T command to view the Linux system log. If a log similar to Memory cgroup out of memory: Kill process 7187 (palo_be) score 1007 or sacrifice child appears at the end, it means that it is caused by OOM.
    
        There may be many reasons for memory problems, such as large queries, imports, compactions, etc. Doris is also constantly optimizing memory usage. Welcome to the WeChat group, github discussion or dev mailing group for help.
    
    3. Check whether there are logs starting with F in be.INFO.
    
        The log at the beginning of F is the Fatal log. For example, F0916 means the Fatal log on September 16. Fatal logs usually indicate program assertion errors, and assertion errors will directly cause the process to exit (indicating that the program has a bug). Welcome to the WeChat group, github discussion or dev mailing group for help.
        
    4. Minidump
    
        Mindump is a feature added after Doris 0.15. For details, please refer to [Document](../developer-guide/minidump.md).

2. FE

    FE is a java process, and its robustness depends on the C/C++ program. Usually, the cause of FE hanging may be OOM (Out-of-Memory) or metadata writing failure. These errors usually have an error stack in fe.log or fe.out. You need to investigate further based on the error stack information.

### Q18. About the configuration of the data directory SSD and HDD.

Doris supports a BE node to configure multiple storage paths. Normally, it is sufficient to configure one storage path for each disk. At the same time, Doris supports storage media attributes of specified paths, such as SSD or HDD. SSD stands for high-speed storage devices, and HDD stands for low-speed storage devices.

By specifying the storage medium properties of the path, we can use Doris's hot and cold data partition storage function to store hot data in the SSD at the partition level, and the cold data will be automatically transferred to the HDD.

It should be noted that Doris does not automatically perceive the actual storage medium type of the disk where the storage path is located. This type needs to be explicitly indicated by the user in the path configuration. For example, the path "/path/to/data1.SSD" means that this path is an SSD storage medium. And "data1.SSD" is the actual directory name. Doris determines the storage medium type based on the ".SSD" suffix behind the directory name, not the actual storage medium type. In other words, the user can specify any path as the SSD storage medium, and Doris only recognizes the directory suffix and will not judge whether the storage medium matches. If you do not write the suffix, the default is HDD.

In other words, ".HDD" and ".SSD" are only used to identify the "relative" "low speed" and "high speed" of the storage directory, not the actual storage medium type. Therefore, if the storage path on the BE node has no difference in media, there is no need to fill in the suffix.

### Q19. `Lost connection to MySQL server at'reading initial communication packet', system error: 0`

If the following problems occur when using the MySQL client to connect to Doris, this is usually caused by the difference between the jdk version used when compiling FE and the jdk version used when running FE.
Note that when using docker image to compile, the default JDK version is openjdk 11, you can switch to openjdk 8 by command (see the compilation document for details).

### Q20. -214 error

When performing operations such as load and query, you may encounter the following errors:

```
failed to initialize storage reader. tablet=63416.1050661139.aa4d304e7a7aff9c-f0fa7579928c85a0, res=-214, backend=192.168.100.10
```

A -214 error means that the data version of the corresponding tablet is missing. For example, the above error indicates that the data version of the replica of tablet 63416 on the BE of 192.168.100.10 is missing. (There may be other similar error codes, which can be checked and repaired in the following ways).

Normally, if your data has multiple replicas, the system will automatically repair these problematic replicas. You can troubleshoot through the following steps:

First, use the `show tablet 63416` statement and execute the `show proc xxx` statement in the result to view the status of each replica of the corresponding tablet. Usually we need to care about the data in the `Version` column.

Under normal circumstances, the Version of multiple replicas of a tablet should be the same. And it is the same as the VisibleVersion of the corresponding partition.

You can use `show partitions from tblx` to view the corresponding partition version (the partition corresponding to the tablet can be obtained in the `show tablet` statement.)

At the same time, you can also visit the URL in the CompactionStatus column of the `show proc` statement (just open it in the browser) to view more specific version information, to check which version is missing.

If there is no automatic repair for a long time, you need to use the `show proc "/cluster_balance"` statement to view the tablet repair and scheduling tasks currently being performed by the system. It may be because there are a large number of tablets waiting to be scheduled, which leads to a long repair time. You can follow the records in `pending_tablets` and `running_tablets`.

Furthermore, you can use the `admin repair` statement to specify the priority to repair a table or partition. For details, please refer to `help admin repair`;

If it still cannot be repaired, then in the case of multiple replicas, we use the `admin set replica status` command to force the replica to go offline. For details, please refer to the example of `help admin set replica status` to set the status of the replica to bad. (After set to bad, the replica will not be accessed again. And will be automatically repaired later. But before the operation, you should make sure that the other replicas are normal)

### Q21. Not connected to 192.168.100.1:8060 yet, server_id=384

We may encounter this error when loading or querying. If you go to the corresponding BE log to check, you may also find similar errors.

This is an RPC error, and there are usually two possibilities: 1. The corresponding BE node is down. 2. rpc congestion or other errors.

If the BE node is down, you need to check the specific reason for the downtime. Only the problem of rpc congestion is discussed here.

One situation is OVERCROWDED, which means that a large amount of unsent data at the rpc client exceeds the threshold. BE has two parameters related to it:

1. `brpc_socket_max_unwritten_bytes`: The default is 64MB. If the unwritten data exceeds this value, an error will be reported. You can modify this value appropriately to avoid OVERCROWDED errors. (But this cures the symptoms rather than the root cause, essentially congestion still occurs).
2. `tablet_writer_ignore_eovercrowded`: The default is false. If set to true, Doris will ignore OVERCROWDED errors during the load process. This parameter is mainly used to avoid load failure and improve the stability of load.

The second is that the packet size of rpc exceeds `max_body_size`. This problem may occur if the query contains a very large String type or a Bitmap type. It can be circumvented by modifying the following BE parameters:

1. `brpc_max_body_size`: The default is 200MB, if necessary, it can be modified to 3GB (in bytes).