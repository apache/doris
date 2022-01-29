---
{
    "title": "Common Error",
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

# Common Error

This document is mainly used to record the errors reported during the use of Doris. If you encounter some errors, you are welcome to contribute to us for updates.


### E1. Query error: Failed to get scan range, no queryable replica found in tablet: xxxx

This situation is because the corresponding tablet does not find a copy that can be queried, usually because the BE is down, the copy is missing, and so on. You can use the `show tablet tablet_id` statement first, and then execute the following `show proc` statement to view the copy information corresponding to this tablet, and check whether the copy is complete. At the same time, you can use the `show proc "/cluster_balance"` information to query the progress of replica scheduling and repair in the cluster.

For commands related to data copy management, please refer to [Data Copy Management](../administrator-guide/operation/tablet-repair-and-balance.md).

### E2. FE failed to start, fe.log keeps scrolling "wait catalog to be ready. FE type UNKNOWN"

There are usually two reasons for this problem:

1. The local IP obtained when the FE is started this time is inconsistent with the last time, usually because the `priority_network` is not set correctly, the wrong IP address is matched when the FE is started. Need to modify `priority_network` and restart FE.

2. Most Follower FE nodes in the cluster are not started. For example, there are 3 Followers and only one is started. At this time, at least one other FE needs to be also activated, and the FE electable group can elect the Master to provide services.

If none of the above conditions can be resolved, you can follow the [Metadata Operation and Maintenance Document] (../administrator-guide/operation/metadata-operation.md) in the Doris official website to restore.

### E3. tablet writer write failed, tablet_id=27306172, txn_id=28573520, err=-235 or -215 or -238

This error usually occurs during data import operations. The error code of the new version is -235, and the error code of the old version may be -215. The meaning of this error is that the data version of the corresponding tablet exceeds the maximum limit (default 500, controlled by the BE parameter `max_tablet_version_num`), and subsequent writes will be rejected. For example, the error in the question means that the data version of the tablet 27306172 exceeds the limit.

This error is usually because the import frequency is too high, which is greater than the compaction speed of the background data, which causes the version to accumulate and eventually exceeds the limit. At this point, we can first use the show tablet 27306172 statement, and then execute the show proc statement in the result to view the status of each copy of the tablet. The versionCount in the result represents the number of versions. If you find that there are too many versions of a copy, you need to reduce the import frequency or stop importing, and observe whether the number of versions drops. If the version number still does not decrease after the import is stopped, you need to go to the corresponding BE node to check the be.INFO log, search for the tablet id and compaction keywords, and check whether the compaction is running normally. For compaction tuning related, you can refer to the ApacheDoris public account article: Doris Best Practice-Compaction Tuning (3)

The -238 error usually occurs when the amount of imported data in the same batch is too large, which leads to too many Segment files for a certain tablet (the default is 200, which is controlled by the BE parameter `max_segment_num_per_rowset`). At this time, it is recommended to reduce the amount of data imported in one batch, or to appropriately increase the value of the BE configuration parameter to solve the problem.

### E4. tablet 110309738 has few replicas: 1, alive backends: [10003]

This error may occur during query or import operation. It usually means that the copy of the tablet is abnormal.

At this point, you can first check whether the BE node is down by using the show backends command, such as the isAlive field is false, or LastStartTime is the most recent time (indicating that it has been restarted recently). If the BE is down, you need to go to the node corresponding to the BE and check the be.out log. If the BE is down due to an exception, usually the exception stack will be printed in be.out to help troubleshoot the problem. If there is no error stack in be.out. You can use the linux command dmesg -T to check whether the process is killed by the system because of OOM.

If no BE node is down, you need to use the show tablet 110309738 statement, and then execute the show proc statement in the result to check the status of each copy of the tablet for further investigation.

### E5. disk xxxxx on backend xxx exceed limit usage

It usually appears in operations such as import and Alter. This error means that the usage of the corresponding disk corresponding to the BE exceeds the threshold (95% by default). At this time, you can use the show backends command first, where MaxDiskUsedPct shows the usage of the disk with the highest usage on the corresponding BE. If If it exceeds 95%, this error will be reported.

At this time, you need to go to the corresponding BE node to check the usage in the data directory. The trash directory and snapshot directory can be manually cleaned up to free up space. If the data directory occupies a lot, you need to consider deleting some data to free up space. For details, please refer to [Disk Space Management](../administrator-guide/operation/disk-capacity.md).

### E6. invalid cluster id: xxxx

This error may appear in the results of the show backends or show frontends commands. It usually appears in the error message column of a certain FE or BE node. The meaning of this error is that after Master FE sends heartbeat information to this node, the node finds that the cluster id carried in the heartbeat information is different from the cluster id stored locally, so it refuses to respond to the heartbeat.

Doris' Master FE node will actively send a heartbeat to each FE or BE node, and will carry a cluster_id in the heartbeat information. The cluster_id is the unique cluster ID generated by the Master FE when a cluster is initialized. When the FE or BE receives the heartbeat information for the first time, it will save the cluster_id locally in the form of a file. The FE file is in the image/ directory of the metadata directory, and BE has a cluster_id file in all data directories. After that, every time a node receives a heartbeat, it will compare the content of the local cluster_id with the content in the heartbeat. If it is inconsistent, it will refuse to respond to the heartbeat.

This mechanism is a node authentication mechanism to prevent receiving wrong heartbeat information from nodes outside the cluster.

If you need to recover from this error. First, confirm whether all nodes are the correct nodes in the cluster. After that, for the FE node, you can try to modify the cluster_id value in the image/VERSION file in the metadata directory and restart the FE. For BE nodes, you can delete cluster_id files in all data directories and restart BE.

### E7. Import data by calling stream load through a Java program. When a batch of data is large, a Broken Pipe error may be reported

In addition to Broken Pipe, there may be other strange errors.

This situation usually occurs after opening httpv2. Because httpv2 is an http service implemented using spring boot, and uses tomcat as the default built-in container. But tomcat's handling of 307 forwarding seems to have some problems, so the built-in container will be modified to jetty later. In addition, the version of apache http client in the java program needs to use a version later than 4.5.13. In the previous version, there were also some problems with the processing of forwarding.

So this problem can be solved in two ways:

1. Turn off httpv2

    Add enable_http_server_v2=false in fe.conf and restart FE. However, the new UI interface can no longer be used in this way, and some new interfaces based on httpv2 cannot be used later. (Normal import queries are not affected).

2. Upgrade

    You can upgrade to Doris 0.15 and later versions, this problem has been fixed.


### E8. `Lost connection to MySQL server at'reading initial communication packet', system error: 0`

If the following problems occur when using the MySQL client to connect to Doris, this is usually caused by the difference between the jdk version used when compiling FE and the jdk version used when running FE.
Note that when using docker image to compile, the default JDK version is openjdk 11, you can switch to openjdk 8 by command (see the compilation document for details).

### E9. -214 error

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

### E10. Not connected to 192.168.100.1:8060 yet, server_id=384

We may encounter this error when loading or querying. If you go to the corresponding BE log to check, you may also find similar errors.

This is an RPC error, and there are usually two possibilities: 1. The corresponding BE node is down. 2. rpc congestion or other errors.

If the BE node is down, you need to check the specific reason for the downtime. Only the problem of rpc congestion is discussed here.

One situation is OVERCROWDED, which means that a large amount of unsent data at the rpc client exceeds the threshold. BE has two parameters related to it:

1. `brpc_socket_max_unwritten_bytes`: The default is 1GB. If the unwritten data exceeds this value, an error will be reported. You can modify this value appropriately to avoid OVERCROWDED errors. (But this cures the symptoms rather than the root cause, essentially congestion still occurs).
2. `tablet_writer_ignore_eovercrowded`: The default is false. If set to true, Doris will ignore OVERCROWDED errors during the load process. This parameter is mainly used to avoid load failure and improve the stability of load.

The second is that the packet size of rpc exceeds `max_body_size`. This problem may occur if the query contains a very large String type or a Bitmap type. It can be circumvented by modifying the following BE parameters:

1. `brpc_max_body_size`: The default is 3GB.

### E11. `recoveryTracker should overlap or follow on disk last VLSN of 4,422,880 recoveryFirst= 4,422,882 UNEXPECTED_STATE_FATAL`

Sometimes when restarting the Fe, the above error will occur (usually only in the case of multiple followers), and the difference between the two values in the error is 2. As a result, the Fe startup fails.

This is a bug in bdbje that has not been resolved. In this case, metadata can only be recovered through fault recovery in [metadata operation and maintenance manual](../administrator-guide/operation/metadata-operation.md).

### E12.Doris compile and install JDK version incompatibility problem

When I use Docker to compile Doris myself, start FE after compiling and installing, ```java.lang.Suchmethoderror: java.nio.ByteBuffer.limit (I)Ljava/nio/ByteBuffer; ``` exception information, this is because the default in Docker is JDK 11. If your installation environment is using JDK8, you need to switch the JDK environment to JDK8 in Docker. For the specific switching method, refer to [Compilation](https://doris.apache.org/installing/compilation.html)
