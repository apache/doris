---
{
    "title": "常见报错",
    "language": "zh-CN"
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

# 常见报错

本文档主要用于记录 Doris 使用过程中的报错，如果您有遇见一些报错，欢迎贡献给我们更新。

### E1. 查询报错：Failed to get scan range, no queryable replica found in tablet: xxxx

这种情况是因为对应的 tablet 没有找到可以查询的副本，通常原因可能是 BE 宕机、副本缺失等。可以先通过 `show tablet tablet_id` 语句，然后执行后面的 `show proc` 语句，查看这个 tablet 对应的副本信息，检查副本是否完整。同时还可以通过 `show proc "/cluster_balance"` 信息来查询集群内副本调度和修复的进度。

关于数据副本管理相关的命令，可以参阅 [数据副本管理](../administrator-guide/operation/tablet-repair-and-balance.md)。

### E2. FE启动失败，fe.log中一直滚动 "wait catalog to be ready. FE type UNKNOWN"

这种问题通常有两个原因：

1. 本次FE启动时获取到的本机IP和上次启动不一致，通常是因为没有正确设置 `priority_network` 而导致 FE 启动时匹配到了错误的 IP 地址。需修改 `priority_network` 后重启 FE。

2. 集群内多数 Follower FE 节点未启动。比如有 3 个 Follower，只启动了一个。此时需要将另外至少一个 FE 也启动，FE 可选举组方能选举出 Master 已提供服务。

如果以上情况都不能解决，可以按照 Doris 官网文档中的[元数据运维文档](../administrator-guide/operation/metadata-operation.md)进行恢复：

### E3. tablet writer write failed, tablet_id=27306172, txn_id=28573520, err=-235 or -215 or -238

这个错误通常发生在数据导入操作中。新版错误码为 -235，老版本错误码可能是 -215。这个错误的含义是，对应tablet的数据版本超过了最大限制（默认500，由 BE 参数 `max_tablet_version_num` 控制），后续写入将被拒绝。比如问题中这个错误，即表示 27306172 这个tablet的数据版本超过了限制。

这个错误通常是因为导入的频率过高，大于后台数据的compaction速度，导致版本堆积并最终超过了限制。此时，我们可以先通过show tablet 27306172 语句，然后执行结果中的 show proc 语句，查看tablet各个副本的情况。结果中的 versionCount即表示版本数量。如果发现某个副本的版本数量过多，则需要降低导入频率或停止导入，并观察版本数是否有下降。如果停止导入后，版本数依然没有下降，则需要去对应的BE节点查看be.INFO日志，搜索tablet id以及 compaction关键词，检查compaction是否正常运行。关于compaction调优相关，可以参阅 ApacheDoris 公众号文章：Doris 最佳实践-Compaction调优(3)

-238 错误通常出现在同一批导入数据量过大的情况，从而导致某一个 tablet 的 Segment 文件过多（默认是 200，由 BE 参数 `max_segment_num_per_rowset` 控制）。此时建议减少一批次导入的数据量，或者适当提高 BE 配置参数值来解决。

### E4. tablet 110309738 has few replicas: 1, alive backends: [10003]

这个错误可能发生在查询或者导入操作中。通常意味着对应tablet的副本出现了异常。

此时，可以先通过 show backends 命令检查BE节点是否有宕机，如 isAlive 字段为false，或者 LastStartTime 是最近的某个时间（表示最近重启过）。如果BE有宕机，则需要去BE对应的节点，查看be.out日志。如果BE是因为异常原因宕机，通常be.out中会打印异常堆栈，帮助排查问题。如果be.out中没有错误堆栈。则可以通过linux命令dmesg -T 检查是否是因为OOM导致进程被系统kill掉。

如果没有BE节点宕机，则需要通过show tablet 110309738 语句，然后执行结果中的 show proc 语句，查看tablet各个副本的情况，进一步排查。

### E5. disk xxxxx on backend xxx exceed limit usage

通常出现在导入、Alter等操作中。这个错误意味着对应BE的对应磁盘的使用量超过了阈值（默认95%）此时可以先通过 show backends 命令，其中MaxDiskUsedPct展示的是对应BE上，使用率最高的那块磁盘的使用率，如果超过95%，则会报这个错误。

此时需要前往对应BE节点，查看数据目录下的使用量情况。其中trash目录和snapshot目录可以手动清理以释放空间。如果是data目录占用较大，则需要考虑删除部分数据以释放空间了。具体可以参阅[磁盘空间管理](../administrator-guide/operation/disk-capacity.md)。

### E6. invalid cluster id: xxxx

这个错误可能会在show backends 或 show frontends 命令的结果中出现。通常出现在某个FE或BE节点的错误信息列中。这个错误的含义是，Master FE向这个节点发送心跳信息后，该节点发现心跳信息中携带的 cluster id和本地存储的 cluster id不同，所以拒绝回应心跳。

Doris的 Master FE 节点会主动发送心跳给各个FE或BE节点，并且在心跳信息中会携带一个cluster_id。cluster_id是在一个集群初始化时，由Master FE生成的唯一集群标识。当FE或BE第一次收到心跳信息后，则会将cluster_id以文件的形式保存在本地。FE的该文件在元数据目录的image/目录下，BE则在所有数据目录下都有一个cluster_id文件。之后，每次节点收到心跳后，都会用本地cluster_id的内容和心跳中的内容作比对，如果不一致，则拒绝响应心跳。

该机制是一个节点认证机制，以防止接收到集群外的节点发送来的错误的心跳信息。

如果需要恢复这个错误。首先要先确认所有节点是否都是正确的集群中的节点。之后，对于FE节点，可以尝试修改元数据目录下的 image/VERSION 文件中的 cluster_id 值后重启FE。对于BE节点，则可以删除所有数据目录下的 cluster_id 文件后重启 BE。

### E7. 通过 Java 程序调用 stream load 导入数据，在一批次数据量较大时，可能会报错 Broken Pipe

除了 Broken Pipe 外，还可能出现一些其他的奇怪的错误。

这个情况通常出现在开启httpv2后。因为httpv2是使用spring boot实现的http 服务，并且使用tomcat作为默认内置容器。但是tomcat对307转发的处理似乎有些问题，所以后面将内置容器修改为了jetty。此外，在java程序中的 apache http client的版本需要使用4.5.13以后的版本。之前的版本，对转发的处理也存在一些问题。

所以这个问题可以有两种解决方式：

1. 关闭httpv2

    在fe.conf中添加 enable_http_server_v2=false后重启FE。但是这样无法再使用新版UI界面，并且之后的一些基于httpv2的新接口也无法使用。（正常的导入查询不受影响）。

2. 升级

    可以升级到 Doris 0.15 及之后的版本，已修复这个问题。

### E8. `Lost connection to MySQL server at 'reading initial communication packet', system error: 0`

如果使用 MySQL 客户端连接 Doris 时出现如下问题，这通常是因为编译 FE 时使用的 jdk 版本和运行 FE 时使用的 jdk 版本不同导致的。
注意使用 docker 编译镜像编译时，默认的 JDK 版本是 openjdk 11，可以通过命令切换到 openjdk 8（详见编译文档）。

### E9. -214 错误

在执行导入、查询等操作时，可能会遇到如下错误：

```
failed to initialize storage reader. tablet=63416.1050661139.aa4d304e7a7aff9c-f0fa7579928c85a0, res=-214, backend=192.168.100.10
```

-214 错误意味着对应 tablet 的数据版本缺失。比如如上错误，表示 tablet 63416 在 192.168.100.10 这个 BE 上的副本的数据版本有缺失。（可能还有其他类似错误码，都可以用如下方式进行排查和修复）。

通常情况下，如果你的数据是多副本的，那么系统会自动修复这些有问题的副本。可以通过以下步骤进行排查：

首先通过 `show tablet 63416` 语句并执行结果中的 `show proc xxx` 语句来查看对应 tablet 的各个副本情况。通常我们需要关心 `Version` 这一列的数据。

正常情况下，一个 tablet 的多个副本的 Version 应该是相同的。并且和对应分区的 VisibleVersion 版本相同。

你可以通过 `show partitions from tblx` 来查看对应的分区版本（tablet 对应的分区可以在 `show tablet` 语句中获取。）

同时，你也可以访问 `show proc` 语句中的 CompactionStatus 列中的 URL（在浏览器打开即可）来查看更具体的版本信息，来检查具体丢失的是哪些版本。

如果长时间没有自动修复，则需要通过 `show proc "/cluster_balance"` 语句，查看当前系统正在执行的 tablet 修复和调度任务。可能是因为有大量的 tablet 在等待被调度，导致修复时间较长。可以关注 `pending_tablets` 和 `running_tablets` 中的记录。

更进一步的，可以通过 `admin repair` 语句来指定优先修复某个表或分区，具体可以参阅 `help admin repair`;

如果依然无法修复，那么在多副本的情况下，我们使用 `admin set replica status` 命令强制将有问题的副本下线。具体可参阅 `help admin set replica status` 中将副本状态置为 bad 的示例。（置为 bad 后，副本将不会再被访问。并且会后续自动修复。但在操作前，应先确保其他副本是正常的）

### E10. Not connected to 192.168.100.1:8060 yet, server_id=384

在导入或者查询时，我们可能遇到这个错误。如果你去对应的 BE 日志中查看，也可能会找到类似错误。

这是一个 RPC 错误，通常有两种可能：1. 对应的 BE 节点宕机。2. rpc 拥塞或其他错误。

如果是 BE 节点宕机，则需要查看具体的宕机原因。这里只讨论 rpc 拥塞的问题。

一种情况是 OVERCROWDED，即表示 rpc 源端有大量未发送的数据超过了阈值。BE 有两个参数与之相关：

1. `brpc_socket_max_unwritten_bytes`：默认 1GB，如果未发送数据超过这个值，则会报错。可以适当修改这个值以避免 OVERCROWDED 错误。（但这个治标不治本，本质上还是有拥塞发生）。
2. `tablet_writer_ignore_eovercrowded`：默认为 false。如果设为true，则 Doris 会忽略导入过程中出现的 OVERCROWDED 错误。这个参数主要为了避免导入失败，以提高导入的稳定性。

第二种是 rpc 的包大小超过 max_body_size。如果查询中带有超大 String 类型，或者 bitmap 类型时，可能出现这个问题。可以通过修改以下 BE 参数规避：

1. `brpc_max_body_size`：默认 3GB.

### E11. `recoveryTracker should overlap or follow on disk last VLSN of 4,422,880 recoveryFirst= 4,422,882 UNEXPECTED_STATE_FATAL`

有时重启 FE，会出现如上错误（通常只会出现在多 Follower 的情况下）。并且错误中的两个数值相差2。导致 FE 启动失败。

这是 bdbje 的一个 bug，尚未解决。遇到这种情况，只能通过 [元数据运维手册](../administrator-guide/operation/metadata-operation.md) 中的 故障恢复 进行操作来恢复元数据了。

### E12.Doris编译安装JDK版本不兼容问题

在自己使用 Docker 编译 Doris 的时候，编译完成安装以后启动FE，出现 ```java.lang.Suchmethoderror: java.nio. ByteBuffer. limit (I)Ljava/nio/ByteBuffer;``` 异常信息，这是因为Docker里默认是JDK 11，如果你的安装环境是使用JDK8 ，需要在 Docker 里 JDK 环境切换成 JDK8，具体切换方法参照[编译](https://doris.apache.org/zh-CN/installing/compilation.html)
