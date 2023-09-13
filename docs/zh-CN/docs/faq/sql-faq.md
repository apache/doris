---
{
    "title": "SQL 问题",
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

# SQL问题

### Q1. 查询报错：Failed to get scan range, no queryable replica found in tablet: xxxx

这种情况是因为对应的 tablet 没有找到可以查询的副本，通常原因可能是 BE 宕机、副本缺失等。可以先通过 `show tablet tablet_id` 语句，然后执行后面的 `show proc` 语句，查看这个 tablet 对应的副本信息，检查副本是否完整。同时还可以通过 `show proc "/cluster_balance"` 信息来查询集群内副本调度和修复的进度。

关于数据副本管理相关的命令，可以参阅 [数据副本管理](../admin-manual/maint-monitor/tablet-repair-and-balance.md)。

### Q2. show backends/frontends 查看到的信息不完整

在执行如`show backends/frontends` 等某些语句后，结果中可能会发现有部分列内容不全。比如show backends结果中看不到磁盘容量信息等。

通常这个问题会出现在集群有多个FE的情况下，如果用户连接到非Master FE节点执行这些语句，就会看到不完整的信息。这是因为，部分信息仅存在于Master FE节点。比如BE的磁盘使用量信息等。所以只有在直连Master FE后，才能获得完整信息。

当然，用户也可以在执行这些语句前，先执行 `set forward_to_master=true;` 这个会话变量设置为true后，后续执行的一些信息查看类语句会自动转发到Master FE获取结果。这样，不论用户连接的是哪个FE，都可以获取到完整结果了。

### Q3. invalid cluster id: xxxx

这个错误可能会在show backends 或 show frontends 命令的结果中出现。通常出现在某个FE或BE节点的错误信息列中。这个错误的含义是，Master FE向这个节点发送心跳信息后，该节点发现心跳信息中携带的 cluster id和本地存储的 cluster id不同，所以拒绝回应心跳。

Doris的 Master FE 节点会主动发送心跳给各个FE或BE节点，并且在心跳信息中会携带一个cluster_id。cluster_id是在一个集群初始化时，由Master FE生成的唯一集群标识。当FE或BE第一次收到心跳信息后，则会将cluster_id以文件的形式保存在本地。FE的该文件在元数据目录的image/目录下，BE则在所有数据目录下都有一个cluster_id文件。之后，每次节点收到心跳后，都会用本地cluster_id的内容和心跳中的内容作比对，如果不一致，则拒绝响应心跳。

该机制是一个节点认证机制，以防止接收到集群外的节点发送来的错误的心跳信息。

如果需要恢复这个错误。首先要先确认所有节点是否都是正确的集群中的节点。之后，对于FE节点，可以尝试修改元数据目录下的 image/VERSION 文件中的 cluster_id 值后重启FE。对于BE节点，则可以删除所有数据目录下的 cluster_id 文件后重启 BE。

### Q4. Unique Key 模型查询结果不一致

某些情况下，当用户使用相同的 SQL 查询一个 Unique Key 模型的表时，可能会出现多次查询结果不一致的现象。并且查询结果总在 2-3 种之间变化。

这可能是因为，在同一批导入数据中，出现了 key 相同但 value 不同的数据，这会导致，不同副本间，因数据覆盖的先后顺序不确定而产生的结果不一致的问题。

比如表定义为 k1, v1。一批次导入数据如下：

```text
1, "abc"
1, "def"
```

那么可能副本1 的结果是 `1, "abc"`，而副本2 的结果是 `1, "def"`。从而导致查询结果不一致。

为了确保不同副本之间的数据先后顺序唯一，可以参考 [Sequence Column](../data-operate/update-delete/sequence-column-manual.md) 功能。

### Q5. 查询 bitmap/hll 类型的数据返回 NULL 的问题

在 1.1.x 版本中，在开启向量化的情况下，执行查询数据表中 bitmap 类型字段返回结果为 NULL 的情况下，

1. 首先你要 `set return_object_data_as_binary=true;`
2. 关闭向量化 `set enable_vectorized_engine=false;`
3. 关闭 SQL 缓存 `set [global] enable_sql_cache = false;`

这里是因为 bitmap / hll 类型在向量化执行引擎中：输入均为NULL，则输出的结果也是NULL而不是0

### Q6. 访问对象存储时报错：curl 77: Problem with the SSL CA cert

如果 be.INFO 日志中出现 `curl 77: Problem with the SSL CA cert` 错误。可以尝试通过以下方式解决：

1. 在 [https://curl.se/docs/caextract.html](https://curl.se/docs/caextract.html) 下载证书：cacert.pem
2. 拷贝证书到指定位置：`sudo cp /tmp/cacert.pem /etc/ssl/certs/ca-certificates.crt`
3. 重启 BE 节点。

### Q7. 导入报错："Message": "[INTERNAL_ERROR]single replica load is disabled on BE."

1. be.conf中增加 enable_single_replica_load = true
2. 重启 BE 节点。