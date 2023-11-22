---
{
    "title": "元数据运维",
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

# 元数据运维

本文档主要介绍在实际生产环境中，如何对 Doris 的元数据进行管理。包括 FE 节点建议的部署方式、一些常用的操作方法、以及常见错误的解决方法。

在阅读本文当前，请先阅读 [Doris 元数据设计文档](/community/design/metadata-design) 了解 Doris 元数据的工作原理。

## 重要提示

* 当前元数据的设计是无法向后兼容的。即如果新版本有新增的元数据结构变动（可以查看 FE 代码中的 `FeMetaVersion.java` 文件中是否有新增的 VERSION），那么在升级到新版本后，通常是无法再回滚到旧版本的。所以，在升级 FE 之前，请务必按照 [升级文档](../../admin-manual/cluster-management/upgrade.md) 中的操作，测试元数据兼容性。

## 元数据目录结构

我们假设在 fe.conf 中指定的 `meta_dir` 的路径为 `/path/to/doris-meta`。那么一个正常运行中的 Doris 集群，元数据的目录结构应该如下：

```
/path/to/doris-meta/
            |-- bdb/
            |   |-- 00000000.jdb
            |   |-- je.config.csv
            |   |-- je.info.0
            |   |-- je.info.0.lck
            |   |-- je.lck
            |   `-- je.stat.csv
            `-- image/
                |-- ROLE
                |-- VERSION
                `-- image.xxxx
```

1. bdb 目录

    我们将 [bdbje](https://www.oracle.com/technetwork/database/berkeleydb/overview/index-093405.html) 作为一个分布式的 kv 系统，存放元数据的 journal。这个 bdb 目录相当于 bdbje 的 “数据目录”。
    
    其中 `.jdb` 后缀的是 bdbje 的数据文件。这些数据文件会随着元数据 journal 的不断增多而越来越多。当 Doris 定期做完 image 后，旧的日志就会被删除。所以正常情况下，这些数据文件的总大小从几 MB 到几 GB 不等（取决于使用 Doris 的方式，如导入频率等）。当数据文件的总大小大于 10GB，则可能需要怀疑是否是因为 image 没有成功，或者分发 image 失败导致的历史 journal 一直无法删除。
    
    `je.info.0` 是 bdbje 的运行日志。这个日志中的时间是 UTC+0 时区的。我们可能在后面的某个版本中修复这个问题。通过这个日志，也可以查看一些 bdbje 的运行情况。

2. image 目录

    image 目录用于存放 Doris 定期生成的元数据镜像文件。通常情况下，你会看到有一个 `image.xxxxx` 的镜像文件。其中 `xxxxx` 是一个数字。这个数字表示该镜像包含 `xxxxx` 号之前的所有元数据 journal。而这个文件的生成时间（通过 `ls -al` 查看即可）通常就是镜像的生成时间。
    
    你也可能会看到一个 `image.ckpt` 文件。这是一个正在生成的元数据镜像。通过 `du -sh` 命令应该可以看到这个文件大小在不断变大，说明镜像内容正在写入这个文件。当镜像写完后，会自动重名为一个新的 `image.xxxxx` 并替换旧的 image 文件。
    
    只有角色为 Master 的 FE 才会主动定期生成 image 文件。每次生成完后，都会推送给其他非 Master 角色的 FE。当确认其他所有 FE 都收到这个 image 后，Master FE 会删除 bdbje 中旧的元数据 journal。所以，如果 image 生成失败，或者 image 推送给其他 FE 失败时，都会导致 bdbje 中的数据不断累积。
    
    `ROLE` 文件记录了 FE 的类型（FOLLOWER 或 OBSERVER），是一个文本文件。
    
    `VERSION` 文件记录了这个 Doris 集群的 cluster id，以及用于各个节点之间访问认证的 token，也是一个文本文件。
    
    `ROLE` 文件和 `VERSION` 文件只可能同时存在，或同时不存在（如第一次启动时）。

## 基本操作

### 启动单节点 FE

单节点 FE 是最基本的一种部署方式。一个完整的 Doris 集群，至少需要一个 FE 节点。当只有一个 FE 节点时，这个节点的类型为 Follower，角色为 Master。

1. 第一次启动

    1. 假设在 fe.conf 中指定的 `meta_dir` 的路径为 `/path/to/doris-meta`。
    2. 确保 `/path/to/doris-meta` 已存在，权限正确，且目录为空。
    3. 直接通过 `sh bin/start_fe.sh` 即可启动。
    4. 启动后，你应该可以在 fe.log 中看到如下日志：
    
        * Palo FE starting...
        * image does not exist: /path/to/doris-meta/image/image.0
        * transfer from INIT to UNKNOWN
        * transfer from UNKNOWN to MASTER
        * the very first time to open bdb, dbname is 1
        * start fencing, epoch number is 1
        * finish replay in xxx msec
        * QE service start
        * thrift server started

        以上日志不一定严格按照这个顺序，但基本类似。

    5. 单节点 FE 的第一次启动通常不会遇到问题。如果你没有看到以上日志，一般来说是没有仔细按照文档步骤操作，请仔细阅读相关 wiki。

2. 重启

    1. 直接使用 `sh bin/start_fe.sh` 可以重新启动已经停止的 FE 节点。
    2. 重启后，你应该可以在 fe.log 中看到如下日志：

        * Palo FE starting...
        * finished to get cluster id: xxxx, role: FOLLOWER and node name: xxxx
        * 如果重启前还没有 image 产生，则会看到：
            * image does not exist: /path/to/doris-meta/image/image.0
            
        * 如果重启前有 image 产生，则会看到：
            * start load image from /path/to/doris-meta/image/image.xxx. is ckpt: false
            * finished load image in xxx ms

        * transfer from INIT to UNKNOWN
        * replayed journal id is xxxx, replay to journal id is yyyy
        * transfer from UNKNOWN to MASTER
        * finish replay in xxx msec
        * master finish replay journal, can write now.
        * begin to generate new image: image.xxxx
        *  start save image to /path/to/doris-meta/image/image.ckpt. is ckpt: true
        *  finished save image /path/to/doris-meta/image/image.ckpt in xxx ms. checksum is xxxx
        *  push image.xxx to other nodes. totally xx nodes, push successed xx nodes
        * QE service start
        * thrift server started

        以上日志不一定严格按照这个顺序，但基本类似。
    
3. 常见问题

    对于单节点 FE 的部署，启停通常不会遇到什么问题。如果有问题，请先参照相关 wiki，仔细核对你的操作步骤。

### 添加 FE

添加 FE 流程在 [弹性扩缩容](../../admin-manual/cluster-management/elastic-expansion.md) 有详细介绍，不再赘述。这里主要说明一些注意事项，以及常见问题。

1. 注意事项

    * 在添加新的 FE 之前，一定先确保当前的 Master FE 运行正常（连接是否正常，JVM 是否正常，image 生成是否正常，bdbje 数据目录是否过大等等）
    * 第一次启动新的 FE，一定确保添加了 `--helper` 参数指向 Master FE。再次启动时可不用添加 `--helper`。（如果指定了 `--helper`，FE 会直接询问 helper 节点自己的角色，如果没有指定，FE会尝试从 `doris-meta/image/` 目录下的 `ROLE` 和 `VERSION` 文件中获取信息）。
    * 第一次启动新的 FE，一定确保这个 FE 的 `meta_dir` 已经创建、权限正确且为空。
    * 启动新的 FE，和执行 `ALTER SYSTEM ADD FOLLOWER/OBSERVER` 语句在元数据添加 FE，这两个操作的顺序没有先后要求。如果先启动了新的 FE，而没有执行语句，则新的 FE 日志中会一直滚动 `current node is not added to the group. please add it first.` 字样。当执行语句后，则会进入正常流程。
    * 请确保前一个 FE 添加成功后，再添加下一个 FE。
    * 建议直接连接到 MASTER FE 执行 `ALTER SYSTEM ADD FOLLOWER/OBSERVER` 语句。

2. 常见问题

    1. this node is DETACHED
    
        当第一次启动一个待添加的 FE 时，如果 Master FE 上的 doris-meta/bdb 中的数据很大，则可能在待添加的 FE 日志中看到 `this node is DETACHED.` 字样。这时，bdbje 正在复制数据，你可以看到待添加的 FE 的 `bdb/` 目录正在变大。这个过程通常会在数分钟不等（取决于 bdbje 中的数据量）。之后，fe.log 中可能会有一些 bdbje 相关的错误堆栈信息。如果最终日志中显示 `QE service start` 和 `thrift server started`，则通常表示启动成功。可以通过 mysql-client 连接这个 FE 尝试操作。如果没有出现这些字样，则可能是 bdbje 复制日志超时等问题。这时，直接再次重启这个 FE，通常即可解决问题。
        
    2. 各种原因导致添加失败

        * 如果添加的是 OBSERVER，因为 OBSERVER 类型的 FE 不参与元数据的多数写，理论上可以随意启停。因此，对于添加 OBSERVER 失败的情况。可以直接杀死 OBSERVER FE 的进程，清空 OBSERVER 的元数据目录后，重新进行一遍添加流程。

        * 如果添加的是 FOLLOWER，因为 FOLLOWER 是参与元数据多数写的。所以有可能FOLLOWER 已经加入 bdbje 选举组内。如果这时只有两个 FOLLOWER 节点（包括 MASTER），那么停掉一个 FE，可能导致另一个 FE 也因无法进行多数写而退出。此时，我们应该先通过 `ALTER SYSTEM DROP FOLLOWER` 命令，从元数据中删除新添加的 FOLLOWER 节点，然后再杀死 FOLLOWER 进程，清空元数据，重新进行一遍添加流程。

    
### 删除 FE

通过 `ALTER SYSTEM DROP FOLLOWER/OBSERVER` 命令即可删除对应类型的 FE。以下有几点注意事项：

* 对于 OBSERVER 类型的 FE，直接 DROP 即可，无风险。

* 对于 FOLLOWER 类型的 FE。首先，应保证在有奇数个 FOLLOWER 的情况下（3个或以上），开始删除操作。

    1. 如果删除非 MASTER 角色的 FE，建议连接到 MASTER FE，执行 DROP 命令，再杀死进程即可。
    2. 如果要删除 MASTER FE，先确认有奇数个 FOLLOWER FE 并且运行正常。然后先杀死 MASTER FE 的进程。这时会有某一个 FE 被选举为 MASTER。在确认剩下的 FE 运行正常后，连接到新的 MASTER FE，执行 DROP 命令删除之前老的 MASTER FE 即可。

## 高级操作

### 故障恢复

FE 有可能因为某些原因出现无法启动 bdbje、FE 之间无法同步等问题。现象包括无法进行元数据写操作、没有 MASTER 等等。这时，我们需要手动操作来恢复 FE。手动恢复 FE 的大致原理，是先通过当前 `meta_dir` 中的元数据，启动一个新的 MASTER，然后再逐台添加其他 FE。请严格按照如下步骤操作：

1. 首先，**停止所有 FE 进程，同时停止一切业务访问**。保证在元数据恢复期间，不会因为外部访问导致其他不可预期的问题。(如果没有停止所有FE进程，后续流程可能出现脑裂现象)

2. 确认哪个 FE 节点的元数据是最新：

    * 首先，**务必先备份所有 FE 的 `meta_dir` 目录。**
    * 通常情况下，Master FE 的元数据是最新的。可以查看 `meta_dir/image` 目录下，image.xxxx 文件的后缀，数字越大，则表示元数据越新。
    * 通常，通过比较所有 FOLLOWER FE 的 image 文件，找出最新的元数据即可。
    * 之后，我们要使用这个拥有最新元数据的 FE 节点，进行恢复。
    * 如果使用 OBSERVER 节点的元数据进行恢复会比较麻烦，建议尽量选择 FOLLOWER 节点。

3. 以下操作都在由第2步中选择出来的 FE 节点上进行。

    1. 修改 `fe.conf` 
      - 如果该节点是一个 OBSERVER，先将 `meta_dir/image/ROLE` 文件中的 `role=OBSERVER` 改为 `role=FOLLOWER`。（从 OBSERVER 节点恢复会比较麻烦，先按这里的步骤操作，后面会有单独说明）)
      - 如果 FE 版本< 2.0.2, 则还需要在 fe.conf 中添加配置：`metadata_failure_recovery=true`。
    2. 执行 `sh bin/start_fe.sh --metadata_failure_recovery` 启动这个 FE。
    3. 如果正常，这个 FE 会以 MASTER 的角色启动，类似于前面 `启动单节点 FE` 一节中的描述。在 fe.log 应该会看到 `transfer from XXXX to MASTER` 等字样。
    4. 启动完成后，先连接到这个 FE，执行一些查询导入，检查是否能够正常访问。如果不正常，有可能是操作有误，建议仔细阅读以上步骤，用之前备份的元数据再试一次。如果还是不行，问题可能就比较严重了。
    5. 如果成功，通过 `show frontends;` 命令，应该可以看到之前所添加的所有 FE，并且当前 FE 是 master。
    6. 后重启这个 FE（**重要**）。
    7. **如果 FE 版本 < 2.0.2**，将 fe.conf 中的 `metadata_failure_recovery=true` 配置项删除，或者设置为 `false`，然后重启这个 FE（**重要**）。


    > 如果你是从一个 OBSERVER 节点的元数据进行恢复的，那么完成如上步骤后，通过 `show frontends;` 语句你会发现，当前这个 FE 的角色为 OBSERVER，但是 `IsMaster` 显示为 `true`。这是因为，这里看到的 “OBSERVER” 是记录在 Doris 的元数据中的，而是否是 master，是记录在 bdbje 的元数据中的。因为我们是从一个 OBSERVER 节点恢复的，所以这里出现了不一致。请按如下步骤修复这个问题（这个问题我们会在之后的某个版本修复）：
    
    > 1. 先把除了这个 “OBSERVER” 以外的所有 FE 节点 DROP 掉。
    > 2. 通过 `ADD FOLLOWER` 命令，添加一个新的 FOLLOWER FE，假设在 hostA 上。
    > 3. 在 hostA 上启动一个全新的 FE，通过 `--helper` 的方式加入集群。
    > 4. 启动成功后，通过 `show frontends;` 语句，你应该能看到两个 FE，一个是之前的  OBSERVER，一个是新添加的 FOLLOWER，并且 OBSERVER 是 master。
    > 5. 确认这个新的 FOLLOWER 是可以正常工作之后，用这个新的 FOLLOWER 的元数据，重新执行一遍故障恢复操作。
    > 6. 以上这些步骤的目的，其实就是人为的制造出一个 FOLLOWER 节点的元数据，然后用这个元数据，重新开始故障恢复。这样就避免了从 OBSERVER 恢复元数据所遇到的不一致的问题。
    
    > `metadata_failure_recovery` 的含义是，清空 "bdbje" 的元数据。这样 bdbje 就不会再联系之前的其他 FE 了，而作为一个独立的 FE 启动。这个参数只有在恢复启动时才需要设置为 true。恢复完成后，一定要设置为 false，否则一旦重启，bdbje 的元数据又会被清空，导致其他 FE 无法正常工作。

4. 第3步执行成功后，我们再通过 `ALTER SYSTEM DROP FOLLOWER/OBSERVER` 命令，将之前的其他的 FE 从元数据删除后，按加入新 FE 的方式，重新把这些 FE 添加一遍。

5. 如果以上操作正常，则恢复完毕。

### FE 类型变更

如果你需要将当前已有的 FOLLOWER/OBSERVER 类型的 FE，变更为 OBSERVER/FOLLOWER 类型，请先按照前面所述的方式删除 FE，再添加对应类型的 FE 即可

### FE 迁移

如果你需要将一个 FE 从当前节点迁移到另一个节点，分以下几种情况。

1. 非 MASTER 节点的 FOLLOWER，或者 OBSERVER 迁移

    直接添加新的 FOLLOWER/OBSERVER 成功后，删除旧的 FOLLOWER/OBSERVER 即可。
    
2. 单节点 MASTER 迁移

    当只有一个 FE 时，参考 `故障恢复` 一节。将 FE 的 doris-meta 目录拷贝到新节点上，按照 `故障恢复` 一节中，步骤3的方式启动新的 MASTER
    
3. 一组 FOLLOWER 从一组节点迁移到另一组新的节点

    在新的节点上部署 FE，通过添加 FOLLOWER 的方式先加入新节点。再逐台 DROP 掉旧节点即可。在逐台 DROP 的过程中，MASTER 会自动选择在新的 FOLLOWER 节点上。
    
### 更换 FE 端口

FE 目前有以下几个端口

* edit_log_port：bdbje 的通信端口
* http_port：http 端口，也用于推送 image
* rpc_port：FE 的 thrift server port
* query_port：Mysql 连接端口
* arrow_flight_sql_port: Arrow Flight SQL 连接端口

1. edit_log_port

    如果需要更换这个端口，则需要参照 `故障恢复` 一节中的操作，进行恢复。因为该端口已经被持久化到 bdbje 自己的元数据中（同时也记录在 Doris 自己的元数据中），需要启动 FE 时通过指定 `--metadata_failure_recovery` 来清空 bdbje 的元数据。
    
2. http_port

    所有 FE 的 http_port 必须保持一致。所以如果要修改这个端口，则所有 FE 都需要修改并重启。修改这个端口，在多 FOLLOWER 部署的情况下会比较复杂（涉及到鸡生蛋蛋生鸡的问题...），所以不建议有这种操作。如果必须，直接按照 `故障恢复` 一节中的操作吧。
    
3. rpc_port

    修改配置后，直接重启 FE 即可。Master FE 会通过心跳将新的端口告知 BE。只有 Master FE 的这个端口会被使用。但仍然建议所有 FE 的端口保持一致。
    
4. query_port

    修改配置后，直接重启 FE 即可。这个只影响到 mysql 的连接目标。

5. arrow_flight_sql_port

    修改配置后，直接重启 FE 即可。这个只影响到 Arrow Flight SQL 的连接目标。

### 从 FE 内存中恢复元数据

在某些极端情况下，磁盘上 image 文件可能会损坏，但是内存中的元数据是完好的，此时我们可以先从内存中 dump 出元数据，再替换掉磁盘上的 image 文件，来恢复元数据，整个**不停查询服务**的操作步骤如下：
1. 集群停止所有 Load,Create,Alter 操作
2. 执行以下命令，从 Master FE 内存中 dump 出元数据：(下面称为 image_mem)
```
curl -u $root_user:$password http://$master_hostname:8030/dump
```

3. 用 image_mem 文件替换掉 OBSERVER FE 节点上`meta_dir/image`目录下的 image 文件，重启 OBSERVER FE 节点，
验证 image_mem 文件的完整性和正确性（可以在 FE Web 页面查看 DB 和 Table 的元数据是否正常，查看fe.log 是否有异常，是否在正常 replayed journal）

    自 1.2.0 版本起，推荐使用以下功能验证 `image_mem` 文件：

    ```
    sh start_fe.sh --image path_to_image_mem
    ```

    > 注意：`path_to_image_mem` 是 image_mem 文件的路径。
    >
    > 如果文件有效会输出 `Load image success. Image file /absolute/path/to/image.xxxxxx is valid`。
    >
    > 如果文件无效会输出 `Load image failed. Image file /absolute/path/to/image.xxxxxx is invalid`。

4. 依次用 image_mem 文件替换掉 FOLLOWER FE 节点上`meta_dir/image`目录下的 image 文件，重启 FOLLOWER FE 节点，
确认元数据和查询服务都正常

5. 用 image_mem 文件替换掉 Master FE 节点上`meta_dir/image`目录下的 image 文件，重启 Master FE 节点，
确认 FE Master 切换正常， Master FE 节点可以通过 checkpoint 正常生成新的 image 文件
6. 集群恢复所有 Load,Create,Alter 操作

**注意：如果 Image 文件很大，整个操作过程耗时可能会很长，所以在此期间，要确保 Master FE 不会通过 checkpoint 生成新的 image 文件。
当观察到 Master FE 节点上 `meta_dir/image`目录下的 `image.ckpt` 文件快和 `image.xxx` 文件一样大时，可以直接删除掉`image.ckpt` 文件。**

### 查看 BDBJE 中的数据

FE 的元数据日志以 Key-Value 的方式存储在 BDBJE 中。某些异常情况下，可能因为元数据错误而无法启动 FE。在这种情况下，Doris 提供一种方式可以帮助用户查询 BDBJE 中存储的数据，以方便进行问题排查。

首先需在 fe.conf 中增加配置：`enable_bdbje_debug_mode=true`，之后通过 `sh start_fe.sh --daemon` 启动 FE。

此时，FE 将进入 debug 模式，仅会启动 http server 和 MySQL server，并打开 BDBJE 实例，但不会进行任何元数据的加载及后续其他启动流程。

这时，我们可以通过访问 FE 的 web 页面，或通过 MySQL 客户端连接到 Doris 后，通过 `show proc "/bdbje";` 来查看 BDBJE 中存储的数据。

```
mysql> show proc "/bdbje";
+----------+---------------+---------+
| DbNames  | JournalNumber | Comment |
+----------+---------------+---------+
| 110589   | 4273          |         |
| epochDB  | 4             |         |
| metricDB | 430694        |         |
+----------+---------------+---------+
```

第一级目录会展示 BDBJE 中所有的 database 名称，以及每个 database 中的 entry 数量。

```
mysql> show proc "/bdbje/110589";
+-----------+
| JournalId |
+-----------+
| 1         |
| 2         |

...
| 114858    |
| 114859    |
| 114860    |
| 114861    |
+-----------+
4273 rows in set (0.06 sec)
```

进入第二级，则会罗列指定 database 下的所有 entry 的 key。

```
mysql> show proc "/bdbje/110589/114861";
+-----------+--------------+---------------------------------------------+
| JournalId | OpType       | Data                                        |
+-----------+--------------+---------------------------------------------+
| 114861    | OP_HEARTBEAT | org.apache.doris.persist.HbPackage@6583d5fb |
+-----------+--------------+---------------------------------------------+
1 row in set (0.05 sec)
```

第三级则可以展示指定 key 的 value 信息。

## 最佳实践

FE 的部署推荐，在 [安装与部署文档](../../install/standard-deployment.md) 中有介绍，这里再做一些补充。

* **如果你并不十分了解 FE 元数据的运行逻辑，或者没有足够 FE 元数据的运维经验，我们强烈建议在实际使用中，只部署一个 FOLLOWER 类型的 FE 作为 MASTER，其余 FE 都是 OBSERVER，这样可以减少很多复杂的运维问题！** 不用过于担心 MASTER 单点故障导致无法进行元数据写操作。首先，如果你配置合理，FE 作为 java 进程很难挂掉。其次，如果 MASTER 磁盘损坏（概率非常低），我们也可以用 OBSERVER 上的元数据，通过 `故障恢复` 的方式手动恢复。

* FE 进程的 JVM 一定要保证足够的内存。我们**强烈建议** FE 的 JVM 内存至少在 10GB 以上，推荐 32GB 至 64GB。并且部署监控来监控 JVM 的内存使用情况。因为如果FE出现OOM，可能导致元数据写入失败，造成一些**无法恢复**的故障！

* FE 所在节点要有足够的磁盘空间，以防止元数据过大导致磁盘空间不足。同时 FE 日志也会占用十几G 的磁盘空间。

## 其他常见问题

1. fe.log 中一直滚动 `meta out of date. current time: xxx, synchronized time: xxx, has log: xxx, fe type: xxx`

    这个通常是因为 FE 无法选举出 Master。比如配置了 3 个 FOLLOWER，但是只启动了一个 FOLLOWER，则这个 FOLLOWER 会出现这个问题。通常，只要把剩余的 FOLLOWER 启动起来就可以了。如果启动起来后，仍然没有解决问题，那么可能需要按照 `故障恢复` 一节中的方式，手动进行恢复。
    
2. `Clock delta: xxxx ms. between Feeder: xxxx and this Replica exceeds max permissible delta: xxxx ms.`

    bdbje 要求各个节点之间的时钟误差不能超过一定阈值。如果超过，节点会异常退出。我们默认设置的阈值为 5000 ms，由 FE 的参数 `max_bdbje_clock_delta_ms` 控制，可以酌情修改。但我们建议使用 ntp 等时钟同步方式保证 Doris 集群各主机的时钟同步。
    
3. `image/` 目录下的镜像文件很久没有更新

    Master FE 会默认每 50000 条元数据 journal，生成一个镜像文件。在一个频繁使用的集群中，通常每隔半天到几天的时间，就会生成一个新的 image 文件。如果你发现 image 文件已经很久没有更新了（比如超过一个星期），则可以顺序的按照如下方法，查看具体原因：
    
    1. 在 Master FE 的 fe.log 中搜索 `memory is not enough to do checkpoint. Committed memory xxxx Bytes, used memory xxxx Bytes.` 字样。如果找到，则说明当前 FE 的 JVM 内存不足以用于生成镜像（通常我们需要预留一半的 FE 内存用于 image 的生成）。那么需要增加 JVM 的内存并重启 FE 后，再观察。每次 Master FE 重启后，都会直接生成一个新的 image。也可用这种重启方式，主动地生成新的 image。注意，如果是多 FOLLOWER 部署，那么当你重启当前 Master FE 后，另一个 FOLLOWER FE 会变成 MASTER，则后续的 image 生成会由新的 Master 负责。因此，你可能需要修改所有 FOLLOWER FE 的 JVM 内存配置。

    2. 在 Master FE 的 fe.log 中搜索 `begin to generate new image: image.xxxx`。如果找到，则说明开始生成 image 了。检查这个线程的后续日志，如果出现 `checkpoint finished save image.xxxx`，则说明 image 写入成功。如果出现 `Exception when generate new image file`，则生成失败，需要查看具体的错误信息。


4. `bdb/` 目录的大小非常大，达到几个G或更多

    如果在排除无法生成新的 image 的错误后，bdb 目录在一段时间内依然很大。则可能是因为 Master FE 推送 image 不成功。可以在 Master FE 的 fe.log 中搜索 `push image.xxxx to other nodes. totally xx nodes, push successed yy nodes`。如果 yy 比 xx 小，则说明有的 FE 没有被推送成功。可以在 fe.log 中查看到具体的错误 `Exception when pushing image file. url = xxx`。

    同时，你也可以在 FE 的配置文件中添加配置：`edit_log_roll_num=xxxx`。该参数设定了每多少条元数据 journal，做一次 image。默认是 50000。可以适当改小这个数字，使得 image 更加频繁，从而加速删除旧的 journal。

5. FOLLOWER FE 接连挂掉

    因为 Doris 的元数据采用多数写策略，即一条元数据 journal 必须至少写入多数个 FOLLOWER FE 后（比如 3 个 FOLLOWER，必须写成功 2 个），才算成功。而如果写入失败，FE 进程会主动退出。那么假设有 A、B、C 三个 FOLLOWER，C 先挂掉，然后 B 再挂掉，那么 A 也会跟着挂掉。所以如 `最佳实践` 一节中所述，如果你没有丰富的元数据运维经验，不建议部署多 FOLLOWER。

6. fe.log 中出现 `get exception when try to close previously opened bdb database. ignore it`

    如果后面有 `ignore it` 字样，通常无需处理。如果你有兴趣，可以在 `BDBEnvironment.java` 搜索这个错误，查看相关注释说明。

7. 从 `show frontends;` 看，某个 FE 的 `Join` 列为 `true`，但是实际该 FE 不正常

    通过 `show frontends;` 查看到的 `Join` 信息。该列如果为 `true`，仅表示这个 FE **曾经加入过** 集群。并不能表示当前仍然正常的存在于集群中。如果为 `false`，则表示这个 FE **从未加入过** 集群。

8. 关于 FE 的配置 `master_sync_policy`, `replica_sync_policy` 和 `txn_rollback_limit`

    `master_sync_policy` 用于指定当 Leader FE 写元数据日志时，是否调用 fsync(), `replica_sync_policy` 用于指定当 FE HA 部署时，其他 Follower FE 在同步元数据时，是否调用 fsync()。在早期的 Doris 版本中，这两个参数默认是 `WRITE_NO_SYNC`，即都不调用 fsync()。在最新版本的 Doris 中，默认已修改为 `SYNC`，即都调用 fsync()。调用 fsync() 会显著降低元数据写盘的效率。在某些环境下，IOPS 可能降至几百，延迟增加到2-3ms（但对于 Doris 元数据操作依然够用）。因此我们建议以下配置：

    1. 对于单 Follower FE 部署，`master_sync_policy` 设置为 `SYNC`，防止 FE 系统宕机导致元数据丢失。
    2. 对于多 Follower FE 部署，可以将 `master_sync_policy` 和 `replica_sync_policy` 设为 `WRITE_NO_SYNC`，因为我们认为多个系统同时宕机的概率非常低。

    如果在单 Follower FE 部署中，`master_sync_policy` 设置为 `WRITE_NO_SYNC`，则可能出现 FE 系统宕机导致元数据丢失。这时如果有其他 Observer FE 尝试重启时，可能会报错：

    ```
    Node xxx must rollback xx total commits(numPassedDurableCommits of which were durable) to the earliest point indicated by transaction xxxx in order to rejoin the replication group, but the transaction rollback limit of xxx prohibits this.
    ```

    意思有部分已经持久化的事务需要回滚，但条数超过上限。这里我们的默认上限是 100，可以通过设置 `txn_rollback_limit` 改变。该操作仅用于尝试正常启动 FE，但已丢失的元数据无法恢复。
