---
{
    "title": "标准部署",
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

# 标准部署

该文档主要介绍了部署 Doris 所需软硬件环境、建议的部署方式、集群扩容缩容，以及集群搭建到运行过程中的常见问题。  
在阅读本文档前，请先根据编译文档编译 Doris。

## 软硬件需求

### 概述

Doris 作为一款开源的 MPP 架构 OLAP 数据库，能够运行在绝大多数主流的商用服务器上。为了能够充分运用 MPP 架构的并发优势，以及 Doris 的高可用特性，我们建议 Doris 的部署遵循以下需求：

#### Linux 操作系统版本需求

| Linux 系统 | 版本 |
|---|---|
| CentOS | 7.1 及以上 |
| Ubuntu | 16.04 及以上 |

#### 软件需求

| 软件 | 版本 |
|---|---|
| Java | 1.8  |
| GCC  | 4.8.2 及以上 |

#### 操作系统安装要求

##### 设置系统最大打开文件句柄数

```
vi /etc/security/limits.conf 
* soft nofile 65536
* hard nofile 65536
```

##### 时钟同步

Doris 的元数据要求时间精度要小于5000ms，所以所有集群所有机器要进行时钟同步，避免因为时钟问题引发的元数据不一致导致服务出现异常。

##### 关闭交换分区（swap）

Linux交换分区会给Doris带来很严重的性能问题，需要在安装之前禁用交换分区

##### Linux文件系统

ext4和xfs文件系统均支持。

#### 开发测试环境

| 模块 | CPU | 内存 | 磁盘 | 网络 | 实例数量 |
|---|---|---|---|---|---|
| Frontend | 8核+ | 8GB+ | SSD 或 SATA，10GB+ * | 千兆网卡 | 1 |
| Backend | 8核+ | 16GB+ | SSD 或 SATA，50GB+ * | 千兆网卡 | 1-3 * |

#### 生产环境

| 模块 | CPU | 内存 | 磁盘 | 网络 | 实例数量（最低要求） |
|---|---|---|---|---|------------|
| Frontend | 16核+ | 64GB+ | SSD 或 RAID 卡，100GB+ * | 万兆网卡 | 1-3 *      |
| Backend | 16核+ | 64GB+ | SSD 或 SATA，100G+ * | 万兆网卡 | 3 *        |

> 注1：
> 1. FE 的磁盘空间主要用于存储元数据，包括日志和 image。通常从几百 MB 到几个 GB 不等。
> 2. BE 的磁盘空间主要用于存放用户数据，总磁盘空间按用户总数据量 * 3（3副本）计算，然后再预留额外 40% 的空间用作后台 compaction 以及一些中间数据的存放。
> 3. 一台机器上虽然可以部署多个 BE，**但只建议部署一个实例**，同时**只能部署一个 FE**。如果需要 3 副本数据，那么至少需要 3 台机器各部署一个 BE 实例（而不是1台机器部署3个BE实例）。**多个FE所在服务器的时钟必须保持一致（允许最多5秒的时钟偏差）**
> 4. 测试环境也可以仅适用一个 BE 进行测试。实际生产环境，BE 实例数量直接决定了整体查询延迟。
> 5. 所有部署节点关闭 Swap。

> 注2：FE 节点的数量
> 1. FE 角色分为 Follower 和 Observer，（Leader 为 Follower 组中选举出来的一种角色，以下统称 Follower）。
> 2. FE 节点数据至少为1（1 个 Follower）。当部署 1 个 Follower 和 1 个 Observer 时，可以实现读高可用。当部署 3 个 Follower 时，可以实现读写高可用（HA）。
> 3. Follower 的数量**必须**为奇数，Observer 数量随意。
> 4. 根据以往经验，当集群可用性要求很高时（比如提供在线业务），可以部署 3 个 Follower 和 1-3 个 Observer。如果是离线业务，建议部署 1 个 Follower 和 1-3 个 Observer。

* **通常我们建议 10 ~ 100 台左右的机器，来充分发挥 Doris 的性能（其中 3 台部署 FE（HA），剩余的部署 BE）**
* **当然，Doris的性能与节点数量及配置正相关。在最少4台机器（一台 FE，三台 BE，其中一台 BE 混部一个 Observer FE 提供元数据备份），以及较低配置的情况下，依然可以平稳的运行 Doris。**
* **如果 FE 和 BE 混部，需注意资源竞争问题，并保证元数据目录和数据目录分属不同磁盘。**

#### Broker 部署

Broker 是用于访问外部数据源（如 hdfs）的进程。通常，在每台机器上部署一个 broker 实例即可。

#### 网络需求

Doris 各个实例直接通过网络进行通讯。以下表格展示了所有需要的端口

| 实例名称 | 端口名称 | 默认端口 | 通讯方向 | 说明 |
|---|---|---|---| ---|
| BE | be_port | 9060 | FE --> BE | BE 上 thrift server 的端口，用于接收来自 FE 的请求 |
| BE | webserver_port | 8040 | BE <--> BE | BE 上的 http server 的端口 |
| BE | heartbeat\_service_port | 9050 | FE --> BE | BE 上心跳服务端口（thrift），用于接收来自 FE 的心跳 |
| BE | brpc\_port | 8060 | FE <--> BE, BE <--> BE | BE 上的 brpc 端口，用于 BE 之间通讯 |
| FE | http_port  | 8030 | FE <--> FE，用户 <--> FE |FE 上的 http server 端口 |
| FE | rpc_port | 9020 | BE --> FE, FE <--> FE | FE 上的 thrift server 端口，每个fe的配置需要保持一致|
| FE | query_port | 9030 | 用户 <--> FE | FE 上的 mysql server 端口 |
| FE | arrow_flight_sql_port | 9040 | 用户 <--> FE | FE 上的 Arrow Flight SQL server 端口 |
| FE | edit\_log_port | 9010 | FE <--> FE | FE 上的 bdbje 之间通信用的端口 |
| Broker | broker\_ipc_port | 8000 | FE --> Broker, BE --> Broker | Broker 上的 thrift server，用于接收请求 |

> 注：
> 1. 当部署多个 FE 实例时，要保证 FE 的 http\_port 配置相同。
> 2. 部署前请确保各个端口在应有方向上的访问权限。

#### IP 绑定

因为有多网卡的存在，或因为安装过 docker 等环境导致的虚拟网卡的存在，同一个主机可能存在多个不同的 ip。当前 Doris 并不能自动识别可用 IP。所以当遇到部署主机上有多个 IP 时，必须通过 priority\_networks 配置项来强制指定正确的 IP。

priority\_networks 是 FE 和 BE 都有的一个配置，配置项需写在 fe.conf 和 be.conf 中。该配置项用于在 FE 或 BE 启动时，告诉进程应该绑定哪个IP。示例如下：

`priority_networks=10.1.3.0/24`

这是一种 [CIDR](https://en.wikipedia.org/wiki/Classless_Inter-Domain_Routing) 的表示方法。FE 或 BE 会根据这个配置项来寻找匹配的IP，作为自己的 localIP。

**注意**：当配置完 priority\_networks 并启动 FE 或 BE 后，只是保证了 FE 或 BE 自身的 IP 进行了正确的绑定。而在使用 ADD BACKEND 或 ADD FRONTEND 语句中，也需要指定和 priority\_networks 配置匹配的 IP，否则集群无法建立。举例：

BE 的配置为：`priority_networks=10.1.3.0/24`

但是在 ADD BACKEND 时使用的是：`ALTER SYSTEM ADD BACKEND "192.168.0.1:9050";`

则 FE 和 BE 将无法正常通信。

这时，必须 DROP 掉这个添加错误的 BE，重新使用正确的 IP 执行 ADD BACKEND。

FE 同理。

BROKER 当前没有，也不需要 priority\_networks 这个选项。Broker 的服务默认绑定在 0.0.0.0 上。只需在 ADD BROKER 时，执行正确可访问的 BROKER IP 即可。

#### 表名大小写敏感性设置

doris默认为表名大小写敏感，如有表名大小写不敏感的需求需在集群初始化时进行设置。表名大小写敏感性在集群初始化完成后不可再修改。

详细参见 [变量](../advanced/variables.md##支持的变量) 中关于`lower_case_table_names`变量的介绍。

## 集群部署

### 手动部署

#### FE 部署

* 拷贝 FE 部署文件到指定节点

  将源码编译生成的 output 下的 fe 文件夹拷贝到 FE 的节点指定部署路径下并进入该目录。

* 配置 FE

    1. 配置文件为 conf/fe.conf。其中注意：`meta_dir`是元数据存放位置。默认值为 `${DORIS_HOME}/doris-meta`。需**手动创建**该目录。

       **注意：生产环境强烈建议单独指定目录不要放在Doris安装目录下，最好是单独的磁盘（如果有SSD最好），测试开发环境可以使用默认配置**

    2. fe.conf 中 JAVA_OPTS 默认 java 最大堆内存为 8GB。

* 启动FE

  `bin/start_fe.sh --daemon`

  FE进程启动进入后台执行。日志默认存放在 log/ 目录下。如启动失败，可以通过查看 log/fe.log 或者 log/fe.out 查看错误信息。

* 如需部署多 FE，请参见 "FE 扩容和缩容" 章节

#### BE 部署

* 拷贝 BE 部署文件到所有要部署 BE 的节点

  将源码编译生成的 output 下的 be 文件夹拷贝到 BE 的节点的指定部署路径下。

  > 注意：`output/be/lib/debug_info/` 目录下为调试信息文件，文件较大，但实际运行不需要这些文件，可以不部署。

* 修改所有 BE 的配置

  修改 be/conf/be.conf。主要是配置 `storage_root_path`：数据存放目录。默认在be/storage下，若需要指定目录的话，需要**预创建目录**。多个路径之间使用英文状态的分号 `;` 分隔。  
  可以通过路径区别节点内的冷热数据存储目录，HDD（冷数据目录）或 SSD（热数据目录）。如果不需要 BE 节点内的冷热机制，那么只需要配置路径即可，无需指定 medium 类型；也不需要修改FE的默认存储介质配置  

  **注意：**
  1. 如果未指定存储路径的存储类型，则默认全部为 HDD（冷数据目录）。
  2. 这里的 HDD 和 SSD 与物理存储介质无关，只为了区分存储路径的存储类型，即可以在 HDD 介质的盘上标记某个目录为 SSD（热数据目录）。

  示例1如下：

  `storage_root_path=/home/disk1/doris;/home/disk2/doris;/home/disk2/doris`

  示例2如下：

  **使用 storage_root_path 参数里指定 medium**

  `storage_root_path=/home/disk1/doris,medium:HDD;/home/disk2/doris,medium:SSD`

  **说明**

    - /home/disk1/doris,medium:HDD： 表示该目录存储冷数据;
    - /home/disk2/doris,medium:SSD： 表示该目录存储热数据;

* BE webserver_port端口配置

  如果 be 部署在 hadoop 集群中，注意调整 be.conf 中的 `webserver_port = 8040` ,以免造成端口冲突

* 配置 JAVA_HOME 环境变量

  <version since="1.2.0"></version>  
  由于从 1.2 版本开始支持 Java UDF 函数，BE 依赖于 Java 环境。所以要预先配置 `JAVA_HOME` 环境变量，也可以在 `start_be.sh` 启动脚本第一行添加 `export JAVA_HOME=your_java_home_path` 来添加环境变量。

* 在 FE 中添加所有 BE 节点

  BE 节点需要先在 FE 中添加，才可加入集群。可以使用 mysql-client([下载MySQL 5.7](https://dev.mysql.com/downloads/mysql/5.7.html)) 连接到 FE：

  `./mysql-client -h fe_host -P query_port -uroot`

  其中 fe_host 为 FE 所在节点 ip；query_port 在 fe/conf/fe.conf 中的；默认使用 root 账户，无密码登录。

  登录后，执行以下命令来添加每一个 BE：

  `ALTER SYSTEM ADD BACKEND "be_host:heartbeat-service_port";`

  其中 be_host 为 BE 所在节点 ip；heartbeat_service_port 在 be/conf/be.conf 中。

* 启动 BE

  `bin/start_be.sh --daemon`

  BE 进程将启动并进入后台执行。日志默认存放在 be/log/ 目录下。如启动失败，可以通过查看 be/log/be.log 或者 be/log/be.out 查看错误信息。

* 查看BE状态

  使用 mysql-client 连接到 FE，并执行 `SHOW PROC '/backends';` 查看 BE 运行情况。如一切正常，`isAlive` 列应为 `true`。

#### （可选）FS_Broker 部署

Broker 以插件的形式，独立于 Doris 部署。如果需要从第三方存储系统导入数据，需要部署相应的 Broker，默认提供了读取 HDFS 、对象存储的 fs_broker。fs_broker 是无状态的，建议每一个 FE 和 BE 节点都部署一个 Broker。

* 拷贝源码 fs_broker 的 output 目录下的相应 Broker 目录到需要部署的所有节点上。建议和 BE 或者 FE 目录保持同级。

* 修改相应 Broker 配置

  在相应 broker/conf/ 目录下对应的配置文件中，可以修改相应配置。

* 启动 Broker

  `bin/start_broker.sh --daemon`

* 添加 Broker

  要让 Doris 的 FE 和 BE 知道 Broker 在哪些节点上，通过 sql 命令添加 Broker 节点列表。

  使用 mysql-client 连接启动的 FE，执行以下命令：

  `ALTER SYSTEM ADD BROKER broker_name "broker_host1:broker_ipc_port1","broker_host2:broker_ipc_port2",...;`

  其中 broker_host 为 Broker 所在节点 ip；broker_ipc_port 在 Broker 配置文件中的conf/apache_hdfs_broker.conf。

* 查看 Broker 状态

  使用 mysql-client 连接任一已启动的 FE，执行以下命令查看 Broker 状态：`SHOW PROC "/brokers";`

#### FE 和 BE 的启动方式

##### 版本 >=2.0.2

1. 使用 start_xx.sh 启动，该方式会将日志输出至文件，同时不会退出启动脚本进程，通常使用 supervisor 等工具自动拉起时建议采用这种方式。
2. 使用 start_xx.sh --daemon 启动，FE/BE 将作为一个后台进程在后台运行，并且日志输出将默认写入到指定的日志文件中。 这种启动方式适用于生产环境。
3. 使用 start_xx.sh --console 启动，该参数用于以控制台模式启动 FE/BE 。当使用 --console 参数启动时，服务器将在当前终端会话中启动，并将日志输出和控制台交互打印到该终端。
   这种启动方式适用于开发和测试场景.
##### 版本 < 2.0.2

1. 使用 start_xx.sh --daemon 启动，FE/BE 将作为一个后台进程在后台运行，并且日志输出将默认写入到指定的日志文件中。 这种启动方式适用于生产环境。
2. 使用 start_xx.sh 启动，该参数用于以控制台模式启动 FE/BE 。当使用 --console 参数启动时，服务器将在当前终端会话中启动，并将日志输出和控制台交互打印到该终端。
   这种启动方式适用于开发和测试场景.

**注：在生产环境中，所有实例都应使用守护进程启动，以保证进程退出后，会被自动拉起，如 [Supervisor](http://supervisord.org/)。如需使用守护进程启动，在 0.9.0 及之前版本中，需要修改各个 start_xx.sh 脚本，去掉最后的 & 符号**。从 0.10.0 版本开始，直接调用 `sh start_xx.sh` 启动即可。也可参考 [这里](https://www.cnblogs.com/lenmom/p/9973401.html)


## 常见问题

### 进程相关

1. 如何确定 FE 进程启动成功

   FE 进程启动后，会首先加载元数据，根据 FE 角色的不同，在日志中会看到 ```transfer from UNKNOWN to MASTER/FOLLOWER/OBSERVER```。最终会看到 ```thrift server started``` 日志，并且可以通过 mysql 客户端连接到 FE，则表示 FE 启动成功。

   也可以通过如下连接查看是否启动成功：  
   `http://fe_host:fe_http_port/api/bootstrap`

   如果返回：  
   `{"status":"OK","msg":"Success"}`

   则表示启动成功，其余情况，则可能存在问题。

   > 注：如果在 fe.log 中查看不到启动失败的信息，也许在 fe.out 中可以看到。

2. 如何确定 BE 进程启动成功

   BE 进程启动后，如果之前有数据，则可能有数分钟不等的数据索引加载时间。

   如果是 BE 的第一次启动，或者该 BE 尚未加入任何集群，则 BE 日志会定期滚动 ```waiting to receive first heartbeat from frontend``` 字样。表示 BE 还未通过 FE 的心跳收到 Master 的地址，正在被动等待。这种错误日志，在 FE 中 ADD BACKEND 并发送心跳后，就会消失。如果在接到心跳后，又重复出现 ``````master client, get client from cache failed.host: , port: 0, code: 7`````` 字样，说明 FE 成功连接了 BE，但 BE 无法主动连接 FE。可能需要检查 BE 到 FE 的 rpc_port 的连通性。

   如果 BE 已经被加入集群，日志中应该每隔 5 秒滚动来自 FE 的心跳日志：```get heartbeat, host: xx.xx.xx.xx, port: 9020, cluster id: xxxxxx```，表示心跳正常。

   其次，日志中应该每隔 10 秒滚动 ```finish report task success. return code: 0``` 的字样，表示 BE 向 FE 的通信正常。

   同时，如果有数据查询，应该能看到不停滚动的日志，并且有 ```execute time is xxx``` 日志，表示 BE 启动成功，并且查询正常。

   也可以通过如下连接查看是否启动成功：  
   `http://be_host:webserver_port/api/health`

   如果返回：  
   `{"status": "OK","msg": "To Be Added"}`

   则表示启动成功，其余情况，则可能存在问题。

   > 注：如果在 be.INFO 中查看不到启动失败的信息，也许在 be.out 中可以看到。

3. 搭建系统后，如何确定 FE、BE 连通性正常

   首先确认 FE 和 BE 进程都已经单独正常启动，并确认已经通过 `ADD BACKEND` 或者 `ADD FOLLOWER/OBSERVER` 语句添加了所有节点。

   如果心跳正常，BE 的日志中会显示 ```get heartbeat, host: xx.xx.xx.xx, port: 9020, cluster id: xxxxxx```。如果心跳失败，在 FE 的日志中会出现 ```backend[10001] got Exception: org.apache.thrift.transport.TTransportException``` 类似的字样，或者其他 thrift 通信异常日志，表示 FE 向 10001 这个 BE 的心跳失败。这里需要检查 FE 向 BE host 的心跳端口的连通性。

   如果 BE 向 FE 的通信正常，则 BE 日志中会显示 ```finish report task success. return code: 0``` 的字样。否则会出现 ```master client, get client from cache failed``` 的字样。这种情况下，需要检查 BE 向 FE 的 rpc_port 的连通性。

4. Doris 各节点认证机制

   除了 Master FE 以外，其余角色节点（Follower FE，Observer FE，Backend），都需要通过 `ALTER SYSTEM ADD` 语句先注册到集群，然后才能加入集群。

   Master FE 在第一次启动时，会在 doris-meta/image/VERSION 文件中生成一个 cluster_id。

   FE 在第一次加入集群时，会首先从 Master FE 获取这个文件。之后每次 FE 之间的重新连接（FE 重启），都会校验自身 cluster id 是否与已存在的其它 FE 的 cluster id 相同。如果不同，则该 FE 会自动退出。

   BE 在第一次接收到 Master FE 的心跳时，会从心跳中获取到 cluster id，并记录到数据目录的 `cluster_id` 文件中。之后的每次心跳都会比对 FE 发来的 cluster id。如果 cluster id 不相等，则 BE 会拒绝响应 FE 的心跳。

   心跳中同时会包含 Master FE 的 ip。当 FE 切主时，新的 Master FE 会携带自身的 ip 发送心跳给 BE，BE 会更新自身保存的 Master FE 的 ip。

   > **priority\_network**
   >
   > priority\_network 是 FE 和 BE 都有一个配置，其主要目的是在多网卡的情况下，协助 FE 或 BE 识别自身 ip 地址。priority\_network 采用 CIDR 表示法：[RFC 4632](https://tools.ietf.org/html/rfc4632)
   >
   > 当确认 FE 和 BE 连通性正常后，如果仍然出现建表 Timeout 的情况，并且 FE 的日志中有 `backend does not found. host: xxx.xxx.xxx.xxx` 字样的错误信息。则表示 Doris 自动识别的 IP 地址有问题，需要手动设置 priority\_network 参数。
   >
   > 出现这个问题的主要原因是：当用户通过 `ADD BACKEND` 语句添加 BE 后，FE 会识别该语句中指定的是 hostname 还是 IP。如果是 hostname，则 FE 会自动将其转换为 IP 地址并存储到元数据中。当 BE 在汇报任务完成信息时，会携带自己的 IP 地址。而如果 FE 发现 BE 汇报的 IP 地址和元数据中不一致时，就会出现如上错误。
   >
   > 这个错误的解决方法：1）分别在 FE 和 BE 设置 **priority\_network** 参数。通常 FE 和 BE 都处于一个网段，所以该参数设置为相同即可。2）在 `ADD BACKEND` 语句中直接填写 BE 正确的 IP 地址而不是 hostname，以避免 FE 获取到错误的 IP 地址。

5. BE 进程文件句柄数

   BE进程文件句柄数，受min_file_descriptor_number/max_file_descriptor_number两个参数控制。

   如果不在[min_file_descriptor_number, max_file_descriptor_number]区间内，BE进程启动会出错，可以使用ulimit进行设置。

   min_file_descriptor_number的默认值为65536。

   max_file_descriptor_number的默认值为131072.

   举例而言：ulimit -n 65536; 表示将文件句柄设成65536。

   启动BE进程之后，可以通过 cat /proc/$pid/limits 查看进程实际生效的句柄数

   如果使用了supervisord，遇到句柄数错误，可以通过修改supervisord的minfds参数解决。

   ```shell
   vim /etc/supervisord.conf
   
   minfds=65535                 ; (min. avail startup file descriptors;default 1024)
   ```
