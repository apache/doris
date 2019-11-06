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

# Broker

Broker 是 Doris 集群中一种可选进程，主要用于支持 Doris 读写远端存储上的文件和目录，如 HDFS、BOS 和 AFS 等。

Broker 通过提供一个 RPC 服务端口来提供服务，是一个无状态的 Java 进程，负责为远端存储的读写操作封装一些类 POSIX 的文件操作，如 open，pread，pwrite 等等。除此之外，Broker 不记录任何其他信息，所以包括远端存储的连接信息、文件信息、权限信息等等，都需要通过参数在 RPC 调用中传递给 Broker 进程，才能使得 Broker 能够正确读写文件。

Broker 仅作为一个数据通路，并不参与任何计算，因此仅需占用较少的内存。通常一个 Doris 系统中会部署一个或多个 Broker 进程。并且相同类型的 Broker 会组成一个组，并设定一个 **名称（Broker name）**。

Broker 在 Doris 系统架构中的位置如下：

```
+----+   +----+
| FE |   | BE |
+-^--+   +--^-+
  |         |
  |         |
+-v---------v-+
|   Broker    |
+------^------+
       |
       |
+------v------+
|HDFS/BOS/AFS |
+-------------+
```

本文档主要介绍 Broker 在访问不同远端存储时需要的参数，如连接信息、权限认证信息等等。

## 支持的存储系统

不同的 Broker 类型支持不同的存储系统。

1. 社区版 HDFS

    * 支持简单认证访问
    * 支持通过 kerberos 认证访问
    * 支持 HDFS HA 模式访问

2. 百度 HDFS/AFS（开源版本不支持）

    * 支持通过 ugi 简单认证访问

3. 百度对象存储 BOS（开源版本不支持）

    * 支持通过 AK/SK 认证访问

## 需要 Broker 的操作

1. Broker Load

    Broker Load 功能通过 Broker 进程读取远端存储上的文件数据并导入到 Doris 中。示例如下：
    
    ```
    LOAD LABEL example_db.label6
    (
        DATA INFILE("bos://my_bucket/input/file")
        INTO TABLE `my_table`
    )
    WITH BROKER "broker_name"
    (
        "bos_endpoint" = "http://bj.bcebos.com",
        "bos_accesskey" = "xxxxxxxxxxxxxxxxxxxxxxxxxx",
        "bos_secret_accesskey" = "yyyyyyyyyyyyyyyyyyyy"
    )
    ```

    其中 `WITH BROKER` 以及之后的 Property Map 用于提供 Broker 相关信息。
    
2. 数据导出（Export）

    Export 功能通过 Broker 进程，将 Doris 中存储的数据以文本的格式导出到远端存储的文件中。示例如下：
    
    ```
    EXPORT TABLE testTbl 
    TO "hdfs://hdfs_host:port/a/b/c" 
    WITH BROKER "broker_name" 
    (
        "username" = "xxx",
        "password" = "yyy"
    );
    ```

    其中 `WITH BROKER` 以及之后的 Property Map 用于提供 Broker 相关信息。

3. 创建用于备份恢复的仓库（Create Repository）

    当用户需要使用备份恢复功能时，需要先通过 `CREATE REPOSITORY` 命令创建一个 “仓库”，仓库元信息中记录了所使用的 Broker 以及相关信息。之后的备份恢复操作，会通过 Broker 将数据备份到这个仓库，或从这个仓库读取数据恢复到 Doris 中。示例如下：
    
    ```
    CREATE REPOSITORY `bos_repo`
    WITH BROKER `broker_name`
    ON LOCATION "bos://doris_backup"
    PROPERTIES
    (
        "bos_endpoint" = "http://gz.bcebos.com",
        "bos_accesskey" = "069fc2786e664e63a5f111111114ddbs22",
        "bos_secret_accesskey" = "70999999999999de274d59eaa980a"
    );
    ```
    
    其中 `WITH BROKER` 以及之后的 Property Map 用于提供 Broker 相关信息。
    

## Broker 信息

Broker 的信息包括 **名称（Broker name）** 和 **认证信息** 两部分。通常的语法格式如下：

```
WITH BROKER "broker_name" 
(
    "username" = "xxx",
    "password" = "yyy",
    "other_prop" = "prop_value",
    ...
);
```

### 名称

通常用户需要通过操作命令中的 `WITH BROKER "broker_name"` 子句来指定一个已经存在的 Broker Name。Broker Name 是用户在通过 `ALTER SYSTEM ADD BROKER` 命令添加 Broker 进程时指定的一个名称。一个名称通常对应一个或多个 Broker 进程。Doris 会根据名称选择可用的 Broker 进程。用户可以通过 `SHOW BROKER` 命令查看当前集群中已经存在的 Broker。

**注：Broker Name 只是一个用户自定义名称，不代表 Broker 的类型。**

### 认证信息

不同的 Broker 类型，以及不同的访问方式需要提供不同的认证信息。认证信息通常在 `WITH BROKER "broker_name"` 之后的 Property Map 中以 Key-Value 的方式提供。

#### 社区版 HDFS

1. 简单认证

    简单认证即 Hadoop 配置 `hadoop.security.authentication` 为 `simple`。

    使用系统用户访问 HDFS。或者在 Broker 启动的环境变量中添加：```HADOOP_USER_NAME```。
    
    ```
    (
        "username" = "user",
        "password" = ""
    );
    ```

    密码置空即可。

2. Kerberos 认证

    该认证方式需提供以下信息：
    
    * `hadoop.security.authentication`：指定认证方式为 kerberos。
    * `kerberos_principal`：指定 kerberos 的 principal。
    * `kerberos_keytab`：指定 kerberos 的 keytab 文件路径。该文件必须为 Broker 进程所在服务器上的文件的绝对路径。并且可以被 Broker 进程访问。
    * `kerberos_keytab_content`：指定 kerberos 中 keytab 文件内容经过 base64 编码之后的内容。这个跟 `kerberos_keytab` 配置二选一即可。

    示例如下：
    
    ```
    (
        "hadoop.security.authentication" = "kerberos",
        "kerberos_principal" = "doris@YOUR.COM",
        "kerberos_keytab" = "/home/doris/my.keytab"
    )
    ```
    ```
    (
        "hadoop.security.authentication" = "kerberos",
        "kerberos_principal" = "doris@YOUR.COM",
        "kerberos_keytab_content" = "ASDOWHDLAWIDJHWLDKSALDJSDIWALD"
    )
    ```
    
3. HDFS HA 模式

    这个配置用于访问以 HA 模式部署的 HDFS 集群。
    
    * `dfs.nameservices`：指定 hdfs 服务的名字，自定义，如："dfs.nameservices" = "my_ha"。
    * `dfs.ha.namenodes.xxx`：自定义 namenode 的名字,多个名字以逗号分隔。其中 xxx 为 `dfs.nameservices` 中自定义的名字，如 "dfs.ha.namenodes.my_ha" = "my_nn"。
    * `dfs.namenode.rpc-address.xxx.nn`：指定 namenode 的rpc地址信息。其中 nn 表示 `dfs.ha.namenodes.xxx` 中配置的 namenode 的名字，如："dfs.namenode.rpc-address.my_ha.my_nn" = "host:port"。
    * `dfs.client.failover.proxy.provider`：指定 client 连接 namenode 的 provider，默认为：org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider。

    示例如下：
    
    ```
    (
        "dfs.nameservices" = "my_ha",
        "dfs.ha.namenodes.my_ha" = "my_namenode1, my_namenode2",
        "dfs.namenode.rpc-address.my_ha.my_namenode1" = "nn1_host:rpc_port",
        "dfs.namenode.rpc-address.my_ha.my_namenode2" = "nn2_host:rpc_port",
        "dfs.client.failover.proxy.provider" = "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
    )
    ```
    
    HA 模式可以和前面两种认证方式组合，进行集群访问。如通过简单认证访问 HA HDFS：
    
    ```
    (
        "username"="user",
        "password"="passwd",
        "dfs.nameservices" = "my_ha",
        "dfs.ha.namenodes.my_ha" = "my_namenode1, my_namenode2",
        "dfs.namenode.rpc-address.my_ha.my_namenode1" = "nn1_host:rpc_port",
        "dfs.namenode.rpc-address.my_ha.my_namenode2" = "nn2_host:rpc_port",
        "dfs.client.failover.proxy.provider" = "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
    )
    ```
    
#### 百度对象存储 BOS

**（开源版本不支持）**

1. 通过 AK/SK 访问

    * AK/SK：Access Key 和 Secret Key。在百度云安全认证中心可以查看用户的 AK/SK。
    * Region Endpoint：BOS 所在地区的 Endpoint：

        * 华北-北京：http://bj.bcebos.com
        * 华北-保定：http://bd.bcebos.com
        * 华南-广州：http://gz.bcebos.com
        * 华东-苏州：http://sz.bcebos.com


    示例如下：

    ```
    (
        "bos_endpoint" = "http://bj.bcebos.com",
        "bos_accesskey" = "xxxxxxxxxxxxxxxxxxxxxxxxxx",
        "bos_secret_accesskey" = "yyyyyyyyyyyyyyyyyyyyyyyyyy"
    )
    ```

#### 百度 HDFS/AFS

**（开源版本不支持）**

百度 AFS 和 HDFS 仅支持使用 ugi 的简单认证访问。示例如下：

```
(
    "username" = "user",
    "password" = "passwd"
);
```

其中 user 和 passwd 为 Hadoop 的 UGI 配置。
