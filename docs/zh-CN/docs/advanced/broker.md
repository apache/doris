---
{
    "title": "Broker",
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

# Broker

Broker 是 Doris 集群中一种可选进程，主要用于支持 Doris 读写远端存储上的文件和目录。目前支持以下远端存储：

- Apache HDFS
- 阿里云 OSS
- 百度云 BOS
- 腾讯云 CHDFS
- 腾讯云 GFS (1.2.0 版本支持)
- 华为云 OBS (1.2.0 版本后支持)
- 亚马逊 S3
- JuiceFS (2.0.0 版本支持)
- GCS (2.0.0 版本支持)

Broker 通过提供一个 RPC 服务端口来提供服务，是一个无状态的 Java 进程，负责为远端存储的读写操作封装一些类 POSIX 的文件操作，如 open，pread，pwrite 等等。除此之外，Broker 不记录任何其他信息，所以包括远端存储的连接信息、文件信息、权限信息等等，都需要通过参数在 RPC 调用中传递给 Broker 进程，才能使得 Broker 能够正确读写文件。

Broker 仅作为一个数据通路，并不参与任何计算，因此仅需占用较少的内存。通常一个 Doris 系统中会部署一个或多个 Broker 进程。并且相同类型的 Broker 会组成一个组，并设定一个 **名称（Broker name）**。

Broker 在 Doris 系统架构中的位置如下：

```text
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
   - 支持简单认证访问
   - 支持通过 kerberos 认证访问
   - 支持 HDFS HA 模式访问
2. 对象存储
   - 所有支持S3协议的对象存储

1. [Broker Load](../data-operate/import/import-way/broker-load-manual)
2. [数据导出（Export）](../data-operate/export/export-manual)
3. [数据备份](../admin-manual/data-admin/backup)

## Broker 信息

Broker 的信息包括 **名称（Broker name）** 和 **认证信息** 两部分。通常的语法格式如下：

```text
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

#### Apache HDFS

1. 简单认证

   简单认证即 Hadoop 配置 `hadoop.security.authentication` 为 `simple`。

   使用系统用户访问 HDFS。或者在 Broker 启动的环境变量中添加：`HADOOP_USER_NAME`。

   ```text
   (
       "username" = "user",
       "password" = ""
   );
   ```

   密码置空即可。

2. Kerberos 认证

   该认证方式需提供以下信息：

   - `hadoop.security.authentication`：指定认证方式为 kerberos。
   - `hadoop.kerberos.principal`：指定 kerberos 的 principal。
   - `hadoop.kerberos.keytab`：指定 kerberos 的 keytab 文件路径。该文件必须为 Broker 进程所在服务器上的文件的绝对路径。并且可以被 Broker 进程访问。
   - `kerberos_keytab_content`：指定 kerberos 中 keytab 文件内容经过 base64 编码之后的内容。这个跟 `kerberos_keytab` 配置二选一即可。

   示例如下：

   ```text
   (
       "hadoop.security.authentication" = "kerberos",
       "hadoop.kerberos.principal" = "doris@YOUR.COM",
       "hadoop.kerberos.keytab" = "/home/doris/my.keytab"
   )
   ```

   ```text
   (
       "hadoop.security.authentication" = "kerberos",
       "hadoop.kerberos.principal" = "doris@YOUR.COM",
       "kerberos_keytab_content" = "ASDOWHDLAWIDJHWLDKSALDJSDIWALD"
   )
   ```

   如果采用Kerberos认证方式，则部署Broker进程的时候需要[krb5.conf (opens new window)](https://web.mit.edu/kerberos/krb5-1.12/doc/admin/conf_files/krb5_conf.html)文件， krb5.conf文件包含Kerberos的配置信息，通常，您应该将krb5.conf文件安装在目录/etc中。您可以通过设置环境变量KRB5_CONFIG覆盖默认位置。 krb5.conf文件的内容示例如下：

   ```text
   [libdefaults]
       default_realm = DORIS.HADOOP
       default_tkt_enctypes = des3-hmac-sha1 des-cbc-crc
       default_tgs_enctypes = des3-hmac-sha1 des-cbc-crc
       dns_lookup_kdc = true
       dns_lookup_realm = false
   
   [realms]
       DORIS.HADOOP = {
           kdc = kerberos-doris.hadoop.service:7005
       }
   ```

3. HDFS HA 模式

   这个配置用于访问以 HA 模式部署的 HDFS 集群。

   - `dfs.nameservices`：指定 hdfs 服务的名字，自定义，如："dfs.nameservices" = "my_ha"。
   - `dfs.ha.namenodes.xxx`：自定义 namenode 的名字,多个名字以逗号分隔。其中 xxx 为 `dfs.nameservices` 中自定义的名字，如： "dfs.ha.namenodes.my_ha" = "my_nn"。
   - `dfs.namenode.rpc-address.xxx.nn`：指定 namenode 的rpc地址信息。其中 nn 表示 `dfs.ha.namenodes.xxx` 中配置的 namenode 的名字，如："dfs.namenode.rpc-address.my_ha.my_nn" = "host:port"。
   - `dfs.client.failover.proxy.provider`：指定 client 连接 namenode 的 provider，默认为：org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider。

   示例如下：

   ```text
   (
       "fs.defaultFS" = "hdfs://my_ha",
       "dfs.nameservices" = "my_ha",
       "dfs.ha.namenodes.my_ha" = "my_namenode1, my_namenode2",
       "dfs.namenode.rpc-address.my_ha.my_namenode1" = "nn1_host:rpc_port",
       "dfs.namenode.rpc-address.my_ha.my_namenode2" = "nn2_host:rpc_port",
       "dfs.client.failover.proxy.provider" = "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
   )
   ```

   HA 模式可以和前面两种认证方式组合，进行集群访问。如通过简单认证访问 HA HDFS：

   ```text
   (
       "username"="user",
       "password"="passwd",
       "fs.defaultFS" = "hdfs://my_ha",
       "dfs.nameservices" = "my_ha",
       "dfs.ha.namenodes.my_ha" = "my_namenode1, my_namenode2",
       "dfs.namenode.rpc-address.my_ha.my_namenode1" = "nn1_host:rpc_port",
       "dfs.namenode.rpc-address.my_ha.my_namenode2" = "nn2_host:rpc_port",
       "dfs.client.failover.proxy.provider" = "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
   )
   ```

   关于HDFS集群的配置可以写入hdfs-site.xml文件中，用户使用Broker进程读取HDFS集群的信息时，只需要填写集群的文件路径名和认证信息即可。

#### 腾讯云 CHDFS

同 Apache HDFS

#### 阿里云 OSS

```
(
    "fs.oss.accessKeyId" = "",
    "fs.oss.accessKeySecret" = "",
    "fs.oss.endpoint" = ""
)
```

#### 百度云 BOS
当前使用BOS时需要将[bos-hdfs-sdk-1.0.3-community.jar.zip](https://sdk.bce.baidu.com/console-sdk/bos-hdfs-sdk-1.0.3-community.jar.zip)下载并解压后把jar包放到broker的lib目录下。

```
(
    "fs.bos.access.key" = "xx",
    "fs.bos.secret.access.key" = "xx",
    "fs.bos.endpoint" = "xx"
)
```

#### 华为云 OBS

```
(
    "fs.obs.access.key" = "xx",
    "fs.obs.secret.key" = "xx",
    "fs.obs.endpoint" = "xx"
)
```

#### 亚马逊 S3

```
(
    "fs.s3a.access.key" = "xx",
    "fs.s3a.secret.key" = "xx",
    "fs.s3a.endpoint" = "xx"
)
```

#### JuiceFS

```
(
    "fs.defaultFS" = "jfs://xxx/",
    "fs.jfs.impl" = "io.juicefs.JuiceFileSystem",
    "fs.AbstractFileSystem.jfs.impl" = "io.juicefs.JuiceFS",
    "juicefs.meta" = "xxx",
    "juicefs.access-log" = "xxx"
)
```

#### GCS
 在使用 Broker 访问 GCS 时，Project ID 是必须的，其他参数可选,所有参数配置请参考 [GCS Config](https://github.com/GoogleCloudDataproc/hadoop-connectors/blob/branch-2.2.x/gcs/CONFIGURATION.md)
```
(
    "fs.gs.project.id" = "你的 Project ID",
    "fs.AbstractFileSystem.gs.impl" = "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
    "fs.gs.impl" = "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
)
```
