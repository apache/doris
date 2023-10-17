---
{
    "title": "快速开始",
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

# 快速开始

Apache Doris 是一个基于 MPP 架构的高性能、实时的分析型数据库，以极速易用的特点被人们所熟知，仅需亚秒级响应时间即可返回海量数据下的查询结果，不仅可以支持高并发的点查询场景，也能支持高吞吐的复杂分析场景，这个简短的指南将告诉你如何下载 Doris 最新稳定版本，在单节点上安装并运行它，包括创建数据库、数据表、导入数据及查询等。

## 下载 Doris

Doris 运行在 Linux 环境中，推荐 CentOS 7.x 或者 Ubuntu 16.04 以上版本，同时你需要安装 Java 运行环境（JDK版本要求为8），要检查你所安装的 Java 版本，请运行以下命令：

```
java -version
```

接下来，[下载 Doris 的最新二进制版本](https://doris.apache.org/zh-CN/download)，然后解压。

```
tar xf apache-doris-x.x.x.tar.xz
```

## 配置 Doris

### 配置 FE 

我们进入到 `apache-doris-x.x.x/fe` 目录

```
cd apache-doris-x.x.x/fe
```

修改 FE 配置文件 `conf/fe.conf` ，这里我们主要修改两个参数：`priority_networks` 及 `meta_dir` ，如果你需要更多优化配置，请参考 [FE 参数配置](../admin-manual/config/fe-config.md)说明，进行调整。

1. 添加 priority_networks 参数

```
priority_networks=172.23.16.0/24
```

>注意：
>
>这个参数我们在安装的时候是必须要配置的，特别是当一台机器拥有多个IP地址的时候，我们要为 FE 指定唯一的IP地址。

> 这里假设你的节点 IP 是 `172.23.16.32`，那么我们可以通过掩码的方式配置为 `172.23.16.0/24`。

2. 添加元数据目录

```
meta_dir=/path/your/doris-meta
```

>注意：
>
>这里你可以不配置，默认是在你的Doris FE 安装目录下的 doris-meta，
>
>单独配置元数据目录，需要你提前创建好你指定的目录

### 启动 FE 

在 FE 安装目录下执行下面的命令，来完成 FE 的启动。

```
./bin/start_fe.sh --daemon
```

#### 查看 FE 运行状态

你可以通过下面的命令来检查 Doris 是否启动成功

```
curl http://127.0.0.1:8030/api/bootstrap
```

这里 IP 和 端口分别是 FE 的 IP 和 http_port（默认8030），如果是你在 FE 节点执行，直接运行上面的命令即可。

如果返回结果中带有 `"msg":"success"` 字样，则说明启动成功。

你也可以通过 Doris FE 提供的Web UI 来检查，在浏览器里输入地址

http:// fe_ip:8030

可以看到下面的界面，说明 FE 启动成功

![image-20220822091951739](/images/image-20220822091951739.png)



>注意：
>
>1. 这里我们使用 Doris 内置的默认用户 root 进行登录，密码是空
>2. 这是一个 Doris 的管理界面，只能拥有管理权限的用户才能登录，普通用户不能登录。

#### 连接 FE

我们下面通过 MySQL 客户端来连接 Doris FE，下载免安装的 [MySQL 客户端](https://doris-build-hk.oss-cn-hongkong.aliyuncs.com/mysql-client/mysql-5.7.22-linux-glibc2.12-x86_64.tar.gz)

解压刚才下载的 MySQL 客户端，在 `bin/` 目录下可以找到 `mysql` 命令行工具。然后执行下面的命令连接 Doris。

```
mysql -uroot -P9030 -h127.0.0.1
```

>注意：
>
>1. 这里使用的 root 用户是 doris 内置的默认用户，也是超级管理员用户，具体的用户权限查看 [权限管理](../admin-manual/privilege-ldap/user-privilege.md)
>2. -P ：这里是我们连接 Doris 的查询端口，默认端口是 9030，对应的是fe.conf里的 `query_port`
>3. -h ： 这里是我们连接的 FE IP地址，如果你的客户端和 FE 安装在同一个节点可以使用127.0.0.1。

执行下面的命令查看 FE 运行状态

```sql
show frontends\G;
```

然后你可以看到类似下面的结果：

```sql
mysql> show frontends\G
*************************** 1. row ***************************
             Name: 172.21.32.5_9010_1660549353220
               IP: 172.21.32.5
      EditLogPort: 9010
         HttpPort: 8030
        QueryPort: 9030
          RpcPort: 9020
ArrowFlightSqlPort: 9040
             Role: FOLLOWER
         IsMaster: true
        ClusterId: 1685821635
             Join: true
            Alive: true
ReplayedJournalId: 49292
    LastHeartbeat: 2022-08-17 13:00:45
         IsHelper: true
           ErrMsg:
          Version: 1.1.2-rc03-ca55ac2
 CurrentConnected: Yes
1 row in set (0.03 sec)
```

1. 如果 IsMaster、Join 和 Alive 三列均为true，则表示节点正常。

#### 加密连接 FE

Doris支持基于SSL的加密连接，当前支持TLS1.2，TLS1.3协议，可以通过以下配置开启Doris的SSL模式：
修改FE配置文件`conf/fe.conf`，添加`enable_ssl = true`即可。

接下来通过`mysql`客户端连接Doris，mysql支持五种SSL模式：

1.`mysql -uroot -P9030 -h127.0.0.1`与`mysql --ssl-mode=PREFERRED -uroot -P9030 -h127.0.0.1`一样，都是一开始试图建立SSL加密连接，如果失败，则尝试使用普通连接。

2.`mysql --ssl-mode=DISABLE -uroot -P9030 -h127.0.0.1`，不使用SSL加密连接，直接使用普通连接。

3.`mysql --ssl-mode=REQUIRED -uroot -P9030 -h127.0.0.1`，强制使用SSL加密连接。

4.`mysql --ssl-mode=VERIFY_CA --ssl-ca=ca.pem -uroot -P9030 -h127.0.0.1`，强制使用SSL加密连接，并且通过指定CA证书验证服务端身份是否有效。

5.`mysql --ssl-mode=VERIFY_CA --ssl-ca=ca.pem --ssl-cert=client-cert.pem --ssl-key=client-key.pem -uroot -P9030 -h127.0.0.1`，强制使用SSL加密连接，双向验证。


>注意：
>`--ssl-mode`参数是mysql5.7.11版本引入的，低于此版本的mysql客户端请参考[这里](https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-security.html)。

Doris开启SSL加密连接需要密钥证书文件验证，默认的密钥证书文件位于`Doris/fe/mysql_ssl_default_certificate/`下。密钥证书文件的生成请参考[密钥证书配置](../admin-manual/certificate.md)。

#### 停止 FE 节点

Doris FE 的停止可以通过下面的命令完成

```
./bin/stop_fe.sh
```

### 配置 BE

我们进入到 `apache-doris-x.x.x/be` 目录

```
cd apache-doris-x.x.x/be
```

修改 BE 配置文件 `conf/be.conf` ，这里我们主要修改两个参数：`priority_networks` 及 `storage_root` ，如果你需要更多优化配置，请参考 [BE 参数配置](../admin-manual/config/be-config.md)说明，进行调整。

1. 添加 priority_networks 参数

```
priority_networks=172.23.16.0/24
```

>注意：
>
>这个参数我们在安装的时候是必须要配置的，特别是当一台机器拥有多个IP地址的时候，我们要为 BE 指定唯一的IP地址。

2. 配置 BE 数据存储目录


```
storage_root_path=/path/your/data_dir
```

>注意：
>
>1. 默认目录在 BE安装目录的 storage 目录下。
>2. BE 配置的存储目录必须先创建好

3. 配置 JAVA_HOME 环境变量

<version since="1.2.0"></version>  
由于从 1.2 版本开始支持 Java UDF 函数，BE 依赖于 Java 环境。所以要预先配置 `JAVA_HOME` 环境变量，也可以在 `start_be.sh` 启动脚本第一行添加 `export JAVA_HOME=your_java_home_path` 来添加环境变量。

4. 安装 Java UDF 函数

<version since="1.2.0">安装Java UDF 函数</version>  
因为从1.2 版本开始支持Java UDF 函数，需要从官网下载 Java UDF 函数的 JAR 包放到 BE 的 lib 目录下，否则可能会启动失败。



### 启动 BE

在 BE 安装目录下执行下面的命令，来完成 BE 的启动。

```
./bin/start_be.sh --daemon
```

#### 添加 BE 节点到集群

通过MySQL 客户端连接到 FE 之后执行下面的 SQL，将 BE 添加到集群中

```sql
ALTER SYSTEM ADD BACKEND "be_host_ip:heartbeat_service_port";
```

1. be_host_ip：这里是你 BE 的 IP 地址，和你在 `be.conf` 里的 `priority_networks` 匹配
2. heartbeat_service_port：这里是你 BE 的心跳上报端口，和你在 `be.conf` 里的 `heartbeat_service_port` 匹配，默认是 `9050`。

#### 查看 BE 运行状态

你可以在 MySQL 命令行下执行下面的命令查看 BE 的运行状态。

```sql
SHOW BACKENDS\G
```

示例：

```sql
mysql> SHOW BACKENDS\G
*************************** 1. row ***************************
            BackendId: 10003
              Cluster: default_cluster
                   IP: 172.21.32.5
        HeartbeatPort: 9050
               BePort: 9060
             HttpPort: 8040
             BrpcPort: 8060
   ArrowFlightSqlPort: 8070
        LastStartTime: 2022-08-16 15:31:37
        LastHeartbeat: 2022-08-17 13:33:17
                Alive: true
 SystemDecommissioned: false
ClusterDecommissioned: false
            TabletNum: 170
     DataUsedCapacity: 985.787 KB
        AvailCapacity: 782.729 GB
        TotalCapacity: 984.180 GB
              UsedPct: 20.47 %
       MaxDiskUsedPct: 20.47 %
                  Tag: {"location" : "default"}
               ErrMsg:
              Version: 1.1.2-rc03-ca55ac2
               Status: {"lastSuccessReportTabletsTime":"2022-08-17 13:33:05","lastStreamLoadTime":-1,"isQueryDisabled":false,"isLoadDisabled":false}
1 row in set (0.01 sec)
```

1. Alive : true表示节点运行正常

#### 停止 BE 节点

Doris BE 的停止可以通过下面的命令完成

```
./bin/stop_be.sh
```

## 创建数据表

1. 创建一个数据库

```sql
create database demo;
```

2. 创建数据表

```sql
use demo;

CREATE TABLE IF NOT EXISTS demo.example_tbl
(
    `user_id` LARGEINT NOT NULL COMMENT "用户id",
    `date` DATE NOT NULL COMMENT "数据灌入日期时间",
    `city` VARCHAR(20) COMMENT "用户所在城市",
    `age` SMALLINT COMMENT "用户年龄",
    `sex` TINYINT COMMENT "用户性别",
    `last_visit_date` DATETIME REPLACE DEFAULT "1970-01-01 00:00:00" COMMENT "用户最后一次访问时间",
    `cost` BIGINT SUM DEFAULT "0" COMMENT "用户总消费",
    `max_dwell_time` INT MAX DEFAULT "0" COMMENT "用户最大停留时间",
    `min_dwell_time` INT MIN DEFAULT "99999" COMMENT "用户最小停留时间"
)
AGGREGATE KEY(`user_id`, `date`, `city`, `age`, `sex`)
DISTRIBUTED BY HASH(`user_id`) BUCKETS 1
PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
);
```

3. 示例数据

```
10000,2017-10-01,北京,20,0,2017-10-01 06:00:00,20,10,10
10000,2017-10-01,北京,20,0,2017-10-01 07:00:00,15,2,2
10001,2017-10-01,北京,30,1,2017-10-01 17:05:45,2,22,22
10002,2017-10-02,上海,20,1,2017-10-02 12:59:12,200,5,5
10003,2017-10-02,广州,32,0,2017-10-02 11:20:00,30,11,11
10004,2017-10-01,深圳,35,0,2017-10-01 10:00:15,100,3,3
10004,2017-10-03,深圳,35,0,2017-10-03 10:20:22,11,6,6
```

将上面的数据保存在`test.csv`文件中。

4. 导入数据

这里我们通过Stream load 方式将上面保存到文件中的数据导入到我们刚才创建的表里。

```
curl  --location-trusted -u root: -T test.csv -H "column_separator:," http://127.0.0.1:8030/api/demo/example_tbl/_stream_load
```

- -T test.csv : 这里使我们刚才保存的数据文件，如果路径不一样，请指定完整路径
- -u root :  这里是用户名密码，我们使用默认用户root，密码是空
- 127.0.0.1:8030 : 分别是 fe 的 ip 和 http_port

执行成功之后我们可以看到下面的返回信息

```json
{
    "TxnId": 30303,
    "Label": "8690a5c7-a493-48fc-b274-1bb7cd656f25",
    "TwoPhaseCommit": "false",
    "Status": "Success",
    "Message": "OK",
    "NumberTotalRows": 7,
    "NumberLoadedRows": 7,
    "NumberFilteredRows": 0,
    "NumberUnselectedRows": 0,
    "LoadBytes": 399,
    "LoadTimeMs": 381,
    "BeginTxnTimeMs": 3,
    "StreamLoadPutTimeMs": 5,
    "ReadDataTimeMs": 0,
    "WriteDataTimeMs": 191,
    "CommitAndPublishTimeMs": 175
}
```

1. `NumberLoadedRows`: 表示已经导入的数据记录数

2. `NumberTotalRows`: 表示要导入的总数据量

3. `Status` :Success 表示导入成功

到这里我们已经完成的数据导入，下面就可以根据我们自己的需求对数据进行查询分析了。

## 查询数据

我们上面完成了建表，输数据导入，下面我们就可以体验 Doris 的数据快速查询分析能力。

```sql
mysql> select * from example_tbl;
+---------+------------+--------+------+------+---------------------+------+----------------+----------------+
| user_id | date       | city   | age  | sex  | last_visit_date     | cost | max_dwell_time | min_dwell_time |
+---------+------------+--------+------+------+---------------------+------+----------------+----------------+
| 10000   | 2017-10-01 | 北京   |   20 |    0 | 2017-10-01 07:00:00 |   35 |             10 |              2 |
| 10001   | 2017-10-01 | 北京   |   30 |    1 | 2017-10-01 17:05:45 |    2 |             22 |             22 |
| 10002   | 2017-10-02 | 上海   |   20 |    1 | 2017-10-02 12:59:12 |  200 |              5 |              5 |
| 10003   | 2017-10-02 | 广州   |   32 |    0 | 2017-10-02 11:20:00 |   30 |             11 |             11 |
| 10004   | 2017-10-01 | 深圳   |   35 |    0 | 2017-10-01 10:00:15 |  100 |              3 |              3 |
| 10004   | 2017-10-03 | 深圳   |   35 |    0 | 2017-10-03 10:20:22 |   11 |              6 |              6 |
+---------+------------+--------+------+------+---------------------+------+----------------+----------------+
6 rows in set (0.02 sec)

mysql> select * from example_tbl where city='上海';
+---------+------------+--------+------+------+---------------------+------+----------------+----------------+
| user_id | date       | city   | age  | sex  | last_visit_date     | cost | max_dwell_time | min_dwell_time |
+---------+------------+--------+------+------+---------------------+------+----------------+----------------+
| 10002   | 2017-10-02 | 上海   |   20 |    1 | 2017-10-02 12:59:12 |  200 |              5 |              5 |
+---------+------------+--------+------+------+---------------------+------+----------------+----------------+
1 row in set (0.05 sec)

mysql> select city, sum(cost) as total_cost from example_tbl group by city;
+--------+------------+
| city   | total_cost |
+--------+------------+
| 广州   |         30 |
| 上海   |        200 |
| 北京   |         37 |
| 深圳   |        111 |
+--------+------------+
4 rows in set (0.05 sec)
```



到这里我们整个快速开始就结束了，我们从 Doris 安装部署、启停、创建库表、数据导入及查询，完整的体验了Doris的操作流程，下面开始我们 Doris 使用之旅吧。
