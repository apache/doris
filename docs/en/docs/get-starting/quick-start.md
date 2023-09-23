---
{
    "title": "Quick Start",
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

# Quick Start

Apache Doris is a high-performance, real-time analytic database based on the MPP architecture and is known for its extreme speed and ease of use. It takes only sub-second response times to return query results under massive amounts of data, and can support not only highly concurrent point query scenarios, but also high-throughput complex analytic scenarios. This brief guide will show you how to download the latest stable version of Doris, install and run it on a single node, including creating databases, data tables, importing data and queries, etc.

## Download Doris

Doris runs on a Linux environment, CentOS 7.x or Ubuntu 16.04 or higher is recommended, and you need to have a Java runtime environment installed (the JDK version required is 8). To check the version of Java you have installed, run the following command.

```
java -version
```

Next, [download the latest binary version of Doris](https://doris.apache.org/download) and unzip it.

```
tar xf apache-doris-x.x.x.tar.xz
```

## Configure Doris

### Configure FE 

Go to the `apache-doris-x.x.x/fe` directory

```
cd apache-doris-x.x.x/fe
```

Modify the FE configuration file `conf/fe.conf`, here we mainly modify two parameters: `priority_networks` and `meta_dir`, if you need more optimized configuration, please refer to [FE parameter configuration](../admin-manual/config/fe-config.md) for instructions on how to adjust them.

1. add priority_networks parameter

```
priority_networks=172.23.16.0/24
```

>Note: 
>
>This parameter we have to configure during installation, especially when a machine has multiple IP addresses, we have to specify a unique IP address for FE.

2. Adding a metadata directory

```
meta_dir=/path/your/doris-meta
```

>Note: 
>
>Here you can leave it unconfigured, the default is doris-meta in your Doris FE installation directory.
>
>To configure the metadata directory separately, you need to create the directory you specify in advance

### Start FE 

Execute the following command in the FE installation directory to complete the FE startup.

```
./bin/start_fe.sh --daemon
```

#### View FE operational status

You can check if Doris started successfully with the following command

```
curl http://127.0.0.1:8030/api/bootstrap
```

Here the IP and port are the IP and http_port of FE (default 8030), if you are executing in FE node, just run the above command directly.

If the return result has the word `"msg": "success"`, then the startup was successful.

You can also check this through the web UI provided by Doris FE by entering the address in your browser

http:// fe_ip:8030

You can see the following screen, which indicates that the FE has started successfully

![image-20220822091951739](/images/image-20220822091951739.png)

>Note: 
>
>1. Here we use the Doris built-in default user, root, to log in with an empty password.
>2. This is an administrative interface for Doris, and only users with administrative privileges can log in.

#### Connect FE

We will connect to Doris FE via MySQL client below, download the installation-free [MySQL client](https://doris-build-hk.oss-cn-hongkong.aliyuncs.com/mysql-client/mysql-5.7.22-linux-glibc2.12-x86_64.tar.gz)

Unzip the MySQL client you just downloaded and you can find the `mysql` command line tool in the `bin/` directory. Then execute the following command to connect to Doris.

```
mysql -uroot -P9030 -h127.0.0.1
```

>Note: 
>
>1. The root user used here is the default user built into doris, and is also the super administrator user, see [Rights Management](../admin-manual/privilege-ldap/user-privilege.md)
>2. -P: Here is our query port to connect to Doris, the default port is 9030, which corresponds to `query_port` in fe.conf
>3. -h: Here is the IP address of the FE we are connecting to, if your client and FE are installed on the same node you can use 127.0.0.1

Execute the following command to view the FE running status

```sql
show frontends\G;
```

You can then see a result similar to the following.

```sql
mysql> show frontends\G;
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

1. If the IsMaster, Join and Alive columns are true, the node is normal.

#### Communicate with the server over an encrypted connection

Doris supports SSL-based encrypted connections. It currently supports TLS1.2 and TLS1.3 protocols. Doris' SSL mode can be enabled through the following configuration:
Modify the FE configuration file `conf/fe.conf` and add `enable_ssl = true`.

Next, connect to Doris through `mysql` client, mysql supports five SSL modes:

1. `mysql -uroot -P9030 -h127.0.0.1` is the same as `mysql --ssl-mode=PREFERRED -uroot -P9030 -h127.0.0.1`, both try to establish an SSL encrypted connection at the beginning, if it fails , a normal connection is attempted.

2. `mysql --ssl-mode=DISABLE -uroot -P9030 -h127.0.0.1`, do not use SSL encrypted connection, use normal connection directly.

3. `mysql --ssl-mode=REQUIRED -uroot -P9030 -h127.0.0.1`, force the use of SSL encrypted connections.

4.`mysql --ssl-mode=VERIFY_CA --ssl-ca=ca.pem -uroot -P9030 -h127.0.0.1`, force the use of SSL encrypted connection and verify the validity of the server's identity by specifying the CA certificate。

5.`mysql --ssl-mode=VERIFY_CA --ssl-ca=ca.pem --ssl-cert=client-cert.pem --ssl-key=client-key.pem -uroot -P9030 -h127.0.0.1`, force the use of SSL encrypted connection, two-way ssl。

>Note:
>`--ssl-mode` parameter is introduced by mysql5.7.11 version, please refer to [here](https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-security.html) for mysql client version lower than this version。

Doris needs a key certificate file to verify the SSL encrypted connection. The default key certificate file is located at `Doris/fe/mysql_ssl_default_certificate/`. For the generation of the key certificate file, please refer to [Key Certificate Configuration](../admin-manual/certificate.md)。

#### Stop FE

The stopping of Doris FE can be done with the following command

```
./bin/stop_fe.sh
```

### Configure BE

Go to the `apache-doris-x.x.x/be` directory

```
cd apache-doris-x.x.x/be
```

Modify the BE configuration file `conf/be.conf`, here we mainly modify two parameters: `priority_networks'` and `storage_root`, if you need more optimized configuration, please refer to [BE parameter configuration](../admin-manual/config/be-config.md) instructions to make adjustments.

1. Add priority_networks parameter

```
priority_networks=172.23.16.0/24
```

>Note: 
>
>This parameter we have to configure during installation, especially when a machine has multiple IP addresses, we have to assign a unique IP address to the BE.

2. Configure the BE data storage directory


```
storage_root_path=/path/your/data_dir
```

>Notes.
>
>1. The default directory is in the storage directory of the BE installation directory.
>2. The storage directory for BE configuration must be created first

3. Set JAVA_HOME environment variable

  <version since="1.2.0"></version>  
  Java UDF are supported since version 1.2, so BE are dependent on the Java environment. It is necessary to set the `JAVA_HOME` environment variable before starting. You can also add `export JAVA_HOME=your_java_home_path` to the first line of the `start_be.sh` startup script to set the variable.

4. Install Java UDF functions 

  <version since="1.2.0">Install Java UDF functions</version>  
  Because Java UDF functions are supported from version 1.2, you need to download the JAR package of Java UDF functions from the official website and put them in the lib directory of BE, otherwise it may fail to start.

### Start BE

Execute the following command in the BE installation directory to complete the BE startup.

```
./bin/start_be.sh --daemon
```

#### Adding a BE node to a cluster

Connect to FE via MySQL client and execute the following SQL to add the BE to the cluster

```sql
ALTER SYSTEM ADD BACKEND "be_host_ip:heartbeat_service_port";
```

1. be_host_ip: Here is the IP address of your BE, match with `priority_networks` in `be.conf`.
2. heartbeat_service_port: This is the heartbeat upload port of your BE, match with `heartbeat_service_port` in `be.conf`, default is `9050`.

#### View BE operational status

You can check the running status of BE by executing the following command at the MySQL command line.

```sql
SHOW BACKENDS\G；
```

Example:

```sql
mysql> SHOW BACKENDS\G;
*************************** 1. row ***************************
            BackendId: 10003
              Cluster: default_cluster
                   IP: 172.21.32.5
        HeartbeatPort: 9050
               BePort: 9060
             HttpPort: 8040
             BrpcPort: 8060
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

1. Alive : true means the node is running normally

#### Stop BE

The stopping of Doris BE can be done with the following command

```
./bin/stop_be.sh
```

## Create table

1. Create database

```sql
create database demo;
```

2. Create table

```sql
use demo;

CREATE TABLE IF NOT EXISTS demo.example_tbl
(
    `user_id` LARGEINT NOT NULL COMMENT "user id",
    `date` DATE NOT NULL COMMENT "",
    `city` VARCHAR(20) COMMENT "",
    `age` SMALLINT COMMENT "",
    `sex` TINYINT COMMENT "",
    `last_visit_date` DATETIME REPLACE DEFAULT "1970-01-01 00:00:00" COMMENT "",
    `cost` BIGINT SUM DEFAULT "0" COMMENT "",
    `max_dwell_time` INT MAX DEFAULT "0" COMMENT "",
    `min_dwell_time` INT MIN DEFAULT "99999" COMMENT ""
)
AGGREGATE KEY(`user_id`, `date`, `city`, `age`, `sex`)
DISTRIBUTED BY HASH(`user_id`) BUCKETS 1
PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
);
```

3. Example data

```
10000,2017-10-01,beijing,20,0,2017-10-01 06:00:00,20,10,10
10006,2017-10-01,beijing,20,0,2017-10-01 07:00:00,15,2,2
10001,2017-10-01,beijing,30,1,2017-10-01 17:05:45,2,22,22
10002,2017-10-02,shanghai,20,1,2017-10-02 12:59:12,200,5,5
10003,2017-10-02,guangzhou,32,0,2017-10-02 11:20:00,30,11,11
10004,2017-10-01,shenzhen,35,0,2017-10-01 10:00:15,100,3,3
10004,2017-10-03,shenzhen,35,0,2017-10-03 10:20:22,11,6,6
```

Save the above data into `test.csv` file.

4. Import data

Here we import the data saved to the file above into the table we just created via Stream load。

```
curl  --location-trusted -u root: -T test.csv -H "column_separator:," http://127.0.0.1:8030/api/demo/example_tbl/_stream_load
```

- -T test.csv : This is the data file we just saved, if the path is different, please specify the full path
- -u root: Here is the user name and password, we use the default user root, the password is empty
- 127.0.0.1:8030 : is the ip and http_port of fe, respectively

After successful execution we can see the following return message

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

1. `NumberLoadedRows` indicates the number of data records that have been imported

2. `NumberTotalRows` indicates the total amount of data to be imported

3. `Status` :Success means the import was successful

Here we have finished importing the data, and we can now query and analyze the data according to our own needs.

## Query data

We have finished building tables and importing data above, so we can experience Doris' ability to quickly query and analyze data.

```sql
mysql> select * from example_tbl;
+---------+------------+-----------+------+------+---------------------+------+----------------+----------------+
| user_id | date       | city      | age  | sex  | last_visit_date     | cost | max_dwell_time | min_dwell_time |
+---------+------------+-----------+------+------+---------------------+------+----------------+----------------+
| 10000   | 2017-10-01 | beijing   |   20 |    0 | 2017-10-01 06:00:00 |   20 |             10 |             10 |
| 10001   | 2017-10-01 | beijing   |   30 |    1 | 2017-10-01 17:05:45 |    2 |             22 |             22 |
| 10002   | 2017-10-02 | shanghai  |   20 |    1 | 2017-10-02 12:59:12 |  200 |              5 |              5 |
| 10003   | 2017-10-02 | guangzhou |   32 |    0 | 2017-10-02 11:20:00 |   30 |             11 |             11 |
| 10004   | 2017-10-01 | shenzhen  |   35 |    0 | 2017-10-01 10:00:15 |  100 |              3 |              3 |
| 10004   | 2017-10-03 | shenzhen  |   35 |    0 | 2017-10-03 10:20:22 |   11 |              6 |              6 |
| 10006   | 2017-10-01 | beijing   |   20 |    0 | 2017-10-01 07:00:00 |   15 |              2 |              2 |
+---------+------------+-----------+------+------+---------------------+------+----------------+----------------+
7 rows in set (0.01 sec)

mysql> select * from example_tbl where city='shanghai';
+---------+------------+----------+------+------+---------------------+------+----------------+----------------+
| user_id | date       | city     | age  | sex  | last_visit_date     | cost | max_dwell_time | min_dwell_time |
+---------+------------+----------+------+------+---------------------+------+----------------+----------------+
| 10002   | 2017-10-02 | shanghai |   20 |    1 | 2017-10-02 12:59:12 |  200 |              5 |              5 |
+---------+------------+----------+------+------+---------------------+------+----------------+----------------+
1 row in set (0.00 sec)

mysql> select city, sum(cost) as total_cost from example_tbl group by city;
+-----------+------------+
| city      | total_cost |
+-----------+------------+
| beijing   |         37 |
| shenzhen  |        111 |
| guangzhou |         30 |
| shanghai  |        200 |
+-----------+------------+
4 rows in set (0.00 sec)
```



This is the end of our entire quick start. We have experienced the complete Doris operation process from Doris installation and deployment, start/stop, creation of library tables, data import and query, let's start our Doris usage journey.
