---
{
    "title": "数据备份恢复",
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

# 数据备份恢复

Doris 支持将当前数据以文件的形式，通过 broker 备份到远端存储系统中。之后可以通过 恢复 命令，从远端存储系统中将数据恢复到任意 Doris 集群。通过这个功能，Doris 可以支持将数据定期的进行快照备份。也可以通过这个功能，在不同集群间进行数据迁移。

该功能需要 Doris 版本 0.8.2+

使用该功能，需要部署对应远端存储的 broker。如 BOS、HDFS 等。可以通过 `SHOW BROKER;` 查看当前部署的 broker。

## 简要原理说明

恢复操作需要指定一个远端仓库中已存在的备份，然后将这个备份的内容恢复到本地集群中。当用户提交 Restore 请求后，系统内部会做如下操作：

1. 在本地创建对应的元数据

   这一步首先会在本地集群中，创建恢复对应的表分区等结构。创建完成后，该表可见，但是不可访问。

2. 本地snapshot

   这一步是将上一步创建的表做一个快照。这其实是一个空快照（因为刚创建的表是没有数据的），其目的主要是在 Backend 上产生对应的快照目录，用于之后接收从远端仓库下载的快照文件。

3. 下载快照

   远端仓库中的快照文件，会被下载到对应的上一步生成的快照目录中。这一步由各个 Backend 并发完成。

4. 生效快照

   快照下载完成后，我们要将各个快照映射为当前本地表的元数据。然后重新加载这些快照，使之生效，完成最终的恢复作业。

## 开始恢复

1. 从 example_repo 中恢复备份 snapshot_1 中的表 backup_tbl 到数据库 example_db1，时间版本为 "2018-05-04-16-45-08"。恢复为 1 个副本：

    ```sql
    RESTORE SNAPSHOT example_db1.`snapshot_1`
    FROM `example_repo`
    ON ( `backup_tbl` )
    PROPERTIES
    (
        "backup_timestamp"="2022-04-08-15-52-29",
        "replication_num" = "1"
    );
    ```

2. 从 example_repo 中恢复备份 snapshot_2 中的表 backup_tbl 的分区 p1,p2，以及表 backup_tbl2 到数据库 example_db1，并重命名为 new_tbl，时间版本为 "2018-05-04-17-11-01"。默认恢复为 3 个副本：

    ```sql
    RESTORE SNAPSHOT example_db1.`snapshot_2`
    FROM `example_repo`
    ON
    (
        `backup_tbl` PARTITION (`p1`, `p2`),
        `backup_tbl2` AS `new_tbl`
    )
    PROPERTIES
    (
        "backup_timestamp"="2022-04-08-15-55-43"
    );
    ```

3. 查看 restore 作业的执行情况:

   ```sql
   mysql> SHOW RESTORE\G;
   *************************** 1. row ***************************
                  JobId: 17891851
                  Label: snapshot_label1
              Timestamp: 2022-04-08-15-52-29
                 DbName: default_cluster:example_db1
                  State: FINISHED
              AllowLoad: false
         ReplicationNum: 3
            RestoreObjs: {
     "name": "snapshot_label1",
     "database": "example_db",
     "backup_time": 1649404349050,
     "content": "ALL",
     "olap_table_list": [
       {
         "name": "backup_tbl",
         "partition_names": [
           "p1",
           "p2"
         ]
       }
     ],
     "view_list": [],
     "odbc_table_list": [],
     "odbc_resource_list": []
   }
             CreateTime: 2022-04-08 15:59:01
       MetaPreparedTime: 2022-04-08 15:59:02
   SnapshotFinishedTime: 2022-04-08 15:59:05
   DownloadFinishedTime: 2022-04-08 15:59:12
           FinishedTime: 2022-04-08 15:59:18
        UnfinishedTasks:
               Progress:
             TaskErrMsg:
                 Status: [OK]
                Timeout: 86400
   1 row in set (0.01 sec)
   ```

RESTORE的更多用法可参考 [这里](../../sql-manual/sql-reference/Data-Definition-Statements/Backup-and-Restore/RESTORE.md)。

## 相关命令

和备份恢复功能相关的命令如下。以下命令，都可以通过 mysql-client 连接 Doris 后，使用 `help cmd;` 的方式查看详细帮助。

1. CREATE REPOSITORY

   创建一个远端仓库路径，用于备份或恢复。该命令需要借助 Broker 进程访问远端存储，不同的 Broker 需要提供不同的参数，具体请参阅 [Broker文档](../../advanced/broker.md)，也可以直接通过S3 协议备份到支持AWS S3协议的远程存储上去，也可以直接备份到HDFS，具体参考 [创建远程仓库文档](../../sql-manual/sql-reference/Data-Definition-Statements/Backup-and-Restore/CREATE-REPOSITORY.md)

2. RESTORE

   执行一次恢复操作。

3. SHOW RESTORE

   查看最近一次 restore 作业的执行情况，包括：

   - JobId：本次恢复作业的 id。
   - Label：用户指定的仓库中备份的名称（Label）。
   - Timestamp：用户指定的仓库中备份的时间戳。
   - DbName：恢复作业对应的 Database。
   - State：恢复作业当前所在阶段：
     - PENDING：作业初始状态。
     - SNAPSHOTING：正在进行本地新建表的快照操作。
     - DOWNLOAD：正在发送下载快照任务。
     - DOWNLOADING：快照正在下载。
     - COMMIT：准备生效已下载的快照。
     - COMMITTING：正在生效已下载的快照。
     - FINISHED：恢复完成。
     - CANCELLED：恢复失败或被取消。
   - AllowLoad：恢复期间是否允许导入。
   - ReplicationNum：恢复指定的副本数。
   - RestoreObjs：本次恢复涉及的表和分区的清单。
   - CreateTime：作业创建时间。
   - MetaPreparedTime：本地元数据生成完成时间。
   - SnapshotFinishedTime：本地快照完成时间。
   - DownloadFinishedTime：远端快照下载完成时间。
   - FinishedTime：本次作业完成时间。
   - UnfinishedTasks：在 `SNAPSHOTTING`，`DOWNLOADING`, `COMMITTING` 等阶段，会有多个子任务在同时进行，这里展示的当前阶段，未完成的子任务的 task id。
   - TaskErrMsg：如果有子任务执行出错，这里会显示对应子任务的错误信息。
   - Status：用于记录在整个作业过程中，可能出现的一些状态信息。
   - Timeout：作业的超时时间，单位是秒。

4. CANCEL RESTORE

   取消当前正在执行的恢复作业。

5. DROP REPOSITORY

   删除已创建的远端仓库。删除仓库，仅仅是删除该仓库在 Doris 中的映射，不会删除实际的仓库数据。

## 常见错误

1. RESTORE报错：[20181: invalid md5 of downloaded file:/data/doris.HDD/snapshot/20220607095111.862.86400/19962/668322732/19962.hdr, expected: f05b63cca5533ea0466f62a9897289b5, get: d41d8cd98f00b204e9800998ecf8427e]

   备份和恢复的表的副本数不一致导致的，执行恢复命令时需指定副本个数，具体命令请参阅[RESTORE](../../sql-manual/sql-reference/Data-Definition-Statements/Backup-and-Restore/RESTORE.md) 命令手册

2. RESTORE报错：[COMMON_ERROR, msg: Could not set meta version to 97 since it is lower than minimum required version 100]

   备份和恢复不是同一个版本导致的，使用指定的 meta_version 来读取之前备份的元数据。注意，该参数作为临时方案，仅用于恢复老版本 Doris 备份的数据。最新版本的备份数据中已经包含 meta version，无需再指定，针对上述错误具体解决方案指定meta_version = 100，具体命令请参阅[RESTORE](../../sql-manual/sql-reference/Data-Definition-Statements/Backup-and-Restore/RESTORE.md) 命令手册

## 更多帮助

关于 RESTORE 使用的更多详细语法及最佳实践，请参阅 [RESTORE](../../sql-manual/sql-reference/Data-Definition-Statements/Backup-and-Restore/RESTORE.md) 命令手册，你也可以在 MySql 客户端命令行下输入 `HELP RESTORE` 获取更多帮助信息。

