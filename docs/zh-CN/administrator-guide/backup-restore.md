---
{
    "title": "备份与恢复",
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

# 备份与恢复

Doris 支持将当前数据以文件的形式，通过 broker 备份到远端存储系统中。之后可以通过 恢复 命令，从远端存储系统中将数据恢复到任意 Doris 集群。通过这个功能，Doris 可以支持将数据定期的进行快照备份。也可以通过这个功能，在不同集群间进行数据迁移。

该功能需要 Doris 版本 0.8.2+

使用该功能，需要部署对应远端存储的 broker。如 BOS、HDFS 等。可以通过 `SHOW BROKER;` 查看当前部署的 broker。

## 简要原理说明

### 备份（Backup）

备份操作是将指定表或分区的数据，直接以 Doris 存储的文件的形式，上传到远端仓库中进行存储。当用户提交 Backup 请求后，系统内部会做如下操作：

1. 快照及快照上传

    快照阶段会对指定的表或分区数据文件进行快照。之后，备份都是对快照进行操作。在快照之后，对表进行的更改、导入等操作都不再影响备份的结果。快照只是对当前数据文件产生一个硬链，耗时很少。快照完成后，会开始对这些快照文件进行逐一上传。快照上传由各个 Backend 并发完成。

2. 元数据准备及上传

    数据文件快照上传完成后，Frontend 会首先将对应元数据写成本地文件，然后通过 broker 将本地元数据文件上传到远端仓库。完成最终备份作业
    
2. 动态分区表说明

    如果该表是动态分区表，备份之后会自动禁用动态分区属性，在做恢复的时候需要手动将该表的动态分区属性启用，命令如下:
    ```sql
    ALTER TABLE tbl1 SET ("dynamic_partition.enable"="true")
    ```

### 恢复（Restore）

恢复操作需要指定一个远端仓库中已存在的备份，然后将这个备份的内容恢复到本地集群中。当用户提交 Restore 请求后，系统内部会做如下操作：

1. 在本地创建对应的元数据

    这一步首先会在本地集群中，创建恢复对应的表分区等结构。创建完成后，该表可见，但是不可访问。

2. 本地snapshot

    这一步是将上一步创建的表做一个快照。这其实是一个空快照（因为刚创建的表是没有数据的），其目的主要是在 Backend 上产生对应的快照目录，用于之后接收从远端仓库下载的快照文件。

3. 下载快照

    远端仓库中的快照文件，会被下载到对应的上一步生成的快照目录中。这一步由各个 Backend 并发完成。

4. 生效快照

    快照下载完成后，我们要将各个快照映射为当前本地表的元数据。然后重新加载这些快照，使之生效，完成最终的恢复作业。

## 最佳实践

### 备份

当前我们支持最小分区（Partition）粒度的全量备份（增量备份有可能在未来版本支持）。如果需要对数据进行定期备份，首先需要在建表时，合理的规划表的分区及分桶，比如按时间进行分区。然后在之后的运行过程中，按照分区粒度进行定期的数据备份。

### 数据迁移

用户可以先将数据备份到远端仓库，再通过远端仓库将数据恢复到另一个集群，完成数据迁移。因为数据备份是通过快照的形式完成的，所以，在备份作业的快照阶段之后的新的导入数据，是不会备份的。因此，在快照完成后，到恢复作业完成这期间，在原集群上导入的数据，都需要在新集群上同样导入一遍。

建议在迁移完成后，对新旧两个集群并行导入一段时间。完成数据和业务正确性校验后，再将业务迁移到新的集群。

## 重点说明

1. 备份恢复相关的操作目前只允许拥有 ADMIN 权限的用户执行。
2. 一个 Database 内，只允许有一个正在执行的备份或恢复作业。
3. 备份和恢复都支持最小分区（Partition）级别的操作，当表的数据量很大时，建议按分区分别执行，以降低失败重试的代价。
4. 因为备份恢复操作，操作的都是实际的数据文件。所以当一个表的分片过多，或者一个分片有过多的小版本时，可能即使总数据量很小，依然需要备份或恢复很长时间。用户可以通过 `SHOW PARTITIONS FROM table_name;` 和 `SHOW TABLET FROM table_name;` 来查看各个分区的分片数量，以及各个分片的文件版本数量，来预估作业执行时间。文件数量对作业执行的时间影响非常大，所以建议在建表时，合理规划分区分桶，以避免过多的分片。
5. 当通过 `SHOW BACKUP` 或者 `SHOW RESTORE` 命令查看作业状态时。有可能会在 `TaskErrMsg` 一列中看到错误信息。但只要 `State` 列不为
 `CANCELLED`，则说明作业依然在继续。这些 Task 有可能会重试成功。当然，有些 Task 错误，也会直接导致作业失败。
6. 如果恢复作业是一次覆盖操作（指定恢复数据到已经存在的表或分区中），那么从恢复作业的 `COMMIT` 阶段开始，当前集群上被覆盖的数据有可能不能再被还原。此时如果恢复作业失败或被取消，有可能造成之前的数据已损坏且无法访问。这种情况下，只能通过再次执行恢复操作，并等待作业完成。因此，我们建议，如无必要，尽量不要使用覆盖的方式恢复数据，除非确认当前数据已不再使用。

## 相关命令

和备份恢复功能相关的命令如下。以下命令，都可以通过 mysql-client 连接 Doris 后，使用 `help cmd;` 的方式查看详细帮助。

1. CREATE REPOSITORY

    创建一个远端仓库路径，用于备份或恢复。该命令需要借助 Broker 进程访问远端存储，不同的 Broker 需要提供不同的参数，具体请参阅 [Broker文档](broker.md)，也可以直接通过S3 协议备份到支持AWS S3协议的远程存储上去，具体参考 [创建远程仓库文档](../sql-reference/sql-statements/Data%20Definition/CREATE%20REPOSITORY.md)

1. BACKUP

    执行一次备份操作。

3. SHOW BACKUP

    查看最近一次 backup 作业的执行情况，包括：

    * JobId：本次备份作业的 id。
    * SnapshotName：用户指定的本次备份作业的名称（Label）。
    * DbName：备份作业对应的 Database。
    * State：备份作业当前所在阶段：
        * PENDING：作业初始状态。
        * SNAPSHOTING：正在进行快照操作。
        * UPLOAD_SNAPSHOT：快照结束，准备上传。
        * UPLOADING：正在上传快照。
        * SAVE_META：正在本地生成元数据文件。
        * UPLOAD_INFO：上传元数据文件和本次备份作业的信息。
        * FINISHED：备份完成。
        * CANCELLED：备份失败或被取消。
    * BackupObjs：本次备份涉及的表和分区的清单。
    * CreateTime：作业创建时间。
    * SnapshotFinishedTime：快照完成时间。
    * UploadFinishedTime：快照上传完成时间。
    * FinishedTime：本次作业完成时间。
    * UnfinishedTasks：在 `SNAPSHOTTING`，`UPLOADING` 等阶段，会有多个子任务在同时进行，这里展示的当前阶段，未完成的子任务的 task id。
    * TaskErrMsg：如果有子任务执行出错，这里会显示对应子任务的错误信息。
    * Status：用于记录在整个作业过程中，可能出现的一些状态信息。
    * Timeout：作业的超时时间，单位是秒。

4. SHOW SNAPSHOT

    查看远端仓库中已存在的备份。

    * Snapshot：备份时指定的该备份的名称（Label）。
    * Timestamp：备份的时间戳。
    * Status：该备份是否正常。

    如果在 `SHOW SNAPSHOT` 后指定了 where 子句，则可以显示更详细的备份信息。

    * Database：备份时对应的 Database。
    * Details：展示了该备份完整的数据目录结构。

5. RESTORE

    执行一次恢复操作。

6. SHOW RESTORE

    查看最近一次 restore 作业的执行情况，包括：

    * JobId：本次恢复作业的 id。
    * Label：用户指定的仓库中备份的名称（Label）。
    * Timestamp：用户指定的仓库中备份的时间戳。
    * DbName：恢复作业对应的 Database。
    * State：恢复作业当前所在阶段：
        * PENDING：作业初始状态。
        * SNAPSHOTING：正在进行本地新建表的快照操作。
        * DOWNLOAD：正在发送下载快照任务。
        * DOWNLOADING：快照正在下载。
        * COMMIT：准备生效已下载的快照。
        * COMMITTING：正在生效已下载的快照。
        * FINISHED：恢复完成。
        * CANCELLED：恢复失败或被取消。
    * AllowLoad：恢复期间是否允许导入。
    * ReplicationNum：恢复指定的副本数。
    * RestoreObjs：本次恢复涉及的表和分区的清单。
    * CreateTime：作业创建时间。
    * MetaPreparedTime：本地元数据生成完成时间。
    * SnapshotFinishedTime：本地快照完成时间。
    * DownloadFinishedTime：远端快照下载完成时间。
    * FinishedTime：本次作业完成时间。
    * UnfinishedTasks：在 `SNAPSHOTTING`，`DOWNLOADING`, `COMMITTING` 等阶段，会有多个子任务在同时进行，这里展示的当前阶段，未完成的子任务的 task id。
    * TaskErrMsg：如果有子任务执行出错，这里会显示对应子任务的错误信息。
    * Status：用于记录在整个作业过程中，可能出现的一些状态信息。
    * Timeout：作业的超时时间，单位是秒。

7. CANCEL BACKUP

    取消当前正在执行的备份作业。

8. CANCEL RESTORE

    取消当前正在执行的恢复作业。

9. DROP REPOSITORY

    删除已创建的远端仓库。删除仓库，仅仅是删除该仓库在 Doris 中的映射，不会删除实际的仓库数据。