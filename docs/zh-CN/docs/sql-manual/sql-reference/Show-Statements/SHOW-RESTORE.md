---
{
    "title": "SHOW-RESTORE",
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

## SHOW-RESTORE

### Name

SHOW RESTORE

### Description

该语句用于查看 RESTORE 任务

语法：

```SQL
SHOW [BRIEF] RESTORE [FROM DB_NAME]
```

说明：
        1. Doris 中仅保存最近一次 RESTORE 任务。
                2. 各列含义如下：
            JobId：                  唯一作业id
            Label：                  要恢复的备份的名称
            Timestamp：              要恢复的备份的时间版本
            DbName：                 所属数据库
            State：                  当前阶段
                PENDING：        提交作业后的初始状态
                SNAPSHOTING：    执行快照中
                DOWNLOAD：       快照完成，准备下载仓库中的快照
                DOWNLOADING：    快照下载中
                COMMIT：         快照下载完成，准备生效
                COMMITTING：      生效中
                FINISHED：       作业成功
                CANCELLED：      作业失败
            AllowLoad：              恢复时是否允许导入（当前不支持）
            ReplicationNum：         指定恢复的副本数
            RestoreJobs：            要恢复的表和分区
            CreateTime：             任务提交时间
            MetaPreparedTime：       元数据准备完成时间
            SnapshotFinishedTime：   快照完成时间
            DownloadFinishedTime：   快照下载完成时间
            FinishedTime：           作业结束时间
            UnfinishedTasks：        在 SNAPSHOTING、DOWNLOADING 和 COMMITTING 阶段会显示还未完成的子任务id
            Status：                 如果作业失败，显示失败信息
            Timeout：                作业超时时间，单位秒

<version since="dev">

        2. brief: 仅返回精简格式的 RESTORE 任务信息，不包含 RestoreObjs, Progress, TaskErrMsg 三列 

</version>

### Example

1. 查看 example_db 下最近一次 RESTORE 任务。
    
    ```sql
    SHOW RESTORE FROM example_db;
    ```

### Keywords

    SHOW, RESTORE

### Best Practice

