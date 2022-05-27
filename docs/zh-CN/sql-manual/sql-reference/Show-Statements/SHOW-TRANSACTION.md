---
{
    "title": "SHOW-TRANSACTION",
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

## SHOW-TRANSACTION

### Name

SHOW TRANSACTION

### Description

该语法用于查看指定 transaction id 或 label 的事务详情。

语法：

```sql
SHOW TRANSACTION
[FROM db_name]
WHERE
[id = transaction_id]
[label = label_name];
```

返回结果示例：

```
     TransactionId: 4005
             Label: insert_8d807d5d-bcdd-46eb-be6d-3fa87aa4952d
       Coordinator: FE: 10.74.167.16
 TransactionStatus: VISIBLE
 LoadJobSourceType: INSERT_STREAMING
       PrepareTime: 2020-01-09 14:59:07
        CommitTime: 2020-01-09 14:59:09
        FinishTime: 2020-01-09 14:59:09
            Reason:
ErrorReplicasCount: 0
        ListenerId: -1
         TimeoutMs: 300000
```

* TransactionId：事务id
* Label：导入任务对应的 label
* Coordinator：负责事务协调的节点
* TransactionStatus：事务状态
    * PREPARE：准备阶段
    * COMMITTED：事务成功，但数据不可见
    * VISIBLE：事务成功且数据可见
    * ABORTED：事务失败
* LoadJobSourceType：导入任务的类型。
* PrepareTime：事务开始时间
* CommitTime：事务提交成功的时间
* FinishTime：数据可见的时间
* Reason：错误信息
* ErrorReplicasCount：有错误的副本数
* ListenerId：相关的导入作业的id
* TimeoutMs：事务超时时间，单位毫秒

### Example

1. 查看 id 为 4005 的事务：

    ```sql
    SHOW TRANSACTION WHERE ID=4005;
    ```

2. 指定 db 中，查看 id 为 4005 的事务：

    ```sql
    SHOW TRANSACTION FROM db WHERE ID=4005;
    ```

3. 查看 label 为 label_name的事务：
    
    ```sql
    SHOW TRANSACTION WHERE LABEL = 'label_name';
    ```

### Keywords

    SHOW, TRANSACTION

### Best Practice

