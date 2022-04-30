---
{
    "title": "Tablet 恢复工具",
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

# Tablet 恢复工具

## 从 BE 回收站中恢复数据

用户在使用Doris的过程中，可能会发生因为一些误操作或者线上bug，导致一些有效的tablet被删除（包括元数据和数据）。为了防止在这些异常情况出现数据丢失，Doris提供了回收站机制，来保护用户数据。用户删除的tablet数据不会被直接删除，会被放在回收站中存储一段时间，在一段时间之后会有定时清理机制将过期的数据删除。回收站中的数据包括：tablet的data文件(.dat)，tablet的索引文件(.idx)和tablet的元数据文件(.hdr)。数据将会存放在如下格式的路径：

```
/root_path/trash/time_label/tablet_id/schema_hash/
```

* `root_path`：对应BE节点的某个数据根目录。
* `trash`：回收站的目录。
* `time_label`：时间标签，为了回收站中数据目录的唯一性，同时记录数据时间，使用时间标签作为子目录。

当用户发现线上的数据被误删除，需要从回收站中恢复被删除的tablet，需要用到这个tablet数据恢复功能。

BE提供http接口和 `restore_tablet_tool.sh` 脚本实现这个功能，支持单tablet操作（single mode）和批量操作模式（batch mode）。

* 在single mode下，支持单个tablet的数据恢复。
* 在batch mode下，支持批量tablet的数据恢复。

### 操作

#### single mode

1. http请求方式

    BE中提供单个tablet数据恢复的http接口，接口如下：
    
    ```
    curl -X POST "http://be_host:be_webserver_port/api/restore_tablet?tablet_id=11111\&schema_hash=12345"
    ```
    
    成功的结果如下：
    
    ```
    {"status": "Success", "msg": "OK"}
    ```
    
    失败的话，会返回相应的失败原因，一种可能的结果如下：
    
    ```
    {"status": "Failed", "msg": "create link path failed"}
    ```

2. 脚本方式

    `restore_tablet_tool.sh` 可用来实现单tablet数据恢复的功能。
    
    ```
    sh tools/restore_tablet_tool.sh -b "http://127.0.0.1:8040" -t 12345 -s 11111
    sh tools/restore_tablet_tool.sh --backend "http://127.0.0.1:8040" --tablet_id 12345 --schema_hash 11111
    ```

#### batch mode

批量恢复模式用于实现恢复多个tablet数据的功能。

使用的时候需要预先将恢复的tablet id和schema hash按照逗号分隔的格式放在一个文件中，一个tablet一行。

格式如下：

```
12345,11111
12346,11111
12347,11111
```

然后如下的命令进行恢复(假设文件名为：`tablets.txt`)：

```
sh restore_tablet_tool.sh -b "http://127.0.0.1:8040" -f tablets.txt
sh restore_tablet_tool.sh --backend "http://127.0.0.1:8040" --file tablets.txt
```

## 修复缺失或损坏的 Tablet

在某些极特殊情况下，如代码BUG、或人为误操作等，可能导致部分分片的全部副本都丢失。这种情况下，数据已经实质性的丢失。但是在某些场景下，业务依然希望能够在即使有数据丢失的情况下，保证查询正常不报错，降低用户层的感知程度。此时，我们可以通过使用空白Tablet填充丢失副本的功能，来保证查询能够正常执行。

**注：该操作仅用于规避查询因无法找到可查询副本导致报错的问题，无法恢复已经实质性丢失的数据**

1. 查看 Master FE 日志 `fe.log`

    如果出现数据丢失的情况，则日志中会有类似如下日志：
    
    ```
    backend [10001] invalid situation. tablet[20000] has few replica[1], replica num setting is [3]
    ```

    这个日志表示，Tablet 20000 的所有副本已损坏或丢失。
    
2. 使用空白副本填补缺失副本

    当确认数据已经无法恢复后，可以通过执行以下命令，生成空白副本。
    
    ```
    ADMIN SET FRONTEND CONFIG ("recover_with_empty_tablet" = "true");
    ```

    * 注：可以先通过 `ADMIN SHOW FRONTEND CONFIG;` 命令查看当前版本是否支持该参数。

3. 设置完成几分钟后，应该会在 Master FE 日志 `fe.log` 中看到如下日志：

    ```
    tablet 20000 has only one replica 20001 on backend 10001 and it is lost. create an empty replica to recover it.
    ```

    该日志表示系统已经创建了一个空白 Tablet 用于填补缺失副本。
    
4. 通过查询来判断是否已经修复成功。

5. 全部修复成功后，通过以下命令关闭 `recover_with_empty_tablet` 参数：

    ```
    ADMIN SET FRONTEND CONFIG ("recover_with_empty_tablet" = "false");
    ```
