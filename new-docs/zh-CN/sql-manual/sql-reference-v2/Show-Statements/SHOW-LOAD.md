---
{
    "title": "SHOW-LOAD",
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

## SHOW-LOAD

### Name

SHOW LOAD

### Description

该语句用于展示指定的导入任务的执行情况

语法：

```sql
SHOW LOAD
[FROM db_name]
[
   WHERE
   [LABEL [ = "your_label" | LIKE "label_matcher"]]
   [STATE = ["PENDING"|"ETL"|"LOADING"|"FINISHED"|"CANCELLED"|]]
]
[ORDER BY ...]
[LIMIT limit][OFFSET offset];
```

说明：

1) 如果不指定 db_name，使用当前默认db
    
1)  如果使用 LABEL LIKE，则会匹配导入任务的 label 包含 label_matcher 的导入任务
    
1)  如果使用 LABEL = ，则精确匹配指定的 label
    
1) 如果指定了 STATE，则匹配 LOAD 状态
    
1) 可以使用 ORDER BY 对任意列组合进行排序
    
1)  如果指定了 LIMIT，则显示 limit 条匹配记录。否则全部显示
    
1) 如果指定了 OFFSET，则从偏移量offset开始显示查询结果。默认情况下偏移量为0。
    
1)  如果是使用 broker/mini load，则 URL 列中的连接可以使用以下命令查看：
    
    ```sql
    SHOW LOAD WARNINGS ON 'url'
    ```

### Example

1. 展示默认 db 的所有导入任务
    
    ```sql
    SHOW LOAD;
    ```

1. 展示指定 db 的导入任务，label 中包含字符串 "2014_01_02"，展示最老的10个
    
    ```sql
    SHOW LOAD FROM example_db WHERE LABEL LIKE "2014_01_02" LIMIT 10;
    ```

1. 展示指定 db 的导入任务，指定 label 为 "load_example_db_20140102" 并按 LoadStartTime 降序排序
    
    ```sql
    SHOW LOAD FROM example_db WHERE LABEL = "load_example_db_20140102" ORDER BY LoadStartTime DESC;
    ```

1. 展示指定 db 的导入任务，指定 label 为 "load_example_db_20140102" ，state 为 "loading", 并按 LoadStartTime 降序排序
    
    ```sql
    SHOW LOAD FROM example_db WHERE LABEL = "load_example_db_20140102" AND STATE = "loading" ORDER BY LoadStartTime DESC;
    ```

1. 展示指定 db 的导入任务 并按 LoadStartTime 降序排序,并从偏移量5开始显示10条查询结果
    
    ```sql
    SHOW LOAD FROM example_db ORDER BY LoadStartTime DESC limit 5,10;
    SHOW LOAD FROM example_db ORDER BY LoadStartTime DESC limit 10 offset 5;
    ```
    
1. 小批量导入是查看导入状态的命令
    
    ```
    curl --location-trusted -u {user}:{passwd} http://{hostname}:{port}/api/{database}/_load_info?label={labelname}
    ```

### Keywords

    SHOW, LOAD

### Best Practice

