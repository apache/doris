---
{
    "title": "SHOW LOAD",
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

# SHOW LOAD
## description
    该语句用于展示指定的导入任务的执行情况
    语法：
        SHOW LOAD
        [FROM db_name]
        [
            WHERE 
            [LABEL [ = "your_label" | LIKE "label_matcher"]]
            [STATE = ["PENDING"|"ETL"|"LOADING"|"FINISHED"|"CANCELLED"|]]
        ]
        [ORDER BY ...]
        [LIMIT limit][OFFSET offset];
        
    说明：
        1) 如果不指定 db_name，使用当前默认db
        2) 如果使用 LABEL LIKE，则会匹配导入任务的 label 包含 label_matcher 的导入任务
        3) 如果使用 LABEL = ，则精确匹配指定的 label
        4) 如果指定了 STATE，则匹配 LOAD 状态
        5) 可以使用 ORDER BY 对任意列组合进行排序
        6) 如果指定了 LIMIT，则显示 limit 条匹配记录。否则全部显示
        7) 如果指定了 OFFSET，则从偏移量offset开始显示查询结果。默认情况下偏移量为0。
        8) 如果是使用 broker/mini load，则 URL 列中的连接可以使用以下命令查看：

            SHOW LOAD WARNINGS ON 'url'

## example
    1. 展示默认 db 的所有导入任务
        SHOW LOAD;
    
    2. 展示指定 db 的导入任务，label 中包含字符串 "2014_01_02"，展示最老的10个
        SHOW LOAD FROM example_db WHERE LABEL LIKE "2014_01_02" LIMIT 10;
        
    3. 展示指定 db 的导入任务，指定 label 为 "load_example_db_20140102" 并按 LoadStartTime 降序排序
        SHOW LOAD FROM example_db WHERE LABEL = "load_example_db_20140102" ORDER BY LoadStartTime DESC;
        
    4. 展示指定 db 的导入任务，指定 label 为 "load_example_db_20140102" ，state 为 "loading", 并按 LoadStartTime 降序排序
        SHOW LOAD FROM example_db WHERE LABEL = "load_example_db_20140102" AND STATE = "loading" ORDER BY LoadStartTime DESC;
        
    5. 展示指定 db 的导入任务 并按 LoadStartTime 降序排序,并从偏移量5开始显示10条查询结果
        SHOW LOAD FROM example_db ORDER BY LoadStartTime DESC limit 5,10;
        SHOW LOAD FROM example_db ORDER BY LoadStartTime DESC limit 10 offset 5;

    6. 小批量导入是查看导入状态的命令
        curl --location-trusted -u {user}:{passwd} http://{hostname}:{port}/api/{database}/_load_info?label={labelname}
        
## keyword
    SHOW,LOAD

