---
{
    "title": "SHOW STREAM LOAD",
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

# SHOW STREAM LOAD
## description
    该语句用于展示指定的Stream Load任务的执行情况
    语法：
        SHOW STREAM LOAD
        [FROM db_name]
        [
            WHERE 
            [LABEL [ = "your_label" | LIKE "label_matcher"]]
            [STATUS = ["SUCCESS"|"FAIL"]]
        ]
        [ORDER BY ...]
        [LIMIT limit][OFFSET offset];
        
    说明：
        1) 如果不指定 db_name，使用当前默认db
        2) 如果使用 LABEL LIKE，则会匹配Stream Load任务的 label 包含 label_matcher 的任务
        3) 如果使用 LABEL = ，则精确匹配指定的 label
        4) 如果指定了 STATUS，则匹配 STREAM LOAD 状态
        5) 可以使用 ORDER BY 对任意列组合进行排序
        6) 如果指定了 LIMIT，则显示 limit 条匹配记录。否则全部显示
        7) 如果指定了 OFFSET，则从偏移量offset开始显示查询结果。默认情况下偏移量为0。

## example
    1. 展示默认 db 的所有Stream Load任务
        SHOW STREAM LOAD;
    
    2. 展示指定 db 的Stream Load任务，label 中包含字符串 "2014_01_02"，展示最老的10个
        SHOW STREAM LOAD FROM example_db WHERE LABEL LIKE "2014_01_02" LIMIT 10;
        
    3. 展示指定 db 的Stream Load任务，指定 label 为 "load_example_db_20140102"
        SHOW STREAM LOAD FROM example_db WHERE LABEL = "load_example_db_20140102";
        
    4. 展示指定 db 的Stream Load任务，指定 status 为 "success", 并按 StartTime 降序排序
        SHOW STREAM LOAD FROM example_db WHERE STATUS = "success" ORDER BY StartTime DESC;
        
    5. 展示指定 db 的导入任务 并按 StartTime 降序排序,并从偏移量5开始显示10条查询结果
        SHOW STREAM LOAD FROM example_db ORDER BY StartTime DESC limit 5,10;
        SHOW STREAM LOAD FROM example_db ORDER BY StartTime DESC limit 10 offset 5;

## keyword
    SHOW,STREAM LOAD

