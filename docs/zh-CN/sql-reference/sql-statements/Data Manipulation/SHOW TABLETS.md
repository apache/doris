---
{
    "title": "SHOW TABLETS",
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

# SHOW TABLETS
## description
    该语句用于显示 tablet 相关的信息（仅管理员使用）
    语法：
        SHOW TABLETS
        [FROM [db_name.]table_name] [partiton(partition_name_1, partition_name_1)]
        [where [version=1] [and backendid=10000] [and state="NORMAL|ROLLUP|CLONE|DECOMMISSION"]]
        [order by order_column]
        [limit [offset,]size]
    
    现在show tablets命令支持按照按照以下字段进行过滤：partition, index name, version, backendid,
    state，同时支持按照任意字段进行排序，并且提供limit限制返回条数。

## example
        // 显示指定 db 的下指定表所有 tablet 信息
        SHOW TABLETS FROM example_db.table_name;
    
        // 获取partition p1和p2的tablet信息
        SHOW TABLETS FROM example_db.table_name partition(p1, p2);
    
        // 获取10个结果
        SHOW TABLETS FROM example_db.table_name limit 10;
    
        // 从偏移5开始获取10个结果
        SHOW TABLETS FROM example_db.table_name limit 5,10;
    
        // 按照backendid/version/state字段进行过滤
        SHOW TABLETS FROM example_db.table_name where backendid=10000 and version=1 and state="NORMAL";
    
        // 按照version字段进行排序
        SHOW TABLETS FROM example_db.table_name where backendid=10000 order by version;
    
        // 获取index名字为t1_rollup的tablet相关信息
        SHOW TABLETS FROM example_db.table_name where indexname="t1_rollup";


## keyword
    SHOW,TABLETS,LIMIT

