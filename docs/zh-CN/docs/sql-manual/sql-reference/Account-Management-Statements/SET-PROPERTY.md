---
{
    "title": "SET-PROPERTY",
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

## SET PROPERTY

### Name

SET PROPERTY

### Description

 设置用户的属性，包括分配给用户的资源、导入cluster等

```sql
SET PROPERTY [FOR 'user'] 'key' = 'value' [, 'key' = 'value']
```

这里设置的用户属性，是针对 user 的，而不是 user_identity。即假设通过 CREATE USER 语句创建了两个用户 'jack'@'%' 和 'jack'@'192.%'，则使用 SET PROPERTY 语句，只能针对 jack 这个用户，而不是 'jack'@'%' 或 'jack'@'192.%'

key:

超级用户权限:

​        max_user_connections: 最大连接数。

​        max_query_instances: 用户同一时间点执行查询可以使用的instance个数。

​        sql_block_rules: 设置 sql block rules。设置后，该用户发送的查询如果匹配规则，则会被拒绝。

​        cpu_resource_limit: 限制查询的cpu资源。详见会话变量 `cpu_resource_limit` 的介绍。-1 表示未设置。

​        exec_mem_limit: 限制查询的内存使用。详见会话变量 `exec_mem_limit` 的介绍。-1 表示未设置。

​        resource.cpu_share: cpu资源分配。（已废弃）

​        load_cluster.{cluster_name}.priority: 为指定的cluster分配优先级，可以为 HIGH 或 NORMAL

​        resource_tags：指定用户的资源标签权限。

​        query_timeout：指定用户的查询超时权限。

    注：`cpu_resource_limit`, `exec_mem_limit` 两个属性如果未设置，则默认使用会话变量中值。

普通用户权限：

​        quota.normal: normal级别的资源分配。

​        quota.high: high级别的资源分配。

​        quota.low: low级别的资源分配。

​        load_cluster.{cluster_name}.hadoop_palo_path: palo使用的hadoop目录，需要存放etl程序及etl生成的中间数据供Doris导入。导入完成后会自动清理中间

数据，etl程序自动保留下次使用。

​        load_cluster.{cluster_name}.hadoop_configs: hadoop的配置，其中fs.default.name、mapred.job.tracker、hadoop.job.ugi必须填写。

​        load_cluster.{cluster_name}.hadoop_http_port: hadoop hdfs name node http端口。其中 hdfs 默认为8070，afs 默认 8010。

​        default_load_cluster: 默认的导入cluster。

### Example

1. 修改用户 jack 最大连接数为1000
   
    ```sql
    SET PROPERTY FOR 'jack' 'max_user_connections' = '1000';
    ```
    
2. 修改用户 jack 的cpu_share为1000
   
    ```sql
    SET PROPERTY FOR 'jack' 'resource.cpu_share' = '1000';
    ```
    
3. 修改 jack 用户的normal组的权重
   
    ```sql
    SET PROPERTY FOR 'jack' 'quota.normal' = '400';
    ```
    
4. 为用户 jack 添加导入cluster
   
    ```sql
    SET PROPERTY FOR 'jack'
        'load_cluster.{cluster_name}.hadoop_palo_path' = '/user/doris/doris_path',
        'load_cluster.{cluster_name}.hadoop_configs' = 'fs.default.name=hdfs://dpp.cluster.com:port;mapred.job.tracker=dpp.cluster.com:port;hadoop.job.ugi=user,password;mapred.job.queue.name=job_queue_name_in_hadoop;mapred.job.priority=HIGH;';
    ```
    
5. 删除用户 jack 下的导入cluster。
   
    ```sql
    SET PROPERTY FOR 'jack' 'load_cluster.{cluster_name}' = '';
    ```
    
6. 修改用户 jack 默认的导入cluster
   
    ```sql
    SET PROPERTY FOR 'jack' 'default_load_cluster' = '{cluster_name}';
    ```
    
7. 修改用户 jack 的集群优先级为 HIGH
   
    ```sql
    SET PROPERTY FOR 'jack' 'load_cluster.{cluster_name}.priority' = 'HIGH';
    ```
    
8. 修改用户jack的查询可用instance个数为3000
   
    ```sql
    SET PROPERTY FOR 'jack' 'max_query_instances' = '3000';
    ```
    
9. 修改用户jack的sql block rule
   
    ```sql
    SET PROPERTY FOR 'jack' 'sql_block_rules' = 'rule1, rule2';
    ```
    
10. 修改用户jack的 cpu 使用限制
    
    ```sql
    SET PROPERTY FOR 'jack' 'cpu_resource_limit' = '2';
    ```
    
11. 修改用户的资源标签权限
    
    ```sql
    SET PROPERTY FOR 'jack' 'resource_tags.location' = 'group_a, group_b';
    ```
    
12. 修改用户的查询内存使用限制，单位字节
    
    ```sql
    SET PROPERTY FOR 'jack' 'exec_mem_limit' = '2147483648';
    ```

13. 修改用户的查询超时限制，单位秒

    ```sql
    SET PROPERTY FOR 'jack' 'query_timeout' = '500';
    ```
    
### Keywords

    SET, PROPERTY

### Best Practice

