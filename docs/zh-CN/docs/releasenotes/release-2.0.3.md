---
{
    "title": "Release 2.0.3",
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

亲爱的社区小伙伴们，Apache Doris 2.0.3  版本已于 2023 年 12 月 14 日正式发布，该版本对复杂数据类型、统计信息收集、倒排索引、数据湖分析、分布式副本管理等多个功能进行了优化，有 104 位贡献者为 Apache Doris 2.0.3 版本提交了超过 1000 个功能优化项以及问题修复，进一步提升了系统的稳定性和性能，欢迎大家下载体验。

**GitHub下载**：https://github.com/apache/doris/releases

**官网下载页**：https://doris.apache.org/download/


## 新增特性

### 自动统计信息收集

统计信息是 CBO 优化器进行代价估算时的依赖，通过收集统计信息有助于优化器了解数据分布特性、估算每个执行计划的成本并选择更优的执行计划，以此大幅提升查询效率。从 2.0.3 版本开始，Apache Doris 开始支持自动统计信息收集，默认为开启状态。

在每次导入事务提交后，Apache Doris 将记录导入事务更新的表信息并估算表统计信息的健康度，对于健康度低于配置参数的表会认为统计信息已过时并自动触发表的统计信息收集作业。同时为了降低统计信息作业的资源开销，Apache Doris 会自动采取采样的方式收集统计信息，用户也可以调整参数来采样更多行以获得更准确的数据分布信息。

更多信息请参考：https://doris.apache.org/docs/query-acceleration/statistics/


### 数据湖框架支持复杂数据类型

- Java UDF、JDBC catalog、Hudi MOR 表等功能支持复杂数据类型
  - https://github.com/apache/doris/pull/24810
  - https://github.com/apache/doris/pull/26236

- Paimon catalog 支持复杂数据类型
  - https://github.com/apache/doris/pull/25364

- Paimon catalog 支持 Paimon 0.5 版本
  - https://github.com/apache/doris/pull/24985


### 增加更多内置函数

- 新优化器支持 BitmapAgg 函数
  - https://github.com/apache/doris/pull/25508

- 支持 SHA 系列摘要函数
  - https://github.com/apache/doris/pull/24342 

- 聚合函数 min_by 和 max_by 支持 bitmap 数据类型
  - https://github.com/apache/doris/pull/25430 

- 增加 milliseconds/microseconds_add/sub/diff 函数
  - https://github.com/apache/doris/pull/24114

- 增加 json_insert, json_replace, json_set JSON 函数
  - https://github.com/apache/doris/pull/24384


## 改进优化

### 性能优化

- 在过滤率高的倒排索引 match where 条件和过滤率低的普通 where 条件组合时，大幅降低索引列的 IO
- 优化经过 where 条件过滤后随机读数据的效率
- 优化在 JSON 数据类型上使用老的 get_json_xx 函数的性能，提升 2-4 倍
- 支持配置降低读数据线程的优先级，保证写入的 CPU 资源和实时性
- 增加返回 largeint 的 uuid-numeric 函数，性能比返回 string 的 uuid 函数快 20 倍
- Case when 的性能提升 3 倍
- 在存储引擎执行中裁剪不必要的谓词计算
- 支持 count 算子下推到存储层
- 优化支持 and or 表达式中包含 nullable 类型的计算性能
- 支持更多场景下 limit 算子提前到 join 前执行的改写，以提升执行效率
- 增加消除 inline view 中的无用的 order by 算子，以提升执行效率
- 优化了部分情况下的基数估计和代价模型的准确性，以提升执行效率
- 优化了 JDBC catalog 的谓词下推逻辑和大小写逻辑
- 优化了 file cache 的第一次开启后的读取效率
- 优化 Hive 表 SQL cache 策略，使用 HMS 中存储的分区更新时间作为 cache 是否失效的判断，提高 cache 命中率
- 优化了 Merge-on-Write compaction 效率
- 优化了外表查询的线程分配逻辑，降低内存使用
- 优化 column reader 的内存使用


### 分布式副本管理改进

优化跳过删除分区、colocate group、持续写时均衡失败、冷热分层表不能均衡等；

### 安全性提升

- 审计日志插件的配置使用 token 代替明文密码以增强安全性
  - https://github.com/apache/doris/pull/26278

- log4j 配置安全性增强
  - https://github.com/apache/doris/pull/24861  

- 日志中不显示用户敏感信息
  - https://github.com/apache/doris/pull/26912


## Bugfix 和稳定性提升

### 复杂数据类型

- 修复了 map/struct 对定长 CHAR(n) 没有正确截断的问题
  - https://github.com/apache/doris/pull/25725

- 修复了 struct 嵌套 map/array 写入失败的问题
  - https://github.com/apache/doris/pull/26973

- 修复了 count distinct 不支持 array/map/struct 的问题
  - https://github.com/apache/doris/pull/25483

- 解决 query 中出现 delete 复杂类型之后，升级过程中出现 BE crash 的问题
  - https://github.com/apache/doris/pull/26006

- 修复了 jsonb 在 where 条件中 BE crash 问题
  - https://github.com/apache/doris/pull/27325

- 修复了 outer join 中有 array 类型时 BE crash 的问题
  - https://github.com/apache/doris/pull/25669

- 修复 orc 格式 decimal 类型读取错误的问题
  - https://github.com/apache/doris/pull/26548
  - https://github.com/apache/doris/pull/25977
  - https://github.com/apache/doris/pull/26633

### 倒排索引

- 修复了关闭倒排索引查询时 OR NOT 组合 where 条件结果错误的问题
  - https://github.com/apache/doris/pull/26327

- 修复了空数组的倒排索引写入时 BE crash 的问题
  - https://github.com/apache/doris/pull/25984

- 修复输出为空的情况下index compaction BE crash 的问题
  - https://github.com/apache/doris/pull/25486

- 修复新增列没有写入数据时，增加倒排索引 BE crash 的问题
  - https://github.com/apache/doris/pull/27276

- 修复 1.2 版本误建倒排索引后升级 2.0 等情况下倒排索引硬链缺失和泄露的问题
  - https://github.com/apache/doris/pull/26903

### 物化视图
- 修复 group by 语句中包括重复表达式导致 BE crash 的问题
  - https://github.com/apache/doris/pull/27523

- 禁止视图创建时 group by 子句中使用 float/doubld 类型
  - https://github.com/apache/doris/pull/25823

- 增强支持了 select 查询命中物化视图的功能
  - https://github.com/apache/doris/pull/24691 

- 修复当使用了表的 alias 时物化视图不能命中的问题
  - https://github.com/apache/doris/pull/25321

- 修复了创建物化视图中使用 percentile_approx 的问题
  - https://github.com/apache/doris/pull/26528

### 采样查询

- 修复 table sample 功能在 partition table 上无法正常工作的问题
  - https://github.com/apache/doris/pull/25912  

- 修复 table sample 指定 tablet 无法工作的问题
  - https://github.com/apache/doris/pull/25378 


### 主键表

- 修复基于主键条件更新的空指针异常
  - https://github.com/apache/doris/pull/26881 
   
- 修复部分列更新字段名大小写问题
  - https://github.com/apache/doris/pull/27223 

- 修复 schema change 时 mow 会出现重复 key 的问题
  - https://github.com/apache/doris/pull/25705


### 导入和 Compaction

- 修复 routine load 一流多表时 unkown slot descriptor 错误
  - https://github.com/apache/doris/pull/25762

- 修复内存统计并发访问导致 BE crash 问题
  - https://github.com/apache/doris/pull/27101 

- 修复重复取消导入导致 BE crash 的问题
  - https://github.com/apache/doris/pull/27111

- 修复 broker load 时 broker 连接报错问题
  - https://github.com/apache/doris/pull/26050

- 修复 compaction 和 scan 并发下 delete 谓词可能导致查询结果不对的问题
  - https://github.com/apache/doris/pull/24638

- 修复 compaction task 存在时打印大量 stacktrace 日志的问题
  - https://github.com/apache/doris/pull/25597


### 数据湖兼容性

- 解决 iceberg 表中包含特殊字符导致查询失败的问题
  - https://github.com/apache/doris/pull/27108

- 修复 Hive metastore 不同版本的兼容性问题
  - https://github.com/apache/doris/pull/27327

- 修复读取 MaxCompute 分区表错误的问题
  - https://github.com/apache/doris/pull/24911

- 修复备份到对象存储失败的问题
  - https://github.com/apache/doris/pull/25496
  - https://github.com/apache/doris/pull/25803


### JDBC 外表兼容性

- 修复 JDBC catalog 处理 Oracle 日期类型格式错误的问题
  - https://github.com/apache/doris/pull/25487 

- 修复 JDBC catalog 读取 MySQL 0000-00-00 日期异常的问题
  - https://github.com/apache/doris/pull/26569

- 修复从 MariaDB 读取数据时间类型默认值为 current_timestamp 时空指针异常问题
  - https://github.com/apache/doris/pull/25016

- 修复 JDBC catalog 处理 bitmap 类型时 BE crash 的问题
  - https://github.com/apache/doris/pull/25034
  - https://github.com/apache/doris/pull/26933


### SQL规划和优化

- 修复了部分场景下分区裁剪错误的问题
  - https://github.com/apache/doris/pull/27047
  - https://github.com/apache/doris/pull/26873
  - https://github.com/apache/doris/pull/25769
  - https://github.com/apache/doris/pull/27636

- 修复了部分场景下子查询处理不正确的问题
  - https://github.com/apache/doris/pull/26034
  - https://github.com/apache/doris/pull/25492
  - https://github.com/apache/doris/pull/25955
  - https://github.com/apache/doris/pull/27177

- 修复了部分语义解析的错误
  - https://github.com/apache/doris/pull/24928
  - https://github.com/apache/doris/pull/25627
  
- 修复 right outer/anti join 时，有可能丢失数据的问题
  - https://github.com/apache/doris/pull/26529
  
- 修复了谓词被错误的下推穿过聚合算子的问题
  - https://github.com/apache/doris/pull/25525
  
- 修正了部分情况下返回的结果 header 不正确的问题
  - https://github.com/apache/doris/pull/25372
  
- 包含有 nullsafeEquals 表达式(<=>)作为连接条件时，可以正确对规划出 hash join
  - https://github.com/apache/doris/pull/27127
  
- 修复了 set operation 算子中无法正确列裁剪的问题
  - https://github.com/apache/doris/pull/26884


## 行为变更

- 复杂数据类型 array/map/struct 的输出格式改成跟输入格式以及 JSON 规范保持一致，跟之前版本的主要变化是日期和字符串用双引号括起来，array/map 内部的空值显示为 null 而不是 NULL。
  - https://github.com/apache/doris/pull/25946

- 默认情况下，当用户属性 `resource_tags.location` 没有设置时，只能使用 default 资源组的节点，而之前版本中可以访问任意节点。
  - https://github.com/apache/doris/pull/25331 

- 支持 SHOW_VIEW 权限，拥有 SELECT 或 LOAD 权限的用户将不再能够执行 `SHOW CREATE VIEW` 语句，必须单独授予 SHOW_VIEW 权限。
  - https://github.com/apache/doris/pull/25370


