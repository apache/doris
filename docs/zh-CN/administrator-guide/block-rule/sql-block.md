---
{
"title": "SQL黑名单",
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

# SQL黑名单

支持按用户配置SQL黑名单:

1. 通过正则匹配的方式拒绝指定SQL

2. 通过设置partitionNum, tabletNum, cardinality, 检查一个查询是否达到其中一个限制
  - partitionNum, tabletNum, cardinality 可以一起设置，一旦一个查询达到其中一个限制，查询将会被拦截

## 规则

对SQL规则增删改查
- 创建SQL阻止规则
    - sql：匹配规则(基于正则匹配,特殊字符需要转译)，可选，默认值为 "NULL"
    - sqlHash: sql hash值，用于完全匹配，我们会在`fe.audit.log`打印这个值，可选，默认值为 "NULL"
    - partitionNum: 一个扫描节点会扫描的最大partition数量，默认值为0L
    - tabletNum: 一个扫描节点会扫描的最大扽tablet数量，默认值为0L
    - cardinality: 一个扫描节点粗略的扫描行数，默认值为0L
    - global：是否全局(所有用户)生效，默认为false  
    - enable：是否开启阻止规则，默认为true
```
CREATE SQL_BLOCK_RULE test_rule PROPERTIES("sql"="select \\* from test_table","sqlHash":null,"enable"="true")
```

```
CREATE SQL_BLOCK_RULE test_rule2 PROPERTIES("partitionNum" = "30", "cardinality"="10000000000","global"="false","enable"="true")
```

- 查看已配置的SQL阻止规则，不指定规则名则为查看所有规则
```
SHOW SQL_BLOCK_RULE [FOR RULE_NAME]
```
- 修改SQL阻止规则，允许对sql/sqlHash/partitionNum/tabletNum/cardinality/global/enable等每一项进行修改
  - sql 和 sqlHash 不能同时被设置。这意味着，如果一个rule设置了sql或者sqlHash，则另一个属性将无法被修改
  - sql/sqlHash 和 partitionNum/tabletNum/cardinality 不能同时被设置。举个例子，如果一个rule设置了partitionNum，那么sql或者sqlHash将无法被修改
```
ALTER SQL_BLOCK_RULE test_rule PROPERTIES("sql"="select \\* from test_table","enable"="true")
```

```
ALTER SQL_BLOCK_RULE test_rule2 PROPERTIES("partitionNum" = "10","tabletNum"="300","enable"="true")
```

- 删除SQL阻止规则，支持多规则，以`,`隔开
```
DROP SQL_BLOCK_RULE test_rule1,test_rule2
```

## 用户规则绑定
如果配置global=false，则需要配置指定用户的规则绑定，多个规则使用`,`分隔
```
SET PROPERTY [FOR 'jack'] 'sql_block_rules' = 'test_rule1,test_rule2'
```