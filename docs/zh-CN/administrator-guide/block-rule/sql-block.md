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

支持按用户配置SQL黑名单，通过正则匹配的方式拒绝指定SQL

## 具体操作

对SQL规则增删改查
- 创建SQL阻止规则
    - user：规则生效的用户，default代表所有用户都生效，如果同时命中指定用户和default的规则，default规则优先
    - sql：匹配规则(基于正则匹配,特殊字符需要转译)
    - sqlHash: sql hash值，用于完全匹配，我们会在`fe.audit.log`打印这个值
    - enable：是否开启阻止规则
> CREATE SQL_BLOCK_RULE test_rule PROPERTIES("user"="default","sql"="select \\* from test_table","sqlHash":null,"enable"="true")
- 查看已配置的SQL阻止规则，不指定规则名则为查看所有规则
> SHOW SQL_BLOCK_RULE [FOR RULE_NAME]
- 修改SQL阻止规则，允许对user/sql/enable等每一项进行修改
> ALTER SQL_BLOCK_RULE test_rule PROPERTIES("user"="default","sql"="select \\* from test_table","enable"="true")
- 删除SQL阻止规则，支持多规则，以`,`隔开
> DROP SQL_BLOCK_RULE test_rule1,test_rule2
