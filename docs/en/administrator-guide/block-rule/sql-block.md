---
{
"title": "SQL Block Rule",
"language": "en"
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

# SQL Block Rule

Support SQL block rule by user level:

1. by regex way to deny specify SQL

2. by setting partitionNum, tabletNum, cardinality, check whether a sql reaches one of the limitations
   - partitionNum, tabletNum, cardinality could be set together, and once reach one of them, the sql will be blocked.

## Rule

SQL block rule CRUD
- create SQL block rule
    - sql：Regex pattern，Special characters need to be translated, "NULL" by default
    - sqlHash: Sql hash value, Used to match exactly, We print it in fe.audit.log, "NULL" by default
    - partitionNum: Max number of partitions will be scanned by a scan node, 0L by default
    - tabletNum: Max number of tablets will be scanned by a scan node, 0L by default
    - cardinality: An inaccurate number of scan rows of a scan node, 0L by default
    - global: Whether global(all users)is in effect, false by default
    - enable：Whether to enable block rule，true by default
```
CREATE SQL_BLOCK_RULE test_rule PROPERTIES("sql"="select \\* from test_table","sqlHash":null,"global"="false","enable"="true")
```

```
CREATE SQL_BLOCK_RULE test_rule2 PROPERTIES("partitionNum" = "30", "cardinality"="10000000000","global"="false","enable"="true")
```

- show configured SQL block rules, or show all rules if you do not specify a rule name
```
SHOW SQL_BLOCK_RULE [FOR RULE_NAME]
```
- alter SQL block rule，Allows changes sql/sqlHash/global/enable/partitionNum/tabletNum/cardinality anyone
  - sql and sqlHash cannot be set both. It means if sql or sqlHash is set in a rule, another property will never be allowed to be altered
  - sql/sqlHash and partitionNum/tabletNum/cardinality cannot be set together. For example, partitionNum is set in a rule, then sql or sqlHash will never be allowed to be altered.
```
ALTER SQL_BLOCK_RULE test_rule PROPERTIES("sql"="select \\* from test_table","enable"="true")
```

```
ALTER SQL_BLOCK_RULE test_rule2 PROPERTIES("partitionNum" = "10","tabletNum"="300","enable"="true")
```

- drop SQL block rule，Support multiple rules, separated by `,`
```
DROP SQL_BLOCK_RULE test_rule1,test_rule2
```

## User bind rules
If global=false is configured, the rules binding for the specified user needs to be configured, with multiple rules separated by ', '
```
SET PROPERTY [FOR 'jack'] 'sql_block_rules' = 'test_rule1,test_rule2'
```
