---
{
    "title": "Sql Interception",
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

This function is used to limit any sql statement (no matter DDL or DML statement).
Support SQL block rule by user level:

1. by regex way to deny specify SQL

2. by setting partition_num, tablet_num, cardinality, check whether a sql reaches one of the limitations
    - partition_num, tablet_num, cardinality could be set together, and once reach one of them, the sql will be blocked.

## Rule

SQL block rule CRUD
- create SQL block rule,For more creation syntax see[CREATE SQL BLOCK RULE](../sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-SQL-BLOCK-RULE.md)
    - sql：Regex pattern，Special characters need to be translated, "NULL" by default
    - sqlHash: Sql hash value, Used to match exactly, We print it in fe.audit.log, This parameter is the only choice between sql and sql, "NULL" by default
    - partition_num: Max number of partitions will be scanned by a scan node, 0L by default
    - tablet_num: Max number of tablets will be scanned by a scan node, 0L by default
    - cardinality: An inaccurate number of scan rows of a scan node, 0L by default
    - global: Whether global(all users)is in effect, false by default
    - enable：Whether to enable block rule，true by default
```sql
CREATE SQL_BLOCK_RULE test_rule 
PROPERTIES(
  "sql"="select \\* from order_analysis",
  "global"="false",
  "enable"="true",
  "sqlHash"=""
)
```
> Notes:
>
> That the sql statement here does not end with a semicolon

When we execute the sql that we defined in the rule just now, an exception error will be returned. An example is as follows:

```sql
mysql> select * from order_analysis;
ERROR 1064 (HY000): errCode = 2, detailMessage = sql match regex sql block rule: order_analysis_rule
```

- create test_rule2, limits the maximum number of scanning partitions to 30 and the maximum scanning cardinality to 10 billion rows. As shown in the following example:
```sql
CREATE SQL_BLOCK_RULE test_rule2 PROPERTIES("partition_num" = "30", "cardinality"="10000000000","global"="false","enable"="true")
```

- show configured SQL block rules, or show all rules if you do not specify a rule name,Please see the specific grammar [SHOW SQL BLOCK RULE](../sql-manual/sql-reference/Show-Statements/SHOW-SQL-BLOCK-RULE.md)

```sql
SHOW SQL_BLOCK_RULE [FOR RULE_NAME]
```
- alter SQL block rule，Allows changes sql/sqlHash/global/enable/partition_num/tablet_num/cardinality anyone,Please see the specific grammar[ALTER SQL BLOCK  RULE](../sql-manual/sql-reference/Data-Definition-Statements/Alter/ALTER-SQL-BLOCK-RULE.md)
    - sql and sqlHash cannot be set both. It means if sql or sqlHash is set in a rule, another property will never be allowed to be altered
    - sql/sqlHash and partition_num/tablet_num/cardinality cannot be set together. For example, partition_num is set in a rule, then sql or sqlHash will never be allowed to be altered.
```sql
ALTER SQL_BLOCK_RULE test_rule PROPERTIES("sql"="select \\* from test_table","enable"="true")
```

```
ALTER SQL_BLOCK_RULE test_rule2 PROPERTIES("partition_num" = "10","tablet_num"="300","enable"="true")
```

- drop SQL block rule，Support multiple rules, separated by `,`,Please see the specific grammar[DROP SQL BLOCK RULE](../sql-manual/sql-reference/Data-Definition-Statements/Drop/DROP-SQL-BLOCK-RULE.md)
```sql
DROP SQL_BLOCK_RULE test_rule1,test_rule2
```

## User bind rules
If global=false is configured, the rules binding for the specified user needs to be configured, with multiple rules separated by ', '
```sql
SET PROPERTY [FOR 'jack'] 'sql_block_rules' = 'test_rule1,test_rule2'
```

