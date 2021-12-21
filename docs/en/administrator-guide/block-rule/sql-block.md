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

Support SQL block rule by user level, by regex way to deny specify SQL.
This function is only used to limit the query statement, and does not limit the execution of the explain statement.

## Rule

SQL block rule CRUD
- create SQL block rule
    - sql：Regex pattern，Special characters need to be translated
    - sqlHash: Sql hash value, Used to match exactly, We print it in fe.audit.log，This parameter is the only choice between sql and sql, If you give a null value, it is an empty string not null
    - global: Whether global(all users)is in effect, false by default
    - enable：Whether to enable block rule，true by default
```sql
CREATE SQL_BLOCK_RULE test_rule 
PROPERTIES(
  "sql"="select * from order_analysis",
  "global"="false",
  "enable"="true",
  "sqlHash"=""
)
```
When we execute the sql that we defined in the rule just now, an exception error will be returned. An example is as follows:
```sql
mysql> select * from order_analysis;
ERROR 1064 (HY000): errCode = 2, detailMessage = sql match regex sql block rule: order_analysis_rule
```
- show configured SQL block rules, or show all rules if you do not specify a rule name

```sql
SHOW SQL_BLOCK_RULE [FOR RULE_NAME]
```
- alter SQL block rule，Allows changes sql/global/enable anyone
```sql
ALTER SQL_BLOCK_RULE test_rule PROPERTIES("sql"="select \\* from test_table","enable"="true")
```
- drop SQL block rule，Support multiple rules, separated by `,`
```sql
DROP SQL_BLOCK_RULE test_rule1,test_rule2
```

## User bind rules
If global=false is configured, the rules binding for the specified user needs to be configured, with multiple rules separated by ', '
```sql
SET PROPERTY [FOR 'jack'] 'sql_block_rules' = 'test_rule1,test_rule2'
```
