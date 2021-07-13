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

Support SQL block rule by user level, by regex way to deny specify SQL

## Specific operation

SQL block rule CRUD
- create SQL block rule
    - user：For users whose rule is in effect, default means that all users are in effect. If both the specified user and the default rule are hit, the default rule takes precedence
    - sql：Regex pattern，Special characters need to be translated
    - sqlHash: Sql hash value, Used to match exactly, We print it in fe.audit.log 
    - enable：Whether to enable block rule
> CREATE SQL_BLOCK_RULE test_rule PROPERTIES("user"="default","sql"="select \\* from test_table","sqlHash":null,"enable"="true")
- show configured SQL block rules, or show all rules if you do not specify a rule name
> SHOW SQL_BLOCK_RULE [FOR RULE_NAME]
- alter SQL block rule，Allows changes user/sql/enable anyone
> ALTER SQL_BLOCK_RULE test_rule PROPERTIES("user"="default","sql"="select \\* from test_table","enable"="true")
- drop SQL block rule，Support multiple rules, separated by `,`
> DROP SQL_BLOCK_RULE test_rule1,test_rule2
