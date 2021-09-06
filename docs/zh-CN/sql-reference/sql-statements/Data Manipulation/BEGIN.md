---
{
    "title": "BEGIN",
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

# BEGIN, COMMIT, ROLLBACK
## Description
### Syntax

```
BEGIN;
INSERT INTO table_name ...
COMMIT;
```

```
BEGIN [ WITH LABEL label];
INSERT INTO table_name ...
ROLLBACK;
```
### Parameters

> label: 用于指定当前事务的标签名。

### Note

事务只能对insert使用，而不能对update和delete使用，当指定标签时，可通过以下命令检查事务的运行状态： `SHOW TRANSACTION WHERE LABEL = 'label'`

## example

1. 开启一个事务，不指定标签，执行insert后提交。

```
BEGIN
INSERT INTO test VALUES (1, 2);
INSERT INTO test (c1, c2) VALUES (1, 2);
INSERT INTO test (c1, c2) VALUES (1, DEFAULT);
INSERT INTO test (c1) VALUES (1);
COMMIT:
```

所有在`begin`和`commit`之间的数据会被插入到test表中。 

2. 开启一个事务，不指定标签，执行insert后，回滚。

```
BEGIN
INSERT INTO test VALUES (1, 2);
INSERT INTO test (c1, c2) VALUES (1, 2);
INSERT INTO test (c1, c2) VALUES (1, DEFAULT);
INSERT INTO test (c1) VALUES (1);
ROLLBACK:
```

所有在`begin`和`commit`之间的数据会取消，没有任何数据插入到test表中。 

3. 开启一个事务，指定标签为test_label1，执行insert后提交。

```
BEGIN WITH LABEL test_label1
INSERT INTO test VALUES (1, 2);
INSERT INTO test (c1, c2) VALUES (1, 2);
INSERT INTO test (c1, c2) VALUES (1, DEFAULT);
INSERT INTO test (c1) VALUES (1);
COMMIT:
```

所有在`begin`和`commit`之间的数据会被插入到test表中。 
标签`test_label1`用于标记该事务，可以通过以下命令来检查事务的状态：`SHOW TRANSACTION WHERE LABEL = 'test_label1'`。

## keyword
BEGIN, COMMIT, ROLLBACK
