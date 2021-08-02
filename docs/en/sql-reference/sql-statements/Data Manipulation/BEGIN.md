---
{
    "title": "BEGIN",
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

> label: the label for this transaction, if you need to set it to a string.

### Note

A transaction can only be used on insert, nor update or delete. You can check the state of this transaction by `SHOW TRANSACTION WHERE LABEL = 'label'`

## example

1. Begin a transaction without a label, then commit it

```
BEGIN
INSERT INTO test VALUES (1, 2);
INSERT INTO test (c1, c2) VALUES (1, 2);
INSERT INTO test (c1, c2) VALUES (1, DEFAULT);
INSERT INTO test (c1) VALUES (1);
COMMIT:
```

All the data in the sql between `begin` and `commit` will be inserted into the table. 

2. Begin a transaction without a label, then abort it

```
BEGIN
INSERT INTO test VALUES (1, 2);
INSERT INTO test (c1, c2) VALUES (1, 2);
INSERT INTO test (c1, c2) VALUES (1, DEFAULT);
INSERT INTO test (c1) VALUES (1);
ROLLBACK:
```

All the data in the sql between `begin` and `rollback` will be aborted, nothing will be inserted into the table. 

3. Begin a transaction with a label, then commit it

```
BEGIN WITH LABEL test_label1
INSERT INTO test VALUES (1, 2);
INSERT INTO test (c1, c2) VALUES (1, 2);
INSERT INTO test (c1, c2) VALUES (1, DEFAULT);
INSERT INTO test (c1) VALUES (1);
COMMIT:
```

All the data in the sql between `begin` and `commit` will be inserted into the table.
The label of `test_label1` will be set to mark this transaction. You can check this transaction by `SHOW TRANSACTION WHERE LABEL = 'test_label1'`.

## keyword
BEGIN, COMMIT, ROLLBACK
