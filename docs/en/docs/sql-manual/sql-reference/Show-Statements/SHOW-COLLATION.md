---
{
    "title": "SHOW-COLLATION",
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

## SHOW-COLLATION

### Description

In Doris, the `SHOW COLLATION` command is used to display the character set collations available in the database. A collation is a set of rules that determine how data is sorted and compared. These rules affect the storage and retrieval of character data. Doris currently mainly supports the proofreading method utf8_general_ci.

The `SHOW COLLATION` command returns the following fields:

* Collation: The collation name
* Charset: The character set
* Id: The collation's ID
* Default: Whether this is the default collation for the character set
* Compiled: Whether the collation is compiled
* Sortlen: Sort length

### Example

```sql
mysql> show collation;
+-----------------+---------+------+---------+----------+---------+
| Collation       | Charset | Id   | Default | Compiled | Sortlen |
+-----------------+---------+------+---------+----------+---------+
| utf8_general_ci | utf8    |   33 | Yes     | Yes      |       1 |
+-----------------+---------+------+---------+----------+---------+
```

### Keywords

    SHOW, COLLATION

### Best Practice

Use the `SHOW COLLATION` command to give you an idea of the collations available in the database and their properties. This information can help ensure that your character data is sorted and compared as expected. If you have problems comparing or sorting characters, it can be helpful to check your collation settings to make sure they are what you expect.
