---
{
    "title": "SHOW LAST INSERT",
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

# SHOW LAST INSERT
## description

This syntax is used to view the result of the last insert operation in the current session connection.

Syntax:

```
SHOW LAST INSERT
```

Example of return result.

```
    TransactionId: 64067
            Label: insert_ba8f33aea9544866-8ed77e2844d0cc9b
         Database: default_cluster:db1
            Table: t1
TransactionStatus: VISIBLE
       LoadedRows: 2
     FilteredRows: 0
```

* TransactionId: transaction id
* Label: label corresponding to the insert task
* Database: the database corresponding to the insert
* Table: the table corresponding to the insert
* TransactionStatus: the status of the transaction
    * PREPARE: preparation phase
    * PRECOMMITTED: Pre-committed stage
    * COMMITTED: The transaction is successful, but the data is not visible
    * VISIBLE: The transaction is successful and the data is visible
    * ABORTED: The transaction failed.
* LoadedRows: Number of rows imported
* FilteredRows: number of rows filtered

## example

## keyword

    SHOW, LAST, INSERT
