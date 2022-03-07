---
{
    "title": "SHOW TRANSACTION",
    "language": "en"
}
---

<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements. See the NOTICE file
distributed with this work for additional information
regarding copyright ownership. The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied. See the License for the
specific language governing permissions and limitations
under the License.
-->

# SHOW TRANSACTION
## description

This syntax is used to view transaction details for the specified transaction id, label name or transaction status.

grammar:

```
SHOW TRANSACTION
[FROM db_name]
WHERE
[id = transaction_id]
[label = label_name]
[status = transaction_status];
```

Example return result:

```
     TransactionId: 4005
             Label: insert_8d807d5d-bcdd-46eb-be6d-3fa87aa4952d
       Coordinator: FE: 10.74.167.16
 TransactionStatus: VISIBLE
 LoadJobSourceType: INSERT_STREAMING
       PrepareTime: 2020-01-09 14:59:07
     PreCommitTime: 2020-01-09 14:59:07
        CommitTime: 2020-01-09 14:59:09
        FinishTime: 2020-01-09 14:59:09
            Reason:
ErrorReplicasCount: 0
        ListenerId: -1
         TimeoutMs: 300000
```

* TransactionId: transaction id
* Label: the label of the corresponding load job
* Coordinator: the node responsible for transaction coordination
* TransactionStatus: transaction status
    * PREPARE: preparation stage
    * PRECOMMITTED: The transaction was precommitted
    * COMMITTED: The transaction was successful, but the data is not visible
    * VISIBLE: The transaction was successful and the data is visible
    * ABORTED: transaction failed
* LoadJobSourceType: The type of the load job.
* PrepareTime: transaction start time
* PreCommitTime: the time when the transaction was precommitted
* CommitTime: the time when the transaction was successfully committed
* FinishTime: The time when the data is visible
* Reason: error message
* ErrorReplicasCount: Number of replicas with errors
* ListenerId: the id of the related load job
* TimeoutMs: transaction timeout time in milliseconds

## example

1. View the transaction with id 4005:

    SHOW TRANSACTION WHERE ID = 4005;

2. Specify the db and view the transaction with id 4005:

    SHOW TRANSACTION FROM db WHERE ID = 4005;

3. View the transaction with label `label_name`:

    SHOW TRANSACTION WHERE LABEL = 'label_name';
    
4. View the transactions with status `visible`:

   SHOW TRANSACTION WHERE STATUS = 'visible';

## keyword

    SHOW, TRANSACTION