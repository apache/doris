---
{
    "title": "Update",
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

# Update

This article mainly describes how to use the UPDATE command to operate if we need to modify or update the data in Doris. The data update is limited to the version of Doris and can only be used in Doris **Version 0.15.x +**.

## Applicable scenarios

+ Modify its value for rows that meet certain conditions;
+ Point update, small range update, the row to be updated is preferably a very small part of the entire table;
+ The update command can only be executed on a table with a Unique data model.

## Fundamentals

Use the query engine's own where filtering logic to filter the rows that need to be updated from the table to be updated. Then use the Unique model's own Value column replacement logic to change the rows to be updated and reinsert them into the table. This enables row-level updates.

### Synchronization

The Update syntax is a synchronization syntax in Doris. If the Update statement succeeds, the update succeeds and the data is visible.

### Performance

The performance of the Update statement is closely related to the number of rows to be updated and the retrieval efficiency of the condition.

+ Number of rows to be updated: The more rows to be updated, the slower the Update statement will be. This is consistent with the principle of importing.
        Doris updates are more suitable for occasional update scenarios, such as changing the values of individual rows.
        Doris is not suitable for large batches of data changes. Large modifications can make Update statements take a long time to run.

+ Condition retrieval efficiency: Doris Update implements the principle of reading the rows that satisfy the condition first, so if the condition retrieval efficiency is high, the Update will be faster.
        The condition column should ideally be hit, indexed, or bucket clipped. This way Doris does not need to scan the entire table and can quickly locate the rows that need to be updated. This improves update efficiency.
        It is strongly discouraged to include the UNIQUE model value column in the condition column.

### Concurrency Control

By default, multiple concurrent Update operations on the same table are not allowed at the same time.

The main reason for this is that Doris currently supports row updates, which means that even if the user declares ``SET v2 = 1``, virtually all other Value columns will be overwritten (even though the values are not changed).

This presents a problem in that if two Update operations update the same row at the same time, the behavior may be indeterminate. That is, there may be dirty data.

However, in practice, the concurrency limit can be turned on manually if the user himself can guarantee that even if concurrent updates are performed, they will not operate on the same row at the same time. This is done by modifying the FE configuration ``enable_concurrent_update``. When the configuration value is true, there is no limit on concurrent updates.
> Note: After enabling the configuration, there will be certain performance risks. You can refer to the performance section above to improve update efficiency.

## Risks of Use

Since Doris currently supports row updates and uses a two-step read-and-write operation, there is uncertainty about the outcome of an Update statement if it modifies the same row as another Import or Delete statement.

Therefore, when using Doris, you must be careful to control the concurrency of Update statements and other DML statements on the **user side itself**.

## Usage Examples

Suppose there is an order table in Doris, where the order id is the Key column, the order status and the order amount are the Value column. The data status is as follows:

| order id | order amount | order status    |
| -------- | ------------ | --------------- |
| 1        | 100          | Pending Payment |

```sql
+----------+--------------+-----------------+
| order_id | order_amount | order_status    |
+----------+--------------+-----------------+
| 1        | 100          | Pending Payment |
+----------+--------------+-----------------+
1 row in set (0.01 sec)
```

At this time, after the user clicks to pay, the Doris system needs to change the status of the order with the order id '1' to 'Pending Shipping', and the Update function needs to be used.

```sql
mysql> UPDATE test_order SET order_status = 'Pending Shipping' WHERE order_id = 1;
Query OK, 1 row affected (0.11 sec)
{'label':'update_20ae22daf0354fe0-b5aceeaaddc666c5', 'status':'VISIBLE', 'txnId':'33', 'queryId':'20ae22daf0354fe0-b5aceeaaddc666c5'}
```

The result after the update is as follows

```sql
+----------+--------------+------------------+
| order_id | order_amount | order_status     |
+----------+--------------+------------------+
| 1        |          100 | Pending Shipping |
+----------+--------------+------------------+
1 row in set (0.01 sec)
```

After the user executes the UPDATE command, the system performs the following three steps.

 Step 1: Read the rows that satisfy WHERE order id=1 (1, 100, 'pending payment')
 Step 2: Change the order status of the row from 'Pending Payment' to 'Pending Shipping' (1, 100, 'Pending shipment')
 Step 3: Insert the updated row back into the table to achieve the updated effect. 

  | order id | order amount | order status | 
  | ---| ---| ---| 
  | 1 | 100| Pending Payment | 
  | 1 | 100 | Pending shipments | 

Since the table order is a UNIQUE model, the rows with the same Key, after which the latter will take effect, so the final effect is as follows. 

  | order id | order amount | order status | 
  |---|---|---| 
  | 1 | 100 | Pending shipments |

## Update Primary Key Column
Currently, the Update operation only supports updating the Value column, and the update of the Key column can refer to [Using FlinkCDC to update key column](../../ecosystem/flink-doris-connector.md#Use-FlinkCDC-to-update-Key-column)

## More Help

For more detailed syntax used by **data update**, please refer to the [update](../../sql-manual/sql-reference/Data-Manipulation-Statements/Manipulation/UPDATE.md) command manual , you can also enter `HELP UPDATE` in the Mysql client command line to get more help information.
