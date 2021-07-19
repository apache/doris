---
{
    "title": "update",
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

If we need to modify or update the data in Doris, we can use the UPDATE command.

## Applicable scenarios

+ To modify the value of a row that meets certain conditions.
+ Point updates, small updates, where the rows to be updated are preferably a very small part of the entire table.
+ Only could be used in Unique table

## Explanation of terms

1. Unique model: A data model in the Doris system. When the user imports rows with the same Key, the Value of the latter overrides the existing Value, in the same sense as Unique in Mysql.

## Fundamentals

Use the query engine's own where filtering logic to filter the rows that need to be updated from the table to be updated. Then use the Unique model's own Value column replacement logic to change the rows to be updated and reinsert them into the table. This enables row-level updates.

### Example

Suppose there is an order table in Doris, where order id is the Key column, order status, and order amount are the Value columns. The data state is as follows.

| order id | order amount | order status |
|--|--|--|
| 1 | 100| Pending Payment |

At this time, after the user clicks the payment, Doris system needs to change the order id to '1' order status to 'pending shipment', you need to use the Update function.

```
UPDATE order SET order status='To be shipped' WHERE order id=1;
```

After the user executes the UPDATE command, the system performs the following three steps.

+ Step 1: Read the rows that satisfy WHERE order id=1
        (1, 100, 'pending payment')
+ Step 2: Change the order status of the row from 'Pending Payment' to 'Pending Shipping'
        (1, 100, 'Pending shipment')
+ Step 3: Insert the updated row back into the table to achieve the updated effect.
        | order id | order amount | order status |
        | ---| ---| ---|
        | 1 | 100| Pending Payment |
        | 1 | 100 | Pending shipments |
        Since the table order is a UNIQUE model, the rows with the same Key, after which the latter will take effect, so the final effect is as follows.
        | order id | order amount | order status |
        |--|--|--|
        | 1 | 100 | Pending shipments |

## Basic operations

### UPDATE syntax

```UPDATE table_name SET value=xxx WHERE condition;```

+ ``table_name``: the table to be updated, must be a UNIQUE model table to update.

+ value=xxx: The column to be updated, the left side of the equation must be the value column of the table. The right side of the equation can be a constant or an expression transformation of a column in a table.
        For example, if value = 1, then the value of the column to be updated will be 1.
        For example, if value = value + 1, the value of the column to be updated is incremented by 1.

+ condition: Only rows that satisfy the condition will be updated. condition must be an expression that results in a Boolean type.
        For example, if k1 = 1, only rows with a k1 column value of 1 will be updated.
        For example, if k1 = k2, only rows with the same value in column k1 as in column k2 will be updated.
        No support for unfilled condition, i.e., no support for full table updates.

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

## Risks of use

Since Doris currently supports row updates and uses a two-step read-and-write operation, there is uncertainty about the outcome of an Update statement if it modifies the same row as another Import or Delete statement.

Therefore, when using Doris, you must be careful to control the concurrency of Update statements and other DML statements on the *user side itself*.

## Version

Doris Version 0.15.x +
