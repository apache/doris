"35; INSERT
Description
'35;'35;' 35; Syntax

```
INSERT INTO table_name
[PARTICIPATION [...]
[ (column [, ...]) ]
[ \[ hint [, ...] \] ]
{ VALUES ( { expression | DEFAULT } [, ...] ) [, ...] | query }
```

### Parameters

> tablet_name: Target table for importing data. It can be in the form of `db_name. table_name'.
>
> partition_names: Specifies that the partition to be imported must be a partition that exists in `table_name', with multiple partition names separated by commas
>
> column_name: The specified destination column must be a column that exists in `table_name'.
>
> expression: The corresponding expression that needs to be assigned to a column
>
> DEFAULT: Let the corresponding columns use default values
>
> query: A common query whose results are written to the target
>
> hint: Indicators used to indicate `INSERT'execution. ` Both streaming `and default non `streaming'methods use synchronization to complete `INSERT' statement execution
> The non `streaming'mode returns a label after execution to facilitate users to query the imported status through `SHOW LOAD'.

'35;'35;' 35; Note

When the `INSERT'statement is currently executed, the default behavior for data that does not conform to the target table is filtering, such as string length. However, for business scenarios where data is not filtered, the session variable `enable_insert_strict'can be set to `true' to ensure that `INSERT'will not be successfully executed when data is filtered out.

'35;'35; example

` The test `table contains two columns `c1', `c2'.

1. Import a row of data into the `test'table

```
INSERT INTO test VALUES (1, 2);
INSERT INTO test (c1, c2) VALUES (1, 2);
INSERT INTO test (c1, c2) VALUES (1, DEFAULT);
INSERT INTO test (c1) VALUES (1);
```

The first and second sentences have the same effect. When the target column is not specified, the column order in the table is used as the default target column.
The third and fourth statements express the same meaning, using the default value of `c2'column to complete data import.

2. Import multiline data into the `test'table at one time

```
INSERT INTO test VALUES (1, 2), (3, 2 + 2)
INSERT INTO test (c1, c2) VALUES (1, 2), (3, 2 * 2)
INSERT INTO test (c1) VALUES (1), (3)
Insert in test (C1, C2) values (1, Default), (3, Default)
```

The effect of the first and second statements is the same, and two data are imported into the `test'table at one time.
The effect of the third and fourth statements is known, using the default value of the `c2'column to import two data into the `test' table.

3. Return results of importing a query statement synchronously into the `test'table

```
INSERT INTO test [streaming] SELECT * FROM test2
INSERT INTO test (c1, c2) [streaming] SELECT * from test2
```

4. Import an asynchronous query result into the `test'table

```
INSERT INTO test SELECT * FROM test2
INSERT INTO test (c1, c2) SELECT * from test2
```

Asynchronous imports are, in fact, encapsulated asynchronously by a synchronous import. Filling in streaming is as efficient as not filling in * execution.

Since Doris used to import asynchronously, in order to be compatible with the old usage habits, the `INSERT'statement without streaming will still return a label. Users need to view the status of the `label' import job through the `SHOW LOAD command.
##keyword
INSERT
