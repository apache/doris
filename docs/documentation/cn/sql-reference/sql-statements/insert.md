# insert

## Syntax

```
INSERT INTO table_name
    [ (column [, ...]) ]
    [ \[ hint [, ...] \] ]
    { VALUES ( { expression | DEFAULT } [, ...] ) [, ...] | query }
```

## Description

INSERT 向一张表里插入数据。用户可以通过 VALUES 语法插入一条或者多条数据，或者通过一个查询来插入0条或者多条数据。

column是目标列，可以以任意的顺序存在。如果没有指定目标列，那么默认值是这张表的所有列。

如果表中的某个列没有存在目标列中，那么这个列需要有默认值，否则 INSERT 就会执行失败。

如果表达式的类型与目标列的类型不一致，那么会调用隐式类型转化，如果不能够进行转化，那么 INSERT 语句会报语法解析错误。

## Parameters

> tablet_name: 导入数据的目的表。可以是 `db_name.table_name` 形式
> 
> column_name: 指定的目的列，必须是 `table_name` 中存在的列
> 
> expression: 需要赋值给某个列的对应表达式
> 
> DEFAULT: 让对应列使用默认值
> 
> query: 一个普通查询，查询的结果会写入到目标中
> 
> hint: 用于指示`INSERT`执行行为的一些指示符。`streaming`，用于指示使用同步方式来完成`INSERT`语句执行。

## Examples

`test` 表包含两个列`c1`, `c2`。

1. 向`test`表中导入一行数据

```
INSERT INTO test VALUES (1, 2);
INSERT INTO test (c1, c2) VALUES (1, 2);
INSERT INTO test (c1, c2) VALUES (1, DEFAULT);
INSERT INTO test (c1) VALUES (1);
```

其中第一条、第二条语句是一样的效果。在不指定目标列时，使用表中的列顺序来作为默认的目标列。
第三条、第四条语句表达的意思是一样的，使用`c2`列的默认值，来完成数据导入。

2. 向`test`表中一次性导入多行数据

```
INSERT INTO test VALUES (1, 2), (3, 2 + 2)
INSERT INTO test (c1, c2) VALUES (1, 2), (3, 2 * 2)
INSERT INTO test (c1) VALUES (1), (3)
INSERT INTO test (c1, c2) VALUES (1, DEFAULT), (3, DEFAULT)
```

其中第一条、第二条语句效果一样，向`test`表中一次性导入两条数据
第三条、第四条语句效果已知，使用`c2`列的默认值向`test`表中导入两条数据

3. 向`test`表中同步的导入一个查询语句的返回结果

```
INSERT INTO test [streaming] SELECT * FROM test2
INSERT INTO test (c1, c2) [streaming] SELECT * from test2
```

为了兼容的问题，默认的insert方式是异步完成的，效率比较差。如果需要使用效率比较高的导入方式，需要加上`[streaming]`来使用同步导入方式。

4. 向`test`表中异步的导入一个查询语句结果

```
INSERT INTO test SELECT * FROM test2
INSERT INTO test (c1, c2) SELECT * from test2
```

由于Doris之前的导入方式都是异步导入方式，这个导入语句会返回一个导入作业的`label`，用户需要通过`SHOW LOAD`命令查看此`label`导入作业的运行状况，当状态为`FINISHED`时，导入数据才生效。
