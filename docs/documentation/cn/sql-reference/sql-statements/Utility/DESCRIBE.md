# DESCRIBE
## description
    该语句用于展示指定 table 的 schema 信息
    语法：
        DESC[RIBE] [db_name.]table_name [ALL];

    说明：
        如果指定 ALL，则显示该 table 的所有 index(rollup) 的 schema

## example

1. 显示Base表Schema

    DESC table_name;

2. 显示表所有 index 的 schema

    DESC db1.table_name ALL;

## keyword

    DESCRIBE,DESC
