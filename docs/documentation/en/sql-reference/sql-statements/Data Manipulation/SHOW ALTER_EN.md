# SHOW ALTER
## Description
This statement is used to show the execution of various modification tasks currently under way.
Grammar:
SHOW ALTER [CLUSTER | TABLE [COLUMN | ROLLUP] [FROM db_name]];

Explain:
TABLE COLUMN：展示修改列的 ALTER 任务
TABLE ROLLUP: Shows the task of creating or deleting ROLLUP index
If db_name is not specified, use the current default DB
CLUSTER: Show the cluster operation related tasks (only administrators use! To be realized...

## example
1. Show the task execution of all modified columns of default DB
SHOW ALTER TABLE COLUMN;

2. Show the execution of tasks to create or delete ROLLUP index for specified DB
SHOW ALTER TABLE ROLLUP FROM example_db;

3. Show cluster operations related tasks (only administrators use! To be realized...
SHOW ALTER CLUSTER;

## keyword
SHOW,ALTER

