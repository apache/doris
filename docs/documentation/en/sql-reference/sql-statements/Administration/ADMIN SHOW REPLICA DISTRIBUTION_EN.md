# ADMIN SHOW REPLICA DISTRIBUTION
## Description

This statement is used to show the distribution status of a table or partition replica

Grammar:

ADMIN SHOW REPLICA DISTRIBUTION FROM [db_name.]tbl_name [PARTITION (p1, ...)];

Explain:

The Graph column in the result shows the distribution ratio of replicas graphically

## example

1. View the distribution of replicas of tables

ADMIN SHOW REPLICA DISTRIBUTION FROM tbl1;

2. View the distribution of copies of partitions in the table

ADMIN SHOW REPLICA DISTRIBUTION FROM db1.tbl1 PARTITION(p1, p2);

## keyword
ADMIN,SHOW,REPLICA,DISTRIBUTION
