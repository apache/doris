# ADMIN REPAIR
## Description

This statement is used to try to fix the specified table or partition first

Grammar:

ADMIN REPAIR TABLE table_name[ PARTITION (p1,...)]

Explain:

1. This statement only means that the system attempts to repair a fragmented copy of a specified table or partition with high priority, and it is not guaranteed to be successful. Users can view the repair status through the ADMIN SHOW REPLICA STATUS command.
2. The default timeout is 14400 seconds (4 hours). Timeout means that the system will no longer repair fragmented copies of specified tables or partitions with high priority. The command settings need to be reused.

## example

1. Attempt to fix the specified table

ADMIN REPAIR TABLE tbl1;

2. Attempt to fix the specified partition

ADMIN REPAIR TABLE tbl1 PARTITION (p1, p2);

## keyword
ADMIN,REPAIR
