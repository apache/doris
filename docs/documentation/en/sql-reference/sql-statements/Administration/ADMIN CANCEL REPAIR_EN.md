# ADMIN CANCEL REPAIR
## Description

This statement is used to cancel repairing a specified table or partition with high priority

Grammar:

ADMIN CANCEL REPAIR TABLE table_name[ PARTITION (p1,...)];

Explain:

1. This statement only indicates that the system no longer repairs fragmented copies of specified tables or partitions with high priority. The system will still repair the copy by default scheduling.

## example

1. Cancel High Priority Repair

ADMIN CANCEL REPAIR TABLE tbl PARTITION(p1);

## keyword
ADMIN,CANCEL,REPAIR
