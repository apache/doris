# LINK DATABASE
## Description

This statement allows users to link a database of one logical cluster to another logical cluster. A database is only allowed to be linked once at the same time and the linked database is deleted.

It does not delete data, and the linked database cannot be deleted. Administrator privileges are required.

grammar

LINK DATABASE src u cluster name.src db name of the cluster name.des db name

## example

1. Link test_db in test_cluster A to test_cluster B and name it link_test_db

LINK DATABASE test_clusterA.test_db test_clusterB.link_test_db;

2. Delete linked database link_test_db

DROP DATABASE link_test_db;

## keyword
LINK,DATABASE
