# MIGRATE DATABASE
## Description

This statement is used to migrate a logical cluster database to another logical cluster. Before performing this operation, the database must be in a link state and need to be managed.

Membership authority

grammar

MIGRATE DATABASE src u cluster name.src db name of the cluster name.des db name

## example

1. 迁移test_clusterA中的test_db到test_clusterB

MIGRATE DATABASE test_clusterA.test_db test_clusterB.link_test_db;

## keyword
MIGRATE,DATABASE
