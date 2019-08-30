# CREATE CLUSTER
## Description

This statement is used to create a new logical cluster, requiring administrator privileges. If you don't use multiple tenants, create a cluster named default_cluster directly. Otherwise, create a cluster with a custom name.

grammar

CREATE CLUSTER [IF NOT EXISTS] cluster_name

PROPERTIES ("key"="value", ...)

IDENTIFIED BY 'password'

1. PROPERTIES

Specify attributes of logical clusters

PROPERTIES ("instance_num" = "3")

Instancefn Microsoft Yahei

2. Identify by'password'each logical cluster contains a superuser whose password must be specified when creating a logical cluster

## example

1. Create a new test_cluster with three be nodes and specify its superuser password

CREATE CLUSTER test_cluster PROPERTIES("instance_num"="3") IDENTIFIED BY 'test';

2. Create a new default_cluster with three be nodes (no multi-tenant is used) and specify its superuser password

CREATE CLUSTER default_cluster PROPERTIES("instance_num"="3") IDENTIFIED BY 'test';

## keyword
CREATE,CLUSTER
