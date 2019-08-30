# ALTER CLUSTER
## description

This statement is used to update the logical cluster. Administrator privileges are required

grammar

ALTER CLUSTER cluster_name PROPERTIES ("key"="value", ...);

1. Scaling, scaling (according to the number of be existing in the cluster, large is scaling, small is scaling), scaling for synchronous operation, scaling for asynchronous operation, through the state of backend can be known whether the scaling is completed.

Proerties ("Instrume = Unum"= "3")

Instancefn Microsoft Yahei

## example

1. Reduce the number of be of logical cluster test_cluster containing 3 be by 2.

ALTER CLUSTER test_cluster PROPERTIES ("instance_num"="2");

2. Expansion, increase the number of be of logical cluster test_cluster containing 3 be to 4

ALTER CLUSTER test_cluster PROPERTIES ("instance_num"="4");

## keyword
ALTER,CLUSTER
