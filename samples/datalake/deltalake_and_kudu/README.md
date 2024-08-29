<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Doris + DeltaLake + Kudu + MINIO Environments
Launch spark / doris / hive / deltalake / kudu /minio test environments, and give examples to query deltalake and kudu tables in Doris.

## Launch Docker Compose
**Create Network**
```shell
sudo docker network create -d bridge trinoconnector-net
```
**Launch all components in docker**
```shell
sudo sh start-trinoconnector-compose.sh
```
**Login into Spark**
```shell
sudo sh login-spark.sh
```
**Login into Doris**
```shell
sudo sh login-doris.sh
```

## Prepare DeltaLake Data
There's already a deltalake table named `customer` in default database.

## Create Catalog
The Doris Cluster has created two catalogs called `delta_lake` and `kudu_catalog`. You can view both of them by using the `SHOW CATALOGS` command or the `SHOW CREATE CATALOG ${catalog_name}` command after you log in to the Doris. Here are the creation statements for the two catalogs:

```sql
-- The catalog has been created, and no further action is required.
create catalog delta_lake properties (
  "type"="trino-connector",
  "trino.connector.name"="delta_lake",
  "trino.hive.metastore.uri"="thrift://hive-metastore:9083",
  "trino.hive.s3.endpoint"="http://minio:9000",
  "trino.hive.s3.region"="us-east-1",
  "trino.hive.s3.aws-access-key"="minio",
  "trino.hive.s3.aws-secret-key"="minio123",
  "trino.hive.s3.path-style-access"="true"
);

-- The catalog has been created, and no further action is required.
CREATE CATALOG `kudu_catalog` PROPERTIES (
    "type" = "trino-connector",
    "trino.connector.name" = "kudu",
    "trino.kudu.authentication.type" = "NONE",
    "trino.kudu.client.master-addresses" = "kudu-master-1:7051,kudu-master-2:7151,kudu-master-3:7251"
);
```

## Query Catalog Data
The data of `Delta Lake` and `Kudu` have been prepared in Doris Cluster. You can select these data directly in Doris.

- select Delta Lake data

```sql
mysql> switch delta_lake;
Query OK, 0 rows affected (0.00 sec)

mysql> use default;
Reading table information for completion of table and column names
You can turn off this feature to get a quicker startup with -A

Database changed
mysql> select * from customer limit 10;
+-----------+--------------------+------------------------------------+-------------+-----------------+-----------+--------------+---------------------------------------------------------------------------------------------------------------+
| c_custkey | c_name             | c_address                          | c_nationkey | c_phone         | c_acctbal | c_mktsegment | c_comment                                                                                                     |
+-----------+--------------------+------------------------------------+-------------+-----------------+-----------+--------------+---------------------------------------------------------------------------------------------------------------+
|         2 | Customer#000000002 | XSTf4,NCwDVaWNe6tEgvwfmRchLXak     |          13 | 23-768-687-3665 |    121.65 | AUTOMOBILE   | l accounts. blithely ironic theodolites integrate boldly: caref                                               |
|        34 | Customer#000000034 | Q6G9wZ6dnczmtOx509xgE,M2KV         |          15 | 25-344-968-5422 |   8589.70 | HOUSEHOLD    | nder against the even, pending accounts. even                                                                 |
|        66 | Customer#000000066 | XbsEqXH1ETbJYYtA1A                 |          22 | 32-213-373-5094 |    242.77 | HOUSEHOLD    | le slyly accounts. carefully silent packages benea                                                            |
|        98 | Customer#000000098 | 7yiheXNSpuEAwbswDW                 |          12 | 22-885-845-6889 |   -551.37 | BUILDING     | ages. furiously pending accounts are quickly carefully final foxes: busily pe                                 |
|       130 | Customer#000000130 | RKPx2OfZy0Vn 8wGWZ7F2EAvmMORl1k8iH |           9 | 19-190-993-9281 |   5073.58 | HOUSEHOLD    | ix slowly. express packages along the furiously ironic requests integrate daringly deposits. fur              |
|       162 | Customer#000000162 | JE398sXZt2QuKXfJd7poNpyQFLFtth     |           8 | 18-131-101-2267 |   6268.99 | MACHINERY    | accounts along the doggedly special asymptotes boost blithely during the quickly regular theodolites. slyly   |
|       194 | Customer#000000194 | mksKhdWuQ1pjbc4yffHp8rRmLOMcJ      |          16 | 26-597-636-3003 |   6696.49 | HOUSEHOLD    | quickly across the fluffily dogged requests. regular platelets around the ironic, even requests cajole quickl |
|       226 | Customer#000000226 | ToEmqB90fM TkLqyEgX8MJ8T8NkK       |           3 | 13-452-318-7709 |   9008.61 | AUTOMOBILE   | ic packages. ideas cajole furiously slyly special theodolites: carefully express pinto beans acco             |
|       258 | Customer#000000258 | 7VbADek8qYezQYotxNUmnNI            |          12 | 22-278-425-9944 |   6022.27 | MACHINERY    | about the regular, bold accounts; pending packages use furiously stealthy warhorses. bold accounts sleep fur  |
|       290 | Customer#000000290 | 8OlPT9G 8UqVXmVZNbmxVTPO8          |           4 | 14-458-625-5633 |   1811.35 | MACHINERY    | sts. blithely pending requests sleep fluffily on the regular excuses. carefully expre                         |
+-----------+--------------------+------------------------------------+-------------+-----------------+-----------+--------------+---------------------------------------------------------------------------------------------------------------+
10 rows in set (0.12 sec)
```

- select Kudu data

```sql
mysql> switch kudu_catalog;
Query OK, 0 rows affected (0.00 sec)

mysql> use default;
Reading table information for completion of table and column names
You can turn off this feature to get a quicker startup with -A

Database changed

mysql> select * from test_table limit 10;
+------+----------+--------+
| key  | value    | added  |
+------+----------+--------+
|    0 | NULL     | 12.345 |
|    4 | NULL     | 12.345 |
|   20 | NULL     | 12.345 |
|   26 | NULL     | 12.345 |
|   29 | value 29 | 12.345 |
|   42 | NULL     | 12.345 |
|   50 | NULL     | 12.345 |
|   56 | NULL     | 12.345 |
|   66 | NULL     | 12.345 |
|   74 | NULL     | 12.345 |
+------+----------+--------+
10 rows in set (1.49 sec)
```

- federation query

```sql
mysql> select * from delta_lake.`default`.customer c join kudu_catalog.`default`.test_table t on c.c_custkey = t.`key` where c.c_custkey < 50;
+-----------+--------------------+---------------------------------------+-------------+-----------------+-----------+--------------+--------------------------------------------------------------------------------------------------------+------+----------+--------+
| c_custkey | c_name             | c_address                             | c_nationkey | c_phone         | c_acctbal | c_mktsegment | c_comment                                                                                              | key  | value    | added  |
+-----------+--------------------+---------------------------------------+-------------+-----------------+-----------+--------------+--------------------------------------------------------------------------------------------------------+------+----------+--------+
|         1 | Customer#000000001 | IVhzIApeRb ot,c,E                     |          15 | 25-989-741-2988 |    711.56 | BUILDING     | to the even, regular platelets. regular, ironic epitaphs nag e                                         |    1 | value 1  | 12.345 |
|        33 | Customer#000000033 | qFSlMuLucBmx9xnn5ib2csWUweg D         |          17 | 27-375-391-1280 |    -78.56 | AUTOMOBILE   | s. slyly regular accounts are furiously. carefully pending requests                                    |   33 | value 33 | 12.345 |
|         3 | Customer#000000003 | MG9kdTD2WBHm                          |           1 | 11-719-748-3364 |   7498.12 | AUTOMOBILE   |  deposits eat slyly ironic, even instructions. express foxes detect slyly. blithely even accounts abov |    3 | value 3  | 12.345 |
|        35 | Customer#000000035 | TEjWGE4nBzJL2                         |          17 | 27-566-888-7431 |   1228.24 | HOUSEHOLD    | requests. special, express requests nag slyly furiousl                                                 |   35 | value 35 | 12.345 |
|         2 | Customer#000000002 | XSTf4,NCwDVaWNe6tEgvwfmRchLXak        |          13 | 23-768-687-3665 |    121.65 | AUTOMOBILE   | l accounts. blithely ironic theodolites integrate boldly: caref                                        |    2 | NULL     | 12.345 |
|        34 | Customer#000000034 | Q6G9wZ6dnczmtOx509xgE,M2KV            |          15 | 25-344-968-5422 |   8589.70 | HOUSEHOLD    | nder against the even, pending accounts. even                                                          |   34 | NULL     | 12.345 |
|        32 | Customer#000000032 | jD2xZzi UmId,DCtNBLXKj9q0Tlp2iQ6ZcO3J |          15 | 25-430-914-2194 |   3471.53 | BUILDING     | cial ideas. final, furious requests across the e                                                       |   32 | NULL     | 12.345 |
+-----------+--------------------+---------------------------------------+-------------+-----------------+-----------+--------------+--------------------------------------------------------------------------------------------------------+------+----------+--------+
7 rows in set (0.13 sec)
```
