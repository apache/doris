---
{
    "title": "Doris Error Table",
    "language": "en"
}

---

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

# Doris Error Table

| Error  Code | Info                                                         |
| :--------- | :----------------------------------------------------------- |
| 1005       | Failed to create the table, give the specific reason in the returned error message |
| 1007       | The database already exists, you cannot create a database with the same name |
| 1008       | The database does not exist and cannot be deleted |
| 1044       | The database is not authorized to the user and cannot be accessed |
| 1045       | The user name and password do not match, and the system cannot be accessed |
| 1046       | The target database to be queried is not specified |
| 1047       | The user entered an invalid operation instruction |
| 1049       | The user specified an invalid database |
| 1050       | Data table already exists                      |
| 1051       | Invalid data table                               |
| 1052       | The specified column name is ambiguous, and the corresponding column cannot be uniquely determined |
| 1053       | Illegal data column is specified for Semi-Join/Anti-Join query |
| 1054       | The specified column does not exist in the table |
| 1058       | The number of columns selected in the query statement is inconsistent with the number of columns in the query result |
| 1060       | Duplicate column name                                |
| 1064       | No alive Backend node               |
| 1066       | A duplicate table alias appears in the query statement       |
| 1094       | Invalid thread ID                                  |
| 1095       | The owner of a non-thread cannot terminate the running of the thread |
| 1096       | The query statement does not specify the data table to be queried or operated |
| 1102       | Incorrect database name                        |
| 1104       | Incorrect data table name                      |
| 1105       | Other errors                                         |
| 1110       | Duplicate columns specified in the subquery |
| 1111       | Illegal use of aggregate functions in the Where clause |
| 1113       | The column set of the newly created table cannot be empty |
| 1115       | Unsupported character set used           |
| 1130       | The client used an unauthorized IP address to access the system |
| 1132       | No permission to modify user password      |
| 1141       | When revoking user permissions, the permissions that the user does not have are specified |
| 1142       | The user performed an unauthorized operation |
| 1166       | Incorrect column name                              |
| 1193       | Invalid system variable name used      |
| 1203       | The number of active connections used by the user exceeds the limit |
| 1211       | Not allowed to create new users              |
| 1227       | Access denied, the user performed an unauthorized operation |
| 1228       | Session variables cannot be modified by the SET GLOBAL command |
| 1229       | Global variables should be modified by the SET GLOBAL instruction |
| 1230       | Related system variables have no default values |
| 1231       | An invalid value is set to a system variable |
| 1232       | A value of the wrong data type is set to a system variable |
| 1248       | No alias is set for the inline view    |
| 1251       | The client does not support the authentication protocol requested by the server; please upgrade the MySQL client |
| 1286       | The configured storage engine is incorrect |
| 1298       | The configured time zone is incorrect        |
| 1347       | The object does not match the expected type |
| 1353       | SELECT and view field lists have different number of columns |
| 1364       | The field does not allow NULL values, but no default value is set |
| 1372       | Password length is not enough                    |
| 1396       | The operation performed by the user failed to run |
| 1471       | The specified table does not allow data to be inserted |
| 1507       | Delete a partition that does not exist, and no condition is specified to delete it if it exists |
| 1508       | Unable to delete all partitions, please use DROP TABLE instead |
| 1517       | Duplicate partition name appeared        |
| 1567       | The partition name is incorrect              |
| 1621       | The specified system variable is read-only |
| 1735       | The specified partition name does not exist in the table |
| 1748       | You cannot insert data into a table with empty partitions. Use "SHOW PARTITIONS FROM tbl" to view the current partition of this table |
| 1749       | Table partition does not exist                   |
| 5000       | The specified table is not an OLAP table   |
| 5001       | The specified PROC path is invalid         |
| 5002       | The column name must be explicitly specified in the column substitution |
| 5003       | The Key column should be sorted before the Value column |
| 5004       | The table should contain at least 1 Key column |
| 5005       | Invalid cluster ID                                 |
| 5006       | Invalid query plan                             |
| 5007       | Conflicting query plan                         |
| 5008       | Data insertion tips: only applicable to partitioned data tables |
| 5009       | The PARTITION clause is invalid for INSERT into an unpartitioned table |
| 5010       | The number of columns is not equal to the number of select lists in the SELECT statement |
| 5011       | Unable to resolve table reference              |
| 5012       | The specified value is not a valid number |
| 5013       | Unsupported time unit                        |
| 5014       | Table status is abnormal                         |
| 5015       | Partition status is abnormal                   |
| 5016       | There is a data import task on the partition |
| 5017       | The specified column is not a key column      |
| 5018       | Invalid value format                             |
| 5019       | Data copy does not match the version     |
| 5021       | BE node is offline                               |
| 5022       | The number of partitions in a non-partitioned table is not 1 |
| 5023       | Nothing in the alter statement          |
| 5024       | Task execution timeout                           |
| 5025       | Data insertion operation failed              |
| 5026       | An unsupported data type was used when creating a table with a SELECT statement |
| 5027       | The specified parameter is not set         |
| 5028       | The specified cluster was not found        |
| 5030       | A user does not have permission to access the cluster |
| 5031       | No parameter specified or invalid parameter |
| 5032       | The number of cluster instances is not specified |
| 5034       | Cluster name already exists                    |
| 5035       | Cluster already exists                           |
| 5036       | Insufficient BE nodes in the cluster         |
| 5037       | Before deleting the cluster, all databases in the cluster must be deleted |
| 5037       | The BE node with this ID does not exist in the cluster |
| 5038       | No cluster name specified                    |
| 5040       | Unknown cluster                                    |
| 5041       | No cluster name                                  |
| 5042       | Permission denied                                    |
| 5043       | The number of instances should be greater than 0 |
| 5046       | The source cluster does not exist                |
| 5047       | The target cluster does not exist              |
| 5048       | The source database does not exist             |
| 5049       | The target database does not exist           |
| 5050       | No cluster selected, please enter the cluster |
| 5051       | The source database should be connected to the target database first |
| 5052       | Cluster internal error: BE node error message |
| 5053       | There is no migration task from the source database to the target database |
| 5054       | The specified database is connected to the target database, or data is being migrated |
| 5055       | Data connection or data migration cannot be performed in the same cluster |
| 5056       | The database cannot be deleted: it is linked to another database or data is being migrated |
| 5056       | The database cannot be renamed: it is linked to another database or data is being migrated |
| 5056       | Insufficient BE nodes in the cluster         |
| 5056       | The specified number of BE nodes already exist in the cluster |
| 5059       | There are BE nodes that are offline in the cluster |
| 5062       | Incorrect cluster name (the name'default_cluster' is a reserved name) |
| 5063       | Type name is incorrect                           |
| 5064       | General error message                            |
| 5063       | Colocate function has been disabled by the administrator |
| 5063       | Colocate data table does not exist      |
| 5063       | The Colocate table must be an OLAP table |
| 5063       | Colocate table should have the same number of copies |
| 5063       | Colocate table should have the same number of buckets |
| 5063       | The number of partition columns of the Colocate table must be the same |
| 5063       | The data types of the partition columns of the Colocate table must be consistent |
| 5064       | The specified table is not a colocate table |
| 5065       | The specified operation is invalid         |
| 5065       | The specified time unit is illegal. The correct units include: HOUR / DAY / WEEK / MONTH |
| 5066       | The starting value of the dynamic partition should be less than 0 |
| 5066       | The dynamic partition start value is not a valid number |
| 5066       | The end value of the dynamic partition should be greater than 0 |
| 5066       | The end value of the dynamic partition is not a valid number |
| 5066       | The end value of the dynamic partition is empty |
| 5067       | The number of dynamic partition buckets should be greater than 0 |
| 5067       | The dynamic partition bucket value is not a valid number |
| 5066       | Dynamic partition bucket value is empty    |
| 5068       | Whether to allow the value of dynamic partition is not a valid Boolean value: true or false |
| 5069       | The specified dynamic partition name prefix is illegal |
| 5070       | The specified operation is forbidden                         |
| 5071       | The number of dynamic partition replicas should be greater than 0 |
| 5072       | The dynamic partition copy value is not a valid number |
| 5073       | The original created table stmt is empty   |
| 5074       | Create historical dynamic partition parameters: create_history_partition is invalid, what is expected is: true or false |
| 5076       | The specified dynamic partition reserved_history_periods is null or empty                          |
| 5077       | The specified dynamic partition reserved_history_periods is invalid                           |
| 5078       | The length of specified dynamic partition reserved_history_periods must have pairs of date value              |
| 5079       | The specified dynamic partition reserved_history_periods' first date is larger than the second one           |
