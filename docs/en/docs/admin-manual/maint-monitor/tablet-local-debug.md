

During the online operation of Doris, various bugs may occur for various reasons, e.g., inconsistent replicas, version diffs in the data, etc. At this point, it is necessary to replicate the online tablet data to the local environment and then locate the problem.

In this case, you need to copy the online tablet copy data to the local environment for replication and then locate the problem.

## 1\. Prepare the environment

Deploy a single-node Doris cluster locally, with the same deployment version as the online cluster.

If the online deployment is DORIS-1.0.1, deploy DORIS-1.0.1 in the local environment as well.

After deploying the cluster, create a local table that is the same as the online one, but the changes that need to be made are that the local table has only one copy of the tablet, and the version, version_hash need to be specified.

If the problem is to locate inconsistent data, you can build three different tables corresponding to each copy online.

## 2\. Copy Data

To find the machine where the copy of online tablet is located, find the method, you can find the machine where the corresponding copy is located with the following two commands.

* show tablet meta data
```show tablet 10011```
```
mysql> show tablet 10011;
+-------------------------------+-----------+---------------+-----------+-------+---------+-------------+---------+--------+-------+------------------------------------------------------------+
| DbName                        | TableName | PartitionName | IndexName | DbId  | TableId | PartitionId | IndexId | IsSync | Order | DetailCmd                                                  |
+-------------------------------+-----------+---------------+-----------+-------+---------+-------------+---------+--------+-------+------------------------------------------------------------+
| default_cluster:test_query_qa | baseall   | baseall       | baseall   | 10007 | 10009   | 10008       | 10010   | true   | 0     | SHOW PROC '/dbs/10007/10009/partitions/10008/10010/10011'; |
+-------------------------------+-----------+---------------+-----------+-------+---------+-------------+---------+--------+-------+------------------------------------------------------------+
1 row in set (0.00 sec)
```
* Execute the `DetailCmd` command, locate the BE ip：

```SHOW PROC '/dbs/10007/10009/partitions/10008/10010/10011';```
```
mysql> SHOW PROC '/dbs/10007/10009/partitions/10008/10010/10011';
+-----------+-----------+---------+-------------------+------------------+---------------+------------+----------+----------+--------+-------+--------------+----------------------+-------------------------------------------------+---------------------------------------------------------------+
| ReplicaId | BackendId | Version | LstSuccessVersion | LstFailedVersion | LstFailedTime | SchemaHash | DataSize | RowCount | State  | IsBad | VersionCount | PathHash             | MetaUrl                                         | CompactionStatus                                              |
+-----------+-----------+---------+-------------------+------------------+---------------+------------+----------+----------+--------+-------+--------------+----------------------+-------------------------------------------------+---------------------------------------------------------------+
| 10012     | 10003     | 2       | 2                 | -1               | NULL          | 945014548  | 2195     | 5        | NORMAL | false | 2            | 844142863681807094   | http://192.168.0.2:8001/api/meta/header/10011 | http://192.168.0.2:8001/api/compaction/show?tablet_id=10011 |
| 10013     | 10002     | 2       | 2                 | -1               | NULL          | 945014548  | 2195     | 5        | NORMAL | false | 2            | -6740067817150249792 | http://192.168.0.1:8001/api/meta/header/10011  | http://192.168.0.1:8001/api/compaction/show?tablet_id=10011  |
| 10014     | 10005     | 2       | 2                 | -1               | NULL          | 945014548  | 2195     | 5        | NORMAL | false | 2            | 4758004238194195485  | http://192.168.0.3:8001/api/meta/header/10011  | http://192.168.0.3:8001/api/compaction/show?tablet_id=10011  |
+-----------+-----------+---------+-------------------+------------------+---------------+------------+----------+----------+--------+-------+--------------+----------------------+-------------------------------------------------+---------------------------------------------------------------+
3 rows in set (0.01 sec)
```
* Log in to the corresponding machine and find the directory where the replica is located.

```
ll ./data.HDD/data/0/10011/945014548/
total 4
-rw-rw-r-- 1 palo-qa palo-qa 2195 Jul 14 20:16 0200000000000018bb4a69226c414ace42487209dc145dbb_0.dat
```

Then replica the directory to the local counterpart with the scp command.

## 3\. Download meta data

Get the metadata of the replica

```
wget http://host:be_http_port/api/meta/header/$tablet_id?byte_to_base64=true -O meta_data
```

## 4\. Modify meta data

Take the metadata downloaded online and modify it to identify the online data copied in step 2.

In the same way download the tablet metadata from the local deployment cluster, and according to the correspondence, change the `table_id, tablet_id, partition_id, schema_hash, shard_id` in the metadata to the same case as local, other fields do not need to be changed.

## 5\. Take effect

(1)  Stop the local be in order to take effect the metadata.

(2)  Delete the metadata in the local be by the command, and take effect the new metadata at the same time.
```
./lib/meta_tool --root_path=/home/doris/be --operation=get_meta --tablet_id=10027 --schema_hash=112641656
./lib/meta_tool --root_path=/home/doris/be --operation=delete_meta --tablet_id=10027 --schema_hash=112641656
./lib/meta_tool --root_path=/home/doris/be --operation=load_meta --json_meta_path=/home/doris/error/tablet/112641656_1

```

(3) Restart be query the corresponding data.
