---
{
    "title": "Data Recover",
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

# Data Deletion Recovery

Data deletion recovery includes two situations：

1. After executing the command `drop database/table/partition`,  user can use command `recover` to recover all the data in the entire database/table/partition. It will restore the metadata of the database/table/partition from the FE's catalog recycle bin, change them from invisible to visible again, and the data will also be visible again;

2. Due to some misoperations or online bugs, some tablets on BEs are deleted, and these tablets can be rescued from the BE's trash through maintenance tools.

The above two, the former is aimed at the fact that the database/table/partition is no longer visible on FE, and the metadata of the database/table/partition is still kept in the catalog recycle bin of FE. The latter is aimed at databases/tables/partitions that are visible on FE, but some BE tablet data is deleted.

The two recovery methods are described below.

## Drop Recovery

In order to avoid disasters caused by misoperation, Doris supports data recovery of accidentally deleted databases/tables/partitions. After dropping table or database, Doris will not physically delete the data immediately, but will keep it in Trash for a period of time ( The default is 1 day, which can be configured through the `catalog_trash_expire_second` parameter in fe.conf). The administrator can use the RECOVER command to restore accidentally deleted data.

**Note that if it is deleted using `drop force`, it will be deleted directly and cannot be recovered.**

### Query Catalog Recycle Bin

Query FE catalog recycle bin

```sql
SHOW CATALOG RECYCLE BIN [ WHERE NAME [ = "name" | LIKE "name_matcher"] ]
```

For more detailed syntax and best practices, please refer to the [SHOW-CATALOG-RECYCLE-BIN](../../sql-manual/sql-reference/Show-Statements/SHOW-CATALOG-RECYCLE-BIN.md) command manual, You can also type `help SHOW CATALOG RECYCLE BIN` on the MySql client command line for more help.


### Start Data Recovery

1.restore the database named example_db

```sql
RECOVER DATABASE example_db;
```

2.restore the table named example_tbl

```sql
RECOVER TABLE example_db.example_tbl;
```

3.restore partition named p1 in table example_tbl

```sql
RECOVER PARTITION p1 FROM example_tbl;
```

For more detailed syntax and best practices used by RECOVER, please refer to the [RECOVER](../../sql-manual/sql-reference/Database-Administration-Statements/RECOVER.md) command manual, You can also type `HELP RECOVER` on the MySql client command line for more help.

## Tablet Restore Tool


### Restore data from BE Recycle Bin

During the user's use of Doris, some valid tablets (including metadata and data) may be deleted due to some misoperations or online bugs. In order to prevent data loss in these abnormal situations, Doris provides a recycle bin mechanism to protect user data. Tablet data deleted by users will not be deleted directly, but will be stored in the recycle bin for a period of time. After a period of time, there will be a regular cleaning mechanism to delete expired data. By default, when the disk space usage does not exceed 81% (BE `config.storage_flood_stage_usage_percent` * 0.9 * 100%), the data in the BE recycle bin is kept for up to 3 days (BE `config.trash_file_expire_time_sec`).

The data in the BE recycle bin includes: tablet data file (.dat), tablet index file (.idx) and tablet metadata file (.hdr). The data will be stored in a path in the following format:

```
/root_path/trash/time_label/tablet_id/schema_hash/
```

* `root_path`: a data root directory corresponding to the BE node.
* `trash`: The directory of the recycle bin.
* `time_label`: Time label, for the uniqueness of the data directory in the recycle bin, while recording the data time, use the time label as a subdirectory.

When a user finds that online data has been deleted by mistake, he needs to recover the deleted tablet from the recycle bin. This tablet data recovery function is needed.

BE provides http interface and `restore_tablet_tool.sh` script to achieve this function, and supports single tablet operation (single mode) and batch operation mode (batch mode).

* In single mode, data recovery of a single tablet is supported.
* In batch mode, support batch tablet data recovery.

In addition, users can use the command `show trash` to view the trash data capacity in BE, and use the command `admin clean trash` to clear the trash data in BE.

#### Operation

##### single mode

1. http request method

    BE provides an http interface for single tablet data recovery, the interface is as follows:
    
    ```
    curl -X POST "http://be_host:be_webserver_port/api/restore_tablet?tablet_id=11111\&schema_hash=12345"
    ```
    
    The successful results are as follows:
    
    ```
    {"status": "Success", "msg": "OK"}
    ```
    
    If it fails, the corresponding failure reason will be returned. One possible result is as follows:
    
    ```
    {"status": "Failed", "msg": "create link path failed"}
    ```

2. Script mode

    `restore_tablet_tool.sh` can be used to realize the function of single tablet data recovery.
    
    ```
    sh tools/restore_tablet_tool.sh -b "http://127.0.0.1:8040" -t 12345 -s 11111
    sh tools/restore_tablet_tool.sh --backend "http://127.0.0.1:8040" --tablet_id 12345 --schema_hash 11111
    ```

##### batch mode

The batch recovery mode is used to realize the function of recovering multiple tablet data.

When using, you need to put the restored tablet id and schema hash in a file in a comma-separated format in advance, one tablet per line.

The format is as follows:

```
12345,11111
12346,11111
12347,11111
```

Then perform the recovery with the following command (assuming the file name is: `tablets.txt`):

```
sh restore_tablet_tool.sh -b "http://127.0.0.1:8040" -f tablets.txt
sh restore_tablet_tool.sh --backend "http://127.0.0.1:8040" --file tablets.txt
```

### Repair missing or damaged Tablet

In some very special circumstances, such as code bugs, or human misoperation, etc., all replicas of some tablets may be lost. In this case, the data has been substantially lost. However, in some scenarios, the business still hopes to ensure that the query will not report errors even if there is data loss, and reduce the perception of the user layer. At this point, we can use the blank Tablet to fill the missing replica to ensure that the query can be executed normally.

**Note: This operation is only used to avoid the problem of error reporting due to the inability to find a queryable replica, and it is impossible to recover the data that has been substantially lost.**

1. View Master FE log `fe.log`

    If there is data loss, there will be a log similar to the following in the log:
    
    ```
    backend [10001] invalid situation. tablet[20000] has few replica[1], replica num setting is [3]
    ```

    This log indicates that all replicas of tablet 20000 have been damaged or lost.
    
2. Use blank replicas to fill in missing copies

    After confirming that the data cannot be recovered, you can execute the following command to generate blank replicas.
    
    ```
    ADMIN SET FRONTEND CONFIG ("recover_with_empty_tablet" = "true");
    ```

    * Note: You can first check whether the current version supports this parameter through the `ADMIN SHOW FRONTEND CONFIG;` command.

3. A few minutes after the setup is complete, you should see the following log in the Master FE log `fe.log`:

    ```
    tablet 20000 has only one replica 20001 on backend 10001 and it is lost. create an empty replica to recover it.
    ```

    The log indicates that the system has created a blank tablet to fill in the missing replica.
    
4. Judge whether it has been repaired successfully through query.
