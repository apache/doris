---
{
    "title": "Release 1.2.0",
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



# Feature
## Highlight

1. Full Vectorizied-Engine support, greatly improved performance

	In the standard ssb-100-flat benchmark, the performance of 1.2 is 2 times faster than that of 1.1; in complex TPCH 100 benchmark, the performance of 1.2 is 3 times faster than that of 1.1.

2. Merge-on-Write Unique Key

	Support Merge-On-Write on Unique Key Model. This mode marks the data that needs to be deleted or updated when the data is written, thereby avoiding the overhead of Merge-On-Read when querying, and greatly improving the reading efficiency on the updateable data model.

3. Multi Catalog

	The multi-catalog feature provides Doris with the ability to quickly access external data sources for access. Users can connect to external data sources through the `CREATE CATALOG` command. Doris will automatically map the library and table information of external data sources. After that, users can access the data in these external data sources just like accessing ordinary tables. It avoids the complicated operation that the user needs to manually establish external mapping for each table.
    
    Currently this feature supports the following data sources:
    
    1. Hive Metastore: You can access data tables including Hive, Iceberg, and Hudi. It can also be connected to data sources compatible with Hive Metastore, such as Alibaba Cloud's DataLake Formation. Supports data access on both HDFS and object storage.
    2. Elasticsearch: Access ES data sources.
    3. JDBC: Access MySQL through the JDBC protocol.
    
    Documentation: https://doris.apache.org//docs/dev/lakehouse/multi-catalog)

    > Note: The corresponding permission level will also be changed automatically, see the "Upgrade Notes" section for details.
    
4. Light table structure changes

In the new version, it is no longer necessary to change the data file synchronously for the operation of adding and subtracting columns to the data table, and only need to update the metadata in FE, thus realizing the millisecond-level Schema Change operation. Through this function, the DDL synchronization capability of upstream CDC data can be realized. For example, users can use Flink CDC to realize DML and DDL synchronization from upstream database to Doris.

Documentation: https://doris.apache.org//docs/dev/sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-TABLE

When creating a table, set `"light_schema_change"="true"` in properties.

5. JDBC facade

	Users can connect to external data sources through JDBC. Currently supported:

	  - MySQL
	  - PostgreSQL
	  - Oracle
	  - SQL Server
	  - Clickhouse

	Documentation: [https://doris.apache.org/en/docs/dev/lakehouse/multi-catalog/jdbc](https://doris.apache.org/docs/dev/lakehouse/multi-catalog/jdbc/)

	> Note: The ODBC feature will be removed in a later version, please try to switch to the JDBC.

6. JAVA UDF

	Supports writing UDF/UDAF in Java, which is convenient for users to use custom functions in the Java ecosystem. At the same time, through technologies such as off-heap memory and Zero Copy, the efficiency of cross-language data access has been greatly improved.

	Document: https://doris.apache.org//docs/dev/ecosystem/udf/java-user-defined-function

	Example: https://github.com/apache/doris/tree/master/samples/doris-demo
	
7. Remote UDF

	Supports accessing remote user-defined function services through RPC, thus completely eliminating language restrictions for users to write UDFs. Users can use any programming language to implement custom functions to complete complex data analysis work.

	Documentation: https://doris.apache.org//docs/ecosystem/udf/remote-user-defined-function

	Example: https://github.com/apache/doris/tree/master/samples/doris-demo
        
8. More data types support

	- Array type

		Array types are supported. It also supports nested array types. In some scenarios such as user portraits and tags, the Array type can be used to better adapt to business scenarios. At the same time, in the new version, we have also implemented a large number of data-related functions to better support the application of data types in actual scenarios.

	Documentation: https://doris.apache.org//docs/dev/sql-manual/sql-reference/Data-Types/ARRAY

	Related functions: https://doris.apache.org//docs/dev/sql-manual/sql-functions/array-functions/array_max
        
	- Jsonb type

		Support binary Json data type: Jsonb. This type provides a more compact json encoding format, and at the same time provides data access in the encoding format. Compared with json data stored in strings, it is several times newer and can be improved.

	Documentation: https://doris.apache.org//docs/dev/sql-manual/sql-reference/Data-Types/JSONB

	Related functions: https://doris.apache.org//docs/dev/sql-manual/sql-functions/json-functions/jsonb_parse
          
	- Date V2
	
		Sphere of influence:

		1. The user needs to specify datev2 and datetimev2 when creating the table, and the date and datetime of the original table will not be affected.
		2. When datev2 and datetimev2 are calculated with the original date and datetime (for example, equivalent connection), the original type will be cast into a new type for calculation
		3. The example is in the documentation

		Documentation: https://doris.apache.org/docs/1.2/sql-manual/sql-reference/Data-Types/DATEV2
	 
	 
## More

1. A new memory management framework

	Documentation: https://doris.apache.org//docs/dev/admin-manual/maint-monitor/memory-management/memory-tracker

2. Table Valued Function

	Doris implements a set of Table Valued Function (TVF). TVF can be regarded as an ordinary table, which can appear in all places where "table" can appear in SQL.

	For example, we can use S3 TVF to implement data import on object storage:

	```
	insert into tbl select * from s3("s3://bucket/file.*", "ak" = "xx", "sk" = "xxx") where c1 > 2;
	```

	Or directly query data files on HDFS:
	
	```
	insert into tbl select * from hdfs("hdfs://bucket/file.*") where c1 > 2;
	```
	
	TVF can help users make full use of the rich expressiveness of SQL and flexibly process various data.

    Documentation:
    
    https://doris.apache.org//docs/dev/sql-manual/sql-functions/table-functions/s3
    
    https://doris.apache.org//docs/dev/sql-manual/sql-functions/table-functions/hdfs
        
3. A more convenient way to create partitions

	Support for creating multiple partitions within a time range via the `FROM TO` command.

4. Column renaming

	For tables with Light Schema Change enabled, column renaming is supported.

	Documentation: https://doris.apache.org//docs/dev/sql-manual/sql-reference/Data-Definition-Statements/Alter/ALTER-TABLE-RENAME
	
5. Richer permission management

	- Support row-level permissions
	
		Row-level permissions can be created with the `CREATE ROW POLICY` command.
	
		Documentation: https://doris.apache.org//docs/dev/sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-POLICY
	
	- Support specifying password strength, expiration time, etc.
	
	- Support for locking accounts after multiple failed logins.
	
		Documentation: https://doris.apache.org//docs/dev/sql-manual/sql-reference/Account-Management-Statements/ALTER-USER

6. Import

	- CSV import supports csv files with header.
	
		Search for `csv_with_names` in the documentation: https://doris.apache.org//docs/dev/sql-manual/sql-reference/Data-Manipulation-Statements/Load/STREAM-LOAD/
	
	- Stream Load adds `hidden_columns`, which can explicitly specify the delete flag column and sequence column.
	
		Search for `hidden_columns` in the documentation: https://doris.apache.org//docs/dev/sql-manual/sql-reference/Data-Manipulation-Statements/Load/STREAM-LOAD
	
	- Spark Load supports Parquet and ORC file import.
	
	- Support for cleaning completed imported Labels
	  
	  Documentation: https://doris.apache.org//docs/dev/sql-manual/sql-reference/Data-Manipulation-Statements/Load/CLEAN-LABEL
	
	- Support batch cancellation of import jobs by status
	
		Documentation: https://doris.apache.org//docs/dev/sql-manual/sql-reference/Data-Manipulation-Statements/Load/CANCEL-LOAD
	
	- Added support for Alibaba Cloud oss, Tencent Cloud cos/chdfs and Huawei Cloud obs in broker load.
		
		Documentation: https://doris.apache.org//docs/dev/advanced/broker
	
	- Support access to hdfs through hive-site.xml file configuration.
	
		Documentation: https://doris.apache.org//docs/dev/admin-manual/config/config-dir

7. Support viewing the contents of the catalog recycle bin through `SHOW CATALOG RECYCLE BIN` function.

	Documentation: https://doris.apache.org//docs/dev/sql-manual/sql-reference/Show-Statements/SHOW-CATALOG-RECYCLE-BIN

8. Support `SELECT * EXCEPT` syntax.

	Documentation: https://doris.apache.org//docs/dev/data-table/basic-usage

9. OUTFILE supports ORC format export. And supports multi-byte delimiters.
    
	Documentation: https://doris.apache.org//docs/dev/sql-manual/sql-reference/Data-Manipulation-Statements/OUTFILE

10. Support to modify the number of Query Profiles that can be saved through configuration.

	Document search FE configuration item: max_query_profile_num
	
11. The DELETE statement supports IN predicate conditions. And it supports partition pruning.

	Documentation: https://doris.apache.org//docs/dev/sql-manual/sql-reference/Data-Manipulation-Statements/Manipulation/DELETE

12. The default value of the time column supports using `CURRENT_TIMESTAMP`

	Search for "CURRENT_TIMESTAMP" in the documentation: https://doris.apache.org//docs/dev/sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-TABLE

13. Add two system tables: backends, rowsets

	Documentation:

	https://doris.apache.org//docs/dev/admin-manual/system-table/backends

	https://doris.apache.org//docs/dev/admin-manual/system-table/rowsets

14. Backup and restore

	- The Restore job supports the `reserve_replica` parameter, so that the number of replicas of the restored table is the same as that of the backup.
	
	- The Restore job supports `reserve_dynamic_partition_enable` parameter, so that the restored table keeps the dynamic partition enabled.
	
	Documentation: https://doris.apache.org//docs/dev/sql-manual/sql-reference/Data-Definition-Statements/Backup-and-Restore/RESTORE
	
	- Support backup and restore operations through the built-in libhdfs, no longer rely on broker.
	
	Documentation: https://doris.apache.org//docs/dev/sql-manual/sql-reference/Data-Definition-Statements/Backup-and-Restore/CREATE-REPOSITORY

15. Support data balance between multiple disks on the same machine

	Documentation:
	
	https://doris.apache.org//docs/dev/sql-manual/sql-reference/Database-Administration-Statements/ADMIN-REBALANCE-DISK
	
	https://doris.apache.org//docs/dev/sql-manual/sql-reference/Database-Administration-Statements/ADMIN-CANCEL-REBALANCE-DISK

16. Routine Load supports subscribing to Kerberos-authenticated Kafka services.

	Search for kerberos in the documentation: https://doris.apache.org//docs/dev/data-operate/import/import-way/routine-load-manual

17. New built-in-function

	Added the following built-in functions:
	
	- `cbrt`
	- `sequence_match/sequence_count`
	- `mask/mask_first_n/mask_last_n`
	- `elt`
	- `any/any_value`
	- `group_bitmap_xor`
	- `ntile`
	- `nvl`
	- `uuid`
	- `initcap`
	- `regexp_replace_one/regexp_extract_all`
	- `multi_search_all_positions/multi_match_any`
	- `domain/domain_without_www/protocol`
	- `running_difference`
	- `bitmap_hash64`
	- `murmur_hash3_64`
	- `to_monday`
	- `not_null_or_empty`
	- `window_funnel`
	- `group_bit_and/group_bit_or/group_bit_xor`
	- `outer combine`
	- and all array functions

# Upgrade Notice

## Known Issues

- Use JDK11 will cause BE crash, please use JDK8 instead.

## Behavior Changed

- Permission level changes

	Because the catalog level is introduced, the corresponding user permission level will also be changed automatically. The rules are as follows:
	
	- GlobalPrivs and ResourcePrivs remain unchanged
	- Added CatalogPrivs level.
	- The original DatabasePrivs level is added with the internal prefix (indicating the db in the internal catalog)
	- Add the internal prefix to the original TablePrivs level (representing tbl in the internal catalog)

- In GroupBy and Having clauses, match on column names in preference to aliases. (#14408)

- Creating columns starting with `mv_` is no longer supported. `mv_` is a reserved keyword in materialized views (#14361)

- Removed the default limit of 65535 rows added by the order by statement, and added the session variable `default_order_by_limit` to configure this limit. (#12478)

- In the table generated by "Create Table As Select", all string columns use the string type uniformly, and no longer distinguish varchar/char/string (#14382)

- In the audit log, remove the word `default_cluster` before the db and user names. (#13499) (#11408)

- Add sql digest field in audit log (#8919)

- The union clause always changes the order by logic. In the new version, the order by clause will be executed after the union is executed, unless explicitly associated by parentheses. (#9745)

- During the decommission operation, the tablet in the recycle bin will be ignored to ensure that the decomission can be completed. (#14028)

- The returned result of Decimal will be displayed according to the precision declared in the original column, or according to the precision specified in the cast function. (#13437)

- Changed column name length limit from 64 to 256 (#14671)

- Changes to FE configuration items

  - The `enable_vectorized_load` parameter is enabled by default. (#11833)

  - Increased `create_table_timeout` value. The default timeout for table creation operations will be increased. (#13520)

  - Modify `stream_load_default_timeout_second` default value to 3 days.

  - Modify the default value of `alter_table_timeout_second` to one month.

  - Increase the parameter `max_replica_count_when_schema_change` to limit the number of replicas involved in the alter job, the default is 100000. (#12850)

  - Add `disable_iceberg_hudi_table`. The iceberg and hudi appearances are disabled by default, and the multi catalog function is recommended. (#13932)

- Changes to BE configuration items

  - Removed `disable_stream_load_2pc` parameter. 2PC's stream load can be used directly. (#13520)

  - Modify `tablet_rowset_stale_sweep_time_sec` from 1800 seconds to 300 seconds.

  - Redesigned configuration item name about compaction (#13495)

  - Revisited parameter about memory optimization (#13781)

- Session variable changes

   - Modify the variable `enable_insert_strict` to true by default. This will cause some insert operations that could be executed before, but inserted illegal values, to no longer be executed. (11866)

   - Modified variable `enable_local_exchange` to default to true (#13292)

   - Default data transmission via lz4 compression, controlled by variable `fragment_transmission_compression_codec` (#11955)

   - Add `skip_storage_engine_merge` variable for debugging unique or agg model data (#11952)
    
     Documentation: https://doris.apache.org//docs/dev/advanced/variables

- The BE startup script will check whether the value is greater than 200W through `/proc/sys/vm/max_map_count`. Otherwise, the startup fails. (#11052)

- Removed mini load interface (#10520)

- FE Metadata Version

	FE Meta Version changed from 107 to 114, and cannot be rolled back after upgrading.
	
## During Upgrade

1. Upgrade preparation
  
   - Need to replace: lib, bin directory (start/stop scripts have been modified)
  
   - BE also needs to configure JAVA_HOME, and already supports JDBC Table and Java UDF.
  
   - The default JVM Xmx parameter in fe.conf is changed to 8GB.

2. Possible errors during the upgrade process
  
   - The repeat function cannot be used and an error is reported: `vectorized repeat function cannot be executed`, you can turn off the vectorized execution engine before upgrading. (#13868)
  
   - schema change fails with error: `desc_tbl is not set. Maybe the FE version is not equal to the BE` (#13822)
  
   - Vectorized hash join cannot be used and an error will be reported. `vectorized hash join cannot be executed`. You can turn off the vectorized execution engine before upgrading. (#13753)

	The above errors will return to normal after a full upgrade.
	
## Performance Impact

- By default, JeMalloc is used as the memory allocator of the new version BE, replacing TcMalloc (#13367)

- The batch size in the tablet sink is modified to be at least 8K. (#13912)

- Disable chunk allocator by default (#13285)

## Api change

- BE's http api error return information changed from `{"status": "Fail", "msg": "xxx"}` to more specific ``{"status": "Not found", "msg": "Tablet not found. tablet_id=1202"}``(#9771)

- In `SHOW CREATE TABLE`, the content of comment is changed from double quotes to single quotes (#10327)

- Support ordinary users to obtain query profile through http command. (#14016)
Documentation: https://doris.apache.org//docs/dev/admin-manual/http-actions/fe/manager/query-profile-action

- Optimized the way to specify the sequence column, you can directly specify the column name. (#13872)
Documentation: https://doris.apache.org//docs/dev/data-operate/update-delete/sequence-column-manual

- Increase the space usage of remote storage in the results returned by `show backends` and `show tablets` (#11450)

- Removed Num-Based Compaction related code (#13409)

- Refactored BE's error code mechanism, some returned error messages will change (#8855)
other

- Support Docker official image.

- Support compiling Doris on MacOS(x86/M1) and ubuntu-22.04
  Documentation: https://doris.apache.org//docs/dev/install/source-install/compilation-mac/

- Support for image file verification.

  Documentation: https://doris.apache.org//docs/dev/admin-manual/maint-monitor/metadata-operation/

- script related

  - The stop scripts of FE and BE support exiting FE and BE via the `--grace` parameter (use kill -15 signal instead of kill -9)

  - FE start script supports checking the current FE version via --version (#11563)

 - Support to get the data and related table creation statement of a tablet through the `ADMIN COPY TABLET` command, for local problem debugging (#12176)

	Documentation: https://doris.apache.org//docs/dev/sql-manual/sql-reference/Database-Administration-Statements/ADMIN-COPY-TABLET

- Support to obtain a table creation statement related to a SQL statement through the http api for local problem reproduction (#11979)

	Documentation: https://doris.apache.org//docs/dev/admin-manual/http-actions/fe/query-schema-action

- Support to close the compaction function of this table when creating a table, for testing (#11743)

	Search for "disble_auto_compaction" in the documentation: https://doris.apache.org//docs/dev/sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-TABLE
	
# Big Thanks

Thanks to ALL who contributed to this release! (alphabetically)
```
@924060929
@a19920714liou
@adonis0147
@Aiden-Dong
@aiwenmo
@AshinGau
@b19mud
@BePPPower
@BiteTheDDDDt
@bridgeDream
@ByteYue
@caiconghui
@CalvinKirs
@cambyzju
@caoliang-web
@carlvinhust2012
@catpineapple
@ccoffline
@chenlinzhong
@chovy-3012
@coderjiang
@cxzl25
@dataalive
@dataroaring
@dependabot[bot]
@dinggege1024
@DongLiang-0
@Doris-Extras
@eldenmoon
@EmmyMiao87
@englefly
@FreeOnePlus
@Gabriel39
@gaodayue
@geniusjoe
@gj-zhang
@gnehil
@GoGoWen
@HappenLee
@hello-stephen
@Henry2SS
@hf200012
@huyuanfeng2018
@jacktengg
@jackwener
@jeffreys-cat
@Jibing-Li
@JNSimba
@Kikyou1997
@Lchangliang
@LemonLiTree
@lexoning
@liaoxin01
@lide-reed
@link3280
@liutang123
@liuyaolin
@LOVEGISER
@lsy3993
@luozenglin
@luzhijing
@madongz
@morningman
@morningman-cmy
@morrySnow
@mrhhsg
@Myasuka
@myfjdthink
@nextdreamblue
@pan3793
@pangzhili
@pengxiangyu
@platoneko
@qidaye
@qzsee
@SaintBacchus
@SeekingYang
@smallhibiscus
@sohardforaname
@song7788q
@spaces-X
@ssusieee
@stalary
@starocean999
@SWJTU-ZhangLei
@TaoZex
@timelxy
@Wahno
@wangbo
@wangshuo128
@wangyf0555
@weizhengte
@weizuo93
@wsjz
@wunan1210
@xhmz
@xiaokang
@xiaokangguo
@xinyiZzz
@xy720
@yangzhg
@Yankee24
@yeyudefeng
@yiguolei
@yinzhijian
@yixiutt
@yuanyuan8983
@zbtzbtzbt
@zenoyang
@zhangboya1
@zhangstar333
@zhannngchen
@ZHbamboo
@zhengshiJ
@zhenhb
@zhqu1148980644
@zuochunwei
@zy-kkk
```
