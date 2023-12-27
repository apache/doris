---
{
    "title": "Release 2.0.3",
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

Thanks to our community users and developers, about 1000 improvements and bug fixes have been made in Doris 2.0.3 version, including optimizer statistics, inverted index, complex datatypes, data lake, replica management.



## 1 Behavior change

- The output format of the complex data type array/map/struct has been changed to be consistent to the input format and JSON specification. The main changes from the previous version are that DATE/DATETIME and STRING/VARCHAR are enclosed in double quotes and null values inside ARRAY/MAP are displayed as `null` instead of `NULL`.
  - https://github.com/apache/doris/pull/25946
- SHOW_VIEW permission is supported. Users with SELECT or LOAD permission will no longer be able to execute the 'SHOW CREATE VIEW' statement and must be granted the SHOW_VIEW permission separately.
  - https://github.com/apache/doris/pull/25370


## 2 New features

### 2.1 Support collecting statistics for optimizer automatically

Collecting statistics helps the optimizer understand the data distribution characteristics and choose a better plan to greatly improve query performance. It is officially supported starting from version 2.0.3 and is enabled all day by default.

see more：https://doris.apache.org/docs/query-acceleration/statistics/


### 2.2 Support complex datatypes for more datalake source
- Support complex datatypes for JAVA UDF, JDBC and Hudi MOR
  - https://github.com/apache/doris/pull/24810
  - https://github.com/apache/doris/pull/26236
- Support complex datatypes for Paimon
  - https://github.com/apache/doris/pull/25364
- Suport Paimon version 0.5
  - https://github.com/apache/doris/pull/24985


### 2.3 Add more builtin functions
- Support the BitmapAgg function in new optimizer
  - https://github.com/apache/doris/pull/25508
- Supports SHA series digest functions
  - https://github.com/apache/doris/pull/24342 
- Support the BITMAP datatype in the aggregate functions min_by and max_by 
  - https://github.com/apache/doris/pull/25430 
- Add milliseconds/microseconds_add/sub/diff functions
  - https://github.com/apache/doris/pull/24114
- Add some json functions: json_insert, json_replace, json_set
  - https://github.com/apache/doris/pull/24384


## 3 Improvement and optimizations

### 3.1 Performance optimizations

- When the inverted index MATCH WHERE condition with a high filter rate is combined with the common WHERE condition with a low filter rate, the I/O of the index column is greatly reduced. 
- Optimize the efficiency of random data access after the where filter.
- Optimizes the performance of the old get_json_xx function on JSON data types by 2~4x.
- Supports the configuration to reduce the priority of the data read thread, ensuring the CPU resources for real-time writing.
- Adds `uuid-numeric` function that returns largeint, which is 20 times faster than `uuid` function that returns string.
- Optimized the performance of case when by 3x.
- Cut out unnecessary predicate calculations in storage engine execution.
- Accelerate count performance by pushing down count operator to storage tier.
- Optimizes the computation performance of the nullable type in and or expressions.
- Supports rewriting the limit operator before `join` in more scenarios to improve query performance. 
- Eliminate useless `order by` operators from inline view to improve query performance.
- Optimizes the accuracy of cardinality estimates and cost models in some cases. 
- Optimized jdbc catalog predicate pushdown logic.
- Optimized the read efficiency of the file cache when it's enable for the first time.
- Optimizes the hive table sql cache policy and uses the partition update time stored in HMS to improve the cache hit ratio. 
- Optimize mow compaction efficiency.
- Optimized thread allocation logic for external table query to reduce memory usage 
- Optimize memory usage for column reader.



### 3.2 Distributed replica management improvements

Distributed replica management improvements include skipping partition deletion, colocate group deletion, balance failure due to continuous write, and hot and cold seperation table balance.


### 3.3 Security enhancement
- The audit log plug-in uses a token instead of a plaintext password to enhance security
  - https://github.com/apache/doris/pull/26278
- log4j configures security enhancement
  - https://github.com/apache/doris/pull/24861  
- Sensitive user information is not displayed in logs
  - https://github.com/apache/doris/pull/26912


## 4 Bugfix and stability

### 4.1 Complex datatypes
- Fix issues that fixed-length CHAR(n) was not truncated correctly in map/struct.
  - https://github.com/apache/doris/pull/25725
- Fix write failure for struct datatype nested for map/array
  - https://github.com/apache/doris/pull/26973
- Fix the issue that count distinct did not support array/map/struct 
  - https://github.com/apache/doris/pull/25483
- Fix be crash in updating to 2.0.3 after the delete complex type appeared in query 
  - https://github.com/apache/doris/pull/26006
- Fix be crash when JSON datatype is in WHERE clause.
  - https://github.com/apache/doris/pull/27325
- Fix be crash when ARRAY datatype is in OUTER JOIN clause.
  - https://github.com/apache/doris/pull/25669
- Fix reading incorrect result for DECIMAL datatype in ORC format.
  - https://github.com/apache/doris/pull/26548
  - https://github.com/apache/doris/pull/25977
  - https://github.com/apache/doris/pull/26633

### 4.2 Inverted index
- Fix incorrect result for OR NOT combination in WHERE clause were incorrect when disable inverted index query. 
  - https://github.com/apache/doris/pull/26327
- Fix be crash when write a empty with inverted index
  - https://github.com/apache/doris/pull/25984
- Fix be crash in index compaction when the output of compaction is empty.
  - https://github.com/apache/doris/pull/25486
- Fixed the problem of adding an inverted index to be crashed when no data is written to the newly added column.
- Fix be crash when BUILD INDEX after ADD COLUMN without new data written.
  - https://github.com/apache/doris/pull/27276
- Fix missing and leak problem of hardlink for inverted index file.
  - https://github.com/apache/doris/pull/26903
- Fix index file corrupt when disk is full temporarilly
  - https://github.com/apache/doris/pull/28191
- Fix incorrect result due to optimization for skip reading index column
  - https://github.com/apache/doris/pull/28104

### 4.3 Materialized View
- Fix the problem of BE crash caused by repeated expressions in the group by statement
- Fix be crash when there are duplicate expressions in `group by` statements.
  - https://github.com/apache/doris/pull/27523
- Disables the float/doubld type in the `group by` clause when a view is created. 
  - https://github.com/apache/doris/pull/25823
- Improve the function of select query matching materialized view 
  - https://github.com/apache/doris/pull/24691 
- Fix an issue that materialized views could not be matched when a table alias was used 
  - https://github.com/apache/doris/pull/25321
- Fix the problem using percentile_approx when creating materialized views 
  - https://github.com/apache/doris/pull/26528

### 4.4 Table sample
- Fix the problem that table sample query can not work on table with partitions.
  - https://github.com/apache/doris/pull/25912  
- Fix the problem that table sample query can not work when specify tablet.
  - https://github.com/apache/doris/pull/25378 


### 4.5 Unique with merge on write
- Fix null pointer exception in conditional update based on primary key  
  - https://github.com/apache/doris/pull/26881    
- Fix field name capitalization issues in partial update  
  - https://github.com/apache/doris/pull/27223 
- Fix duplicate keys occur in mow during schema change repairement. 
  - https://github.com/apache/doris/pull/25705


### 4.6 Load and compaction
- Fix unkown slot descriptor error in routineload for running multiple tables 
  - https://github.com/apache/doris/pull/25762
- Fix be crash due to concurrent memory access when caculating memory 
  - https://github.com/apache/doris/pull/27101 
- Fix be crash on duplicate cancel for load.
  - https://github.com/apache/doris/pull/27111
- Fix broker connection error during broker load
  - https://github.com/apache/doris/pull/26050
- Fix incorrect result delete predicates in concurrent case of compation and scan.
  - https://github.com/apache/doris/pull/24638
- Fix the problem tha compaction task would print too many stacktrace logs 
  - https://github.com/apache/doris/pull/25597


### 4.7 Data Lake compatibility
- Solve the problem that the iceberg table contains special characters that cause query failure 
  - https://github.com/apache/doris/pull/27108 
- Fix compatibility issues of different hive metastore versions 
  - https://github.com/apache/doris/pull/27327 
- Fix an error reading max compute partition table 
  - https://github.com/apache/doris/pull/24911 
- Fix the issue that backup to object storage failed 
  - https://github.com/apache/doris/pull/25496 
  - https://github.com/apache/doris/pull/25803


### 4.8 JDBC external table compatibility 
 
- Fix Oracle date type format error in jdbc catalog  
  - https://github.com/apache/doris/pull/25487 
- Fix MySQL 0000-00-00 date exception in jdbc catalog  
  - https://github.com/apache/doris/pull/26569 
- Fix an exception in reading data from Mariadb where the default value of the time type is current_timestamp 
  - https://github.com/apache/doris/pull/25016 
- Fix be crash when processing BITMAP datatype in jdbc catalog
  - https://github.com/apache/doris/pull/25034 
  - https://github.com/apache/doris/pull/26933


### 4.9 SQL Planner and Optimizer

- Fix partition prune error in some scenes
  - https://github.com/apache/doris/pull/27047
  - https://github.com/apache/doris/pull/26873
  - https://github.com/apache/doris/pull/25769
  - https://github.com/apache/doris/pull/27636

- Fix incorrect sub-query processing in some scenarios
  - https://github.com/apache/doris/pull/26034
  - https://github.com/apache/doris/pull/25492
  - https://github.com/apache/doris/pull/25955
  - https://github.com/apache/doris/pull/27177

- Fix some semantic parsing errors
  - https://github.com/apache/doris/pull/24928
  - https://github.com/apache/doris/pull/25627
  
- Fix data loss during right outer/anti join
  - https://github.com/apache/doris/pull/26529
  
- Fix incorrect pushing down of predicate pass aggregation operators.
  - https://github.com/apache/doris/pull/25525
  
- Fix incorrect result header in some cases
  - https://github.com/apache/doris/pull/25372
  
- Fix incorrect plan when the nullsafeEquals expression (<=>) is used as the join condition
  - https://github.com/apache/doris/pull/27127

- Fix correct column prune in set operation operator.
  - https://github.com/apache/doris/pull/26884


### Others

- Fix BE crash when the order of columns in a table is changed and then upgraded to 2.0.3.
  - https://github.com/apache/doris/pull/28205


See the complete list of improvements and bug fixes on [github dev/2.0.3-merged](https://github.com/apache/doris/issues?q=label%3Adev%2F2.0.3-merged+is%3Aclosed) .