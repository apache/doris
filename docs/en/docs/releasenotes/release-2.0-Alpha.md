---
{
    "title": "Release 2.0-Alpha",
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


The Apache Doris 2.0 Alpha version is an alpha release that is aimed to be used for evaluating the new features of Doris 2.0.

It's recommended to deploy 2.0 Alpha version in a new test cluster for testing but **it should not be deployed in production clusters**.


# Highlight Features

### 1. Semi-Structured Data Storage and Fast Analysis

- Inverted Index: Supports both fulltext search and normal equal, range query.
  - Supports fulltext search query
    - Supports Chinese, English and Unicode standard tokenizer.
    - Supports both STRING and ARRAY types.
  - Supports normal equal, range query on STRING, NUMERIC, DATE, DATETIME types.
  - Supports logical combination of multiple conditions, not only AND but also OR and NOT.
  - Much more efficient compared to ElasticSearch in esrally http logs benchmark: 4x speed up for data load, 80% storage space reduced, 2x speed up for 11 queries.

	Refer to: [https://doris.apache.org/docs/dev/data-table/index/inverted-index](https://doris.apache.org/docs/dev/data-table/index/inverted-index)

- Complex Datatypes
  - JSONB data type is more efficient with fast simdjson first time data parsing
  - ARRAY data type is more mature, adding dozens of array functions
  - MAP data type is added for key-value pairs data, such as extensible user behavior properties
  - STRUCT data type is add for traditional struct

### 2. High Concurrent and Low Latency Point Query

- Implement row storage and row cache to speed up fetch whole rows.
- Implement short circuit query plan and execution for primary key query like `SELECT * FROM tablex WHERE id = xxx`
- Cache compiled query plan using PreparedStatement.
- High QPS (30K+) on a single cloud host with 16 cpu core and 64g memory.

	Refer to: [https://doris.apache.org/docs/dev/query-acceleration/hight-concurrent-point-query](https://doris.apache.org/docs/dev/query-acceleration/hight-concurrent-point-query)

### 3. Vertical Compaction Enable by Default

- Vertical compaction divides the schema into column groups, and then merges data by column, which can effectively reduce the memory overhead of compaction and improve the execution speed of compaction.
- In the actual test, the memory used by vertical compaction is only 1/10 of the original compaction algorithm, and the compaction rate is increased by 15%.

	Refer to: [https://doris.apache.org/docs/dev/advanced/best-practice/compaction/#vertical-compaction](https://doris.apache.org/docs/dev/query-acceleration/hight-concurrent-point-query)

### 4. Separation of Hot and Cold Data

- Users can set the hot and cold data strategy through SQL, so as to move historical data to cheap storage such as object storage to reduce storage costs.
- Cold data can still be accessed, and Doris provides a local cache to speed up the access efficiency of cold data.

	Refer to: [https://doris.apache.org/docs/dev/advanced/cold_hot_separation](https://doris.apache.org/docs/dev/advanced/cold_hot_separation)


### 5. Pipeline Execution Engine Adapted to the Architecture of Modern Multi-Core CPUs (disable by default)

- Asynchronous blocking operators: blocking operators will no longer occupy thread resources, and will no longer generate thread switching overhead.
- Adaptive load: adopts Multi-Level Feedback Queue to schedule query priorities. In mixed load scenarios, each query can be fairly allocated to a fixed thread scheduling time slice, thus ensuring that Doris can perform different tasks under different loads with more stable performance.
- Controllable number of threads: The default number of execution threads of the pipeLine execution engine is the number of CPUs and cores, and Doris starts the corresponding execution thread pool to manage the execution threads.

	Refer to: [https://doris.apache.org/docs/dev/query-acceleration/pipeline-execution-engine](https://doris.apache.org/docs/dev/query-acceleration/pipeline-execution-engine)

### 6. Nereids - The Brand New Optimizier (disable by default)

- Smarter: The new optimizer presents the optimization points of each RBO and CBO in the form of rules. For each rule, the new optimizer provides a set of patterns used to describe the shape of the query plan, which can exactly match the query plan that can be optimized. Based on this, the new optimizer can better support more complex query statements such as multi-level subquery nesting. At the same time, the CBO of the new optimizer is based on the advanced cascades framework, uses richer data statistics, and applies a cost model with more scientific dimensions. This makes the new optimizer more handy when faced with multi-table join queries.
- More robust: All optimization rules of the new optimizer are completed on the logical execution plan tree. After the query syntax and semantic analysis is completed, it will be transformed into a tree structure. Compared with the internal data structure of the old optimizer, it is more reasonable and unified. Taking subquery processing as an example, the new optimizer is based on a new data structure, which avoids separate processing of subqueries by many rules in the old optimizer. In turn, the possibility of logic errors in optimization rules is reduced.
- More flexible: The architectural design of the new optimizer is more reasonable and modern. Optimization rules and processing stages can be easily extended. Optimizer developers can respond to user needs more easier and quickly.

	Refer to: [https://doris.apache.org/docs/dev/query-acceleration/nereids](https://doris.apache.org/docs/dev/query-acceleration/nereids)

# Behavior Changed

- Enable light weight schema change by default.
- By default, datev2, datetimev2, and decimalv3 are used to create tables, and datav1, datetimev1, and decimalv2 are not supported for creating tables. 

	Refer to: [https://github.com/apache/doris/pull/19077](https://github.com/apache/doris/pull/19077)

- In the JDBC and Iceberg catalogs, decimalv3 is used by default. 

	Refer to: [https://github.com/apache/doris/pull/18926](https://github.com/apache/doris/pull/18926)

- Added max_openfiles and swap checks in the startup script of be, so if the system configuration is not reasonable, be may fail to start. 

	Refer to: [https://github.com/apache/doris/pull/18888](https://github.com/apache/doris/pull/18888)

- It is forbidden to log in without a password when accessing FE on localhost. 

	Refer to: [https://github.com/apache/doris/pull/18816](https://github.com/apache/doris/pull/18816)

- When there is a multi catalog in the system, when querying the data of the information schema, only the data of the internal catalog will be displayed by default. 

	Refer to: [https://github.com/apache/doris/pull/18662](https://github.com/apache/doris/pull/18662)

- Renamed the process name of Doris to DorisFE and DorisBE. 

	Refer to: [https://github.com/apache/doris/pull/18167](https://github.com/apache/doris/pull/18167)

- The non-vectorized code has been removed from the system, so the enable_vectorized_engine parameter no longer works. 

	Refer to: [https://github.com/apache/doris/pull/18166](https://github.com/apache/doris/pull/18166)

- Limit the depth of the expression tree, the default is 200. 

	Refer to: [https://github.com/apache/doris/pull/17314](https://github.com/apache/doris/pull/17314)

- In order to be compatible with BI tools, datev2 and datetimev2 are displayed as date and datetime when show create table. 

	Refer to: [https://github.com/apache/doris/pull/18358](https://github.com/apache/doris/pull/18358)





