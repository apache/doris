---
{ 'title': 'Introduction to Apache Doris', 'language': 'en' }
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

# Introduction to Apache Doris

Apache Doris is a high-performance, real-time analytical database based on MPP architecture, known for its extreme speed and ease of use. It only requires a sub-second response time to return query results under massive data and can support not only high-concurrent point query scenarios but also high-throughput complex analysis scenarios. Based on this, Apache Doris can better meet the scenarios of report analysis, ad-hoc query, unified data warehouse, Data Lake Query Acceleration, etc. Users can build user behavior analysis, AB test platform, log retrieval analysis, user portrait analysis, order analysis, and other applications on top of this.

Apache Doris was first born as Palo project for Baidu's ad reporting business, officially open-sourced in 2017, donated by Baidu to the Apache Foundation for incubation in July 2018, and then incubated and operated by members of the incubator project management committee under the guidance of Apache mentors. Currently, the Apache Doris community has gathered more than 300 contributors from nearly 100 companies in different industries, and the number of active contributors is close to 100 per month. Apache Doris has graduated from Apache incubator successfully and become a Top-Level Project in June 2022.

Apache Doris now has a wide user base in China and around the world, and as of today, Apache Doris is used in production environments in over 700 companies worldwide. More than 80% of the top 50 Internet companies in China in terms of market capitalization or valuation have been using Apache Doris for a long time, including Baidu, Meituan, Xiaomi, Jingdong, Bytedance, Tencent, NetEase, Kwai, Weibo, and Ke Holdings. It is also widely used in some traditional industries such as finance, energy, manufacturing, and telecommunications.

# Usage Scenarios

As shown in the figure below, after various data integration and processing, the data sources are usually stored in the real-time data warehouse Doris and the offline data lake or data warehouse (in Apache Hive, Apache Iceberg or Apache Hudi).
![Image description](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/sekvbs5ih5rb16wz6n9k.png)

Apache Doris is widely used in the following scenarios:

-   Reporting Analysis

    -   Real-time Dashboards
    -   Reports for in-house analysts and managers
    -   Highly concurrent user-oriented or customer-oriented report analysis: For example, in the scenarios of site analysis for website owners and advertising reports for advertisers, the concurrency usually requires thousands of QPS and the query latency requires sub-seconds response. The famous e-commerce company JD.com uses Doris in advertising reports, writing 10 billion rows of data per day, with tens of thousands of concurrent query QPS and 150ms query latency for the 99th percentile.

-   Ad-Hoc Query. Analyst-oriented self-service analytics with irregular query patterns and high throughput requirements. XiaoMi has built a growth analytics platform (Growth Analytics, GA) based on Doris, using user behavior data for business growth analysis, with an average query latency of 10 seconds and a 95th percentile query latency of 30 seconds or less, and tens of thousands of SQL queries per day.

-   Unified data warehouse construction. A platform to meet the needs of unified data warehouse construction and simplify the complicated data software stack. HaiDiLao's Doris-based unified data warehouse replaces the old architecture consisting of Apache Spark, Apache Hive, Apache Kudu, Apache HBase, and Apache Phoenix, and greatly simplifies the architecture.

-   Data Lake Query. By federating the data located in Apache Hive, Apache Iceberg, and Apache Hudi using external tables, the query performance is greatly improved while avoiding data copying.

# Technical Overview

The overall architecture of Apache Doris is shown in the following figure. The Doris architecture is very simple, with only two types of processes.

-   Frontend（FE）: It is mainly responsible for user request access, query parsing and planning, management of metadata, and node management-related work.
-   Backend（BE）: It is mainly responsible for data storage and query plan execution.

Both types of processes are horizontally scalable, and a single cluster can support up to hundreds of machines and tens of petabytes of storage capacity. And these two types of processes guarantee high availability of services and high reliability of data through consistency protocols. This highly integrated architecture design greatly reduces the operation and maintenance cost of a distributed system.

![Image description](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/mnz20ae3s23vv3e9ltmi.png)

Apache Doris adopts MySQL protocol, highly compatible with MySQL dialect, and supports standard SQL. Users can access Doris through various client tools and support seamless connection with BI tools.

In terms of the storage engine, Doris uses columnar storage to encode and compress and read data by column, enabling a very high compression ratio while reducing a large number of scans of non-relevant data, thus making more efficient use of IO and CPU resources.
Doris also supports a relatively rich index structure to reduce data scans:

-   Support sorted compound key index: Up to three columns can be specified to form a compound sort key. With this index, data can be effectively pruned to better support high concurrent reporting scenarios.
-   Z-order index ：Using Z-order indexing, you can efficiently run range queries on any combination of fields in your schema.
-   MIN/MAX indexing: Effective filtering of equivalence and range queries for numeric types
-   Bloom Filter: very effective for equivalence filtering and pruning of high cardinality columns
-   Invert Index: It enables the fast search of any field

In terms of storage models, Doris supports a variety of storage models, with specific optimizations for different scenarios:

-   Aggregate Key Model: Merge the value columns with the same keys, by aggregating in advance to significantly improve performance.
-   Unique Key model: The key is unique. Data with the same key will be overwritten to achieve row-level data updates.
-   Duplicate Key model: The detailed data model can satisfy the detailed storage of fact tables.

Doris also supports strong consistent materialized views, where updates and selections of materialized views are made automatically within the system and do not require manual selection by the user, thus significantly reducing the cost of materialized view maintenance.

In terms of query engine, Doris adopts the MPP model, with parallel execution between and within nodes, and also supports distributed shuffle join for multiple large tables, which can better cope with complex queries.

![Image description](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/vjlmumwyx728uymsgcw0.png)

The Doris query engine is vectorized, and all memory structures can be laid out in a columnar format to achieve significant reductions in virtual function calls, improved Cache hit rates, and efficient use of SIMD instructions. Performance in wide table aggregation scenarios is 5–10 times higher than in non-vectorized engines.
![Image description](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/ck2m3kbnodn28t28vphp.png)

Apache Doris uses Adaptive Query Execution technology, which can dynamically adjust the execution plan based on runtime statistics, such as runtime filter technology to generate filters to push to the probe side at runtime and to automatically penetrate the filters to the probe side which drastically reduces the amount of data in the probe and speeds up join performance. Doris' runtime filter supports In/Min/Max/Bloom filter.

In terms of the optimizer, Doris uses a combination of CBO and RBO, with RBO supporting constant folding, subquery rewriting, predicate pushdown, etc., and CBO supporting Join Reorder. CBO is still under continuous optimization, mainly focusing on more accurate statistical information collection and derivation, more accurate cost model prediction, etc.

