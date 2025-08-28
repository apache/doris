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

<div align="center">

# Apache Doris

[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)
[![GitHub release](https://img.shields.io/github/release/apache/doris.svg)](https://github.com/apache/doris/releases)
[![OSSRank](https://shields.io/endpoint?url=https://ossrank.com/shield/516)](https://ossrank.com/p/516)
[![Commit activity](https://img.shields.io/github/commit-activity/m/apache/doris)](https://github.com/apache/doris/commits/master/)
[![EN doc](https://img.shields.io/badge/Docs-English-blue.svg)](https://doris.apache.org/docs/gettingStarted/what-is-apache-doris)
[![CN doc](https://img.shields.io/badge/文档-中文版-blue.svg)](https://doris.apache.org/zh-CN/docs/gettingStarted/what-is-apache-doris)

<div>

[![Official Website](<https://img.shields.io/badge/-Visit%20the%20Official%20Website%20%E2%86%92-rgb(15,214,106)?style=for-the-badge>)](https://doris.apache.org/)
[![Quick Download](<https://img.shields.io/badge/-Quick%20%20Download%20%E2%86%92-rgb(66,56,255)?style=for-the-badge>)](https://doris.apache.org/download)


</div>


<div>
    <a href="https://twitter.com/doris_apache"><img src="https://img.shields.io/badge/- @Doris_Apache -424549?style=social&logo=x" height=25></a>
    &nbsp;
    <a href="https://github.com/apache/doris/discussions"><img src="https://img.shields.io/badge/- Discussion -red?style=social&logo=discourse" height=25></a>
    &nbsp;
    <a href="https://join.slack.com/t/apachedoriscommunity/shared_invite/zt-35mzao67o-BrpU70FNKPyB6UlgpXf8_w" height=25></a>
    &nbsp;
    <a href="https://medium.com/@ApacheDoris"><img src="https://img.shields.io/badge/-Medium-red?style=social&logo=medium" height=25></a>

</div>

</div>

---




Apache Doris is an easy-to-use, high-performance and real-time analytical database based on MPP architecture, known for its extreme speed and ease of use. It only requires a sub-second response time to return query results under massive data and can support not only high-concurrency point query scenarios but also high-throughput complex analysis scenarios.

All this makes Apache Doris an ideal tool for scenarios including report analysis, ad-hoc query, unified data warehouse, and data lake query acceleration. On Apache Doris, users can build various applications, such as user behavior analysis, AB test platform, log retrieval analysis, user portrait analysis, and order analysis.

🎉 Check out the 🔗[All releases](https://doris.apache.org/docs/releasenotes/all-release), where you'll find a chronological summary of Apache Doris versions released over the past year.

👀 Explore the 🔗[Official Website](https://doris.apache.org/) to discover Apache Doris's core features, blogs, and user cases in detail.

## 📈 Usage Scenarios

As shown in the figure below, after various data integration and processing, the data sources are usually stored in the real-time data warehouse Apache Doris and the offline data lake or data warehouse (in Apache Hive, Apache Iceberg or Apache Hudi).

<br />

<img src="https://cdn.selectdb.com/static/What_is_Apache_Doris_3_a61692c2ce.png" />

<br />


Apache Doris is widely used in the following scenarios:

- **Real-time Data Analysis**:

  - **Real-time Reporting and Decision-making**: Doris provides real-time updated reports and dashboards for both internal and external enterprise use, supporting real-time decision-making in automated processes.
  
  - **Ad Hoc Analysis**: Doris offers multidimensional data analysis capabilities, enabling rapid business intelligence analysis and ad hoc queries to help users quickly uncover insights from complex data.
  
  - **User Profiling and Behavior Analysis**: Doris can analyze user behaviors such as participation, retention, and conversion, while also supporting scenarios like population insights and crowd selection for behavior analysis.

- **Lakehouse Analytics**:

  - **Lakehouse Query Acceleration**: Doris accelerates lakehouse data queries with its efficient query engine.
  
  - **Federated Analytics**: Doris supports federated queries across multiple data sources, simplifying architecture and eliminating data silos.
  
  - **Real-time Data Processing**: Doris combines real-time data streams and batch data processing capabilities to meet the needs of high concurrency and low-latency complex business requirements.

- **SQL-based Observability**:

  - **Log and Event Analysis**: Doris enables real-time or batch analysis of logs and events in distributed systems, helping to identify issues and optimize performance.


## Overall Architecture

Apache Doris uses the MySQL protocol, is highly compatible with MySQL syntax, and supports standard SQL. Users can access Apache Doris through various client tools, and it seamlessly integrates with BI tools.

### Storage-Compute Integrated Architecture

The storage-compute integrated architecture of Apache Doris is streamlined and easy to maintain. As shown in the figure below, it consists of only two types of processes:

- **Frontend (FE):** Primarily responsible for handling user requests, query parsing and planning, metadata management, and node management tasks.

- **Backend (BE):** Primarily responsible for data storage and query execution. Data is partitioned into shards and stored with multiple replicas across BE nodes.

![The overall architecture of Apache Doris](https://cdn.selectdb.com/static/What_is_Apache_Doris_adb26397e2.png)

<br />

In a production environment, multiple FE nodes can be deployed for disaster recovery. Each FE node maintains a full copy of the metadata. The FE nodes are divided into three roles:

| Role      | Function                                                     |
| --------- | ------------------------------------------------------------ |
| Master    | The FE Master node is responsible for metadata read and write operations. When metadata changes occur in the Master, they are synchronized to Follower or Observer nodes via the BDB JE protocol. |
| Follower  | The Follower node is responsible for reading metadata. If the Master node fails, a Follower node can be selected as the new Master. |
| Observer  | The Observer node is responsible for reading metadata and is mainly used to increase query concurrency. It does not participate in cluster leadership elections. |

Both FE and BE processes are horizontally scalable, enabling a single cluster to support hundreds of machines and tens of petabytes of storage capacity. The FE and BE processes use a consistency protocol to ensure high availability of services and high reliability of data. The storage-compute integrated architecture is highly integrated, significantly reducing the operational complexity of distributed systems.


## Core Features of Apache Doris

- **High Availability**: In Apache Doris, both metadata and data are stored with multiple replicas, synchronizing data logs via the quorum protocol. Data write is considered successful once a majority of replicas have completed the write, ensuring that the cluster remains available even if a few nodes fail. Apache Doris supports both same-city and cross-region disaster recovery, enabling dual-cluster master-slave modes. When some nodes experience failures, the cluster can automatically isolate the faulty nodes, preventing the overall cluster availability from being affected.

- **High Compatibility**: Apache Doris is highly compatible with the MySQL protocol and supports standard SQL syntax, covering most MySQL and Hive functions. This high compatibility allows users to seamlessly migrate and integrate existing applications and tools. Apache Doris supports the MySQL ecosystem, enabling users to connect Doris using MySQL Client tools for more convenient operations and maintenance. It also supports MySQL protocol compatibility for BI reporting tools and data transmission tools, ensuring efficiency and stability in data analysis and data transmission processes.

- **Real-Time Data Warehouse**: Based on Apache Doris, a real-time data warehouse service can be built. Apache Doris offers second-level data ingestion capabilities, capturing incremental changes from upstream online transactional databases into Doris within seconds. Leveraging vectorized engines, MPP architecture, and Pipeline execution engines, Doris provides sub-second data query capabilities, thereby constructing a high-performance, low-latency real-time data warehouse platform.

- **Unified Lakehouse**: Apache Doris can build a unified lakehouse architecture based on external data sources such as data lakes or relational databases. The Doris unified lakehouse solution enables seamless integration and free data flow between data lakes and data warehouses, helping users directly utilize data warehouse capabilities to solve data analysis problems in data lakes while fully leveraging data lake data management capabilities to enhance data value.

- **Flexible Modeling**: Apache Doris offers various modeling approaches, such as wide table models, pre-aggregation models, star/snowflake schemas, etc. During data import, data can be flattened into wide tables and written into Doris through compute engines like Flink or Spark, or data can be directly imported into Doris, performing data modeling operations through views, materialized views, or real-time multi-table joins.

## Technical overview

Doris provides an efficient SQL interface and is fully compatible with the MySQL protocol. Its query engine is based on an MPP (Massively Parallel Processing) architecture, capable of efficiently executing complex analytical queries and achieving low-latency real-time queries. Through columnar storage technology for data encoding and compression, it significantly optimizes query performance and storage compression ratio.

### Interface

Apache Doris adopts the MySQL protocol, supports standard SQL, and is highly compatible with MySQL syntax. Users can access Apache Doris through various client tools and seamlessly integrate it with BI tools, including but not limited to Smartbi, DataEase, FineBI, Tableau, Power BI, and Apache Superset. Apache Doris can work as the data source for any BI tools that support the MySQL protocol.

### Storage engine

Apache Doris has a columnar storage engine, which encodes, compresses, and reads data by column. This enables a very high data compression ratio and largely reduces unnecessary data scanning, thus making more efficient use of IO and CPU resources.

Apache Doris supports various index structures to minimize data scans:

- **Sorted Compound Key Index**: Users can specify three columns at most to form a compound sort key. This can effectively prune data to better support highly concurrent reporting scenarios.

- **Min/Max Index**: This enables effective data filtering in equivalence and range queries of numeric types.

- **BloomFilter Index**: This is very effective in equivalence filtering and pruning of high-cardinality columns.

- **Inverted Index**: This enables fast searching for any field.

Apache Doris supports a variety of data models and has optimized them for different scenarios:

- **Detail Model (Duplicate Key Model):** A detail data model designed to meet the detailed storage requirements of fact tables.

- **Primary Key Model (Unique Key Model):** Ensures unique keys; data with the same key is overwritten, enabling row-level data updates.

- **Aggregate Model (Aggregate Key Model):** Merges value columns with the same key, significantly improving performance through pre-aggregation.

Apache Doris also supports strongly consistent single-table materialized views and asynchronously refreshed multi-table materialized views. Single-table materialized views are automatically refreshed and maintained by the system, requiring no manual intervention from users. Multi-table materialized views can be refreshed periodically using in-cluster scheduling or external scheduling tools, reducing the complexity of data modeling.

### 🔍 Query Engine

Apache Doris has an MPP-based query engine for parallel execution between and within nodes. It supports distributed shuffle join for large tables to better handle complicated queries.

<br />

![Query Engine](https://cdn.selectdb.com/static/What_is_Apache_Doris_1_c6f5ba2af9.png)

<br />

The query engine of Apache Doris is fully vectorized, with all memory structures laid out in a columnar format. This can largely reduce virtual function calls, increase cache hit rates, and make efficient use of SIMD instructions. Apache Doris delivers a 5~10 times higher performance in wide table aggregation scenarios than non-vectorized engines.

<br />

![Doris query engine](https://cdn.selectdb.com/static/What_is_Apache_Doris_2_29cf58cc6b.png)

<br />

Apache Doris uses adaptive query execution technology to dynamically adjust the execution plan based on runtime statistics. For example, it can generate a runtime filter and push it to the probe side. Specifically, it pushes the filters to the lowest-level scan node on the probe side, which largely reduces the data amount to be processed and increases join performance. The runtime filter of Apache Doris supports In/Min/Max/Bloom Filter.

Apache Doris uses a Pipeline execution engine that breaks down queries into multiple sub-tasks for parallel execution, fully leveraging multi-core CPU capabilities. It simultaneously addresses the thread explosion problem by limiting the number of query threads. The Pipeline execution engine reduces data copying and sharing, optimizes sorting and aggregation operations, thereby significantly improving query efficiency and throughput.

In terms of the optimizer, Apache Doris employs a combined optimization strategy of CBO (Cost-Based Optimizer), RBO (Rule-Based Optimizer), and HBO (History-Based Optimizer). RBO supports constant folding, subquery rewriting, predicate pushdown, and more. CBO supports join reordering and other optimizations. HBO recommends the optimal execution plan based on historical query information. These multiple optimization measures ensure that Doris can enumerate high-performance query plans across various types of queries.


## 🎆 Why choose Apache Doris?

- 🎯 **Easy to Use:** Two processes, no other dependencies; online cluster scaling, automatic replica recovery; compatible with MySQL protocol, and using standard SQL.

- 🚀 **High Performance:** Extremely fast performance for low-latency and high-throughput queries with columnar storage engine, modern MPP architecture, vectorized query engine, pre-aggregated materialized view and data index.

- 🖥️ **Single Unified:** A single system can support real-time data serving, interactive data analysis and offline data processing scenarios.

- ⚛️ **Federated Querying:** Supports federated querying of data lakes such as Hive, Iceberg, Hudi, and databases such as MySQL and Elasticsearch.

- ⏩ **Various Data Import Methods:** Supports batch import from HDFS/S3 and stream import from MySQL Binlog/Kafka; supports micro-batch writing through HTTP interface and real-time writing using Insert in JDBC.

- 🚙 **Rich Ecology:** Spark uses Spark-Doris-Connector to read and write Doris; Flink-Doris-Connector enables Flink CDC to implement exactly-once data writing to Doris; DBT Doris Adapter is provided to transform data in Doris with DBT.

## 🙌 Contributors

**Apache Doris has graduated from Apache incubator successfully and become a Top-Level Project in June 2022**. 

We deeply appreciate 🔗[community contributors](https://github.com/apache/doris/graphs/contributors) for their contribution to Apache Doris.

[![contrib graph](https://contrib.rocks/image?repo=apache/doris)](https://github.com/apache/doris/graphs/contributors)

## 👨‍👩‍👧‍👦 Users

Apache Doris now has a wide user base in China and around the world, and as of today, **Apache Doris is used in production environments in thousands of companies worldwide.** More than 80% of the top 50 Internet companies in China in terms of market capitalization or valuation have been using Apache Doris for a long time, including Baidu, Meituan, Xiaomi, Jingdong, Bytedance, Tencent, NetEase, Kwai, Sina, 360, Mihoyo, and Ke Holdings. It is also widely used in some traditional industries such as finance, energy, manufacturing, and telecommunications.

The users of Apache Doris: 🔗[Users](https://doris.apache.org/users)

Add your company logo at Apache Doris Website: 🔗[Add Your Company](https://github.com/apache/doris/discussions/27683)
 
## 👣 Get Started

### 📚 Docs

All Documentation   🔗[Docs](https://doris.apache.org/docs/gettingStarted/what-is-apache-doris)  

### ⬇️ Download 

All release and binary version 🔗[Download](https://doris.apache.org/download) 

### 🗄️ Compile

See how to compile  🔗[Compilation](https://doris.apache.org/community/source-install/compilation-with-docker))

### 📮 Install

See how to install and deploy 🔗[Installation and deployment](https://doris.apache.org/docs/install/preparation/env-checking) 

## 🧩 Components

### 📝 Doris Connector

Doris provides support for Spark/Flink to read data stored in Doris through Connector, and also supports to write data to Doris through Connector.

🔗[apache/doris-flink-connector](https://github.com/apache/doris-flink-connector)

🔗[apache/doris-spark-connector](https://github.com/apache/doris-spark-connector)


## 🌈 Community and Support

### 📤 Subscribe Mailing Lists

Mail List is the most recognized form of communication in Apache community. See how to 🔗[Subscribe Mailing Lists](https://doris.apache.org/community/subscribe-mail-list)

### 🙋 Report Issues or Submit Pull Request

If you meet any questions, feel free to file a 🔗[GitHub Issue](https://github.com/apache/doris/issues) or post it in 🔗[GitHub Discussion](https://github.com/apache/doris/discussions) and fix it by submitting a 🔗[Pull Request](https://github.com/apache/doris/pulls) 

### 🍻 How to Contribute

We welcome your suggestions, comments (including criticisms), comments and contributions. See 🔗[How to Contribute](https://doris.apache.org/community/how-to-contribute/) and 🔗[Code Submission Guide](https://doris.apache.org/community/how-to-contribute/pull-request/)

### ⌨️ Doris Improvement Proposals (DSIP)

🔗[Doris Improvement Proposal (DSIP)](https://cwiki.apache.org/confluence/display/DORIS/Doris+Improvement+Proposals) can be thought of as **A Collection of Design Documents for all Major Feature Updates or Improvements**.

### 🔑 Backend C++ Coding Specification
🔗 [Backend C++ Coding Specification](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=240883637) should be strictly followed, which will help us achieve better code quality.

## 💬 Contact Us

Contact us through the following mailing list.

| Name                                                                          | Scope                           |                                                                 |                                                                     |                                                                              |
|:------------------------------------------------------------------------------|:--------------------------------|:----------------------------------------------------------------|:--------------------------------------------------------------------|:-----------------------------------------------------------------------------|
| [dev@doris.apache.org](mailto:dev@doris.apache.org)     | Development-related discussions | [Subscribe](mailto:dev-subscribe@doris.apache.org)   | [Unsubscribe](mailto:dev-unsubscribe@doris.apache.org)   | [Archives](http://mail-archives.apache.org/mod_mbox/doris-dev/)   |

## 🧰 Links

* Apache Doris Official Website - [Site](https://doris.apache.org)
* Developer Mailing list - <dev@doris.apache.org>. Mail to <dev-subscribe@doris.apache.org>, follow the reply to subscribe the mail list.
* Slack channel - [Join the Slack](https://join.slack.com/t/apachedoriscommunity/shared_invite/zt-35mzao67o-BrpU70FNKPyB6UlgpXf8_w)
* Twitter - [Follow @doris_apache](https://twitter.com/doris_apache)


## 📜 License

[Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0)

> **Note**
> Some licenses of the third-party dependencies are not compatible with Apache 2.0 License. So you need to disable
some Doris features to be complied with Apache 2.0 License. For details, refer to the `thirdparty/LICENSE.txt`



