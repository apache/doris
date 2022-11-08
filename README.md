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
    <img src="https://doris.apache.org/assets/images/home-banner-7f193353c932af31634eca0a028f03ed.png" align="right" height="240"/>
</div>

# Apache Doris
[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)
[![GitHub release](https://img.shields.io/github/release/apache/doris.svg)](https://github.com/apache/doris/releases)
[![Jenkins Vec](https://img.shields.io/jenkins/tests?compact_message&jobUrl=https://ci-builds.apache.org/job/Doris/job/doris_daily_disable_vectorized&label=OriginEngine)](https://ci-builds.apache.org/job/Doris/job/doris_daily_disable_vectorized)
[![Jenkins Ori](https://img.shields.io/jenkins/tests?compact_message&jobUrl=https://ci-builds.apache.org/job/Doris/job/doris_daily_enable_vectorized&label=VectorizedEngine)](https://ci-builds.apache.org/job/Doris/job/doris_daily_enable_vectorized)
[![Total Lines](https://tokei.rs/b1/github/apache/doris?category=lines)](https://github.com/apache/doris)
[![Join the Doris Community at Slack](https://img.shields.io/badge/chat-slack-brightgreen)](https://join.slack.com/t/apachedoriscommunity/shared_invite/zt-1h153f1ar-sTJB_QahY1SHvZdtPFoIOQ)
[![Join the chat at https://gitter.im/apache-doris/Lobby](https://badges.gitter.im/apache-doris/Lobby.svg)](https://gitter.im/apache-doris/Lobby?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![EN doc](https://img.shields.io/badge/Docs-English-blue.svg)](https://doris.apache.org/docs/get-starting/)
[![CN doc](https://img.shields.io/badge/文档-中文版-blue.svg)](https://doris.apache.org/zh-CN/docs/get-starting/)
[![Twitter](https://img.shields.io/twitter/url/https/twitter.com/doris.svg?style=social&label=Follow%20%40doris_apache)](https://twitter.com/doris_apache)

Apache Doris is an easy-to-use, high-performance and real-time analytical database based on MPP architecture, known for its extreme speed and ease of use. It only requires a sub-second response time to return query results under massive data and can support not only high-concurrent point query scenarios but also high-throughput complex analysis scenarios.

Based on this, Apache Doris can better meet the scenarios of report analysis, ad-hoc query, unified data warehouse, Data Lake Query Acceleration, etc. Users can build user behavior analysis, AB test platform, log retrieval analysis, user portrait analysis, order analysis, and other applications on top of this.


🎉 Version 1.1.3 released now! It is also a LTS (long-term support) release and all users are encouraged to upgrade to this release. Check out the 🔗[Release Notes](https://doris.apache.org/docs/releasenotes/release-1.1.3) here. 

👀 Have a look at the 🔗[Official Website](https://doris.apache.org/) for a comprehensive list of Apache Doris's core features, blogs and user cases.

## 📈 Usage Scenarios

As shown in the figure below, after various data integration and processing, the data sources are usually stored in the real-time data warehouse Apache Doris and the offline data lake or data warehouse (in Apache Hive, Apache Iceberg or Apache Hudi).

<img src="https://dev-to-uploads.s3.amazonaws.com/uploads/articles/sekvbs5ih5rb16wz6n9k.png">

Apache Doris is widely used in the following scenarios:

- Reporting Analysis

    - Real-time Dashboards
    - Reports for in-house analysts and managers
    - Highly concurrent user-oriented or customer-oriented report analysis.For example, in the scenarios of site analysis for website owners and advertising reports for advertisers, the concurrency usually requires thousands of QPS and the query latency requires sub-seconds response. The famous e-commerce company JD.com uses Doris in advertising reports, writing 10 billion rows of data per day, with tens of thousands of concurrent query QPS and 150ms query latency for the 99th percentile.

- Ad-Hoc Query. Analyst-oriented self-service analytics with irregular query patterns and high throughput requirements. XiaoMi has built a growth analytics platform (Growth Analytics, GA) based on Doris, using user behavior data for business growth analysis, with an average query latency of 10 seconds and a 95th percentile query latency of 30 seconds or less, and tens of thousands of SQL queries per day.

- Unified data warehouse construction. A platform to meet the needs of unified data warehouse construction and simplify the complicated data software stack. HaiDiLao's Doris-based unified data warehouse replaces the old architecture consisting of Apache Spark, Apache Hive, Apache Kudu, Apache HBase, and Apache Phoenix, and greatly simplifies the architecture.

- Data Lake Query. By federating the data located in Apache Hive, Apache Iceberg, and Apache Hudi using external tables, the query performance is greatly improved while avoiding data copying.

## 🖥️ Core Concepts

### 📂 Architecture of Apache Doris

The overall architecture of Apache Doris is shown in the following figure. The Doris architecture is very simple, with only two types of processes.

- Frontend（FE）: It is mainly responsible for user request access, query parsing and planning, management of metadata, and node management-related work.

- Backend（BE）: It is mainly responsible for data storage and query plan execution.

Both types of processes are horizontally scalable, and a single cluster can support up to hundreds of machines and tens of petabytes of storage capacity. And these two types of processes guarantee high availability of services and high reliability of data through consistency protocols. This highly integrated architecture design greatly reduces the operation and maintenance cost of a distributed system.

![The overall architecture of Apache Doris](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/mnz20ae3s23vv3e9ltmi.png)

Apache Doris adopts MySQL protocol, highly compatible with MySQL dialect, and supports standard SQL. Users can access Doris through various client tools and support seamless connection with BI tools.

### 💾 Storage Engine

In terms of the storage engine, Apache Doris uses columnar storage to encode and compress and read data by column, enabling a very high compression ratio while reducing a large number of scans of non-relevant data, thus making more efficient use of IO and CPU resources. Doris also supports a relatively rich index structure to reduce data scans:

- Support sorted compound key index: Up to three columns can be specified to form a compound sort key. With this index, data can be effectively pruned to better support high concurrent reporting scenarios.
- Z-order index ：Using Z-order indexing, you can efficiently run range queries on any combination of fields in your schema.
- MIN/MAX index: Effective filtering of equivalence and range queries for numeric types
- Bloom Filter index: very effective for equivalence filtering and pruning of high cardinality columns
- Invert index: It enables the fast search of any field.


### 💿 Storage Models

In terms of storage models, Apache Doris supports a variety of storage models, with specific optimizations for different scenarios:

- Aggregate Key model: Merge the value columns with the same keys, by aggregating in advance to significantly improve performance.

- Unique Key model: The key is unique. Data with the same key will be overwritten to achieve row-level data updates.

- Duplicate Key model: The detailed data model can satisfy the detailed storage of fact tables.

Apache Doris also supports strong consistent materialized views, where updates and selections of materialized views are made automatically within the system and do not require manual selection by the user, thus significantly reducing the cost of materialized view maintenance.

### 🔍 Query Engine

In terms of query engine, Apache Doris adopts the MPP model, with parallel execution between and within nodes, and also supports distributed shuffle join for multiple large tables, which can better cope with complex queries.

![](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/vjlmumwyx728uymsgcw0.png)

The Doris query engine is vectorized, and all memory structures can be laid out in a columnar format to achieve significant reductions in virtual function calls, improved Cache hit rates, and efficient use of SIMD instructions. Performance in wide table aggregation scenarios is 5–10 times higher than in non-vectorized engines.

![](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/ck2m3kbnodn28t28vphp.png)

Apache Doris uses Adaptive Query Execution technology, which can dynamically adjust the execution plan based on runtime statistics, such as runtime filter technology to generate filters to push to the probe side at runtime and to automatically penetrate the filters to the probe side which drastically reduces the amount of data in the probe and speeds up join performance. Doris' runtime filter supports In/Min/Max/Bloom filter.

### 🚅 Query Optimizer

In terms of the optimizer, Doris uses a combination of CBO and RBO, with RBO supporting constant folding, subquery rewriting, predicate pushdown, etc., and CBO supporting Join Reorder. CBO is still under continuous optimization, mainly focusing on more accurate statistical information collection and derivation, more accurate cost model prediction, etc.


**Technical Overview**: 🔗[Introduction to Apache Doris](https://doris.apache.org/docs/summary/basic-summary)

## 🎆 Why choose Apache Doris?

- 🎯 **Easy to Use:** Two processes, no other dependencies; online cluster scaling, automatic replica recovery; compatible with MySQL protocol, and using standard SQL.

- 🚀 **High Performance:** Extremely fast performance for low-latency and high-throughput queries with columnar storage engine, modern MPP architecture, vectorized query engine, pre-aggregated materialized view and data index.

- 🖥️ **Single Unified:** A single system can support real-time data serving, interactive data analysis and offline data processing scenarios.

- ⚛️ **Federated Querying:** Supports federated querying of data lakes such as Hive, Iceberg, Hudi, and databases such as MySQL and Elasticsearch.

- ⏩ **Various Data Import Methods:** Supports batch import from HDFS/S3 and stream import from MySQL Binlog/Kafka; supports micro-batch writing through HTTP interface and real-time writing using Insert in JDBC.

- 🚙 **Rich Ecology:** Spark uses Spark-Doris-Connector to read and write Doris; Flink-Doris-Connector enables Flink CDC to implement exactly-once data writing to Doris; DBT Doris Adapter is provided to transform data in Doris with DBT.

## 🙌 Contributors

**Apache Doris has graduated from Apache incubator successfully and become a Top-Level Project in June 2022**. 

Currently, the Apache Doris community has gathered more than 350 contributors from nearly 100 companies in different industries, and the number of active contributors is close to 100 per month.


[![Monthly Active Contributors](https://contributor-overtime-api.apiseven.com/contributors-svg?chart=contributorMonthlyActivity&repo=apache/doris)](https://www.apiseven.com/en/contributor-graph?chart=contributorMonthlyActivity&repo=apache/doris)

[![Contributor over time](https://contributor-overtime-api.apiseven.com/contributors-svg?chart=contributorOverTime&repo=apache/doris)](https://www.apiseven.com/en/contributor-graph?chart=contributorOverTime&repo=apache/doris)

We deeply appreciate 🔗[community contributors](https://github.com/apache/doris/graphs/contributors) for their contribution to Apache Doris.

## 👨‍👩‍👧‍👦 Users

Apache Doris now has a wide user base in China and around the world, and as of today, Apache Doris is used in production environments in over 700 companies worldwide. More than 80% of the top 50 Internet companies in China in terms of market capitalization or valuation have been using Apache Doris for a long time, including Baidu, Meituan, Xiaomi, Jingdong, Bytedance, Tencent, NetEase, Kwai, Weibo, and Ke Holdings. It is also widely used in some traditional industries such as finance, energy, manufacturing, and telecommunications.

The users of Apache Doris: 🔗[https://doris.apache.org/users](https://doris.apache.org/users)

Add your company logo at Apache Doris Website: 🔗[Add Your Company](https://github.com/apache/doris/issues/10229)
 
## 👣 Get Started

### 📚 Docs

All Documentation   🔗[Docs](https://doris.apache.org/docs/get-starting/)  

### ⬇️ Download 

All release and binary version 🔗[Download](https://doris.apache.org/download) 

### 🗄️ Compile

See how to compile  🔗[Compilation](https://doris.apache.org/docs/install/source-install/compilation)

### 📮 Install

See how to install and deploy 🔗[Installation and deployment](https://doris.apache.org/docs/install/install-deploy) 

## 🧩 Components

### 📝 Doris Connector

Doris provides support for Spark/Flink to read data stored in Doris through Connector, and also supports to write data to Doris through Connector.

🔗[apache/doris-flink-connector](https://github.com/apache/doris-flink-connector)

🔗[apache/doris-spark-connector](https://github.com/apache/doris-spark-connector)

### 🛠 Doris Manager 

Doris provides one-click visual automatic installation and deployment, cluster management and monitoring tools for clusters.

🔗[apache/doris-manager](https://github.com/apache/doris-manager)

## 🌈 Community and Support

### 📤 Subscribe Mailing Lists

Mail List is the most recognized form of communication in Apache community. See how to 🔗[Subscribe Mailing Lists](https://doris.apache.org/community/subscribe-mail-list)

### 🙋 Report Issues or Submit Pull Request

If you meet any questions, feel free to file a 🔗[GitHub Issue](https://github.com/apache/doris/issues) or post it in 🔗[GitHub Discussion](https://github.com/apache/doris/discussions) and fix it by submitting a 🔗[Pull Request](https://github.com/apache/doris/pulls) 

### 🍻 How to Contribute

We welcome your suggestions, comments (including criticisms), comments and contributions. See 🔗[How to Contribute](https://doris.apache.org/community/how-to-contribute/) and 🔗[Code Submission Guide](https://doris.apache.org/community/how-to-contribute/pull-request/)

### ⌨️ Doris Improvement Proposals (DSIP)

🔗[Doris Improvement Proposal (DSIP)](https://cwiki.apache.org/confluence/display/DORIS/Doris+Improvement+Proposals) can be thought of as **A Collection of Design Documents for all Major Feature Updates or Improvements**.




## 💬 Contact Us

Contact us through the following mailing list.

| Name                                                                          | Scope                           |                                                                 |                                                                     |                                                                              |
|:------------------------------------------------------------------------------|:--------------------------------|:----------------------------------------------------------------|:--------------------------------------------------------------------|:-----------------------------------------------------------------------------|
| [dev@doris.apache.org](mailto:dev@doris.apache.org)     | Development-related discussions | [Subscribe](mailto:dev-subscribe@doris.apache.org)   | [Unsubscribe](mailto:dev-unsubscribe@doris.apache.org)   | [Archives](http://mail-archives.apache.org/mod_mbox/doris-dev/)   |

## 🧰 Links

* Apache Doris Official Website - [https://doris.apache.org](https://doris.apache.org)
* Developer Mailing list - <dev@doris.apache.org>. Mail to <dev-subscribe@doris.apache.org>, follow the reply to subscribe the mail list.
* Slack channel - [Join the Slack](https://join.slack.com/t/apachedoriscommunity/shared_invite/zt-1h153f1ar-sTJB_QahY1SHvZdtPFoIOQ)
* Twitter - [Follow @doris_apache](https://twitter.com/doris_apache)


## 📜 License

[Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0)

> **Note**
> Some licenses of the third-party dependencies are not compatible with Apache 2.0 License. So you need to disable
some Doris features to be complied with Apache 2.0 License. For details, refer to the `thirdparty/LICENSE.txt`



