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
[![Jenkins Vec](https://img.shields.io/jenkins/tests?compact_message&jobUrl=https://ci-builds.apache.org/job/Doris/job/doris_daily_enable_vectorized&label=VectorizedEngine)](https://ci-builds.apache.org/job/Doris/job/doris_daily_enable_vectorized)
[![Total Lines](https://tokei.rs/b1/github/apache/doris?category=lines)](https://github.com/apache/doris)
[![Join the Doris Community at Slack](https://img.shields.io/badge/chat-slack-brightgreen)](https://join.slack.com/t/apachedoriscommunity/shared_invite/zt-1x7x8fger-F7NoshFQn~djlvGdnEtxUQ)
[![Join the chat at https://gitter.im/apache-doris/Lobby](https://badges.gitter.im/apache-doris/Lobby.svg)](https://gitter.im/apache-doris/Lobby?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![EN doc](https://img.shields.io/badge/Docs-English-blue.svg)](https://doris.apache.org/docs/dev/summary/basic-summary/)
[![CN doc](https://img.shields.io/badge/文档-中文版-blue.svg)](https://doris.apache.org/zh-CN/docs/dev/summary/basic-summary)
[![Twitter](https://img.shields.io/twitter/url/https/twitter.com/doris.svg?style=social&label=Follow%20%40doris_apache)](https://twitter.com/doris_apache)

Apache Doris is an easy-to-use, high-performance and real-time analytical database based on MPP architecture, known for its extreme speed and ease of use. It only requires a sub-second response time to return query results under massive data and can support not only high-concurrent point query scenarios but also high-throughput complex analysis scenarios.

All this makes Apache Doris an ideal tool for scenarios including report analysis, ad-hoc query, unified data warehouse, and data lake query acceleration. On Apache Doris, users can build various applications, such as user behavior analysis, AB test platform, log retrieval analysis, user portrait analysis, and order analysis.

🎉 Version 2.0.0 version released now. The 2.0.0 version has achieved over 10x performance improvements on standard Benchmark, comprehensive enhancement in log analysis and lakehouse scenarios, more efficient and stable data update and write efficiency, support for more comprehensive multi-tenant and resource isolation mechanisms, and take a new step in the direction of resource elasticity and storage computing separation. It has also been added a series of usability features for enterprise users. We welcome all users who have requirements for the new features of the 2.0 version to deploy and upgrade. Check out the 🔗[Release Notes](https://github.com/apache/doris/issues/22647) here. 

🎉 Version 1.2.6 released now! It is fully evolved release and all users are encouraged to upgrade to this release. Check out the 🔗[Release Notes](https://doris.apache.org/docs/dev/releasenotes/release-1.2.6) here. 

🎉 Version 1.1.5 released now. It is a stability improvement and bugfix release based on version 1.1. Check out the 🔗[Release Notes](https://doris.apache.org/docs/dev/releasenotes/release-1.1.5) here. 

👀 Have a look at the 🔗[Official Website](https://doris.apache.org/) for a comprehensive list of Apache Doris's core features, blogs and user cases.

## 📈 Usage Scenarios

As shown in the figure below, after various data integration and processing, the data sources are usually stored in the real-time data warehouse Apache Doris and the offline data lake or data warehouse (in Apache Hive, Apache Iceberg or Apache Hudi).

<img src="https://dev-to-uploads.s3.amazonaws.com/uploads/articles/sekvbs5ih5rb16wz6n9k.png">

Apache Doris is widely used in the following scenarios:

- Reporting Analysis

    - Real-time dashboards
    - Reports for in-house analysts and managers
    - Highly concurrent user-oriented or customer-oriented report analysis: such as website analysis and ad reporting that usually require thousands of QPS and quick response times measured in milliseconds. A successful user case is that Doris has been used by the Chinese e-commerce giant JD.com in ad reporting, where it receives 10 billion rows of data per day, handles over 10,000 QPS, and delivers a 99 percentile query latency of 150 ms.

- Ad-Hoc Query. Analyst-oriented self-service analytics with irregular query patterns and high throughput requirements. XiaoMi has built a growth analytics platform (Growth Analytics, GA) based on Doris, using user behavior data for business growth analysis, with an average query latency of 10 seconds and a 95th percentile query latency of 30 seconds or less, and tens of thousands of SQL queries per day.

- Unified Data Warehouse Construction. Apache Doris allows users to build a unified data warehouse via one single platform and save the trouble of handling complicated software stacks. Chinese hot pot chain Haidilao has built a unified data warehouse with Doris to replace its old complex architecture consisting of Apache Spark, Apache Hive, Apache Kudu, Apache HBase, and Apache Phoenix.

- Data Lake Query. Apache Doris avoids data copying by federating the data in Apache Hive, Apache Iceberg, and Apache Hudi using external tables, and thus achieves outstanding query performance.

## 🖥️ Core Concepts

### 📂 Architecture of Apache Doris

The overall architecture of Apache Doris is shown in the following figure. The Doris architecture is very simple, with only two types of processes.

- Frontend (FE): user request access, query parsing and planning, metadata management, node management, etc.

- Backend (BE): data storage and query plan execution

Both types of processes are horizontally scalable, and a single cluster can support up to hundreds of machines and tens of petabytes of storage capacity. And these two types of processes guarantee high availability of services and high reliability of data through consistency protocols. This highly integrated architecture design greatly reduces the operation and maintenance cost of a distributed system.

![The overall architecture of Apache Doris](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/mnz20ae3s23vv3e9ltmi.png)

In terms of interfaces, Apache Doris adopts MySQL protocol, supports standard SQL, and is highly compatible with MySQL dialect. Users can access Doris through various client tools and it supports seamless connection with BI tools.

### 💾 Storage Engine

Doris uses a columnar storage engine, which encodes, compresses, and reads data by column. This enables a very high compression ratio and largely reduces irrelavant data scans, thus making more efficient use of IO and CPU resources. Doris supports various index structures to minimize data scans:

- Sorted Compound Key Index: Users can specify three columns at most to form a compound sort key. This can effectively prune data to better support highly concurrent reporting scenarios.
- Z-order Index: This allows users to efficiently run range queries on any combination of fields in their schema.
- MIN/MAX Indexing: This enables effective filtering of equivalence and range queries for numeric types.
- Bloom Filter: very effective in equivalence filtering and pruning of high cardinality columns
- Invert Index: This enables fast search for any field.


### 💿 Storage Models

Doris supports a variety of storage models and has optimized them for different scenarios:

- Aggregate Key Model: able to merge the value columns with the same keys and significantly improve performance

- Unique Key Model: Keys are unique in this model and data with the same key will be overwritten to achieve row-level data updates.

- Duplicate Key Model: This is a detailed data model capable of detailed storage of fact tables.

Doris also supports strongly consistent materialized views. Materialized views are automatically selected and updated, which greatly reduces maintenance costs for users.

### 🔍 Query Engine

Doris adopts the MPP model in its query engine to realize parallel execution between and within nodes. It also supports distributed shuffle join for multiple large tables so as to handle complex queries.

![](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/vjlmumwyx728uymsgcw0.png)

The Doris query engine is vectorized, with all memory structures laid out in a columnar format. This can largely reduce virtual function calls, improve cache hit rates, and make efficient use of SIMD instructions. Doris delivers a 5–10 times higher performance in wide table aggregation scenarios than non-vectorized engines.

![](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/ck2m3kbnodn28t28vphp.png)

Apache Doris uses Adaptive Query Execution technology to dynamically adjust the execution plan based on runtime statistics. For example, it can generate runtime filter, push it to the probe side, and automatically penetrate it to the Scan node at the bottom, which drastically reduces the amount of data in the probe and increases join performance. The runtime filter in Doris supports In/Min/Max/Bloom filter.

### 🚅 Query Optimizer

In terms of optimizers, Doris uses a combination of CBO and RBO. RBO supports constant folding, subquery rewriting, predicate pushdown and CBO supports Join Reorder. The Doris CBO is under continuous optimization for more accurate statistical information collection and derivation, and more accurate cost model prediction.


**Technical Overview**: 🔗[Introduction to Apache Doris](https://doris.apache.org/docs/dev/summary/basic-summary)

## 🎆 Why choose Apache Doris?

- 🎯 **Easy to Use:** Two processes, no other dependencies; online cluster scaling, automatic replica recovery; compatible with MySQL protocol, and using standard SQL.

- 🚀 **High Performance:** Extremely fast performance for low-latency and high-throughput queries with columnar storage engine, modern MPP architecture, vectorized query engine, pre-aggregated materialized view and data index.

- 🖥️ **Single Unified:** A single system can support real-time data serving, interactive data analysis and offline data processing scenarios.

- ⚛️ **Federated Querying:** Supports federated querying of data lakes such as Hive, Iceberg, Hudi, and databases such as MySQL and Elasticsearch.

- ⏩ **Various Data Import Methods:** Supports batch import from HDFS/S3 and stream import from MySQL Binlog/Kafka; supports micro-batch writing through HTTP interface and real-time writing using Insert in JDBC.

- 🚙 **Rich Ecology:** Spark uses Spark-Doris-Connector to read and write Doris; Flink-Doris-Connector enables Flink CDC to implement exactly-once data writing to Doris; DBT Doris Adapter is provided to transform data in Doris with DBT.

## 🙌 Contributors

**Apache Doris has graduated from Apache incubator successfully and become a Top-Level Project in June 2022**. 

Currently, the Apache Doris community has gathered more than 400 contributors from nearly 200 companies in different industries, and the number of active contributors is close to 100 per month.


[![Monthly Active Contributors](https://contributor-overtime-api.apiseven.com/contributors-svg?chart=contributorMonthlyActivity&repo=apache/doris)](https://www.apiseven.com/en/contributor-graph?chart=contributorMonthlyActivity&repo=apache/doris)

[![Contributor over time](https://contributor-overtime-api.apiseven.com/contributors-svg?chart=contributorOverTime&repo=apache/doris)](https://www.apiseven.com/en/contributor-graph?chart=contributorOverTime&repo=apache/doris)

We deeply appreciate 🔗[community contributors](https://github.com/apache/doris/graphs/contributors) for their contribution to Apache Doris.

## 👨‍👩‍👧‍👦 Users

Apache Doris now has a wide user base in China and around the world, and as of today, **Apache Doris is used in production environments in thousands of companies worldwide.** More than 80% of the top 50 Internet companies in China in terms of market capitalization or valuation have been using Apache Doris for a long time, including Baidu, Meituan, Xiaomi, Jingdong, Bytedance, Tencent, NetEase, Kwai, Sina, 360, Mihoyo, and Ke Holdings. It is also widely used in some traditional industries such as finance, energy, manufacturing, and telecommunications.

The users of Apache Doris: 🔗[Users](https://doris.apache.org/users)

Add your company logo at Apache Doris Website: 🔗[Add Your Company](https://github.com/apache/doris/issues/10229)
 
## 👣 Get Started

### 📚 Docs

All Documentation   🔗[Docs](https://doris.apache.org/docs/dev/get-starting/)  

### ⬇️ Download 

All release and binary version 🔗[Download](https://doris.apache.org/download) 

### 🗄️ Compile

See how to compile  🔗[Compilation](https://doris.apache.org/docs/dev/install/source-install/compilation-general)

### 📮 Install

See how to install and deploy 🔗[Installation and deployment](https://doris.apache.org/docs/dev/install/standard-deployment) 

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
* Slack channel - [Join the Slack](https://join.slack.com/t/apachedoriscommunity/shared_invite/zt-1x7x8fger-F7NoshFQn~djlvGdnEtxUQ)
* Twitter - [Follow @doris_apache](https://twitter.com/doris_apache)


## 📜 License

[Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0)

> **Note**
> Some licenses of the third-party dependencies are not compatible with Apache 2.0 License. So you need to disable
some Doris features to be complied with Apache 2.0 License. For details, refer to the `thirdparty/LICENSE.txt`



