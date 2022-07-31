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

<a href="https://doris.apache.org/">
    <img src="https://doris.apache.org/images/logo.svg" alt="doris logo" title="doris" align="right" height="40" />
</a>

# Apache Doris
[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)
[![GitHub release](https://img.shields.io/github/release/apache/doris.svg)](https://github.com/apache/doris/releases)
[![Total Lines](https://tokei.rs/b1/github/apache/doris?category=lines)](https://github.com/apache/doris)
[![Join the Doris Community at Slack](https://img.shields.io/badge/chat-slack-brightgreen)](https://join.slack.com/t/apachedoriscommunity/shared_invite/zt-11jb8gesh-7IukzSrdea6mqoG0HB4gZg)
[![Join the chat at https://gitter.im/apache-doris/Lobby](https://badges.gitter.im/apache-doris/Lobby.svg)](https://gitter.im/apache-doris/Lobby?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![EN doc](https://img.shields.io/badge/Docs-English-blue.svg)](https://doris.apache.org/docs/get-starting/)
[![CN doc](https://img.shields.io/badge/文档-中文版-blue.svg)](https://doris.apache.org/zh-CN/docs/get-starting/)
[![Twitter](https://img.shields.io/twitter/url/https/twitter.com/doris.svg?style=social&label=Follow%20%40ApacheDoris)](https://twitter.com/doris_apache)

Apache Doris is an easy-to-use, high-performance and real-time analytical database based on MPP architecture, known for its extreme speed and ease of use. It only requires a sub-second response time to return query results under massive data and can support not only high-concurrent point query scenarios but also high-throughput complex analysis scenarios.

Based on this, Apache Doris can better meet the scenarios of report analysis, ad-hoc query, unified data warehouse, Data Lake Query Acceleration, etc. Users can build user behavior analysis, AB test platform, log retrieval analysis, user portrait analysis, order analysis, and other applications on top of this.

![Image description](https://doris.apache.org/assets/images/what-is-doris-2ed5ac7fffa3799871d5d33993b1de09.png)

**Apache Doris Official Website:** 🔗[https://doris.apache.org/](https://doris.apache.org/)

**Technical Overview**: 🔗[Introduction to Apache Doris](https://doris.apache.org/docs/summary/basic-summary)

## Core Features

- **Easy to Use:** Two processes, no other dependencies; online cluster scaling, automatic replica recovery; compatible with MySQL protocol, and using standard SQL.

- **High Performance:** Extremely fast performance for low-latency and high-throughput queries with columnar storage engine, modern MPP architecture, vectorized query engine, pre-aggregated materialized view and data index.

- **Single Unified:** A single system can support real-time data serving, interactive data analysis and offline data processing scenarios.

- **Federated Querying:** Supports federated querying of data lakes such as Hive, Iceberg, Hudi, and databases such as MySQL and Elasticsearch.

- **Various Data Import Methods:** Supports batch import from HDFS/S3 and stream import from MySQL Binlog/Kafka; supports micro-batch writing through HTTP interface and real-time writing using Insert in JDBC.

- **Rich Ecology:** Spark uses Spark Doris Connector to read and write Doris; Flink Doris Connector enables Flink CDC to implement exactly-once data writing to Doris; DBT Doris Adapter is provided to transform data in Doris with DBT.

## Contributors

**Apache Doris has graduated from Apache incubator successfully and become a Top-Level Project in June 2022**. 

Currently, the Apache Doris community has gathered more than 300 contributors from nearly 100 companies in different industries, and the number of active contributors is close to 100 per month.


[![Monthly Active Contributors](https://contributor-overtime-api.apiseven.com/contributors-svg?chart=contributorMonthlyActivity&repo=apache/doris)](https://www.apiseven.com/en/contributor-graph?chart=contributorMonthlyActivity&repo=apache/doris)

[![Contributor over time](https://contributor-overtime-api.apiseven.com/contributors-svg?chart=contributorOverTime&repo=apache/doris)](https://www.apiseven.com/en/contributor-graph?chart=contributorOverTime&repo=apache/doris)

We deeply appreciate 🔗[community contributors](https://github.com/apache/doris/graphs/contributors) for their dedication to Apache Doris.

## Users

Apache Doris now has a wide user base in China and around the world, and as of today, Apache Doris is used in production environments in over 500 companies worldwide. More than 80% of the top 50 Internet companies in China in terms of market capitalization or valuation have been using Apache Doris for a long time, including Baidu, Meituan, Xiaomi, Jingdong, Bytedance, Tencent, NetEase, Kwai, Weibo, and Ke Holdings. It is also widely used in some traditional industries such as finance, energy, manufacturing, and telecommunications.

The users of Apache Doris: 🔗[https://doris.apache.org/users](https://doris.apache.org/users)

Add your company logo at Apache Doris Website: 🔗[Add Your Company](https://github.com/apache/doris/issues/10229)
 
## Get Started

### Docs

All Documentation   🔗[Docs](https://doris.apache.org/docs/get-starting/)  

### Download 

All release and binary version 🔗[Download](https://doris.apache.org/download) 

### Compile

See how to compile  🔗[Compilation](https://doris.apache.org/docs/install/source-install/compilation)

### Install

See how to install and deploy 🔗[Installation and deployment](https://doris.apache.org/docs/install/install-deploy) 

## Components

### Doris Connector

Doris provides support for Spark/Flink to read data stored in Doris through Connector, and also supports to write data to Doris through Connector.

🔗[apache/doris-flink-connector](https://github.com/apache/doris-flink-connector)

🔗[apache/doris-spark-connector](https://github.com/apache/doris-spark-connector)

### Doris Manager 

Doris provides one-click visual automatic installation and deployment, cluster management and monitoring tools for clusters.

🔗[apache/doris-manager](https://github.com/apache/doris-manager)

## Community and Support

### Subscribe Mailing Lists

Mail List is the most recognized form of communication in Apache community. See how to 🔗[Subscribe Mailing Lists](https://doris.apache.org/community/subscribe-mail-list)

### Report Issues or Submit Pull Request

If you meet any questions, feel free to file a 🔗[GitHub Issue](https://github.com/apache/doris/issues) or post it in 🔗[GitHub Discussion](https://github.com/apache/doris/discussions) and fix it by submitting a 🔗[Pull Request](https://github.com/apache/doris/pulls) 


### How to Contribute

We welcome your suggestions, comments (including criticisms), comments and contributions. See 🔗[How to Contribute](https://doris.apache.org/community/how-to-contribute/) and 🔗[Code Submission Guide](https://doris.apache.org/community/how-to-contribute/pull-request/)

### Doris Improvement Proposals (DSIP)

🔗[Doris Improvement Proposal (DSIP)](https://cwiki.apache.org/confluence/display/DORIS/Doris+Improvement+Proposals) can be thought of as **A Collection of Design Documents for all Major Feature Updates or Improvements**.




## Contact Us

Contact us through the following mailing list.

| Name                                                                          | Scope                           |                                                                 |                                                                     |                                                                              |
|:------------------------------------------------------------------------------|:--------------------------------|:----------------------------------------------------------------|:--------------------------------------------------------------------|:-----------------------------------------------------------------------------|
| [dev@doris.apache.org](mailto:dev@doris.apache.org)     | Development-related discussions | [Subscribe](mailto:dev-subscribe@doris.apache.org)   | [Unsubscribe](mailto:dev-unsubscribe@doris.apache.org)   | [Archives](http://mail-archives.apache.org/mod_mbox/doris-dev/)   |

## Links

* Apache Doris Official Website  <https://doris.apache.org
* Developer Mailing list - <dev@doris.apache.org>. Mail to <dev-subscribe@doris.apache.org>, follow the reply to subscribe the mail list.
* Slack channel - [Join the Slack](https://join.slack.com/t/apachedoriscommunity/shared_invite/zt-18u6vjopj-Th15vTVfmCzVfhhL5rz26A)
* Twitter - [Follow doris_apache](https://twitter.com/doris_apache)


## License

[Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0)

> **Note**
> Some licenses of the third-party dependencies are not compatible with Apache 2.0 License. So you need to disable
some Doris features to be complied with Apache 2.0 License. For details, refer to the `thirdparty/LICENSE.txt`



