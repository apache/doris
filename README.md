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

# Apache Doris (incubating)
[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)
[![Total Lines](https://tokei.rs/b1/github/apache/incubator-doris?category=lines)](https://github.com/apache/incubator-doris)
[![GitHub release](https://img.shields.io/github/release/apache/incubator-doris.svg)](https://github.com/apache/incubator-doris/releases)
[![Join the chat at https://gitter.im/apache-doris/Lobby](https://badges.gitter.im/apache-doris/Lobby.svg)](https://gitter.im/apache-doris/Lobby?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

Doris is an MPP-based interactive SQL data warehousing for reporting and analysis.
Its original name was Palo, developed in Baidu. After donated to Apache Software Foundation, it was renamed Doris.

**Official website: https://doris.apache.org/**

[![Monthly Active Contributors](https://contributor-overtime-api.apiseven.com/contributors-svg?chart=contributorMonthlyActivity&repo=apache/incubator-doris)](https://www.apiseven.com/en/contributor-graph?chart=contributorMonthlyActivity&repo=apache/incubator-doris)

[![Contributor over time](https://contributor-overtime-api.apiseven.com/contributors-svg?chart=contributorOverTime&repo=apache/incubator-doris)](https://www.apiseven.com/en/contributor-graph?chart=contributorOverTime&repo=apache/incubator-doris)

## 1. License

[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)

## 2. Technology
Doris mainly integrates the technology of Google Mesa and Apache Impala, and it is based on a column-oriented storage engine and can communicate by MySQL client.

## 3. User cases
Doris not only provides high concurrent low latency point query performance, but also provides high throughput queries of ad-hoc analysis.

Doris not only provides batch data loading, but also provides near real-time mini-batch data loading.

Doris also provides high availability, reliability, fault tolerance, and scalability.

The simplicity (of developing, deploying and using) and meeting many data serving requirements in single system are the main features of Doris (refer to [Overview](https://github.com/apache/incubator-doris/wiki/Doris-Overview)).

## 4. Compile and install

See [Compilation](https://github.com/apache/incubator-doris/blob/master/docs/en/installing/compilation.md) for details.

## 5. License Notice

Some of the third-party dependencies' license are not compatible with Apache 2.0 License. So you may have to disable
some features of Doris to be complied with Apache 2.0 License. Details can be found in `thirdparty/LICENSE.txt`

## 6. Reporting Issues

If you find any bugs, please file a [GitHub issue](https://github.com/apache/incubator-doris/issues).

## 7. Contact Us

### Mailing lists

| Name                                                                          | Scope                           |                                                                 |                                                                     |                                                                              |
|:------------------------------------------------------------------------------|:--------------------------------|:----------------------------------------------------------------|:--------------------------------------------------------------------|:-----------------------------------------------------------------------------|
| [dev@doris.apache.org](mailto:dev@doris.apache.org)     | Development-related discussions | [Subscribe](mailto:dev-subscribe@doris.apache.org)   | [Unsubscribe](mailto:dev-unsubscribe@doris.apache.org)   | [Archives](http://mail-archives.apache.org/mod_mbox/doris-dev/)   |

## 8. Links

* Doris official site - <http://doris.incubator.apache.org>
* User Manual (GitHub Wiki) - <https://github.com/apache/incubator-doris/wiki>
* Developer Mailing list - <dev@doris.apache.org>. Mail to <dev-subscribe@doris.apache.org>, follow the reply to subscribe the mail list.
* Gitter channel - <https://gitter.im/apache-doris/Lobby> - Online chat room with Doris developers.
* Overview - <https://github.com/apache/incubator-doris/wiki/Doris-Overview>
* Compile and install - <https://github.com/apache/incubator-doris/wiki/Doris-Install>
* Getting start - <https://github.com/apache/incubator-doris/wiki/Getting-start>
* Deploy and Upgrade - <https://github.com/apache/incubator-doris/wiki/Doris-Deploy-%26-Upgrade>
* User Manual - <https://github.com/apache/incubator-doris/wiki/Doris-Create%2C-Load-and-Delete>
* FAQs - <https://github.com/apache/incubator-doris/wiki/Doris-FAQ>
