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

- Doris provides high concurrent low latency point query performance, as well as high throughput queries of ad-hoc analysis.

- Doris provides batch data loading and real-time mini-batch data loading.

- Doris provides high availability, reliability, fault tolerance, and scalability.

The main advantages of Doris are the simplicity (of developing, deploying and using) and meeting many data serving requirements in a single system. For details, refer to [Overview](https://github.com/apache/incubator-doris/wiki/Doris-Overview).

**Official website: https://doris.apache.org/**

[![Monthly Active Contributors](https://contributor-overtime-api.apiseven.com/contributors-svg?chart=contributorMonthlyActivity&repo=apache/incubator-doris)](https://www.apiseven.com/en/contributor-graph?chart=contributorMonthlyActivity&repo=apache/incubator-doris)

[![Contributor over time](https://contributor-overtime-api.apiseven.com/contributors-svg?chart=contributorOverTime&repo=apache/incubator-doris)](https://www.apiseven.com/en/contributor-graph?chart=contributorOverTime&repo=apache/incubator-doris)

## License

[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)

> **Note**
> Some licenses of the third-party dependencies are not compatible with Apache 2.0 License. So you need to disable
some Doris features to be complied with Apache 2.0 License. For details, refer to the `thirdparty/LICENSE.txt`

## Technology

Doris mainly integrates the technology of [Google Mesa](https://research.google/pubs/pub42851/) and [Apache Impala](https://impala.apache.org/), and it is based on a column-oriented storage engine and can communicate by MySQL client.

## Compile and install

See [Compilation](https://github.com/apache/incubator-doris/blob/master/docs/en/installing/compilation.md) for details.

## Report issues or submit pull request

If you find any bugs, feel free to file a [GitHub issue](https://github.com/apache/incubator-doris/issues) or fix it by submitting a [pull request](https://github.com/apache/incubator-doris/pulls).

## Contact Us

Contact us through the following mailing list.

| Name                                                                          | Scope                           |                                                                 |                                                                     |                                                                              |
|:------------------------------------------------------------------------------|:--------------------------------|:----------------------------------------------------------------|:--------------------------------------------------------------------|:-----------------------------------------------------------------------------|
| [dev@doris.apache.org](mailto:dev@doris.apache.org)     | Development-related discussions | [Subscribe](mailto:dev-subscribe@doris.apache.org)   | [Unsubscribe](mailto:dev-unsubscribe@doris.apache.org)   | [Archives](http://mail-archives.apache.org/mod_mbox/doris-dev/)   |

## Links

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
