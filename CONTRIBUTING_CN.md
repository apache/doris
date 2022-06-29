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

# 为 Doris 做贡献

非常感谢您对 Doris 项目感兴趣，我们非常欢迎您对 Doris 项目的各种建议、意见（包括批评）、评论和贡献。

您对 Doris 的各种建议、意见、评论可以直接通过 GitHub 的 [Issues](https://github.com/apache/doris/issues/new/choose) 提出。

参与 Doris 项目并为其作出贡献的方法有很多：代码实现、测试编写、流程工具改进、文档完善等等。任何贡献我们都会非常欢迎，并将您加入贡献者列表，进一步，有了足够的贡献后，您还可以有机会成为 Apache 的 Commiter，拥有 Apache 邮箱，并被收录到 [Apache Commiter 列表中](http://people.apache.org/committer-index.html)。

任何问题，您都可以联系我们得到及时解答，联系方式包括 dev 邮件组，Slack 等。

## 初次接触

初次来到 Doris 社区，您可以：

* 关注 Doris [Github 代码库](https://github.com/apache/doris)
* 订阅我们的 [邮件列表](./subscribe-mail-list.md)；
* 加入 Doris 的 [Slack](https://join.slack.com/t/apachedoriscommunity/shared_invite/zt-11jb8gesh-7IukzSrdea6mqoG0HB4gZg)

通过以上方式及时了解 Doris 项目的开发动态并为您关注的话题发表意见。

## Doris 的代码和文档

正如您在 [GitHub] (https://github.com/apache/doris) 上看到的，Apache Doris 的代码库主要包括三部分：Frontend (FE), Backend (BE) 和 Broker (为了支持 HDFS 等外部存储系统上的文件读取)。文档主要是 Doris 网站和 GitHub 上的 wiki，还有运行 Doris 的时候的在线帮助手册。这些组件的详细情况参见下表：

| 组件名称 | 组件描述 | 相关语言 |
|--------|----------------------------|----------|
| [Frontend daemon (FE)](https://github.com/apache/doris)| 由“查询协调器”和“元数据管理器”组成 | Java|
| [Backend daemon (BE)](https://github.com/apache/doris) | 负责存储数据和执行查询片段 | C++|
| [Broker](https://github.com/apache/doris) | 读取 HDFS 数据到 Doris | Java |
| [Website](https://github.com/apache/doris-website) | Doris 网站 | Markdown |
| [GitHub Wiki](https://github.com/apache/doris/wiki) | Doris GitHub Wiki | Markdown |
| Doris 运行时 help 文档 | 运行 Doris 的时候的在线帮助手册 | Markdown |

## 改进文档

文档是您了解 Apache Doris 的最主要的方式，也是我们最需要帮助的地方！

浏览文档，可以加深您对 Doris 的了解，也可以帮助您理解 Doris 的功能和技术细节，如果您发现文档有问题，请及时联系我们；

如果您对改进文档的质量感兴趣，不论是修订一个页面的地址、更正一个链接、以及写一篇更优秀的入门文档，我们都非常欢迎！

我们的文档大多数是使用 markdown 格式编写的，您可以直接通过在 [GitHub] (https://github.com/apache/doris) 中的 `docs/` 中修改并提交文档变更。如果提交代码变更，可以参阅 [Pull Request](./pull-request.md)。

## 如果发现了一个 Bug 或问题

如果发现了一个 Bug 或问题，您可以直接通过 GitHub 的 [Issues](https://github.com/apache/doris/issues/new/choose) 提一个新的 Issue，我们会有人定期处理。

您也可以通过阅读分析代码自己修复（当然在这之前最好能和我们交流下，或许已经有人在修复同样的问题了），然后提交一个 [Pull Request](./pull-request.md)。

## 修改代码和提交PR（Pull Request）

您可以下载代码，编译安装，部署运行试一试（可以参考[编译文档](../installing/compilation.md)），看看是否与您预想的一样工作。如果有问题，您可以直接联系我们，提 Issue 或者通过阅读和分析源代码自己修复。

无论是修复 Bug 还是增加 Feature，我们都非常欢迎。如果您希望给 Doris 提交代码，您需要从 GitHub 上 fork 代码库至您的项目空间下，为您提交的代码创建一个新的分支，添加源项目为upstream，并提交PR。
提交PR的方式可以参考文档 [Pull Request](./pull-request.md)。
