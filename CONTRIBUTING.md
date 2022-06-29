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

# Contribute to Doris

Thank you very much for your interest in the Doris project. We welcome your suggestions, comments (including criticisms), comments and contributions to the Doris project.

Your suggestions, comments and comments on Doris can be made directly through GitHub's [Issues] (https://github.com/apache/doris/issues/new/selection).

There are many ways to participate in and contribute to Doris projects: code implementation, test writing, process tool improvement, document improvement, and so on. Any contribution will be welcomed and you will be added to the list of contributors. Further, with sufficient contributions, you will have the opportunity to become a Commiter of Apache with Apache mailbox and be included in the list of [Apache Commiters] (http://people.apache.org/committer-index.html).

Any questions, you can contact us to get timely answers, including dev mail list or Slack.

## Initial contact

For the first time in Doris community, you can:

* Follow [Doris Github](https://github.com/apache/doris)
* Subscribe to our [mailing list] (./subscribe-mail-list.md);
* Join Doris [Slack](https://join.slack.com/t/apachedoriscommunity/shared_invite/zt-11jb8gesh-7IukzSrdea6mqoG0HB4gZg)

Learn the development trends of Doris project in time and give your opinions on the topics you are concerned about.

## Doris's code and documentation

As you can see from [GitHub] (https://github.com/apache/doris), Apache Doris code base mainly consists of three parts: Frontend (FE), Backend (BE) and Broker (to support file reading on external storage systems such as HDFS). Documents are mainly the wiki on Doris website and GitHub, as well as the online help manual when running Doris. Details of these components can be found in the following table:

| Component Name | Component Description | Related Language|
|--------|----------------------------|----------|
| [Frontend daemon (FE)] (https://github.com/apache/doris) | consists of a query coordinator and a metadata manager | Java|
| [Backend daemon (BE)] (https://github.com/apache/doris) | Responsible for storing data and executing query fragments | C++|
| [Broker] (https://github.com/apache/doris) | Read HDFS data to Doris | Java|
| [Website](https://github.com/apache/doris-website) | Doris Website | Markdown |
+ [Github Wiki] (https://github.com/apache/doris/wiki); Doris Github Wiki; Markdown_;
| Doris Runtime Help Document | Online Help Manual at Doris Runtime | Markdown|

## Improving documentation

Documentation is the most important way for you to understand Apache Doris, and it's where we need help most!

Browse the document, you can deepen your understanding of Doris, can also help you understand Doris's function and technical details, if you find that the document has problems, please contact us in time;

If you are interested in improving the quality of documents, whether it is revising the address of a page, correcting a link, and writing a better introductory document, we are very welcome!

Most of our documents are written in markdown format, and you can modify and submit document changes directly through `docs/` in [GitHub] (https://github.com/apache/doris). If you submit code changes, you can refer to [Pull Request] (./pull-request.md).

## If a Bug or problem is found

If a Bug or problem is found, you can directly raise a new Issue through GitHub's [Issues] (https://github.com/apache/doris/issues/new/select), and we will have someone deal with it regularly.

You can also fix it yourself by reading the analysis code (of course, it's better to talk to us before that, maybe someone has fixed the same problem) and submit a [Pull Request] (./pull-request.md).

## Modify the code and submit PR (Pull Request)

You can download the code, compile and install it, deploy and run it for a try (refer to the [compilation document] (./installing/compilation.md)) to see if it works as you expected. If you have problems, you can contact us directly, ask questions or fix them by reading and analyzing the source code.

Whether it's fixing Bugs or adding Features, we're all very welcome. If you want to submit code to Doris, you need to create a new branch for your submitted code from the fork code library on GitHub to your project space, add the source project upstream, and submit PR.

About how to submit a PR refer to [Pull Request] (./pull-request.md).
