---
{
    "title": "下载",
    "language": "zh-CN"
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

# 下载

## Apache Doris

您可以通过以下链接下载版本的 Doris 源码进行编译和部署，1.0版本之后可以下载二进制版本直接安装部署。

### 最新版本

| 版本 | 发布日期 | 源码下载 | 二进制版本 |
|---|---| --- | --- |
| 1.1.0 | 2022-07-14 | [Source](http://www.apache.org/dyn/closer.cgi/doris/1.1/1.1.0-rc05/apache-doris-1.1.0-src.tar.gz) ([Signature](http://www.apache.org/dyn/closer.cgi/doris/1.1/1.1.0-rc05/apache-doris-1.1.0-src.tar.gz.asc) [SHA512](http://www.apache.org/dyn/closer.cgi/doris/1.1/1.1.0-rc05/apache-doris-1.1.0-src.tar.gz.sha512))| [x86/jdk8 [401MB]](http://www.apache.org/dyn/closer.cgi/doris/1.1/1.1.0-rc05/apache-doris-1.1.0-bin-x86-jdk8.tar.gz) ([Signature](http://www.apache.org/dyn/closer.cgi/doris/1.1/1.1.0-rc05/apache-doris-1.1.0-bin-x86-jdk8.tar.gz.asc) [SHA512](http://www.apache.org/dyn/closer.cgi/doris/1.1/1.1.0-rc05/apache-doris-1.1.0-bin-x86-jdk8.tar.gz.sha512))<br> [x86/jdk11 [396MB]](http://www.apache.org/dyn/closer.cgi/doris/1.1/1.1.0-rc05/apache-doris-1.1.0-bin-x86-jdk11.tar.gz) ([Signature](http://www.apache.org/dyn/closer.cgi/doris/1.1/1.1.0-rc05/apache-doris-1.1.0-bin-x86-jdk11.tar.gz.asc) [SHA512](http://www.apache.org/dyn/closer.cgi/doris/1.1/1.1.0-rc05/apache-doris-1.1.0-bin-x86-jdk11.tar.gz.sha512))<br>[x86/noavx2/jdk8 [400MB]](http://www.apache.org/dyn/closer.cgi/doris/1.1/1.1.0-rc05/apache-doris-1.1.0-bin-x86-noavx2-jdk8.tar.gz) ([Signature](http://www.apache.org/dyn/closer.cgi/doris/1.1/1.1.0-rc05/apache-doris-1.1.0-bin-x86-noavx2-jdk8.tar.gz.asc) [SHA512](http://www.apache.org/dyn/closer.cgi/doris/1.1/1.1.0-rc05/apache-doris-1.1.0-bin-x86-noavx2-jdk8.tar.gz.sha512))<br>[x86/noavx2/jdk11 [400MB]](http://www.apache.org/dyn/closer.cgi/doris/1.1/1.1.0-rc05/apache-doris-1.1.0-bin-x86-noavx2-jdk11.tar.gz) ([Signature](http://www.apache.org/dyn/closer.cgi/doris/1.1/1.1.0-rc05/apache-doris-1.1.0-bin-x86-noavx2-jdk11.tar.gz.asc) [SHA512](http://www.apache.org/dyn/closer.cgi/doris/1.1/1.1.0-rc05/apache-doris-1.1.0-bin-x86-noavx2-jdk11.tar.gz.sha512))<br>[arm/jdk8 [395MB]](http://www.apache.org/dyn/closer.cgi/doris/1.1/1.1.0-rc05/apache-doris-1.1.0-bin-arm-jdk8.tar.gz) ([Signature](http://www.apache.org/dyn/closer.cgi/doris/1.1/1.1.0-rc05/apache-doris-1.1.0-bin-arm-jdk8.tar.gz.asc) [SHA512](http://www.apache.org/dyn/closer.cgi/doris/1.1/1.1.0-rc05/apache-doris-1.1.0-bin-arm-jdk8.tar.gz.sha512))<br> [arm/jdk11 [395MB]](http://www.apache.org/dyn/closer.cgi/doris/1.1/1.1.0-rc05/apache-doris-1.1.0-bin-arm-jdk11.tar.gz) ([Signature](http://www.apache.org/dyn/closer.cgi/doris/1.1/1.1.0-rc05/apache-doris-1.1.0-bin-arm-jdk11.tar.gz.asc) [SHA512](http://www.apache.org/dyn/closer.cgi/doris/1.1/1.1.0-rc05/apache-doris-1.1.0-bin-arm-jdk11.tar.gz.sha512))|

> 请选择对应平台和运行环境的二进制版本。
> 
> x86/arm: CPU 型号。
> 
> jdk8/jdk11: 运行 FE 和 Broker 的 JDK 运行时环境。
> 
> noavx2: 如果CPU不支持avx2指令集，请选择 noavx2 版本。可以通过 `cat /proc/cpuinfo` 查看是否支持。avx2 指令会提升 bloomfilter 等数据结构的计算效率。

### 历史版本

| 版本 | 发布日期 | 源码下载 | 二进制版本 |
|---|---|---| --- |
| 1.0.0 | 2022-04-18 | [Source](https://archive.apache.org/dist/doris/1.0/1.0.0-incubating/apache-doris-1.0.0-incubating-src.tar.gz) ([Signature](https://archive.apache.org/dist/doris/1.0/1.0.0-incubating/apache-doris-1.0.0-incubating-src.tar.gz.asc) [SHA512](https://archive.apache.org/dist/doris/1.0/1.0.0-incubating/apache-doris-1.0.0-incubating-src.tar.gz.sha512))| [x86/jdk8 [362MB]](https://archive.apache.org/dist/doris/1.0/1.0.0-incubating/apache-doris-1.0.0-incubating-bin.tar.gz) ([Signature](https://archive.apache.org/dist/doris/1.0/1.0.0-incubating/apache-doris-1.0.0-incubating-bin.tar.gz.asc) [SHA512](https://archive.apache.org/dist/doris/1.0/1.0.0-incubating/apache-doris-1.0.0-incubating-bin.tar.gz.sha512))|
| 0.15.0 | 2021-11-29 | [Source](http://archive.apache.org/dist/incubator/doris/0.15.0-incubating/apache-doris-0.15.0-incubating-src.tar.gz) ([Signature](http://archive.apache.org/dist/incubator/doris/0.15.0-incubating/apache-doris-0.15.0-incubating-src.tar.gz.asc) [SHA512](http://archive.apache.org/dist/incubator/doris/0.15.0-incubating/apache-doris-0.15.0-incubating-src.tar.gz.sha512))||
| 0.14.0 | 2021-05-26 | [Source](https://archive.apache.org/dist/incubator/doris/0.14.0-incubating/apache-doris-0.14.0-incubating-src.tar.gz) ([Signature](https://archive.apache.org/dist/incubator/doris/0.14.0-incubating/apache-doris-0.14.0-incubating-src.tar.gz.asc) [SHA512](https://archive.apache.org/dist/incubator/doris/0.14.0-incubating/apache-doris-0.14.0-incubating-src.tar.gz.sha512))||
| 0.13.0 | 2020-10-24 | [Source](https://archive.apache.org/dist/incubator/doris/0.13.0-incubating/apache-doris-0.13.0-incubating-src.tar.gz) ([Signature](https://archive.apache.org/dist/incubator/doris/0.13.0-incubating/apache-doris-0.13.0-incubating-src.tar.gz.asc) [SHA512](https://archive.apache.org/dist/incubator/doris/0.13.0-incubating/apache-doris-0.13.0-incubating-src.tar.gz.sha512))||
| 0.12.0 | 2020-04-24 | [Source](https://archive.apache.org/dist/incubator/doris/0.12.0-incubating/apache-doris-0.12.0-incubating-src.tar.gz) ([Signature](https://archive.apache.org/dist/incubator/doris/0.12.0-incubating/apache-doris-0.12.0-incubating-src.tar.gz.asc) [SHA512](https://archive.apache.org/dist/incubator/doris/0.12.0-incubating/apache-doris-0.12.0-incubating-src.tar.gz.sha512)) ||
| 0.11.0 | 2019-11-29 | [Source](https://archive.apache.org/dist/incubator/doris/0.11.0-incubating/apache-doris-0.11.0-incubating-src.tar.gz) ([Signature](https://archive.apache.org/dist/incubator/doris/0.11.0-incubating/apache-doris-0.11.0-incubating-src.tar.gz.asc) [SHA512](https://archive.apache.org/dist/incubator/doris/0.11.0-incubating/apache-doris-0.11.0-incubating-src.tar.gz.sha512)) ||
| 0.10.0 | 2019-07-02 | [Source](https://archive.apache.org/dist/incubator/doris/0.10.0-incubating/apache-doris-0.10.0-incubating-src.tar.gz) ([Signature](https://archive.apache.org/dist/incubator/doris/0.10.0-incubating/apache-doris-0.10.0-incubating-src.tar.gz.asc) [SHA512](https://archive.apache.org/dist/incubator/doris/0.10.0-incubating/apache-doris-0.10.0-incubating-src.tar.gz.sha512)) ||
| 0.9.0 | 2019-02-18 | [Source](https://archive.apache.org/dist/incubator/doris/0.9.0-incubating/apache-doris-0.9.0-incubating-src.tar.gz) ([Signature](https://archive.apache.org/dist/incubator/doris/0.9.0-incubating/apache-doris-0.9.0-incubating-src.tar.gz.asc) [SHA512](https://archive.apache.org/dist/incubator/doris/0.9.0-incubating/apache-doris-0.9.0-incubating-src.tar.gz.sha512)) ||


## Flink Doris Connector

### 最新版本

| 版本 | Flink 版本 | Scala 版本 | 发布日期 | 从镜像网站下载 |
|---|---|---|---|---|
| 1.1.0 | 1.14 | 2.12 | 2022-07-11 | [Source](https://dist.apache.org/repos/dist/release/doris/flink-connector/1.1.0/apache-doris-flink-connector-1.14_2.12-1.1.0-src.tar.gz) ([Signature](https://dist.apache.org/repos/dist/release/doris/flink-connector/1.1.0/apache-doris-flink-connector-1.14_2.12-1.1.0-src.tar.gz.asc) [SHA512](https://dist.apache.org/repos/dist/release/doris/flink-connector/1.1.0/apache-doris-flink-connector-1.14_2.12-1.1.0-src.tar.gz.sha512)) |
| 1.1.0 | 1.14 | 2.11 | 2022-07-11 | [Source](https://dist.apache.org/repos/dist/release/doris/flink-connector/1.1.0/apache-doris-flink-connector-1.14_2.11-1.1.0-src.tar.gz) ([Signature](https://dist.apache.org/repos/dist/release/doris/flink-connector/1.1.0/apache-doris-flink-connector-1.14_2.11-1.1.0-src.tar.gz.asc) [SHA512](https://dist.apache.org/repos/dist/release/doris/flink-connector/1.1.0/apache-doris-flink-connector-1.14_2.11-1.1.0-src.tar.gz.sha512)) |

### 历史版本

| 版本 | Flink 版本 | Scala 版本 | 发布日期 | 从镜像网站下载 |
|---|---|---|---|---|
| 1.0.3 | 1.14 | 2.12 | 2021-03-18 | [Source](https://archive.apache.org/dist/doris/flink-connector/1.0.3/apache-doris-flink-connector-1.14_2.12-1.0.3-incubating-src.tar.gz) ([Signature](https://archive.apache.org/dist/doris/flink-connector/1.0.3/apache-doris-flink-connector-1.14_2.12-1.0.3-incubating-src.tar.gz.asc) [SHA512](https://archive.apache.org/dist/doris/flink-connector/1.0.3/apache-doris-flink-connector-1.14_2.12-1.0.3-incubating-src.tar.gz.sha512)) |
| 1.0.3 | 1.13 | 2.12 | 2021-03-18 | [Source](https://archive.apache.org/dist/doris/flink-connector/1.0.3/apache-doris-flink-connector-1.13_2.12-1.0.3-incubating-src.tar.gz) ([Signature](https://archive.apache.org/dist/doris/flink-connector/1.0.3/apache-doris-flink-connector-1.13_2.12-1.0.3-incubating-src.tar.gz.asc) [SHA512](https://archive.apache.org/dist/doris/flink-connector/1.0.3/apache-doris-flink-connector-1.13_2.12-1.0.3-incubating-src.tar.gz.sha512)) |
| 1.0.3 | 1.12 | 2.12 | 2021-03-18 | [Source](https://archive.apache.org/dist/doris/flink-connector/1.0.3/apache-doris-flink-connector-1.12_2.12-1.0.3-incubating-src.tar.gz) ([Signature](https://archive.apache.org/dist/doris/flink-connector/1.0.3/apache-doris-flink-connector-1.12_2.12-1.0.3-incubating-src.tar.gz.asc) [SHA512](https://archive.apache.org/dist/doris/flink-connector/1.0.3/apache-doris-flink-connector-1.12_2.12-1.0.3-incubating-src.tar.gz.sha512)) |
| 1.0.3 | 1.11 | 2.12 | 2021-03-18 | [Source](https://archive.apache.org/dist/doris/flink-connector/1.0.3/apache-doris-flink-connector-1.11_2.12-1.0.3-incubating-src.tar.gz) ([Signature](https://archive.apache.org/dist/doris/flink-connector/1.0.3/apache-doris-flink-connector-1.11_2.12-1.0.3-incubating-src.tar.gz.asc) [SHA512](https://archive.apache.org/dist/doris/flink-connector/1.0.3/apache-doris-flink-connector-1.11_2.12-1.0.3-incubating-src.tar.gz.sha512)) |


### Maven

```xml
<dependency>
  <groupId>org.apache.doris</groupId>
  <artifactId>flink-doris-connector-1.14_2.12</artifactId>
  <!--artifactId>flink-doris-connector-1.13_2.12</artifactId-->
  <!--artifactId>flink-doris-connector-1.12_2.12</artifactId-->
  <!--artifactId>flink-doris-connector-1.11_2.12</artifactId-->
  <!--version>1.0.3</version-->
  <version>1.1.0</version>
</dependency>
```

## Spark Doris Connector

### 最新版本

| 版本 | Spark 版本 | Scala 版本 | 发布日期 | 从镜像网站下载 |
|---|---|---|---|---|
| 1.1.0 | 3.1 | 2.12 | 2022-07-11 | [Source](https://dist.apache.org/repos/dist/release/doris/spark-connector/1.1.0/apache-doris-spark-connector-3.1_2.12-1.1.0-src.tar.gz) ([Signature](https://dist.apache.org/repos/dist/release/doris/spark-connector/1.1.0/apache-doris-spark-connector-3.1_2.12-1.1.0-src.tar.gz.asc) [SHA512](https://dist.apache.org/repos/dist/release/doris/spark-connector/1.1.0/apache-doris-spark-connector-3.1_2.12-1.1.0-src.tar.gz.sha512)) |
| 1.1.0 | 3.2 | 2.12 | 2022-07-11 | [Source](https://dist.apache.org/repos/dist/release/doris/spark-connector/1.1.0/apache-doris-spark-connector-3.2_2.12-1.1.0-src.tar.gz) ([Signature](hhttps://dist.apache.org/repos/dist/release/doris/spark-connector/1.1.0/apache-doris-spark-connector-3.2_2.12-1.1.0-src.tar.gz.asc) [SHA512](https://dist.apache.org/repos/dist/release/doris/spark-connector/1.1.0/apache-doris-spark-connector-3.2_2.12-1.1.0-src.tar.gz.sha512)) |
| 1.1.0 | 2.3 | 2.11 | 2022-07-11 | [Source](https://dist.apache.org/repos/dist/release/doris/spark-connector/1.1.0/apache-doris-spark-connector-2.3_2.11-1.1.0-src.tar.gz) ([Signature](https://dist.apache.org/repos/dist/release/doris/spark-connector/1.1.0/apache-doris-spark-connector-2.3_2.11-1.1.0-src.tar.gz.asc) [SHA512](https://dist.apache.org/repos/dist/release/doris/spark-connector/1.1.0/apache-doris-spark-connector-2.3_2.11-1.1.0-src.tar.gz.sha512)) |

### 历史版本

| 版本 | Spark 版本 | Scala 版本 | 发布日期 | 从镜像网站下载 |
|---|---|---|---|---|
| 1.0.1 | 3.x | 2.12 | 2021-03-18 | [Source](https://archive.apache.org/dist/doris/spark-connector/1.0.1/apache-doris-spark-connector-3.1_2.12-1.0.1-incubating-src.tar.gz) ([Signature](https://archive.apache.org/dist/doris/spark-connector/1.0.1/apache-doris-spark-connector-3.1_2.12-1.0.1-incubating-src.tar.gz.asc) [SHA512](https://archive.apache.org/dist/doris/spark-connector/1.0.1/apache-doris-spark-connector-3.1_2.12-1.0.1-incubating-src.tar.gz.sha512)) |
| 1.0.1 | 2.x | 2.11 | 2021-03-18 | [Source](https://archive.apache.org/dist/doris/spark-connector/1.0.1/apache-doris-spark-connector-2.3_2.11-1.0.1-incubating-src.tar.gz) ([Signature](https://archive.apache.org/dist/doris/spark-connector/1.0.1/apache-doris-spark-connector-2.3_2.11-1.0.1-incubating-src.tar.gz.asc) [SHA512](https://archive.apache.org/dist/doris/spark-connector/1.0.1/apache-doris-spark-connector-2.3_2.11-1.0.1-incubating-src.tar.gz.sha512)) |

### Maven

```xml
<dependency>
  <groupId>org.apache.doris</groupId>
  <artifactId>spark-doris-connector-3.2_2.12</artifactId>
  <!--artifactId>spark-doris-connector-3.1_2.12</artifactId-->
  <!--artifactId>spark-doris-connector-2.3_2.11</artifactId-->
  <!--version>1.0.1</version-->
  <version>1.1.0</version>
</dependency>
```

# 校验

关于如何校验下载文件，请参阅 [校验下载文件](../community/release-and-verify/release-verify.md)，并使用这些[KEYS](https://downloads.apache.org/incubator/doris/KEYS)。

校验完成后，可以参阅 [编译文档](../install/source-install/compilation.md) 以及 [安装与部署文档](../install/install-deploy.md) 进行 Doris 的编译、安装与部署。
