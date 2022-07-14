---
{
    "title": "Downloads",
    "language": "en"
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

# Downloads

## Apache Doris

You can download the Doris source code from the following link for compilation and deployment. After version 1.0, you can download the binary version for direct installation and deployment.

### Latest

| Version | Release Date | Download Source | Binary Version |
|---|---|---| --- |
| 1.1.0 | 2022-07-14 | [Source](http://www.apache.org/dyn/closer.cgi/doris/1.1/1.1.0-rc05/apache-doris-1.1.0-src.tar.gz) ([Signature](http://www.apache.org/dyn/closer.cgi/doris/1.1/1.1.0-rc05/apache-doris-1.1.0-src.tar.gz.asc) [SHA512](http://www.apache.org/dyn/closer.cgi/doris/1.1/1.1.0-rc05/apache-doris-1.1.0-src.tar.gz.sha512))| [x86/jdk8 [401MB]](http://www.apache.org/dyn/closer.cgi/doris/1.1/1.1.0-rc05/apache-doris-1.1.0-bin-x86-jdk8.tar.gz) ([Signature](http://www.apache.org/dyn/closer.cgi/doris/1.1/1.1.0-rc05/apache-doris-1.1.0-bin-x86-jdk8.tar.gz.asc) [SHA512](http://www.apache.org/dyn/closer.cgi/doris/1.1/1.1.0-rc05/apache-doris-1.1.0-bin-x86-jdk8.tar.gz.sha512))<br> [x86/jdk11 [396MB]](http://www.apache.org/dyn/closer.cgi/doris/1.1/1.1.0-rc05/apache-doris-1.1.0-bin-x86-jdk11.tar.gz) ([Signature](http://www.apache.org/dyn/closer.cgi/doris/1.1/1.1.0-rc05/apache-doris-1.1.0-bin-x86-jdk11.tar.gz.asc) [SHA512](http://www.apache.org/dyn/closer.cgi/doris/1.1/1.1.0-rc05/apache-doris-1.1.0-bin-x86-jdk11.tar.gz.sha512))<br>[x86/noavx2/jdk8 [400MB]](http://www.apache.org/dyn/closer.cgi/doris/1.1/1.1.0-rc05/apache-doris-1.1.0-bin-x86-noavx2-jdk8.tar.gz) ([Signature](http://www.apache.org/dyn/closer.cgi/doris/1.1/1.1.0-rc05/apache-doris-1.1.0-bin-x86-noavx2-jdk8.tar.gz.asc) [SHA512](http://www.apache.org/dyn/closer.cgi/doris/1.1/1.1.0-rc05/apache-doris-1.1.0-bin-x86-noavx2-jdk8.tar.gz.sha512))<br>[x86/noavx2/jdk11 [400MB]](http://www.apache.org/dyn/closer.cgi/doris/1.1/1.1.0-rc05/apache-doris-1.1.0-bin-x86-noavx2-jdk11.tar.gz) ([Signature](http://www.apache.org/dyn/closer.cgi/doris/1.1/1.1.0-rc05/apache-doris-1.1.0-bin-x86-noavx2-jdk11.tar.gz.asc) [SHA512](http://www.apache.org/dyn/closer.cgi/doris/1.1/1.1.0-rc05/apache-doris-1.1.0-bin-x86-noavx2-jdk11.tar.gz.sha512))<br>[arm/jdk8 [395MB]](http://www.apache.org/dyn/closer.cgi/doris/1.1/1.1.0-rc05/apache-doris-1.1.0-bin-arm-jdk8.tar.gz) ([Signature](http://www.apache.org/dyn/closer.cgi/doris/1.1/1.1.0-rc05/apache-doris-1.1.0-bin-arm-jdk8.tar.gz.asc) [SHA512](http://www.apache.org/dyn/closer.cgi/doris/1.1/1.1.0-rc05/apache-doris-1.1.0-bin-arm-jdk8.tar.gz.sha512))<br> [arm/jdk11 [395MB]](http://www.apache.org/dyn/closer.cgi/doris/1.1/1.1.0-rc05/apache-doris-1.1.0-bin-arm-jdk11.tar.gz) ([Signature](http://www.apache.org/dyn/closer.cgi/doris/1.1/1.1.0-rc05/apache-doris-1.1.0-bin-arm-jdk11.tar.gz.asc) [SHA512](http://www.apache.org/dyn/closer.cgi/doris/1.1/1.1.0-rc05/apache-doris-1.1.0-bin-arm-jdk11.tar.gz.sha512))|

> Please select the binary version corresponding to the platform and runtime environment.
>
> x86/arm: CPU model.
>
> jdk8/jdk11: JDK runtime environment for running FE and Broker.
>
> noavx2: If the CPU does not support the avx2 instruction set, select the `noavx2` version. You can check whether it is supported by `cat /proc/cpuinfo`. The avx2 instruction will improve the computational efficiency of data structures such as bloomfilter.

### Old Releases 

| Version | Release Date | Download Source | Binary Version |
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

### Latest

| Version | Flink Version | Scala Version | Release Date | Download Source from Mirror |
|---|---|---|---|---|
| 1.1.0 | 1.14 | 2.12 | 2022-07-11 | [Source](https://dist.apache.org/repos/dist/release/doris/flink-connector/1.1.0/apache-doris-flink-connector-1.14_2.12-1.1.0-src.tar.gz) ([Signature](https://dist.apache.org/repos/dist/release/doris/flink-connector/1.1.0/apache-doris-flink-connector-1.14_2.12-1.1.0-src.tar.gz.asc) [SHA512](https://dist.apache.org/repos/dist/release/doris/flink-connector/1.1.0/apache-doris-flink-connector-1.14_2.12-1.1.0-src.tar.gz.sha512)) |
| 1.1.0 | 1.14 | 2.11 | 2022-07-11 | [Source](https://dist.apache.org/repos/dist/release/doris/flink-connector/1.1.0/apache-doris-flink-connector-1.14_2.11-1.1.0-src.tar.gz) ([Signature](https://dist.apache.org/repos/dist/release/doris/flink-connector/1.1.0/apache-doris-flink-connector-1.14_2.11-1.1.0-src.tar.gz.asc) [SHA512](https://dist.apache.org/repos/dist/release/doris/flink-connector/1.1.0/apache-doris-flink-connector-1.14_2.11-1.1.0-src.tar.gz.sha512)) |

### Old Releases

| Version | Flink Version | Scala Version | Release Date | Download Source from Mirror |
|---|---|---|---|---|
| 1.0.3 | 1.14 | 2.12 | 2021-03-18 | [Source](https://archive.apache.org/dist/doris/flink-connector/1.0.3/apache-doris-flink-connector-1.14_2.12-1.0.3-incubating-src.tar.gz) ([Signature](https://archive.apache.org/dist/doris/flink-connector/1.0.3/apache-doris-flink-connector-1.14_2.12-1.0.3-incubating-src.tar.gz.asc) [SHA512](https://archive.apache.org/dist/doris/flink-connector/1.0.3/apache-doris-flink-connector-1.14_2.12-1.0.3-incubating-src.tar.gz.sha512)) |
| 1.0.3 | 1.13 | 2.12 | 2021-03-18 | [Source](https://archive.apache.org/dist/doris/flink-connector/1.0.3/apache-doris-flink-connector-1.13_2.12-1.0.3-incubating-src.tar.gz) ([Signature](https://archive.apache.org/dist/doris/flink-connector/1.0.3/apache-doris-flink-connector-1.13_2.12-1.0.3-incubating-src.tar.gz.asc) [SHA512](https://archive.apache.org/dist/doris/flink-connector/1.0.3/apache-doris-flink-connector-1.13_2.12-1.0.3-incubating-src.tar.gz.sha512)) |
| 1.0.3 | 1.12 | 2.12 | 2021-03-18 | [Source](https://archive.apache.org/dist/doris/flink-connector/1.0.3/apache-doris-flink-connector-1.12_2.12-1.0.3-incubating-src.tar.gz) ([Signature](https://archive.apache.org/dist/doris/flink-connector/1.0.3/apache-doris-flink-connector-1.12_2.12-1.0.3-incubating-src.tar.gz.asc) [SHA512](https://archive.apache.org/dist/doris/flink-connector/1.0.3/apache-doris-flink-connector-1.12_2.12-1.0.3-incubating-src.tar.gz.sha512)) |
| 1.0.3 | 1.11 | 2.12 | 2021-03-18 | [Source](https://archive.apache.org/dist/doris/flink-connector/1.0.3/apache-doris-flink-connector-1.11_2.12-1.0.3-incubating-src.tar.gz) ([Signature](https://archive.apache.org/dist/doris/flink-connector/1.0.3/apache-doris-flink-connector-1.11_2.12-1.0.3-incubating-src.tar.gz.asc) [SHA512](https://archive.apache.org/dist/doris/flink-connector/1.0.3/apache-doris-flink-connector-1.11_2.12-1.0.3-incubating-src.tar.gz.sha512)) |

### Maven

```
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

### Latest

| Version | Spark Version | Scala Version | Release Date | Download Source from Mirror |
|---|---|---|---|---|
| 1.1.0 | 3.1 | 2.12 | 2022-07-11 | [Source](https://dist.apache.org/repos/dist/release/doris/spark-connector/1.1.0/apache-doris-spark-connector-3.1_2.12-1.1.0-src.tar.gz) ([Signature](https://dist.apache.org/repos/dist/release/doris/spark-connector/1.1.0/apache-doris-spark-connector-3.1_2.12-1.1.0-src.tar.gz.asc) [SHA512](https://dist.apache.org/repos/dist/release/doris/spark-connector/1.1.0/apache-doris-spark-connector-3.1_2.12-1.1.0-src.tar.gz.sha512)) |
| 1.1.0 | 3.2 | 2.12 | 2022-07-11 | [Source](https://dist.apache.org/repos/dist/release/doris/spark-connector/1.1.0/apache-doris-spark-connector-3.2_2.12-1.1.0-src.tar.gz) ([Signature](hhttps://dist.apache.org/repos/dist/release/doris/spark-connector/1.1.0/apache-doris-spark-connector-3.2_2.12-1.1.0-src.tar.gz.asc) [SHA512](https://dist.apache.org/repos/dist/release/doris/spark-connector/1.1.0/apache-doris-spark-connector-3.2_2.12-1.1.0-src.tar.gz.sha512)) |
| 1.1.0 | 2.3 | 2.11 | 2022-07-11 | [Source](https://dist.apache.org/repos/dist/release/doris/spark-connector/1.1.0/apache-doris-spark-connector-2.3_2.11-1.1.0-src.tar.gz) ([Signature](https://dist.apache.org/repos/dist/release/doris/spark-connector/1.1.0/apache-doris-spark-connector-2.3_2.11-1.1.0-src.tar.gz.asc) [SHA512](https://dist.apache.org/repos/dist/release/doris/spark-connector/1.1.0/apache-doris-spark-connector-2.3_2.11-1.1.0-src.tar.gz.sha512)) |

### Old Releases

| Version | Spark Version | Scala Version | Release Date | Download Source from Mirror |
|---|---|---|---|---|
| 1.0.1 | 3.x | 2.12 | 2021-03-18 | [Source](https://archive.apache.org/dist/doris/spark-connector/1.0.1/apache-doris-spark-connector-3.1_2.12-1.0.1-incubating-src.tar.gz) ([Signature](https://archive.apache.org/dist/doris/spark-connector/1.0.1/apache-doris-spark-connector-3.1_2.12-1.0.1-incubating-src.tar.gz.asc) [SHA512](https://archive.apache.org/dist/doris/spark-connector/1.0.1/apache-doris-spark-connector-3.1_2.12-1.0.1-incubating-src.tar.gz.sha512)) |
| 1.0.1 | 2.x | 2.11 | 2021-03-18 | [Source](https://archive.apache.org/dist/doris/spark-connector/1.0.1/apache-doris-spark-connector-2.3_2.11-1.0.1-incubating-src.tar.gz) ([Signature](https://archive.apache.org/dist/doris/spark-connector/1.0.1/apache-doris-spark-connector-2.3_2.11-1.0.1-incubating-src.tar.gz.asc) [SHA512](https://archive.apache.org/dist/doris/spark-connector/1.0.1/apache-doris-spark-connector-2.3_2.11-1.0.1-incubating-src.tar.gz.sha512)) |

### Maven

```
<dependency>
  <groupId>org.apache.doris</groupId>
  <artifactId>spark-doris-connector-3.2_2.12</artifactId>
  <!--artifactId>spark-doris-connector-3.1_2.12</artifactId-->
  <!--artifactId>spark-doris-connector-2.3_2.11</artifactId-->
  <!--version>1.0.1</version-->
  <version>1.1.0</version>
</dependency>
```

# Verify

To verify the downloaded files, please read [Verify Apache Release](../community/release-and-verify/release-verify.md) and using these [KEYS](https://downloads.apache.org/incubator/doris/KEYS).

After verification, please read [Compilation](../install/source-install/compilation.md) and [Installation and deployment](../install/install-deploy.md) to compile and install Doris.
