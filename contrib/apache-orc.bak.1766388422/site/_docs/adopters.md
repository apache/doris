---
layout: docs
title: ORC Adopters
permalink: /docs/adopters.html
---

If your company or tool uses ORC, please let us know so that we can update
this page.

### [Apache Hadoop](https://hadoop.apache.org/)

ORC files have always supporting reading and writing from Hadoop's MapReduce,
but with the ORC 1.1.0 release it is now easier than ever without pulling in
Hive's exec jar and all of its dependencies. OrcStruct now also implements
WritableComparable and can be serialized through the MapReduce shuffle.

### [Apache Spark](https://spark.apache.org/)

Apache Spark has [added
support](https://databricks.com/blog/2015/07/16/joint-blog-post-bringing-orc-support-into-apache-spark.html)
for reading and writing ORC files with support for column project and
predicate push down.

### [Apache Arrow](https://arrow.apache.org/)

Apache Arrow supports reading and writing [ORC file format](https://arrow.apache.org/docs/index.html?highlight=orc#apache-arrow).

### [Apache Flink](https://flink.apache.org/)

Apache Flink supports
[ORC format in Table API](https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/connectors/table/formats/orc/)
for reading and writing ORC files.

### [Apache Iceberg](https://iceberg.apache.org/)

Apache Iceberg supports [ORC spec](https://iceberg.apache.org/#spec/#orc) to use ORC tables.

### [Apache Druid](https://druid.apache.org/)

Apache Druid supports
[ORC extension](https://druid.apache.org/docs/0.22.1/development/extensions-core/orc.html#orc-extension)
to ingest and understand the Apache ORC data format.

### [Apache Hive](https://hive.apache.org/)

Apache Hive was the original use case and home for ORC.  ORC's strong
type system, advanced compression, column projection, predicate push
down, and vectorization support make Hive [perform
better](https://hortonworks.com/blog/orcfile-in-hdp-2-better-compression-better-performance/)
than any other format for your data.

### [Apache Impala](https://impala.apache.org/)

Apache Impala supports [reading from ORC format Hive tables](https://impala.apache.org/docs/build/html/topics/impala_orc.html)
by leveraging the ORC C++ library.

### [Apache Gobblin](https://gobblin.apache.org/)

Apache Gobblin supports
[writing data to ORC files](https://gobblin.apache.org/docs/case-studies/Writing-ORC-Data/)
by leveraging Apache Hive's SerDe library.

### [Apache Nifi](https://nifi.apache.org/)

Apache Nifi is [adding
support](https://issues.apache.org/jira/browse/NIFI-1663) for writing
ORC files.

### [Apache Pig](https://pig.apache.org/)

Apache Pig added support for reading and writing ORC files in [Pig
14.0](https://hortonworks.com/blog/announcing-apache-pig-0-14-0/).

### [EEL](https://github.com/51zero/eel-sdk)

EEL is a Scala BigData API that supports reading and writing data for
various file formats and storage systems including to and from ORC. It
is designed as a in-process low level API for manipulating data. Data
is lazily streamed from source to sink and using standard Scala
operations such as map, flatMap and filter, it is especially suited
for ETL style applications. EEL supports ORC predicate and projection
pushdowns and correct handles conversions from other formats including
complex types such as maps, lists or nested structs. A typical use
case would be to extract data from JDBC to ORC files housed in HDFS,
or directly into Hive tables backed by an ORC file format.

### [Facebook](https://facebook.com)

With more than 300 PB of data, Facebook was an [early adopter of
ORC](https://code.facebook.com/posts/229861827208629/scaling-the-facebook-data-warehouse-to-300-pb/) and quickly put it into production.

### [LinkedIn](https://linkedin.com)

LinkedIn uses
[the ORC file format](https://engineering.linkedin.com/blog/2021/fastingest-low-latency-gobblin)
with Apache Iceberg metadata catalog and Apache Gobblin to provide our data customers with high-query performance.

https://engineering.linkedin.com/blog/2021/fastingest-low-latency-gobblin

### [Trino (formerly Presto SQL)](https://trino.io/)

The Trino team has done a lot of work [integrating
ORC](https://code.facebook.com/posts/370832626374903/even-faster-data-at-the-speed-of-presto-orc/) into their SQL engine.

### [Timber](https://timber.io/)

Timber adopted ORC for it's S3 based logging platform that stores
petabytes of log data. ORC has been key in ensuring a fast,
cost-effective strategy for persisting and querying that data.

### [Vertica](http://www8.hp.com/us/en/software-solutions/advanced-sql-big-data-analytics/)

HPE Vertica has contributed significantly to the ORC C++ library. ORC
is a significant part of Vertica SQL-on-Hadoop (VSQLoH) which brings
the performance, reliability and standards compliance of the Vertica
Analytic Database to the Hadoop ecosystem.
