Introduction to Apache Doris (incubating)
=========================================

Apache Doris is an MPP-based interactive SQL data warehousing for
reporting and analysis. Doris mainly integrates the technology of Google
Mesa and Apache Impala. Unlike other popular SQL-on-Hadoop systems,
Doris is designed to be a simple and single tightly coupled system, not
depending on other systems. Doris not only provides high concurrent low
latency point query performance, but also provides high throughput
queries of ad-hoc analysis. Doris not only provides batch data loading,
but also provides near real-time mini-batch data loading. Doris also
provides high availability, reliability, fault tolerance, and
scalability. The simplicity (of developing, deploying and using) and
meeting many data serving requirements in single system are the main
features of Doris.

1. Background
-------------

In Baidu, the largest Chinese search engine, we run a two-tiered data
warehousing system for data processing, reporting and analysis. Similar
to lambda architecture, the whole data warehouse comprises data
processing and data serving. Data processing does the heavy lifting of
big data: cleaning data, merging and transforming it, analyzing it and
preparing it for use by end user queries; data serving is designed to
serve queries against that data for different use cases. Currently data
processing includes batch data processing and stream data processing
technology, like Hadoop, Spark and Storm; Doris is a SQL data warehouse
for serving online and interactive data reporting and analysis querying.

Prior to Doris, different tools were deployed to solve diverse
requirements in many ways. For example, the advertising platform needs
to provide some detailed statistics associated with each served ad for
every advertiser. The platform must support continuous updates, both new
rows and incremental updates to existing rows within minutes. It must
support latency-sensitive users serving live customer reports with very
low latency requirements and batch ad-hoc multiple dimensions data
analysis requiring very high throughput. In the past,this platform was
built on top of sharded MySQL. But with the growth of data, MySQL cannot
meet the requirements. Then, based on our existing KV system, we
developed our own proprietary distributed statistical database. But, the
simple KV storage was not efficient on scan performance. Because the
system depends on many other systems, it is very complex to operate and
maintain. Using RPC API, more complex querying usually required code
programming, but users wants an MPP SQL engine. In addition to
advertising system, a large number of internal BI Reporting / Analysis,
also used a variety of tools. Some used the combination of SparkSQL /
Impala + HDFS / HBASE. Some used MySQL to store the results that were
prepared by distributed MapReduce computing. Some also bought commercial
databases to use.

However, when a use case requires the simultaneous availability of
capabilities that cannot all be provided by a single tool, users were
forced to build hybrid architectures that stitch multiple tools
together. Users often choose to ingest and update data in one storage
system, but later reorganize this data to optimize for an analytical
reporting use-case served from another. Our users had been successfully
deploying and maintaining these hybrid architectures, but we believe
that they shouldn’t need to accept their inherent complexity. A storage
system built to provide great performance across a broad range of
workloads provides a more elegant solution to the problems that hybrid
architectures aim to solve. Doris is the solution. Doris is designed to
be a simple and single tightly coupled system, not depending on other
systems. Doris provides high concurrent low latency point query
performance, but also provides high throughput queries of ad-hoc
analysis. Doris provides bulk-batch data loading, but also provides near
real-time mini-batch data loading. Doris also provides high
availability, reliability, fault tolerance, and scalability.

Generally speaking, Doris is the technology combination of Google Mesa
and Apache Impala. Mesa is a highly scalable analytic data storage
system that stores critical measurement data related to Google’s
Internet advertising business. Mesa is designed to satisfy complex and
challenging set of users’ and systems’ requirements, including near
real-time data ingestion and query ability, as well as high
availability, reliability, fault tolerance, and scalability for large
data and query volumes. Impala is a modern, open-source MPP SQL engine
architected from the ground up for the Hadoop data processing
environment. At present, by virtue of its superior performance and rich
functionality, Impala has been comparable to many commercial MPP
database query engine. Mesa can satisfy the needs of many of our storage
requirements, however Mesa itself does not provide a SQL query engine;
Impala is a very good MPP SQL query engine, but the lack of a perfect
distributed storage engine. So in the end we chose the combination of
these two technologies.

Learning from Mesa’s data model, we developed a distributed storage
engine. Unlike Mesa, this storage engine does not rely on any
distributed file system. Then we deeply integrate this storage engine
with Impala query engine. Query compiling, query execution coordination
and catalog management of storage engine are integrated to be frontend
daemon; query execution and data storage are integrated to be backend
daemon. With this integration, we implemented a single, full-featured,
high performance state the art of MPP database, as well as maintaining
the simplicity.

2. System Overview
------------------

Doris’ implementation consists of two daemons: frontend (FE) and backend
(BE). The following figures gives the overview of architecture and
usage.

.. figure:: https://raw.githubusercontent.com/apache/incubator-doris/master/docs/resources/images/palo_architecture.jpg
   :alt: Doris Architecture

   Doris Architecture

Frontend daemon consists of query coordinator and catalog manager. Query
coordinator is responsible for receiving user’s sql queries, compiling
queries and managing queries execution. Catalog manager is responsible
for managing metadata such as databases, tables, partitions, replicas
and etc. Several frontend daemons could be deployed to guarantee
fault-tolerance, and load balancing.

Backend daemon stores the data and executes the query fragments. Many
backend daemons could also be deployed to provide scalability and
fault-tolerance.

A typical Doris cluster generally composes of several frontend daemons
and dozens to hundreds of backend daemons.

Clients can use MySQL-related tools to connect any frontend daemon to
submit SQL query. The frontend receives the query and compiles it into
query plans executable by the backends. Then frontend sends the query
plan fragments to backend. Backends will build a query execution DAG.
Data is fetched and pipelined into the DAG. The final result response is
sent to client via frontend. The distribution of query fragment
execution takes minimizing data movement and maximizing scan locality as
the main goal. Because Doris is designed to provide interactive
analysis, so the average execution time of queries is short. Considering
this, we adopt query re-execution to meet the fault tolerance of query
execution.

A table is splitted into many tablets. Tablets are managed by backends.
The backend daemon could be configured to use multiple directories. Any
directory’s IO failure doesn’t influence the normal running of backend
daemon. Doris will recover and rebalance the whole cluster automatically
when necessary.

3. Frontend
-----------

In-memory catalog, multiple frontends, MySQL networking protocol,
consistency guarantee, and two-level table partitioning are the main
features of Doris’ frontend design.

3.1 In-Memory Catalog
~~~~~~~~~~~~~~~~~~~~~

Traditional data warehouse always uses a RDBMS database to store their
catalog metadata. In order to produce query execution plan, frontend
needs to look up the catalog metadata. This kind of catalog storage may
be enough for low concurrent ad-hoc analysis queries. But for online
high concurrent queries, its performance is very bad,resulting in
increased response latency. For example, Hive metadata query latency is
sometimes up to tens of seconds or even minutes. In order to speedup the
metadata access, we adopt the in-memory catalog storage.

.. figure:: ./resources/iamges/log_replication.jpg
   :alt: log replication

   log replication

In-memory catalog storage has three functional modules: real-time memory
data structures, memory checkpoints on local disk and an operation relay
log. When modifying catalog, the mutation operation is written into the
log file firstly. Then, the mutation operation is applied into the
memory data structures. Periodically, a thread does the checkpoint that
dumps memory data structure image into local disk. Checkpoint mechanism
enables the fast startup of frontend and reduces the disk storage
occupancy. Actually, in-memory catalog also simplifies the
implementation of multiple frontends.

3.2 Multiple Frontends
~~~~~~~~~~~~~~~~~~~~~~

Many data warehouses only support single frontend-like node. There are
some systems supporting master and slave deploying. But for online data
serving, high availability is an essential feature. Further, the number
of queries per seconds may be very large, so high scalability is also
needed. In Doris, we provide the feature of multiple frontends using
replicated-state-machine technology.

Frontends can be configured to three kinds of roles: leader, follower
and observer. Through a voting protocol, follower frontends firstly
elect a leader frontend. All the write requests of metadata are
forwarded to the leader, then the leader writes the operation into the
replicated log file. If the new log entry will be replicated to at least
quorum followers successfully, the leader commits the operation into
memory, and responses the write request. Followers always replay the
replicated logs to apply them into their memory metadata. If the leader
crashes, a new leader will be elected from the leftover followers.
Leader and follower mainly solve the problem of write availability and
partly solve the problem of read scalability.

Usually one leader frontend and several follower frontends can meet most
applications’ write availability and read scalability requirements. For
very high concurrent reading, continuing to increase the number of
followers is not a good practice. Leader replicates log stream to
followers synchronously, so adding more followers will increases write
latency. Like Zookeeper,we have introduced a new type of frontend node
called observer that helps addressing this problem and further improving
metadata read scalability. Leader replicates log stream to observers
asynchronously. Observers don’t involve leader election.

The replicated-state-machine is implemented based on BerkeleyDB java
version (BDB-JE). BDB-JE has achieved high availability by implementing
a Paxos-like consensus algorithm. We use BDB-JE to implement Doris’ log
replication and leader election.

3.3 Consistency Guarantee
~~~~~~~~~~~~~~~~~~~~~~~~~

If a client process connects to the leader, it will see up-to-date
metadata, so that strong consistency semantics is guaranteed. If the
client connects to followers or observers, it will see metadata lagging
a little behind of the leader, but the monotonic consistency is
guaranteed. In most Doris’ use cases, monotonic consistency is accepted.

If the client always connects to the same frontend, monotonic
consistency semantics is obviously guaranteed; however if the client
connects to other frontends due to failover, the semantics may be
violated. Doris provides a SYNC command to guarantee metadata monotonic
consistency semantics during failover. When failover happens, the client
can send a SYNC command to the new connected frontend, who will get the
latest operation log number from the leader. The SYNC command will not
return to client as long as local applied log number is still less than
fetched operation log number. This mechanism can guarantee the metadata
on the connected frontend is newer than the client have seen during its
last connection.

3.4 MySQL Networking Protocol
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

MySQL compatible networking protocol is implemented in Doris’ frontend.
Firstly, SQL interface is preferred for engineers; Secondly,
compatibility with MySQL protocol makes the integrating with current
existing BI software, such as Tableau, easier; Lastly, rich MySQL client
libraries and tools reduce our development costs, but also reduces the
user’s using cost.

Through the SQL interface, administrator can adjust system
configuration, add and remove frontend nodes or backend nodes, and
create new database for user; user can create tables, load data, and
submit SQL query.

Online help document and Linux Proc-like mechanism are also supported in
SQL. Users can submit queries to get the help of related SQL statements
or show Doris’ internal running state.

In frontend, a small response buffer is allocated to every MySQL
connection. The maximum size of this buffer is limited to 1MB. The
buffer is responsible for buffering the query response data. Only if the
response is finished or the buffer size reaches the 1MB,the response
data will begin to be sent to client. Through this small trick, frontend
can re-execution most of queries if errors occurred during query
execution.

3.5 Two-Level Partitioning
~~~~~~~~~~~~~~~~~~~~~~~~~~

Like most of the distributed database system, data in Doris is
horizontally partitioned. However, a single-level partitioning rule
(hash partitioning or range partitioning) may not be a good solution to
all scenarios. For example, there have a user-based fact table that
stores rows of the form (date, userid, metric). Choosing only hash
partitioning by column userid may lead to uneven distribution of data,
when one user’s data is very large. If choosing range partitioning
according to column date, it will also lead to uneven distribution of
data due to the likely data explosion in a certain period of time.

Therefore we support the two-level partitioning rule. The first level is
range partitioning. User can specify a column (usually the time series
column) range of values for the data partition. In one partition, the
user can also specify one or more columns and a number of buckets to do
the hash partitioning. User can combine with different partitioning
rules to better divide the data. Figure 4 gives an example of two-level
partitioning.

Three benefits are gained by using the two-level partitioning mechanism.
Firstly, old and new data could be separated, and stored on different
storage mediums; Secondly, storage engine of backend can reduce the
consumption of IO and CPU for unnecessary data merging, because the data
in some partitions is no longer be updated; Lastly,every partition’s
buckets number can be different and adjusted according to the change of
data size.

.. code:: sql

   -- Create partitions using CREATE TABLE --
   CREATE TABLE example_tbl (
       `date`      DATE,
       userid      BIGINT,
       metric      BIGINT SUM
   ) PARTITION BY RANGE (`date`) (
       PARTITION p201601 VALUES LESS THAN ("2016-02-01"),
       PARTITION p201602 VALUES LESS THAN ("2016-03-01"),
       PARTITION p201603 VALUES LESS THAN ("2016-04-01"),
       PARTITION p201604 VALUES LESS THAN ("2016-05-01")
   ) DISTRIBUTED BY HASH(userid) BUCKETS 32;

   -- Add partition using ALTER TABLE --
   ALTER TABLE example_tbl ADD PARTITION p201605 VALUES LESS THAN ("2016-06-01");

4. Backend
----------

4.1 Data Storage Model
~~~~~~~~~~~~~~~~~~~~~~

Doris combines Google Mesa’s data model and ORCFile / Parquet storage
technology.

Data in Mesa is inherently multi-dimensional fact table. These facts in
table typically consist of two types of attributes: dimensional
attributes (which we call keys) and measure attributes (which we call
values). The table schema also specifies the aggregation function F: V
×V → V which is used to aggregate the values corresponding to the same
key. To achieve high update throughput, Mesa loads data in batch. Each
batch of data will be converted to a delta file. Mesa uses MVCC approach
to manage these delta files, and so to enforce update atomicity. Mesa
also supports creating materialized rollups, which contain a column
subset of schema to gain better aggregation effect.

Mesa’s data model performs well in many interactive data service, but it
also has some drawbacks: 1. Users have difficulty in understanding key
and value space, as well as aggregation function, especially when they
rarely have such aggregation demand in analysis query scenarios.

2. In order to ensure the aggregation semantic, count operation on a
   single column must read all columns in key space, resulting in a
   large number of additional read overheads. There is also unable to
   push down the predicates on the value column to storage engine, which
   also leads to additional read overheads.

3. Essentially, it is still a key-value model. In order to aggregate the
   values corresponding to the same key, all key columns must store in
   order. When a table contains hundreds of columns, sorting cost
   becomes the bottleneck of ETL process.

To solve these problems, we introduce ORCFile / Parquet technology
widely used in the open source community, such as MapReduce + ORCFile,
SparkSQL + Parquet, mainly used for ad-hoc analysis of large amounts of
data with low concurrency. These data does not distinguish between key
and value. In addition, compared with the row-oriented database,
column-oriented organization is more efficient when an aggregate needs
to be computed over many rows but only for a small subset of all columns
of data, because reading that smaller subset of data can be faster than
reading all data. And columnar storage is also space-friendly due to the
high compression ratio of each column. Further, column support
block-level storage technology such as min/max index and bloom filter
index. Query executor can filter out a lot of blocks that do not meet
the predicate, to further improve the query performance. However, due to
the underlying storage does not require data order, query time
complexity is linear corresponding to the data volume.

Like traditional databases, Doris stores structured data represented as
tables. Each table has a well-defined schema consisting of a finite
number of columns. We combine Mesa data model and ORCFile/Parquet
technology to develop a distributed analytical database. User can create
two types of table to meet different needs in interactive query
scenarios.

In non-aggregation type of table, columns are not distinguished between
dimensions and metrics, but should specify the sort columns in order to
sort all rows. Doris will sort the table data according to the sort
columns without any aggregation. The following figure gives an example
of creating non-aggregation table.

.. code:: sql

   -- Create non-aggregation table --
   CREATE TABLE example_tbl (
       `date`      DATE,
       id          BIGINT,
       country     VARCHAR(32),
       click       BIGINT,
       cost        BIGINT
   ) DUPLICATE KEY(`date`, id, country)
   DISTRIBUTED BY HASH(id) BUCKETS 32;

In aggregation data analysis case, we reference Mesa’s data model, and
distinguish columns between key and value, and specify the value columns
with aggregation method, such as SUM, REPLACE, etc. In the following
figure, we create an aggregation table like the non-aggregation table,
including two SUM aggregation columns (clicks, cost). Different from the
non-aggregation table, data in the table needs to be sorted on all key
columns for delta compaction and value aggregation.

.. code:: sql

   -- Create aggregation table --
   CREATE TABLE example_tbl (
       `date`      DATE,
       id          BIGINT,
       country     VARCHAR(32),
       click       BIGINT          SUM,
       cost        BIGINT          SUM
   ) DISTRIBUTED BY HASH(id) BUCKETS 32;

Rollup is a materialized view that contains a column subset of schema in
Doris. A table may contain multiple rollups with columns in different
order. According to sort key index and column covering of the rollups,
Doris can select the best rollup for different query. Because most
rollups only contain a few columns, the size of aggregated data is
typically much smaller and query performance can greatly be improved.
All the rollups in the same table are updated atomically. Because
rollups are materialized, users should make a trade-off between query
latency and storage space when using them.

To achieve high update throughput, Doris only applies updates in batches
at the smallest frequency of every minute. Each update batch specifies
an increased version number and generates a delta data file, commits the
version when updates of quorum replicas are complete. You can query all
committed data using the committed version, and the uncommitted version
would not be used in query. All update versions are strictly be in
increasing order. If an update contains more than one table, the
versions of these tables are committed atomically. The MVCC mechanism
allows Doris to guarantee multiple table atomic updates and query
consistency. In addition, Doris uses compaction policies to merge delta
files to reduce delta number, also reduce the cost of delta merging
during query for higher performance.

Doris’ data file is stored by column. The rows are stored in sorted
order by the sort columns in delta data files, and are organized into
row blocks, each block is compressed by type-specific columnar
encodings, such as run-length encoding for integer columns, then stored
into separate streams. In order to improve the performance of queries
that have a specific key, we also store a sparse sort key index file
corresponding to each delta data file. An index entry contains the short
key for the row block, which is a fixed size prefix of the first sort
columns for the row block, and the block id in the data file. Index
files are usually directly loaded into memory, as they are very small.
The algorithm for querying a specific key includes two steps. First, use
a binary search on the sort key index to find blocks that may contain
the specific key, and then perform a binary search on the compressed
blocks in the data files to find the desired key. We also store
block-level min/max index into separate index streams, and queries can
use this to filter undesired blocks. In addition to those basic columnar
features, we also offers an optional block-level bloom filter index for
queries with IN or EQUAL conditions to further filter undesired blocks.
Bloom filter index is stored in a separate stream, and is loaded on
demand.

4.2 Data Loading
~~~~~~~~~~~~~~~~

Doris applies updates in batches. Three types of data loading are
supported: Hadoop-batch loading, loading ,mini-batch loading.

1. Hadoop-batch loading. When a large amount of data volume needs to be
   loaded into Doris, the hadoop-batch loading is recommended to achieve
   high loading throughput. The data batches themselves are produced by
   an external Hadoop system, typically at a frequency of every few
   minutes. Unlike traditional data warehouses that use their own
   computing resource to do the heavy data preparation, Doris could use
   Hadoop to prepare the data (shuffle, sort and aggregate, etc.). By
   using this approach, the most time-consuming computations are handed
   over to Hadoop to complete. This will not only improve computational
   efficiency, but also reduce the performance pressure of Doris cluster
   and ensure the stability of the query service. The stability of the
   online data services is the most important point.

2. Loading. After deploying the fs-brokers, you can use Doris’ query
   engine to import data. This type of loading is recommended for
   incremental data loading.

3. Mini-batch loading. When a small amount of data needs to be loaded
   into Doris, the mini-batch loading is recommended to achieve low
   loading latency. By using http interface, raw data is pushed into a
   backend. Then the backend does the data preparing computing and
   completes the final loading. Http tools could connect frontend or
   backend. If frontend is connected, it will redirect the request
   randomly to a backend.

All the loading work is handled asynchronously. When load request is
submitted, a label needs to be provided. By using the load label, users
can submit show load request to get the loading status or submit cancel
load request to cancel the loading. If the status of loading task is
successful or in progress, its load label is not allowed to reuse again.
The label of failed task is allowed to be reused.

4.3 Resource Isolation
~~~~~~~~~~~~~~~~~~~~~~

1. Multi-tenancy Isolation：Multiple virtual cluster can be created in
   one pysical Doris cluster. Every backend node can deploy multiple
   backend processes. Every backend process only belongs to one virtual
   cluster. Virtual cluster is one tenancy.

2. User Isolation: There are many users in one virtual cluster. You can
   allocate the resource among different users and ensure that all
   users’ tasks are executed under limited resource quota.

3. Priority Isolation: There are three priorities isolation group for
   one user. User could control resource allocated to different tasks
   submitted by themselves, for example user’s query task and loading
   tasks require different resource quota.

4.4 Multi-Medium Storage
~~~~~~~~~~~~~~~~~~~~~~~~

Most machines in modern datacenter are equipped with both SSDs and HDDs.
SSD has good random read capability that is the ideal medium for query
that needs a large number of random read operations. However, SSD’s
capacity is small and is very expensive, we could not deploy it at a
large scale. HDD is cheap and has huge capacity that is suitable to
store large scale data but with high read latency. In OLAP scenario, we
find user usually submit a lot of queries to query the latest data (hot
data) and expect low latency. User occasionally executes query on
historical data (cold data). This kind of query usually needs to scan
large scale of data and is high latency. Multi-Medium Storage allows
users to manage the storage medium of the data to meet different query
scenarios and reduce the latency. For example, user could put latest
data on SSD and historical data which is not used frequently on HDD,
user will get low latency when querying latest data while get high
latency when query historical data which is normal because it needs scan
large scale data.

In the following figure, user alters partition ‘p201601’ storage_medium
to SSD and storage_cooldown_time to ‘2016-07-01 00:00:00’. The setting
means data in this partition will be put on SSD and it will start to
migrate to HDD after the time of storage_cooldown_time.

.. code:: sql

   ALTER TABLE example_tbl MODIFY PARTITION p201601
   SET ("storage_medium" = "SSD", "storage_cooldown_time" = "2016-07-01 00:00:00");

4.5 Vectorized Query Execution
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Runtime code generation using LLVM is one of the techniques employed
extensively by Impala to improve query execution times. Performance
could gains of 5X or more are typical for representative workloads.

But, runtime code generation is not suitable for low latency query,
because the generation overhead costs about 100ms. Runtime code
generation is more suitable for large-scale ad-hoc query. To accelerate
the small queries (of course, big queries will also obtain benefits), we
introduced vectorized query execution into Doris.

Vectorized query execution is a feature that greatly reduces the CPU
usage for typical query operations like scans, filters, aggregates, and
joins. A standard query execution system processes one row at a time.
This involves long code paths and significant metadata interpretation in
the inner loop of execution. Vectorized query execution streamlines
operations by processing a block of many rows at a time. Within the
block, each column is stored as a vector (an array of a primitive data
type). Simple operations like arithmetic and comparisons are done by
quickly iterating through the vectors in a tight loop, with no or very
few function calls or conditional branches inside the loop. These loops
compile in a streamlined way that uses relatively few instructions and
finishes each instruction in fewer clock cycles, on average, by
effectively using the processor pipeline and cache memory.

The result of benchmark shows 2x~4x speedup in our typical queries.

5. Backup and Recovery
----------------------

Data backup function is provided to enhance data security. The minimum
granularity of backup and recovery is partition. Users can develop
plugins to backup data to any specified remote storage. The backup data
can always be recovered to Doris at all time, to achieve the data
rollback purpose.

Currently we only support full data backup data rather than incremental
backups for the following reasons:

1. Remote storage system is beyond the control of the Doris system. We
   cannot guarantee whether the data has been changed between two backup
   operations. And data verification operations always come at a high
   price.

2. We support data backup on partition granularity. And majority of
   applications are time series applications. By dividing data using
   time column, it has been able to meet the needs of the vast majority
   of incremental backup in chronological order.

In addition to improving data security, the backup function also
provides a way to export the data. Data can be exported to other
downstream systems for further processing.

.. toctree::
    :hidden:

    Docs/cn/installing/index
    Docs/cn/getting-started/index
    Docs/cn/administrator-guide/index
    Docs/cn/extending-doris/index
    Docs/cn/internal/index
    Docs/cn/sql-reference/index
    Docs/cn/community/index
