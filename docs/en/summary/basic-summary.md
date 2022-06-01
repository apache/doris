---
{
    "title": "Doris base concept",
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

# Doris base concept

Apache Doris is a modern, high-performance, real-time analytical database based on MPP, well-known in the industry for its extremely fast and easy-to-use features. It only needs sub-second response time to return query results under massive data, which can not only support high-concurrency store query scenarios, but also support high-throughput complex query scenarios.

The distributed architecture of Apache Doris is very simple, easy to operate and maintain, and can support large data sets of more than 10PB.

Apache Doris can meet a variety of data analysis needs, such as fixed historical reports, real-time data analysis, interactive data analysis, and exploratory data analysis. Make your data analysis work easier and more efficient!

Based on Apache Doris, users can flexibly build wide tables and star schemas. Various data models, including the snowflake model.

Apache Doris is highly compatible with the MySQL protocol and supports standard SQL syntax. Users can easily use Apache Doris to connect with other systems. The entire system has no external dependencies, provides complete high availability, and is easy to operate and manage.

**The following are several core concepts of the Doris system:**

##  FE (Frontend)

That is, the front-end node of Doris. It is mainly responsible for receiving and returning client requests, metadata, cluster management, and query plan generation.

FE mainly has three roles, one is leader, one is follower, and one is observer. Leader and follower are mainly used to achieve high availability of metadata, ensuring that in the case of a single node downtime, metadata can be restored online in real time without affecting the entire service.

Observer is only used to expand query nodes, that is, if you need to expand the ability of the entire query when the cluster pressure is found to be very high, you can add observer nodes. The observer does not participate in any writes, only reads.

**The main functions are as follows:**

- Manage metadata, execute SQL DDL commands, use Catalog to record information such as libraries, tables, partitions, tablet replicas, etc.
- FE high-availability deployment, using replication protocol to select master and master-slave to synchronize metadata, all metadata modification operations are completed by the FE leader node, and the FE follower node can perform read operations. Metadata reads and writes meet sequential consistency. The number of FE nodes is 2n+1, which can tolerate n node failures. When the FE leader fails, the existing follower node is re-elected to complete the failover.
- FE's SQL layer parses, analyzes, rewrites, semantically analyzes and optimizes relational algebra to the SQL submitted by users, and produces logic execution plans.
- The Planner load of the FE converts the logical plan into a physical plan that can be executed in a distributed manner and distributes it to a group of BEs.
- FE supervises BE, manages the online and offline of BE, and maintains the number of tablet copies according to the survival and health status of BE.
- FE coordinates data import to ensure the consistency of data import.

## BE (Backend)

Backend node of Doris. Mainly responsible for data storage and management, query plan execution and other work.

Doris data is mainly stored in BE. The reliability of physical data on BE nodes is achieved through multiple copies. The default is 3 copies. The number of copies is configurable and can be dynamically adjusted at any time to meet the business needs of different availability levels. FE schedules the distribution and completion of replicas on BE.

The main functions are as follows:

- BE manages tablet copies. Tablets are sub-tables formed by partitioning and bucketing, and are stored in columnar format.
- BE is guided by FE to create or delete child tables.
- BE receives the physical execution plan distributed by FE and designates the BE coordinator node. Under the scheduling of the BE coordinator, it cooperates with other BE workers to complete the execution.
- BE reads the local column storage engine, obtains data, and quickly filters data through index and predicate sinking.
- BE performs compact tasks in the background to reduce read amplification during query.
- During data import, the BE coordinator is designated by the FE to write the data to the BE where multiple copies of the tablet are located.

## Doris metadata

Doris uses the Paxos protocol and the mechanism of Memory + Checkpoint + Journal to ensure high performance and high reliability of metadata.

- Each update of metadata is first written to the log file on the disk, then written to the memory, and finally periodically checkpointed to the local disk.
- It is equivalent to a structure of pure memory, that is to say, all metadata will be cached in memory, so as to ensure that FE can quickly restore metadata after downtime without losing metadata.
- Leader, follower and observer These three constitute a reliable service.
- If we want to keep the service available in the event of node downtime, we generally deploy one leader and two followers, that is, three nodes to achieve a high-availability service. When a single-machine node fails, basically three are enough, because after all, the FE node only stores one piece of metadata, and its pressure is not large, so if there are too many FEs, it will consume machine resources, so most In this case, three are enough to achieve a highly available metadata service.

## Broker (optional component)

Broker is an independent stateless process. It encapsulates the file system interface and provides Doris with the ability to read files in remote storage systems, including HDFS, S3, BOS, etc.

## Tablet

Tablet is the actual physical storage unit of a table. A table is stored in units of Tablet in the distributed storage layer formed by BE after partitioning and bucketing. Each Tablet includes meta information and several consecutive RowSets.

## Rowset

Rowset is a data collection of a data change in Tablet. Data changes include data import, deletion, and update. Rowset records by version information. A version is generated for each change.

### Version

It consists of two attributes, Start and End, and maintains the record information of data changes. Usually used to indicate the version range of Rowset, after a new import, a Rowset with equal Start and End is generated, and a Rowset version with a range is generated after Compaction.

## Segment

Represents a data segment in a Rowset. Multiple Segments form a Rowset.

### Compaction

The process of merging consecutive versions of Rowset is called Compaction, and the data is compressed during the merging process.