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

- FE: Frontend, the front-end node of Doris. It is mainly responsible for receiving and returning client requests, metadata, cluster management, and query plan generation.
- BE: Backend, the backend node of Doris. Mainly responsible for data storage and management, query plan execution and other work.
- Broker: Broker is a stateless process. It is mainly to help Doris access external data sources such as data on HDFS in a Unix-like file system interface. For example, it is used in data import or data export operations.
- Tablet: Tablet is the actual physical storage unit of a table. A table is stored in units of Tablet in the distributed storage layer composed of BE after partitioning and bucketing. Each Tablet includes meta information and several consecutive RowSets. .
- Rowset: Rowset is a data collection of a data change in the tablet, and the data change includes data import, deletion, and update. Rowset records by version information. A version is generated for each change.
- Version: It consists of two attributes, Start and End, and maintains the record information of data changes. Usually used to indicate the version range of Rowset, after a new import, a Rowset with equal Start and End is generated, and a Rowset version with a range is generated after Compaction.
- Segment: Indicates the data segment in the Rowset. Multiple Segments form a Rowset.
- Compaction: The process of merging consecutive versions of Rowset is called Compaction, and the data will be compressed during the merging process.