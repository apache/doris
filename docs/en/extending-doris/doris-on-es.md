---
{
    "title": "Doris On ES",
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

# Doris On ES

Doris-On-ES combines Doris's distributed query planning capability with ES (Elastic search)'s full-text search capability to provide a more complete OLAP scenario solution:

1. Multi-index Distributed Join Query in ES
2. Joint Query of Tables in Doris and ES, More Complex Full-Text Retrieval and Filtering
3. Aggregated queries for fields of ES keyword type: suitable for frequent changes in index, tens of millions or more of single fragmented documents, and the cardinality of the field is very large

This document mainly introduces the realization principle and usage of this function.

## Noun Interpretation

* FE: Frontend, the front-end node of Doris. Responsible for metadata management and request access.
* BE: Backend, Doris's back-end node. Responsible for query execution and data storage.
* Elastic search (ES): The most popular open source distributed search engine.
* DataNode: The data storage and computing node of ES.
* MasterNode: The Master node of ES, which manages metadata, nodes, data distribution, etc.
* scroll: The built-in data set cursor feature of ES for streaming scanning and filtering of data.


## How to use it

### Create appearance

```
CREATE EXTERNAL TABLE `es_table` (
  `id` bigint(20) COMMENT "",
  `k1` bigint(20) COMMENT "",
  `k2` datetime COMMENT "",
  `k3` varchar(20) COMMENT "",
  `k4` varchar(100) COMMENT "",
  `k5` float COMMENT ""
) ENGINE=ELASTICSEARCH
PARTITION BY RANGE(`id`)
()
PROPERTIES (
"host" = "http://192.168.0.1:8200,http://192.168.0.2:8200",
"user" = "root",
"password" = "root",
"index" = "tindex”,
"type" = "doc"
);
```

Description of parameters:

Parameter | description
---|---
Host | ES Cluster Connection Address, which can specify one or more, through which Doris obtains the share distribution information of ES version number and index
User | Open the user name of the ES cluster authenticated by basic, you need to ensure that the user has access to: / cluster / state / nodes / HTTP and other path permissions and read permissions for index
Password | corresponding user's password information
The index name of the ES corresponding to the table in index | Doris can be alias
Type | Specifies the type of index, defaulting to _doc
Transport | Internal reservation, default to http

### Query

#### Basic Conditions Filtration

```
select * from es_table where k1 > 1000 and k3 ='term' or k4 like 'fu*z_'
```

#### Extended esquery SQL grammar
The first column name parameter of `esquery` is used to associate `index`, the second parameter is the JSON expression of the basic `Query DSL`, and the curly bracket `{}` is used to include `root` of json. There is and can only be one key of json, such as mat. Ch, geo_shape, bool, etc.

Match query:

```
select * from es_table where esquery(k4, '{
        "match": {
           "k4": "doris on elasticsearch"
        }
    }');
```
Geo related queries:

```
select * from es_table where esquery(k4, '{
      "geo_shape": {
         "location": {
            "shape": {
               "type": "envelope",
               "coordinates": [
                  [
                     13,
                     53
                  ],
                  [
                     14,
                     52
                  ]
               ]
            },
            "relation": "within"
         }
      }
   }');
```

Bool query:

```
select * from es_table where esquery(k4, ' {
         "bool": {
            "must": [
               {
                  "terms": {
                     "k1": [
                        11,
                        12
                     ]
                  }
               },
               {
                  "terms": {
                     "k2": [
                        100
                     ]
                  }
               }
            ]
         }
      }');
```



## Principle

```              
+----------------------------------------------+
|                                              |
| Doris      +------------------+              |
|            |       FE         +--------------+-------+
|            |                  |  Request Shard Location
|            +--+-------------+-+              |       |
|               ^             ^                |       |
|               |             |                |       |
|  +-------------------+ +------------------+  |       |
|  |            |      | |    |             |  |       |
|  | +----------+----+ | | +--+-----------+ |  |       |
|  | |      BE       | | | |      BE      | |  |       |
|  | +---------------+ | | +--------------+ |  |       |
+----------------------------------------------+       |
   |        |          | |        |         |          |
   |        |          | |        |         |          |
   |    HTTP SCROLL    | |    HTTP SCROLL   |          |
+-----------+---------------------+------------+       |
|  |        v          | |        v         |  |       |
|  | +------+--------+ | | +------+-------+ |  |       |
|  | |               | | | |              | |  |       |
|  | |   DataNode    | | | |   DataNode   +<-----------+
|  | |               | | | |              | |  |       |
|  | |               +<--------------------------------+
|  | +---------------+ | | |--------------| |  |       |
|  +-------------------+ +------------------+  |       |
|   Same Physical Node                         |       |
|                                              |       |
|           +-----------------------+          |       |
|           |                       |          |       |
|           |      MasterNode       +<-----------------+
| ES        |                       |          |
|           +-----------------------+          |
+----------------------------------------------+


```

1. After the ES appearance is created, FE requests the host specified by the table to obtain HTTP port information of all nodes and share distribution information of index. If the request fails, it will traverse the host list sequentially until it succeeds or fails completely.

2. When querying, the query plan will be generated and sent to the corresponding BE node according to some node information obtained by FE and metadata information of index.

3. The BE node requests locally deployed ES nodes in accordance with the `proximity principle`. The BE receives data concurrently from each fragment of ES index in the `HTTP Scroll` mode.

4. After calculating the result, return it to client

## Push-Down operations
An important function of `Doris On Elastic` search is to push down filtering conditions: push ES under filtering conditions, so that only data that really meets the conditions can be returned, which can significantly improve query performance and reduce the CPU, memory and IO utilization of Doris and Elastic search.

The following operators are optimized to push down filters as follows:

| SQL syntax  | ES 5.x+ syntax | 
|-------|:---:|
| =   | term query|
| in  | terms query   |
| > , < , >= , ⇐  | range   |
| and  | bool.filter   |
| or  | bool.should   |
| not  | bool.must_not   |
| not in  | bool.must_not + terms  |
| esquery  | ES Query DSL   |


## Other notes

1. ES Version Requirements

	The main version of ES is larger than 5. The scanning mode of ES data before 2. X and after 5. x is different. At present, the scanning mode of ES data after 5. x is supported.

2. Does ES Cluster Support X-Pack Authentication

	Support all ES clusters using HTTP Basic authentication

3. Some queries are much slower than requesting ES

	Yes, for example, query related to _count, etc., the ES internal will directly read the number of documents that meet the requirements of the relevant metadata, without the need to filter the real data.
