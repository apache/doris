---
{
    "title": "Elasticsearch External Table",
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

# Elasticsearch External Table

<version deprecated="1.2.2">

Please use [ES Catalog](../multi-catalog/es) to access Elasticsearch (ES) data sources, this function will no longer be maintained after version 1.2.2.

</version>

Doris-on-ES provides an advanced OLAP solution, where you can benefit from both the distributed query planning capability of Doris and the full-text search capability of ES: 

1. Multi-index distributed Join queries in ES;
2. Join queries across Doris and ES as well as full-text search and filter.

This topic is about how ES External Tables are implemented and used in Doris.

## Basic Concepts

### Doris-Related Concepts
* FE: Frontend of Doris, responsible for metadata management and request processing
* BE: Backend of Doris, responsible for query execution and data storage

### ES-Related Concepts
* DataNode: nodes for data storage and computing in ES
* MasterNode: nodes for managing metadata, nodes, and data distribution in ES
* scroll: built-in dataset cursor in ES, used to stream scan and filter data 
* _source: the original JSON file in data ingestion
* doc_values: the columnar storage definition of fields in ES/Lucene
* keyword: string field, ES/Lucene not tokenizing texts
* text: string field, ES/Lucene tokenizing texts using the specified tokenizer (the standard tokenizer, if not specified)


## Usage

### Create ES Index

```
PUT test
{
   "settings": {
      "index": {
         "number_of_shards": "1",
         "number_of_replicas": "0"
      }
   },
   "mappings": {
      "doc": { // In ES 7.x or newer, you don't have to specify the type when creating an index. It will come with a unique `_doc` type by default.
         "properties": {
            "k1": {
               "type": "long"
            },
            "k2": {
               "type": "date"
            },
            "k3": {
               "type": "keyword"
            },
            "k4": {
               "type": "text",
               "analyzer": "standard"
            },
            "k5": {
               "type": "float"
            }
         }
      }
   }
}
```

### Data Ingestion

```
POST /_bulk
{"index":{"_index":"test","_type":"doc"}}
{ "k1" : 100, "k2": "2020-01-01", "k3": "Trying out Elasticsearch", "k4": "Trying out Elasticsearch", "k5": 10.0}
{"index":{"_index":"test","_type":"doc"}}
{ "k1" : 100, "k2": "2020-01-01", "k3": "Trying out Doris", "k4": "Trying out Doris", "k5": 10.0}
{"index":{"_index":"test","_type":"doc"}}
{ "k1" : 100, "k2": "2020-01-01", "k3": "Doris On ES", "k4": "Doris On ES", "k5": 10.0}
{"index":{"_index":"test","_type":"doc"}}
{ "k1" : 100, "k2": "2020-01-01", "k3": "Doris", "k4": "Doris", "k5": 10.0}
{"index":{"_index":"test","_type":"doc"}}
{ "k1" : 100, "k2": "2020-01-01", "k3": "ES", "k4": "ES", "k5": 10.0}
```

### Create ES External Table in Doris

See [CREATE TABLE](https://doris.apache.org/docs/dev/sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-TABLE/) for syntax details.

```
CREATE EXTERNAL TABLE `test` // You don't have to specify the schema. The system will auto-pull the ES mapping for tabale creation.
ENGINE=ELASTICSEARCH 
PROPERTIES (
"hosts" = "http://192.168.0.1:8200,http://192.168.0.2:8200",
"index" = "test",
"type" = "doc",
"user" = "root",
"password" = "root"
);

CREATE EXTERNAL TABLE `test` (
  `k1` bigint(20) COMMENT "",
  `k2` datetime COMMENT "",
  `k3` varchar(20) COMMENT "",
  `k4` varchar(100) COMMENT "",
  `k5` float COMMENT ""
) ENGINE=ELASTICSEARCH // ENGINE should be Elasticsearch.
PROPERTIES (
"hosts" = "http://192.168.0.1:8200,http://192.168.0.2:8200",
"index" = "test",
"type" = "doc",
"user" = "root",
"password" = "root"
);
```

Parameter Description:

| **Parameter** | **Description**                                              |
| ------------- | ------------------------------------------------------------ |
| **hosts**     | One or multiple ES cluster addresses or the load balancer address of ES frontend |
| **index**     | The corresponding ES index; supports alias, but not when doc_value is used. |
| **type**      | Type of index (no longer needed in ES 7.x or newer)          |
| **user**      | Username for the ES cluster                                  |
| **password**  | The corresponding password                                   |

* In the ES versions before 7.x, please choose the correct **index type** when creating tables.
* Only HTTP Basic authentication is supported. Please make sure the user has access to the relevant paths (/\_cluster/state/, _nodes/http) and read privilege on the index. If you have not enabled security authentication for the clusters, you don't have to set the username and password.
* Please ensure that the column names and types in the Doris are consistent with the field names and types in ES.
*  The **ENGINE** should be **Elasticsearch**.

##### Predicate Pushdown
A key feature of `Doris On ES` is predicate pushdown: The filter conditions will be pushed down to ES so only the filtered data will be returned. This can largely improve query performance and reduce usage of CPU, memory, and IO in Doris and ES.

Operators will be converted into ES queries as follows:

| SQL syntax     |            ES 5.x+ syntax             |
| -------------- | :-----------------------------------: |
| =              |              term query               |
| in             |              terms query              |
| > , < , >= , ⇐ |              range query              |
| and            |              bool.filter              |
| or             |              bool.should              |
| not            |             bool.must_not             |
| not in         |      bool.must_not + terms query      |
| is\_not\_null  |             exists query              |
| is\_null       |     bool.must_not + exists query      |
| esquery        | QueryDSL in the ES-native JSON format |

##### Data Type Mapping

| Doris\ES | byte    | short   | integer | long    | float   | double  | keyword | text    | date    |
| -------- | ------- | ------- | ------- | ------- | ------- | ------- | ------- | ------- | ------- |
| tinyint  | &radic; |         |         |         |         |         |         |         |         |
| smallint | &radic; | &radic; |         |         |         |         |         |         |         |
| int      | &radic; | &radic; | &radic; |         |         |         |         |         |         |
| bigint   | &radic; | &radic; | &radic; | &radic; |         |         |         |         |         |
| float    |         |         |         |         | &radic; |         |         |         |         |
| double   |         |         |         |         |         | &radic; |         |         |         |
| char     |         |         |         |         |         |         | &radic; | &radic; |         |
| varchar  |         |         |         |         |         |         | &radic; | &radic; |         |
| date     |         |         |         |         |         |         |         |         | &radic; |
| datetime |         |         |         |         |         |         |         |         | &radic; |


### Improve Query Speed by Enabling Columnar Scan (enable\_docvalue\_scan=true)

```
CREATE EXTERNAL TABLE `test` (
  `k1` bigint(20) COMMENT "",
  `k2` datetime COMMENT "",
  `k3` varchar(20) COMMENT "",
  `k4` varchar(100) COMMENT "",
  `k5` float COMMENT ""
) ENGINE=ELASTICSEARCH
PROPERTIES (
"hosts" = "http://192.168.0.1:8200,http://192.168.0.2:8200",
"index" = "test",
"user" = "root",
"password" = "root",
"enable_docvalue_scan" = "true"
);
```

Parameter Description:

| Parameter                  | Description                                                  |
| -------------------------- | ------------------------------------------------------------ |
| **enable\_docvalue\_scan** | This specifies whether to acquire value from the query field via ES/Lucene columnar storage. It is set to false by default. |

If this parameter is set to true, Doris will follow these rules when obtaining data from ES:

* **Try and see**: Doris will automatically check if columnar storage is enabled for the target fields (doc_value: true), if it is, Doris will obtain all values in the fields from the columnar storage.
* **Auto-downgrading**: If any one of the target fields is not available in columnar storage, Doris will parse and obtain all target data from row storage (`_source`).

##### Benefits:

By default, Doris-on-ES obtains all target columns from `_source`, which is in row storage and JSON format. Compared to columnar storage, `_source` is slow in batch read. In particular, when the system only needs to read small number of columns, the performance of `docvalue` can be about a dozen times faster than that of `_source`.

##### Note
1. Columnar storage is not available for `text` fields in ES. Thus, if you need to obtain fields containing `text` values, you will need to obtain them from `_source`.
2. When obtaining large numbers of fields (`>= 25`), the performances of `docvalue` and `_source` are basically equivalent.

### Sniff Keyword Fields (enable\_keyword\_sniff=true)

```
CREATE EXTERNAL TABLE `test` (
  `k1` bigint(20) COMMENT "",
  `k2` datetime COMMENT "",
  `k3` varchar(20) COMMENT "",
  `k4` varchar(100) COMMENT "",
  `k5` float COMMENT ""
) ENGINE=ELASTICSEARCH
PROPERTIES (
"hosts" = "http://192.168.0.1:8200,http://192.168.0.2:8200",
"index" = "test",
"user" = "root",
"password" = "root",
"enable_keyword_sniff" = "true"
);
```

Parameter Description:

| Parameter                  | Description                                                  |
| -------------------------- | ------------------------------------------------------------ |
| **enable\_keyword\_sniff** | This specifies whether to sniff (**text**)  `fields` for untokenized (**keyword**) fields (multi-fields mechanism) |

You can start data ingestion without creating an index since ES will generate a new index automatically. For string fields, ES will create a field of both `text` type and `keyword`  type. This is the multi-fields mechanism of ES. The mapping goes as follows:

```
"k4": {
   "type": "text",
   "fields": {
      "keyword": {   
         "type": "keyword",
         "ignore_above": 256
      }
   }
}
```
In conditional filtering of k4, "=" filtering for example，Doris-on-ES will convert the query into an ES TermQuery.

SQL filter:

```
k4 = "Doris On ES"
```

Converted query DSL in ES:

```
"term" : {
    "k4": "Doris On ES"

}
```

The primary field type of k4 is `text` so on data ingestion, the designated tokenizer (or the standard tokenizer, if no specification) for k4 will split it into three terms: "doris", "on", and "es". 

For example:

```
POST /_analyze
{
  "analyzer": "standard",
  "text": "Doris On ES"
}
```
It will be tokenized as follows:

```
{
   "tokens": [
      {
         "token": "doris",
         "start_offset": 0,
         "end_offset": 5,
         "type": "<ALPHANUM>",
         "position": 0
      },
      {
         "token": "on",
         "start_offset": 6,
         "end_offset": 8,
         "type": "<ALPHANUM>",
         "position": 1
      },
      {
         "token": "es",
         "start_offset": 9,
         "end_offset": 11,
         "type": "<ALPHANUM>",
         "position": 2
      }
   ]
}
```
The term used in the query is:

```
"term" : {
    "k4": "Doris On ES"
}
```
Since `Doris On ES` does not match any term in the dictionary, no result will be returned. However, if you `enable_keyword_sniff: true` , then  `k4 = "Doris On ES"` will be turned into `k4.keyword = "Doris On ES"`. The converted ES query DSL will be:

```
"term" : {
    "k4.keyword": "Doris On ES"
}
```

In this case, `k4.keyword` is of `keyword` type and the data writted into ES is a complete term so the matching can be done.

### Enable Node Discovery (nodes\_discovery=true)

```
CREATE EXTERNAL TABLE `test` (
  `k1` bigint(20) COMMENT "",
  `k2` datetime COMMENT "",
  `k3` varchar(20) COMMENT "",
  `k4` varchar(100) COMMENT "",
  `k5` float COMMENT ""
) ENGINE=ELASTICSEARCH
PROPERTIES (
"hosts" = "http://192.168.0.1:8200,http://192.168.0.2:8200",
"index" = "test",
"user" = "root",
"password" = "root",
"nodes_discovery" = "true"
);
```

Parameter Description:

| Parameter            | Description                                                  |
| -------------------- | ------------------------------------------------------------ |
| **nodes\_discovery** | This specifies whether to enable ES node discovery. It is set to true by default. |

If this is set to true, Doris will locate all relevant data nodes (the allocated tablets) that are available. If the data node addresses are not accessed by Doris BE, this should be set to false. The deployment of ES clusters is done in an intranet so users require proxy access.

### Enable HTTPS Access Mode for ES Clusters (http_ssl_enabled=true)

```
CREATE EXTERNAL TABLE `test` (
  `k1` bigint(20) COMMENT "",
  `k2` datetime COMMENT "",
  `k3` varchar(20) COMMENT "",
  `k4` varchar(100) COMMENT "",
  `k5` float COMMENT ""
) ENGINE=ELASTICSEARCH
PROPERTIES (
"hosts" = "http://192.168.0.1:8200,http://192.168.0.2:8200",
"index" = "test",
"user" = "root",
"password" = "root",
"http_ssl_enabled" = "true"
);
```

Parameter Description:

| Parameter              | Description                                                  |
| ---------------------- | ------------------------------------------------------------ |
| **http\_ssl\_enabled** | This specifies whether to enable HTTPS access mode for ES cluster. It is set to false by default. |

Currently, the FE and BE implement a trust-all method, which is temporary solution. The actual user configuration certificate will be used in the future.

### Query

After creating an ES External Table in Doris, you can query data from ES as simply as querying data in Doris itself, except that you won't be able to use the Doris data models (rollup, pre-aggregation, and materialized view).

#### Basic Query

```
select * from es_table where k1 > 1000 and k3 ='term' or k4 like 'fu*z_'
```

#### Extended esquery (field, QueryDSL)
For queries that cannot be expressed in SQL, such as match_phrase and geoshape, you can use the `esquery(field, QueryDSL)` function to push them down to ES for filtering. The first parameter `field`  associates with  `index` ; the second one is the Json expression of ES query DSL, which should be surrounded by `{}`. There should be one and only one `root key`, such as match_phrase, geo_shape, and bool.

For example, a match_phrase query:

```
select * from es_table where esquery(k4, '{
        "match_phrase": {
           "k4": "doris on es"
        }
    }');
```
A geo query:

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

A bool query:

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



## Illustration

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

1. After an ES External Table is created, Doris FE will send a request to the designated host for information regarding HTTP port and index shard allocation. If the request fails, Doris FE will traverse all hosts until the request succeeds or completely fails.
2. Based on the nodes and metadata in indexes, Doris FE will generate a query plan and send it to the relevant BE nodes.
3. The BE nodes will send requests to locally deployed ES nodes. Via `HTTP Scroll`, BE nodes obtain data in `_source` and `docvalue` concurrently from each tablet in ES index.
4. Doris returns the query results to the user.

## Best Practice

### Usage of Time Field 

ES allows flexible use of time fields, but improper configuration of time field types can lead to predicate pushdown failure.

When creating an index, allow the greatest format compatibility for time data types:

```
 "dt": {
     "type": "date",
     "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis"
 }
```

It is recommended to set the corresponding fields in Doris to `date` or `datetime` type (or `varchar`). Then you can use the following SQL statement to push the filters down to ES:

```
select * from doe where k2 > '2020-06-21';

select * from doe where k2 < '2020-06-21 12:00:00'; 

select * from doe where k2 < 1593497011; 

select * from doe where k2 < now();

select * from doe where k2 < date_format(now(), '%Y-%m-%d');
```

Note:

* If you don't specify the `format` for time fields in ES, the default format will be: 

```
strict_date_optional_time||epoch_millis
```

* Timestamps should be converted to `ms`  before they are imported into ES; otherwise errors might occur in Doris-on-ES.

### Obtain ES Metadata Field `_id`

You can specify an informative `_id` for a file on ingestion. If not, ES will assign a globally unique `_id` (the primary key) to the file. If you need to acquire the `_id` through Doris-on-ES, you can add a `_id` field of `varchar`  type upon table creation. 

```
CREATE EXTERNAL TABLE `doe` (
  `_id` varchar COMMENT "",
  `city`  varchar COMMENT ""
) ENGINE=ELASTICSEARCH
PROPERTIES (
"hosts" = "http://127.0.0.1:8200",
"user" = "root",
"password" = "root",
"index" = "doe"
}
```

Note:

1. `_id` fields only support `=` and `in` filters.
2. `_id` field should be of `varchar` type.

## FAQ

1. What versions of ES does Doris-on-ES support?

   Doris-on-ES supports ES 5.x or newer since the data scanning works differently in older versions of ES.

2. Are X-Pack authenticated  ES clusters supported？

   All ES clusters with HTTP Basic authentication are supported.

3. Why are some queries a lot slower than direct queries oo ES?

   For certain queries such as `_count`, ES can directly read the metadata for the number of files that meet the conditions, which is much faster than reading and filtering all the data.

4. Can aggregation operations be pushed down?

   Currently, Doris-on-ES does not support pushing down aggregation operations such as `sum`, `avg`, and `min`/`max`. Instead, all relevant files from ES will be streamed into Doris in batches, where the computation will be performed.

