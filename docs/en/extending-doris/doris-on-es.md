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

Doris-On-ES not only take advantage of Doris's distributed query planning capability but also ES (Elastic search)'s full-text search capability, provide a more complete OLAP scenario solution:

1. Multi-index Distributed Join Query in ES
2. Joint Query of Tables in Doris and ES, More Complex Full-Text Retrieval and Filtering

This document mainly introduces the realization principle and usage of this function.

## Glossary

### Noun in Doris

* FE: Frontend, the front-end node of Doris. Responsible for metadata management and request access.
* BE: Backend, Doris's back-end node. Responsible for query execution and data storage.

### Noun in ES

* DataNode: The data storage and computing node of ES.
* MasterNode: The Master node of ES, which manages metadata, nodes, data distribution, etc.
* scroll: The built-in data set cursor feature of ES for streaming scanning and filtering of data.
* _source: contains the original JSON document body that was passed at index time
* doc_values: store the same values as the _source but in a column-oriented fashion
* keyword: string datatype in ES, but the content not analyzed by analyzer
* text: string datatype in ES, the content analyzed by analyzer


## How To Use

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
      "doc": { // There is no need to specify the type when creating indexes after ES7.x version, there is one and only type of `_doc`
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

### Add JSON documents to ES index

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

### Create external ES table

```
CREATE EXTERNAL TABLE `test` (
  `k1` bigint(20) COMMENT "",
  `k2` datetime COMMENT "",
  `k3` varchar(20) COMMENT "",
  `k4` varchar(100) COMMENT "",
  `k5` float COMMENT ""
) ENGINE=ELASTICSEARCH // ENGINE must be Elasticsearch
PROPERTIES (
"hosts" = "http://192.168.0.1:8200,http://192.168.0.2:8200",
"index" = "test",
"type" = "doc",
"user" = "root",
"password" = "root"
);
```

The following parameters are accepted by ES table:

Parameter | Description
---|---
**hosts** | ES Cluster Connection Address, maybe one or more node, load-balance is also accepted
**index** | the related ES index name, alias is supported, and if you use doc_value, you need to use the real name
**type** | the type for this index, ES 7.x and later versions do not pass this parameter
**user** | username for ES
**password** | password for the user

* For clusters before 7.x, please pay attention to choosing the correct type when building the table
* The authentication method only supports Http Basic authentication, need to ensure that this user has access to: /\_cluster/state/, \_nodes/http and other paths and index read permissions;The cluster has not turned on security authentication, and the user name and password do not need to be set
* The column names in the Doris table need to exactly match the field names in the ES, and the field types should be as consistent as possible
*  **ENGINE** must be: **Elasticsearch**

##### Filter to push down

An important ability of `Doris On ES` is the push-down of filter conditions: The filtering conditions are pushed to ES, so that only the data that really meets the conditions will be returned, which can significantly improve query performance and reduce CPU, memory, and IO utilization of Doris and ES

The following operators (Operators) will be optimized to the following ES Query:

| SQL syntax  | ES 5.x+ syntax | 
|-------|:---:|
| =   | term query|
| in  | terms query   |
| > , < , >= , ⇐  | range query |
| and  | bool.filter   |
| or  | bool.should   |
| not  | bool.must_not   |
| not in  | bool.must_not + terms query |
| is\_not\_null  | exists query |
| is\_null  | bool.must_not + exists query |
| esquery  | QueryDSL in ES native json form   |

##### Data type mapping

Doris\ES  |  byte | short | integer | long | float | double| keyword | text | date
------------- | ------------- | ------  | ---- | ----- | ----  | ------ | ----| --- | --- |
tinyint  | &radic; |  |  |  |   |   |   |   |  
smallint | &radic; | &radic; |  | |   |   |   |   |  
int | &radic; |  &radic; | &radic; | |   |   |   |   |  
bigint | &radic;  | &radic;  | &radic;  | &radic; |   |   |   |   | 
float |   |   |   |   | &radic; |   |   |   |  
double |   |   |   |   |   | &radic; |   |   |  
char |   |   |   |   |   |   | &radic; | &radic; |  
varchar |  |   |   |   |   |   | &radic; | &radic; |   
date |   |   |   |   |   |   |   |   | &radic;|  
datetime |   |   |   |   |   |   |   |   | &radic;|  


### Enable column scan to optimize query speed(enable\_docvalue\_scan=true)

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

Parameter | Description
---|---
**enable\_docvalue\_scan** | whether to enable ES/Lucene column storage to get the value of the query field, the default is false

Doris obtains data from ES following the following two principles:

* **Best effort**: Automatically detect whether the column to be read has column storage enabled (doc_value: true).If all the fields obtained have column storage, Doris will obtain the values ​​of all fields from the column storage(doc_values)
* **Automatic downgrade**: If the field to be obtained has one or more field that is not have doc_value, the values ​​of all fields will be parsed from the line store `_source`

##### Advantage:

By default, Doris On ES will get all the required columns from the row storage, which is `_source`, and the storage of `_source` is the origin json format document, Inferior to column storage in batch read performance, Especially obvious when only a few columns are needed, When only a few columns are obtained, the performance of docvalue is about ten times that of _source

##### Tip
1. Fields of type `text` are not column-stored in ES, so if the value of the field to be obtained has a field of type `text`, it will be automatically downgraded to get from `_source`
2. In the case of too many fields obtained (`>= 25`), the performance of getting field values ​​from `docvalue` will be basically the same as getting field values ​​from `_source`


### Detect keyword type field(enable\_keyword\_sniff=true)

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

Parameter | Description
---|---
**enable\_keyword\_sniff** | Whether to detect the string type (**text**) `fields` in ES to obtain additional not analyzed (**keyword**) field name(multi-fields mechanism)

You can directly import data without creating an index. At this time, ES will automatically create a new index in ES, For a field of type string, a field of type `text` and field of type `keyword` will be created meantime, This is the multi-fields feature of ES, mapping is as follows:

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
When performing conditional filtering on k4, for example =, Doris On ES will convert the query to ES's TermQuery

SQL filter:

```
k4 = "Doris On ES"
```

The query DSL converted into ES is:

```
"term" : {
    "k4": "Doris On ES"

}
```

Because the first field type of k4 is `text`, when data is imported, it will perform word segmentation processing according to the word segmentator set by k4 (if it is not set, it is the standard word segmenter) to get three Term of doris, on, and es, as follows ES analyze API analysis: 

```
POST /_analyze
{
  "analyzer": "standard",
  "text": "Doris On ES"
}
```
The result of analyzed is:

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
The query uses:

```
"term" : {
    "k4": "Doris On ES"
}
```
This term does not match any term in the dictionary, and will not return any results, enable `enable_keyword_sniff: true` will automatically convert `k4 = "Doris On ES"` into `k4.keyword = "Doris On ES"`to exactly match SQL semantics, The converted ES query DSL is:

```
"term" : {
    "k4.keyword": "Doris On ES"
}
```

The type of `k4.keyword` is `keyword`, and writing data into ES is a complete term, so it can be matched

### Enable node discovery mechanism, default is true(es\_nodes\_discovery=true)

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

Parameter Description：

Parameter | Description
---|---
**es\_nodes\_discovery** | Whether or not to enable ES node discovery. the default is true

Doris would find all available related data nodes (shards allocated on)from ES when this is true.  Just set false if address of  ES data nodes are not accessed by Doris BE, eg. the ES cluster is deployed in the intranet which isolated from your public Internet, and users access through a proxy

### Whether ES cluster enables https access mode, if enabled should set value with`true`, default is false(http\_ssl\_enable=true)

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

Parameter Description：

Parameter | Description
---|---
**http\_ssl\_enabled** | Whether ES cluster enables https access mode

The current FE/BE implementation is to trust all, this is a temporary solution, and the real user configuration certificate will be used later

### Query usage

After create the ES external table in Doris, there is no difference except that the data model (rollup, pre-aggregation, materialized view, etc.) with other table in Doris

#### Basic usage

```
select * from es_table where k1 > 1000 and k3 ='term' or k4 like 'fu*z_'
```

#### Extended esquery(field, QueryDSL)
Through the `esquery(field, QueryDSL)` function, some queries that cannot be expressed in sql, such as match_phrase, geoshape, etc., are pushed down to the ES for filtering. The first column name parameter of `esquery` is used to associate the `index`, the second This parameter is the basic JSON expression of ES's `Query DSL`, which is contained in curly braces `{}`, and there can be only one root key of json, such as match_phrase, geo_shape, bool, etc.
Match query:

```
select * from es_table where esquery(k4, '{
        "match": {
           "k4": "doris on es"
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

1. FE requests the hosts specified by the table to obtain node‘s HTTP port, shards location of the index. If the request fails, it will traverse the host list sequentially until it succeeds or fails completely.

2. When querying, the query plan will be generated and sent to the corresponding BE node according to some node information obtained by FE and metadata information of index.

3. The BE node requests locally deployed ES nodes in accordance with the `proximity principle`. The BE receives data concurrently from each fragment of ES index in the `HTTP Scroll` mode.

4. After calculating the result, return it to client

## Best Practices

### Suggestions for using Date type fields

The use of Datetype fields in ES is very flexible, but in Doris On ES, if the type of the Date type field is not set properly, it will cause the filter condition cannot be pushed down.

When creating an index, do maximum format compatibility with the setting of the Date type format:

```
 "dt": {
     "type": "date",
     "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis"
 }
```

When creating this field in Doris, it is recommended to set it to `date` or `datetime`, and it can also be set to `varchar` type. The following SQL statements can be used to directly push the filter condition down to ES


```
select * from doe where k2 > '2020-06-21';

select * from doe where k2 < '2020-06-21 12:00:00'; 

select * from doe where k2 < 1593497011; 

select * from doe where k2 < now();

select * from doe where k2 < date_format(now(), '%Y-%m-%d');
```

`Notice`:

* If you don’t set the format for the time type field In ES, the default format for Date-type field is

```
strict_date_optional_time||epoch_millis
```
* If the date field indexed into ES is unix timestamp, it needs to be converted to `ms`, and the internal timestamp of ES is processed according to `ms` unit, otherwise Doris On ES will display wrong column data

### Fetch ES metadata field `_id`

When indexing documents without specifying `_id`, ES will assign a globally unique `_id` field  to each document. Users can also specify a `_id` with special represent some business meaning for the document when indexing; if needed, Doris On ES can get the value of this field by adding the `_id` field of type `varchar` when creating the ES external table

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
`Notice`:

1. The filtering condition of the `_id` field only supports two types: `=` and `in`
2. The `_id` field can only be of type `varchar`

## Q&A

1. ES Version Requirements

	The main version of ES is larger than 5. The scanning mode of ES data before 2. X and after 5. x is different. At present, the scanning mode of ES data after 5. x is supported.

2. Does ES Cluster Support X-Pack Authentication

	Support all ES clusters using HTTP Basic authentication

3. Some queries are much slower than requesting ES

	Yes, for example, query related to _count, etc., the ES internal will directly read the number of documents that meet the requirements of the relevant metadata, without the need to filter the real data.
	
4. Whether the aggregation operation can be pushed down

   At present, Doris On ES does not support push-down operations such as sum, avg, min/max, etc., all documents satisfying the conditions are obtained from the ES in batch flow, and then calculated in Doris
