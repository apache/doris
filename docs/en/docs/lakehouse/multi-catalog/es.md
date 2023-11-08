---
{
    "title": "Elasticsearch",
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

# Elasticsearch

Elasticsearch (ES) Catalogs in Doris support auto-mapping of ES metadata. Users can utilize the full-text search capability of ES in combination of the distributed query planning capability of Doris to provide a full-fledged OLAP solution that is able to perform:

1. Multi-index distributed Join queries in ES;
2. Join queries across Doris and ES as well as full-text search and filter.

## Usage

1. Doris supports Elasticsearch 5.x and newer versions.

## Create Catalog

```sql
CREATE CATALOG es PROPERTIES (
    "type"="es",
    "hosts"="http://127.0.0.1:9200"
);
```

Since there is no concept of "database" in ES, after connecting to ES, Doris will automatically generate a unique database: `default_db`.

After switching to the ES Catalog, you will be in the `dafault_db`  so you don't need to execute the `USE default_db` command.

### Parameter Description

| Parameter              | Required or Not | Default Value | Description                                                  |
| ---------------------- | --------------- | ------------- | ------------------------------------------------------------ |
| `hosts`                | Yes             |               | ES address, can be one or multiple addresses, or the load balancer address of ES |
| `user`                 | No              | Empty         | ES username                                                  |
| `password`             | No              | Empty         | Password of the corresponding user                           |
| `doc_value_scan`       | No              | true          | Whether to obtain value of the target field by ES/Lucene columnar storage |
| `keyword_sniff`        | No              | true          | Whether to sniff the text.fields in ES based on keyword; If this is set to false, the system will perform matching after tokenization. |
| `nodes_discovery`      | No              | true          | Whether to enable ES node discovery, set to true by default; set to false in network isolation environments and only connected to specified nodes |
| `ssl`                  | No              | false         | Whether to enable HTTPS access mode for ES, currently follows a "Trust All" method in FE/BE |
| `mapping_es_id`        | No              | false         | Whether to map the  `_id`  field in the ES index             |
| `like_push_down`       | No              | true          | Whether to transform like to wildcard push down to es, this increases the cpu consumption of the es. |
| `include_hidden_index` | No              | false         | Whether to include hidden index, default to false.           |

> 1. In terms of authentication, only HTTP Basic authentication is supported and it requires the user to have read privilege for the index and paths including `/_cluster/state/` and `_nodes/http` ; if you have not enabled security authentication for the cluster, you don't need to set the  `user` and `password`.
>
> 2. If there are multiple types in the index in 5.x and 6.x, the first type is taken by default.

## Column Type Mapping

| ES Type       | Doris Type  | Comment                                                                 |
| ------------- | ----------- |-------------------------------------------------------------------------|
| null          | null        |                                                                         |
| boolean       | boolean     |                                                                         |
| byte          | tinyint     |                                                                         |
| short         | smallint    |                                                                         |
| integer       | int         |                                                                         |
| long          | bigint      |                                                                         |
| unsigned_long | largeint    |                                                                         |
| float         | float       |                                                                         |
| half_float    | float       |                                                                         |
| double        | double      |                                                                         |
| scaled_float  | double      |                                                                         |
| date          | date        | Only support default/yyyy-MM-dd HH:mm:ss/yyyy-MM-dd/epoch_millis format |
| keyword       | string      |                                                                         |
| text          | string      |                                                                         |
| ip            | string      |                                                                         |
| nested        | string      |                                                                         |
| object        | string      |                                                                         |
| other         | unsupported |                                                                         |

<version since="dev">

### Array Type

Elasticsearch does not have an explicit array type, but one of its fields can contain 
[0 or more values](https://www.elastic.co/guide/en/elasticsearch/reference/current/array.html).
To indicate that a field is an array type, a specific `doris` structural annotation can be added to the 
[_meta](https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-meta-field.html) section of the index mapping.
For Elasticsearch 6.x and before release, please refer [_meta](https://www.elastic.co/guide/en/elasticsearch/reference/6.8/mapping-meta-field.html).

For example, suppose there is an index `doc` containing the following data structure.

```json
{
  "array_int_field": [1, 2, 3, 4],
  "array_string_field": ["doris", "is", "the", "best"],
  "id_field": "id-xxx-xxx",
  "timestamp_field": "2022-11-12T12:08:56Z",
  "array_object_field": [
    {
      "name": "xxx",
      "age": 18
    }
  ]
}
```

The array fields of this structure can be defined by using the following command to add the field property definition 
to the `_meta.doris` property of the target index mapping.

```bash
# ES 7.x and above
curl -X PUT "localhost:9200/doc/_mapping?pretty" -H 'Content-Type:application/json' -d '
{
    "_meta": {
        "doris":{
            "array_fields":[
                "array_int_field",
                "array_string_field",
                "array_object_field"
            ]
        }
    }
}'

# ES 6.x and before
curl -X PUT "localhost:9200/doc/_mapping?pretty" -H 'Content-Type: application/json' -d '
{
    "_doc": {
        "_meta": {
            "doris":{
                "array_fields":[
                    "array_int_field",
                    "array_string_field",
                    "array_object_field"
                ]
            }
    }
    }
}
'

```

`array_fields`：Used to indicate a field that is an array type.

</version>

## Best Practice

### Predicate Pushdown

ES Catalogs support predicate pushdown to ES, which means only the filtered data will be returned. This can markedly improve query performance and reduce usage of CPU, memory, and IO in both Doris and ES.

For the sake of optimization, operators will be converted into the following ES queries:

| SQL syntax     |        ES 5.x+ syntax        |
| -------------- | :--------------------------: |
| =              |          term query          |
| in             |         terms query          |
| > , < , >= , ⇐ |         range query          |
| and            |         bool.filter          |
| or             |         bool.should          |
| not            |        bool.must_not         |
| not in         | bool.must_not + terms query  |
| is\_not\_null  |         exists query         |
| is\_null       | bool.must_not + exists query |
| esquery        |   ES-native JSON QueryDSL    |

### Columnar Scan for Faster Queries (enable\_docvalue\_scan=true)

Set  `"enable_docvalue_scan" = "true"`.

After this, when obtaining data from ES, Doris will follow these rules:  

* **Try and see**: Doris will automatically check if columnar storage is enabled for the target fields (doc_value: true), if it is, Doris will obtain all values in the fields from the columnar storage.
* **Auto-downgrading**: If any one of the target fields is not available in columnar storage, Doris will parse and obtain all target data from row storage (`_source`).

**Benefits**

By default, Doris On ES obtains all target columns from `_source`, which is in row storage and JSON format. Compared to columnar storage, `_source` is slow in batch read. In particular, when the system only needs to read small number of columns, the performance of  `docvalue`  can be about a dozen times faster than that of  `_source`.

**Note**

1. Columnar storage is not available for `text` fields in ES. Thus, if you need to obtain fields containing `text` values, you will need to obtain them from `_source`.
2. When obtaining large numbers of fields (`>= 25`), the performances of `docvalue`  and  `_source` are basically equivalent.

### Sniff Keyword Fields

Set  `"enable_keyword_sniff" = "true"`.

ES allows direct data ingestion without an index since it will automatically create an index after ingestion. For string fields, ES will create a field with both `text` and `keyword` types. This is how the Multi-Field feature of ES works. The mapping is as follows:

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

For example, to conduct "=" filtering on `k4`, Doris on ES will convert the filtering operation into an ES TermQuery. 

The original SQL filter:

```
k4 = "Doris On ES"
```

The converted ES query DSL:

```
"term" : {
    "k4": "Doris On ES"

}
```

Since the first field of `k4` is `text`, it will be tokenized by the analyzer set for `k4` (or by the standard analyzer if no analyzer has been set for `k4`) after data ingestion. As a result, it will be tokenized into three terms: "Doris", "on", and "ES". 

The details are as follows:

```
POST /_analyze
{
  "analyzer": "standard",
  "text": "Doris On ES"
}
```

The tokenization results:

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

If you conduct a query as follows:

```
"term" : {
    "k4": "Doris On ES"
}
```

Since there is no term in the dictionary that matches the term `Doris On ES`, no result will be returned.

However, if you have set `enable_keyword_sniff: true`, the system will convert `k4 = "Doris On ES"` to `k4.keyword = "Doris On ES"`  to match the SQL semantics. The converted ES query DSL will be:

```
"term" : {
    "k4.keyword": "Doris On ES"
}
```

`k4.keyword`  is of `keyword` type, so the data is written in ES as a complete term, allowing for successful matching.

### Auto Node Discovery, Set to True by Default (nodes\_discovery=true)

Set  `"nodes_discovery" = "true"`.

Then, Doris will discover all available data nodes (the allocated shards) in ES. If Doris BE hasn't accessed the ES data node addresses, then set `"nodes_discovery" = "false"` . ES clusters are deployed in private networks that are isolated from public Internet, so users will need proxy access.

### HTTPS Access Mode for ES Clusters

Set  `"ssl" = "true"`.

A temporary solution is to implement a "Trust All" method in FE/BE. In the future, the real user configuration certificates will be used.

### Query Usage

You can use the ES external tables in Doris the same way as using Doris internal tables, except that the Doris data models (Rollup, Pre-Aggregation, and Materialized Views) are unavailable.

#### Basic Query

```
select * from es_table where k1 > 1000 and k3 ='term' or k4 like 'fu*z_'
```

#### Extended esquery(field, QueryDSL)

The `esquery(field, QueryDSL)` function can be used to push queries that cannot be expressed in SQL, such as `match_phrase` and `geoshape` , to ES for filtering.

In `esquery`, the first parameter (the column name) is used to associate with `index`, while the second parameter is the JSON expression of basic `Query DSL` in ES, which is surrounded by `{}`. The `root key` in JSON is unique, which can be `match_phrase`, `geo_shape` or `bool` , etc.

A `match_phrase` query:

```
select * from es_table where esquery(k4, '{
        "match_phrase": {
           "k4": "doris on es"
        }
    }');
```

A `geo` query:

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

A `bool` query:

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

### Suggestions for Time Fields

> These are only applicable for ES external tables. Time fields will be automatically mapped to Date or Datetime type in ES Catalogs.

ES boasts flexible usage of time fields, but in ES external tables, improper type setting of time fields will result in predicate pushdown failures.

It is recommended to allow the highest level of format compatibility for time fields when creating an index:

```
 "dt": {
     "type": "date",
     "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis"
 }
```

When creating this field in Doris, it is recommended to set its type to `date` or `datetime`  (or `varchar` ) . You can use the following SQL statements to push the filters down to ES.

```
select * from doe where k2 > '2020-06-21';

select * from doe where k2 < '2020-06-21 12:00:00'; 

select * from doe where k2 < 1593497011; 

select * from doe where k2 < now();

select * from doe where k2 < date_format(now(), '%Y-%m-%d');
```

Note:

* The default format of time fields in ES is:

```
strict_date_optional_time||epoch_millis
```

* Timestamps ingested into ES need to be converted into `ms`, which is the internal processing format in ES; otherwise errors will occur in ES external tables.

### Obtain ES Metadata Field `_id`

Each ingested files, if not specified with an  `_id` , will be given a globally unique `_id`, which is the primary key. Users can assign an `_id`  with unique business meanings to the files during ingestion. 

To obtain such field values from ES external tables, you can add an `_id`  field of `varchar`  type when creating tables.

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

To obtain such field values from ES Catalogs, please set `"mapping_es_id" = "true"`.

Note:

1. The `_id`  field only supports `=` and `in` filtering.
2. The`_id`  field must be of  `varchar`  type.

## FAQ

1. Are X-Pack authenticated ES clusters supported?

   All ES clusters with HTTP Basic authentications are supported.

2. Why are some queries require longer response time than those in ES?

   For `_count ` queries, ES can directly read the metadata regarding the number of the specified files instead of filtering the original data. This is a huge time saver.

3. Can aggregation operations be pushed down?

   Currently, Doris On ES does not support pushdown for aggregations such as sum, avg, and min/max. In such operations, Doris obtains all files that met the specified conditions from ES and then conducts computing internally.


## Appendix

### How Doris Conducts Queries in ES

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

1. Doris FE sends a request to the specified host for table creation in order to obtain information about the HTTP port and the index shard allocation.

2. Based on the information about node and index metadata from FE, Doris generates a query plan and send it to the corresponding BE node.

3. Following the principle of proximity, the BE node sends request to the locally deployed ES node, and obtain data from `_source` or `docvalue`  from each shard of ES index concurrently by way of `HTTP Scroll`.

4. Doris returns the computing results to the user.
