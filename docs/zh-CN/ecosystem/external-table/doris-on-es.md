---
{
    "title": "Doris On ES",
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

# Doris On ES

Doris-On-ES将Doris的分布式查询规划能力和ES(Elasticsearch)的全文检索能力相结合，提供更完善的OLAP分析场景解决方案：

1. ES中的多index分布式Join查询
2. Doris和ES中的表联合查询，更复杂的全文检索过滤

本文档主要介绍该功能的实现原理、使用方式等。

## 名词解释

### Doris相关
* FE：Frontend，Doris 的前端节点,负责元数据管理和请求接入
* BE：Backend，Doris 的后端节点,负责查询执行和数据存储

### ES相关
* DataNode：ES的数据存储与计算节点
* MasterNode：ES的Master节点，管理元数据、节点、数据分布等
* scroll：ES内置的数据集游标特性，用来对数据进行流式扫描和过滤
* _source: 导入时传入的原始JSON格式文档内容
* doc_values: ES/Lucene 中字段的列式存储定义
* keyword: 字符串类型字段，ES/Lucene不会对文本内容进行分词处理
* text: 字符串类型字段，ES/Lucene会对文本内容进行分词处理，分词器需要用户指定，默认为standard英文分词器


## 使用方法

### 创建ES索引

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
      "doc": { // ES 7.x版本之后创建索引时不需要指定type，会有一个默认且唯一的`_doc` type
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

### ES索引导入数据

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

### Doris中创建ES外表

具体建表语法参照：[CREATE TABLE](../../sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-TABLE.html)

```
CREATE EXTERNAL TABLE `test` (
  `k1` bigint(20) COMMENT "",
  `k2` datetime COMMENT "",
  `k3` varchar(20) COMMENT "",
  `k4` varchar(100) COMMENT "",
  `k5` float COMMENT ""
) ENGINE=ELASTICSEARCH // ENGINE必须是Elasticsearch
PROPERTIES (
"hosts" = "http://192.168.0.1:8200,http://192.168.0.2:8200",
"index" = "test",
"type" = "doc",

"user" = "root",
"password" = "root"
);
```

参数说明：

参数 | 说明
---|---
**hosts** | ES集群地址，可以是一个或多个，也可以是ES前端的负载均衡地址
**index** | 对应的ES的index名字，支持alias，如果使用doc_value，需要使用真实的名称
**type** | index的type，不指定的情况会使用_doc
**user** | ES集群用户名
**password** | 对应用户的密码信息

* ES 7.x之前的集群请注意在建表的时候选择正确的**索引类型type**
* 认证方式目前仅支持Http Basic认证，并且需要确保该用户有访问: /\_cluster/state/、\_nodes/http等路径和index的读权限; 集群未开启安全认证，用户名和密码不需要设置
* Doris表中的列名需要和ES中的字段名完全匹配，字段类型应该保持一致
*  **ENGINE**必须是 **Elasticsearch**

##### 过滤条件下推
`Doris On ES`一个重要的功能就是过滤条件的下推: 过滤条件下推给ES，这样只有真正满足条件的数据才会被返回，能够显著的提高查询性能和降低Doris和Elasticsearch的CPU、memory、IO使用量

下面的操作符(Operators)会被优化成如下ES Query:

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
| esquery  | ES原生json形式的QueryDSL   |

##### 数据类型映射

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


### 启用列式扫描优化查询速度(enable\_docvalue\_scan=true)

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
"type" = "doc",
"user" = "root",
"password" = "root",

"enable_docvalue_scan" = "true"
);
```

参数说明：

参数 | 说明
---|---
**enable\_docvalue\_scan** | 是否开启通过ES/Lucene列式存储获取查询字段的值，默认为false

开启后Doris从ES中获取数据会遵循以下两个原则：

* **尽力而为**: 自动探测要读取的字段是否开启列式存储(doc_value: true)，如果获取的字段全部有列存，Doris会从列式存储中获取所有字段的值
* **自动降级**: 如果要获取的字段只要有一个字段没有列存，所有字段的值都会从行存`_source`中解析获取

##### 优势：

默认情况下，Doris On ES会从行存也就是`_source`中获取所需的所有列，`_source`的存储采用的行式+json的形式存储，在批量读取性能上要劣于列式存储，尤其在只需要少数列的情况下尤为明显，只获取少数列的情况下，docvalue的性能大约是_source性能的十几倍

##### 注意
1. `text`类型的字段在ES中是没有列式存储，因此如果要获取的字段值有`text`类型字段会自动降级为从`_source`中获取
2. 在获取的字段数量过多的情况下(`>= 25`)，从`docvalue`中获取字段值的性能会和从`_source`中获取字段值基本一样


### 探测keyword类型字段(enable\_keyword\_sniff=true)

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
"type" = "doc",
"user" = "root",
"password" = "root",

"enable_keyword_sniff" = "true"
);
```

参数说明：

参数 | 说明
---|---
**enable\_keyword\_sniff** | 是否对ES中字符串类型分词类型(**text**) `fields` 进行探测，获取额外的未分词(**keyword**)字段名(multi-fields机制)

在ES中可以不建立index直接进行数据导入，这时候ES会自动创建一个新的索引，针对字符串类型的字段ES会创建一个既有`text`类型的字段又有`keyword`类型的字段，这就是ES的multi fields特性，mapping如下：

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
对k4进行条件过滤时比如=，Doris On ES会将查询转换为ES的TermQuery

SQL过滤条件：

```
k4 = "Doris On ES"
```

转换成ES的query DSL为：

```
"term" : {
    "k4": "Doris On ES"

}
```

因为k4的第一字段类型为`text`，在数据导入的时候就会根据k4设置的分词器(如果没有设置，就是standard分词器)进行分词处理得到doris、on、es三个Term，如下ES analyze API分析：

```
POST /_analyze
{
  "analyzer": "standard",
  "text": "Doris On ES"
}
```
分词的结果是：

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
查询时使用的是：

```
"term" : {
    "k4": "Doris On ES"
}
```
`Doris On ES`这个term匹配不到词典中的任何term，不会返回任何结果，而启用`enable_keyword_sniff: true`会自动将`k4 = "Doris On ES"`转换成`k4.keyword = "Doris On ES"`来完全匹配SQL语义，转换后的ES query DSL为:

```
"term" : {
    "k4.keyword": "Doris On ES"
}
```

`k4.keyword` 的类型是`keyword`，数据写入ES中是一个完整的term，所以可以匹配

### 开启节点自动发现, 默认为true(es\_nodes\_discovery=true)

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
"type" = "doc",
"user" = "root",
"password" = "root",

"nodes_discovery" = "true"
);
```

参数说明：

参数 | 说明
---|---
**es\_nodes\_discovery** | 是否开启es节点发现，默认为true

当配置为true时，Doris将从ES找到所有可用的相关数据节点(在上面分配的分片)。如果ES数据节点的地址没有被Doris BE访问，则设置为false。ES集群部署在与公共Internet隔离的内网，用户通过代理访问

### ES集群是否开启https访问模式，如果开启应设置为`true`，默认为false(http\_ssl\_enabled=true)

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
"type" = "doc",
"user" = "root",
"password" = "root",

"http_ssl_enabled" = "true"
);
```

参数说明：

参数 | 说明
---|---
**http\_ssl\_enabled** | ES集群是否开启https访问模式

目前会fe/be实现方式为信任所有，这是临时解决方案，后续会使用真实的用户配置证书

### 查询用法

完成在Doris中建立ES外表后，除了无法使用Doris中的数据模型(rollup、预聚合、物化视图等)外并无区别

#### 基本查询

```
select * from es_table where k1 > 1000 and k3 ='term' or k4 like 'fu*z_'
```

#### 扩展的esquery(field, QueryDSL)
通过`esquery(field, QueryDSL)`函数将一些无法用sql表述的query如match_phrase、geoshape等下推给ES进行过滤处理，`esquery`的第一个列名参数用于关联`index`，第二个参数是ES的基本`Query DSL`的json表述，使用花括号`{}`包含，json的`root key`有且只能有一个，如match_phrase、geo_shape、bool等

match_phrase查询：

```
select * from es_table where esquery(k4, '{
        "match_phrase": {
           "k4": "doris on es"
        }
    }');
```
geo相关查询：

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

bool查询：

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



## 原理

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

1. 创建ES外表后，FE会请求建表指定的主机，获取所有节点的HTTP端口信息以及index的shard分布信息等，如果请求失败会顺序遍历host列表直至成功或完全失败

2. 查询时会根据FE得到的一些节点信息和index的元数据信息，生成查询计划并发给对应的BE节点

3. BE节点会根据`就近原则`即优先请求本地部署的ES节点，BE通过`HTTP Scroll`方式流式的从ES index的每个分片中并发的从`_source`或`docvalue`中获取数据

4. Doris计算完结果后，返回给用户

## 最佳实践

### 时间类型字段使用建议

在ES中，时间类型的字段使用十分灵活，但是在Doris On ES中如果对时间类型字段的类型设置不当，则会造成过滤条件无法下推

创建索引时对时间类型格式的设置做最大程度的格式兼容:

```
 "dt": {
     "type": "date",
     "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis"
 }
```

在Doris中建立该字段时建议设置为`date`或`datetime`,也可以设置为`varchar`类型, 使用如下SQL语句都可以直接将过滤条件下推至ES：

```
select * from doe where k2 > '2020-06-21';

select * from doe where k2 < '2020-06-21 12:00:00'; 

select * from doe where k2 < 1593497011; 

select * from doe where k2 < now();

select * from doe where k2 < date_format(now(), '%Y-%m-%d');
```

注意:

* 在ES中如果不对时间类型的字段设置`format`, 默认的时间类型字段格式为

```
strict_date_optional_time||epoch_millis
```

* 导入到ES的日期字段如果是时间戳需要转换成`ms`, ES内部处理时间戳都是按照`ms`进行处理的, 否则Doris On ES会出现显示错误

### 获取ES元数据字段`_id`

导入文档在不指定`_id`的情况下ES会给每个文档分配一个全局唯一的`_id`即主键, 用户也可以在导入时为文档指定一个含有特殊业务意义的`_id`; 如果需要在Doris On ES中获取该字段值，建表时可以增加类型为`varchar`的`_id`字段：

```
CREATE EXTERNAL TABLE `doe` (
  `_id` varchar COMMENT "",
  `city`  varchar COMMENT ""
) ENGINE=ELASTICSEARCH
PROPERTIES (
"hosts" = "http://127.0.0.1:8200",
"user" = "root",
"password" = "root",
"index" = "doe",
"type" = "doc"
}
```

注意:

1. `_id`字段的过滤条件仅支持`=`和`in`两种
2. `_id`字段只能是`varchar`类型

## Q&A

1. Doris On ES对ES的版本要求

   ES主版本大于5，ES在2.x之前和5.x之后数据的扫描方式不同，目前支持仅5.x之后的

2. 是否支持X-Pack认证的ES集群

   支持所有使用HTTP Basic认证方式的ES集群
3. 一些查询比请求ES慢很多

   是，比如_count相关的query等，ES内部会直接读取满足条件的文档个数相关的元数据，不需要对真实的数据进行过滤

4. 聚合操作是否可以下推

   目前Doris On ES不支持聚合操作如sum, avg, min/max 等下推，计算方式是批量流式的从ES获取所有满足条件的文档，然后在Doris中进行计算
