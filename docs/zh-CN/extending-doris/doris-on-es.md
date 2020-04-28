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
 3. ES keyword类型字段的聚合查询：适用于index 频繁发生变化、单个分片文档数量千万级以上且该字段基数(cardinality)非常大

本文档主要介绍该功能的实现原理、使用方式等。

## 名词解释

* FE：Frontend，Doris 的前端节点。负责元数据管理和请求接入。
* BE：Backend，Doris 的后端节点。负责查询执行和数据存储。
* Elasticsearch(ES)：目前最流行的开源分布式搜索引擎。
* DataNode：ES的数据存储与计算节点。
* MasterNode：ES的Master节点，管理元数据、节点、数据分布等。
* scroll：ES内置的数据集游标特性，用来对数据进行流式扫描和过滤。


## 如何使用

### 创建外表

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

参数说明：

参数 | 说明
---|---
host | ES集群连接地址，可指定一个或多个，Doris通过这个地址获取到ES版本号、index的shard分布信息
user | 开启basic认证的ES集群的用户名，需要确保该用户有访问: /\_cluster/state/\_nodes/http等路径权限和对index的读权限
password | 对应用户的密码信息
index | Doris中的表对应的ES的index名字，可以是alias
type | 指定index的type，默认是_doc
transport | 内部保留，默认为http

### 查询

#### 基本条件过滤

```
select * from es_table where k1 > 1000 and k3 ='term' or k4 like 'fu*z_'
```

#### 扩展的esquery sql语法
通过`esquery`函数将一些无法用sql表述的ES query如match、geoshape等下推给ES进行过滤处理，`esquery`的第一个列名参数用于关联`index`，第二个参数是ES的基本`Query DSL`的json表述，使用花括号`{}`包含，json的`root key`有且只能有一个，如match、geo_shape、bool等

match查询：

```
select * from es_table where esquery(k4, '{
        "match": {
           "k4": "doris on elasticsearch"
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

2. 查询时，会根据FE得到的一些节点信息和index的元数据信息，生成查询计划并发给对应的BE节点

3. BE节点会根据`就近原则`即优先请求本地部署的ES节点，BE通过`HTTP Scroll`方式流式的从ES index的每个分片中并发的获取数据

4. 计算完结果后，返回给client端

## Push-Down operations
`Doris On Elasticsearch`一个重要的功能就是过滤条件的下推: 过滤条件下推给ES，这样只有真正满足条件的数据才会被返回，能够显著的提高查询性能和降低Doris和Elasticsearch的CPU、memory、IO利用率

下面的操作符(Operators)会被优化成如下下推filters:

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


## 其他说明

1. ES的版本要求

    ES主版本大于5，ES在2.x之前和5.x之后数据的扫描方式不同，目前支持5.x之后的
2. 是否支持X-Pack认证的ES集群

    支持所有使用HTTP Basic认证方式的ES集群
3. 一些查询比请求ES慢很多

    是，比如_count相关的query等，ES内部会直接读取满足条件的文档个数相关的元数据，不需要对真实的数据进行过滤
