---
{
    "title": "Elasticsearch",
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

# Elasticsearch

Elasticsearch Catalog 除了支持自动映射 ES 元数据外，也可以利用 Doris 的分布式查询规划能力和 ES(Elasticsearch) 的全文检索能力相结合，提供更完善的 OLAP 分析场景解决方案：

1. ES 中的多 index 分布式 Join 查询。
2. Doris 和 ES 中的表联合查询，更复杂的全文检索过滤。

## 使用限制

1. 支持 Elasticsearch 5.x 及以上版本。

## 创建 Catalog

```sql
CREATE CATALOG es PROPERTIES (
    "type"="es",
    "hosts"="http://127.0.0.1:9200"
);
```

因为 Elasticsearch 没有 Database 的概念，所以连接 ES 后，会自动生成一个唯一的 Database：`default_db`。

并且在通过 SWITCH 命令切换到 ES Catalog 后，会自动切换到 `default_db`。无需再执行 `USE default_db` 命令。

### 参数说明

| 参数                     | 是否必须 | 默认值   | 说明                                                                     |
|------------------------|------|-------|------------------------------------------------------------------------|
| `hosts`                | 是    |       | ES 地址，可以是一个或多个，也可以是 ES 的负载均衡地址                                         |
| `user`                 | 否    | 空     | ES 用户名                                                                 |
| `password`             | 否    | 空     | 对应用户的密码信息                                                              |
| `doc_value_scan`       | 否    | true  | 是否开启通过 ES/Lucene 列式存储获取查询字段的值                                          |
| `keyword_sniff`        | 否    | true  | 是否对 ES 中字符串分词类型 text.fields 进行探测，通过 keyword 进行查询。设置为 false 会按照分词后的内容匹配 |
| `nodes_discovery`      | 否    | true  | 是否开启 ES 节点发现，默认为 true，在网络隔离环境下设置为 false，只连接指定节点                        |
| `ssl`                  | 否    | false | ES 是否开启 https 访问模式，目前在 fe/be 实现方式为信任所有                                 |
| `mapping_es_id`        | 否    | false | 是否映射 ES 索引中的 `_id` 字段                                                  |
| `like_push_down`       | 否    | true  | 是否将 like 转化为 wildchard 下推到 ES，会增加 ES cpu 消耗                            |
| `include_hidden_index` | 否    | false | 是否包含隐藏的索引，默认为false。                                                    |

> 1. 认证方式目前仅支持 Http Basic 认证，并且需要确保该用户有访问: `/_cluster/state/、_nodes/http` 等路径和 index 的读权限; 集群未开启安全认证，用户名和密码不需要设置。
> 
> 2. 5.x 和 6.x 中一个 index 中的多个 type 默认取第一个。

## 列类型映射

| ES Type | Doris Type | Comment                                                    |
|---|---|------------------------------------------------------------|
|null| null||
| boolean | boolean |                                                            |
| byte| tinyint|                                                            |
| short| smallint|                                                            |
| integer| int|                                                            |
| long| bigint|                                                            |
| unsigned_long| largeint |                                                            |
| float| float|                                                            |
| half_float| float|                                                            |
| double | double |                                                            |
| scaled_float| double |                                                            |
| date | date | 仅支持 default/yyyy-MM-dd HH:mm:ss/yyyy-MM-dd/epoch_millis 格式 |
| keyword | string |                                                            |
| text |string |                                                            |
| ip |string |                                                            |
| nested |string |                                                            |
| object |string |                                                            |
|other| unsupported ||

<version since="dev">

### Array 类型

Elasticsearch 没有明确的数组类型，但是它的某个字段可以含有[0个或多个值](https://www.elastic.co/guide/en/elasticsearch/reference/current/array.html)。
为了表示一个字段是数组类型，可以在索引映射的[_meta](https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-meta-field.html)部分添加特定的`doris`结构注释。
对于 Elasticsearch 6.x 及之前版本，请参考[_meta](https://www.elastic.co/guide/en/elasticsearch/reference/6.8/mapping-meta-field.html)。

举例说明，假设有一个索引`doc`包含以下的数据结构：

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

该结构的数组字段可以通过使用以下命令将字段属性定义添加到目标索引映射的`_meta.doris`属性来定义。

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
```

`array_fields`：用来表示是数组类型的字段。

</version>

## 最佳实践

### 过滤条件下推

ES Catalog 支持过滤条件的下推: 过滤条件下推给ES，这样只有真正满足条件的数据才会被返回，能够显著的提高查询性能和降低Doris和Elasticsearch的CPU、memory、IO使用量

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

### 启用列式扫描优化查询速度(enable\_docvalue\_scan=true)

设置 `"enable_docvalue_scan" = "true"`

开启后Doris从ES中获取数据会遵循以下两个原则：

* **尽力而为**: 自动探测要读取的字段是否开启列式存储(doc_value: true)，如果获取的字段全部有列存，Doris会从列式存储中获取所有字段的值
* **自动降级**: 如果要获取的字段只要有一个字段没有列存，所有字段的值都会从行存`_source`中解析获取

**优势**

默认情况下，Doris On ES会从行存也就是`_source`中获取所需的所有列，`_source`的存储采用的行式+json的形式存储，在批量读取性能上要劣于列式存储，尤其在只需要少数列的情况下尤为明显，只获取少数列的情况下，docvalue的性能大约是_source性能的十几倍

**注意**

1. `text`类型的字段在ES中是没有列式存储，因此如果要获取的字段值有`text`类型字段会自动降级为从`_source`中获取
2. 在获取的字段数量过多的情况下(`>= 25`)，从`docvalue`中获取字段值的性能会和从`_source`中获取字段值基本一样

### 探测keyword类型字段

设置 `"enable_keyword_sniff" = "true"`

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

### 开启节点自动发现, 默认为true(nodes\_discovery=true)

设置 `"nodes_discovery" = "true"`

当配置为true时，Doris将从ES找到所有可用的相关数据节点(在上面分配的分片)。如果ES数据节点的地址没有被Doris BE访问，则设置为false。ES集群部署在与公共Internet隔离的内网，用户通过代理访问

### ES集群是否开启https访问模式

设置 `"ssl" = "true"`

目前会fe/be实现方式为信任所有，这是临时解决方案，后续会使用真实的用户配置证书

### 查询用法

完成在Doris中建立ES外表后，除了无法使用Doris中的数据模型(rollup、预聚合、物化视图等)外并无区别

#### 基本查询

```
select * from es_table where k1 > 1000 and k3 ='term' or k4 like 'fu*z_'
```

#### 扩展的 esquery(field, QueryDSL)

通过`esquery(field, QueryDSL)`函数将一些无法用sql表述的query如match_phrase、geoshape等下推给ES进行过滤处理，`esquery`的第一个列名参数用于关联`index`，第二个参数是ES的基本`Query DSL`的json表述，使用花括号`{}`包含，json的`root key`有且只能有一个，如 `match_phrase`、`geo_shape`、`bool` 等

`match_phrase` 查询：

```
select * from es_table where esquery(k4, '{
        "match_phrase": {
           "k4": "doris on es"
        }
    }');
```

`geo` 相关查询：

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

`bool` 查询：

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

### 时间类型字段使用建议

> 仅 ES 外表适用，ES Catalog 中自动映射日期类型为 Date 或 Datetime

在ES中，时间类型的字段使用十分灵活，但是在 ES 外表中如果对时间类型字段的类型设置不当，则会造成过滤条件无法下推

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

* 导入到ES的日期字段如果是时间戳需要转换成`ms`, ES内部处理时间戳都是按照`ms`进行处理的, 否则 ES 外表会出现显示错误

### 获取ES元数据字段 `_id`

导入文档在不指定 `_id` 的情况下，ES会给每个文档分配一个全局唯一的 `_id` 即主键, 用户也可以在导入时为文档指定一个含有特殊业务意义的 `_id`;

如果需要在 ES 外表中获取该字段值，建表时可以增加类型为`varchar`的`_id`字段：

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

如果需要在 ES Catalog 中获取该字段值，请设置 `"mapping_es_id" = "true"`

注意:

1. `_id` 字段的过滤条件仅支持`=`和`in`两种
2. `_id` 字段必须为 `varchar` 类型

## 常见问题

1. 是否支持X-Pack认证的ES集群

   支持所有使用HTTP Basic认证方式的ES集群

2. 一些查询比请求ES慢很多

   是，比如_count相关的query等，ES内部会直接读取满足条件的文档个数相关的元数据，不需要对真实的数据进行过滤

3. 聚合操作是否可以下推

   目前Doris On ES不支持聚合操作如sum, avg, min/max 等下推，计算方式是批量流式的从ES获取所有满足条件的文档，然后在Doris中进行计算


## 附录

### Doris 查询 ES 原理

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

1. FE会请求建表指定的主机，获取所有节点的HTTP端口信息以及index的shard分布信息等，如果请求失败会顺序遍历host列表直至成功或完全失败

2. 查询时会根据FE得到的一些节点信息和index的元数据信息，生成查询计划并发给对应的BE节点

3. BE节点会根据`就近原则`即优先请求本地部署的ES节点，BE通过`HTTP Scroll`方式流式的从ES index的每个分片中并发的从`_source`或`docvalue`中获取数据

4. Doris计算完结果后，返回给用户
