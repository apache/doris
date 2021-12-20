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

# Seatunnel
最新版本的[Seatunnel (waterdrop)](https://interestinglab.github.io/seatunnel-docs/#/) 已经支持Doris的连接器,seatunnel 可以用过Spark引擎和Flink引擎同步数据至Doirs中.

事实上,Seatunnel通过Stream load方式同步数据,性能强劲,欢迎大家使用

## Spark Sink Doris

### 插件代码
Spark Sink Doris的插件代码在[这里](https://github.com/InterestingLab/seatunnel/tree/dev/seatunnel-connectors/plugin-spark-sink-doris)
### 参数列表
| 参数名 | 参数类型 | 是否必要 | 默认值 | 引擎类型 |
| --- | --- | --- | --- | --- |
| host | string | yes | - | Spark |
| database | string | yes | - | Spark |
| tableName	 | string | yes | - | Spark |
| user	 | string | yes | - | Spark |
| password	 | string | yes | - | Spark |
| bulk_size	 | int | yes | - | Spark |
| doris.*	 | string | no | - | Spark |

`host [string]`

Dioris Fe节点地址:8030

`database [string]`

写入Doris的库名

`tableName [string]`

写入Doris的表名

`user [string]`

Doris访问用户

`password [string]`

Doris访问用户密码

`bulk_size [string]`

Spark通过Stream_load方式写入,每个批次提交条数

`doris. [string]`

Stream_load方式写入的Http参数优化,在官网参数前加上'Doris.'前缀

[更多stream_load参数配置](https://doris.apache.org/master/zh-CN/administrator-guide/load-data/stream-load-manual.html)

### Examples

```
doris {
            host="0.0.0.0:8030"
            database="test"
            tableName="user"
            user="doris"
            password="doris"
            bulk_size=10000
            doris.column_separator="\t"
            doris.columns="id,user_name,user_name_cn,create_time,last_login_time"
      
      }
```