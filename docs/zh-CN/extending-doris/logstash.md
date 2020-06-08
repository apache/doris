---
{
    "title": "Logstash Doris Output Plugin",
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

# Doris output plugin

该插件用于logstash输出数据到Doris，使用 HTTP 协议与 Doris FE Http接口交互，并通过 Doris 的 stream load 的方式进行数据导入.

[了解Doris Stream Load ](http://doris.apache.org/master/zh-CN/administrator-guide/load-data/stream-load-manual.html)

[了解更多关于Doris](http://doris.apache.org/master/zh-CN/)


## 安装和编译
### 1.下载插件源码

### 2.编译 ##
在extension/logstash/ 目录下执行

`gem build logstash-output-doris.gemspec`

你将在同目录下得到 logstash-output-doris-{version}.gem 文件

### 3.插件安装
copy logstash-output-doris-{version}.gem 到 logstash 安装目录下

执行命令`./bin/logstash-plugin install logstash-output-doris-{version}.gem` 安装 logstash-output-doris 插件

## 配置
### 示例：

在config目录下新建一个配置配置文件，命名为 logstash-doris.conf

具体配置如下：

    output {
        doris {
            http_hosts => [ "http://fehost:8030" ]
            user => user_name
            password => password
            db => "db_name"
            table => "table_name"
            label_prefix => "label_prefix"
            column_separator => ","
        }
    }

配置说明：

连接相关配置：

配置 | 说明
--- | ---
`http_hosts` | FE的HTTP交互地址 eg | ["http://fe1:8030", "http://fe2:8030"]
`user` | 用户名，该用户需要有doris对应库表的导入权限
`password` | 密码
`db` | 数据库名
`table` | 表名
`label_prefix` | 导入标识前缀，最终生成的标识为 *{label\_prefix}\_{db}\_{table}\_{time_stamp}*


导入相关配置：([参考文档](http://doris.apache.org/master/zh-CN/administrator-guide/load-data/stream-load-manual.html))

配置 | 说明
--- | ---
`column_separator` | 列分割符，默认为\t。
`columns` | 用于指定导入文件中的列和 table 中的列的对应关系。
`where` | 导入任务指定的过滤条件。
`max_filter_ratio` | 导入任务的最大容忍率，默认零容忍。
`partition` | 待导入表的 Partition 信息。
`timeout` | 超时时间，默认为600s。
`strict_mode` | 严格模式，默认为false。
`timezone` | 指定本次导入所使用的时区，默认为东八区。
`exec_mem_limit` | 导入内存限制，默认为 2GB，单位为字节。

其他配置

配置 | 说明
--- | ---
`save_on_failure` | 如果导入失败是否在本地保存，默认为true
`save_dir` | 本地保存目录，默认为 /tmp
`automatic_retries` | 失败时重试最大次数，默认为3
`batch_size` | 每批次最多处理的event数量，默认为100000
`idle_flush_time` | 最大间隔时间，默认为20（秒）


## 启动
执行命令启动doris output plugin：

`{logstash-home}/bin/logstash -f {logstash-home}/config/logstash-doris.conf --config.reload.automatic`

