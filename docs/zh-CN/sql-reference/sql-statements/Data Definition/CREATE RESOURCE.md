---
{
    "title": "CREATE RESOURCE",
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

# CREATE RESOURCE
## description
    该语句用于创建资源。仅 root 或 admin 用户可以创建资源。目前仅支持 Spark 外部资源。将来其他外部资源可能会加入到 Doris 中使用，如 Spark/GPU 用于查询，HDFS/S3 用于外部存储，MapReduce 用于 ETL 等。
    语法：
        CREATE [EXTERNAL] RESOURCE "resource_name"
        PROPERTIES ("key"="value", ...);
            
    说明：
        1. PROPERTIES中需要指定资源的类型 "type" = "spark"，目前仅支持 spark。
        2. 根据资源类型的不同 PROPERTIES 有所不同，具体见示例。

## example
    1. 创建yarn cluster 模式，名为 spark0 的 Spark 资源。
      CREATE EXTERNAL RESOURCE "spark0"
      PROPERTIES
      (
          "type" = "spark",
          "spark.master" = "yarn",
          "spark.submit.deployMode" = "cluster",
          "spark.jars" = "xxx.jar,yyy.jar",
          "spark.files" = "/tmp/aaa,/tmp/bbb",
          "spark.executor.memory" = "1g",
          "spark.yarn.queue" = "queue0",
          "spark.hadoop.yarn.resourcemanager.address" = "127.0.0.1:9999",
          "spark.hadoop.fs.defaultFS" = "hdfs://127.0.0.1:10000",
          "working_dir" = "hdfs://127.0.0.1:10000/tmp/doris",
          "broker" = "broker0",
          "broker.username" = "user0",
          "broker.password" = "password0"
      );
      
                                                                                                                                                                                                              
     Spark 相关参数如下：                                                              
     1. spark.master: 必填，目前支持yarn，spark://host:port。                         
     2. spark.submit.deployMode: Spark 程序的部署模式，必填，支持 cluster，client 两种。
     3. spark.hadoop.yarn.resourcemanager.address: master为yarn时必填。               
     4. spark.hadoop.fs.defaultFS: master为yarn时必填。                               
     5. 其他参数为可选，参考http://spark.apache.org/docs/latest/configuration.html 
     
     Spark 用于 ETL 时需要指定 working_dir 和 broker。说明如下：
     working_dir: ETL 使用的目录。spark作为ETL资源使用时必填。例如：hdfs://host:port/tmp/doris。
     broker: broker 名字。spark作为ETL资源使用时必填。需要使用`ALTER SYSTEM ADD BROKER` 命令提前完成配置。
     broker.property_key: broker读取ETL生成的中间文件时需要指定的认证信息等。

## keyword
    CREATE, RESOURCE

