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

# 外表数据湖回归测试工具

用于测试在各家云厂商的对象存储上建立的Doris外表

> 支持的存储格式：HDFS、阿里云OSS、腾讯云COS、华为云OBS

> 支持的数据湖表格式：Iceberg

使用EMR数据湖测试工具的命令行参数举例如下：

```
sh tools/emr_storage_regression/emr_tools.sh --profile default_emr_env.sh
```

或

```
sh tools/emr_storage_regression/emr_tools.sh --case CASE --endpoint ENDPOINT --region REGION  --service SERVICE --ak AK --sk SK  --host HOST --user USER --port PORT
```

下文将给出各个参数的用法示例。

## 连通性测试

`--case`选项设置为ping时，测试Doris外表访问EMR存储的连通性。 配置项如下：

- `--endpoint`，对象存储的Endpoint。

- `--region`，对象存储所在地域。

- `--ak`，访问对象存储的Access Key。

- `--sk`，访问对象存储的Secret Key。

- `--host`，Doris Mysql客户端的IP地址。

- `--user`，Doris Mysql客户端的用户名。

- `--port`，Doris Mysql客户端的端口。

- `--service`，EMR类型。支持ali（阿里云）、hw（华为云）、tx（腾讯云）。

### 环境变量

修改`default_emr_env.sh`中的环境变量，脚本会执行`source default_emr_env.sh`使环境变量生效。

如果已经在`default_emr_env.sh`配置了环境变量，可以使用以下命令直接测试：

```
sh emr_tools.sh --profile default_emr_env.sh
```

### 执行过程

1. 在EMR上创建Spark和Hive表
2. 用Spark和Hive命令行插入样例数据
3. Doris创建连通性测试Catalog
4. 执行连通性测试SQL`ping.sql`

### 阿里云

```
sh emr_tools.sh --profile default_emr_env.sh
```

或

`--service`设置为ali，在阿里云上测试外表连通性。

```
sh emr_tools.sh --case ping --endpoint oss-cn-beijing-internal.aliyuncs.com --region cn-beijing  --service ali --ak ak --sk sk --host 127.0.0.1 --user root --port 9030 > log
```

阿里云EMR上还支持测试DLF元数据，以及OSS-HDFS存储的连通性。

- DLF元数据连通性测试，需要在DLF作为元数据存储的EMR集群上测试，配置同测试OSS连通性。默认`DLF_ENDPOINT`配置为`datalake-vpc.cn-beijing.aliyuncs.com`，在ping_test/ping_poc.sh中配置。

- OSS-HDFS存储连通性测试，需要OSS存储开启HDFS服务，配置同测试OSS连通性。默认`JINDO_ENDPOINT`为`cn-beijing.oss-dls.aliyuncs.com`，在ping_test/ping_poc.sh中配置。

### 腾讯云

```
sh emr_tools.sh --profile default_emr_env.sh
```

或

`--service`设置为tx，在腾讯云上测试外表连通性。

```
sh emr_tools.sh --case ping --endpoint cos.ap-beijing.myqcloud.com --region ap-beijing --service tx --ak ak --sk sk --host 127.0.0.1 --user root --port 9030 > log
```

### 华为云

```
sh emr_tools.sh --profile default_emr_env.sh
```
或

`--service`设置为hw，在华为云上测试外表连通性。

```
sh emr_tools.sh --case ping --endpoint obs.cn-north-4.myhuaweicloud.com --region cn-north-4  --service hw --ak ak --sk sk --host 127.0.0.1 --user root --port 9030 > log 
```

## 标准测试集性能测试

`--case`选项设置为data_set时，测试Doris外表标准测试集的查询性能。 配置项如下：

- `--test`，测试数据集，可选：ssb, ssb_flat, tpch, clickbench, all, 默认是all

- `--service`，Doris Mysql客户端的IP地址。

- `--host`，Doris Mysql客户端的IP地址。

- `--user`，Doris Mysql客户端的用户名。

- `--port`，Doris Mysql客户端的端口。

### 环境变量

修改`default_emr_env.sh`中的环境变量，只需修改上述的几个配置项，脚本会执行`source default_emr_env.sh`使环境变量生效。

如果已经在`default_emr_env.sh`配置了环境变量，可以使用以下命令直接测试：

```
sh emr_tools.sh --profile default_emr_env.sh
```

### 数据准备

1. 使用`emr_tools.sh`脚本运行标准测试集需要改写`BUCKET`变量对应的对象存储bucket地址，然后提前准备好数据放到bucket下，脚本会基于给定bucket生成建表语句。

2. 导入到bucket的数据，`emr_tools.sh`脚本目前支持测试ssb、ssb_flat、tpch、clickbench的parquet和orc数据。

### 使用步骤

1. 在连通性测试之后，标准测试集对应的Doris Catalog都会创建完毕，然后再进行测试
2. 基于填写的`BUCKET`变量指定的对象存储bucket地址，准备好测试集数据
3. 生成Spark建表语句，并在EMR上创建Spark对象存储表
4. 在本地HDFS的目录`hdfs:///benchmark-hdfs`下，创建spark表
5. 可以选择提前analyze Doris表，在Doris Catalog中，手动执行`analyze.sql`中的语句
6. 执行标准测试集测试脚本`run_standard_set.sh`

### 标准测试集ssb, ssb_flat, tpch, clickbench

- 全量测试。执行测试命令后，Doris将会按顺序开始运行ssb, ssb_flat, tpch, clickbench的测试，测试结果包括HDFS上以及`--service`指定的云厂商对象存储上的用例。

```
sh emr_tools.sh --case data_set --service ali  --host 127.0.0.1 --user root --port 9030 > log
```

- 指定单个测试集。执行命令时支持使用`--test`选项指定ssb, ssb_flat, tpch, clickbench中的一个数据集测试。

```
sh emr_tools.sh --case data_set --test ssb --service ali  --host 127.0.0.1 --user root --port 9030 > log
```
