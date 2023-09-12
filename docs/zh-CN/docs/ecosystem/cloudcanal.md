---
{
    "title": "CloudCanal 数据导入",
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

# CloudCanal 数据导入

## 介绍

CloudCanal 社区版是一款由 [ClouGence 公司](https://www.clougence.com) 发行的集结构迁移、数据全量迁移/校验/订正、增量实时同步为一体的免费数据迁移同步平台。产品包含完整的产品化能力，助力企业打破数据孤岛、完成数据互融互通，从而更好的使用数据。
![image.png](/images/cloudcanal/cloudcanal-1.jpg)

## 下载安装

[CloudCanal 最新版下载地址](https://www.clougence.com/)

## 功能说明

- 推荐使用2.2.5.0及以上的CloudCanal版本写入Doris
- 建议您在使用 CloudCanal 将 **增量数据** 导入至 Doris 时，控制导入的频率，CloudCanal写入Doris的默认导入频率可以通过参数`realFlushPauseSec`调整，默认为10秒。
- 当前社区版本最大的内存配置为2g，如果同步任务运行产生OOM异常或者GC停顿严重可以调小以下参数减少批次大小从而减少内存占用。全量参数为`fullBatchSize`和`fullRingBufferSize`，增量参数为`increBatchSize`和`increRingBufferSize`
- 支持的源端以及功能项：

  | 数据源 \ 功能项 | 结构迁移 | 全量数据迁移 | 增量实时同步 | 数据校验 |
    | --- | --- | --- | --- | --- |
  | Oracle 源端 | 支持 | 支持 | 支持 | 支持 |
  | PostgreSQL 源端 | 支持 | 支持 | 支持 | 支持 |
  | Greenplum 源端 | 支持 | 支持 | 不支持 | 支持 |
  | MySQL 源端 | 支持 | 支持 | 支持 | 支持 |

## 使用方法

CloudCanal 提供了完整的产品化能力，用户在可视化界面完成数据源添加和任务创建即可自动完成结构迁移、全量迁移、增量实时同步。下文演示如何将 MySQL 数据库中的数据迁移同步到对端 Doris 中。其他源端同步到 Doris 也可以按照类似的方式进行。

### 前置条件

首先参考 [CloudCanal 快速开始](https://www.clougence.com/cc-doc/quick/quick_start) 完成 CloudCanal 社区版的安装和部署。

### 添加数据源

- 登录 CloudCanal 平台
- 数据源管理-> 新增数据源
- 选择自建数据库中 Doris

![image.png](/images/cloudcanal/cloudcanal-1.png)

> Tips:
>
> - Client 地址： 为 Doris 提供给 MySQL Client 的服务端口，CloudCanal 主要用其查询库表的元数据信息
>
> - Http 地址： Http 地址主要用于接收 CloudCanal 数据导入的请求

### 任务创建

添加好数据源之后可以按照如下步骤进行数据迁移、同步任务的创建。

- **任务管理**-> **任务创建**
- 选择 **源** 和 **目标** 数据库
- 点击 下一步

![image.png](/images/cloudcanal/cloudcanal-2.png)

- 选择 **增量同步**，并且启用 **全量数据初始化**
- 勾选 DDL 同步
- 点击下一步

![image.png](/images/cloudcanal/cloudcanal-3.png)

- 选择订阅的表，**结构迁移自动创建的表为主键模型的表，因此暂不支持无主键表**
- 点击下一步

![image.png](/images/cloudcanal/cloudcanal-5.png)

- 配置列映射
- 点击下一步

![image.png](/images/cloudcanal/cloudcanal-6.png)

- 创建任务

![image.png](/images/cloudcanal/cloudcanal-7.png)

- 查看任务状态。任务创建后，会自动完成结构迁移、全量、增量阶段。

![image.png](/images/cloudcanal/cloudcanal-8.jpg)

## 参考资料

更多关于 CloudCanal 同步 Doris 的资料，可以查看

- [5 分钟搞定 PostgreSQL 到 Doris 数据迁移同步-CloudCanal 实战](https://www.clougence.com/cc-doc/blog/postgresql_doris_sync/)

- [CloudCanal 官网](https://www.clougence.com/)
