---
{
    "title": "阿里云 Max Compute",
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


# 阿里云 MaxCompute

MaxCompute是阿里云上的企业级SaaS（Software as a Service）模式云数据仓库。

> [什么是 MaxCompute](https://help.aliyun.com/zh/maxcompute/product-overview/what-is-maxcompute?spm=a2c4g.11174283.0.i1)

## 连接 Max Compute

```sql
CREATE CATALOG mc PROPERTIES (
  "type" = "max_compute",
  "mc.region" = "cn-beijing",
  "mc.default.project" = "your-project",
  "mc.access_key" = "ak",
  "mc.secret_key" = "sk"
);
```

* `mc.region`：MaxCompute开通的地域。可以从Endpoint中找到对应的Region，参阅[Endpoints](https://help.aliyun.com/zh/maxcompute/user-guide/endpoints?spm=a2c4g.11186623.0.0)。
* `mc.default.project`：MaxCompute项目。可以在[MaxCompute项目列表](https://maxcompute.console.aliyun.com/cn-beijing/project-list)中创建和管理。
* `mc.access_key`：AccessKey。可以在 [阿里云控制台](https://ram.console.aliyun.com/manage/ak) 中创建和管理。
* `mc.secret_key`：SecretKey。可以在 [阿里云控制台](https://ram.console.aliyun.com/manage/ak) 中创建和管理。
* `mc.public_access`: 当配置了`"mc.public_access"="true"`，可以开启公网访问，建议测试时使用。

## 限额

连接MaxCompute时，按量付费的Quota查询并发和使用量有限，如需增加资源，请参照MaxCompute文档。参见[配额管理](https://help.aliyun.com/zh/maxcompute/user-guide/manage-quotas-in-the-new-maxcompute-console).

## 列类型映射

和 Hive Catalog 一致，可参阅 [Hive Catalog](./hive.md) 中 **列类型映射** 一节。


