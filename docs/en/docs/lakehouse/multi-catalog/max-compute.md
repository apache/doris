---
{
    "title": "Alibaba Cloud Max Compute",
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


# Alibaba Cloud MaxCompute

MaxCompute (previously known as ODPS) is a data warehousing solution that can process terabytes or petabytes of data.

> [What is MaxCompute](https://www.alibabacloud.com/help/en/maxcompute/product-overview/what-is-maxcompute)

## Connect to MaxCompute

```sql
CREATE CATALOG mc PROPERTIES (
  "type" = "max_compute",
  "mc.region" = "cn-beijing",
  "mc.default.project" = "your-project",
  "mc.access_key" = "ak",
  "mc.secret_key" = "sk"
);
```

* `mc.region`: MaxCompute Region. Can Get the Region From [Endpoints](https://www.alibabacloud.com/help/en/maxcompute/user-guide/endpoints).
* `mc.default.project`: MaxCompute Project. See Your [MaxCompute Projects](https://maxcompute.console.aliyun.com/cn-beijing/project-list).
* `mc.access_key`: AccessKey, which you can create and manage on the [Alibaba Cloud console](https://ram.console.aliyun.com/manage/ak).
* `mc.secret_key`: SecretKey, which you can create and manage on the [Alibaba Cloud console](https://ram.console.aliyun.com/manage/ak).
* `mc.public_access`: You can enable public network access for test, when set `"mc.public_access"="true"`.

## Quotas

Pay-as-you-go quota has limited concurrency and usage. For additional resources, please refer to the documentation. See [Manage quotas](https://www.alibabacloud.com/help/en/maxcompute/user-guide/manage-quotas-in-the-new-maxcompute-console).

## Column type mapping

Consistent with Hive Catalog, please refer to the **column type mapping** section in [Hive Catalog](./hive.md).


