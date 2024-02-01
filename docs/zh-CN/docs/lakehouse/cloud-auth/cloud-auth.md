---
{
    "title": "云服务认证接入",
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

# 概述

当访问云上的服务时，我们需要提供访问服务所需要的凭证，以便服务能够通过各云厂商IAM的认证。

## AWS

现在Doris访问AWS服务时，能够支持两种类型的身份认证。

### 使用Catalog属性认证

Catalog支持填写基本的Credentials属性，比如：
1. 访问S3时，可以使用s3.endpoint，s3.access_key，s3.secret_key。
2. 访问Glue时，可以使用glue.endpoint，glue.access_key，glue.secret_key。

以Iceberg Catalog访问Glue为例，我们可以填写以下属性访问在Glue上托管的表：

```sql
CREATE CATALOG glue PROPERTIES (
    "type"="iceberg",
    "iceberg.catalog.type" = "glue",
    "glue.endpoint" = "https://glue.us-east-1.amazonaws.com",
    "glue.access_key" = "ak",
    "glue.secret_key" = "sk"
);
```

### 使用系统属性认证

用于运行在AWS资源(如EC2实例)上的应用程序。可以避免硬编码写入Credentials，能够增强数据安全性。

当我们在创建Catalog时，未填写Credentials属性，那么此时会使用DefaultAWSCredentialsProviderChain，它能够读取系统环境变量或者instance profile中配置的属性。

配置环境变量和系统属性的方式可以参考：[AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-envvars.html) 。
- 可以选择的配置的环境变量有：`AWS_ACCESS_KEY_ID`、`AWS_SECRET_ACCESS_KEY`、`AWS_SESSION_TOKEN`、`AWS_ROLE_ARN`、`AWS_WEB_IDENTITY_TOKEN_FILE`等
- 另外，还可以使用[aws configure](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html)直接配置Credentials信息，同时在`~/.aws`目录下生成credentials文件。
