---
{
"title": "S3 Load",
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

# S3 Load

从0.14 版本开始，Doris 支持通过S3协议直接从支持S3协议的在线存储系统导入数据。

本文档主要介绍如何导入 AWS S3 中存储的数据。也支持导入其他支持S3协议的对象存储系统导入，如果百度云的BOS，阿里云的OSS和腾讯云的COS等、

## 适用场景

* 源数据在 支持S3协议的存储系统中，如 S3,BOS 等。
* 数据量在 几十到百GB 级别。

## 准备工作
1. 准本AK 和 SK
   首先需要找到或者重新生成 AWS `Access keys`，可以在AWS console 的 `My Security Credentials` 找到生成方式， 如下图所示：
   [AK_SK](/images/aws_ak_sk.png)
   选择 `Create New Access Key` 注意保存生成 AK和SK.
2. 准备 REGION 和 ENDPOINT
   REGION 可以在创建桶的时候选择也可以在桶列表中查看到。ENDPOINT 可以通过如下页面通过REGION查到 [AWS 文档](https://docs.aws.amazon.com/general/latest/gr/s3.html#s3_region)

其他云存储系统可以相应的文档找到与S3兼容的相关信息

## 开始导入
导入方式和Broker Load 基本相同，只需要将 `WITH BROKER broker_name ()` 语句替换成如下部分
```
    WITH S3
    (
        "AWS_ENDPOINT" = "AWS_ENDPOINT",
        "AWS_ACCESS_KEY" = "AWS_ACCESS_KEY",
        "AWS_SECRET_KEY"="AWS_SECRET_KEY",
        "AWS_REGION" = "AWS_REGION"
    )
```

完整示例如下
```
    LOAD LABEL example_db.exmpale_label_1
    (
        DATA INFILE("s3://your_bucket_name/your_file.txt")
        INTO TABLE load_test
        COLUMNS TERMINATED BY ","
    )
    WITH S3
    (
        "AWS_ENDPOINT" = "AWS_ENDPOINT",
        "AWS_ACCESS_KEY" = "AWS_ACCESS_KEY",
        "AWS_SECRET_KEY"="AWS_SECRET_KEY",
        "AWS_REGION" = "AWS_REGION"
    )
    PROPERTIES
    (
        "timeout" = "3600"
    );
```

## 常见问题

S3 SDK 默认使用 virtual-hosted style 方式。但某些对象存储系统可能没开启或没支持 virtual-hosted style 方式的访问，此时我们可以添加 `use_path_style` 参数来强制使用 path style 方式：

```
  WITH S3
  (
        "AWS_ENDPOINT" = "AWS_ENDPOINT",
        "AWS_ACCESS_KEY" = "AWS_ACCESS_KEY",
        "AWS_SECRET_KEY"="AWS_SECRET_KEY",
        "AWS_REGION" = "AWS_REGION",
        "use_path_style" = "true"
  )
```
