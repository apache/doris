---
{
    "title": "FE SSL密钥证书配置",
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

# SSL密钥证书配置

<version since="2.0">

SSL密钥证书配置

</version>

Doris FE 接口开启 SSL 功能需要配置密钥证书，步骤如下：

1.购买或生成自签名 SSL 证书，生产环境建议使用 CA 颁发的证书

2.将 SSL 证书复制到指定路径下，默认路径为 `${DORIS_HOME}/conf/ssl/`，用户也可以自己指定路径

3.修改 FE 配置文件 `conf/fe.conf`，注意以下参数与购买或生成的 SSL 证书保持一致
    设置 `enable_https = true` 开启 https 功能，默认为 `false`
    设置证书路径 `key_store_path`，默认为 `${DORIS_HOME}/conf/ssl/doris_ssl_certificate.keystore`
    设置证书密码 `key_store_password`，默认为空
    设置证书类型 `key_store_type` ，默认为 `JKS`
    设置证书别名 `key_store_alias`，默认为 `doris_ssl_certificate`
