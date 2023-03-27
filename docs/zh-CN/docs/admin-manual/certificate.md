---
{
    "title": "SSL密钥证书配置",
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

Doris开启SSL功能需要配置密钥证书，默认的密钥证书文件位于`Doris/fe/mysql_ssl_default_certificate/certificate.p12`，默认密码为`doris`，您可以通过修改FE配置文件`conf/fe.conf`，添加`mysql_ssl_default_certificate = /path/to/your/certificate`修改密钥证书文件，同时也可以通过`mysql_ssl_default_certificate_password = your_password`添加对应您自定义密钥证书文件的密码。

## 自定义密钥证书文件

除了Doris默认的证书文件，您也可以通过`openssl`生成自定义的证书文件。步骤如下：

1.运行以下OpenSSL命令以生成您的私钥和公共证书，回答问题并在出现提示时输入答案。

```bash
openssl req -newkey rsa:2048 -nodes -keyout key.pem -x509 -days 365 -out certificate.pem
```

2.查看创建的证书。

```bash
openssl x509 -text -noout -in certificate.pem
```

3.将您的密钥和证书合并到 PKCS#12 (P12) 包中。

```bash
 openssl pkcs12 -inkey key.pem -in certificate.pem -export -out certificate.p12
```

4.验证您的P12文件。

```bash
openssl pkcs12 -in certificate.p12 -noout -info
```

完成这些操作后即可得到certificate.p12文件。

>[参考文档](https://www.ibm.com/docs/en/api-connect/2018.x?topic=overview-generating-self-signed-certificate-using-openssl)
