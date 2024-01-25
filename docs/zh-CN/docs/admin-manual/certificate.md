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

Doris开启SSL功能需要配置CA密钥证书和Server端密钥证书，如需开启双向认证，还需生成Client端密钥证书：
* 默认的CA密钥证书文件位于`Doris/fe/mysql_ssl_default_certificate/ca_certificate.p12`，默认密码为`doris`，您可以通过修改FE配置文件`conf/fe.conf`，添加`mysql_ssl_default_ca_certificate = /path/to/your/certificate`修改CA密钥证书文件，同时也可以通过`mysql_ssl_default_ca_certificate_password = your_password`添加对应您自定义密钥证书文件的密码。
* 默认的Server端密钥证书文件位于`Doris/fe/mysql_ssl_default_certificate/server_certificate.p12`，默认密码为`doris`，您可以通过修改FE配置文件`conf/fe.conf`，添加`mysql_ssl_default_server_certificate = /path/to/your/certificate`修改Server端密钥证书文件，同时也可以通过`mysql_ssl_default_server_certificate_password = your_password`添加对应您自定义密钥证书文件的密码。
* 默认生成了一份Client端的密钥证书，分别存放在`Doris/fe/mysql_ssl_default_certificate/client-key.pem`和`Doris/fe/mysql_ssl_default_certificate/client_certificate/`。

## 自定义密钥证书文件

除了Doris默认的证书文件，您也可以通过`openssl`生成自定义的证书文件。步骤参考[mysql生成ssl证书](https://dev.mysql.com/doc/refman/8.0/en/creating-ssl-files-using-openssl.html)
具体如下：
1. 生成CA、Server端和Client端的密钥和证书
```
# 生成CA certificate
openssl genrsa 2048 > ca-key.pem
openssl req -new -x509 -nodes -days 3600 \
        -key ca-key.pem -out ca.pem

# 生成server certificate, 并用上述CA签名
# server-cert.pem = public key, server-key.pem = private key
openssl req -newkey rsa:2048 -days 3600 \
        -nodes -keyout server-key.pem -out server-req.pem
openssl rsa -in server-key.pem -out server-key.pem
openssl x509 -req -in server-req.pem -days 3600 \
        -CA ca.pem -CAkey ca-key.pem -set_serial 01 -out server-cert.pem

# 生成client certificate, 并用上述CA签名
# client-cert.pem = public key, client-key.pem = private key
openssl req -newkey rsa:2048 -days 3600 \
        -nodes -keyout client-key.pem -out client-req.pem
openssl rsa -in client-key.pem -out client-key.pem
openssl x509 -req -in client-req.pem -days 3600 \
        -CA ca.pem -CAkey ca-key.pem -set_serial 01 -out client-cert.pem
```

2.验证创建的证书。

```bash
openssl verify -CAfile ca.pem server-cert.pem client-cert.pem
```

3.将您的CA密钥和证书和Sever端密钥和证书分别合并到 PKCS#12 (P12) 包中。您也可以指定某个证书格式，默认PKCS12，可以通过修改conf/fe.conf配置文件，添加参数ssl_trust_store_type指定证书格式

```bash
# 打包CA密钥和证书
openssl pkcs12 -inkey ca-key.pem -in ca.pem -export -out ca_certificate.p12

# 打包Server端密钥和证书
openssl pkcs12 -inkey server-key.pem -in server.pem -export -out server_certificate.p12
```

>[参考文档](https://www.ibm.com/docs/en/api-connect/2018.x?topic=overview-generating-self-signed-certificate-using-openssl)
