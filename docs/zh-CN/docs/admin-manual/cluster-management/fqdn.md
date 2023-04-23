---
{
"title": "FQDN",
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

# FQDN

## 概念介绍

<version since="dev"></version>

完全限定域名fully qualified domain name(FQDN)是internet上特定计算机或主机的完整域名。

Doris支持FQDN之后，添加各类节点时可以直接指定域名，例如添加be节点的命令为`ALTER SYSTEM ADD BACKEND "be_host:heartbeat_service_port`，

"be_host"此前是be节点的ip，启动FQDN后，be_host应指定be节点的域名。

## 前置条件

1. fe.conf 文件 设置 `enable_fqdn_mode = true`
2. fe节点都能解析出doris所有节点的域名

## 最佳实践

### k8s部署doris

pod意外重启后，k8s不能保证pod的ip不发生变化，但是能保证域名不变，基于这一特性，doris开启fqdn时，能保证pod意外重启后，还能正常提供服务。

k8s部署doris的方法请参考[K8s部署doris](../../install/construct-docker/k8s-deploy.md)

### 服务器切换网卡

例如有一个be节点的服务器有两个网卡，对应的ip分别为192.192.192.2和10.10.10.3,目前用192.192.192.2对应的网卡，可以按照下述步骤操作：

1. 在fe所在机器的`etc/hosts`文件增加一行 `192.192.192.2 be1_fqdn`
2. 更改be.conf文件`priority_networks = 192.192.192.2`并启动be
3. 连接执行sql命令 `ALTER SYSTEM ADD BACKEND "be1_fqdn:9050`

将来需要切换到10.10.10.3对应的网卡时，可以按照下述步骤操作：

1. 把`etc/hosts`配置的`192.192.192.2 be1_fqdn` 改为 `10.10.10.3  be1_fqdn`

### 旧集群启用FQDN

前提条件：当前程序支持`ALTER SYSTEM MODIFY FRONTEND "<fe_ip>:<edit_log_port>" HOSTNAME "<fe_hostname>"`语法，
如果不支持，需要升级到支持该语法的版本

接下来按照如下步骤操作：

1. 逐一对follower、observer节点进行以下操作(最后操作master节点)：

    1. 停止节点。
    2. 检查节点是否停止。通过 MySQL 客户端执行`show frontends`，查看该 FE 节点的 Alive 状态直至变为 false
    3. 为节点设置FQDN: `ALTER SYSTEM MODIFY FRONTEND "<fe_ip>:<edit_log_port>" HOSTNAME "<fe_hostname>"`
    4. 修改节点配置。修改FE根目录中的`conf/fe.conf`文件，添加配置：`enable_fqdn_mode = true`
    5. 启动节点。
    
2. BE节点启用FQDN只需要通过MYSQL执行以下命令，不需要对BE执行重启操作。

   `ALTER SYSTEM MODIFY BACKEND "<backend_ip>:<backend_port>" HOSTNAME "<be_hostname>"`


