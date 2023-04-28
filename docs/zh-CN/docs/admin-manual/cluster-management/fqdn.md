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

<version since="dev"></version>

本文介绍如何启用基于完全限定域名（fully qualified domain name，FQDN）使用doris。FQDN是internet上特定计算机或主机的完整域名。

Doris支持FQDN之后，各节点之间通信完全基于FQDN。添加各类节点时应直接指定FQDN，例如添加be节点的命令为`ALTER SYSTEM ADD BACKEND "be_host:heartbeat_service_port`，

"be_host"此前是be节点的ip，启动FQDN后，be_host应指定be节点的FQDN。

## 前置条件

1. fe.conf 文件 设置 `enable_fqdn_mode = true`。
2. 集群中的所有机器都必须配置有主机名。
3. 必须在集群中每台机器的 `/etc/hosts` 文件中指定集群中其他机器对应的 IP 地址和 FQDN。
4. /etc/hosts 文件中不能有重复的 IP 地址。

## 最佳实践

### 新集群启用FQDN

1. 准备机器，例如想部署3fe3be的集群，可以准备六台机器。
2. 每台机器执行`host`返回结果都唯一，假设六台机器的执行结果分别为fe1,fe2,fe3,be1,be2,be3。
3. 在6台机器的`/etc/hosts` 中配置6个fqdn对应的真实ip，例如:
   ```
   172.22.0.1 fe1
   172.22.0.2 fe2
   172.22.0.3 fe3
   172.22.0.4 be1
   172.22.0.5 be2
   172.22.0.6 be3
   ```
4. 验证：可以在fe1上`ping fe2`等，能解析出正确的ip并且能ping通，代表网络环境可用。
5. 每个fe节点的fe.conf设置`enable_fqdn_mode = true`。
6. 参考[标准部署](../../install/standard-deployment.md)

### k8s部署doris

pod意外重启后，k8s不能保证pod的ip不发生变化，但是能保证域名不变，基于这一特性，doris开启fqdn时，能保证pod意外重启后，还能正常提供服务。

k8s部署doris的方法请参考[K8s部署doris](../../install/k8s-deploy.md)

### 服务器变更ip

按照'新集群启用FQDN'部署好集群后，如果想变更机器的ip，无论是切换网卡，或者是更换机器，只需要更改各机器的`/etc/hosts`即可。

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


## 常见问题

- 配置项enable_fqdn_mode可以随意更改么？
 
  不能随意更改，更改该配置要按照'旧集群启用FQDN'进行操作。