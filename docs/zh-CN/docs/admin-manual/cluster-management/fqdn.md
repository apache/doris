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

<version since="2.0"></version>

本文介绍如何启用基于 FQDN（Fully Qualified Domain Name，完全限定域名 ）使用 Apache Doris。FQDN 是 Internet 上特定计算机或主机的完整域名。

Doris 支持 FQDN 之后，各节点之间通信完全基于 FQDN。添加各类节点时应直接指定 FQDN，例如添加 BE 节点的命令为`ALTER SYSTEM ADD BACKEND "be_host:heartbeat_service_port"`，

"be_host" 此前是 BE 节点的 IP，启动 FQDN 后，be_host 应指定 BE 节点的 FQDN。

## 前置条件

1. fe.conf 文件 设置 `enable_fqdn_mode = true`。
2. 集群中的所有机器都必须配置有主机名。
3. 必须在集群中每台机器的 `/etc/hosts` 文件中指定集群中其他机器对应的 IP 地址和 FQDN。
4. /etc/hosts 文件中不能有重复的 IP 地址。

## 最佳实践

### 新集群启用 FQDN

1. 准备机器，例如想部署 3FE 3BE 的集群，可以准备 6 台机器。
2. 每台机器执行`host`返回结果都唯一，假设六台机器的执行结果分别为 fe1,fe2,fe3,be1,be2,be3。
3. 在 6 台机器的`/etc/hosts` 中配置 6 个 FQDN 对应的真实 IP，例如:
   ```
   172.22.0.1 fe1
   172.22.0.2 fe2
   172.22.0.3 fe3
   172.22.0.4 be1
   172.22.0.5 be2
   172.22.0.6 be3
   ```
4. 验证：可以在 FE1 上 `ping fe2` 等，能解析出正确的 IP 并且能 Ping 通，代表网络环境可用。
5. 每个 FE 节点的 fe.conf 设置 `enable_fqdn_mode = true`。
6. 参考[标准部署](../../install/standard-deployment.md)
7. 按需在六台机器上选择几台机器部署broker，执行`ALTER SYSTEM ADD BROKER broker_name "fe1:8000","be1:8000",...;`。

### K8s 部署 Doris

Pod 意外重启后，K8s 不能保证 Pod 的 IP 不发生变化，但是能保证域名不变，基于这一特性，Doris 开启 FQDN 时，能保证 Pod 意外重启后，还能正常提供服务。

K8s 部署 Doris 的方法请参考[K8s 部署doris](../../install/k8s-deploy.md)

### 服务器变更 IP

按照'新集群启用 FQDN'部署好集群后，如果想变更机器的 IP，无论是切换网卡，或者是更换机器，只需要更改各机器的`/etc/hosts`即可。

### 旧集群启用 FQDN

前提条件：当前程序支持`ALTER SYSTEM MODIFY FRONTEND "<fe_ip>:<edit_log_port>" HOSTNAME "<fe_hostname>"`语法，
如果不支持，需要升级到支持该语法的版本

>注意：
>
> 至少有三台follower才能进行如下操作，否则会造成集群无法正常启动

接下来按照如下步骤操作：

1. 逐一对 Follower、Observer 节点进行以下操作(最后操作 Master 节点)：

    1. 停止节点。
    2. 检查节点是否停止。通过 MySQL 客户端执行`show frontends`，查看该 FE 节点的 Alive 状态直至变为 false
    3. 为节点设置 FQDN: `ALTER SYSTEM MODIFY FRONTEND "<fe_ip>:<edit_log_port>" HOSTNAME "<fe_hostname>"`（停掉master后，会选举出新的master节点，用新的master节点来执行sql语句）
    4. 修改节点配置。修改 FE 根目录中的`conf/fe.conf`文件，添加配置：`enable_fqdn_mode = true`
    5. 启动节点。
    
2. BE 节点启用 FQDN 只需要通过 MySQL 执行以下命令，不需要对 BE 执行重启操作。

   `ALTER SYSTEM MODIFY BACKEND "<backend_ip>:<backend_port>" HOSTNAME "<be_hostname>"`


## 常见问题

- 配置项 enable_fqdn_mode 可以随意更改么？
 
  不能随意更改，更改该配置要按照'旧集群启用 FQDN'进行操作。
