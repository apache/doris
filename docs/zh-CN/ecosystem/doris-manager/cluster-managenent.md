---
{
    "title": "集群管理",
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

# 集群管理

超级管理员和空间管理员在集群模块下主要可进行如下操作：

- 查看集群概览
- 查看节点列表
- 编辑参数配置

## 集群概览

### 查看集群基本信息

集群功能，展示以集群为粒度的监控面板。

进入首页，点击导航栏中的“集群”，进入集群功能。

![](/images/doris-manager/iclustermanagenent-1.png)

运维监控面板提供集群的各类性能监控指标，供用户洞察集群状态。用户可以在右上角通过按钮控制集群的启动和停止操作。

### 查看集群资源使用量

用户可以通过饼图查看磁盘使用率，以及查看数据库的数量等。

## 节点列表

展示集群中FE节点、BE节点和Broker相关信息。
提供包括节点ID、节点类型、主机IP以及节点状态字段。

![](/images/doris-manager/iclustermanagenent-2.png)

## 参数配置

参数配置提供参数名称、参数类型、参数值类型、热生效和操作字段。

![](/images/doris-manager/iclustermanagenent-3.png)

- **操作**：点击“编辑”按钮，可编辑修改对应配置值，可以选择对应的生效方式；点击“查看当前值”按钮，可查看主机IP对应当前值

![](/images/doris-manager/iclustermanagenent-4.png)

![](/images/doris-manager/iclustermanagenent-5.png)

