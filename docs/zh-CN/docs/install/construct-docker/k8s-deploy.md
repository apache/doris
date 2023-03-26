---
{
"title": "K8s部署doris",
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

# K8s部署doris

<version since="dev"></version>

## 环境准备

- 安装 k8s
- 构建或下载doris镜像
    - 构建镜像 [Doris Docker 快速搭建开发环境](./docker-dev) 
    - 下载镜像 https://hub.docker.com/r/apache/doris/tags
- 创建或下载doris on k8s的yml文件
    - https://github.com/apache/doris/blob/master/docker/runtime/k8s/doris_follower.yml
    - https://github.com/apache/doris/blob/master/docker/runtime/k8s/doris_be.yml
    - https://github.com/apache/doris/blob/master/docker/runtime/k8s/doris_cn.yml

## 启动集群
启动 FE（角色类型为 Follower）：`kubectl create -f doris_follower.yml` 

启动 BE ：`kubectl create -f doris_be.yml` 

启动 BE（角色类型为 Compute Node）：`kubectl create -f doris_cn.yml`

## 扩缩容

- FE
  - 目前不支持扩缩容，建议按需初始化1个或者3个节点
- BE
  - 命令：`kubectl scale statefulset doris-be-cluster1 --replicas=4`
- BE(角色类型为 Compute Node)
  - 命令：`kubectl scale statefulset doris-cn-cluster1 --replicas=4`

## 验证

使用 mysql-client 连接到 FE，执行 `show backends`，`show frontends`等操作查看各节点状态

## k8s简易操作命令

- 首次执行yml文件 `kubectl create -f xxx.yml`
- 修改yml文件后执行 `kubectl apply -f xxx.yml`
- 删除yml定义的所有资源 `kubectl delete -f xxx.yml`
- 查看pod列表 `kubectl get pods`
- 进入容器 `kubectl exec -it xxx（podName） -- /bin/sh`
- 查看日志 `kubectl logs xxx（podName）`
- 查看ip和端口信息 `kubectl get ep`
- [更多k8s知识](https://kubernetes.io)

## 常见问题

- 数据怎么持久化？

  用户需要自行挂载pvc，持久化元数据信息，数据信息或者日志信息等
- 怎么安全缩容BE节点？

  BE:当前缩容之前需要用户手动执行[ALTER-SYSTEM-DECOMMISSION-BACKEND](../../docs/sql-manual/sql-reference/Cluster-Management-Statements/ALTER-SYSTEM-DECOMMISSION-BACKEND.md)

  BE(角色类型为 Compute Node): 不存储数据文件，可以直接进行缩容，[关于计算节点](../../docs/advanced/compute_node.md)
- FE启动报错"failed to init statefulSetName"

  doris_follower.yml的环境变量 statefulSetName和serviceName必须成对出现，比如配置了CN_SERVICE，就必须配置CN_STATEFULSET




