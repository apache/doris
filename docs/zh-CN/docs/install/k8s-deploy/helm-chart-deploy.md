---
{
"title": "基于 Helm Chart 部署",
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

[Helm](https://helm.sh/zh/) 是查找、分享和使用软件构建 Kubernetes 的最优方式。即使是最复杂的 Kubernetes 应用程序，都可以帮助您定义，安装和升级。
通过 Helm Chart ，可以轻松部署 Doris 集群，减少繁琐的配置步骤。

## 添加部署仓库

该 [Doris 仓库](https://artifacthub.io/packages/search?ts_query_web=doris&sort=relevance&page=1) 包含有关 doris-operator 运行的 RBAC 、部署等资源。
1. 添加 Doris 的远程仓库
```Bash
$ helm repo add doris-repo https://charts.selectdb.com
```
2. 更新 Chart 为最新版本
```Bash
$ helm repo update doris-repo
```
3. 检查更新结果
```Bash
$ helm search repo doris-repo
NAME                          CHART VERSION    APP VERSION   DESCRIPTION
doris-repo/doris-operator     1.3.1            1.3.1         Doris-operator for doris creat ...
doris-repo/doris              1.3.1            2.0.3         Apache Doris is an easy-to-use ...
```

## 安装 doris-operator

### 1. 安装
- 使用默认配置安装 [doris-operator](https://artifacthub.io/packages/helm/doris/doris-operator)，默认在名为 `doris` 的 namespace 里面
```Bash
$ helm install operator doris-repo/doris-operator
```
- 如果需要自定义装配 [values.yaml](https://artifacthub.io/packages/helm/doris/doris-operator?modal=values) ，可以参考如下命令:
```Bash
$ helm install -f values.yaml operator doris-repo/doris-operator 
```
### 2. 验证
- 通过 `kubectl get pods` 命令查看 Pod 的部署状态。当 doris-operator 的 Pod 处于 Running 状态且 Pod 内所有容器都已经就绪，即部署成功
```Bash
$ kubectl get pod --namespace doris
NAME                              READY   STATUS    RESTARTS   AGE
doris-operator-866bd449bb-zl5mr   1/1     Running   0          18m
```

## 安装 doriscluster

### 1. 安装
- 安装 [doriscluster](https://artifacthub.io/packages/helm/doris/doris)，使用默认配置此部署仅部署 3个 FE 和 3个 BE 组件，使用默认 `storageClass` 实现 PV 动态供给
```Bash
$ helm install doriscluster doris-repo/doris
```
- 如果需要自定义资源和集群形态，请根据 [values.yaml](https://artifacthub.io/packages/helm/doris/doris?modal=values) 的各个资源配置的注解自定义资源配置，并执行如下命令:
```Bash
$ helm install -f values.yaml doriscluster doris-repo/doris 
```
### 2. 验证
执行完安装命令后，部署下发，服务部署调度以及启动会耗费一定时间

- 通过 `kubectl get pods` 命令查看 Pod 的部署状态。当 `doriscluster` 的 Pod 处于 `Running` 状态且 Pod 内所有容器都已经就绪，即部署成功
```Bash
$  kubectl get pod --namespace doris
NAME                     READY   STATUS    RESTARTS   AGE
doriscluster-helm-fe-0   1/1     Running   0          1m39s
doriscluster-helm-fe-1   1/1     Running   0          1m39s
doriscluster-helm-fe-2   1/1     Running   0          1m39s
doriscluster-helm-be-0   1/1     Running   0          16s
doriscluster-helm-be-1   1/1     Running   0          16s
doriscluster-helm-be-2   1/1     Running   0          16s
```

## 通过 Helm 卸载 Doris

### 卸载 doriscluster
确保在该 Doris 集群不再使用后，使用如下命令来卸载 `doriscluster`。
```bash
$ helm uninstall doriscluster
```
### 卸载 doris-operator
确保在 Kubernetes 中没有运行 Doris 后，使用如下命令来卸载 `doris-operator`。
```bash
$ helm uninstall operator
```