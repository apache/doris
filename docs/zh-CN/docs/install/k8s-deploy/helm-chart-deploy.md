---
{
      "title": "基于 Helm-Chart 部署",
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

[Helm](https://helm.sh/zh/) 是查找、分享和使用软件构建 Kubernetes 的最优方式。即使是最复杂的应用，Helm Chart 依然可以描述， 提供使用单点授权的可重复安装应用程序。Doris Helm-Chart 完全继承 doris-operator 的能力，
通过Helm-Chart，可以轻松部署Doris集群，减少繁琐的配置步骤。

## 添加 Doris-Operator 的 helm-chart 远程仓库 。

该 [Helm Chart 仓库](https://artifacthub.io/packages/search?ts_query_web=doris&sort=relevance&page=1) 包含有关 doris-operator 运行的 RBAC 、部署等资源。
1. 添加selectdb 的远程仓库 `https://charts.selectdb.com`。
```Bash
$ helm repo add selectdb https://charts.selectdb.com
```
2. 从远程仓库拉取最新版本的 Helm Chart。
```Bash
$ helm repo update selectdb
```
3. 查看所添加的仓库内容。
```Bash
$ helm search repo selectdb
NAME                       CHART VERSION    APP VERSION   DESCRIPTION
selectdb/doris-operator    1.3.1            1.3.1         Doris-operator for doris creat ...
selectdb/doris             1.3.1            2.0.3         Apache Doris is an easy-to-use ...
```

## 安装 doris-operator Chart


### 安装
- 安装 [doris-operator](https://artifacthub.io/packages/helm/doris/doris-operator)，使用默认配置，默认在名为 `doris` 的 `namespace` 里面。
```Bash
$ helm install operator selectdb/doris-operator
```
- 如果需要自定义装配 [values.yaml](https://artifacthub.io/packages/helm/doris/doris-operator?modal=values) ，可以参考如下命令:
```Bash
$ helm install --namespace doris -f values.yaml operator selectdb/doris-operator 
```
### 验证

- 通过 `helm list` 命令查看 helm 安装后的资源状态列表
```Bash
$ helm list
NAME    	NAMESPACE   REVISION	UPDATED              STATUS  	CHART               	APP VERSION
operator	doris       1       	2024-01-04 18:58:39  deployed	doris-operator-1.3.1	1.3.1
```
- 通过 `kubectl get pods` 命令查看 Pod 的部署状态。注意观察 doris-operator 的 Pod 处于 Running 状态且 Pod 内所有容器都已经就绪，即成功部署。
```Bash
$ kubectl get pod --namespace doris
NAME                              READY   STATUS    RESTARTS   AGE
doris-operator-866bd449bb-zl5mr   1/1     Running   0          18m
```


## 安装 doriscluster Chart

### 安装
- 安装 [doriscluster](https://artifacthub.io/packages/helm/doris/doris)，使用默认配置此部署仅部署 fe 和 be 组件，使用默认 `storageClass` 实现PV动态供给。
```Bash
$ helm install --namespace doris doriscluster selectdb/doris
```
- 如果需要自定义资源和集群形态参考配置 [values.yaml](https://artifacthub.io/packages/helm/doris/doris?modal=values) ，并执行如下命令:
```Bash
$ helm install --namespace doris -f values.yaml doriscluster selectdb/doris 
```
### 验证
执行完安装命令后，需要进行资源划分、镜像拉取、容器初始化、fe启动选主、be启动等步骤，会耗费一定时间。
- 通过 `helm list` 命令查看helm安装后的资源状态列表。
```Bash
$ helm list --namespace doris
NAME            NAMESPACE   REVISION	UPDATED               STATUS  	CHART               	APP VERSION
operator        doris       1       	2024-01-04 18:58:39   deployed	doris-operator-1.3.1    1.3.1
doriscluster	doris       1       	2024-01-04 19:06:36   deployed	doris-1.3.1             2.0.3
```
- 通过 `kubectl get pods` 命令查看 Pod 的部署状态。注意观察 doriscluster 的 Pod 处于 Running 状态且 Pod 内所有容器都已经就绪，即成功部署。
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

## 卸载 doriscluster Chart

### 卸载 doriscluster Chart
确保在该 doris 集群不再使用后，使用如下命令来卸载 doriscluster。
```bash
$ helm uninstall doriscluster
```

### 卸载 doris-operator Chart
确保在 kubernetes 中没有运行 doris 后，使用如下命令来卸载 doris-operator。
```bash
$ helm uninstall operator
```