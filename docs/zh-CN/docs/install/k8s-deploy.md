---
{
  "title": "基于doris-operator 部署",
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

# Kubernetes上部署Doris集群

<version since="dev"></version>
Doris-Operator是按照Kubernetes原则构建的在Kubernetes平台之上管理运维Doris集群的管理软件，允许用户按照资源定义的方式在Kubernetes平台之上部署管理Doris服务。Doris Operator能够管理doris的所有部署形态，能够实现Doris大规模形态下智能化和并行化管理。
## 环境准备
使用Doris-Operator部署doris前提需要一个Kubernetes(简称K8S)集群,如果已拥有可直接跳过环境准备阶段。
### 创建K8S集群
用户可在喜欢的云平台上申请云托管的K8S集群服务，例如：[阿里云的ACK](https://www.aliyun.com/product/kubernetes)或者[腾讯的 TKE](https://cloud.tencent.com/product/tke)等等，也可以按照[Kubernetes](https://kubernetes.io/docs/setup/)官方推荐的方式手动搭建K8S集群。 
- 创建ACK集群  
您可按照阿里云官方文档在阿里云平台创建[ACK集群](https://help.aliyun.com/zh/ack/ack-managed-and-ack-dedicated/getting-started/getting-started/)。
- 创建TKE集群  
如果你使用腾讯云可以按照腾讯云TKE相关文档创建[TKE集群](https://cloud.tencent.com/document/product/457/54231)。
- 创建私有集群  
私有集群搭建，我们建议按照官方推荐的方式搭建，比如:[minikube](https://minikube.sigs.k8s.io/docs/start/)，[kops](https://kubernetes.io/zh-cn/docs/setup/production-environment/tools/kops/)。

## 部署Doris Operator
1. 添加DorisCluster[资源定义](https://kubernetes.io/zh-cn/docs/concepts/extend-kubernetes/api-extension/custom-resources/)。
```shell
kubectl apply -f https://raw.githubusercontent.com/selectdb/doris-operator/master/config/crd/bases/doris.selectdb.com_dorisclusters.yaml
```
2. 部署Doris Operator  
   方式一：默认部署模式  
   直接通过仓库中operator的定义进行部署   
   ```shell
   kubectl apply -f https://raw.githubusercontent.com/selectdb/doris-operator/master/config/operator/operator.yaml
   ```
   方式二：自定义部署  
   [operator.yaml](https://github.com/selectdb/doris-operator/blob/master/config/operator/operator.yaml)中各个配置是部署Operator服务的最低要求。为提高管理效率或者有定制化的需求，下载operator.yaml进行自定义部署。  
   - 下载operator的部署范例[operator.yaml](https://raw.githubusercontent.com/selectdb/doris-operator/master/config/operator/operator.yaml)，可直接通过wget进行下载。
   - 按期望更新operator.yaml中各种配置信息。
   - 通过如下命令部署Doris Operator服务。
   ```shell
   kubectl apply -f operator.yaml
   ```
3. 检查Doris Operator服务部署状态  
Operator服务部署后，可通过如下命令查看服务的状态。当`STATUS`为`Running`状态，且Pod中所有容器都为`Ready`状态时服务部署成功。
```
 kubectl -n doris get pods
 NAME                              READY   STATUS    RESTARTS        AGE
 doris-operator-5b9f7f57bf-tsvjz   1/1     Running   66 (164m ago)   6d22h
```
operator.yaml中namespace默认为doris，如果更改了namespace，在查询服务状态的时候请替换正确的namespace名称。
## 部署Doris集群
1. 部署集群   
`Doris Operator`仓库的[doc/examples](https://github.com/selectdb/doris-operator/tree/master/doc/examples)目录提供众多场景的使用范例,可直接使用范例进行部署。以最基础的范例为例：
```
kubectl apply -f https://raw.githubusercontent.com/selectdb/doris-operator/master/doc/examples/doriscluster-sample.yaml
```
在doris-operator仓库中，[how_to_use.md](https://github.com/selectdb/doris-operator/tree/master/doc/how_to_use.md)梳理了Operator管理运维Doris集群的主要能力，[DorisCluster](https://github.com/selectdb/doris-operator/blob/master/api/doris/v1/types.go)展示了资源定义和从属结构，[api.md](https://github.com/selectdb/doris-operator/tree/master/doc/api.md)可读性展示了资源定义和从属结构。可根据相关文档规划部署Doris集群。  

2. 检测集群状态
- 检查所有pod的状态  
集群部署资源下发后，通过如下命令检查集群状态。当所有pod的`STATUS`都是`Running`状态， 且所有组件的Pod中所有容器都`READY`表示整个集群部署正常。
  ```shell
  kubectl get pods
  NAME                       READY   STATUS    RESTARTS   AGE
  doriscluster-sample-fe-0   1/1     Running   0          20m
  doriscluster-sample-be-0   1/1     Running   0          19m
  ```
- 检查部署资源状态  
Doris Operator会收集集群服务的状态显示到下发的资源中。Doris Operator定义了`DorisCluster`类型资源名称的简写`dcr`,在使用资源类型查看集群状态时可用简写替代。当配置的相关服务的`STATUS`都为`available`时，集群部署成功。
  ```shell
  kubectl get dcr
  NAME                  FESTATUS    BESTATUS    CNSTATUS   BROKERSTATUS
  doriscluster-sample   available   available
  ```
## 访问集群
Doris Operator为每个组件提供K8S的Service作为访问入口，可通过`kubectl -n {namespace} get svc -l "app.doris.ownerreference/name={dorisCluster.Name}"`来查看Doris集群有关的Service。`dorisCluster.Name`为部署`DorisCluster`资源定义的名称。
```shell
kubectl -n default get svc -l "app.doris.ownerreference/name=doriscluster-sample"
NAME                              TYPE        CLUSTER-IP       EXTERNAL-IP                                           PORT(S)                               AGE
doriscluster-sample-fe-internal   ClusterIP   None             <none>                                                9030/TCP                              30m
doriscluster-sample-fe-service    ClusterIP   10.152.183.37    a7509284bf3784983a596c6eec7fc212-618xxxxxx.com        8030/TCP,9020/TCP,9030/TCP,9010/TCP   30m
doriscluster-sample-be-internal   ClusterIP   None             <none>                                                9050/TCP                              29m
doriscluster-sample-be-service    ClusterIP   10.152.183.141   <none>                                                9060/TCP,8040/TCP,9050/TCP,8060/TCP   29m
```
Doris Operator部署的Service分为两类，后缀`-internal`为集群内部组件通信使用的Service，后缀`-service`为用户可使用的service。 
### 集群内部访问  
在K8S内部可通过Service的`CLUSTER-IP`访问对应的组件。如上图可使用访问FE的Service`doriscluster-sample-fe-service`对应的CLUSTER-IP为`10.152.183.37`，使用如下命令连接FE服务。
```shell
mysql -h 10.152.183.37 -uroot -P9030
```
### 集群外部访问  
Doris集群部署默认不提供K8S外部访问，如果集群需要被集群外部访问，需要集群能够申请lb资源。具备前提后，参考[api.md](https://github.com/selectdb/doris-operator/blob/master/doc/api.md)文档配置相关组件`service`字段，部署后通过对应Service的`EXTERNAL-IP`进行访问。以上图中FE为例，使用如下命令连接：
```shell
mysql -h a7509284bf3784983a596c6eec7fc212-618xxxxxx.com -uroot -P9030
```
## 后记
本文简述Doris在Kubernetes的部署使用，Doris Operator提供的其他能力请参看Operator提供的[主要能力介绍](https://github.com/selectdb/doris-operator/tree/master/doc/how_to_use.md)，DorisCluster资源的[api](https://github.com/selectdb/doris-operator/blob/master/doc/api.md)可读性文档定制化部署Doris集群。

