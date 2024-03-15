---
{
    "title": "访问 Doris 集群",
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

在 Kubernetes 中，Service 是用于定义一组 pod，并提供对这组 pod 的稳定访问能力的资源，因此它可以与一组 Pod 关联。
通过 Doris-Operator 部署集群，会根据 `spec.*Spec.service` 配置自动生成对应的 Service 资源，目前支持 ClusterIP、LoadBalancer 和 NodePort 模式。支持用户不同场景的访问需求。

## 在 Kubernetes 集群内部访问
### ClusterIP 模式
Doris 在 kubernetes 上默认提供 kubernetes 集群内部 `ClusterIP` 的访问方式，对于 FE 和 BE 组件我们分别提供相应的 Service 资源供用户在 kubernetes 上按需使用。使用如下命令查看对应组件的 Service，Doris-Operator 提供的 Service 命名规则为 `{clusterName}-{fe/be}-service` 的方式。
```shell
$ kubectl -n {namespace} get service
```
在使用过程中，请将 {namespace} 替换成部署时候指定的 namespace。以我们默认的样例部署的 Doris 集群为例：
```yaml
apiVersion: doris.selectdb.com/v1
kind: DorisCluster
metadata:
  labels:
    app.kubernetes.io/name: doriscluster
    app.kubernetes.io/instance: doriscluster-sample
    app.kubernetes.io/part-of: doris-operator
  name: doriscluster-sample
spec:
  feSpec:
    replicas: 3
    limits:
      cpu: 6
      memory: 12Gi
    requests:
      cpu: 6
      memory: 12Gi
    image: selectdb/doris.fe-ubuntu:2.0.2
  beSpec:
    replicas: 3
    limits:
      cpu: 8
      memory: 16Gi
    requests:
      cpu: 8
      memory: 16Gi
    image: selectdb/doris.be-ubuntu:2.0.2
```
我们通过命令查看 `kubectl get service` 如下：
```shell
$ kubectl get service
NAME                              TYPE        CLUSTER-IP       EXTERNAL-IP   PORT(S)                               AGE
doriscluster-sample-be-internal   ClusterIP   None             <none>        9050/TCP                              12h
doriscluster-sample-be-service    ClusterIP   172.20.217.234   <none>        9060/TCP,8040/TCP,9050/TCP,8060/TCP   12h
doriscluster-sample-fe-internal   ClusterIP   None             <none>        9030/TCP                              12h
doriscluster-sample-fe-service    ClusterIP   172.20.183.136   <none>        8030/TCP,9020/TCP,9030/TCP,9010/TCP   12h
```
通过命令展示有 FE 和 BE 的两类 Service，后缀为 `internal` 的 Service 为 Doris 内部通信使用的 Service，外部不可用。后缀为 `-service` 为供用户使用的 Service。  
在这个例子中， 在 Kubernetes 集群之上可以使用 `doriscluster-sample-fe-service` 对应的 `CLUSTER-IP` 以及后面对应的 `PORT` 来访问 FE 的不同端口服务。使用 `doriscluster-sample-be-service` Service
以及对应的 `PORT` 的端口来访问 BE 的服务。

## 在 Kubernetes 集群外部访问
### LoadBalancer 模式
如果集群在相关云平台创建，建议使用 `LoadBalancer` 的模式来访问集群内部的 FE 和 BE 服务。 默认情况下使用的是 `ClusterIP` 的模式，如果想要使用 `LoadBalancer` 模式，请在每个组件的 spec 配置如下配置：
```yaml
service:
  type: LoadBalancer
```
以默认的配置作为修改蓝本为例，我们在云平台上使用 `LoadBalancer` 作为 FE 和 BE 的访问模式，部署配置如下：
```yaml
apiVersion: doris.selectdb.com/v1
kind: DorisCluster
metadata:
  labels:
    app.kubernetes.io/name: doriscluster
    app.kubernetes.io/instance: doriscluster-sample
    app.kubernetes.io/part-of: doris-operator
  name: doriscluster-sample
spec:
  feSpec:
    replicas: 3
    service:
      type: LoadBalancer
    limits:
      cpu: 6
      memory: 12Gi
    requests:
      cpu: 6
      memory: 12Gi
    image: selectdb/doris.fe-ubuntu:2.0.2
  beSpec:
    replicas: 3
    service:
      type: LoadBalancer
    limits:
      cpu: 8
      memory: 16Gi
    requests:
      cpu: 8
      memory: 16Gi
    image: selectdb/doris.be-ubuntu:2.0.2
```
通过查看 `kubectl get service` 的命令，查看相应的 Service 展示如下：
```shell
$ kubectl get service
NAME                              TYPE           CLUSTER-IP       EXTERNAL-IP                                                               PORT(S)                                                       AGE
doriscluster-sample-be-internal   ClusterIP      None             <none>                                                                    9050/TCP                                                      14h
doriscluster-sample-be-service    LoadBalancer   172.20.217.234   a46bbcd6998c7436bab8ee8fba9f5e81-808549982.us-east-1.elb.amazonaws.com    9060:32060/TCP,8040:30615/TCP,9050:31742/TCP,8060:31127/TCP   14h
doriscluster-sample-fe-internal   ClusterIP      None             <none>                                                                    9030/TCP                                                      14h
doriscluster-sample-fe-service    LoadBalancer   172.20.183.136   ac48284932b044251bfac389b453118f-1412731848.us-east-1.elb.amazonaws.com   8030:32213/TCP,9020:31080/TCP,9030:31433/TCP,9010:30585/TCP   14h
```
在 Kubernetes 外部可以使用 `EXTERNAL-IP` 以及 `PORT` 对应的外部端口来访问 Kubernetes 内部各个组件的服务。 比如访问 FE 的 9030 对应的 `mysql` client 服务，就可以用如下命令连接：
```shell
mysql -h ac48284932b044251bfac389b453118f-1412731848.us-east-1.elb.amazonaws.com -P 9030 -uroot
```
### NodePort 模式
私网环境下，在 Kubernetes 外部访问内部服务，推荐使用 Kubernetes 的 `NodePort` 模式， 使用默认的配置为蓝本配置私网下 `NodePort` 访问模式如下：
```yaml
apiVersion: doris.selectdb.com/v1
kind: DorisCluster
metadata:
  labels:
    app.kubernetes.io/name: doriscluster
    app.kubernetes.io/instance: doriscluster-sample
    app.kubernetes.io/part-of: doris-operator
  name: doriscluster-sample
spec:
  feSpec:
    replicas: 3
    service:
      type: NodePort
    limits:
      cpu: 6
      memory: 12Gi
    requests:
      cpu: 6
      memory: 12Gi
    image: selectdb/doris.fe-ubuntu:2.0.2
  beSpec:
    replicas: 3
    service:
      type: NodePort
    limits:
      cpu: 8
      memory: 16Gi
    requests:
      cpu: 8
      memory: 16Gi
    image: selectdb/doris.be-ubuntu:2.0.2
```
部署后，通过查看 `kubectl get service` 的命令查看相应的 Service ：
```shell
$ kubectl get service
NAME                              TYPE        CLUSTER-IP       EXTERNAL-IP   PORT(S)                                                       AGE
kubernetes                        ClusterIP   10.152.183.1     <none>        443/TCP                                                       169d
doriscluster-sample-fe-internal   ClusterIP   None             <none>        9030/TCP                                                      2d
doriscluster-sample-fe-service    NodePort    10.152.183.58    <none>        8030:31041/TCP,9020:30783/TCP,9030:31545/TCP,9010:31610/TCP   2d
doriscluster-sample-be-internal   ClusterIP   None             <none>        9050/TCP                                                      2d
doriscluster-sample-be-service    NodePort    10.152.183.244   <none>        9060:30940/TCP,8040:32713/TCP,9050:30621/TCP,8060:30926/TCP   2d
```
上述命令获取到在 Kubernetes 外部可使用的端口，通过如下命令获取 Kubernetes 管理的宿主机：
```shell
$ kubectl get nodes -owide
NAME             STATUS   ROLES    AGE    VERSION                     INTERNAL-IP   EXTERNAL-IP   OS-IMAGE                       KERNEL-VERSION       CONTAINER-RUNTIME
vm-10-7-centos   Ready    <none>   88d    v1.23.17-2+40cc20cc310518   10.16.10.7    <none>        TencentOS Server 3.1 (Final)   5.4.119-19.0009.25   containerd://1.5.13
vm-10-8-centos   Ready    <none>   169d   v1.23.17-2+40cc20cc310518   10.16.10.8    <none>        TencentOS Server 3.1 (Final)   5.4.119-19-0009.3    containerd://1.5.13
```
私网环境下，使用 Kubernetes 的宿主机和映射端口访问 Kubernetes 内部的服务。例如，我们使用宿主机的 IP 和 FE 的 9030 映射端口（31545）进行 `mysql` 的连接：
```shell
$ mysql -h 10.16.10.8 -P 31545 -uroot
```

另外，可以根据自身平台需要，指定自己需要的 nodePort。
Kubernetes master 将从给定的配置范围内（一般默认：30000-32767）分配端口，每个 Node 将从该端口（每个 Node 上的同一端口）代理到 Service。像上面的例子那样如果不指定的话会自动生成一个随机的端口。
```yaml
apiVersion: doris.selectdb.com/v1
kind: DorisCluster
metadata:
  labels:
    app.kubernetes.io/name: doriscluster
    app.kubernetes.io/instance: doriscluster-sample
    app.kubernetes.io/part-of: doris-operator
  name: doriscluster-sample
spec:
  feSpec:
    replicas: 3
    service:
      type: NodePort
      servicePorts:
        - nodePort: 31001
          targetPort: 8030
        - nodePort: 31002
          targetPort: 9020
        - nodePort: 31003
          targetPort: 9030
        - nodePort: 31004
          targetPort: 9010
    limits:
      cpu: 6
      memory: 12Gi
    requests:
      cpu: 6
      memory: 12Gi
    image: selectdb/doris.fe-ubuntu:2.0.2
  beSpec:
    replicas: 3
    service:
      type: NodePort
      servicePorts:
        - nodePort: 31005
          targetPort: 9060
        - nodePort: 31006
          targetPort: 8040
        - nodePort: 31007
          targetPort: 9050
        - nodePort: 31008
          targetPort: 8060
    limits:
      cpu: 8
      memory: 16Gi
    requests:
      cpu: 8
      memory: 16Gi
    image: selectdb/doris.be-ubuntu:2.0.2
```
部署后，通过查看 `kubectl get service` 的命令查看相应的 Service，访问方式可以参考上文 ：
```shell
$ kubectl get service
NAME                              TYPE        CLUSTER-IP       EXTERNAL-IP   PORT(S)                                                       AGE
kubernetes                        ClusterIP   10.152.183.1     <none>        443/TCP                                                       169d
doriscluster-sample-fe-internal   ClusterIP   None             <none>        9030/TCP                                                      2d
doriscluster-sample-fe-service    NodePort    10.152.183.67    <none>        8030:31001/TCP,9020:31002/TCP,9030:31003/TCP,9010:31004/TCP   2d
doriscluster-sample-be-internal   ClusterIP   None             <none>        9050/TCP                                                      2d
doriscluster-sample-be-service    NodePort    10.152.183.24    <none>        9060:31005/TCP,8040:31006/TCP,9050:31007/TCP,8060:31008/TCP   2d
```

## Doris 数据交互
### Stream load
[Stream load](https://doris.apache.org/zh-CN/docs/dev/data-operate/import/import-way/stream-load-manual) 是一个同步的导入方式，用户通过发送 HTTP 协议发送请求将本地文件或数据流导入到 Doris 中。
在常规部署中，用户通过 HTTP 协议提交导入命令。一般用户会将请求提交到 FE，则 FE 会通过 HTTP redirect 指令将请求转发给某一个 BE。但是，在基于 Kubernetes 部署的场景下，推荐用户 **直接提交导入命令 BE 的 Srevice** ，再由 Service 依据 Kubernetes 规则负载均衡到某一 BE 的 pod 上。
这两种操作效果的实际效果都是一样的，在 Flink 或 Spark 使用官方 connecter 提交的时候，也可以将写入请求提交给 BE Service。

### ErrorURL 查看
诸如 [Stream load](https://doris.apache.org/zh-CN/docs/dev/data-operate/import/import-way/stream-load-manual) ，[Routine load](https://doris.apache.org/zh-CN/docs/dev/data-operate/import/import-way/routine-load-manual)
这些导入方式，在遇到像数据格式有误等错误的时候，会在返回结构体或者日志中打印 `errorURL` 或 `tracking_url`。 通过访问此链接可以定位导入错误原因。
但是此 URL 是仅在 Kubernetes 部署的集群中某一个特定的 BE 节点容器内部环境可访问。

以下方案，以 Doris 返回的 errorURL 为例展开：
```http://doriscluster-sample-be-2.doriscluster-sample-be-internal.doris.svc.cluster.local:8040/api/_load_error_log?file=__shard_1/error_log_insert_stmt_af474190276a2e9c-49bb9d175b8e968e_af474190276a2e9c_49bb9d175b8e968e```

**1. Kubernetes 集群内部访问**

需要通过 `kubectl get service` 或 `kubectl get pod -o wide` 命令获取 BE 的 Service 或 pod 的访问方式来进行原 URL 的域名端口替换，然后再访问。

比如：
```shell
$ kubectl get pod -o wide
NAME                       READY   STATUS    RESTARTS     AGE   IP           NODE                      NOMINATED NODE   READINESS GATES
doriscluster-sample-be-0   1/1     Running   0            9h    10.0.2.105   10-0-2-47.ec2.internal    <none>           <none>
doriscluster-sample-be-1   1/1     Running   0            9h    10.0.2.104   10-0-2-5.ec2.internal     <none>           <none>
doriscluster-sample-be-2   1/1     Running   0            9h    10.0.2.103   10-0-2-6.ec2.internal     <none>           <none>
doriscluster-sample-fe-0   1/1     Running   0            9h    10.0.2.102   10-0-2-47.ec2.internal    <none>           <none>
doriscluster-sample-fe-1   1/1     Running   0            9h    10.0.2.101   10-0-2-5.ec2.internal     <none>           <none>
doriscluster-sample-fe-2   1/1     Running   0            9h    10.0.2.100   10-0-2-6.ec2.internal     <none>           <none>
```
上述 errorURL 则改为：
```http://10.0.2.103:8040/api/_load_error_log?file=__shard_1/error_log_insert_stmt_af474190276a2e9c-49bb9d175b8e968e_af474190276a2e9c_49bb9d175b8e968e```


**2. Kubernetes 集群外部访问 NodePort 模式**

从 Kubernetes 外部获取报错详情 需要额外的桥接⼿段实现，以下是在部署 Doris 时采用 NodePort 模式的 Service 的处理步骤，通过新建 Service 的⽅式来获取报错详情：
处理 Service 模板 be_streamload_errror_service.yaml :
```yaml
apiVersion: v1
kind: Service
metadata:
  labels:
    app.doris.service/role: debug
    app.kubernetes.io/component: be
  name: doriscluster-detail-error
spec:
  externalTrafficPolicy: Cluster
  internalTrafficPolicy: Cluster
  ipFamilies:
    - IPv4
  ipFamilyPolicy: SingleStack
  ports:
    - name: webserver-port
      port: 8040
      protocol: TCP
      targetPort: 8040
  selector:
    app.kubernetes.io/component: be
    statefulset.kubernetes.io/pod-name: ${podName}
  sessionAffinity: None
  type: NodePort
```

`podName` 则替换为 errorURL 的最⾼级域名：`doriscluster-sample-be-2`。

在 Doris 部署的 `namespace`（一般默认 `doris` ， 以下操作使用 `doris`） 使⽤如下命令部署上述处理过的 Service：

```shell
$ kubectl apply -n doris -f be_streamload_errror_service.yaml 
```

通过如下命令查看 Service 8040 端⼝在宿主机的映射
```shell
$ kubectl get service -n doris doriscluster-detail-error
NAME                              TYPE        CLUSTER-IP       EXTERNAL-IP   PORT(S)            AGE
doriscluster-detail-error         NodePort    10.152.183.35    <none>        8040:31201/TCP     32s
```
使⽤任何⼀台宿主机的 IP 和上面获得的 NodePort（31201）端⼝访问 替换 errorURL 获取详细报错报告。
上述 errorURL 则改为：
```http://10.152.183.35:31201/api/_load_error_log?file=__shard_1/error_log_insert_stmt_af474190276a2e9c-49bb9d175b8e968e_af474190276a2e9c_49bb9d175b8e968e```

**3. Kubernetes 集群外部访问 LoadBalancer 模式**

从 Kubernetes 外部获取报错详情 需要额外的桥接⼿段实现，以下是在部署 Doris 时采用 LoadBalancer 模式的 Service 的处理步骤，通过新建 Service 的⽅式来获取报错详情：
处理 Service 模板 be_streamload_errror_service.yaml :
```yaml
apiVersion: v1
kind: Service
metadata:
  labels:
    app.doris.service/role: debug
    app.kubernetes.io/component: be
  name: doriscluster-detail-error
spec:
  externalTrafficPolicy: Cluster
  internalTrafficPolicy: Cluster
  ipFamilies:
    - IPv4
  ipFamilyPolicy: SingleStack
  ports:
    - name: webserver-port
      port: 8040
      protocol: TCP
      targetPort: 8040
  selector:
    app.kubernetes.io/component: be
    statefulset.kubernetes.io/pod-name: ${podName}
  sessionAffinity: None
  type: LoadBalancer
```

`podName` 则替换为 errorURL 的最⾼级域名：`doriscluster-sample-be-2`。

在 Doris 部署的 `namespace`（一般默认 `doris` ， 以下操作使用 `doris`） 使⽤如下命令部署上述处理过的 Service：

```shell
$ kubectl apply -n doris -f be_streamload_errror_service.yaml 
```

通过如下命令查看 Service 8040 端⼝在宿主机的映射
```shell
$ kubectl get service -n doris doriscluster-detail-error
NAME                         TYPE          CLUSTER-IP       EXTERNAL-IP                                                              PORT(S)           AGE
doriscluster-detail-error    LoadBalancer  172.20.183.136   ac4828493dgrftb884g67wg4tb68gyut-1137856348.us-east-1.elb.amazonaws.com  8040:32003/TCP    14s
```

使⽤ `EXTERNAL-IP` 和 Port（8040）端⼝访问 替换 errorURL 获取详细报错报告。
上述 errorURL 则改为：
```http://ac4828493dgrftb884g67wg4tb68gyut-1137856348.us-east-1.elb.amazonaws.com:8040/api/_load_error_log?file=__shard_1/error_log_insert_stmt_af474190276a2e9c-49bb9d175b8e968e_af474190276a2e9c_49bb9d175b8e968e```

注意：上述设置集群外访问的方法，建议使用完毕后清除掉当前 Service，参考命令如下：
```shell
$ kubectl delete service -n doris doriscluster-detail-error
```