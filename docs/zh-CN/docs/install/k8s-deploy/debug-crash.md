---
{
      "title": "服务 Crash 情况下如何进入容器",
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

在 k8s 环境中服务因为一些预期之外的事情会进入 `CrashLoopBackOff` 状态，通过 `kubectl get pod --namespace ${namespace}` 命令可以查看指定 namespace 下的 pod 状态和 pod_name。

在这种状态下，单纯通过 describe 和 logs 命令无法判定服务出问题的原因。当服务进入 `CrashLoopBackOff` 状态时，需要有一种机制允许部署服务的 pod 进入 `running` 状态方便用户通过 exec 进入容器内进行 debug。

doris-operator 提供了 `debug` 的运行模式，本质上是通过 debug 进程占用对应节点的探活端口，绕过 k8s 探活机制，制造一个平稳运行的容器环境来方便用户进入并定位问题。

下面描述了当服务进入 `CrashLoopBackOff` 时如何进入 debug 模式进行人工 debug ，以及解决后如何恢复到正常启动状态。



## 启动 Debug 模式

当服务一个 pod 进入 CrashLoopBackOff 或者正常运行过程中无法再正常启动时，通过一下步骤让服务进入 `debug` 模式，进行手动启动服务查找问题。

**1.通过以下命令给运行有问题的 pod 进行添加 annnotation**
```shell
$ kubectl annotate pod ${pod_name} --namespace ${namespace} selectdb.com.doris/runmode=debug
```
当服务进行下一次重启时候，服务会检测到标识 `debug` 模式启动的 annotation 就会进入 `debug` 模式启动，pod 状态为 `running`。

**2.当服务进入 `debug` 模式，此时服务的 pod 显示为正常状态，用户可以通过如下命令进入 pod 内部**

```shell
$ kubectl --namespace ${namespace} exec -ti ${pod_name} bash
```

**3. `debug` 下手动启动服务，当用户进入 pod 内部，通过修改对应配置文件有关端口进行手动执行 `start_xx.sh` 脚本，脚本目录为 `/opt/apache-doris/xx/bin` 下。**

FE 需要修改 `query_port`，BE 需要修改 `heartbeat_service_port`
主要是避免`debug`模式下还能通过 service 访问到 crash 的节点导致误导流。

## 退出 Debug 模式

当服务定位到问题后需要退出 `debug` 运行，此时只需要按照如下命令删除对应的 pod，服务就会按照正常的模式启动。
```shell
$ kubectl delete pod ${pod_name} --namespace ${namespace}
```



## 注意事项

**进入 pod 内部后，需要修改配置文件的端口信息，才能手动启动 相应的 Doris 组件**

- FE 需要修改默认路径为：`/opt/apache-doris/fe/conf/fe.conf` 的 `http_port=8030` 配置。
- BE 需要修改默认路径为：`/opt/apache-doris/be/conf/be.conf` 的 `webserver_port=8040` 配置。

