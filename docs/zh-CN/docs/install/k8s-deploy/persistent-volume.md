---
{
    "title": "持久化存储 与 ConfigMap",
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

Doris-Operator 支持 Doris 各个组件的 pod 挂载 PV（Persistent Volume）。

PV 一般由 kubernetes 系统管理员创建，Doris-Operator 部署 Doris 服务的时候不直接使用 PV，而是通过 PVC 声明一组资源来向 kubernetes 集群申请 PV。
当 PVC 被创建时，Kubernetes 将尝试将其与符合要求的可用 PV 进行绑定。
StorageClass 屏蔽了管理员手动创建 PV 的过程，对于没有现成的 PV 满足 PVC 需求时，可以根据 StorageClass 动态分配 PV。
PV 提供多种存储类型，主要分为两大类：网络存储、本地存储。两者基于各自原理和实现，为用户提供不同的性能和使用方式的体验，用户可以依据自己的容器化的服务类型和自身需求选择。

如果部署时未对 PVC 进行配置，Doris-Operator 默认 使用 `emptyDir` 模式来存储 元数据 数据文件 和 运行日志。当 pod 重新启动时，相关数据将会丢失。

建议持久化存储的节点目录类型：
* FE：doris-meta、log
* BE：storage、log
* CN：storage、log
* BROKER：log

Doris-Operator 同时将日志输出到 console 和 指定目录下。如果用户的 Kubernetes 系统有完整的日志收集能力，可通过 console 输出来收集 Doris INFO 级别（默认）的日志信息。
但是这里仍然推荐配置 PVC 来持久化日志文件，因为除了 INFO 级别日志还会有诸如 fe.out、be.out、audit.log 以及 垃圾回收日志，便于快速定位问题和审计日志回溯。


ConfigMap 是 Kubernetes 中用于存储配置文件的资源对象，它允许动态挂载配置文件，并将配置文件与应用程序解耦，使得配置的管理更加灵活和可维护。
像 PVC 一样 ConfigMap 可以被 Pod 引用，以便在应用程序中使用配置数据。


## StorageClass

Doris-Operator 提供了使用 Kubernetes 默认 `StorageClass` 模式来支持 FE 和 BE 数据存储，其中存储路径（mountPath）使用镜像里的默认配置。
如果用户需要自己指定 StorageClass 则需要在 `spec.feSpec.persistentVolumes` 内修改 `persistentVolumeClaimSpec.storageClassName`，参考如下：
```yaml
apiVersion: doris.selectdb.com/v1
kind: DorisCluster
metadata:
  labels:
    app.kubernetes.io/name: doriscluster
  name: doriscluster-sample-storageclass1
spec:
  feSpec:
    replicas: 3
    image: selectdb/doris.fe-ubuntu:2.0.2
    limits:
      cpu: 8
      memory: 16Gi
    requests:
      cpu: 8
      memory: 16Gi
    persistentVolumes:
    - mountPath: /opt/apache-doris/fe/doris-meta
      name: storage0
      persistentVolumeClaimSpec:
        # when use specific storageclass, the storageClassName should reConfig, example as annotation.
        storageClassName: ${your_storageclass}
        accessModes:
        - ReadWriteOnce
        resources:
          # notice: if the storage size less 5G, fe will not start normal.
          requests:
            storage: 100Gi
    - mountPath: /opt/apache-doris/fe/log
      name: storage1
      persistentVolumeClaimSpec:
        # when use specific storageclass, the storageClassName should reConfig, example as annotation.
        storageClassName: ${your_storageclass}
        accessModes:
        - ReadWriteOnce
        resources:
          requests:
            storage: 100Gi
  beSpec:
    replicas: 3
    image: selectdb/doris.be-ubuntu:2.0.2
    limits:
      cpu: 8
      memory: 16Gi
    requests:
      cpu: 8
      memory: 16Gi
    persistentVolumes:
    - mountPath: /opt/apache-doris/be/storage
      name: storage2
      persistentVolumeClaimSpec:
        # when use specific storageclass, the storageClassName should reConfig, example as annotation.
        storageClassName: ${your_storageclass}
        accessModes:
        - ReadWriteOnce
        resources:
          requests:
            storage: 100Gi
    - mountPath: /opt/apache-doris/be/log
      name: storage3
      persistentVolumeClaimSpec:
        # when use specific storageclass, the storageClassName should reConfig, example as annotation.
        storageClassName: ${your_storageclass}
        accessModes:
        - ReadWriteOnce
        resources:
          requests:
            storage: 100Gi
```

## 定制化 ConfigMap
Doris 在 Kubernetes 使用 `ConfigMap` 实现配置文件和服务解耦。 在部署 `doriscluster` 之前需要提前在同 `namespace` 下部署想要使用的 `ConfigMap`，以下样例展示了 FE 使用名称为 fe-configmap 的 `ConfigMap`， BE 使用名称为 be-configmap 的 `ConfigMap` 的集群相关 yaml:  

FE 的 ConfigMap 样例
```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: fe-configmap
  labels:
    app.kubernetes.io/component: fe
data:
  fe.conf: |
    CUR_DATE=`date +%Y%m%d-%H%M%S`

    # the output dir of stderr and stdout
    LOG_DIR = ${DORIS_HOME}/log

    JAVA_OPTS="-Djavax.security.auth.useSubjectCredsOnly=false -Xss4m -Xmx8192m -XX:+UseMembar -XX:SurvivorRatio=8 -XX:MaxTenuringThreshold=7 -XX:+PrintGCDateStamps -XX:+PrintGCDetails -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:+CMSClassUnloadingEnabled -XX:-CMSParallelRemarkEnabled -XX:CMSInitiatingOccupancyFraction=80 -XX:SoftRefLRUPolicyMSPerMB=0 -Xloggc:$DORIS_HOME/log/fe.gc.log.$CUR_DATE"

    # For jdk 9+, this JAVA_OPTS will be used as default JVM options
    JAVA_OPTS_FOR_JDK_9="-Djavax.security.auth.useSubjectCredsOnly=false -Xss4m -Xmx8192m -XX:SurvivorRatio=8 -XX:MaxTenuringThreshold=7 -XX:+CMSClassUnloadingEnabled -XX:-CMSParallelRemarkEnabled -XX:CMSInitiatingOccupancyFraction=80 -XX:SoftRefLRUPolicyMSPerMB=0 -Xlog:gc*:$DORIS_HOME/log/fe.gc.log.$CUR_DATE:time"

    # INFO, WARN, ERROR, FATAL
    sys_log_level = INFO

    # NORMAL, BRIEF, ASYNC
    sys_log_mode = NORMAL

    # Default dirs to put jdbc drivers,default value is ${DORIS_HOME}/jdbc_drivers
    # jdbc_drivers_dir = ${DORIS_HOME}/jdbc_drivers

    http_port = 8030
    rpc_port = 9020
    query_port = 9030
    edit_log_port = 9010
    
    enable_fqdn_mode = true
```

注意，使用 FE 的 ConfigMap ，必须为 `fe.conf` 添加 `enable_fqdn_mode = true`，具体原因可参考 [此处文档](https://doris.apache.org/zh-CN/docs/dev/admin-manual/cluster-management/fqdn)

BE 的 ConfigMap 样例
```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: be-configmap
  labels:
    app.kubernetes.io/component: be
data:
  be.conf: |
    CUR_DATE=`date +%Y%m%d-%H%M%S`

    PPROF_TMPDIR="$DORIS_HOME/log/"

    JAVA_OPTS="-Xmx1024m -DlogPath=$DORIS_HOME/log/jni.log -Xloggc:$DORIS_HOME/log/be.gc.log.$CUR_DATE -Djavax.security.auth.useSubjectCredsOnly=false -Dsun.java.command=DorisBE -XX:-CriticalJNINatives -DJDBC_MIN_POOL=1 -DJDBC_MAX_POOL=100 -DJDBC_MAX_IDLE_TIME=300000 -DJDBC_MAX_WAIT_TIME=5000"

    # For jdk 9+, this JAVA_OPTS will be used as default JVM options
    JAVA_OPTS_FOR_JDK_9="-Xmx1024m -DlogPath=$DORIS_HOME/log/jni.log -Xlog:gc:$DORIS_HOME/log/be.gc.log.$CUR_DATE -Djavax.security.auth.useSubjectCredsOnly=false -Dsun.java.command=DorisBE -XX:-CriticalJNINatives -DJDBC_MIN_POOL=1 -DJDBC_MAX_POOL=100 -DJDBC_MAX_IDLE_TIME=300000 -DJDBC_MAX_WAIT_TIME=5000"

    # since 1.2, the JAVA_HOME need to be set to run BE process.
    # JAVA_HOME=/path/to/jdk/

    # https://github.com/apache/doris/blob/master/docs/zh-CN/community/developer-guide/debug-tool.md#jemalloc-heap-profile
    # https://jemalloc.net/jemalloc.3.html
    JEMALLOC_CONF="percpu_arena:percpu,background_thread:true,metadata_thp:auto,muzzy_decay_ms:15000,dirty_decay_ms:15000,oversize_threshold:0,prof:false,lg_prof_interval:32,lg_prof_sample:19,prof_gdump:false,prof_accum:false,prof_leak:false,prof_final:false"
    JEMALLOC_PROF_PRFIX=""

    # INFO, WARNING, ERROR, FATAL
    sys_log_level = INFO

    # ports for admin, web, heartbeat service
    be_port = 9060
    webserver_port = 8040
    heartbeat_service_port = 9050
    brpc_port = 8060
```
使用以上两个 `ConfigMap` 的 `doriscluster` 部署样例:
```yaml
apiVersion: doris.selectdb.com/v1
kind: DorisCluster
metadata:
  labels:
    app.kubernetes.io/name: doriscluster
  name: doriscluster-sample-configmap
spec:
  feSpec:
    replicas: 3
    image: selectdb/doris.fe-ubuntu:2.0.2
    limits:
      cpu: 8
      memory: 16Gi
    requests:
      cpu: 8
      memory: 16Gi
    configMapInfo:
      # use kubectl create configmap fe-configmap --from-file=fe.conf
      configMapName: fe-configmap
      resolveKey: fe.conf
  beSpec:
    replicas: 3
    image: selectdb/doris.be-ubuntu:2.0.2
    limits:
      cpu: 8
      memory: 16Gi
    requests:
      cpu: 8
      memory: 16Gi
    configMapInfo:
      # use kubectl create configmap be-configmap --from-file=be.conf
      configMapName: be-configmap
      resolveKey: be.conf
  brokerSpec:
    replicas: 3
    image: selectdb/doris.broker-ubuntu:2.0.2
    limits:
      cpu: 2
      memory: 4Gi
    requests:
      cpu: 2
      memory: 4Gi
    configMapInfo:
      # use kubectl create configmap broker-configmap --from-file=apache_hdfs_broker.conf
      configMapName: broker-configmap
      resolveKey: apache_hdfs_broker.conf

```
这里的 `resolveKey` 是传入配置文件名（必须是`fe.conf`，`be.conf` 或 `apache_hdfs_broker.conf`，cn 节点也是 `be.conf`） 用以解析传入的 Doris 集群配置的文件，doris-operator 会去解析该文件去指导 doriscluster 的定制化部署。

## 为 conf 目录添加特殊配置文件
本段落用来供参考 需要在 Doris 节点的 conf 目录放置配置其他文件的容器化部署方案。比如常见的 [数据湖联邦查询](https://doris.apache.org/zh-CN/docs/dev/lakehouse/multi-catalog/hive) 的 hdfs 配置文件映射。

这里以 BE 的 ConfigMap 和 需要添加的 core-site.xml 文件为例：
```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: be-configmap
  labels:
    app.kubernetes.io/component: be
data:
  be.conf: |
    be_port = 9060
    webserver_port = 8040
    heartbeat_service_port = 9050
    brpc_port = 8060
  core-site.xml: |
    <?xml version="1.0" encoding="UTF-8"?>
    <?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
    <configuration>
      <property>
      <name>hadoop.security.authentication</name>
        <value>kerberos</value>
      </property>
    </configuration>
    ...
```
注意，`data` 内数据结构如下键值对映射：
 ```yaml
data:
  文件名_1:
    文件文本内容_1
  文件名_2:
    文件文本内容_2
  文件名_3:
    文件文本内容_3
 ```

## BE 多盘配置
Doris 的 BE 服务支持多盘挂载，在服务器时代能够很好满足一个计算资源和存储资源不匹配的问题，同时使用多盘也能够很好提高 Doris 的存储效率。在 Kubernetes 上 Doris 同样可以挂载多盘来实现存储效益最大化。在 Kubernetes 上使用多盘需要配合配置文件一起使用。
为实现服务和配置解耦，Doris 采用 `ConfigMap` 来作为配置的承载，实现配置文件动态挂载给服务使用。
以下为 BE 服务使用 `ConfigMap` 来承载配置文件，挂载两块盘供BE使用的 doriscluster 配置：
```yaml
apiVersion: doris.selectdb.com/v1
kind: DorisCluster
metadata:
  labels:
    app.kubernetes.io/name: doriscluster
  name: doriscluster-sample-storageclass1
spec:
  feSpec:
    replicas: 3
    image: selectdb/doris.fe-ubuntu:2.0.2
    limits:
      cpu: 8
      memory: 16Gi
    requests:
      cpu: 8
      memory: 16Gi
    persistentVolumes:
    - mountPath: /opt/apache-doris/fe/doris-meta
      name: storage0
      persistentVolumeClaimSpec:
        # when use specific storageclass, the storageClassName should reConfig, example as annotation.
        #storageClassName: openebs-jiva-csi-default
        accessModes:
        - ReadWriteOnce
        resources:
          # notice: if the storage size less 5G, fe will not start normal.
          requests:
            storage: 100Gi
    - mountPath: /opt/apache-doris/fe/log
      name: storage1
      persistentVolumeClaimSpec:
        # when use specific storageclass, the storageClassName should reConfig, example as annotation.
        #storageClassName: openebs-jiva-csi-default
        accessModes:
        - ReadWriteOnce
        resources:
          requests:
            storage: 100Gi
  beSpec:
    replicas: 3
    image: selectdb/doris.be-ubuntu:2.0.2
    limits:
      cpu: 8
      memory: 16Gi
    requests:
      cpu: 8
      memory: 16Gi
    configMapInfo:
      configMapName: be-configmap
      resolveKey: be.conf
    persistentVolumes:
    - mountPath: /opt/apache-doris/be/storage
      name: storage2
      persistentVolumeClaimSpec:
        # when use specific storageclass, the storageClassName should reConfig, example as annotation.
        #storageClassName: openebs-jiva-csi-default
        accessModes:
        - ReadWriteOnce
        resources:
          requests:
            storage: 100Gi
    - mountPath: /opt/apache-doris/be/storage1
      name: storage3
      persistentVolumeClaimSpec:
        # when use specific storageclass, the storageClassName should reConfig, example as annotation.
        #storageClassName: openebs-jiva-csi-default
        accessModes:
        - ReadWriteOnce
        resources:
          requests:
            storage: 100Gi
    - mountPath: /opt/apache-doris/be/log
      name: storage4
      persistentVolumeClaimSpec:
        # when use specific storageclass, the storageClassName should reConfig, example as annotation.
        #storageClassName: openebs-jiva-csi-default
        accessModes:
        - ReadWriteOnce
        resources:
          requests:
            storage: 100Gi
```
与默认样例相比增加了 `configMapInfo` 的配置，同时也增加了一个 `persistentVolumeClaimSpec` 的配置，[`persistentVolumeClaimSpec`](https://kubernetes.io/docs/reference/kubernetes-api/config-and-storage-resources/persistent-volume-claim-v1/#PersistentVolumeClaimSpec) 完全遵循 Kubernetes 原生资源 PVC spec 的定义格式。
样例中 `configMapInfo` 标识 BE 部署后使用同 `namespace` 下哪一个 ConfigMap 以及 哪一个 key 对应的内容作为配置文件启动，其中 key 为必须为 be.conf。以下为需要预先部署的配合上述 `doriscluster` ConfigMap 样例：
```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: be-configmap
  labels:
    app.kubernetes.io/component: be
data:
  be.conf: |
    CUR_DATE=`date +%Y%m%d-%H%M%S`

    PPROF_TMPDIR="$DORIS_HOME/log/"

    JAVA_OPTS="-Xmx1024m -DlogPath=$DORIS_HOME/log/jni.log -Xloggc:$DORIS_HOME/log/be.gc.log.$CUR_DATE -Djavax.security.auth.useSubjectCredsOnly=false -Dsun.java.command=DorisBE -XX:-CriticalJNINatives -DJDBC_MIN_POOL=1 -DJDBC_MAX_POOL=100 -DJDBC_MAX_IDLE_TIME=300000 -DJDBC_MAX_WAIT_TIME=5000"

    # For jdk 9+, this JAVA_OPTS will be used as default JVM options
    JAVA_OPTS_FOR_JDK_9="-Xmx1024m -DlogPath=$DORIS_HOME/log/jni.log -Xlog:gc:$DORIS_HOME/log/be.gc.log.$CUR_DATE -Djavax.security.auth.useSubjectCredsOnly=false -Dsun.java.command=DorisBE -XX:-CriticalJNINatives -DJDBC_MIN_POOL=1 -DJDBC_MAX_POOL=100 -DJDBC_MAX_IDLE_TIME=300000 -DJDBC_MAX_WAIT_TIME=5000"

    # since 1.2, the JAVA_HOME need to be set to run BE process.
    # JAVA_HOME=/path/to/jdk/

    # https://github.com/apache/doris/blob/master/docs/zh-CN/community/developer-guide/debug-tool.md#jemalloc-heap-profile
    # https://jemalloc.net/jemalloc.3.html
    JEMALLOC_CONF="percpu_arena:percpu,background_thread:true,metadata_thp:auto,muzzy_decay_ms:15000,dirty_decay_ms:15000,oversize_threshold:0,prof:false,lg_prof_interval:32,lg_prof_sample:19,prof_gdump:false,prof_accum:false,prof_leak:false,prof_final:false"
    JEMALLOC_PROF_PRFIX=""

    # INFO, WARNING, ERROR, FATAL
    sys_log_level = INFO

    # ports for admin, web, heartbeat service
    be_port = 9060
    webserver_port = 8040
    heartbeat_service_port = 9050
    brpc_port = 8060
    
    storage_root_path = /opt/apache-doris/be/storage,medium:ssd;/opt/apache-doris/be/storage1,medium:ssd
```
在使用多盘时，`ConfigMap` 中 `storage_root_path` 对应值中的路径要与 `doriscluster` 中 `persistentVolume` 各个挂载路径对应。[`storage_root_path`](https://doris.apache.org/zh-CN/docs/dev/admin-manual/config/be-config/#storage_root_path) 对应的书写规则请参考链接中文档。
在使用云盘的情形下，介质统一使用 `SSD`。