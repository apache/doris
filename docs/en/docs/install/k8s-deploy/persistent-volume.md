---
{
    "title": "Persistent Volume and ConfigMap",
    "language": "en"
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

Doris-Operator supports mounting PV (Persistent Volume) on pods of various Doris components.

PV is generally created by the kubernetes system administrator. Doris-Operator does not use PV directly when deploying Doris services. Instead, it declares a set of resources through PVC to apply for PV from the kubernetes cluster.
When a PVC is created, Kubernetes will attempt to bind it to an available PV that meets the requirements.
StorageClass shields administrators from the process of manually creating PVs. When there are no ready-made PVs that meet PVC requirements, PVs can be dynamically allocated based on StorageClass.
PV provides a variety of storage types, mainly divided into two categories: network storage and local storage. Based on their respective principles and implementations, the two provide users with different performance and usage experiences. Users can choose according to their own containerized service types and their own needs.

If PVC is not configured during deployment, Doris-Operator uses the `emptyDir` mode by default to store metadata data files and run logs. When the pod is restarted, related data will be lost.

Recommended node directory type for persistent storage:
* FE: doris-meta, log
* BE: storage, log
* CN: storage, log
* BROKER: log

Doris-Operator outputs logs to the console and the specified directory at the same time. If the user's Kubernetes system has complete log collection capabilities, log information at the Doris INFO level (default) can be collected through console output.
However, it is still recommended to configure PVC to persist log files, because in addition to INFO level logs, there are also logs such as fe.out, be.out, audit.log and garbage collection logs, which facilitates quick problem location and audit log backtracking.

ConfigMap is a resource object used to store configuration files in Kubernetes. It allows dynamically mounting configuration files and decouples configuration files from applications, making configuration management more flexible and maintainable.
Like PVCs, ConfigMap can be referenced by Pods in order to use configuration data in the application.


## StorageClass

Doris-Operator provides Kubernetes default `StorageClass` mode to support FE and BE data storage, where the storage path (mountPath) uses the default configuration in the image.
If users need to specify the StorageClass themselves, they need to modify `persistentVolumeClaimSpec.storageClassName` in `spec.feSpec.persistentVolumes`, as shown below:
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

## Customized ConfigMap
Doris uses `ConfigMap` in Kubernetes to decouple configuration files and services. Before deploying `doriscluster`, you need to deploy the `ConfigMap` you want to use under the same `namespace` in advance. The following example shows that FE uses `ConfigMap` named fe-configmap and BE uses `ConfigMap` named be-configmap. Cluster related yaml:

ConfigMap sample for FE
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

Note that when using FE's ConfigMap, you must add `enable_fqdn_mode = true` to `fe.conf`. For specific reasons, please refer to [document here](https://doris.apache.org/docs/admin-manual/cluster-management/fqdn)

BE's ConfigMap sample
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
    JEMALLOC_CONF="percpu_arena:percpu,background_thread:true,metadata_thp:auto,muzzy_decay_ms:15000,dirty_decay_ms:15000,oversize_threshold:0,lg_tcache_max:20,prof:false,lg_prof_interval:32,lg_prof_sample:19,prof_gdump:false,prof_accum:false,prof_leak:false,prof_final:false"
    JEMALLOC_PROF_PRFIX=""

    # INFO, WARNING, ERROR, FATAL
    sys_log_level = INFO

    # ports for admin, web, heartbeat service
    be_port = 9060
    webserver_port = 8040
    heartbeat_service_port = 9050
    brpc_port = 8060
```
`doriscluster` deployment example using the above two `ConfigMap`:
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
The `resolveKey` here is the name of the incoming configuration file (must be `fe.conf`, `be.conf` or `apache_hdfs_broker.conf`, the cn node is also `be.conf`) used to parse the incoming Doris cluster configuration file, doris-operator will parse the file to guide the customized deployment of doriscluster.

## Add special configuration files to the conf directory
This paragraph is for reference. Containerized deployment solutions that configure other files need to be placed in the conf directory of the Doris node. For example, the common HDFS/Hive configuration file mapping of [Data Lake Multi-catalog](https://doris.apache.org/docs/lakehouse/multi-catalog/hive).

Here we take BE's ConfigMap and the core-site.xml file that needs to be added as an example:
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
Note that the data structure in `data` is as follows: key-value pair mapping:
```yaml
data:
  file_name_1:
    file_content_1
  file_name_2:
    file_content_2
  file_name_3:
    file_content_3
```

## BE multi-disk configuration
Doris' BE service supports multi-disk mounting, which can well solve the problem of mismatch between computing resources and storage resources in the server era. At the same time, using multiple disks can also greatly improve the storage efficiency of doris. On Kubernetes, Doris can also mount multiple disks to maximize storage efficiency. Using multiple disks on Kubernetes requires using configuration files.
In order to achieve decoupling of service and configuration, doris uses `ConfigMap` as the bearer of configuration to dynamically mount configuration files for service use.
The following is the doriscluster configuration in which the BE service uses `ConfigMap` to host the configuration file and mount two disks for BE use:
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
Compared with the default example, the configuration of `configMapInfo` is added, and a configuration of `persistentVolumeClaimSpec` is also added, [`persistentVolumeClaimSpec`](https://kubernetes.io/docs/reference/kubernetes-api/config-and-storage-resources/persistent-volume-claim-v1/#PersistentVolumeClaimSpec) fully follows the definition format of the Kubernetes native resource PVC spec.
In the example, `configMapInfo` identifies which ConfigMap under the same `namespace` and which key corresponding content will be used as the configuration file after BE is deployed, where the key must be be.conf. The following is an example of the above `doriscluster` ConfigMap that needs to be pre-deployed:
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
    JEMALLOC_CONF="percpu_arena:percpu,background_thread:true,metadata_thp:auto,muzzy_decay_ms:15000,dirty_decay_ms:15000,oversize_threshold:0,lg_tcache_max:20,prof:false,lg_prof_interval:32,lg_prof_sample:19,prof_gdump:false,prof_accum:false,prof_leak:false,prof_final:false"
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
When using multiple disks, the path in the corresponding value of `storage_root_path` in `ConfigMap` should correspond to each mounting path of `persistentVolume` in `doriscluster`. [`storage_root_path`](https://doris.apache.org/docs/admin-manual/config/be-config/?_highlight=storage_root_path#%E6%9C%8D%E5%8A%A1) For the corresponding writing rules, please refer to the document in the link.
When using cloud disks, the media is uniformly `SSD`.