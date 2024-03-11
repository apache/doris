---
{
"title": "升级基于 Doris-Operator 部署的 Apache Doris 集群",
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


本文介绍如何使用 更新来升级 基于 Doris-Operator 部署的 Apache Doris 集群。

和常规部署的集群升级类似，Doris-Operator 部署的 Doris 集群依然需要 BE 到 FE 节点滚动升级，Doris-Operator 基于 Kubernetes 的 [滚动更新功能](https://kubernetes.io/docs/tutorials/kubernetes-basics/update/update-intro/) 提供了滚动升级能力。

## 升级前注意事项

- 升级操作推荐在业务低峰期进行。
- 滚动升级过程中，会导致连接到被关闭节点的连接失效，造成请求失败，对于这类业务，推荐在客户端添加重试能力。
- 升级前可以阅读 [常规升级手册](https://doris.apache.org/zh-CN/docs/dev/admin-manual/cluster-management/upgrade)，便于理解升级中的一些原理和注意事项。
- 升级前无法对数据和元数据的兼容性进行验证，因此集群升级一定要避免数据存在 单副本 情况 和 集群单 FE FOLLOWER 节点。
- 升级过程中会有节点重启，所以可能会触发不必要的集群均衡和副本修复逻辑，先通过以下命令关闭
```
admin set frontend config("disable_balance" = "true");
admin set frontend config("disable_colocate_balance" = "true");
admin set frontend config("disable_tablet_scheduler" = "true");
```
- Doris 升级请遵守不要跨两个及以上关键节点版本升级的原则，若要跨多个关键节点版本升级，先升级到最近的关键节点版本，随后再依次往后升级，若是非关键节点版本，则可忽略跳过。具体参考 [升级版本说明](https://doris.apache.org/zh-CN/docs/dev/admin-manual/cluster-management/upgrade#doris-%E7%89%88%E6%9C%AC%E8%AF%B4%E6%98%8E)

## 升级操作

升级过程节点类型顺序如下，如果某类型节点不存在则跳过：
```
  cn/be -> fe -> broker
```
建议依次修改对应集群组件的 `image` 然后 应用该配置，待当前类型的组件完全升级成功状态恢复正常后，再进行下一个类型节点的滚动升级。

### 升级 BE 

如果保留了集群的 crd（Doris-Operator 定义了 `DorisCluster` 类型资源名称的简写）文件，则可以通过修改该配置文件并且 `kubectl apply` 的命令来进行升级。
 
1. 修改 `spec.beSpec.image`  

    将 `selectdb/doris.be-ubuntu:2.0.4` 变为 `selectdb/doris.be-ubuntu:2.1.0`
```
$ vim doriscluster-sample.yaml
```

2. 保存修改后应用本次修改进行be升级:
```
$ kubectl apply -f doriscluster-sample.yaml -n doris
```
也可通过 `kubectl edit dcr` 的方式直接修改。

1. 查看 namespace 为 'doris' 下的 dcr 列表，获取需要更新的 `cluster_name`
```
$ kubectl get dcr -n doris
NAME                  FESTATUS    BESTATUS    CNSTATUS
doriscluster-sample   available   available
```

2. 修改、保存并生效
```
$ kubectl edit dcr doriscluster-sample -n doris
```
    进入文本编辑器后，将找到`spec.beSpec.image`，将 `selectdb/doris.be-ubuntu:2.0.4` 修改为 `selectdb/doris.be-ubuntu:2.1.0`

3. 查看升级过程和结果：
```
$ kubectl get pod -n doris
```

当所有 Pod 都重建完毕进入 Running 状态后，升级完成。

### 升级 FE 

如果保留了集群的 crd （ Doris-Operator 定义了 `DorisCluster` 类型资源名称的简写）文件，则可以通过修改该配置文件并且 `kubectl apply` 的命令来进行升级。

1. 修改 `spec.feSpec.image`

    将 `selectdb/doris.fe-ubuntu:2.0.4` 变为 `selectdb/doris.fe-ubuntu:2.1.0`
```
$ vim doriscluster-sample.yaml
```

2. 保存修改后应用本次修改进行be升级:
```
$ kubectl apply -f doriscluster-sample.yaml -n doris
```

也可通过 `kubectl edit dcr` 的方式直接修改。

1. 修改、保存并生效
```
$ kubectl edit dcr doriscluster-sample -n doris
```
    进入文本编辑器后，将找到`spec.feSpec.image`，将 `selectdb/doris.fe-ubuntu:2.0.4` 修改为 `selectdb/doris.fe-ubuntu:2.1.0`

2. 查看升级过程和结果
```
$ kubectl get pod -n doris
```

当所有 Pod 都重建完毕进入 Running 状态后，升级完成。

## 升级完成后
### 验证集群节点状态
通过  [访问 Doris 集群](https://doris.apache.org/zh-CN/docs/dev/install/k8s-deploy/network) 文档提供的方式，通过 `mysql-client` 访问 Doris。 
使用 `show frontends` 和 `show backends` 等 SQL 查看各个组件的 版本 和 状态。
```
mysql> show frontends\G;
*************************** 1. row ***************************
              Name: fe_13c132aa_3281_4f4f_97e8_655d01287425
              Host: doriscluster-sample-fe-0.doriscluster-sample-fe-internal.doris.svc.cluster.local
       EditLogPort: 9010
          HttpPort: 8030
         QueryPort: 9030
           RpcPort: 9020
ArrowFlightSqlPort: -1
              Role: FOLLOWER
          IsMaster: false
         ClusterId: 1779160761
              Join: true
             Alive: true
 ReplayedJournalId: 2422
     LastStartTime: 2024-02-19 06:38:47
     LastHeartbeat: 2024-02-19 09:31:33
          IsHelper: true
            ErrMsg:
           Version: doris-2.1.0
  CurrentConnected: Yes
*************************** 2. row ***************************
              Name: fe_f1a9d008_d110_4780_8e60_13d392faa54e
              Host: doriscluster-sample-fe-2.doriscluster-sample-fe-internal.doris.svc.cluster.local
       EditLogPort: 9010
          HttpPort: 8030
         QueryPort: 9030
           RpcPort: 9020
ArrowFlightSqlPort: -1
              Role: FOLLOWER
          IsMaster: true
         ClusterId: 1779160761
              Join: true
             Alive: true
 ReplayedJournalId: 2423
     LastStartTime: 2024-02-19 06:37:35
     LastHeartbeat: 2024-02-19 09:31:33
          IsHelper: true
            ErrMsg:
           Version: doris-2.1.0
  CurrentConnected: No
*************************** 3. row ***************************
              Name: fe_e42bf9da_006f_4302_b861_770d2c955a47
              Host: doriscluster-sample-fe-1.doriscluster-sample-fe-internal.doris.svc.cluster.local
       EditLogPort: 9010
          HttpPort: 8030
         QueryPort: 9030
           RpcPort: 9020
ArrowFlightSqlPort: -1
              Role: FOLLOWER
          IsMaster: false
         ClusterId: 1779160761
              Join: true
             Alive: true
 ReplayedJournalId: 2422
     LastStartTime: 2024-02-19 06:38:17
     LastHeartbeat: 2024-02-19 09:31:33
          IsHelper: true
            ErrMsg:
           Version: doris-2.1.0
  CurrentConnected: No
3 rows in set (0.02 sec)
```
若 FE 节点 `alive` 状态为 true，且 `Version` 值为新版本，则该 FE 节点升级成功。

```
mysql> show backends\G;
*************************** 1. row ***************************
              BackendId: 10002
                   Host: doriscluster-sample-be-0.doriscluster-sample-be-internal.doris.svc.cluster.local
          HeartbeatPort: 9050
                 BePort: 9060
               HttpPort: 8040
               BrpcPort: 8060
     ArrowFlightSqlPort: -1
          LastStartTime: 2024-02-19 06:37:56
          LastHeartbeat: 2024-02-19 09:32:43
                  Alive: true
   SystemDecommissioned: false
              TabletNum: 14
       DataUsedCapacity: 0.000
     TrashUsedCapcacity: 0.000
          AvailCapacity: 12.719 GB
          TotalCapacity: 295.167 GB
                UsedPct: 95.69 %
         MaxDiskUsedPct: 95.69 %
     RemoteUsedCapacity: 0.000
                    Tag: {"location" : "default"}
                 ErrMsg:
                Version: doris-2.1.0
                 Status: {"lastSuccessReportTabletsTime":"2024-02-19 09:31:48","lastStreamLoadTime":-1,"isQueryDisabled":false,"isLoadDisabled":false}
HeartbeatFailureCounter: 0
               NodeRole: mix
*************************** 2. row ***************************
              BackendId: 10003
                   Host: doriscluster-sample-be-1.doriscluster-sample-be-internal.doris.svc.cluster.local
          HeartbeatPort: 9050
                 BePort: 9060
               HttpPort: 8040
               BrpcPort: 8060
     ArrowFlightSqlPort: -1
          LastStartTime: 2024-02-19 06:37:35
          LastHeartbeat: 2024-02-19 09:32:43
                  Alive: true
   SystemDecommissioned: false
              TabletNum: 8
       DataUsedCapacity: 0.000
     TrashUsedCapcacity: 0.000
          AvailCapacity: 12.719 GB
          TotalCapacity: 295.167 GB
                UsedPct: 95.69 %
         MaxDiskUsedPct: 95.69 %
     RemoteUsedCapacity: 0.000
                    Tag: {"location" : "default"}
                 ErrMsg:
                Version: doris-2.1.0
                 Status: {"lastSuccessReportTabletsTime":"2024-02-19 09:31:43","lastStreamLoadTime":-1,"isQueryDisabled":false,"isLoadDisabled":false}
HeartbeatFailureCounter: 0
               NodeRole: mix
*************************** 3. row ***************************
              BackendId: 11024
                   Host: doriscluster-sample-be-2.doriscluster-sample-be-internal.doris.svc.cluster.local
          HeartbeatPort: 9050
                 BePort: 9060
               HttpPort: 8040
               BrpcPort: 8060
     ArrowFlightSqlPort: -1
          LastStartTime: 2024-02-19 08:50:36
          LastHeartbeat: 2024-02-19 09:32:43
                  Alive: true
   SystemDecommissioned: false
              TabletNum: 0
       DataUsedCapacity: 0.000
     TrashUsedCapcacity: 0.000
          AvailCapacity: 12.719 GB
          TotalCapacity: 295.167 GB
                UsedPct: 95.69 %
         MaxDiskUsedPct: 95.69 %
     RemoteUsedCapacity: 0.000
                    Tag: {"location" : "default"}
                 ErrMsg:
                Version: doris-2.1.0
                 Status: {"lastSuccessReportTabletsTime":"2024-02-19 09:32:04","lastStreamLoadTime":-1,"isQueryDisabled":false,"isLoadDisabled":false}
HeartbeatFailureCounter: 0
               NodeRole: mix
3 rows in set (0.01 sec)
```
若 BE 节点 `alive` 状态为 true，且 `Version` 值为新版本，则该 BE 节点升级成功

### 恢复集群副本同步和均衡
在确认各个节点状态无误后，执行以下 SQL 恢复集群均衡和副本修复：
```
admin set frontend config("disable_balance" = "false");
admin set frontend config("disable_colocate_balance" = "false");
admin set frontend config("disable_tablet_scheduler" = "false");
```









