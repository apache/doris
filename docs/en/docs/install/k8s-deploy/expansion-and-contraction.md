---
{
      "title": "Service expansion and contraction",
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

The expansion and contraction of Doris on K8S can be achieved by modifying the replicas field of the corresponding component of the DorisCluster resource. Modifications can be made by directly editing the corresponding resources, or by using commands.

## Get DorisCluster resources

Use the command `kubectl --namespace {namespace} get doriscluster` to get the name of the deployed DorisCluster (referred to as dcr) resource. In this article, we use doris as the namespace.

```shell
$ kubectl --namespace doris get doriscluster
NAME                  FESTATUS    BESTATUS    CNSTATUS   BROKERSTATUS
doriscluster-sample   available   available
```

## Expansion and contraction

All K8S operation and maintenance operations are performed by modifying the resources to the final state, and the Operator service automatically completes the operation and maintenance. For expansion and contraction operations, you can directly enter the edit mode to modify the replicas value of the corresponding spec through `kubectl --namespace {namespace} edit doriscluster {dcr_name}`. After saving and exiting, Doris-Operator completes the operation and maintenance. You can also use the following commands to implement different components. Expansion and contraction.

### FE expansion

**1. Check the current number of FE services**

```shell
$ kubectl --namespace doris get pods -l "app.kubernetes.io/component=fe"
NAME                       READY   STATUS    RESTARTS       AGE
doriscluster-sample-fe-0   1/1     Running   0              10d
```

**2. Expansion FE**

```shell
$ kubectl --namespace doris patch doriscluster doriscluster-sample --type merge --patch '{"spec":{"feSpec":{"replicas":3}}}'
```

**3. Check expansion results**
```shell
$ kubectl --namespace doris get pods -l "app.kubernetes.io/component=fe"
NAME                       READY   STATUS    RESTARTS   AGE
doriscluster-sample-fe-2   1/1     Running   0          9m37s
doriscluster-sample-fe-1   1/1     Running   0          9m37s
doriscluster-sample-fe-0   1/1     Running   0          8m49s
```

### BE expansion

**1. Check the current number of BE services**

```shell
$ kubectl --namespace doris get pods -l "app.kubernetes.io/component=be"
NAME                       READY   STATUS    RESTARTS      AGE
doriscluster-sample-be-0   1/1     Running   0             3d2h
```

**2. Expansion BE**

```shell
$ kubectl --namespace doris patch doriscluster doriscluster-sample --type merge --patch '{"spec":{"beSpec":{"replicas":3}}}'
```

**3. Check expansion results**
```shell
$ kubectl --namespace doris get pods -l "app.kubernetes.io/component=be"
NAME                       READY   STATUS    RESTARTS      AGE
doriscluster-sample-be-0   1/1     Running   0             3d2h
doriscluster-sample-be-2   1/1     Running   0             12m
doriscluster-sample-be-1   1/1     Running   0             12m
```

### Service contraction

Regarding the issue of node shrinkage, Doris-Operator currently does not support the safe shutdown of nodes. Here, the purpose of reducing FE or BE can still be achieved by reducing the replicas attribute of the cluster component. Here, the node is directly stopped to achieve node shutdown. line, the current version of Doris-Operator fails to implement [decommission](../../sql-manual/sql-reference/Cluster-Management-Statements/ALTER-SYSTEM-DECOMMISSION-BACKEND) and goes offline after safely transferring the copy.  This may cause some problems and precautions as follows:

- If the BE node is rashly taken offline when there is a single copy of the table, there will definitely be data loss, so avoid this operation as much as possible.
- FE Follower nodes try to avoid being offline at will, which may cause metadata damage and affect services.
- FE Observer type nodes can be taken offline at will without risk.
- The CN node does not hold a copy of the data and can be offline at will. However, the remote data cache existing on the CN node will be lost, resulting in a certain performance regression in data query within a short period of time.
