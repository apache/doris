---
{
"title": "Kubernetes Deployment",
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

# K8s Deployment Doris

<version since="dev"></version>

## Environmental Preparation

- Installation k8s
- Build or download a Doris image
    - Building an image [Build Docker Image](./construct-docker/construct-docker-image)
    - Download Image https://hub.docker.com/r/apache/doris/tags
- Create or download the yml file for Doris on k8s
    - https://github.com/apache/doris/blob/master/docker/runtime/k8s/doris_follower.yml
    - https://github.com/apache/doris/blob/master/docker/runtime/k8s/doris_be.yml
    - https://github.com/apache/doris/blob/master/docker/runtime/k8s/doris_cn.yml

## Starting A Cluster
Start FE (role type is Follower):`kubectl create -f doris_follower.yml` 

Start BE:`kubectl create -f doris_be.yml` 

Start the BE (role type is Compute Node):`kubectl create -f doris_cn.yml`

## Expansion and Contraction Capacity

- FE
  - Currently, scaling is not supported. It is recommended to initialize 1 or 3 nodes as needed
- BE
  - Command:`kubectl scale statefulset doris-be-cluster1 --replicas=4`
- BE (role type is Compute Node)
  - Command:`kubectl scale statefulset doris-cn-cluster1 --replicas=4`

## Test and Verify

Connect to the FE using mysql-client and perform operations such as' show backends' and 'show frontends' to view the status of each node

## K8s Simple Operation Command

- Executing the yml file for the first time `kubectl create -f xxx.yml`
- Execute after modifying the yml file `kubectl apply -f xxx.yml`
- Delete all resources defined by yml `kubectl delete -f xxx.yml`
- View the pod list `kubectl get pods`
- Entering the container `kubectl exec -it xxx（podName） -- /bin/sh`
- view log `kubectl logs xxx（podName）`
- View IP and port information `kubectl get ep`
- [More knowledge of k8s](https://kubernetes.io)

## Common Problem

- How is data persistent?

  Users need to mount PVC on their own to persist metadata information, data information, or log information
- How to safely shrink the BE node?

  BE:User manual execution is required before current resizing[ALTER-SYSTEM-DECOMMISSION-BACKEND](../../sql-manual/sql-reference/Cluster-Management-Statements/ALTER-SYSTEM-DECOMMISSION-BACKEND)

  BE(The role type is Compute Node): Do not store data files and can directly shrink，[About Computing Nodes](../../advanced/compute_node)
- FE startup error "failed to init statefulSetName"

  doris_ The environment variables statefulSetName and serviceName for follower. yml must appear in pairs, such as CN configured_ SERVICE, CN must be configured_ STATEFULSET




