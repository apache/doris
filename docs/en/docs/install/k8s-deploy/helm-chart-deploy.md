---
{
  "title": "Deploy Doris on Helm-Chart",
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

Doris-Operator is software extension to Kubernetes that make use of custom resource to manage Doris and it components. It provides [DorisCluster](https://github.com/selectdb/doris-operator/blob/master/config/crd/bases/doris.selectdb.com_dorisclusters.yaml) a Kubernetes [CustomResourceDefinition](https://kubernetes.io/docs/reference/kubernetes-api/extend-resources/custom-resource-definition-v1/) for user to custom resource.
## Deploy Doris-Operator by Helm-Chart

[![Artifact Hub](https://img.shields.io/endpoint?url=https://artifacthub.io/badge/repository/doris)](https://artifacthub.io/packages/search?repo=doris)

### Install doris-operator Chart

1. Add helm-chart repo of Doris-Operator in helm. This helm chart have resources about RBAC , deployment ...etc for doris-operator running.
  1. Add the selectdb repository.
    ```Bash
    helm repo add selectdb https://charts.selectdb.com
    ```

  2. Update the Helm Chart Repo to the latest version.
   ```Bash
   helm repo update selectdb
   ```

  3. Check the Helm Chart Repo is the latest version.
   ```Bash
   helm search repo selectdb
   NAME                       CHART VERSION    APP VERSION   DESCRIPTION
   selectdb/doris-operator    1.3.1            1.3.1         Doris-operator for doris creat ...
   selectdb/doris             1.3.1            2.0.3         Apache Doris is an easy-to-use ...
   ```
2. we install doris operator in `doris` namespace, so the first we should create `doris` namespace.
   ```Bash
   kubectl create namespace doris
   ```
3. Install the Doris Operator.
- install doris operator in doris namespace.
   ```Bash
   helm install --namespace doris operator selectdb/doris-operator
   ```
- The repo defines the basic function for running doris-operator, Please use next command to deploy doris-operator, when you have completed customization of [`values.yaml`](./values.yaml).
   ```Bash
   helm install --namespace doris -f values.yaml operator selectdb/doris-operator 
   ```

## Uninstall Doris-Operator
When you have confirmed have not `doris` running in kubernetes, Please use next command to uninstall operator.
```Bash
helm uninstall operator
```