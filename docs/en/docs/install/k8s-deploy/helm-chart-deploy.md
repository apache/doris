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


[Helm](https://helm.sh/) is the best way to find, share, and use software built for Kubernetes. Charts describe even the most complex apps, provide repeatable application installation, and serve as a single point of authority.
Helm-Chart makes it easy to deploy Doris clusters and skip difficult configuration steps.

## Add Helm repository of Doris-Operator

This [helm chart](https://artifacthub.io/packages/search?ts_query_web=doris&sort=relevance&page=1) have resources about RBAC , deployment ...etc for doris-operator running.

1. Add the selectdb repository
```Bash
$ helm repo add selectdb https://charts.selectdb.com
```
2. Update the Helm Chart Repo to the latest version
```Bash
$ helm repo update selectdb
```
3. Check the Helm Chart Repo is the latest version
```Bash
$ helm search repo selectdb
NAME                       CHART VERSION    APP VERSION   DESCRIPTION
selectdb/doris-operator    1.3.1            1.3.1         Doris-operator for doris creat ...
selectdb/doris             1.3.1            2.0.3         Apache Doris is an easy-to-use ...
```



## Install doris-operator

### 1. Install
- install [doris-operator](https://artifacthub.io/packages/helm/doris/doris-operator)，with default config  in a namespace named doris
```Bash
$ helm install operator selectdb/doris-operator
```
- The repo defines the basic function for running doris-operator, Please use next command to deploy operator, when you have completed customization of [values.yaml](https://artifacthub.io/packages/helm/doris/doris-operator?modal=values):
```Bash
$ helm install -f values.yaml operator selectdb/doris-operator 
```
### 2. Confirm installation is complete

Check the deployment status of Pods through the `kubectl get pods` command.
Observe that the Pod of doris-operator is in the Running state and all containers in the Pod are ready, that means, the deployment is successful.
```Bash
$ kubectl get pod --namespace doris
NAME                              READY   STATUS    RESTARTS   AGE
doris-operator-866bd449bb-zl5mr   1/1     Running   0          18m
```



## Install doriscluster

### 1. Install
- use default config for deploying [doriscluster](https://artifacthub.io/packages/helm/doris/doris). This deploys only for fe and be components using default `storageClass` for providing persistent volume.
```Bash
$ helm install doriscluster selectdb/doris
```
- custom doris deploying, specify resources or different deployment type, please custom the [values.yaml](https://artifacthub.io/packages/helm/doris/doris?modal=values) and use next command for deploying.
```Bash
$ helm install -f values.yaml doriscluster selectdb/doris 
```
### 2. Confirm installation is complete
After executing the installation command, deployment and distribution, service deployment scheduling and startup will take a certain amount of time.
Check the deployment status of Pods through the `kubectl get pods` command.

Observe that the Pod of doris-operator is in the Running state and all containers in the Pod are ready, that means, the deployment is successful.
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



## Uninstall doris by helm

### Uninstall doriscluster
Please confirm the doriscluster is not used, When using next command to uninstall doris.
```bash
$ helm uninstall doriscluster
```

### Uninstall doris-operator
When you have confirmed have not `doris` running in kubernetes, Please use next command to uninstall operator.
```bash
$ helm uninstall operator
```
