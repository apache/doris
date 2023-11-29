---
{
  "title": "Deploy Doris on Kubernetes",
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
## Deploy Doris on Kubernetes

### Start Kubernetes
Having a Kubernetes environment is the premise to deploy Doris on Kubernetes. If you already have it, please ignore this step. 
Hosted Kubernetes on cloud platform or set-up by yourself are all good choice.  

**Hosted EKS**  
1. Check that the following [command-line](https://docs.aws.amazon.com/eks/latest/userguide/getting-started.html) tools are installed in your environment:  
- Install and configure AWS command-line tool AWS CLI.
- Install EKS cluster command-line tool eksctl.
- Install Kubernetes cluster command-line tool kubectl. 

2. Use one of the following methods to create an EKS cluster:  
- [Use eksctl to quickly create an EKS cluster](https://docs.aws.amazon.com/eks/latest/userguide/getting-started-eksctl.html).
- [Manually create an EKS cluster with the AWS console and AWS CLI](https://docs.aws.amazon.com/eks/latest/userguide/getting-started-console.html).

**Hosted GKE**    
Complete all the [prerequisites](https://cloud.google.com/kubernetes-engine/docs/deploy-app-cluster#before-you-begin) when [create a GKE cluster](https://cloud.google.com/kubernetes-engine/docs/deploy-app-cluster#create_cluster).  

**Create as Kubernetes recommend**    
Kubernetes official documents recommends some ways to set up Kubernetes, as [minikube](https://minikube.sigs.k8s.io/docs/start/),[kOps](https://kubernetes.io/zh-cn/docs/setup/production-environment/tools/kops/).

### Deploy Doris-Operator on Kubernetes
**1. Apply the [Custom Resource Definition(CRD)](https://kubernetes.io/docs/concepts/extend-kubernetes/api-extension/custom-resources/#customresourcedefinitions)**  
```shell
kubectl apply -f https://raw.githubusercontent.com/selectdb/doris-operator/master/config/crd/bases/doris.selectdb.com_dorisclusters.yaml    
```
**2. Install Doris-Operator**  
If you want to use the defaults operator resource:
```shell
kubectl apply -f https://raw.githubusercontent.com/selectdb/doris-operator/master/config/operator/operator.yaml
```  
The user defined deployment in github repo are simply:  
Instead of using the command below, apply your local version of the Operator manifest to the cluster when you custom operator resource.
```shell
kubectl apply -f operator.yaml  
```  
**3. Validate The Operator is Running**  
Using the command `kubectl -n {namespace} get pods` get the status of deployed operator. 
```shell
 kubectl -n doris get pods
 NAME                              READY   STATUS    RESTARTS        AGE
 doris-operator-5b9f7f57bf-tsvjz   1/1     Running   66 (164m ago)   6d22h
```
Expected result, the Pod `STATUS` is `Running` and all containers in Pod are all `READY`.

### Start Doris on Kubernetes
**1. Initialize Doris Cluster**    
User can directly deploy Doris by [examples](https://github.com/selectdb/doris-operator/tree/master/doc/examples) provided by Doris-Operator. Below is the command:    
```shell
kubectl apply -f https://raw.githubusercontent.com/selectdb/doris-operator/master/doc/examples/doriscluster-sample.yaml  
```
Or download [doriscluster-sample](https://github.com/selectdb/doris-operator/tree/master/doc/examples/doriscluster-sample.yaml) a custom resource that tells the Operator how to configure the Kubernetes cluster, and custom resource as [api.md](https://github.com/selectdb/doris-operator/blob/master/doc/api.md) and 
[how_to_use](https://github.com/selectdb/doris-operator/tree/master/doc/how_to_use.md) docs. Instead of using the command below, apply the customized resource.
```shell
kubeectl apply -f doriscluster-sample.yaml  
```
**2. Validate Doris Cluster Status**  
Using the command `kubectl -n {namespace} get pods` check pods status.
```shell
kubectl get pods
  NAME                       READY   STATUS    RESTARTS   AGE
  doriscluster-sample-fe-0   1/1     Running   0          20m
  doriscluster-sample-be-0   1/1     Running   0          19m
```
All Pods created by DorisCluster resource should be in `Running` STATUS, and each pod's containers should be `RREADY`.
### Use Doris Cluster  
On kubernetes Doris-Operator provide `Service` a resource build in kubernetes for access to Doris.  

The command `kubectl -n {namespace} get svc -l "app.doris.ownerreference/name={dorisCluster.Name}"` used to get `service` created by Doris-Operator. `dorisCluster.Nmae` is the name of DorisCluster resource deployed by step 1.
```shell
kubectl -n default get svc -l "app.doris.ownerreference/name=doriscluster-sample"
NAME                              TYPE        CLUSTER-IP       EXTERNAL-IP                                           PORT(S)                               AGE
doriscluster-sample-fe-internal   ClusterIP   None             <none>                                                9030/TCP                              30m
doriscluster-sample-fe-service    ClusterIP   10.152.183.37    a7509284bf3784983a596c6eec7fc212-618xxxxxx.com        8030/TCP,9020/TCP,9030/TCP,9010/TCP   30m
doriscluster-sample-be-internal   ClusterIP   None             <none>                                                9050/TCP                              29m
doriscluster-sample-be-service    ClusterIP   10.152.183.141   <none>                                                9060/TCP,8040/TCP,9050/TCP,8060/TCP   29m
```
**Use SQL Client for Access**  
Service created by Doris-Operator have two types, suffix is `-internal` or `-service`. Service have the `-internal` suffix for communicating in Doris components, Service have `-service` suffix for user to access.  

- In Kubernetes  
  In kubernetes, Using `CLUSTER-IP`  is recommended. For example, the fe service's `CLUSTER-IP`  is `10.152.183.37` that displayed by above command. Using below command to access fe service.

  ```shell
  mysql -h 10.152.183.37 -uroot -P9030
  ```

- Out Kubernetes  
Using `EXTERNAL-IP` to access fe from Kubernetes external. In default, Doris-Operator not provided `EXTERNAL-IP` mode. If you want to use `EXTERNAL-IP`, should custom resource `Service` field, reference the doc [api.md](https://github.com/selectdb/doris-operator/blob/master/doc/api.md) to deploy.

:::tip
If the doc not cover your requirements, Pleaser reference the docs [how_to_use.md](https://github.com/selectdb/doris-operator/tree/master/doc/how_to_use.md) and [api.md](https://github.com/selectdb/doris-operator/blob/master/doc/api.md) to custom `DorisCluster` resource to deploy.
:::