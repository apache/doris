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

Doris-Operator is software extension to manage, operate, and maintain Doris clusters on Kubernetes. It allows users to deploy Doris in the way that is defined by their resources. It can manage Doris clusters in all deployment configurations and supports intelligent and parallel management of Doris at scale.

## Deploy Doris on Kubernetes

### Start Kubernetes
To deploy Doris on Kubernetes using the Doris-Operator, you need a Kubernetes (K8s) cluster first. If you do, you can jump to "Deploy Doris-Operator". 

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
 **Option 1: default mode**  

   Use the definition in the Doris-Operator repository:

```shell
kubectl apply -f https://raw.githubusercontent.com/selectdb/doris-operator/master/config/operator/operator.yaml
```

   **Option 2: custom deployment**  
  Configurations in [operator.yaml](https://github.com/selectdb/doris-operator/blob/master/config/operator/operator.yaml) are the basics for successful deployment. For higher cluster management efficiency or to serve specific needs, you can follow these steps:

   - Download the [operator.yaml](https://raw.githubusercontent.com/selectdb/doris-operator/master/config/operator/operator.yaml) file (using wget, for example)
   - Tailor the configurations in operator.yaml to your case
   - Deploy Doris-Operator using the following command

```shell
kubectl apply -f operator.yaml
```

**3. Check deployment status**   
Use the following command to check the service status of Doris-Operator. If the status is `Running` and all containers in the pods are `READY`, that means Doris-Operator is successfully deployed.

```
 kubectl -n doris get pods
 NAME                              READY   STATUS    RESTARTS        AGE
 doris-operator-5b9f7f57bf-tsvjz   1/1     Running   66 (164m ago)   6d22h
```

The default namespace in operator.yaml is Doris. If you have changed that, remember to use the correct namespace in the check status command.

### Start Doris on Kubernetes
**1. Deploy cluster**  
The `Doris-Operator` repository includes examples for various use cases in the [doc/examples ](https://github.com/selectdb/doris-operator/tree/master/doc/examples) directory. The command is as below:

```
kubectl apply -f https://raw.githubusercontent.com/selectdb/doris-operator/master/doc/examples/doriscluster-sample.yaml
```

You may refer to the following docs for more information: The [how_to_use.md](https://github.com/selectdb/doris-operator/tree/master/doc/how_to_use.md) file is about what the Doris-Operator can do. The [DorisCluster](https://github.com/selectdb/doris-operator/blob/master/api/doris/v1/types.go) file is about resource definition and hierarchy. The [api.md](https://github.com/selectdb/doris-operator/tree/master/doc/api.md) file presents the resources definition and hierarchy in a more readable way.

**2. Check cluster status**

- Check pod status  
  After the cluster deployment resources are distributed, you can check the cluster status using the following command. If all pods are `Running` and all containers in the pods of all components are `READY`, that means the cluster is successfully deployed.

  ```shell
  kubectl get pods
  NAME                       READY   STATUS    RESTARTS   AGE
  doriscluster-sample-fe-0   1/1     Running   0          20m
  doriscluster-sample-be-0   1/1     Running   0          19m
  ```

### Access Doris Cluster  
On Kubernetes, Doris-Operator provide `Service`, which is a resource built in Kubernetes for access to Doris.  

You can view all DorisCluster-related services via  `kubectl -n {namespace} get svc -l "app.doris.ownerreference/name={dorisCluster.Name}"` , in which `dorisCluster.Name` is the name of the DorisCluster resource deployed in step 1.

```shell
kubectl -n default get svc -l "app.doris.ownerreference/name=doriscluster-sample"
NAME                              TYPE        CLUSTER-IP       EXTERNAL-IP                                           PORT(S)                               AGE
doriscluster-sample-fe-internal   ClusterIP   None             <none>                                                9030/TCP                              30m
doriscluster-sample-fe-service    ClusterIP   10.152.183.37    a7509284bf3784983a596c6eec7fc212-618xxxxxx.com        8030/TCP,9020/TCP,9030/TCP,9010/TCP   30m
doriscluster-sample-be-internal   ClusterIP   None             <none>                                                9050/TCP                              29m
doriscluster-sample-be-service    ClusterIP   10.152.183.141   <none>                                                9060/TCP,8040/TCP,9050/TCP,8060/TCP   29m
```

The services are divided into two types. Those with  `-internal` as a suffix are for internal component communication in the cluster, and those with `-service` are accessible to users.

**Access from within the cluster**  

You can access components via the `CLUSTER-IP` of the relevant service. For example, to access `doriscluster-sample-fe-service`  (with its `CLUSTERIP` shown above), you can use the following command: 

```shell
mysql -h 10.152.183.37 -uroot -P9030
```

**Access from outside the cluster**  

Be default, K8s deployment of Doris cluster does not provide external access. If you need external access, you need to apply for load balancing resources for the cluster. After the resource is ready, you can configure the relevant `service` according to the [api.md](https://github.com/selectdb/doris-operator/blob/master/doc/api.md) file. After it is successfully deployed, the service is accessible via its  `EXTERNAL-IP`. For example, to access `doriscluster-sample-fe-service`  (with its `EXTERNAL-IP` shown above), you can use the following command: 

```shell
mysql -h a7509284bf3784983a596c6eec7fc212-618xxxxxx.com -uroot -P9030
```

### Note

This guide is about how to deploy Doris on Kubernetes. For more information, you may check [Doris-Operator](https://github.com/selectdb/doris-operator/tree/master/doc/how_to_use.md) for the full list of features and capabilities, and [DorisCluster](https://github.com/selectdb/doris-operator/blob/master/doc/api.md) for details about customized deployment of Doris clusters.


