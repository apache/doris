---
{
    "title": "Access the Doris cluster",
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

In Kubernetes, a Service is a resource that defines a set of pods and provides stable access to this set of pods, so it can be associated with a set of pods.
Deploying a cluster through Doris-Operator will automatically generate corresponding Service resources according to the `spec.*Spec.service` configuration. Currently, ClusterIP, LoadBalancer and NodePort modes are supported. Support users’ access needs in different scenarios.

## Access within the Kubernetes cluster
### ClusterIP mode
Doris provides `ClusterIP` access within the kubernetes cluster by default on kubernetes. For FE and BE components, we provide corresponding Service resources for users to use on demand on kubernetes. Use the following command to view the Service of the corresponding component. The Service naming rule provided by Doris-Operator is `{clusterName}-{fe/be}-service`.
```shell
$ kubectl -n {namespace} get service
```
During use, please replace {namespace} with the namespace specified during deployment. Take our default sample deployed Doris cluster as an example:
```yaml
apiVersion: doris.selectdb.com/v1
kind: DorisCluster
metadata:
  labels:
    app.kubernetes.io/name: doriscluster
    app.kubernetes.io/instance: doriscluster-sample
    app.kubernetes.io/part-of: doris-operator
  name: doriscluster-sample
spec:
  feSpec:
    replicas: 3
    limits:
      cpu: 6
      memory: 12Gi
    requests:
      cpu: 6
      memory: 12Gi
    image: selectdb/doris.fe-ubuntu:2.0.2
  beSpec:
    replicas: 3
    limits:
      cpu: 8
      memory: 16Gi
    requests:
      cpu: 8
      memory: 16Gi
    image: selectdb/doris.be-ubuntu:2.0.2
```
We view `kubectl get service` through the command as follows:
```shell
$ kubectl get service
NAME                              TYPE        CLUSTER-IP       EXTERNAL-IP   PORT(S)                               AGE
doriscluster-sample-be-internal   ClusterIP   None             <none>        9050/TCP                              12h
doriscluster-sample-be-service    ClusterIP   172.20.217.234   <none>        9060/TCP,8040/TCP,9050/TCP,8060/TCP   12h
doriscluster-sample-fe-internal   ClusterIP   None             <none>        9030/TCP                              12h
doriscluster-sample-fe-service    ClusterIP   172.20.183.136   <none>        8030/TCP,9020/TCP,9030/TCP,9010/TCP   12h
```
There are two types of services, FE and BE, displayed through the command. The service with the suffix `internal` is the service used by Doris for internal communication and is not available externally. The suffix `-service` is a Service for users to use.
In this example, the `CLUSTER-IP` corresponding to `doriscluster-sample-fe-service` and the corresponding `PORT` can be used on the K8s cluster to access different port services of FE. Using `doriscluster-sample-be-service` Service
And the corresponding `PORT` port to access BE's services.

## Access outside the Kubernetes cluster
### LoadBalancer Mode
If the cluster is created on a relevant cloud platform, it is recommended to use the `LoadBalancer` mode to access the FE and BE services within the cluster. The `ClusterIP` mode is used by default. If you want to use the `LoadBalancer` mode, please configure the following configuration in the spec of each component:
```yaml
service:
  type: LoadBalancer
```
Taking the default configuration as a modification blueprint for example, we use `LoadBalancer` as the access mode of FE and BE on the cloud platform. The deployment configuration is as follows:
```yaml
apiVersion: doris.selectdb.com/v1
kind: DorisCluster
metadata:
  labels:
    app.kubernetes.io/name: doriscluster
    app.kubernetes.io/instance: doriscluster-sample
    app.kubernetes.io/part-of: doris-operator
  name: doriscluster-sample
spec:
  feSpec:
    replicas: 3
    service:
      type: LoadBalancer
    limits:
      cpu: 6
      memory: 12Gi
    requests:
      cpu: 6
      memory: 12Gi
    image: selectdb/doris.fe-ubuntu:2.0.2
  beSpec:
    replicas: 3
    service:
      type: LoadBalancer
    limits:
      cpu: 8
      memory: 16Gi
    requests:
      cpu: 8
      memory: 16Gi
    image: selectdb/doris.be-ubuntu:2.0.2
```
By viewing the `kubectl get service` command, view the corresponding Service display as follows:
```shell
$ kubectl get service
NAME                              TYPE           CLUSTER-IP       EXTERNAL-IP                                                               PORT(S)                                                       AGE
doriscluster-sample-be-internal   ClusterIP      None             <none>                                                                    9050/TCP                                                      14h
doriscluster-sample-be-service    LoadBalancer   172.20.217.234   a46bbcd6998c7436bab8ee8fba9f5e81-808549982.us-east-1.elb.amazonaws.com    9060:32060/TCP,8040:30615/TCP,9050:31742/TCP,8060:31127/TCP   14h
doriscluster-sample-fe-internal   ClusterIP      None             <none>                                                                    9030/TCP                                                      14h
doriscluster-sample-fe-service    LoadBalancer   172.20.183.136   ac48284932b044251bfac389b453118f-1412731848.us-east-1.elb.amazonaws.com   8030:32213/TCP,9020:31080/TCP,9030:31433/TCP,9010:30585/TCP   14h
```
External ports corresponding to `EXTERNAL-IP` and `PORT` can be used outside K8s to access the services of various components within K8s. For example, to access the `mysql` client service corresponding to FE's 9030, you can use the following command to connect:
```shell
mysql -h ac48284932b044251bfac389b453118f-1412731848.us-east-1.elb.amazonaws.com -P 9030 -uroot
```
### NodePort Mode
In a private network environment, to access internal services outside K8s, it is recommended to use the `NodePort` mode of K8s. Use the default configuration as a blueprint to configure the `NodePort` access mode in the private network as follows:
```yaml
apiVersion: doris.selectdb.com/v1
kind: DorisCluster
metadata:
  labels:
    app.kubernetes.io/name: doriscluster
    app.kubernetes.io/instance: doriscluster-sample
    app.kubernetes.io/part-of: doris-operator
  name: doriscluster-sample
spec:
  feSpec:
    replicas: 3
    service:
      type: NodePort
    limits:
      cpu: 6
      memory: 12Gi
    requests:
      cpu: 6
      memory: 12Gi
    image: selectdb/doris.fe-ubuntu:2.0.2
  beSpec:
    replicas: 3
    service:
      type: NodePort
    limits:
      cpu: 8
      memory: 16Gi
    requests:
      cpu: 8
      memory: 16Gi
    image: selectdb/doris.be-ubuntu:2.0.2
```
After deployment, view the corresponding Service by viewing the `kubectl get service` command:
```shell
$ kubectl get service
NAME                              TYPE        CLUSTER-IP       EXTERNAL-IP   PORT(S)                                                       AGE
kubernetes                        ClusterIP   10.152.183.1     <none>        443/TCP                                                       169d
doriscluster-sample-fe-internal   ClusterIP   None             <none>        9030/TCP                                                      2d
doriscluster-sample-fe-service    NodePort    10.152.183.58    <none>        8030:31041/TCP,9020:30783/TCP,9030:31545/TCP,9010:31610/TCP   2d
doriscluster-sample-be-internal   ClusterIP   None             <none>        9050/TCP                                                      2d
doriscluster-sample-be-service    NodePort    10.152.183.244   <none>        9060:30940/TCP,8040:32713/TCP,9050:30621/TCP,8060:30926/TCP   2d
```
The above command obtains the port that can be used outside K8s, and obtains the host managed by K8s through the following command:
```shell
$ kubectl get nodes -owide
NAME             STATUS   ROLES    AGE    VERSION                     INTERNAL-IP   EXTERNAL-IP   OS-IMAGE                       KERNEL-VERSION       CONTAINER-RUNTIME
vm-10-7-centos   Ready    <none>   88d    v1.23.17-2+40cc20cc310518   10.16.10.7    <none>        TencentOS Server 3.1 (Final)   5.4.119-19.0009.25   containerd://1.5.13
vm-10-8-centos   Ready    <none>   169d   v1.23.17-2+40cc20cc310518   10.16.10.8    <none>        TencentOS Server 3.1 (Final)   5.4.119-19-0009.3    containerd://1.5.13
```
In a private network environment, use the K8s host and mapped ports to access K8s internal services. For example, we use the host's IP and FE's 9030 mapped port (31545) to connect to `mysql`:
```shell
$ mysql -h 10.16.10.8 -P 31545 -uroot
```

In addition, you can specify the nodePort you need according to your own platform needs.
The Kubernetes master will allocate a port from the given configuration range (general default: 30000-32767) and each Node will proxy to the Service from that port (the same port on each Node). Like the example above, a random port will be automatically generated if not specified.
```yaml
apiVersion: doris.selectdb.com/v1
kind: DorisCluster
metadata:
  labels:
    app.kubernetes.io/name: doriscluster
    app.kubernetes.io/instance: doriscluster-sample
    app.kubernetes.io/part-of: doris-operator
  name: doriscluster-sample
spec:
  feSpec:
    replicas: 3
    service:
      type: NodePort
      servicePorts:
        - nodePort: 31001
          targetPort: 8030
        - nodePort: 31002
          targetPort: 9020
        - nodePort: 31003
          targetPort: 9030
        - nodePort: 31004
          targetPort: 9010
    limits:
      cpu: 6
      memory: 12Gi
    requests:
      cpu: 6
      memory: 12Gi
    image: selectdb/doris.fe-ubuntu:2.0.2
  beSpec:
    replicas: 3
    service:
      type: NodePort
      servicePorts:
        - nodePort: 31005
          targetPort: 9060
        - nodePort: 31006
          targetPort: 8040
        - nodePort: 31007
          targetPort: 9050
        - nodePort: 31008
          targetPort: 8060
    limits:
      cpu: 8
      memory: 16Gi
    requests:
      cpu: 8
      memory: 16Gi
    image: selectdb/doris.be-ubuntu:2.0.2
```
After deployment, check the corresponding Service by viewing the `kubectl get service` command. For access methods, please refer to the above:
```shell
$ kubectl get service
NAME                              TYPE        CLUSTER-IP       EXTERNAL-IP   PORT(S)                                                       AGE
kubernetes                        ClusterIP   10.152.183.1     <none>        443/TCP                                                       169d
doriscluster-sample-fe-internal   ClusterIP   None             <none>        9030/TCP                                                      2d
doriscluster-sample-fe-service    NodePort    10.152.183.67    <none>        8030:31001/TCP,9020:31002/TCP,9030:31003/TCP,9010:31004/TCP   2d
doriscluster-sample-be-internal   ClusterIP   None             <none>        9050/TCP                                                      2d
doriscluster-sample-be-service    NodePort    10.152.183.24    <none>        9060:31005/TCP,8040:31006/TCP,9050:31007/TCP,8060:31008/TCP   2d
```

## Doris data exchange
### Stream load
[Stream load](https://doris.apache.org/docs/data-operate/import/import-way/stream-load-manual) is a synchronous import method. Users send requests to import local files or data streams into Doris by sending HTTP protocol.
In a regular deployment, users submit import commands via the HTTP protocol. Generally, users will submit the request to FE, and FE will forward the request to a certain BE through the HTTP redirect command. However, in a Kubernetes-based deployment scenario, it is recommended that users **directly submit the import command to BE's Srevice**, and then the Service will be load balanced to a certain BE pod based on Kubernetes rules.
The actual effects of these two operations are the same. When Flink or Spark uses the official connector to submit, the write request can also be submitted to the BE Service.

### ErrorURL
When import methods such as [Stream load](https://doris.apache.org/docs/data-operate/import/import-way/stream-load-manual) and [Routine load](https://doris.apache.org/docs/data-operate/import/import-way/routine-load-manual)
These import methods will print `errorURL` or `tracking_url` in the return structure or log when encountering errors such as incorrect data format. You can locate the cause of the import error by visiting this link.
However, this URL is only accessible within the internal environment of a specific BE node container in a Kubernetes deployed cluster.

The following scenario takes the errorURL returned by Doris as an example:
```http://doriscluster-sample-be-2.doriscluster-sample-be-internal.doris.svc.cluster.local:8040/api/_load_error_log?file=__shard_1/error_log_insert_stmt_af474190276a2e9c-49bb9d175b8e968e_af474190276a2e9c_49bb9d175b8e968e```

**1. Kubernetes cluster internal access**

You need to obtain the access method of BE's Service or pod through the `kubectl get service` or `kubectl get pod -o wide` command, replace the domain name and port of the original URL, and then access again.

for example:
```shell
$ kubectl get pod -o wide
NAME                       READY   STATUS    RESTARTS     AGE   IP           NODE                      NOMINATED NODE   READINESS GATES
doriscluster-sample-be-0   1/1     Running   0            9h    10.0.2.105   10-0-2-47.ec2.internal    <none>           <none>
doriscluster-sample-be-1   1/1     Running   0            9h    10.0.2.104   10-0-2-5.ec2.internal     <none>           <none>
doriscluster-sample-be-2   1/1     Running   0            9h    10.0.2.103   10-0-2-6.ec2.internal     <none>           <none>
doriscluster-sample-fe-0   1/1     Running   0            9h    10.0.2.102   10-0-2-47.ec2.internal    <none>           <none>
doriscluster-sample-fe-1   1/1     Running   0            9h    10.0.2.101   10-0-2-5.ec2.internal     <none>           <none>
doriscluster-sample-fe-2   1/1     Running   0            9h    10.0.2.100   10-0-2-6.ec2.internal     <none>           <none>
```
The above errorURL is changed to:
```http://10.0.2.103:8040/api/_load_error_log?file=__shard_1/error_log_insert_stmt_af474190276a2e9c-49bb9d175b8e968e_af474190276a2e9c_49bb9d175b8e968e```


**2. NodePort mode for external access to Kubernetes cluster**

Obtaining error report details from outside Kubernetes requires additional bridging means. The following are the processing steps for using NodePort mode Service when deploying Doris. Obtain error report details by creating a new Service:
Handle Service template be_streamload_errror_service.yaml:
```yaml
apiVersion: v1
kind: Service
metadata:
  labels:
    app.doris.service/role: debug
    app.kubernetes.io/component: be
  name: doriscluster-detail-error
spec:
  externalTrafficPolicy: Cluster
  internalTrafficPolicy: Cluster
  ipFamilies:
    - IPv4
  ipFamilyPolicy: SingleStack
  ports:
    - name: webserver-port
      port: 8040
      protocol: TCP
      targetPort: 8040
  selector:
    app.kubernetes.io/component: be
    statefulset.kubernetes.io/pod-name: ${podName}
  sessionAffinity: None
  type: NodePort
```
Use the following command to view the mapping of Service 8040 port on the host machine
```shell
$ kubectl get service -n doris doriscluster-detail-error
NAME TYPE CLUSTER-IP EXTERNAL-IP PORT(S) AGE
doriscluster-detail-error NodePort 10.152.183.35 <none> 8040:31201/TCP 32s
```
Use the IP of any host and the NodePort (31201) port obtained above to access and replace errorURL to obtain a detailed error report.
The above errorURL is changed to:
```http://10.152.183.35:31201/api/_load_error_log?file=__shard_1/error_log_insert_stmt_af474190276a2e9c-49bb9d175b8e968e_af474190276a2e9c_49bb9d175b8e968e```

**3. Access LoadBalancer mode from outside the Kubernetes cluster**

Obtaining error report details from outside Kubernetes requires additional bridging means. The following are the processing steps for using the LoadBalancer mode Service when deploying Doris. Obtain error report details by creating a new Service:
Handle Service template be_streamload_errror_service.yaml:
```yaml
apiVersion: v1
kind: Service
metadata:
  labels:
    app.doris.service/role: debug
    app.kubernetes.io/component: be
  name: doriscluster-detail-error
spec:
  externalTrafficPolicy: Cluster
  internalTrafficPolicy: Cluster
  ipFamilies:
    - IPv4
  ipFamilyPolicy: SingleStack
  ports:
    - name: webserver-port
      port: 8040
      protocol: TCP
      targetPort: 8040
  selector:
    app.kubernetes.io/component: be
    statefulset.kubernetes.io/pod-name: ${podName}
  sessionAffinity: None
  type: LoadBalancer
```
`podName` is replaced with the highest-level domain name of errorURL: `doriscluster-sample-be-2`.

In the `namespace` deployed by Doris (generally the default is `doris`, use `doris` for the following operations), use the following command to deploy the service processed above:

```shell
$ kubectl apply -n doris -f be_streamload_errror_service.yaml
```

Use the following command to view the mapping of Service 8040 port on the host machine
```shell
$ kubectl get service -n doris doriscluster-detail-error
NAME                         TYPE          CLUSTER-IP       EXTERNAL-IP                                                              PORT(S)           AGE
doriscluster-detail-error    LoadBalancer  172.20.183.136   ac4828493dgrftb884g67wg4tb68gyut-1137856348.us-east-1.elb.amazonaws.com  8040:32003/TCP    14s
```

Use `EXTERNAL-IP` and Port (8040) port access to replace errorURL to obtain a detailed error report.
The above errorURL is changed to:
```http://ac4828493dgrftb884g67wg4tb68gyut-1137856348.us-east-1.elb.amazonaws.com:8040/api/_load_error_log?file=__shard_1/error_log_insert_stmt_af474190276a2e9c-49bb9d175b8e968 e_af474190276a2e9c_49bb9d175b8e968e```

Note: For the above method of setting up access outside the cluster, it is recommended to clear the current Service after use. The reference command is as follows:
```shell
$ kubectl delete service -n doris doriscluster-detail-error
```