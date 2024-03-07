---
{
      "title": "How to enter the container when the service crashes",
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

In the k8s environment, the service will enter the `CrashLoopBackOff` state due to some unexpected things. You can view the pod status and pod_name under the specified namespace through the `kubectl get pod --namespace ${namespace}` command.

In this state, the cause of the service problem cannot be determined simply by using the describe and logs commands. When the service enters the `CrashLoopBackOff` state, there needs to be a mechanism that allows the pod deploying the service to enter the `running` state so that users can enter the container for debugging through exec.

doris-operator provides a `debug` running mode. In essence, the debug process occupies the active detection port of the corresponding node, bypasses the k8s active detection mechanism, and creates a smoothly running container environment to facilitate users to enter and locate problems.

The following describes how to enter debug mode for manual debugging when the service enters `CrashLoopBackOff`, and how to return to normal startup state after solving the problem.



## Start Debug mode

When a pod of the service enters CrashLoopBackOff or cannot be started normally during normal operation, take the following steps to put the service into `debug` mode and manually start the service to find the problem.

**1.Use the following command to add annotation to the pod with problems.**
```shell
$ kubectl annotate pod ${pod_name} --namespace ${namespace} selectdb.com.doris/runmode=debug
```
When the service is restarted next time, the service will detect the annotation that identifies the `debug` mode startup, and will enter the `debug` mode to start, and the pod status will be `running`.

**2.When the service enters `debug` mode, the pod of the service is displayed in a normal state. Users can enter the inside of the pod through the following command**

```shell
$ kubectl --namespace ${namespace} exec -ti ${pod_name} bash
```

**3. Manually start the service under `debug`. When the user enters the pod, manually execute the `start_xx.sh` script by modifying the port of the corresponding configuration file. The script directory is under `/opt/apache-doris/xx/bin`.**

FE needs to modify `query_port`, BE needs to modify `heartbeat_service_port`
The main purpose is to avoid misleading the flow by accessing the crashed node through service in `debug` mode.

## Exit Debug mode

When the service locates the problem, it needs to exit the `debug` operation. At this time, you only need to delete the corresponding pod according to the following command, and the service will start in the normal mode.
```shell
$ kubectl delete pod ${pod_name} --namespace ${namespace}
```



## 注意事项

**After entering the pod, you need to modify the port information of the configuration file before you can manually start the corresponding Doris component.**

- FE needs to modify the `http_port=8030` configuration with the default path: `/opt/apache-doris/fe/conf/fe.conf`.
- BE needs to modify the `webserver_port=8040` configuration with the default path: `/opt/apache-doris/be/conf/be.conf`.

