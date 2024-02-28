---
{
      "title": "Used by Root users",
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

Doris-Operator uses the root account without password mode when deploying and managing related service nodes. The username and password can only be reset after deployment.

## Modify root account and password

1. Refer to the [Privilege Management](../../admin-manual/privilege-ldap/user-privilege) document, modify or create the corresponding password or account name, and give the account the permission to manage nodes in Doris.

2. An example of adding spec.adminUser.* to the configuration in the DorisCluster CRD file is as follows:

```yaml
  apiVersion: doris.selectdb.com/v1
  kind: DorisCluster
  metadata:
    annotations:
      selectdb/doriscluster: doriscluster-sample
    labels:
      app.kubernetes.io/instance: doris-sample
    name: doris-sample
    namespace: doris
  spec:
    adminUser:
      name: root
      password: root_pwd
```

3. Update the new account and password to the deployed DorisCluster, and issue them through Doris-Operator so that each node can sense and take effect. Reference command:

```shell
  kubectl apply --namespace ${your_namespace} -f ${your_crd_yaml_file} 
```

### Precautions

- The cluster management account is root and has no password by default.
- The username and password can only be reset after successful deployment. During initial deployment, adding `adminUser` may cause startup exceptions.
- Modifying the user name and password is not a necessary operation. Only when the current cluster management user (default root) or password is modified in Doris, it needs to be issued through Doris-Operator.
- If you modify the user name `spec.adminUser.name`, you need to assign the new user the permission to manage Doris nodes.
- This operation restarts all nodes in sequence.
