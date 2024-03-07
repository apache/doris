---
{
      "title": "Root 用户使用",
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

Doris-Operator 在部署管理相关服务节点使用的是 root 账号无密码的模式。用户名密码只有在部署后才能重新设置。

## 修改 root 账号及其密码

1. 参阅 [权限管理](../../admin-manual/privilege-ldap/user-privilege) 文档，修改或创建相应密码或账户名，并在 Doris 中给予该账号管理节点的权限。

2. 在 DorisCluster CRD 文件中的配置添加 spec.adminUser.* 样例如下：

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

3. 将新的账号和密码更新到部署的 DorisCluster 中, 经过 Doris-Operator 下发，让各个节点感知并生效。参考命令：

```shell
  kubectl apply --namespace ${your_namespace} -f ${your_crd_yaml_file} 
```

### 注意事项

- 集群管理账户是 root ，默认无密码。
- 用户名密码只有在部署成功后才能重新设置。初次部署，添加 `adminUser` 可能会导致启动异常。
- 修改用户名和密码并不是必须的操作，只有在 Doris 内修改了当前的集群管理的用户（默认 root ）或密码时 需要通过 Doris-Operator 下发。
- 如果修改用户名 `spec.adminUser.name` 需要给新的用户分配拥有管理 Doris 的节点的权限。
- 此操作会依次重启所有节点。
