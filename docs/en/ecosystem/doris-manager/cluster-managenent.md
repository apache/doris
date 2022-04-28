---
{
    "title": "Cluster management",
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

# Cluster management

The super administrator and space administrator can mainly perform the following operations under the cluster module:

- View cluster overview
- View node list
- Edit parameter configuration

## Cluster overview

### View basic cluster information

Cluster function, showing a cluster-based monitoring panel.

On the home page, click "Cluster" in the navigation bar to enter the cluster function.

![](/images/doris-manager/iclustermanager-1.png)

The operation and maintenance monitoring panel provides various performance monitoring indicators of the cluster for users to gain insight into the cluster status. Users can control the start and stop operations of the cluster through buttons in the upper right corner.

### View cluster resource usage

Users can view disk usage through pie charts, and view the number of databases, etc.

## Node list

Displays information about FE nodes, BE nodes, and brokers in the cluster.
Provides fields including Node ID, Node Type, Host IP, and Node Status.

![](/images/doris-manager/iclustermanager-2.png)

## Parameter configuration

Parameter configuration provides parameter name, parameter type, parameter value type, thermal effect and operation fields.

![](/images/doris-manager/iclustermanager-3.png)

- **Operation**: Click the "Edit" button, you can edit and modify the corresponding configuration value, you can choose the corresponding effective method; click the "View current value" button, you can view the current value corresponding to the host IP

![](/images/doris-manager/iclustermanager-4.png)

![](/images/doris-manager/iclustermanager-5.png)

