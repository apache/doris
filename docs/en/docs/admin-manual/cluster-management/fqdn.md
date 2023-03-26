---
{
"title": "FQDN",
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

# FQDN

## Concept Introduction

<version since="dev"></version>

A fully qualified domain name (FQDN) is the full domain name of a specific computer or host on the Internet.

After Doris supports FQDN, you can directly specify the domain name when adding various types of nodes. For example, the command to add a be node is `ALTER SYSTEM ADD BACKEND "be_host:heartbeat_service_port`,

"Be_host" was previously the IP address of the be node. After starting the FQDN, be_ The host should specify the domain name of the be node.

## Preconditions

1. fe.conf file set `enable_fqdn_mode = true`
2. The fe node can resolve the domain names of all nodes in Doris

## Best Practices

### Deployment of Doris for K8S

After an accidental restart of a pod, the K8S cannot ensure that the pod's IP address does not change, but it can ensure that the domain name remains unchanged. Based on this feature, when Doris starts fqdn, it can ensure that the pod can still provide services normally after an accidental restart.
For the method of deploying Doris on the K8S, please refer to [K8s Deployment Doris](../../install/construct-docker/k8s-deploy.md)

### Server switching network card

For example, a server with a be node has two network cards with corresponding IPs of 192.192.192.2 and 10.10.10.3. Currently, the network card corresponding to 192.192.192.2 is used, and the following steps can be followed:

1. Add a line '192.192.192.2 be1' to the 'etc/hosts' file of the machine where fe is located_ fqdn`
2. Change be.conf File ` priority_ Networks=192.192.192.2 ` and start be
3. Connect and execute the sql command ` ALTER SYSTEM ADD BACKEND "be1_fqdn: 9050`

When switching to the network card corresponding to 10.10.10.3 in the future, the following steps can be followed:

1. Configure '192.192.192.2 be1' for 'etc/hosts'_ Fqdn 'is changed to' 10.10.10.3 be1_ fqdn`

### Legacy Cluster Enable FQDN

Prerequisite: The current program supports the 'ALTER SYSTEM MODIFY FRONT "<fe_ip>:<edit_log_port>" HOSTNAME "<fe_hostname>" syntax,
If not, you need to upgrade to a version that supports this syntax

Next, follow the steps below:

1. Perform the following operations on the follower and observer nodes one by one (finally, operate the master node):

    1. Stop the node
    2. Check if the node is stopped. Execute 'show frontends' on the MySQL client to view the Alive status of the FE node until it becomes false
    3. Set FQDN for node: `ALTER SYSTEM MODIFY FRONTEND "<fe_ip>:<edit_log_port>" HOSTNAME "<fe_hostname>"`
    4. Modify the node configuration. Modify the 'conf/fe. conf' file in the FE root directory and add the configuration: 'enable'_ fqdn_ mode = true`
    5. Start the node.
    
2. To enable FQDN for a BE node, you only need to execute the following commands through MYSQL, and there is no need to restart the BE.

   `ALTER SYSTEM MODIFY BACKEND "<backend_ip>:<backend_port>" HOSTNAME "<be_hostname>"`


