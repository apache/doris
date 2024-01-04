--- 
{
    "title": "Compute Node",
    "language": "en"
}
--- 
  
 <! -- 
 Licensed to the Apache Software Foundation (ASF) under one 
 Or more contributor license agreements. See the NOTICE file 
 Distributed with this work for additional information 
 Regarding copyright ownership. The ASF licenses this file 
 To you under the Apache License, Version 2.0 (the 
 "License"); you may not use this file except in compliance 
 With the License. You may obtain a copy of the License at 
  
 http://www.apache.org/licenses/LICENSE-2.0 
  
 Unless required by applicable law or agreed to in writing, 
 Software distributed under the License is distributed on an 
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY 
 KIND, either expressed or implied. See the License for the 
 Specific language governing permissions and limitations 
 Under the License. 
 -- > 
  

# Compute Node

<version since="1.2.1">
</version>

## Scenarios
  
At present, Doris is a typical Share-Nothing architecture, which achieves very high performance by binding data and computing resources in the same node.
With the continuous improvement of the performance for the Doris computing engine, more and more users have begun to use Doris to directly query data on data lake.
This is a Share-Disk scenario that data is often stored on the remote HDFS/S3, and calculated in Doris.
Doris will get the data through the network, and then completes the computation in memory.
For these two mixed loads in one cluster, current Doris architecture will appear some disadvantages:
1. Poor resource isolation, the response requirements of these two loads are different, and the hybrid deployment will have mutual effects.
2. Poor disk usage, the data lake query only needs the computing resources, while doris binding the storage and computing and we have to expand them together, and cause a low utilization rate for disk.
3. Poor expansion efficiency, when the cluster is expanded, Doris will start the migration of Tablet data, and this process will take a lot of time. And the data lake query load has obvious peaks and valleys, it need hourly flexibility.
  
## Solutions
Implement a BE node role specially used for federated computing named `Compute node`.
`Compute node` is used to handle remote federated queries such as this query of data lake.
The original BE node type is called `hybrid node`, and this type of node can not only execute SQL query, but also handle tablet data storage.
And the `Compute node` only can execute SQL query, it have no data on node.
  
With the computing node, the cluster deployment topology will also change:
- the `hybrid node` is used for the data calculation of the OLAP type table, the node is expanded according to the storage demand
- the `computing node` is used for the external computing, and this node is expanded according to the query load.
  
  Since the compute node has no storage, the compute node can be deployed on an HDD disk machine with other workload or on a container.
  
  
## Usage of ComputeNode 
  
### Configure 
Add configuration items to BE's configuration file `be.conf`:
```
 be_node_role = computation 
```
  
This defualt value of this is `mix`, and this is original BE node type. After setting to `computation`, the node is a computing node.
  
You can see the value of the'NodeRole 'field through the `show backends\G` command. If it is'mix ', it is a mixed node, and if it is'computation', it is a computing node
  
```sql
*************************** 1. row ***************************
              BackendId: 10010
                Cluster: default_cluster
                     IP: 10.248.181.219
          HeartbeatPort: 9050
                 BePort: 9060
               HttpPort: 8040
               BrpcPort: 8060
          LastStartTime: 2022-11-30 23:01:40
          LastHeartbeat: 2022-12-05 15:01:18
                  Alive: true
   SystemDecommissioned: false
  ClusterDecommissioned: false
              TabletNum: 753
       DataUsedCapacity: 1.955 GB
          AvailCapacity: 202.987 GB
          TotalCapacity: 491.153 GB
                UsedPct: 58.67 %
         MaxDiskUsedPct: 58.67 %
     RemoteUsedCapacity: 0.000
                    Tag: {"location" : "default"}
                 ErrMsg:
                Version: doris-0.0.0-trunk-80baca264
                 Status: {"lastSuccessReportTabletsTime":"2022-12-05 15:00:38","lastStreamLoadTime":-1,"isQueryDisabled":false,"isLoadDisabled":false}
HeartbeatFailureCounter: 0
               NodeRole: computation
```

### Usage

Add configuration items in fe.conf

```
prefer_compute_node_for_external_table=true
min_backend_num_for_external_table=3
```

> For parameter description, please refer to: [FE configuration item](../admin-manual/config/fe-config.md)

When using the [MultiCatalog](../lakehouse/multi-catalog/multi-catalog.md) function when querying, the query will be dispatched to the computing node first.

### Some Restrictions

- Compute nodes are controlled by configuration items, so do not configure mixed type nodes, modify the configuration to compute nodes.
  
## TODO

- Computational spillover: Doris inner table query, when the cluster load is high, the upper layer (outside TableScan) operator can be scheduled to the compute node.
- Graceful offline:
  - When the compute node goes offline, the new task of the task is automatically scheduled to online nodes
  - the node go offline after all the old tasks on the node are completed
  - when the old task cannot be completed on time, the task can kill by itself
