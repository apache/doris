--- 
{
    "title": "Compute Node",
    "language": "zh-CN"
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
  

# compute node 
  
## demand scene 
  
 At present, Doris is a typical Share-Nothing architecture, which achieves very high performance by binding data and computing resources in the same node. With the continuous improvement of the performance of the Doris computing engine, more and more users have begun to choose to use Doris to directly query data lake data. 
 This kind of scenario is a Share-Disk scenario, the data is often stored on the remote HDFS/S3, calculated in Doris, Doris obtains the data through the network, and then completes the calculation in memory. These two mixed loads, in the current Doris architecture will appear insufficient: 
 1. Poor resource isolation, the response requirements of the two loads to the cluster are different, and the hybrid deployment will have mutual effects. 
 2. When the cluster is expanded, the data lake query only needs the storage capacity computing resources, while doris can only store computing and expand together, resulting in a relatively low disk utilization rate. 
 3. The expansion efficiency is poor. At present, Doris will start the migration of Tablet data, and the overall process is relatively heavy. The data lake query has obvious peaks and valleys, which can achieve hourly flexibility. 
  
 ##solution 
 Implement a BE node role specially used for federated computing: compute node, compute node specializes in handling remote federated queries such as this type of data lake. The original BE node type is called hybrid node, and this type of node can not only direct SQL tasks, but also Tablet data storage. 
  
 With the computing node, the cluster deployment topology will also change: the hybrid node is used for the data calculation of the OLAP type table, the node is expanded according to the storage demand, and the computing node is used for the external computing, and the node type is used for computing load. Expand capacity. 
  
 Since the compute node has no storage, the compute node can use an HDD disk machine, and the compute node can achieve rapid volume expansion and contraction. 
  
  
## Usage of ComputeNode 
  
### Configure 
 Add configuration items to BE's profile `be.conf`: 
```
 be_node_role = computation 
```
  
 This configuration item defaults to `mix`, that is, the original BE node type. After setting to `computation`, the node is a computing node. 
  
 You can see the value of the'NodeRole 'field through the `show backend\G` command. If it is'mix ', it is a mixed node, and if it is'computation', it is a computing node 
  
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
 When using the MultiCatalog function when querying, the query will be preferentially scheduled to the compute node. In order to balance task scheduling, FE has a backend_num_for_federation configuration item, which defaults to 3. 
 When executing a federated query, the optimizer will select backend_num_for_federation as an alternative to the scheduler, and the retriever will decide which node to execute on to prevent the task from being tilted. 
 When the number of compute nodes is less than backend_num_for_federation, the mixed nodes will be randomly selected to make up the number; when the compute node is greater than backend_num_for_federation, the federated query task will only be executed on the compute node. 
  
  
  
 ### some restrictions 
 - The calculation node currently only supports the query syntax corresponding to MultiCatalog, and the calculation of the normal appearance is still on the hybrid node. 
 - Compute nodes are controlled by configuration items, but do not configure mixed type nodes, modify the configuration to compute nodes. 
  
  
  
 ## Unfinished business 
 - Computational spillover: Doris inner table query, when the cluster load is high, the upper layer (outside TableScan) operator is scheduled to the compute node. 
 - Graceful offline: When the calculation is offline, the new task of the task is automatically scheduled to other nodes; the old task is offline after all the old tasks are completed; when the old task cannot be completed on time, the task can end by itself.
