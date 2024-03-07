---
{
    "title": "Compute Node",
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

# Compute Node

<version since="1.2.1">
</version>

Starting from version 1.2.1, Doris supports the Compute Node.

Starting from this release, BE nodes can be divided into two categories:

- Mix

	Mix node. The default type of BE node. This type of node can not only participate in calculations, but also be responsible for the storage of user data.

- Computation

	Compute Node. It is not responsible for data storage, only data calculation.

As a special type of BE node, the computing node has no data storage capability and is only responsible for data calculation.
Therefore, the compute node can be regarded as a stateless BE node, and nodes can be easily added and deleted.

Compute nodes are suitable for the following scenarios:

- Query external data sources

	Compute nodes can be used to query external data sources, such as Hive, Iceberg, JDBC, etc. Doris is not responsible for the storage of external data source data, so you can use compute nodes to easily expand the computing capabilities of external data sources. At the same time, computing nodes can also be configured with cache directories to cache hotspot data from external data sources to further accelerate data reading.

## Usage of Compute Node

### Add Compute Node

Add configuration in BE's `be.conf` file:

`be_node_role=computation`

Then start the BE node, which will run as a compute node type.

You can then connect to Doris through the MySQL client and execute:

`ALTER SYSTEM ADD BACKEND`

Add this BE node. After the addition is successful, you can see that the node type is `computation` in the `NodeRole` column of `SHOW BACKENDS`.

### Use Compute Node

To use compute nodes, the following conditions need to be met:

- The cluster contains compute nodes.
- Added configuration in `fe.conf`: `prefer_compute_node_for_external_table = true`

At the same time, the following FE configuration will affect the usage strategy of compute node:

- `min_backend_num_for_external_table`

	Before Doris 2.0 (inclusive), the default value of this parameter is 3. After version 2.1, the default parameter is -1.
	
	This parameter indicates: the minimum number of BE nodes expected to participate in external data query. `-1` means that the value is equal to the number of compute nodes in the current cluster.
	
	for example. Assume that there are 3 compute nodes and 5 mix nodes in the cluster.
	
	If `min_backend_num_for_external_table` is set to less than or equal to 3. Then the external table's query will only use 3 compute nodes. If the setting is greater than 3, assuming it is 6, in addition to using 3 compute nodes, the external table's query will also select 3 additional mix nodes.
	
	In summary, this configuration is mainly used for the minimum number of BE nodes that can participate in query process, and will prefer to use compute nodes.
	
> Note:
>
> 1. Only after version 2.1, `min_backend_num_for_external_table` is supported to be set to `-1`. In previous versions, this parameter had to be a positive number. And this configuration only takes effect when `prefer_compute_node_for_external_table = true`.
>
> 2. If `prefer_compute_node_for_external_table` is `false`. Then the external table's query will select any BE node.
>
> 3. If there are no compute nodes in the cluster, none of the above configurations will take effect.
>
> 4. If the `min_backend_num_for_external_table` value is greater than the total number of BE nodes, at most number of BE will be selected.
>
> 5. The above configurations can be modified at runtime without restarting the FE node. And all FE nodes need to be configured.

## Best Practices

### Resource isolation and elastic scaling for federated queries

In federated query scenarios, users can deploy a dedicated group of compute nodes specifically for querying external table data. This allows for the isolation of query loads for external tables (such as large-scale analysis on Hive) from the query loads for internal tables (such as low-latency, fast data analysis).

Moreover, as compute nodes are stateless Backend (BE) nodes, they can be easily scaled up or down. For instance, a cluster of elastic compute nodes can be deployed using Kubernetes, allowing for the utilization of more compute nodes for data lake analysis during peak business periods, and rapid scaling down during off-peak times to reduce costs.

## FAQ

1. Can compute nodes and mix nodes be interconverted?

    Compute nodes can be converted to mix nodes. However, mix nodes cannot be converted to compute nodes.
    
    - Converting compute nodes to mix nodes

        1. Stop the BE node.
        2. Remove the `be_node_role` configuration from `be.conf`, or set it to `be_node_role=mix`.
        3. Configure the correct `storage_root_path` for data storage directory.
        4. Start the BE node.

    - Converting mix nodes to compute nodes

        In principle, this operation is not supported because mix nodes already store data. If conversion is necessary, first perform a safe node decommission, then set up as a compute node in the manner of a new node.

		
2. Do compute nodes need to configure a data storage directory?

    Yes. The data storage directory of a compute node will not store user data but will hold some information files of the BE node itself, such as `cluster_id`, as well as some temporary files generated during running.

    The storage directory for compute nodes requires very little disk space (on the order of MBs) and can be destroyed at any time along with the node without affecting user data.

3. Can compute nodes and mix nodes configure a file cache directory?

    [File cache](./filecache.md) accelerates subsequent queries for the same data by caching data files from recently accessed remote storage systems (HDFS or object storage).
    
    Both compute and mix nodes can set up a file cache directory, which needs to be created in advance.
    
    Additionally, Doris employs strategies like consistent hashing to minimize the probability of cache invalidation when nodes are scaled up or down.

	
4. Do compute nodes need to be decommissioned through the DECOMMISSION operation?

    No. Compute nodes can be removed directly using the `DROP BACKEND` operation.
