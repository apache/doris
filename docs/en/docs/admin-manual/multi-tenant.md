---
{
    "title": "Multi-tenancy",
    "language": "en"
}
---

<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements. See the NOTICE file
distributed with this work for additional information
regarding copyright ownership. The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied. See the License for the
specific language governing permissions and limitations
under the License.
-->

# Multi-tenancy

The main purpose of Doris's multi-tenant and resource isolation solution is to reduce interference between multiple users when performing data operations in the same Doris cluster, and to allocate cluster resources to each user more reasonably.

The scheme is mainly divided into two parts, one is the division of resource groups at the node level in the cluster, and the other is the resource limit for a single query.

## Nodes in Doris

First, let's briefly introduce the node composition of Doris. There are two types of nodes in a Doris cluster: Frontend (FE) and Backend (BE).

FE is mainly responsible for metadata management, cluster management, user request access and query plan analysis.

BE is mainly responsible for data storage and execution of query plans.

FE does not participate in the processing and calculation of user data, so it is a node with low resource consumption. The BE is responsible for all data calculations and task processing, and is a resource-consuming node. Therefore, the resource division and resource restriction schemes introduced in this article are all aimed at BE nodes. Because the FE node consumes relatively low resources and can also be scaled horizontally, there is usually no need to isolate and restrict resources, and the FE node can be shared by all users.

## Node resource division

Node resource division refers to setting tags for BE nodes in a Doris cluster, and the BE nodes with the same tags form a resource group. Resource group can be regarded as a management unit of data storage and calculation. Below we use a specific example to introduce the use of resource groups.

1. Set labels for BE nodes

    Assume that the current Doris cluster has 6 BE nodes. They are host[1-6] respectively. In the initial situation, all nodes belong to a default resource group (Default).
    
    We can use the following command to divide these 6 nodes into 3 resource groups: group_a, group_b, group_c:
    
    ```sql
    alter system modify backend "host1:9050" set ("tag.location" = "group_a");
    alter system modify backend "host2:9050" set ("tag.location" = "group_a");
    alter system modify backend "host3:9050" set ("tag.location" = "group_b");
    alter system modify backend "host4:9050" set ("tag.location" = "group_b");
    alter system modify backend "host5:9050" set ("tag.location" = "group_c");
    alter system modify backend "host6:9050" set ("tag.location" = "group_c");
    ```
    
    Here we combine `host[1-2]` to form a resource group `group_a`, `host[3-4]` to form a resource group `group_b`, and `host[5-6]` to form a resource group `group_c`.
    
    > Note: One BE only supports setting one Tag.
    
2. Distribution of data according to resource groups

    After the resource group is divided. We can distribute different copies of user data in different resource groups. Assume a user table UserTable. We want to store a copy in each of the three resource groups, which can be achieved by the following table creation statement:
    
    ```sql
    create table UserTable
    (k1 int, k2 int)
    distributed by hash(k1) buckets 1
    properties(
        "replication_allocation"="tag.location.group_a:1, tag.location.group_b:1, tag.location.group_c:1"
    )
    ```
    
    In this way, the data in the UserTable table will be stored in the form of 3 copies in the nodes where the resource groups group_a, group_b, and group_c are located.
    
    The following figure shows the current node division and data distribution:
    
    ```
     ┌────────────────────────────────────────────────────┐
     │                                                    │
     │         ┌──────────────────┐  ┌──────────────────┐ │
     │         │ host1            │  │ host2            │ │
     │         │  ┌─────────────┐ │  │                  │ │
     │ group_a │  │   replica1  │ │  │                  │ │
     │         │  └─────────────┘ │  │                  │ │
     │         │                  │  │                  │ │
     │         └──────────────────┘  └──────────────────┘ │
     │                                                    │
     ├────────────────────────────────────────────────────┤
     ├────────────────────────────────────────────────────┤
     │                                                    │
     │         ┌──────────────────┐  ┌──────────────────┐ │
     │         │ host3            │  │ host4            │ │
     │         │                  │  │  ┌─────────────┐ │ │
     │ group_b │                  │  │  │   replica2  │ │ │
     │         │                  │  │  └─────────────┘ │ │
     │         │                  │  │                  │ │
     │         └──────────────────┘  └──────────────────┘ │
     │                                                    │
     ├────────────────────────────────────────────────────┤
     ├────────────────────────────────────────────────────┤
     │                                                    │
     │         ┌──────────────────┐  ┌──────────────────┐ │
     │         │ host5            │  │ host6            │ │
     │         │                  │  │  ┌─────────────┐ │ │
     │ group_c │                  │  │  │   replica3  │ │ │
     │         │                  │  │  └─────────────┘ │ │
     │         │                  │  │                  │ │
     │         └──────────────────┘  └──────────────────┘ │
     │                                                    │
     └────────────────────────────────────────────────────┘
    ```
    
3. Use different resource groups for data query

    After the execution of the first two steps is completed, we can limit a user's query by setting the user's resource usage permissions, and can only use the nodes in the specified resource group to execute.

    For example, we can use the following statement to restrict user1 to only use nodes in the `group_a` resource group for data query, user2 can only use the `group_b` resource group, and user3 can use 3 resource groups at the same time:
    
    ```sql
    set property for'user1''resource_tags.location' = 'group_a';
    set property for'user2''resource_tags.location' = 'group_b';
    set property for'user3''resource_tags.location' = 'group_a, group_b, group_c';
    ```
    
    After the setting is complete, when user1 initiates a query on the UserTable table, it will only access the data copy on the nodes in the `group_a` resource group, and the query will only use the node computing resources in the `group_a` resource group. The query of user3 can use copies and computing resources in any resource group.
    
    In this way, we have achieved physical resource isolation for different user queries by dividing nodes and restricting user resource usage. Furthermore, we can create different users for different business departments and restrict each user from using different resource groups. In order to avoid the use of resource interference between different business parts. For example, there is a business table in the cluster that needs to be shared by all 9 business departments, but it is hoped that resource preemption between different departments can be avoided as much as possible. Then we can create 3 copies of this table and store them in 3 resource groups. Next, we create 9 users for 9 business departments, and limit the use of one resource group for every 3 users. In this way, the degree of competition for resources is reduced from 9 to 3.
    
    On the other hand, for the isolation of online and offline tasks. We can use resource groups to achieve this. For example, we can divide nodes into two resource groups, Online and Offline. The table data is still stored in 3 copies, of which 2 copies are stored in the Online resource group, and 1 copy is stored in the Offline resource group. The Online resource group is mainly used for online data services with high concurrency and low latency. Some large queries or offline ETL operations can be executed using nodes in the Offline resource group. So as to realize the ability to provide online and offline services simultaneously in a unified cluster.

4. Resource group assignments for load job

    The resource usage of load jobs (including insert, broker load, routine load, stream load, etc.) can be divided into two parts:
    1. Computing resources: responsible for reading data sources, data transformation and distribution.
    2. Write resource: responsible for data encoding, compression and writing to disk.

    The write resource must be the node where the replica is located, and the computing resource can theoretically select any node to complete. Therefore, the allocation of resource groups for load jobs is divided into two steps:
    1. Use user-level resource tags to limit the resource groups that computing resources can use.
    2. Use the resource tag of the replica to limit the resource group that the write resource can use.

    So if you want all the resources used by the load operation to be limited to the resource group where the data is located, you only need to set the resource tag of the user level to the same as the resource tag of the replica.
    
## Single query resource limit

The resource group method mentioned earlier is resource isolation and restriction at the node level. In the resource group, resource preemption problems may still occur. For example, as mentioned above, the three business departments are arranged in the same resource group. Although the degree of resource competition is reduced, the queries of these three departments may still affect each other.

Therefore, in addition to the resource group solution, Doris also provides a single query resource restriction function.

At present, Doris's resource restrictions on single queries are mainly divided into two aspects: CPU and memory restrictions.

1. Memory Limitation

    Doris can limit the maximum memory overhead that a query is allowed to use. To ensure that the memory resources of the cluster will not be fully occupied by a query. We can set the memory limit in the following ways:
    
    ```sql
    # Set the session variable exec_mem_limit. Then all subsequent queries in the session (within the connection) use this memory limit.
    set exec_mem_limit=1G;
    # Set the global variable exec_mem_limit. Then all subsequent queries of all new sessions (new connections) use this memory limit.
    set global exec_mem_limit=1G;
    # Set the variable exec_mem_limit in SQL(Unit bytes). Then the variable only affects this SQL.
    select /*+ SET_VAR(exec_mem_limit=1073741824) */ id, name from tbl where xxx;
    ```
    
    Because Doris' query engine is based on the full-memory MPP query framework. Therefore, when the memory usage of a query exceeds the limit, the query will be terminated. Therefore, when a query cannot run under a reasonable memory limit, we need to solve it through some SQL optimization methods or cluster expansion.
    
2. CPU limitations

    Users can limit the CPU resources of the query in the following ways:
    
    ```sql
    # Set the session variable cpu_resource_limit. Then all queries in the session (within the connection) will use this CPU limit.
    set cpu_resource_limit = 2
    # Set the user's attribute cpu_resource_limit, then all queries of this user will use this CPU limit. The priority of this attribute is higher than the session variable cpu_resource_limit
    set property for'user1''cpu_resource_limit' = '3';
    ```
    
    The value of `cpu_resource_limit` is a relative value. The larger the value, the more CPU resources can be used. However, the upper limit of the CPU that can be used by a query also depends on the number of partitions and buckets of the table. In principle, the maximum CPU usage of a query is positively related to the number of tablets involved in the query. In extreme cases, assuming that a query involves only one tablet, even if `cpu_resource_limit` is set to a larger value, only 1 CPU resource can be used.
    

Through memory and CPU resource limits. We can divide user queries into more fine-grained resources within a resource group. For example, we can make some offline tasks with low timeliness requirements, but with a large amount of calculation, use less CPU resources and more memory resources. Some delay-sensitive online tasks use more CPU resources and reasonable memory resources.

## Best practices and forward compatibility

Tag division and CPU limitation are new features in version 0.15. In order to ensure a smooth upgrade from the old version, Doris has made the following forward compatibility:

1. Each BE node will have a default Tag: `"tag.location": "default"`.
2. The BE node added through the `alter system add backend` statement will also set Tag: `"tag.location": "default"` by default.
2. The copy distribution of all tables is modified by default to: `"tag.location.default:xx`. xx is the number of original copies.
3. Users can still specify the number of replicas in the table creation statement by `"replication_num" = "xx"`, this attribute will be automatically converted to: `"tag.location.default:xx`. This ensures that there is no need to modify the original creation. Table statement.
4. By default, the memory limit for a single query is 2GB for a single node, and the CPU resources are unlimited, which is consistent with the original behavior. And the user's `resource_tags.location` attribute is empty, that is, by default, the user can access the BE of any Tag, which is consistent with the original behavior.

Here we give an example of the steps to start using the resource division function after upgrading from the original cluster to version 0.15:

1. Turn off data repair and balance logic

    After the upgrade, the default Tag of BE is `"tag.location": "default"`, and the default copy distribution of the table is: `"tag.location.default:xx`. So if you directly modify the Tag of BE, the system will Automatically detect changes in the distribution of copies, and start data redistribution. This may occupy some system resources. So we can turn off the data repair and balance logic before modifying the tag to ensure that there will be no copies when we plan resources Redistribution operation.
    
    ```sql
    ADMIN SET FRONTEND CONFIG ("disable_balance" = "true");
    ADMIN SET FRONTEND CONFIG ("disable_tablet_scheduler" = "true");
    ```
    
2. Set Tag and table copy distribution

    Next, you can use the `alter system modify backend` statement to set the BE Tag. And through the `alter table` statement to modify the copy distribution strategy of the table. Examples are as follows:
    
    ```sql
    alter system modify backend "host1:9050, 1212:9050" set ("tag.location" = "group_a");
    alter table my_table modify partition p1 set ("replication_allocation" = "tag.location.group_a:2");
    ```

3. Turn on data repair and balance logic

    After the tag and copy distribution are set, we can turn on the data repair and equalization logic to trigger data redistribution.
    
    ```sql
    ADMIN SET FRONTEND CONFIG ("disable_balance" = "false");
    ADMIN SET FRONTEND CONFIG ("disable_tablet_scheduler" = "false");
    ```
    
    This process will continue for a period of time depending on the amount of data involved. And it will cause some colocation tables to fail colocation planning (because the copy is being migrated). You can view the progress by `show proc "/cluster_balance/"`. You can also judge the progress by the number of `UnhealthyTabletNum` in `show proc "/statistic"`. When `UnhealthyTabletNum` drops to 0, it means that the data redistribution is completed. .
    
4. Set the user's resource label permissions.
   
    After the data is redistributed. We can start to set the user's resource label permissions. Because by default, the user's `resource_tags.location` attribute is empty, that is, the BE of any tag can be accessed. Therefore, in the previous steps, the normal query of existing users will not be affected. When the `resource_tags.location` property is not empty, the user will be restricted from accessing the BE of the specified Tag.
    

Through the above 4 steps, we can smoothly use the resource division function after the original cluster is upgraded.
