---
{
    "title": "多租户(弃用)",
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

# 多租户(已弃用)

该功能已弃用。新方案请参阅：[多租户和资源划分](../multi-tenant.md)。

## 背景
Doris 作为一款 PB 级别的在线报表与多维分析数据库，对外通过开放云提供云端的数据库服务，并且对于每个云上的客户都单独部署了一套物理集群。对内，一套物理集群部署了多个业务，对于隔离性要求比较高的业务单独搭建了集群。针对以上存在几点问题：

- 部署多套物理集群维护代价大（升级、功能上线、bug修复）。
- 一个用户的查询或者查询引起的bug经常会影响其他用户。
- 实际生产环境单机只能部署一个BE进程。而多个BE可以更好的解决胖节点问题。并且对于join、聚合操作可以提供更高的并发度。

综合以上三点，Doris需要新的多租户方案，既能做到较好的资源隔离和故障隔离，同时也能减少维护的代价，满足公有云和私有云的需求。

## 设计原则

- 使用简单
- 开发代价小
- 方便现有集群的迁移

## 名词解释

- FE: Frontend，即 Doris 中用于元数据管理即查询规划的模块。
- BE: Backend，即 Doris 中用于存储和查询数据的模块。
- Master: FE 的一种角色。一个Doris集群只有一个Master，其他的FE为Observer或者Follower。
- instance：一个 BE 进程即是一个 instance。
- host：单个物理机
- cluster：即一个集群，由多个instance组成。
- 租户：一个cluster属于一个租户。cluster和租户之间是一对一关系。
- database：一个用户创建的数据库

## 主要思路

- 一个host上部署多个BE的instance，在进程级别做资源隔离。
- 多个instance形成一个cluster，一个cluster分配给一个业务独立的的租户。
- FE增加cluster这一级并负责cluster的管理。
- CPU,IO,内存等资源隔离采用cgroup。

## 设计方案

为了能够达到隔离的目的，引入了**虚拟cluster**的概念。

1. cluster表示一个虚拟的集群，由多个BE的instance组成。多个cluster共享FE。
2. 一个host上可以启动多个instance。cluster创建时，选取任意指定数量的instance，组成一个cluster。
3. 创建cluster的同时，会创建一个名为superuser的账户，隶属于该cluster。superuser可以对cluster进行管理、创建数据库、分配权限等。
4. Doris启动后，会创建一个默认的cluster：default_cluster。如果用户不希望使用多cluster的功能，则会提供这个默认的cluster，并隐藏多cluster的其他操作细节。

具体架构如下图：
![](/images/multi_tenant_arch.png)
 
## SQL 接口

- 登录

    默认集群登录名： user_name@default_cluster 或者 user_name
    
    自定义集群登录名：user_name@cluster_name
   
    `mysqlclient -h host -P port -u user_name@cluster_name -p password`

- 添加、删除、下线（decommission）以及取消下线BE

    `ALTER SYSTEM ADD BACKEND "host:port"`
    `ALTER SYSTEM DROP BACKEND "host:port"`
    `ALTER SYSTEM DECOMMISSION BACKEND "host:port"`
    `CANCEL DECOMMISSION BACKEND "host:port"`
    
    强烈建议使用 DECOMMISSION 而不是 DROP 来删除 BACKEND。DECOMMISSION 操作会首先将需要下线节点上的数据拷贝到集群内其他instance上。之后，才会真正下线。
    
- 创建集群，并指定superuser账户的密码

    `CREATE CLUSTER cluster_name PROPERTIES ("instance_num" = "10") identified by "password"`
    
- 进入一个集群

    `ENTER cluster_name`
    
- 集群扩容、缩容

    `ALTER CLUSTER cluster_name PROPERTIES ("instance_num" = "10")`
 
    当指定的实例个数多于cluster现有be的个数，则为扩容，如果少于则为缩容。
   
- 链接、迁移db

    `LINK DATABASE src_cluster_name.db_name dest_cluster_name.db_name`

    软链一个cluster的db到另外一个cluster的db ，对于需要临时访问其他cluster的db却不需要进行实际数据迁移的用户可以采用这种方式。

    `MIGRATE DATABASE src_cluster_name.db_name dest_cluster_name.db_name`

    如果需要对db进行跨cluster的迁移，在链接之后，执行migrate对数据进行实际的迁移。
   
   迁移不影响当前两个db的查询、导入等操作，这是一个异步的操作，可以通过`SHOW MIGRATIONS`查看迁移的进度。
    
- 删除集群

    `DROP CLUSTER cluster_name`
    
    删除集群，要求先手动删除的集群内所有database。
    
- 其他

    `SHOW CLUSTERS`

    展示系统内已经创建的集群。只有root用户有该权限。

    `SHOW BACKENDS`
    
    查看集群内的BE instance。

    `SHOW MIGRATIONS`
    
    展示当前正在进行的db迁移任务。执行完db的迁移后可以通过此命令查看迁移的进度。

## 详细设计

1. 命名空间隔离

    为了引入多租户，需要对系统内的cluster之间的命名空间进行隔离。
    
   Doris现有的元数据采用的是image + journal 的方式(元数据的设计见相关文档)。Doris会把涉及元数据的操作的记录为一个 journal （操作日志），然后定时的按照**图1**的方式写成image，加载的时候按照写入的顺序读即可。但是这样就带来一个问题已经写入的格式不容易修改，比如记录数据分布的元数据格式为：database+table+tablet+replica 嵌套，如果按照以往的方式要做cluster之间的命名空间隔离，则需要在database上增加一层cluster，内部元数据的层级变为:cluster+database+table+tablet+replica，如**图2**所示。但加一层带来的问题有：
   
    - 增加一层带来的元数据改动，不兼容，需要按照图2的方式cluster＋db＋table＋tablet＋replica层级写,这样就改变了以往的元数据组织方式，老版本的升级会比较麻烦，比较理想的方式是按照图3在现有元数据的格式下顺序写入cluster的元数据。

    - 代码里所有用到db、user等，都需要加一层cluster，一工作量大改动的地方多，层级深，多数代码都获取db，现有功能几乎都要改一遍，并且需要在db的锁的基础上嵌套一层cluster的锁。
    
    ![](/images/palo_meta.png)
    
    综上这里采用了一种通过给db、user名加前缀的方式去隔离内部因为cluster之间db、user名字冲突的问题。
    
    如下，所有的sql输入涉及db名、user名的，都需要根据自己所在的cluster来拼写db、user的全名。
    
   ![](/images/cluster_namaspace.png)
    
    采用这种方式以上两个问题不再有。元数据的组织方式也比较简单。即采用**图3**每个cluster记录下属于自己cluster的db、user，以及节点即可。
    
2. BE 节点管理

    每个cluster都有属于自己的一组instance，可以通过`SHOW BACKENDS`查看，为了区分出instance属于哪个cluster以及使用情况，BE引入了多个状态：

    - free：当一个BE节点被加入系统内，此时be不属于任何cluster的时候处于空闲状态
    - using：当创建集群、或者扩容被选取到一个cluster内则处于使用中。
   - cluster decommission：如果执行缩容量，则正在执行缩容的be处于此状态。结束后，be状态变为free。
   - system decommission：be正在下线中。下线完成后，该be将会被永久删除。
   
    只有root用户可以通过`SHOW PROC "/backends"`中cluster这一项查看集群内所有be的是否被使用。为空则为空闲，否则为使用中。`SHOW BACKENDS`只能看到所在cluster的节点。以下是be节点状态变化的示意图。 
    
    ![](/images/backend_state.png)
    
3. 创建集群

    只有root用户可以创建一个cluster，并指定任意数量的BE instance。
    
    支持在相同机器上选取多个instance。选择instance的大致原则是：尽可能选取不同机器上的be并且使所有机器上使用的be数尽可能均匀。
    
    对于使用来讲，每一个user、db都属于一个cluster（root除外）。为了创建user、db，首先需要进入一个cluster。在创建cluster的时候系统会默认生成这个cluster的管理员，即superuser账户。superuser具有在所属cluster内创建db、user，以及查看be节点数的权限。所有的非root用户登录必须指定一个cluster，即`user_name@cluster_name`。
    
    只有root用户可以通过`SHOW CLUSTER`查看系统内所有的cluster，并且可以通过@不同的集群名来进入不同的cluster。对于除了root之外的用户cluster都是不可见的。
    
    为了兼容老版本Doris内置了一个名字叫做default_cluster的集群，这个名字在创建集群的时候不能使用。
    
    ![](/images/user_authority.png)
    
4. 集群扩容

    集群扩容的流程同创建集群。会优先选取不在集群之外的host上的BE instance。选取的原则同创建集群。
    
5. 集群缩容、CLUSTER DECOMMISSION

    用户可以通过设置 cluster 的 instance num 来进行集群缩容。

   集群的缩容会优先在BE instance 数量最多的 host 上选取 instance 进行下线。
    
    用户也可以直接使用 `ALTER CLUSTER DECOMMISSION BACKEND` 来指定BE，进行集群缩容。
    
![](/images/replica_recover.png)
    
6. 建表

    为了保证高可用，每个分片的副本必需在不同的机器上。所以建表时，选择副本所在be的策略为在每个host上随机选取一个be。然后从这些be中随机选取所需副本数量的be。总体上做到每个机器上分片分布均匀。
    
    因此，假如需要创建一个3副本的分片，即使cluster包含3个或以上的instance，但是只有2个或以下的host，依然不能创建该分片。
    
7. 负载均衡

    负载均衡的粒度为cluster级别，cluster之间不做负载均衡。但是在计算负载是在host一级进行的，而一个host上可能存在多个不同cluster的BE instance。 cluster内，会通过每个host上所有分片数目、存储使用率计算负载，然后把负载高的机器上的分片往负载低的机器上拷贝（详见负载均衡相关文档）。

8. LINK DATABASE（软链）

    多个集群之间可以通过软链的方式访问彼此的数据。链接的级别为不同cluster的db。
    
   通过在一个cluster内，添加需要访问的其他cluster的db的信息，来访问其他cluster中的db。
   
   当查询链接的db时，所使用的计算以及存储资源为源db所在cluster的资源。
   
   被软链的db不能在源cluster中删除。只有链接的db被删除后，才可以删除源db。而删除链接db，不会删除源db。
   
9. MIGRATE DATABASE

    db可以在cluster之间进行物理迁移。
    
   要迁移db，必须先链接db。执行迁移后数据会迁移到链接的db所在的cluster，并且执行迁移后源db被删除，链接断开。
   
   数据的迁移，复用了负载均衡以及副本恢复中，复制数据的流程（详见负载均衡相关文档）。具体实现上，在执行`MIRAGTE`命令后，Doris会在元数据中，将源db的所有副本所属的cluster，修改为目的cluster。
   
   Doris会定期检查集群内机器之间是否均衡、副本是否齐全、是否有多余的副本。db的迁移即借用了这个流程，在检查副本齐全的时候同时检查副本所在的be是否属于该cluster，如果不属于，则记入要恢复的副本。并且副本多余要删除的时候会优先删除cluster外的副本，然后再按照现有的策略选择：宕机的be的副本->clone的副本->版本落后的副本->负载高的host上的副本，直到副本没有多余。
   
![](/images/cluster_link_and_migrate_db.png)

10. BE的进程隔离

  为了实现be进程之间实际cpu、io以及内存的隔离，需要依赖于be的部署。部署的时候需要在外围配置cgroup，把要部署的be的进程都写入cgroup。如果要实现io的物理隔离各be配置的数据存放路径需要在不同磁盘上，这里不做过多的介绍。

