---
{
    "title": "WORKLOAD GROUP",
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

# WORKLOAD GROUP

<version since="dev"></version>

workload group 可限制组内任务在单个be节点上的计算资源和内存资源的使用。当前支持query绑定到workload group。

## 版本说明
Workload Group是从2.0版本开始支持的功能，Workload Group在2.0版本和2.1版本的主要区别在于，2.0版本的Workload Group不依赖CGroup，而2.1版本的Workload Group依赖CGroup，因此使用2.1版本的Workload Group时要配置CGroup的环境。

#### 升级到2.0版本
1 如果是从1.2版本升级到2.0版本时，建议Doris集群整体升级完成后，再开启WorkloadGroup功能。因为如果只升级单台Follower就开启此功能，由于Master的FE代码还没有更新，此时Doris集群中并没有Workload Group的元数据信息，这可能导致已升级的Follower节点的查询失败。建议的升级流程如下：
* 先把Doris集群整体代码升级到2.0版本。
* 再根据下文中***workload group使用***的章节开始使用该功能。

#### 升级到2.1版本
2 如果代码版本是从2.0升级到2.1的，分为以下两种情况：

情况1：在2.1版本如果已经使用了Workload Group功能，那么只需要参考下文中配置cgroup v1的流程即可使用新版本的Workload Group功能。

情况2：如果在2.0版本没有使用Workload Group功能，那么也需要先把Doris集群整体升级到2.1版本后，再根据下文的***workload group使用***的章节开始使用该功能。

## workload group属性

* cpu_share: 可选，默认值为1024，取值范围是正整数。用于设置workload group获取cpu时间的多少，可以实现cpu资源软隔离。cpu_share 是相对值，表示正在运行的workload group可获取cpu资源的权重。例如，用户创建了3个workload group g-a、g-b和g-c，cpu_share 分别为 10、30、40，某一时刻g-a和g-b正在跑任务，而g-c没有任务，此时g-a可获得 25% (10 / (10 + 30))的cpu资源，而g-b可获得75%的cpu资源。如果系统只有一个workload group正在运行，则不管其cpu_share的值为多少，它都可获取全部的cpu资源。

* memory_limit: 可选，默认值0%，不限制，取值范围1%~100%，用于设置workload group可以使用be内存的百分比。Workload Group可用的最大内存，所有group的累加值不可以超过100%，通常与enable_memory_overcommit配合使用。如果一个机器的内存为64G，mem_limit=50%，那么该group的实际物理内存=64G * 90%(be conf mem_limit) * 50%= 28.8G，这里的90%是BE进程级别的mem_limit参数，限制整个BE进程的内存用量。一个集群中所有Workload Group的memory_limit的累加值不能超过100%。

* enable_memory_overcommit: 可选，用于开启workload group内存软隔离，默认为true。如果设置为false，则该workload group为内存硬隔离，系统检测到workload group内存使用超出限制后将立即cancel组内内存占用最大的若干个任务，以释放超出的内存；如果设置为true，则该workload group为内存软隔离，如果系统有空闲内存资源则该workload group在超出memory_limit的限制后可继续使用系统内存，在系统总内存紧张时会cancel组内内存占用最大的若干个任务，释放部分超出的内存以缓解系统内存压力。建议在有workload group开启该配置时，所有workload group的 memory_limit 总和低于100%，剩余部分用于workload group内存超发。

* cpu_hard_limit：可选，默认值-1%，不限制。取值范围1%~100%，CPU硬限制模式下，Workload Group最大可用的CPU百分比，不管当前机器的CPU资源是否被用满，Workload Group的最大CPU用量都不能超过cpu_hard_limit，
  所有Workload Group的cpu_hard_limit累加值不能超过100%。2.1版本新增属性
* max_concurrency：可选，最大查询并发数，默认值为整型最大值，也就是不做并发的限制。运行中的查询数量达到该值时，新来的查询会进入排队的逻辑。
* max_queue_size：可选，查询排队队列的长度，当排队队列已满时，新来的查询会被拒绝。默认值为0，含义是不排队。
* queue_timeout：可选，查询在排队队列中的超时时间，单位为毫秒，如果查询在队列中的排队时间超过这个值，那么就会直接抛出异常给客户端。默认值为0，含义是不排队。
* scan_thread_num：可选，当前workload group用于scan的线程个数，默认值为-1，含义是不生效，此时以be配置中的scan线程数为准。取值为大于0的整数。

注意事项：

1 目前暂不支持CPU的软限和硬限的同时使用，一个集群某一时刻只能是软限或者硬限，下文中会描述切换方法。

2 所有属性均为可选，但是在创建Workload Group时需要指定至少一个属性。

## 配置cgroup v1的环境
Doris的2.0版本使用基于Doris的调度实现CPU资源的限制，但是从2.1版本起，Doris默认使用基于CGroup v1版本对CPU资源进行限制（暂不支持CGroup v2），因此如果期望在2.1版本对CPU资源进行约束，那么需要BE所在的节点上已经安装好CGroup v1的环境。

用户如果在2.0版本使用了Workload Group的软限并升级到了2.1版本，那么也需要配置CGroup，否则可能导致软限失效。

在不配置cgroup的情况下，用户可以使用workload group除CPU限制外的所有功能。

1 首先确认BE所在节点已经安装好CGroup v1版本，确认存在路径```/sys/fs/cgroup/cpu/```即可

2 在cgroup的cpu路径下新建一个名为doris的目录，这个目录名用户可以自行指定

```mkdir /sys/fs/cgroup/cpu/doris```

3 需要保证Doris的BE进程对于这个目录有读/写/执行权限
```
// 修改这个目录的权限为可读可写可执行
chmod 770 /sys/fs/cgroup/cpu/doris

// 把这个目录的归属划分给doris的账户
chonw -R doris:doris /sys/fs/cgroup/cpu/doris
```

4 修改BE的配置，指定cgroup的路径
```
doris_cgroup_cpu_path = /sys/fs/cgroup/cpu/doris
```

5 重启BE，在日志（be.INFO）可以看到"add thread xxx to group"的字样代表配置成功

需要注意的是，目前的workload group暂时不支持一个机器多个BE的部署方式。

## workload group使用

1. 手动创建一个名为normal的Workload Group，这个Workload Group为系统默认的Workload Group，不可删除。
```
create workload group if not exists normal 
properties (
	'cpu_share'='1024',
	'memory_limit'='30%',
	'enable_memory_overcommit'='true'
);
```
normal Group的作用在于，当你不为查询指定Workload Group时，查询会默认使用该Group，从而避免查询失败。

2. 开启 experimental_enable_workload_group 配置项，在fe.conf中设置：
```
experimental_enable_workload_group=true
```

3. 如果期望使用其他group进行测试，那么可以创建一个自定义的workload group，
```
create workload group if not exists g1
properties (
    "cpu_share"="1024",
    "memory_limit"="30%",
    "enable_memory_overcommit"="true"
);
```
此时配置的CPU限制为软限。

创建workload group详细可参考：[CREATE-WORKLOAD-GROUP](../sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-WORKLOAD-GROUP.md)，另删除workload group可参考[DROP-WORKLOAD-GROUP](../sql-manual/sql-reference/Data-Definition-Statements/Drop/DROP-WORKLOAD-GROUP.md)；修改workload group可参考：[ALTER-WORKLOAD-GROUP](../sql-manual/sql-reference/Data-Definition-Statements/Alter/ALTER-WORKLOAD-GROUP.md)；查看workload group可参考：[WORKLOAD_GROUPS()](../sql-manual/sql-functions/table-functions/workload-group.md)和[SHOW-WORKLOAD-GROUPS](../sql-manual/sql-reference/Show-Statements/SHOW-WORKLOAD-GROUPS.md)。

4. 开启pipeline执行引擎，workload group cpu隔离基于pipeline执行引擎实现，因此需开启session变量：
```
set experimental_enable_pipeline_engine = true;
```

5. 绑定workload group。
* 通过设置user property 将user默认绑定到workload group，默认为`normal`:
```
set property 'default_workload_group' = 'g1';
```
当前用户的查询将默认使用'g1'。
* 通过session变量指定workload group, 默认为空:
```
set workload_group = 'g2';
```
session变量`workload_group`优先于 user property `default_workload_group`, 在`workload_group`为空时，查询将绑定到`default_workload_group`, 在session变量`workload_group`不为空时，查询将绑定到`workload_group`。

如果是非admin用户，需要先执行[SHOW-WORKLOAD-GROUPS](../sql-manual/sql-reference/Show-Statements/SHOW-WORKLOAD-GROUPS.md) 确认下当前用户能否看到该 workload group，不能看到的 workload group 可能不存在或者当前用户没有权限，执行查询时会报错。给 workload group 授权参考：[grant语句](../sql-manual/sql-reference/Account-Management-Statements/GRANT.md)。

6. 执行查询，查询将关联到指定的 workload group。

### 查询排队功能
```
create workload group if not exists test_group
properties (
    "cpu_share"="10",
    "memory_limit"="30%",
    "max_concurrency" = "10",
    "max_queue_size" = "20",
    "queue_timeout" = "3000"
);
```

需要注意的是，目前的排队设计是不感知FE的个数的，排队的参数只在单FE粒度生效，例如：

一个Doris集群配置了一个work load group，设置max_concurrency = 1
如果集群中有1FE，那么这个workload group在Doris集群视角看同时只会运行一个SQL
如果有3台FE，那么在Doris集群视角看最大可运行的SQL个数为3

### 配置CPU的硬限
目前Doris默认运行CPU的软限，如果期望使用Workload Group的硬限功能，可以按照如下流程操作。

1 在FE中开启CPU的硬限的功能，如果有多个FE，那么需要在每个FE上都进行相同操作。
```
1 修改磁盘上fe.conf的配置
experimental_enable_cpu_hard_limit = true

2 修改内存中的配置
ADMIN SET FRONTEND CONFIG ("enable_cpu_hard_limit" = "true");
```

2 修改Workload Group的cpu_hard_limit属性
```
alter workload group group1 properties ( 'cpu_hard_limit'='20%' );
```

3 查看当前的Workload Group的配置，可以看到尽管此时cpu_share的值可能不为0，但是由于开启了硬限模式，那么查询在执行时也会走CPU的硬限。也就是说CPU软硬限的开关不影响元数据的修改。
```
mysql [(none)]>select name, cpu_share,memory_limit,enable_memory_overcommit,cpu_hard_limit from workload_groups() where name='group1';
+--------+-----------+--------------+--------------------------+----------------+
| Name   | cpu_share | memory_limit | enable_memory_overcommit | cpu_hard_limit |
+--------+-----------+--------------+--------------------------+----------------+
| group1 |        10 | 45%          | true                     | 20%            |
+--------+-----------+--------------+--------------------------+----------------+
1 row in set (0.03 sec)
```

### CPU软硬限模式切换的说明
目前Doris暂不支持同时运行CPU的软限和硬限，一个Doris集群在任意时刻只能是CPU软限或者CPU硬限。
用户可以在两种模式之间进行切换，主要切换方法如下：

1 假如当前的集群配置是默认的CPU软限制，然后期望改成CPU的硬限，那么首先需要把Workload Group的cpu_hard_limit参数修改成一个有效的值
```
alter workload group group1 properties ( 'cpu_hard_limit'='20%' );
```
需要修改当前集群中所有的Workload Group的这个属性，所有Workload Group的cpu_hard_limit的累加值不能超过100%
由于CPU的硬限无法给出一个有效的默认值，因此如果只打开开关但是不修改属性，那么CPU的硬限也无法生效。

2 在所有FE中打开CPU硬限的开关
```
1 修改磁盘上fe.conf的配置
experimental_enable_cpu_hard_limit = true

2 修改内存中的配置
ADMIN SET FRONTEND CONFIG ("enable_cpu_hard_limit" = "true");
```

如果用户期望从CPU的硬限切换回CPU的软限，那么只需要在FE修改enable_cpu_hard_limit的值为false即可。
CPU软限的属性cpu_share默认会填充一个有效值1024(如果之前未指定cpu_share的值)，用户可以根据group的优先级对cpu_share的值进行重新调整。