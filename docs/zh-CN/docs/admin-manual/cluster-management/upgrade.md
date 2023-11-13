---
{
    "title": "集群升级",
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

# 集群升级

## 概述

升级请使用本章节中推荐的步骤进行集群升级，Doris 集群升级可使用**滚动升级**的方式进行升级，无需集群节点全部停机升级，极大程度上降低对上层应用的影响。

## Doris 版本说明

:::tip

Doris 升级请遵守**不要跨两个及以上关键节点版本升级**的原则，若要跨多个关键节点版本升级，先升级到最近的关键节点版本，随后再依次往后升级，若是非关键节点版本，则可忽略跳过。

关键节点版本：升级时必须要经历的版本，可能是单独一个版本，也可能是一个版本区间，如 `1.1.3 - 1.1.5`，则表示升级至该区间任意一版本即可继续后续升级。

:::

| 版本号                   | 关键节点版本 | LTS 版本 |
| ------------------------ | ------------ | -------- |
| 0.12.x                   | 是           | 否       |
| 0.13.x                   | 是           | 否       |
| 0.14.x                   | 是           | 否       |
| 0.15.x                   | 是           | 否       |
| 1.0.0 - 1.1.2            | 否           | 否       |
| 1.1.3 - 1.1.5            | 是           | 1.1-LTS  |
| 1.2.0 - 1.2.5            | 是           | 1.2-LTS  |
| 2.0.0-alpha - 2.0.0-beta | 是           | 2.0-LTS  |

示例：

当前版本为 `0.12`，升级到 `2.0.0-beta` 版本的升级路线

`0.12` -> `0.13` -> `0.14` -> `0.15` -> `1.1.3 - 1.1.5` 任意版本 -> `1.2.0 - 1.2.5` 任意版本 -> `2.0.0-beta`

:::tip

LTS 版本：Long-time Support，LTS 版本提供长期支持，会持续维护六个月以上，通常而言，**版本号第三位数越大的版本，稳定性越好**。

Alpha 版本：内部测试版本，功能还未完全确定，或许存在重大 BUG，只推荐上测试集群做测试，**不推荐上生产集群！**

Beta 版本：公开测试版本，功能已基本确定，或许存在非重大 BUG，只推荐上测试集群做测试，**不推荐上生产集群！**

Release 版本：公开发行版，已完成基本重要 BUG 的修复和功能性缺陷修复验证，推荐上生产集群。

:::

## 升级步骤

### 升级说明

1. 在升级过程中，由于 Doris 的 RoutineLoad、Flink-Doris-Connector、Spark-Doris-Connector 都已在代码中实现了重试机制，所以在多 BE 节点的集群中，滚动升级不会导致任务失败。
2. StreamLoad 任务需要您在自己的代码中实现重试机制，否则会导致任务失败。
3. 集群副本修复和均衡功能在单次升级任务中务必要前置关闭和结束后打开，无论您集群节点是否全部升级完成。

### 升级流程概览

1. 元数据备份
2. 关闭集群副本修复和均衡功能
3. 兼容性测试
4. 升级 BE
5. 升级 FE
6. 打开集群副本修复和均衡功能

### 升级前置工作

请按升级流程顺次执行升级

#### 元数据备份（重要）

**将 FE-Master 节点的 `doris-meta` 目录进行完整备份！**

#### 关闭集群副本修复和均衡功能

升级过程中会有节点重启，所以可能会触发不必要的集群均衡和副本修复逻辑，先通过以下命令关闭：

```sql
admin set frontend config("disable_balance" = "true");
admin set frontend config("disable_colocate_balance" = "true");
admin set frontend config("disable_tablet_scheduler" = "true");
```

#### 兼容性测试

:::tip

**元数据兼容非常重要，如果因为元数据不兼容导致的升级失败，那可能会导致数据丢失！建议每次升级前都进行元数据兼容性测试！**

:::

##### FE 兼容性测试

:::tip

**重要**

1. 建议在自己本地的开发机，或者 BE 节点做 FE 兼容性测试。

2. 不建议在 Follower 或者 Observer 节点上测试，避免出现链接异常
3. 如果一定在 Follower 或者 Observer 节点上，需要停止已启动的 FE 进程

:::

1. 单独使用新版本部署一个测试用的 FE 进程

   ```shell
   sh ${DORIS_NEW_HOME}/bin/start_fe.sh --daemon
   ```

2. 修改测试用的 FE 的配置文件 fe.conf

   ```shell
   vi ${DORIS_NEW_HOME}/conf/fe.conf
   ```

   修改以下端口信息，将**所有端口**设置为**与线上不同**

   ```shell
   ...
   http_port = 18030
   rpc_port = 19020
   query_port = 19030
   arrow_flight_sql_port = 19040
   edit_log_port = 19010
   ...
   ```

   保存并退出

3. 修改 fe.conf
  - 在 fe.conf 添加 ClusterID 配置

   ```shell
   echo "cluster_id=123456" >> ${DORIS_NEW_HOME}/conf/fe.conf
   ```

   - 添加元数据故障恢复配置 （**2.0.2 + 版本无需进行此操作**）
   ```shell
   echo "metadata_failure_recovery=true" >> ${DORIS_NEW_HOME}/conf/fe.conf
   ```

4. 拷贝线上环境 Master FE 的元数据目录 doris-meta 到测试环境

   ```shell
   cp ${DORIS_OLD_HOME}/fe/doris-meta/* ${DORIS_NEW_HOME}/fe/doris-meta
   ```

5. 将拷贝到测试环境中的 VERSION 文件中的 cluster_id 修改为 123456（即与第3步中相同）

   ```shell
   vi ${DORIS_NEW_HOME}/fe/doris-meta/image/VERSION
   clusterId=123456
   ```

6. 在测试环境中，运行启动 FE （**请按照版本选择启动 FE 的方式**）

- 2.0.2(包含2.0.2) + 版本
   ```shell
   sh ${DORIS_NEW_HOME}/bin/start_fe.sh --daemon --metadata_failure_recovery
   ```
- 2.0.1（包含2.0.1） 以前的版本
  ```shell
  sh ${DORIS_NEW_HOME}/bin/start_fe.sh --daemon 
   ```

7. 通过 FE 日志 fe.log 观察是否启动成功

   ```shell
   tail -f ${DORIS_NEW_HOME}/log/fe.log
   ```

8. 如果启动成功，则代表兼容性没有问题，停止测试环境的 FE 进程，准备升级

   ```
   sh ${DORIS_NEW_HOME}/bin/stop_fe.sh
   ```

##### BE 兼容性测试

可利用灰度升级方案，先升级单个 BE，无异常和报错情况下即视为兼容性正常，可执行后续升级动作

### 升级流程

:::tip

先升级 BE，后升级FE

一般而言，Doris 只需要升级 FE 目录下的 `/bin` 和 `/lib` 以及 BE 目录下的  `/bin` 和 `/lib`

在 2.0.2 及之后的版本，FE 和 BE 部署路径下新增了 `custom_lib/` 目录（如没有可以手动创建）。`custom_lib/` 目录用于存放一些用户自定义的第三方 jar 包，如 `hadoop-lzo-*.jar`，`orai18n.jar` 等。

这个目录在升级时不需要替换。

但是在大版本升级时，可能会有新的特性增加或者老功能的重构，这些修改可能会需要升级时**替换/新增**更多的目录来保证所有新功能的可用性，请大版本升级时仔细关注该版本的 Release-Note，以免出现升级故障

:::

#### 升级 BE

:::tip

为了保证您的数据安全，请使用 3 副本来存储您的数据，以避免升级误操作或失败导致的数据丢失问题

:::

1. 在多副本的前提下，选择一台 BE 节点停止运行，进行灰度升级

   ```shell
   sh ${DORIS_OLD_HOME}/be/bin/stop_be.sh
   ```

2. 重命名 BE 目录下的 `/bin`，`/lib` 目录

   ```shell
   mv ${DORIS_OLD_HOME}/be/bin ${DORIS_OLD_HOME}/be/bin_back
   mv ${DORIS_OLD_HOME}/be/lib ${DORIS_OLD_HOME}/be/lib_back
   ```

3. 复制新版本的  `/bin`，`/lib` 目录到原 BE 目录下

   ```shell
   cp ${DORIS_NEW_HOME}/be/bin ${DORIS_OLD_HOME}/be/bin
   cp ${DORIS_NEW_HOME}/be/lib ${DORIS_OLD_HOME}/be/lib
   ```

4. 启动该 BE 节点

   ```shell
   sh ${DORIS_OLD_HOME}/be/bin/start_be.sh --daemon
   ```

5. 链接集群，查看该节点信息

   ```mysql
   show backends\G
   ```

   若该 BE 节点 `alive` 状态为 `true`，且 `Version` 值为新版本，则该节点升级成功

6. 依次完成其他 BE 节点升级

#### 升级 FE

:::tip

先升级非 Master 节点，后升级 Master 节点。

:::

1. 多个 FE 节点情况下，选择一个非 Master 节点进行升级，先停止运行

   ```shell
   sh ${DORIS_OLD_HOME}/fe/bin/stop_fe.sh
   ```

2. 重命名 FE 目录下的 `/bin`，`/lib`，`/mysql_ssl_default_certificate` 目录

   ```shell
   mv ${DORIS_OLD_HOME}/fe/bin ${DORIS_OLD_HOME}/fe/bin_back
   mv ${DORIS_OLD_HOME}/fe/lib ${DORIS_OLD_HOME}/fe/lib_back
   mv ${DORIS_OLD_HOME}/fe/mysql_ssl_default_certificate ${DORIS_OLD_HOME}/fe/mysql_ssl_default_certificate_back
   ```

3. 复制新版本的  `/bin`，`/lib`，`/mysql_ssl_default_certificate` 目录到原 FE 目录下

   ```shell
   cp ${DORIS_NEW_HOME}/fe/bin ${DORIS_OLD_HOME}/fe/bin
   cp ${DORIS_NEW_HOME}/fe/lib ${DORIS_OLD_HOME}/fe/lib
   cp -r ${DORIS_NEW_HOME}/fe/mysql_ssl_default_certificate ${DORIS_OLD_HOME}/fe/mysql_ssl_default_certificate
   ```

4. 启动该 FE 节点

   ```shell
   sh ${DORIS_OLD_HOME}/fe/bin/start_fe.sh --daemon
   ```

5. 链接集群，查看该节点信息

   ```mysql
   show frontends\G
   ```

   若该 FE 节点 `alive` 状态为 `true`，且 `Version` 值为新版本，则该节点升级成功

6. 依次完成其他 FE 节点升级，**最后完成 Master 节点的升级**

#### 打开集群副本修复和均衡功能

升级完成，并且所有 BE 节点状态变为 `Alive` 后，打开集群副本修复和均衡功能：

```sql
admin set frontend config("disable_balance" = "false");
admin set frontend config("disable_colocate_balance" = "false");
admin set frontend config("disable_tablet_scheduler" = "false");
```

