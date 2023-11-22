---
{
    "title": "Cluster upgrade",
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

# Cluster upgrade

## Overview

To upgrade, please use the steps recommended in this chapter to upgrade the cluster. The Doris cluster upgrade can be upgraded using the **rolling upgrade** method, which does not require all cluster nodes to be shut down for upgrade, which greatly reduces the impact on upper-layer applications.

## Doris Release Notes

:::tip

For Doris upgrade, please follow the principle of **Do not upgrade across two or more key node versions**. If you want to upgrade across multiple key node versions, first upgrade to the nearest key node version, and then upgrade in turn. If it is not critical node version, it can be ignored and skipped.

Key node version: the version that must be experienced when upgrading, it may be a single version, or a version range, such as `1.1.3 - 1.1.5`, it means that you can continue to upgrade after upgrading to any version in this range .

:::

| Version number | Key node version | LTS version |
| ------------------------ | ------------ | -------- |
| 0.12.x | Yes | No |
| 0.13.x | Yes | No |
| 0.14.x | Yes | No |
| 0.15.x | Yes | No |
| 1.0.0 - 1.1.2 | No | No |
| 1.1.3 - 1.1.5 | Yes | 1.1-LTS |
| 1.2.0 - 1.2.5 | Yes | 1.2-LTS |
| 2.0.0-alpha - 2.0.0-beta | Yes | 2.0-LTS |

Example:

The current version is `0.12`, upgrade route to `2.0.0-beta` version

`0.12` -> `0.13` -> `0.14` -> `0.15` -> `1.1.3 - 1.1.5` any version -> `1.2.0 - 1.2.5` any version -> `2.0.0 -beta`

:::tip

LTS version: Long-time Support, LTS version provides long-term support and will be maintained for more than six months. Generally speaking, the version with the larger third digit of the version number is more stable**.

Alpha version: an internal test version, the function has not been fully determined, and there may be major bugs. It is only recommended to use the test cluster for testing, ** it is not recommended to use the production cluster! **

Beta version: public test version, the function has been basically confirmed, there may be non-major bugs, it is only recommended to use the test cluster for testing, ** it is not recommended to use the production cluster! **

Release version: a public release version, which has completed the repair of basic important bugs and verification of functional defect fixes, and is recommended for production clusters.

:::

## Upgrade steps

### Upgrade Instructions

1. During the upgrade process, since Doris's RoutineLoad, Flink-Doris-Connector, and Spark-Doris-Connector have implemented a retry mechanism in the code, in a multi-BE node cluster, the rolling upgrade will not cause the task to fail .
2. The StreamLoad task requires you to implement a retry mechanism in your own code, otherwise the task will fail.
3. The cluster copy repair and balance function must be closed before and opened after the completion of a single upgrade task, regardless of whether all your cluster nodes have been upgraded.

### Overview of the upgrade process

1. Metadata backup
2. Turn off the cluster copy repair and balance function
3. Compatibility testing
4. Upgrade BE
5. Upgrade FE
6. Turn on the cluster replica repair and balance function

### Upgrade pre-work

Please perform the upgrade in sequence according to the upgrade process

#### metadata backup (important)

** Make a full backup of the `doris-meta` directory of the FE-Master node! **

#### Turn off the cluster replica repair and balance function

There will be node restart during the upgrade process, so unnecessary cluster balancing and replica repair logic may be triggered, first close it with the following command:

```sql
admin set frontend config("disable_balance" = "true");
admin set frontend config("disable_colocate_balance" = "true");
admin set frontend config("disable_tablet_scheduler" = "true");
```

#### Compatibility testing

:::tip

**Metadata compatibility is very important, if the upgrade fails due to incompatible metadata, it may lead to data loss! It is recommended to perform a metadata compatibility test before each upgrade! **

:::

##### FE Compatibility Test

:::tip

**important**

1. It is recommended to do FE compatibility test on your local development machine or BE node.

2. It is not recommended to test on Follower or Observer nodes to avoid link exceptions
3. If it must be on the Follower or Observer node, the started FE process needs to be stopped

:::

1. Use the new version alone to deploy a test FE process

    ```shell
    sh ${DORIS_NEW_HOME}/bin/start_fe.sh --daemon
    ```

2. Modify the FE configuration file fe.conf for testing

    ```shell
    vi ${DORIS_NEW_HOME}/conf/fe.conf
    ```

   Modify the following port information, set **all ports** to **different from online**

    ```shell
    ...
    http_port = 18030
    rpc_port = 19020
    query_port = 19030
    arrow_flight_sql_port = 19040
    edit_log_port = 19010
    ...
    ```

   save and exit

3. Modify fe.conf

   - Add ClusterID configuration in fe.conf

    ```shell
    echo "cluster_id=123456" >> ${DORIS_NEW_HOME}/conf/fe.conf
    ```

   - Add metadata failover configuration in fe.conf (**>=2.0.2 + version does not require this operation**)
   ```shell
   echo "metadata_failure_recovery=true" >> ${DORIS_NEW_HOME}/conf/fe.conf
   ```

4. Copy the metadata directory doris-meta of the online environment Master FE to the test environment

    ```shell
    cp ${DORIS_OLD_HOME}/fe/doris-meta/* ${DORIS_NEW_HOME}/fe/doris-meta
    ```

5. Change the cluster_id in the VERSION file copied to the test environment to 123456 (that is, the same as in step 3)

    ```shell
    vi ${DORIS_NEW_HOME}/fe/doris-meta/image/VERSION
    clusterId=123456
    ```

6. In the test environment, run the startup FE

- If the version is greater than or equal to 2.0.2, run the following command

  ```shell
  sh ${DORIS_NEW_HOME}/bin/start_fe.sh --daemon --metadata_failure_recovery
  ```
- If the version is less than 2.0.2, run the following command
   ```shell
   sh ${DORIS_NEW_HOME}/bin/start_fe.sh --daemon
   ```

7. Observe whether the startup is successful through the FE log fe.log

    ```shell
    tail -f ${DORIS_NEW_HOME}/log/fe.log
    ```

8. If the startup is successful, it means that there is no problem with the compatibility, stop the FE process of the test environment, and prepare for the upgrade

    ```
    sh ${DORIS_NEW_HOME}/bin/stop_fe.sh
    ```

##### BE Compatibility Test

You can use the grayscale upgrade scheme to upgrade a single BE first. If there is no exception or error, the compatibility is considered normal, and subsequent upgrade actions can be performed

### Upgrade process

:::tip

Upgrade BE first, then FE

Generally speaking, Doris only needs to upgrade `/bin` and `/lib` under the FE directory and `/bin` and `/lib` under the BE directory

In versions 2.0.2 and later, the `custom_lib/` directory is added to the FE and BE deployment paths (if not, it can be created manually). The `custom_lib/` directory is used to store some user-defined third-party jar packages, such as `hadoop-lzo-*.jar`, `orai18n.jar`, etc.

This directory does not need to be replaced during upgrade.

However, when a major version is upgraded, new features may be added or old functions refactored. These modifications may require **replace/add** more directories during the upgrade to ensure the availability of all new features. Please Carefully pay attention to the Release-Note of this version when upgrading the version to avoid upgrade failures

:::

#### Upgrade BE

:::tip

In order to ensure the safety of your data, please use 3 copies to store your data to avoid data loss caused by misoperation or failure of the upgrade

:::

1. Under the premise of multiple copies, select a BE node to stop running and perform grayscale upgrade

    ```shell
    sh ${DORIS_OLD_HOME}/be/bin/stop_be.sh
    ```

2. Rename the `/bin`, `/lib` directories under the BE directory

    ```shell
    mv ${DORIS_OLD_HOME}/be/bin ${DORIS_OLD_HOME}/be/bin_back
    mv ${DORIS_OLD_HOME}/be/lib ${DORIS_OLD_HOME}/be/lib_back
    ```

3. Copy the new version of `/bin`, `/lib` directory to the original BE directory

    ```shell
    cp ${DORIS_NEW_HOME}/be/bin ${DORIS_OLD_HOME}/be/bin
    cp ${DORIS_NEW_HOME}/be/lib ${DORIS_OLD_HOME}/be/lib
    ```

4. Start the BE node

    ```shell
    sh ${DORIS_OLD_HOME}/be/bin/start_be.sh --daemon
    ```

5. Link the cluster to view the node information

    ```mysql
    show backends\G
    ```

   If the `alive` status of the BE node is `true`, and the value of `Version` is the new version, the node upgrade is successful

6. Complete the upgrade of other BE nodes in sequence

#### Upgrade FE

:::tip

Upgrade the non-Master nodes first, and then upgrade the Master nodes.

:::

1. In the case of multiple FE nodes, select a non-Master node to upgrade and stop running first

    ```shell
    sh ${DORIS_OLD_HOME}/fe/bin/stop_fe.sh
    ```

2. Rename the `/bin`, `/lib` directories under the FE directory

    ```shell
    mv ${DORIS_OLD_HOME}/fe/bin ${DORIS_OLD_HOME}/fe/bin_back
    mv ${DORIS_OLD_HOME}/fe/lib ${DORIS_OLD_HOME}/fe/lib_back
    ```

3. Copy the new version of `/bin`, `/lib` directory to the original FE directory

    ```shell
    cp ${DORIS_NEW_HOME}/fe/bin ${DORIS_OLD_HOME}/fe/bin
    cp ${DORIS_NEW_HOME}/fe/lib ${DORIS_OLD_HOME}/fe/lib
    ```

4. Start the FE node

    ```shell
    sh ${DORIS_OLD_HOME}/fe/bin/start_fe.sh --daemon
    ```

5. Link the cluster to view the node information

    ```mysql
    show frontends\G
    ```

   If the FE node `alive` status is `true`, and the value of `Version` is the new version, the node is upgraded successfully

6. Complete the upgrade of other FE nodes in turn, **finally complete the upgrade of the Master node**

#### Turn on the cluster replica repair and balance function

After the upgrade is complete and all BE nodes become `Alive`, enable the cluster copy repair and balance function:

```sql
admin set frontend config("disable_balance" = "false");
admin set frontend config("disable_colocate_balance" = "false");
admin set frontend config("disable_tablet_scheduler" = "false");
```
