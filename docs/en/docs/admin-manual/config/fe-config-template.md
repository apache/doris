---
{
    "title": "FE Configuration",
    "language": "en",
    "toc_min_heading_level": 2,
    "toc_max_heading_level": 4
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

<!-- Please sort the configuration alphabetically -->

# FE Configuration

This document mainly introduces the relevant configuration items of FE.

The FE configuration file `fe.conf` is usually stored in the `conf/` directory of the FE deployment path. In version 0.14, another configuration file `fe_custom.conf` will be introduced. The configuration file is used to record the configuration items that are dynamically configured and persisted by the user during operation.

After the FE process is started, it will read the configuration items in `fe.conf` first, and then read the configuration items in `fe_custom.conf`. The configuration items in `fe_custom.conf` will overwrite the same configuration items in `fe.conf`.

The location of the `fe_custom.conf` file can be configured in `fe.conf` through the `custom_config_dir` configuration item.

## Precautions

**1.** For the purpose of simplifying the architecture, modifying the configuration through the mysql protocol will only modify the data in the local FE memory, and will not synchronize the changes to all FEs.
For Config items that only take effect on the Master FE, the modification request will be automatically forwarded to the Master FE.

**2.** Note that the option ```forward_to_master``` will affect the display results of ```admin show frontend config```, if ```forward_to_master=true```, ```admin show frontend config``` shows the Config of Master FE (Even if you are connecting to a Follower FE currently), this may cause you to be unable to see the modification of the local FE configuration; if you expect show config of the FE you're connecting, then execute the command ```set forward_to_master=false```.


## View configuration items

There are two ways to view the configuration items of FE:

1. FE web page

    Open the FE web page `http://fe_host:fe_http_port/Configure` in the browser. You can see the currently effective FE configuration items in `Configure Info`.

2. View by command

    After the FE is started, you can view the configuration items of the FE in the MySQL client with the following command:

    `ADMIN SHOW FRONTEND CONFIG;`

    The meanings of the columns in the results are as follows:

    * Key: the name of the configuration item.
    * Value: The value of the current configuration item.
    * Type: The configuration item value type, such as integer or string.
    * IsMutable: whether it can be dynamically configured. If true, the configuration item can be dynamically configured at runtime. If false, it means that the configuration item can only be configured in `fe.conf` and takes effect after restarting FE.
    * MasterOnly: Whether it is a unique configuration item of Master FE node. If it is true, it means that the configuration item is meaningful only at the Master FE node, and is meaningless to other types of FE nodes. If false, it means that the configuration item is meaningful in all types of FE nodes.
    * Comment: The description of the configuration item.

## Set configuration items

There are two ways to configure FE configuration items:

1. Static configuration

    Add and set configuration items in the `conf/fe.conf` file. The configuration items in `fe.conf` will be read when the FE process starts. Configuration items not in `fe.conf` will use default values.

2. Dynamic configuration via MySQL protocol

    After the FE starts, you can set the configuration items dynamically through the following commands. This command requires administrator privilege.

    `ADMIN SET FRONTEND CONFIG (" fe_config_name "=" fe_config_value ");`

    Not all configuration items support dynamic configuration. You can check whether the dynamic configuration is supported by the `IsMutable` column in the` ADMIN SHOW FRONTEND CONFIG; `command result.

    If the configuration item of `MasterOnly` is modified, the command will be directly forwarded to the Master FE and only the corresponding configuration item in the Master FE will be modified.

    **Configuration items modified in this way will become invalid after the FE process restarts.**

    For more help on this command, you can view it through the `HELP ADMIN SET CONFIG;` command.
    
3. Dynamic configuration via HTTP protocol

    For details, please refer to [Set Config Action](../http-actions/fe/set-config-action.md)

    This method can also persist the modified configuration items. The configuration items will be persisted in the `fe_custom.conf` file and will still take effect after FE is restarted.

## Examples

1. Modify `async_pending_load_task_pool_size`

    Through `ADMIN SHOW FRONTEND CONFIG;` you can see that this configuration item cannot be dynamically configured (`IsMutable` is false). You need to add in `fe.conf`:

    `async_pending_load_task_pool_size = 20`

    Then restart the FE process to take effect the configuration.

2. Modify `dynamic_partition_enable`

    Through `ADMIN SHOW FRONTEND CONFIG;` you can see that the configuration item can be dynamically configured (`IsMutable` is true). And it is the unique configuration of Master FE. Then first we can connect to any FE and execute the following command to modify the configuration:

    ```
    ADMIN SET FRONTEND CONFIG ("dynamic_partition_enable" = "true"); `
    ```

    Afterwards, you can view the modified value with the following command:

    ```
    set forward_to_master = true;
    ADMIN SHOW FRONTEND CONFIG;
    ```

    After modification in the above manner, if the Master FE restarts or a Master election is performed, the configuration will be invalid. You can add the configuration item directly in `fe.conf` and restart the FE to make the configuration item permanent.

3. Modify `max_distribution_pruner_recursion_depth`

    Through `ADMIN SHOW FRONTEND CONFIG;` you can see that the configuration item can be dynamically configured (`IsMutable` is true). It is not unique to Master FE.

    Similarly, we can modify the configuration by dynamically modifying the configuration command. Because this configuration is not unique to the Master FE, user need to connect to different FEs separately to modify the configuration dynamically, so that all FEs use the modified configuration values.

## Configurations

> Note:
>
> The following content is automatically generated by `docs/generate-config-and-variable-doc.sh`.
>
> If you need to modify, please modify the description information in `fe/fe-common/src/main/java/org/apache/doris/common/Config.java`.

<--DOC_PLACEHOLDER-->
