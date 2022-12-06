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

Doris can upgrade smoothly by rolling upgrades. The following steps are recommended for security upgrade.

**The name of the BE binary that appears in this doc is `doris_be`, which was `palo_be` in previous versions.**

> **Note:**
> 1. Doris does not support upgrading across two-digit version numbers, for example: you cannot upgrade directly from 0.13 to 0.15, only through 0.13.x -> 0.14.x -> 0.15.x, and the three-digit version number can be upgraded across versions, such as from 0.13 .15 can be directly upgraded to 0.14.13.1, it is not necessary to upgrade 0.14.7 or 0.14.12.1
> 2. The following approaches are based on highly available deployments. That is, data 3 replicas, FE high availability.

## Preparen

1. Turn off the replica repair and balance operation.

     There will be node restarts during the upgrade process, so unnecessary cluster balancing and replica repair logic may be triggered. You can close it first with the following command:

     ```
     # Turn off the replica ealance logic. After it is closed, the balancing operation of the ordinary table replica will no longer be triggered.
     $ mysql-client> admin set frontend config("disable_balance" = "true");
     
     # Turn off the replica balance logic of the colocation table. After it is closed, the replica redistribution operation of the colocation table will no longer be triggered.
     $ mysql-client> admin set frontend config("disable_colocate_balance" = "true");
     
     # Turn off the replica scheduling logic. After shutting down, all generated replica repair and balancing tasks will no longer be scheduled.
     $ mysql-client> admin set frontend config("disable_tablet_scheduler" = "true");
     ```

     After the cluster is upgraded, just use the above command to set the corresponding configuration to the original value.

2. **important! ! Metadata needs to be backed up before upgrading(The entire directory needs to be backed up)! !**

## Test the correctness of BE upgrade

1. Arbitrarily select a BE node and deploy the latest doris_be binary file.
2. Restart the BE node and check the BE log be.INFO to see if the boot was successful.
3. If the startup fails, you can check the reason first. If the error is not recoverable, you can delete the BE directly through DROP BACKEND, clean up the data, and restart the BE using the previous version of doris_be. Then re-ADD BACKEND. (**This method will result in the loss of a copy of the data, please make sure that three copies are complete, and perform this operation!!!**)
4. Install Java UDF function
<version since="1.2.0">Install Java UDF function: </version>, because Java UDF function is supported from version 1.2, you need to download the JAR package of Java UDF function from the official website and put it in the lib directory of BE, otherwise it may will fail to start.

## Testing FE Metadata Compatibility

0. **Important! Exceptional metadata compatibility is likely to cause data cannot be restored!!**
1. Deploy a test FE process (It is recommended to use your own local development machine, or BE node. If it is on the Follower or Observer node, you need to stop the started process, but it is not recommended to test on the Follower or Observer node) using the new version alone.
2. Modify the FE configuration file fe.conf for testing and set all ports to **different from online**.
3. Add configuration in fe.conf: cluster_id=123456
4. Add configuration in fe.conf: metadata_failure_recovery=true
5. Copy the metadata directory doris-meta of the online environment master Fe to the test environment
6.The cluster_ID where copy to the doris-meta/image/VERSION file in the test environment is modified to 123456 (that is, the same as in Step 3)
7. In the test environment,running sh sh bin/start_fe.sh,start FE.
8. Observe whether the start-up is successful through FE log fe.log.
9. If the startup is successful, run sh bin/stop_fe.sh to stop the FE process of the test environment.
10. **The purpose of the above 2-6 steps is to prevent the FE of the test environment from being misconnected to the online environment after it starts.**

## Upgrade preparation

1. After data validation, the new version of BE and FE binary files are distributed to their respective directories.
2. Usually small version upgrade, BE only needs to upgrade doris_be; FE only needs to upgrade palo-fe.jar. If it is a large version upgrade, you may need to upgrade other files (including but not limited to bin / lib / etc.) If you are not sure whether you need to replace other files, it is recommended to replace all of them.

## rolling upgrade

1. Confirm that the new version of the file is deployed. Restart FE and BE instances one by one.
2. It is suggested that BE be restarted one by one and FE be restarted one by one. Because Doris usually guarantees backward compatibility between FE and BE, that is, the old version of FE can access the new version of BE. However, the old version of BE may not be supported to access the new version of FE.
3. It is recommended to restart the next instance after confirming the previous instance started successfully. Refer to the Installation Deployment Document for the identification of successful instance startup.

## About version rollback
Because the database is a stateful service, Doris cannot support version rollback (version downgrade) in most cases. In some cases, the rollback of the 3-bit or 4-bit version can be supported, but the rollback of the 2-bit version will not be supported.

Therefore, it is recommended to upgrade some nodes and observe the business operation (gray upgrade) to reduce the upgrade risk.

**Illegal rollback operation may cause data loss and damage.** 
