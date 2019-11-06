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

> Note:
> 1. The following approaches are based on highly available deployments. That is, data 3 replicas, FE high availability.

## Test the correctness of BE upgrade

1. Arbitrarily select a BE node and deploy the latest palo_be binary file.
2. Restart the BE node and check the BE log be.INFO to see if the boot was successful.
3. If the startup fails, you can check the reason first. If the error is not recoverable, you can delete the BE directly through DROP BACKEND, clean up the data, and restart the BE using the previous version of palo_be. Then re-ADD BACKEND. (**This method will result in the loss of a copy of the data, please make sure that three copies are complete, and perform this operation!!!**

## Testing FE Metadata Compatibility

0. **Important! Exceptional metadata compatibility is likely to cause data can not be restored!!**
1. Deploy a test FE process (such as your own local developer) using the new version alone.
2. Modify the FE configuration file fe.conf for testing and set all ports to **different from online**.
3. Add configuration in fe.conf: cluster_id=123456
4. Add the configuration in fe.conf: metadatafailure_recovery=true
5. Copy the metadata directory palo-meta of the online environment Master FE to the test environment
6. Modify the cluster_id in the palo-meta/image/VERSION file copied into the test environment to 123456 (that is, the same as in Step 3)
7. "27979;" "35797;" "3681616;" sh bin /start fe.sh "21551;" FE
8. Observe whether the start-up is successful through FE log fe.log.
9. If the startup is successful, run sh bin/stop_fe.sh to stop the FE process of the test environment.
10. **The purpose of the above 2-6 steps is to prevent the FE of the test environment from being misconnected to the online environment after it starts.**

## Upgrade preparation

1. After data validation, the new version of BE and FE binary files are distributed to their respective directories.
2. Usually small version upgrade, BE only needs to upgrade palo_be; FE only needs to upgrade palo-fe.jar. If it is a large version upgrade, you may need to upgrade other files (including but not limited to bin / lib / etc.) If you are not sure whether you need to replace other files, it is recommended to replace all of them.

## rolling upgrade

1. Confirm that the new version of the file is deployed. Restart FE and BE instances one by one.
2. It is suggested that BE be restarted one by one and FE be restarted one by one. Because Doris usually guarantees backward compatibility between FE and BE, that is, the old version of FE can access the new version of BE. However, the old version of BE may not be supported to access the new version of FE.
3. It is recommended to restart the next instance after confirming that the previous instance started successfully. Refer to the Installation Deployment Document for the identification of successful instance startup.
