// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.cloud.catalog;

import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.DebugPointUtil;
import org.apache.doris.common.util.MasterDaemon;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Refreshes each FE's cloud metadata from one MS snapshot per cycle. Only the master reconciles nodes;
 * every FE refreshes its local instance and compute group state.
 */
public class CloudMetadataSynchronizer extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(CloudMetadataSynchronizer.class);

    private final CloudSystemInfoService cloudSystemInfoService;
    private final CloudClusterChecker clusterChecker;
    private final CloudInstanceStatusChecker instanceStatusChecker;
    // Serialize fetching as well as applying so a manual refresh cannot be overwritten by an older cycle.
    private final Object checkLock = new Object();

    public CloudMetadataSynchronizer(CloudSystemInfoService cloudSystemInfoService) {
        this(cloudSystemInfoService, new CloudClusterChecker(cloudSystemInfoService),
                new CloudInstanceStatusChecker(cloudSystemInfoService));
    }

    CloudMetadataSynchronizer(CloudSystemInfoService cloudSystemInfoService,
            CloudClusterChecker clusterChecker, CloudInstanceStatusChecker instanceStatusChecker) {
        super("cloud metadata sync", Config.cloud_cluster_check_interval_second * 1000L);
        this.cloudSystemInfoService = cloudSystemInfoService;
        this.clusterChecker = clusterChecker;
        this.instanceStatusChecker = instanceStatusChecker;
    }

    @Override
    protected void runAfterCatalogReady() {
        synchronized (checkLock) {
            try {
                if (DebugPointUtil.isEnable("CloudMetadataSynchronizer.beforeFetch.pause")) {
                    LOG.info("CloudMetadataSynchronizer.beforeFetch.pause phase={}",
                            DebugPointUtil.getDebugParamOrDefault(
                                    "CloudMetadataSynchronizer.beforeFetch.pause", "phase", ""));
                    return;
                }
                Cloud.GetInstanceResponse response = cloudSystemInfoService.getCloudInstance();
                if (response == null || !response.hasStatus() || !response.getStatus().hasCode()
                        || response.getStatus().getCode() != Cloud.MetaServiceCode.OK || !response.hasInstance()) {
                    LOG.warn("failed to get complete cloud instance snapshot, cloud_unique_id={}, status={}",
                            Config.cloud_unique_id, response == null ? null : response.getStatus());
                    return;
                }
                Cloud.InstanceInfoPB instance = response.getInstance();
                if (DebugPointUtil.isEnable("CloudMetadataSynchronizer.afterGetInstance.pause")
                        && instance.getClustersList().stream().anyMatch(c -> c.getClusterId().equals(
                                DebugPointUtil.getDebugParamOrDefault(
                                        "CloudMetadataSynchronizer.afterGetInstance.pause", "cluster_id", "")))) {
                    LOG.info("CloudMetadataSynchronizer.afterGetInstance.pause phase={}",
                            DebugPointUtil.getDebugParamOrDefault(
                                    "CloudMetadataSynchronizer.afterGetInstance.pause", "phase", ""));
                    while (DebugPointUtil.isEnable("CloudMetadataSynchronizer.afterGetInstance.pause")) {
                        Thread.sleep(100);
                    }
                }
                if (Env.getCurrentEnv().isMaster()) {
                    try {
                        // Install physical groups before refreshing their properties and virtual group references.
                        clusterChecker.check(instance);
                    } catch (Exception e) {
                        LOG.warn("failed to reconcile cloud nodes", e);
                    }
                }
                instanceStatusChecker.check(instance);
            } catch (Exception e) {
                LOG.warn("failed to synchronize cloud metadata", e);
            }
        }
    }

    public void checkNow() {
        if (Env.getCurrentEnv().isMaster()) {
            runAfterCatalogReady();
        }
    }
}
