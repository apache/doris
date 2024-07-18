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
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.MasterDaemon;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

public class CloudInstanceStatusChecker extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(CloudInstanceStatusChecker.class);
    private CloudSystemInfoService cloudSystemInfoService;

    public CloudInstanceStatusChecker(CloudSystemInfoService cloudSystemInfoService) {
        super("cloud instance check");
        this.cloudSystemInfoService = cloudSystemInfoService;
    }

    @Override
    protected void runAfterCatalogReady() {
        try {
            Cloud.GetInstanceResponse response = cloudSystemInfoService.getCloudInstance();
            if (LOG.isDebugEnabled()) {
                LOG.debug("get from ms response {}", response);
            }
            if (response == null || !response.hasStatus() || !response.getStatus().hasCode()
                    || response.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
                LOG.warn("failed to get cloud instance due to incomplete response, "
                        + "cloud_unique_id={}, response={}", Config.cloud_unique_id, response);
            } else {
                cloudSystemInfoService.setInstanceStatus(response.getInstance().getStatus());
                Map<String, String> vaultMap = new HashMap<>();
                int cnt = response.getInstance().getResourceIdsCount();
                for (int i = 0; i < cnt; i++) {
                    String name = response.getInstance().getStorageVaultNames(i);
                    String id = response.getInstance().getResourceIds(i);
                    vaultMap.put(name, id);
                }
                Env.getCurrentEnv().getStorageVaultMgr().refreshVaultMap(vaultMap);
                Env.getCurrentEnv().getStorageVaultMgr().setDefaultStorageVault(
                        Pair.of(response.getInstance().getDefaultStorageVaultName(),
                                response.getInstance().getDefaultStorageVaultId()));
            }
        } catch (Exception e) {
            LOG.warn("get instance from ms exception", e);
        }
    }
}
