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

package org.apache.doris.common.plugin;

import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.catalog.CloudEnv;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.rpc.MetaServiceProxy;
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;

import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * CloudPluginConfigProvider retrieves S3 authentication info and plugin base paths
 * from Doris cloud mode environment.
 * <p>
 * Responsibilities:
 * - Configuration retrieval only, no download logic
 * - Converts complex cloud mode configuration to simple S3 config objects
 * - Provides unified configuration interface, hiding implementation details
 * <p>
 * Uses StorageVaultMgr to get default storage configuration, more stable and reliable.
 */
public class CloudPluginConfigProvider {
    private static final Logger LOG = LogManager.getLogger(CloudPluginConfigProvider.class);

    /**
     * Get S3 configuration from cloud mode
     *
     * @return S3 config object, or null if failed
     */
    public static S3PluginDownloader.S3Config getCloudS3Config() {
        try {
            Cloud.ObjectStoreInfoPB objInfo = getDefaultStorageVaultInfo();
            if (objInfo == null) {
                LOG.warn("Cannot get default storage vault info for plugin download");
                return null;
            }

            if (Strings.isNullOrEmpty(objInfo.getBucket())
                    || Strings.isNullOrEmpty(objInfo.getAk())
                    || Strings.isNullOrEmpty(objInfo.getSk())) {
                LOG.warn("Incomplete S3 configuration: bucket={}, ak={}, sk={}",
                        objInfo.getBucket(),
                        Strings.isNullOrEmpty(objInfo.getAk()) ? "empty" : "***",
                        Strings.isNullOrEmpty(objInfo.getSk()) ? "empty" : "***");
                return null;
            }

            return new S3PluginDownloader.S3Config(
                    objInfo.getEndpoint(),
                    objInfo.getRegion(),
                    objInfo.getBucket(),
                    objInfo.getAk(),
                    objInfo.getSk()
            );

        } catch (Exception e) {
            LOG.warn("Failed to get cloud S3 configuration: {}", e.getMessage());
            return null;
        }
    }

    /**
     * Get plugin base path (used to build complete S3 paths)
     *
     * @return base path like "instanceId/plugins"
     */
    public static String getPluginBasePath() {
        try {
            // Get instance ID
            String instanceId = getCloudInstanceId();
            if (Strings.isNullOrEmpty(instanceId)) {
                LOG.warn("Cannot get cloud instance ID");
                return null;
            }

            // Build base path: instanceId/plugins
            return instanceId + "/plugins";

        } catch (Exception e) {
            LOG.warn("Failed to get plugin base path: {}", e.getMessage());
            return null;
        }
    }

    /**
     * Build complete S3 plugin path
     *
     * @param pluginType plugin type (e.g., "jdbc_drivers")
     * @param pluginName plugin name (e.g., "mysql-connector.jar"), can be null for directory downloads
     * @return complete path like "instanceId/plugins/jdbc_drivers/mysql-connector.jar"
     */
    public static String buildPluginPath(String pluginType, String pluginName) {
        String basePath = getPluginBasePath();
        if (basePath == null) {
            return null;
        }

        StringBuilder pathBuilder = new StringBuilder(basePath)
                .append("/")
                .append(pluginType);

        if (!Strings.isNullOrEmpty(pluginName)) {
            pathBuilder.append("/").append(pluginName);
        }

        return pathBuilder.toString();
    }

    // ======================== Private Helper Methods ========================

    /**
     * Get default storage configuration info - using StorageVaultMgr
     * This method is copied from the working version you provided, more stable and reliable
     */
    private static Cloud.ObjectStoreInfoPB getDefaultStorageVaultInfo() {
        try {
            // Get default storage vault info from StorageVaultMgr
            Pair<String, String> defaultVault = Env.getCurrentEnv().getStorageVaultMgr().getDefaultStorageVault();
            if (defaultVault == null) {
                LOG.warn("No default storage vault configured for plugin downloader");
                return null;
            }

            String vaultName = defaultVault.first;
            LOG.info("Using default storage vault for plugin download: {}", vaultName);

            // Use the dedicated RPC to get storage vault object store information
            Cloud.GetObjStoreInfoRequest request = Cloud.GetObjStoreInfoRequest.newBuilder()
                    .setCloudUniqueId(Config.cloud_unique_id)
                    .build();

            try {
                Cloud.GetObjStoreInfoResponse response = MetaServiceProxy.getInstance().getObjStoreInfo(request);

                if (response.getStatus().getCode() == Cloud.MetaServiceCode.OK) {
                    // Find the default storage vault in the response
                    for (Cloud.StorageVaultPB vault : response.getStorageVaultList()) {
                        if (vaultName.equals(vault.getName())) {
                            if (vault.hasObjInfo()) {
                                LOG.info("Found storage vault {} with object store info", vaultName);
                                return vault.getObjInfo();
                            } else {
                                LOG.warn("Storage vault {} does not have object store info", vaultName);
                                return null;
                            }
                        }
                    }

                    // If not found by name, try to use the first available vault
                    if (!response.getStorageVaultList().isEmpty()) {
                        Cloud.StorageVaultPB firstVault = response.getStorageVaultList().get(0);
                        if (firstVault.hasObjInfo()) {
                            LOG.info("Using first available storage vault: {}", firstVault.getName());
                            return firstVault.getObjInfo();
                        }
                    }

                    LOG.warn("No suitable storage vault found for plugin download");
                    return null;
                } else {
                    LOG.warn("Failed to get storage vault info: {}", response.getStatus().getMsg());
                    return null;
                }
            } catch (Exception e) {
                LOG.warn("Failed to call getObjStoreInfo RPC: {}", e.getMessage());
                return null;
            }
        } catch (Exception e) {
            LOG.warn("Failed to get default storage vault info: {}", e.getMessage());
            return null;
        }
    }

    private static String getCloudInstanceId() {
        try {
            // Get from CloudEnv first
            if (Env.getCurrentEnv() instanceof CloudEnv) {
                String instanceId = ((CloudEnv) Env.getCurrentEnv()).getCloudInstanceId();
                if (!Strings.isNullOrEmpty(instanceId)) {
                    return instanceId;
                }
            }

            // Fallback to configuration item
            if (!Strings.isNullOrEmpty(Config.cloud_unique_id)) {
                return Config.cloud_unique_id;
            }

            LOG.warn("Cannot determine cloud instance ID");
            return null;

        } catch (Exception e) {
            LOG.warn("Failed to get cloud instance ID: {}", e.getMessage());
            return null;
        }
    }
}
