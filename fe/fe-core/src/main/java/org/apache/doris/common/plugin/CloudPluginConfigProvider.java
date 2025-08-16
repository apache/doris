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
import org.apache.doris.rpc.RpcException;

import com.google.common.base.Strings;

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

    /**
     * Get S3 configuration from cloud mode
     *
     * @return S3 config object
     * @throws RuntimeException if configuration retrieval fails
     */
    public static S3PluginDownloader.S3Config getCloudS3Config() {
        Cloud.ObjectStoreInfoPB objInfo = getDefaultStorageVaultInfo();

        if (Strings.isNullOrEmpty(objInfo.getBucket())
                || Strings.isNullOrEmpty(objInfo.getAk())
                || Strings.isNullOrEmpty(objInfo.getSk())) {
            throw new RuntimeException("Incomplete S3 configuration: bucket=" + objInfo.getBucket()
                    + ", ak=" + (Strings.isNullOrEmpty(objInfo.getAk()) ? "empty" : "***")
                    + ", sk=" + (Strings.isNullOrEmpty(objInfo.getSk()) ? "empty" : "***"));
        }

        return new S3PluginDownloader.S3Config(
                objInfo.getEndpoint(),
                objInfo.getRegion(),
                objInfo.getBucket(),
                objInfo.getAk(),
                objInfo.getSk()
        );
    }

    /**
     * Get cloud instance ID - exposed for direct path construction
     *
     * @return cloud instance ID
     */
    public static String getCloudInstanceId() {
        // Get from CloudEnv first
        if (Env.getCurrentEnv() instanceof CloudEnv) {
            String instanceId = ((CloudEnv) Env.getCurrentEnv()).getCloudInstanceId();
            if (!Strings.isNullOrEmpty(instanceId)) {
                return instanceId;
            } else {
                throw new RuntimeException("CloudEnv instance ID is null or empty");
            }
        } else {
            throw new RuntimeException("CloudEnv instance ID is null or empty");
        }
    }

    // ======================== Private Helper Methods ========================

    /**
     * Get default storage configuration info - using StorageVaultMgr
     * This method is copied from the working version you provided, more stable and reliable
     */
    private static Cloud.ObjectStoreInfoPB getDefaultStorageVaultInfo() {
        // Get default storage vault info from StorageVaultMgr
        Pair<String, String> defaultVault = Env.getCurrentEnv().getStorageVaultMgr().getDefaultStorageVault();
        if (defaultVault == null) {
            throw new RuntimeException("No default storage vault configured for plugin downloader");
        }

        String vaultName = defaultVault.first;

        // Use the dedicated RPC to get storage vault object store information
        Cloud.GetObjStoreInfoRequest request = Cloud.GetObjStoreInfoRequest.newBuilder()
                .setCloudUniqueId(Config.cloud_unique_id)
                .build();

        Cloud.GetObjStoreInfoResponse response;
        try {
            response = MetaServiceProxy.getInstance().getObjStoreInfo(request);
        } catch (RpcException e) {
            throw new RuntimeException("Failed to get storage vault info: " + e.getMessage());
        }

        if (response.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
            throw new RuntimeException("Failed to get storage vault info: " + response.getStatus().getMsg());
        }

        // Find the default storage vault in the response
        for (Cloud.StorageVaultPB vault : response.getStorageVaultList()) {
            if (vaultName.equals(vault.getName())) {
                if (vault.hasObjInfo()) {
                    return vault.getObjInfo();
                } else {
                    throw new RuntimeException("Storage vault " + vaultName + " does not have object store info");
                }
            }
        }

        // If not found by name, try to use the first available vault
        if (!response.getStorageVaultList().isEmpty()) {
            Cloud.StorageVaultPB firstVault = response.getStorageVaultList().get(0);
            if (firstVault.hasObjInfo()) {
                return firstVault.getObjInfo();
            }
        }

        throw new RuntimeException("No suitable storage vault found for plugin download");
    }
}
