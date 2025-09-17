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

package org.apache.doris.catalog;

import org.apache.doris.catalog.StorageVault.StorageVaultType;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.proto.Cloud.AlterObjStoreInfoRequest.Operation;
import org.apache.doris.cloud.proto.Cloud.CredProviderTypePB;
import org.apache.doris.cloud.proto.Cloud.ObjectStoreInfoPB.Provider;
import org.apache.doris.cloud.rpc.MetaServiceProxy;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.lock.MonitoredReentrantReadWriteLock;
import org.apache.doris.datasource.property.storage.S3Properties;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.nereids.trees.plans.commands.CreateStorageVaultCommand;
import org.apache.doris.proto.InternalService.PAlterVaultSyncRequest;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TNetworkAddress;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class StorageVaultMgr {
    private static final Logger LOG = LogManager.getLogger(StorageVaultMgr.class);
    private static final ExecutorService ALTER_BE_SYNC_THREAD_POOL = Executors.newFixedThreadPool(1);
    private final SystemInfoService systemInfoService;

    public static final String S3_ROOT_PATH = "s3.root.path";
    // <VaultName, VaultId>
    private Pair<String, String> defaultVaultInfo;
    private Map<String, String> vaultNameToVaultId = new HashMap<>();
    private MonitoredReentrantReadWriteLock rwLock = new MonitoredReentrantReadWriteLock();

    public StorageVaultMgr(SystemInfoService systemInfoService) {
        this.systemInfoService = systemInfoService;
    }

    public void createStorageVaultResource(CreateStorageVaultCommand command) throws Exception {
        switch (command.getVaultType()) {
            case HDFS:
                createHdfsVault(StorageVault.fromCommand(command));
                break;
            case S3:
                createS3Vault(StorageVault.fromCommand(command));
                break;
            case UNKNOWN:
            default:
                throw new DdlException("Only support S3, HDFS storage vault.");
        }
        // Make BE eagerly fetch the storage vault info from Meta Service
        ALTER_BE_SYNC_THREAD_POOL.execute(() -> alterSyncVaultTask());
    }

    public void refreshVaultMap(Map<String, String> vaultMap, Pair<String, String> defaultVault) {
        try {
            rwLock.writeLock().lock();
            vaultNameToVaultId = vaultMap;
            defaultVaultInfo = defaultVault;
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public String getVaultIdByName(String vaultName) {
        try {
            rwLock.readLock().lock();
            return vaultNameToVaultId.getOrDefault(vaultName, "");
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public String getVaultNameById(String vaultId) {
        try {
            rwLock.readLock().lock();
            for (Map.Entry<String, String> entry : vaultNameToVaultId.entrySet()) {
                if (entry.getValue().equals(vaultId)) {
                    return entry.getKey();
                }
            }
            return "";
        } finally {
            rwLock.readLock().unlock();
        }
    }

    private void addStorageVaultToCache(String vaultName, String vaultId, boolean defaultVault) {
        try {
            rwLock.writeLock().lock();
            vaultNameToVaultId.put(vaultName, vaultId);
            if (defaultVault) {
                defaultVaultInfo = Pair.of(vaultName, vaultId);
            }
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    private void updateStorageVaultCache(String oldVaultName, String newVaultName, String vaultId) {
        try {
            rwLock.writeLock().lock();
            String cachedVaultId = vaultNameToVaultId.get(oldVaultName);
            vaultNameToVaultId.remove(oldVaultName);
            Preconditions.checkArgument(!Strings.isNullOrEmpty(cachedVaultId),
                    "Cached vault id %s is null or empty", cachedVaultId);
            Preconditions.checkArgument(cachedVaultId.equals(vaultId),
                    "Cached vault id not equal to remote storage. %s vs %s", cachedVaultId, vaultId);
            vaultNameToVaultId.put(newVaultName, vaultId);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    private void updateDefaultStorageVaultCache(Pair<String, String> newDefaultVaultInfo) {
        try {
            rwLock.writeLock().lock();
            defaultVaultInfo = newDefaultVaultInfo;
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    private Cloud.StorageVaultPB.Builder buildAlterS3VaultRequest(Map<String, String> properties, String name)
            throws Exception {
        Cloud.ObjectStoreInfoPB.Builder objBuilder = getObjStoreInfoPB(properties);
        Cloud.StorageVaultPB.Builder alterObjVaultBuilder = Cloud.StorageVaultPB.newBuilder();
        alterObjVaultBuilder.setName(name);
        alterObjVaultBuilder.setObjInfo(objBuilder.build());
        if (properties.containsKey(StorageVault.PropertyKey.VAULT_NAME)) {
            alterObjVaultBuilder.setAlterName(properties.get(StorageVault.PropertyKey.VAULT_NAME));
        }
        return alterObjVaultBuilder;
    }

    private static Cloud.ObjectStoreInfoPB.Builder getObjStoreInfoPB(Map<String, String> properties) {
        Cloud.ObjectStoreInfoPB.Builder builder = Cloud.ObjectStoreInfoPB.newBuilder();
        if (properties.containsKey(S3Properties.ENDPOINT)) {
            builder.setEndpoint(properties.get(S3Properties.ENDPOINT));
        }
        if (properties.containsKey(S3Properties.REGION)) {
            builder.setRegion(properties.get(S3Properties.REGION));
        }
        if (properties.containsKey(S3Properties.ACCESS_KEY)) {
            builder.setAk(properties.get(S3Properties.ACCESS_KEY));
        }
        if (properties.containsKey(S3Properties.SECRET_KEY)) {
            builder.setSk(properties.get(S3Properties.SECRET_KEY));
        }
        if (properties.containsKey(S3_ROOT_PATH)) {
            Preconditions.checkArgument(!Strings.isNullOrEmpty(properties.get(S3_ROOT_PATH)),
                    "%s cannot be empty", S3_ROOT_PATH);
            builder.setPrefix(properties.get(S3_ROOT_PATH));
        }
        if (properties.containsKey(S3Properties.BUCKET)) {
            builder.setBucket(properties.get(S3Properties.BUCKET));
        }
        if (properties.containsKey(S3Properties.EXTERNAL_ENDPOINT)) {
            builder.setExternalEndpoint(properties.get(S3Properties.EXTERNAL_ENDPOINT));
        }
        if (properties.containsKey(StorageProperties.FS_PROVIDER_KEY)) {
            // S3 Provider properties should be case insensitive.
            builder.setProvider(Provider.valueOf(properties.get(StorageProperties.FS_PROVIDER_KEY).toUpperCase()));
        }

        if (properties.containsKey(S3Properties.USE_PATH_STYLE)) {
            String value = properties.get(S3Properties.USE_PATH_STYLE);
            Preconditions.checkArgument(!Strings.isNullOrEmpty(value), "use_path_style cannot be empty");
            Preconditions.checkArgument(value.equalsIgnoreCase("true")
                            || value.equalsIgnoreCase("false"),
                    "Invalid use_path_style value: %s only 'true' or 'false' is acceptable", value);
            builder.setUsePathStyle(value.equalsIgnoreCase("true"));
        }

        if (properties.containsKey(S3Properties.ROLE_ARN)) {
            builder.setRoleArn(properties.get(S3Properties.ROLE_ARN));
            if (properties.containsKey(S3Properties.EXTERNAL_ID)) {
                builder.setExternalId(properties.get(S3Properties.EXTERNAL_ID));
            }
            builder.setCredProviderType(CredProviderTypePB.INSTANCE_PROFILE);
        }

        return builder;
    }

    private Cloud.StorageVaultPB.Builder buildAlterHdfsVaultRequest(Map<String, String> properties, String name)
            throws Exception {
        Cloud.HdfsVaultInfo hdfsInfos = HdfsStorageVault.generateHdfsParam(properties);
        Cloud.StorageVaultPB.Builder alterHdfsInfoBuilder = Cloud.StorageVaultPB.newBuilder();
        alterHdfsInfoBuilder.setName(name);
        alterHdfsInfoBuilder.setHdfsInfo(hdfsInfos);
        if (properties.containsKey(StorageVault.PropertyKey.VAULT_NAME)) {
            alterHdfsInfoBuilder.setAlterName(properties.get(StorageVault.PropertyKey.VAULT_NAME));
        }
        return alterHdfsInfoBuilder;
    }

    private Cloud.StorageVaultPB.Builder buildAlterStorageVaultRequest(StorageVaultType type,
            Map<String, String> properties, String name) throws Exception {
        Cloud.StorageVaultPB.Builder builder;
        if (type == StorageVaultType.S3) {
            builder = buildAlterS3VaultRequest(properties, name);
        } else if (type == StorageVaultType.HDFS) {
            builder = buildAlterHdfsVaultRequest(properties, name);
        } else {
            throw new DdlException("Unknown storage vault type");
        }
        return builder;
    }

    private Cloud.StorageVaultPB.Builder buildAlterStorageVaultRequest(StorageVault vault) throws Exception {
        Cloud.StorageVaultPB.Builder builder = buildAlterStorageVaultRequest(vault.getType(),
                vault.getCopiedProperties(), vault.getName());
        Cloud.StorageVaultPB.PathFormat.Builder pathBuilder = Cloud.StorageVaultPB.PathFormat.newBuilder();
        pathBuilder.setShardNum(vault.getNumShard());
        pathBuilder.setPathVersion(vault.getPathVersion());
        builder.setPathFormat(pathBuilder);
        return builder;
    }

    public void alterStorageVault(StorageVaultType type, Map<String, String> properties, String name) throws Exception {
        if (type == StorageVaultType.UNKNOWN) {
            throw new DdlException("Unknown storage vault type");
        }
        try {
            Cloud.AlterObjStoreInfoRequest.Builder request = Cloud.AlterObjStoreInfoRequest.newBuilder();
            if (type == StorageVaultType.S3) {
                properties.keySet().stream()
                        .filter(key -> !S3StorageVault.ALLOW_ALTER_PROPERTIES.contains(key))
                        .findAny()
                        .ifPresent(key -> {
                            throw new IllegalArgumentException("Alter property " + key + " is not allowed.");
                        });
                request.setOp(Operation.ALTER_S3_VAULT);
            } else if (type == StorageVaultType.HDFS) {
                properties.keySet().stream()
                        .filter(key -> HdfsStorageVault.FORBID_ALTER_PROPERTIES.contains(key)
                                || key.toLowerCase().contains(S3Properties.S3_PREFIX)
                                || key.toLowerCase().contains(StorageProperties.FS_PROVIDER_KEY))
                        .findAny()
                        .ifPresent(key -> {
                            throw new IllegalArgumentException("Alter property " + key + " is not allowed.");
                        });
                request.setOp(Operation.ALTER_HDFS_VAULT);
            }
            Cloud.StorageVaultPB.Builder vaultBuilder = buildAlterStorageVaultRequest(type, properties, name);
            request.setVault(vaultBuilder);
            Cloud.AlterObjStoreInfoResponse response =
                    MetaServiceProxy.getInstance().alterStorageVault(request.build());
            if (response.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
                LOG.warn("failed to alter storage vault response: {} ", response);
                throw new DdlException(response.getStatus().getMsg());
            }

            if (request.hasVault() && request.getVault().hasAlterName()) {
                updateStorageVaultCache(name, request.getVault().getAlterName(), response.getStorageVaultId());
                LOG.info("Succeed to alter storage vault, old name:{} new name: {} id:{}", name,
                        request.getVault().getAlterName(), response.getStorageVaultId());
            }

            // Make BE eagerly fetch the storage vault info from Meta Service
            ALTER_BE_SYNC_THREAD_POOL.execute(() -> alterSyncVaultTask());
        } catch (RpcException e) {
            LOG.warn("failed to alter storage vault due to RpcException: {}", e);
            throw new DdlException(e.getMessage());
        }
    }

    public void setDefaultStorageVault(String vaultName) throws DdlException {
        Cloud.AlterObjStoreInfoRequest.Builder builder = Cloud.AlterObjStoreInfoRequest.newBuilder();
        Cloud.StorageVaultPB.Builder vaultBuilder = Cloud.StorageVaultPB.newBuilder();
        vaultBuilder.setName(vaultName);
        builder.setVault(vaultBuilder.build());
        builder.setOp(Operation.SET_DEFAULT_VAULT);
        String vaultId;
        LOG.info("try to set vault {} as default vault", vaultName);
        try {
            Cloud.AlterObjStoreInfoResponse resp =
                    MetaServiceProxy.getInstance().alterStorageVault(builder.build());
            if (resp.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
                LOG.warn("failed to set default storage vault response: {}, vault name {}",
                        resp, vaultName);
                throw new DdlException(resp.getStatus().getMsg());
            }
            vaultId = resp.getStorageVaultId();
        } catch (RpcException e) {
            LOG.warn("failed to set default storage vault due to RpcException: {}, vault name {}",
                    e, vaultName);
            throw new DdlException(e.getMessage());
        }
        LOG.info("succeed to set {} as default vault, vault id {}", vaultName, vaultId);
        updateDefaultStorageVaultCache(Pair.of(vaultName, vaultId));
    }

    public void unsetDefaultStorageVault() throws DdlException {
        Cloud.AlterObjStoreInfoRequest.Builder builder = Cloud.AlterObjStoreInfoRequest.newBuilder();
        builder.setOp(Operation.UNSET_DEFAULT_VAULT);
        try {
            Cloud.AlterObjStoreInfoResponse resp =
                    MetaServiceProxy.getInstance().alterStorageVault(builder.build());
            if (resp.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
                LOG.warn("failed to unset default storage vault");
                throw new DdlException(resp.getStatus().getMsg());
            }
        } catch (RpcException e) {
            LOG.warn("failed to unset default storage vault");
            throw new DdlException(e.getMessage());
        }
        updateDefaultStorageVaultCache(null);
    }

    public Pair<String, String> getDefaultStorageVault() {
        try {
            rwLock.readLock().lock();
            return defaultVaultInfo;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public StorageVaultType getStorageVaultTypeByName(String vaultName) throws DdlException {
        try {
            Cloud.GetObjStoreInfoResponse resp = MetaServiceProxy.getInstance()
                    .getObjStoreInfo(Cloud.GetObjStoreInfoRequest.newBuilder().build());

            for (Cloud.StorageVaultPB vault : resp.getStorageVaultList()) {
                if (vault.getName().equals(vaultName)) {
                    if (vault.hasHdfsInfo()) {
                        return StorageVaultType.HDFS;
                    } else if (vault.hasObjInfo()) {
                        return StorageVaultType.S3;
                    }
                }
            }
            return StorageVaultType.UNKNOWN;
        } catch (RpcException e) {
            LOG.warn("failed to get storage vault type due to RpcException: {}", e);
            throw new DdlException(e.getMessage());
        }
    }

    @VisibleForTesting
    public void createHdfsVault(StorageVault vault) throws Exception {
        Cloud.StorageVaultPB.Builder alterHdfsInfoBuilder = buildAlterStorageVaultRequest(vault);
        Cloud.AlterObjStoreInfoRequest.Builder requestBuilder
                = Cloud.AlterObjStoreInfoRequest.newBuilder();
        requestBuilder.setOp(Cloud.AlterObjStoreInfoRequest.Operation.ADD_HDFS_INFO);
        requestBuilder.setVault(alterHdfsInfoBuilder.build());
        requestBuilder.setSetAsDefaultStorageVault(vault.setAsDefault());
        try {
            Cloud.AlterObjStoreInfoResponse response =
                    MetaServiceProxy.getInstance().alterStorageVault(requestBuilder.build());
            if (response.getStatus().getCode() == Cloud.MetaServiceCode.ALREADY_EXISTED
                    && vault.ifNotExists()) {
                LOG.info("Hdfs vault {} already existed", vault.getName());
                return;
            }
            if (response.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
                LOG.warn("failed to create hdfs storage vault, vault name {}, response: {} ",
                        vault.getName(), response);
                throw new DdlException(response.getStatus().getMsg());
            }

            LOG.info("Succeed to create hdfs vault {}, id {}, origin default vault replaced {}",
                    vault.getName(), response.getStorageVaultId(),
                    response.getDefaultStorageVaultReplaced());
            addStorageVaultToCache(vault.getName(), response.getStorageVaultId(), vault.setAsDefault());
        } catch (RpcException e) {
            LOG.warn("failed to alter storage vault due to RpcException: {}", e);
            throw new DdlException(e.getMessage());
        }
    }

    private void alterSyncVaultTask() {
        List<Backend> bes;
        try {
            // get system all backends
            bes = systemInfoService.getAllBackendsByAllCluster().values().asList();
        } catch (UserException e) {
            LOG.warn("failed to get current cluster backends: {}", e);
            return;
        }
        bes.forEach(backend -> {
            TNetworkAddress address = backend.getBrpcAddress();
            try {
                BackendServiceProxy.getInstance().alterVaultSync(address, PAlterVaultSyncRequest.newBuilder().build());
            } catch (RpcException e) {
                LOG.warn("failed to alter sync vault");
            }
        });
    }

    public void createS3Vault(StorageVault vault) throws Exception {
        Cloud.StorageVaultPB.Builder s3StorageVaultBuilder = buildAlterStorageVaultRequest(vault);
        Cloud.AlterObjStoreInfoRequest.Builder requestBuilder
                = Cloud.AlterObjStoreInfoRequest.newBuilder();
        requestBuilder.setOp(Cloud.AlterObjStoreInfoRequest.Operation.ADD_S3_VAULT);
        requestBuilder.setVault(s3StorageVaultBuilder);
        requestBuilder.setSetAsDefaultStorageVault(vault.setAsDefault());
        try {
            Cloud.AlterObjStoreInfoResponse response =
                    MetaServiceProxy.getInstance().alterStorageVault(requestBuilder.build());
            if (response.getStatus().getCode() == Cloud.MetaServiceCode.ALREADY_EXISTED
                    && vault.ifNotExists()) {
                LOG.info("S3 vault {} already existed", vault.getName());
                return;
            }
            if (response.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
                LOG.warn("failed to alter storage vault response: {} ", response);
                throw new DdlException(response.getStatus().getMsg());
            }

            LOG.info("Succeed to create s3 vault {}, id {}, origin default vault replaced {}",
                    vault.getName(), response.getStorageVaultId(), response.getDefaultStorageVaultReplaced());
            addStorageVaultToCache(vault.getName(), response.getStorageVaultId(), vault.setAsDefault());
        } catch (RpcException e) {
            LOG.warn("failed to alter storage vault due to RpcException: {}", e);
            throw new DdlException(e.getMessage());
        }
    }
}
