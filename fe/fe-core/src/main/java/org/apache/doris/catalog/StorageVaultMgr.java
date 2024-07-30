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

import org.apache.doris.analysis.CreateStorageVaultStmt;
import org.apache.doris.analysis.SetDefaultStorageVaultStmt;
import org.apache.doris.catalog.StorageVault.StorageVaultType;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.proto.Cloud.AlterObjStoreInfoRequest.Operation;
import org.apache.doris.cloud.rpc.MetaServiceProxy;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.property.constants.S3Properties;
import org.apache.doris.proto.InternalService.PAlterVaultSyncRequest;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TNetworkAddress;

import com.google.common.annotations.VisibleForTesting;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class StorageVaultMgr {
    private static final Logger LOG = LogManager.getLogger(StorageVaultMgr.class);

    // <VaultName, VaultId>
    private Pair<String, String> defaultVaultInfo;

    private Map<String, String> vaultNameToVaultId = new HashMap<>();

    private ReadWriteLock rwLock = new ReentrantReadWriteLock();

    private static final ExecutorService ALTER_BE_SYNC_THREAD_POOL = Executors.newFixedThreadPool(1);

    private final SystemInfoService systemInfoService;

    public StorageVaultMgr(SystemInfoService systemInfoService) {
        this.systemInfoService = systemInfoService;
    }

    public void createStorageVaultResource(CreateStorageVaultStmt stmt) throws Exception {
        switch (stmt.getStorageVaultType()) {
            case HDFS:
                createHdfsVault(StorageVault.fromStmt(stmt));
                break;
            case S3:
                createS3Vault(StorageVault.fromStmt(stmt));
                break;
            case UNKNOWN:
            default:
                throw new DdlException("Only support S3, HDFS storage vault.");
        }
        // Make BE eagerly fetch the storage vault info from Meta Service
        ALTER_BE_SYNC_THREAD_POOL.execute(() -> alterSyncVaultTask());
    }

    public void refreshVaultMap(Map<String, String> vaultMap) {
        rwLock.writeLock().lock();
        vaultNameToVaultId = vaultMap;
        rwLock.writeLock().unlock();
    }

    public String getVaultIdByName(String name) {
        String vaultId;
        rwLock.readLock().lock();
        vaultId = vaultNameToVaultId.getOrDefault(name, "");
        rwLock.readLock().unlock();
        return vaultId;
    }

    public void alterStorageVault(StorageVaultType type, Map<String, String> properties, String name) throws Exception {
        if (type != StorageVaultType.S3) {
            throw new DdlException("Only support alter s3 storage vault");
        }
        properties.keySet().stream()
                .filter(key -> !S3StorageVault.ALTER_CHECK_PROPERTIES.contains(key))
                .findAny()
                .ifPresent(key -> {
                    throw new IllegalArgumentException("Alter property " + key + " is not allowed.");
                });
        Cloud.AlterObjStoreInfoRequest.Builder requestBuilder
                = Cloud.AlterObjStoreInfoRequest.newBuilder();
        requestBuilder.setOp(Cloud.AlterObjStoreInfoRequest.Operation.ALTER_S3_VAULT);
        Cloud.ObjectStoreInfoPB.Builder objBuilder = S3Properties.getObjStoreInfoPB(properties);
        Cloud.StorageVaultPB.Builder alterObjVaultBuilder = Cloud.StorageVaultPB.newBuilder();
        alterObjVaultBuilder.setName(name);
        alterObjVaultBuilder.setObjInfo(objBuilder.build());
        requestBuilder.setVault(alterObjVaultBuilder.build());
        try {
            Cloud.AlterObjStoreInfoResponse response =
                    MetaServiceProxy.getInstance().alterStorageVault(requestBuilder.build());
            if (response.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
                LOG.warn("failed to alter storage vault response: {} ", response);
                throw new DdlException(response.getStatus().getMsg());
            }
            LOG.info("Succeed to alter s3 vault {}, id {}, origin default vault replaced {}",
                    name, response.getStorageVaultId(), response.getDefaultStorageVaultReplaced());
        } catch (RpcException e) {
            LOG.warn("failed to alter storage vault due to RpcException: {}", e);
            throw new DdlException(e.getMessage());
        }
    }

    @VisibleForTesting
    public void setDefaultStorageVault(SetDefaultStorageVaultStmt stmt) throws DdlException {
        Cloud.AlterObjStoreInfoRequest.Builder builder = Cloud.AlterObjStoreInfoRequest.newBuilder();
        Cloud.StorageVaultPB.Builder vaultBuilder = Cloud.StorageVaultPB.newBuilder();
        vaultBuilder.setName(stmt.getStorageVaultName());
        builder.setVault(vaultBuilder.build());
        builder.setOp(Operation.SET_DEFAULT_VAULT);
        String vaultId;
        LOG.info("try to set vault {} as default vault", stmt.getStorageVaultName());
        try {
            Cloud.AlterObjStoreInfoResponse resp =
                    MetaServiceProxy.getInstance().alterStorageVault(builder.build());
            if (resp.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
                LOG.warn("failed to set default storage vault response: {}, vault name {}",
                        resp, stmt.getStorageVaultName());
                throw new DdlException(resp.getStatus().getMsg());
            }
            vaultId = resp.getStorageVaultId();
        } catch (RpcException e) {
            LOG.warn("failed to set default storage vault due to RpcException: {}, vault name {}",
                    e, stmt.getStorageVaultName());
            throw new DdlException(e.getMessage());
        }
        LOG.info("succeed to set {} as default vault, vault id {}", stmt.getStorageVaultName(), vaultId);
        setDefaultStorageVault(Pair.of(stmt.getStorageVaultName(), vaultId));
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
        defaultVaultInfo = null;
    }

    public void setDefaultStorageVault(Pair<String, String> vaultInfo) {
        try {
            rwLock.writeLock().lock();
            defaultVaultInfo = vaultInfo;
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public Pair getDefaultStorageVaultInfo() {
        Pair vault = null;
        try {
            rwLock.readLock().lock();
            if (defaultVaultInfo != null) {
                vault = defaultVaultInfo;
            }
        } finally {
            rwLock.readLock().unlock();
        }
        return vault;
    }

    @VisibleForTesting
    public void createHdfsVault(StorageVault vault) throws DdlException {
        HdfsStorageVault hdfsStorageVault = (HdfsStorageVault) vault;
        Cloud.HdfsVaultInfo hdfsInfos = HdfsStorageVault.generateHdfsParam(hdfsStorageVault.getCopiedProperties());
        Cloud.StorageVaultPB.Builder alterHdfsInfoBuilder = Cloud.StorageVaultPB.newBuilder();
        alterHdfsInfoBuilder.setName(hdfsStorageVault.getName());
        alterHdfsInfoBuilder.setHdfsInfo(hdfsInfos);
        Cloud.AlterObjStoreInfoRequest.Builder requestBuilder
                = Cloud.AlterObjStoreInfoRequest.newBuilder();
        requestBuilder.setOp(Cloud.AlterObjStoreInfoRequest.Operation.ADD_HDFS_INFO);
        requestBuilder.setVault(alterHdfsInfoBuilder.build());
        requestBuilder.setSetAsDefaultStorageVault(vault.setAsDefault());
        try {
            Cloud.AlterObjStoreInfoResponse response =
                    MetaServiceProxy.getInstance().alterStorageVault(requestBuilder.build());
            if (response.getStatus().getCode() == Cloud.MetaServiceCode.ALREADY_EXISTED
                    && hdfsStorageVault.ifNotExists()) {
                LOG.info("Hdfs vault {} already existed", hdfsStorageVault.getName());
                return;
            }
            if (response.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
                LOG.warn("failed to create hdfs storage vault, vault name {}, response: {} ",
                        hdfsStorageVault.getName(), response);
                throw new DdlException(response.getStatus().getMsg());
            }
            rwLock.writeLock().lock();
            vaultNameToVaultId.put(hdfsStorageVault.getName(), response.getStorageVaultId());
            rwLock.writeLock().unlock();
            LOG.info("Succeed to create hdfs vault {}, id {}, origin default vault replaced {}",
                    hdfsStorageVault.getName(), response.getStorageVaultId(),
                            response.getDefaultStorageVaultReplaced());
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

    public void createS3Vault(StorageVault vault) throws DdlException {
        S3StorageVault s3StorageVault = (S3StorageVault) vault;
        Cloud.AlterObjStoreInfoRequest.Builder requestBuilder
                = Cloud.AlterObjStoreInfoRequest.newBuilder();
        requestBuilder.setOp(Cloud.AlterObjStoreInfoRequest.Operation.ADD_S3_VAULT);
        Cloud.ObjectStoreInfoPB.Builder objBuilder = S3Properties.getObjStoreInfoPB(vault.getCopiedProperties());
        Cloud.StorageVaultPB.Builder alterObjVaultBuilder = Cloud.StorageVaultPB.newBuilder();
        alterObjVaultBuilder.setName(s3StorageVault.getName());
        alterObjVaultBuilder.setObjInfo(objBuilder.build());
        requestBuilder.setVault(alterObjVaultBuilder.build());
        requestBuilder.setSetAsDefaultStorageVault(vault.setAsDefault());
        try {
            Cloud.AlterObjStoreInfoResponse response =
                    MetaServiceProxy.getInstance().alterStorageVault(requestBuilder.build());
            if (response.getStatus().getCode() == Cloud.MetaServiceCode.ALREADY_EXISTED
                    && s3StorageVault.ifNotExists()) {
                LOG.info("S3 vault {} already existed", s3StorageVault.getName());
                return;
            }
            if (response.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
                LOG.warn("failed to alter storage vault response: {} ", response);
                throw new DdlException(response.getStatus().getMsg());
            }
            rwLock.writeLock().lock();
            vaultNameToVaultId.put(s3StorageVault.getName(), response.getStorageVaultId());
            rwLock.writeLock().unlock();
            LOG.info("Succeed to create s3 vault {}, id {}, origin default vault replaced {}",
                    s3StorageVault.getName(), response.getStorageVaultId(), response.getDefaultStorageVaultReplaced());
        } catch (RpcException e) {
            LOG.warn("failed to alter storage vault due to RpcException: {}", e);
            throw new DdlException(e.getMessage());
        }
    }
}
