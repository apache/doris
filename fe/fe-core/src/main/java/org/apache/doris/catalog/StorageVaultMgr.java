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
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.proto.Cloud.AlterObjStoreInfoRequest.Operation;
import org.apache.doris.cloud.rpc.MetaServiceProxy;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;
import org.apache.doris.rpc.RpcException;

import com.google.common.annotations.VisibleForTesting;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class StorageVaultMgr {
    private static final Logger LOG = LogManager.getLogger(StorageVaultMgr.class);

    // <VaultName, VaultId>
    private Pair<String, String> defaultVaultInfo;

    private ReadWriteLock rwLock = new ReentrantReadWriteLock();

    public StorageVaultMgr() {
    }

    // TODO(ByteYue): The CreateStorageVault should only be handled by master
    // which indicates we can maintains one <VaultName, VaultId> map in FE master

    public void createStorageVaultResource(CreateStorageVaultStmt stmt) throws Exception {
        switch (stmt.getStorageVaultType()) {
            case HDFS:
                createHdfsVault(StorageVault.fromStmt(stmt));
                break;
            case S3:
                throw new DdlException("Currently S3 is not support.");
            case UNKNOWN:
            default:
                throw new DdlException("Only support S3, HDFS storage vault.");
        }
    }

    @VisibleForTesting
    public void setDefaultStorageVault(SetDefaultStorageVaultStmt stmt) throws DdlException {
        Cloud.AlterObjStoreInfoRequest.Builder builder = Cloud.AlterObjStoreInfoRequest.newBuilder();
        Cloud.StorageVaultPB.Builder vaultBuilder = Cloud.StorageVaultPB.newBuilder();
        vaultBuilder.setName(stmt.getStorageVaultName());
        builder.setHdfs(vaultBuilder.build());
        builder.setOp(Operation.SET_DEFAULT_VAULT);
        String vaultId;
        try {
            Cloud.AlterObjStoreInfoResponse resp =
                    MetaServiceProxy.getInstance().alterObjStoreInfo(builder.build());
            if (resp.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
                LOG.warn("failed to alter storage vault response: {} ", resp);
                throw new DdlException(resp.getStatus().getMsg());
            }
            vaultId = resp.getStorageVaultId();
        } catch (RpcException e) {
            LOG.warn("failed to alter storage vault due to RpcException: {}", e);
            throw new DdlException(e.getMessage());
        }
        setDefaultStorageVault(Pair.of(stmt.getStorageVaultName(), vaultId));
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
        requestBuilder.setHdfs(alterHdfsInfoBuilder.build());
        try {
            Cloud.AlterObjStoreInfoResponse response =
                    MetaServiceProxy.getInstance().alterObjStoreInfo(requestBuilder.build());
            if (response.getStatus().getCode() == Cloud.MetaServiceCode.ALREADY_EXISTED
                    && hdfsStorageVault.ifNotExists()) {
                return;
            }
            if (response.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
                LOG.warn("failed to alter storage vault response: {} ", response);
                throw new DdlException(response.getStatus().getMsg());
            }
        } catch (RpcException e) {
            LOG.warn("failed to alter storage vault due to RpcException: {}", e);
            throw new DdlException(e.getMessage());
        }
    }
}
