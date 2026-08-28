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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.StorageVault;
import org.apache.doris.catalog.StorageVault.StorageVaultType;
import org.apache.doris.catalog.StorageVaultMgr;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.Pair;
import org.apache.doris.datasource.storage.S3ResourceCompat;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.base.Preconditions;

import java.util.Map;

/**
 * Alter Storage Vault command
 */
public class AlterStorageVaultCommand extends Command implements NeedAuditEncryption, ForwardWithSync {
    private static final String TYPE = "type";
    private final Map<String, String> properties;
    private final String name;

    public AlterStorageVaultCommand(String name, final Map<String, String> properties) {
        super(PlanType.ALTER_STORAGE_VAULT);
        this.name = name;
        this.properties = properties;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        // check auth
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }

        StorageVaultMgr storageVaultMgr = Env.getCurrentEnv().getStorageVaultMgr();
        StorageVault.StorageVaultType vaultType;
        if (properties.containsKey(TYPE)) {
            vaultType = StorageVaultType.fromString(properties.get(TYPE));
            if (vaultType == StorageVaultType.UNKNOWN) {
                throw new AnalysisException("Unsupported Storage Vault type: " + type);
            }
        } else {
            // auto-detect
            try {
                vaultType = storageVaultMgr.getStorageVaultTypeByName(name);
                if (vaultType == StorageVaultType.UNKNOWN) {
                    throw new AnalysisException("Storage vault '" + name + "' does not exist or has unknown type. "
                            + "You can use `SHOW STORAGE VAULT` to get all available vaults.");
                }
            } catch (DdlException e) {
                throw new AnalysisException("Failed to get storage vault type: " + e.getMessage());
            }
        }

        FeNameFormat.checkStorageVaultName(name);
        if (properties.containsKey(StorageVault.PropertyKey.VAULT_NAME)) {
            String newVaultName = properties.get(StorageVault.PropertyKey.VAULT_NAME);
            Pair<String, String> info = Env.getCurrentEnv().getStorageVaultMgr().getDefaultStorageVault();
            if (info != null && name.equalsIgnoreCase(info.first)) {
                throw new AnalysisException("Cannot rename default storage vault. Before rename it, you should execute"
                        + " `UNSET DEFAULT STORAGE VAULT` sql to unset default storage vault. After rename it, you can"
                        + " execute `SET " + newVaultName + " AS DEFAULT STORAGE VAULT` sql to set it as default"
                        + " storage vault.");
            }
            String newName = properties.get(StorageVault.PropertyKey.VAULT_NAME);
            FeNameFormat.checkStorageVaultName(newName);
            Preconditions.checkArgument(!name.equalsIgnoreCase(newName), "Vault name has not been changed");
        }
        boolean altersPathStyle = properties.containsKey(S3ResourceCompat.USE_PATH_STYLE);
        boolean altersCredentialsProvider = properties.containsKey(S3ResourceCompat.CREDENTIALS_PROVIDER_TYPE);
        if (vaultType == StorageVaultType.S3 && (altersPathStyle || altersCredentialsProvider)
                && storageVaultMgr.isS3ExpressStorageVault(name)) {
            if (altersPathStyle
                    && !"false".equalsIgnoreCase(properties.get(S3ResourceCompat.USE_PATH_STYLE))) {
                throw new AnalysisException("S3 Express requires use_path_style=false");
            }
            if (altersCredentialsProvider
                    && "ANONYMOUS".equalsIgnoreCase(
                            properties.get(S3ResourceCompat.CREDENTIALS_PROVIDER_TYPE).trim())) {
                throw new AnalysisException("S3 Express does not support anonymous access");
            }
        }
        storageVaultMgr.alterStorageVault(vaultType, properties, name);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitCommand(this, context);
    }

    @Override
    public boolean needAuditEncryption() {
        return true;
    }
}
