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
import org.apache.doris.cloud.catalog.CloudEnv;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.property.PropertyConverter;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * CREATE STORAGE VAULT vault_name
 * PROPERTIES (key1 = value1, ...)
 */
public class CreateStorageVaultCommand extends Command implements ForwardWithSync, NeedAuditEncryption {
    private static final String PATH_VERSION = "path_version";

    private static final String SHARD_NUM = "shard_num";

    private static final String SET_AS_DEFAULT = "set_as_default";

    private final boolean ifNotExists;
    private final String vaultName;
    private ImmutableMap<String, String> properties;
    private boolean setAsDefault;
    private int pathVersion = 0;
    private int numShard = 0;
    private StorageVault.StorageVaultType vaultType;

    public CreateStorageVaultCommand(boolean ifNotExists, String vaultName, Map<String, String> properties) {
        super(PlanType.CREATE_STORAGE_VAULT_COMMAND);
        this.ifNotExists = ifNotExists;
        this.vaultName = vaultName;
        this.properties = ImmutableMap.copyOf(properties);
        this.vaultType = vaultType.UNKNOWN;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate();
        Env.getCurrentEnv().getStorageVaultMgr().createStorageVaultResource(this);
    }

    /**
     * validate
     */
    public void validate() throws UserException {
        if (Config.isNotCloudMode()) {
            throw new AnalysisException("Storage Vault is only supported for cloud mode");
        }
        if (!FeConstants.runningUnitTest) {
            // In legacy cloud mode, some s3 back-ended storage does need to use storage vault.
            if (!((CloudEnv) Env.getCurrentEnv()).getEnableStorageVault()) {
                throw new AnalysisException("Your cloud instance doesn't support storage vault");
            }
        }

        // check auth
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }

        // check name
        FeNameFormat.checkStorageVaultName(vaultName);

        // check type in properties
        if (properties == null || properties.isEmpty()) {
            throw new AnalysisException("Storage Vault properties can't be null");
        }

        String type = null;
        for (Map.Entry<String, String> property : properties.entrySet()) {
            if (property.getKey().equalsIgnoreCase(StorageVault.PropertyKey.TYPE)) {
                type = property.getValue();
            }
        }
        if (type == null) {
            throw new AnalysisException("Missing property " + StorageVault.PropertyKey.TYPE);
        }
        if (type.isEmpty()) {
            throw new AnalysisException("Property " + StorageVault.PropertyKey.TYPE + " cannot be empty");
        }

        final String pathVersionString = properties.get(PATH_VERSION);
        if (pathVersionString != null) {
            this.pathVersion = Integer.parseInt(pathVersionString);
            properties.remove(PATH_VERSION);
        }
        final String numShardString = properties.get(SHARD_NUM);
        if (numShardString != null) {
            this.numShard = Integer.parseInt(numShardString);
            properties.remove(SHARD_NUM);
        }
        setAsDefault = Boolean.parseBoolean(properties.getOrDefault(SET_AS_DEFAULT, "false"));
        setStorageVaultType(StorageVault.StorageVaultType.fromString(type));

        if (vaultType == StorageVault.StorageVaultType.S3
                && !properties.containsKey(PropertyConverter.USE_PATH_STYLE)) {
            properties = ImmutableMap.<String, String>builder()
                .putAll(properties)
                .put(PropertyConverter.USE_PATH_STYLE, "true")
                .build();
        }
    }

    public void setStorageVaultType(StorageVault.StorageVaultType type) throws UserException {
        if (type == StorageVault.StorageVaultType.UNKNOWN) {
            throw new AnalysisException("Unsupported Storage Vault type: " + type);
        }
        this.vaultType = type;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitCreateStorageVaultCommand(this, context);
    }

    @Override
    public boolean needAuditEncryption() {
        return true;
    }

    public boolean isIfNotExists() {
        return ifNotExists;
    }

    public String getVaultName() {
        return vaultName;
    }

    public ImmutableMap<String, String> getProperties() {
        return properties;
    }

    public boolean isSetAsDefault() {
        return setAsDefault;
    }

    public int getPathVersion() {
        return pathVersion;
    }

    public int getNumShard() {
        return numShard;
    }

    public StorageVault.StorageVaultType getVaultType() {
        return vaultType;
    }
}
