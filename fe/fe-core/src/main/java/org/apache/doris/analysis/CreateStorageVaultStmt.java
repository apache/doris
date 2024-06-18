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

package org.apache.doris.analysis;

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
import org.apache.doris.common.util.PrintableMap;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import java.util.Map;

// CREATE STORAGE VAULT vault_name
// PROPERTIES (key1 = value1, ...)
public class CreateStorageVaultStmt extends DdlStmt {
    private static final String TYPE = "type";
    private static final String SET_AS_DEFAULT = "set_as_default";

    private final boolean ifNotExists;
    private final String vaultName;
    private final Map<String, String> properties;
    private boolean setAsDefault;
    private StorageVault.StorageVaultType vaultType;

    public CreateStorageVaultStmt(boolean ifNotExists, String vaultName, Map<String, String> properties) {
        this.ifNotExists = ifNotExists;
        this.vaultName = vaultName;
        this.properties = properties;
        this.vaultType = vaultType.UNKNOWN;
    }

    public boolean isIfNotExists() {
        return ifNotExists;
    }

    public boolean setAsDefault() {
        return setAsDefault;
    }

    public String getStorageVaultName() {
        return vaultName;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public StorageVault.StorageVaultType getStorageVaultType() {
        return vaultType;
    }

    public void setStorageVaultType(StorageVault.StorageVaultType type) throws UserException {
        if (type == StorageVault.StorageVaultType.UNKNOWN) {
            throw new AnalysisException("Unsupported Storage Vault type: " + type);
        }
        this.vaultType = type;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        if (Config.isNotCloudMode()) {
            throw new AnalysisException("Storage Vault is only supported for cloud mode");
        }
        if (!FeConstants.runningUnitTest) {
            // In legacy cloud mode, some s3 back-ended storage does need to use storage vault.
            if (!((CloudEnv) Env.getCurrentEnv()).getEnableStorageVault()) {
                throw new AnalysisException("Your cloud instance doesn't support storage vault");
            }
        }
        super.analyze(analyzer);

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
        String type = properties.get(TYPE);
        if (type == null) {
            throw new AnalysisException("Storage Vault type can't be null");
        }
        setAsDefault = Boolean.parseBoolean(properties.getOrDefault(SET_AS_DEFAULT, "false"));
        setStorageVaultType(StorageVault.StorageVaultType.fromString(type));
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE ");
        sb.append("STORAGE VAULT '").append(vaultName).append("' ");
        sb.append("PROPERTIES(").append(new PrintableMap<>(properties, " = ", true, false, true)).append(")");
        return sb.toString();
    }

    @Override
    public boolean needAuditEncryption() {
        return true;
    }
}
