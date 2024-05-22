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
import org.apache.doris.cloud.catalog.CloudEnv;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

// SET vault_name DEFAULT STORAGE VAULT
public class SetDefaultStorageVaultStmt extends DdlStmt {
    public final String vaultName;

    public SetDefaultStorageVaultStmt(String vaultName) {
        this.vaultName = vaultName;
    }

    public String getStorageVaultName() {
        return vaultName;
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

        // check auth
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }

        super.analyze(analyzer);
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("SET ").append(vaultName).append(" AS DEFAULT STORAGE VAULT");
        return sb.toString();
    }
}
