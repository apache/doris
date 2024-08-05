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
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.UserException;
import org.apache.doris.qe.ShowResultSetMetaData;

/**
 * Show storage vault statement
 * syntax:
 * SHOW STORAGE VAULT
 **/
public class ShowStorageVaultStmt extends ShowStmt {

    private final String stmt = "SHOW STORAGE VAULT";

    public ShowStorageVaultStmt() {
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
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
    }

    @Override
    public String toSql() {
        return this.stmt;
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return StorageVault.STORAGE_VAULT_META_DATA;
    }
}
