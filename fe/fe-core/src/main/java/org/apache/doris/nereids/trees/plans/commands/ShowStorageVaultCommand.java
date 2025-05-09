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

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.StorageVault;
import org.apache.doris.cloud.catalog.CloudEnv;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.rpc.MetaServiceProxy;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.rpc.RpcException;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Show storage vault statement
 * syntax:
 * SHOW STORAGE VAULT
 **/
public class ShowStorageVaultCommand extends ShowCommand {

    public ShowStorageVaultCommand() {
        super(PlanType.SHOW_STORAGE_VAULT_COMMAND);
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate();
        return handleShowStorageVault(ctx);
    }

    private ShowResultSet handleShowStorageVault(ConnectContext ctx) throws AnalysisException {
        // [vault name, vault id, vault properties, isDefault]
        List<List<String>> rows;
        try {
            Cloud.GetObjStoreInfoResponse resp = MetaServiceProxy.getInstance()
                    .getObjStoreInfo(Cloud.GetObjStoreInfoRequest.newBuilder().build());
            Auth auth = Env.getCurrentEnv().getAuth();
            UserIdentity user = ctx.getCurrentUserIdentity();
            rows = resp.getStorageVaultList().stream()
                .filter(storageVault -> auth.checkStorageVaultPriv(user, storageVault.getName(),
                    PrivPredicate.USAGE))
                .map(StorageVault::convertToShowStorageVaultProperties)
                .collect(Collectors.toList());
            if (resp.hasDefaultStorageVaultId()) {
                StorageVault.setDefaultVaultToShowVaultResult(rows, resp.getDefaultStorageVaultId());
            }
        } catch (RpcException e) {
            throw new AnalysisException(e.getMessage());
        }
        return new ShowResultSet(showStmt.getMetaData(), rows);
    }

    /**
     * validate
     */
    public void validate() throws AnalysisException {
        if (Config.isNotCloudMode()) {
            throw new AnalysisException("Storage Vault is only supported for cloud mode");
        }
        if (!FeConstants.runningUnitTest) {
            // In legacy cloud mode, some s3 back-ended storage does need to use storage vault.
            if (!((CloudEnv) Env.getCurrentEnv()).getEnableStorageVault()) {
                throw new AnalysisException("Your cloud instance doesn't support storage vault");
            }
        }
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowStorageVaultCommand(this, context);
    }

    public ShowResultSetMetaData getMetaData() {
        return StorageVault.STORAGE_VAULT_META_DATA;
    }
}
