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
import org.apache.doris.cloud.catalog.CloudEnv;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.base.Strings;

/**
 * Alter System Rename Compute Group
 */
public class AlterSystemRenameComputeGroupCommand extends Command implements ForwardWithSync {
    private final String originalName;
    private final String newName;

    public AlterSystemRenameComputeGroupCommand(String originalName, String newName) {
        super(PlanType.ALTER_SYSTEM_RENAME_COMPUTE_GROUP);
        this.originalName = originalName;
        this.newName = newName;
    }

    private void validate() throws AnalysisException {
        // check admin or root auth, can rename
        if (!Env.getCurrentEnv().getAccessManager()
                .checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN_OR_NODE)) {
            String message = ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR.formatErrorMsg(
                    PrivPredicate.ADMIN_OR_NODE.getPrivs().toString());
            throw new org.apache.doris.nereids.exceptions.AnalysisException(message);
        }
        if (Strings.isNullOrEmpty(originalName) || Strings.isNullOrEmpty(newName)) {
            throw new AnalysisException("rename group requires non-empty or non-empty name");
        }
        if (originalName.equals(newName)) {
            throw new AnalysisException("rename compute group original name eq new name");
        }
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate();
        doRun(ctx);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitCommand(this, context);
    }

    private void doRun(ConnectContext ctx) throws Exception {
        try {
            // 1. send rename rpc to ms
            ((CloudSystemInfoService) Env.getCurrentSystemInfo()).renameComputeGroup(this.originalName, this.newName);
            // 2. if 1 not throw exception, refresh cloud cluster
            // if not do 2, will wait 10s to get new name
            ((CloudEnv) Env.getCurrentEnv()).getCloudClusterChecker().checkNow();
        } catch (Exception e) {
            throw new DdlException(e.getMessage());
        }
    }
}
