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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

import java.util.List;

/**
 * Represents the command for SHOW STORAGE POLICY.
 */
public class ShowStoragePolicyCommand extends ShowCommand {
    private static final ShowResultSetMetaData USING_META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("PolicyName", ScalarType.createVarchar(100)))
                    .addColumn(new Column("Database", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Table", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Partitions", ScalarType.createVarchar(60)))
                    .build();

    private final String policyName;
    private final boolean isUsing;

    public ShowStoragePolicyCommand(String policyName, boolean isUsing) {
        super(PlanType.SHOW_STORAGE_POLICY_COMMAND);
        this.policyName = policyName;
        this.isUsing = isUsing;
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return USING_META_DATA;
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        // check auth
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }

        // Fetch the storage policy information from the environment
        if (isUsing) {
            List<List<String>> rows = Env.getCurrentEnv().getPolicyMgr().showStoragePolicyUsing(policyName);
            return new ShowResultSet(getMetaData(), rows);
        }
        return Env.getCurrentEnv().getPolicyMgr().showStoragePolicy();
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowStoragePolicyCommand(this, context);
    }
}
