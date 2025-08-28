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

import org.apache.doris.analysis.StmtType;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.util.Util;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.PartitionNamesInfo;
import org.apache.doris.nereids.trees.plans.commands.info.TableRefInfo;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Objects;

/**
 * AdminCancelRepairTableCommand
 */
public class AdminCancelRepairTableCommand extends Command implements ForwardWithSync {
    private static final Logger LOG = LogManager.getLogger(AdminCancelRepairTableCommand.class);
    private final TableRefInfo tableRefInfo;
    private List<String> partitions = Lists.newArrayList();

    public AdminCancelRepairTableCommand(TableRefInfo tableRefInfo) {
        super(PlanType.ADMIN_CANCEL_REPAIR_TABLE_COMMAND);
        Objects.requireNonNull(tableRefInfo, "tableRefInfo is null");
        this.tableRefInfo = tableRefInfo;
    }

    public String getDbName() {
        return tableRefInfo.getTableNameInfo().getDb();
    }

    public String getTblName() {
        return tableRefInfo.getTableNameInfo().getTbl();
    }

    public List<String> getPartitions() {
        return partitions;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate(ctx);
        ctx.getEnv().getTabletChecker().cancelRepairTable(this);
    }

    /**
     * validate
     * @param ctx ctx
     * @throws AnalysisException AnalysisException
     */
    public void validate(ConnectContext ctx) throws AnalysisException {
        // check auth
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }

        tableRefInfo.getTableNameInfo().analyze(ctx);
        Util.prohibitExternalCatalog(tableRefInfo.getTableNameInfo().getCtl(), this.getClass().getSimpleName());

        PartitionNamesInfo partitionNamesInfo = tableRefInfo.getPartitionNamesInfo();
        if (partitionNamesInfo != null) {
            if (partitionNamesInfo.isTemp()) {
                throw new AnalysisException("Do not support (cancel)repair temporary partitions");
            }
            partitions.addAll(partitionNamesInfo.getPartitionNames());
        }
    }

    @Override
    protected void checkSupportedInCloudMode(ConnectContext ctx) throws DdlException {
        LOG.info("AdminCancelRepairTableCommand not supported in cloud mode");
        throw new DdlException("denied");
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitAdminCancelRepairTableCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.ADMIN;
    }
}
