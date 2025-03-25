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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.Util;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.TableRefInfo;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * AdminCompactTableCommand
 */
public class AdminCompactTableCommand extends Command implements ForwardWithSync {
    private static final Logger LOG = LogManager.getLogger(AdminCompactTableCommand.class);
    private TableRefInfo tableRefInfo;
    private EqualTo where;

    /**
     * compact type
     */
    public enum CompactionType {
        CUMULATIVE,
        BASE
    }

    private CompactionType typeFilter;

    public AdminCompactTableCommand(TableRefInfo tableRefInfo, EqualTo where) {
        super(PlanType.ADD_CONSTRAINT_COMMAND);
        this.tableRefInfo = tableRefInfo;
        this.where = where;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate(ctx);
        String dbName = tableRefInfo.getTableNameInfo().getDb();
        String tableName = tableRefInfo.getTableNameInfo().getTbl();
        String type = getCompactionType();
        List<String> partitionNames = tableRefInfo.getPartitionNamesInfo().getPartitionNames();
        ctx.getEnv().compactTable(dbName, tableName, type, partitionNames);
    }

    private void validate(ConnectContext ctx) throws UserException {
        // check auth
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }
        tableRefInfo.analyze(ctx);
        Util.prohibitExternalCatalog(tableRefInfo.getTableNameInfo().getCtl(), this.getClass().getSimpleName());

        List<String> partitionNames = tableRefInfo.getPartitionNamesInfo().getPartitionNames();
        if (partitionNames != null) {
            if (partitionNames.size() != 1) {
                throw new AnalysisException("Only support single partition for compaction");
            }
        } else {
            throw new AnalysisException("No partition selected for compaction");
        }

        // analyze where clause if not null
        if (where == null) {
            throw new AnalysisException("Compaction type must be specified in"
                + " Where clause like: type = 'BASE/CUMULATIVE'");
        }

        if (!analyzeWhere()) {
            throw new AnalysisException(
                "Where clause should looks like: type = 'BASE/CUMULATIVE'");
        }
    }

    private boolean analyzeWhere() {
        try {
            typeFilter = CompactionType.valueOf(((StringLiteral) where.right()).getStringValue().toUpperCase());
        } catch (Exception e) {
            return false;
        }

        if (typeFilter == null || (typeFilter != CompactionType.CUMULATIVE && typeFilter != CompactionType.BASE)) {
            return false;
        }

        return true;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitAdminCompactTableCommand(this, context);
    }

    private String getCompactionType() {
        if (typeFilter == CompactionType.CUMULATIVE) {
            return "cumulative";
        } else {
            return "base";
        }
    }

    @Override
    protected void checkSupportedInCloudMode(ConnectContext ctx) throws DdlException {
        LOG.info("AdminCompactTableCommand not supported in cloud mode");
        throw new DdlException("Unsupported operation");
    }
}
