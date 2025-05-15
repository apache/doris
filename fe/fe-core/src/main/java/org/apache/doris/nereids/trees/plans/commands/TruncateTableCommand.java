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
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.util.InternalDatabaseUtil;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.PartitionNamesInfo;
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import java.util.Objects;
import java.util.Optional;

/**
 * TRUNCATE TABLE tbl [PARTITION(p1, p2, ...)]
 */
public class TruncateTableCommand extends Command implements ForwardWithSync {
    private final TableNameInfo tableNameInfo;
    private final Optional<PartitionNamesInfo> partitionNamesInfo;
    private final boolean forceDrop;

    public TruncateTableCommand(TableNameInfo tableNameInfo,
            Optional<PartitionNamesInfo> partitionNamesInfo, boolean forceDrop) {
        super(PlanType.TRUNCATE_TABLE_COMMAND);
        this.tableNameInfo = Objects.requireNonNull(tableNameInfo, "tableNameInfo is null");
        this.partitionNamesInfo = Objects.requireNonNull(partitionNamesInfo, "partitionNamesInfo is null");
        this.forceDrop = Objects.requireNonNull(forceDrop, "forceDrop is null");
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate(ctx);
        Env.getCurrentEnv().truncateTable(this);
    }

    /**
     * validate
     */
    public void validate(ConnectContext ctx) throws AnalysisException {
        tableNameInfo.analyze(ctx);

        InternalDatabaseUtil.checkDatabase(tableNameInfo.getDb(), ctx);
        // check access
        // it requires LOAD privilege, because we consider this operation as 'delete data', which is also a
        // 'load' operation.
        if (!Env.getCurrentEnv().getAccessManager()
                .checkTblPriv(ConnectContext.get(), tableNameInfo.getCtl(), tableNameInfo.getDb(),
                tableNameInfo.getTbl(), PrivPredicate.LOAD)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "LOAD");
        }

        // check partition if specified. do not support truncate temp partitions
        if (partitionNamesInfo.isPresent()) {
            partitionNamesInfo.get().validate();
            if (partitionNamesInfo.get().isTemp()) {
                throw new AnalysisException("Not support truncate temp partitions");
            }
        }
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitTruncateTableCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.TRUNCATE;
    }

    /**
     * toSqlWithoutTable
     */
    public String toSqlWithoutTable() {
        StringBuilder sb = new StringBuilder();
        if (partitionNamesInfo.isPresent()) {
            sb.append(partitionNamesInfo.get().toSql());
        }
        if (isForceDrop()) {
            sb.append(" FORCE");
        }
        return sb.toString();
    }

    public TableNameInfo getTableNameInfo() {
        return tableNameInfo;
    }

    public Optional<PartitionNamesInfo> getPartitionNamesInfo() {
        return partitionNamesInfo;
    }

    public boolean isForceDrop() {
        return forceDrop;
    }
}
