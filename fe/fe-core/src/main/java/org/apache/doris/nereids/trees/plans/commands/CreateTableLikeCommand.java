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
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.CreateTableInfo;
import org.apache.doris.nereids.trees.plans.commands.info.CreateTableLikeInfo;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;

/** CreateTableLikeCommand */
public class CreateTableLikeCommand extends Command implements ForwardWithSync {
    private final CreateTableLikeInfo info;

    public CreateTableLikeCommand(CreateTableLikeInfo info) {
        super(PlanType.CREATE_TABLE_LIKE_COMMAND);
        this.info = info;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        executor.checkBlockRules();
        info.validate(ctx);
        doRun(info, ctx, executor);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitCreateTableLikeCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.CREATE;
    }

    private void doRun(CreateTableLikeInfo createTableLikeInfo, ConnectContext ctx, StmtExecutor executor)
            throws Exception {
        try {
            DatabaseIf db = Env.getCurrentInternalCatalog().getDbOrDdlException(createTableLikeInfo.getExistedDbName());
            TableIf table = db.getTableOrDdlException(createTableLikeInfo.getExistedTableName());

            if (table.getType() == TableIf.TableType.VIEW) {
                throw new DdlException("Not support create table from a View");
            }

            List<String> createTableStmt = Lists.newArrayList();
            table.readLock();
            try {
                if (table.isManagedTable()) {
                    if (!CollectionUtils.isEmpty(createTableLikeInfo.getRollupNames())) {
                        OlapTable olapTable = (OlapTable) table;
                        for (String rollupIndexName : createTableLikeInfo.getRollupNames()) {
                            if (!olapTable.hasMaterializedIndex(rollupIndexName)) {
                                throw new DdlException("Rollup index[" + rollupIndexName + "] not exists in Table["
                                    + olapTable.getName() + "]");
                            }
                        }
                    }
                } else if (!CollectionUtils.isEmpty(createTableLikeInfo.getRollupNames())
                        || createTableLikeInfo.isWithAllRollup()) {
                    throw new DdlException("Table[" + table.getName() + "] is external, not support rollup copy");
                }

                Env.getCreateTableLikeStmt(createTableLikeInfo, createTableLikeInfo.getDbName(), table, createTableStmt,
                        null, null, false, false, true, -1L,
                            false, false);
                if (createTableStmt.isEmpty()) {
                    ErrorReport.reportDdlException(ErrorCode.ERROR_CREATE_TABLE_LIKE_EMPTY, "CREATE");
                }
            } finally {
                table.readUnlock();
            }

            try {
                // analyze CreateTableStmt will check create_priv of existedTable, create table like only need
                // create_priv of newTable, and select_priv of existedTable, and priv check has done in
                // CreateTableStmt/CreateTableCommand, so we skip it
                ctx.setSkipAuth(true);
                NereidsParser nereidsParser = new NereidsParser();
                CreateTableCommand createTableCommand = (CreateTableCommand) nereidsParser
                        .parseSingle(createTableStmt.get(0));
                CreateTableInfo createTableInfo = createTableCommand.getCreateTableInfo();
                createTableCommand = new CreateTableCommand(createTableCommand.getCtasQuery(),
                    createTableInfo.withTableNameAndIfNotExists(createTableLikeInfo.getTableName(),
                            createTableLikeInfo.isIfNotExists()));
                createTableCommand.run(ctx, executor);
            } finally {
                ctx.setSkipAuth(false);
            }
        } catch (UserException e) {
            throw new DdlException("Failed to execute CREATE TABLE LIKE "
                + createTableLikeInfo.getExistedTableName() + ". Reason: "
                + e.getMessage(), e);
        }
    }
}
