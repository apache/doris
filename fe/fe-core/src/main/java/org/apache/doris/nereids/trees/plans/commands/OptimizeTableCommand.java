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
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.util.InternalDatabaseUtil;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.action.OptimizeAction;
import org.apache.doris.nereids.trees.plans.commands.info.PartitionNamesInfo;
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * OPTIMIZE TABLE tbl [PARTITION(p1, p2, ...)] [WHERE expr] PROPERTIES("action"
 * = "xx", ...)
 */
public class OptimizeTableCommand extends Command implements ForwardWithSync {
    private final TableNameInfo tableNameInfo;
    private final Optional<PartitionNamesInfo> partitionNamesInfo;
    private final Optional<Expression> whereClause;
    private final Map<String, String> properties;

    public OptimizeTableCommand(TableNameInfo tableNameInfo,
            Optional<PartitionNamesInfo> partitionNamesInfo,
            Optional<Expression> whereClause,
            Map<String, String> properties) {
        super(PlanType.OPTIMIZE_TABLE_COMMAND);
        this.tableNameInfo = Objects.requireNonNull(tableNameInfo, "tableNameInfo is null");
        this.partitionNamesInfo = Objects.requireNonNull(partitionNamesInfo, "partitionNamesInfo is null");
        this.whereClause = Objects.requireNonNull(whereClause, "whereClause is null");
        this.properties = Objects.requireNonNull(properties, "properties is null");
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate(ctx);

        // Get the table
        CatalogIf<?> catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(tableNameInfo.getCtl());
        if (catalog == null) {
            throw new AnalysisException("Catalog " + tableNameInfo.getCtl() + " does not exist");
        }

        DatabaseIf<?> database = catalog.getDbNullable(tableNameInfo.getDb());
        if (database == null) {
            throw new AnalysisException("Database " + tableNameInfo.getDb() + " does not exist");
        }

        TableIf table = database.getTableNullable(tableNameInfo.getTbl());
        if (table == null) {
            throw new AnalysisException("Table " + tableNameInfo.getTbl() + " does not exist");
        }

        if (!(table instanceof ExternalTable)) {
            throw new AnalysisException("OPTIMIZE TABLE is currently only supported for external tables");
        }

        ExternalTable externalTable = (ExternalTable) table;

        // Get action type from properties
        String actionType = properties.get("action");
        if (actionType == null || actionType.isEmpty()) {
            throw new AnalysisException("OPTIMIZE TABLE requires 'action' property to be specified");
        }

        // Create and execute the appropriate action
        try {
            OptimizeAction action = OptimizeAction.createAction(
                    actionType, properties, partitionNamesInfo, whereClause, externalTable);

            if (!action.isSupported(externalTable)) {
                throw new AnalysisException("Action '" + actionType + "' is not supported for this table engine");
            }

            action.validate(tableNameInfo, ctx.getCurrentUserIdentity());
            action.execute(externalTable);

        } catch (DdlException e) {
            throw new AnalysisException(e.getMessage());
        }
    }

    /**
     * validate
     */
    public void validate(ConnectContext ctx) throws AnalysisException {
        tableNameInfo.analyze(ctx);

        InternalDatabaseUtil.checkDatabase(tableNameInfo.getDb(), ctx);
        // check access
        if (!Env.getCurrentEnv().getAccessManager()
                .checkTblPriv(ConnectContext.get(), tableNameInfo.getCtl(), tableNameInfo.getDb(),
                        tableNameInfo.getTbl(), PrivPredicate.ALTER)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ALTER");
        }

        // check partition if specified
        if (partitionNamesInfo.isPresent()) {
            partitionNamesInfo.get().validate();
        }

        // validate properties
        if (properties.isEmpty()) {
            throw new AnalysisException("OPTIMIZE TABLE requires PROPERTIES to be specified");
        }

        String action = properties.get("action");
        if (action == null || action.isEmpty()) {
            throw new AnalysisException("OPTIMIZE TABLE requires 'action' property to be specified");
        }
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitOptimizeTableCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.OPTIMIZE;
    }

    public TableNameInfo getTableNameInfo() {
        return tableNameInfo;
    }

    public Optional<PartitionNamesInfo> getPartitionNamesInfo() {
        return partitionNamesInfo;
    }

    public Optional<Expression> getWhereClause() {
        return whereClause;
    }

    public Map<String, String> getProperties() {
        return properties;
    }
}
