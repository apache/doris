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
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.ExternalObjectLog;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.info.PartitionNamesInfo;
import org.apache.doris.info.TableNameInfo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.execute.ExecuteAction;
import org.apache.doris.nereids.trees.plans.commands.execute.ExecuteActionFactory;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ResultSet;
import org.apache.doris.qe.StmtExecutor;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * ALTER TABLE table EXECUTE action("k" = "v", ...) [PARTITION (partition_list)]
 * [WHERE condition]
 */
public class ExecuteActionCommand extends Command implements ForwardWithSync {
    private final TableNameInfo tableNameInfo;
    private final String actionName;
    private final Map<String, String> properties;
    private final Optional<PartitionNamesInfo> partitionNamesInfo;
    private final Optional<Expression> whereCondition;

    /**
     * Constructor for ExecuteActionCommand.
     *
     * @param tableNameInfo      table name information
     * @param actionName         name of the action to execute
     * @param properties         action properties as key-value pairs
     * @param partitionNamesInfo optional partition information
     * @param whereCondition     optional where condition for filtering
     */
    public ExecuteActionCommand(TableNameInfo tableNameInfo, String actionName,
            Map<String, String> properties, Optional<PartitionNamesInfo> partitionNamesInfo,
            Optional<Expression> whereCondition) {
        super(PlanType.OPTIMIZE_TABLE_COMMAND);
        this.tableNameInfo = Objects.requireNonNull(tableNameInfo, "tableNameInfo is null");
        this.actionName = Objects.requireNonNull(actionName, "actionName is null");
        this.properties = Objects.requireNonNull(properties, "properties is null");
        this.partitionNamesInfo = Objects.requireNonNull(partitionNamesInfo, "partitionNamesInfo is null");
        this.whereCondition = Objects.requireNonNull(whereCondition, "whereCondition is null");
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        tableNameInfo.analyze(ctx);
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
            throw new AnalysisException("ALTER TABLE EXECUTE is currently only supported for external tables");
        }

        try {
            ExecuteAction action = ExecuteActionFactory.createAction(
                    actionName, properties, partitionNamesInfo, whereCondition, table);

            if (!action.isSupported(table)) {
                throw new AnalysisException("Action '" + actionName + "' is not supported for this table engine");
            }

            action.validate(tableNameInfo, ctx.getCurrentUserIdentity());
            ResultSet resultSet = action.execute(table);
            logRefreshTable(table, System.currentTimeMillis());
            if (resultSet != null) {
                executor.sendResultSet(resultSet);
            }
        } catch (UserException e) {
            throw new DdlException("Failed to execute action: " + e.getMessage(), e);
        }
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitExecuteActionCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.OPTIMIZE;
    }

    public TableNameInfo getTableNameInfo() {
        return tableNameInfo;
    }

    public String getActionName() {
        return actionName;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public Optional<PartitionNamesInfo> getPartitionNamesInfo() {
        return partitionNamesInfo;
    }

    public Optional<Expression> getWhereCondition() {
        return whereCondition;
    }

    /**
     * Log refresh table to make follow fe metadata cache refresh.
     *
     * @param table the table to log
     * @throws UserException if the table type is not supported
     */
    private void logRefreshTable(TableIf table, long updateTime) throws UserException {
        if (table instanceof ExternalTable) {
            ExternalTable externalTable = (ExternalTable) table;
            Env.getCurrentEnv().getEditLog()
                    .logRefreshExternalTable(
                            ExternalObjectLog.createForRefreshTable(
                                    externalTable.getCatalog().getId(),
                                    externalTable.getDbName(),
                                    externalTable.getName(), updateTime));
        } else {
            // support more table in future
            throw new UserException("Unsupported table type: " + table.getClass().getName() + " for refresh table");
        }
    }
}
