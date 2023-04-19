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

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Table;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.txn.Transaction;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Optional;

/**
 * insert into select command
 */
public class InsertIntoSelectCommand extends Command implements ForwardWithSync {
    private final String tableName;
    private final List<String> colNames;
    private final LogicalPlan logicalQuery;
    private final PhysicalPlan physicalQuery;
    private final String labelName;
    private Database database;
    private Table table;

    /**
     * constructor
     */
    public InsertIntoSelectCommand(String tableName, String labelName, List<String> colNames,
            LogicalPlan logicalQuery, PhysicalPlan physicalQuery) {
        super(PlanType.INSERT_INTO_SELECT_COMMAND);
        Preconditions.checkArgument(tableName != null, "tableName cannot be null in insert-into-select command");
        Preconditions.checkArgument(logicalQuery != null, "logicalQuery cannot be null in insert-into-select command");
        this.tableName = tableName;
        this.labelName = labelName;
        this.colNames = colNames;
        this.logicalQuery = logicalQuery;
        this.physicalQuery = physicalQuery;
    }

    public InsertIntoSelectCommand(String tableName, String labelName, List<String> colNames,
            LogicalPlan logicalQuery) {
        this(tableName, labelName, colNames, logicalQuery, null);
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        checkDatabaseAndTable(ctx);
        LogicalPlanAdapter logicalPlanAdapter = new LogicalPlanAdapter(logicalQuery, ctx.getStatementContext());
        executor.setParsedStmt(logicalPlanAdapter);
        NereidsPlanner planner = new NereidsPlanner(ctx.getStatementContext());
        planner.plan(logicalPlanAdapter, ctx.getSessionVariable().toThrift());

        if (ctx.getMysqlChannel() != null) {
            ctx.getMysqlChannel().reset();
        }
        String label = this.labelName;
        if (label == null) {
            label = String.format("label_%x_%x", ctx.queryId().hi, ctx.queryId().lo);
        }
        Transaction txn = new Transaction(ctx, database, table, label, planner);
        txn.executeInsertIntoSelectCommand(this);
    }

    public LogicalPlan getLogicalQuery() {
        return logicalQuery;
    }

    public PhysicalPlan getPhysicalQuery() {
        return physicalQuery;
    }

    public String getLabelName() {
        return labelName;
    }

    private void checkDatabaseAndTable(ConnectContext ctx) {
        Optional<Database> database = Env.getCurrentInternalCatalog().getDb(ctx.getDatabase());
        if (!database.isPresent()) {
            throw new AnalysisException("Unknown database: " + ctx.getDatabase());
        }
        this.database = database.get();
        Optional<Table> table = this.database.getTable(tableName);
        if (!table.isPresent()) {
            throw new AnalysisException("Unknown table: " + ctx.getDatabase());
        }
        this.table = table.get();
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitInsertIntoCommand(this, context);
    }
}
