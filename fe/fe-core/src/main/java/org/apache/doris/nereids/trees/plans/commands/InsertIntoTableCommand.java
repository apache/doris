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

import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.txn.Transaction;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.planner.DataSink;
import org.apache.doris.planner.OlapTableSink;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * insert into select command
 */
public class InsertIntoTableCommand extends Command implements ForwardWithSync {
    private final String tableName;
    private final List<String> colNames;
    private final LogicalPlan logicalQuery;
    private final PhysicalPlan physicalQuery;
    private final String labelName;
    private Database database;
    private Table table;
    private NereidsPlanner planner;
    private TupleDescriptor olapTuple;
    private List<String> partitions;
    private List<String> hints;

    /**
     * constructor
     */
    public InsertIntoTableCommand(String tableName, String labelName, List<String> colNames, List<String> partitions,
            List<String> hints, LogicalPlan logicalQuery, PhysicalPlan physicalQuery) {
        super(PlanType.INSERT_INTO_SELECT_COMMAND);
        Preconditions.checkArgument(tableName != null, "tableName cannot be null in insert-into-select command");
        Preconditions.checkArgument(logicalQuery != null, "logicalQuery cannot be null in insert-into-select command");
        this.tableName = tableName;
        this.labelName = labelName;
        this.colNames = colNames;
        this.partitions = partitions;
        this.hints = hints;
        this.logicalQuery = logicalQuery;
        this.physicalQuery = physicalQuery;
    }

    public InsertIntoTableCommand(String tableName, String labelName, List<String> colNames, List<String> partitions,
            List<String> hints, LogicalPlan logicalQuery) {
        this(tableName, labelName, colNames, partitions, hints, logicalQuery, null);
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        checkDatabaseAndTable(ctx);
        getTupleDesc();

        ctx.getStatementContext().setInsertTargetSchema(olapTuple.getSlots()
                .stream().map(slot -> DataType.fromCatalogType(slot.getType())).collect(Collectors.toList()));

        LogicalPlanAdapter logicalPlanAdapter = new LogicalPlanAdapter(logicalQuery, ctx.getStatementContext());
        executor.setParsedStmt(logicalPlanAdapter);
        planner = new NereidsPlanner(ctx.getStatementContext());
        planner.plan(logicalPlanAdapter, ctx.getSessionVariable().toThrift());

        if (ctx.getMysqlChannel() != null) {
            ctx.getMysqlChannel().reset();
        }
        String label = this.labelName;
        if (label == null) {
            label = String.format("label_%x_%x", ctx.queryId().hi, ctx.queryId().lo);
        }

        PlanFragment root = planner.getFragments().get(0);
        DataSink sink = createDataSink(ctx, root);
        Preconditions.checkArgument(sink instanceof OlapTableSink, "olap table sink is expected when"
                + " running insert into select");
        Transaction txn = new Transaction(ctx, database, table, label, planner);

        OlapTableSink olapTableSink = ((OlapTableSink) sink);
        olapTableSink.init(ctx.queryId(), txn.getTxnId(), database.getId(), ctx.getExecTimeout(),
                ctx.getSessionVariable().getSendBatchParallelism(), false);
        olapTableSink.complete();
        root.resetSink(olapTableSink);

        txn.executeInsertIntoSelectCommand(this);
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

    private DataSink createDataSink(ConnectContext ctx, PlanFragment root)
            throws org.apache.doris.common.AnalysisException {
        DataSink dataSink;
        if (table instanceof OlapTable) {
            dataSink = new OlapTableSink((OlapTable) table, olapTuple,
                    ((OlapTable) table).getPartitionIds(),
                    ctx.getSessionVariable().isEnableSingleReplicaInsert());
        } else {
            dataSink = DataSink.createDataSink(table);
        }
        return dataSink;
    }

    private void getTupleDesc() {
        // create insert target table's tupledesc.
        olapTuple = planner.getDescTable().createTupleDescriptor();
        List<Column> columns = Lists.newArrayList();
        if (colNames == null) {
            columns = table.getFullSchema();
        } else {
            for (String colName : colNames) {
                Column col = table.getColumn(colName);
                if (col == null) {
                    throw new AnalysisException(String.format("Column: %s is not in table: %s",
                            colName, table.getName()));
                }
                columns.add(col);
            }
        }
        for (Column col : columns) {
            SlotDescriptor slotDesc = planner.getDescTable().addSlotDescriptor(olapTuple);
            slotDesc.setIsMaterialized(true);
            slotDesc.setType(col.getType());
            slotDesc.setColumn(col);
            slotDesc.setIsNullable(col.isAllowNull());
        }
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitInsertIntoCommand(this, context);
    }
}
