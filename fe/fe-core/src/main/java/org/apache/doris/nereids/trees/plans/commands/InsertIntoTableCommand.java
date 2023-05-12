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

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Table;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.properties.DistributionSpecHash;
import org.apache.doris.nereids.properties.DistributionSpecHash.ShuffleType;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.txn.Transaction;
import org.apache.doris.nereids.util.RelationUtil;
import org.apache.doris.planner.DataSink;
import org.apache.doris.planner.OlapTableSink;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.stream.Collectors;

/**
 * insert into select command implementation
 * insert into select command support the grammer: explain? insert into table columns? partitions? hints? query
 * InsertIntoTableCommand is a command to represent insert the answer of a query into a table.
 * class structure's:
 *  InsertIntoTableCommand(Query())
 *  ExplainCommand(InsertIntoTableCommand(Query()))
 */
public class InsertIntoTableCommand extends Command implements ForwardWithSync {
    public static final Logger LOG = LogManager.getLogger(InsertIntoTableCommand.class);

    private final List<String> nameParts;
    private final List<String> colNames;
    private final LogicalPlan logicalQuery;
    private final String labelName;
    private Table table;
    private NereidsPlanner planner;
    private final List<String> partitions;

    /**
     * constructor
     */
    public InsertIntoTableCommand(List<String> nameParts, String labelName, List<String> colNames,
            List<String> partitions, LogicalPlan logicalQuery) {
        super(PlanType.INSERT_INTO_TABLE_COMMAND);
        Preconditions.checkArgument(nameParts != null, "tableName cannot be null in InsertIntoTableCommand");
        Preconditions.checkArgument(logicalQuery != null, "logicalQuery cannot be null in InsertIntoTableCommand");
        this.nameParts = nameParts;
        this.labelName = labelName;
        this.colNames = colNames;
        this.partitions = partitions;
        this.logicalQuery = logicalQuery;
    }

    public NereidsPlanner getPlanner() {
        return planner;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        if (ctx.isTxnModel()) {
            // in original planner with txn model, we can execute sql like: insert into t select 1, 2, 3
            // but no data will be inserted, now we adjust forbid it.
            throw new AnalysisException("insert into table command is not supported in txn model");
        }

        // Check database and table
        List<String> qualifiedTableName = RelationUtil.getQualifierName(ctx, nameParts);
        String catalogName = qualifiedTableName.get(0);
        String dbName = qualifiedTableName.get(1);
        String tableName = qualifiedTableName.get(2);
        CatalogIf<?> catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(catalogName);
        if (catalog == null) {
            throw new AnalysisException(String.format("Catalog %s does not exist.", catalogName));
        }
        Database database;
        try {
            database = ((Database) catalog.getDb(dbName).orElseThrow(() ->
                    new AnalysisException("Database [" + dbName + "] does not exist.")));
            table = database.getTable(tableName).orElseThrow(() ->
                    new AnalysisException("Table [" + tableName + "] does not exist in database [" + dbName + "]."));
        } catch (Throwable e) {
            throw new AnalysisException(e.getMessage(), e.getCause());
        }

        // collect column
        List<Column> targetColumns;
        if (colNames == null) {
            targetColumns = table.getFullSchema();
        } else {
            targetColumns = Lists.newArrayList();
            for (String colName : colNames) {
                Column col = table.getColumn(colName);
                if (col == null) {
                    throw new AnalysisException(String.format("Column: %s is not in table: %s",
                            colName, table.getName()));
                }
                targetColumns.add(col);
            }
        }

        // collect partitions
        List<Long> partitionIds = null;
        if (partitions == null) {
            partitionIds = partitions.stream().map(pn -> {
                Partition p = table.getPartition(pn);
                if (p == null) {
                    throw new AnalysisException(String.format("Unknown partition: %s in table: %s", pn,
                            table.getName()));
                }
                return p.getId();
            }).collect(Collectors.toList());
        }

        ctx.getStatementContext().getInsertIntoContext().setTargetSchema(targetColumns);

        LogicalPlanAdapter logicalPlanAdapter = new LogicalPlanAdapter(logicalQuery, ctx.getStatementContext());
        planner = new NereidsPlanner(ctx.getStatementContext());
        planner.plan(logicalPlanAdapter, ctx.getSessionVariable().toThrift());

        // create insert target table's tupledesc.
        TupleDescriptor olapTuple = planner.getDescTable().createTupleDescriptor();

        for (Column col : targetColumns) {
            SlotDescriptor slotDesc = planner.getDescTable().addSlotDescriptor(olapTuple);
            slotDesc.setIsMaterialized(true);
            slotDesc.setType(col.getType());
            slotDesc.setColumn(col);
            slotDesc.setIsNullable(col.isAllowNull());
        }

        PlanFragment root = planner.getFragments().get(0);
        List<Expr> outputs = root.getOutputExprs();
        // handle insert a duplicate key table to a unique key table.
        if (outputs.size() == table.getFullSchema().size()) {
            return;
        }
        int i = 0;
        List<Expr> newOutputs = Lists.newArrayListWithCapacity(table.getFullSchema().size());
        for (Column column : table.getFullSchema()) {
            if (column.isVisible()) {
                newOutputs.add(outputs.get(i++));
            } else {
                newOutputs.add(LiteralExpr.create("0", column.getType()));
            }
        }
        root.setOutputExprs(newOutputs);

        if (ctx.getMysqlChannel() != null) {
            ctx.getMysqlChannel().reset();
        }
        String label = this.labelName;
        if (label == null) {
            label = String.format("label_%x_%x", ctx.queryId().hi, ctx.queryId().lo);
        }

        DataSink sink;
        if (table instanceof OlapTable) {
            sink = new OlapTableSink((OlapTable) table, olapTuple, partitionIds,
                    ctx.getSessionVariable().isEnableSingleReplicaInsert());
        } else {
            sink = DataSink.createDataSink(table);
        }
        Preconditions.checkArgument(sink instanceof OlapTableSink, "olap table sink is expected when"
                + " running insert into select");

        Transaction txn;
        txn = new Transaction(ctx, database, table, label, planner);

        OlapTableSink olapTableSink = ((OlapTableSink) sink);
        olapTableSink.init(ctx.queryId(), txn.getTxnId(), database.getId(), ctx.getExecTimeout(),
                ctx.getSessionVariable().getSendBatchParallelism(), false);
        olapTableSink.complete();
        root.resetSink(olapTableSink);

        txn.executeInsertIntoSelectCommand(executor);
    }

    public LogicalPlan extractQueryPlan() {
        return logicalQuery;
    }

    /**
     * calculate PhysicalProperties.
     */
    public PhysicalProperties calculatePhysicalProperties(List<Slot> outputs) {
        // it will be used at set physical properties.

        List<ExprId> exprIds = outputs.subList(0, ((OlapTable) table).getKeysNum()).stream()
                .map(NamedExpression::getExprId).collect(Collectors.toList());
        return PhysicalProperties.createHash(new DistributionSpecHash(exprIds, ShuffleType.NATURAL));
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitInsertIntoCommand(this, context);
    }
}
