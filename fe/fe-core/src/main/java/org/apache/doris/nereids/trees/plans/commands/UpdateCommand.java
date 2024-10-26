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
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.nereids.analyzer.UnboundAlias;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.analyzer.UnboundTableSinkCreator;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.rules.analysis.ExpressionAnalyzer;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.Explainable;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.DMLCommandType;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.RelationUtil;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * update command
 * the two case will be handled as:
 * case 1:
 *  update table t1 set v1 = v1 + 1 where k1 = 1 and k2 = 2;
 * =>
 *  insert into table (v1) select v1 + 1 from table t1 where k1 = 1 and k2 = 2
 * case 2:
 *  update t1 set t1.c1 = t2.c1, t1.c3 = t2.c3 * 100
 *  from t2 inner join t3 on t2.id = t3.id
 *  where t1.id = t2.id;
 * =>
 *  insert into t1 (c1, c3) select t2.c1, t2.c3 * 100 from t1 join t2 inner join t3 on t2.id = t3.id where t1.id = t2.id
 */
public class UpdateCommand extends Command implements ForwardWithSync, Explainable {
    private final List<EqualTo> assignments;
    private final List<String> nameParts;
    private final @Nullable String tableAlias;
    private final LogicalPlan logicalQuery;
    private OlapTable targetTable;
    private final Optional<LogicalPlan> cte;

    /**
     * constructor
     */
    public UpdateCommand(List<String> nameParts, @Nullable String tableAlias, List<EqualTo> assignments,
            LogicalPlan logicalQuery, Optional<LogicalPlan> cte) {
        super(PlanType.UPDATE_COMMAND);
        this.nameParts = Utils.copyRequiredList(nameParts);
        this.assignments = Utils.copyRequiredList(assignments);
        this.tableAlias = tableAlias;
        this.logicalQuery = Objects.requireNonNull(logicalQuery, "logicalQuery is required in update command");
        this.cte = cte;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        // NOTE: update command is executed as insert command, so txn insert can support it
        new InsertIntoTableCommand(completeQueryPlan(ctx, logicalQuery), Optional.empty(), Optional.empty(),
                Optional.empty()).run(ctx, executor);
    }

    /**
     * add LogicalOlapTableSink node, public for test.
     */
    @VisibleForTesting
    public LogicalPlan completeQueryPlan(ConnectContext ctx, LogicalPlan logicalQuery) {
        checkTable(ctx);

        Map<String, Expression> colNameToExpression = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        Map<String, Expression> partialUpdateColNameToExpression = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        for (EqualTo equalTo : assignments) {
            List<String> nameParts = ((UnboundSlot) equalTo.left()).getNameParts();
            checkAssignmentColumn(ctx, nameParts);
            colNameToExpression.put(nameParts.get(nameParts.size() - 1), equalTo.right());
            partialUpdateColNameToExpression.put(nameParts.get(nameParts.size() - 1), equalTo.right());
        }
        // check if any key in update clause
        if (targetTable.getFullSchema().stream().filter(Column::isKey)
                .anyMatch(column -> partialUpdateColNameToExpression.containsKey(column.getName()))) {
            throw new AnalysisException("Only value columns of unique table could be updated");
        }
        List<NamedExpression> selectItems = Lists.newArrayList();
        String tableName = tableAlias != null ? tableAlias : targetTable.getName();
        Expression setExpr = null;
        for (Column column : targetTable.getFullSchema()) {
            // if it sets sequence column in stream load phase, the sequence map column is null, we query it.
            if (!column.isVisible() && !column.isSequenceColumn()) {
                continue;
            }
            if (colNameToExpression.containsKey(column.getName())) {
                Expression expr = colNameToExpression.get(column.getName());
                // when updating the sequence map column, the real sequence column need to set with the same value.
                boolean isSequenceMapColumn = targetTable.hasSequenceCol()
                        && targetTable.getSequenceMapCol() != null
                        && column.getName().equalsIgnoreCase(targetTable.getSequenceMapCol());
                if (setExpr == null && isSequenceMapColumn) {
                    setExpr = expr;
                }
                selectItems.add(expr instanceof UnboundSlot
                        ? ((NamedExpression) expr)
                        : new UnboundAlias(expr));
                colNameToExpression.remove(column.getName());
            } else {
                if (column.isSequenceColumn() && setExpr != null) {
                    selectItems.add(new UnboundAlias(setExpr, column.getName()));
                } else if (column.hasOnUpdateDefaultValue()) {
                    Expression defualtValueExpression =
                            new NereidsParser().parseExpression(column.getOnUpdateDefaultValueExpr()
                                    .toSqlWithoutTbl());
                    selectItems.add(new UnboundAlias(defualtValueExpression, column.getName()));
                } else {
                    selectItems.add(new UnboundSlot(tableName, column.getName()));
                }
            }
        }
        if (!colNameToExpression.isEmpty()) {
            throw new AnalysisException("unknown column in assignment list: "
                    + String.join(", ", colNameToExpression.keySet()));
        }

        boolean isPartialUpdate = targetTable.getEnableUniqueKeyMergeOnWrite()
                && selectItems.size() < targetTable.getColumns().size()
                && targetTable.getSequenceCol() == null
                && partialUpdateColNameToExpression.size() <= targetTable.getFullSchema().size() * 3 / 10;

        List<String> partialUpdateColNames = new ArrayList<>();
        List<NamedExpression> partialUpdateSelectItems = new ArrayList<>();
        if (isPartialUpdate) {
            for (Column column : targetTable.getFullSchema()) {
                Expression expr = new UnboundSlot(tableName, column.getName());
                boolean existInExpr = false;
                for (String colName : partialUpdateColNameToExpression.keySet()) {
                    if (colName.equalsIgnoreCase(column.getName())) {
                        expr = partialUpdateColNameToExpression.get(column.getName());
                        existInExpr = true;
                        break;
                    }
                }
                if (column.isKey() || existInExpr) {
                    partialUpdateSelectItems.add(expr instanceof UnboundSlot
                            ? ((NamedExpression) expr)
                            : new UnboundAlias(expr));
                    partialUpdateColNames.add(column.getName());
                }
            }
        }

        logicalQuery = new LogicalProject<>(isPartialUpdate ? partialUpdateSelectItems : selectItems, logicalQuery);
        if (cte.isPresent()) {
            logicalQuery = ((LogicalPlan) cte.get().withChildren(logicalQuery));
        }
        // make UnboundTableSink
        return UnboundTableSinkCreator.createUnboundTableSink(nameParts,
                isPartialUpdate ? partialUpdateColNames : ImmutableList.of(), ImmutableList.of(),
                false, ImmutableList.of(), isPartialUpdate, DMLCommandType.UPDATE, logicalQuery);
    }

    private void checkAssignmentColumn(ConnectContext ctx, List<String> columnNameParts) {
        if (columnNameParts.size() <= 1) {
            return;
        }
        String dbName = null;
        String tableName = null;
        if (columnNameParts.size() == 3) {
            dbName = columnNameParts.get(0);
            tableName = columnNameParts.get(1);
        } else if (columnNameParts.size() == 2) {
            tableName = columnNameParts.get(0);
        } else {
            throw new AnalysisException("column in assignment list is invalid, " + String.join(".", columnNameParts));
        }
        if (dbName != null && this.tableAlias != null) {
            throw new AnalysisException("column in assignment list is invalid, " + String.join(".", columnNameParts));
        }
        List<String> tableQualifier = RelationUtil.getQualifierName(ctx, nameParts);
        if (!ExpressionAnalyzer.sameTableName(tableAlias == null ? tableQualifier.get(2) : tableAlias, tableName)
                || (dbName != null
                && !ExpressionAnalyzer.compareDbNameIgnoreClusterName(tableQualifier.get(1), dbName))) {
            throw new AnalysisException("column in assignment list is invalid, " + String.join(".", columnNameParts));
        }
    }

    private void checkTable(ConnectContext ctx) {
        if (ctx.getSessionVariable().isInDebugMode()) {
            throw new AnalysisException("Update is forbidden since current session is in debug mode."
                    + " Please check the following session variables: "
                    + ctx.getSessionVariable().printDebugModeVariables());
        }
        List<String> tableQualifier = RelationUtil.getQualifierName(ctx, nameParts);
        TableIf table = RelationUtil.getTable(tableQualifier, ctx.getEnv());
        if (!(table instanceof OlapTable)) {
            throw new AnalysisException("target table in update command should be an olapTable");
        }
        targetTable = ((OlapTable) table);
        if (targetTable.getType() != Table.TableType.OLAP
                || targetTable.getKeysType() != KeysType.UNIQUE_KEYS) {
            throw new AnalysisException("Only unique table could be updated.");
        }
    }

    @Override
    public Plan getExplainPlan(ConnectContext ctx) {
        return completeQueryPlan(ctx, logicalQuery);
    }

    public LogicalPlan getLogicalQuery() {
        return logicalQuery;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitUpdateCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.UPDATE;
    }
}
