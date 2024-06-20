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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.analysis.ColumnDef.DefaultValue;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.analyzer.UnboundOlapTableSink;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapTableSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.util.RelationUtil;
import org.apache.doris.nereids.util.TypeCoercionUtils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * bind an unbound logicalOlapTableSink represent the target table of an insert command
 */
public class BindSink implements AnalysisRuleFactory {

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                RuleType.BINDING_INSERT_TARGET_TABLE.build(
                        unboundOlapTableSink().thenApply(ctx -> {
                            UnboundOlapTableSink<?> sink = ctx.root;
                            Pair<Database, OlapTable> pair = bind(ctx.cascadesContext, sink);
                            Database database = pair.first;
                            OlapTable table = pair.second;

                            LogicalPlan child = ((LogicalPlan) sink.child());

                            if (sink.getColNames().isEmpty() && sink.isFromNativeInsertStmt()
                                    && sink.isPartialUpdate()) {
                                throw new AnalysisException("You must explicitly specify the columns to be updated "
                                        + "when updating partial columns using the INSERT statement.");
                            }

                            LogicalOlapTableSink<?> boundSink = new LogicalOlapTableSink<>(
                                    database,
                                    table,
                                    bindTargetColumns(table, sink.getColNames()),
                                    bindPartitionIds(table, sink.getPartitions()),
                                    child.getOutput().stream()
                                            .map(NamedExpression.class::cast)
                                            .collect(ImmutableList.toImmutableList()),
                                    sink.isPartialUpdate(),
                                    sink.isFromNativeInsertStmt(),
                                    sink.child());

                            // we need to insert all the columns of the target table
                            // although some columns are not mentions.
                            // so we add a projects to supply the default value.

                            if (boundSink.getCols().size() != child.getOutput().size()) {
                                throw new AnalysisException(
                                        "insert into cols should be corresponding to the query output");
                            }

                            try {
                                // For Unique Key table with sequence column
                                // (which default value is not CURRENT_TIMESTAMP),
                                // user MUST specify the sequence column while inserting data
                                //
                                // case1: create table by `function_column.sequence_col`
                                //        a) insert with column list, must include the
                                //        sequence map column
                                //        b) insert without column list, already contains the column,
                                //        don't need to check
                                // case2: create table by `function_column.sequence_type`
                                //        a) insert with column list, must include the hidden column
                                //        __DORIS_SEQUENCE_COL__
                                //        b) insert without column list, don't include the hidden column
                                //        __DORIS_SEQUENCE_COL__
                                //           by default, will fail.
                                if (table.hasSequenceCol()) {
                                    boolean haveInputSeqCol = false;
                                    Optional<Column> seqColInTable = Optional.empty();
                                    if (table.getSequenceMapCol() != null) {
                                        if (!sink.getColNames().isEmpty()) {
                                            if (sink.getColNames().contains(table.getSequenceMapCol())) {
                                                haveInputSeqCol = true; // case1.a
                                            }
                                        } else {
                                            haveInputSeqCol = true; // case1.b
                                        }
                                        seqColInTable = table.getFullSchema().stream()
                                                .filter(col -> col.getName().equals(table.getSequenceMapCol()))
                                                .findFirst();
                                    } else {
                                        if (!sink.getColNames().isEmpty()) {
                                            if (sink.getColNames().contains(Column.SEQUENCE_COL)) {
                                                haveInputSeqCol = true; // case2.a
                                            } // else case2.b
                                        }
                                    }

                                    if (!haveInputSeqCol && !sink.isPartialUpdate()) {
                                        if (!seqColInTable.isPresent() || seqColInTable.get().getDefaultValue() == null
                                                || !seqColInTable.get().getDefaultValue()
                                                .equals(DefaultValue.CURRENT_TIMESTAMP)) {
                                            throw new org.apache.doris.common.AnalysisException(
                                                    "Table " + table.getName()
                                                            + " has sequence column, "
                                                            + "need to specify the sequence column");
                                        }
                                    }
                                }
                            } catch (Exception e) {
                                throw new AnalysisException(e.getMessage(), e.getCause());
                            }

                            Map<Column, NamedExpression> columnToChildOutput = Maps.newHashMap();
                            for (int i = 0; i < boundSink.getCols().size(); ++i) {
                                columnToChildOutput.put(boundSink.getCols().get(i), child.getOutput().get(i));
                            }

                            Map<String, NamedExpression> columnToOutput = Maps.newLinkedHashMap();
                            NereidsParser expressionParser = new NereidsParser();

                            // this is a trick way to avoid legacy planner's slotRef toSql output include label.
                            // see more in org.apache.doris.analysis.SlotRef.toSqlImpl
                            if (ConnectContext.get() != null) {
                                ConnectContext.get().getState().setIsQuery(true);
                            }
                            // generate slots not mentioned in sql, mv slots and shaded slots.
                            try {
                                for (Column column : boundSink.getTargetTable().getFullSchema()) {
                                    if (column.isMaterializedViewColumn()) {
                                        List<SlotRef> refs = column.getRefColumns();
                                        // now we have to replace the column to slots.
                                        Preconditions.checkArgument(refs != null,
                                                "mv column's ref column cannot be null");
                                        Expression parsedExpression = expressionParser.parseExpression(
                                                column.getDefineExpr().toSql());
                                        Expression boundExpression = SlotReplacer.INSTANCE
                                                .replace(parsedExpression, columnToOutput);

                                        NamedExpression slot = boundExpression instanceof NamedExpression
                                                ? ((NamedExpression) boundExpression)
                                                : new Alias(boundExpression);

                                        columnToOutput.put(column.getName(), slot);
                                    } else if (columnToChildOutput.containsKey(column)) {
                                        columnToOutput.put(column.getName(), columnToChildOutput.get(column));
                                    } else {
                                        if (table.hasSequenceCol()
                                                && column.getName().equals(Column.SEQUENCE_COL)
                                                && table.getSequenceMapCol() != null) {
                                            Optional<Column> seqCol = table.getFullSchema().stream()
                                                    .filter(col -> col.getName().equals(table.getSequenceMapCol()))
                                                    .findFirst();
                                            if (!seqCol.isPresent()) {
                                                throw new AnalysisException("sequence column is not contained in"
                                                        + " target table " + table.getName());
                                            }
                                            if (columnToOutput.get(seqCol.get().getName()) != null) {
                                                // should generate diff exprId for seq column
                                                NamedExpression seqColumn = columnToOutput.get(seqCol.get().getName());
                                                if (seqColumn instanceof Alias) {
                                                    seqColumn = new Alias(seqColumn.child(0), column.getName());
                                                } else {
                                                    seqColumn = new Alias(seqColumn, column.getName());
                                                }
                                                columnToOutput.put(column.getName(), seqColumn);
                                            }
                                        } else if (sink.isPartialUpdate()) {
                                            // If the current load is a partial update, the values of unmentioned
                                            // columns will be filled in SegmentWriter. And the output of sink node
                                            // should not contain these unmentioned columns, so we just skip them.
                                            continue;
                                        } else if (column.getDefaultValue() == null) {
                                            // Otherwise, the unmentioned columns should be filled with default values
                                            // or null values
                                            columnToOutput.put(column.getName(), new Alias(
                                                    new NullLiteral(DataType.fromCatalogType(column.getType())),
                                                    column.getName()));
                                        } else {
                                            columnToOutput.put(column.getName(),
                                                    new Alias(Literal.of(column.getDefaultValue())
                                                            .checkedCastTo(DataType.fromCatalogType(column.getType())),
                                                            column.getName()));
                                        }
                                    }
                                }
                            } finally {
                                if (ConnectContext.get() != null) {
                                    // this is a trick way to avoid legacy planner's slotRef toSql output include label
                                    // set back to original value.
                                    ConnectContext.get().getState().setIsQuery(false);
                                }
                            }
                            List<NamedExpression> fullOutputExprs = ImmutableList.copyOf(columnToOutput.values());

                            LogicalProject<?> fullOutputProject = new LogicalProject<>(fullOutputExprs,
                                    boundSink.child());

                            // add cast project
                            List<NamedExpression> castExprs = Lists.newArrayList();
                            for (int i = 0; i < table.getFullSchema().size(); ++i) {
                                Column col = table.getFullSchema().get(i);
                                NamedExpression expr = (NamedExpression) columnToOutput.get(col.getName());
                                if (expr == null) {
                                    // If `expr` is null, it means that the current load is a partial update
                                    // and `col` should not be contained in the output of the sink node so
                                    // we skip it.
                                    continue;
                                }
                                Expression castExpr = TypeCoercionUtils.castIfNotSameType(
                                        expr,
                                        DataType.fromCatalogType(col.getType()));
                                if (castExpr instanceof NamedExpression) {
                                    castExprs.add(((NamedExpression) castExpr));
                                } else {
                                    castExprs.add(new Alias(castExpr));
                                }
                            }
                            if (!castExprs.equals(fullOutputExprs)) {
                                fullOutputProject = new LogicalProject<Plan>(castExprs, fullOutputProject);
                            }

                            return boundSink.withChildAndUpdateOutput(fullOutputProject);

                        })),
                RuleType.BINDING_INSERT_FILE.build(
                        logicalFileSink().when(s -> s.getOutputExprs().isEmpty())
                                .then(fileSink -> fileSink.withOutputExprs(
                                        fileSink.child().getOutput().stream()
                                                .map(NamedExpression.class::cast)
                                                .collect(ImmutableList.toImmutableList())))
                )
        );
    }

    private Pair<Database, OlapTable> bind(CascadesContext cascadesContext, UnboundOlapTableSink<? extends Plan> sink) {
        List<String> tableQualifier = RelationUtil.getQualifierName(cascadesContext.getConnectContext(),
                sink.getNameParts());
        Pair<DatabaseIf, TableIf> pair = RelationUtil.getDbAndTable(tableQualifier,
                cascadesContext.getConnectContext().getEnv());
        if (!(pair.second instanceof OlapTable)) {
            throw new AnalysisException("the target table of insert into is not an OLAP table");
        }
        return Pair.of(((Database) pair.first), (OlapTable) pair.second);
    }

    private List<Long> bindPartitionIds(OlapTable table, List<String> partitions) {
        return partitions.isEmpty()
                ? ImmutableList.of()
                : partitions.stream().map(pn -> {
                    Partition partition = table.getPartition(pn);
                    if (partition == null) {
                        throw new AnalysisException(String.format("partition %s is not found in table %s",
                                pn, table.getName()));
                    }
                    return partition.getId();
                }).collect(Collectors.toList());
    }

    private List<Column> bindTargetColumns(OlapTable table, List<String> colsName) {
        return colsName.isEmpty()
                ? table.getFullSchema().stream().filter(column -> column.isVisible()
                        && !column.isMaterializedViewColumn())
                .collect(Collectors.toList())
                : colsName.stream().map(cn -> {
                    Column column = table.getColumn(cn);
                    if (column == null) {
                        throw new AnalysisException(String.format("column %s is not found in table %s",
                                cn, table.getName()));
                    }
                    return column;
                }).collect(Collectors.toList());
    }

    private static class SlotReplacer extends DefaultExpressionRewriter<Map<String, NamedExpression>> {
        public static final SlotReplacer INSTANCE = new SlotReplacer();

        public Expression replace(Expression e, Map<String, NamedExpression> replaceMap) {
            return e.accept(this, replaceMap);
        }

        @Override
        public Expression visitUnboundSlot(UnboundSlot unboundSlot, Map<String, NamedExpression> replaceMap) {
            return replaceMap.get(unboundSlot.getName());
        }
    }
}
