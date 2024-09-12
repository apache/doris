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
import org.apache.doris.catalog.GeneratedColumnInfo;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Pair;
import org.apache.doris.datasource.hive.HMSExternalDatabase;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.iceberg.IcebergExternalDatabase;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.datasource.jdbc.JdbcExternalDatabase;
import org.apache.doris.datasource.jdbc.JdbcExternalTable;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.analyzer.Scope;
import org.apache.doris.nereids.analyzer.UnboundHiveTableSink;
import org.apache.doris.nereids.analyzer.UnboundIcebergTableSink;
import org.apache.doris.nereids.analyzer.UnboundJdbcTableSink;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.analyzer.UnboundTableSink;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.pattern.MatchingContext;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.DefaultValueSlot;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Substring;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.info.DMLCommandType;
import org.apache.doris.nereids.trees.plans.logical.LogicalHiveTableSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalIcebergTableSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalJdbcTableSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapTableSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalTableSink;
import org.apache.doris.nereids.trees.plans.logical.UnboundLogicalSink;
import org.apache.doris.nereids.trees.plans.visitor.InferPlanOutputAlias;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.coercion.CharacterType;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.RelationUtil;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * bind an unbound logicalTableSink represent the target table of an insert command
 */
public class BindSink implements AnalysisRuleFactory {

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                RuleType.BINDING_INSERT_TARGET_TABLE.build(unboundTableSink().thenApply(this::bindOlapTableSink)),
                RuleType.BINDING_INSERT_FILE.build(logicalFileSink().when(s -> s.getOutputExprs().isEmpty())
                        .then(fileSink -> {
                            ImmutableListMultimap.Builder<ExprId, Integer> exprIdToIndexMapBuilder =
                                    ImmutableListMultimap.builder();
                            List<Slot> childOutput = fileSink.child().getOutput();
                            for (int index = 0; index < childOutput.size(); index++) {
                                exprIdToIndexMapBuilder.put(childOutput.get(index).getExprId(), index);
                            }
                            InferPlanOutputAlias aliasInfer = new InferPlanOutputAlias(childOutput);
                            List<NamedExpression> output = aliasInfer.infer(fileSink.child(),
                                    exprIdToIndexMapBuilder.build());
                            return fileSink.withOutputExprs(output);
                        })
                ),
                // TODO: bind hive target table
                RuleType.BINDING_INSERT_HIVE_TABLE.build(unboundHiveTableSink().thenApply(this::bindHiveTableSink)),
                RuleType.BINDING_INSERT_ICEBERG_TABLE.build(
                    unboundIcebergTableSink().thenApply(this::bindIcebergTableSink)),
                RuleType.BINDING_INSERT_JDBC_TABLE.build(unboundJdbcTableSink().thenApply(this::bindJdbcTableSink))
        );
    }

    private Plan bindOlapTableSink(MatchingContext<UnboundTableSink<Plan>> ctx) {
        UnboundTableSink<?> sink = ctx.root;
        Pair<Database, OlapTable> pair = bind(ctx.cascadesContext, sink);
        Database database = pair.first;
        OlapTable table = pair.second;
        boolean isPartialUpdate = sink.isPartialUpdate() && table.getKeysType() == KeysType.UNIQUE_KEYS;

        LogicalPlan child = ((LogicalPlan) sink.child());
        boolean childHasSeqCol = child.getOutput().stream()
                .anyMatch(slot -> slot.getName().equals(Column.SEQUENCE_COL));
        boolean needExtraSeqCol = isPartialUpdate && !childHasSeqCol && table.hasSequenceCol()
                && table.getSequenceMapCol() != null
                && sink.getColNames().contains(table.getSequenceMapCol());
        Pair<List<Column>, Integer> bindColumnsResult =
                bindTargetColumns(table, sink.getColNames(), childHasSeqCol, needExtraSeqCol,
                        sink.getDMLCommandType() == DMLCommandType.GROUP_COMMIT);
        List<Column> bindColumns = bindColumnsResult.first;
        int extraColumnsNum = bindColumnsResult.second;

        LogicalOlapTableSink<?> boundSink = new LogicalOlapTableSink<>(
                database,
                table,
                bindColumns,
                bindPartitionIds(table, sink.getPartitions(), sink.isTemporaryPartition()),
                child.getOutput().stream()
                        .map(NamedExpression.class::cast)
                        .collect(ImmutableList.toImmutableList()),
                isPartialUpdate,
                sink.getDMLCommandType(),
                child);

        // we need to insert all the columns of the target table
        // although some columns are not mentions.
        // so we add a projects to supply the default value.
        if (boundSink.getCols().size() != child.getOutput().size() + extraColumnsNum) {
            throw new AnalysisException("insert into cols should be corresponding to the query output");
        }

        try {
            // For Unique Key table with sequence column (which default value is not CURRENT_TIMESTAMP),
            // user MUST specify the sequence column while inserting data
            //
            // case1: create table by `function_column.sequence_col`
            //        a) insert with column list, must include the sequence map column
            //        b) insert without column list, already contains the column, don't need to check
            // case2: create table by `function_column.sequence_type`
            //        a) insert with column list, must include the hidden column __DORIS_SEQUENCE_COL__
            //        b) insert without column list, don't include the hidden column __DORIS_SEQUENCE_COL__
            //           by default, will fail.
            if (table.hasSequenceCol()) {
                boolean haveInputSeqCol = false;
                Optional<Column> seqColInTable = Optional.empty();
                if (table.getSequenceMapCol() != null) {
                    if (!sink.getColNames().isEmpty()) {
                        if (sink.getColNames().stream()
                                .anyMatch(c -> c.equalsIgnoreCase(table.getSequenceMapCol()))) {
                            haveInputSeqCol = true; // case1.a
                        }
                    } else {
                        haveInputSeqCol = true; // case1.b
                    }
                    seqColInTable = table.getFullSchema().stream()
                            .filter(col -> col.getName().equalsIgnoreCase(table.getSequenceMapCol()))
                            .findFirst();
                } else {
                    // ATTN: must use bindColumns here. Because of insert into from group_commit tvf submitted by BE
                    //   do not follow any column list with target table, but it contains all inviable data in sink's
                    //   child. THis is different with other insert action that contain non-inviable data by default.
                    if (!bindColumns.isEmpty()) {
                        if (bindColumns.stream()
                                .map(Column::getName)
                                .anyMatch(c -> c.equalsIgnoreCase(Column.SEQUENCE_COL))) {
                            haveInputSeqCol = true; // case2.a
                        } // else case2.b
                    }
                }

                // Don't require user to provide sequence column for partial updates,
                // including the following cases:
                // 1. it's a load job with `partial_columns=true`
                // 2. UPDATE and DELETE, planner will automatically add these hidden columns
                if (!haveInputSeqCol && !isPartialUpdate && (
                        boundSink.getDmlCommandType() != DMLCommandType.UPDATE
                                && boundSink.getDmlCommandType() != DMLCommandType.DELETE)) {
                    if (!seqColInTable.isPresent() || seqColInTable.get().getDefaultValue() == null
                            || !seqColInTable.get().getDefaultValue()
                            .equalsIgnoreCase(DefaultValue.CURRENT_TIMESTAMP)) {
                        throw new org.apache.doris.common.AnalysisException("Table " + table.getName()
                                + " has sequence column, need to specify the sequence column");
                    }
                }
            }
        } catch (Exception e) {
            throw new AnalysisException(e.getMessage(), e.getCause());
        }

        Map<String, NamedExpression> columnToOutput = getColumnToOutput(
                ctx, table, isPartialUpdate, boundSink, child);
        LogicalProject<?> fullOutputProject = getOutputProjectByCoercion(
                table.getFullSchema(), child, columnToOutput);
        return boundSink.withChildAndUpdateOutput(fullOutputProject);
    }

    private LogicalProject<?> getOutputProjectByCoercion(List<Column> tableSchema, LogicalPlan child,
                                                         Map<String, NamedExpression> columnToOutput) {
        List<NamedExpression> fullOutputExprs = ImmutableList.copyOf(columnToOutput.values());
        if (child instanceof LogicalOneRowRelation) {
            // remove default value slot in one row relation
            child = ((LogicalOneRowRelation) child).withProjects(((LogicalOneRowRelation) child)
                    .getProjects().stream()
                    .filter(p -> !(p instanceof DefaultValueSlot))
                    .collect(ImmutableList.toImmutableList()));
        }
        LogicalProject<?> fullOutputProject = new LogicalProject<>(fullOutputExprs, child);

        // add cast project
        List<NamedExpression> castExprs = Lists.newArrayList();
        for (int i = 0; i < tableSchema.size(); ++i) {
            Column col = tableSchema.get(i);
            NamedExpression expr = columnToOutput.get(col.getName());
            if (expr == null) {
                // If `expr` is null, it means that the current load is a partial update
                // and `col` should not be contained in the output of the sink node so
                // we skip it.
                continue;
            }
            DataType inputType = expr.getDataType();
            DataType targetType = DataType.fromCatalogType(tableSchema.get(i).getType());
            Expression castExpr = expr;
            // TODO move string like type logic into TypeCoercionUtils#castIfNotSameType
            if (isSourceAndTargetStringLikeType(inputType, targetType) && !inputType.equals(targetType)) {
                int sourceLength = ((CharacterType) inputType).getLen();
                int targetLength = ((CharacterType) targetType).getLen();
                if (sourceLength == targetLength) {
                    castExpr = TypeCoercionUtils.castIfNotSameType(castExpr, targetType);
                } else if (sourceLength > targetLength && targetLength >= 0) {
                    castExpr = new Substring(castExpr, Literal.of(1), Literal.of(targetLength));
                } else if (targetType.isStringType()) {
                    castExpr = new Cast(castExpr, StringType.INSTANCE);
                }
            } else {
                castExpr = TypeCoercionUtils.castIfNotSameType(castExpr, targetType);
            }
            if (castExpr instanceof NamedExpression) {
                castExprs.add(((NamedExpression) castExpr));
            } else {
                castExprs.add(new Alias(castExpr));
            }
        }
        if (!castExprs.equals(fullOutputExprs)) {
            fullOutputProject = new LogicalProject<Plan>(castExprs, fullOutputProject);
        }
        return fullOutputProject;
    }

    private static Map<String, NamedExpression> getColumnToOutput(
            MatchingContext<? extends UnboundLogicalSink<Plan>> ctx,
            TableIf table, boolean isPartialUpdate, LogicalTableSink<?> boundSink, LogicalPlan child) {
        // we need to insert all the columns of the target table
        // although some columns are not mentions.
        // so we add a projects to supply the default value.
        Map<Column, NamedExpression> columnToChildOutput = Maps.newHashMap();
        for (int i = 0; i < child.getOutput().size(); ++i) {
            columnToChildOutput.put(boundSink.getCols().get(i), child.getOutput().get(i));
        }
        Map<String, NamedExpression> columnToOutput = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        Map<String, NamedExpression> columnToReplaced = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        Map<Expression, Expression> replaceMap = Maps.newHashMap();
        NereidsParser expressionParser = new NereidsParser();
        List<Column> generatedColumns = Lists.newArrayList();
        List<Column> materializedViewColumn = Lists.newArrayList();
        // generate slots not mentioned in sql, mv slots and shaded slots.
        for (Column column : boundSink.getTargetTable().getFullSchema()) {
            if (column.getGeneratedColumnInfo() != null) {
                generatedColumns.add(column);
                continue;
            } else if (column.isMaterializedViewColumn()) {
                materializedViewColumn.add(column);
                continue;
            }
            if (columnToChildOutput.containsKey(column)
                    // do not process explicitly use DEFAULT value here:
                    // insert into table t values(DEFAULT)
                    && !(columnToChildOutput.get(column) instanceof DefaultValueSlot)) {
                Alias output = new Alias(TypeCoercionUtils.castIfNotSameType(
                        columnToChildOutput.get(column), DataType.fromCatalogType(column.getType())),
                        column.getName());
                columnToOutput.put(column.getName(), output);
                columnToReplaced.put(column.getName(), output.toSlot());
                replaceMap.put(output.toSlot(), output.child());
            } else {
                if (table instanceof OlapTable && ((OlapTable) table).hasSequenceCol()
                        && column.getName().equals(Column.SEQUENCE_COL)
                        && ((OlapTable) table).getSequenceMapCol() != null) {
                    Optional<Column> seqCol = table.getFullSchema().stream()
                            .filter(col -> col.getName().equals(((OlapTable) table).getSequenceMapCol()))
                            .findFirst();
                    if (!seqCol.isPresent()) {
                        throw new AnalysisException("sequence column is not contained in"
                                + " target table " + table.getName());
                    }
                    if (columnToOutput.get(seqCol.get().getName()) != null) {
                        // should generate diff exprId for seq column
                        NamedExpression seqColumn = columnToOutput.get(seqCol.get().getName());
                        if (seqColumn instanceof Alias) {
                            seqColumn = new Alias(((Alias) seqColumn).child(), column.getName());
                        } else {
                            seqColumn = new Alias(seqColumn, column.getName());
                        }
                        columnToOutput.put(column.getName(), seqColumn);
                        columnToReplaced.put(column.getName(), seqColumn.toSlot());
                        replaceMap.put(seqColumn.toSlot(), seqColumn.child(0));
                    }
                } else if (isPartialUpdate) {
                    // If the current load is a partial update, the values of unmentioned
                    // columns will be filled in SegmentWriter. And the output of sink node
                    // should not contain these unmentioned columns, so we just skip them.

                    // But if the column has 'on update value', we should unconditionally
                    // update the value of the column to the current timestamp whenever there
                    // is an update on the row
                    if (column.hasOnUpdateDefaultValue()) {
                        Expression unboundFunctionDefaultValue = new NereidsParser().parseExpression(
                                column.getOnUpdateDefaultValueExpr().toSqlWithoutTbl()
                        );
                        Expression defualtValueExpression = ExpressionAnalyzer.analyzeFunction(
                                boundSink, ctx.cascadesContext, unboundFunctionDefaultValue
                        );
                        Alias output = new Alias(TypeCoercionUtils.castIfNotSameType(
                                defualtValueExpression, DataType.fromCatalogType(column.getType())),
                                column.getName());
                        columnToOutput.put(column.getName(), output);
                        columnToReplaced.put(column.getName(), output.toSlot());
                        replaceMap.put(output.toSlot(), output.child());
                    } else {
                        continue;
                    }
                } else if (column.getDefaultValue() == null) {
                    // throw exception if explicitly use Default value but no default value present
                    // insert into table t values(DEFAULT)
                    if (!column.isAllowNull() && !column.isAutoInc()) {
                        throw new AnalysisException("Column has no default value,"
                                + " column=" + column.getName());
                    }
                    // Otherwise, the unmentioned columns should be filled with default values
                    // or null values
                    Alias output = new Alias(new NullLiteral(DataType.fromCatalogType(column.getType())),
                            column.getName());
                    columnToOutput.put(column.getName(), output);
                    columnToReplaced.put(column.getName(), output.toSlot());
                    replaceMap.put(output.toSlot(), output.child());
                } else {
                    try {
                        // it comes from the original planner, if default value expression is
                        // null, we use the literal string of the default value, or it may be
                        // default value function, like CURRENT_TIMESTAMP.
                        if (column.getDefaultValueExpr() == null) {
                            columnToOutput.put(column.getName(),
                                    new Alias(Literal.of(column.getDefaultValue())
                                            .checkedCastTo(DataType.fromCatalogType(column.getType())),
                                            column.getName()));
                        } else {
                            Expression unboundDefaultValue = new NereidsParser().parseExpression(
                                    column.getDefaultValueExpr().toSqlWithoutTbl());
                            Expression defualtValueExpression = ExpressionAnalyzer.analyzeFunction(
                                    boundSink, ctx.cascadesContext, unboundDefaultValue);
                            if (defualtValueExpression instanceof Alias) {
                                defualtValueExpression = ((Alias) defualtValueExpression).child();
                            }
                            Alias output = new Alias((TypeCoercionUtils.castIfNotSameType(
                                    defualtValueExpression, DataType.fromCatalogType(column.getType()))),
                                    column.getName());
                            columnToOutput.put(column.getName(), output);
                            columnToReplaced.put(column.getName(), output.toSlot());
                            replaceMap.put(output.toSlot(), output.child());
                        }
                    } catch (Exception e) {
                        throw new AnalysisException(e.getMessage(), e.getCause());
                    }
                }
            }
        }
        // the generated columns can use all ordinary columns,
        // if processed in upper for loop, will lead to not found slot error
        // It's the same reason for moving the processing of materialized columns down.
        for (Column column : generatedColumns) {
            GeneratedColumnInfo info = column.getGeneratedColumnInfo();
            Expression parsedExpression = new NereidsParser().parseExpression(info.getExpr().toSqlWithoutTbl());
            Expression boundExpression = new CustomExpressionAnalyzer(boundSink, ctx.cascadesContext, columnToReplaced)
                    .analyze(parsedExpression);
            if (boundExpression instanceof Alias) {
                boundExpression = ((Alias) boundExpression).child();
            }
            boundExpression = ExpressionUtils.replace(boundExpression, replaceMap);
            Alias output = new Alias(boundExpression, info.getExprSql());
            columnToOutput.put(column.getName(), output);
            columnToReplaced.put(column.getName(), output.toSlot());
            replaceMap.put(output.toSlot(), output.child());
        }
        for (Column column : materializedViewColumn) {
            if (column.isMaterializedViewColumn()) {
                List<SlotRef> refs = column.getRefColumns();
                // now we have to replace the column to slots.
                Preconditions.checkArgument(refs != null,
                        "mv column %s 's ref column cannot be null", column);
                Expression parsedExpression = expressionParser.parseExpression(
                        column.getDefineExpr().toSqlWithoutTbl());
                // the boundSlotExpression is an expression whose slots are bound but function
                // may not be bound, we have to bind it again.
                // for example: to_bitmap.
                Expression boundExpression = new CustomExpressionAnalyzer(
                        boundSink, ctx.cascadesContext, columnToReplaced).analyze(parsedExpression);
                if (boundExpression instanceof Alias) {
                    boundExpression = ((Alias) boundExpression).child();
                }
                boundExpression = ExpressionUtils.replace(boundExpression, replaceMap);
                boundExpression = TypeCoercionUtils.castIfNotSameType(boundExpression,
                        DataType.fromCatalogType(column.getType()));
                Alias output = new Alias(boundExpression, column.getDefineExpr().toSqlWithoutTbl());
                columnToOutput.put(column.getName(), output);
            }
        }
        return columnToOutput;
    }

    private Plan bindHiveTableSink(MatchingContext<UnboundHiveTableSink<Plan>> ctx) {
        UnboundHiveTableSink<?> sink = ctx.root;
        Pair<HMSExternalDatabase, HMSExternalTable> pair = bind(ctx.cascadesContext, sink);
        HMSExternalDatabase database = pair.first;
        HMSExternalTable table = pair.second;
        LogicalPlan child = ((LogicalPlan) sink.child());

        if (!sink.getPartitions().isEmpty()) {
            throw new AnalysisException("Not support insert with partition spec in hive catalog.");
        }

        List<Column> bindColumns;
        if (sink.getColNames().isEmpty()) {
            bindColumns = table.getBaseSchema(true).stream().collect(ImmutableList.toImmutableList());
        } else {
            bindColumns = sink.getColNames().stream().map(cn -> {
                Column column = table.getColumn(cn);
                if (column == null) {
                    throw new AnalysisException(String.format("column %s is not found in table %s",
                            cn, table.getName()));
                }
                return column;
            }).collect(ImmutableList.toImmutableList());
        }
        LogicalHiveTableSink<?> boundSink = new LogicalHiveTableSink<>(
                database,
                table,
                bindColumns,
                child.getOutput().stream()
                    .map(NamedExpression.class::cast)
                    .collect(ImmutableList.toImmutableList()),
                sink.getDMLCommandType(),
                Optional.empty(),
                Optional.empty(),
                child);
        // we need to insert all the columns of the target table
        if (boundSink.getCols().size() != child.getOutput().size()) {
            throw new AnalysisException("insert into cols should be corresponding to the query output");
        }
        Map<String, NamedExpression> columnToOutput = getColumnToOutput(ctx, table, false,
                boundSink, child);
        LogicalProject<?> fullOutputProject = getOutputProjectByCoercion(table.getFullSchema(), child, columnToOutput);
        return boundSink.withChildAndUpdateOutput(fullOutputProject);
    }

    private Plan bindIcebergTableSink(MatchingContext<UnboundIcebergTableSink<Plan>> ctx) {
        UnboundIcebergTableSink<?> sink = ctx.root;
        Pair<IcebergExternalDatabase, IcebergExternalTable> pair = bind(ctx.cascadesContext, sink);
        IcebergExternalDatabase database = pair.first;
        IcebergExternalTable table = pair.second;
        LogicalPlan child = ((LogicalPlan) sink.child());

        List<Column> bindColumns;
        if (sink.getColNames().isEmpty()) {
            bindColumns = table.getBaseSchema(true).stream().collect(ImmutableList.toImmutableList());
        } else {
            bindColumns = sink.getColNames().stream().map(cn -> {
                Column column = table.getColumn(cn);
                if (column == null) {
                    throw new AnalysisException(String.format("column %s is not found in table %s",
                            cn, table.getName()));
                }
                return column;
            }).collect(ImmutableList.toImmutableList());
        }
        LogicalIcebergTableSink<?> boundSink = new LogicalIcebergTableSink<>(
                database,
                table,
                bindColumns,
                child.getOutput().stream()
                        .map(NamedExpression.class::cast)
                        .collect(ImmutableList.toImmutableList()),
                sink.getDMLCommandType(),
                Optional.empty(),
                Optional.empty(),
                child);
        // we need to insert all the columns of the target table
        if (boundSink.getCols().size() != child.getOutput().size()) {
            throw new AnalysisException("insert into cols should be corresponding to the query output");
        }
        Map<String, NamedExpression> columnToOutput = getColumnToOutput(ctx, table, false,
                boundSink, child);
        LogicalProject<?> fullOutputProject = getOutputProjectByCoercion(table.getFullSchema(), child, columnToOutput);
        return boundSink.withChildAndUpdateOutput(fullOutputProject);
    }

    private Plan bindJdbcTableSink(MatchingContext<UnboundJdbcTableSink<Plan>> ctx) {
        UnboundJdbcTableSink<?> sink = ctx.root;
        Pair<JdbcExternalDatabase, JdbcExternalTable> pair = bind(ctx.cascadesContext, sink);
        JdbcExternalDatabase database = pair.first;
        JdbcExternalTable table = pair.second;
        LogicalPlan child = ((LogicalPlan) sink.child());

        List<Column> bindColumns;
        if (sink.getColNames().isEmpty()) {
            bindColumns = table.getBaseSchema(true).stream().collect(ImmutableList.toImmutableList());
        } else {
            bindColumns = sink.getColNames().stream().map(cn -> {
                Column column = table.getColumn(cn);
                if (column == null) {
                    throw new AnalysisException(String.format("column %s is not found in table %s",
                            cn, table.getName()));
                }
                return column;
            }).collect(ImmutableList.toImmutableList());
        }
        LogicalJdbcTableSink<?> boundSink = new LogicalJdbcTableSink<>(
                database,
                table,
                bindColumns,
                child.getOutput().stream()
                        .map(NamedExpression.class::cast)
                        .collect(ImmutableList.toImmutableList()),
                sink.getDMLCommandType(),
                Optional.empty(),
                Optional.empty(),
                child);
        // we need to insert all the columns of the target table
        if (boundSink.getCols().size() != child.getOutput().size()) {
            throw new AnalysisException("insert into cols should be corresponding to the query output");
        }
        Map<String, NamedExpression> columnToOutput = getJdbcColumnToOutput(bindColumns, child);
        // We don't need to insert unmentioned columns, only user specified columns
        LogicalProject<?> outputProject = getOutputProjectByCoercion(bindColumns, child, columnToOutput);
        return boundSink.withChildAndUpdateOutput(outputProject);
    }

    private static Map<String, NamedExpression> getJdbcColumnToOutput(
            List<Column> bindColumns, LogicalPlan child) {
        Map<String, NamedExpression> columnToOutput = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);

        for (int i = 0; i < bindColumns.size(); i++) {
            Column column = bindColumns.get(i);
            NamedExpression outputExpr = child.getOutput().get(i);
            Alias output = new Alias(
                    TypeCoercionUtils.castIfNotSameType(outputExpr, DataType.fromCatalogType(column.getType())),
                    column.getName()
            );
            columnToOutput.put(column.getName(), output);
        }

        return columnToOutput;
    }

    private Pair<Database, OlapTable> bind(CascadesContext cascadesContext, UnboundTableSink<? extends Plan> sink) {
        List<String> tableQualifier = RelationUtil.getQualifierName(cascadesContext.getConnectContext(),
                sink.getNameParts());
        Pair<DatabaseIf<?>, TableIf> pair = RelationUtil.getDbAndTable(tableQualifier,
                cascadesContext.getConnectContext().getEnv());
        if (!(pair.second instanceof OlapTable)) {
            throw new AnalysisException("the target table of insert into is not an OLAP table");
        }
        return Pair.of(((Database) pair.first), (OlapTable) pair.second);
    }

    private Pair<HMSExternalDatabase, HMSExternalTable> bind(CascadesContext cascadesContext,
                                                             UnboundHiveTableSink<? extends Plan> sink) {
        List<String> tableQualifier = RelationUtil.getQualifierName(cascadesContext.getConnectContext(),
                sink.getNameParts());
        Pair<DatabaseIf<?>, TableIf> pair = RelationUtil.getDbAndTable(tableQualifier,
                cascadesContext.getConnectContext().getEnv());
        if (pair.second instanceof HMSExternalTable) {
            HMSExternalTable table = (HMSExternalTable) pair.second;
            if (table.getDlaType() == HMSExternalTable.DLAType.HIVE) {
                return Pair.of(((HMSExternalDatabase) pair.first), table);
            }
        }
        throw new AnalysisException("the target table of insert into is not a Hive table");
    }

    private Pair<IcebergExternalDatabase, IcebergExternalTable> bind(CascadesContext cascadesContext,
                                                                     UnboundIcebergTableSink<? extends Plan> sink) {
        List<String> tableQualifier = RelationUtil.getQualifierName(cascadesContext.getConnectContext(),
                sink.getNameParts());
        Pair<DatabaseIf<?>, TableIf> pair = RelationUtil.getDbAndTable(tableQualifier,
                cascadesContext.getConnectContext().getEnv());
        if (pair.second instanceof IcebergExternalTable) {
            return Pair.of(((IcebergExternalDatabase) pair.first), (IcebergExternalTable) pair.second);
        }
        throw new AnalysisException("the target table of insert into is not an iceberg table");
    }

    private Pair<JdbcExternalDatabase, JdbcExternalTable> bind(CascadesContext cascadesContext,
            UnboundJdbcTableSink<? extends Plan> sink) {
        List<String> tableQualifier = RelationUtil.getQualifierName(cascadesContext.getConnectContext(),
                sink.getNameParts());
        Pair<DatabaseIf<?>, TableIf> pair = RelationUtil.getDbAndTable(tableQualifier,
                cascadesContext.getConnectContext().getEnv());
        if (pair.second instanceof JdbcExternalTable) {
            return Pair.of(((JdbcExternalDatabase) pair.first), (JdbcExternalTable) pair.second);
        }
        throw new AnalysisException("the target table of insert into is not an jdbc table");
    }

    private List<Long> bindPartitionIds(OlapTable table, List<String> partitions, boolean temp) {
        return partitions.isEmpty()
                ? ImmutableList.of()
                : partitions.stream().map(pn -> {
                    Partition partition = table.getPartition(pn, temp);
                    if (partition == null) {
                        throw new AnalysisException(String.format("partition %s is not found in table %s",
                                pn, table.getName()));
                    }
                    return partition.getId();
                }).collect(Collectors.toList());
    }

    private Pair<List<Column>, Integer> bindTargetColumns(OlapTable table, List<String> colsName,
            boolean childHasSeqCol, boolean needExtraSeqCol, boolean isGroupCommit) {
        // if the table set sequence column in stream load phase, the sequence map column is null, we query it.
        if (colsName.isEmpty()) {
            // ATTN: group commit without column list should return all base index column
            //   because it already prepares data for these columns.
            return Pair.of(table.getBaseSchema(true).stream()
                    .filter(c -> isGroupCommit || validColumn(c, childHasSeqCol))
                    .collect(ImmutableList.toImmutableList()), 0);
        } else {
            int extraColumnsNum = (needExtraSeqCol ? 1 : 0);
            List<String> processedColsName = Lists.newArrayList(colsName);
            for (Column col : table.getFullSchema()) {
                if (col.hasOnUpdateDefaultValue()) {
                    Optional<String> colName = colsName.stream().filter(c -> c.equals(col.getName())).findFirst();
                    if (!colName.isPresent()) {
                        ++extraColumnsNum;
                        processedColsName.add(col.getName());
                    }
                } else if (col.getGeneratedColumnInfo() != null) {
                    ++extraColumnsNum;
                    processedColsName.add(col.getName());
                }
            }
            if (!processedColsName.contains(Column.SEQUENCE_COL) && (childHasSeqCol || needExtraSeqCol)) {
                processedColsName.add(Column.SEQUENCE_COL);
            }
            return Pair.of(processedColsName.stream().map(cn -> {
                Column column = table.getColumn(cn);
                if (column == null) {
                    throw new AnalysisException(String.format("column %s is not found in table %s",
                            cn, table.getName()));
                }
                return column;
            }).collect(ImmutableList.toImmutableList()), extraColumnsNum);
        }
    }

    private boolean isSourceAndTargetStringLikeType(DataType input, DataType target) {
        return input.isStringLikeType() && target.isStringLikeType();
    }

    private boolean validColumn(Column column, boolean isNeedSequenceCol) {
        return (column.isVisible() || (isNeedSequenceCol && column.isSequenceColumn()))
                && !column.isMaterializedViewColumn();
    }

    private static class CustomExpressionAnalyzer extends ExpressionAnalyzer {
        private Map<String, NamedExpression> slotBinder;

        public CustomExpressionAnalyzer(
                Plan currentPlan, CascadesContext cascadesContext, Map<String, NamedExpression> slotBinder) {
            super(currentPlan, new Scope(ImmutableList.of()), cascadesContext, false, false);
            this.slotBinder = slotBinder;
        }

        @Override
        public Expression visitUnboundSlot(UnboundSlot unboundSlot, ExpressionRewriteContext context) {
            if (!slotBinder.containsKey(unboundSlot.getName())) {
                throw new AnalysisException("cannot find column from target table " + unboundSlot.getNameParts());
            }
            return slotBinder.get(unboundSlot.getName());
        }
    }
}
