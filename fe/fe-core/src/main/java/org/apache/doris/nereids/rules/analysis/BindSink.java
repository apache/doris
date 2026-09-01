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
import org.apache.doris.analysis.ExprToSqlVisitor;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.ToSqlParams;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.GeneratedColumnInfo;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Config;
import org.apache.doris.common.IdGenerator;
import org.apache.doris.common.Pair;
import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.doris.RemoteDorisExternalTable;
import org.apache.doris.datasource.plugin.PluginDrivenExternalCatalog;
import org.apache.doris.datasource.plugin.PluginDrivenExternalTable;
import org.apache.doris.datasource.plugin.PluginDrivenMetadata;
import org.apache.doris.dictionary.Dictionary;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.Scope;
import org.apache.doris.nereids.analyzer.UnboundBlackholeSink;
import org.apache.doris.nereids.analyzer.UnboundConnectorTableSink;
import org.apache.doris.nereids.analyzer.UnboundDictionarySink;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.analyzer.UnboundTVFTableSink;
import org.apache.doris.nereids.analyzer.UnboundTableSink;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.pattern.MatchingContext;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.analysis.SessionVarGuardRewriter.AddSessionVarGuardRewriter;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.DefaultValueSlot;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Substring;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.info.DMLCommandType;
import org.apache.doris.nereids.trees.plans.logical.LogicalBlackholeSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalConnectorTableSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalDictionarySink;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapTableSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalTVFTableSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalTableSink;
import org.apache.doris.nereids.trees.plans.logical.UnboundLogicalSink;
import org.apache.doris.nereids.trees.plans.visitor.InferPlanOutputAlias;
import org.apache.doris.nereids.types.ConnectorComputeVariantType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.JsonType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.VariantType;
import org.apache.doris.nereids.types.coercion.CharacterType;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.RelationUtil;
import org.apache.doris.nereids.util.TypeCoercionUtils;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.qe.AutoCloseSessionVariable;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.thrift.TPartialUpdateNewRowPolicy;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * bind an unbound logicalTableSink represent the target table of an insert command
 */
public class BindSink implements AnalysisRuleFactory {
    private static final Logger LOG = LogManager.getLogger(BindSink.class);

    public boolean needTruncateStringWhenInsert;

    public BindSink() {
        this(true);
    }

    public BindSink(boolean needTruncateStringWhenInsert) {
        this.needTruncateStringWhenInsert = needTruncateStringWhenInsert;
    }

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
                RuleType.BINDING_INSERT_CONNECTOR_TABLE.build(
                    unboundConnectorTableSink().thenApply(this::bindConnectorTableSink)),
                RuleType.BINDING_INSERT_DICTIONARY_TABLE
                        .build(unboundDictionarySink().thenApply(this::bindDictionarySink)),
                RuleType.BINDING_INSERT_BLACKHOLE_SINK.build(unboundBlackholeSink().thenApply(this::bindBlackHoleSink)),
                RuleType.BINDING_INSERT_TVF_TABLE.build(unboundTVFTableSink().thenApply(this::bindTVFTableSink))
                );
    }

    private Plan bindOlapTableSink(MatchingContext<UnboundTableSink<Plan>> ctx) {
        UnboundTableSink<?> sink = ctx.root;
        Pair<DatabaseIf, OlapTable> pair = bind(ctx.cascadesContext, sink);
        DatabaseIf database = pair.first;
        OlapTable table = pair.second;
        boolean isPartialUpdate = sink.isPartialUpdate() && table.getKeysType() == KeysType.UNIQUE_KEYS;
        boolean isDeletePartialUpdate = isPartialUpdate && sink.getDMLCommandType() == DMLCommandType.DELETE;
        TPartialUpdateNewRowPolicy partialUpdateNewKeyPolicy = sink.getPartialUpdateNewRowPolicy();

        LogicalPlan child = ((LogicalPlan) sink.child());
        boolean childHasSeqCol = child.getOutput().stream()
                .anyMatch(slot -> slot.getName().equals(Column.SEQUENCE_COL));
        boolean needExtraSeqCol = isPartialUpdate && !childHasSeqCol && table.hasSequenceCol()
                && table.getSequenceMapCol() != null
                && sink.getColNames().contains(table.getSequenceMapCol());
        // 1. bind target columns: from sink's column names to target tables' Columns
        Pair<List<Column>, Integer> bindColumnsResult =
                bindTargetColumns(table, sink.getColNames(), childHasSeqCol, needExtraSeqCol,
                        sink.getDMLCommandType() == DMLCommandType.GROUP_COMMIT, isDeletePartialUpdate);
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
                partialUpdateNewKeyPolicy,
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
                // 3. session value `require_sequence_in_insert` is false
                if (!haveInputSeqCol && !isPartialUpdate && (
                        boundSink.getDmlCommandType() != DMLCommandType.UPDATE
                                && boundSink.getDmlCommandType() != DMLCommandType.DELETE) && (
                        boundSink.getDmlCommandType() != DMLCommandType.INSERT
                                || ConnectContext.get().getSessionVariable().isRequireSequenceInInsert())) {
                    if (!seqColInTable.isPresent() || seqColInTable.get().getDefaultValue() == null
                            || !DefaultValue.isCurrentTimeStampDefaultValue(seqColInTable.get().getDefaultValue())) {
                        throw new org.apache.doris.common.AnalysisException("Table " + table.getName()
                                + " has sequence column, need to specify the sequence column");
                    }
                }
            }
        } catch (Exception e) {
            throw new AnalysisException(e.getMessage(), e.getCause());
        }

        Map<String, NamedExpression> columnToOutput = getColumnToOutput(
                ctx, table, isPartialUpdate, isDeletePartialUpdate, boundSink, child);
        LogicalProject<?> fullOutputProject = getOutputProjectByCoercion(
                table.getFullSchema(), child, columnToOutput);
        List<Column> columns = new ArrayList<>(table.getFullSchema().size());
        for (int i = 0; i < table.getFullSchema().size(); ++i) {
            Column col = table.getFullSchema().get(i);
            if (columnToOutput.get(col.getName()) != null) {
                columns.add(col);
            }
        }
        if (fullOutputProject.getOutputs().size() != columns.size()) {
            throw new AnalysisException("output's size should be same as columns's size");
        }

        int size = columns.size();
        List<Slot> targetTableSlots = new ArrayList<>(size);
        IdGenerator<ExprId> exprIdGenerator = StatementScopeIdGenerator.getExprIdGenerator();
        for (int i = 0; i < size; ++i) {
            targetTableSlots.add(SlotReference.fromColumn(
                    exprIdGenerator.getNextId(), table, columns.get(i), table.getFullQualifiers())
            );
        }
        LegacyExprTranslator exprTranslator = new LegacyExprTranslator(table, targetTableSlots);
        return boundSink.withChildAndUpdateOutput(fullOutputProject, exprTranslator.createPartitionExprList(),
                exprTranslator.createSyncMvWhereClause(), targetTableSlots);
    }

    private LogicalProject<?> getOutputProjectByCoercion(List<Column> tableSchema, LogicalPlan child,
                                                         Map<String, NamedExpression> columnToOutput) {
        List<NamedExpression> fullOutputExprs = Utils.fastToImmutableList(columnToOutput.values());
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
        ConnectContext connCtx = ConnectContext.get();
        final boolean truncateString = needTruncateStringWhenInsert
                && (connCtx == null || connCtx.getSessionVariable().enableInsertValueAutoCast)
                && !SessionVariable.enableStrictCast();
        for (int i = 0; i < tableSchema.size(); ++i) {
            Column col = tableSchema.get(i);
            NamedExpression expr = columnToOutput.get(col.getName()); // relative outputExpr
            if (expr == null) {
                // If `expr` is null, it means that the current load is a partial update
                // and `col` should not be contained in the output of the sink node so
                // we skip it.
                continue;
            }
            expr = expr.toSlot();
            DataType inputType = expr.getDataType();
            DataType targetType = DataType.fromCatalogType(tableSchema.get(i).getType());
            Expression castExpr = expr;
            // TODO move string like type logic into TypeCoercionUtils#castIfNotSameType
            if (isSourceAndTargetStringLikeType(inputType, targetType) && !inputType.equals(targetType)) {
                int sourceLength = ((CharacterType) inputType).getLen();
                int targetLength = ((CharacterType) targetType).getLen();
                if (sourceLength == targetLength) {
                    castExpr = TypeCoercionUtils.castIfNotSameType(castExpr, targetType);
                } else if (truncateString && targetLength >= 0
                        && (sourceLength < 0 || sourceLength > targetLength)) {
                    // sourceLength < 0 means the source is an unbounded string like type
                    // (e.g. text/string whose getLen() returns -1), which is always longer
                    // than a bounded char/varchar target and therefore needs truncation.
                    castExpr = new Substring(castExpr, Literal.of(1), Literal.of(targetLength));
                } else if (targetType.isStringType()) {
                    castExpr = new Cast(castExpr, StringType.INSTANCE);
                }
            } else {
                castExpr = coerceSinkExpression(castExpr, targetType);
            }
            if (castExpr instanceof NamedExpression) {
                castExprs.add(((NamedExpression) castExpr));
            } else {
                // use expr's original name as alias name
                // so that the LogicalPostFilter node in stream load can bind its slot successfully
                castExprs.add(new Alias(castExpr, expr.getName()));
            }
        }
        if (!castExprs.equals(fullOutputExprs)) {
            fullOutputProject = new LogicalProject<Plan>(castExprs, fullOutputProject);
        }
        return fullOutputProject;
    }

    @VisibleForTesting
    static Expression coerceSinkExpression(Expression expression, DataType targetType) {
        if (!Config.enable_variant_v2
                && expression.getDataType() instanceof ConnectorComputeVariantType
                && targetType instanceof VariantType
                && !(targetType instanceof ConnectorComputeVariantType)) {
            // JSONB is the executable carrier shared by compute-only V2 and legacy Variant;
            // a direct cast crosses incompatible physical columns at CTAS/MTMV sink boundaries.
            return new Cast(new Cast(expression, JsonType.INSTANCE), targetType);
        }
        return TypeCoercionUtils.castIfNotSameType(expression, targetType);
    }

    private static Map<String, NamedExpression> getColumnToOutput(
            MatchingContext<? extends UnboundLogicalSink<Plan>> ctx,
            TableIf table, boolean isPartialUpdate, boolean isDeletePartialUpdate,
            LogicalTableSink<?> boundSink, LogicalPlan child) {
        return getColumnToOutput(ctx, table, isPartialUpdate, isDeletePartialUpdate,
                boundSink, child, sinkTargetFullSchema(boundSink.getTargetTable()));
    }

    private static Map<String, NamedExpression> getColumnToOutput(
            MatchingContext<? extends UnboundLogicalSink<Plan>> ctx,
            TableIf table, boolean isPartialUpdate, boolean isDeletePartialUpdate,
            LogicalTableSink<?> boundSink, LogicalPlan child, List<Column> targetSchema) {
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
        List<Column> shadowColumns = Lists.newArrayList();
        // generate slots not mentioned in sql, mv slots and shaded slots.
        for (Column column : targetSchema) {
            if (column.isGeneratedColumn()) {
                generatedColumns.add(column);
                continue;
            } else if (column.isMaterializedViewColumn()) {
                materializedViewColumn.add(column);
                continue;
            } else if (Column.isShadowColumn(column.getName())) {
                shadowColumns.add(column);
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
                                column.getOnUpdateDefaultValueSql()
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
                } else if (column.getDefaultValue() == null
                        && column.getDefaultValueSql() == null) {
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
                        Expression unboundDefaultValue = new NereidsParser().parseExpression(
                                column.getDefaultValueSql());
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
            if (isDeletePartialUpdate) {
                NamedExpression childOutput = columnToChildOutput.get(column);
                if (childOutput == null) {
                    continue;
                }
                Alias output = new Alias(TypeCoercionUtils.castIfNotSameType(
                        childOutput, DataType.fromCatalogType(column.getType())), column.getName());
                columnToOutput.put(column.getName(), output);
                columnToReplaced.put(column.getName(), output.toSlot());
                replaceMap.put(output.toSlot(), output.child());
                continue;
            }
            Map<String, String> currentSessionVars =
                    ctx.connectContext.getSessionVariable().getAffectQueryResultInPlanVariables();
            try (AutoCloseSessionVariable autoClose = new AutoCloseSessionVariable(ctx.connectContext,
                    column.getSessionVariables())) {
                GeneratedColumnInfo info = column.getGeneratedColumnInfo();
                Expression parsedExpression = new NereidsParser().parseExpression(
                        info.getExpr().accept(ExprToSqlVisitor.INSTANCE, ToSqlParams.WITHOUT_TABLE));
                Expression boundExpression = new CustomExpressionAnalyzer(boundSink, ctx.cascadesContext,
                        columnToReplaced)
                        .analyze(parsedExpression);
                if (boundExpression instanceof Alias) {
                    boundExpression = ((Alias) boundExpression).child();
                }
                boundExpression = ExpressionUtils.replace(boundExpression, replaceMap);
                if (!SessionVarGuardRewriter.checkSessionVariablesMatch(
                        currentSessionVars, column.getSessionVariables())) {
                    boundExpression = boundExpression.accept(
                            new AddSessionVarGuardRewriter(column.getSessionVariables()), Boolean.FALSE);
                }
                Alias output = new Alias(boundExpression, column.getName());
                columnToOutput.put(column.getName(), output);
                columnToReplaced.put(column.getName(), output.toSlot());
                replaceMap.put(output.toSlot(), output.child());
            }
        }
        for (Column column : materializedViewColumn) {
            List<SlotRef> refs = column.getRefColumns();
            // now we have to replace the column to slots.
            Preconditions.checkArgument(refs != null,
                    "mv column %s 's ref column cannot be null", column);
            Map<String, String> currentSessionVars =
                    ctx.connectContext.getSessionVariable().getAffectQueryResultInPlanVariables();
            try (AutoCloseSessionVariable autoClose = new AutoCloseSessionVariable(ctx.connectContext,
                    column.getSessionVariables())) {
                Expression parsedExpression = expressionParser.parseExpression(
                        column.getDefineExpr().accept(ExprToSqlVisitor.INSTANCE, ToSqlParams.WITHOUT_TABLE));
                // the boundSlotExpression is an expression whose slots are bound but function
                // may not be bound, we have to bind it again.
                // for example: to_bitmap.
                Expression boundExpression = new CustomExpressionAnalyzer(
                        boundSink, ctx.cascadesContext, columnToReplaced).analyze(parsedExpression);
                if (boundExpression instanceof Alias) {
                    boundExpression = ((Alias) boundExpression).child();
                }
                boundExpression = ExpressionUtils.replace(boundExpression, replaceMap);
                if (!SessionVarGuardRewriter.checkSessionVariablesMatch(
                        currentSessionVars, column.getSessionVariables())) {
                    boundExpression = boundExpression.accept(
                            new AddSessionVarGuardRewriter(column.getSessionVariables()), Boolean.FALSE);
                }
                boundExpression = TypeCoercionUtils.castIfNotSameType(boundExpression,
                        DataType.fromCatalogType(column.getType()));
                Alias output = new Alias(boundExpression, column.getDefineExpr().accept(
                        ExprToSqlVisitor.INSTANCE, ToSqlParams.WITHOUT_TABLE));
                columnToOutput.put(column.getName(), output);
                columnToReplaced.put(column.getName(), output.toSlot());
                replaceMap.put(output.toSlot(), output.child());
            }
        }
        for (Column column : shadowColumns) {
            NamedExpression expression = columnToOutput.get(column.getNonShadowName());
            if (expression != null) {
                Alias alias = (Alias) expression;
                Expression newExpr = TypeCoercionUtils.castIfNotSameType(alias.child(),
                        DataType.fromCatalogType(column.getType()));
                columnToOutput.put(column.getName(), new Alias(newExpr, column.getName()));
            }
        }
        return columnToOutput;
    }

    private Plan bindBlackHoleSink(MatchingContext<UnboundBlackholeSink<Plan>> ctx) {
        UnboundBlackholeSink<?> sink = ctx.root;
        LogicalPlan child = ((LogicalPlan) sink.child());
        if (sink.getContext().isForWarmUp() && Config.isNotCloudMode() && child.containsType(LogicalOlapScan.class)) {
            throw new AnalysisException("WARM UP SELECT doesn't support olap table in non-cloud mode.");
        }
        LogicalBlackholeSink<?> boundSink = new LogicalBlackholeSink<>(
                child.getOutput().stream()
                        .map(NamedExpression.class::cast)
                        .collect(ImmutableList.toImmutableList()),
                Optional.empty(),
                Optional.empty(),
                child);
        return boundSink;
    }

    private Plan bindTVFTableSink(MatchingContext<UnboundTVFTableSink<Plan>> ctx) {
        UnboundTVFTableSink<?> sink = ctx.root;
        String tvfName = sink.getTvfName().toLowerCase();
        Map<String, String> properties = sink.getProperties();

        // Validate tvfName
        if (!tvfName.equals("local") && !tvfName.equals("s3") && !tvfName.equals("hdfs")) {
            throw new AnalysisException(
                    "INSERT INTO TVF only supports local/s3/hdfs, but got: " + tvfName);
        }

        // Validate required properties
        if (!properties.containsKey("file_path")) {
            throw new AnalysisException("TVF sink requires 'file_path' property");
        }
        if (!properties.containsKey("format")) {
            throw new AnalysisException("TVF sink requires 'format' property");
        }
        if (tvfName.equals("local") && !properties.containsKey("backend_id")) {
            throw new AnalysisException("local TVF sink requires 'backend_id' property");
        }

        // Validate file_path must not contain wildcards
        String filePath = properties.get("file_path");
        if (filePath.contains("*") || filePath.contains("?") || filePath.contains("[")) {
            throw new AnalysisException(
                    "TVF sink file_path must not contain wildcards: " + filePath);
        }

        // local TVF does not support delete_existing_files=true
        boolean deleteExisting = Boolean.parseBoolean(
                properties.getOrDefault("delete_existing_files", "false"));
        if (tvfName.equals("local") && deleteExisting) {
            throw new AnalysisException(
                    "delete_existing_files=true is not supported for local TVF");
        }

        LogicalPlan child = ((LogicalPlan) sink.child());

        // Always derive schema from child query output
        List<Column> cols = child.getOutput().stream()
                .map(slot -> new Column(slot.getName(), slot.getDataType().toCatalogDataType()))
                .collect(ImmutableList.toImmutableList());

        // Validate column count
        if (cols.size() != child.getOutput().size()) {
            throw new AnalysisException(
                    "insert into cols should be corresponding to the query output"
                            + ", target columns: " + cols.size()
                            + ", query output: " + child.getOutput().size());
        }

        // Build columnToOutput mapping and reuse getOutputProjectByCoercion for type cast,
        // same as OlapTable INSERT INTO.
        Map<String, NamedExpression> columnToOutput = Maps.newLinkedHashMap();
        for (int i = 0; i < cols.size(); i++) {
            Column col = cols.get(i);
            NamedExpression childExpr = (NamedExpression) child.getOutput().get(i);
            Alias output = new Alias(TypeCoercionUtils.castIfNotSameType(
                    childExpr, DataType.fromCatalogType(col.getType())), col.getName());
            columnToOutput.put(col.getName(), output);
        }
        LogicalProject<?> projectWithCast = getOutputProjectByCoercion(cols, child, columnToOutput);

        List<NamedExpression> outputExprs = projectWithCast.getOutput().stream()
                .map(NamedExpression.class::cast)
                .collect(ImmutableList.toImmutableList());

        return new LogicalTVFTableSink<>(tvfName, properties, cols, outputExprs,
                Optional.empty(), Optional.empty(), projectWithCast);
    }

    /**
     * Returns the schema of a write target without inheriting a snapshot pinned by a source relation.
     *
     * <p>An INSERT may read an older version of the same connector table. The no-arg external-table schema
     * lookup consults that statement-level ambient pin, but a sink is not that source reference and must bind
     * against one coherent latest write-schema generation. Non-connector tables retain their existing lookup.</p>
     */
    private static List<Column> sinkTargetFullSchema(TableIf table) {
        if (table instanceof PluginDrivenExternalTable) {
            // Schema, synthetic columns, and write identity must come from the same cache value.
            return ((PluginDrivenExternalTable) table).getWriteSchemaSnapshot().getFullSchema();
        }
        return table.getFullSchema();
    }

    /**
     * Connector analogue of the retired legacy iceberg static-partition validation: validates a
     * flipped-connector table's
     * static-partition spec through the neutral {@code ConnectorMetadata#validateStaticPartitionColumns} SPI, so
     * the partition-spec knowledge (unknown column / non-identity transform / unpartitioned) and its messages
     * stay in the connector (iceberg). A connector {@link DorisConnectorException} is surfaced as the
     * analysis-time {@link AnalysisException} the legacy native path threw, preserving the user-facing message
     * and the exception type. The literal-value check is connector-agnostic and stays here, where the Nereids
     * expression is available. Plumbing mirrors {@code IcebergRowLevelDmlTransform.checkPluginMode}.
     */
    private void checkConnectorStaticPartitions(PluginDrivenExternalTable table,
            Map<String, Expression> staticPartitions, Set<String> staticPartitionColNames) {
        if (staticPartitions == null || staticPartitions.isEmpty()) {
            return;
        }
        if (!(table.getCatalog() instanceof PluginDrivenExternalCatalog)) {
            return;
        }
        PluginDrivenExternalCatalog catalog = (PluginDrivenExternalCatalog) table.getCatalog();
        ConnectorSession session = catalog.buildConnectorSession();
        ConnectorMetadata metadata = PluginDrivenMetadata.get(session, catalog.getConnector());
        ConnectorTableHandle handle = metadata.getTableHandle(
                        session, table.getRemoteDbName(), table.getRemoteName())
                .orElseThrow(() -> new AnalysisException("Table not found: "
                        + table.getRemoteDbName() + "." + table.getRemoteName()
                        + " in catalog " + catalog.getName()));
        try {
            metadata.validateStaticPartitionColumns(session, handle, new ArrayList<>(staticPartitionColNames));
        } catch (DorisConnectorException e) {
            throw new AnalysisException(e.getMessage(), e);
        }
        // Partition values must be literals (mirrors the retired legacy iceberg literal check; connector-agnostic).
        for (Map.Entry<String, Expression> entry : staticPartitions.entrySet()) {
            if (!(entry.getValue() instanceof Literal)) {
                throw new AnalysisException(String.format(
                        "Partition value for column '%s' must be a literal, but got: %s",
                        entry.getKey(), entry.getValue()));
            }
        }
    }

    /**
     * Connector analogue of the legacy hive partition-spec reject (retired legacy {@code bindHiveTableSink}):
     * rejects the dynamic partition-NAME list form ({@code INSERT ... PARTITION(p1, p2)}) through the neutral
     * {@code ConnectorMetadata#validateWritePartitionNames} SPI, so the rejection and its message stay in the
     * connector (hive rejects, iceberg accepts). A connector {@link DorisConnectorException} is surfaced as the
     * analysis-time {@link AnalysisException} the legacy native path threw, preserving the message and exception
     * type. The handle round-trip + SPI call happen only when the list is non-empty, so a plain {@code INSERT ...
     * SELECT} (empty list) is byte-unchanged for every live connector. Mirrors {@link #checkConnectorStaticPartitions}.
     */
    private void checkConnectorWritePartitionNames(PluginDrivenExternalTable table, List<String> partitionNames) {
        if (partitionNames == null || partitionNames.isEmpty()) {
            return;
        }
        if (!(table.getCatalog() instanceof PluginDrivenExternalCatalog)) {
            return;
        }
        PluginDrivenExternalCatalog catalog = (PluginDrivenExternalCatalog) table.getCatalog();
        ConnectorSession session = catalog.buildConnectorSession();
        ConnectorMetadata metadata = PluginDrivenMetadata.get(session, catalog.getConnector());
        ConnectorTableHandle handle = metadata.getTableHandle(
                        session, table.getRemoteDbName(), table.getRemoteName())
                .orElseThrow(() -> new AnalysisException("Table not found: "
                        + table.getRemoteDbName() + "." + table.getRemoteName()
                        + " in catalog " + catalog.getName()));
        try {
            metadata.validateWritePartitionNames(session, handle, partitionNames);
        } catch (DorisConnectorException e) {
            throw new AnalysisException(e.getMessage(), e);
        }
    }

    /**
     * Resolves the user-typed static-partition column names ({@code PARTITION(TS_DATE='x')}) to their canonical
     * schema names ({@code ts_date}), and rejects a column named twice.
     *
     * <p>Resolution goes through {@link org.apache.doris.datasource.ExternalTable#getColumn}, which already
     * matches with {@code equalsIgnoreCase} — the same lookup the two sibling statements in
     * {@link #bindConnectorTableSink} use (the materialize block and the explicit-column-list bind). Only the
     * exclusion filter in {@link #selectConnectorSinkBindColumns} compared raw names, so this removes an
     * inconsistency rather than introducing a case rule into the engine. It is safe because no plugin-driven
     * schema may hold two columns differing only by case ({@code SchemaCacheValue.validateSchema} rejects that
     * on every schema load), so the fold cannot merge two distinct columns.</p>
     *
     * <p>A name that resolves to no column is kept VERBATIM: on iceberg the PARTITION clause names a partition
     * FIELD (e.g. {@code category_bucket} for {@code bucket(4, category)}), which is not a table column. The
     * connector already validated those names, so fe-core must not rewrite them.</p>
     *
     * <p>The duplicate check is why this is not a plain {@code map()}: {@code PARTITION(dt='1', DT='2')} is two
     * entries here but ONE entry in the case-insensitive {@code columnToOutput} map (see
     * {@link #getColumnToOutput}), so without this the later value would silently overwrite the earlier one.
     * The message quotes the user's spelling.</p>
     */
    @VisibleForTesting
    static Set<String> canonicalStaticPartitionColNames(PluginDrivenExternalTable table,
            Map<String, Expression> staticPartitions) {
        return canonicalStaticPartitionColNames(
                sinkTargetFullSchema(table), staticPartitions);
    }

    private static Set<String> canonicalStaticPartitionColNames(
            List<Column> schema, Map<String, Expression> staticPartitions) {
        if (staticPartitions == null || staticPartitions.isEmpty()) {
            return Sets.newHashSet();
        }
        Set<String> canonical = Sets.newLinkedHashSet();
        for (String name : staticPartitions.keySet()) {
            Column column = findColumn(schema, name);
            if (!canonical.add(column != null ? column.getName() : name)) {
                throw new AnalysisException("Duplicate partition column: " + name);
            }
        }
        return canonical;
    }

    private Plan bindConnectorTableSink(MatchingContext<UnboundConnectorTableSink<Plan>> ctx) {
        UnboundConnectorTableSink<?> sink = ctx.root;
        Pair<ExternalDatabase, PluginDrivenExternalTable> pair = bind(ctx.cascadesContext, sink);
        ExternalDatabase database = pair.first;
        PluginDrivenExternalTable table = pair.second;
        LogicalPlan child = ((LogicalPlan) sink.child());
        PluginDrivenExternalTable.WriteSchemaSnapshot targetMetadata = table.getWriteSchemaSnapshot();
        List<Column> resolvedTargetSchema = ctx.cascadesContext.getStatementContext()
                .getConnectorWriteSchema(table.getId())
                .orElseGet(targetMetadata::getFullSchema);

        // Static-partition columns (e.g. MaxCompute `PARTITION(pt='x')`) carry their value via the
        // static partition spec rather than the query output, so they are excluded from the bound
        // columns when no explicit column list is given (mirrors legacy bindMaxComputeTableSink).
        Map<String, Expression> staticPartitions = sink.getStaticPartitionKeyValues();
        Set<String> staticPartitionColNames = staticPartitions != null
                ? staticPartitions.keySet()
                : Sets.newHashSet();

        // Validate the static-partition spec against the connector's partition metadata (unknown column /
        // non-identity transform / unpartitioned table) via the neutral SPI, so the iceberg PartitionSpec
        // knowledge and its messages stay in the connector — the retired legacy validation never ran on this
        // path. Fail loud at analysis time, before the write plan is synthesized (otherwise an unknown column is
        // silently swallowed by the materialize block below and surfaces as an unrelated planning error).
        // Deliberately fed the RAW (user-typed) names, so a connector message quotes what the user wrote.
        checkConnectorStaticPartitions(table, staticPartitions, staticPartitionColNames);

        // Resolve the user-typed static-partition names to their canonical schema names before they are used
        // to exclude / reject bound columns below. Runs AFTER the connector validated them.
        staticPartitionColNames =
                canonicalStaticPartitionColNames(resolvedTargetSchema, staticPartitions);

        // Reject the dynamic partition-NAME list form (INSERT ... PARTITION(p1, p2)) via the neutral SPI, so the
        // reject and its message stay in the connector (hive rejects with the legacy message; iceberg accepts).
        // The retired legacy hive path threw "Not support insert with partition spec in hive catalog." here.
        // Guarded on non-empty inside the helper, so a plain INSERT ... SELECT is byte-unchanged for live connectors.
        checkConnectorWritePartitionNames(table, sink.getPartitions());

        List<Column> targetWriteSchema = resolvedTargetSchema.stream()
                .filter(column -> isConnectorSinkWriteColumn(column, sink.isRewrite()))
                .collect(ImmutableList.toImmutableList());
        boolean changelogRowChange = sink.getRowChangeSpec().isPresent();
        if (changelogRowChange) {
            child = ConnectorChangelogPlanBuilder.build(targetWriteSchema,
                    table.getConnectorRowLevelPrimaryKeyColumns(), sink.getRowChangeSpec().get(),
                    child, ctx.cascadesContext);
        }
        if (sink.isRewrite()) {
            List<NamedExpression> rewriteOutputs = selectConnectorRewriteOutputs(
                    targetWriteSchema, child.getOutput());
            if (!rewriteOutputs.equals(child.getOutput())) {
                child = new LogicalProject<>(rewriteOutputs, child);
            }
        }
        List<Column> bindColumns = changelogRowChange
                ? targetWriteSchema
                : selectConnectorSinkBindColumns(table, targetWriteSchema, sink.getColNames(),
                        staticPartitionColNames, sink.isRewrite());
        LogicalConnectorTableSink<?> boundSink = new LogicalConnectorTableSink<>(
                database,
                table,
                targetWriteSchema,
                targetMetadata.getPartitionColumns(),
                targetMetadata.getWriteMetadataIdentity(),
                bindColumns,
                child.getOutput().stream()
                        .map(NamedExpression.class::cast)
                        .collect(ImmutableList.toImmutableList()),
                sink.getDMLCommandType(),
                sink.isRewrite(),
                Optional.empty(),
                Optional.empty(),
                child);
        int expectedOutputSize = boundSink.getCols().size() + (changelogRowChange ? 1 : 0);
        if (expectedOutputSize != child.getOutput().size()) {
            // Carry the "Expected N columns but got M" detail that legacy (and the sibling count-check in this
            // file) emit; the terser form dropped it on the connector path.
            throw new AnalysisException("insert into cols should be corresponding to the query output. "
                    + "Expected " + expectedOutputSize + " columns but got " + child.getOutput().size());
        }
        if (changelogRowChange) {
            return boundSink;
        }
        if (table.requiresFullSchemaWriteOrder()) {
            // Positional-write connector (e.g. MaxCompute): its BE writer maps data columns positionally
            // against the full table schema, so project the child to FULL-SCHEMA order with any
            // unmentioned / static-partition columns filled in (NULL literals), exactly like legacy
            // bindMaxComputeTableSink — for ALL such writes, partitioned or not. Required on three
            // counts: (1) a reordered/partial explicit column list must land values in the correct
            // remote columns (not user order); (2) for a static-partition write the BE writer strips the
            // trailing partition columns by position, so they must sit at their full-schema (tail)
            // positions; and (3) PhysicalConnectorTableSink.getRequirePhysicalProperties locates
            // partition columns by their full-schema position, so the child must be in full-schema order.
            Map<String, NamedExpression> columnToOutput = getColumnToOutput(
                    ctx, table, false, false, boundSink, child, targetWriteSchema);
            if (table.materializeStaticPartitionValues() && !staticPartitionColNames.isEmpty()) {
                // Connectors that consume the partition value FROM THE ROW must write the static partition value
                // INTO the data column: getColumnToOutput excluded it from the bound columns and NULL-filled it,
                // so re-project the PARTITION-clause literal here (mirrors the retired legacy iceberg bind).
                // Two reasons put a connector here — its files retain the column (Iceberg), or its files strip
                // the column but the BE derives the partition DIRECTORY from the row value (Hive, where a NULL
                // would become __HIVE_DEFAULT_PARTITION__). Connectors that STRIP partition columns and refill
                // them from static_partition_values (e.g. MaxCompute) do not declare the capability and keep the
                // NULL fill.
                for (Map.Entry<String, Expression> entry : staticPartitions.entrySet()) {
                    Column column = findColumn(targetWriteSchema, entry.getKey());
                    if (column != null) {
                        Expression castExpr = TypeCoercionUtils.castIfNotSameType(
                                entry.getValue(), DataType.fromCatalogType(column.getType()));
                        // Key and alias use the canonical schema name, so they line up with
                        // getOutputProjectByCoercion, which looks columnToOutput up by getFullSchema() names.
                        columnToOutput.put(column.getName(), new Alias(castExpr, column.getName()));
                    }
                }
            }
            LogicalProject<?> fullOutputProject =
                    getOutputProjectByCoercion(targetWriteSchema, child, columnToOutput);
            return boundSink.withChildAndUpdateOutput(fullOutputProject);
        }
        // Name-mapped connector tables (JDBC / ES): keep columns in user-specified order because the
        // INSERT SQL column list is built from cols (user order) and the data values must match; only
        // project user-specified columns in user order.
        Map<String, NamedExpression> columnToOutput = getConnectorColumnToOutput(bindColumns, child);
        LogicalProject<?> outputProject = getOutputProjectByCoercion(bindColumns, child, columnToOutput);
        return boundSink.withChildAndUpdateOutput(outputProject);
    }

    /**
     * Selects the bound columns for a connector table sink. With an explicit column list, binds those
     * columns in user order. Without one, binds the base schema minus any static partition columns
     * (their value comes from the static partition spec, not the query output, so they must not be
     * matched against the query columns) — mirrors legacy {@code bindMaxComputeTableSink}.
     *
     * <p>Invisible columns (e.g. iceberg v3 row-lineage {@code _row_id} /
     * {@code _last_updated_sequence_number}) are excluded from an ordinary write's default target — the
     * user never supplies their values, so counting them would break the "insert cols == query output"
     * check. They are RETAINED for a {@code rewrite} (a distributed {@code rewrite_data_files} reads and
     * rewrites full rows, preserving the engine-managed lineage values) only when the connector marks
     * them {@link Column#isReservedPassthrough reserved passthrough}. Request-scoped invisible columns
     * are never physical rewrite fields.
     *
     * <p>{@code staticPartitionColNames} must already be canonicalized by
     * {@link #canonicalStaticPartitionColNames}, so both the exclusion filter and the explicit-column-list
     * rejection below can compare against schema names directly.</p>
     */
    @VisibleForTesting
    static List<Column> selectConnectorSinkBindColumns(PluginDrivenExternalTable table,
            List<String> colNames, Set<String> staticPartitionColNames, boolean isRewrite) {
        return selectConnectorSinkBindColumns(table, sinkTargetFullSchema(table),
                colNames, staticPartitionColNames, isRewrite);
    }

    @VisibleForTesting
    static List<Column> selectConnectorSinkBindColumns(PluginDrivenExternalTable table,
            List<Column> targetSchema, List<String> colNames,
            Set<String> staticPartitionColNames, boolean isRewrite) {
        if (colNames.isEmpty()) {
            return targetSchema.stream()
                    .filter(col -> !staticPartitionColNames.contains(col.getName()))
                    .filter(col -> isConnectorSinkWriteColumn(col, isRewrite))
                    .collect(ImmutableList.toImmutableList());
        }
        return colNames.stream().map(cn -> {
            Column column = findColumn(targetSchema, cn);
            if (column == null) {
                // The pinned writer schema deliberately contains data columns only. The latest full target
                // schema still owns engine-managed invisible columns, which must reach the explicit-invisible
                // rejection below instead of being misclassified as an unknown data column.
                column = sinkTargetFullSchema(table).stream()
                        .filter(candidate -> !candidate.isVisible())
                        .filter(candidate -> candidate.getName().equalsIgnoreCase(cn))
                        .findFirst()
                        .orElse(null);
            }
            if (column == null) {
                throw new AnalysisException(String.format("column %s is not found in table %s",
                        cn, table.getName()));
            }
            // A column whose value comes from the PARTITION clause must not ALSO be supplied by the query:
            // the materialize block would overwrite the query's value, silently discarding it. Both sides are
            // canonical schema names here, so this catches PARTITION(TS_DATE=..) (col1, ts_date) too.
            if (staticPartitionColNames.contains(column.getName())) {
                throw new AnalysisException(String.format(
                        "column %s is a static partition column, should not be in the insert column list", cn));
            }
            if (!isConnectorSinkWriteColumn(column, isRewrite)) {
                throw new AnalysisException(String.format(
                        "Cannot specify invisible column '%s' in INSERT statement", cn));
            }
            return column;
        }).collect(ImmutableList.toImmutableList());
    }

    private static Column findColumn(List<Column> schema, String name) {
        return schema.stream()
                .filter(column -> column.getName().equalsIgnoreCase(name))
                .findFirst()
                .orElse(null);
    }

    private static boolean isConnectorSinkWriteColumn(Column column, boolean isRewrite) {
        // Hidden request-scoped scan columns are not sink fields; only connector-declared persistent
        // passthrough columns may survive a rewrite and change its physical arity.
        return column.isVisible() || (isRewrite && column.isReservedPassthrough());
    }

    @VisibleForTesting
    static List<NamedExpression> selectConnectorRewriteOutputs(
            List<Column> writeSchema, List<? extends NamedExpression> sourceOutputs) {
        Map<String, NamedExpression> outputByName = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        for (NamedExpression output : sourceOutputs) {
            if (outputByName.put(output.getName(), output) != null) {
                throw new AnalysisException("Duplicate column in connector rewrite source: " + output.getName());
            }
        }
        // Source scans can expose request-scoped hidden columns under show-hidden. Selecting by the physical
        // write schema keeps source output, bound schema, and BE sink arity on one shared invariant.
        return writeSchema.stream().map(column -> {
            NamedExpression output = outputByName.get(column.getName());
            if (output == null) {
                throw new AnalysisException("Column " + column.getName()
                        + " is missing from connector rewrite source");
            }
            return output;
        }).collect(ImmutableList.toImmutableList());
    }

    /**
     * Build column-to-output mapping for connector table sinks.
     * Maps each user-specified column to the corresponding child output expression
     * with type coercion, preserving user-specified column order.
     */
    private static Map<String, NamedExpression> getConnectorColumnToOutput(
            List<Column> bindColumns, LogicalPlan child) {
        Map<String, NamedExpression> columnToOutput = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        for (int i = 0; i < bindColumns.size(); i++) {
            Column column = bindColumns.get(i);
            NamedExpression outputExpr = child.getOutput().get(i);
            Alias output = new Alias(
                    TypeCoercionUtils.castIfNotSameType(outputExpr, DataType.fromCatalogType(column.getType())),
                    column.getName());
            columnToOutput.put(column.getName(), output);
        }
        return columnToOutput;
    }

    private Plan bindDictionarySink(MatchingContext<UnboundDictionarySink<Plan>> ctx) {
        UnboundDictionarySink<?> sink = ctx.root;
        Database database = sink.getDatabase();
        Dictionary dictionary = sink.getDictionary();
        LogicalPlan child = ((LogicalPlan) sink.child());

        // 1. bind target columns: from sink's column names to target tables' Columns
        // bindTargetColumns for dictionary: now will sink exactly all dictionaries' columns.
        List<Column> sinkColumns = dictionary.getFullSchema();

        // Dictionary sink is special. It allows sink with columns which not SAME like upstream's output.
        // e.g. Scan Column(A|B|C|D), Sink Column(D|B|A) is OK.
        // so we have to re-calculate LogicalDictionarySink's output expr.
        List<NamedExpression> upstreamOutput = child.getOutput().stream().map(NamedExpression.class::cast)
                .collect(ImmutableList.toImmutableList());
        List<NamedExpression> outputExprs = new ArrayList<>(sinkColumns.size());
        for (Column column : sinkColumns) {
            // find the SlotRef from child's output
            Optional<NamedExpression> output = upstreamOutput.stream()
                    .filter(expr -> expr.getName().equalsIgnoreCase(column.getName())).findFirst();
            if (output.isPresent()) {
                outputExprs.add(output.get());
            } else {
                throw new AnalysisException("Unknown column " + column.getName());
            }
        }

        // Create LogicalDictionarySink. from child's output to OutputExprs.
        // if source table has A,B,C,D, dictionary has D,B,A, then outputExprs and sinkColumns are both D,B,A
        LogicalDictionarySink<?> boundSink = new LogicalDictionarySink<>(database, dictionary, sink.allowAdaptiveLoad(),
                sinkColumns, outputExprs, child);

        // Get column to output mapping and handle type coercion. sink column to its accepted expr
        Map<String, NamedExpression> sinkColumnToExpr = getDictColumnToOutput(ctx, sinkColumns, boundSink, child);
        // before we get A|B|C|D and only sink D|B|A. here deal the PROJECT between them.
        LogicalProject<?> fullOutputProject = getOutputProjectByCoercion(sinkColumns, child, sinkColumnToExpr);

        // Return the bound sink with updated child and outputExprs here.
        return boundSink.withChildAndUpdateOutput(fullOutputProject);
    }

    private static Map<String, NamedExpression> getDictColumnToOutput(
            MatchingContext<? extends UnboundLogicalSink<Plan>> ctx, List<Column> sinkSchema,
            LogicalDictionarySink<?> boundSink, LogicalPlan child) {
        // as we said, dictionary sink is special - unordered and inconsistent.
        Map<String, NamedExpression> upstreamOutputs = Maps.newHashMap();

        // push A|B|C|D.
        for (int i = 0; i < child.getOutput().size(); ++i) {
            upstreamOutputs.put(child.getOutput().get(i).getName(), child.getOutput().get(i));
        }
        Map<String, NamedExpression> columnToOutput = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        // for sink columns D|B|A, link them with child's outputs
        for (Column column : sinkSchema) {
            if (upstreamOutputs.containsKey(column.getName())) {
                // dictionary's type must be same with source table.
                Alias output = new Alias(upstreamOutputs.get(column.getName()), column.getName());
                columnToOutput.put(column.getName(), output);
            } else {
                throw new AnalysisException("Unknown column " + column.getName());
            }
        }
        return columnToOutput;
    }

    private Pair<DatabaseIf, OlapTable> bind(CascadesContext cascadesContext, UnboundTableSink<? extends Plan> sink) {
        List<String> tableQualifier = RelationUtil.getQualifierName(cascadesContext.getConnectContext(),
                sink.getNameParts());
        Pair<DatabaseIf<?>, TableIf> pair = RelationUtil.getDbAndTable(tableQualifier,
                cascadesContext.getConnectContext().getEnv(), Optional.empty());
        if (!(pair.second instanceof OlapTable) && !(pair.second instanceof RemoteDorisExternalTable)) {
            throw new AnalysisException("the target table of insert into is not an OLAP table");
        }
        return Pair.of(pair.first, pair.second instanceof RemoteDorisExternalTable
                ? ((RemoteDorisExternalTable) pair.second).getOlapTable() : (OlapTable) pair.second);
    }

    @SuppressWarnings("rawtypes")
    private Pair<ExternalDatabase, PluginDrivenExternalTable> bind(CascadesContext cascadesContext,
            UnboundConnectorTableSink<? extends Plan> sink) {
        List<String> tableQualifier = RelationUtil.getQualifierName(cascadesContext.getConnectContext(),
                sink.getNameParts());
        Pair<DatabaseIf<?>, TableIf> pair = RelationUtil.getDbAndTable(tableQualifier,
                cascadesContext.getConnectContext().getEnv(), Optional.empty());
        if (pair.second instanceof PluginDrivenExternalTable) {
            return Pair.of(((ExternalDatabase) pair.first), (PluginDrivenExternalTable) pair.second);
        }
        throw new AnalysisException("the target table of insert into is not a plugin-driven connector table");
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

    // bindTargetColumns means bind sink node's target columns' names to target table's columns
    private Pair<List<Column>, Integer> bindTargetColumns(OlapTable table, List<String> colsName,
            boolean childHasSeqCol, boolean needExtraSeqCol, boolean isGroupCommit, boolean isDeletePartialUpdate) {
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
                } else if (col.isGeneratedColumn() && !isDeletePartialUpdate) {
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

    private static class LegacyExprTranslator {
        private final Map<String, Slot> nereidsSlotReplaceMap = new HashMap<>();
        private final OlapTable olapTable;

        public LegacyExprTranslator(OlapTable table, List<Slot> outputs) {
            for (Slot expression : outputs) {
                String name = expression.getName();
                nereidsSlotReplaceMap.put(name.toLowerCase(), expression.toSlot());
            }
            this.olapTable = table;
        }

        public List<Expression> createPartitionExprList() {
            return olapTable.getPartitionInfo().getPartitionExprs().stream()
                    .map(expr -> analyze(expr.accept(ExprToSqlVisitor.INSTANCE, ToSqlParams.WITH_TABLE)))
                    .collect(Collectors.toList());
        }

        public Map<Long, Expression> createSyncMvWhereClause() {
            Map<Long, Expression> mvWhereClauses = new HashMap<>();
            long baseIndexId = olapTable.getBaseIndexId();
            for (Map.Entry<Long, MaterializedIndexMeta> entry : olapTable.getVisibleIndexIdToMeta().entrySet()) {
                try (AutoCloseSessionVariable auto = new AutoCloseSessionVariable(
                        ConnectContext.get(), entry.getValue().getSessionVariables())) {
                    if (entry.getKey() == baseIndexId || entry.getValue().getWhereClause() == null) {
                        continue;
                    }
                    Expression predicate = analyze(entry.getValue().getWhereClause().accept(
                            ExprToSqlVisitor.INSTANCE, ToSqlParams.WITHOUT_TABLE));
                    predicate = predicate.accept(new AddSessionVarGuardRewriter(
                            entry.getValue().getSessionVariables()), Boolean.FALSE);
                    mvWhereClauses.put(entry.getKey(), predicate);
                }
            }
            return mvWhereClauses;
        }

        private Expression analyze(String exprSql) {
            Expression expression = new NereidsParser().parseExpression(exprSql);
            Expression boundSlotExpression = SlotReplacer.INSTANCE.replace(expression, nereidsSlotReplaceMap);
            Scope scope = new Scope(Lists.newArrayList(nereidsSlotReplaceMap.values()));
            StatementContext statementContext = new StatementContext();
            LogicalEmptyRelation dummyPlan = new LogicalEmptyRelation(
                    statementContext.getNextRelationId(), new ArrayList<>());
            CascadesContext cascadesContext = CascadesContext.initContext(
                    statementContext, dummyPlan, PhysicalProperties.ANY);
            ExpressionAnalyzer analyzer = new ExpressionAnalyzer(null, scope, cascadesContext, false, false);
            return analyzer.analyze(boundSlotExpression, new ExpressionRewriteContext(cascadesContext));
        }

        private static class SlotReplacer extends DefaultExpressionRewriter<Map<String, Slot>> {
            public static final SlotReplacer INSTANCE = new SlotReplacer();

            public Expression replace(Expression e, Map<String, Slot> replaceMap) {
                return e.accept(this, replaceMap);
            }

            @Override
            public Expression visitUnboundSlot(UnboundSlot unboundSlot,
                                               Map<String, Slot> replaceMap) {
                if (!replaceMap.containsKey(unboundSlot.getName().toLowerCase())) {
                    throw new org.apache.doris.nereids.exceptions.AnalysisException(
                            "Unknown column " + unboundSlot.getName());
                }
                return replaceMap.get(unboundSlot.getName().toLowerCase());
            }
        }
    }
}
