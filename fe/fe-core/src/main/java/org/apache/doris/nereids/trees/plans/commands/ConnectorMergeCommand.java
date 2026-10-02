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
import org.apache.doris.catalog.TableIf;
import org.apache.doris.connector.spi.handle.WriteOperation;
import org.apache.doris.datasource.plugin.PluginDrivenExternalTable;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.analyzer.UnboundAlias;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.analyzer.UnboundStar;
import org.apache.doris.nereids.analyzer.UnboundTableSinkCreator;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.parser.LogicalPlanBuilderAssistant;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.rules.exploration.join.JoinReorderContext;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.IsTrue;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.SubqueryExpr;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.plans.Explainable;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.DMLCommandType;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertOverwriteTableCommand;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertOverwriteTableCommand.ConnectorSourceSnapshot;
import org.apache.doris.nereids.trees.plans.commands.merge.MergeMatchedClause;
import org.apache.doris.nereids.trees.plans.commands.merge.MergeNotMatchedClause;
import org.apache.doris.nereids.trees.plans.logical.LogicalCheckPolicy;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.RelationUtil;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.thrift.TPartialUpdateNewRowPolicy;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** Snapshot-pinned copy-on-write MERGE for plugin connector tables. */
public final class ConnectorMergeCommand extends Command implements ForwardWithSync, Explainable {
    private static final String TARGET_PRESENT = "__DORIS_CONNECTOR_MERGE_TARGET_PRESENT__";
    private static final String SOURCE_PRESENT = "__DORIS_CONNECTOR_MERGE_SOURCE_PRESENT__";
    private static final String BRANCH_LABEL = "__DORIS_CONNECTOR_MERGE_BRANCH__";
    private static final int DROP_BRANCH = -2;
    private static final int PRESERVE_BRANCH = -1;
    private static final Set<String> INTERNAL_COLUMN_NAMES = ImmutableSet.of(
            TARGET_PRESENT.toLowerCase(Locale.ROOT),
            SOURCE_PRESENT.toLowerCase(Locale.ROOT),
            BRANCH_LABEL.toLowerCase(Locale.ROOT));

    private final List<String> targetNameParts;
    private final Optional<String> targetAlias;
    private final Optional<LogicalPlan> cte;
    private final LogicalPlan source;
    private final Expression onClause;
    private final List<MergeMatchedClause> matchedClauses;
    private final List<MergeNotMatchedClause> notMatchedClauses;

    /** Creates a connector MERGE command from parser-owned clauses and source plan. */
    public ConnectorMergeCommand(List<String> targetNameParts, Optional<String> targetAlias,
            Optional<LogicalPlan> cte, LogicalPlan source, Expression onClause,
            List<MergeMatchedClause> matchedClauses,
            List<MergeNotMatchedClause> notMatchedClauses) {
        super(PlanType.MERGE_INTO_COMMAND);
        this.targetNameParts = ImmutableList.copyOf(targetNameParts);
        this.targetAlias = targetAlias;
        this.cte = cte;
        this.source = source;
        this.onClause = onClause;
        this.matchedClauses = ImmutableList.copyOf(matchedClauses);
        this.notMatchedClauses = ImmutableList.copyOf(notMatchedClauses);
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        PluginDrivenExternalTable targetTable = requireMergeTable(ctx);
        ConnectorCopyOnWriteUtils.requireUnrestrictedSource(ctx, targetTable);
        requireTargetPrivileges(ctx, targetTable);
        SourceInfo sourceInfo = requireUniqueSource(ctx);
        validateClauses(ctx, targetTable);
        ConnectorCopyOnWriteUtils.CopyOnWriteSnapshot snapshot =
                ConnectorCopyOnWriteUtils.captureSnapshot(targetTable, "MERGE");
        ConnectorSourceSnapshot sourceSnapshot =
                InsertOverwriteTableCommand.snapshotConnectorSource(sourceInfo.table);
        long affectedRows = ConnectorCopyOnWriteUtils.countRows(
                ctx, buildCountSql(snapshot.version, sourceInfo), "MERGE");
        if (!sourceSnapshot.equals(
                InsertOverwriteTableCommand.snapshotConnectorSource(sourceInfo.table))) {
            throw new AnalysisException(
                    "Connector source table changed while counting copy-on-write MERGE rows");
        }
        if (affectedRows == 0) {
            ctx.getState().setOk(0, 0, "");
            ctx.updateReturnRows(0);
            return;
        }
        InsertOverwriteTableCommand overwrite = buildOverwriteCommand(
                buildMergeQuery(ctx, targetTable));
        overwrite.setConnectorOverwriteBaseHandle(snapshot.handle);
        overwrite.setConnectorAffectedRowCount(affectedRows);
        overwrite.setConnectorSourceSnapshot(sourceSnapshot);
        overwrite.run(ctx, executor);
    }

    @Override
    public Plan getExplainPlan(ConnectContext ctx) {
        PluginDrivenExternalTable targetTable = requireMergeTable(ctx);
        ConnectorCopyOnWriteUtils.requireUnrestrictedSource(ctx, targetTable);
        requireUniqueSource(ctx);
        validateClauses(ctx, targetTable);
        return buildOverwriteCommand(buildMergeQuery(ctx, targetTable)).getExplainPlan(ctx);
    }

    private PluginDrivenExternalTable requireMergeTable(ConnectContext ctx) {
        TableIf table = RelationUtil.getTable(
                RelationUtil.getQualifierName(ctx, targetNameParts),
                ctx.getEnv(), Optional.empty());
        if (!(table instanceof PluginDrivenExternalTable)) {
            throw new AnalysisException("Connector MERGE requires a plugin-driven external table");
        }
        PluginDrivenExternalTable connectorTable = (PluginDrivenExternalTable) table;
        if (!connectorTable.connectorSupportedWriteOperations().contains(WriteOperation.MERGE)
                || !connectorTable.connectorSupportsCopyOnWriteDml()) {
            throw new AnalysisException("Connector does not support MERGE for table: "
                    + connectorTable.getName());
        }
        if (!targetAlias.isPresent()) {
            throw new AnalysisException(
                    "Connector copy-on-write MERGE requires a target table alias");
        }
        if (cte.isPresent()) {
            throw new AnalysisException("Connector copy-on-write MERGE does not support CTE");
        }
        requireNoInternalColumnCollision(connectorTable.getBaseSchema(true), "target");
        return connectorTable;
    }

    private static void requireTargetPrivileges(
            ConnectContext ctx, PluginDrivenExternalTable table) {
        String catalogName = table.getDatabase().getCatalog().getName();
        String databaseName = table.getDatabase().getFullName();
        boolean canRead = ctx.getEnv().getAccessManager().checkTblPriv(
                ctx, catalogName, databaseName, table.getName(), PrivPredicate.SELECT);
        boolean canWrite = ctx.getEnv().getAccessManager().checkTblPriv(
                ctx, catalogName, databaseName, table.getName(), PrivPredicate.LOAD);
        if (!canRead || !canWrite) {
            throw new AnalysisException(
                    "Connector copy-on-write MERGE requires SELECT and LOAD privileges on target table");
        }
    }

    private SourceInfo requireUniqueSource(ConnectContext ctx) {
        LogicalPlan sourcePlan = source;
        String sourceAlias;
        if (source instanceof LogicalSubQueryAlias) {
            LogicalSubQueryAlias<?> alias = (LogicalSubQueryAlias<?>) source;
            if (alias.getColumnAliases().isPresent()) {
                throw new AnalysisException(
                        "Connector copy-on-write MERGE requires a directly aliased source table");
            }
            sourceAlias = alias.getAlias();
            sourcePlan = (LogicalPlan) alias.child();
        } else {
            throw new AnalysisException(
                    "Connector copy-on-write MERGE requires a directly aliased source table");
        }
        if (sourceAlias.equalsIgnoreCase(targetAlias.get())) {
            throw new AnalysisException(
                    "Connector copy-on-write MERGE requires distinct target and source aliases");
        }
        if (sourcePlan instanceof LogicalCheckPolicy) {
            sourcePlan = (LogicalPlan) sourcePlan.child(0);
        }
        if (!(sourcePlan instanceof UnboundRelation)) {
            throw new AnalysisException(
                    "Connector copy-on-write MERGE requires a directly aliased source table");
        }
        UnboundRelation relation = (UnboundRelation) sourcePlan;
        requirePlainSourceRelation(relation);
        TableIf sourceTable = RelationUtil.getTable(
                RelationUtil.getQualifierName(ctx, relation.getNameParts()),
                ctx.getEnv(), Optional.empty());
        if (!(sourceTable instanceof OlapTable)
                || ((OlapTable) sourceTable).getKeysType() != KeysType.UNIQUE_KEYS) {
            throw new AnalysisException(
                    "Connector copy-on-write MERGE requires a UNIQUE KEY Doris source table");
        }
        requireNoInternalColumnCollision(sourceTable.getBaseSchema(), "source");
        SourceInfo sourceInfo = new SourceInfo(
                (OlapTable) sourceTable, relation.getNameParts(), sourceAlias);
        requireSourceKeyCoverage(sourceInfo);
        return sourceInfo;
    }

    static void requirePlainSourceRelation(UnboundRelation relation) {
        if (!relation.getPartNames().isEmpty()
                || relation.isTempPart()
                || !relation.getTabletIds().isEmpty()
                || !relation.getHints().isEmpty()
                || relation.getTableSample().isPresent()
                || relation.getIndexName().isPresent()
                || relation.getScanParams() != null
                || relation.getTableSnapshot().isPresent()) {
            throw new AnalysisException(
                    "Connector copy-on-write MERGE source does not support scan modifiers");
        }
    }

    private static void requireNoInternalColumnCollision(
            List<Column> columns, String tableRole) {
        Optional<String> collision = columns.stream().map(Column::getName)
                .filter(name -> INTERNAL_COLUMN_NAMES.contains(name.toLowerCase(Locale.ROOT)))
                .findFirst();
        if (collision.isPresent()) {
            throw new AnalysisException("Connector copy-on-write MERGE " + tableRole
                    + " column conflicts with internal column name: " + collision.get());
        }
    }

    private void requireSourceKeyCoverage(SourceInfo sourceInfo) {
        requireDeterministic(onClause, "ON predicate");
        Set<String> coveredKeys = new HashSet<>();
        for (Expression conjunct : ExpressionUtils.extractConjunction(onClause)) {
            if (!(conjunct instanceof EqualTo)) {
                continue;
            }
            EqualTo equal = (EqualTo) conjunct;
            collectCoveredSourceKey(equal.left(), equal.right(), sourceInfo, coveredKeys);
            collectCoveredSourceKey(equal.right(), equal.left(), sourceInfo, coveredKeys);
        }
        Set<String> sourceKeys = sourceInfo.table.getBaseSchema().stream()
                .filter(Column::isKey)
                .map(column -> column.getName().toLowerCase(Locale.ROOT))
                .collect(Collectors.toSet());
        if (!coveredKeys.containsAll(sourceKeys)) {
            throw new AnalysisException(
                    "Connector copy-on-write MERGE ON clause must cover every source UNIQUE KEY column");
        }
    }

    private void collectCoveredSourceKey(Expression candidate, Expression targetExpression,
            SourceInfo sourceInfo, Set<String> coveredKeys) {
        if (!(candidate instanceof UnboundSlot)
                || !hasOnlyQualifier(candidate, sourceInfo.alias)
                || !hasOnlyQualifier(targetExpression, targetAlias.get())) {
            return;
        }
        UnboundSlot slot = (UnboundSlot) candidate;
        coveredKeys.add(slot.getNameParts().get(slot.getNameParts().size() - 1)
                .toLowerCase(Locale.ROOT));
    }

    private static boolean hasOnlyQualifier(Expression expression, String qualifier) {
        Set<UnboundSlot> slots = expression.collect(UnboundSlot.class::isInstance);
        return !slots.isEmpty() && slots.stream().allMatch(slot -> {
            List<String> parts = slot.getNameParts();
            return parts.size() >= 2
                    && parts.get(parts.size() - 2).equalsIgnoreCase(qualifier);
        });
    }

    private void validateClauses(
            ConnectContext ctx, PluginDrivenExternalTable table) {
        validateClauseOrder(matchedClauses.stream()
                .map(MergeMatchedClause::getCasePredicate).collect(Collectors.toList()), "matched");
        validateClauseOrder(notMatchedClauses.stream()
                .map(MergeNotMatchedClause::getCasePredicate).collect(Collectors.toList()), "not matched");
        matchedClauses.forEach(clause -> clause.getCasePredicate()
                .ifPresent(predicate -> requireDeterministic(predicate, "matched predicate")));
        notMatchedClauses.forEach(clause -> clause.getCasePredicate()
                .ifPresent(predicate -> requireDeterministic(predicate, "not matched predicate")));
        for (MergeMatchedClause clause : matchedClauses) {
            if (!clause.isDelete()) {
                buildUpdateValues(ctx, table, clause);
            }
        }
        for (MergeNotMatchedClause clause : notMatchedClauses) {
            buildInsertValues(table, clause);
        }
    }

    private static void validateClauseOrder(
            List<Optional<Expression>> predicates, String clauseType) {
        for (int index = 0; index + 1 < predicates.size(); index++) {
            if (!predicates.get(index).isPresent()) {
                throw new AnalysisException(
                        "Only the last " + clauseType + " clause may omit its predicate");
            }
        }
    }

    private static void requireDeterministic(Expression expression, String label) {
        if (expression.anyMatch(value -> value instanceof SubqueryExpr)
                || expression.containsNondeterministic()) {
            throw new AnalysisException(
                    "Connector copy-on-write MERGE requires a deterministic "
                            + label + " without subqueries");
        }
    }

    private LogicalPlan buildMergeQuery(
            ConnectContext ctx, PluginDrivenExternalTable targetTable) {
        LogicalPlan target = LogicalPlanBuilderAssistant.withCheckPolicy(
                new UnboundRelation(
                        org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator
                                .newRelationId(), targetNameParts));
        target = new LogicalSubQueryAlias<>(targetAlias.get(), target);
        target = addPresenceMarker(target, TARGET_PRESENT);
        LogicalPlan markedSource = addPresenceMarker(source, SOURCE_PRESENT);
        LogicalPlan joined = new LogicalJoin<>(JoinType.FULL_OUTER_JOIN,
                ImmutableList.of(), ImmutableList.of(onClause), target, markedSource,
                JoinReorderContext.EMPTY);

        Expression branch = buildBranchExpression();
        LogicalPlan branched = new LogicalProject<>(ImmutableList.of(
                new UnboundStar(ImmutableList.of()), new UnboundAlias(branch, BRANCH_LABEL)), joined);
        LogicalPlan retained = new LogicalFilter<>(ImmutableSet.of(
                new Not(new EqualTo(new UnboundSlot(BRANCH_LABEL),
                        new IntegerLiteral(DROP_BRANCH)))), branched);
        return new LogicalProject<>(buildFinalProjects(ctx, targetTable), retained);
    }

    private static LogicalPlan addPresenceMarker(LogicalPlan plan, String markerName) {
        return new LogicalProject<>(ImmutableList.of(
                new UnboundStar(ImmutableList.of()),
                new UnboundAlias(new IntegerLiteral(1), markerName)), plan);
    }

    Expression buildBranchExpression() {
        Expression targetPresent = new Not(new IsNull(new UnboundSlot(TARGET_PRESENT)));
        Expression sourcePresent = new Not(new IsNull(new UnboundSlot(SOURCE_PRESENT)));
        Expression matched = buildMatchedBranch();
        Expression notMatched = buildNotMatchedBranch();
        return new If(targetPresent,
                new If(sourcePresent, matched, new IntegerLiteral(PRESERVE_BRANCH)),
                new If(sourcePresent, notMatched, new IntegerLiteral(DROP_BRANCH)));
    }

    private Expression buildMatchedBranch() {
        Expression result = new IntegerLiteral(PRESERVE_BRANCH);
        for (int index = matchedClauses.size() - 1; index >= 0; index--) {
            MergeMatchedClause clause = matchedClauses.get(index);
            Expression label = new IntegerLiteral(clause.isDelete() ? DROP_BRANCH : index);
            Expression previous = result;
            result = clause.getCasePredicate()
                    .<Expression>map(predicate -> new If(new IsTrue(predicate), label, previous))
                    .orElse(label);
        }
        return result;
    }

    private Expression buildNotMatchedBranch() {
        Expression result = new IntegerLiteral(DROP_BRANCH);
        for (int index = notMatchedClauses.size() - 1; index >= 0; index--) {
            Expression label = new IntegerLiteral(matchedClauses.size() + index);
            Optional<Expression> predicate = notMatchedClauses.get(index).getCasePredicate();
            Expression previous = result;
            result = predicate
                    .<Expression>map(value -> new If(new IsTrue(value), label, previous))
                    .orElse(label);
        }
        return result;
    }

    private List<NamedExpression> buildFinalProjects(
            ConnectContext ctx, PluginDrivenExternalTable targetTable) {
        List<Column> columns = targetTable.getBaseSchema(true).stream()
                .filter(Column::isVisible).collect(Collectors.toList());
        List<Map<String, Expression>> updateValues = new ArrayList<>();
        for (MergeMatchedClause clause : matchedClauses) {
            updateValues.add(clause.isDelete()
                    ? Map.of() : buildUpdateValues(ctx, targetTable, clause));
        }
        List<Map<String, Expression>> insertValues = notMatchedClauses.stream()
                .map(clause -> buildInsertValues(targetTable, clause))
                .collect(Collectors.toList());

        List<NamedExpression> projects = new ArrayList<>();
        for (Column column : columns) {
            Expression targetValue = new UnboundSlot(targetAlias.get(), column.getName());
            Expression value = targetValue;
            for (int index = 0; index < matchedClauses.size(); index++) {
                if (matchedClauses.get(index).isDelete()) {
                    continue;
                }
                Expression updated = updateValues.get(index)
                        .getOrDefault(column.getName(), targetValue);
                value = new If(branchEquals(index), updated, value);
            }
            for (int index = 0; index < notMatchedClauses.size(); index++) {
                Expression inserted = insertValues.get(index).get(column.getName());
                value = new If(branchEquals(matchedClauses.size() + index), inserted, value);
            }
            projects.add(new UnboundAlias(value, column.getName()));
        }
        return projects;
    }

    private static Expression branchEquals(int branch) {
        return new EqualTo(new UnboundSlot(BRANCH_LABEL), new IntegerLiteral(branch));
    }

    private Map<String, Expression> buildUpdateValues(ConnectContext ctx,
            PluginDrivenExternalTable table, MergeMatchedClause clause) {
        Map<String, Expression> values = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        for (EqualTo assignment : clause.getAssignments()) {
            List<String> parts = ((UnboundSlot) assignment.left()).getNameParts();
            UpdateCommand.checkAssignmentColumn(
                    ctx, parts, targetNameParts, targetAlias.get());
            String columnName = parts.get(parts.size() - 1);
            if (values.put(columnName, assignment.right()) != null) {
                throw new AnalysisException("Duplicate column name in MERGE UPDATE: " + columnName);
            }
        }
        Set<String> columns = table.getBaseSchema(true).stream()
                .filter(Column::isVisible).map(Column::getName)
                .map(name -> name.toLowerCase(Locale.ROOT)).collect(Collectors.toSet());
        List<String> unknownColumns = values.keySet().stream()
                .filter(name -> !columns.contains(name.toLowerCase(Locale.ROOT)))
                .collect(Collectors.toList());
        if (!unknownColumns.isEmpty()) {
            throw new AnalysisException("unknown column in MERGE UPDATE: "
                    + String.join(", ", unknownColumns));
        }
        return values;
    }

    private Map<String, Expression> buildInsertValues(
            PluginDrivenExternalTable table, MergeNotMatchedClause clause) {
        List<Column> columns = table.getBaseSchema(true).stream()
                .filter(Column::isVisible).collect(Collectors.toList());
        Map<String, Expression> supplied = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        if (clause.getColNames().isEmpty()) {
            if (clause.getRow().size() != columns.size()) {
                throw new AnalysisException("Column count doesn't match value count in MERGE INSERT");
            }
            for (int index = 0; index < columns.size(); index++) {
                supplied.put(columns.get(index).getName(), unwrap(clause.getRow().get(index)));
            }
        } else {
            if (clause.getColNames().size() != clause.getRow().size()) {
                throw new AnalysisException("Column count doesn't match value count in MERGE INSERT");
            }
            for (int index = 0; index < clause.getColNames().size(); index++) {
                if (supplied.put(clause.getColNames().get(index),
                        unwrap(clause.getRow().get(index))) != null) {
                    throw new AnalysisException("Duplicate column name in MERGE INSERT");
                }
            }
        }

        Map<String, Expression> result = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        for (Column column : columns) {
            Expression value = supplied.remove(column.getName());
            if (value == null || value instanceof org.apache.doris.nereids.trees.expressions.DefaultValueSlot) {
                value = defaultValue(table, column);
            }
            result.put(column.getName(), value);
        }
        if (!supplied.isEmpty()) {
            throw new AnalysisException("unknown column in MERGE INSERT: "
                    + String.join(", ", supplied.keySet()));
        }
        return result;
    }

    private static Expression unwrap(NamedExpression expression) {
        return expression instanceof Alias || expression instanceof UnboundAlias
                ? expression.child(0) : expression;
    }

    private static Expression defaultValue(
            PluginDrivenExternalTable table, Column column) {
        if (column.getDefaultValueSql() != null) {
            Expression value = new NereidsParser().parseExpression(column.getDefaultValueSql());
            return value instanceof UnboundAlias ? value.child(0) : value;
        }
        if (column.isAllowNull()) {
            return new NullLiteral(DataType.fromCatalogType(column.getType()));
        }
        throw new AnalysisException("Column has no default value in MERGE INSERT: "
                + table.getName() + "." + column.getName());
    }

    private InsertOverwriteTableCommand buildOverwriteCommand(LogicalPlan mergeQuery) {
        LogicalPlan sink = (LogicalPlan) UnboundTableSinkCreator.createUnboundTableSink(
                targetNameParts, ImmutableList.of(), ImmutableList.of(), false,
                ImmutableList.of(), false, TPartialUpdateNewRowPolicy.APPEND,
                DMLCommandType.MERGE, mergeQuery);
        return new InsertOverwriteTableCommand(
                sink, Optional.empty(), Optional.empty(), Optional.empty());
    }

    String buildCountSql(long version, SourceInfo sourceInfo) {
        String targetName = ConnectorCopyOnWriteUtils.quoteQualifiedName(targetNameParts);
        String sourceName = ConnectorCopyOnWriteUtils.quoteQualifiedName(sourceInfo.nameParts);
        String targetAliasSql = ConnectorCopyOnWriteUtils.quoteIdentifier(targetAlias.get());
        String sourceAliasSql = ConnectorCopyOnWriteUtils.quoteIdentifier(sourceInfo.alias);
        String targetMarker = ConnectorCopyOnWriteUtils.quoteIdentifier(TARGET_PRESENT);
        String sourceMarker = ConnectorCopyOnWriteUtils.quoteIdentifier(SOURCE_PRESENT);
        String matched = applicablePredicate(matchedClauses.stream()
                .map(MergeMatchedClause::getCasePredicate).collect(Collectors.toList()));
        String notMatched = applicablePredicate(notMatchedClauses.stream()
                .map(MergeNotMatchedClause::getCasePredicate).collect(Collectors.toList()));
        return "SELECT COUNT(*) FROM (SELECT *, 1 AS " + targetMarker
                + " FROM " + targetName + " FOR VERSION AS OF " + version
                + ") AS " + targetAliasSql
                + " FULL OUTER JOIN (SELECT *, 1 AS " + sourceMarker + " FROM " + sourceName
                + ") AS " + sourceAliasSql + " ON " + onClause.toSql()
                + " WHERE ((" + targetAliasSql + "." + targetMarker + " IS NOT NULL AND "
                + sourceAliasSql + "." + sourceMarker + " IS NOT NULL AND " + matched + ") OR ("
                + targetAliasSql + "." + targetMarker + " IS NULL AND "
                + sourceAliasSql + "." + sourceMarker + " IS NOT NULL AND " + notMatched + "))";
    }

    private static String applicablePredicate(List<Optional<Expression>> predicates) {
        if (predicates.isEmpty()) {
            return "FALSE";
        }
        if (predicates.stream().anyMatch(predicate -> !predicate.isPresent())) {
            return "TRUE";
        }
        return predicates.stream().map(predicate -> "(" + predicate.get().toSql() + ")")
                .collect(Collectors.joining(" OR ", "(", ")"));
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.MERGE_INTO;
    }

    static final class SourceInfo {
        private final OlapTable table;
        private final List<String> nameParts;
        private final String alias;

        SourceInfo(OlapTable table, List<String> nameParts, String alias) {
            this.table = table;
            this.nameParts = ImmutableList.copyOf(nameParts);
            this.alias = alias;
        }
    }
}
