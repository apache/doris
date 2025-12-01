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

package org.apache.doris.nereids.trees.plans.commands.merge;

import org.apache.doris.analysis.ColumnDef.DefaultValue;
import org.apache.doris.analysis.StmtType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
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
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.DefaultValueSlot;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Now;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.plans.Explainable;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.commands.ForwardWithSync;
import org.apache.doris.nereids.trees.plans.commands.UpdateCommand;
import org.apache.doris.nereids.trees.plans.commands.info.DMLCommandType;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.RelationUtil;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.thrift.TPartialUpdateNewRowPolicy;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * merge into table
 */
public class MergeIntoCommand extends Command implements ForwardWithSync, Explainable {
    private static final String BRANCH_LABEL = "__DORIS_MERGE_INTO_BRANCH_LABEL__";

    private final List<String> targetNameParts;
    private final Optional<String> targetAlias;
    private final List<String> targetNameInPlan;
    private final Optional<LogicalPlan> cte;
    private final LogicalPlan source;
    private final Expression onClause;
    private final List<MergeMatchedClause> matchedClauses;
    private final List<MergeNotMatchedClause> notMatchedClauses;

    /**
     * constructor.
     */
    public MergeIntoCommand(List<String> targetNameParts, Optional<String> targetAlias,
            Optional<LogicalPlan> cte, LogicalPlan source,
            Expression onClause, List<MergeMatchedClause> matchedClauses,
            List<MergeNotMatchedClause> notMatchedClauses) {
        super(PlanType.MERGE_INTO_COMMAND);
        this.targetNameParts = Utils.fastToImmutableList(
                Objects.requireNonNull(targetNameParts, "targetNameParts should not be null"));
        this.targetAlias = Objects.requireNonNull(targetAlias, "targetAlias should not be null");
        if (targetAlias.isPresent()) {
            this.targetNameInPlan = ImmutableList.of(targetAlias.get());
        } else {
            this.targetNameInPlan = ImmutableList.copyOf(targetNameParts);
        }
        this.cte = Objects.requireNonNull(cte, "cte should not be null");
        this.source = Objects.requireNonNull(source, "source should not be null");
        this.onClause = Objects.requireNonNull(onClause, "onClause should not be null");
        this.matchedClauses = Utils.fastToImmutableList(
                Objects.requireNonNull(matchedClauses, "matchedClauses should not be null"));
        this.notMatchedClauses = Utils.fastToImmutableList(
                Objects.requireNonNull(notMatchedClauses, "notMatchedClauses should not be null"));
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        new InsertIntoTableCommand(completeQueryPlan(ctx), Optional.empty(), Optional.empty(),
                Optional.empty(), true, Optional.empty()).run(ctx, executor);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitMergeIntoCommand(this, context);
    }

    @Override
    public Plan getExplainPlan(ConnectContext ctx) {
        return completeQueryPlan(ctx);
    }

    private OlapTable getTargetTable(ConnectContext ctx) {
        List<String> qualifiedTableName = RelationUtil.getQualifierName(ctx, targetNameParts);
        TableIf table = RelationUtil.getTable(qualifiedTableName, ctx.getEnv(), Optional.empty());
        if (!(table instanceof OlapTable) || !((OlapTable) table).getEnableUniqueKeyMergeOnWrite()) {
            throw new AnalysisException("merge into command only support MOW unique key olapTable");
        }
        return ((OlapTable) table);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.MERGE_INTO;
    }

    /**
     * generate target right outer join source.
     */
    private LogicalPlan generateBasePlan() {
        LogicalPlan plan = LogicalPlanBuilderAssistant.withCheckPolicy(
                new UnboundRelation(
                        StatementScopeIdGenerator.newRelationId(),
                        targetNameParts
                )
        );
        if (targetAlias.isPresent()) {
            plan = new LogicalSubQueryAlias<>(targetAlias.get(), plan);
        }
        return new LogicalJoin<>(JoinType.LEFT_OUTER_JOIN,
                ImmutableList.of(), ImmutableList.of(onClause),
                source, plan, JoinReorderContext.EMPTY);
    }

    /**
     * generate a branch number column to indicate this row matched witch branch
     */
    private NamedExpression generateBranchLabel(NamedExpression deleteSign) {
        Expression matchedLabel = new NullLiteral(IntegerType.INSTANCE);
        for (int i = matchedClauses.size() - 1; i >= 0; i--) {
            MergeMatchedClause clause = matchedClauses.get(i);
            if (i != matchedClauses.size() - 1 && !clause.getCasePredicate().isPresent()) {
                throw new AnalysisException("Only the last matched clause could without case predicate.");
            }
            Expression currentResult = new IntegerLiteral(i);
            if (clause.getCasePredicate().isPresent()) {
                matchedLabel = new If(clause.getCasePredicate().get(), currentResult, matchedLabel);
            } else {
                matchedLabel = currentResult;
            }
        }
        Expression notMatchedLabel = new NullLiteral(IntegerType.INSTANCE);
        for (int i = notMatchedClauses.size() - 1; i >= 0; i--) {
            MergeNotMatchedClause clause = notMatchedClauses.get(i);
            if (i != notMatchedClauses.size() - 1 && !clause.getCasePredicate().isPresent()) {
                throw new AnalysisException("Only the last not matched clause could without case predicate.");
            }
            Expression currentResult = new IntegerLiteral(i + matchedClauses.size());
            if (clause.getCasePredicate().isPresent()) {
                notMatchedLabel = new If(clause.getCasePredicate().get(), currentResult, notMatchedLabel);
            } else {
                notMatchedLabel = currentResult;
            }
        }
        return new UnboundAlias(new If(new Not(new IsNull(deleteSign)),
                matchedLabel, notMatchedLabel), BRANCH_LABEL);
    }

    private List<Expression> generateDeleteProjection(List<Column> columns) {
        ImmutableList.Builder<Expression> builder = ImmutableList.builder();
        for (Column column : columns) {
            // delete
            if (column.isDeleteSignColumn()) {
                builder.add(new TinyIntLiteral(((byte) 1)));
            } else if ((!column.isVisible() && !column.isSequenceColumn()) || column.isGeneratedColumn()) {
                // skip this column
                continue;
            } else {
                List<String> nameParts = Lists.newArrayList(targetNameInPlan);
                nameParts.add(column.getName());
                builder.add(new UnboundSlot(nameParts));
            }
        }
        return builder.build();
    }

    private List<Expression> generateUpdateProjection(MergeMatchedClause clause,
            List<Column> columns, OlapTable targetTable, ConnectContext ctx) {
        ImmutableList.Builder<Expression> builder = ImmutableList.builder();
        Map<String, Expression> colNameToExpression = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        // update
        for (EqualTo equalTo : clause.getAssignments()) {
            List<String> nameParts = ((UnboundSlot) equalTo.left()).getNameParts();
            UpdateCommand.checkAssignmentColumn(ctx, nameParts, targetNameParts, targetAlias.orElse(null));
            if (colNameToExpression.put(nameParts.get(nameParts.size() - 1), equalTo.right()) != null) {
                throw new AnalysisException("Duplicate column name in update: " + nameParts.get(nameParts.size() - 1));
            }
        }
        for (Column column : columns) {
            DataType dataType = DataType.fromCatalogType(column.getType());
            if (colNameToExpression.containsKey(column.getName())) {
                if (column.isKey()) {
                    throw new AnalysisException("Only value columns of unique table could be updated");
                }
                if (column.isGeneratedColumn()) {
                    throw new AnalysisException("The value specified for generated column '"
                            + column.getName() + "' in table '" + targetTable.getName() + "' is not allowed.");
                }
                builder.add(new Cast(colNameToExpression.get(column.getName()), dataType));
                colNameToExpression.remove(column.getName());
            } else if (column.isGeneratedColumn() || (!column.isVisible()
                    && !column.isDeleteSignColumn() && !column.isSequenceColumn())) {
                // skip these columns
                continue;
            } else if (column.hasOnUpdateDefaultValue()) {
                builder.add(new Cast(new NereidsParser().parseExpression(
                        column.getOnUpdateDefaultValueSql()), dataType));
            } else {
                List<String> nameParts = Lists.newArrayList(targetNameInPlan);
                nameParts.add(column.getName());
                builder.add(new Cast(new UnboundSlot(nameParts), dataType));
            }
        }
        if (!colNameToExpression.isEmpty()) {
            throw new AnalysisException("unknown column in assignment list: "
                    + String.join(", ", colNameToExpression.keySet()));
        }
        return builder.build();
    }

    private List<Expression> generateInsertWithoutColListProjection(MergeNotMatchedClause clause,
            List<Column> columns, OlapTable targetTable, boolean hasSequenceCol, int seqColumnIndex,
            Optional<Column> seqMappingColInTable, Optional<Type> seqColType) {
        ImmutableList.Builder<Expression> builder = ImmutableList.builder();
        if (hasSequenceCol && seqColumnIndex < 0) {
            if ((!seqMappingColInTable.isPresent() || seqMappingColInTable.get().getDefaultValue() == null
                    || !seqMappingColInTable.get().getDefaultValue()
                    .equalsIgnoreCase(DefaultValue.CURRENT_TIMESTAMP))) {
                throw new AnalysisException("Table " + targetTable.getName()
                        + " has sequence column, need to specify the sequence column");
            }
        }
        Expression sqlColExpr = new Now();
        for (int i = 0; i < clause.getRow().size(); i++) {
            DataType columnType = DataType.fromCatalogType(columns.get(i).getType());
            NamedExpression rowItem = clause.getRow().get(i);
            Expression value;
            if (rowItem instanceof Alias || rowItem instanceof UnboundAlias) {
                value = rowItem.child(0);
            } else {
                value = rowItem;
            }
            if (columns.get(i).isGeneratedColumn()) {
                if (!(value instanceof DefaultValueSlot)) {
                    throw new AnalysisException("The value specified for generated column '"
                            + columns.get(i).getName()
                            + "' in table '" + targetTable.getName() + "' is not allowed.");
                }
                continue;
            }
            value = new Cast(value, columnType);
            if (i == seqColumnIndex) {
                sqlColExpr = value;
            }
            builder.add(value);
        }
        // delete sign
        builder.add(new TinyIntLiteral(((byte) 0)));
        // sequence column
        if (hasSequenceCol) {
            builder.add(new Cast(sqlColExpr, seqColType.map(DataType::fromCatalogType).get()));
        }
        return builder.build();
    }

    private List<Expression> generateInsertWithColListProjection(MergeNotMatchedClause clause,
            List<Column> columns, OlapTable targetTable, boolean hasSequenceCol,
            String seqColumnName, Optional<Column> seqMappingColInTable, Optional<Type> seqColType) {
        ImmutableList.Builder<Expression> builder = ImmutableList.builder();
        Map<String, Expression> colNameToExpression = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        for (int i = 0; i < clause.getColNames().size(); i++) {
            String targetColumnName = clause.getColNames().get(i);
            NamedExpression rowItem = clause.getRow().get(i);
            if (rowItem instanceof Alias || rowItem instanceof UnboundAlias) {
                colNameToExpression.put(targetColumnName, rowItem.child(0));
            } else {
                colNameToExpression.put(targetColumnName, rowItem);
            }
        }
        if (colNameToExpression.size() != clause.getColNames().size()) {
            throw new AnalysisException("insert has duplicate column names");
        }
        if (hasSequenceCol) {
            if (seqColumnName == null || seqColumnName.isEmpty()) {
                seqColumnName = Column.SEQUENCE_COL;
            }
            if (!colNameToExpression.containsKey(seqColumnName)
                    && (!seqMappingColInTable.isPresent() || seqMappingColInTable.get().getDefaultValue() == null
                    || !seqMappingColInTable.get().getDefaultValue()
                    .equalsIgnoreCase(DefaultValue.CURRENT_TIMESTAMP))) {
                throw new AnalysisException("Table " + targetTable.getName()
                        + " has sequence column, need to specify the sequence column");
            }
        }
        for (Column column : columns) {
            DataType type = DataType.fromCatalogType(column.getType());
            if (column.isGeneratedColumn()) {
                if (colNameToExpression.containsKey(column.getName())) {
                    if (!(colNameToExpression.get(column.getName()) instanceof DefaultValueSlot)) {
                        throw new AnalysisException("The value specified for generated column '"
                                + column.getName() + "' in table '" + targetTable.getName() + "' is not allowed.");
                    }
                    colNameToExpression.remove(column.getName());
                }
                continue;
            } else if (!column.isVisible()) {
                // skip these columns
                continue;
            } else if (colNameToExpression.containsKey(column.getName())) {
                builder.add(new Cast(colNameToExpression.get(column.getName()), type));
                if (!column.getName().equalsIgnoreCase(seqColumnName)) {
                    colNameToExpression.remove(column.getName());
                }
            } else {
                if (column.getDefaultValueSql() == null) {
                    if (!column.isAllowNull() && !column.isAutoInc()) {
                        throw new AnalysisException("Column has no default value,"
                                + " column=" + column.getName());
                    }
                    builder.add(new NullLiteral(type));
                } else {
                    Expression defaultExpr;
                    try {
                        // it comes from the original planner, if default value expression is
                        // null, we use the literal string of the default value, or it may be
                        // default value function, like CURRENT_TIMESTAMP.
                        Expression unboundDefaultValue = new NereidsParser().parseExpression(
                                column.getDefaultValueSql());
                        if (unboundDefaultValue instanceof UnboundAlias) {
                            unboundDefaultValue = ((UnboundAlias) unboundDefaultValue).child();
                        }
                        defaultExpr = new Cast(unboundDefaultValue, type);
                    } catch (Exception e) {
                        throw new AnalysisException(e.getMessage(), e.getCause());
                    }
                    builder.add(defaultExpr);
                }
            }
        }
        builder.add(colNameToExpression.getOrDefault(Column.DELETE_SIGN, new TinyIntLiteral(((byte) 0))));
        colNameToExpression.remove(Column.DELETE_SIGN);
        if (hasSequenceCol) {
            Expression forSeqCol;
            if (colNameToExpression.containsKey(Column.SEQUENCE_COL)) {
                forSeqCol = colNameToExpression.get(Column.SEQUENCE_COL);
                colNameToExpression.remove(Column.SEQUENCE_COL);
                colNameToExpression.remove(seqColumnName);
            } else if (colNameToExpression.containsKey(seqColumnName)) {
                forSeqCol = colNameToExpression.get(seqColumnName);
                colNameToExpression.remove(seqColumnName);
            } else {
                forSeqCol = new Now();
            }
            builder.add(new Cast(forSeqCol, seqColType.map(DataType::fromCatalogType).get()));
        }
        if (!colNameToExpression.isEmpty()) {
            throw new AnalysisException("unknown column in target table: "
                    + String.join(", ", colNameToExpression.keySet()));
        }
        return builder.build();
    }

    private List<NamedExpression> generateFinalProjections(List<String> colNames,
            List<List<Expression>> finalProjections) {
        for (List<Expression> projection : finalProjections) {
            if (projection.size() != finalProjections.get(0).size()) {
                throw new AnalysisException("Column count doesn't match each other");
            }
        }
        ImmutableList.Builder<NamedExpression> outputProjectionsBuilder = ImmutableList.builder();
        for (int i = 0; i < finalProjections.get(0).size(); i++) {
            Expression project = new NullLiteral();
            for (int j = 0; j < finalProjections.size(); j++) {
                project = new If(new EqualTo(new UnboundSlot(BRANCH_LABEL), new IntegerLiteral(j)),
                        finalProjections.get(j).get(i), project);
            }
            outputProjectionsBuilder.add(new UnboundAlias(project, colNames.get(i)));
        }
        return outputProjectionsBuilder.build();
    }

    /**
     * complete merge into plan.
     */
    private LogicalPlan completeQueryPlan(ConnectContext ctx) {
        // check insert include all keys
        OlapTable targetTable = getTargetTable(ctx);
        List<Column> columns = targetTable.getBaseSchema(true);
        // compute sequence column info
        boolean hasSequenceCol = targetTable.hasSequenceCol();
        String seqColName = null;
        int seqColumnIndex = -1;
        Optional<Column> seqMappingColInTable = Optional.empty();
        if (hasSequenceCol) {
            seqColName = targetTable.getSequenceMapCol();
            String finalSeqColName = seqColName;
            if (seqColName != null) {
                for (int i = 0; i < columns.size(); i++) {
                    Column column = columns.get(i);
                    if (column.getName().equalsIgnoreCase(seqColName)) {
                        seqColumnIndex = i;
                        break;
                    }
                }
                seqMappingColInTable = columns.stream()
                        .filter(col -> col.getName().equalsIgnoreCase(finalSeqColName))
                        .findFirst();
            }

        }
        if (seqColumnIndex != -1 && !seqMappingColInTable.isPresent()) {
            throw new AnalysisException("sequence column is not contained in"
                    + " target table " + targetTable.getName());
        }

        // generate base plan
        LogicalPlan plan = generateBasePlan();
        // generate a project to add delete sign, seq column, label and mark
        ImmutableList.Builder<NamedExpression> outputProjections = ImmutableList.builder();
        outputProjections.add(new UnboundStar(ImmutableList.of()));
        List<String> targetDeleteSignNameParts = Lists.newArrayList(targetNameInPlan);
        targetDeleteSignNameParts.add(Column.DELETE_SIGN);
        NamedExpression deleteSign = new UnboundSlot(targetDeleteSignNameParts);
        outputProjections.add(deleteSign);
        outputProjections.add(generateBranchLabel(deleteSign));
        if (hasSequenceCol) {
            List<String> targetSeqColNameParts = Lists.newArrayList(targetNameInPlan);
            targetSeqColNameParts.add(Column.SEQUENCE_COL);
            NamedExpression seqCol = new UnboundSlot(targetSeqColNameParts);
            outputProjections.add(seqCol);
        }
        plan = new LogicalProject<>(outputProjections.build(), plan);
        // remove all lines that do not be used for update, delete and insert
        plan = new LogicalFilter<>(ImmutableSet.of(new Not(new IsNull(new UnboundSlot(BRANCH_LABEL)))), plan);
        // compute final project by branch number and add delete sign
        List<List<Expression>> finalProjections = Lists.newArrayList();
        // matched
        for (MergeMatchedClause clause : matchedClauses) {
            if (clause.isDelete()) {
                finalProjections.add(generateDeleteProjection(columns));
            } else {
                finalProjections.add(generateUpdateProjection(clause, columns, targetTable, ctx));
            }
        }
        // not matched
        long columnCount = columns.stream().filter(Column::isVisible).count();
        for (MergeNotMatchedClause clause : notMatchedClauses) {
            if (clause.getColNames().isEmpty()) {
                if (columnCount != clause.getRow().size()) {
                    throw new AnalysisException("Column count doesn't match value count");
                }
                finalProjections.add(generateInsertWithoutColListProjection(clause, columns, targetTable,
                        hasSequenceCol, seqColumnIndex, seqMappingColInTable,
                        Optional.ofNullable(targetTable.getSequenceType())));
            } else {
                if (clause.getColNames().size() != clause.getRow().size()) {
                    throw new AnalysisException("Column count doesn't match value count");
                }
                finalProjections.add(generateInsertWithColListProjection(clause, columns, targetTable,
                        hasSequenceCol, seqColName, seqMappingColInTable,
                        Optional.ofNullable(targetTable.getSequenceType())));
            }
        }
        List<String> colNames = columns.stream()
                .filter(c -> (c.isVisible() && !c.isGeneratedColumn())
                        || c.isDeleteSignColumn() || c.isSequenceColumn())
                .map(Column::getName)
                .collect(ImmutableList.toImmutableList());
        plan = new LogicalProject<>(generateFinalProjections(colNames, finalProjections), plan);

        // TODO 6, 7, 8, 9 strict mode
        // 6. add a set of new columns used for group by: if(mark = 1, target keys + mark, insert keys + mark)
        // 7. add window node, partition by group by key, order by 1, row number, count(update) as uc, max(delete) as dc
        // 8. get row_number = 1
        // 9. assert_true(uc <= 1 and (uc = 0 || dc = 0) (optional)

        if (cte.isPresent()) {
            plan = (LogicalPlan) cte.get().withChildren(plan);
        }
        plan = UnboundTableSinkCreator.createUnboundTableSink(targetNameParts, colNames, ImmutableList.of(),
                false, ImmutableList.of(), false, TPartialUpdateNewRowPolicy.APPEND,
                DMLCommandType.INSERT, plan);
        return plan;
    }
}
