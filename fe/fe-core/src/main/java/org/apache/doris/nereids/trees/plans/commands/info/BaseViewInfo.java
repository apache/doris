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

package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.catalog.Column;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.DorisParser;
import org.apache.doris.nereids.DorisParser.NamedExpressionContext;
import org.apache.doris.nereids.DorisParser.NamedExpressionSeqContext;
import org.apache.doris.nereids.DorisParserBaseVisitor;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.UnboundResultSink;
import org.apache.doris.nereids.jobs.executor.AbstractBatchJobExecutor;
import org.apache.doris.nereids.jobs.rewrite.RewriteJob;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.analysis.AnalyzeCTE;
import org.apache.doris.nereids.rules.analysis.BindExpression;
import org.apache.doris.nereids.rules.analysis.BindRelation;
import org.apache.doris.nereids.rules.analysis.CheckPolicy;
import org.apache.doris.nereids.rules.analysis.EliminateLogicalSelectHint;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionVisitor;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalGenerate;
import org.apache.doris.nereids.trees.plans.logical.LogicalHaving;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;
import org.apache.doris.nereids.trees.plans.logical.LogicalView;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanVisitor;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Lists;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.RuleNode;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/** BaseViewInfo */
public class BaseViewInfo {
    protected final TableNameInfo viewName;
    protected LogicalPlan logicalQuery;
    protected final String querySql;
    protected final List<SimpleColumnDefinition> simpleColumnDefinitions;
    protected final List<Column> finalCols = Lists.newArrayList();
    protected Plan analyzedPlan;

    public BaseViewInfo(TableNameInfo viewName,
            String querySql, List<SimpleColumnDefinition> simpleColumnDefinitions) {
        this.viewName = viewName;
        this.querySql = querySql;
        this.simpleColumnDefinitions = simpleColumnDefinitions;
    }

    protected void analyzeAndFillRewriteSqlMap(String sql, ConnectContext ctx) throws AnalysisException {
        StatementContext stmtCtx = ctx.getStatementContext();
        LogicalPlan parsedViewPlan = new NereidsParser().parseForCreateView(sql);
        logicalQuery = parsedViewPlan;
        if (logicalQuery instanceof LogicalFileSink) {
            throw new AnalysisException("Not support OUTFILE clause in CREATE VIEW statement");
        }
        if (parsedViewPlan instanceof UnboundResultSink) {
            parsedViewPlan = (LogicalPlan) ((UnboundResultSink<?>) parsedViewPlan).child();
        }
        CascadesContext viewContextForStar = CascadesContext.initContext(
                stmtCtx, parsedViewPlan, PhysicalProperties.ANY);
        AnalyzerForCreateView analyzerForStar = new AnalyzerForCreateView(viewContextForStar);
        analyzerForStar.analyze();
        analyzedPlan = viewContextForStar.getRewritePlan();
        // Traverse all slots in the plan, and add the slot's location information
        // and the fully qualified replacement string to the indexInSqlToString of the StatementContext.
        analyzedPlan.accept(PlanSlotFinder.INSTANCE, ctx.getStatementContext());
    }

    protected String rewriteSql(TreeMap<Pair<Integer, Integer>, String> indexStringSqlMap) {
        StringBuilder builder = new StringBuilder();
        int beg = 0;
        for (Map.Entry<Pair<Integer, Integer>, String> entry : indexStringSqlMap.entrySet()) {
            Pair<Integer, Integer> index = entry.getKey();
            builder.append(querySql, beg, index.first);
            builder.append(entry.getValue());
            beg = index.second + 1;
        }
        builder.append(querySql, beg, querySql.length());
        return builder.toString();
    }

    protected String rewriteProjectsToUserDefineAlias(String resSql) {
        IndexFinder finder = new IndexFinder();
        ParserRuleContext tree = NereidsParser.toAst(resSql, DorisParser::singleStatement);
        finder.visit(tree);
        if (simpleColumnDefinitions.isEmpty()) {
            return resSql;
        }
        List<NamedExpressionContext> namedExpressionContexts = finder.getNamedExpressionContexts();
        StringBuilder replaceWithColsBuilder = new StringBuilder();
        for (int i = 0; i < namedExpressionContexts.size(); ++i) {
            NamedExpressionContext namedExpressionContext = namedExpressionContexts.get(i);
            int start = namedExpressionContext.expression().start.getStartIndex();
            int stop = namedExpressionContext.expression().stop.getStopIndex();
            replaceWithColsBuilder.append(resSql, start, stop + 1);
            replaceWithColsBuilder.append(" AS `");
            String escapeBacktick = finalCols.get(i).getName().replace("`", "``");
            replaceWithColsBuilder.append(escapeBacktick);
            replaceWithColsBuilder.append('`');
            if (i != namedExpressionContexts.size() - 1) {
                replaceWithColsBuilder.append(", ");
            }
        }
        String replaceWithCols = replaceWithColsBuilder.toString();
        return StringUtils.overlay(resSql, replaceWithCols, finder.getIndex().first,
                finder.getIndex().second + 1);
    }

    protected void createFinalCols(List<Slot> outputs) throws org.apache.doris.common.AnalysisException {
        if (simpleColumnDefinitions.isEmpty()) {
            for (Slot output : outputs) {
                Column column = new Column(output.getName(), output.getDataType().toCatalogDataType());
                finalCols.add(column);
            }
        } else {
            if (outputs.size() != simpleColumnDefinitions.size()) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_VIEW_WRONG_LIST);
            }
            for (int i = 0; i < simpleColumnDefinitions.size(); ++i) {
                Column column = new Column(simpleColumnDefinitions.get(i).getName(),
                        outputs.get(i).getDataType().toCatalogDataType());
                column.setComment(simpleColumnDefinitions.get(i).getComment());
                finalCols.add(column);
            }
        }
    }

    /** traverse ast to find the outermost project list location information in sql*/
    protected static class IndexFinder extends DorisParserBaseVisitor<Void> {
        private boolean found = false;
        private int startIndex;
        private int stopIndex;
        private List<NamedExpressionContext> namedExpressionContexts = Lists.newArrayList();

        @Override
        public Void visitChildren(RuleNode node) {
            if (found) {
                return null;
            }
            int n = node.getChildCount();
            for (int i = 0; i < n; ++i) {
                ParseTree c = node.getChild(i);
                c.accept(this);
            }
            return null;
        }

        @Override
        public Void visitCte(DorisParser.CteContext ctx) {
            return null;
        }

        @Override
        public Void visitSelectColumnClause(DorisParser.SelectColumnClauseContext ctx) {
            if (found) {
                return null;
            }
            startIndex = ctx.getStart().getStartIndex();
            stopIndex = ctx.getStop().getStopIndex();
            found = true;

            NamedExpressionSeqContext namedExpressionSeqContext = ctx.namedExpressionSeq();
            namedExpressionContexts = namedExpressionSeqContext.namedExpression();
            return null;
        }

        public Pair<Integer, Integer> getIndex() {
            return Pair.of(startIndex, stopIndex);
        }

        public List<NamedExpressionContext> getNamedExpressionContexts() {
            return namedExpressionContexts;
        }
    }

    /**AnalyzerForCreateView*/
    protected static class AnalyzerForCreateView extends AbstractBatchJobExecutor {
        private final List<RewriteJob> jobs;

        public AnalyzerForCreateView(CascadesContext cascadesContext) {
            super(cascadesContext);
            jobs = buildAnalyzeViewJobsForStar();
        }

        public void analyze() {
            execute();
        }

        @Override
        public List<RewriteJob> getJobs() {
            return jobs;
        }

        private static List<RewriteJob> buildAnalyzeViewJobsForStar() {
            return jobs(
                    topDown(new EliminateLogicalSelectHint()),
                    topDown(new AnalyzeCTE()),
                    bottomUp(
                            new BindRelation(),
                            new CheckPolicy(),
                            new BindExpression()
                    )
            );
        }
    }

    private static class PlanSlotFinder extends DefaultPlanVisitor<Void, StatementContext> {
        private static PlanSlotFinder INSTANCE = new PlanSlotFinder();

        @Override
        public Void visitLogicalView(LogicalView<? extends Plan> alias, StatementContext context) {
            return null;
        }

        @Override
        public Void visitLogicalAggregate(LogicalAggregate<? extends Plan> aggregate, StatementContext context) {
            for (Expression expr : aggregate.getGroupByExpressions()) {
                expr.accept(SlotDealer.INSTANCE, context);
            }
            for (NamedExpression expr : aggregate.getOutputExpressions()) {
                expr.accept(SlotDealer.INSTANCE, context);
            }
            return aggregate.child().accept(this, context);
        }

        @Override
        public Void visitLogicalFilter(LogicalFilter<? extends Plan> filter, StatementContext context) {
            for (Expression expr : filter.getConjuncts()) {
                expr.accept(SlotDealer.INSTANCE, context);
            }
            return filter.child().accept(this, context);
        }

        public Void visitLogicalGenerate(LogicalGenerate<? extends Plan> generate, StatementContext context) {
            for (Expression expr : generate.getGenerators()) {
                expr.accept(SlotDealer.INSTANCE, context);
            }
            return generate.child().accept(this, context);
        }

        @Override
        public Void visitLogicalHaving(LogicalHaving<? extends Plan> having, StatementContext context) {
            for (Expression expr : having.getConjuncts()) {
                expr.accept(SlotDealer.INSTANCE, context);
            }
            return having.child().accept(this, context);
        }

        @Override
        public Void visitLogicalJoin(LogicalJoin<? extends Plan, ? extends Plan> join, StatementContext context) {
            for (Expression expr : join.getOtherJoinConjuncts()) {
                expr.accept(SlotDealer.INSTANCE, context);
            }
            for (Expression expr : join.getHashJoinConjuncts()) {
                expr.accept(SlotDealer.INSTANCE, context);
            }
            join.child(0).accept(this, context);
            return join.child(1).accept(this, context);
        }

        @Override
        public Void visitLogicalProject(LogicalProject<? extends Plan> project, StatementContext context) {
            for (Expression expr : project.getProjects()) {
                expr.accept(SlotDealer.INSTANCE, context);
            }
            return project.child().accept(this, context);
        }

        @Override
        public Void visitLogicalRepeat(LogicalRepeat<? extends Plan> repeat, StatementContext context) {
            for (Expression expr : repeat.getOutputExpressions()) {
                expr.accept(SlotDealer.INSTANCE, context);
            }
            for (List<Expression> exprs : repeat.getGroupingSets()) {
                for (Expression expr : exprs) {
                    expr.accept(SlotDealer.INSTANCE, context);
                }
            }
            return repeat.child().accept(this, context);
        }

        @Override
        public Void visitLogicalSort(LogicalSort<? extends Plan> sort, StatementContext context) {
            for (OrderKey key : sort.getOrderKeys()) {
                key.getExpr().accept(SlotDealer.INSTANCE, context);
            }
            return sort.child().accept(this, context);
        }

        @Override
        public Void visitLogicalTopN(LogicalTopN<? extends Plan> topN, StatementContext context) {
            for (OrderKey key : topN.getOrderKeys()) {
                key.getExpr().accept(SlotDealer.INSTANCE, context);
            }
            return topN.child().accept(this, context);
        }

        @Override
        public Void visitLogicalWindow(LogicalWindow<? extends Plan> window, StatementContext context) {
            for (Expression expr : window.getWindowExpressions()) {
                expr.accept(SlotDealer.INSTANCE, context);
            }
            return window.child().accept(this, context);
        }
    }

    private static class SlotDealer extends DefaultExpressionVisitor<Void, StatementContext> {
        private static final SlotDealer INSTANCE = new SlotDealer();

        @Override
        public Void visitSlot(Slot slot, StatementContext ctx) {
            slot.getIndexInSqlString().ifPresent(index ->
                    ctx.addIndexInSqlToString(index,
                            Utils.qualifiedNameWithBackquote(slot.getQualifier(), slot.getName()))
            );
            return null;
        }
    }
}
