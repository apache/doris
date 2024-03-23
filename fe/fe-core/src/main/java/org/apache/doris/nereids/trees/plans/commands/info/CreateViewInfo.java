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

import org.apache.doris.analysis.ColWithComment;
import org.apache.doris.analysis.CreateViewStmt;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.Util;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.DorisParser;
import org.apache.doris.nereids.DorisParserBaseVisitor;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.UnboundResultSink;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.BoundStar;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand.ExplainLevel;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEProducer;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanVisitor;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.RuleNode;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * CreateViewInfo
 */
public class CreateViewInfo {
    private final boolean ifNotExists;
    private final TableNameInfo viewName;
    private final String comment;
    private final LogicalPlan logicalQuery;
    private final String querySql;
    private final List<SimpleColumnDefinition> simpleColumnDefinitions;
    private final List<Column> finalCols = Lists.newArrayList();
    private final List<Slot> outputs = Lists.newArrayList();
    private final List<NamedExpression> projects = Lists.newArrayList();
    private Plan analyzedPlan;

    /** constructor*/
    public CreateViewInfo(boolean ifNotExists, TableNameInfo viewName, String comment, LogicalPlan logicalQuery,
            String querySql, List<SimpleColumnDefinition> simpleColumnDefinitions) {
        this.ifNotExists = ifNotExists;
        this.viewName = viewName;
        this.comment = comment;
        if (logicalQuery instanceof LogicalFileSink) {
            throw new AnalysisException("Not support OUTFILE clause in CREATE VIEW statement");
        }
        this.logicalQuery = logicalQuery;
        this.querySql = querySql;
        this.simpleColumnDefinitions = simpleColumnDefinitions;
    }

    /**validate*/
    public void validate(ConnectContext ctx) throws UserException {
        NereidsPlanner planner = new NereidsPlanner(ctx.getStatementContext());
        planner.plan(new UnboundResultSink<>(logicalQuery), PhysicalProperties.ANY, ExplainLevel.NONE);
        viewName.analyze(ctx);
        FeNameFormat.checkTableName(viewName.getTbl());
        // disallow external catalog
        Util.prohibitExternalCatalog(viewName.getCtl(), "CreateViewStmt");
        // check privilege
        if (!Env.getCurrentEnv().getAccessManager().checkTblPriv(ConnectContext.get(), viewName.getDb(),
                viewName.getTbl(), PrivPredicate.CREATE)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "CREATE");
        }

        analyzedPlan = parseAndAnalyzeView(querySql, ctx);
        OutermostProjectFinder projectPlanFinder = new OutermostProjectFinder();
        AtomicReference<LogicalProject<Plan>> outermostProject = new AtomicReference<>();
        analyzedPlan.accept(projectPlanFinder, outermostProject);
        outputs.addAll(outermostProject.get().getOutput());
        projects.addAll(outermostProject.get().getProjects());
        createFinalCols();
        Set<String> colSets = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        for (Column col : finalCols) {
            if (!colSets.add(col.getName())) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_DUP_FIELDNAME, col.getName());
            }
        }
    }

    /**translateToLegacyStmt*/
    public CreateViewStmt translateToLegacyStmt(ConnectContext ctx) {
        List<ColWithComment> cols = Lists.newArrayList();
        for (SimpleColumnDefinition def : simpleColumnDefinitions) {
            cols.add(def.transferToColWithComment());
        }
        CreateViewStmt createViewStmt = new CreateViewStmt(ifNotExists, viewName.transferToTableName(), cols, comment,
                null);
        // expand star(*) in project list
        Map<Pair<Integer, Integer>, String> indexStringSqlMap = collectBoundStar(analyzedPlan);
        String rewrittenSql = rewriteStarToColumn(indexStringSqlMap);

        // rewrite project alias
        rewrittenSql = rewriteProjectsToUserDefineAlias(rewrittenSql);

        createViewStmt.setInlineViewDef(rewrittenSql);
        createViewStmt.setFinalColumns(finalCols);
        return createViewStmt;
    }

    private Plan parseAndAnalyzeView(String ddlSql, ConnectContext ctx) {
        StatementContext stmtCtx = ctx.getStatementContext();
        LogicalPlan parsedViewPlan = new NereidsParser().parseSingle(ddlSql);
        // TODO: use a good to do this, such as eliminate UnboundResultSink
        if (parsedViewPlan instanceof UnboundResultSink) {
            parsedViewPlan = (LogicalPlan) ((UnboundResultSink<?>) parsedViewPlan).child();
        }
        CascadesContext viewContext = CascadesContext.initContext(
                stmtCtx, parsedViewPlan, PhysicalProperties.ANY);
        viewContext.keepOrShowPlanProcess(false, () -> {
            viewContext.newAnalyzer(true).analyze();
        });
        return viewContext.getRewritePlan();
    }

    private String rewriteStarToColumn(Map<Pair<Integer, Integer>, String> indexStringSqlMap) {
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

    private String rewriteProjectsToUserDefineAlias(String resSql) {
        List<Expression> projectExprs = Lists.newArrayList();
        for (int i = 0; i < projects.size(); i++) {
            NamedExpression namedExpression = projects.get(i);
            if (namedExpression instanceof Alias) {
                projectExprs.add(namedExpression.child(0));
            } else {
                projectExprs.add(namedExpression);
            }
        }
        IndexFinder finder = new IndexFinder();
        ParserRuleContext tree = NereidsParser.toAst(resSql, DorisParser::singleStatement);
        finder.visit(tree);
        StringBuilder replaceWithColsBuilder = new StringBuilder();
        for (int i = 0; i < projectExprs.size(); ++i) {
            replaceWithColsBuilder.append(projectExprs.get(i).toSql());
            replaceWithColsBuilder.append(" AS `");
            replaceWithColsBuilder.append(finalCols.get(i).getName());
            replaceWithColsBuilder.append('`');
            if (i != projectExprs.size() - 1) {
                replaceWithColsBuilder.append(", ");
            }
        }
        String replaceWithCols = replaceWithColsBuilder.toString();
        return StringUtils.overlay(resSql, replaceWithCols, finder.getIndex().first,
                finder.getIndex().second + 1);
    }

    private void createFinalCols() throws org.apache.doris.common.AnalysisException {
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

    private Map<Pair<Integer, Integer>, String> collectBoundStar(Plan plan) {
        TreeMap<Pair<Integer, Integer>, String> result = new TreeMap<>(new Pair.PairComparator<>());
        plan.foreach(node -> {
            if (node instanceof LogicalProject) {
                LogicalProject<Plan> project = (LogicalProject) node;
                for (BoundStar star : project.getBoundStars()) {
                    result.put(star.getIndexInSqlString(), star.toSql());
                }
            }
        });
        return result;
    }

    private static class OutermostProjectFinder extends DefaultPlanVisitor<Void,
            AtomicReference<LogicalProject<Plan>>> {
        boolean found = false;

        @Override
        public Void visit(Plan plan, AtomicReference<LogicalProject<Plan>> target) {
            if (found) {
                return null;
            }
            if (plan instanceof LogicalCTEProducer) {
                return null;
            } else if (plan instanceof LogicalProject) {
                target.set((LogicalProject<Plan>) plan);
                found = true;
            }
            return super.visit(plan, target);
        }
    }

    /** traverse ast to find the outermost project list location information in sql*/
    private static class IndexFinder extends DorisParserBaseVisitor<Void> {
        private boolean found = false;
        private int startIndex;
        private int stopIndex;

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
            return null;
        }

        public Pair<Integer, Integer> getIndex() {
            return Pair.of(startIndex, stopIndex);
        }
    }
}
