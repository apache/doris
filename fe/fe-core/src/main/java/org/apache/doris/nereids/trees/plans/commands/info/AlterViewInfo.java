package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.analysis.AlterViewStmt;
import org.apache.doris.analysis.ColWithComment;
import org.apache.doris.analysis.TableName;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.View;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.Util;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.DorisParser;
import org.apache.doris.nereids.DorisParser.NamedExpressionContext;
import org.apache.doris.nereids.DorisParser.NamedExpressionSeqContext;
import org.apache.doris.nereids.DorisParserBaseVisitor;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.UnboundResultSink;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.jobs.executor.AbstractBatchJobExecutor;
import org.apache.doris.nereids.jobs.rewrite.RewriteJob;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.analysis.AnalyzeCTE;
import org.apache.doris.nereids.rules.analysis.BindExpression;
import org.apache.doris.nereids.rules.analysis.BindRelation;
import org.apache.doris.nereids.rules.analysis.CheckPolicy;
import org.apache.doris.nereids.rules.analysis.EliminateLogicalSelectHint;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand.ExplainLevel;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEAnchor;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanVisitor;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.RuleNode;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.Set;

/** AlterViewInfo */
public class AlterViewInfo {
    private final TableNameInfo viewName;
    private final LogicalPlan logicalQuery;
    private final String querySql;
    private final List<SimpleColumnDefinition> simpleColumnDefinitions;
    private final List<Column> finalCols = Lists.newArrayList();
    private Plan analyzedPlan;

    /** constructor*/
    public AlterViewInfo(TableNameInfo viewName, LogicalPlan logicalQuery,
            String querySql, List<SimpleColumnDefinition> simpleColumnDefinitions) {
        this.viewName = viewName;
        if (logicalQuery instanceof LogicalFileSink) {
            throw new AnalysisException("Not support OUTFILE clause in CREATE VIEW statement");
        }
        this.logicalQuery = logicalQuery;
        this.querySql = querySql;
        this.simpleColumnDefinitions = simpleColumnDefinitions;
    }

    /** init */
    public void init(ConnectContext ctx) throws UserException {
        if (viewName == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_TABLES_USED);
        }
        viewName.analyze(ctx);
        FeNameFormat.checkTableName(viewName.getTbl());
        // disallow external catalog
        Util.prohibitExternalCatalog(viewName.getCtl(), "AlterViewStmt");

        DatabaseIf db = Env.getCurrentInternalCatalog().getDbOrAnalysisException(viewName.getDb());
        TableIf table = db.getTableOrAnalysisException(viewName.getTbl());
        if (!(table instanceof View)) {
            throw new org.apache.doris.common.AnalysisException(
                    String.format("ALTER VIEW not allowed on a table:%s.%s", viewName.getDb(), viewName.getTbl()));
        }

        // check privilege
        if (!Env.getCurrentEnv().getAccessManager().checkTblPriv(ctx, new TableName(viewName.getCtl(), viewName.getDb(),
                viewName.getTbl()), PrivPredicate.ALTER)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLE_ACCESS_DENIED_ERROR,
                    PrivPredicate.ALTER.getPrivs().toString(), viewName.getTbl());
        }
        analyzeAndFillRewriteSqlMap(querySql, ctx);
        OutermostPlanFinderContext
                outermostPlanFinderContext = new OutermostPlanFinderContext();
        analyzedPlan.accept(OutermostPlanFinder.INSTANCE, outermostPlanFinderContext);
        List<Slot> outputs = outermostPlanFinderContext.outermostPlan.getOutput();
        createFinalCols(outputs);
    }

    /**validate*/
    public void validate(ConnectContext ctx) throws UserException {
        NereidsPlanner planner = new NereidsPlanner(ctx.getStatementContext());
        planner.plan(new UnboundResultSink<>(logicalQuery), PhysicalProperties.ANY, ExplainLevel.NONE);
        Set<String> colSets = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        for (Column col : finalCols) {
            if (!colSets.add(col.getName())) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_DUP_FIELDNAME, col.getName());
            }
        }
    }

    /**translateToLegacyStmt*/
    public AlterViewStmt translateToLegacyStmt(ConnectContext ctx) {
        List<ColWithComment> cols = Lists.newArrayList();
        for (SimpleColumnDefinition def : simpleColumnDefinitions) {
            cols.add(def.translateToColWithComment());
        }
        AlterViewStmt alterViewStmt = new AlterViewStmt(viewName.transferToTableName(), cols,
                null);
        // expand star(*) in project list and replace table name with qualifier
        String rewrittenSql = rewriteSql(ctx.getStatementContext().getIndexInSqlToString());

        // rewrite project alias
        rewrittenSql = rewriteProjectsToUserDefineAlias(rewrittenSql);

        alterViewStmt.setInlineViewDef(rewrittenSql);
        alterViewStmt.setFinalColumns(finalCols);
        return alterViewStmt;
    }

    private void analyzeAndFillRewriteSqlMap(String sql, ConnectContext ctx) {
        StatementContext stmtCtx = ctx.getStatementContext();
        LogicalPlan parsedViewPlan = new NereidsParser().parseForCreateView(sql);
        if (parsedViewPlan instanceof UnboundResultSink) {
            parsedViewPlan = (LogicalPlan) ((UnboundResultSink<?>) parsedViewPlan).child();
        }
        CascadesContext viewContextForStar = CascadesContext.initContext(
                stmtCtx, parsedViewPlan, PhysicalProperties.ANY);
        AlterViewInfo.AnalyzerForCreateView analyzerForStar = new AlterViewInfo
                .AnalyzerForCreateView(viewContextForStar);
        analyzerForStar.analyze();
        analyzedPlan = viewContextForStar.getRewritePlan();
    }

    private String rewriteSql(Map<Pair<Integer, Integer>, String> indexStringSqlMap) {
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
        AlterViewInfo.IndexFinder finder = new AlterViewInfo.IndexFinder();
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

    private void createFinalCols(List<Slot> outputs) throws org.apache.doris.common.AnalysisException {
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

    private static class OutermostPlanFinderContext {
        public Plan outermostPlan = null;
        public boolean found = false;
    }

    private static class OutermostPlanFinder extends
            DefaultPlanVisitor<Void, AlterViewInfo.OutermostPlanFinderContext> {
        public static final OutermostPlanFinder INSTANCE = new OutermostPlanFinder();

        @Override
        public Void visit(Plan plan, OutermostPlanFinderContext ctx) {
            if (ctx.found) {
                return null;
            }
            ctx.outermostPlan = plan;
            ctx.found = true;
            return null;
        }

        @Override
        public Void visitLogicalCTEAnchor(LogicalCTEAnchor<? extends Plan, ? extends Plan> cteAnchor,
                AlterViewInfo.OutermostPlanFinderContext ctx) {
            if (ctx.found) {
                return null;
            }
            return super.visit(cteAnchor, ctx);
        }

        @Override
        public Void visitLogicalCTEProducer(LogicalCTEProducer<? extends Plan> cteProducer,
                AlterViewInfo.OutermostPlanFinderContext ctx) {
            return null;
        }
    }

    /** traverse ast to find the outermost project list location information in sql*/
    private static class IndexFinder extends DorisParserBaseVisitor<Void> {
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

    private static class AnalyzerForCreateView extends AbstractBatchJobExecutor {
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
}
