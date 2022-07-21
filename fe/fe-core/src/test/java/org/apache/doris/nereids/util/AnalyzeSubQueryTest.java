package org.apache.doris.nereids.util;

import org.apache.doris.nereids.PlannerContext;
import org.apache.doris.nereids.analyzer.Unbound;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.jobs.rewrite.RewriteBottomUpJob;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.Memo;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.RuleFactory;
import org.apache.doris.nereids.rules.analysis.BindFunction;
import org.apache.doris.nereids.rules.analysis.BindRelation;
import org.apache.doris.nereids.rules.analysis.BindSlotReference;
import org.apache.doris.nereids.rules.analysis.BindSubQueryAlias;
import org.apache.doris.nereids.rules.analysis.ProjectToGlobalAggregate;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class AnalyzeSubQueryTest extends TestWithFeService {
    private final NereidsParser parser = new NereidsParser();

    private final List<String> testSql = Lists.newArrayList(
            "SELECT * FROM T1",
            "SELECT * FROM T1 JOIN T2 ON T1.ID = T2.ID",
            "SELECT * FROM T1 T",
            "SELECT * FROM (SELECT * FROM T1) T",
            "SELECT T1.ID ID FROM T1",
            "SELECT T.ID FROM T1 T",
            "SELECT X.ID FROM (SELECT * FROM (T1) A JOIN T1 AS B ON T1.ID = T2.ID) X WHERE X.SCORE < 20"
    );
    //    private final String testSql = "SELECT X.ID FROM (SELECT * FROM (T1) A JOIN T1 AS B ON T1.ID = T2.ID) X WHERE X.SCORE < 20";

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("default_cluster:test");

        createTables(
                "CREATE TABLE IF NOT EXISTS T1 (\n"
                        + "    id bigint,\n"
                        + "    score bigint\n"
                        + ")\n"
                        + "DUPLICATE KEY(id)\n"
                        + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                        + "PROPERTIES (\n"
                        + "  \"replication_num\" = \"1\"\n"
                        + ")\n",
                "CREATE TABLE IF NOT EXISTS T2 (\n"
                        + "    id bigint,\n"
                        + "    score bigint\n"
                        + ")\n"
                        + "DUPLICATE KEY(id)\n"
                        + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                        + "PROPERTIES (\n"
                        + "  \"replication_num\" = \"1\"\n"
                        + ")\n"
        );
    }

    /**
     * TODO: check bound plan and expression details.
     */
    @Test
    public void testAnalyze() {
        checkAnalyze(testSql.get(0));
    }

    @Test
    public void testParse() {
        System.out.println(parser.parseSingle(testSql.get(3)).treeString());
    }

    private void checkAnalyze(String sql) {
        LogicalPlan analyzed = analyze(sql);
        System.out.println(analyzed.treeString());
        Assertions.assertTrue(checkBound(analyzed));
    }

    private LogicalPlan analyze(String sql) {
        try {
            LogicalPlan parsed = parser.parseSingle(sql);
            System.out.println(parsed.treeString());
            return analyze(parsed, connectContext);
        } catch (Throwable t) {
            throw new IllegalStateException("Analyze failed", t);
        }
    }

    private LogicalPlan analyze(LogicalPlan inputPlan, ConnectContext connectContext) {
        Memo memo = new Memo(inputPlan);

        PlannerContext plannerContext = new PlannerContext(memo, connectContext);
        JobContext jobContext = new JobContext(plannerContext, new PhysicalProperties(), Double.MAX_VALUE);
        plannerContext.setCurrentJobContext(jobContext);

        executeRewriteBottomUpJob(plannerContext,
                new BindFunction(),
                new BindRelation(),
                new BindSubQueryAlias(),
                new BindSlotReference(),
                new ProjectToGlobalAggregate());
        return (LogicalPlan) memo.copyOut();
    }

    private void executeRewriteBottomUpJob(PlannerContext plannerContext, RuleFactory... ruleFactory) {
        Group rootGroup = plannerContext.getMemo().getRoot();
        RewriteBottomUpJob job = new RewriteBottomUpJob(rootGroup,
                plannerContext.getCurrentJobContext(), Lists.newArrayList(ruleFactory));
        plannerContext.pushJob(job);
        plannerContext.getJobScheduler().executeJobPool(plannerContext);
    }

    /**
     * PlanNode and its expressions are all bound.
     */
    private boolean checkBound(LogicalPlan plan) {
        if (plan instanceof Unbound) {
            return false;
        }

        List<Plan> children = plan.children();
        for (Plan child : children) {
            if (!checkBound((LogicalPlan) child)) {
                return false;
            }
        }

        List<Expression> expressions = plan.getExpressions();
        return expressions.stream().allMatch(this::checkExpressionBound);
    }

    private boolean checkExpressionBound(Expression expr) {
        if (expr instanceof Unbound) {
            return false;
        }

        List<Expression> children = expr.children();
        for (Expression child : children) {
            if (!checkExpressionBound(child)) {
                return false;
            }
        }
        return true;
    }
}
