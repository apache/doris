package org.apache.doris.nereids.util;

import com.google.common.collect.ImmutableList;
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
import org.apache.doris.nereids.rules.analysis.ProjectToGlobalAggregate;
import org.apache.doris.nereids.ssb.SSBUtils;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.TestWithFeService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class AnalyzeSubQuery extends TestWithFeService {
    private final NereidsParser parser = new NereidsParser();

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("default_cluster:test");

        createTables("CREATE TABLE IF NOT EXISTS T1 (\n" +
            "    id bigint,\n" +
            "    name char(16),\n" +
            "    score bigint\n" +
            ")\n" +
            "DUPLICATE KEY(id, name)\n" +
            "DISTRIBUTED BY HASH(id) BUCKETS 1\n" +
            "PROPERTIES (\n" +
            "  \"replication_num\" = \"1\"\n" +
            ")\n");
    }

    /**
     * TODO: check bound plan and expression details.
     */
    @Test
    public void q() {
        checkAnalyze("SELECT * FROM (SELECT * FROM T1) AS T");
    }

    private void checkAnalyze(String sql) {
        LogicalPlan analyzed = analyze(sql);
        System.out.println(analyzed.treeString());
        Assertions.assertTrue(checkBound(analyzed));
    }

    private LogicalPlan analyze(String sql) {
        try {
            LogicalPlan parsed = parser.parseSingle(sql);
            return analyze(parsed, connectContext);
        } catch (Throwable t) {
            throw new IllegalStateException("Analyze failed", t);
        }
    }

    private LogicalPlan analyze(LogicalPlan inputPlan, ConnectContext connectContext) {
        Memo memo = new Memo();
        memo.initialize(inputPlan);

        PlannerContext plannerContext = new PlannerContext(memo, connectContext);
        JobContext jobContext = new JobContext(plannerContext, new PhysicalProperties(), Double.MAX_VALUE);
        plannerContext.setCurrentJobContext(jobContext);

        executeRewriteBottomUpJob(plannerContext, new BindFunction());
        executeRewriteBottomUpJob(plannerContext, new BindRelation());
        executeRewriteBottomUpJob(plannerContext, new BindSlotReference());
        executeRewriteBottomUpJob(plannerContext, new ProjectToGlobalAggregate());
        return (LogicalPlan) memo.copyOut();
    }

    private void executeRewriteBottomUpJob(PlannerContext plannerContext, RuleFactory<Plan> ruleFactory) {
        Group rootGroup = plannerContext.getMemo().getRoot();
        RewriteBottomUpJob job = new RewriteBottomUpJob(rootGroup,
            plannerContext.getCurrentJobContext(), ImmutableList.of(ruleFactory));
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
