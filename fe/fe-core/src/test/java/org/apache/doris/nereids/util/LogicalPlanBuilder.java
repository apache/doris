package org.apache.doris.nereids.util;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import com.clearspring.analytics.util.Lists;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class LogicalPlanBuilder {
    private final LogicalPlan plan;

    public LogicalPlanBuilder(LogicalPlan plan) {
        this.plan = plan;
    }

    public LogicalPlan build() {
        return plan;
    }

    public LogicalPlanBuilder from(LogicalPlan plan) {
        return new LogicalPlanBuilder(plan);
    }

    public LogicalPlanBuilder scan(long tableId, String tableName, int hashColumn) {
        LogicalOlapScan scan = PlanConstructor.newLogicalOlapScan(tableId, tableName, hashColumn);
        return from(scan);
    }

    public LogicalPlanBuilder projectWithExprs(List<NamedExpression> projectExprs) {
        LogicalProject<LogicalPlan> project = new LogicalProject<>(projectExprs, this.plan);
        return from(project);
    }

    public LogicalPlanBuilder project(List<Integer> slots) {
        List<NamedExpression> projectExprs = Lists.newArrayList();
        for (int i = 0; i < slots.size(); i++) {
            projectExprs.add(this.plan.getOutput().get(i));
        }
        LogicalProject<LogicalPlan> project = new LogicalProject<>(projectExprs, this.plan);
        return from(project);
    }

    public LogicalPlanBuilder hashJoinUsing(LogicalPlan right, JoinType joinType, Pair<Integer, Integer> hashOnSlots) {
        ImmutableList<EqualTo> hashConjunts = ImmutableList.of(
                new EqualTo(this.plan.getOutput().get(hashOnSlots.first), right.getOutput().get(hashOnSlots.second)));

        LogicalJoin<LogicalPlan, LogicalPlan> join = new LogicalJoin<>(joinType, new ArrayList<>(hashConjunts),
                Optional.empty(), this.plan, right);
        return from(join);
    }
}
