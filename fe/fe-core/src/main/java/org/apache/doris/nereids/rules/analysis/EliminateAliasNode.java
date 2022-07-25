package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;

public class EliminateAliasNode implements AnalysisRuleFactory {
    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                RuleType.PROJECT_ELIMINATE_ALIAS_NODE.build(
                        logicalProject().then(project -> eliminateSubQueryAliasNode(project, project.children()))
                ),
                RuleType.FILTER_ELIMINATE_ALIAS_NODE.build(
                        logicalFilter().then(filter -> eliminateSubQueryAliasNode(filter, filter.children()))
                ),
                RuleType.JOIN_ELIMINATE_ALIAS_NODE.build(
                        logicalJoin().then(join -> eliminateSubQueryAliasNode(join, join.children()))
                ),
                RuleType.AGGREGATE_ELIMINATE_ALIAS_NODE.build(
                        logicalAggregate().then(agg -> eliminateSubQueryAliasNode(agg, agg.children()))
                )
        );
    }

    private LogicalPlan eliminateSubQueryAliasNode(LogicalPlan node, List<Plan> aliasNodes) {
        ArrayList<Plan> nodes = Lists.newArrayList();
        aliasNodes.forEach(child -> {
                    if (checkIsSubQueryAliasNode(child)) {
                        nodes.add(getPlan(child));
                    } else {
                        nodes.add(child);
                    }
                }
        );
        return (LogicalPlan) node.withChildren(nodes);
    }

    private boolean checkIsSubQueryAliasNode(Plan node) {
        return ((GroupPlan) node).getGroup().getLogicalExpression().getPlan().getType()
                == PlanType.LOGICAL_SUBQUERY_ALIAS;
    }

    private Plan getPlan(Plan node) {
        return ((GroupPlan) node).getGroup().getLogicalExpression().child(0).getLogicalExpression().getPlan();
    }
}
