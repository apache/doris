package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.Collections;
import java.util.List;

public class EliminateAliasNode implements AnalysisRuleFactory {
    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                RuleType.PROJECT_ELIMINATE_ALIAS_NODE.build(
                        logicalProject().then(project -> eliminateAliasNode(project, project.children()))
                ),
                RuleType.FILTER_ELIMINATE_ALIAS_NODE.build(
                        logicalFilter().then(filter -> eliminateAliasNode(filter, filter.children()))
                ),
                RuleType.JOIN_ELIMINATE_ALIAS_NODE.build(
                        logicalJoin().then(join -> joinEliminateAliasNode(join, join.children())
                        )
                )
        );
    }

    public LogicalPlan eliminateAliasNode(LogicalPlan plan, List<Plan> aliasNode) {
        Preconditions.checkArgument(aliasNode.size() == 1);
        List<Plan> children = aliasNode.get(0).children();
        Preconditions.checkArgument(children.size() == 1);
        return (LogicalPlan) plan.withChildren(children);
    }

    public LogicalPlan joinEliminateAliasNode(LogicalPlan plan, List<Plan> aliasNode) {
        Preconditions.checkArgument(aliasNode.size() == 2);
        Preconditions.checkArgument(aliasNode.stream().anyMatch(node -> node.children().size() == 1));
        return (LogicalPlan) plan.withChildren(Lists.newArrayList(
                aliasNode.get(0).child(0),
                aliasNode.get(1).child(0)
        ));
    }
}
