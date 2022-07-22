package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;

import com.google.common.collect.ImmutableList;

import java.util.List;

public class BindSubQueryAlias implements AnalysisRuleFactory {
    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                RuleType.BINDING_ALIAS_SLOT.build(
                        logicalSubQueryAlias().then(alias -> new LogicalSubQueryAlias<>(alias.getAlias(), alias.child())
                        )
                )
        );
    }
}
