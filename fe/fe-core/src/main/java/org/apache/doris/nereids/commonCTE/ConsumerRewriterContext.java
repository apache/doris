package org.apache.doris.nereids.commonCTE;

import org.apache.doris.nereids.trees.expressions.CTEId;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.AbstractLogicalPlan;

import java.util.List;

public class ConsumerRewriterContext {
    private final List<Plan> targets;
    public Plan producer;
    public final CTEId cteId;

    public ConsumerRewriterContext(List<Plan> targets, Plan producer, CTEId cteId) {
        this.targets = targets;
        this.producer = producer;
        this.cteId = cteId;
    }
    public boolean isTarget(Plan plan) {
        return targets.contains(plan);
    }
}
