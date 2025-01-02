package org.apache.doris.nereids.commonCTE;

import org.apache.doris.nereids.trees.expressions.CTEId;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.AbstractLogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;

import java.util.ArrayList;
import java.util.List;

public class ConsumerRewriter extends DefaultPlanRewriter<ConsumerRewriterContext> {
    public static final ConsumerRewriter INSTANCE = new ConsumerRewriter();
    @Override
    public Plan visit(Plan plan, ConsumerRewriterContext context) {
        if (context.isTarget(plan)) {
            context.producer.getOutput();
            plan.getOutput();
            ImmutableMap.Builder<Slot, Slot> cToPBuilder = ImmutableMap.builder();
            ImmutableMultimap.Builder<Slot, Slot> pToCBuilder = ImmutableMultimap.builder();
            for (int i = 0; i< context.producer.getOutput().size(); i++) {
                cToPBuilder.put(plan.getOutput().get(i), context.producer.getOutput().get(i));
                pToCBuilder.put(context.producer.getOutput().get(i), plan.getOutput().get(i));
            }
            return new LogicalCTEConsumer(StatementScopeIdGenerator.newRelationId(), context.cteId,
                    "comm_cte_" + context.cteId.asInt(), cToPBuilder.build(), pToCBuilder.build());
        }
        return visitChildren(this, plan, context);
    }
}
