package org.apache.doris.nereids.jobs.batch;

import org.apache.doris.nereids.PlannerContext;
import org.apache.doris.nereids.rules.expression.rewrite.NormalizeExpression;

import com.google.common.collect.ImmutableList;

public class NormalizeExpressionRulesJob extends BatchRulesJob {

    public NormalizeExpressionRulesJob(PlannerContext plannerContext) {
        super(plannerContext);
        rulesJob.addAll(ImmutableList.of(
                topDownBatch(ImmutableList.of(
                        new NormalizeExpression()
                ))
        ));
    }
}
