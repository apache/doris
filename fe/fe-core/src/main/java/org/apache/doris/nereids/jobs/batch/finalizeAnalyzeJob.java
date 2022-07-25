package org.apache.doris.nereids.jobs.batch;

import org.apache.doris.nereids.PlannerContext;
import org.apache.doris.nereids.rules.analysis.EliminateAliasNode;

import com.google.common.collect.ImmutableList;

public class finalizeAnalyzeJob extends BatchRulesJob {

    public finalizeAnalyzeJob(PlannerContext plannerContext) {
        super(plannerContext);
        rulesJob.addAll(ImmutableList.of(
                bottomUpBatch(ImmutableList.of(
                                new EliminateAliasNode()
                        )
                )
        ));
    }
}
