package org.apache.doris.nereids.simple;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.jobs.executor.AbstractBatchJobExecutor;
import org.apache.doris.nereids.jobs.rewrite.RewriteJob;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.implementation.LogicalEmptyRelationToPhysicalEmptyRelation;
import org.apache.doris.nereids.rules.rewrite.ColumnPruning;
import org.apache.doris.nereids.rules.rewrite.EliminateUnnecessaryProject;
import org.apache.doris.nereids.rules.rewrite.LimitSortToTopN;
import org.apache.doris.nereids.rules.rewrite.MergeFilters;
import org.apache.doris.nereids.rules.rewrite.MergeProjects;
import org.apache.doris.nereids.rules.rewrite.PruneOlapScanPartition;
import org.apache.doris.nereids.rules.rewrite.PushDownFilterThroughProject;
import org.apache.doris.nereids.rules.rewrite.SplitLimit;

import java.util.List;

public class SimpleRewriter extends AbstractBatchJobExecutor {
    public static final List<RewriteJob> REWRITE_JOBS = buildRewriteJobs();

    public SimpleRewriter(CascadesContext cascadesContext) {
        super(cascadesContext);
    }

    private static List<RewriteJob> buildRewriteJobs() {
        return jobs(
            topDown(
                new LimitSortToTopN(),
                new PushDownFilterThroughProject(),
                new MergeProjects(),
                new MergeFilters(),
                new PruneOlapScanPartition()
            ),
            custom(RuleType.ELIMINATE_UNNECESSARY_PROJECT, EliminateUnnecessaryProject::new),
            topDown(
                new SplitLimit()
            ),
            custom(RuleType.COLUMN_PRUNING, ColumnPruning::new)
        );
    }

    @Override
    public List<RewriteJob> getJobs() {
        return REWRITE_JOBS;
    }
}
