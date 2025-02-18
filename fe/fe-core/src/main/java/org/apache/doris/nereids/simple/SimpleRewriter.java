package org.apache.doris.nereids.simple;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.jobs.executor.AbstractBatchJobExecutor;
import org.apache.doris.nereids.jobs.rewrite.RewriteJob;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.AdjustPreAggStatus;
import org.apache.doris.nereids.rules.rewrite.ColumnPruning;
import org.apache.doris.nereids.rules.rewrite.LimitSortToTopN;
import org.apache.doris.nereids.rules.rewrite.MergeFilters;
import org.apache.doris.nereids.rules.rewrite.MergeLimits;
import org.apache.doris.nereids.rules.rewrite.MergeProjects;
import org.apache.doris.nereids.rules.rewrite.PruneEmptyPartition;
import org.apache.doris.nereids.rules.rewrite.PruneOlapScanPartition;
import org.apache.doris.nereids.rules.rewrite.PruneOlapScanTablet;
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
            custom(RuleType.COLUMN_PRUNING, ColumnPruning::new),
            bottomUp(
                new LimitSortToTopN(),
                new PushDownFilterThroughProject(),
                new MergeProjects(),
                new MergeFilters(),
                new MergeLimits(),
                new PruneOlapScanPartition(),
                new PruneEmptyPartition(),
                new PruneOlapScanTablet()
            ),
            topDown(
                new SplitLimit()
            ),
            custom(RuleType.COLUMN_PRUNING, ColumnPruning::new),
            bottomUp(
                    new PushDownFilterThroughProject(),
                    new MergeProjects(),
                    new MergeFilters(),
                    new MergeLimits()
            ),
            topDown(
                    new AdjustPreAggStatus()
            )
        );
    }

    @Override
    public List<RewriteJob> getJobs() {
        return REWRITE_JOBS;
    }
}
