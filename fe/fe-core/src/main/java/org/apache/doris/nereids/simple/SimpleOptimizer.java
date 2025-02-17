package org.apache.doris.nereids.simple;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.jobs.executor.AbstractBatchJobExecutor;
import org.apache.doris.nereids.jobs.rewrite.RewriteJob;
import org.apache.doris.nereids.rules.implementation.LogicalEmptyRelationToPhysicalEmptyRelation;
import org.apache.doris.nereids.rules.implementation.LogicalFilterToPhysicalFilter;
import org.apache.doris.nereids.rules.implementation.LogicalOlapScanToPhysicalOlapScan;
import org.apache.doris.nereids.rules.implementation.LogicalProjectToPhysicalProject;
import org.apache.doris.nereids.rules.implementation.LogicalResultSinkToPhysicalResultSink;
import org.apache.doris.nereids.rules.implementation.LogicalTopNToPhysicalTopN;
import org.apache.doris.nereids.rules.rewrite.PruneOlapScanPartition;

import java.util.List;

public class SimpleOptimizer extends AbstractBatchJobExecutor {
    public static final List<RewriteJob> IMPL_JOBS = buildOptimizerJobs();

    public SimpleOptimizer(CascadesContext cascadesContext) {
        super(cascadesContext);
    }

    private static List<RewriteJob> buildOptimizerJobs() {
        return jobs(
                bottomUp(
                        new LogicalOlapScanToPhysicalOlapScan(),
                        new LogicalFilterToPhysicalFilter(),
                        new LogicalProjectToPhysicalProject(),
                        new LogicalTopNToPhysicalTopN(),
                        new LogicalResultSinkToPhysicalResultSink(),
                        new LogicalEmptyRelationToPhysicalEmptyRelation()
                )
        );
    }

    @Override
    public List<RewriteJob> getJobs() {
        return IMPL_JOBS;
    }
}
