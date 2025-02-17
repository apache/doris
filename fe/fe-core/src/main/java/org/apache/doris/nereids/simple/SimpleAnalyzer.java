package org.apache.doris.nereids.simple;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.jobs.executor.AbstractBatchJobExecutor;
import org.apache.doris.nereids.jobs.rewrite.RewriteJob;
import org.apache.doris.nereids.rules.analysis.BindExpression;
import org.apache.doris.nereids.rules.analysis.BindRelation;
import org.apache.doris.nereids.rules.analysis.CheckAfterBind;
import org.apache.doris.nereids.rules.analysis.CheckPolicy;
import org.apache.doris.nereids.rules.analysis.HavingToFilter;
import org.apache.doris.nereids.rules.analysis.NormalizeGenerate;
import org.apache.doris.nereids.rules.analysis.OneRowRelationExtractAggregate;
import org.apache.doris.nereids.rules.analysis.ProjectToGlobalAggregate;
import org.apache.doris.nereids.rules.analysis.ProjectWithDistinctToAggregate;
import org.apache.doris.nereids.rules.analysis.ReplaceExpressionByChildOutput;
import org.apache.doris.nereids.rules.rewrite.MergeFilters;
import org.apache.doris.nereids.rules.rewrite.MergeProjects;

import com.google.common.collect.ImmutableList;

import java.util.List;

/** SimpleAnalyzer */
public class SimpleAnalyzer extends AbstractBatchJobExecutor {
    public static final List<RewriteJob> ANALYZE_JOBS = buildAnalyzeJobs();

    public SimpleAnalyzer(CascadesContext cascadesContext) {
        super(cascadesContext);
    }

    private static List<RewriteJob> buildAnalyzeJobs() {
        return ImmutableList.of(
                bottomUp(
                        new BindRelation(),
                        new CheckPolicy(),
                        new BindExpression(),
                        new ReplaceExpressionByChildOutput(),
                        new OneRowRelationExtractAggregate(),
                        new ProjectToGlobalAggregate(),
                        new ProjectWithDistinctToAggregate(),
                        new CheckAfterBind(),
                        new HavingToFilter(),
                        new NormalizeGenerate(),
                        new MergeProjects(),
                        new MergeFilters()
                )
        );
    }

    @Override
    public List<RewriteJob> getJobs() {
        return ANALYZE_JOBS;
    }
}
