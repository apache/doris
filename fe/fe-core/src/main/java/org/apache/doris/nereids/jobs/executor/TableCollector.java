// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.nereids.jobs.executor;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.jobs.rewrite.RewriteJob;
import org.apache.doris.nereids.rules.analysis.CollectRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalView;

import com.google.common.collect.ImmutableSet;

import java.util.List;

/**
 * Bind symbols according to metadata in the catalog, perform semantic analysis, etc.
 * TODO: revisit the interface after subquery analysis is supported.
 */
public class TableCollector extends AbstractBatchJobExecutor {

    public static final List<RewriteJob> COLLECT_JOBS = buildCollectTableJobs();

    /**
     * constructor of Analyzer. For view, we only do bind relation since other analyze step will do by outer Analyzer.
     *
     * @param cascadesContext current context for analyzer
     */
    public TableCollector(CascadesContext cascadesContext) {
        super(cascadesContext);

    }

    @Override
    public List<RewriteJob> getJobs() {
        return COLLECT_JOBS;
    }

    /**
     * nereids analyze sql.
     */
    public void collect() {
        execute();
    }

    private static List<RewriteJob> buildCollectTableJobs() {
        return notTraverseChildrenOf(
                ImmutableSet.of(LogicalView.class),
                TableCollector::buildCollectorJobs
        );
    }

    private static List<RewriteJob> buildCollectorJobs() {
        return jobs(
                topDown(new CollectRelation())
        );
    }
}
