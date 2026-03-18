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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.catalog.OlapTable;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import com.google.common.collect.ImmutableList;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class IvmRewriteMtmvPlanTest {

    @Test
    public void testBasePlanRewriterGetsContextBasePlan(@Mocked OlapTable olapTable) {
        LogicalOlapScan scan = new LogicalOlapScan(StatementScopeIdGenerator.newRelationId(), olapTable,
                ImmutableList.of("ctl", "db"));
        CapturingBasePlanRewriter rewriter = new CapturingBasePlanRewriter();

        Plan rewritten = rewriter.rewriteRoot(scan, newJobContext(false, scan));

        Assertions.assertSame(scan, rewritten);
        Assertions.assertSame(scan, rewriter.basePlan);
    }

    @Test
    public void testIvmRewriteRulePlaceholderKeepsPlan(@Mocked OlapTable olapTable) {
        LogicalOlapScan scan = new LogicalOlapScan(StatementScopeIdGenerator.newRelationId(), olapTable,
                ImmutableList.of("ctl", "db"));

        Plan rewritten = new IvmRewriteMtmvPlan().rewriteRoot(scan, newJobContext(true, scan));

        Assertions.assertSame(scan, rewritten);
    }

    private JobContext newJobContext(boolean enableIvmRewrite, Plan rootPlan) {
        ConnectContext connectContext = new ConnectContext();
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setEnableIvmRewriteInNereids(enableIvmRewrite);
        connectContext.setSessionVariable(sessionVariable);
        StatementContext statementContext = new StatementContext(connectContext, null);
        CascadesContext cascadesContext = CascadesContext.initContext(statementContext, rootPlan, PhysicalProperties.ANY);
        return new JobContext(cascadesContext, PhysicalProperties.ANY);
    }

    private static class CapturingBasePlanRewriter implements CustomRewriter {
        private Plan basePlan;

        @Override
        public Plan rewriteRoot(Plan plan, JobContext jobContext) {
            this.basePlan = jobContext.getCascadesContext().getRewriteRootPlan();
            return plan;
        }
    }
}
