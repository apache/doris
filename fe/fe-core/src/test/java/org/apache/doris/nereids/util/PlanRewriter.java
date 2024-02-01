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

package org.apache.doris.nereids.util;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.jobs.executor.Rewriter;
import org.apache.doris.nereids.memo.Memo;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.RuleFactory;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;

import com.google.common.collect.ImmutableList;

/**
 * Utility to copy plan into {@link Memo} and apply rewrite rules.
 */
public class PlanRewriter {
    public static Plan bottomUpRewrite(Plan plan, ConnectContext connectContext, RuleFactory... rules) {
        CascadesContext cascadesContext = CascadesContext.initContext(
                new StatementContext(connectContext, new OriginStatement("", 0)),
                plan,
                PhysicalProperties.GATHER);
        Rewriter.getWholeTreeRewriterWithCustomJobs(cascadesContext,
                ImmutableList.of(Rewriter.bottomUp(rules))).execute();
        return cascadesContext.getRewritePlan();
    }
}
