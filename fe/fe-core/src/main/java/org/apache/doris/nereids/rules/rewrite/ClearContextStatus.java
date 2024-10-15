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

import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;

/**
 * pull up LogicalCteAnchor to the top of plan to avoid CteAnchor break other rewrite rules pattern
 * The front producer may depend on the back producer in {@code List<LogicalCTEProducer<Plan>>}
 * After this rule, we normalize all CteAnchor in plan, all CteAnchor under CteProducer should pull out
 * and put all of them to the top of plan depends on dependency tree of them.
 */
public class ClearContextStatus implements CustomRewriter {

    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        jobContext.getCascadesContext().getStatementContext().getRewrittenCteConsumer().clear();
        jobContext.getCascadesContext().getStatementContext().getRewrittenCteProducer().clear();
        jobContext.getCascadesContext().getStatementContext().getCteIdToOutputIds().clear();
        jobContext.getCascadesContext().getStatementContext().getConsumerIdToFilters().clear();
        return plan;
    }
}
