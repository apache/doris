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

package org.apache.doris.nereids.analyzer;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.jobs.batch.AnalyzeRulesJob;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.memo.Memo;
import org.apache.doris.nereids.rules.analysis.Scope;
import org.apache.doris.nereids.trees.plans.Plan;

import java.util.Optional;

/**
 * Bind symbols according to metadata in the catalog, perform semantic analysis, etc.
 * TODO: revisit the interface after subquery analysis is supported.
 */
public class NereidsAnalyzer {
    private final CascadesContext cascadesContext;

    public NereidsAnalyzer(CascadesContext cascadesContext) {
        this.cascadesContext = cascadesContext;
    }

    public void analyze() {
        new AnalyzeRulesJob(cascadesContext, Optional.empty()).execute();
    }

    /**
     * copyIn the plan, and analyze plan with scope then copyOut the plan.
     */
    public Plan analyze(Plan plan, Optional<Scope> scope) {
        Memo memo = cascadesContext.getMemo();
        Pair<Boolean, GroupExpression> copyInResult = memo.copyIn(plan, null, false);

        if (!copyInResult.first) {
            throw new AnalysisException("Subquery can not copy into memo");
        }
        Group newGroup = copyInResult.second.getOwnerGroup();
        new AnalyzeRulesJob(cascadesContext, scope, Optional.of(newGroup)).execute();
        return memo.copyOut(newGroup);
    }
}
