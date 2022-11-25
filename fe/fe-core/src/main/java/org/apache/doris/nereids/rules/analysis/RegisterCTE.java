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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.memo.Memo;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTE;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;

import com.google.common.collect.ImmutableList;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

/**
 * Register CTE, includes checking columnAliases, checking CTE name, analyzing each CTE and store the
 * analyzed logicalPlan of CTE's query in CTEContext;
 * A LogicalProject node will be added to the root of the initial logicalPlan if there exist columnAliases.
 * Node LogicalCTE will be eliminated after registering.
 */
public class RegisterCTE extends OneAnalysisRuleFactory {

    @Override
    public Rule build() {
        return logicalCTE().thenApply(ctx -> {
            LogicalCTE<GroupPlan> logicalCTE = ctx.root;
            register(logicalCTE.getAliasQueries(), ctx.cascadesContext);
            return logicalCTE.child();
        }).toRule(RuleType.REGISTER_CTE);
    }

    /**
     * register and store CTEs in CTEContext
     */
    private void register(List<LogicalSubQueryAlias<Plan>> aliasQueryList, CascadesContext cascadesContext) {
        CTEContext cteCtx = cascadesContext.getCteContext();
        for (LogicalSubQueryAlias<Plan> aliasQuery : aliasQueryList) {
            String cteName = aliasQuery.getAlias();
            if (cteCtx.containsCTE(cteName)) {
                throw new AnalysisException("CTE name [" + cteName + "] cannot be used more than once.");
            }

            // we should use a chain to ensure visible of cte
            CTEContext localCteContext = cteCtx;

            Function<Plan, LogicalPlan> analyzeCte = parsePlan -> {
                CascadesContext localCascadesContext = new Memo(parsePlan)
                        .newCascadesContext(cascadesContext.getStatementContext(), localCteContext);
                localCascadesContext.newAnalyzer().analyze();
                return (LogicalPlan) localCascadesContext.getMemo().copyOut(false);
            };

            LogicalPlan analyzedCteBody = analyzeCte.apply(aliasQuery.child());
            if (aliasQuery.getColumnAliases().isPresent()) {
                checkColumnAlias(aliasQuery, analyzedCteBody.getOutput());
            }

            cteCtx = new CTEContext(aliasQuery, localCteContext);

            // now we can simply wrap aliasQuery for the first usage of this cte
            cteCtx.setAnalyzedPlanCacheOnce(aliasQuery.withChildren(ImmutableList.of(analyzedCteBody)));
            cteCtx.setAnalyzePlanBuilder(analyzeCte);
        }
        cascadesContext.setCteContext(cteCtx);
    }

    /**
     * check columnAliases' size and name
     */
    private void checkColumnAlias(LogicalSubQueryAlias<Plan> aliasQuery, List<Slot> outputSlots) {
        List<String> columnAlias = aliasQuery.getColumnAliases().get();
        // if the size of columnAlias is smaller than outputSlots' size, we will replace the corresponding number
        // of front slots with columnAlias.
        if (columnAlias.size() > outputSlots.size()) {
            throw new AnalysisException("CTE [" + aliasQuery.getAlias() + "] returns " + columnAlias.size()
                + " columns, but " + outputSlots.size() + " labels were specified. The number of column labels must "
                + "be smaller or equal to the number of returned columns.");
        }

        Set<String> names = new HashSet<>();
        // column alias cannot be used more than once
        columnAlias.forEach(alias -> {
            if (names.contains(alias.toLowerCase())) {
                throw new AnalysisException("Duplicated CTE column alias: [" + alias.toLowerCase()
                    + "] in CTE [" + aliasQuery.getAlias() + "]");
            }
            names.add(alias);
        });
    }
}
