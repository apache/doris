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

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.CTEContext;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.CTEId;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTE;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEAnchor;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEProducer;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Register CTE, includes checking columnAliases, checking CTE name, analyzing each CTE and store the
 * analyzed logicalPlan of CTE's query in CTEContext;
 * A LogicalProject node will be added to the root of the initial logicalPlan if there exist columnAliases.
 * Node LogicalCTE will be eliminated after registering.
 */
public class AnalyzeCTE extends OneAnalysisRuleFactory {

    @Override
    public Rule build() {
        return logicalCTE().thenApply(ctx -> {
            LogicalCTE<Plan> logicalCTE = ctx.root;

            // step 0. check duplicate cte name
            Set<String> uniqueAlias = Sets.newHashSet();
            List<String> aliases = logicalCTE.getAliasQueries().stream()
                    .map(LogicalSubQueryAlias::getAlias)
                    .collect(Collectors.toList());
            for (String alias : aliases) {
                if (uniqueAlias.contains(alias)) {
                    throw new AnalysisException("CTE name [" + alias + "] cannot be used more than once.");
                }
                uniqueAlias.add(alias);
            }

            // step 1. analyzed all cte plan
            Pair<CTEContext, List<LogicalCTEProducer<Plan>>> result = analyzeCte(logicalCTE, ctx.cascadesContext);
            CascadesContext outerCascadesCtx = CascadesContext.newContextWithCteContext(
                    ctx.cascadesContext, logicalCTE.child(), result.first);
            outerCascadesCtx.newAnalyzer().analyze();
            Plan root = outerCascadesCtx.getRewritePlan();
            // should construct anchor from back to front, because the cte behind depends on the front
            for (int i = result.second.size() - 1; i >= 0; i--) {
                root = new LogicalCTEAnchor<>(result.second.get(i).getCteId(), result.second.get(i), root);
            }
            return root;
        }).toRule(RuleType.ANALYZE_CTE);
    }

    /**
     * register and store CTEs in CTEContext
     */
    private Pair<CTEContext, List<LogicalCTEProducer<Plan>>> analyzeCte(
            LogicalCTE<Plan> logicalCTE, CascadesContext cascadesContext) {
        CTEContext outerCteCtx = cascadesContext.getCteContext();
        List<LogicalSubQueryAlias<Plan>> aliasQueries = logicalCTE.getAliasQueries();
        List<LogicalCTEProducer<Plan>> cteProducerPlans = new ArrayList<>();
        for (LogicalSubQueryAlias<Plan> aliasQuery : aliasQueries) {
            // we should use a chain to ensure visible of cte
            LogicalPlan parsedCtePlan = (LogicalPlan) aliasQuery.child();
            CascadesContext innerCascadesCtx = CascadesContext.newContextWithCteContext(
                    cascadesContext, parsedCtePlan, outerCteCtx);
            innerCascadesCtx.newAnalyzer().analyze();
            LogicalPlan analyzedCtePlan = (LogicalPlan) innerCascadesCtx.getRewritePlan();
            checkColumnAlias(aliasQuery, analyzedCtePlan.getOutput());
            CTEId cteId = StatementScopeIdGenerator.newCTEId();
            LogicalSubQueryAlias<Plan> logicalSubQueryAlias =
                    aliasQuery.withChildren(ImmutableList.of(analyzedCtePlan));
            outerCteCtx = new CTEContext(cteId, logicalSubQueryAlias, outerCteCtx);
            outerCteCtx.setAnalyzedPlan(logicalSubQueryAlias);
            cteProducerPlans.add(new LogicalCTEProducer<>(cteId, logicalSubQueryAlias));
        }
        return Pair.of(outerCteCtx, cteProducerPlans);
    }

    /**
     * check columnAliases' size and name
     */
    private void checkColumnAlias(LogicalSubQueryAlias<Plan> aliasQuery, List<Slot> outputSlots) {
        if (aliasQuery.getColumnAliases().isPresent()) {
            List<String> columnAlias = aliasQuery.getColumnAliases().get();
            // if the size of columnAlias is smaller than outputSlots' size, we will replace the corresponding number
            // of front slots with columnAlias.
            if (columnAlias.size() > outputSlots.size()) {
                throw new AnalysisException("CTE [" + aliasQuery.getAlias() + "] returns "
                        + columnAlias.size() + " columns, but " + outputSlots.size() + " labels were specified."
                        + " The number of column labels must be smaller or equal to the number of returned columns.");
            }

            Set<String> names = new HashSet<>();
            // column alias cannot be used more than once
            columnAlias.forEach(alias -> {
                if (names.contains(alias.toLowerCase())) {
                    throw new AnalysisException("Duplicated CTE column alias:"
                            + " [" + alias.toLowerCase() + "] in CTE [" + aliasQuery.getAlias() + "]");
                }
                names.add(alias);
            });
        }
    }
}
