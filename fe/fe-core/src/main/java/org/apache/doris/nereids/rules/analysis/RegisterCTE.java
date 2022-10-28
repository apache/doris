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
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.UnboundAlias;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.memo.Memo;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTE;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
            register(logicalCTE.getAliasQueries(), ctx.statementContext);
            return (LogicalPlan) logicalCTE.child();
        }).toRule(RuleType.REGISTER_CTE);
    }

    /**
     * register and store CTEs in CTEContext
     */
    private void register(List<LogicalSubQueryAlias> aliasQueryList, StatementContext statementContext) {
        CTEContext cteContext = statementContext.getCteContext();

        for (LogicalSubQueryAlias<LogicalPlan> aliasQuery : aliasQueryList) {
            String cteName = aliasQuery.getAlias();
            if (cteContext.containsCTE(cteName)) {
                throw new AnalysisException("CTE name [" + cteName + "] cannot be used more than once.");
            }

            // inline CTE's initialPlan if it is referenced by another CTE
            LogicalPlan plan = aliasQuery.child();
            plan = (LogicalPlan) new CTEVisitor().inlineCTE(cteContext, plan);
            cteContext.putInitialPlan(cteName, plan);

            // analyze CTE's initialPlan
            CascadesContext cascadesContext = new Memo(plan).newCascadesContext(statementContext);
            cascadesContext.newAnalyzer().analyze();
            LogicalPlan analyzedPlan = (LogicalPlan) cascadesContext.getMemo().copyOut(false);

            if (aliasQuery.getColumnAliases().isPresent()) {
                analyzedPlan = withColumnAliases(analyzedPlan, aliasQuery, cteContext);
            }

            cteContext.putAnalyzedPlan(cteName, analyzedPlan);
        }
    }

    /**
     * deal with columnAliases of CTE
     */
    private LogicalPlan withColumnAliases(LogicalPlan analyzedPlan,
                                          LogicalSubQueryAlias<LogicalPlan> aliasQuery, CTEContext cteContext) {
        List<Slot> outputSlots = analyzedPlan.getOutput();
        List<String> columnAliases = aliasQuery.getColumnAliases().get();

        checkColumnAlias(aliasQuery, outputSlots);

        // if this CTE has columnAlias, we should add an extra LogicalProject to both its initialPlan and analyzedPlan,
        // which is used to store columnAlias

        // projects for initialPlan
        List<NamedExpression> unboundProjects = IntStream.range(0, outputSlots.size())
                .mapToObj(i -> i >= columnAliases.size()
                    ? new UnboundSlot(outputSlots.get(i).getName())
                    : new UnboundAlias(new UnboundSlot(outputSlots.get(i).getName()), columnAliases.get(i)))
                .collect(Collectors.toList());

        String name = aliasQuery.getAlias();
        LogicalPlan initialPlan = cteContext.getInitialCTEPlan(name);
        cteContext.putInitialPlan(name, new LogicalProject<>(unboundProjects, initialPlan));

        // projects for analyzedPlan
        List<NamedExpression> boundedProjects = IntStream.range(0, outputSlots.size())
                .mapToObj(i -> i >= columnAliases.size()
                    ? outputSlots.get(i)
                    : new Alias(outputSlots.get(i), columnAliases.get(i)))
                .collect(Collectors.toList());
        return new LogicalProject<>(boundedProjects, analyzedPlan);
    }

    /**
     * check columnAliases' size and name
     */
    private void checkColumnAlias(LogicalSubQueryAlias<LogicalPlan> aliasQuery, List<Slot> outputSlots) {
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
        columnAlias.stream().forEach(alias -> {
            if (names.contains(alias.toLowerCase())) {
                throw new AnalysisException("Duplicated CTE column alias: [" + alias.toLowerCase()
                    + "] in CTE [" + aliasQuery.getAlias() + "]");
            }
            names.add(alias);
        });
    }

    private class CTEVisitor extends DefaultPlanRewriter<CTEContext> {
        @Override
        public LogicalPlan visitUnboundRelation(UnboundRelation unboundRelation, CTEContext cteContext) {
            // confirm if it is a CTE
            if (unboundRelation.getNameParts().size() != 1) {
                return unboundRelation;
            }
            String name = unboundRelation.getTableName();
            if (cteContext.containsCTE(name)) {
                return new LogicalSubQueryAlias<>(name, cteContext.getInitialCTEPlan(name));
            }
            return unboundRelation;
        }

        public Plan inlineCTE(CTEContext cteContext, LogicalPlan ctePlan) {
            return ctePlan.accept(this, cteContext);
        }
    }

}
