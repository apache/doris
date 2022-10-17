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
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.memo.Memo;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.WithClause;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Context used for CTE analysis and register
 */
public class CTEContext {

    // store CTE name and non-analyzed LogicalPlan of with query, which means the CTE query plan will be inline
    // everywhere it is referenced and analyzed more than once.
    private Map<String, LogicalPlan> ctePlans;
    private Map<String, Boolean> cteIsAnalyzed;
    private Map<String, WithClause> withClauses;
    private Map<String, CTEScope> cteScopes;

    public CTEContext() {
        ctePlans = new HashMap<>();
        cteIsAnalyzed = new HashMap<>();
        withClauses = new HashMap<>();
        cteScopes = new HashMap<>();
    }

    public boolean containsCTE(String cteName) {
        if (cteScopes.containsKey(cteName)) {
            return cteScopes.get(cteName).containsCTE(cteName);
        }
        return false;
    }

    public void register(List<WithClause> withClauseList) {
        CTEScope cteScope = new CTEScope(Optional.empty(), ctePlans);

        for (WithClause withClause : withClauseList) {
            String cteName = withClause.getName();
            if (containsCTE(cteName)) {
                throw new AnalysisException("CTE name [" + cteName + "] cannot be used more than once.");
            }
            ctePlans.put(cteName, withClause.extractQueryPlan());
            cteIsAnalyzed.put(cteName, false);
            withClauses.put(cteName, withClause);
            cteScope = new CTEScope(Optional.of(cteScope), ctePlans);
            cteScopes.put(cteName, cteScope);
        }
    }

    public LogicalPlan findCTEPlan(String name, StatementContext statementContext) {
        WithClause withClause = withClauses.get(name);
        LogicalPlan originPlan = ctePlans.get(name);

        if (withClause.getColumnAliases().isPresent() && !cteIsAnalyzed.get(name)) {
            // first time to analyze this originPlan
            CascadesContext cascadesContext = new Memo(originPlan).newCascadesContext(statementContext);
            cascadesContext.newAnalyzer().analyze();
            LogicalPlan analyzedPlan = (LogicalPlan) cascadesContext.getMemo().copyOut(false);
            originPlan = withColumnAliases(analyzedPlan, withClause);

            ctePlans.put(name, originPlan);
            cteIsAnalyzed.put(name, true);
        }

        return originPlan;
    }

    private LogicalPlan withColumnAliases(LogicalPlan analyzedPlan, WithClause withClause) {
        List<Slot> outputSlots = analyzedPlan.getOutput();
        List<String> columnAliases = withClause.getColumnAliases().get();

        checkColumnAlias(withClause, outputSlots);

        List<NamedExpression> projects = IntStream.range(0, outputSlots.size())
            .mapToObj(i -> i >= columnAliases.size()
                ? new UnboundSlot(outputSlots.get(i).getName())
                : new Alias(new UnboundSlot(outputSlots.get(i).getName()), columnAliases.get(i)))
            .collect(Collectors.toList());
        LogicalPlan originPlan = withClause.extractQueryPlan();
        return new LogicalProject<>(projects, originPlan);
    }

    private void checkColumnAlias(WithClause withClause, List<Slot> outputSlots) {
        List<String> columnAlias = withClause.getColumnAliases().get();
        // if the size of columnAlias is smaller than outputSlots' size, we will replace the corresponding number
        // of front slots with columnAlias.
        if (columnAlias.size() > outputSlots.size()) {
            throw new AnalysisException("WITH-clause '" + withClause.getName() + "' returns " + columnAlias.size()
                + " columns, but " + outputSlots.size() + " labels were specified. The number of column labels must "
                + "be smaller or equal to the number of returned columns.");
        }

        Set<String> names = new HashSet<>();
        // column alias cannot be used more than once
        columnAlias.stream().forEach(alias -> {
            if (names.contains(alias.toLowerCase())) {
                throw new AnalysisException("Duplicated CTE column alias: '" + alias.toLowerCase()
                    + "' in CTE " + withClause.getName());
            }
            names.add(alias);
        });
    }

    private class CTEScope {
        Optional<CTEScope> parent;

        Map<String, LogicalPlan> ctePlans;

        public CTEScope(Optional<CTEScope> parent, Map<String, LogicalPlan> ctePlans) {
            this.parent = parent;
            this.ctePlans = ctePlans;
        }

        public boolean containsCTE(String cteName) {
            if (ctePlans.containsKey(cteName)) {
                return true;
            }

            if (parent.isPresent()) {
                return parent.get().containsCTE(cteName);
            }

            return false;
        }
    }

}
