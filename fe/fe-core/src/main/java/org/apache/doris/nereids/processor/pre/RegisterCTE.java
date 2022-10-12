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

package org.apache.doris.nereids.processor.pre;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.memo.Memo;
import org.apache.doris.nereids.rules.analysis.CTEContext;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.WithClause;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTE;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Register CTE, includes checking columnAliases, checking CTE name, analyzing each CTE and store the
 * analyzed logicalPlan of CTE's query in CTEContext;
 * A LogicalProject node will be added to the root of the origin logicalPlan if there exist columnAliases.
 * Node LogicalCTE will be eliminated after registering.
 */
public class RegisterCTE extends PlanPreprocessor {

    private final CTEContext cteContext;

    public RegisterCTE(CTEContext cteContext) {
        this.cteContext = Objects.requireNonNull(cteContext, "cteContext cannot be null");
    }

    @Override
    public LogicalPlan visitLogicalCTE(
            LogicalCTE<? extends Plan> logicalCTE,
            StatementContext statementContext) {
        List<WithClause> withClauses = logicalCTE.getWithClauses();
        withClauses.stream().forEach(withClause -> {
            registerWithQuery(withClause, statementContext);
        });
        // eliminate LogicalCTE node
        return (LogicalPlan) logicalCTE.child();
    }

    private void registerWithQuery(WithClause withClause, StatementContext statementContext) {
        String name = withClause.getName();
        LogicalPlan originPlan = withClause.getQuery();

        if (cteContext.containsCTE(name)) {
            throw new AnalysisException("CTE name [" + name + "] cannot be used more than once.");
        }

        CascadesContext cascadesContext = new Memo(withClause.getQuery()).newCascadesContext(statementContext);
        cascadesContext.newAnalyzer(cteContext).analyze();

        if (withClause.getColumnAliases().isPresent()) {
            LogicalPlan analyzedPlan = (LogicalPlan) cascadesContext.getMemo().copyOut(false);
            originPlan = withColumnAliases(analyzedPlan, withClause);
        }

        cteContext.addCTE(name, originPlan);
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
        LogicalPlan originPlan = withClause.getQuery();
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
}
