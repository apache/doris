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
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.memo.Memo;
import org.apache.doris.nereids.trees.expressions.WithClause;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Context used for CTE analysis and register
 */
public class CTEContext {

    private Map<String, LogicalPlan> withQueries;

    public CTEContext() {
        this.withQueries = new HashMap<>();
    }

    public Map<String, LogicalPlan> getWithQueries() {
        return withQueries;
    }

    public Optional<LogicalPlan> findCTE(String name) {
        return Optional.ofNullable(withQueries.get(name));
    }

    /**
     * register with queries in CTEContext
     * @param withClause includes with query
     * @param parentContext parent CascadesContext
     */
    public void registerWithQuery(WithClause withClause, CascadesContext parentContext) {
        String name = withClause.getName();
        if (withQueries.containsKey(name)) {
            throw new AnalysisException("Name " + name + " of CTE cannot be used more than once.");
        }

        CascadesContext cascadesContext = new Memo(withClause.getQuery())
                .newCascadesContext(parentContext.getStatementContext());
        cascadesContext.newAnalyzer(this).analyze();
        // withQueries.put(name, (LogicalPlan) cascadesContext.getMemo().copyOut(false));
        withQueries.put(name, withClause.getQuery());
    }

    /**
     * register with queries in CTEContext
     * @param withClause includes with query
     * @param statementContext global statementContext
     */
    public void registerWithQuery(WithClause withClause, StatementContext statementContext) {
        String name = withClause.getName();
        if (withQueries.containsKey(name)) {
            throw new AnalysisException("Name " + name + " of CTE cannot be used more than once.");
        }

        CascadesContext cascadesContext = new Memo(withClause.getQuery())
                .newCascadesContext(statementContext);
        cascadesContext.newAnalyzer(this).analyze();
        // withQueries.put(name, (LogicalPlan) cascadesContext.getMemo().copyOut(false));
        withQueries.put(name, withClause.getQuery());
    }
}
