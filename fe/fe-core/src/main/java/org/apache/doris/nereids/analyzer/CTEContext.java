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

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;

import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import javax.annotation.Nullable;

/**
 * Context used for CTE analysis and register
 */
public class CTEContext {
    private Map<String, CTEContext> cteContextMap;

    private String name;
    private LogicalSubQueryAlias<Plan> parsedPlan;
    // this cache only use once
    private LogicalPlan analyzedPlan;
    private Function<Plan, LogicalPlan> analyzePlanBuilder;

    private int uniqueId;

    /* build head CTEContext */
    public CTEContext() {
        this(null, null, Integer.MIN_VALUE);
    }

    /** CTEContext */
    public CTEContext(@Nullable LogicalSubQueryAlias<Plan> parsedPlan, @Nullable CTEContext previousCteContext,
            int uniqueId) {
        if ((parsedPlan == null && previousCteContext != null) || (parsedPlan != null && previousCteContext == null)) {
            throw new AnalysisException("Only first CteContext can contains null cte plan or previousCteContext");
        }
        this.parsedPlan = parsedPlan;
        this.name = parsedPlan == null ? null : parsedPlan.getAlias();
        this.cteContextMap = previousCteContext == null
                ? ImmutableMap.of()
                : ImmutableMap.<String, CTEContext>builder()
                        .putAll(previousCteContext.cteContextMap)
                        .put(name, this)
                        .build();
        this.uniqueId = uniqueId;
    }

    public void setAnalyzedPlan(LogicalPlan analyzedPlan) {
        this.analyzedPlan = analyzedPlan;
    }

    public void setAnalyzePlanBuilder(Function<Plan, LogicalPlan> analyzePlanBuilder) {
        this.analyzePlanBuilder = analyzePlanBuilder;
    }

    /**
     * check if cteName can be found in current order
     */
    public boolean containsCTE(String cteName) {
        return findCTEContext(cteName).isPresent();
    }

    public Optional<LogicalSubQueryAlias<Plan>> getParsedCtePlan(String cteName) {
        return findCTEContext(cteName).map(cte -> cte.parsedPlan);
    }

    /** getAnalyzedCTE */
    public Optional<LogicalPlan> getAnalyzedCTE(String cteName) {
        if (!findCTEContext(cteName).isPresent()) {
            return Optional.empty();
        }
        return Optional.of(findCTEContext(cteName).get().analyzedPlan);
    }

    public Optional<LogicalPlan> getForInline(String cteName) {
        return findCTEContext(cteName).map(CTEContext::doAnalyzeCTE);
    }

    /** findCTEContext */
    public Optional<CTEContext> findCTEContext(String cteName) {
        CTEContext cteContext = cteContextMap.get(cteName);
        return Optional.ofNullable(cteContext);
    }

    private LogicalPlan doAnalyzeCTE() {
        return analyzePlanBuilder.apply(parsedPlan);
    }

    public int getUniqueId() {
        return uniqueId;
    }
}
