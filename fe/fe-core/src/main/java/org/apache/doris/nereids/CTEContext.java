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

package org.apache.doris.nereids;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.CTEId;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;

import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * Context used for CTE analysis and register
 */
public class CTEContext {

    private final CTEId cteId;
    private final String name;
    // this cache only use once
    private LogicalPlan analyzedPlan;

    private final Map<String, CTEContext> cteContextMap;

    /* build head CTEContext */
    public CTEContext() {
        this(CTEId.DEFAULT, null, null);
    }

    /**
     * CTEContext
     */
    public CTEContext(CTEId cteId, @Nullable LogicalSubQueryAlias<Plan> parsedPlan,
            @Nullable CTEContext previousCteContext) {
        if ((parsedPlan == null && previousCteContext != null) || (parsedPlan != null && previousCteContext == null)) {
            throw new AnalysisException("Only first CteContext can contains null cte plan or previousCteContext");
        }
        this.name = parsedPlan == null ? null : parsedPlan.getAlias();
        this.cteContextMap = previousCteContext == null
                ? ImmutableMap.of()
                : ImmutableMap.<String, CTEContext>builder()
                        .putAll(previousCteContext.cteContextMap)
                        .put(name, this)
                        // if inner name same with outer name, use inner name in this scope.
                        .buildKeepingLast();
        this.cteId = cteId;
    }

    public void setAnalyzedPlan(LogicalPlan analyzedPlan) {
        this.analyzedPlan = analyzedPlan;
    }

    /**
     * Get for CTE reuse.
     */
    public Optional<LogicalPlan> getAnalyzedCTEPlan(String cteName) {
        if (!findCTEContext(cteName).isPresent()) {
            return Optional.empty();
        }
        return Optional.of(findCTEContext(cteName).get().analyzedPlan);
    }

    /**
     * findCTEContext
     */
    public Optional<CTEContext> findCTEContext(String cteName) {
        if (cteName.equals(name)) {
            return Optional.of(this);
        }
        CTEContext cteContext = cteContextMap.get(cteName);
        return Optional.ofNullable(cteContext);
    }

    public CTEId getCteId() {
        return cteId;
    }
}
