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

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.WithClause;

import java.util.List;

/**
 * Rule to register logical plans of CTEs
 */
public class AnalyzeCTE extends OneAnalysisRuleFactory {

    public CTEContext cteContext;

    public AnalyzeCTE() {
        this.cteContext = null;
    }

    public AnalyzeCTE(CTEContext cteContext) {
        this.cteContext = cteContext;
    }

    @Override
    public Rule build() {
        return logicalCTE().thenApply(ctx -> {
            List<WithClause> withClauses = ctx.root.getWithClauses();
            withClauses.stream().forEach(withClause -> cteContext.registerWithQuery(withClause, ctx.cascadesContext));
            // todo: recursive and columnNames
            return ctx.root.child(0);
        }).toRule(RuleType.REGISTER_CTE);
    }
}
