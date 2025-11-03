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

package org.apache.doris.nereids.pattern;

import org.apache.doris.nereids.CTEContext;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.qe.ConnectContext;

/**
 * Define a context when match a pattern pass through a MatchedAction.
 */
public class MatchingContext<TYPE extends Plan> {
    public final TYPE root;
    public final Pattern<TYPE> pattern;
    public final CascadesContext cascadesContext;
    public final StatementContext statementContext;
    public final ConnectContext connectContext;
    public final CTEContext cteContext;

    /**
     * the MatchingContext is the param pass through the MatchedAction.
     *
     * @param root the matched tree node root
     * @param pattern the defined pattern
     * @param cascadesContext the planner context
     */
    public MatchingContext(TYPE root, Pattern<TYPE> pattern, CascadesContext cascadesContext) {
        this.root = root;
        this.pattern = pattern;
        this.cascadesContext = cascadesContext;
        this.statementContext = cascadesContext.getStatementContext();
        this.connectContext = cascadesContext.getConnectContext();
        this.cteContext = cascadesContext.getCteContext();
    }

    public boolean isRewriteRoot() {
        return cascadesContext.isRewriteRoot();
    }
}
