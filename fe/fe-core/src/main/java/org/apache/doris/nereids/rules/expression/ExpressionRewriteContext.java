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

package org.apache.doris.nereids.rules.expression;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.trees.plans.Plan;

import java.util.Objects;
import java.util.Optional;

/**
 * expression rewrite context.
 */
public class ExpressionRewriteContext {

    public final Optional<Plan> plan;
    public final Optional<ExpressionSource> source;
    public final CascadesContext cascadesContext;

    public ExpressionRewriteContext(CascadesContext cascadesContext) {
        this(Optional.empty(), Optional.empty(), cascadesContext);
    }

    public ExpressionRewriteContext(Plan plan, CascadesContext cascadesContext) {
        this(Optional.of(plan), Optional.empty(), cascadesContext);
    }

    public ExpressionRewriteContext(Plan plan, ExpressionSource source, CascadesContext cascadesContext) {
        this(Optional.of(plan), Optional.of(source), cascadesContext);
    }

    private ExpressionRewriteContext(Optional<Plan> plan, Optional<ExpressionSource> source,
            CascadesContext cascadesContext) {
        this.plan = Objects.requireNonNull(plan, "plan can not be null, or use Optional.empty()");
        this.source = Objects.requireNonNull(source, "source can not be null, or use Optional.empty()");
        this.cascadesContext = Objects.requireNonNull(cascadesContext, "cascadesContext can not be null");
    }

    /**
     * Expression detail source from.
     * Currently only used in Join, add more if needed.
     */
    public enum ExpressionSource {
        JOIN_HASH_CONDITION,
        JOIN_OTHER_CONDITION,
        JOIN_MARK_CONDITION,
    }
}
