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

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.jobs.batch.AnalyzeRulesJob;
import org.apache.doris.nereids.jobs.batch.AnalyzeSubqueryRulesJob;
import org.apache.doris.nereids.jobs.batch.CheckAnalysisJob;
import org.apache.doris.nereids.jobs.batch.TypeCoercionJob;
import org.apache.doris.nereids.rules.analysis.Scope;

import java.util.Objects;
import java.util.Optional;

/**
 * Bind symbols according to metadata in the catalog, perform semantic analysis, etc.
 * TODO: revisit the interface after subquery analysis is supported.
 */
public class NereidsAnalyzer {
    private final CascadesContext cascadesContext;
    private final Optional<Scope> outerScope;

    public NereidsAnalyzer(CascadesContext cascadesContext) {
        this(cascadesContext, Optional.empty());
    }

    public NereidsAnalyzer(CascadesContext cascadesContext, Optional<Scope> outerScope) {
        this.cascadesContext = Objects.requireNonNull(cascadesContext, "cascadesContext cannot be null");
        this.outerScope = Objects.requireNonNull(outerScope, "outerScope cannot be null");
    }

    /**
     * nereids analyze sql.
     */
    public void analyze() {
        new AnalyzeRulesJob(cascadesContext, outerScope).execute();
        new AnalyzeSubqueryRulesJob(cascadesContext).execute();
        new TypeCoercionJob(cascadesContext).execute();
        // check whether analyze result is meaningful
        new CheckAnalysisJob(cascadesContext).execute();
    }
}
