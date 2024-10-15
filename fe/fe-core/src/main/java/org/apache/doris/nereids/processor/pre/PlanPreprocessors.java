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

import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

/**
 * PlanPreprocessors: before copy the plan into the memo, we use this rewriter to rewrite plan by visitor.
 */
public class PlanPreprocessors {
    private final StatementContext statementContext;

    public PlanPreprocessors(StatementContext statementContext) {
        this.statementContext = Objects.requireNonNull(statementContext, "statementContext can not be null");
    }

    public LogicalPlan process(LogicalPlan logicalPlan) {
        LogicalPlan resultPlan = logicalPlan;
        for (PlanPreprocessor processor : getProcessors()) {
            resultPlan = (LogicalPlan) resultPlan.accept(processor, statementContext);
        }
        return resultPlan;
    }

    /**
     * get preprocessors before doing optimize
     * @return preprocessors
     */
    public List<PlanPreprocessor> getProcessors() {
        // add processor if we need
        return ImmutableList.of(
                new TurnOffPageCacheForInsertIntoSelect(),
                new PullUpSubqueryAliasToCTE()
        );
    }
}
