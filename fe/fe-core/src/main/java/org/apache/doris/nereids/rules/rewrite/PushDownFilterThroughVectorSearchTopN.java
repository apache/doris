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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalTVFRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;
import org.apache.doris.tablefunction.VectorSearchTableValuedFunction;

/**
 * Move an outer vector_search WHERE predicate below its Doris merge TopN.
 *
 * <p>The TopN immediately above a vector_search TVF is added by {@code BindExpression} to merge
 * the candidates returned by all Lance fragment scans. The SQL WHERE predicate must therefore be
 * evaluated below this TopN so it can become a residual conjunct on the Doris Lance scan node:
 *
 * <pre>
 * Filter                         TopN
 *   TopN            ->            Filter
 *     vector_search                 vector_search
 * </pre>
 *
 * <p>This remains a postfilter relative to Lance nearest(): every fragment first returns its ANN
 * candidates, and Doris filters those candidates before the local/global TopN. It is deliberately
 * not converted into the Lance prefilter carried by the TVF's {@code filter} property.
 */
public class PushDownFilterThroughVectorSearchTopN extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalFilter(logicalTopN(logicalTVFRelation()))
                .then(filter -> {
                    LogicalTopN<LogicalTVFRelation> topN = filter.child();
                    if (!VectorSearchTableValuedFunction.NAME.equals(
                            topN.child().getFunction().getName())) {
                        return null;
                    }
                    LogicalFilter<Plan> scanFilter = new LogicalFilter<>(
                            filter.getConjuncts(), topN.child());
                    return topN.withChildren(scanFilter);
                }).toRule(RuleType.PUSH_DOWN_FILTER_THROUGH_VECTOR_SEARCH_TOPN);
    }
}
