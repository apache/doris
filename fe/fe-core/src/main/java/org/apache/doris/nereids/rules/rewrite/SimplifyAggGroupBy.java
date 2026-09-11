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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.types.DataType;

import com.google.common.annotations.VisibleForTesting;

import java.util.List;

/**
 * Remove aggregate grouping expressions that are functionally dependent on an existing key.
 * <p>
 * GROUP BY ClientIP, ClientIP + 1, ClientIP + 2
 * -->
 * GROUP BY ClientIP
 *
 * <p>The determinant must be a bare slot already present in the grouping list. A dependent
 * expression may contain a proven lossless cast of that slot, but a cast cannot replace the bare
 * determinant because aggregate outputs cannot generally reconstruct the original slot from a
 * cast group key. The rule never synthesizes a slot from derived keys.</p>
 */
public class SimplifyAggGroupBy extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalAggregate()
                .when(agg -> agg.getGroupByExpressions().size() > 1)
                .then(agg -> {
                    List<Expression> simplified = simplifyGroupBy(agg.getGroupByExpressions());
                    if (simplified == null) {
                        return null;
                    }
                    return agg.withGroupByAndOutput(simplified, agg.getOutputExpressions());
                })
                .toRule(RuleType.SIMPLIFY_AGG_GROUP_BY);
    }

    @VisibleForTesting
    protected static List<Expression> simplifyGroupBy(List<Expression> groupByExpressions) {
        return AggregateGroupKeyUtils.simplifyGroupBy(groupByExpressions);
    }

    @VisibleForTesting
    protected static boolean isProvenInjectiveCast(DataType sourceType, DataType targetType) {
        return AggregateGroupKeyUtils.isProvenInjectiveCast(sourceType, targetType);
    }

}
