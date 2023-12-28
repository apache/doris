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
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;

/**
 * This rule aims to merge consecutive limits.
 * <pre>
 * input plan:
 *   LIMIT1(limit=10, offset=0)
 *     |
 *   LIMIT2(limit=3, offset=5)
 *
 * output plan:
 *    LIMIT(limit=3, offset=5)
 *
 * merged limit = min(LIMIT1.limit, LIMIT2.limit)
 * merged offset = LIMIT2.offset
 * </pre>
 * Note that the top limit should not have valid offset info.
 */
public class MergeLimits extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalLimit(logicalLimit())
                .when(upperLimit -> upperLimit.getPhase().equals(upperLimit.child().getPhase())
                        || (upperLimit.getPhase().isLocal() && upperLimit.child().getPhase().isGlobal()))
                .then(upperLimit -> {
                    LogicalLimit<? extends Plan> bottomLimit = upperLimit.child();
                    return new LogicalLimit<>(
                            mergeLimit(upperLimit.getLimit(), upperLimit.getOffset(), bottomLimit.getLimit()),
                            mergeOffset(upperLimit.getOffset(), bottomLimit.getOffset()),
                            upperLimit.child().getPhase(), bottomLimit.child()
                    );
                }).toRule(RuleType.MERGE_LIMITS);
    }

    public static long mergeOffset(long upperOffset, long bottomOffset) {
        return upperOffset + bottomOffset;
    }

    public static long mergeLimit(long upperLimit, long upperOffset, long bottomLimit) {
        if (bottomLimit < 0) {
            return upperLimit;
        }
        return Math.min(upperLimit, Math.max(bottomLimit - upperOffset, 0));
    }
}
