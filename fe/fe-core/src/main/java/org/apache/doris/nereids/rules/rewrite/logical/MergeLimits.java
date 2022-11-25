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

package org.apache.doris.nereids.rules.rewrite.logical;

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.OneRewriteRuleFactory;
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
        return logicalLimit(logicalLimit()).then(limit -> {
            long bottomLimit = limit.child().getLimit();
            long bottomOffset = limit.child().getOffset();
            long upLimit = limit.getLimit();
            long upOffset = limit.getOffset();
            // Child range [bottomOffset, bottomOffset + bottomLimit)
            // Parent range [bottomOffset + upOffset, bottomOffset + upOffset + upLimit)
            // Merge -> [bottomOffset + upOffset, min(bottomOffset + upOffset + upLimit, bottomOffset + bottomLimit) )
            // Merge LimitPlan -> [bottomOffset + upOffset, min(upLimit, bottomLimit - upOffset) )
            long newLimit;
            if (bottomLimit >= 0 && upLimit >= 0) {
                newLimit = Math.min(upLimit, minus(bottomLimit, upOffset));
            } else if (upLimit < 0 && bottomLimit < 0) {
                newLimit = -1;
            } else if (bottomLimit < 0) {
                newLimit = upLimit;
            } else {
                newLimit = minus(bottomLimit, upOffset);
            }

            return new LogicalLimit<>(
                    newLimit,
                    bottomOffset + upOffset,
                    limit.child().child()
            );
        }).toRule(RuleType.MERGE_LIMITS);
    }

    private long minus(long limit, long offset) {
        if (limit > offset) {
            return limit - offset;
        } else {
            return 0;
        }
    }
}
