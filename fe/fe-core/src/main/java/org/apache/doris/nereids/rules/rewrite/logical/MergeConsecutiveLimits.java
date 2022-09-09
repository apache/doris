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
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;

import java.util.List;

/**
 * this rule aims to merge consecutive limits.
 *   LIMIT1(limit=10, offset=4)
 *     |
 *   LIMIT2(limit=3, offset=5)
 *
 *   transformed to
 *    LIMITl(limit=3, offset=5)
 *   where
 *   LIMIT.limit = min(LIMIT1.limit, LIMIT2.limit)
 *   LIMIT.offset = LIMIT2.offset
 *   LIMIT1.offset is ignored
 */
public class MergeConsecutiveLimits extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalLimit(logicalLimit()).then(upperLimit -> {
            LogicalLimit<? extends Plan> bottomLimit = upperLimit.child();
            List<Plan> children = bottomLimit.children();
            return new LogicalLimit<>(
                    Math.min(upperLimit.getLimit(), bottomLimit.getLimit()),
                    bottomLimit.getOffset(),
                    children.get(0)
            );
        }).toRule(RuleType.MERGE_CONSECUTIVE_LIMITS);
    }
}
