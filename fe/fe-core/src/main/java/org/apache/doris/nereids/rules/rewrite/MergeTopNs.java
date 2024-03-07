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

import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;

import java.util.List;

/**
 * this rule aims to merge consecutive topN
 * <p>
 * topN - child topN
 * If child topN orderby list is prefix subList of topN =>
 * topN with limit = min(topN.limit, childTopN.limit), offset = topN.offset + childTopN.offset
 */
public class MergeTopNs extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalTopN(logicalTopN())
                .then(topN -> {
                    LogicalTopN<Plan> childTopN = topN.child();
                    List<OrderKey> orderKeys = topN.getOrderKeys();
                    List<OrderKey> childOrderKeys = childTopN.getOrderKeys();
                    if (!orderKeys.subList(0, childOrderKeys.size()).equals(childOrderKeys)) {
                        return null;
                    }
                    long offset = topN.getOffset();
                    long limit = topN.getLimit();
                    long childOffset = childTopN.getOffset();
                    long childLimit = childTopN.getLimit();
                    long newOffset = offset + childOffset;
                    // choose min limit
                    long newLimit = Math.min(limit, childLimit);
                    return topN.withLimitChild(newLimit, newOffset, childTopN.child());
                }).toRule(RuleType.MERGE_TOP_N);
    }
}
