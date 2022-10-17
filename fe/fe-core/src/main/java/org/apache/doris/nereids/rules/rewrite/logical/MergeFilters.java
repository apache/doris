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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.util.ExpressionUtils;

/**
 * this rule aims to merge consecutive filters.
 * For example:
 * logical plan tree:
 *               project
 *                  |
 *                filter(a>0)
 *                  |
 *                filter(b>0)
 *                  |
 *                scan
 * transformed to:
 *                project
 *                   |
 *                filter(a>0 and b>0)
 *                   |
 *                 scan
 */
public class MergeFilters extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalFilter(logicalFilter()).then(filter -> {
            LogicalFilter<? extends Plan> childFilter = filter.child();
            Expression predicates = filter.getPredicates();
            Expression childPredicates = childFilter.getPredicates();
            Expression mergedPredicates = ExpressionUtils.and(predicates, childPredicates);
            return new LogicalFilter<>(mergedPredicates, childFilter.child());
        }).toRule(RuleType.MERGE_FILTERS);
    }

}
