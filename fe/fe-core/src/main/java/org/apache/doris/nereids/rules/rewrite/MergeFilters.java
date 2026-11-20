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
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;

import com.google.common.collect.ImmutableSet;

/**
 * this rule aims to merge consecutive filters.
 * For example:
 * logical plan tree:
 *                filter(a>0)
 *                  |
 *                filter(b>0)
 *                  |
 *                scan
 * transformed to:
 *                filter(a>0 and b>0)
 *                   |
 *                 scan
 */
public class MergeFilters extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalFilter(logicalFilter()).then(MergeFilters::mergeFilters).toRule(RuleType.MERGE_FILTERS);
    }

    /** Merge all consecutive filters, preserving the original node when there is nothing to merge. */
    public static LogicalFilter<?> mergeFilters(LogicalFilter<?> filter) {
        if (!(filter.child() instanceof LogicalFilter)) {
            return filter;
        }
        ImmutableSet.Builder<Expression> conjuncts = ImmutableSet.<Expression>builder().addAll(filter.getConjuncts());
        Plan child = filter.child();
        while (child instanceof LogicalFilter) {
            LogicalFilter<?> childFilter = (LogicalFilter<?>) child;
            conjuncts.addAll(childFilter.getConjuncts());
            child = childFilter.child();
        }
        return new LogicalFilter<>(conjuncts.build(), child);
    }
}
