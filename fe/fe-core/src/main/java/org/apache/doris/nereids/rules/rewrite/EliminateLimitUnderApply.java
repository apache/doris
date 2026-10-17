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

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * EliminateLimitUnderApply.
 * SQL examples (t1/t2 are two tables with columns c1/c2):
 * - select t1.c1 from t1 where exists (select 1 from t2 where t2.c1 = t1.c1 limit 3): correlated,
 *   the LIMIT cannot change the result of the apply (existence is checked per outer row), so it
 *   is removed;
 * - select t1.c1, (select t2.c1 from t2 limit 1) from t1: uncorrelated, the LIMIT decides whether
 *   and how many rows the subquery returns, so it is kept and this rule returns null.
 */
public class EliminateLimitUnderApply extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalApply(any(), logicalLimit()).then(apply -> {
            if (!apply.isCorrelated()) {
                // must keep the limit if it's an uncorrelated because the return number of rows is affected by limit
                return null;
            }
            List<Plan> children = new ImmutableList.Builder<Plan>()
                    .add(apply.left())
                    .add(apply.right().child())
                    .build();
            return apply.withChildren(children);
        }).toRule(RuleType.ELIMINATE_LIMIT_UNDER_APPLY);
    }
}
