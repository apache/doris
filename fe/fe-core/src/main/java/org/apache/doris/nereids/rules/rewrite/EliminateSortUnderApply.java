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
 * EliminateSortUnderApply.
 * SQL example: `select t1.c1 from t1 where t1.c1 in (select t2.c1 from t2 order by t2.c1)`;
 * the ORDER BY of the subquery does not carry any meaning for the apply (only the set of values
 * matters), so the LogicalSort is removed, also when it sits under a Project.
 * A sort which feeds a LIMIT (a TopN) is not the shape of this rule and is left alone.
 */
public class EliminateSortUnderApply implements RewriteRuleFactory {
    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
            RuleType.ELIMINATE_SORT_UNDER_APPLY.build(
                logicalApply(any(), logicalSort()).then(apply -> {
                    List<Plan> children = new ImmutableList.Builder<Plan>()
                            .add(apply.left())
                            .add(apply.right().child())
                            .build();
                    return apply.withChildren(children);
                })
            ),
            RuleType.ELIMINATE_SORT_UNDER_APPLY_PROJECT.build(
                logicalApply(any(), logicalProject(logicalSort())).then(apply -> {
                    List<Plan> children = new ImmutableList.Builder<Plan>()
                            .add(apply.left())
                            .add(apply.right().withChildren(apply.right().child().child()))
                            .build();
                    return apply.withChildren(children);
                })
            )
        );
    }
}
