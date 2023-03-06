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

import org.apache.doris.nereids.pattern.PatternDescriptor;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.RewriteRuleFactory;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSetOperation;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * eliminate set operation for empty relation
 */
public class EliminateSetOpForEmpty implements RewriteRuleFactory {

    @Override
    public List<Rule> buildRules() {
        PatternDescriptor<LogicalSetOperation> base = logicalSetOperation().when(this::checkEmpty);
        return ImmutableList.of(
                RuleType.ELIMINATE_INTERSECT.build(
                        base.then(intersect -> new LogicalEmptyRelation(intersect.child(0).getOutput()))
                ),
                RuleType.ELIMINATE_UNION.build(
                        base.then(union -> {
                            int emptyIndex = union.child(1) instanceof LogicalEmptyRelation ? 0 : 1;
                            return union.child(emptyIndex);
                        })
                ),
                RuleType.ELIMINATE_EXCEPT.build(
                        base.then(except -> except.child(0))
                )
        );
    }

    private boolean checkEmpty(LogicalPlan plan) {
        return plan.child(0) instanceof LogicalEmptyRelation
                || plan.child(1) instanceof LogicalEmptyRelation;
    }
}
