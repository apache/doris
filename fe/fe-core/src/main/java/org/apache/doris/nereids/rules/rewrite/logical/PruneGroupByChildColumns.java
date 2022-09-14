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
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * prune its child output according to groupBy.
 * pattern: groupBy()
 * table a: k1,k2,k3,v1
 * select k1,sum(v1) from a group by grouping sets ((k1))
 * plan tree:
 *    groupBy
 *     |
 *    scan(k1,k2,k3,v1)
 * transformed:
 *    groupBy
 *     |
 *   project(k1,v1)
 *     |
 *    scan(k1,k2,k3,v1)
 */
public class PruneGroupByChildColumns extends OneRewriteRuleFactory {

    @Override
    public Rule build() {
        return RuleType.COLUMN_PRUNE_GROUP_BY_CHILD.build(logicalGroupBy().then(groupBy -> {
            List<Slot> childOutput = groupBy.child().getOutput();
            Set<Slot> groupByInputSlots = groupBy.getInputSlots();
            List<NamedExpression> prunedOutputs = childOutput.stream().filter(groupByInputSlots::contains)
                    .collect(Collectors.toList());
            if (prunedOutputs.size() == groupBy.child().getOutput().size()) {
                return groupBy;
            }
            return groupBy.withChildren(ImmutableList.of(new LogicalProject<>(prunedOutputs, groupBy.child())));
        }));
    }
}
