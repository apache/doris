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
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalExcept;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Merge left deep except
 * <pre>
 *     Except
 *    /     \
 *  Except  scan3
 *   /    \
 * scan1 scan2
 * -->
 *      Except
 *     /  |  \
 * scan1 scan2 scan3
 * </pre>
 */
public class MergeSetOperationsExcept extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalExcept(logicalExcept(), any())
                .when(e -> e.getQualifier() == ((LogicalExcept) e.child(0)).getQualifier())
                .then(parentExcept -> {
                    LogicalExcept childExcept = (LogicalExcept) parentExcept.child(0);

                    ImmutableList.Builder<Plan> newChildren = ImmutableList.builder();
                    ImmutableList.Builder<List<SlotReference>> newChildrenOutputs = ImmutableList.builder();

                    newChildren.addAll(childExcept.children());
                    newChildren.addAll(parentExcept.children().subList(1, parentExcept.children().size()));
                    newChildrenOutputs.addAll(childExcept.getRegularChildrenOutputs());
                    newChildrenOutputs.addAll(
                            parentExcept.getRegularChildrenOutputs().subList(1, parentExcept.children().size()));

                    return parentExcept.withChildrenAndTheirOutputs(newChildren.build(), newChildrenOutputs.build());
                }).toRule(RuleType.MERGE_SET_OPERATION_EXCEPT);
    }
}
