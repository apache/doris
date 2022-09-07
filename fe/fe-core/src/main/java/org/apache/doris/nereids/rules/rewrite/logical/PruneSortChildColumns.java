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

import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * prune join children output.
 * pattern: project(sort())
 */
public class PruneSortChildColumns extends AbstractPushDownProjectRule<LogicalSort<GroupPlan>> {

    public PruneSortChildColumns() {
        setRuleType(RuleType.COLUMN_PRUNE_SORT_CHILD);
        setTarget(logicalSort());
    }

    @Override
    protected Plan pushDownProject(LogicalSort<GroupPlan> sortPlan, Set<Slot> references) {
        Set<Slot> sortSlots = sortPlan.getOutputSet();
        Set<Slot> required = Stream.concat(references.stream(), sortSlots.stream()).collect(Collectors.toSet());
        if (required.containsAll(sortPlan.child().getOutput())) {
            return sortPlan;
        }
        return sortPlan.withChildren(
            ImmutableList.of(new LogicalProject<>(Lists.newArrayList(required), sortPlan.child()))
        );
    }
}
