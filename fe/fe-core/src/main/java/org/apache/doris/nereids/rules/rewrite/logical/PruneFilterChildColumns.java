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
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * prune filter output.
 * pattern: project(filter())
 * table a: k1,k2,k3,v1
 * select k1 from a where k2 > 3
 * plan tree:
 *  project(k1)
 *    |
 *  filter(k2 > 3)
 *    |
 *  scan(k1,k2,k3,v1)
 * transformed:
 * 　project(k1)
 *    |
 *  filter(k2 > 3)
 *   |
 *  project(k1,k2)
 *   |
 * scan(k1,k2,k3,v1)
 */
public class PruneFilterChildColumns extends AbstractPushDownProjectRule<LogicalFilter<GroupPlan>> {

    public PruneFilterChildColumns() {
        setRuleType(RuleType.COLUMN_PRUNE_FILTER_CHILD);
        setTarget(logicalFilter());
    }

    @Override
    protected Plan pushDownProject(LogicalFilter<GroupPlan> filter, Set<Slot> references) {
        Set<Slot> filterInputSlots = filter.getInputSlots();
        Set<Slot> required = Stream.concat(references.stream(), filterInputSlots.stream()).collect(Collectors.toSet());
        if (required.containsAll(filter.child().getOutput())) {
            return filter;
        }
        return filter.withChildren(
            ImmutableList.of(new LogicalProject<>(Lists.newArrayList(required), filter.child()))
        );
    }
}
