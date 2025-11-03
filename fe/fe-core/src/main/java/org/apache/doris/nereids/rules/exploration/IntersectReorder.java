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

package org.apache.doris.nereids.rules.exploration;

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;

import com.google.common.collect.Lists;

import java.util.List;

/**
 * this rule put the child with least row count to the first child of intersect op
 */
public class IntersectReorder extends OneExplorationRuleFactory {

    public static final IntersectReorder INSTANCE = new IntersectReorder();

    @Override
    public Rule build() {
        return logicalIntersect()
                .when(logicalIntersect -> logicalIntersect.getQualifier().equals(Qualifier.DISTINCT))
                .then(logicalIntersect -> {
                    int minChildIdx = 0;
                    double minRowCount = Double.MAX_VALUE;
                    for (int i = 0; i < logicalIntersect.children().size(); i++) {
                        GroupPlan child = (GroupPlan) logicalIntersect.child(i);
                        if (child.getGroup().getStatistics().getRowCount() < minRowCount) {
                            minChildIdx = i;
                            minRowCount = child.getGroup().getStatistics().getRowCount();
                        }
                    }
                    if (minChildIdx == 0) {
                        return null;
                    }
                    List<Plan> children = Lists.newArrayList(logicalIntersect.children());
                    List<List<SlotReference>> regularOutput =
                            Lists.newArrayList(logicalIntersect.getRegularChildrenOutputs());
                    children.set(0, logicalIntersect.child(minChildIdx));
                    children.set(minChildIdx, logicalIntersect.child(0));
                    if (regularOutput.isEmpty()) {
                        return logicalIntersect.withChildren(children);
                    }
                    regularOutput.set(0, logicalIntersect.getRegularChildOutput(minChildIdx));
                    regularOutput.set(minChildIdx, logicalIntersect.getRegularChildOutput(0));
                    return logicalIntersect.withChildrenAndTheirOutputs(children, regularOutput);
                })
                .toRule(RuleType.REORDER_INTERSECT);
    }
}
