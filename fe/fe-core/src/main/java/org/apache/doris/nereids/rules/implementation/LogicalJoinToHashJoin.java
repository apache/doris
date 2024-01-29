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

package org.apache.doris.nereids.rules.implementation;

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.util.JoinUtils;

/**
 * Implementation rule that convert logical join to physical hash join.
 */
public class LogicalJoinToHashJoin extends OneImplementationRuleFactory {
    @Override
    public Rule build() {
        return logicalJoin()
                .whenNot(JoinUtils::shouldNestedLoopJoin)
                .then(join -> new PhysicalHashJoin<>(
            join.getJoinType(),
            join.getHashJoinConjuncts(),
            join.getOtherJoinConjuncts(),
            join.getMarkJoinConjuncts(),
            join.getDistributeHint(),
            join.getMarkJoinSlotReference(),
            join.getLogicalProperties(),
            join.left(),
            join.right())
        ).toRule(RuleType.LOGICAL_JOIN_TO_HASH_JOIN_RULE);
    }
}
