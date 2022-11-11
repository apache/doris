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
import org.apache.doris.nereids.trees.plans.physical.PhysicalTVFRelation;

/**
 * Implementation rule that convert LogicalTVFRelation to PhysicalTVFRelation.
 */
public class LogicalTVFRelationToPhysicalTVFRelation extends OneImplementationRuleFactory {
    @Override
    public Rule build() {
        return logicalTVFRelation()
                .then(relation -> new PhysicalTVFRelation(relation.getId(),
                        relation.getFunction(), relation.getLogicalProperties()))
                .toRule(RuleType.LOGICAL_TVF_RELATION_TO_PHYSICAL_TVF_RELATION);
    }
}
