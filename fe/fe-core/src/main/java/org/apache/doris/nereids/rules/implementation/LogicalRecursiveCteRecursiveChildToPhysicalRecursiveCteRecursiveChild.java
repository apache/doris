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
import org.apache.doris.nereids.trees.plans.physical.PhysicalRecursiveCteRecursiveChild;

/**
 * Implementation rule that convert logical recursive cte's recursive child to physical recursive child.
 */
public class LogicalRecursiveCteRecursiveChildToPhysicalRecursiveCteRecursiveChild
        extends OneImplementationRuleFactory {
    @Override
    public Rule build() {
        return logicalRecursiveCteRecursiveChild().then(recursiveCte -> new PhysicalRecursiveCteRecursiveChild(
                recursiveCte.getCteName(),
                recursiveCte.getLogicalProperties(),
                recursiveCte.child()))
                .toRule(RuleType.LOGICAL_RECURSIVE_CTE_RECURSIVE_CHILD_TO_PHYSICAL_RECURSIVE_CTE_RECURSIVE_CHILD);
    }
}
