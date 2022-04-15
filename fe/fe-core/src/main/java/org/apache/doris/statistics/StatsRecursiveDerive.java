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

package org.apache.doris.statistics;

import org.apache.doris.planner.PlanNode;


public class StatsRecursiveDerive {
    private StatsRecursiveDerive() {}

    public static StatsRecursiveDerive getStatsRecursiveDerive() {
        return Inner.INSTANCE;
    }

    private static class Inner {
        private static final StatsRecursiveDerive INSTANCE = new StatsRecursiveDerive();
    }

    /**
     * Recursively complete the derivation of statistics for this node and all its children
     * @param node
     * This parameter is an input and output parameter,
     * which will store the derivation result of statistical information in the corresponding node
     */
    public void statsRecursiveDerive(PlanNode node) {
        if (node.getStatsDeriveResult().get().isStatsDerived()) {
            return;
        }
        for (PlanNode childNode : node.getChildren()) {
            if (!childNode.getStatsDeriveResult().get().isStatsDerived()) {
                statsRecursiveDerive(childNode);
            }
        }
        DeriveFactory deriveFactory = new DeriveFactory();
        node.setStatsDeriveResult(deriveFactory.getStatsDerive(node.getNodeType()).init(node).deriveStats());
    }
}
