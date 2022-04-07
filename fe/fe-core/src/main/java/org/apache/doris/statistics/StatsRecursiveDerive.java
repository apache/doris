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

import java.util.HashMap;
import java.util.Map;


public class StatsRecursiveDerive {
    Map<PlanNode.NodeType, BaseStatsDerive> typeToDerive = new HashMap<>();

    /**
     * Recursively complete the derivation of statistics for this node and all its children
     * @param node
     * This parameter is an input and output parameter,
     * which will store the derivation result of statistical information in the corresponding node
     */
    public void statsRecursiveDerive(PlanNode node) {
        if (node.getStatsDeriveResult().isStatsDerived()) {
            return;
        }
        for (PlanNode childNode : node.getChildren()) {
            if (!childNode.getStatsDeriveResult().isStatsDerived()) {
                statsRecursiveDerive(childNode);
            }
        }

        node.setStatsDeriveResult(typeToDerive.get(node.getNodeType()).init(node).deriveStats());
    }

    public void creteNodeTypeToDeriveMap() {
        typeToDerive.put(PlanNode.NodeType.DEFAULT, new BaseStatsDerive() {
            @Override
            public StatsDeriveResult deriveStats() {
                return new StatsDeriveResult();
            }
        });
        typeToDerive.put(PlanNode.NodeType.OLAP_SCAN_NODE, new ScanStatsDerive());
    }
}
