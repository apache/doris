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

package org.apache.doris.planner;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.planner.LocalExchangeNode.LocalExchangeType;
import org.apache.doris.planner.LocalExchangeNode.LocalExchangeTypeRequire;

import java.util.List;

/** AddLocalExchange */
public class AddLocalExchange {
    public void addLocalExchange(List<PlanFragment> fragments, PlanTranslatorContext context) {
        for (PlanFragment fragment : fragments) {
            DataSink sink = fragment.getSink();
            LocalExchangeTypeRequire require = sink == null
                    ? LocalExchangeTypeRequire.noRequire() : sink.getLocalExchangeTypeRequire();
            PlanNode root = fragment.getPlanRoot();
            Pair<PlanNode, LocalExchangeType> output = root
                    .enforceAndDeriveLocalExchange(context, null, require);
            if (output.first != root) {
                fragment.setPlanRoot(output.first);
            }
        }
    }

    public static boolean isColocated(PlanNode plan) {
        if (plan instanceof AggregationNode) {
            return ((AggregationNode) plan).isColocate() && isColocated(plan.getChild(0));
        } else if (plan instanceof OlapScanNode) {
            return true;
        } else if (plan instanceof SelectNode) {
            return isColocated(plan.getChild(0));
        } else if (plan instanceof HashJoinNode) {
            return ((HashJoinNode) plan).isColocate()
                    && (isColocated(plan.getChild(0)) || isColocated(plan.getChild(1)));
        } else if (plan instanceof SetOperationNode) {
            if (!((SetOperationNode) plan).isColocate()) {
                return false;
            }
            for (PlanNode child : plan.getChildren()) {
                if (isColocated(child)) {
                    return true;
                }
            }
            return false;
        } else {
            return false;
        }
    }
}
