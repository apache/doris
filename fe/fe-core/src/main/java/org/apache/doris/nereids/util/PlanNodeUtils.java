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

package org.apache.doris.nereids.util;

import org.apache.doris.planner.PlanNode;
import org.apache.doris.planner.PlanNodeId;

/**
 * Util for PlanNode
 */
public class PlanNodeUtils {
    /**
     * find planNode recursively based on the planNodeId
     */
    public static PlanNode findPlanNodeFromPlanNodeId(PlanNode root, PlanNodeId id) {
        if (root == null || root.getId() == null || id == null) {
            return null;
        } else if (root.getId().equals(id)) {
            return root;
        } else {
            for (PlanNode child : root.getChildren()) {
                PlanNode retNode = findPlanNodeFromPlanNodeId(child, id);
                if (retNode != null) {
                    return retNode;
                }
            }
            return null;
        }
    }
}
