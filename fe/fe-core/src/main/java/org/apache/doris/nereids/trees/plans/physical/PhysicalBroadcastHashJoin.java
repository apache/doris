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

package org.apache.doris.nereids.trees.plans.physical;

import org.apache.doris.nereids.trees.NodeType;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.JoinType;

import org.apache.commons.lang3.StringUtils;

import java.util.Optional;

/**
 * Physical node represents broadcast hash join.
 */
public class PhysicalBroadcastHashJoin extends PhysicalPlan {
    private final JoinType joinType;
    private final Expression onClause;

    /**
     * Constructor for PhysicalBroadcastHashJoin.
     *
     * @param joinType logical join type in Nereids
     * @param onClause on clause expression
     */
    public PhysicalBroadcastHashJoin(JoinType joinType, Expression onClause) {
        super(NodeType.PHYSICAL_BROADCAST_HASH_JOIN);
        this.joinType = joinType;
        this.onClause = onClause;
    }

    public JoinType getJoinType() {
        return joinType;
    }

    public Optional<Expression> getOnClause() {
        return Optional.ofNullable(onClause);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Broadcast Hash Join (").append(joinType);
        if (onClause != null) {
            sb.append(", ").append(onClause);
        }
        if (logicalProperties != null && !logicalProperties.getOutput().isEmpty()) {
            sb.append(", output: ").append(StringUtils.join(logicalProperties.getOutput(), ", "));
        }
        return sb.append(")").toString();
    }
}
