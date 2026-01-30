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

package org.apache.doris.nereids;

import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.DistributionSpecReplicated;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.statistics.Statistics;

import java.util.List;

/**
 * Context for plan.
 * Abstraction for group expressions and stand-alone expressions/DAGs.
 * A ExpressionHandle is attached to either {@link Plan} or {@link GroupExpression}.
 * Inspired by GPORCA-CExpressionHandle.
 */
public class PlanContext {
    private final ConnectContext connectContext;
    private final GroupExpression groupExpression;
    private final boolean isBroadcastJoin;

    /**
     * Constructor for PlanContext.
     */
    public PlanContext(ConnectContext connectContext, GroupExpression groupExpression,
            List<PhysicalProperties> childrenProperties) {
        this.connectContext = connectContext;
        this.groupExpression = groupExpression;
        if (childrenProperties.size() >= 2
                && childrenProperties.get(1).getDistributionSpec() instanceof DistributionSpecReplicated) {
            isBroadcastJoin = true;
        } else {
            isBroadcastJoin = false;
        }
    }

    public SessionVariable getSessionVariable() {
        return connectContext.getSessionVariable();
    }

    public boolean isBroadcastJoin() {
        return isBroadcastJoin;
    }

    public int arity() {
        return groupExpression.arity();
    }

    public Statistics getStatisticsWithCheck() {
        return groupExpression.getOwnerGroup().getStatistics();
    }

    public boolean isStatsReliable() {
        return groupExpression.getOwnerGroup().isStatsReliable();
    }

    /**
     * Get child statistics.
     */
    public Statistics getChildStatistics(int index) {
        return groupExpression.childStatistics(index);
    }

    public StatementContext getStatementContext() {
        return connectContext.getStatementContext();
    }

    public ConnectContext getConnectContext() {
        return connectContext;
    }
}
