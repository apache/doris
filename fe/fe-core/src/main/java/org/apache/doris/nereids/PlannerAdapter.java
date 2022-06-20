// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.nereids;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.common.UserException;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.plans.PhysicalPlanTranslator;
import org.apache.doris.nereids.trees.plans.PlanContext;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlanAdapter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.planner.Planner;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TQueryOptions;

import java.util.ArrayList;

/**
 * This class is used for the compatibility and code reuse in.
 * @see org.apache.doris.qe.StmtExecutor
 */
public class PlannerAdapter extends Planner {

    private final org.apache.doris.nereids.Planner planner;
    private final ConnectContext ctx;

    public PlannerAdapter(ConnectContext ctx) {
        this.planner = new org.apache.doris.nereids.Planner();
        this.ctx = ctx;
    }

    @Override
    public void plan(StatementBase queryStmt, Analyzer analyzer, TQueryOptions queryOptions) throws UserException {
        if (!(queryStmt instanceof LogicalPlanAdapter)) {
            throw new RuntimeException("Wrong type of queryStmt, expected: <? extends LogicalPlanAdapter>");
        }
        LogicalPlanAdapter logicalPlanAdapter = (LogicalPlanAdapter) queryStmt;
        PhysicalPlan physicalPlan = planner.plan(logicalPlanAdapter.getLogicalPlan(), new PhysicalProperties(), ctx);
        PhysicalPlanTranslator physicalPlanTranslator = new PhysicalPlanTranslator();
        PlanContext planContext = new PlanContext();
        physicalPlanTranslator.translatePlan(physicalPlan, planContext);
        fragments = new ArrayList<>(planContext.getPlanFragmentList());
    }
}
