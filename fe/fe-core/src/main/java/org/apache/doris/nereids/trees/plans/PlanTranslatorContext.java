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

package org.apache.doris.nereids.trees.plans;

import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.common.IdGenerator;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanFragmentId;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanNode;

import com.clearspring.analytics.util.Lists;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Context of physical plan.
 */
public class PlanTranslatorContext {
    private final List<PlanFragment> planFragmentList = Lists.newArrayList();

    private final DescriptorTable descTable = new DescriptorTable();

    /**
     * Map expressions of new optimizer to the stale expr.
     */
    private Map<Expression, Expr> expressionToExecExpr = new HashMap<>();

    private final List<ScanNode> scanNodeList = new ArrayList<>();

    private final IdGenerator<PlanFragmentId> fragmentIdGenerator = PlanFragmentId.createGenerator();

    private final IdGenerator<PlanNodeId> nodeIdGenerator = PlanNodeId.createGenerator();

    public List<PlanFragment> getPlanFragmentList() {
        return planFragmentList;
    }

    public TupleDescriptor generateTupleDesc() {
        return descTable.createTupleDescriptor();
    }

    public PlanNodeId nextNodeId() {
        return nodeIdGenerator.getNextId();
    }

    public SlotDescriptor addSlotDesc(TupleDescriptor t) {
        return descTable.addSlotDescriptor(t);
    }

    public SlotDescriptor addSlotDesc(TupleDescriptor t, int id) {
        return descTable.addSlotDescriptor(t, id);
    }

    public PlanFragmentId nextFragmentId() {
        return fragmentIdGenerator.getNextId();
    }

    public void addPlanFragment(PlanFragment planFragment) {
        this.planFragmentList.add(planFragment);
    }

    public void addSlotRefMapping(Expression expression, Expr expr) {
        expressionToExecExpr.put(expression, expr);
    }

    public Expr findExpr(Expression expression) {
        return expressionToExecExpr.get(expression);
    }

    public void addScanNode(ScanNode scanNode) {
        scanNodeList.add(scanNode);
    }

    public List<ScanNode> getScanNodeList() {
        return scanNodeList;
    }
}
