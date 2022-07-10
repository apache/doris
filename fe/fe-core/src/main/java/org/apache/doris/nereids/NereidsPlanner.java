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

import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.glue.translator.PhysicalPlanTranslator;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.nereids.jobs.AnalyzeRulesJob;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.jobs.OptimizeRulesJob;
import org.apache.doris.nereids.jobs.PredicatePushDownRulesJob;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.memo.Memo;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.Planner;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Planner to do query plan in Nereids.
 */
public class NereidsPlanner extends Planner {

    private PlannerContext plannerContext;
    private final ConnectContext ctx;
    private List<ScanNode> scanNodeList = null;
    private DescriptorTable descTable;

    public NereidsPlanner(ConnectContext ctx) {
        this.ctx = ctx;
    }

    @Override
    public void plan(StatementBase queryStmt,
            org.apache.doris.thrift.TQueryOptions queryOptions) throws UserException {
        if (!(queryStmt instanceof LogicalPlanAdapter)) {
            throw new RuntimeException("Wrong type of queryStmt, expected: <? extends LogicalPlanAdapter>");
        }

        LogicalPlanAdapter logicalPlanAdapter = (LogicalPlanAdapter) queryStmt;
        PhysicalPlan physicalPlan = plan(logicalPlanAdapter.getLogicalPlan(), new PhysicalProperties(), ctx);

        PhysicalPlanTranslator physicalPlanTranslator = new PhysicalPlanTranslator();
        PlanTranslatorContext planTranslatorContext = new PlanTranslatorContext();
        physicalPlanTranslator.translatePlan(physicalPlan, planTranslatorContext);

        scanNodeList = planTranslatorContext.getScanNodeList();
        descTable = planTranslatorContext.getDescTable();
        fragments = new ArrayList<>(planTranslatorContext.getPlanFragmentList());
        for (PlanFragment fragment : fragments) {
            fragment.finalize(queryStmt);
        }
        Collections.reverse(fragments);
        PlanFragment root = fragments.get(0);

        // compute output exprs
        Map<Integer, Expr> outputCandidates = Maps.newHashMap();
        List<Expr> outputExprs = Lists.newArrayList();
        for (TupleId tupleId : root.getPlanRoot().getTupleIds()) {
            TupleDescriptor tupleDescriptor = descTable.getTupleDesc(tupleId);
            for (SlotDescriptor slotDescriptor : tupleDescriptor.getSlots()) {
                SlotRef slotRef = new SlotRef(slotDescriptor);
                outputCandidates.put(slotDescriptor.getId().asInt(), slotRef);
            }
        }
        physicalPlan.getOutput().stream()
                .forEach(i -> outputExprs.add(planTranslatorContext.findExpr(i)));
        root.setOutputExprs(outputExprs);
        root.getPlanRoot().convertToVectoriezd();

        logicalPlanAdapter.setResultExprs(outputExprs);
        ArrayList<String> columnLabelList = physicalPlan.getOutput().stream()
                .map(NamedExpression::getName).collect(Collectors.toCollection(ArrayList::new));
        logicalPlanAdapter.setColLabels(columnLabelList);
    }

    /**
     * Do analyze and optimize for query plan.
     *
     * @param plan wait for plan
     * @param outputProperties physical properties constraints
     * @param connectContext connect context for this query
     * @return physical plan generated by this planner
     * @throws AnalysisException throw exception if failed in ant stage
     */
    // TODO: refactor, just demo code here
    public PhysicalPlan plan(LogicalPlan plan, PhysicalProperties outputProperties, ConnectContext connectContext)
            throws AnalysisException {
        Memo memo = new Memo();
        memo.initialize(plan);

        plannerContext = new PlannerContext(memo, connectContext);
        JobContext jobContext = new JobContext(plannerContext, outputProperties, Double.MAX_VALUE);
        plannerContext.setCurrentJobContext(jobContext);

        // Get plan directly. Just for SSB.
        return doPlan();
    }

    /**
     * The actual execution of the plan, including the generation and execution of the job.
     * @return PhysicalPlan.
     */
    private PhysicalPlan doPlan() {
        AnalyzeRulesJob analyzeRulesJob = new AnalyzeRulesJob(plannerContext);
        analyzeRulesJob.execute();

        PredicatePushDownRulesJob predicatePushDownRulesJob = new PredicatePushDownRulesJob(plannerContext);
        predicatePushDownRulesJob.execute();

        OptimizeRulesJob optimizeRulesJob = new OptimizeRulesJob(plannerContext);
        optimizeRulesJob.execute();

        return getRoot().extractPlan();
    }

    @Override
    public List<ScanNode> getScanNodes() {
        return scanNodeList;
    }

    public Group getRoot() {
        return plannerContext.getMemo().getRoot();
    }

    private PhysicalPlan chooseBestPlan(Group rootGroup, PhysicalProperties physicalProperties)
            throws AnalysisException {
        GroupExpression groupExpression = rootGroup.getLowestCostPlan(physicalProperties).orElseThrow(
                () -> new AnalysisException("lowestCostPlans with physicalProperties doesn't exist")).second;
        List<PhysicalProperties> inputPropertiesList = groupExpression.getInputPropertiesList(physicalProperties);

        List<Plan> planChildren = Lists.newArrayList();
        for (int i = 0; i < groupExpression.arity(); i++) {
            planChildren.add(chooseBestPlan(groupExpression.child(i), inputPropertiesList.get(i)));
        }

        Plan plan = ((PhysicalPlan) groupExpression.getOperator().toTreeNode(groupExpression)).withChildren(
                planChildren);
        if (!(plan instanceof PhysicalPlan)) {
            throw new AnalysisException("generate logical plan");
        }
        PhysicalPlan physicalPlan = (PhysicalPlan) plan;

        // TODO: set (logical and physical)properties/statistics/... for physicalPlan.

        return physicalPlan;
    }

    @Override
    public boolean isBlockQuery() {
        return true;
    }

    @Override
    public DescriptorTable getDescTable() {
        return descTable;
    }
}
