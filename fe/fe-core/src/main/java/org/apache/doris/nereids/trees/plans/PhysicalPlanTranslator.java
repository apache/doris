package org.apache.doris.nereids.trees.plans;

import org.apache.doris.analysis.AggregateInfo;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.UserException;
import org.apache.doris.nereids.PlanOperatorVisitor;
import org.apache.doris.nereids.operators.AbstractOperator;
import org.apache.doris.nereids.operators.plans.physical.PhysicalAggregation;
import org.apache.doris.nereids.operators.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.operators.plans.physical.PhysicalOperator;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.ExpressionConverter;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.planner.AggregationNode;
import org.apache.doris.planner.DataPartition;
import org.apache.doris.planner.ExchangeNode;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PlanFragment;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@SuppressWarnings("rawtypes")
public class PhysicalPlanTranslator extends PlanOperatorVisitor<PlanFragment, PlanContext> {

    public void translatePlan(PhysicalPlan<? extends PhysicalPlan, ? extends AbstractOperator> physicalPlan, PlanContext context) {
        visit(physicalPlan, context);
    }


    @Override
    public PlanFragment visit(PhysicalPlan<? extends PhysicalPlan, ? extends PhysicalOperator> physicalPlan,
                              PlanContext context) {
        PhysicalOperator<?> operator = physicalPlan.getOperator();
        return operator.accept(this, physicalPlan, context);
    }

    @Override
    public PlanFragment visitPhysicalAggregationPlan(PhysicalPlan<? extends PhysicalPlan, ? extends PhysicalOperator> physicalPlan,
                                                     PlanContext context) {

        PlanFragment inputPlanFragment = visit((PhysicalPlan<? extends PhysicalPlan, ? extends PhysicalOperator>) physicalPlan.child(0), context);

        AggregationNode aggregationNode = null;
        List<Slot> slotList = physicalPlan.getOutput();
        TupleDescriptor outputTupleDesc = generateTupleDesc(slotList, context, null);
        PhysicalAggregation physicalAggregation = (PhysicalAggregation) physicalPlan.getOperator();
        AggregateInfo.AggPhase phase = physicalAggregation.getAggPhase();

        List<Expression> groupByExpressionList = physicalAggregation.getGroupByExprList();
        ArrayList<Expr> execGroupingExpressions = groupByExpressionList
            .stream()
            .map(e -> ExpressionConverter.converter.convert(e))
            .collect(Collectors.toCollection(ArrayList::new));

        List<Expression> aggExpressionList = physicalAggregation.getAggExprList();
        // TODO: agg function could be other expr type either
        ArrayList<FunctionCallExpr> execAggExpressions = aggExpressionList
            .stream()
            .map(e -> (FunctionCallExpr)ExpressionConverter.converter.convert(e))
            .collect(Collectors.toCollection(ArrayList::new));

        List<Expression> partitionExpressionList = physicalAggregation.getPartitionExprList();
        List<Expr> execPartitionExpressions = partitionExpressionList
            .stream()
            .map(e -> (FunctionCallExpr)ExpressionConverter.converter.convert(e))
            .collect(Collectors.toList());

        AggregateInfo aggInfo = null;
        switch (phase) {
            case FIRST:
                aggInfo = AggregateInfo.create(
                    execGroupingExpressions,
                    execAggExpressions,
                    outputTupleDesc,
                    outputTupleDesc,
                    AggregateInfo.AggPhase.FIRST,
                    context.getAnalyzer());
                aggregationNode =
                    new AggregationNode(context.nextNodeId(), inputPlanFragment.getPlanRoot(), aggInfo);
                aggregationNode.unsetNeedsFinalize();
                aggregationNode.setUseStreamingPreagg(physicalAggregation.isUsingStream());
                aggregationNode.setIntermediateTuple();
                if (!partitionExpressionList.isEmpty()) {
                    inputPlanFragment.setOutputPartition(DataPartition.hashPartitioned(execPartitionExpressions));
                }
                break;
            case FIRST_MERGE:
                 aggInfo = AggregateInfo.create(
                    execGroupingExpressions,
                    execAggExpressions,
                    outputTupleDesc,
                    outputTupleDesc,
                    AggregateInfo.AggPhase.FIRST_MERGE,
                    context.getAnalyzer());
                 aggregationNode = new AggregationNode(context.nextNodeId(), inputPlanFragment.getPlanRoot(), aggInfo);
                break;
            default:
                throw new RuntimeException("Unsupported yet");
        }
        inputPlanFragment.setPlanRoot(aggregationNode);
        return inputPlanFragment;
    }

    @Override
    public PlanFragment visitPhysicalOlapScanPlan(PhysicalPlan<? extends PhysicalPlan, ? extends PhysicalOperator> physicalPlan,
                                                  PlanContext context) {
        // Create OlapScanNode
        List<Slot> slotList = physicalPlan.getOutput();
        PhysicalOlapScan physicalOlapScan = (PhysicalOlapScan) physicalPlan.getOperator();
        OlapTable olapTable = physicalOlapScan.getTable();
        TupleDescriptor tupleDescriptor = generateTupleDesc(slotList, context, olapTable);
        OlapScanNode olapScanNode = new OlapScanNode(context.nextNodeId(), tupleDescriptor, olapTable.getName());
        // Create PlanFragment
        PlanFragment planFragment = new PlanFragment(context.nextFragmentId(), olapScanNode, DataPartition.RANDOM);
        context.addPlanFragment(planFragment);
        return planFragment;
    }

    @Override
    public PlanFragment visitPhysicalSortPlan(PhysicalPlan<? extends PhysicalPlan, ? extends PhysicalOperator> physicalPlan,
                                              PlanContext context) {
        
    }

    @Override
    public PlanFragment visitPhysicalHashJoinPlan(PhysicalPlan<? extends PhysicalPlan, ? extends PhysicalOperator> physicalPlan,
                                                  PlanContext context) {

    }

    @Override
    public PlanFragment visitPhysicalProjectPlan(PhysicalPlan<? extends PhysicalPlan, ? extends PhysicalOperator> physicalPlan,
                                                 PlanContext context) {
        return visit((PhysicalPlan<? extends PhysicalPlan, ? extends PhysicalOperator>) physicalPlan.child(0),  context);
    }

    @Override
    public PlanFragment visitPhysicalFilter(PhysicalPlan<? extends PhysicalPlan, ? extends PhysicalOperator> physicalPlan,
                                            PlanContext context) {
        return visit((PhysicalPlan<? extends PhysicalPlan, ? extends PhysicalOperator>) physicalPlan.child(0),  context);
    }

    private TupleDescriptor generateTupleDesc(List<Slot> slotList, PlanContext context, Table table) {
        TupleDescriptor tupleDescriptor = context.generateTupleDesc();
        tupleDescriptor.setTable(table);
        for (Slot slot : slotList) {
            SlotReference slotReference = (SlotReference) slot;
            SlotDescriptor slotDescriptor = context.addSlotDesc(tupleDescriptor);
            slotDescriptor.setColumn(slotReference.getColumn());
            slotDescriptor.setType(slotReference.getDataType().toCatalogDataType());
        }
        return tupleDescriptor;
    }

    @Override
    public PlanFragment visitPhysicalExchange(PhysicalPlan<? extends PhysicalPlan, ? extends PhysicalOperator> physicalPlan, PlanContext context) {

        PlanFragment childFragment = visit((PhysicalPlan<? extends PhysicalPlan, ? extends PhysicalOperator>) physicalPlan.child(0),  context);

        ExchangeNode exchangeNode = new ExchangeNode(context.nextNodeId(), childFragment.getPlanRoot(), false);
        exchangeNode.setNumInstances(childFragment.getPlanRoot().getNumInstances());
        try {
            exchangeNode.init(context.getAnalyzer());
        } catch (UserException e) {
            throw new RuntimeException("Unexpected UserException ", e);
        }

        childFragment.setDestination(exchangeNode);

        return new PlanFragment(
            context.nextFragmentId(),
            exchangeNode,
            childFragment.getOutputPartition(),
            childFragment.getOutputPartition());
    }

}
