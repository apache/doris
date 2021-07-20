/*
 * // Licensed to the Apache Software Foundation (ASF) under one
 * // or more contributor license agreements.  See the NOTICE file
 * // distributed with this work for additional information
 * // regarding copyright ownership.  The ASF licenses this file
 * // to you under the Apache License, Version 2.0 (the
 * // "License"); you may not use this file except in compliance
 * // with the License.  You may obtain a copy of the License at
 * //
 * //   http://www.apache.org/licenses/LICENSE-2.0
 * //
 * // Unless required by applicable law or agreed to in writing,
 * // software distributed under the License is distributed on an
 * // "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * // KIND, either express or implied.  See the License for the
 * // specific language governing permissions and limitations
 * // under the License.
 *
 */

package org.apache.doris.planner;

import mockit.Mock;
import mockit.MockUp;
import mockit.Tested;
import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.BaseTableRef;
import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ExprSubstitutionMap;
import org.apache.doris.analysis.JoinOperator;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TableName;
import org.apache.doris.analysis.TableRef;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;

import com.google.common.collect.Lists;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import mockit.Expectations;
import mockit.Injectable;

public class SingleNodePlannerTest {

    @Test
    public void testMaterializeBaseTableRefResultForCrossJoinOrCountStar(@Injectable Table table,
                                                                         @Injectable TableName tableName,
                                                                         @Injectable Analyzer analyzer,
                                                                         @Injectable PlannerContext plannerContext,
                                                                         @Injectable Column column) {
        TableRef tableRef = new TableRef();
        Deencapsulation.setField(tableRef, "isAnalyzed", true);
        BaseTableRef baseTableRef = new BaseTableRef(tableRef, table, tableName);
        TupleDescriptor tupleDescriptor = new TupleDescriptor(new TupleId(1));
        SlotDescriptor slotDescriptor = new SlotDescriptor(new SlotId(1), tupleDescriptor);
        slotDescriptor.setIsMaterialized(false);
        tupleDescriptor.addSlot(slotDescriptor);
        Deencapsulation.setField(tableRef, "desc", tupleDescriptor);
        Deencapsulation.setField(baseTableRef, "desc", tupleDescriptor);
        tupleDescriptor.setTable(table);
        List<Column> columnList = Lists.newArrayList();
        columnList.add(column);
        new Expectations() {
            {
                table.getBaseSchema();
                result = columnList;
            }
        };
        SingleNodePlanner singleNodePlanner = new SingleNodePlanner(plannerContext);
        Deencapsulation.invoke(singleNodePlanner, "materializeSlotForEmptyMaterializedTableRef",
                baseTableRef, analyzer);
    }

    /*
    Assumptions:
    1. The order of materialized size from smallest to largest is t1, t2 ... tn
    2. The predicates are orthogonal to each other and don't affect each other.
     */

    /*
    Query: select * from t1 inner join t2 on t1.k1=t2.k1
    Original Query: select * from test1 inner join test2 on test1.k1=test2.k2
    Expect: without changed
     */
    @Test
    public void testJoinReorderWithTwoTuple1(@Injectable PlannerContext context,
                                             @Injectable Analyzer analyzer,
                                             @Injectable BaseTableRef tableRef1,
                                             @Injectable OlapScanNode scanNode1,
                                             @Injectable BaseTableRef tableRef2,
                                             @Injectable OlapScanNode scanNode2,
                                             @Injectable TupleDescriptor tupleDescriptor1,
                                             @Injectable SlotDescriptor slotDescriptor1,
                                             @Injectable SlotDescriptor slotDescriptor2,
                                             @Injectable BinaryPredicate eqBinaryPredicate,
                                             @Injectable SlotRef eqSlot1,
                                             @Injectable SlotRef eqSlot2,
                                             @Tested ExprSubstitutionMap exprSubstitutionMap) {
        List<SlotDescriptor> slotDescriptors1 = Lists.newArrayList();
        slotDescriptors1.add(slotDescriptor1);
        List<SlotDescriptor> slotDescriptors2 = Lists.newArrayList();
        slotDescriptors2.add(slotDescriptor2);
        tableRef1.setJoinOp(JoinOperator.INNER_JOIN);
        tableRef2.setJoinOp(JoinOperator.INNER_JOIN);
        List<Expr> eqConjuncts = Lists.newArrayList();
        eqConjuncts.add(eqBinaryPredicate);
        new Expectations() {
            {
                tableRef1.isAnalyzed();
                result = true;
                tableRef2.isAnalyzed();
                result = true;
                scanNode1.getCardinality();
                result = 1;
                scanNode2.getCardinality();
                result = 2;
                tableRef1.getDesc();
                result = tupleDescriptor1;
                tupleDescriptor1.getMaterializedSlots();
                result = slotDescriptors1;
                analyzer.getEqJoinConjuncts(new ArrayList<>(), new ArrayList<>());
                result = eqConjuncts;
                scanNode1.getTblRefIds();
                result = Lists.newArrayList();
                scanNode2.getTblRefIds();
                result = Lists.newArrayList();
                eqBinaryPredicate.getChild(0);
                result = eqSlot1;
                eqSlot1.isBoundByTupleIds(new ArrayList<>());
                result = true;
                eqBinaryPredicate.getChild(1);
                result = eqSlot2;
                eqSlot2.isBoundByTupleIds(new ArrayList<>());
                result = true;
                scanNode1.getTupleIds();
                result = Lists.newArrayList();
                scanNode2.getTupleIds();
                result = Lists.newArrayList();
                scanNode1.getOutputSmap();
                result = null;
                scanNode2.getOutputSmap();
                result = null;
                tableRef1.getUniqueAlias();
                result = "t1";
                tableRef2.getUniqueAlias();
                result = "t2";
            }
        };
        new MockUp<ExprSubstitutionMap>() {
            @Mock
            public ExprSubstitutionMap compose(ExprSubstitutionMap f, ExprSubstitutionMap g,
                                               Analyzer analyzer) {
                return exprSubstitutionMap;
            }

            @Mock
            public ExprSubstitutionMap combine(ExprSubstitutionMap f, ExprSubstitutionMap g) {
                return exprSubstitutionMap;
            }
        };
        SingleNodePlanner singleNodePlanner = new SingleNodePlanner(context);
        Pair<TableRef, PlanNode> pair1 = new Pair<>(tableRef1, scanNode1);
        Pair<TableRef, PlanNode> pair2 = new Pair<>(tableRef2, scanNode2);
        List<Pair<TableRef, PlanNode>> refPlans = Lists.newArrayList();
        refPlans.add(pair1);
        refPlans.add(pair2);

        PlanNode cheapestJoinNode =
                Deencapsulation.invoke(singleNodePlanner, "createCheapestJoinPlan", analyzer, refPlans);
        Assert.assertEquals(2, cheapestJoinNode.getChildren().size());
        Assert.assertEquals(scanNode2, cheapestJoinNode.getChild(0));
        Assert.assertEquals(scanNode1, cheapestJoinNode.getChild(1));
    }

    /*
    Query: select * from t1 left join t2 on t1.k1=t2.k1
    Original Query: select * from test1 left join test2 on test1.k1=test2.k2
    Expect: without changed
     */
    @Test
    public void testJoinReorderWithTwoTuple2(@Injectable PlannerContext context,
                                             @Injectable Analyzer analyzer,
                                             @Injectable BaseTableRef tableRef1,
                                             @Injectable OlapScanNode scanNode1,
                                             @Injectable BaseTableRef tableRef2,
                                             @Injectable OlapScanNode scanNode2,
                                             @Injectable TupleDescriptor tupleDescriptor2,
                                             @Injectable SlotDescriptor slotDescriptor1,
                                             @Injectable SlotDescriptor slotDescriptor2,
                                             @Injectable BinaryPredicate eqBinaryPredicate,
                                             @Injectable SlotRef eqSlot1,
                                             @Injectable SlotRef eqSlot2,
                                             @Tested ExprSubstitutionMap exprSubstitutionMap) {
        Pair<TableRef, PlanNode> pair1 = new Pair<>(tableRef1, scanNode1);
        Pair<TableRef, PlanNode> pair2 = new Pair<>(tableRef2, scanNode2);
        List<Pair<TableRef, PlanNode>> refPlans = Lists.newArrayList();
        refPlans.add(pair1);
        refPlans.add(pair2);
        tableRef1.setJoinOp(JoinOperator.INNER_JOIN);
        tableRef2.setJoinOp(JoinOperator.LEFT_OUTER_JOIN);

        List<SlotDescriptor> slotDescriptors1 = Lists.newArrayList();
        slotDescriptors1.add(slotDescriptor1);
        List<SlotDescriptor> slotDescriptors2 = Lists.newArrayList();
        slotDescriptors2.add(slotDescriptor2);
        List<Expr> eqConjuncts = Lists.newArrayList();
        eqConjuncts.add(eqBinaryPredicate);

        new Expectations() {
            {
                tableRef1.isAnalyzed();
                result = true;
                scanNode1.getCardinality();
                result = 1;
                scanNode2.getCardinality();
                result = 2;
                tableRef2.getDesc();
                result = tupleDescriptor2;
                tupleDescriptor2.getMaterializedSlots();
                result = slotDescriptors2;
                analyzer.getEqJoinConjuncts(new ArrayList<>(), new ArrayList<>());
                result = eqConjuncts;
                scanNode1.getTblRefIds();
                result = Lists.newArrayList();
                scanNode2.getTblRefIds();
                result = Lists.newArrayList();
                eqBinaryPredicate.getChild(0);
                result = eqSlot1;
                eqSlot1.isBoundByTupleIds(new ArrayList<>());
                result = true;
                eqBinaryPredicate.getChild(1);
                result = eqSlot2;
                eqSlot2.isBoundByTupleIds(new ArrayList<>());
                result = true;
                scanNode1.getTupleIds();
                result = Lists.newArrayList();
                scanNode2.getTupleIds();
                result = Lists.newArrayList();
                scanNode1.getOutputSmap();
                result = null;
                scanNode2.getOutputSmap();
                result = null;
                tableRef1.getUniqueAlias();
                result = "t1";
                tableRef2.getUniqueAlias();
                result = "t2";
                tableRef1.getJoinOp();
                result = JoinOperator.INNER_JOIN;
                tableRef2.getJoinOp();
                result = JoinOperator.LEFT_OUTER_JOIN;
            }
        };
        new MockUp<ExprSubstitutionMap>() {
            @Mock
            public ExprSubstitutionMap compose(ExprSubstitutionMap f, ExprSubstitutionMap g,
                                               Analyzer analyzer) {
                return exprSubstitutionMap;
            }

            @Mock
            public ExprSubstitutionMap combine(ExprSubstitutionMap f, ExprSubstitutionMap g) {
                return exprSubstitutionMap;
            }
        };

        SingleNodePlanner singleNodePlanner = new SingleNodePlanner(context);
        PlanNode cheapestJoinNode = Deencapsulation.invoke(singleNodePlanner, "createCheapestJoinPlan", analyzer, refPlans);
        Assert.assertEquals(2, cheapestJoinNode.getChildren().size());
        Assert.assertEquals(true, cheapestJoinNode instanceof HashJoinNode);
        Assert.assertEquals(JoinOperator.LEFT_OUTER_JOIN, ((HashJoinNode) cheapestJoinNode).getJoinOp());
        Assert.assertEquals(scanNode1, cheapestJoinNode.getChild(0));
        Assert.assertEquals(scanNode2, cheapestJoinNode.getChild(1));
    }

    /*
    Query: select * from t1 right join t2 on t1.k1=t2.k1
    Original Query: select * from test1 right join test2 on test1.k1=test2.k2
    Expect: without changed
     */
    @Test
    public void testJoinReorderWithTwoTuple3(@Injectable PlannerContext context,
                                             @Injectable Analyzer analyzer,
                                             @Injectable BaseTableRef tableRef1,
                                             @Injectable OlapScanNode scanNode1,
                                             @Injectable BaseTableRef tableRef2,
                                             @Injectable OlapScanNode scanNode2,
                                             @Injectable TupleDescriptor tupleDescriptor2,
                                             @Injectable SlotDescriptor slotDescriptor1,
                                             @Injectable SlotDescriptor slotDescriptor2,
                                             @Injectable BinaryPredicate eqBinaryPredicate,
                                             @Injectable SlotRef eqSlot1,
                                             @Injectable SlotRef eqSlot2,
                                             @Tested ExprSubstitutionMap exprSubstitutionMap) {
        Pair<TableRef, PlanNode> pair1 = new Pair<>(tableRef1, scanNode1);
        Pair<TableRef, PlanNode> pair2 = new Pair<>(tableRef2, scanNode2);
        List<Pair<TableRef, PlanNode>> refPlans = Lists.newArrayList();
        refPlans.add(pair1);
        refPlans.add(pair2);

        List<SlotDescriptor> slotDescriptors1 = Lists.newArrayList();
        slotDescriptors1.add(slotDescriptor1);
        List<SlotDescriptor> slotDescriptors2 = Lists.newArrayList();
        slotDescriptors2.add(slotDescriptor2);
        List<Expr> eqConjuncts = Lists.newArrayList();
        eqConjuncts.add(eqBinaryPredicate);

        new Expectations() {
            {
                tableRef1.isAnalyzed();
                result = true;
                scanNode1.getCardinality();
                result = 1;
                scanNode2.getCardinality();
                result = 2;
                tableRef2.getDesc();
                result = tupleDescriptor2;
                tupleDescriptor2.getMaterializedSlots();
                result = slotDescriptors2;
                analyzer.getEqJoinConjuncts(new ArrayList<>(), new ArrayList<>());
                result = eqConjuncts;
                scanNode1.getTblRefIds();
                result = Lists.newArrayList();
                scanNode2.getTblRefIds();
                result = Lists.newArrayList();
                eqBinaryPredicate.getChild(0);
                result = eqSlot1;
                eqSlot1.isBoundByTupleIds(new ArrayList<>());
                result = true;
                eqBinaryPredicate.getChild(1);
                result = eqSlot2;
                eqSlot2.isBoundByTupleIds(new ArrayList<>());
                result = true;
                scanNode1.getTupleIds();
                result = Lists.newArrayList();
                scanNode2.getTupleIds();
                result = Lists.newArrayList();
                scanNode1.getOutputSmap();
                result = null;
                scanNode2.getOutputSmap();
                result = null;
                tableRef1.getUniqueAlias();
                result = "t1";
                tableRef2.getUniqueAlias();
                result = "t2";
                tableRef1.getJoinOp();
                result = JoinOperator.INNER_JOIN;
                tableRef2.getJoinOp();
                result = JoinOperator.RIGHT_OUTER_JOIN;
            }
        };
        new MockUp<ExprSubstitutionMap>() {
            @Mock
            public ExprSubstitutionMap compose(ExprSubstitutionMap f, ExprSubstitutionMap g,
                                               Analyzer analyzer) {
                return exprSubstitutionMap;
            }

            @Mock
            public ExprSubstitutionMap combine(ExprSubstitutionMap f, ExprSubstitutionMap g) {
                return exprSubstitutionMap;
            }
        };

        SingleNodePlanner singleNodePlanner = new SingleNodePlanner(context);
        PlanNode cheapestJoinNode = Deencapsulation.invoke(singleNodePlanner, "createCheapestJoinPlan", analyzer, refPlans);
        Assert.assertEquals(2, cheapestJoinNode.getChildren().size());
        Assert.assertEquals(true, cheapestJoinNode instanceof HashJoinNode);
        Assert.assertEquals(JoinOperator.RIGHT_OUTER_JOIN, ((HashJoinNode) cheapestJoinNode).getJoinOp());
        Assert.assertEquals(scanNode1, cheapestJoinNode.getChild(0));
        Assert.assertEquals(scanNode2, cheapestJoinNode.getChild(1));
    }

    /*
    Query: select * from t1 left join t2 on t1.k1=t2.k1 inner join t3 on xxx
    Original Query: select * from test1 left join test2 on test1.k1=test2.k1 inner join test3 where test2.k1=test3.k1;
    Expect: without changed
     */
    @Test
    public void testKeepRightTableRefOnLeftJoin(@Injectable PlannerContext context,
                                                @Injectable Analyzer analyzer,
                                                @Injectable BaseTableRef tableRef1,
                                                @Injectable OlapScanNode scanNode1,
                                                @Injectable BaseTableRef tableRef2,
                                                @Injectable OlapScanNode scanNode2,
                                                @Injectable BaseTableRef tableRef3,
                                                @Injectable OlapScanNode scanNode3,
                                                @Injectable TupleDescriptor tupleDescriptor1,
                                                @Injectable TupleDescriptor tupleDescriptor2,
                                                @Injectable TupleDescriptor tupleDescriptor3,
                                                @Injectable SlotDescriptor slotDescriptor1,
                                                @Injectable SlotDescriptor slotDescriptor2,
                                                @Injectable SlotDescriptor slotDescriptor3,
                                                @Injectable BinaryPredicate eqBinaryPredicate1,
                                                @Injectable BinaryPredicate eqBinaryPredicate2,
                                                @Injectable BinaryPredicate eqBinaryPredicate3,
                                                @Injectable SlotRef eqT1Slot1,
                                                @Injectable SlotRef eqT2Slot2,
                                                @Injectable SlotRef eqT3Slot3,
                                                @Tested ExprSubstitutionMap exprSubstitutionMap) {
        Pair<TableRef, PlanNode> pair1 = new Pair<>(tableRef1, scanNode1);
        Pair<TableRef, PlanNode> pair2 = new Pair<>(tableRef2, scanNode2);
        Pair<TableRef, PlanNode> pair3 = new Pair<>(tableRef3, scanNode3);
        List<Pair<TableRef, PlanNode>> refPlans = Lists.newArrayList();
        refPlans.add(pair1);
        refPlans.add(pair2);
        refPlans.add(pair3);

        TupleId tupleId1 = new TupleId(1);
        TupleId tupleId2 = new TupleId(2);
        TupleId tupleId3 = new TupleId(3);
        List<TupleId> tupleIds1 = Lists.newArrayList(tupleId1);
        List<TupleId> tupleIds2 = Lists.newArrayList(tupleId2);
        List<TupleId> tupleIds3 = Lists.newArrayList(tupleId3);

        List<SlotDescriptor> slotDescriptors1 = Lists.newArrayList();
        slotDescriptors1.add(slotDescriptor1);
        List<SlotDescriptor> slotDescriptors2 = Lists.newArrayList();
        slotDescriptors2.add(slotDescriptor2);
        List<Expr> eqConjuncts1 = Lists.newArrayList();
        eqConjuncts1.add(eqBinaryPredicate1);
        List<Expr> eqConjuncts2 = Lists.newArrayList();
        eqConjuncts2.add(eqBinaryPredicate2);
        List<Expr> eqConjuncts3 = Lists.newArrayList();
        eqConjuncts3.add(eqBinaryPredicate3);


        new Expectations() {
            {
                tableRef1.isAnalyzed();
                result = true;
                tableRef3.isAnalyzed();
                result = true;
                scanNode1.getCardinality();
                result = 1;
                scanNode2.getCardinality();
                result = 2;
                scanNode3.getCardinality();
                result = 3;
                tableRef1.getDesc();
                result = tupleDescriptor1;
                tupleDescriptor1.getMaterializedSlots();
                result = slotDescriptors1;
                tableRef2.getDesc();
                result = tupleDescriptor2;
                tupleDescriptor2.getMaterializedSlots();
                result = slotDescriptors2;
                tableRef3.getDesc();
                result = tupleDescriptor3;
                tupleDescriptor3.getMaterializedSlots();
                result = slotDescriptor3;
                analyzer.getEqJoinConjuncts(tupleIds1, tupleIds2);
                result = eqConjuncts1;
                analyzer.getEqJoinConjuncts(Lists.newArrayList(tupleId1, tupleId2), tupleIds3);
                result = eqConjuncts2;
                analyzer.getEqJoinConjuncts(tupleIds3, tupleIds1);
                result = eqConjuncts3;
                scanNode1.getTblRefIds();
                result = Lists.newArrayList(tupleIds1);
                scanNode2.getTblRefIds();
                result = Lists.newArrayList(tupleIds2);
                scanNode3.getTblRefIds();
                result = Lists.newArrayList(tupleIds3);
                eqBinaryPredicate1.getChild(0);
                result = eqT1Slot1;
                eqT1Slot1.isBoundByTupleIds(tupleIds1);
                result = true;
                eqBinaryPredicate1.getChild(1);
                result = eqT2Slot2;
                eqT2Slot2.isBoundByTupleIds(tupleIds2);
                result = true;
                eqT2Slot2.isBoundByTupleIds(Lists.newArrayList(tupleId1, tupleId2));
                result = true;
                eqBinaryPredicate2.getChild(0);
                result = eqT2Slot2;
                eqBinaryPredicate2.getChild(1);
                result = eqT3Slot3;
                eqT3Slot3.isBoundByTupleIds(tupleIds3);
                result = true;
                eqBinaryPredicate3.getChild(0);
                result = eqT1Slot1;
                eqBinaryPredicate3.getChild(1);
                result = eqT3Slot3;
                scanNode1.getTupleIds();
                result = tupleIds1;
                scanNode2.getTupleIds();
                result = tupleIds2;
                scanNode3.getTupleIds();
                result = tupleId3;
                scanNode1.getOutputSmap();
                result = null;
                scanNode2.getOutputSmap();
                result = null;
                tableRef1.getUniqueAlias();
                result = "t1";
                tableRef2.getUniqueAlias();
                result = "t2";
                tableRef3.getUniqueAlias();
                result = "t3";
                tableRef1.getJoinOp();
                result = JoinOperator.INNER_JOIN;
                tableRef2.getJoinOp();
                result = JoinOperator.LEFT_OUTER_JOIN;
                tableRef3.getJoinOp();
                result = JoinOperator.INNER_JOIN;
            }
        };
        new MockUp<ExprSubstitutionMap>() {
            @Mock
            public ExprSubstitutionMap compose(ExprSubstitutionMap f, ExprSubstitutionMap g,
                                               Analyzer analyzer) {
                return exprSubstitutionMap;
            }

            @Mock
            public ExprSubstitutionMap combine(ExprSubstitutionMap f, ExprSubstitutionMap g) {
                return exprSubstitutionMap;
            }
        };

        SingleNodePlanner singleNodePlanner = new SingleNodePlanner(context);
        PlanNode cheapestJoinNode = Deencapsulation.invoke(singleNodePlanner, "createCheapestJoinPlan", analyzer, refPlans);
        Assert.assertEquals(2, cheapestJoinNode.getChildren().size());
        Assert.assertEquals(true, cheapestJoinNode instanceof HashJoinNode);
        Assert.assertTrue(((HashJoinNode) cheapestJoinNode).getJoinOp().isInnerJoin());
        Assert.assertEquals(true, cheapestJoinNode.getChild(0) instanceof HashJoinNode);
        HashJoinNode child0 = (HashJoinNode) cheapestJoinNode.getChild(0);
        Assert.assertTrue(child0.getJoinOp().isOuterJoin());
        Assert.assertEquals(2, child0.getChildren().size());
        Assert.assertEquals(scanNode1, child0.getChild(0));
        Assert.assertEquals(scanNode2, child0.getChild(1));
        Assert.assertEquals(scanNode3, cheapestJoinNode.getChild(1));

    }

    /*
    Query: select * from t1 right join t2 on t1.k1=t2.k1 inner join t3 on xxx
    Original Query: select * from test1 right join test2 on test1.k1=test2.k1 inner join test3 where test2.k1=test3.k1
    Expect: without changed
     */
    @Test
    public void testKeepRightTableRefOnRightJoin(@Injectable PlannerContext context,
                                                 @Injectable Analyzer analyzer,
                                                 @Injectable BaseTableRef tableRef1,
                                                 @Injectable OlapScanNode scanNode1,
                                                 @Injectable BaseTableRef tableRef2,
                                                 @Injectable OlapScanNode scanNode2,
                                                 @Injectable BaseTableRef tableRef3,
                                                 @Injectable OlapScanNode scanNode3,
                                                 @Injectable TupleDescriptor tupleDescriptor1,
                                                 @Injectable TupleDescriptor tupleDescriptor2,
                                                 @Injectable TupleDescriptor tupleDescriptor3,
                                                 @Injectable SlotDescriptor slotDescriptor1,
                                                 @Injectable SlotDescriptor slotDescriptor2,
                                                 @Injectable SlotDescriptor slotDescriptor3,
                                                 @Injectable BinaryPredicate eqBinaryPredicate1,
                                                 @Injectable BinaryPredicate eqBinaryPredicate2,
                                                 @Injectable BinaryPredicate eqBinaryPredicate3,
                                                 @Injectable SlotRef eqT1Slot1,
                                                 @Injectable SlotRef eqT2Slot2,
                                                 @Injectable SlotRef eqT3Slot3,
                                                 @Tested ExprSubstitutionMap exprSubstitutionMap) {
        Pair<TableRef, PlanNode> pair1 = new Pair<>(tableRef1, scanNode1);
        Pair<TableRef, PlanNode> pair2 = new Pair<>(tableRef2, scanNode2);
        Pair<TableRef, PlanNode> pair3 = new Pair<>(tableRef3, scanNode3);
        List<Pair<TableRef, PlanNode>> refPlans = Lists.newArrayList();
        refPlans.add(pair1);
        refPlans.add(pair2);
        refPlans.add(pair3);

        TupleId tupleId1 = new TupleId(1);
        TupleId tupleId2 = new TupleId(2);
        TupleId tupleId3 = new TupleId(3);
        List<TupleId> tupleIds1 = Lists.newArrayList(tupleId1);
        List<TupleId> tupleIds2 = Lists.newArrayList(tupleId2);
        List<TupleId> tupleIds3 = Lists.newArrayList(tupleId3);

        List<SlotDescriptor> slotDescriptors1 = Lists.newArrayList();
        slotDescriptors1.add(slotDescriptor1);
        List<SlotDescriptor> slotDescriptors2 = Lists.newArrayList();
        slotDescriptors2.add(slotDescriptor2);
        List<Expr> eqConjuncts1 = Lists.newArrayList();
        eqConjuncts1.add(eqBinaryPredicate1);
        List<Expr> eqConjuncts2 = Lists.newArrayList();
        eqConjuncts2.add(eqBinaryPredicate2);
        List<Expr> eqConjuncts3 = Lists.newArrayList();
        eqConjuncts3.add(eqBinaryPredicate3);


        new Expectations() {
            {
                tableRef1.isAnalyzed();
                result = true;
                tableRef3.isAnalyzed();
                result = true;
                scanNode1.getCardinality();
                result = 1;
                scanNode2.getCardinality();
                result = 2;
                scanNode3.getCardinality();
                result = 3;
                tableRef1.getDesc();
                result = tupleDescriptor1;
                tupleDescriptor1.getMaterializedSlots();
                result = slotDescriptors1;
                tableRef2.getDesc();
                result = tupleDescriptor2;
                tupleDescriptor2.getMaterializedSlots();
                result = slotDescriptors2;
                tableRef3.getDesc();
                result = tupleDescriptor3;
                tupleDescriptor3.getMaterializedSlots();
                result = Lists.newArrayList(slotDescriptor3);
                analyzer.getEqJoinConjuncts(tupleIds1, tupleIds2);
                result = eqConjuncts1;
                analyzer.getEqJoinConjuncts(Lists.newArrayList(tupleId1, tupleId2), tupleIds3);
                result = eqConjuncts2;
                analyzer.getEqJoinConjuncts(tupleIds3, tupleIds1);
                result = eqConjuncts3;
                scanNode1.getTblRefIds();
                result = Lists.newArrayList(tupleIds1);
                scanNode2.getTblRefIds();
                result = Lists.newArrayList(tupleIds2);
                scanNode3.getTblRefIds();
                result = Lists.newArrayList(tupleIds3);
                eqBinaryPredicate1.getChild(0);
                result = eqT1Slot1;
                eqT1Slot1.isBoundByTupleIds(tupleIds1);
                result = true;
                eqBinaryPredicate1.getChild(1);
                result = eqT2Slot2;
                eqT2Slot2.isBoundByTupleIds(tupleIds2);
                result = true;
                eqT2Slot2.isBoundByTupleIds(Lists.newArrayList(tupleId1, tupleId2));
                result = true;
                eqBinaryPredicate2.getChild(0);
                result = eqT2Slot2;
                eqBinaryPredicate2.getChild(1);
                result = eqT3Slot3;
                eqT3Slot3.isBoundByTupleIds(tupleIds3);
                result = true;
                eqBinaryPredicate3.getChild(0);
                result = eqT1Slot1;
                eqBinaryPredicate3.getChild(1);
                result = eqT3Slot3;
                scanNode1.getTupleIds();
                result = tupleIds1;
                scanNode2.getTupleIds();
                result = tupleIds2;
                scanNode3.getTupleIds();
                result = tupleId3;
                scanNode1.getOutputSmap();
                result = null;
                scanNode2.getOutputSmap();
                result = null;
                tableRef1.getUniqueAlias();
                result = "t1";
                tableRef2.getUniqueAlias();
                result = "t2";
                tableRef3.getUniqueAlias();
                result = "t3";
                tableRef1.getJoinOp();
                result = JoinOperator.INNER_JOIN;
                tableRef2.getJoinOp();
                result = JoinOperator.RIGHT_OUTER_JOIN;
                tableRef3.getJoinOp();
                result = JoinOperator.INNER_JOIN;
            }
        };
        new MockUp<ExprSubstitutionMap>() {
            @Mock
            public ExprSubstitutionMap compose(ExprSubstitutionMap f, ExprSubstitutionMap g,
                                               Analyzer analyzer) {
                return exprSubstitutionMap;
            }

            @Mock
            public ExprSubstitutionMap combine(ExprSubstitutionMap f, ExprSubstitutionMap g) {
                return exprSubstitutionMap;
            }
        };

        SingleNodePlanner singleNodePlanner = new SingleNodePlanner(context);
        PlanNode cheapestJoinNode = Deencapsulation.invoke(singleNodePlanner, "createCheapestJoinPlan", analyzer, refPlans);
        Assert.assertEquals(2, cheapestJoinNode.getChildren().size());
        Assert.assertEquals(true, cheapestJoinNode instanceof HashJoinNode);
        Assert.assertTrue(((HashJoinNode) cheapestJoinNode).getJoinOp().isInnerJoin());
        Assert.assertEquals(true, cheapestJoinNode.getChild(0) instanceof HashJoinNode);
        HashJoinNode child0 = (HashJoinNode) cheapestJoinNode.getChild(0);
        Assert.assertTrue(child0.getJoinOp().isOuterJoin());
        Assert.assertEquals(2, child0.getChildren().size());
        Assert.assertEquals(scanNode1, child0.getChild(0));
        Assert.assertEquals(scanNode2, child0.getChild(1));
        Assert.assertEquals(scanNode3, cheapestJoinNode.getChild(1));
    }

    /*
    Query: select * from t1,t2 right join t3,t5,t4 left join t6,t7
    Expect: keep t3, t6 position
            t2, t1 right join t3, t4,t5 left join t6,t7
     */
    @Test
    public void testKeepMultiOuterJoin(@Injectable PlannerContext context,
                                       @Injectable Analyzer analyzer,
                                       @Injectable BaseTableRef tableRef1, @Injectable OlapScanNode scanNode1,
                                       @Injectable BaseTableRef tableRef2, @Injectable OlapScanNode scanNode2,
                                       @Injectable BaseTableRef tableRef3, @Injectable OlapScanNode scanNode3,
                                       @Injectable BaseTableRef tableRef4, @Injectable OlapScanNode scanNode4,
                                       @Injectable BaseTableRef tableRef5, @Injectable OlapScanNode scanNode5,
                                       @Injectable BaseTableRef tableRef6, @Injectable OlapScanNode scanNode6,
                                       @Injectable BaseTableRef tableRef7, @Injectable OlapScanNode scanNode7,
                                       @Injectable TupleDescriptor tupleDescriptor1,
                                       @Injectable TupleDescriptor tupleDescriptor2,
                                       @Injectable TupleDescriptor tupleDescriptor3,
                                       @Injectable TupleDescriptor tupleDescriptor4,
                                       @Injectable TupleDescriptor tupleDescriptor5,
                                       @Injectable TupleDescriptor tupleDescriptor6,
                                       @Injectable TupleDescriptor tupleDescriptor7,
                                       @Injectable SlotDescriptor slotDescriptor1,
                                       @Injectable SlotDescriptor slotDescriptor2,
                                       @Injectable SlotDescriptor slotDescriptor3,
                                       @Injectable SlotDescriptor slotDescriptor4,
                                       @Injectable SlotDescriptor slotDescriptor5,
                                       @Injectable SlotDescriptor slotDescriptor6,
                                       @Injectable SlotDescriptor slotDescriptor7,
                                       @Injectable BinaryPredicate eqBinaryPredicate1,
                                       @Injectable BinaryPredicate eqBinaryPredicate2,
                                       @Injectable BinaryPredicate eqBinaryPredicate3,
                                       @Injectable BinaryPredicate eqBinaryPredicate4,
                                       @Injectable BinaryPredicate eqBinaryPredicate5,
                                       @Injectable BinaryPredicate eqBinaryPredicate6,
                                       @Injectable BinaryPredicate eqBinaryPredicate7,
                                       @Injectable SlotRef eqT1Slot1,
                                       @Injectable SlotRef eqT2Slot2,
                                       @Injectable SlotRef eqT3Slot3,
                                       @Injectable SlotRef eqT4Slot4,
                                       @Injectable SlotRef eqT5Slot5,
                                       @Injectable SlotRef eqT6Slot6,
                                       @Injectable SlotRef eqT7Slot7,
                                       @Tested ExprSubstitutionMap exprSubstitutionMap) {
        Pair<TableRef, PlanNode> pair1 = new Pair<>(tableRef1, scanNode1);
        Pair<TableRef, PlanNode> pair2 = new Pair<>(tableRef2, scanNode2);
        Pair<TableRef, PlanNode> pair3 = new Pair<>(tableRef3, scanNode3);
        Pair<TableRef, PlanNode> pair4 = new Pair<>(tableRef4, scanNode4);
        Pair<TableRef, PlanNode> pair5 = new Pair<>(tableRef5, scanNode5);
        Pair<TableRef, PlanNode> pair6 = new Pair<>(tableRef6, scanNode6);
        Pair<TableRef, PlanNode> pair7 = new Pair<>(tableRef7, scanNode7);
        List<Pair<TableRef, PlanNode>> refPlans = Lists.newArrayList();
        refPlans.add(pair1);
        refPlans.add(pair2);
        refPlans.add(pair3);
        refPlans.add(pair5);
        refPlans.add(pair4);
        refPlans.add(pair6);
        refPlans.add(pair7);

        TupleId tupleId1 = new TupleId(1);
        TupleId tupleId2 = new TupleId(2);
        TupleId tupleId3 = new TupleId(3);
        TupleId tupleId4 = new TupleId(4);
        TupleId tupleId5 = new TupleId(5);
        TupleId tupleId6 = new TupleId(6);
        TupleId tupleId7 = new TupleId(7);
        List<TupleId> tupleIds1 = Lists.newArrayList(tupleId1);
        List<TupleId> tupleIds2 = Lists.newArrayList(tupleId2);
        List<TupleId> tupleIds3 = Lists.newArrayList(tupleId3);
        List<TupleId> tupleIds4 = Lists.newArrayList(tupleId4);
        List<TupleId> tupleIds5 = Lists.newArrayList(tupleId5);
        List<TupleId> tupleIds6 = Lists.newArrayList(tupleId6);
        List<TupleId> tupleIds7 = Lists.newArrayList(tupleId7);
        List<TupleId> tupleIds213 = Lists.newArrayList(tupleId2, tupleId1, tupleId3);
        List<TupleId> tupleIds21345 = new ArrayList<>();
        tupleIds21345.addAll(tupleIds213);
        tupleIds21345.add(tupleId4);
        tupleIds21345.add(tupleId5);
        List<TupleId> tupleIds213456 = Lists.newArrayList(tupleId2, tupleId1, tupleId3, tupleId4, tupleId5, tupleId6);

        List<SlotDescriptor> slotDescriptors1 = Lists.newArrayList();
        slotDescriptors1.add(slotDescriptor1);
        List<SlotDescriptor> slotDescriptors2 = Lists.newArrayList();
        slotDescriptors2.add(slotDescriptor2);
        List<Expr> eqConjuncts1 = Lists.newArrayList();
        eqConjuncts1.add(eqBinaryPredicate1);
        List<Expr> eqConjuncts2 = Lists.newArrayList();
        eqConjuncts2.add(eqBinaryPredicate2);
        List<Expr> eqConjuncts3 = Lists.newArrayList();
        eqConjuncts3.add(eqBinaryPredicate3);


        new Expectations() {
            {
                tableRef1.isAnalyzed();
                result = true;
                tableRef2.isAnalyzed();
                result = true;
                tableRef4.isAnalyzed();
                result = true;
                tableRef5.isAnalyzed();
                result = true;
                tableRef7.isAnalyzed();
                result = true;
                scanNode1.getCardinality();
                result = 1;
                scanNode2.getCardinality();
                result = 2;
                scanNode3.getCardinality();
                result = 3;
                scanNode4.getCardinality();
                result = 4;
                scanNode5.getCardinality();
                result = 5;
                scanNode6.getCardinality();
                result = 6;
                scanNode7.getCardinality();
                result = 7;
                tableRef1.getDesc();
                result = tupleDescriptor1;
                tupleDescriptor1.getMaterializedSlots();
                result = slotDescriptors1;
                tableRef2.getDesc();
                result = tupleDescriptor2;
                tupleDescriptor2.getMaterializedSlots();
                result = slotDescriptors2;
                tableRef3.getDesc();
                result = tupleDescriptor3;
                tupleDescriptor3.getMaterializedSlots();
                result = Lists.newArrayList(slotDescriptor3);
                tableRef4.getDesc();
                result = tupleDescriptor4;
                tupleDescriptor4.getMaterializedSlots();
                result = Lists.newArrayList(slotDescriptor4);
                tableRef5.getDesc();
                result = tupleDescriptor5;
                tupleDescriptor5.getMaterializedSlots();
                result = Lists.newArrayList(slotDescriptor5);
                tableRef6.getDesc();
                result = tupleDescriptor6;
                tupleDescriptor6.getMaterializedSlots();
                result = Lists.newArrayList(slotDescriptor6);
                tableRef7.getDesc();
                result = tupleDescriptor7;
                tupleDescriptor7.getMaterializedSlots();
                result = Lists.newArrayList(slotDescriptor7);
                analyzer.getEqJoinConjuncts(tupleIds7, tupleIds1);
                result = eqConjuncts1;
                eqBinaryPredicate1.getChild(0);
                result = eqT7Slot7;
                eqT7Slot7.isBoundByTupleIds(tupleIds7);
                result = true;
                eqBinaryPredicate1.getChild(1);
                result = eqT1Slot1;
                eqT1Slot1.isBoundByTupleIds(tupleIds1);
                result = true;
                analyzer.getEqJoinConjuncts(Lists.newArrayList(tupleId2, tupleId1), tupleIds3);
                result = eqConjuncts2;
                analyzer.getEqJoinConjuncts(tupleIds213, tupleIds5);
                result = Lists.newArrayList(eqBinaryPredicate4);
                analyzer.getEqJoinConjuncts(tupleIds213, tupleIds4);
                result = Lists.newArrayList(eqBinaryPredicate5);
                analyzer.getEqJoinConjuncts(tupleIds21345, tupleIds6);
                result = Lists.newArrayList(eqBinaryPredicate6);
                eqBinaryPredicate6.getChild(0);
                result = eqT5Slot5;
                eqBinaryPredicate6.getChild(1);
                result = eqT6Slot6;
                eqT5Slot5.isBoundByTupleIds(tupleIds21345);
                result = true;
                eqT6Slot6.isBoundByTupleIds(tupleIds6);
                result = true;
                analyzer.getEqJoinConjuncts(tupleIds213456, tupleIds7);
                result = Lists.newArrayList(eqBinaryPredicate7);
                eqBinaryPredicate7.getChild(0);
                result = eqT6Slot6;
                eqBinaryPredicate7.getChild(1);
                result = eqT7Slot7;
                eqT6Slot6.isBoundByTupleIds(tupleIds213456);
                result = true;
                eqT7Slot7.isBoundByTupleIds(tupleIds7);
                result = true;
                scanNode1.getTblRefIds();
                result = Lists.newArrayList(tupleIds1);
                scanNode2.getTblRefIds();
                result = Lists.newArrayList(tupleIds2);
                scanNode3.getTblRefIds();
                result = Lists.newArrayList(tupleIds3);
                scanNode4.getTblRefIds();
                result = Lists.newArrayList(tupleId4);
                scanNode5.getTblRefIds();
                result = Lists.newArrayList(tupleId5);
                scanNode6.getTblRefIds();
                result = Lists.newArrayList(tupleId6);
                scanNode7.getTblRefIds();
                result = Lists.newArrayList(tupleId7);

                eqT2Slot2.isBoundByTupleIds(Lists.newArrayList(tupleId2, tupleId1));
                result = true;
                eqBinaryPredicate2.getChild(0);
                result = eqT2Slot2;
                eqBinaryPredicate2.getChild(1);
                result = eqT3Slot3;
                eqT3Slot3.isBoundByTupleIds(tupleIds3);
                result = true;
                eqBinaryPredicate4.getChild(0);
                result = eqT3Slot3;
                eqBinaryPredicate4.getChild(1);
                result = eqT5Slot5;
                eqT3Slot3.isBoundByTupleIds(tupleIds213);
                result = true;
                eqT5Slot5.isBoundByTupleIds(Lists.newArrayList(tupleId5));
                result = true;
                eqBinaryPredicate5.getChild(0);
                result = eqT3Slot3;
                eqBinaryPredicate5.getChild(1);
                result = eqT4Slot4;
                eqT4Slot4.isBoundByTupleIds(Lists.newArrayList(tupleId4));
                result = true;
                scanNode1.getTupleIds();
                result = tupleIds1;
                scanNode2.getTupleIds();
                result = tupleIds2;
                scanNode3.getTupleIds();
                result = tupleIds3;
                scanNode4.getTupleIds();
                result = tupleIds4;
                scanNode5.getTupleIds();
                result = tupleIds5;
                scanNode6.getTupleIds();
                result = tupleIds6;
                scanNode7.getTupleIds();
                result = tupleIds7;
                scanNode1.getOutputSmap();
                result = null;
                scanNode2.getOutputSmap();
                result = null;
                scanNode3.getOutputSmap();
                result = null;
                scanNode4.getOutputSmap();
                result = null;
                scanNode5.getOutputSmap();
                result = null;
                scanNode6.getOutputSmap();
                result = null;
                scanNode7.getOutputSmap();
                result = null;
                tableRef1.getUniqueAlias();
                result = "t1";
                tableRef2.getUniqueAlias();
                result = "t2";
                tableRef3.getUniqueAlias();
                result = "t3";
                tableRef4.getUniqueAlias();
                result = "t4";
                tableRef5.getUniqueAlias();
                result = "t5";
                tableRef6.getUniqueAlias();
                result = "t6";
                tableRef7.getUniqueAlias();
                result = "t7";
                tableRef1.getJoinOp();
                result = JoinOperator.INNER_JOIN;
                tableRef2.getJoinOp();
                result = JoinOperator.INNER_JOIN;
                tableRef3.getJoinOp();
                result = JoinOperator.RIGHT_OUTER_JOIN;
                tableRef4.getJoinOp();
                result = JoinOperator.INNER_JOIN;
                tableRef5.getJoinOp();
                result = JoinOperator.INNER_JOIN;
                tableRef6.getJoinOp();
                result = JoinOperator.LEFT_OUTER_JOIN;
                tableRef7.getJoinOp();
                result = JoinOperator.INNER_JOIN;
            }
        };
        new MockUp<ExprSubstitutionMap>() {
            @Mock
            public ExprSubstitutionMap compose(ExprSubstitutionMap f, ExprSubstitutionMap g,
                                               Analyzer analyzer) {
                return exprSubstitutionMap;
            }

            @Mock
            public ExprSubstitutionMap combine(ExprSubstitutionMap f, ExprSubstitutionMap g) {
                return exprSubstitutionMap;
            }
        };

        SingleNodePlanner singleNodePlanner = new SingleNodePlanner(context);
        PlanNode cheapestJoinNode = Deencapsulation.invoke(singleNodePlanner, "createCheapestJoinPlan", analyzer, refPlans);
        Assert.assertEquals(2, cheapestJoinNode.getChildren().size());
        Assert.assertEquals(Lists.newArrayList(tupleId2, tupleId1, tupleId3, tupleId4, tupleId5, tupleId6, tupleId7),
                cheapestJoinNode.getTupleIds());
    }

    /*
    Query: select * from t1, t3, t2, t4 where (all of inner join condition)
    Original Query: select * from test1, test3, test2, test4
                where test1.k1=test3.k1 and test3.k2=test2.k2 and test2.k3=test4.k3;
    Expect: t4(the largest), t2, t3, t1 (without cross join)
    Round1: (t4 cross t1) pk (t4 cross t3) pk (t4 inner t2) => t4, t2
    Round2: ([t4,t2] cross t1) pk ([t4,t2] inner t3) => t4, t2, t3
    Round3: t4, t2, t3, t1 without pk
    */
    @Test
    public void testMultiInnerJoinReorderAvoidCrossJoin(@Injectable PlannerContext context,
                                          @Injectable Analyzer analyzer,
                                          @Injectable BaseTableRef tableRef1, @Injectable OlapScanNode scanNode1,
                                          @Injectable BaseTableRef tableRef2, @Injectable OlapScanNode scanNode2,
                                          @Injectable BaseTableRef tableRef3, @Injectable OlapScanNode scanNode3,
                                          @Injectable BaseTableRef tableRef4, @Injectable OlapScanNode scanNode4,
                                          @Injectable TupleDescriptor tupleDescriptor1,
                                          @Injectable TupleDescriptor tupleDescriptor2,
                                          @Injectable TupleDescriptor tupleDescriptor3,
                                          @Injectable SlotDescriptor slotDescriptor1,
                                          @Injectable SlotDescriptor slotDescriptor2,
                                          @Injectable SlotDescriptor slotDescriptor3,
                                          @Injectable BinaryPredicate eqBinaryPredicate3,
                                          @Injectable BinaryPredicate eqBinaryPredicate5,
                                          @Injectable BinaryPredicate eqBinaryPredicate6,
                                          @Injectable SlotRef eqT1Slot1,
                                          @Injectable SlotRef eqT2Slot2,
                                          @Injectable SlotRef eqT3Slot3,
                                          @Injectable SlotRef eqT4Slot4,
                                          @Tested ExprSubstitutionMap exprSubstitutionMap) {
        Pair<TableRef, PlanNode> pair1 = new Pair<>(tableRef1, scanNode1);
        Pair<TableRef, PlanNode> pair2 = new Pair<>(tableRef2, scanNode2);
        Pair<TableRef, PlanNode> pair3 = new Pair<>(tableRef3, scanNode3);
        Pair<TableRef, PlanNode> pair4 = new Pair<>(tableRef4, scanNode4);
        List<Pair<TableRef, PlanNode>> refPlans = Lists.newArrayList();
        refPlans.add(pair3);
        refPlans.add(pair2);
        refPlans.add(pair1);
        refPlans.add(pair4);

        TupleId tupleId1 = new TupleId(1);
        TupleId tupleId2 = new TupleId(2);
        TupleId tupleId3 = new TupleId(3);
        TupleId tupleId4 = new TupleId(4);
        List<TupleId> tupleIds1 = Lists.newArrayList(tupleId1);
        List<TupleId> tupleIds2 = Lists.newArrayList(tupleId2);
        List<TupleId> tupleIds3 = Lists.newArrayList(tupleId3);
        List<TupleId> tupleIds4 = Lists.newArrayList(tupleId4);
        List<TupleId> tupleIds41 = Lists.newArrayList(tupleId4, tupleId1);
        List<TupleId> tupleIds412 = Lists.newArrayList(tupleId4, tupleId1, tupleId2);

        new Expectations() {
            {
                tableRef1.isAnalyzed();
                result = true;
                tableRef2.isAnalyzed();
                result = true;
                tableRef3.isAnalyzed();
                result = true;
                tableRef4.isAnalyzed();
                result = true;
                scanNode1.getCardinality();
                result = 1;
                scanNode2.getCardinality();
                result = 2;
                scanNode3.getCardinality();
                result = 3;
                scanNode4.getCardinality();
                result = 4;
                tableRef1.getDesc();
                result = tupleDescriptor1;
                tupleDescriptor1.getMaterializedSlots();
                result = Lists.newArrayList(slotDescriptor1);
                tableRef2.getDesc();
                result = tupleDescriptor2;
                tupleDescriptor2.getMaterializedSlots();
                result = Lists.newArrayList(slotDescriptor2);
                tableRef3.getDesc();
                result = tupleDescriptor3;
                tupleDescriptor3.getMaterializedSlots();
                result = Lists.newArrayList(slotDescriptor3);

                // where t4.k1=t1.k1
                analyzer.getEqJoinConjuncts(tupleIds4, tupleIds1);
                result = Lists.newArrayList(eqBinaryPredicate3);
                eqBinaryPredicate3.getChild(0);
                result = eqT4Slot4;
                eqT4Slot4.isBoundByTupleIds(tupleIds4);
                result = true;
                eqBinaryPredicate3.getChild(1);
                result = eqT1Slot1;
                eqT1Slot1.isBoundByTupleIds(tupleIds1);
                result = true;
                // where t1.k1=t2.k1
                analyzer.getEqJoinConjuncts(tupleIds41, tupleIds2);
                result = Lists.newArrayList(eqBinaryPredicate5);
                eqBinaryPredicate5.getChild(0);
                result = eqT1Slot1;
                eqT1Slot1.isBoundByTupleIds(tupleIds41);
                result = true;
                eqBinaryPredicate5.getChild(1);
                result = eqT2Slot2;
                eqT2Slot2.isBoundByTupleIds(tupleIds2);
                result = true;
                // where t2.k1 = t3.k1
                analyzer.getEqJoinConjuncts(tupleIds412, tupleIds3);
                result = Lists.newArrayList(eqBinaryPredicate6);
                eqBinaryPredicate6.getChild(0);
                result = eqT2Slot2;
                eqT2Slot2.isBoundByTupleIds(tupleIds412);
                result = true;
                eqBinaryPredicate6.getChild(1);
                result = eqT3Slot3;
                eqT3Slot3.isBoundByTupleIds(tupleIds3);
                result = true;

                scanNode1.getTblRefIds();
                result = tupleIds1;
                scanNode2.getTblRefIds();
                result = tupleIds2;
                scanNode3.getTblRefIds();
                result = tupleIds3;
                scanNode4.getTblRefIds();
                result = tupleIds4;

                scanNode1.getTupleIds();
                result = tupleIds1;
                scanNode2.getTupleIds();
                result = tupleIds2;
                scanNode3.getTupleIds();
                result = tupleIds3;
                scanNode4.getTupleIds();
                result = tupleIds4;
                scanNode1.getOutputSmap();
                result = null;
                scanNode2.getOutputSmap();
                result = null;
                scanNode3.getOutputSmap();
                result = null;
                scanNode4.getOutputSmap();
                result = null;
                tableRef1.getUniqueAlias();
                result = "t1";
                tableRef2.getUniqueAlias();
                result = "t2";
                tableRef3.getUniqueAlias();
                result = "t3";
                tableRef4.getUniqueAlias();
                result = "t4";
                tableRef1.getJoinOp();
                result = JoinOperator.INNER_JOIN;
                tableRef2.getJoinOp();
                result = JoinOperator.INNER_JOIN;
                tableRef3.getJoinOp();
                result = JoinOperator.INNER_JOIN;
                tableRef4.getJoinOp();
                result = JoinOperator.INNER_JOIN;
            }
        };
        new MockUp<ExprSubstitutionMap>() {
            @Mock
            public ExprSubstitutionMap compose(ExprSubstitutionMap f, ExprSubstitutionMap g,
                                               Analyzer analyzer) {
                return exprSubstitutionMap;
            }

            @Mock
            public ExprSubstitutionMap combine(ExprSubstitutionMap f, ExprSubstitutionMap g) {
                return exprSubstitutionMap;
            }
        };

        SingleNodePlanner singleNodePlanner = new SingleNodePlanner(context);
        PlanNode cheapestJoinNode = Deencapsulation.invoke(singleNodePlanner, "createCheapestJoinPlan", analyzer, refPlans);
        Assert.assertEquals(2, cheapestJoinNode.getChildren().size());
        Assert.assertEquals(Lists.newArrayList(tupleId4, tupleId1, tupleId2, tupleId3),
                cheapestJoinNode.getTupleIds());
    }

    /*
    Query: select * from t3, t2, t1, t4 where (multi inner join condition)
    Original Query: select * from test3, test2, test1, test4
                    where test3.k1=test2.k1 and test2.k1=test1.k1 and test1.k1=test4.k1 and test4.k2=test2.k2
                          and test4.k2=test3.k2 and test3.k3=test1.k3;
    Expect: same as above
     */
    @Test
    public void testMultiInnerJoinMultiJoinPredicateReorder(@Injectable PlannerContext context,
                                                            @Injectable Analyzer analyzer,
                                                            @Injectable BaseTableRef tableRef1, @Injectable OlapScanNode scanNode1,
                                                            @Injectable BaseTableRef tableRef2, @Injectable OlapScanNode scanNode2,
                                                            @Injectable BaseTableRef tableRef3, @Injectable OlapScanNode scanNode3,
                                                            @Injectable BaseTableRef tableRef4, @Injectable OlapScanNode scanNode4,
                                                            @Injectable TupleDescriptor tupleDescriptor1,
                                                            @Injectable TupleDescriptor tupleDescriptor2,
                                                            @Injectable TupleDescriptor tupleDescriptor3,
                                                            @Injectable TupleDescriptor tupleDescriptor4,
                                                            @Injectable SlotDescriptor slotDescriptor1,
                                                            @Injectable SlotDescriptor slotDescriptor2,
                                                            @Injectable SlotDescriptor slotDescriptor3,
                                                            @Injectable SlotDescriptor slotDescriptor4,
                                                            @Injectable BinaryPredicate eqBinaryPredicate1,
                                                            @Injectable BinaryPredicate eqBinaryPredicate2,
                                                            @Injectable BinaryPredicate eqBinaryPredicate3,
                                                            @Injectable BinaryPredicate eqBinaryPredicate4,
                                                            @Injectable BinaryPredicate eqBinaryPredicate5,
                                                            @Injectable BinaryPredicate eqBinaryPredicate6,
                                                            @Injectable SlotRef eqT1Slot1,
                                                            @Injectable SlotRef eqT2Slot2,
                                                            @Injectable SlotRef eqT3Slot3,
                                                            @Injectable SlotRef eqT4Slot4,
                                                            @Tested ExprSubstitutionMap exprSubstitutionMap) {
        Pair<TableRef, PlanNode> pair1 = new Pair<>(tableRef1, scanNode1);
        Pair<TableRef, PlanNode> pair2 = new Pair<>(tableRef2, scanNode2);
        Pair<TableRef, PlanNode> pair3 = new Pair<>(tableRef3, scanNode3);
        Pair<TableRef, PlanNode> pair4 = new Pair<>(tableRef4, scanNode4);
        List<Pair<TableRef, PlanNode>> refPlans = Lists.newArrayList();
        refPlans.add(pair3);
        refPlans.add(pair2);
        refPlans.add(pair1);
        refPlans.add(pair4);

        TupleId tupleId1 = new TupleId(1);
        TupleId tupleId2 = new TupleId(2);
        TupleId tupleId3 = new TupleId(3);
        TupleId tupleId4 = new TupleId(4);
        List<TupleId> tupleIds1 = Lists.newArrayList(tupleId1);
        List<TupleId> tupleIds2 = Lists.newArrayList(tupleId2);
        List<TupleId> tupleIds3 = Lists.newArrayList(tupleId3);
        List<TupleId> tupleIds4 = Lists.newArrayList(tupleId4);
        List<TupleId> tupleIds41 = Lists.newArrayList(tupleId4, tupleId1);
        List<TupleId> tupleIds412 = Lists.newArrayList(tupleId4, tupleId1, tupleId2);

        new Expectations() {
            {
                tableRef1.isAnalyzed();
                result = true;
                tableRef2.isAnalyzed();
                result = true;
                tableRef3.isAnalyzed();
                result = true;
                tableRef4.isAnalyzed();
                result = true;
                scanNode1.getCardinality();
                result = 1;
                scanNode2.getCardinality();
                result = 2;
                scanNode3.getCardinality();
                result = 3;
                scanNode4.getCardinality();
                result = 4;
                tableRef1.getDesc();
                result = tupleDescriptor1;
                tupleDescriptor1.getMaterializedSlots();
                result = Lists.newArrayList(slotDescriptor1);
                tableRef2.getDesc();
                result = tupleDescriptor2;
                tupleDescriptor2.getMaterializedSlots();
                result = Lists.newArrayList(slotDescriptor2);
                tableRef3.getDesc();
                result = tupleDescriptor3;
                tupleDescriptor3.getMaterializedSlots();
                result = Lists.newArrayList(slotDescriptor3);

                // where t4.k1=t3.k1
                analyzer.getEqJoinConjuncts(tupleIds4, tupleIds3);
                result = Lists.newArrayList(eqBinaryPredicate1);
                eqBinaryPredicate1.getChild(0);
                result = eqT4Slot4;
                eqT4Slot4.isBoundByTupleIds(tupleIds4);
                result = true;
                eqBinaryPredicate1.getChild(1);
                result = eqT3Slot3;
                eqT3Slot3.isBoundByTupleIds(tupleIds3);
                result = true;
                // where t4.k1 = t2.k1
                analyzer.getEqJoinConjuncts(tupleIds4, tupleIds2);
                result = Lists.newArrayList(eqBinaryPredicate2);
                eqBinaryPredicate2.getChild(0);
                result = eqT4Slot4;
                eqBinaryPredicate2.getChild(1);
                result = eqT2Slot2;
                eqT2Slot2.isBoundByTupleIds(tupleIds2);
                result = true;
                // where t4.k1=t1.k1
                analyzer.getEqJoinConjuncts(tupleIds4, tupleIds1);
                result = Lists.newArrayList(eqBinaryPredicate3);
                eqBinaryPredicate3.getChild(0);
                result = eqT4Slot4;
                eqBinaryPredicate3.getChild(1);
                result = eqT1Slot1;
                eqT1Slot1.isBoundByTupleIds(tupleIds1);
                result = true;
                // where t1.k1=t3.k1
                analyzer.getEqJoinConjuncts(tupleIds41, tupleIds3);
                result = Lists.newArrayList(eqBinaryPredicate4);
                eqBinaryPredicate4.getChild(0);
                result = eqT1Slot1;
                eqT1Slot1.isBoundByTupleIds(tupleIds41);
                result = true;
                eqBinaryPredicate4.getChild(1);
                result = eqT3Slot3;
                // where t1.k1=t2.k1
                analyzer.getEqJoinConjuncts(tupleIds41, tupleIds2);
                result = Lists.newArrayList(eqBinaryPredicate5);
                eqBinaryPredicate5.getChild(0);
                result = eqT1Slot1;
                eqT1Slot1.isBoundByTupleIds(tupleIds41);
                result = true;
                eqBinaryPredicate5.getChild(1);
                result = eqT2Slot2;
                eqT2Slot2.isBoundByTupleIds(tupleIds2);
                result = true;
                // where t2.k1 = t3.k1
                analyzer.getEqJoinConjuncts(tupleIds412, tupleIds3);
                result = Lists.newArrayList(eqBinaryPredicate6);
                eqBinaryPredicate6.getChild(0);
                result = eqT2Slot2;
                eqT2Slot2.isBoundByTupleIds(tupleIds412);
                result = true;
                eqBinaryPredicate6.getChild(1);
                result = eqT3Slot3;
                eqT3Slot3.isBoundByTupleIds(tupleIds3);
                result = true;

                scanNode1.getTblRefIds();
                result = tupleIds1;
                scanNode2.getTblRefIds();
                result = tupleIds2;
                scanNode3.getTblRefIds();
                result = tupleIds3;
                scanNode4.getTblRefIds();
                result = tupleIds4;

                scanNode1.getTupleIds();
                result = tupleIds1;
                scanNode2.getTupleIds();
                result = tupleIds2;
                scanNode3.getTupleIds();
                result = tupleIds3;
                scanNode4.getTupleIds();
                result = tupleIds4;
                scanNode1.getOutputSmap();
                result = null;
                scanNode2.getOutputSmap();
                result = null;
                scanNode3.getOutputSmap();
                result = null;
                scanNode4.getOutputSmap();
                result = null;
                tableRef1.getUniqueAlias();
                result = "t1";
                tableRef2.getUniqueAlias();
                result = "t2";
                tableRef3.getUniqueAlias();
                result = "t3";
                tableRef4.getUniqueAlias();
                result = "t4";
                tableRef1.getJoinOp();
                result = JoinOperator.INNER_JOIN;
                tableRef2.getJoinOp();
                result = JoinOperator.INNER_JOIN;
                tableRef3.getJoinOp();
                result = JoinOperator.INNER_JOIN;
                tableRef4.getJoinOp();
                result = JoinOperator.INNER_JOIN;
            }
        };
        new MockUp<ExprSubstitutionMap>() {
            @Mock
            public ExprSubstitutionMap compose(ExprSubstitutionMap f, ExprSubstitutionMap g,
                                               Analyzer analyzer) {
                return exprSubstitutionMap;
            }

            @Mock
            public ExprSubstitutionMap combine(ExprSubstitutionMap f, ExprSubstitutionMap g) {
                return exprSubstitutionMap;
            }
        };

        SingleNodePlanner singleNodePlanner = new SingleNodePlanner(context);
        PlanNode cheapestJoinNode = Deencapsulation.invoke(singleNodePlanner, "createCheapestJoinPlan", analyzer, refPlans);
        Assert.assertEquals(2, cheapestJoinNode.getChildren().size());
        Assert.assertEquals(Lists.newArrayList(tupleId4, tupleId1, tupleId2, tupleId3),
                cheapestJoinNode.getTupleIds());
    }

    /*
    Query: select * from t3, t2, t1, t4 where (there is no predicate related to t2)
    Expect: t4(the largest), t1, t3, t2
    Round1: (t4,t3) pk (t4,t2) pk (t4,t1) => t4,t1
    Round2: ([t4,t1],t3) pk ([t4,t1],t2) => t4,t1,t3
    Round3: t4,t1,t3,t2 without pk
     */
    @Test
    public void testInnerPriorToCrossJoinReorder() {

    }

    /*
    Query: select * from t3, t2, t1, t4
    Original Query: select * from test3, test1, test2, test4;
    Expect: t4(the largest), t1, t2, t3 (from the smallest to the second largest)
     */
    @Test
    public void testMultiCrossJoinReorder() {

    }

    /*
    Test explicit cross join
    Query: select * from t3, t2, t1, t4 where ('>', '<' etc predicates)
    Original Query: select * from test3,test2,test1,test4
                    where test3.k1>test2.k1 and test2.k2<test1.k2 and test4.k3>=test1.k3;
    Expect: need same as implicit cross join
     */
    @Test
    public void testExplicitCrossJoinReorder() {

    }
}
