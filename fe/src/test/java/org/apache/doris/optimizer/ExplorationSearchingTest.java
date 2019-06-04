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

package org.apache.doris.optimizer;

import com.google.common.collect.Lists;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.optimizer.base.*;
import org.apache.doris.optimizer.cost.SimpleCostModel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class ExplorationSearchingTest {
    private static final Logger LOG = LogManager.getLogger(ExplorationSearchingTest.class);
    private List<OptColumnRef> outputColumns;
    private OptColumnRefFactory columnRefFactory;

    @Before
    public void init() {
        outputColumns = Lists.newArrayList();
        final OptColumnRef column1 = new OptColumnRef(1, ScalarType.TINYINT, "t1");
        outputColumns.add(column1);
        columnRefFactory = new OptColumnRefFactory();
    }

    @Test
    public void testSingleJoin() {
        final OptExpression topJoin = Utils.createUtInternal(
                Utils.createUtLeaf(),
                Utils.createUtLeaf());
        LOG.info("init expr=\n{}", topJoin.getExplainString());
        final SearchVariable variable = new SearchVariable();
        variable.setExecuteOptimization(false);
        final QueryContext queryContext = new QueryContext(topJoin,
                RequiredPhysicalProperty.createTestProperty(), outputColumns, variable);
        final Optimizer optimizer = new Optimizer(queryContext, new SimpleCostModel(), columnRefFactory);
        optimizer.addRule(OptUTInternalCommutativityRule.INSTANCE);
        optimizer.addRule(OptUTInternalAssociativityRule.INSTANCE);
        optimizer.optimize();

        final UTMemoStatus utMemoChecker = new UTMemoStatus(2, optimizer.getMemo());
        utMemoChecker.checkExploreStatus();

        Assert.assertEquals(3, optimizer.getMemo().getGroups().size());
        Assert.assertEquals(4, optimizer.getMemo().getMExprs().size());
        Assert.assertEquals(1, optimizer.getMemo().getGroups().get(0).getMultiExpressions().size());
        Assert.assertEquals(1, optimizer.getMemo().getGroups().get(1).getMultiExpressions().size());
        Assert.assertEquals(2, optimizer.getMemo().getGroups().get(2).getMultiExpressions().size());
    }

    @Test
    public void testTwoJoin() {
        final OptExpression bottomJoin = Utils.createUtInternal(
                Utils.createUtLeaf(),
                Utils.createUtLeaf());
        final OptExpression topJoin = Utils.createUtInternal(
                bottomJoin,
                Utils.createUtLeaf());
        LOG.info("init expr=\n{}", topJoin.getExplainString());

        final SearchVariable variable = new SearchVariable();
        variable.setExecuteOptimization(false);
        final QueryContext queryContext = new QueryContext(topJoin,
                RequiredPhysicalProperty.createTestProperty(), outputColumns, variable);
        final Optimizer optimizer = new Optimizer(queryContext, new SimpleCostModel(), columnRefFactory);
        optimizer.addRule(OptUTInternalCommutativityRule.INSTANCE);
        optimizer.addRule(OptUTInternalAssociativityRule.INSTANCE);
        optimizer.optimize();

        final UTMemoStatus utMemoChecker = new UTMemoStatus(3, optimizer.getMemo());
        utMemoChecker.checkExploreStatus();

        Assert.assertEquals(7, optimizer.getMemo().getGroups().size());
        Assert.assertEquals(15, optimizer.getMemo().getMExprs().size());
        Assert.assertEquals(1, optimizer.getMemo().getGroups().get(0).getMultiExpressions().size());
        Assert.assertEquals(1, optimizer.getMemo().getGroups().get(1).getMultiExpressions().size());
        Assert.assertEquals(2, optimizer.getMemo().getGroups().get(2).getMultiExpressions().size());
        Assert.assertEquals(1, optimizer.getMemo().getGroups().get(3).getMultiExpressions().size());
        Assert.assertEquals(6, optimizer.getMemo().getGroups().get(4).getMultiExpressions().size());
        Assert.assertEquals(2, optimizer.getMemo().getGroups().get(5).getMultiExpressions().size());
        Assert.assertEquals(2, optimizer.getMemo().getGroups().get(6).getMultiExpressions().size());
    }

    @Test
    public void testThreeJoin() {
        final OptExpression bottomJoin = Utils.createUtInternal(
                Utils.createUtLeaf(),
                Utils.createUtLeaf());
        final OptExpression bottom2Join = Utils.createUtInternal(
                bottomJoin,
                Utils.createUtLeaf());
        final OptExpression topJoin = Utils.createUtInternal(
                bottom2Join,
                Utils.createUtLeaf());
        LOG.info("init expr=\n{}", topJoin.getExplainString());

        final SearchVariable variable = new SearchVariable();
        variable.setExecuteOptimization(false);
        final QueryContext queryContext = new QueryContext(topJoin,
                RequiredPhysicalProperty.createTestProperty(), outputColumns, variable);
        final Optimizer optimizer = new Optimizer(queryContext, new SimpleCostModel(), columnRefFactory);
        optimizer.addRule(OptUTInternalCommutativityRule.INSTANCE);
        optimizer.addRule(OptUTInternalAssociativityRule.INSTANCE);
        optimizer.optimize();

        final UTMemoStatus utMemoChecker = new UTMemoStatus(4, optimizer.getMemo());
        utMemoChecker.checkExploreStatus();
    }

    @Test
    public void testFourJoin() {
        final OptExpression bottomJoin = Utils.createUtInternal(
                Utils.createUtLeaf(),
                Utils.createUtLeaf());
        final OptExpression bottom2Join = Utils.createUtInternal(
                bottomJoin,
                Utils.createUtLeaf());
        final OptExpression bottom3Join = Utils.createUtInternal(
                bottom2Join,
                Utils.createUtLeaf());
        final OptExpression topJoin = Utils.createUtInternal(
                bottom3Join,
                Utils.createUtLeaf());
        LOG.info("init expr=\n{}", topJoin.getExplainString());

        final SearchVariable variable = new SearchVariable();
        variable.setExecuteOptimization(false);
        final QueryContext queryContext = new QueryContext(topJoin,
                RequiredPhysicalProperty.createTestProperty(), outputColumns, variable);
        final Optimizer optimizer = new Optimizer(queryContext, new SimpleCostModel(), columnRefFactory);
        optimizer.addRule(OptUTInternalCommutativityRule.INSTANCE);
        optimizer.addRule(OptUTInternalAssociativityRule.INSTANCE);
        optimizer.optimize();

        final UTMemoStatus utMemoChecker = new UTMemoStatus(5, optimizer.getMemo());
        utMemoChecker.checkExploreStatus();
    }

    @Test
    public void testSevenJoin() {
        final OptExpression bottomJoin = Utils.createUtInternal(
                Utils.createUtLeaf(),
                Utils.createUtLeaf());
        final OptExpression bottom2Join = Utils.createUtInternal(
                bottomJoin,
                Utils.createUtLeaf());
        final OptExpression bottom3Join = Utils.createUtInternal(
                bottom2Join,
                Utils.createUtLeaf());
        final OptExpression bottom4Join = Utils.createUtInternal(
                bottom3Join,
                Utils.createUtLeaf());
        final OptExpression bottom5Join = Utils.createUtInternal(
                bottom4Join,
                Utils.createUtLeaf());
        final OptExpression bottom6Join = Utils.createUtInternal(
                bottom5Join,
                Utils.createUtLeaf());
        final OptExpression topJoin = Utils.createUtInternal(
                bottom6Join,
                Utils.createUtLeaf());
        LOG.info("init expr=\n{}", topJoin.getExplainString());

        final SearchVariable variable = new SearchVariable();
        variable.setExecuteOptimization(false);
        final QueryContext queryContext = new QueryContext(topJoin,
                RequiredPhysicalProperty.createTestProperty(), outputColumns, variable);
        final Optimizer optimizer = new Optimizer(queryContext, new SimpleCostModel(), columnRefFactory);
        optimizer.addRule(OptUTInternalCommutativityRule.INSTANCE);
        optimizer.addRule(OptUTInternalAssociativityRule.INSTANCE);
        optimizer.optimize();

        final UTMemoStatus utMemoChecker = new UTMemoStatus(8, optimizer.getMemo());
        utMemoChecker.checkExploreStatus();
    }

    @Test
    public void testEightJoin() {
        final OptExpression bottomJoin = Utils.createUtInternal(
                Utils.createUtLeaf(),
                Utils.createUtLeaf());
        final OptExpression bottom2Join = Utils.createUtInternal(
                bottomJoin,
                Utils.createUtLeaf());
        final OptExpression bottom3Join = Utils.createUtInternal(
                bottom2Join,
                Utils.createUtLeaf());
        final OptExpression bottom4Join = Utils.createUtInternal(
                bottom3Join,
                Utils.createUtLeaf());
        final OptExpression bottom5Join = Utils.createUtInternal(
                bottom4Join,
                Utils.createUtLeaf());
        final OptExpression bottom6Join = Utils.createUtInternal(
                bottom5Join,
                Utils.createUtLeaf());
        final OptExpression bottom7Join = Utils.createUtInternal(
                bottom6Join,
                Utils.createUtLeaf());
        final OptExpression topJoin = Utils.createUtInternal(
                bottom7Join,
                Utils.createUtLeaf());
        LOG.info("init expr=\n{}", topJoin.getExplainString());

        final SearchVariable variable = new SearchVariable();
        variable.setExecuteOptimization(false);
        final QueryContext queryContext = new QueryContext(topJoin,
                RequiredPhysicalProperty.createTestProperty(), outputColumns, variable);
        final Optimizer optimizer = new Optimizer(queryContext, new SimpleCostModel(), columnRefFactory);
        optimizer.addRule(OptUTInternalCommutativityRule.INSTANCE);
        optimizer.addRule(OptUTInternalAssociativityRule.INSTANCE);
        optimizer.optimize();

        final UTMemoStatus utMemoChecker = new UTMemoStatus(9, optimizer.getMemo());
        utMemoChecker.checkExploreStatus();
    }
//
//    @Test
//    public void testNineJoin() {
//        final OptExpression bottomJoin = Utils.createUtInternal(
//                Utils.createUtLeaf(),
//                Utils.createUtLeaf());
//        final OptExpression bottom2Join = Utils.createUtInternal(
//                bottomJoin,
//                Utils.createUtLeaf());
//        final OptExpression bottom3Join = Utils.createUtInternal(
//                bottom2Join,
//                Utils.createUtLeaf());
//        final OptExpression bottom4Join = Utils.createUtInternal(
//                bottom3Join,
//                Utils.createUtLeaf());
//        final OptExpression bottom5Join = Utils.createUtInternal(
//                bottom4Join,
//                Utils.createUtLeaf());
//        final OptExpression bottom6Join = Utils.createUtInternal(
//                bottom5Join,
//                Utils.createUtLeaf());
//        final OptExpression bottom7Join = Utils.createUtInternal(
//                bottom6Join,
//                Utils.createUtLeaf());
//        final OptExpression bottom8Join = Utils.createUtInternal(
//                bottom7Join,
//                Utils.createUtLeaf());
//        final OptExpression topJoin = Utils.createUtInternal(
//                bottom8Join,
//                Utils.createUtLeaf());
//        LOG.info("init expr=\n{}", topJoin.getExplainString());
//
//        final SearchVariable variable = new SearchVariable();
//        variable.setExecuteOptimization(false);
//        final QueryContext queryContext = new QueryContext(topJoin,
//                RequiredPhysicalProperty.createTestProperty(), outputColumns, variable);
//        final Optimizer optimizer = new Optimizer(queryContext, new SimpleCostModel(), columnRefFactory);
//        optimizer.addRule(OptUTInternalCommutativityRule.INSTANCE);
//        optimizer.addRule(OptUTInternalAssociativityRule.INSTANCE);
//        optimizer.optimize();
//
//        Assert.assertTrue(MemoStatus.checkStatusForExplore(optimizer.getMemo()));
//        Assert.assertEquals(1023, optimizer.getMemo().getGroups().size());
//        Assert.assertEquals(57012, optimizer.getMemo().getMExprs().size());
//        final long planCount = PlansStatus.getLogicalPlanCount(optimizer.getRoot());
//        LOG.info("Plans:" + planCount);
//        Assert.assertEquals(17643225600L, planCount);
//    }
}
