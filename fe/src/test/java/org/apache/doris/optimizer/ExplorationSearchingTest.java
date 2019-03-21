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
import org.apache.doris.optimizer.base.SearchVariable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class ExplorationSearchingTest {
    private static final Logger LOG = LogManager.getLogger(ExplorationSearchingTest.class);

    @Test
    public void testSingleJoin() {
        final OptExpression topJoin = Utils.createUtInternal(
                Utils.createUtLeaf(),
                Utils.createUtLeaf());
        LOG.info("init expr=\n{}", topJoin.getExplainString());
        final SearchVariable variable = new SearchVariable();
        variable.setExecuteOptimization(false);
        final Optimizer optimizer = new Optimizer(topJoin, variable);
        optimizer.addRule(OptUTInternalCommutativityRule.INSTANCE);
        optimizer.addRule(OptUTInternalAssociativityRule.INSTANCE);
        optimizer.optimize();

        Assert.assertTrue(checkMemoStatusAfterOptimization(optimizer.getMemo()));
        Assert.assertEquals(3, optimizer.getMemo().getGroups().size());
        Assert.assertEquals(4, optimizer.getMemo().getMExprs().size());
        Assert.assertEquals(1, optimizer.getMemo().getGroups().get(0).getMultiExpressions().size());
        Assert.assertEquals(1, optimizer.getMemo().getGroups().get(1).getMultiExpressions().size());
        Assert.assertEquals(2, optimizer.getMemo().getGroups().get(2).getMultiExpressions().size());
        final long planCount = getPlanCount(optimizer.getRoot());
        LOG.info("Plans:" + planCount);
        Assert.assertEquals(2, planCount);

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
        final Optimizer optimizer = new Optimizer(topJoin, variable);
        optimizer.addRule(OptUTInternalCommutativityRule.INSTANCE);
        optimizer.addRule(OptUTInternalAssociativityRule.INSTANCE);
        optimizer.optimize();

        Assert.assertTrue(checkMemoStatusAfterOptimization(optimizer.getMemo()));
        Assert.assertEquals(7, optimizer.getMemo().getGroups().size());
        Assert.assertEquals(15, optimizer.getMemo().getMExprs().size());
        Assert.assertEquals(1, optimizer.getMemo().getGroups().get(0).getMultiExpressions().size());
        Assert.assertEquals(1, optimizer.getMemo().getGroups().get(1).getMultiExpressions().size());
        Assert.assertEquals(2, optimizer.getMemo().getGroups().get(2).getMultiExpressions().size());
        Assert.assertEquals(1, optimizer.getMemo().getGroups().get(3).getMultiExpressions().size());
        Assert.assertEquals(6, optimizer.getMemo().getGroups().get(4).getMultiExpressions().size());
        Assert.assertEquals(2, optimizer.getMemo().getGroups().get(5).getMultiExpressions().size());
        Assert.assertEquals(2, optimizer.getMemo().getGroups().get(6).getMultiExpressions().size());
        final long planCount = getPlanCount(optimizer.getRoot());
        LOG.info("Plans:" + planCount);
        Assert.assertEquals(12, planCount);
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
        final Optimizer optimizer = new Optimizer(topJoin, variable);
        optimizer.addRule(OptUTInternalCommutativityRule.INSTANCE);
        optimizer.addRule(OptUTInternalAssociativityRule.INSTANCE);
        optimizer.optimize();

        Assert.assertTrue(checkMemoStatusAfterOptimization(optimizer.getMemo()));
        Assert.assertEquals(15, optimizer.getMemo().getGroups().size());
        Assert.assertEquals(54, optimizer.getMemo().getMExprs().size());
        final long planCount = getPlanCount(optimizer.getRoot());
        LOG.info("Plans:" + planCount);
        Assert.assertEquals(120, planCount);
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
        final Optimizer optimizer = new Optimizer(topJoin, variable);
        optimizer.addRule(OptUTInternalCommutativityRule.INSTANCE);
        optimizer.addRule(OptUTInternalAssociativityRule.INSTANCE);
        optimizer.optimize();

        Assert.assertTrue(checkMemoStatusAfterOptimization(optimizer.getMemo()));
        Assert.assertEquals(31, optimizer.getMemo().getGroups().size());
        Assert.assertEquals(185, optimizer.getMemo().getMExprs().size());
        final long planCount = getPlanCount(optimizer.getRoot());
        LOG.info("Plans:" + planCount);
        Assert.assertEquals(1680, planCount);
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
        final Optimizer optimizer = new Optimizer(topJoin, variable);
        optimizer.addRule(OptUTInternalCommutativityRule.INSTANCE);
        optimizer.addRule(OptUTInternalAssociativityRule.INSTANCE);
        optimizer.optimize();

        Assert.assertTrue(checkMemoStatusAfterOptimization(optimizer.getMemo()));
        Assert.assertEquals(511, optimizer.getMemo().getGroups().size());
        Assert.assertEquals(18669, optimizer.getMemo().getMExprs().size());
        final long planCount = getPlanCount(optimizer.getRoot());
        LOG.info("Plans:" + planCount);
        Assert.assertEquals(518918400L, planCount);
    }

    @Test
    public void testNineJoin() {
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
        final OptExpression bottom8Join = Utils.createUtInternal(
                bottom7Join,
                Utils.createUtLeaf());
        final OptExpression topJoin = Utils.createUtInternal(
                bottom8Join,
                Utils.createUtLeaf());
        LOG.info("init expr=\n{}", topJoin.getExplainString());

        final SearchVariable variable = new SearchVariable();
        variable.setExecuteOptimization(false);
        final Optimizer optimizer = new Optimizer(topJoin, variable);
        optimizer.addRule(OptUTInternalCommutativityRule.INSTANCE);
        optimizer.addRule(OptUTInternalAssociativityRule.INSTANCE);
        optimizer.optimize();

        Assert.assertTrue(checkMemoStatusAfterOptimization(optimizer.getMemo()));
        Assert.assertEquals(1023, optimizer.getMemo().getGroups().size());
        Assert.assertEquals(57012, optimizer.getMemo().getMExprs().size());
        final long planCount = getPlanCount(optimizer.getRoot());
        LOG.info("Plans:" + planCount);
        Assert.assertEquals(17643225600L, planCount);
    }

    private boolean checkMemoStatusAfterOptimization(OptMemo memo) {
        final List<OptGroup> groups = memo.getGroups();
        LOG.info("Groups:" + memo.getGroups().size() + " MultExpressions:" + memo.getMExprs().size());
        final List<OptGroup> unoptimizedGroups = Lists.newArrayList();
        final List<MultiExpression> unimplementedMExprs = Lists.newArrayList();
        for (OptGroup group : groups) {
            if (!group.isOptimized()) {
                unoptimizedGroups.add(group);
            }
            for (MultiExpression mExpr : group.getMultiExpressions()) {
                if (!mExpr.isImplemented()) {
                    unimplementedMExprs.add(mExpr);
                }
            }
        }

        final StringBuilder unoptimizedGroupsLog = new StringBuilder("Unoptimized Group ids:");
        for (OptGroup group : unoptimizedGroups) {
            unoptimizedGroupsLog.append(group.getId()).append(" status:")
                    .append(group.getStatus()).append(" MultiExpression ids:");
            for (MultiExpression mExpr : group.getMultiExpressions()) {
                unoptimizedGroupsLog.append(mExpr.getId()).append(" status:").append(mExpr.getStatus()).append(", ");
            }
        }
        LOG.info(unoptimizedGroupsLog);

        final StringBuilder unimplementedMExprLog = new StringBuilder("Unimplemented MultiExpression ids:");
        for (MultiExpression mExpr : unimplementedMExprs) {
            unimplementedMExprLog.append(mExpr.getOp()).append(" ").append(mExpr.getId()).append(", ");
        }
        LOG.info(unimplementedMExprLog);

        return unoptimizedGroups.size() == 0 && unimplementedMExprs.size() == 0 ? true : false;
    }

    // This will take long time for large dag.
    private long getPlanCount(OptGroup group) {
        long planCountTotal = 0;
        for (MultiExpression mExpr : group.getMultiExpressions()) {
            long planCount = 1;
            for (OptGroup child : mExpr.getInputs()) {
                planCount *= getPlanCount(child);
            }
            planCountTotal += planCount;
        }
        return planCountTotal;
    }
}
