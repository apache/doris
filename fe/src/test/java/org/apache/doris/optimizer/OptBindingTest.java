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

import org.apache.doris.optimizer.operator.OptPatternLeaf;
import org.apache.doris.optimizer.operator.OptLogicalUTLeafNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

public class OptBindingTest {
    private static final Logger LOG = LogManager.getLogger(OptBinding.class);

    @Test
    public void testBind() {
        // 1. create a
        // create a tree (internal(1), leaf(2), leaf(3))
        OptExpression expr = Utils.createUtInternal(Utils.createUtLeaf(),
                Utils.createUtLeaf());
        OptMemo memo = new OptMemo();
        MultiExpression mExpr = memo.init(expr);
        LOG.info("mExpr=\n{}", mExpr.getExplainString());
        // 2. create pattern used to bind
        OptExpression pattern = Utils.createUtInternal(
                OptExpression.create(new OptPatternLeaf()),
                OptExpression.create(new OptPatternLeaf()));

        OptExpression bind = OptBinding.bind(pattern, mExpr, null);
        Assert.assertNotNull(bind);
        LOG.info("bound=\n{}", bind.getExplainString());
        Assert.assertFalse(bind == expr);
        Assert.assertNotNull(bind.getMExpr());
        Assert.assertNotNull(bind.getInput(0).getMExpr());
        Assert.assertNotNull(bind.getInput(1).getMExpr());

        bind = OptBinding.bind(pattern, mExpr, bind);
        Assert.assertNull(bind);
    }

    @Test
    public void testBindMultiTimes() {
        // 1. create a
        // create a tree (internal(1), leaf(2), leaf(3))
        OptExpression expr = Utils.createUtInternal(
                Utils.createUtLeaf(),
                Utils.createUtLeaf());
        OptMemo memo = new OptMemo();
        MultiExpression mExpr = memo.init(expr);
        // 2. add another to this group
        {
            OptExpression expr2 = Utils.createUtLeaf();
            memo.copyIn(mExpr.getInput(0), expr2);
        }
        {
            OptExpression expr2 = Utils.createUtLeaf();
            memo.copyIn(mExpr.getInput(1), expr2);
        }
        LOG.info("mExpr=\n{}", mExpr.getExplainString());
        // 2. create pattern used to bind
        OptExpression pattern = Utils.createUtInternal(
                OptExpression.create(new OptLogicalUTLeafNode()),
                OptExpression.create(new OptPatternLeaf()));

        OptExpression bind = OptBinding.bind(pattern, mExpr, null);
        Assert.assertNotNull(bind);
        LOG.info("bound=\n{}", bind.getExplainString());
        Assert.assertFalse(bind == expr);
        Assert.assertNotNull(bind.getMExpr());
        Assert.assertNotNull(bind.getInput(0).getMExpr());
        Assert.assertNotNull(bind.getInput(1).getMExpr());

        bind = OptBinding.bind(pattern, mExpr, bind);
        Assert.assertNotNull(bind);

        bind = OptBinding.bind(pattern, mExpr, bind);
        Assert.assertNull(bind);
    }

    @Test
    public void testBind4Times() {
        // 1. create a
        // create a tree (internal(1), leaf(2), leaf(3))
        OptExpression expr = Utils.createUtInternal(
                Utils.createUtLeaf(),  // 1
                Utils.createUtLeaf()); // 2
        OptMemo memo = new OptMemo();
        MultiExpression mExpr = memo.init(expr);
        // 2. add another to this group
        OptExpression expr2 = Utils.createUtLeaf(); // 3
        memo.copyIn(mExpr.getInput(0), expr2);

        OptExpression expr3 = Utils.createUtLeaf(); // 4
        memo.copyIn(mExpr.getInput(1), expr3);

        LOG.info("mExpr=\n{}", mExpr.getExplainString());
        // 2. create pattern used to bind
        OptExpression pattern = Utils.createUtInternal(
                OptExpression.create(new OptLogicalUTLeafNode()),
                OptExpression.create(new OptLogicalUTLeafNode()));

        OptExpression bind = OptBinding.bind(pattern, mExpr, null);
        Assert.assertNotNull(bind);
        LOG.info("bound=\n{}", bind.getExplainString());
        Assert.assertFalse(bind == expr);
        Assert.assertNotNull(bind.getMExpr());
        Assert.assertNotNull(bind.getInput(0).getMExpr());
        Assert.assertNotNull(bind.getInput(1).getMExpr());

        final int exprLeftOpValue = ((OptLogicalUTLeafNode)expr.getInput(0).getOp()).getValue();
        final int exprRightOpValue = ((OptLogicalUTLeafNode)expr.getInput(1).getOp()).getValue();
        final int expr2OpValue = ((OptLogicalUTLeafNode)expr2.getOp()).getValue();
        final int expr3OpValue = ((OptLogicalUTLeafNode)expr3.getOp()).getValue();

        Assert.assertEquals(exprLeftOpValue,
                ((OptLogicalUTLeafNode) bind.getInput(0).getOp()).getValue());
        Assert.assertEquals(exprRightOpValue,
                ((OptLogicalUTLeafNode) bind.getInput(1).getOp()).getValue());

        bind = OptBinding.bind(pattern, mExpr, bind);
        Assert.assertNotNull(bind);
        Assert.assertEquals(expr2OpValue, ((OptLogicalUTLeafNode) bind.getInput(0).getOp()).getValue());
        Assert.assertEquals(exprRightOpValue, ((OptLogicalUTLeafNode) bind.getInput(1).getOp()).getValue());

        bind = OptBinding.bind(pattern, mExpr, bind);
        Assert.assertNotNull(bind);
        Assert.assertEquals(exprLeftOpValue, ((OptLogicalUTLeafNode) bind.getInput(0).getOp()).getValue());
        Assert.assertEquals(expr3OpValue, ((OptLogicalUTLeafNode) bind.getInput(1).getOp()).getValue());

        bind = OptBinding.bind(pattern, mExpr, bind);
        Assert.assertNotNull(bind);
        Assert.assertEquals(expr2OpValue, ((OptLogicalUTLeafNode) bind.getInput(0).getOp()).getValue());
        Assert.assertEquals(expr3OpValue, ((OptLogicalUTLeafNode) bind.getInput(1).getOp()).getValue());

        bind = OptBinding.bind(pattern, mExpr, bind);
        Assert.assertNull(bind);
    }

    @Test
    public void testBindDifferentOp() {
        // 1. create a
        // create a tree (internal(1), leaf(2), leaf(3))
        OptExpression expr = Utils.createUtInternal(
                Utils.createUtLeaf(),
                Utils.createUtLeaf());
        OptMemo memo = new OptMemo();
        MultiExpression mExpr = memo.init(expr);
        // 2. create pattern used to bind
        OptExpression pattern = Utils.createUtInternal(
                OptExpression.create(new OptPatternLeaf()));

        // Because pattern has only one input, not equal with expression's, so can't match
        OptExpression bind = OptBinding.bind(pattern, mExpr, null);
        Assert.assertNull(bind);
    }

}