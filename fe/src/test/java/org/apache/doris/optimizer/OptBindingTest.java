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

import static org.junit.Assert.*;

import org.apache.doris.optimizer.operator.OptPatternLeaf;
import org.apache.doris.optimizer.operator.OptUTLeafNode;
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
        OptExpression expr = Utils.createUtInternal(1,
                Utils.createUtLeaf(2),
                Utils.createUtLeaf(3));
        OptMemo memo = new OptMemo();
        MultiExpression mExpr = memo.copyIn(expr);
        LOG.info("mExpr=\n{}", mExpr.getExplainString());
        // 2. create pattern used to bind
        OptExpression pattern = Utils.createUtInternal(1,
                new OptExpression(new OptPatternLeaf()),
                new OptExpression(new OptPatternLeaf()));

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
        OptExpression expr = Utils.createUtInternal(1,
                Utils.createUtLeaf(2),
                Utils.createUtLeaf(3));
        OptMemo memo = new OptMemo();
        MultiExpression mExpr = memo.copyIn(expr);
        // 2. add another to this group
        {
            OptExpression expr2 = Utils.createUtLeaf(4);
            memo.copyIn(mExpr.getInput(0), expr2);
        }
        {
            OptExpression expr2 = Utils.createUtLeaf(5);
            memo.copyIn(mExpr.getInput(1), expr2);
        }
        LOG.info("mExpr=\n{}", mExpr.getExplainString());
        // 2. create pattern used to bind
        OptExpression pattern = Utils.createUtInternal(1,
                new OptExpression(new OptUTLeafNode(1)),
                new OptExpression(new OptPatternLeaf()));

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
        OptExpression expr = Utils.createUtInternal(1,
                Utils.createUtLeaf(2),
                Utils.createUtLeaf(3));
        OptMemo memo = new OptMemo();
        MultiExpression mExpr = memo.copyIn(expr);
        // 2. add another to this group
        {
            OptExpression expr2 = Utils.createUtLeaf(4);
            memo.copyIn(mExpr.getInput(0), expr2);
        }
        {
            OptExpression expr2 = Utils.createUtLeaf(5);
            memo.copyIn(mExpr.getInput(1), expr2);
        }
        LOG.info("mExpr=\n{}", mExpr.getExplainString());
        // 2. create pattern used to bind
        OptExpression pattern = Utils.createUtInternal(1,
                new OptExpression(new OptUTLeafNode(1)),
                new OptExpression(new OptUTLeafNode(1)));

        OptExpression bind = OptBinding.bind(pattern, mExpr, null);
        Assert.assertNotNull(bind);
        LOG.info("bound=\n{}", bind.getExplainString());
        Assert.assertFalse(bind == expr);
        Assert.assertNotNull(bind.getMExpr());
        Assert.assertNotNull(bind.getInput(0).getMExpr());
        Assert.assertNotNull(bind.getInput(1).getMExpr());
        Assert.assertEquals(2, ((OptUTLeafNode) bind.getInput(0).getOp()).getValue());
        Assert.assertEquals(3, ((OptUTLeafNode) bind.getInput(1).getOp()).getValue());

        bind = OptBinding.bind(pattern, mExpr, bind);
        Assert.assertNotNull(bind);
        Assert.assertEquals(4, ((OptUTLeafNode) bind.getInput(0).getOp()).getValue());
        Assert.assertEquals(3, ((OptUTLeafNode) bind.getInput(1).getOp()).getValue());

        bind = OptBinding.bind(pattern, mExpr, bind);
        Assert.assertNotNull(bind);
        Assert.assertEquals(2, ((OptUTLeafNode) bind.getInput(0).getOp()).getValue());
        Assert.assertEquals(5, ((OptUTLeafNode) bind.getInput(1).getOp()).getValue());

        bind = OptBinding.bind(pattern, mExpr, bind);
        Assert.assertNotNull(bind);
        Assert.assertEquals(4, ((OptUTLeafNode) bind.getInput(0).getOp()).getValue());
        Assert.assertEquals(5, ((OptUTLeafNode) bind.getInput(1).getOp()).getValue());

        bind = OptBinding.bind(pattern, mExpr, bind);
        Assert.assertNull(bind);
    }

    @Test
    public void testBindDifferentOp() {
        // 1. create a
        // create a tree (internal(1), leaf(2), leaf(3))
        OptExpression expr = Utils.createUtInternal(1,
                Utils.createUtLeaf(2),
                Utils.createUtLeaf(3));
        OptMemo memo = new OptMemo();
        MultiExpression mExpr = memo.copyIn(expr);
        // 2. create pattern used to bind
        OptExpression pattern = Utils.createUtInternal(1,
                new OptExpression(new OptPatternLeaf()));

        // Because pattern has only one input, not equal with expression's, so can't match
        OptExpression bind = OptBinding.bind(pattern, mExpr, null);
        Assert.assertNull(bind);
    }

}