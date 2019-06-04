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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

public class OptMemoTest {
    private static final Logger LOG = LogManager.getLogger(OptMemo.class);

    @Test
    public void copyIn() {
        // create a tree (internal(1), leaf(2), leaf(3))
        OptExpression expr = Utils.createUtInternal(
                Utils.createUtLeaf(),
                Utils.createUtLeaf());
        LOG.info("expr=\n{}", expr.getExplainString());
        OptMemo memo = new OptMemo();
        MultiExpression mExpr = memo.init(expr);
        Assert.assertNotNull(mExpr);
        LOG.info("mExpr=\n{}", mExpr.getExplainString());
        Assert.assertTrue(mExpr.getOp() instanceof OptLogicalUTInternalNode);
        Assert.assertEquals(2, mExpr.arity());
    }
}