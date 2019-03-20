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
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class OptExpressionTest {
    private static final Logger LOG = LogManager.getLogger(OptExpression.class);

    @Test
    public void getExplainString() {
        // create a tree (internal(1), (internal-2, leaf-3), (internal-4, leaf-5))
        OptExpression expr = Utils.createUtInternal(
                Utils.createUtInternal(Utils.createUtLeaf()),
                Utils.createUtInternal(Utils.createUtLeaf()));
        final int exprLeftOpValue = ((OptLogicalUTLeafNode) expr.getInput(0).getInput(0).getOp()).getValue();
        final int exprRightOpValue = ((OptLogicalUTLeafNode) expr.getInput(1).getInput(0).getOp()).getValue();
        System.out.println(expr.getExplainString());
        assertEquals("LogicalUnitTestInternalNode\n" +
                "->  LogicalUnitTestInternalNode\n" +
                "    ->  LogicalUnitTestLeafNode (value=" + exprLeftOpValue + ")\n" +
                "->  LogicalUnitTestInternalNode\n" +
                "    ->  LogicalUnitTestLeafNode (value=" + exprRightOpValue + ")\n", expr.getExplainString());
        LOG.info("expr=\n{}", expr.getExplainString());
    }
}