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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

public class OptExpressionTest {
    private static final Logger LOG = LogManager.getLogger(OptExpression.class);
    @Test
    public void getExplainString() {
        // create a tree (internal(1), (internal-2, leaf-3), (internal-4, leaf-5))
        OptExpression expr = Utils.createUtInternal(1,
                Utils.createUtInternal(2, Utils.createUtLeaf(3)),
                Utils.createUtInternal(4, Utils.createUtLeaf(5)));

        assertEquals("UnitTestInternalNode (value=1)\n" +
                "->  UnitTestInternalNode (value=2)\n" +
                "    ->  UnitTestLeafNode (value=3)\n" +
                "->  UnitTestInternalNode (value=4)\n" +
                "    ->  UnitTestLeafNode (value=5)\n", expr.getExplainString());
        LOG.info("expr=\n{}", expr.getExplainString());
    }
}