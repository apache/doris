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

package org.apache.doris.analysis;

import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class LambdaFunctionExprTest {
    @Test
    public void testSerializesArgumentNames() {
        SlotRef slotX = new SlotRef(null, "x");
        SlotRef slotY = new SlotRef(null, "yy");
        LambdaFunctionExpr expr = new LambdaFunctionExpr(
                new IntLiteral(1L), Lists.newArrayList("x", "yy"),
                Lists.newArrayList(slotX, slotY));

        TExprNode node = expr.treeToThrift().getNodes().get(0);

        Assertions.assertEquals(TExprNodeType.LAMBDA_FUNCTION_EXPR, node.node_type);
        Assertions.assertTrue(node.isSetLambdaArgumentNames());
        Assertions.assertEquals(Lists.newArrayList("x", "yy"), node.getLambdaArgumentNames());
    }
}
