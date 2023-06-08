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

package org.apache.doris.nereids.trees.plans;

import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.rules.expression.rules.FunctionBinder;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.scalar.HoursAdd;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Now;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.plans.commands.CreateFunctionCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CreateFunctionCommandTest extends TestWithFeService {
    @Test
    public void testSimpleAliasFunction() {
        String sql = "create alias function f1(int) with parameter(num) as hours_add(now(3), num)";
        CreateFunctionCommand createFunction = ((CreateFunctionCommand) new NereidsParser().parseSingle(sql));

        LogicalOlapScan scan = PlanConstructor.newLogicalOlapScan(0, "table", 0);
        
        Expression unboundOriginalFunction = createFunction.getOriginalFunction();
        
        Expression boundFunction = FunctionBinder.INSTANCE.visit(unboundOriginalFunction,
                new ExpressionRewriteContext(MemoTestUtils.createCascadesContext(connectContext, scan)));
        Assertions.assertEquals(new HoursAdd(new Now(new TinyIntLiteral((byte) 3)), new UnboundSlot("num")),
                boundFunction);
    }
}
