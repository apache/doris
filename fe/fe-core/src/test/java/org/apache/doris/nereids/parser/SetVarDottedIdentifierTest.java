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

package org.apache.doris.nereids.parser;

import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.trees.plans.commands.SetOptionsCommand;
import org.apache.doris.nereids.trees.plans.commands.info.SetSessionVarOp;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SetVarDottedIdentifierTest {

    private Expression getExpression(String sql) {
        NereidsParser parser = new NereidsParser();
        LogicalPlan plan = parser.parseSingle(sql);
        SetOptionsCommand command = (SetOptionsCommand) plan;
        SetSessionVarOp varOp = (SetSessionVarOp) command.getSetVarOps().get(0);
        return varOp.getExpression();
    }

    @Test
    public void testSimpleIdentifierIsUnboundSlot() {
        Expression expr = getExpression("SET query_timeout = aaa");
        Assertions.assertTrue(expr instanceof UnboundSlot);
        UnboundSlot slot = (UnboundSlot) expr;
        Assertions.assertEquals(1, slot.getNameParts().size());
        Assertions.assertEquals("aaa", slot.getNameParts().get(0));
    }

    @Test
    public void testDottedIdentifierIsUnboundSlotWithTwoParts() {
        Expression expr = getExpression("SET query_timeout = aaa.bbb");
        Assertions.assertTrue(expr instanceof UnboundSlot);
        UnboundSlot slot = (UnboundSlot) expr;
        Assertions.assertEquals(2, slot.getNameParts().size());
        Assertions.assertEquals("aaa", slot.getNameParts().get(0));
        Assertions.assertEquals("bbb", slot.getNameParts().get(1));
    }

    @Test
    public void testMultiDottedIdentifierIsUnboundSlotWithThreeParts() {
        Expression expr = getExpression("SET query_timeout = aaa.bbb.ccc");
        Assertions.assertTrue(expr instanceof UnboundSlot);
        UnboundSlot slot = (UnboundSlot) expr;
        Assertions.assertEquals(3, slot.getNameParts().size());
        Assertions.assertEquals("aaa", slot.getNameParts().get(0));
        Assertions.assertEquals("bbb", slot.getNameParts().get(1));
        Assertions.assertEquals("ccc", slot.getNameParts().get(2));
    }

    @Test
    public void testQuotedDottedStringIsVarcharLiteral() {
        Expression expr = getExpression("SET query_timeout = 'aaa.bbb'");
        Assertions.assertTrue(expr instanceof VarcharLiteral);
        Assertions.assertEquals("aaa.bbb", ((VarcharLiteral) expr).getStringValue());
    }
}
