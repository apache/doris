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

import org.apache.doris.catalog.AggregateFunction;
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.FunctionSet;
import org.apache.doris.common.jmockit.Deencapsulation;

import com.google.common.collect.Lists;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;

import mockit.Expectations;
import mockit.Injectable;

public class MVColumnOneChildPatternTest {

    @Test
    public void testCorrectSum(@Injectable AggregateFunction aggregateFunction) {
        TableName tableName = new TableName("db", "table");
        SlotRef slotRef = new SlotRef(tableName, "c1");
        List<Expr> params = Lists.newArrayList();
        params.add(slotRef);
        FunctionCallExpr functionCallExpr = new FunctionCallExpr(AggregateType.SUM.name(), params);
        Deencapsulation.setField(functionCallExpr, "fn", aggregateFunction);
        MVColumnOneChildPattern mvColumnOneChildPattern = new MVColumnOneChildPattern(
                AggregateType.SUM.name().toLowerCase());
        Assert.assertTrue(mvColumnOneChildPattern.match(functionCallExpr));
    }

    @Test
    public void testCorrectMin(@Injectable CastExpr castExpr, @Injectable AggregateFunction aggregateFunction) {
        TableName tableName = new TableName("db", "table");
        SlotRef slotRef = new SlotRef(tableName, "c1");
        List<Expr> child0Params = Lists.newArrayList();
        child0Params.add(slotRef);
        new Expectations() {
            {
                castExpr.unwrapSlotRef();
                result = slotRef;
            }
        };
        List<Expr> params = Lists.newArrayList();
        params.add(castExpr);
        FunctionCallExpr functionCallExpr = new FunctionCallExpr(AggregateType.MIN.name(), params);
        Deencapsulation.setField(functionCallExpr, "fn", aggregateFunction);
        MVColumnOneChildPattern mvColumnOneChildPattern = new MVColumnOneChildPattern(
                AggregateType.MIN.name().toLowerCase());
        Assert.assertTrue(mvColumnOneChildPattern.match(functionCallExpr));
    }

    @Test
    public void testCorrectCountField(@Injectable AggregateFunction aggregateFunction) {
        TableName tableName = new TableName("db", "table");
        SlotRef slotRef = new SlotRef(tableName, "c1");
        List<Expr> params = Lists.newArrayList();
        params.add(slotRef);
        FunctionCallExpr functionCallExpr = new FunctionCallExpr(FunctionSet.COUNT, params);
        Deencapsulation.setField(functionCallExpr, "fn", aggregateFunction);
        MVColumnOneChildPattern mvColumnOneChildPattern = new MVColumnOneChildPattern(FunctionSet.COUNT.toLowerCase());
        Assert.assertTrue(mvColumnOneChildPattern.match(functionCallExpr));
    }

    @Test
    public void testIncorrectLiteral(@Injectable AggregateFunction aggregateFunction) {
        IntLiteral intLiteral = new IntLiteral(1);
        List<Expr> params = Lists.newArrayList();
        params.add(intLiteral);
        FunctionCallExpr functionCallExpr = new FunctionCallExpr(AggregateType.SUM.name(), params);
        Deencapsulation.setField(functionCallExpr, "fn", aggregateFunction);
        MVColumnOneChildPattern mvColumnOneChildPattern = new MVColumnOneChildPattern(
                AggregateType.SUM.name().toLowerCase());
        Assert.assertFalse(mvColumnOneChildPattern.match(functionCallExpr));
    }

    @Test
    public void testIncorrectArithmeticExpr(@Injectable AggregateFunction aggregateFunction) {
        TableName tableName = new TableName("db", "table");
        SlotRef slotRef1 = new SlotRef(tableName, "c1");
        SlotRef slotRef2 = new SlotRef(tableName, "c2");
        ArithmeticExpr arithmeticExpr = new ArithmeticExpr(ArithmeticExpr.Operator.ADD, slotRef1, slotRef2);
        List<Expr> params = Lists.newArrayList();
        params.add(arithmeticExpr);
        FunctionCallExpr functionCallExpr = new FunctionCallExpr(AggregateType.SUM.name(), params);
        Deencapsulation.setField(functionCallExpr, "fn", aggregateFunction);
        MVColumnOneChildPattern mvColumnOneChildPattern = new MVColumnOneChildPattern(
                AggregateType.SUM.name().toLowerCase());
        Assert.assertFalse(mvColumnOneChildPattern.match(functionCallExpr));
    }
}
