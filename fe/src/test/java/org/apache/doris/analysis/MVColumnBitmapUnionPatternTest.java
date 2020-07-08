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

import org.apache.doris.catalog.FunctionSet;

import com.google.common.collect.Lists;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;

import mockit.Expectations;
import mockit.Injectable;

public class MVColumnBitmapUnionPatternTest {

    @Test
    public void testCorrectExpr1() {
        TableName tableName = new TableName("db", "table");
        SlotRef slotRef = new SlotRef(tableName, "c1");
        List<Expr> child0Params = Lists.newArrayList();
        child0Params.add(slotRef);
        FunctionCallExpr child0 = new FunctionCallExpr(FunctionSet.TO_BITMAP, child0Params);
        List<Expr> params = Lists.newArrayList();
        params.add(child0);
        FunctionCallExpr expr = new FunctionCallExpr(FunctionSet.BITMAP_UNION, params);
        MVColumnBitmapUnionPattern pattern = new MVColumnBitmapUnionPattern();
        Assert.assertTrue(pattern.match(expr));
    }

    @Test
    public void testCorrectExpr2(@Injectable CastExpr castExpr) {
        TableName tableName = new TableName("db", "table");
        SlotRef slotRef = new SlotRef(tableName, "c1");
        new Expectations() {
            {
                castExpr.getChild(0);
                result = slotRef;
            }
        };
        List<Expr> child0Params = Lists.newArrayList();
        child0Params.add(castExpr);
        FunctionCallExpr child0 = new FunctionCallExpr(FunctionSet.TO_BITMAP, child0Params);
        List<Expr> params = Lists.newArrayList();
        params.add(child0);
        FunctionCallExpr expr = new FunctionCallExpr(FunctionSet.BITMAP_UNION, params);
        MVColumnBitmapUnionPattern pattern = new MVColumnBitmapUnionPattern();
        Assert.assertTrue(pattern.match(expr));
    }

    @Test
    public void testUpperCaseOfFunction() {
        TableName tableName = new TableName("db", "table");
        SlotRef slotRef = new SlotRef(tableName, "c1");
        List<Expr> child0Params = Lists.newArrayList();
        child0Params.add(slotRef);
        FunctionCallExpr child0 = new FunctionCallExpr(FunctionSet.TO_BITMAP.toUpperCase(), child0Params);
        List<Expr> params = Lists.newArrayList();
        params.add(child0);
        FunctionCallExpr expr = new FunctionCallExpr(FunctionSet.BITMAP_UNION.toUpperCase(), params);
        MVColumnBitmapUnionPattern pattern = new MVColumnBitmapUnionPattern();
        Assert.assertTrue(pattern.match(expr));
    }

    @Test
    public void testIncorrectArithmeticExpr1() {
        TableName tableName = new TableName("db", "table");
        SlotRef slotRef1 = new SlotRef(tableName, "c1");
        SlotRef slotRef2 = new SlotRef(tableName, "c2");
        ArithmeticExpr arithmeticExpr = new ArithmeticExpr(ArithmeticExpr.Operator.ADD, slotRef1, slotRef2);
        List<Expr> params = Lists.newArrayList();
        params.add(arithmeticExpr);
        FunctionCallExpr expr = new FunctionCallExpr(FunctionSet.BITMAP_UNION, params);
        MVColumnBitmapUnionPattern pattern = new MVColumnBitmapUnionPattern();
        Assert.assertFalse(pattern.match(expr));
    }

    @Test
    public void testIncorrectArithmeticExpr2() {
        TableName tableName = new TableName("db", "table");
        SlotRef slotRef1 = new SlotRef(tableName, "c1");
        SlotRef slotRef2 = new SlotRef(tableName, "c2");
        ArithmeticExpr arithmeticExpr = new ArithmeticExpr(ArithmeticExpr.Operator.ADD, slotRef1, slotRef2);
        List<Expr> child0Params = Lists.newArrayList();
        child0Params.add(arithmeticExpr);
        FunctionCallExpr child0 = new FunctionCallExpr(FunctionSet.TO_BITMAP, child0Params);
        List<Expr> params = Lists.newArrayList();
        params.add(child0);
        FunctionCallExpr expr = new FunctionCallExpr(FunctionSet.BITMAP_UNION, params);
        MVColumnBitmapUnionPattern pattern = new MVColumnBitmapUnionPattern();
        Assert.assertFalse(pattern.match(expr));
    }
}
