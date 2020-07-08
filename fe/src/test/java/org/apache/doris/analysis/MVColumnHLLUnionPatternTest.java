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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.FunctionSet;
import org.apache.doris.catalog.Type;

import com.google.common.collect.Lists;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;

import mockit.Expectations;
import mockit.Injectable;

public class MVColumnHLLUnionPatternTest {

    @Test
    public void testCorrectExpr1() {
        TableName tableName = new TableName("db", "table");
        SlotRef slotRef = new SlotRef(tableName, "c1");
        List<Expr> child0Params = Lists.newArrayList();
        child0Params.add(slotRef);
        FunctionCallExpr child0 = new FunctionCallExpr(FunctionSet.HLL_HASH, child0Params);
        List<Expr> params = Lists.newArrayList();
        params.add(child0);
        FunctionCallExpr expr = new FunctionCallExpr(FunctionSet.HLL_UNION, params);
        MVColumnHLLUnionPattern pattern = new MVColumnHLLUnionPattern();
        Assert.assertTrue(pattern.match(expr));
    }

    @Test
    public void testCorrectExpr2(@Injectable CastExpr castExpr) {
        TableName tableName = new TableName("db", "table");
        SlotRef slotRef = new SlotRef(tableName, "c1");
        new Expectations() {
            {
                castExpr.unwrapSlotRef();
                result = slotRef;
            }
        };
        List<Expr> child0Params = Lists.newArrayList();
        child0Params.add(castExpr);
        FunctionCallExpr child0 = new FunctionCallExpr(FunctionSet.HLL_HASH, child0Params);
        List<Expr> params = Lists.newArrayList();
        params.add(child0);
        FunctionCallExpr expr = new FunctionCallExpr(FunctionSet.HLL_UNION, params);
        MVColumnHLLUnionPattern pattern = new MVColumnHLLUnionPattern();
        Assert.assertTrue(pattern.match(expr));
    }

    @Test
    public void testUpperCaseOfFunction() {
        TableName tableName = new TableName("db", "table");
        SlotRef slotRef = new SlotRef(tableName, "c1");
        List<Expr> child0Params = Lists.newArrayList();
        child0Params.add(slotRef);
        FunctionCallExpr child0 = new FunctionCallExpr(FunctionSet.HLL_HASH.toUpperCase(), child0Params);
        List<Expr> params = Lists.newArrayList();
        params.add(child0);
        FunctionCallExpr expr = new FunctionCallExpr(FunctionSet.HLL_UNION.toUpperCase(), params);
        MVColumnHLLUnionPattern pattern = new MVColumnHLLUnionPattern();
        Assert.assertTrue(pattern.match(expr));
    }

    @Test
    public void testIncorrectLiteralExpr1() {
        IntLiteral intLiteral = new IntLiteral(1);
        List<Expr> params = Lists.newArrayList();
        params.add(intLiteral);
        FunctionCallExpr expr = new FunctionCallExpr(FunctionSet.HLL_UNION, params);
        MVColumnHLLUnionPattern pattern = new MVColumnHLLUnionPattern();
        Assert.assertFalse(pattern.match(expr));
    }

    @Test
    public void testIncorrectLiteralExpr2() {
        IntLiteral intLiteral = new IntLiteral(1);
        List<Expr> child0Params = Lists.newArrayList();
        child0Params.add(intLiteral);
        FunctionCallExpr child0 = new FunctionCallExpr(FunctionSet.HLL_HASH, child0Params);
        List<Expr> params = Lists.newArrayList();
        params.add(child0);
        FunctionCallExpr expr = new FunctionCallExpr(FunctionSet.HLL_UNION, params);
        MVColumnHLLUnionPattern pattern = new MVColumnHLLUnionPattern();
        Assert.assertFalse(pattern.match(expr));
    }

    @Test
    public void testAggTableHLLColumn(@Injectable SlotDescriptor desc,
            @Injectable Column column) {
        TableName tableName = new TableName("db", "table");
        SlotRef slotRef1 = new SlotRef(tableName, "c1");
        List<Expr> params = Lists.newArrayList();
        params.add(slotRef1);
        FunctionCallExpr expr = new FunctionCallExpr(FunctionSet.HLL_UNION, params);
        slotRef1.setType(Type.HLL);
        slotRef1.setDesc(desc);
        new Expectations() {
            {
                desc.getColumn();
                result = column;
            }
        };
        MVColumnHLLUnionPattern pattern = new MVColumnHLLUnionPattern();
        Assert.assertTrue(pattern.match(expr));
    }

}
