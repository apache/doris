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
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.FunctionSet;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.InternalCatalog;

import com.google.common.collect.Lists;
import mockit.Expectations;
import mockit.Injectable;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class MVColumnHLLUnionPatternTest {
    private static final String internalCtl = InternalCatalog.INTERNAL_CATALOG_NAME;

    @Test
    public void testCorrectExpr1(@Injectable AggregateFunction aggregateFunction) {
        TableName tableName = new TableName(internalCtl, "db", "table");
        SlotRef slotRef = new SlotRef(tableName, "c1");
        List<Expr> child0Params = Lists.newArrayList();
        child0Params.add(slotRef);
        FunctionCallExpr child0 = new FunctionCallExpr(FunctionSet.HLL_HASH, child0Params);
        child0.setType(Type.HLL);
        List<Expr> params = Lists.newArrayList();
        params.add(child0);
        FunctionCallExpr expr = new FunctionCallExpr(FunctionSet.HLL_UNION, params);
        Deencapsulation.setField(expr, "fn", aggregateFunction);
        MVColumnHLLUnionPattern pattern = new MVColumnHLLUnionPattern();
        Assert.assertTrue(pattern.match(expr));
    }

    @Test
    public void testCorrectExpr2(@Injectable CastExpr castExpr, @Injectable AggregateFunction aggregateFunction) {
        TableName tableName = new TableName(internalCtl, "db", "table");
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
        child0.setType(Type.HLL);
        List<Expr> params = Lists.newArrayList();
        params.add(child0);
        FunctionCallExpr expr = new FunctionCallExpr(FunctionSet.HLL_UNION, params);
        Deencapsulation.setField(expr, "fn", aggregateFunction);
        MVColumnHLLUnionPattern pattern = new MVColumnHLLUnionPattern();
        Assert.assertTrue(pattern.match(expr));
    }

    @Test
    public void testUpperCaseOfFunction(@Injectable AggregateFunction aggregateFunction) {
        TableName tableName = new TableName(internalCtl, "db", "table");
        SlotRef slotRef = new SlotRef(tableName, "c1");
        List<Expr> child0Params = Lists.newArrayList();
        child0Params.add(slotRef);
        FunctionCallExpr child0 = new FunctionCallExpr(FunctionSet.HLL_HASH.toUpperCase(), child0Params);
        child0.setType(Type.HLL);
        List<Expr> params = Lists.newArrayList();
        params.add(child0);
        FunctionCallExpr expr = new FunctionCallExpr(FunctionSet.HLL_UNION.toUpperCase(), params);
        Deencapsulation.setField(expr, "fn", aggregateFunction);
        MVColumnHLLUnionPattern pattern = new MVColumnHLLUnionPattern();
        Assert.assertTrue(pattern.match(expr));
    }

    @Test
    public void testIncorrectLiteralExpr1(@Injectable AggregateFunction aggregateFunction) {
        IntLiteral intLiteral = new IntLiteral(1);
        List<Expr> params = Lists.newArrayList();
        params.add(intLiteral);
        FunctionCallExpr expr = new FunctionCallExpr(FunctionSet.HLL_UNION, params);
        Deencapsulation.setField(expr, "fn", aggregateFunction);
        MVColumnHLLUnionPattern pattern = new MVColumnHLLUnionPattern();
        Assert.assertFalse(pattern.match(expr));
    }

    @Test
    public void testIncorrectLiteralExpr2(@Injectable AggregateFunction aggregateFunction) {
        IntLiteral intLiteral = new IntLiteral(1);
        List<Expr> child0Params = Lists.newArrayList();
        child0Params.add(intLiteral);
        FunctionCallExpr child0 = new FunctionCallExpr(FunctionSet.HLL_HASH, child0Params);
        child0.setType(Type.HLL);
        List<Expr> params = Lists.newArrayList();
        params.add(child0);
        FunctionCallExpr expr = new FunctionCallExpr(FunctionSet.HLL_UNION, params);
        Deencapsulation.setField(expr, "fn", aggregateFunction);
        MVColumnHLLUnionPattern pattern = new MVColumnHLLUnionPattern();
        Assert.assertFalse(pattern.match(expr));
    }

    @Test
    public void testIncorrectDecimalSlotRef(@Injectable AggregateFunction aggregateFunction) {
        TableName tableName = new TableName(internalCtl, "db", "table");
        SlotRef slotRef = new SlotRef(tableName, "c1");
        Deencapsulation.setField(slotRef, "type", Type.DECIMALV2);
        List<Expr> child0Params = Lists.newArrayList();
        child0Params.add(slotRef);
        FunctionCallExpr child0 = new FunctionCallExpr(FunctionSet.HLL_HASH, child0Params);
        child0.setType(Type.HLL);
        List<Expr> params = Lists.newArrayList();
        params.add(child0);
        FunctionCallExpr expr = new FunctionCallExpr(FunctionSet.HLL_UNION, params);
        Deencapsulation.setField(expr, "fn", aggregateFunction);
        MVColumnHLLUnionPattern pattern = new MVColumnHLLUnionPattern();
        Assert.assertFalse(pattern.match(expr));
    }

    @Test
    public void testAggTableHLLColumn(@Injectable SlotDescriptor desc,
            @Injectable Column column, @Injectable AggregateFunction aggregateFunction) {
        TableName tableName = new TableName(internalCtl, "db", "table");
        SlotRef slotRef1 = new SlotRef(tableName, "c1");
        List<Expr> params = Lists.newArrayList();
        params.add(slotRef1);
        FunctionCallExpr expr = new FunctionCallExpr(FunctionSet.HLL_UNION, params);
        Deencapsulation.setField(expr, "fn", aggregateFunction);
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
