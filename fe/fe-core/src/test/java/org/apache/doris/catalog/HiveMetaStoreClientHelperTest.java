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

package org.apache.doris.catalog;

import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.BinaryPredicate.Operator;
import org.apache.doris.analysis.CastExpr;
import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.InPredicate;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.analysis.TableName;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;

import com.google.common.collect.Lists;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class HiveMetaStoreClientHelperTest {
    private static String tableNameString;
    private static TableName tableName;
    private static final String col1 = "col1";
    private static final List<String> partitionKeys = new ArrayList<>();
    private static final String p1 = "p_col1";
    private static final String p2 = "p_col2";

    private static final String alwaysTrue = "(1 = 1)";

    @BeforeClass
    public static void beforeAll() {
        tableNameString = "test_table";
        tableName = new TableName("hive", "", tableNameString);
        partitionKeys.add(p1);
        partitionKeys.add(p2);
    }

    @Test
    public void testSingleConvertExpr1() throws DdlException, AnalysisException {
        List<Expr> exprs = new ArrayList<>();
        SlotRef slotref = new SlotRef(tableName, col1);
        slotref.setType(Type.STRING);
        exprs.add(new BinaryPredicate(Operator.EQ, slotref, LiteralExpr.create("1", Type.STRING)));

        // where col1 = 1 => no match => (1 = 1)
        ExprNodeGenericFuncDesc funcDesc = HiveMetaStoreClientHelper.convertToHivePartitionExpr(
                exprs, partitionKeys, tableNameString);
        Assert.assertEquals(alwaysTrue, funcDesc.getExprString());
    }

    @Test
    public void testSingleConvertExpr2() throws DdlException, AnalysisException {
        List<Expr> exprs = new ArrayList<>();
        SlotRef slotref = new SlotRef(tableName, p1);
        slotref.setType(Type.STRING);
        exprs.add(new BinaryPredicate(Operator.EQ, slotref, LiteralExpr.create("1", Type.STRING)));

        // where p_col1 = 1 => match => (p_col1 = 1)
        ExprNodeGenericFuncDesc funcDesc = HiveMetaStoreClientHelper.convertToHivePartitionExpr(
                exprs, partitionKeys, tableNameString);
        Assert.assertEquals("(p_col1 = '1')", funcDesc.getExprString());
    }

    @Test
    public void testSingleConvertExpr3() throws DdlException, AnalysisException {
        List<Expr> exprs = new ArrayList<>();
        SlotRef slotref = new SlotRef(tableName, p1);
        slotref.setType(Type.STRING);
        exprs.add(new BinaryPredicate(Operator.GE, slotref, LiteralExpr.create("1", Type.STRING)));

        // where p_col1 >= 1 => match => (p_col1 >= 1)
        ExprNodeGenericFuncDesc funcDesc = HiveMetaStoreClientHelper.convertToHivePartitionExpr(
                exprs, partitionKeys, tableNameString);
        Assert.assertEquals("(p_col1 >= '1')", funcDesc.getExprString());
    }

    @Test
    public void testSingleConvertExpr4() throws DdlException, AnalysisException {
        List<Expr> exprs = new ArrayList<>();
        SlotRef slotref = new SlotRef(tableName, p1);
        slotref.setType(Type.STRING);
        List<Expr> inList = Lists.newArrayList();
        inList.add(new StringLiteral("1"));

        exprs.add(new InPredicate(slotref, inList, true));

        // where p_col1 in (1) => no match => (1 = 1)
        ExprNodeGenericFuncDesc funcDesc = HiveMetaStoreClientHelper.convertToHivePartitionExpr(
                exprs, partitionKeys, tableNameString);
        Assert.assertEquals(alwaysTrue, funcDesc.getExprString());
    }

    @Test
    public void testSingleConvertExpr5() throws DdlException, AnalysisException {
        List<Expr> exprs = new ArrayList<>();
        SlotRef slotref = new SlotRef(tableName, p1);
        slotref.setType(Type.STRING);
        CastExpr castExpr = new CastExpr(Type.STRING, LiteralExpr.create("1", Type.INT));
        exprs.add(new BinaryPredicate(Operator.EQ, slotref, castExpr));

        // where p_col1 = cast 1 as String =>  match => (p_col1 = 1)
        ExprNodeGenericFuncDesc funcDesc = HiveMetaStoreClientHelper.convertToHivePartitionExpr(
                exprs, partitionKeys, tableNameString);
        Assert.assertEquals("(p_col1 = '1')", funcDesc.getExprString());
    }

    @Test
    public void testCompoundConvertExpr1() throws DdlException, AnalysisException {
        SlotRef slotref1 = new SlotRef(tableName, p1);
        slotref1.setType(Type.STRING);
        BinaryPredicate pCol1 = new BinaryPredicate(Operator.EQ, slotref1,
                LiteralExpr.create("1", Type.STRING));

        SlotRef slotref2 = new SlotRef(tableName, col1);
        slotref2.setType(Type.STRING);
        BinaryPredicate col1 = new BinaryPredicate(Operator.EQ, slotref2,
                LiteralExpr.create("1", Type.STRING));

        CompoundPredicate pCol1AndCol1 = new CompoundPredicate(CompoundPredicate.Operator.AND,
                pCol1, col1);

        List<Expr> exprs1 = new ArrayList<>();
        exprs1.add(pCol1AndCol1);

        // where p_col1 = 1 and col1 = 1 => only match (p_col1 = 1)  => (p_col1 = 1)
        ExprNodeGenericFuncDesc funcDesc1 = HiveMetaStoreClientHelper.convertToHivePartitionExpr(
                exprs1, partitionKeys, tableNameString);
        Assert.assertEquals("(p_col1 = '1')", funcDesc1.getExprString());

        CompoundPredicate pCol1OrCol1 = new CompoundPredicate(CompoundPredicate.Operator.OR,
                pCol1, col1);
        List<Expr> exprs2 = new ArrayList<>();
        exprs2.add(pCol1OrCol1);
        // where p_col1 = 1 or col1 = 1 => always true  => (1 = 1)
        ExprNodeGenericFuncDesc funcDesc2 = HiveMetaStoreClientHelper.convertToHivePartitionExpr(
                exprs2, partitionKeys, tableNameString);
        Assert.assertEquals(alwaysTrue, funcDesc2.getExprString());


        CompoundPredicate notPCol1 = new CompoundPredicate(CompoundPredicate.Operator.NOT, pCol1, null);

        CompoundPredicate notPCol1AndCol1 = new CompoundPredicate(CompoundPredicate.Operator.AND,
                notPCol1, col1);
        List<Expr> exprs3 = new ArrayList<>();
        exprs3.add(notPCol1AndCol1);
        // where p_col1 = 1 and not (col1 = 1) => no match  => (1 = 1)
        ExprNodeGenericFuncDesc funcDesc3 = HiveMetaStoreClientHelper.convertToHivePartitionExpr(
                exprs3, partitionKeys, tableNameString);
        Assert.assertEquals(alwaysTrue, funcDesc3.getExprString());

        CompoundPredicate notPCol1OrCol1 = new CompoundPredicate(CompoundPredicate.Operator.OR,
                notPCol1, col1);
        List<Expr> exprs4 = new ArrayList<>();
        exprs4.add(notPCol1OrCol1);
        // where p_col1 = 1 or not (col1 = 1) => not (col1 = 1) as always true  =>  1 = 1
        ExprNodeGenericFuncDesc funcDesc4 = HiveMetaStoreClientHelper.convertToHivePartitionExpr(
                exprs4, partitionKeys, tableNameString);
        Assert.assertEquals(alwaysTrue, funcDesc4.getExprString());
    }

    @Test
    public void testCompoundConvertExpr2() throws DdlException, AnalysisException {
        SlotRef slotref1 = new SlotRef(tableName, p1);
        slotref1.setType(Type.STRING);
        BinaryPredicate pCol1 = new BinaryPredicate(Operator.EQ, slotref1,
                LiteralExpr.create("1", Type.STRING));

        SlotRef slotref2 = new SlotRef(tableName, col1);
        slotref2.setType(Type.STRING);
        BinaryPredicate col1 = new BinaryPredicate(Operator.EQ, slotref2,
                LiteralExpr.create("1", Type.STRING));
        CompoundPredicate compoundPredicate1 = new CompoundPredicate(CompoundPredicate.Operator.AND,
                pCol1, col1);
        CompoundPredicate compoundPredicate2 = new CompoundPredicate(CompoundPredicate.Operator.OR,
                pCol1, col1);

        SlotRef slotref3 = new SlotRef(tableName, p2);
        slotref3.setType(Type.STRING);
        BinaryPredicate pCol2 = new BinaryPredicate(Operator.EQ, slotref3,
                LiteralExpr.create("1", Type.STRING));

        List<Expr> exprs1 = new ArrayList<>();
        exprs1.add(new CompoundPredicate(CompoundPredicate.Operator.AND, compoundPredicate1, pCol2));
        // where (p_col1 = 1 and col1 = 1) and (p_col2 = 1) => match (p_col1 = 1 and p_col2 = 1)
        ExprNodeGenericFuncDesc funcDesc1 = HiveMetaStoreClientHelper.convertToHivePartitionExpr(
                exprs1, partitionKeys, tableNameString);
        Assert.assertEquals("((p_col1 = '1') and (p_col2 = '1'))", funcDesc1.getExprString());

        List<Expr> exprs2 = new ArrayList<>();
        exprs2.add(new CompoundPredicate(CompoundPredicate.Operator.AND, compoundPredicate2, pCol2));
        // where (p_col1 = 1 or col1 = 1) and (p_col2 = 1) => match (p_col2 = 1) => (p_col2 = '1')
        ExprNodeGenericFuncDesc funcDesc2 = HiveMetaStoreClientHelper.convertToHivePartitionExpr(
                exprs2, partitionKeys, tableNameString);
        Assert.assertEquals("(p_col2 = '1')", funcDesc2.getExprString());

        List<Expr> exprs3 = new ArrayList<>();
        exprs3.add(new CompoundPredicate(CompoundPredicate.Operator.OR, compoundPredicate1, pCol2));
        // where (p_col1 = 1 and col1 = 1) orc (p_col2 = 1) => match (p_col1 = 1 or p_col2 = 1)
        ExprNodeGenericFuncDesc funcDesc3 = HiveMetaStoreClientHelper.convertToHivePartitionExpr(
                exprs3, partitionKeys, tableNameString);
        Assert.assertEquals("((p_col1 = '1') or (p_col2 = '1'))", funcDesc3.getExprString());

        List<Expr> exprs4 = new ArrayList<>();
        exprs4.add(new CompoundPredicate(CompoundPredicate.Operator.OR, compoundPredicate2, pCol2));
        // where (p_col1 = 1 or col1 = 1) or (p_col2 = 1) => no match => (1 = 1)
        ExprNodeGenericFuncDesc funcDesc4 = HiveMetaStoreClientHelper.convertToHivePartitionExpr(
                exprs4, partitionKeys, tableNameString);
        Assert.assertEquals(alwaysTrue, funcDesc4.getExprString());
    }

    @Test
    public void testCompoundConvertExpr3() throws DdlException, AnalysisException {
        SlotRef slotref1 = new SlotRef(tableName, p1);
        slotref1.setType(Type.STRING);
        BinaryPredicate pCol1 = new BinaryPredicate(Operator.EQ, slotref1,
                LiteralExpr.create("1", Type.STRING));

        SlotRef slotref2 = new SlotRef(tableName, col1);
        slotref2.setType(Type.STRING);
        BinaryPredicate col1 = new BinaryPredicate(Operator.EQ, slotref2,
                LiteralExpr.create("1", Type.STRING));

        CompoundPredicate pCol1AndCol1 = new CompoundPredicate(CompoundPredicate.Operator.AND,
                pCol1, col1);

        SlotRef slotref3 = new SlotRef(tableName, p2);
        slotref3.setType(Type.STRING);
        BinaryPredicate pCol2 = new BinaryPredicate(Operator.EQ, slotref3,
                LiteralExpr.create("1", Type.STRING));

        List<Expr> exprs1 = new ArrayList<>();
        exprs1.add(pCol1AndCol1);
        exprs1.add(pCol2);

        // where (p_col1 = 1 and col1 = 1) and (p_col2 = 1) => match (p_col1 = 1) and  (p_col2 = 1) =>
        ExprNodeGenericFuncDesc funcDesc1 = HiveMetaStoreClientHelper.convertToHivePartitionExpr(
                exprs1, partitionKeys, tableNameString);
        Assert.assertEquals("((p_col1 = '1') and (p_col2 = '1'))", funcDesc1.getExprString());


        CompoundPredicate pCol1OrCol1 = new CompoundPredicate(CompoundPredicate.Operator.OR,
                pCol1, col1);
        List<Expr> exprs2 = new ArrayList<>();
        exprs2.add(pCol1OrCol1);
        exprs2.add(pCol2);

        // where (p_col1 = 1 or col1 = 1) and (p_col2 = 1) => match (p_col2 = 1) => (p_col2 = '1')
        ExprNodeGenericFuncDesc funcDesc2 = HiveMetaStoreClientHelper.convertToHivePartitionExpr(
                exprs2, partitionKeys, tableNameString);
        Assert.assertEquals("(p_col2 = '1')", funcDesc2.getExprString());
    }

}
