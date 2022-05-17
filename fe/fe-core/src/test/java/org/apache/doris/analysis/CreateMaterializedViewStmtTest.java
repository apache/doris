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
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.FunctionSet;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Lists;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;

import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;

public class CreateMaterializedViewStmtTest {

    @Mocked
    private Analyzer analyzer;
    @Mocked
    private ExprSubstitutionMap exprSubstitutionMap;
    @Mocked
    private ConnectContext connectContext;
    @Mocked
    private Config config;

    @Before
    public void initTest() {

    }

    @Test
    public void testFunctionColumnInSelectClause(@Injectable ArithmeticExpr arithmeticExpr) throws UserException {
        SelectList selectList = new SelectList();
        SelectListItem selectListItem = new SelectListItem(arithmeticExpr, null);
        selectList.addItem(selectListItem);
        FromClause fromClause = new FromClause();
        SelectStmt selectStmt = new SelectStmt(selectList, fromClause, null, null, null, null, LimitElement.NO_LIMIT);

        new Expectations() {
            {
                analyzer.getClusterName();
                result = "default";
            }
        };
        CreateMaterializedViewStmt createMaterializedViewStmt = new CreateMaterializedViewStmt("test", selectStmt, null);
        try {
            createMaterializedViewStmt.analyze(analyzer);
            Assert.fail();
        } catch (UserException e) {
            System.out.print(e.getMessage());
        }
    }

    @Test
    public void testCountDistinct(@Injectable SlotRef slotRef, @Injectable ArithmeticExpr arithmeticExpr,
                                  @Injectable SelectStmt selectStmt, @Injectable Column column,
                                  @Injectable TableRef tableRef,
                                  @Injectable SlotDescriptor slotDescriptor) throws UserException {
        SelectList selectList = new SelectList();
        SelectListItem selectListItem = new SelectListItem(slotRef, null);
        selectList.addItem(selectListItem);

        TableName tableName = new TableName("db", "table");
        SlotRef slotRef2 = new SlotRef(tableName, "v1");
        List<Expr> fnChildren = Lists.newArrayList(slotRef2);
        Deencapsulation.setField(slotRef2, "desc", slotDescriptor);
        FunctionParams functionParams = new FunctionParams(true, fnChildren);
        FunctionCallExpr functionCallExpr = new FunctionCallExpr(FunctionSet.COUNT, functionParams);
        functionCallExpr.setFn(AggregateFunction.createBuiltin(FunctionSet.COUNT,
                new ArrayList<>(), Type.BIGINT, Type.BIGINT, false, true, true));
        SelectListItem selectListItem2 = new SelectListItem(functionCallExpr, null);
        selectList.addItem(selectListItem2);

        new Expectations() {
            {
                analyzer.getClusterName();
                result = "default";
                selectStmt.analyze(analyzer);
                selectStmt.getSelectList();
                result = selectList;
                arithmeticExpr.toString();
                result = "a+b";
                slotRef.getColumnName();
                result = "k1";
                selectStmt.getWhereClause();
                minTimes = 0;
                result = null;
                selectStmt.getHavingPred();
                minTimes = 0;
                result = null;
                selectStmt.getTableRefs();
                minTimes = 0;
                result = Lists.newArrayList(tableRef);
                slotDescriptor.getColumn();
                minTimes = 0;
                result = column;
                selectStmt.getLimit();
                minTimes = 0;
                result = -1;
                column.getType();
                minTimes = 0;
                result = Type.INT;
                slotRef.getType();
                result = Type.INT;
            }
        };
        CreateMaterializedViewStmt createMaterializedViewStmt =
                new CreateMaterializedViewStmt("test", selectStmt, null);
        try {
            createMaterializedViewStmt.analyze(analyzer);
            Assert.fail();
        } catch (AnalysisException e) {
            Assert.assertTrue(
                    e.getMessage().contains("The function count must match pattern:count(column)"));
            System.out.print(e.getMessage());
        }
    }

    @Test
    public void testAggregateWithFunctionColumnInSelectClause(@Injectable ArithmeticExpr arithmeticExpr,
                                                              @Injectable SelectStmt selectStmt,
                                                              @Injectable AggregateFunction aggregateFunction) throws UserException {
        FunctionCallExpr functionCallExpr = new FunctionCallExpr("sum", Lists.newArrayList(arithmeticExpr));
        Deencapsulation.setField(functionCallExpr, "fn", aggregateFunction);
        SelectList selectList = new SelectList();
        SelectListItem selectListItem = new SelectListItem(functionCallExpr, null);
        selectList.addItem(selectListItem);
        new Expectations() {
            {
                analyzer.getClusterName();
                result = "default";
                selectStmt.analyze(analyzer);
                selectStmt.getSelectList();
                result = selectList;
                arithmeticExpr.toString();
                result = "a+b";
            }
        };

        CreateMaterializedViewStmt createMaterializedViewStmt = new CreateMaterializedViewStmt("test", selectStmt, null);
        try {
            createMaterializedViewStmt.analyze(analyzer);
            Assert.fail();
        } catch (IllegalArgumentException e) {
            System.out.print(e.getMessage());
        }
    }

    @Test
    public void testJoinSelectClause(@Injectable SlotRef slotRef,
                                     @Injectable TableRef tableRef1,
                                     @Injectable TableRef tableRef2,
                                     @Injectable SelectStmt selectStmt) throws UserException {
        SelectListItem selectListItem = new SelectListItem(slotRef, null);
        SelectList selectList = new SelectList();
        selectList.addItem(selectListItem);
        new Expectations() {
            {
                analyzer.getClusterName();
                result = "default";
                selectStmt.analyze(analyzer);
                selectStmt.getTableRefs();
                result = Lists.newArrayList(tableRef1, tableRef2);
                selectStmt.getSelectList();
                result = selectList;
                slotRef.getColumnName();
                result = "k1";
            }
        };

        CreateMaterializedViewStmt createMaterializedViewStmt = new CreateMaterializedViewStmt("test", selectStmt, null);
        try {
            createMaterializedViewStmt.analyze(analyzer);
            Assert.fail();
        } catch (UserException e) {
            System.out.print(e.getMessage());
        }
    }

    @Test
    public void testSelectClauseWithWhereClause(@Injectable SlotRef slotRef,
                                                @Injectable TableRef tableRef,
                                                @Injectable Expr whereClause,
                                                @Injectable SelectStmt selectStmt) throws UserException {
        SelectList selectList = new SelectList();
        SelectListItem selectListItem = new SelectListItem(slotRef, null);
        selectList.addItem(selectListItem);
        new Expectations() {
            {
                analyzer.getClusterName();
                result = "default";
                selectStmt.analyze(analyzer);
                selectStmt.getSelectList();
                result = selectList;
                selectStmt.getTableRefs();
                result = Lists.newArrayList(tableRef);
                selectStmt.getWhereClause();
                result = whereClause;
                slotRef.getColumnName();
                result = "k1";
            }
        };

        CreateMaterializedViewStmt createMaterializedViewStmt = new CreateMaterializedViewStmt("test", selectStmt, null);
        try {
            createMaterializedViewStmt.analyze(analyzer);
            Assert.fail();
        } catch (UserException e) {
            System.out.print(e.getMessage());
        }
    }

    @Test
    public void testSelectClauseWithHavingClause(@Injectable SlotRef slotRef,
                                                 @Injectable TableRef tableRef,
                                                 @Injectable Expr havingClause,
                                                 @Injectable SelectStmt selectStmt) throws UserException {
        SelectList selectList = new SelectList();
        SelectListItem selectListItem = new SelectListItem(slotRef, null);
        selectList.addItem(selectListItem);

        new Expectations() {
            {
                analyzer.getClusterName();
                result = "default";
                selectStmt.analyze(analyzer);
                selectStmt.getSelectList();
                result = selectList;
                selectStmt.getTableRefs();
                result = Lists.newArrayList(tableRef);
                selectStmt.getWhereClause();
                result = null;
                selectStmt.getHavingPred();
                result = havingClause;
                slotRef.getColumnName();
                result = "k1";
            }
        };
        CreateMaterializedViewStmt createMaterializedViewStmt = new CreateMaterializedViewStmt("test", selectStmt, null);
        try {
            createMaterializedViewStmt.analyze(analyzer);
            Assert.fail();
        } catch (UserException e) {
            System.out.print(e.getMessage());
        }
    }

    @Test
    public void testOrderOfColumn(@Injectable SlotRef slotRef1,
                                  @Injectable SlotRef slotRef2,
                                  @Injectable TableRef tableRef,
                                  @Injectable SelectStmt selectStmt) throws UserException {
        SelectList selectList = new SelectList();
        SelectListItem selectListItem1 = new SelectListItem(slotRef1, null);
        selectList.addItem(selectListItem1);
        SelectListItem selectListItem2 = new SelectListItem(slotRef2, null);
        selectList.addItem(selectListItem2);
        OrderByElement orderByElement1 = new OrderByElement(slotRef2, false, false);
        OrderByElement orderByElement2 = new OrderByElement(slotRef1, false, false);
        ArrayList<OrderByElement> orderByElementList = Lists.newArrayList(orderByElement1, orderByElement2);

        new Expectations() {
            {
                analyzer.getClusterName();
                result = "default";
                selectStmt.analyze(analyzer);
                selectStmt.getSelectList();
                result = selectList;
                selectStmt.getTableRefs();
                result = Lists.newArrayList(tableRef);
                selectStmt.getWhereClause();
                result = null;
                selectStmt.getHavingPred();
                result = null;
                selectStmt.getOrderByElements();
                result = orderByElementList;
                slotRef1.getColumnName();
                result = "k1";
                slotRef2.getColumnName();
                result = "k2";
            }
        };
        CreateMaterializedViewStmt createMaterializedViewStmt = new CreateMaterializedViewStmt("test", selectStmt, null);
        try {
            createMaterializedViewStmt.analyze(analyzer);
            Assert.fail();
        } catch (UserException e) {
            System.out.print(e.getMessage());
        }
    }

    @Test
    public void testOrderByAggregateColumn(@Injectable SlotRef slotRef1,
                                           @Injectable TableRef tableRef,
                                           @Injectable SelectStmt selectStmt,
                                           @Injectable Column column2,
                                           @Injectable SlotDescriptor slotDescriptor,
                                           @Injectable AggregateFunction aggregateFunction) throws UserException {
        SelectList selectList = new SelectList();
        SelectListItem selectListItem1 = new SelectListItem(slotRef1, null);
        selectList.addItem(selectListItem1);
        TableName tableName = new TableName("db", "table");
        SlotRef slotRef2 = new SlotRef(tableName, "v1");
        Deencapsulation.setField(slotRef2, "desc", slotDescriptor);
        List<Expr> fnChildren = Lists.newArrayList(slotRef2);
        FunctionCallExpr functionCallExpr = new FunctionCallExpr("sum", fnChildren);
        Deencapsulation.setField(functionCallExpr, "fn", aggregateFunction);
        SelectListItem selectListItem2 = new SelectListItem(functionCallExpr, null);
        selectList.addItem(selectListItem2);
        OrderByElement orderByElement1 = new OrderByElement(functionCallExpr, false, false);
        OrderByElement orderByElement2 = new OrderByElement(slotRef1, false, false);
        ArrayList<OrderByElement> orderByElementList = Lists.newArrayList(orderByElement1, orderByElement2);

        new Expectations() {
            {
                analyzer.getClusterName();
                result = "default";
                selectStmt.analyze(analyzer);
                selectStmt.getSelectList();
                result = selectList;
                selectStmt.getTableRefs();
                result = Lists.newArrayList(tableRef);
                selectStmt.getWhereClause();
                result = null;
                selectStmt.getHavingPred();
                result = null;
                selectStmt.getOrderByElements();
                result = orderByElementList;
                slotRef1.getColumnName();
                result = "k1";
                slotDescriptor.getColumn();
                result = column2;
                column2.getOriginType();
                result = Type.INT;
            }
        };
        CreateMaterializedViewStmt createMaterializedViewStmt = new CreateMaterializedViewStmt("test", selectStmt, null);
        try {
            createMaterializedViewStmt.analyze(analyzer);
            Assert.fail();
        } catch (UserException e) {
            System.out.print(e.getMessage());
        }
    }

    @Test
    public void testDuplicateColumn(@Injectable SelectStmt selectStmt, @Injectable AggregateFunction aggregateFunction) throws UserException {
        SelectList selectList = new SelectList();
        TableName tableName = new TableName("db", "table");
        SlotRef slotRef1 = new SlotRef(tableName, "k1");
        SelectListItem selectListItem1 = new SelectListItem(slotRef1, null);
        selectList.addItem(selectListItem1);
        List<Expr> fnChildren = Lists.newArrayList(slotRef1);
        FunctionCallExpr functionCallExpr = new FunctionCallExpr("sum", fnChildren);
        Deencapsulation.setField(functionCallExpr, "fn", aggregateFunction);
        SelectListItem selectListItem2 = new SelectListItem(functionCallExpr, null);
        selectList.addItem(selectListItem2);

        new Expectations() {
            {
                analyzer.getClusterName();
                result = "default";
                selectStmt.analyze(analyzer);
                selectStmt.getSelectList();
                result = selectList;
            }
        };
        CreateMaterializedViewStmt createMaterializedViewStmt = new CreateMaterializedViewStmt("test", selectStmt, null);
        try {
            createMaterializedViewStmt.analyze(analyzer);
            Assert.fail();
        } catch (UserException e) {
            System.out.print(e.getMessage());
        }
    }

    @Test
    public void testDuplicateColumn1(@Injectable SlotRef slotRef1,
                                     @Injectable SelectStmt selectStmt,
                                     @Injectable Column column2,
                                     @Injectable SlotDescriptor slotDescriptor,
                                     @Injectable AggregateFunction aggregateFunction) throws UserException {
        SelectList selectList = new SelectList();
        SelectListItem selectListItem1 = new SelectListItem(slotRef1, null);
        selectList.addItem(selectListItem1);
        TableName tableName = new TableName("db", "table");
        SlotRef slotRef2 = new SlotRef(tableName, "k2");
        Deencapsulation.setField(slotRef2, "desc", slotDescriptor);
        List<Expr> fn1Children = Lists.newArrayList(slotRef2);
        FunctionCallExpr functionCallExpr1 = new FunctionCallExpr("sum", fn1Children);
        Deencapsulation.setField(functionCallExpr1, "fn", aggregateFunction);
        SelectListItem selectListItem2 = new SelectListItem(functionCallExpr1, null);
        selectList.addItem(selectListItem2);
        FunctionCallExpr functionCallExpr2 = new FunctionCallExpr("max", fn1Children);
        Deencapsulation.setField(functionCallExpr2, "fn", aggregateFunction);
        SelectListItem selectListItem3 = new SelectListItem(functionCallExpr2, null);
        selectList.addItem(selectListItem3);

        new Expectations() {
            {
                analyzer.getClusterName();
                result = "default";
                selectStmt.analyze(analyzer);
                selectStmt.getSelectList();
                result = selectList;
                slotRef1.getColumnName();
                result = "k1";
                slotDescriptor.getColumn();
                result = column2;
                column2.getOriginType();
                result = Type.INT;
            }
        };
        CreateMaterializedViewStmt createMaterializedViewStmt = new CreateMaterializedViewStmt("test", selectStmt, null);
        try {
            createMaterializedViewStmt.analyze(analyzer);
            Assert.fail();
        } catch (UserException e) {
            System.out.print(e.getMessage());
        }
    }

    @Test
    public void testOrderByColumnsLessThenGroupByColumns(@Injectable SlotRef slotRef1,
                                                         @Injectable SlotRef slotRef2,
                                                         @Injectable TableRef tableRef,
                                                         @Injectable SelectStmt selectStmt,
                                                         @Injectable Column column3,
                                                         @Injectable SlotDescriptor slotDescriptor,
                                                         @Injectable AggregateFunction aggregateFunction) throws UserException {
        SelectList selectList = new SelectList();
        SelectListItem selectListItem1 = new SelectListItem(slotRef1, null);
        selectList.addItem(selectListItem1);
        SelectListItem selectListItem2 = new SelectListItem(slotRef2, null);
        selectList.addItem(selectListItem2);
        TableName tableName = new TableName("db", "table");
        SlotRef functionChild0 = new SlotRef(tableName, "v1");
        Deencapsulation.setField(functionChild0, "desc", slotDescriptor);
        List<Expr> fn1Children = Lists.newArrayList(functionChild0);
        FunctionCallExpr functionCallExpr = new FunctionCallExpr("sum", fn1Children);
        Deencapsulation.setField(functionCallExpr, "fn", aggregateFunction);
        SelectListItem selectListItem3 = new SelectListItem(functionCallExpr, null);
        selectList.addItem(selectListItem3);
        OrderByElement orderByElement1 = new OrderByElement(slotRef1, false, false);
        ArrayList<OrderByElement> orderByElementList = Lists.newArrayList(orderByElement1);

        new Expectations() {
            {
                analyzer.getClusterName();
                result = "default";
                selectStmt.analyze(analyzer);
                selectStmt.getSelectList();
                result = selectList;
                selectStmt.getTableRefs();
                result = Lists.newArrayList(tableRef);
                selectStmt.getWhereClause();
                result = null;
                selectStmt.getHavingPred();
                result = null;
                selectStmt.getOrderByElements();
                result = orderByElementList;
                slotRef1.getColumnName();
                result = "k1";
                slotRef2.getColumnName();
                result = "non-k2";
                slotDescriptor.getColumn();
                result = column3;
                column3.getOriginType();
                result = Type.INT;
            }
        };
        CreateMaterializedViewStmt createMaterializedViewStmt = new CreateMaterializedViewStmt("test", selectStmt, null);
        try {
            createMaterializedViewStmt.analyze(analyzer);
            Assert.fail();
        } catch (UserException e) {
            System.out.print(e.getMessage());
        }
    }

    @Test
    public void testMVColumnsWithoutOrderby(@Injectable SlotRef slotRef1,
                                            @Injectable SlotRef slotRef2,
                                            @Injectable SlotRef slotRef3,
                                            @Injectable SlotRef slotRef4,
                                            @Injectable TableRef tableRef,
                                            @Injectable SelectStmt selectStmt,
                                            @Injectable AggregateInfo aggregateInfo,
                                            @Injectable Column column5,
                                            @Injectable SlotDescriptor slotDescriptor,
                                            @Injectable AggregateFunction aggregateFunction) throws UserException {
        SelectList selectList = new SelectList();
        SelectListItem selectListItem1 = new SelectListItem(slotRef1, null);
        selectList.addItem(selectListItem1);
        SelectListItem selectListItem2 = new SelectListItem(slotRef2, null);
        selectList.addItem(selectListItem2);
        SelectListItem selectListItem3 = new SelectListItem(slotRef3, null);
        selectList.addItem(selectListItem3);
        SelectListItem selectListItem4 = new SelectListItem(slotRef4, null);
        selectList.addItem(selectListItem4);
        TableName tableName = new TableName("db", "table");
        final String columnName5 = "sum_v2";
        SlotRef functionChild0 = new SlotRef(tableName, columnName5);
        Deencapsulation.setField(functionChild0, "desc", slotDescriptor);
        List<Expr> fn1Children = Lists.newArrayList(functionChild0);
        FunctionCallExpr functionCallExpr = new FunctionCallExpr("sum", fn1Children);
        Deencapsulation.setField(functionCallExpr, "fn", aggregateFunction);
        SelectListItem selectListItem5 = new SelectListItem(functionCallExpr, null);
        selectList.addItem(selectListItem5);
        final String columnName1 = "k1";
        final String columnName2 = "k2";
        final String columnName3 = "k3";
        final String columnName4 = "v1";

        new Expectations() {
            {
                analyzer.getClusterName();
                result = "default";
                selectStmt.getAggInfo();
                result = aggregateInfo;
                selectStmt.getSelectList();
                result = selectList;
                selectStmt.getTableRefs();
                result = Lists.newArrayList(tableRef);
                selectStmt.getWhereClause();
                result = null;
                selectStmt.getHavingPred();
                result = null;
                selectStmt.getOrderByElements();
                result = null;
                selectStmt.getLimit();
                result = -1;
                selectStmt.analyze(analyzer);
                slotRef1.getColumnName();
                result = columnName1;
                slotRef2.getColumnName();
                result = columnName2;
                slotRef3.getColumnName();
                result = columnName3;
                slotRef4.getColumnName();
                result = columnName4;
                functionChild0.getColumn();
                result = column5;
                column5.getOriginType();
                result = Type.INT;
            }
        };


        CreateMaterializedViewStmt createMaterializedViewStmt = new CreateMaterializedViewStmt("test", selectStmt, null);
        try {
            createMaterializedViewStmt.analyze(analyzer);
            Assert.assertEquals(KeysType.AGG_KEYS, createMaterializedViewStmt.getMVKeysType());
            List<MVColumnItem> mvColumns = createMaterializedViewStmt.getMVColumnItemList();
            Assert.assertEquals(5, mvColumns.size());
            MVColumnItem mvColumn0 = mvColumns.get(0);
            Assert.assertTrue(mvColumn0.isKey());
            Assert.assertFalse(mvColumn0.isAggregationTypeImplicit());
            Assert.assertEquals(columnName1, mvColumn0.getName());
            Assert.assertEquals(null, mvColumn0.getAggregationType());
            MVColumnItem mvColumn1 = mvColumns.get(1);
            Assert.assertTrue(mvColumn1.isKey());
            Assert.assertFalse(mvColumn1.isAggregationTypeImplicit());
            Assert.assertEquals(columnName2, mvColumn1.getName());
            Assert.assertEquals(null, mvColumn1.getAggregationType());
            MVColumnItem mvColumn2 = mvColumns.get(2);
            Assert.assertTrue(mvColumn2.isKey());
            Assert.assertFalse(mvColumn2.isAggregationTypeImplicit());
            Assert.assertEquals(columnName3, mvColumn2.getName());
            Assert.assertEquals(null, mvColumn2.getAggregationType());
            MVColumnItem mvColumn3 = mvColumns.get(3);
            Assert.assertTrue(mvColumn3.isKey());
            Assert.assertFalse(mvColumn3.isAggregationTypeImplicit());
            Assert.assertEquals(columnName4, mvColumn3.getName());
            Assert.assertEquals(null, mvColumn3.getAggregationType());
            MVColumnItem mvColumn4 = mvColumns.get(4);
            Assert.assertFalse(mvColumn4.isKey());
            Assert.assertFalse(mvColumn4.isAggregationTypeImplicit());
            Assert.assertEquals(columnName5, mvColumn4.getName());
            Assert.assertEquals(AggregateType.SUM, mvColumn4.getAggregationType());
        } catch (UserException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testMVColumnsWithoutOrderbyWithoutAggregation(@Injectable SlotRef slotRef1,
                                                              @Injectable SlotRef slotRef2, @Injectable SlotRef slotRef3, @Injectable SlotRef slotRef4,
                                                              @Injectable TableRef tableRef, @Injectable SelectStmt selectStmt) throws UserException {
        SelectList selectList = new SelectList();
        SelectListItem selectListItem1 = new SelectListItem(slotRef1, null);
        selectList.addItem(selectListItem1);
        SelectListItem selectListItem2 = new SelectListItem(slotRef2, null);
        selectList.addItem(selectListItem2);
        SelectListItem selectListItem3 = new SelectListItem(slotRef3, null);
        selectList.addItem(selectListItem3);
        SelectListItem selectListItem4 = new SelectListItem(slotRef4, null);
        selectList.addItem(selectListItem4);

        final String columnName1 = "k1";
        final String columnName2 = "k2";
        final String columnName3 = "k3";
        final String columnName4 = "v1";

        new Expectations() {
            {
                analyzer.getClusterName();
                result = "default";
                selectStmt.getAggInfo();
                result = null;
                selectStmt.getSelectList();
                result = selectList;
                selectStmt.getTableRefs();
                result = Lists.newArrayList(tableRef);
                selectStmt.getWhereClause();
                result = null;
                selectStmt.getHavingPred();
                result = null;
                selectStmt.getOrderByElements();
                result = null;
                selectStmt.getLimit();
                result = -1;
                selectStmt.analyze(analyzer);
                slotRef1.getColumnName();
                result = columnName1;
                slotRef2.getColumnName();
                result = columnName2;
                slotRef3.getColumnName();
                result = columnName3;
                slotRef4.getColumnName();
                result = columnName4;
                slotRef1.getType().getIndexSize();
                result = 34;
                slotRef1.getType().getPrimitiveType();
                result = PrimitiveType.INT;
                slotRef2.getType().getIndexSize();
                result = 1;
                slotRef2.getType().getPrimitiveType();
                result = PrimitiveType.INT;
                slotRef3.getType().getIndexSize();
                result = 1;
                slotRef3.getType().getPrimitiveType();
                result = PrimitiveType.INT;
                slotRef4.getType().getIndexSize();
                result = 4;
                selectStmt.getAggInfo(); // return null, so that the mv can be a duplicate mv
                result = null;
            }
        };


        CreateMaterializedViewStmt createMaterializedViewStmt = new CreateMaterializedViewStmt("test", selectStmt, null);
        try {
            createMaterializedViewStmt.analyze(analyzer);
            Assert.assertEquals(KeysType.DUP_KEYS, createMaterializedViewStmt.getMVKeysType());
            List<MVColumnItem> mvColumns = createMaterializedViewStmt.getMVColumnItemList();
            Assert.assertEquals(4, mvColumns.size());
            MVColumnItem mvColumn0 = mvColumns.get(0);
            Assert.assertTrue(mvColumn0.isKey());
            Assert.assertFalse(mvColumn0.isAggregationTypeImplicit());
            Assert.assertEquals(columnName1, mvColumn0.getName());
            Assert.assertEquals(null, mvColumn0.getAggregationType());
            MVColumnItem mvColumn1 = mvColumns.get(1);
            Assert.assertTrue(mvColumn1.isKey());
            Assert.assertFalse(mvColumn1.isAggregationTypeImplicit());
            Assert.assertEquals(columnName2, mvColumn1.getName());
            Assert.assertEquals(null, mvColumn1.getAggregationType());
            MVColumnItem mvColumn2 = mvColumns.get(2);
            Assert.assertTrue(mvColumn2.isKey());
            Assert.assertFalse(mvColumn2.isAggregationTypeImplicit());
            Assert.assertEquals(columnName3, mvColumn2.getName());
            Assert.assertEquals(null, mvColumn2.getAggregationType());
            MVColumnItem mvColumn3 = mvColumns.get(3);
            Assert.assertFalse(mvColumn3.isKey());
            Assert.assertTrue(mvColumn3.isAggregationTypeImplicit());
            Assert.assertEquals(columnName4, mvColumn3.getName());
            Assert.assertEquals(AggregateType.NONE, mvColumn3.getAggregationType());
        } catch (UserException e) {
            Assert.fail(e.getMessage());
        }
    }

    /*
    ISSUE: #3811
     */
    @Test
    public void testMVColumnsWithoutOrderbyWithoutAggregationWithFloat(@Injectable SlotRef slotRef1,
                                                                       @Injectable SlotRef slotRef2, @Injectable SlotRef slotRef3, @Injectable SlotRef slotRef4,
                                                                       @Injectable TableRef tableRef, @Injectable SelectStmt selectStmt) throws UserException {
        SelectList selectList = new SelectList();
        SelectListItem selectListItem1 = new SelectListItem(slotRef1, null);
        selectList.addItem(selectListItem1);
        SelectListItem selectListItem2 = new SelectListItem(slotRef2, null);
        selectList.addItem(selectListItem2);
        SelectListItem selectListItem3 = new SelectListItem(slotRef3, null);
        selectList.addItem(selectListItem3);
        SelectListItem selectListItem4 = new SelectListItem(slotRef4, null);
        selectList.addItem(selectListItem4);

        final String columnName1 = "k1";
        final String columnName2 = "k2";
        final String columnName3 = "v1";
        final String columnName4 = "v2";

        new Expectations() {
            {
                analyzer.getClusterName();
                result = "default";
                selectStmt.getAggInfo();
                result = null;
                selectStmt.getSelectList();
                result = selectList;
                selectStmt.getTableRefs();
                result = Lists.newArrayList(tableRef);
                selectStmt.getWhereClause();
                result = null;
                selectStmt.getHavingPred();
                result = null;
                selectStmt.getOrderByElements();
                result = null;
                selectStmt.getLimit();
                result = -1;
                selectStmt.analyze(analyzer);
                slotRef1.getColumnName();
                result = columnName1;
                slotRef2.getColumnName();
                result = columnName2;
                slotRef3.getColumnName();
                result = columnName3;
                slotRef4.getColumnName();
                result = columnName4;
                slotRef1.getType().getIndexSize();
                result = 1;
                slotRef1.getType().getPrimitiveType();
                result = PrimitiveType.INT;
                slotRef2.getType().getIndexSize();
                result = 2;
                slotRef2.getType().getPrimitiveType();
                result = PrimitiveType.INT;
                slotRef3.getType().getIndexSize();
                result = 3;
                slotRef3.getType().isFloatingPointType();
                result = true;
                selectStmt.getAggInfo(); // return null, so that the mv can be a duplicate mv
                result = null;
            }
        };


        CreateMaterializedViewStmt createMaterializedViewStmt = new CreateMaterializedViewStmt("test", selectStmt, null);
        try {
            createMaterializedViewStmt.analyze(analyzer);
            Assert.assertEquals(KeysType.DUP_KEYS, createMaterializedViewStmt.getMVKeysType());
            List<MVColumnItem> mvColumns = createMaterializedViewStmt.getMVColumnItemList();
            Assert.assertEquals(4, mvColumns.size());
            MVColumnItem mvColumn0 = mvColumns.get(0);
            Assert.assertTrue(mvColumn0.isKey());
            Assert.assertFalse(mvColumn0.isAggregationTypeImplicit());
            Assert.assertEquals(columnName1, mvColumn0.getName());
            Assert.assertEquals(null, mvColumn0.getAggregationType());
            MVColumnItem mvColumn1 = mvColumns.get(1);
            Assert.assertTrue(mvColumn1.isKey());
            Assert.assertFalse(mvColumn1.isAggregationTypeImplicit());
            Assert.assertEquals(columnName2, mvColumn1.getName());
            Assert.assertEquals(null, mvColumn1.getAggregationType());
            MVColumnItem mvColumn2 = mvColumns.get(2);
            Assert.assertFalse(mvColumn2.isKey());
            Assert.assertTrue(mvColumn2.isAggregationTypeImplicit());
            Assert.assertEquals(columnName3, mvColumn2.getName());
            Assert.assertEquals(AggregateType.NONE, mvColumn2.getAggregationType());
            MVColumnItem mvColumn3 = mvColumns.get(3);
            Assert.assertFalse(mvColumn3.isKey());
            Assert.assertTrue(mvColumn3.isAggregationTypeImplicit());
            Assert.assertEquals(columnName4, mvColumn3.getName());
            Assert.assertEquals(AggregateType.NONE, mvColumn3.getAggregationType());
        } catch (UserException e) {
            Assert.fail(e.getMessage());
        }
    }

    /*
    ISSUE: #3811
    */
    @Test
    public void testMVColumnsWithoutOrderbyWithoutAggregationWithVarchar(@Injectable SlotRef slotRef1,
                                                                         @Injectable SlotRef slotRef2, @Injectable SlotRef slotRef3, @Injectable SlotRef slotRef4,
                                                                         @Injectable TableRef tableRef, @Injectable SelectStmt selectStmt) throws UserException {
        SelectList selectList = new SelectList();
        SelectListItem selectListItem1 = new SelectListItem(slotRef1, null);
        selectList.addItem(selectListItem1);
        SelectListItem selectListItem2 = new SelectListItem(slotRef2, null);
        selectList.addItem(selectListItem2);
        SelectListItem selectListItem3 = new SelectListItem(slotRef3, null);
        selectList.addItem(selectListItem3);
        SelectListItem selectListItem4 = new SelectListItem(slotRef4, null);
        selectList.addItem(selectListItem4);

        final String columnName1 = "k1";
        final String columnName2 = "k2";
        final String columnName3 = "v1";
        final String columnName4 = "v2";

        new Expectations() {
            {
                analyzer.getClusterName();
                result = "default";
                selectStmt.getAggInfo();
                result = null;
                selectStmt.getSelectList();
                result = selectList;
                selectStmt.getTableRefs();
                result = Lists.newArrayList(tableRef);
                selectStmt.getWhereClause();
                result = null;
                selectStmt.getHavingPred();
                result = null;
                selectStmt.getOrderByElements();
                result = null;
                selectStmt.getLimit();
                result = -1;
                selectStmt.analyze(analyzer);
                slotRef1.getColumnName();
                result = columnName1;
                slotRef2.getColumnName();
                result = columnName2;
                slotRef3.getColumnName();
                result = columnName3;
                slotRef4.getColumnName();
                result = columnName4;
                slotRef1.getType().getIndexSize();
                result = 1;
                slotRef1.getType().getPrimitiveType();
                result = PrimitiveType.INT;
                slotRef2.getType().getIndexSize();
                result = 2;
                slotRef2.getType().getPrimitiveType();
                result = PrimitiveType.INT;
                slotRef3.getType().getIndexSize();
                result = 3;
                slotRef3.getType().getPrimitiveType();
                result = PrimitiveType.VARCHAR;
                selectStmt.getAggInfo(); // return null, so that the mv can be a duplicate mv
                result = null;
            }
        };


        CreateMaterializedViewStmt createMaterializedViewStmt = new CreateMaterializedViewStmt("test", selectStmt, null);
        try {
            createMaterializedViewStmt.analyze(analyzer);
            Assert.assertEquals(KeysType.DUP_KEYS, createMaterializedViewStmt.getMVKeysType());
            List<MVColumnItem> mvColumns = createMaterializedViewStmt.getMVColumnItemList();
            Assert.assertEquals(4, mvColumns.size());
            MVColumnItem mvColumn0 = mvColumns.get(0);
            Assert.assertTrue(mvColumn0.isKey());
            Assert.assertFalse(mvColumn0.isAggregationTypeImplicit());
            Assert.assertEquals(columnName1, mvColumn0.getName());
            Assert.assertEquals(null, mvColumn0.getAggregationType());
            MVColumnItem mvColumn1 = mvColumns.get(1);
            Assert.assertTrue(mvColumn1.isKey());
            Assert.assertFalse(mvColumn1.isAggregationTypeImplicit());
            Assert.assertEquals(columnName2, mvColumn1.getName());
            Assert.assertEquals(null, mvColumn1.getAggregationType());
            MVColumnItem mvColumn2 = mvColumns.get(2);
            Assert.assertTrue(mvColumn2.isKey());
            Assert.assertFalse(mvColumn2.isAggregationTypeImplicit());
            Assert.assertEquals(columnName3, mvColumn2.getName());
            Assert.assertEquals(null, mvColumn2.getAggregationType());
            MVColumnItem mvColumn3 = mvColumns.get(3);
            Assert.assertFalse(mvColumn3.isKey());
            Assert.assertTrue(mvColumn3.isAggregationTypeImplicit());
            Assert.assertEquals(columnName4, mvColumn3.getName());
            Assert.assertEquals(AggregateType.NONE, mvColumn3.getAggregationType());
        } catch (UserException e) {
            Assert.fail(e.getMessage());
        }
    }

    /*
    ISSUE: #3811
     */
    @Test
    public void testMVColumnsWithFirstFloat(@Injectable SlotRef slotRef1,
                                            @Injectable TableRef tableRef, @Injectable SelectStmt selectStmt) throws UserException {
        SelectList selectList = new SelectList();
        SelectListItem selectListItem1 = new SelectListItem(slotRef1, null);
        selectList.addItem(selectListItem1);

        final String columnName1 = "k1";

        new Expectations() {
            {
                analyzer.getClusterName();
                result = "default";
                selectStmt.getAggInfo();
                result = null;
                selectStmt.getSelectList();
                result = selectList;
                selectStmt.getTableRefs();
                result = Lists.newArrayList(tableRef);
                selectStmt.getWhereClause();
                result = null;
                selectStmt.getHavingPred();
                result = null;
                selectStmt.getOrderByElements();
                result = null;
                selectStmt.analyze(analyzer);
                slotRef1.getColumnName();
                result = columnName1;
                slotRef1.getType().isFloatingPointType();
                result = true;
                selectStmt.getAggInfo(); // return null, so that the mv can be a duplicate mv
                result = null;
            }
        };


        CreateMaterializedViewStmt createMaterializedViewStmt = new CreateMaterializedViewStmt("test", selectStmt, null);
        try {
            createMaterializedViewStmt.analyze(analyzer);
            Assert.fail("The first column could not be float or double, use decimal instead");
        } catch (UserException e) {
            System.out.print(e.getMessage());
        }
    }

    /*
    ISSUE: #3811
    */
    @Test
    public void testMVColumnsWithFirstVarchar(@Injectable SlotRef slotRef1,
                                              @Injectable TableRef tableRef, @Injectable SelectStmt selectStmt) throws UserException {
        SelectList selectList = new SelectList();
        SelectListItem selectListItem1 = new SelectListItem(slotRef1, null);
        selectList.addItem(selectListItem1);

        final String columnName1 = "k1";

        new Expectations() {
            {
                analyzer.getClusterName();
                result = "default";
                selectStmt.getAggInfo();
                result = null;
                selectStmt.getSelectList();
                result = selectList;
                selectStmt.getTableRefs();
                result = Lists.newArrayList(tableRef);
                selectStmt.getWhereClause();
                result = null;
                selectStmt.getHavingPred();
                result = null;
                selectStmt.getOrderByElements();
                result = null;
                selectStmt.getLimit();
                result = -1;
                selectStmt.analyze(analyzer);
                slotRef1.getColumnName();
                result = columnName1;
                slotRef1.getType().getPrimitiveType();
                result = PrimitiveType.VARCHAR;
            }
        };


        CreateMaterializedViewStmt createMaterializedViewStmt = new CreateMaterializedViewStmt("test", selectStmt, null);
        try {
            createMaterializedViewStmt.analyze(analyzer);
            Assert.assertEquals(KeysType.DUP_KEYS, createMaterializedViewStmt.getMVKeysType());
            List<MVColumnItem> mvColumns = createMaterializedViewStmt.getMVColumnItemList();
            Assert.assertEquals(1, mvColumns.size());
            MVColumnItem mvColumn0 = mvColumns.get(0);
            Assert.assertTrue(mvColumn0.isKey());
            Assert.assertFalse(mvColumn0.isAggregationTypeImplicit());
            Assert.assertEquals(columnName1, mvColumn0.getName());
            Assert.assertEquals(null, mvColumn0.getAggregationType());
        } catch (UserException e) {
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void testMVColumns(@Injectable SlotRef slotRef1,
                              @Injectable SlotRef slotRef2,
                              @Injectable TableRef tableRef,
                              @Injectable SelectStmt selectStmt,
                              @Injectable AggregateInfo aggregateInfo,
                              @Injectable Column column1,
                              @Injectable SlotDescriptor slotDescriptor,
                              @Injectable AggregateFunction aggregateFunction) throws UserException {
        SelectList selectList = new SelectList();
        SelectListItem selectListItem1 = new SelectListItem(slotRef1, null);
        selectList.addItem(selectListItem1);
        SelectListItem selectListItem2 = new SelectListItem(slotRef2, null);
        selectList.addItem(selectListItem2);
        TableName tableName = new TableName("db", "table");
        final String columnName3 = "sum_v2";
        SlotRef slotRef = new SlotRef(tableName, columnName3);
        Deencapsulation.setField(slotRef, "desc", slotDescriptor);
        List<Expr> children = Lists.newArrayList(slotRef);
        FunctionCallExpr functionCallExpr = new FunctionCallExpr("sum", children);
        Deencapsulation.setField(functionCallExpr, "fn", aggregateFunction);
        SelectListItem selectListItem3 = new SelectListItem(functionCallExpr, null);
        selectList.addItem(selectListItem3);
        OrderByElement orderByElement1 = new OrderByElement(slotRef1, false, false);
        OrderByElement orderByElement2 = new OrderByElement(slotRef2, false, false);
        ArrayList<OrderByElement> orderByElementList = Lists.newArrayList(orderByElement1, orderByElement2);
        final String columnName1 = "k1";
        final String columnName2 = "v1";

        new Expectations() {
            {
                analyzer.getClusterName();
                result = "default";
                selectStmt.getAggInfo();
                result = aggregateInfo;
                selectStmt.getSelectList();
                result = selectList;
                selectStmt.getTableRefs();
                result = Lists.newArrayList(tableRef);
                selectStmt.getWhereClause();
                result = null;
                selectStmt.getHavingPred();
                result = null;
                selectStmt.getOrderByElements();
                result = orderByElementList;
                selectStmt.getLimit();
                result = -1;
                selectStmt.analyze(analyzer);
                slotRef1.getColumnName();
                result = columnName1;
                slotRef2.getColumnName();
                result = columnName2;
                slotDescriptor.getColumn();
                result = column1;
                column1.getOriginType();
                result = Type.INT;
            }
        };

        CreateMaterializedViewStmt createMaterializedViewStmt = new CreateMaterializedViewStmt("test", selectStmt, null);
        try {
            createMaterializedViewStmt.analyze(analyzer);
            Assert.assertEquals(KeysType.AGG_KEYS, createMaterializedViewStmt.getMVKeysType());
            List<MVColumnItem> mvColumns = createMaterializedViewStmt.getMVColumnItemList();
            Assert.assertEquals(3, mvColumns.size());
            MVColumnItem mvColumn0 = mvColumns.get(0);
            Assert.assertTrue(mvColumn0.isKey());
            Assert.assertFalse(mvColumn0.isAggregationTypeImplicit());
            Assert.assertEquals(columnName1, mvColumn0.getName());
            Assert.assertEquals(null, mvColumn0.getAggregationType());
            MVColumnItem mvColumn1 = mvColumns.get(1);
            Assert.assertTrue(mvColumn1.isKey());
            Assert.assertFalse(mvColumn1.isAggregationTypeImplicit());
            Assert.assertEquals(columnName2, mvColumn1.getName());
            Assert.assertEquals(null, mvColumn1.getAggregationType());
            MVColumnItem mvColumn2 = mvColumns.get(2);
            Assert.assertFalse(mvColumn2.isKey());
            Assert.assertFalse(mvColumn2.isAggregationTypeImplicit());
            Assert.assertEquals(columnName3, mvColumn2.getName());
            Assert.assertEquals(AggregateType.SUM, mvColumn2.getAggregationType());
            Assert.assertEquals(KeysType.AGG_KEYS, createMaterializedViewStmt.getMVKeysType());
        } catch (UserException e) {
            Assert.fail(e.getMessage());
        }

    }

    @Test
    public void testDeduplicateMV(@Injectable SlotRef slotRef1,
                                  @Injectable TableRef tableRef,
                                  @Injectable SelectStmt selectStmt,
                                  @Injectable AggregateInfo aggregateInfo) throws UserException {
        SelectList selectList = new SelectList();
        SelectListItem selectListItem1 = new SelectListItem(slotRef1, null);
        selectList.addItem(selectListItem1);
        final String columnName1 = "k1";
        new Expectations() {
            {
                analyzer.getClusterName();
                result = "default";
                selectStmt.getAggInfo();
                result = aggregateInfo;
                selectStmt.getSelectList();
                result = selectList;
                selectStmt.analyze(analyzer);
                selectStmt.getTableRefs();
                result = Lists.newArrayList(tableRef);
                selectStmt.getWhereClause();
                result = null;
                slotRef1.getColumnName();
                result = columnName1;
                selectStmt.getHavingPred();
                result = null;
                selectStmt.getLimit();
                result = -1;
            }
        };

        CreateMaterializedViewStmt createMaterializedViewStmt = new CreateMaterializedViewStmt("test", selectStmt, null);
        try {
            createMaterializedViewStmt.analyze(analyzer);
            Assert.assertEquals(KeysType.AGG_KEYS, createMaterializedViewStmt.getMVKeysType());
            List<MVColumnItem> mvSchema = createMaterializedViewStmt.getMVColumnItemList();
            Assert.assertEquals(1, mvSchema.size());
            Assert.assertTrue(mvSchema.get(0).isKey());
        } catch (UserException e) {
            Assert.fail(e.getMessage());
        }

    }

    @Test
    public void testBuildMVColumnItem(@Injectable SelectStmt selectStmt,
                                      @Injectable Column column1,
                                      @Injectable Column column2,
                                      @Injectable Column column3,
                                      @Injectable Column column4,
                                      @Injectable SlotDescriptor slotDescriptor1,
                                      @Injectable SlotDescriptor slotDescriptor2,
                                      @Injectable SlotDescriptor slotDescriptor3,
                                      @Injectable SlotDescriptor slotDescriptor4) {
        CreateMaterializedViewStmt createMaterializedViewStmt = new CreateMaterializedViewStmt("test", selectStmt, null);
        SlotRef slotRef = new SlotRef(new TableName("db", "table"), "a");
        List<Expr> params = Lists.newArrayList();
        params.add(slotRef);
        FunctionCallExpr functionCallExpr = new FunctionCallExpr("sum", params);
        Deencapsulation.setField(slotRef, "desc", slotDescriptor1);
        new Expectations() {
            {
                slotDescriptor1.getColumn();
                result = column1;
                column1.getOriginType();
                result = Type.LARGEINT;
            }
        };
        MVColumnItem mvColumnItem = Deencapsulation.invoke(createMaterializedViewStmt, "buildMVColumnItem", functionCallExpr);
        Assert.assertEquals(Type.LARGEINT, mvColumnItem.getType());

        SlotRef slotRef2 = new SlotRef(new TableName("db", "table"), "a");
        List<Expr> params2 = Lists.newArrayList();
        params2.add(slotRef2);
        FunctionCallExpr functionCallExpr2 = new FunctionCallExpr("sum", params2);
        Deencapsulation.setField(slotRef2, "desc", slotDescriptor2);
        new Expectations() {
            {
                slotDescriptor2.getColumn();
                result = column2;
                column2.getOriginType();
                result = Type.BIGINT;
            }
        };
        MVColumnItem mvColumnItem2 = Deencapsulation.invoke(createMaterializedViewStmt, "buildMVColumnItem", functionCallExpr2);
        Assert.assertEquals(Type.BIGINT, mvColumnItem2.getType());

        SlotRef slotRef3 = new SlotRef(new TableName("db", "table"), "a");
        List<Expr> params3 = Lists.newArrayList();
        params3.add(slotRef3);
        FunctionCallExpr functionCallExpr3 = new FunctionCallExpr("min", params3);
        Deencapsulation.setField(slotRef3, "desc", slotDescriptor3);
        new Expectations() {
            {
                slotDescriptor3.getColumn();
                result = column3;
                column3.getOriginType();
                result = Type.VARCHAR;
            }
        };
        MVColumnItem mvColumnItem3 = Deencapsulation.invoke(createMaterializedViewStmt, "buildMVColumnItem", functionCallExpr3);
        Assert.assertEquals(Type.VARCHAR, mvColumnItem3.getType());

        SlotRef slotRef4 = new SlotRef(new TableName("db", "table"), "a");
        List<Expr> params4 = Lists.newArrayList();
        params4.add(slotRef4);
        FunctionCallExpr functionCallExpr4 = new FunctionCallExpr("sum", params4);
        Deencapsulation.setField(slotRef4, "desc", slotDescriptor4);
        new Expectations() {
            {
                slotDescriptor4.getColumn();
                result = column4;
                column4.getOriginType();
                result = Type.DOUBLE;
            }
        };
        MVColumnItem mvColumnItem4 = Deencapsulation.invoke(createMaterializedViewStmt, "buildMVColumnItem", functionCallExpr4);
        Assert.assertEquals(Type.DOUBLE, mvColumnItem4.getType());

    }

    @Test
    public void testKeepScaleAndPrecisionOfType(@Injectable SelectStmt selectStmt,
                                                @Injectable SlotDescriptor slotDescriptor1,
                                                @Injectable Column column1,
                                                @Injectable SlotDescriptor slotDescriptor2,
                                                @Injectable Column column2,
                                                @Injectable SlotDescriptor slotDescriptor3,
                                                @Injectable Column column3) {
        CreateMaterializedViewStmt createMaterializedViewStmt = new CreateMaterializedViewStmt("test", selectStmt, null);
        SlotRef slotRef = new SlotRef(new TableName("db", "table"), "a");
        List<Expr> params = Lists.newArrayList();
        params.add(slotRef);
        FunctionCallExpr functionCallExpr = new FunctionCallExpr("min", params);
        Deencapsulation.setField(slotRef, "desc", slotDescriptor1);
        new Expectations() {
            {
                slotDescriptor1.getColumn();
                result = column1;
                column1.getOriginType();
                result = ScalarType.createVarchar(50);
            }
        };
        MVColumnItem mvColumnItem = Deencapsulation.invoke(createMaterializedViewStmt, "buildMVColumnItem", functionCallExpr);
        Assert.assertEquals(50, ((ScalarType)mvColumnItem.getType()).getLength());

        SlotRef slotRef2 = new SlotRef(new TableName("db", "table"), "a");
        List<Expr> params2 = Lists.newArrayList();
        params2.add(slotRef2);
        FunctionCallExpr functionCallExpr2 = new FunctionCallExpr("min", params2);
        Deencapsulation.setField(slotRef2, "desc", slotDescriptor2);
        new Expectations() {
            {
                slotDescriptor2.getColumn();
                result = column2;
                column2.getOriginType();
                result = ScalarType.createDecimalV2Type(10,1);
            }
        };
        MVColumnItem mvColumnItem2 = Deencapsulation.invoke(createMaterializedViewStmt, "buildMVColumnItem", functionCallExpr2);
        Assert.assertEquals(new Integer(10), ((ScalarType)mvColumnItem2.getType()).getPrecision());
        Assert.assertEquals(1, ((ScalarType)mvColumnItem2.getType()).getScalarScale());

        SlotRef slotRef3 = new SlotRef(new TableName("db", "table"), "a");
        List<Expr> params3 = Lists.newArrayList();
        params3.add(slotRef3);
        FunctionCallExpr functionCallExpr3 = new FunctionCallExpr("min", params3);
        Deencapsulation.setField(slotRef3, "desc", slotDescriptor3);
        new Expectations() {
            {
                slotDescriptor3.getColumn();
                result = column3;
                column3.getOriginType();
                result = ScalarType.createChar(5);
            }
        };
        MVColumnItem mvColumnItem3 = Deencapsulation.invoke(createMaterializedViewStmt, "buildMVColumnItem", functionCallExpr3);
        Assert.assertEquals(5, ((ScalarType)mvColumnItem3.getType()).getLength());
    }
}

