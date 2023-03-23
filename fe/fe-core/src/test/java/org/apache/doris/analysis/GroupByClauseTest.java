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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.InternalCatalog;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class GroupByClauseTest {
    private static final String internalCtl = InternalCatalog.INTERNAL_CATALOG_NAME;

    private Analyzer analyzer;

    @Before
    public void setUp() throws AnalysisException {
        Analyzer analyzerBase = AccessTestUtil.fetchTableAnalyzer();
        analyzer = new Analyzer(analyzerBase.getEnv(), analyzerBase.getContext());
        try {
            Field f = analyzer.getClass().getDeclaredField("tupleByAlias");
            f.setAccessible(true);
            Multimap<String, TupleDescriptor> tupleByAlias = ArrayListMultimap.create();
            TupleDescriptor td = new TupleDescriptor(new TupleId(0));
            td.setTable(analyzerBase.getTableOrAnalysisException(new TableName(internalCtl, "testdb", "t")));
            tupleByAlias.put("testdb.t", td);
            f.set(analyzer, tupleByAlias);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testGroupingSets() {
        List<ArrayList<Expr>> groupingExprsList = new ArrayList<>();
        ArrayList<Expr> groupByExprs = new ArrayList<>();
        String[][] colsLists = {
                {"k3", "k1"},
                {"k2", "k3", "k2"},
                {"k1", "k3"},
                {"k4"},
                {"k1", "k2", "k3", "k4"}
        };
        for (String[] colsList : colsLists) {
            ArrayList<Expr> exprList = new ArrayList<>();
            for (String col : colsList) {
                exprList.add(new SlotRef(new TableName(internalCtl, "testdb", "t"), col));
            }
            groupingExprsList.add(exprList);
        }
        String[] groupByCols = {"k1", "k2", "k3", "k4"};
        for (String col : groupByCols) {
            groupByExprs.add(new SlotRef(new TableName(internalCtl, "testdb", "t"), col));
        }
        GroupByClause groupByClause = new GroupByClause(groupingExprsList,
                GroupByClause.GroupingType.GROUPING_SETS);
        GroupingInfo groupingInfo = null;
        try {
            groupByClause.genGroupingExprs();
            groupByClause.analyze(analyzer);
            groupingInfo = new GroupingInfo(analyzer, groupByClause);
            groupingInfo.buildRepeat(groupByClause.getGroupingExprs(), groupByClause.getGroupingSetList());
        } catch (AnalysisException exception) {
            exception.printStackTrace();
            Assert.assertTrue(false);
        }
        Assert.assertEquals(5, groupByClause.getGroupingExprs().size());

        Assert.assertEquals("GROUPING SETS ((`testdb`.`t`.`k3`, `testdb`.`t`.`k1`), (`testdb`.`t`.`k2`, `testdb`.`t`"
                + ".`k3`, `testdb`.`t`.`k2`), (`testdb`.`t`.`k1`, `testdb`.`t`.`k3`), (`testdb`.`t`.`k4`), (`testdb`"
                + ".`t`.`k1`, `testdb`.`t`.`k2`, `testdb`.`t`.`k3`, `testdb`.`t`.`k4`))", groupByClause.toSql());
        List<BitSet> bitSetList = groupingInfo.getGroupingIdList();
        { // CHECKSTYLE IGNORE THIS LINE
            String[] answer = {"{0, 1, 2, 3}", "{0, 1}", "{0, 2}", "{3}"};
            Set<String> answerSet = new HashSet<String>(Arrays.asList(answer));
            Set<String> resultSet = new HashSet<>();
            for (BitSet aBitSetList : bitSetList) {
                String s = aBitSetList.toString();
                resultSet.add(s);
            }
            Assert.assertEquals(answerSet, resultSet);
        } // CHECKSTYLE IGNORE THIS LINE
    }

    @Test
    public void testRollUp() {
        ArrayList<Expr> groupingExprs = new ArrayList<>();
        String[] cols = {"k2", "k3", "k4", "k3"};
        for (String col : cols) {
            Expr expr = new SlotRef(new TableName(internalCtl, "testdb", "t"), col);
            groupingExprs.add(expr);
        }

        GroupByClause groupByClause =
                new GroupByClause(
                        Expr.cloneList(groupingExprs),
                        GroupByClause.GroupingType.ROLLUP);
        GroupingInfo groupingInfo = null;
        try {
            groupByClause.genGroupingExprs();
            groupByClause.analyze(analyzer);
            groupingInfo = new GroupingInfo(analyzer, groupByClause);
            groupingInfo.buildRepeat(groupByClause.getGroupingExprs(), groupByClause.getGroupingSetList());
        } catch (AnalysisException execption) {
            Assert.assertTrue(false);
        }
        Assert.assertEquals(4, groupByClause.getGroupingExprs().size());
        Assert.assertEquals("ROLLUP (`testdb`.`t`.`k2`, `testdb`.`t`.`k3`, "
                + "`testdb`.`t`.`k4`, `testdb`.`t`.`k3`)", groupByClause.toSql());
        List<BitSet> bitSetList = groupingInfo.getGroupingIdList();
        { // CHECKSTYLE IGNORE THIS LINE
            String[] answer = {"{}", "{0}", "{0, 1}", "{0, 1, 2}"};
            Set<String> answerSet = new HashSet<String>(Arrays.asList(answer));
            Set<String> resultSet = new HashSet<>();
            for (BitSet aBitSetList : bitSetList) {
                String s = aBitSetList.toString();
                resultSet.add(s);
            }
            Assert.assertEquals(answerSet, resultSet);
        } // CHECKSTYLE IGNORE THIS LINE
    }

    @Test
    public void testCube() {
        ArrayList<Expr> groupingExprs = new ArrayList<>();
        String[] cols = {"k1", "k2", "k3", "k1"};
        for (String col : cols) {
            Expr expr = new SlotRef(new TableName(internalCtl, "testdb", "t"), col);
            groupingExprs.add(expr);
        }

        GroupByClause groupByClause = new GroupByClause(Expr.cloneList(groupingExprs),
                GroupByClause.GroupingType.CUBE);
        GroupingInfo groupingInfo = null;
        try {
            groupByClause.genGroupingExprs();
            groupByClause.analyze(analyzer);
            groupingInfo = new GroupingInfo(analyzer, groupByClause);
            groupingInfo.buildRepeat(groupByClause.getGroupingExprs(), groupByClause.getGroupingSetList());
        } catch (AnalysisException exception) {
            Assert.assertTrue(false);
        }
        Assert.assertEquals("CUBE (`testdb`.`t`.`k1`, `testdb`.`t`.`k2`, "
                + "`testdb`.`t`.`k3`, `testdb`.`t`.`k1`)", groupByClause.toSql());
        Assert.assertEquals(4, groupByClause.getGroupingExprs().size());

        List<BitSet> bitSetList = groupingInfo.getGroupingIdList();
        { // CHECKSTYLE IGNORE THIS LINE
            String[] answer = {"{}", "{1}", "{0}", "{0, 1}", "{2}", "{1, 2}", "{0, 1, 2}", "{0, 2}"};
            Set<String> answerSet = new HashSet<String>(Arrays.asList(answer));
            Set<String> resultSet = new HashSet<>();
            for (BitSet aBitSetList : bitSetList) {
                String s = aBitSetList.toString();
                resultSet.add(s);
            }

            Assert.assertEquals(answerSet, resultSet);
        } // CHECKSTYLE IGNORE THIS LINE
    }

    @Test
    public void testGroupBy() {
        ArrayList<Expr> groupingExprs = new ArrayList<>();
        String[] cols = {"k2", "k2", "k3", "k1"};
        for (String col : cols) {
            Expr expr = new SlotRef(new TableName(internalCtl, "testdb", "t"), col);
            groupingExprs.add(expr);
        }

        GroupByClause groupByClause = new GroupByClause(Expr.cloneList(groupingExprs),
                GroupByClause.GroupingType.GROUP_BY);
        try {
            groupByClause.analyze(analyzer);
        } catch (AnalysisException exception) {
            Assert.assertTrue(false);
        }
        Assert.assertEquals("`testdb`.`t`.`k2`, `testdb`.`t`.`k2`, `testdb`.`t`.`k3`, `testdb`.`t`.`k1`", groupByClause.toSql());
        Assert.assertEquals(3, groupByClause.getGroupingExprs().size());
        groupingExprs.remove(0);
        Assert.assertEquals(groupByClause.getGroupingExprs(), groupingExprs);
    }

    @Test
    public void testReset() {
        ArrayList<Expr> groupingExprs = new ArrayList<>();
        String[] cols = {"k2", "k2", "k3", "k1"};
        for (String col : cols) {
            Expr expr = new SlotRef(new TableName(internalCtl, "testdb", "t"), col);
            groupingExprs.add(expr);
        }

        GroupByClause groupByClause = new GroupByClause(Expr.cloneList(groupingExprs),
                GroupByClause.GroupingType.GROUP_BY);
        try {
            groupByClause.analyze(analyzer);
        } catch (AnalysisException exception) {
            Assert.assertTrue(false);
        }
        try {
            groupByClause.reset();
        } catch (Exception e) {
            Assert.fail("reset throw exceptions!" + e);
        }
    }

    @Test
    public void testGetTuple() throws AnalysisException {
        ArrayList<Expr> groupingExprs = new ArrayList<>();
        String[] cols = {"k1", "k2", "k3", "k1"};
        for (String col : cols) {
            Expr expr = new SlotRef(new TableName(internalCtl, "testdb", "t"), col);
            groupingExprs.add(expr);
        }
        GroupByClause groupByClause = new GroupByClause(Expr.cloneList(groupingExprs),
                GroupByClause.GroupingType.GROUP_BY);
        try {
            groupByClause.analyze(analyzer);
        } catch (AnalysisException exception) {
            exception.printStackTrace();
            Assert.assertTrue(false);
        }
    }

    @Test
    public void testGenGroupingList() throws AnalysisException {
        ArrayList<Expr> groupingExprs = new ArrayList<>();
        String[] cols = {"k1", "k2", "k3"};
        for (String col : cols) {
            Expr expr = new SlotRef(new TableName(internalCtl, "testdb", "t"), col);
            groupingExprs.add(expr);
        }

        GroupByClause groupByClause = new GroupByClause(Expr.cloneList(groupingExprs),
                GroupByClause.GroupingType.CUBE);
        List<Expr> slots = new ArrayList<>();
        for (String col : cols) {
            SlotRef expr = new SlotRef(new TableName(internalCtl, "testdb", "t"), col);
            slots.add(expr);
        }
        GroupingInfo groupingInfo = null;
        try {
            groupByClause.genGroupingExprs();
            groupByClause.analyze(analyzer);
            groupingInfo = new GroupingInfo(analyzer, groupByClause);
            groupingInfo.addGroupingSlots(slots, analyzer);
            groupingInfo.buildRepeat(groupByClause.getGroupingExprs(), groupByClause.getGroupingSetList());
        } catch (AnalysisException exception) {
            Assert.assertTrue(false);
        }
        List<List<Long>> list = groupingInfo.genGroupingList(groupByClause.getGroupingExprs());
        Assert.assertEquals(2, list.size());
        Assert.assertEquals(list.get(0), list.get(1));
    }
}
