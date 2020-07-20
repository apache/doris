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

package org.apache.doris.planner;

import org.apache.doris.analysis.AggregateInfo;
import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.SelectStmt;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TableName;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.jmockit.Deencapsulation;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import mockit.Expectations;
import mockit.Injectable;

public class MaterializedViewSelectorTest {


    @Test
    public void initTest(@Injectable SelectStmt selectStmt,
                         @Injectable SlotDescriptor tableAColumn1Desc,
                         @Injectable AggregateInfo aggregateInfo,
                         @Injectable Table tableA,
                         @Injectable TupleDescriptor tableADesc,
                         @Injectable SlotDescriptor tableAColumn2Desc,
                         @Injectable SlotDescriptor tableBColumn1Desc,
                         @Injectable TupleDescriptor tableBDesc,
                         @Injectable Table tableB,
                         @Injectable Analyzer analyzer) {
        TableName tableAName = new TableName("test", "tableA");
        TableName tableBName = new TableName("test", "tableB");
        SlotRef tableAColumn1 = new SlotRef(tableAName, "c1");
        Deencapsulation.setField(tableAColumn1, "isAnalyzed", true);
        SlotRef tableAColumn2 = new SlotRef(tableAName, "c2");
        Deencapsulation.setField(tableAColumn2, "isAnalyzed", true);
        SlotRef tableBColumn1 = new SlotRef(tableBName, "c1");
        Deencapsulation.setField(tableBColumn1, "isAnalyzed", true);
        Deencapsulation.setField(tableAColumn1, "desc", tableAColumn1Desc);
        Deencapsulation.setField(tableAColumn2, "desc", tableAColumn2Desc);
        Deencapsulation.setField(tableBColumn1, "desc", tableBColumn1Desc);
        FunctionCallExpr tableAColumn2Sum = new FunctionCallExpr("SUM", Lists.newArrayList(tableAColumn2));
        FunctionCallExpr tableBColumn1Max = new FunctionCallExpr("MAX", Lists.newArrayList(tableBColumn1));
        new Expectations() {
            {
                selectStmt.getAggInfo();
                result = aggregateInfo;
                aggregateInfo.getGroupingExprs();
                result = Lists.newArrayList(tableAColumn1);
                tableAColumn1Desc.isMaterialized();
                result = true;
                tableAColumn1Desc.getColumn().getName();
                result = "c1";
                tableAColumn1Desc.getParent();
                result = tableADesc;
                tableADesc.getTable();
                result = tableA;
                tableA.getId();
                result = 1;

                aggregateInfo.getAggregateExprs();
                result = Lists.newArrayList(tableAColumn2Sum, tableBColumn1Max);
                tableAColumn2Sum.getChildren();
                result = Lists.newArrayList(tableAColumn2);
                tableBColumn1Max.getChildren();
                result = Lists.newArrayList(tableBColumn1);
                tableAColumn2.getColumnName();
                result = "c2";
                tableBColumn1.getColumnName();
                result = "c1";
                tableAColumn2.getTableName().getTbl();
                result = "tableA";
                tableBColumn1.getTableName().getTbl();
                result = "tableB";

                tableAColumn2Desc.getParent();
                result = tableADesc;
                tableBColumn1Desc.getParent();
                result = tableBDesc;
                tableBDesc.getTable();
                result = tableB;
                tableB.getId();
                result = 2;

                tableAColumn2Desc.isMaterialized();
                result = true;
                tableBColumn1Desc.isMaterialized();
                result = true;
                tableAColumn2Desc.getColumn().getName();
                result = "c2";
                tableBColumn1Desc.getColumn().getName();
                result = "c1";
            }
        };

        MaterializedViewSelector materializedViewSelector = new MaterializedViewSelector(selectStmt, analyzer);
        Map<Long, Set<String>> columnNamesInPredicates =
                Deencapsulation.getField(materializedViewSelector, "columnNamesInPredicates");
        Assert.assertEquals(0, columnNamesInPredicates.size());
        Assert.assertFalse(Deencapsulation.getField(materializedViewSelector, "isSPJQuery"));
        Map<Long, Set<String>> columnNamesInGrouping =
                Deencapsulation.getField(materializedViewSelector, "columnNamesInGrouping");
        Assert.assertEquals(1, columnNamesInGrouping.size());
        Set<String> tableAColumnNamesInGrouping = columnNamesInGrouping.get(new Long(1));
        Assert.assertNotEquals(tableAColumnNamesInGrouping, null);
        Assert.assertEquals(1, tableAColumnNamesInGrouping.size());
        Assert.assertTrue(tableAColumnNamesInGrouping.contains("c1"));
        Map<Long, Set<FunctionCallExpr>> aggregateColumnsInQuery =
                Deencapsulation.getField(materializedViewSelector, "aggColumnsInQuery");
        Assert.assertEquals(2, aggregateColumnsInQuery.size());
        Set<FunctionCallExpr> tableAAgggregatedColumns = aggregateColumnsInQuery.get(new Long(1));
        Assert.assertEquals(1, tableAAgggregatedColumns.size());
        FunctionCallExpr aggregatedColumn1 = tableAAgggregatedColumns.iterator().next();
        List<Expr> aggColumn1Params = aggregatedColumn1.getParams().exprs();
        Assert.assertEquals(1, aggColumn1Params.size());
        Assert.assertTrue(aggColumn1Params.get(0) instanceof SlotRef);
        Assert.assertEquals("c2", ((SlotRef) aggColumn1Params.get(0)).getColumnName());
        Assert.assertTrue("SUM".equalsIgnoreCase(aggregatedColumn1.getFnName().getFunction()));
        Set<FunctionCallExpr> tableBAgggregatedColumns = aggregateColumnsInQuery.get(new Long(2));
        Assert.assertEquals(1, tableBAgggregatedColumns.size());
        FunctionCallExpr aggregatedColumn2 = tableBAgggregatedColumns.iterator().next();
        List<Expr> aggColumn2Params = aggregatedColumn2.getParams().exprs();
        Assert.assertEquals(1, aggColumn2Params.size());
        Assert.assertTrue(aggColumn2Params.get(0) instanceof SlotRef);
        Assert.assertEquals("c1", ((SlotRef) aggColumn2Params.get(0)).getColumnName());
        Assert.assertTrue("MAX".equalsIgnoreCase(aggregatedColumn2.getFnName().getFunction()));
    }

    @Test
    public void testCheckCompensatingPredicates(@Injectable SelectStmt selectStmt, @Injectable Analyzer analyzer,
            @Injectable MaterializedIndexMeta indexMeta1,
            @Injectable MaterializedIndexMeta indexMeta2,
            @Injectable MaterializedIndexMeta indexMeta3,
            @Injectable MaterializedIndexMeta indexMeta4) {
        Set<String> tableAColumnNames = Sets.newHashSet();
        tableAColumnNames.add("C1");
        Map<Long, MaterializedIndexMeta> candidateIndexIdToSchema = Maps.newHashMap();
        List<Column> index1Columns = Lists.newArrayList();
        Column index1Column1 = new Column("c1", Type.INT, true, null, true, "", "");
        index1Columns.add(index1Column1);
        candidateIndexIdToSchema.put(new Long(1), indexMeta1);
        List<Column> index2Columns = Lists.newArrayList();
        Column index2Column1 = new Column("c1", Type.INT, false, AggregateType.NONE, true, "", "");
        index2Columns.add(index2Column1);
        candidateIndexIdToSchema.put(new Long(2), indexMeta2);
        List<Column> index3Columns = Lists.newArrayList();
        Column index3Column1 = new Column("c1", Type.INT, false, AggregateType.SUM, true, "", "");
        index3Columns.add(index3Column1);
        candidateIndexIdToSchema.put(new Long(3), indexMeta3);
        List<Column> index4Columns = Lists.newArrayList();
        Column index4Column2 = new Column("c2", Type.INT, true, null, true, "", "");
        index4Columns.add(index4Column2);
        candidateIndexIdToSchema.put(new Long(4), indexMeta4);
        new Expectations() {
            {
                selectStmt.getAggInfo();
                result = null;
                indexMeta1.getSchema();
                result = index1Columns;
                indexMeta2.getSchema();
                result = index2Columns;
                indexMeta3.getSchema();
                result = index3Columns;
                indexMeta4.getSchema();
                result = index4Columns;
            }
        };

        MaterializedViewSelector selector = new MaterializedViewSelector(selectStmt, analyzer);
        Deencapsulation.invoke(selector, "checkCompensatingPredicates", tableAColumnNames, candidateIndexIdToSchema);
        Assert.assertEquals(2, candidateIndexIdToSchema.size());
        Assert.assertTrue(candidateIndexIdToSchema.keySet().contains(new Long(1)));
        Assert.assertTrue(candidateIndexIdToSchema.keySet().contains(new Long(2)));
    }

    @Test
    public void testCheckGrouping(@Injectable SelectStmt selectStmt, @Injectable Analyzer analyzer,
            @Injectable MaterializedIndexMeta indexMeta1,
            @Injectable MaterializedIndexMeta indexMeta2,
            @Injectable MaterializedIndexMeta indexMeta3) {
        Set<String> tableAColumnNames = Sets.newHashSet();
        tableAColumnNames.add("C1");
        Map<Long, MaterializedIndexMeta> candidateIndexIdToSchema = Maps.newHashMap();
        List<Column> index1Columns = Lists.newArrayList();
        Column index1Column1 = new Column("c2", Type.INT, true, null, true, "", "");
        index1Columns.add(index1Column1);
        candidateIndexIdToSchema.put(new Long(1), indexMeta1);
        List<Column> index2Columns = Lists.newArrayList();
        Column index2Column1 = new Column("c1", Type.INT, true, null, true, "", "");
        index2Columns.add(index2Column1);
        Column index2Column2 = new Column("c2", Type.INT, false, AggregateType.SUM, true, "", "");
        index2Columns.add(index2Column2);
        candidateIndexIdToSchema.put(new Long(2), indexMeta2);
        List<Column> index3Columns = Lists.newArrayList();
        Column index3Column1 = new Column("c2", Type.INT, true, null, true, "", "");
        index3Columns.add(index3Column1);
        Column index3Column2 = new Column("c1", Type.INT, false, AggregateType.SUM, true, "", "");
        index3Columns.add(index3Column2);
        candidateIndexIdToSchema.put(new Long(3), indexMeta3);
        new Expectations() {
            {
                selectStmt.getAggInfo();
                result = null;
                indexMeta1.getSchema();
                result = index1Columns;
                indexMeta1.getKeysType();
                result = KeysType.DUP_KEYS;
                indexMeta2.getSchema();
                result = index2Columns;
                indexMeta3.getSchema();
                result = index3Columns;
            }
        };

        MaterializedViewSelector selector = new MaterializedViewSelector(selectStmt, analyzer);
        Deencapsulation.setField(selector, "isSPJQuery", false);
        Deencapsulation.invoke(selector, "checkGrouping", tableAColumnNames, candidateIndexIdToSchema);
        Assert.assertEquals(2, candidateIndexIdToSchema.size());
        Assert.assertTrue(candidateIndexIdToSchema.keySet().contains(new Long(1)));
        Assert.assertTrue(candidateIndexIdToSchema.keySet().contains(new Long(2)));
    }

    @Test
    public void testCheckAggregationFunction(@Injectable SelectStmt selectStmt, @Injectable Analyzer analyzer,
            @Injectable MaterializedIndexMeta indexMeta1,
            @Injectable MaterializedIndexMeta indexMeta2,
            @Injectable MaterializedIndexMeta indexMeta3) {
        Map<Long, MaterializedIndexMeta> candidateIndexIdToSchema = Maps.newHashMap();
        List<Column> index1Columns = Lists.newArrayList();
        Column index1Column1 = new Column("c2", Type.INT, true, null, true, "", "");
        index1Columns.add(index1Column1);
        candidateIndexIdToSchema.put(new Long(1), indexMeta1);
        List<Column> index2Columns = Lists.newArrayList();
        Column index2Column1 = new Column("c1", Type.INT, true, null, true, "", "");
        index2Columns.add(index2Column1);
        Column index2Column2 = new Column("c2", Type.INT, false, AggregateType.SUM, true, "", "");
        index2Columns.add(index2Column2);
        candidateIndexIdToSchema.put(new Long(2), indexMeta2);
        List<Column> index3Columns = Lists.newArrayList();
        Column index3Column1 = new Column("c2", Type.INT, true, null, true, "", "");
        index3Columns.add(index3Column1);
        Column index3Column2 = new Column("c1", Type.INT, false, AggregateType.SUM, true, "", "");
        index3Columns.add(index3Column2);
        candidateIndexIdToSchema.put(new Long(3), indexMeta3);
        new Expectations() {
            {
                selectStmt.getAggInfo();
                result = null;
                indexMeta1.getSchema();
                result = index1Columns;
                indexMeta1.getKeysType();
                result = KeysType.DUP_KEYS;
                indexMeta2.getSchema();
                result = index2Columns;
                indexMeta3.getSchema();
                result = index3Columns;
            }
        };

        MaterializedViewSelector selector = new MaterializedViewSelector(selectStmt, analyzer);
        TableName tableName = new TableName("db1", "table1");
        SlotRef slotRef = new SlotRef(tableName, "C1");
        FunctionCallExpr functionCallExpr = new FunctionCallExpr("sum", Lists.newArrayList(slotRef));
        Set<FunctionCallExpr> aggregatedColumnsInQueryOutput = Sets.newHashSet();
        aggregatedColumnsInQueryOutput.add(functionCallExpr);
        Deencapsulation.setField(selector, "isSPJQuery", false);
        Deencapsulation.invoke(selector, "checkAggregationFunction", aggregatedColumnsInQueryOutput,
                               candidateIndexIdToSchema);
        Assert.assertEquals(2, candidateIndexIdToSchema.size());
        Assert.assertTrue(candidateIndexIdToSchema.keySet().contains(new Long(1)));
        Assert.assertTrue(candidateIndexIdToSchema.keySet().contains(new Long(3)));
    }

    @Test
    public void testCheckOutputColumns(@Injectable SelectStmt selectStmt, @Injectable Analyzer analyzer,
            @Injectable MaterializedIndexMeta indexMeta1,
            @Injectable MaterializedIndexMeta indexMeta2,
            @Injectable MaterializedIndexMeta indexMeta3) {
        Map<Long, MaterializedIndexMeta> candidateIndexIdToSchema = Maps.newHashMap();
        List<Column> index1Columns = Lists.newArrayList();
        Column index1Column1 = new Column("c2", Type.INT, true, null, true, "", "");
        index1Columns.add(index1Column1);
        candidateIndexIdToSchema.put(new Long(1), indexMeta1);
        List<Column> index2Columns = Lists.newArrayList();
        Column index2Column1 = new Column("c1", Type.INT, true, null, true, "", "");
        index2Columns.add(index2Column1);
        Column index2Column2 = new Column("c2", Type.INT, false, AggregateType.NONE, true, "", "");
        index2Columns.add(index2Column2);
        candidateIndexIdToSchema.put(new Long(2), indexMeta2);
        List<Column> index3Columns = Lists.newArrayList();
        Column index3Column1 = new Column("C2", Type.INT, true, null, true, "", "");
        index3Columns.add(index3Column1);
        Column index3Column2 = new Column("c1", Type.INT, false, AggregateType.SUM, true, "", "");
        index3Columns.add(index3Column2);
        candidateIndexIdToSchema.put(new Long(3), indexMeta3);
        new Expectations() {
            {
                selectStmt.getAggInfo();
                result = null;
                indexMeta1.getSchema();
                result = index1Columns;
                indexMeta2.getSchema();
                result = index2Columns;
                indexMeta3.getSchema();
                result = index3Columns;
            }
        };

        MaterializedViewSelector selector = new MaterializedViewSelector(selectStmt, analyzer);
        Set<String> columnNamesInQueryOutput = Sets.newHashSet();
        columnNamesInQueryOutput.add("c1");
        columnNamesInQueryOutput.add("c2");
        Deencapsulation.invoke(selector, "checkOutputColumns", columnNamesInQueryOutput,
                               candidateIndexIdToSchema);
        Assert.assertEquals(2, candidateIndexIdToSchema.size());
        Assert.assertTrue(candidateIndexIdToSchema.keySet().contains(new Long(2)));
        Assert.assertTrue(candidateIndexIdToSchema.keySet().contains(new Long(3)));
    }

    @Test
    public void testCompensateIndex(@Injectable SelectStmt selectStmt, @Injectable Analyzer analyzer,
            @Injectable OlapTable table) {
        Map<Long, List<Column>> candidateIndexIdToSchema = Maps.newHashMap();
        Map<Long, List<Column>> allVisibleIndexes = Maps.newHashMap();
        List<Column> index1Columns = Lists.newArrayList();
        Column index1Column1 = new Column("c2", Type.INT, true, AggregateType.SUM, true, "", "");
        index1Columns.add(index1Column1);
        allVisibleIndexes.put(new Long(1), index1Columns);
        List<Column> index2Columns = Lists.newArrayList();
        Column index2Column1 = new Column("c1", Type.INT, true, null, true, "", "");
        index2Columns.add(index2Column1);
        Column index2Column2 = new Column("c2", Type.INT, false, AggregateType.SUM, true, "", "");
        index2Columns.add(index2Column2);
        allVisibleIndexes.put(new Long(2), index2Columns);
        List<Column> index3Columns = Lists.newArrayList();
        Column index3Column1 = new Column("c1", Type.INT, true, null, true, "", "");
        index3Columns.add(index3Column1);
        Column index3Column2 = new Column("c3", Type.INT, false, AggregateType.SUM, true, "", "");
        index3Columns.add(index3Column2);
        allVisibleIndexes.put(new Long(3), index3Columns);
        List<Column> keyColumns = Lists.newArrayList();
        keyColumns.add(index2Column1);
        new Expectations() {
            {
                selectStmt.getAggInfo();
                result = null;
                table.getBaseIndexId();
                result = -1L;
                table.getKeyColumnsByIndexId(-1L);
                result = keyColumns;
                table.getKeyColumnsByIndexId(1L);
                result = Lists.newArrayList();
                table.getKeyColumnsByIndexId(2L);
                result = keyColumns;
                table.getKeyColumnsByIndexId(3L);
                result = keyColumns;
            }
        };

        MaterializedViewSelector selector = new MaterializedViewSelector(selectStmt, analyzer);
        Deencapsulation.invoke(selector, "compensateCandidateIndex", candidateIndexIdToSchema,
                               allVisibleIndexes, table);
        Assert.assertEquals(2, candidateIndexIdToSchema.size());
        Assert.assertTrue(candidateIndexIdToSchema.keySet().contains(new Long(2)));
        Assert.assertTrue(candidateIndexIdToSchema.keySet().contains(new Long(3)));
    }

    @Test
    public void testSelectBestRowCountIndex(@Injectable SelectStmt selectStmt, @Injectable Analyzer analyzer) {
        Map<Long, List<Column>> candidateIndexIdToSchema = Maps.newHashMap();
        List<Column> index1Columns = Lists.newArrayList();
        Column index1Column1 = new Column("c1", Type.INT, true, null, true, "", "");
        index1Columns.add(index1Column1);
        Column index1Column2 = new Column("c2", Type.INT, false, AggregateType.NONE, true, "", "");
        index1Columns.add(index1Column2);
        Column index1Column3 = new Column("c3", Type.INT, false, AggregateType.NONE, true, "", "");
        index1Columns.add(index1Column3);
        candidateIndexIdToSchema.put(new Long(1), index1Columns);
        List<Column> index2Columns = Lists.newArrayList();
        Column index2Column1 = new Column("c2", Type.INT, true, null, true, "", "");
        index2Columns.add(index2Column1);
        Column index2Column2 = new Column("c1", Type.INT, false, AggregateType.NONE, true, "", "");
        index2Columns.add(index2Column2);
        Column index2Column3 = new Column("c3", Type.INT, false, AggregateType.NONE, true, "", "");
        index2Columns.add(index2Column3);
        candidateIndexIdToSchema.put(new Long(2), index2Columns);
        List<Column> index3Columns = Lists.newArrayList();
        Column index3Column1 = new Column("c1", Type.INT, true, null, true, "", "");
        index3Columns.add(index3Column1);
        Column index3Column2 = new Column("c3", Type.INT, false, AggregateType.NONE, true, "", "");
        index3Columns.add(index3Column2);
        Column index3Column3 = new Column("c2", Type.INT, false, AggregateType.NONE, true, "", "");
        index3Columns.add(index3Column3);
        candidateIndexIdToSchema.put(new Long(3), index3Columns);
        new Expectations() {
            {
                selectStmt.getAggInfo();
                result = null;
            }
        };
        Set<String> equivalenceColumns = Sets.newHashSet();
        equivalenceColumns.add("c1");
        equivalenceColumns.add("c2");
        Set<String> unequivalenceColumns = Sets.newHashSet();
        unequivalenceColumns.add("c3");

        MaterializedViewSelector selector = new MaterializedViewSelector(selectStmt, analyzer);
        Set<Long> result = Deencapsulation.invoke(selector, "matchBestPrefixIndex", candidateIndexIdToSchema,
                               equivalenceColumns, unequivalenceColumns);
        Assert.assertEquals(2, result.size());
        Assert.assertTrue(result.contains(new Long(1)));
        Assert.assertTrue(result.contains(new Long(2)));
    }


}
