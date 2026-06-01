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

package org.apache.doris.mtmv;

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.PartitionExprUtil;
import org.apache.doris.catalog.DynamicPartitionProperty;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.TableProperty;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.DynamicPartitionUtil;
import org.apache.doris.common.util.DynamicPartitionUtil.StartOfDate;
import org.apache.doris.datasource.hive.HMSExternalTable;

import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;

public class MTMVPartitionCheckUtilTest {
    private HMSExternalTable hmsExternalTable = Mockito.mock(HMSExternalTable.class);
    private OlapTable originalTable = Mockito.mock(OlapTable.class);
    private OlapTable relatedTable = Mockito.mock(OlapTable.class);
    private PartitionInfo originalPartitionInfo = Mockito.mock(PartitionInfo.class);
    private PartitionInfo relatedPartitionInfo = Mockito.mock(PartitionInfo.class);
    private TableProperty originalTableProperty = Mockito.mock(TableProperty.class);
    private TableProperty relatedTableProperty = Mockito.mock(TableProperty.class);
    private DynamicPartitionProperty originalDynamicPartitionProperty = Mockito.mock(DynamicPartitionProperty.class);
    private DynamicPartitionProperty relatedDynamicPartitionProperty = Mockito.mock(DynamicPartitionProperty.class);
    private Expr expr1 = Mockito.mock(Expr.class);
    private PartitionExprUtil partitionExprUtilInstance = new PartitionExprUtil();
    private ArrayList<Expr> originalExprs = Lists.newArrayList();
    private ArrayList<Expr> relatedExprs = Lists.newArrayList(expr1);

    private MockedStatic<DynamicPartitionUtil> dynamicPartitionUtilStatic;
    private MockedStatic<PartitionExprUtil> partitionExprUtilStatic;

    @Before
    public void setUp()
            throws NoSuchMethodException, SecurityException, AnalysisException, DdlException, MetaNotFoundException {

        dynamicPartitionUtilStatic = Mockito.mockStatic(DynamicPartitionUtil.class);
        partitionExprUtilStatic = Mockito.mockStatic(PartitionExprUtil.class);

        Mockito.when(originalTable.getPartitionInfo()).thenReturn(originalPartitionInfo);
        Mockito.when(originalTable.getPartitionType()).thenReturn(PartitionType.RANGE);
        Mockito.when(originalTable.getTableProperty()).thenReturn(originalTableProperty);
        Mockito.when(originalTableProperty.getDynamicPartitionProperty()).thenReturn(originalDynamicPartitionProperty);
        Mockito.when(relatedTable.getPartitionInfo()).thenReturn(relatedPartitionInfo);
        Mockito.when(relatedTable.getPartitionType()).thenReturn(PartitionType.RANGE);
        Mockito.when(relatedTable.getTableProperty()).thenReturn(relatedTableProperty);
        Mockito.when(relatedTableProperty.getDynamicPartitionProperty()).thenReturn(relatedDynamicPartitionProperty);
        dynamicPartitionUtilStatic.when(() -> DynamicPartitionUtil.isDynamicPartitionTable(relatedTable))
                .thenReturn(true);
        Mockito.when(originalDynamicPartitionProperty.getStartOfMonth()).thenReturn(new StartOfDate(1, 1, 1));
        Mockito.when(relatedDynamicPartitionProperty.getStartOfMonth()).thenReturn(new StartOfDate(1, 1, 1));
        Mockito.when(relatedDynamicPartitionProperty.getStartOfWeek()).thenReturn(new StartOfDate(1, 1, 1));
        Mockito.when(originalDynamicPartitionProperty.getStartOfWeek()).thenReturn(new StartOfDate(1, 1, 1));
        Mockito.when(originalPartitionInfo.getPartitionExprs()).thenReturn(originalExprs);
        Mockito.when(relatedPartitionInfo.getPartitionExprs()).thenReturn(relatedExprs);
    }

    @After
    public void tearDown() {
        partitionExprUtilStatic.close();
        dynamicPartitionUtilStatic.close();
    }

    @Test
    public void testCheckIfAllowMultiTablePartitionRefreshNotOlapTable() {
        Pair<Boolean, String> res = MTMVPartitionCheckUtil.checkIfAllowMultiTablePartitionRefresh(
                hmsExternalTable);
        Assert.assertFalse(res.first);
    }

    @Test
    public void testCheckIfAllowMultiTablePartitionRefreshNotRangePartition() {
        Mockito.when(originalTable.getPartitionType()).thenReturn(PartitionType.LIST);
        Pair<Boolean, String> res = MTMVPartitionCheckUtil.checkIfAllowMultiTablePartitionRefresh(
                originalTable);
        Assert.assertFalse(res.first);
    }

    @Test
    public void testCheckIfAllowMultiTablePartitionRefreshNotDynamicAndAuto() {
        Mockito.when(originalPartitionInfo.enableAutomaticPartition()).thenReturn(false);
        dynamicPartitionUtilStatic.when(() -> DynamicPartitionUtil.isDynamicPartitionTable(originalTable))
                .thenReturn(false);
        Pair<Boolean, String> res = MTMVPartitionCheckUtil.checkIfAllowMultiTablePartitionRefresh(
                originalTable);
        Assert.assertFalse(res.first);
    }

    @Test
    public void testCheckIfAllowMultiTablePartitionRefreshDynamic() {
        Mockito.when(originalPartitionInfo.enableAutomaticPartition()).thenReturn(true);
        dynamicPartitionUtilStatic.when(() -> DynamicPartitionUtil.isDynamicPartitionTable(originalTable))
                .thenReturn(false);
        Pair<Boolean, String> res = MTMVPartitionCheckUtil.checkIfAllowMultiTablePartitionRefresh(
                originalTable);
        Assert.assertTrue(res.first);
    }

    @Test
    public void testCheckIfAllowMultiTablePartitionRefreshAuto() {
        Mockito.when(originalPartitionInfo.enableAutomaticPartition()).thenReturn(false);
        dynamicPartitionUtilStatic.when(() -> DynamicPartitionUtil.isDynamicPartitionTable(originalTable))
                .thenReturn(true);
        Pair<Boolean, String> res = MTMVPartitionCheckUtil.checkIfAllowMultiTablePartitionRefresh(
                originalTable);
        Assert.assertTrue(res.first);
    }

    @Test
    public void testCompareDynamicPartition() throws AnalysisException {
        Pair<Boolean, String> res = MTMVPartitionCheckUtil.compareDynamicPartition(originalTable, relatedTable);
        Assert.assertTrue(res.first);
    }

    @Test
    public void testCompareDynamicPartitionNotEqual() throws AnalysisException {
        Mockito.when(relatedDynamicPartitionProperty.getStartOfWeek()).thenReturn(new StartOfDate(1, 1, 1));
        Mockito.when(originalDynamicPartitionProperty.getStartOfWeek()).thenReturn(new StartOfDate(1, 1, 2));
        Pair<Boolean, String> res = MTMVPartitionCheckUtil.compareDynamicPartition(originalTable, relatedTable);
        Assert.assertFalse(res.first);
    }

    @Test
    public void testCompareAutoPartition() throws AnalysisException {
        Mockito.when(relatedPartitionInfo.enableAutomaticPartition()).thenReturn(true);
        partitionExprUtilStatic.when(() -> PartitionExprUtil.getFunctionIntervalInfo(
                Mockito.eq(originalExprs), Mockito.any(PartitionType.class)))
                .thenReturn(partitionExprUtilInstance.new FunctionIntervalInfo("datetrunc", "week", 1));
        partitionExprUtilStatic.when(() -> PartitionExprUtil.getFunctionIntervalInfo(
                Mockito.eq(relatedExprs), Mockito.any(PartitionType.class)))
                .thenReturn(partitionExprUtilInstance.new FunctionIntervalInfo("datetrunc", "week", 1));
        Pair<Boolean, String> res = MTMVPartitionCheckUtil.compareAutoPartition(originalTable, relatedTable);
        Assert.assertTrue(res.first);
    }

    @Test
    public void testCompareAutoPartitionNotEqual() throws AnalysisException {
        partitionExprUtilStatic.when(() -> PartitionExprUtil.getFunctionIntervalInfo(
                Mockito.eq(originalExprs), Mockito.any(PartitionType.class)))
                .thenReturn(partitionExprUtilInstance.new FunctionIntervalInfo("datetrunc", "week", 1));
        partitionExprUtilStatic.when(() -> PartitionExprUtil.getFunctionIntervalInfo(
                Mockito.eq(relatedExprs), Mockito.any(PartitionType.class)))
                .thenReturn(partitionExprUtilInstance.new FunctionIntervalInfo("datetrunc", "week", 2));
        Pair<Boolean, String> res = MTMVPartitionCheckUtil.compareAutoPartition(originalTable, relatedTable);
        Assert.assertFalse(res.first);
    }
}
