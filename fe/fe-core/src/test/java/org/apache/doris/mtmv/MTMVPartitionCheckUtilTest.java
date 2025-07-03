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
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;

public class MTMVPartitionCheckUtilTest {
    @Mocked
    private HMSExternalTable hmsExternalTable;
    @Mocked
    private OlapTable originalTable;
    @Mocked
    private OlapTable relatedTable;
    @Mocked
    private DynamicPartitionUtil dynamicPartitionUtil;
    @Mocked
    private PartitionExprUtil partitionExprUtil;
    @Mocked
    private PartitionInfo originalPartitionInfo;
    @Mocked
    private PartitionInfo relatedPartitionInfo;
    @Mocked
    private TableProperty originalTableProperty;
    @Mocked
    private TableProperty relatedTableProperty;
    @Mocked
    private DynamicPartitionProperty originalDynamicPartitionProperty;
    @Mocked
    private DynamicPartitionProperty relatedDynamicPartitionProperty;
    @Mocked
    private Expr expr1;
    private ArrayList<Expr> originalExprs = Lists.newArrayList();
    private ArrayList<Expr> relatedExprs = Lists.newArrayList(expr1);


    @Before
    public void setUp()
            throws NoSuchMethodException, SecurityException, AnalysisException, DdlException, MetaNotFoundException {

        new Expectations() {
            {
                originalTable.getPartitionInfo();
                minTimes = 0;
                result = originalPartitionInfo;

                originalTable.getPartitionType();
                minTimes = 0;
                result = PartitionType.RANGE;

                originalTable.getTableProperty();
                minTimes = 0;
                result = originalTableProperty;

                originalTableProperty.getDynamicPartitionProperty();
                minTimes = 0;
                result = originalDynamicPartitionProperty;

                relatedTable.getPartitionInfo();
                minTimes = 0;
                result = relatedPartitionInfo;

                relatedTable.getTableProperty();
                minTimes = 0;
                result = relatedTableProperty;

                relatedTableProperty.getDynamicPartitionProperty();
                minTimes = 0;
                result = relatedDynamicPartitionProperty;

                dynamicPartitionUtil.isDynamicPartitionTable(relatedTable);
                minTimes = 0;
                result = true;

                originalDynamicPartitionProperty.getStartOfMonth();
                minTimes = 0;
                result = new StartOfDate(1, 1, 1);

                relatedDynamicPartitionProperty.getStartOfMonth();
                minTimes = 0;
                result = new StartOfDate(1, 1, 1);

                relatedDynamicPartitionProperty.getStartOfWeek();
                minTimes = 0;
                result = new StartOfDate(1, 1, 1);

                originalDynamicPartitionProperty.getStartOfWeek();
                minTimes = 0;
                result = new StartOfDate(1, 1, 1);

                originalPartitionInfo.getPartitionExprs();
                minTimes = 0;
                result = originalExprs;

                relatedPartitionInfo.getPartitionExprs();
                minTimes = 0;
                result = relatedExprs;
            }
        };
    }

    @Test
    public void testCheckIfAllowMultiTablePartitionRefreshNotOlapTable() {
        Pair<Boolean, String> res = MTMVPartitionCheckUtil.checkIfAllowMultiTablePartitionRefresh(
                hmsExternalTable);
        Assert.assertFalse(res.first);
    }

    @Test
    public void testCheckIfAllowMultiTablePartitionRefreshNotRangePartition() {
        new Expectations() {
            {
                originalTable.getPartitionType();
                minTimes = 0;
                result = PartitionType.LIST;
            }
        };
        Pair<Boolean, String> res = MTMVPartitionCheckUtil.checkIfAllowMultiTablePartitionRefresh(
                originalTable);
        Assert.assertFalse(res.first);
    }

    @Test
    public void testCheckIfAllowMultiTablePartitionRefreshNotDynamicAndAuto() {
        new Expectations() {
            {
                originalPartitionInfo.enableAutomaticPartition();
                minTimes = 0;
                result = false;

                dynamicPartitionUtil.isDynamicPartitionTable(originalTable);
                minTimes = 0;
                result = false;
            }
        };
        Pair<Boolean, String> res = MTMVPartitionCheckUtil.checkIfAllowMultiTablePartitionRefresh(
                originalTable);
        Assert.assertFalse(res.first);
    }

    @Test
    public void testCheckIfAllowMultiTablePartitionRefreshDynamic() {
        new Expectations() {
            {
                originalPartitionInfo.enableAutomaticPartition();
                minTimes = 0;
                result = true;

                dynamicPartitionUtil.isDynamicPartitionTable(originalTable);
                minTimes = 0;
                result = false;
            }
        };
        Pair<Boolean, String> res = MTMVPartitionCheckUtil.checkIfAllowMultiTablePartitionRefresh(
                originalTable);
        Assert.assertTrue(res.first);
    }

    @Test
    public void testCheckIfAllowMultiTablePartitionRefreshAuto() {
        new Expectations() {
            {
                originalPartitionInfo.enableAutomaticPartition();
                minTimes = 0;
                result = false;

                dynamicPartitionUtil.isDynamicPartitionTable(originalTable);
                minTimes = 0;
                result = true;
            }
        };
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
        new Expectations() {
            {
                relatedDynamicPartitionProperty.getStartOfWeek();
                minTimes = 0;
                result = new StartOfDate(1, 1, 1);

                originalDynamicPartitionProperty.getStartOfWeek();
                minTimes = 0;
                result = new StartOfDate(1, 1, 2);
            }
        };
        Pair<Boolean, String> res = MTMVPartitionCheckUtil.compareDynamicPartition(originalTable, relatedTable);
        Assert.assertFalse(res.first);
    }

    @Test
    public void testCompareAutpPartition() throws AnalysisException {
        new Expectations() {
            {
                partitionExprUtil.getFunctionIntervalInfo(originalExprs, (PartitionType) any);
                minTimes = 0;
                result = partitionExprUtil.new FunctionIntervalInfo("datetrunc", "week", 1);

                partitionExprUtil.getFunctionIntervalInfo(relatedExprs, (PartitionType) any);
                minTimes = 0;
                result = partitionExprUtil.new FunctionIntervalInfo("datetrunc", "week", 1);
            }
        };
        Pair<Boolean, String> res = MTMVPartitionCheckUtil.compareAutoPartition(originalTable, relatedTable);
        Assert.assertTrue(res.first);
    }

    @Test
    public void testCompareAutpPartitionNotEqual() throws AnalysisException {
        new Expectations() {
            {
                partitionExprUtil.getFunctionIntervalInfo(originalExprs, (PartitionType) any);
                minTimes = 0;
                result = partitionExprUtil.new FunctionIntervalInfo("datetrunc", "week", 1);

                partitionExprUtil.getFunctionIntervalInfo(relatedExprs, (PartitionType) any);
                minTimes = 0;
                result = partitionExprUtil.new FunctionIntervalInfo("datetrunc", "week", 2);
            }
        };
        Pair<Boolean, String> res = MTMVPartitionCheckUtil.compareAutoPartition(originalTable, relatedTable);
        Assert.assertFalse(res.first);
    }
}
