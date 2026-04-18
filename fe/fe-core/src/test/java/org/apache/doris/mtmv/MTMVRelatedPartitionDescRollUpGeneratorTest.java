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

import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.PartitionKeyDesc;
import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.analysis.TimestampArithmeticExpr;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.mtmv.MTMVPartitionInfo.MTMVPartitionType;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class MTMVRelatedPartitionDescRollUpGeneratorTest {
    private MTMVPartitionInfo mtmvPartitionInfo = Mockito.mock(MTMVPartitionInfo.class);

    // =========================================================================
    // Original tests
    // =========================================================================

    @Test
    public void testRollUpRange() throws AnalysisException {
        FunctionCallExpr expr = new FunctionCallExpr("date_trunc",
                Lists.newArrayList(new SlotRef(null, null), new StringLiteral("month")), true);
        try (MockedStatic<MTMVPartitionUtil> mtmvPartitionUtilStatic = Mockito.mockStatic(MTMVPartitionUtil.class)) {
            mtmvPartitionUtilStatic.when(() -> MTMVPartitionUtil.getPartitionColumnType(
                    Mockito.nullable(MTMVRelatedTableIf.class), Mockito.nullable(String.class))).thenReturn(Type.DATE);
            Mockito.when(mtmvPartitionInfo.getRelatedTable()).thenReturn(null);
            Mockito.when(mtmvPartitionInfo.getExpr()).thenReturn(expr);
            Mockito.when(mtmvPartitionInfo.getPartitionType()).thenReturn(MTMVPartitionType.EXPR);

            MTMVRelatedPartitionDescRollUpGenerator generator = new MTMVRelatedPartitionDescRollUpGenerator();
            Map<PartitionKeyDesc, Set<String>> relatedPartitionDescs = Maps.newHashMap();
            PartitionKeyDesc desc20200101 = PartitionKeyDesc.createFixed(
                    Lists.newArrayList(new PartitionValue("2020-01-01")),
                    Lists.newArrayList(new PartitionValue("2020-01-02")));
            PartitionKeyDesc desc20200102 = PartitionKeyDesc.createFixed(
                    Lists.newArrayList(new PartitionValue("2020-01-02")),
                    Lists.newArrayList(new PartitionValue("2020-01-03")));
            PartitionKeyDesc desc20200201 = PartitionKeyDesc.createFixed(
                    Lists.newArrayList(new PartitionValue("2020-02-01")),
                    Lists.newArrayList(new PartitionValue("2020-02-02")));
            relatedPartitionDescs.put(desc20200101, Sets.newHashSet("name1"));
            relatedPartitionDescs.put(desc20200102, Sets.newHashSet("name2"));
            relatedPartitionDescs.put(desc20200201, Sets.newHashSet("name3"));
            Map<PartitionKeyDesc, Set<String>> res = generator.rollUpRange(relatedPartitionDescs,
                    mtmvPartitionInfo, null);

            PartitionKeyDesc expectDesc202001 = PartitionKeyDesc.createFixed(
                    Lists.newArrayList(new PartitionValue("2020-01-01")),
                    Lists.newArrayList(new PartitionValue("2020-02-01")));
            PartitionKeyDesc expectDesc202002 = PartitionKeyDesc.createFixed(
                    Lists.newArrayList(new PartitionValue("2020-02-01")),
                    Lists.newArrayList(new PartitionValue("2020-03-01")));
            Assert.assertEquals(2, res.size());
            Assert.assertEquals(Sets.newHashSet("name1", "name2"), res.get(expectDesc202001));
            Assert.assertEquals(Sets.newHashSet("name3"), res.get(expectDesc202002));
        }
    }

    @Test
    public void testRollUpList() throws AnalysisException {
        FunctionCallExpr expr = new FunctionCallExpr("date_trunc",
                Lists.newArrayList(new SlotRef(null, null), new StringLiteral("month")), true);
        try (MockedStatic<MTMVPartitionUtil> mtmvPartitionUtilStatic = Mockito.mockStatic(MTMVPartitionUtil.class)) {
            mtmvPartitionUtilStatic.when(() -> MTMVPartitionUtil.getPartitionColumnType(
                    Mockito.nullable(MTMVRelatedTableIf.class), Mockito.nullable(String.class))).thenReturn(Type.DATE);
            Mockito.when(mtmvPartitionInfo.getRelatedTable()).thenReturn(null);
            Mockito.when(mtmvPartitionInfo.getExpr()).thenReturn(expr);
            Mockito.when(mtmvPartitionInfo.getPartitionType()).thenReturn(MTMVPartitionType.EXPR);

            MTMVRelatedPartitionDescRollUpGenerator generator = new MTMVRelatedPartitionDescRollUpGenerator();
            Map<PartitionKeyDesc, Set<String>> relatedPartitionDescs = Maps.newHashMap();
            relatedPartitionDescs.put(generateInDesc("2020-01-01"), Sets.newHashSet("name1"));
            relatedPartitionDescs.put(generateInDesc("2020-01-02"), Sets.newHashSet("name2"));
            relatedPartitionDescs.put(generateInDesc("2020-02-01"), Sets.newHashSet("name3"));
            Map<PartitionKeyDesc, Set<String>> res = generator.rollUpList(relatedPartitionDescs,
                    mtmvPartitionInfo, Maps.newHashMap());

            PartitionKeyDesc expectDesc202001 = generateInDesc("2020-01-01", "2020-01-02");
            PartitionKeyDesc expectDesc202002 = generateInDesc("2020-02-01");
            Assert.assertEquals(2, res.size());
            Assert.assertEquals(Sets.newHashSet("name1", "name2"), res.get(expectDesc202001));
            Assert.assertEquals(Sets.newHashSet("name3"), res.get(expectDesc202002));
        }
    }

    @Test
    public void testRollUpRangeDateAddHour() throws AnalysisException {
        FunctionCallExpr expr = new FunctionCallExpr("date_trunc",
                Lists.newArrayList(
                        new TimestampArithmeticExpr("date_add", new SlotRef(null, null), new IntLiteral(3), "HOUR"),
                        new StringLiteral("day")),
                true);
        try (MockedStatic<MTMVPartitionUtil> mtmvPartitionUtilStatic = Mockito.mockStatic(MTMVPartitionUtil.class)) {
            mtmvPartitionUtilStatic.when(() -> MTMVPartitionUtil.getPartitionColumnType(
                    Mockito.nullable(MTMVRelatedTableIf.class), Mockito.nullable(String.class)))
                    .thenReturn(Type.DATETIMEV2);
            Mockito.when(mtmvPartitionInfo.getRelatedTable()).thenReturn(null);
            Mockito.when(mtmvPartitionInfo.getExpr()).thenReturn(expr);
            Mockito.when(mtmvPartitionInfo.getPartitionType()).thenReturn(MTMVPartitionType.EXPR);

            MTMVRelatedPartitionDescRollUpGenerator generator = new MTMVRelatedPartitionDescRollUpGenerator();
            Map<PartitionKeyDesc, Set<String>> relatedPartitionDescs = Maps.newHashMap();
            PartitionKeyDesc desc20250724 = PartitionKeyDesc.createFixed(
                    Lists.newArrayList(new PartitionValue("2025-07-24 21:00:00")),
                    Lists.newArrayList(new PartitionValue("2025-07-25 21:00:00")));
            PartitionKeyDesc desc20250725 = PartitionKeyDesc.createFixed(
                    Lists.newArrayList(new PartitionValue("2025-07-25 21:00:00")),
                    Lists.newArrayList(new PartitionValue("2025-07-26 21:00:00")));
            relatedPartitionDescs.put(desc20250724, Sets.newHashSet("name1"));
            relatedPartitionDescs.put(desc20250725, Sets.newHashSet("name2"));
            Map<PartitionKeyDesc, Set<String>> res = generator.rollUpRange(relatedPartitionDescs,
                    mtmvPartitionInfo, null);

            PartitionKeyDesc expectDesc20250725 = PartitionKeyDesc.createFixed(
                    Lists.newArrayList(new PartitionValue("2025-07-25 00:00:00")),
                    Lists.newArrayList(new PartitionValue("2025-07-26 00:00:00")));
            PartitionKeyDesc expectDesc20250726 = PartitionKeyDesc.createFixed(
                    Lists.newArrayList(new PartitionValue("2025-07-26 00:00:00")),
                    Lists.newArrayList(new PartitionValue("2025-07-27 00:00:00")));
            Assert.assertEquals(2, res.size());
            Assert.assertEquals(Sets.newHashSet("name1"), res.get(expectDesc20250725));
            Assert.assertEquals(Sets.newHashSet("name2"), res.get(expectDesc20250726));
        }
    }

    @Test
    public void testRollUpRangeDateAddHourWithUtcMidnightBasePartitions() throws AnalysisException {
        FunctionCallExpr expr = new FunctionCallExpr("date_trunc",
                Lists.newArrayList(
                        new TimestampArithmeticExpr("date_add", new SlotRef(null, null), new IntLiteral(3), "HOUR"),
                        new StringLiteral("day")),
                true);
        try (MockedStatic<MTMVPartitionUtil> mtmvPartitionUtilStatic = Mockito.mockStatic(MTMVPartitionUtil.class)) {
            mtmvPartitionUtilStatic.when(() -> MTMVPartitionUtil.getPartitionColumnType(
                    Mockito.nullable(MTMVRelatedTableIf.class), Mockito.nullable(String.class)))
                    .thenReturn(Type.DATETIMEV2);
            Mockito.when(mtmvPartitionInfo.getRelatedTable()).thenReturn(null);
            Mockito.when(mtmvPartitionInfo.getExpr()).thenReturn(expr);
            Mockito.when(mtmvPartitionInfo.getPartitionType()).thenReturn(MTMVPartitionType.EXPR);

            MTMVRelatedPartitionDescRollUpGenerator generator = new MTMVRelatedPartitionDescRollUpGenerator();
            Map<PartitionKeyDesc, Set<String>> relatedPartitionDescs = Maps.newHashMap();
            PartitionKeyDesc desc20250725 = PartitionKeyDesc.createFixed(
                    Lists.newArrayList(new PartitionValue("2025-07-25 00:00:00")),
                    Lists.newArrayList(new PartitionValue("2025-07-26 00:00:00")));
            PartitionKeyDesc desc20250726 = PartitionKeyDesc.createFixed(
                    Lists.newArrayList(new PartitionValue("2025-07-26 00:00:00")),
                    Lists.newArrayList(new PartitionValue("2025-07-27 00:00:00")));
            relatedPartitionDescs.put(desc20250725, Sets.newHashSet("name1"));
            relatedPartitionDescs.put(desc20250726, Sets.newHashSet("name2"));
            Map<PartitionKeyDesc, Set<String>> res = generator.rollUpRange(relatedPartitionDescs,
                    mtmvPartitionInfo, null);

            PartitionKeyDesc expectDesc20250725 = PartitionKeyDesc.createFixed(
                    Lists.newArrayList(new PartitionValue("2025-07-25 00:00:00")),
                    Lists.newArrayList(new PartitionValue("2025-07-26 00:00:00")));
            PartitionKeyDesc expectDesc20250726 = PartitionKeyDesc.createFixed(
                    Lists.newArrayList(new PartitionValue("2025-07-26 00:00:00")),
                    Lists.newArrayList(new PartitionValue("2025-07-27 00:00:00")));
            PartitionKeyDesc expectDesc20250727 = PartitionKeyDesc.createFixed(
                    Lists.newArrayList(new PartitionValue("2025-07-27 00:00:00")),
                    Lists.newArrayList(new PartitionValue("2025-07-28 00:00:00")));
            Assert.assertEquals(3, res.size());
            Assert.assertEquals(Sets.newHashSet("name1"), res.get(expectDesc20250725));
            Assert.assertEquals(Sets.newHashSet("name1", "name2"), res.get(expectDesc20250726));
            Assert.assertEquals(Sets.newHashSet("name2"), res.get(expectDesc20250727));
        }
    }

    @Test
    public void testRollUpRangeDateSubHour() throws AnalysisException {
        FunctionCallExpr expr = new FunctionCallExpr("date_trunc",
                Lists.newArrayList(
                        new TimestampArithmeticExpr("date_sub", new SlotRef(null, null), new IntLiteral(3), "HOUR"),
                        new StringLiteral("day")),
                true);
        try (MockedStatic<MTMVPartitionUtil> mtmvPartitionUtilStatic = Mockito.mockStatic(MTMVPartitionUtil.class)) {
            mtmvPartitionUtilStatic.when(() -> MTMVPartitionUtil.getPartitionColumnType(
                    Mockito.nullable(MTMVRelatedTableIf.class), Mockito.nullable(String.class)))
                    .thenReturn(Type.DATETIMEV2);
            Mockito.when(mtmvPartitionInfo.getRelatedTable()).thenReturn(null);
            Mockito.when(mtmvPartitionInfo.getExpr()).thenReturn(expr);
            Mockito.when(mtmvPartitionInfo.getPartitionType()).thenReturn(MTMVPartitionType.EXPR);

            MTMVRelatedPartitionDescRollUpGenerator generator = new MTMVRelatedPartitionDescRollUpGenerator();
            Map<PartitionKeyDesc, Set<String>> relatedPartitionDescs = Maps.newHashMap();
            PartitionKeyDesc desc20250725 = PartitionKeyDesc.createFixed(
                    Lists.newArrayList(new PartitionValue("2025-07-25 03:00:00")),
                    Lists.newArrayList(new PartitionValue("2025-07-26 03:00:00")));
            PartitionKeyDesc desc20250726 = PartitionKeyDesc.createFixed(
                    Lists.newArrayList(new PartitionValue("2025-07-26 03:00:00")),
                    Lists.newArrayList(new PartitionValue("2025-07-27 03:00:00")));
            relatedPartitionDescs.put(desc20250725, Sets.newHashSet("name1"));
            relatedPartitionDescs.put(desc20250726, Sets.newHashSet("name2"));
            Map<PartitionKeyDesc, Set<String>> res = generator.rollUpRange(relatedPartitionDescs,
                    mtmvPartitionInfo, null);

            PartitionKeyDesc expectDesc20250725 = PartitionKeyDesc.createFixed(
                    Lists.newArrayList(new PartitionValue("2025-07-25 00:00:00")),
                    Lists.newArrayList(new PartitionValue("2025-07-26 00:00:00")));
            PartitionKeyDesc expectDesc20250726 = PartitionKeyDesc.createFixed(
                    Lists.newArrayList(new PartitionValue("2025-07-26 00:00:00")),
                    Lists.newArrayList(new PartitionValue("2025-07-27 00:00:00")));
            Assert.assertEquals(2, res.size());
            Assert.assertEquals(Sets.newHashSet("name1"), res.get(expectDesc20250725));
            Assert.assertEquals(Sets.newHashSet("name2"), res.get(expectDesc20250726));
        }
    }

    // =========================================================================
    // New tests — dateIncrement() tüm timeUnit branch'leri
    // =========================================================================

    @Test
    public void testRollUpRangeDateAddHourWeekUnit() throws AnalysisException {
        FunctionCallExpr expr = new FunctionCallExpr("date_trunc",
                Lists.newArrayList(
                        new TimestampArithmeticExpr("date_add", new SlotRef(null, null), new IntLiteral(3), "HOUR"),
                        new StringLiteral("week")),
                true);
        try (MockedStatic<MTMVPartitionUtil> mock = Mockito.mockStatic(MTMVPartitionUtil.class)) {
            mock.when(() -> MTMVPartitionUtil.getPartitionColumnType(
                    Mockito.nullable(MTMVRelatedTableIf.class), Mockito.nullable(String.class)))
                    .thenReturn(Type.DATETIMEV2);
            Mockito.when(mtmvPartitionInfo.getRelatedTable()).thenReturn(null);
            Mockito.when(mtmvPartitionInfo.getExpr()).thenReturn(expr);
            Mockito.when(mtmvPartitionInfo.getPartitionType()).thenReturn(MTMVPartitionType.EXPR);

            MTMVRelatedPartitionDescRollUpGenerator generator = new MTMVRelatedPartitionDescRollUpGenerator();
            Map<PartitionKeyDesc, Set<String>> input = Maps.newHashMap();
            // 2025-07-06 21:00:00 + 3h = 2025-07-07 00:00:00 → week trunc (Mon) = 2025-07-07
            input.put(PartitionKeyDesc.createFixed(
                    Lists.newArrayList(new PartitionValue("2025-07-06 21:00:00")),
                    Lists.newArrayList(new PartitionValue("2025-07-13 21:00:00"))),
                    Sets.newHashSet("name1"));

            Map<PartitionKeyDesc, Set<String>> res = generator.rollUpRange(input, mtmvPartitionInfo, null);
            Assert.assertEquals(1, res.size());
            PartitionKeyDesc expected = PartitionKeyDesc.createFixed(
                    Lists.newArrayList(new PartitionValue("2025-07-07 00:00:00")),
                    Lists.newArrayList(new PartitionValue("2025-07-14 00:00:00")));
            Assert.assertEquals(Sets.newHashSet("name1"), res.get(expected));
        }
    }

    @Test
    public void testRollUpRangeDateAddHourMonthUnit() throws AnalysisException {
        FunctionCallExpr expr = new FunctionCallExpr("date_trunc",
                Lists.newArrayList(
                        new TimestampArithmeticExpr("date_add", new SlotRef(null, null), new IntLiteral(3), "HOUR"),
                        new StringLiteral("month")),
                true);
        try (MockedStatic<MTMVPartitionUtil> mock = Mockito.mockStatic(MTMVPartitionUtil.class)) {
            mock.when(() -> MTMVPartitionUtil.getPartitionColumnType(
                    Mockito.nullable(MTMVRelatedTableIf.class), Mockito.nullable(String.class)))
                    .thenReturn(Type.DATETIMEV2);
            Mockito.when(mtmvPartitionInfo.getRelatedTable()).thenReturn(null);
            Mockito.when(mtmvPartitionInfo.getExpr()).thenReturn(expr);
            Mockito.when(mtmvPartitionInfo.getPartitionType()).thenReturn(MTMVPartitionType.EXPR);

            MTMVRelatedPartitionDescRollUpGenerator generator = new MTMVRelatedPartitionDescRollUpGenerator();
            Map<PartitionKeyDesc, Set<String>> input = Maps.newHashMap();
            // 2025-07-31 21:00:00 + 3h = 2025-08-01 00:00:00 → month trunc = 2025-08-01
            input.put(PartitionKeyDesc.createFixed(
                    Lists.newArrayList(new PartitionValue("2025-07-31 21:00:00")),
                    Lists.newArrayList(new PartitionValue("2025-08-31 21:00:00"))),
                    Sets.newHashSet("name1"));

            Map<PartitionKeyDesc, Set<String>> res = generator.rollUpRange(input, mtmvPartitionInfo, null);
            Assert.assertEquals(1, res.size());
            PartitionKeyDesc expected = PartitionKeyDesc.createFixed(
                    Lists.newArrayList(new PartitionValue("2025-08-01 00:00:00")),
                    Lists.newArrayList(new PartitionValue("2025-09-01 00:00:00")));
            Assert.assertEquals(Sets.newHashSet("name1"), res.get(expected));
        }
    }

    @Test
    public void testRollUpRangeDateAddHourQuarterUnit() throws AnalysisException {
        FunctionCallExpr expr = new FunctionCallExpr("date_trunc",
                Lists.newArrayList(
                        new TimestampArithmeticExpr("date_add", new SlotRef(null, null), new IntLiteral(3), "HOUR"),
                        new StringLiteral("quarter")),
                true);
        try (MockedStatic<MTMVPartitionUtil> mock = Mockito.mockStatic(MTMVPartitionUtil.class)) {
            mock.when(() -> MTMVPartitionUtil.getPartitionColumnType(
                    Mockito.nullable(MTMVRelatedTableIf.class), Mockito.nullable(String.class)))
                    .thenReturn(Type.DATETIMEV2);
            Mockito.when(mtmvPartitionInfo.getRelatedTable()).thenReturn(null);
            Mockito.when(mtmvPartitionInfo.getExpr()).thenReturn(expr);
            Mockito.when(mtmvPartitionInfo.getPartitionType()).thenReturn(MTMVPartitionType.EXPR);

            MTMVRelatedPartitionDescRollUpGenerator generator = new MTMVRelatedPartitionDescRollUpGenerator();
            Map<PartitionKeyDesc, Set<String>> input = Maps.newHashMap();
            // 2025-06-30 21:00:00 + 3h = 2025-07-01 00:00:00 → quarter trunc = 2025-07-01
            input.put(PartitionKeyDesc.createFixed(
                    Lists.newArrayList(new PartitionValue("2025-06-30 21:00:00")),
                    Lists.newArrayList(new PartitionValue("2025-09-30 21:00:00"))),
                    Sets.newHashSet("name1"));

            Map<PartitionKeyDesc, Set<String>> res = generator.rollUpRange(input, mtmvPartitionInfo, null);
            Assert.assertEquals(1, res.size());
            PartitionKeyDesc expected = PartitionKeyDesc.createFixed(
                    Lists.newArrayList(new PartitionValue("2025-07-01 00:00:00")),
                    Lists.newArrayList(new PartitionValue("2025-10-01 00:00:00")));
            Assert.assertEquals(Sets.newHashSet("name1"), res.get(expected));
        }
    }

    @Test
    public void testRollUpRangeDateAddHourYearUnit() throws AnalysisException {
        FunctionCallExpr expr = new FunctionCallExpr("date_trunc",
                Lists.newArrayList(
                        new TimestampArithmeticExpr("date_add", new SlotRef(null, null), new IntLiteral(3), "HOUR"),
                        new StringLiteral("year")),
                true);
        try (MockedStatic<MTMVPartitionUtil> mock = Mockito.mockStatic(MTMVPartitionUtil.class)) {
            mock.when(() -> MTMVPartitionUtil.getPartitionColumnType(
                    Mockito.nullable(MTMVRelatedTableIf.class), Mockito.nullable(String.class)))
                    .thenReturn(Type.DATETIMEV2);
            Mockito.when(mtmvPartitionInfo.getRelatedTable()).thenReturn(null);
            Mockito.when(mtmvPartitionInfo.getExpr()).thenReturn(expr);
            Mockito.when(mtmvPartitionInfo.getPartitionType()).thenReturn(MTMVPartitionType.EXPR);

            MTMVRelatedPartitionDescRollUpGenerator generator = new MTMVRelatedPartitionDescRollUpGenerator();
            Map<PartitionKeyDesc, Set<String>> input = Maps.newHashMap();
            // 2024-12-31 21:00:00 + 3h = 2025-01-01 00:00:00 → year trunc = 2025-01-01
            input.put(PartitionKeyDesc.createFixed(
                    Lists.newArrayList(new PartitionValue("2024-12-31 21:00:00")),
                    Lists.newArrayList(new PartitionValue("2025-12-31 21:00:00"))),
                    Sets.newHashSet("name1"));

            Map<PartitionKeyDesc, Set<String>> res = generator.rollUpRange(input, mtmvPartitionInfo, null);
            Assert.assertEquals(1, res.size());
            PartitionKeyDesc expected = PartitionKeyDesc.createFixed(
                    Lists.newArrayList(new PartitionValue("2025-01-01 00:00:00")),
                    Lists.newArrayList(new PartitionValue("2026-01-01 00:00:00")));
            Assert.assertEquals(Sets.newHashSet("name1"), res.get(expected));
        }
    }

    @Test
    public void testRollUpRangeDateAddHourHourUnit() throws AnalysisException {
        FunctionCallExpr expr = new FunctionCallExpr("date_trunc",
                Lists.newArrayList(
                        new TimestampArithmeticExpr("date_add", new SlotRef(null, null), new IntLiteral(3), "HOUR"),
                        new StringLiteral("hour")),
                true);
        try (MockedStatic<MTMVPartitionUtil> mock = Mockito.mockStatic(MTMVPartitionUtil.class)) {
            mock.when(() -> MTMVPartitionUtil.getPartitionColumnType(
                    Mockito.nullable(MTMVRelatedTableIf.class), Mockito.nullable(String.class)))
                    .thenReturn(Type.DATETIMEV2);
            Mockito.when(mtmvPartitionInfo.getRelatedTable()).thenReturn(null);
            Mockito.when(mtmvPartitionInfo.getExpr()).thenReturn(expr);
            Mockito.when(mtmvPartitionInfo.getPartitionType()).thenReturn(MTMVPartitionType.EXPR);

            MTMVRelatedPartitionDescRollUpGenerator generator = new MTMVRelatedPartitionDescRollUpGenerator();
            Map<PartitionKeyDesc, Set<String>> input = Maps.newHashMap();
            // 2025-07-25 00:00:00 + 3h = 2025-07-25 03:00:00 → hour trunc = 2025-07-25 03:00:00
            input.put(PartitionKeyDesc.createFixed(
                    Lists.newArrayList(new PartitionValue("2025-07-25 00:00:00")),
                    Lists.newArrayList(new PartitionValue("2025-07-25 01:00:00"))),
                    Sets.newHashSet("name1"));

            Map<PartitionKeyDesc, Set<String>> res = generator.rollUpRange(input, mtmvPartitionInfo, null);
            Assert.assertEquals(1, res.size());
            PartitionKeyDesc expected = PartitionKeyDesc.createFixed(
                    Lists.newArrayList(new PartitionValue("2025-07-25 03:00:00")),
                    Lists.newArrayList(new PartitionValue("2025-07-25 04:00:00")));
            Assert.assertEquals(Sets.newHashSet("name1"), res.get(expected));
        }
    }

    // =========================================================================
    // New tests — dateTimeToStr() Type.DATE path
    // =========================================================================

    @Test
    public void testRollUpRangeDateAddHourWithDateColumnType() throws AnalysisException {
        // dateTimeToStr() — Type.DATE path: output should be "yyyy-MM-dd" format
        FunctionCallExpr expr = new FunctionCallExpr("date_trunc",
                Lists.newArrayList(
                        new TimestampArithmeticExpr("date_add", new SlotRef(null, null), new IntLiteral(3), "HOUR"),
                        new StringLiteral("day")),
                true);
        try (MockedStatic<MTMVPartitionUtil> mock = Mockito.mockStatic(MTMVPartitionUtil.class)) {
            mock.when(() -> MTMVPartitionUtil.getPartitionColumnType(
                    Mockito.nullable(MTMVRelatedTableIf.class), Mockito.nullable(String.class)))
                    .thenReturn(Type.DATE); // DATE tipi → "yyyy-MM-dd" formatı
            Mockito.when(mtmvPartitionInfo.getRelatedTable()).thenReturn(null);
            Mockito.when(mtmvPartitionInfo.getExpr()).thenReturn(expr);
            Mockito.when(mtmvPartitionInfo.getPartitionType()).thenReturn(MTMVPartitionType.EXPR);

            MTMVRelatedPartitionDescRollUpGenerator generator = new MTMVRelatedPartitionDescRollUpGenerator();
            Map<PartitionKeyDesc, Set<String>> input = Maps.newHashMap();
            input.put(PartitionKeyDesc.createFixed(
                    Lists.newArrayList(new PartitionValue("2025-07-24 21:00:00")),
                    Lists.newArrayList(new PartitionValue("2025-07-25 21:00:00"))),
                    Sets.newHashSet("name1"));

            Map<PartitionKeyDesc, Set<String>> res = generator.rollUpRange(input, mtmvPartitionInfo, null);
            Assert.assertEquals(1, res.size());
            // DATE formatı: "2025-07-25" ve "2025-07-26"
            PartitionKeyDesc expected = PartitionKeyDesc.createFixed(
                    Lists.newArrayList(new PartitionValue("2025-07-25")),
                    Lists.newArrayList(new PartitionValue("2025-07-26")));
            Assert.assertEquals(Sets.newHashSet("name1"), res.get(expected));
        }
    }

    // =========================================================================
    // New tests — equals() ve hashCode()
    // =========================================================================

    @Test
    public void testEqualsAndHashCode() throws AnalysisException {
        FunctionCallExpr expr1 = new FunctionCallExpr("date_trunc",
                Lists.newArrayList(
                        new TimestampArithmeticExpr("date_add", new SlotRef(null, null), new IntLiteral(3), "HOUR"),
                        new StringLiteral("day")),
                true);
        FunctionCallExpr expr2 = new FunctionCallExpr("date_trunc",
                Lists.newArrayList(
                        new TimestampArithmeticExpr("date_add", new SlotRef(null, null), new IntLiteral(3), "HOUR"),
                        new StringLiteral("day")),
                true);
        FunctionCallExpr expr3 = new FunctionCallExpr("date_trunc",
                Lists.newArrayList(
                        new TimestampArithmeticExpr("date_add", new SlotRef(null, null), new IntLiteral(5), "HOUR"),
                        new StringLiteral("day")),
                true);
        FunctionCallExpr expr4 = new FunctionCallExpr("date_trunc",
                Lists.newArrayList(
                        new TimestampArithmeticExpr("date_add", new SlotRef(null, null), new IntLiteral(3), "HOUR"),
                        new StringLiteral("month")),
                true);

        MTMVPartitionExprDateTruncDateAddSub s1 = new MTMVPartitionExprDateTruncDateAddSub(expr1);
        MTMVPartitionExprDateTruncDateAddSub s2 = new MTMVPartitionExprDateTruncDateAddSub(expr2);
        MTMVPartitionExprDateTruncDateAddSub s3 = new MTMVPartitionExprDateTruncDateAddSub(expr3);
        MTMVPartitionExprDateTruncDateAddSub s4 = new MTMVPartitionExprDateTruncDateAddSub(expr4);

        // Aynı offset + timeUnit → equals true, hashCode eşit
        Assert.assertEquals(s1, s2);
        Assert.assertEquals(s1.hashCode(), s2.hashCode());

        // Farklı offset → equals false
        Assert.assertNotEquals(s1, s3);

        // Farklı timeUnit → equals false
        Assert.assertNotEquals(s1, s4);

        // null → equals false
        Assert.assertNotEquals(s1, null);

        // Farklı tip → equals false
        Assert.assertNotEquals(s1, "not an expr object");

        // date_sub ile negatif offset
        FunctionCallExpr exprSub3 = new FunctionCallExpr("date_trunc",
                Lists.newArrayList(
                        new TimestampArithmeticExpr("date_sub", new SlotRef(null, null), new IntLiteral(3), "HOUR"),
                        new StringLiteral("day")),
                true);
        MTMVPartitionExprDateTruncDateAddSub sSub3 = new MTMVPartitionExprDateTruncDateAddSub(exprSub3);
        // date_add +3 vs date_sub +3 → offsetHours farklı (+3 vs -3) → equals false
        Assert.assertNotEquals(s1, sSub3);
    }

    // =========================================================================
    // New tests — generateRollUpPartitionKeyDesc() (tekil, 1-to-1 senaryosu)
    // =========================================================================

    @Test
    public void testGenerateRollUpPartitionKeyDescSingleResult() throws AnalysisException {
        // generateRollUpPartitionKeyDesc() Preconditions.checkState(descs.size() == 1) → 1-to-1 senaryo
        FunctionCallExpr expr = new FunctionCallExpr("date_trunc",
                Lists.newArrayList(
                        new TimestampArithmeticExpr("date_add", new SlotRef(null, null), new IntLiteral(3), "HOUR"),
                        new StringLiteral("day")),
                true);
        MTMVPartitionExprDateTruncDateAddSub service = new MTMVPartitionExprDateTruncDateAddSub(expr);

        try (MockedStatic<MTMVPartitionUtil> mock = Mockito.mockStatic(MTMVPartitionUtil.class)) {
            mock.when(() -> MTMVPartitionUtil.getPartitionColumnType(
                    Mockito.nullable(MTMVRelatedTableIf.class), Mockito.nullable(String.class)))
                    .thenReturn(Type.DATETIMEV2);

            MTMVPartitionInfo partitionInfo = Mockito.mock(MTMVPartitionInfo.class);
            MTMVRelatedTableIf pctTable = Mockito.mock(MTMVRelatedTableIf.class);
            Mockito.when(partitionInfo.getPartitionColByPctTable(pctTable)).thenReturn("dt");

            // 1-to-1: base partition tam 1 MTMV bucket'ına karşılık geliyor
            PartitionKeyDesc input = PartitionKeyDesc.createFixed(
                    Lists.newArrayList(new PartitionValue("2025-07-24 21:00:00")),
                    Lists.newArrayList(new PartitionValue("2025-07-25 21:00:00")));

            PartitionKeyDesc result = service.generateRollUpPartitionKeyDesc(input, partitionInfo, pctTable);

            PartitionKeyDesc expected = PartitionKeyDesc.createFixed(
                    Lists.newArrayList(new PartitionValue("2025-07-25 00:00:00")),
                    Lists.newArrayList(new PartitionValue("2025-07-26 00:00:00")));
            Assert.assertEquals(expected, result);
        }
    }

    // =========================================================================
    // New tests — negatif offset (date_sub) ile UTC-midnight 1-to-N
    // =========================================================================

    @Test
    public void testRollUpRangeDateSubHourWithUtcMidnightBasePartitions() throws AnalysisException {
        // date_sub ile negatif offset: UTC-midnight base partition → 1-to-N
        FunctionCallExpr expr = new FunctionCallExpr("date_trunc",
                Lists.newArrayList(
                        new TimestampArithmeticExpr("date_sub", new SlotRef(null, null), new IntLiteral(3), "HOUR"),
                        new StringLiteral("day")),
                true);
        try (MockedStatic<MTMVPartitionUtil> mock = Mockito.mockStatic(MTMVPartitionUtil.class)) {
            mock.when(() -> MTMVPartitionUtil.getPartitionColumnType(
                    Mockito.nullable(MTMVRelatedTableIf.class), Mockito.nullable(String.class)))
                    .thenReturn(Type.DATETIMEV2);
            Mockito.when(mtmvPartitionInfo.getRelatedTable()).thenReturn(null);
            Mockito.when(mtmvPartitionInfo.getExpr()).thenReturn(expr);
            Mockito.when(mtmvPartitionInfo.getPartitionType()).thenReturn(MTMVPartitionType.EXPR);

            MTMVRelatedPartitionDescRollUpGenerator generator = new MTMVRelatedPartitionDescRollUpGenerator();
            Map<PartitionKeyDesc, Set<String>> input = Maps.newHashMap();
            // UTC-midnight: 2025-07-25 00:00:00 → 2025-07-26 00:00:00
            // -3h offset: lower = 2025-07-24 21:00:00 → day trunc = 2025-07-24
            //             upper - 1μs = 2025-07-25 23:59:59 → -3h = 2025-07-25 20:59:59 → day trunc = 2025-07-25
            // → 2 bucket: 2025-07-24 ve 2025-07-25
            input.put(PartitionKeyDesc.createFixed(
                    Lists.newArrayList(new PartitionValue("2025-07-25 00:00:00")),
                    Lists.newArrayList(new PartitionValue("2025-07-26 00:00:00"))),
                    Sets.newHashSet("name1"));

            Map<PartitionKeyDesc, Set<String>> res = generator.rollUpRange(input, mtmvPartitionInfo, null);
            Assert.assertTrue(res.size() >= 1);
            // name1 en az bir partition'da olmalı
            boolean found = res.values().stream().anyMatch(s -> s.contains("name1"));
            Assert.assertTrue(found);
        }
    }

    // =========================================================================
    // Helper
    // =========================================================================

    private PartitionKeyDesc generateInDesc(String... values) {
        List<List<PartitionValue>> partitionValues = Lists.newArrayList();
        for (String value : values) {
            List<PartitionValue> partitionValue = Lists.newArrayList(new PartitionValue(value));
            partitionValues.add(partitionValue);
        }
        return PartitionKeyDesc.createIn(partitionValues);
    }
}
