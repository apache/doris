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
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.PartitionKeyDesc;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.mtmv.MTMVPartitionInfo.MTMVPartitionType;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MTMVRelatedPartitionDescGeneratorTest extends TestWithFeService {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabaseAndUse("test");

        createTable("CREATE TABLE `t1` (`c1` date, `c2` int)\n"
                + "ENGINE=OLAP\n"
                + "DUPLICATE KEY(`c1`)\n"
                + "PARTITION BY RANGE(c1) (PARTITION p20210201 VALUES [('2021-02-01'), ('2021-02-02')),"
                + "PARTITION p20210202 VALUES [('2021-02-02'), ('2021-02-03')),"
                + "PARTITION p20500203 VALUES [('2050-02-03'), ('2050-02-04'))) distributed by hash(c1) "
                + "buckets 1 properties('replication_num' = '1');");

        createTable("CREATE TABLE `t2` (`c1` int, `c2` VARCHAR(100))\n"
                + "ENGINE=OLAP\n"
                + "DUPLICATE KEY(`c1`)\n"
                + "PARTITION BY List(c1,c2) (PARTITION p1_bj VALUES IN (('1','bj')),"
                + "PARTITION p2_bj VALUES IN (('2','bj')),"
                + "PARTITION p1_sh VALUES IN (('1','sh'))) distributed by hash(c1) "
                + "buckets 1 properties('replication_num' = '1');");

        createTable("CREATE TABLE `t3` (`c1` date, `c2` int)\n"
                + "ENGINE=OLAP\n"
                + "DUPLICATE KEY(`c1`)\n"
                + "PARTITION BY RANGE(c1) (PARTITION p20210201 VALUES [('2021-02-01'), ('2021-02-03')),"
                + "PARTITION p20500203 VALUES [('2050-02-03'), ('2050-02-04'))) distributed by hash(c1) "
                + "buckets 1 properties('replication_num' = '1');");

        createTable("CREATE TABLE `t4` (`c1` date, `c2` int)\n"
                + "ENGINE=OLAP\n"
                + "DUPLICATE KEY(`c1`)\n"
                + "PARTITION BY RANGE(c1) (PARTITION p20500204 VALUES [('2050-02-04'), ('2050-02-05')),"
                + "PARTITION p20500203 VALUES [('2050-02-03'), ('2050-02-04'))) distributed by hash(c1) "
                + "buckets 1 properties('replication_num' = '1');");

        createTable("CREATE TABLE `t5` (`c1` int, `c2` VARCHAR(100))\n"
                + "ENGINE=OLAP\n"
                + "DUPLICATE KEY(`c1`)\n"
                + "PARTITION BY List(c1,c2) (PARTITION p1_bj VALUES IN (('1','bj')),"
                + "PARTITION p2_bj VALUES IN (('2','bj')),"
                + "PARTITION p1_sh VALUES IN (('1','sh'))) distributed by hash(c1) "
                + "buckets 1 properties('replication_num' = '1');");

        createTable("CREATE TABLE `t6` (`c1` int, `c2` VARCHAR(100))\n"
                + "ENGINE=OLAP\n"
                + "DUPLICATE KEY(`c1`)\n"
                + "PARTITION BY List(c1) (PARTITION p1 VALUES IN (('1')),"
                + "PARTITION p2 VALUES IN (('2'))) distributed by hash(c1) "
                + "buckets 1 properties('replication_num' = '1');");

        createTable("CREATE TABLE `t7` (`c1` int, `c2` VARCHAR(100))\n"
                + "ENGINE=OLAP\n"
                + "DUPLICATE KEY(`c1`)\n"
                + "PARTITION BY List(c1) (PARTITION p1_3 VALUES IN (('1'),('3')),"
                + "PARTITION p2 VALUES IN (('2'))) distributed by hash(c1) "
                + "buckets 1 properties('replication_num' = '1');");
    }

    @Test
    public void testSimple() throws Exception {
        MTMVPartitionInfo mtmvPartitionInfo = getMTMVPartitionInfo(Lists.newArrayList("t1"));
        Column c1Column = new Column("c1", PrimitiveType.DATE);
        Map<PartitionKeyDesc, Map<MTMVRelatedTableIf, Set<String>>> partitionKeyDescMap
                = MTMVPartitionUtil.generateRelatedPartitionDescs(mtmvPartitionInfo, Maps.newHashMap(),
                Lists.newArrayList(c1Column));
        // 3 partition
        Assertions.assertEquals(3, partitionKeyDescMap.size());
        OlapTable t1 = (OlapTable) Env.getCurrentEnv().getInternalCatalog().getDbOrAnalysisException("test")
                .getTableOrAnalysisException("t1");
        for (Map<MTMVRelatedTableIf, Set<String>> onePartitionMap : partitionKeyDescMap.values()) {
            // key is t1
            // value like p20210201
            Assertions.assertEquals(1, onePartitionMap.size());
            Assertions.assertEquals(1, onePartitionMap.get(t1).size());
        }
    }

    @Test
    public void testLimit() throws Exception {
        MTMVPartitionInfo mtmvPartitionInfo = getMTMVPartitionInfo(Lists.newArrayList("t1"));
        Column c1Column = new Column("c1", PrimitiveType.DATE);
        HashMap<String, String> mvProperty = Maps.newHashMap();
        mvProperty.put(PropertyAnalyzer.PROPERTIES_PARTITION_SYNC_LIMIT, "1");
        Map<PartitionKeyDesc, Map<MTMVRelatedTableIf, Set<String>>> partitionKeyDescMap
                = MTMVPartitionUtil.generateRelatedPartitionDescs(mtmvPartitionInfo, mvProperty,
                Lists.newArrayList(c1Column));
        // 3 partition
        Assertions.assertEquals(1, partitionKeyDescMap.size());
        OlapTable t1 = (OlapTable) Env.getCurrentEnv().getInternalCatalog().getDbOrAnalysisException("test")
                .getTableOrAnalysisException("t1");
        for (Map<MTMVRelatedTableIf, Set<String>> onePartitionMap : partitionKeyDescMap.values()) {
            // key is t1
            // value is p20210201
            Assertions.assertEquals(1, onePartitionMap.size());
            Assertions.assertEquals(1, onePartitionMap.get(t1).size());
        }
    }

    @Test
    public void testOneCol() throws Exception {
        MTMVPartitionInfo mtmvPartitionInfo = getMTMVPartitionInfo(Lists.newArrayList("t2"));
        Column c1Column = new Column("c1", PrimitiveType.INT);
        Map<PartitionKeyDesc, Map<MTMVRelatedTableIf, Set<String>>> partitionKeyDescMap
                = MTMVPartitionUtil.generateRelatedPartitionDescs(mtmvPartitionInfo, Maps.newHashMap(),
                Lists.newArrayList(c1Column));
        // 3 partition
        Assertions.assertEquals(2, partitionKeyDescMap.size());
        OlapTable t2 = (OlapTable) Env.getCurrentEnv().getInternalCatalog().getDbOrAnalysisException("test")
                .getTableOrAnalysisException("t2");
        for (Map<MTMVRelatedTableIf, Set<String>> onePartitionMap : partitionKeyDescMap.values()) {
            // key is t1
            // value like p20210201
            Assertions.assertEquals(1, onePartitionMap.size());
            int partitionNum = onePartitionMap.get(t2).size();
            Assertions.assertTrue(partitionNum == 1 || partitionNum == 2);
        }
    }

    @Test
    public void testDateTrunc() throws Exception {
        MTMVPartitionInfo mtmvPartitionInfo = getMTMVPartitionInfo(Lists.newArrayList("t1"));
        mtmvPartitionInfo.setPartitionType(MTMVPartitionType.EXPR);
        List<Expr> params = new ArrayList<>();
        params.add(new StringLiteral("c1"));
        params.add(new StringLiteral("month"));
        FunctionCallExpr functionCallExpr = new FunctionCallExpr("date_trunc", params, true);
        mtmvPartitionInfo.setExpr(functionCallExpr);
        Column c1Column = new Column("c1", PrimitiveType.INT);
        Map<PartitionKeyDesc, Map<MTMVRelatedTableIf, Set<String>>> partitionKeyDescMap
                = MTMVPartitionUtil.generateRelatedPartitionDescs(mtmvPartitionInfo, Maps.newHashMap(),
                Lists.newArrayList(c1Column));
        // 2 partition
        Assertions.assertEquals(2, partitionKeyDescMap.size());
        OlapTable t1 = (OlapTable) Env.getCurrentEnv().getInternalCatalog().getDbOrAnalysisException("test")
                .getTableOrAnalysisException("t1");
        for (Map<MTMVRelatedTableIf, Set<String>> onePartitionMap : partitionKeyDescMap.values()) {
            Assertions.assertEquals(1, onePartitionMap.size());
            int partitionNum = onePartitionMap.get(t1).size();
            Assertions.assertTrue(partitionNum == 1 || partitionNum == 2);
        }
    }

    @Test
    public void testIntersect() throws Exception {
        MTMVPartitionInfo mtmvPartitionInfo = getMTMVPartitionInfo(Lists.newArrayList("t1", "t3"));
        Column c1Column = new Column("c1", PrimitiveType.DATE);
        Assertions.assertThrows(AnalysisException.class,
                () -> MTMVPartitionUtil.generateRelatedPartitionDescs(mtmvPartitionInfo, Maps.newHashMap(),
                        Lists.newArrayList(c1Column)));
    }

    @Test
    public void testIntersectList() throws Exception {
        MTMVPartitionInfo mtmvPartitionInfo = getMTMVPartitionInfo(Lists.newArrayList("t6", "t7"));
        Column c1Column = new Column("c1", PrimitiveType.DATE);
        Assertions.assertThrows(AnalysisException.class,
                () -> MTMVPartitionUtil.generateRelatedPartitionDescs(mtmvPartitionInfo, Maps.newHashMap(),
                        Lists.newArrayList(c1Column)));
    }

    @Test
    public void testMultiPctTables() throws Exception {
        MTMVPartitionInfo mtmvPartitionInfo = getMTMVPartitionInfo(Lists.newArrayList("t1", "t4"));
        Column c1Column = new Column("c1", PrimitiveType.DATE);
        Map<PartitionKeyDesc, Map<MTMVRelatedTableIf, Set<String>>> partitionKeyDescMap
                = MTMVPartitionUtil.generateRelatedPartitionDescs(mtmvPartitionInfo, Maps.newHashMap(),
                Lists.newArrayList(c1Column));
        // 4 partition
        Assertions.assertEquals(4, partitionKeyDescMap.size());
        boolean hasOne = false;
        boolean hasTwo = false;
        for (Map<MTMVRelatedTableIf, Set<String>> onePartitionMap : partitionKeyDescMap.values()) {
            if (onePartitionMap.size() == 1) {
                hasOne = true;
            } else if (onePartitionMap.size() == 2) {
                hasTwo = true;
            } else {
                throw new RuntimeException("failed");
            }
        }
        Assertions.assertTrue(hasOne);
        Assertions.assertTrue(hasTwo);
    }

    @Test
    public void testMultiList() throws Exception {
        MTMVPartitionInfo mtmvPartitionInfo = getMTMVPartitionInfo(Lists.newArrayList("t2", "t5"));
        Column c1Column = new Column("c1", PrimitiveType.INT);
        Map<PartitionKeyDesc, Map<MTMVRelatedTableIf, Set<String>>> partitionKeyDescMap
                = MTMVPartitionUtil.generateRelatedPartitionDescs(mtmvPartitionInfo, Maps.newHashMap(),
                Lists.newArrayList(c1Column));
        // 2 partition
        Assertions.assertEquals(2, partitionKeyDescMap.size());
        OlapTable t2 = (OlapTable) Env.getCurrentEnv().getInternalCatalog().getDbOrAnalysisException("test")
                .getTableOrAnalysisException("t2");
        for (Map<MTMVRelatedTableIf, Set<String>> onePartitionMap : partitionKeyDescMap.values()) {
            // key is t1
            // value like p20210201
            Assertions.assertEquals(2, onePartitionMap.size());
            int partitionNum = onePartitionMap.get(t2).size();
            Assertions.assertTrue(partitionNum == 1 || partitionNum == 2);
        }
    }

    @Test
    public void testMultiListComplex() throws Exception {
        MTMVPartitionInfo mtmvPartitionInfo = getMTMVPartitionInfo(Lists.newArrayList("t2", "t6"));
        Column c1Column = new Column("c1", PrimitiveType.DATE);
        Map<PartitionKeyDesc, Map<MTMVRelatedTableIf, Set<String>>> partitionKeyDescMap
                = MTMVPartitionUtil.generateRelatedPartitionDescs(mtmvPartitionInfo, Maps.newHashMap(),
                Lists.newArrayList(c1Column));
        // 2 partition
        Assertions.assertEquals(2, partitionKeyDescMap.size());
        OlapTable t6 = (OlapTable) Env.getCurrentEnv().getInternalCatalog().getDbOrAnalysisException("test")
                .getTableOrAnalysisException("t6");
        for (Map<MTMVRelatedTableIf, Set<String>> onePartitionMap : partitionKeyDescMap.values()) {
            // key is t1
            // value like p20210201
            Assertions.assertEquals(2, onePartitionMap.size());
            int partitionNum = onePartitionMap.get(t6).size();
            Assertions.assertTrue(partitionNum == 1);
        }
    }

    @Test
    public void testBothListAndRange() throws Exception {
        MTMVPartitionInfo mtmvPartitionInfo = getMTMVPartitionInfo(Lists.newArrayList("t1", "t2"));
        Column c1Column = new Column("c1", PrimitiveType.DATE);
        Assertions.assertThrows(AnalysisException.class,
                () -> MTMVPartitionUtil.generateRelatedPartitionDescs(mtmvPartitionInfo, Maps.newHashMap(),
                        Lists.newArrayList(c1Column)));
    }

    private MTMVPartitionInfo getMTMVPartitionInfo(List<String> pctTableNames) throws AnalysisException {
        MTMVPartitionInfo mtmvPartitionInfo = new MTMVPartitionInfo();
        mtmvPartitionInfo.setPartitionType(MTMVPartitionType.FOLLOW_BASE_TABLE);
        mtmvPartitionInfo.setPartitionCol("c1");
        List<BaseColInfo> pctInfos = new ArrayList<>();
        for (String pctTableName : pctTableNames) {
            OlapTable pctOlapTable = (OlapTable) Env.getCurrentEnv().getInternalCatalog()
                    .getDbOrAnalysisException("test")
                    .getTableOrAnalysisException(pctTableName);
            BaseTableInfo pctTableInfo = new BaseTableInfo(pctOlapTable);
            BaseColInfo pctColInfo = new BaseColInfo("c1", pctTableInfo);
            pctInfos.add(pctColInfo);
        }
        mtmvPartitionInfo.setPctInfos(pctInfos);
        return mtmvPartitionInfo;
    }

}
