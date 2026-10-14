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
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.mtmv.MTMVPartitionInfo.MTMVPartitionType;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
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

        // Two partitions that meet at c1: p_single holds (c1=2020-01-01, c2=1) and p_double holds
        // (c1=2020-01-01, c2=2) and (c1=2038-01-01, c2=2), so an MV partitioned by c1 sees them both describe
        // the key 2020-01-01.
        createTable("CREATE TABLE `t8` (`c1` date, `c2` int)\n"
                + "ENGINE=OLAP\n"
                + "DUPLICATE KEY(`c1`)\n"
                + "PARTITION BY List(c1,c2) (PARTITION p_single VALUES IN (('2020-01-01', 1)),"
                + "PARTITION p_double VALUES IN (('2020-01-01', 2), ('2038-01-01', 2))) distributed by hash(c1) "
                + "buckets 1 properties('replication_num' = '1');");

        // Two tables of a multi-table MV whose keys meet at c1: t9's partitions cover 2020-2022, t10's
        // 2021-2022, so the two tables describe the same MV partitions for 2021-2022.
        createTable("CREATE TABLE `t9` (`c1` date, `c2` int)\n"
                + "ENGINE=OLAP\n"
                + "DUPLICATE KEY(`c1`)\n"
                + "PARTITION BY List(c1,c2) (PARTITION t9_a1 VALUES IN (('2020-01-01', 1), ('2021-01-01', 1)),"
                + "PARTITION t9_a2 VALUES IN (('2021-01-01', 2), ('2022-01-01', 2))) distributed by hash(c1) "
                + "buckets 1 properties('replication_num' = '1');");
        createTable("CREATE TABLE `t10` (`c1` date, `c2` int)\n"
                + "ENGINE=OLAP\n"
                + "DUPLICATE KEY(`c1`)\n"
                + "PARTITION BY List(c1,c2) (PARTITION t10_b1 VALUES IN (('2021-01-01', 5), ('2022-01-01', 5)))"
                + " distributed by hash(c1) buckets 1 properties('replication_num' = '1');");

        // Three partitions whose keys chain: 2020-2021, 2021-2022, 2022-2023.
        createTable("CREATE TABLE `t11` (`c1` date, `c2` int)\n"
                + "ENGINE=OLAP\n"
                + "DUPLICATE KEY(`c1`)\n"
                + "PARTITION BY List(c1,c2) (PARTITION t11_a VALUES IN (('2020-01-01', 1), ('2021-01-01', 1)),"
                + "PARTITION t11_b VALUES IN (('2021-01-01', 2), ('2022-01-01', 2)),"
                + "PARTITION t11_c VALUES IN (('2022-01-01', 3), ('2023-01-01', 3))) distributed by hash(c1) "
                + "buckets 1 properties('replication_num' = '1');");

        // The same three keys in one partition, and then a partition whose key is one of them: the second
        // table is what adding a partition over a key the first one already covers looks like.
        createTable("CREATE TABLE `t12` (`c1` date, `c2` int)\n"
                + "ENGINE=OLAP\n"
                + "DUPLICATE KEY(`c1`)\n"
                + "PARTITION BY List(c1,c2) (PARTITION t12_all VALUES IN (('2020-01-01', 1), ('2021-01-01', 2),"
                + " ('2022-01-01', 3))) distributed by hash(c1) "
                + "buckets 1 properties('replication_num' = '1');");
        createTable("CREATE TABLE `t13` (`c1` date, `c2` int)\n"
                + "ENGINE=OLAP\n"
                + "DUPLICATE KEY(`c1`)\n"
                + "PARTITION BY List(c1,c2) (PARTITION t13_all VALUES IN (('2020-01-01', 1), ('2021-01-01', 2),"
                + " ('2022-01-01', 3)), PARTITION t13_first VALUES IN (('2020-01-01', 9))) distributed by hash(c1)"
                + " buckets 1 properties('replication_num' = '1');");

        // Two tables of a two-table MV whose keys meet across them: t14 covers 2020-2021, t15 covers 2020 and
        // 2021-2022, so the MV partition holding those keys covers all three.
        createTable("CREATE TABLE `t14` (`c1` date, `c2` int)\n"
                + "ENGINE=OLAP\n"
                + "DUPLICATE KEY(`c1`)\n"
                + "PARTITION BY List(c1,c2) (PARTITION t14_p12 VALUES IN (('2020-01-01', 1),"
                + " ('2021-01-01', 1))) distributed by hash(c1)"
                + " buckets 1 properties('replication_num' = '1');");
        createTable("CREATE TABLE `t15` (`c1` date, `c2` int)\n"
                + "ENGINE=OLAP\n"
                + "DUPLICATE KEY(`c1`)\n"
                + "PARTITION BY List(c1,c2) (PARTITION t15_p1 VALUES IN (('2020-01-01', 5)),"
                + " PARTITION t15_p23 VALUES IN (('2021-01-01', 5), ('2022-01-01', 5))) distributed by hash(c1)"
                + " buckets 1 properties('replication_num' = '1');");

        // The same two keys written down in the two orders: 'Aa' and 'BB' are the classic pair that hash
        // alike, so a hash set of them iterates in the order they were added in.
        createTable("CREATE TABLE `t16` (`c1` varchar(4), `c2` int)\n"
                + "ENGINE=OLAP\n"
                + "DUPLICATE KEY(`c1`)\n"
                + "PARTITION BY List(c1,c2) (PARTITION t16_ab VALUES IN (('Aa', 1), ('BB', 1)))"
                + " distributed by hash(c1) buckets 1 properties('replication_num' = '1');");
        createTable("CREATE TABLE `t17` (`c1` varchar(4), `c2` int)\n"
                + "ENGINE=OLAP\n"
                + "DUPLICATE KEY(`c1`)\n"
                + "PARTITION BY List(c1,c2) (PARTITION t17_ba VALUES IN (('BB', 1), ('Aa', 1)))"
                + " distributed by hash(c1) buckets 1 properties('replication_num' = '1');");
    }

    @Test
    public void testTheSameCollidingKeysAreOneDescInEitherOrder() throws Exception {
        // 'Aa' and 'BB' hash alike, so a hash set of them iterates in the order it was filled in; a desc built
        // from one partition and a desc built from a partition listing the same keys the other way round used
        // to be two different descs, and `PartitionKeyDesc.equals` compares that order. The keys are written
        // out in a canonical order instead, so the key set is the desc wherever it came from.
        Column c1Column = new Column("c1", ScalarType.createVarchar(4));
        Map<PartitionKeyDesc, Map<MTMVRelatedTableIf, Set<String>>> abDescs
                = MTMVPartitionUtil.generateRelatedPartitionDescs(
                        getMTMVPartitionInfo(Lists.newArrayList("t16")), Maps.newHashMap(),
                        Lists.newArrayList(c1Column), Maps.newHashMap());
        Map<PartitionKeyDesc, Map<MTMVRelatedTableIf, Set<String>>> baDescs
                = MTMVPartitionUtil.generateRelatedPartitionDescs(
                        getMTMVPartitionInfo(Lists.newArrayList("t17")), Maps.newHashMap(),
                        Lists.newArrayList(c1Column), Maps.newHashMap());
        Assertions.assertEquals(1, abDescs.size());
        Assertions.assertEquals(1, baDescs.size());
        Assertions.assertEquals(abDescs.keySet().iterator().next(), baDescs.keySet().iterator().next());
    }

    @Test
    public void testAPrunedQueryOnOneTableKeepsTheCrossTableDesc() throws Exception {
        // At CREATE these two tables make one MV partition covering 2020-2022. A query pruned to t14's p12 and
        // t15's p1 names no partition of t15's 2021-2022 partition, but that partition's keys are still in the
        // MV partition the query reads: the desc has to come out as the one the MV holds, or the mapping
        // cannot match it and the rewrite is rejected.
        MTMVPartitionInfo mtmvPartitionInfo = getMTMVPartitionInfo(Lists.newArrayList("t14", "t15"));
        Column c1Column = new Column("c1", PrimitiveType.DATE);
        Map<List<String>, Set<String>> queryUsed = Maps.newHashMap();
        queryUsed.put(Lists.newArrayList("internal", "test", "t14"), Sets.newHashSet("t14_p12"));
        queryUsed.put(Lists.newArrayList("internal", "test", "t15"), Sets.newHashSet("t15_p1"));
        Map<PartitionKeyDesc, Map<MTMVRelatedTableIf, Set<String>>> partitionKeyDescMap
                = MTMVPartitionUtil.generateRelatedPartitionDescs(mtmvPartitionInfo, Maps.newHashMap(),
                Lists.newArrayList(c1Column), queryUsed);
        Assertions.assertEquals(1, partitionKeyDescMap.size());
        Assertions.assertEquals(3, partitionKeyDescMap.keySet().iterator().next().getInValues().size());
        OlapTable t14 = (OlapTable) Env.getCurrentEnv().getInternalCatalog().getDbOrAnalysisException("test")
                .getTableOrAnalysisException("t14");
        OlapTable t15 = (OlapTable) Env.getCurrentEnv().getInternalCatalog().getDbOrAnalysisException("test")
                .getTableOrAnalysisException("t15");
        Map<MTMVRelatedTableIf, Set<String>> onePartition = partitionKeyDescMap.values().iterator().next();
        Assertions.assertEquals(Sets.newHashSet("t14_p12"), onePartition.get(t14));
        Assertions.assertEquals(Sets.newHashSet("t15_p1"), onePartition.get(t15));
    }

    @Test
    public void testOverlappingListDescsAreOnePartition() throws Exception {
        // A partition of a list partitioned table can hold several keys of the MV's partition column, so two
        // base partitions can describe keys that meet. An MV's own partitions cannot overlap, so the two are
        // one partition whose keys are the union of theirs, and it is recorded with both of them -- which is
        // what a refresh reads for it. Two descs here would be a create failure, not a wider partition: the
        // MV would have two partition items repeating the key 2020-01-01.
        MTMVPartitionInfo mtmvPartitionInfo = getMTMVPartitionInfo(Lists.newArrayList("t8"));
        Column c1Column = new Column("c1", PrimitiveType.DATE);
        Map<PartitionKeyDesc, Map<MTMVRelatedTableIf, Set<String>>> partitionKeyDescMap
                = MTMVPartitionUtil.generateRelatedPartitionDescs(mtmvPartitionInfo, Maps.newHashMap(),
                Lists.newArrayList(c1Column), Maps.newHashMap());
        Assertions.assertEquals(1, partitionKeyDescMap.size());
        OlapTable t8 = (OlapTable) Env.getCurrentEnv().getInternalCatalog().getDbOrAnalysisException("test")
                .getTableOrAnalysisException("t8");
        Assertions.assertEquals(Sets.newHashSet("p_single", "p_double"),
                partitionKeyDescMap.values().iterator().next().get(t8));
    }

    @Test
    public void testOverlappingListDescsAreMergedAcrossTables() throws Exception {
        // Two tables of a multi-table MV describing keys that meet: at the MV's column t9 covers 2020-2022 and
        // t10 covers 2021-2022. Merged within each table, t9 comes out as 2020-2022 and t10 as 2021-2022, and
        // the MV's own partitions would repeat 2021-2022 -- which is what `checkIntersect` rejects, so the MV
        // could not be created. The keys have to be grouped across the tables, with both tables' partitions
        // named in the one partition that holds them.
        MTMVPartitionInfo mtmvPartitionInfo = getMTMVPartitionInfo(Lists.newArrayList("t9", "t10"));
        Column c1Column = new Column("c1", PrimitiveType.DATE);
        Map<PartitionKeyDesc, Map<MTMVRelatedTableIf, Set<String>>> partitionKeyDescMap
                = MTMVPartitionUtil.generateRelatedPartitionDescs(mtmvPartitionInfo, Maps.newHashMap(),
                Lists.newArrayList(c1Column), Maps.newHashMap());
        Assertions.assertEquals(1, partitionKeyDescMap.size());
        Map<MTMVRelatedTableIf, Set<String>> onePartition = partitionKeyDescMap.values().iterator().next();
        OlapTable t9 = (OlapTable) Env.getCurrentEnv().getInternalCatalog().getDbOrAnalysisException("test")
                .getTableOrAnalysisException("t9");
        OlapTable t10 = (OlapTable) Env.getCurrentEnv().getInternalCatalog().getDbOrAnalysisException("test")
                .getTableOrAnalysisException("t10");
        Assertions.assertEquals(Sets.newHashSet("t9_a1", "t9_a2"), onePartition.get(t9));
        Assertions.assertEquals(Sets.newHashSet("t10_b1"), onePartition.get(t10));
    }

    @Test
    public void testAChainOfOverlappingDescsIsOnePartition() throws Exception {
        // t11's partitions project to 2020-2021, 2021-2022 and 2022-2023: the second shares a key with the
        // first and with the third, so all four keys are one MV partition. A group of three descs has to keep
        // the whole of its keys -- taking the last desc that joined it would leave at least one key, and so
        // one committed row, with no MV partition to be read into.
        MTMVPartitionInfo mtmvPartitionInfo = getMTMVPartitionInfo(Lists.newArrayList("t11"));
        Column c1Column = new Column("c1", PrimitiveType.DATE);
        Map<PartitionKeyDesc, Map<MTMVRelatedTableIf, Set<String>>> partitionKeyDescMap
                = MTMVPartitionUtil.generateRelatedPartitionDescs(mtmvPartitionInfo, Maps.newHashMap(),
                Lists.newArrayList(c1Column), Maps.newHashMap());
        Assertions.assertEquals(1, partitionKeyDescMap.size());
        Assertions.assertEquals(4, partitionKeyDescMap.keySet().iterator().next().getInValues().size());
        OlapTable t11 = (OlapTable) Env.getCurrentEnv().getInternalCatalog().getDbOrAnalysisException("test")
                .getTableOrAnalysisException("t11");
        Assertions.assertEquals(Sets.newHashSet("t11_a", "t11_b", "t11_c"),
                partitionKeyDescMap.values().iterator().next().get(t11));
    }

    @Test
    public void testAMergedDescIsTheOneTheKeySetAlreadyHad() throws Exception {
        // t12 holds the three keys in one partition; t13 holds them in one partition plus a partition whose
        // key is one of them, which is what `ADD PARTITION` over an existing key leaves behind. The MV
        // partition for those keys has to come out as the same desc either way: `PartitionKeyDesc.equals`
        // compares the key list, so a desc whose keys are written out in another order is another desc, and an
        // alignment would drop the partition the MV holds -- with its rows -- and add an empty one.
        Column c1Column = new Column("c1", PrimitiveType.DATE);
        Map<PartitionKeyDesc, Map<MTMVRelatedTableIf, Set<String>>> t12Descs
                = MTMVPartitionUtil.generateRelatedPartitionDescs(
                        getMTMVPartitionInfo(Lists.newArrayList("t12")), Maps.newHashMap(),
                        Lists.newArrayList(c1Column), Maps.newHashMap());
        Map<PartitionKeyDesc, Map<MTMVRelatedTableIf, Set<String>>> t13Descs
                = MTMVPartitionUtil.generateRelatedPartitionDescs(
                        getMTMVPartitionInfo(Lists.newArrayList("t13")), Maps.newHashMap(),
                        Lists.newArrayList(c1Column), Maps.newHashMap());
        Assertions.assertEquals(1, t12Descs.size());
        Assertions.assertEquals(1, t13Descs.size());
        Assertions.assertEquals(t12Descs.keySet().iterator().next(), t13Descs.keySet().iterator().next());
    }

    @Test
    public void testAPrunedQueryKeepsTheMergedDesc() throws Exception {
        // A query pruned to t8's p_single passes only that partition in, but the MV partition that holds it
        // also holds p_double's keys: the desc it is looked up by -- and therefore recorded and rewritten
        // with -- is the merged one, not p_single's own key.
        MTMVPartitionInfo mtmvPartitionInfo = getMTMVPartitionInfo(Lists.newArrayList("t8"));
        Column c1Column = new Column("c1", PrimitiveType.DATE);
        Map<List<String>, Set<String>> queryUsed = Maps.newHashMap();
        queryUsed.put(Lists.newArrayList("internal", "test", "t8"), Sets.newHashSet("p_single"));
        Map<PartitionKeyDesc, Map<MTMVRelatedTableIf, Set<String>>> partitionKeyDescMap
                = MTMVPartitionUtil.generateRelatedPartitionDescs(mtmvPartitionInfo, Maps.newHashMap(),
                Lists.newArrayList(c1Column), queryUsed);
        Assertions.assertEquals(1, partitionKeyDescMap.size());
        Assertions.assertEquals(2, partitionKeyDescMap.keySet().iterator().next().getInValues().size());
        OlapTable t8 = (OlapTable) Env.getCurrentEnv().getInternalCatalog().getDbOrAnalysisException("test")
                .getTableOrAnalysisException("t8");
        Assertions.assertEquals(Sets.newHashSet("p_single"), partitionKeyDescMap.values().iterator().next().get(t8));
    }

    @Test
    public void testDisjointListDescsKeepTheirPartitions() throws Exception {
        // The control: partitions whose keys do not meet keep one MV partition each, and their names are the
        // ones they had, since a desc that was not merged is not written out again.
        MTMVPartitionInfo mtmvPartitionInfo = getMTMVPartitionInfo(Lists.newArrayList("t7"));
        Column c1Column = new Column("c1", PrimitiveType.INT);
        Map<PartitionKeyDesc, Map<MTMVRelatedTableIf, Set<String>>> partitionKeyDescMap
                = MTMVPartitionUtil.generateRelatedPartitionDescs(mtmvPartitionInfo, Maps.newHashMap(),
                Lists.newArrayList(c1Column), Maps.newHashMap());
        Assertions.assertEquals(2, partitionKeyDescMap.size());
    }

    @Test
    public void testSimple() throws Exception {
        MTMVPartitionInfo mtmvPartitionInfo = getMTMVPartitionInfo(Lists.newArrayList("t1"));
        Column c1Column = new Column("c1", PrimitiveType.DATE);
        Map<PartitionKeyDesc, Map<MTMVRelatedTableIf, Set<String>>> partitionKeyDescMap
                = MTMVPartitionUtil.generateRelatedPartitionDescs(mtmvPartitionInfo, Maps.newHashMap(),
                Lists.newArrayList(c1Column), Maps.newHashMap());
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
    public void testQueryUsedPartitionsRange() throws Exception {
        MTMVPartitionInfo mtmvPartitionInfo = getMTMVPartitionInfo(Lists.newArrayList("t1"));
        Column c1Column = new Column("c1", PrimitiveType.DATE);
        Map<List<String>, Set<String>> queryUsed = Maps.newHashMap();
        queryUsed.put(Lists.newArrayList("internal", "test", "t1"), Sets.newHashSet("p20210201"));

        Map<PartitionKeyDesc, Map<MTMVRelatedTableIf, Set<String>>> partitionKeyDescMap
                = MTMVPartitionUtil.generateRelatedPartitionDescs(mtmvPartitionInfo, Maps.newHashMap(),
                Lists.newArrayList(c1Column), queryUsed);
        Assertions.assertEquals(1, partitionKeyDescMap.size());

        Set<String> collected = Sets.newHashSet();
        partitionKeyDescMap.values().forEach(m -> m.values().forEach(collected::addAll));
        Assertions.assertEquals(Sets.newHashSet("p20210201"), collected);
    }

    @Test
    public void testQueryUsedPartitionsList() throws Exception {
        MTMVPartitionInfo mtmvPartitionInfo = getMTMVPartitionInfo(Lists.newArrayList("t2"));
        Column c1Column = new Column("c1", PrimitiveType.INT);
        Map<List<String>, Set<String>> queryUsed = Maps.newHashMap();
        queryUsed.put(Lists.newArrayList("internal", "test", "t2"), Sets.newHashSet("p1_bj"));

        Map<PartitionKeyDesc, Map<MTMVRelatedTableIf, Set<String>>> partitionKeyDescMap
                = MTMVPartitionUtil.generateRelatedPartitionDescs(mtmvPartitionInfo, Maps.newHashMap(),
                Lists.newArrayList(c1Column), queryUsed);
        Assertions.assertEquals(1, partitionKeyDescMap.size());

        Set<String> collected = Sets.newHashSet();
        partitionKeyDescMap.values().forEach(m -> m.values().forEach(collected::addAll));
        Assertions.assertEquals(Sets.newHashSet("p1_bj"), collected);
    }

    @Test
    public void testLimit() throws Exception {
        MTMVPartitionInfo mtmvPartitionInfo = getMTMVPartitionInfo(Lists.newArrayList("t1"));
        Column c1Column = new Column("c1", PrimitiveType.DATE);
        HashMap<String, String> mvProperty = Maps.newHashMap();
        mvProperty.put(PropertyAnalyzer.PROPERTIES_PARTITION_SYNC_LIMIT, "1");
        Map<PartitionKeyDesc, Map<MTMVRelatedTableIf, Set<String>>> partitionKeyDescMap
                = MTMVPartitionUtil.generateRelatedPartitionDescs(mtmvPartitionInfo, mvProperty,
                Lists.newArrayList(c1Column), Maps.newHashMap());
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
                Lists.newArrayList(c1Column), Maps.newHashMap());
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
                Lists.newArrayList(c1Column), Maps.newHashMap());
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
                        Lists.newArrayList(c1Column), Maps.newHashMap()));
    }

    @Test
    public void testIntersectList() throws Exception {
        // t6 describes the keys 1 and 2, t7 the keys 1, 3 and 2: the two tables' descs meet. They are one MV
        // partition holding all three keys and naming each table's partitions, rather than a rejection -- the
        // MV cannot have two partitions repeating a key, but it can have one that names both tables'.
        MTMVPartitionInfo mtmvPartitionInfo = getMTMVPartitionInfo(Lists.newArrayList("t6", "t7"));
        Column c1Column = new Column("c1", PrimitiveType.DATE);
        Map<PartitionKeyDesc, Map<MTMVRelatedTableIf, Set<String>>> partitionKeyDescMap
                = MTMVPartitionUtil.generateRelatedPartitionDescs(mtmvPartitionInfo, Maps.newHashMap(),
                Lists.newArrayList(c1Column), Maps.newHashMap());
        OlapTable t6 = (OlapTable) Env.getCurrentEnv().getInternalCatalog().getDbOrAnalysisException("test")
                .getTableOrAnalysisException("t6");
        OlapTable t7 = (OlapTable) Env.getCurrentEnv().getInternalCatalog().getDbOrAnalysisException("test")
                .getTableOrAnalysisException("t7");
        // The keys 1 and 3 meet through t7's p1_3 and are one partition; key 2 is its own, and each names the
        // partitions of both tables that hold its keys.
        Assertions.assertEquals(2, partitionKeyDescMap.size());
        Map<MTMVRelatedTableIf, Set<String>> twoKeyPartition = null;
        Map<MTMVRelatedTableIf, Set<String>> oneKeyPartition = null;
        for (Map.Entry<PartitionKeyDesc, Map<MTMVRelatedTableIf, Set<String>>> entry
                : partitionKeyDescMap.entrySet()) {
            if (entry.getKey().getInValues().size() == 2) {
                twoKeyPartition = entry.getValue();
            } else {
                oneKeyPartition = entry.getValue();
            }
        }
        Assertions.assertEquals(Sets.newHashSet("p1"), twoKeyPartition.get(t6));
        Assertions.assertEquals(Sets.newHashSet("p1_3"), twoKeyPartition.get(t7));
        Assertions.assertEquals(Sets.newHashSet("p2"), oneKeyPartition.get(t6));
        Assertions.assertEquals(Sets.newHashSet("p2"), oneKeyPartition.get(t7));
    }

    @Test
    public void testMultiPctTables() throws Exception {
        MTMVPartitionInfo mtmvPartitionInfo = getMTMVPartitionInfo(Lists.newArrayList("t1", "t4"));
        Column c1Column = new Column("c1", PrimitiveType.DATE);
        Map<PartitionKeyDesc, Map<MTMVRelatedTableIf, Set<String>>> partitionKeyDescMap
                = MTMVPartitionUtil.generateRelatedPartitionDescs(mtmvPartitionInfo, Maps.newHashMap(),
                Lists.newArrayList(c1Column), Maps.newHashMap());
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
                Lists.newArrayList(c1Column), Maps.newHashMap());
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
                Lists.newArrayList(c1Column), Maps.newHashMap());
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
                        Lists.newArrayList(c1Column), Maps.newHashMap()));
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
