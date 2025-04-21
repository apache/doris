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

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.InPredicate;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TableName;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.RandomDistributionInfo;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Replica.ReplicaState;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.qe.GlobalVariable;
import org.apache.doris.system.Backend;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;


public class OlapScanNodeTest {
    // columnA in (1) hashmode=3
    @Test
    public void testHashDistributionOneUser() throws AnalysisException {

        List<Long> partitions = new ArrayList<>();
        partitions.add(new Long(0));
        partitions.add(new Long(1));
        partitions.add(new Long(2));


        List<Column> columns = Lists.newArrayList();
        columns.add(new Column("columnA", PrimitiveType.BIGINT));

        List<Expr> inList = Lists.newArrayList();
        inList.add(new IntLiteral(1));

        Expr compareExpr = new SlotRef(new TableName(InternalCatalog.INTERNAL_CATALOG_NAME, "db", "tableName"),
                "columnA");
        InPredicate inPredicate = new InPredicate(compareExpr, inList, false);

        PartitionColumnFilter  columnFilter = new PartitionColumnFilter();
        columnFilter.setInPredicate(inPredicate);
        Map<String, PartitionColumnFilter> filterMap = Maps.newHashMap();
        filterMap.put("columnA", columnFilter);

        DistributionPruner partitionPruner  = new HashDistributionPruner(
                partitions,
                columns,
                filterMap,
                3,
                true);

        Collection<Long> ids = partitionPruner.prune();
        Assert.assertEquals(ids.size(), 1);

        for (Long id : ids) {
            Assert.assertEquals((1 & 0xffffffff) % 3, id.intValue());
        }
    }

    // columnA in (1, 2 ,3, 4, 5, 6) hashmode=3
    @Test
    public void testHashPartitionManyUser() throws AnalysisException {

        List<Long> partitions = new ArrayList<>();
        partitions.add(new Long(0));
        partitions.add(new Long(1));
        partitions.add(new Long(2));

        List<Column> columns = Lists.newArrayList();
        columns.add(new Column("columnA", PrimitiveType.BIGINT));

        List<Expr> inList = Lists.newArrayList();
        inList.add(new IntLiteral(1));
        inList.add(new IntLiteral(2));
        inList.add(new IntLiteral(3));
        inList.add(new IntLiteral(4));
        inList.add(new IntLiteral(5));
        inList.add(new IntLiteral(6));

        Expr compareExpr = new SlotRef(new TableName(InternalCatalog.INTERNAL_CATALOG_NAME, "db", "tableName"),
                "columnA");
        InPredicate inPredicate = new InPredicate(compareExpr, inList, false);

        PartitionColumnFilter  columnFilter = new PartitionColumnFilter();
        columnFilter.setInPredicate(inPredicate);
        Map<String, PartitionColumnFilter> filterMap = Maps.newHashMap();
        filterMap.put("columnA", columnFilter);

        DistributionPruner partitionPruner  = new HashDistributionPruner(
                partitions,
                columns,
                filterMap,
                3,
                true);

        Collection<Long> ids = partitionPruner.prune();
        Assert.assertEquals(ids.size(), 3);
    }

    @Test
    public void testHashForIntLiteral() {
        { // CHECKSTYLE IGNORE THIS LINE
            PartitionKey hashKey = new PartitionKey();
            hashKey.pushColumn(new IntLiteral(1), PrimitiveType.BIGINT);
            long hashValue = hashKey.getHashValue();
            long mod = (int) ((hashValue & 0xffffffff) % 3);
            Assert.assertEquals(mod, 1);
        } // CHECKSTYLE IGNORE THIS LINE
        { // CHECKSTYLE IGNORE THIS LINE
            PartitionKey hashKey = new PartitionKey();
            hashKey.pushColumn(new IntLiteral(2), PrimitiveType.BIGINT);
            long hashValue = hashKey.getHashValue();
            long mod = (int) ((hashValue & 0xffffffff) % 3);
            Assert.assertEquals(mod, 0);
        } // CHECKSTYLE IGNORE THIS LINE
        { // CHECKSTYLE IGNORE THIS LINE
            PartitionKey hashKey = new PartitionKey();
            hashKey.pushColumn(new IntLiteral(3), PrimitiveType.BIGINT);
            long hashValue = hashKey.getHashValue();
            long mod = (int) ((hashValue & 0xffffffff) % 3);
            Assert.assertEquals(mod, 0);
        } // CHECKSTYLE IGNORE THIS LINE
        { // CHECKSTYLE IGNORE THIS LINE
            PartitionKey hashKey = new PartitionKey();
            hashKey.pushColumn(new IntLiteral(4), PrimitiveType.BIGINT);
            long hashValue = hashKey.getHashValue();
            long mod = (int) ((hashValue & 0xffffffff) % 3);
            Assert.assertEquals(mod, 1);
        } // CHECKSTYLE IGNORE THIS LINE
        { // CHECKSTYLE IGNORE THIS LINE
            PartitionKey hashKey = new PartitionKey();
            hashKey.pushColumn(new IntLiteral(5), PrimitiveType.BIGINT);
            long hashValue = hashKey.getHashValue();
            long mod = (int) ((hashValue & 0xffffffff) % 3);
            Assert.assertEquals(mod, 2);
        } // CHECKSTYLE IGNORE THIS LINE
        { // CHECKSTYLE IGNORE THIS LINE
            PartitionKey hashKey = new PartitionKey();
            hashKey.pushColumn(new IntLiteral(6), PrimitiveType.BIGINT);
            long hashValue = hashKey.getHashValue();
            long mod = (int) ((hashValue & 0xffffffff) % 3);
            Assert.assertEquals(mod, 2);
        } // CHECKSTYLE IGNORE THIS LINE
    }

    @Test
    public void testTableNameWithAlias() {
        GlobalVariable.lowerCaseTableNames = 1;
        SlotRef slot = new SlotRef(new TableName("DB.TBL"), Column.DELETE_SIGN);
        Assert.assertTrue(slot.getTableName().toString().equals("DB.tbl"));
    }

    @Test
    public void testSelectLoadBalanceBeForTablets() {
        final int[] tabletNums = { 1, 8, 16, 32, 64, 128 };
        final int[] backendNums = { 1, 3, 4, 6, 8, 16 };
        final int[] replicaNums = { 1, 2, 3, 4, 5 };
        final int visibleVersion = 1;
        final boolean skipMissingVersion = false;
        int checkTimes = 10 * tabletNums.length * backendNums.length * replicaNums.length;
        Random random = new Random();
        while (checkTimes > 0) {
            int tabletNum = tabletNums[random.nextInt(1000) % tabletNums.length];
            int backendNum = backendNums[random.nextInt(1000) % backendNums.length];
            int replicaNum = replicaNums[random.nextInt(1000) % replicaNums.length];
            if (replicaNum > backendNum) {
                replicaNum = backendNum;
            }
            checkTimes--;

            List<Tablet> tablets = new ArrayList<>();
            List<Backend> backends = new ArrayList<>();
            MaterializedIndex baseIndex = new MaterializedIndex(30001, MaterializedIndex.IndexState.NORMAL);
            RandomDistributionInfo distributionInfo = new RandomDistributionInfo(tabletNum);
            Partition partition = new Partition(20000L, "testTbl", baseIndex, distributionInfo);
            partition.setVisibleVersionAndTime(visibleVersion, 1578762000000L);     //2020-01-12 1:00:00

            for (int i = 0; i < tabletNum; i++) {
                Tablet tablet = new Tablet(i);
                tablets.add(tablet);
            }
            for (int i = 0; i < backendNum; i++) {
                Backend backend = new Backend(i, "127.0.0.1" + i, 9030 + i);
                backend.setAlive(true);
                Env.getCurrentSystemInfo().addBackend(backend);
                backends.add(backend);
            }

            for (int i = 0; i < tabletNum; i++) {
                HashSet<Integer> beSet = new HashSet<>();
                Tablet tablet = tablets.get(i);
                for (int j = 0; j < replicaNum; j++) {
                    long replicaId = i * 1000 + j;
                    int index = random.nextInt(1000) % backendNum;
                    while (beSet.contains(index)) {
                        index = random.nextInt(1000) % backendNum;
                    }
                    beSet.add(index);
                    Backend backend = backends.get(index);
                    Replica replica = new Replica(replicaId, backend.getId(), ReplicaState.NORMAL, visibleVersion, 0);
                    tablet.addReplica(replica, true);
                }
            }


            HashMap<Long, Long> tabletToLoadBalanceBe = new HashMap<>();
            OlapScanNode.selectLoadBalanceBeForTablets(tablets, partition, false, tabletToLoadBalanceBe);
            assert !tabletToLoadBalanceBe.isEmpty();

            // check result
            // key : backend id  value: tablet ids that can be applyed to key
            final HashMap<Long, List<Long>> beToTablets = new HashMap<>();

            for (Tablet tablet : tablets) {
                long tabletId = tablet.getId();

                // random shuffle List && only collect one copy
                List<Replica> replicas = tablet.getQueryableReplicas(visibleVersion, skipMissingVersion);
                // init
                replicas.forEach(replica -> {
                    long backendId = replica.getBackendId();
                    Backend backend = Env.getCurrentSystemInfo().getBackend(replica.getBackendId());
                    if (backend != null && backend.isAlive()) {

                        List<Long> beTabletIds = beToTablets.getOrDefault(backendId, new ArrayList<>());
                        if (!beTabletIds.contains(tabletId)) {
                            beTabletIds.add(tabletId);
                            beToTablets.put(backendId, beTabletIds);
                        }
                    }
                });
            }

            HashMap<Long, List<Long>> bes = new HashMap<>();
            for (Map.Entry<Long, Long> entry : tabletToLoadBalanceBe.entrySet()) {
                long tabletId = entry.getKey();
                long beId = entry.getValue();
                List<Long> tabletIds = bes.getOrDefault(beId, new ArrayList<>());
                if (!tabletIds.contains(tabletId)) {
                    tabletIds.add(tabletId);
                    bes.put(beId, tabletIds);
                }
            }

            assert !bes.isEmpty();
            int beCapacity = (int) (Math.ceil((float) tabletNum / bes.size()));
            for (Map.Entry<Long, List<Long>> entry : bes.entrySet()) {
                if (entry.getValue().size() > beCapacity) {
                    System.out.println("tabletNum: " + tabletNum);
                    System.out.println("beNum: " + bes.size());
                    System.out.println("replicaNum: " + replicaNum);
                    System.out.println("beCapacity: " + beCapacity);
                    System.out.println("beToTablets: " + beToTablets);
                    System.out.println("result: " + bes);
                    // calculate tolerance
                    int count = 0;
                    long backendId = entry.getKey();
                    int size = beToTablets.get(backendId).size();
                    System.out.println("out of beCapacity, tablets: " + entry.getValue());

                    int sum = 0;
                    for (Map.Entry<Long, List<Long>> tmp : beToTablets.entrySet()) {
                        if (tmp.getValue().size() <= size) {
                            count++;
                            sum += size - tmp.getValue().size();
                        }
                    }
                    int tolerance = (int) Math.ceil((float) sum / count);
                    if (tolerance == 0) {
                        tolerance = 1;
                    }
                    System.out.println("tolerance: " + tolerance);
                    assert entry.getValue().size() <= beCapacity + tolerance;
                }
            }
        }
    }
}
