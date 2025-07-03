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

package org.apache.doris.clone;

import org.apache.doris.alter.Alter;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.DebugPointUtil;
import org.apache.doris.resource.Tag;
import org.apache.doris.system.Backend;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DecreaseReplicationNumTest extends TestWithFeService {

    private Database db;

    @Override
    protected void beforeCreatingConnectContext() throws Exception {
        Config.enable_debug_points = true;
        Config.disable_balance = true;
        Config.drop_backend_after_decommission = false;
        Config.tablet_schedule_interval_ms = 1000L;
        Config.tablet_checker_interval_ms = 1000L;
    }

    @Override
    protected int backendNum() {
        return 5;
    }

    @Override
    protected void runBeforeAll() throws Exception {
        Thread.sleep(1000);
        createDatabase("test");
        useDatabase("test");
        db = Env.getCurrentInternalCatalog().getDbOrMetaException("test");
    }

    @Override
    protected void runBeforeEach() throws Exception {
        // set back to default value
        Config.max_scheduling_tablets = 2000;
        for (Table table : db.getTables()) {
            dropTable(table.getName(), true);
        }
        Env.getCurrentEnv().getTabletScheduler().clear();
        DebugPointUtil.clearDebugPoints();
        Assertions.assertTrue(checkBEHeartbeat(Env.getCurrentSystemInfo().getBackendsByTag(Tag.DEFAULT_BACKEND_TAG)));
    }


    @Test
    public void testDecreaseReplicaNum() throws Exception {
        createTable("CREATE TABLE tbl1 (k INT) DISTRIBUTED BY HASH(k) BUCKETS 10"
                + " PROPERTIES ('replication_num' = '5')");

        OlapTable table = (OlapTable) db.getTableOrMetaException("tbl1");
        Partition partition = table.getPartitions().iterator().next();
        Map<Long, Long> beIdToTabletNum = Alter.getReplicaCountByBackend(partition);
        Assertions.assertEquals(5, beIdToTabletNum.size());
        Assertions.assertEquals(Lists.newArrayList(10L, 10L, 10L, 10L, 10L), new ArrayList<>(beIdToTabletNum.values()));
        beIdToTabletNum.forEach((key, value) -> Assertions.assertEquals(value, 10L));

        List<Backend> backends = Env.getCurrentSystemInfo().getBackendsByTag(Tag.DEFAULT_BACKEND_TAG);
        Assertions.assertEquals(backendNum(), backends.size());
        Backend highLoadBe = backends.get(0);
        DebugPointUtil.addDebugPointWithValue("FE.HIGH_LOAD_BE_ID", highLoadBe.getId());

        alterTableSync("ALTER TABLE tbl1 MODIFY PARTITION(*) SET ('replication_num' = '3')");
        boolean succ = false;
        for (int i = 0; i < 100; i++) {
            beIdToTabletNum = Alter.getReplicaCountByBackend(partition);
            Set<Long> afterAlter = new HashSet<>(beIdToTabletNum.values());
            // wait for scheduler
            if (afterAlter.size() == 1 && !beIdToTabletNum.containsValue(10L)) {
                Assertions.assertTrue(afterAlter.contains(6L));
                Assertions.assertEquals(Lists.newArrayList(6L, 6L, 6L, 6L, 6L), new ArrayList<>(beIdToTabletNum.values()));
                succ = true;
                break;
            }
            Thread.sleep(1000);
        }
        Assertions.assertTrue(succ);
    }

    @Test
    public void testDecreaseMultiPartitionReplicaNum() throws Exception {
        createTable("create table test_multi(id int, part int) "
                + "partition by range(part) ("
                + "  partition p1 values[('1'), ('2')),"
                + "  partition p2 values[('2'), ('3')),"
                + "  partition p3 values[('3'), ('4'))"
                + ") "
                + "distributed by hash(id) BUCKETS 9 "
                + "properties ('replication_num'='4')");

        OlapTable table = (OlapTable) db.getTableOrMetaException("test_multi");
        List<Partition> partitions = table.getAllPartitions();
        partitions.forEach(p -> {
            Map<Long, Long> beIdToTabletNum = Alter.getReplicaCountByBackend(p);
            Assertions.assertEquals(5, beIdToTabletNum.size());
            List<Long> sortedValues = new ArrayList<>(beIdToTabletNum.values());
            sortedValues.sort(Collections.reverseOrder());
            Assertions.assertEquals(Lists.newArrayList(8L, 7L, 7L, 7L, 7L), sortedValues);
        });

        List<Backend> backends = Env.getCurrentSystemInfo().getBackendsByTag(Tag.DEFAULT_BACKEND_TAG);
        Assertions.assertEquals(backendNum(), backends.size());
        Backend highLoadBe = backends.get(0);
        DebugPointUtil.addDebugPointWithValue("FE.HIGH_LOAD_BE_ID", highLoadBe.getId());

        alterTableSync("ALTER TABLE test_multi MODIFY PARTITION(*) SET ('replication_num' = '2')");
        partitions.forEach(p -> {
            boolean succ = false;
            for (int i = 0; i < 100; i++) {
                Map<Long, Long> beIdToTabletNum = Alter.getReplicaCountByBackend(p);
                Set<Long> afterAlter = new HashSet<>(beIdToTabletNum.values());
                List<Long> sortedValues = new ArrayList<>(beIdToTabletNum.values());
                sortedValues.sort(Collections.reverseOrder());
                // wait for scheduler
                if (sortedValues.equals(Lists.newArrayList(4L, 4L, 4L, 3L, 3L))) {
                    Assertions.assertEquals(afterAlter.size(), 2);
                    succ = true;
                    break;
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ignored) {
                    System.out.println(ignored);
                }
            }
            Assertions.assertTrue(succ);
        });

    }
}
