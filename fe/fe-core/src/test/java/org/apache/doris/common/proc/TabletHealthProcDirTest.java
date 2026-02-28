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

package org.apache.doris.common.proc;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.Tablet.TabletStatus;
import org.apache.doris.common.Config;
import org.apache.doris.utframe.TestWithFeService;

import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

public class TabletHealthProcDirTest extends TestWithFeService {
    private Database db;

    @Override
    protected int backendNum() {
        return 1;
    }

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        useDatabase("test");
        db = Env.getCurrentInternalCatalog().getDbOrMetaException("test");
    }

    @Override
    protected void runBeforeEach() throws Exception {
        for (Table table : db.getTables()) {
            dropTable(table.getName(), true);
        }
    }

    @Test
    public void testCloudModeCountsTabletAsHealthy() throws Exception {
        createTable("CREATE TABLE tbl_proc_health (k INT) DISTRIBUTED BY HASH(k) BUCKETS 1"
                + " PROPERTIES ('replication_num' = '1')");

        OlapTable table = (OlapTable) db.getTableOrMetaException("tbl_proc_health");
        Partition partition = table.getPartitions().iterator().next();
        Tablet tablet = partition.getMaterializedIndices(IndexExtState.ALL).iterator().next()
                .getTablets().iterator().next();

        partition.updateVisibleVersion(10L);
        Replica replica = tablet.getReplicas().get(0);
        replica.updateVersion(10L);
        replica.setBad(true);

        TabletStatus localStatus = tablet.getHealth(Env.getCurrentSystemInfo(), partition.getVisibleVersion(),
                table.getPartitionInfo().getReplicaAllocation(partition.getId()),
                Env.getCurrentSystemInfo().getAllBackendIds(true)).status;
        Assertions.assertEquals(TabletStatus.UNRECOVERABLE, localStatus);

        new MockUp<Config>() {
            @Mock
            public boolean isCloudMode() {
                return true;
            }
        };
        new MockUp<Partition>() {
            @Mock
            public List<Long> getVisibleVersions(List<? extends Partition> partitions) {
                return partitions.stream().map(Partition::getVisibleVersion).collect(Collectors.toList());
            }
        };

        ProcResult result = new TabletHealthProcDir(Env.getCurrentEnv()).fetchResult();
        List<String> row = findDbRow(result, db.getId());
        int tabletNumIdx = TabletHealthProcDir.TITLE_NAMES.indexOf("TabletNum");
        int healthyNumIdx = TabletHealthProcDir.TITLE_NAMES.indexOf("HealthyNum");
        int unrecoverableNumIdx = TabletHealthProcDir.TITLE_NAMES.indexOf("UnrecoverableNum");

        Assertions.assertEquals("1", row.get(tabletNumIdx));
        Assertions.assertEquals("1", row.get(healthyNumIdx));
        Assertions.assertEquals("0", row.get(unrecoverableNumIdx));
    }

    private List<String> findDbRow(ProcResult result, long dbId) {
        int dbIdIdx = TabletHealthProcDir.TITLE_NAMES.indexOf("DbId");
        return result.getRows().stream()
                .filter(row -> String.valueOf(dbId).equals(row.get(dbIdIdx)))
                .findFirst()
                .orElseThrow(() -> new AssertionError("can not find db row in tablet health proc result"));
    }
}
