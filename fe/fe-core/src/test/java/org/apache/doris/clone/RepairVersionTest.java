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

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.DebugPointUtil;
import org.apache.doris.common.util.DebugPointUtil.DebugPoint;
import org.apache.doris.master.ReportHandler;
import org.apache.doris.thrift.TTablet;
import org.apache.doris.thrift.TTabletInfo;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class RepairVersionTest extends TestWithFeService {
    private class TableInfo {
        Partition partition;
        Tablet tablet;
        Replica replica;
    }

    @Override
    protected void beforeCreatingConnectContext() throws Exception {
        Config.enable_debug_points = true;
        Config.disable_balance = true;
        Config.disable_tablet_scheduler = true;
        Config.allow_replica_on_same_host = true;
        Config.tablet_checker_interval_ms = 100;
        Config.tablet_schedule_interval_ms = 100;
    }

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
    }

    @Override
    protected int backendNum() {
        return 2;
    }

    @Test
    public void testRepairLastFailedVersionByClone() throws Exception {
        TableInfo info = prepareTableForTest("tbl_repair_last_fail_version_by_clone");
        Partition partition = info.partition;
        Replica replica = info.replica;

        replica.updateLastFailedVersion(replica.getVersion() + 1);
        Assertions.assertEquals(partition.getCommittedVersion() + 1, replica.getLastFailedVersion());

        Config.disable_tablet_scheduler = false;
        Thread.sleep(1000);
        Config.disable_tablet_scheduler = true;

        Assertions.assertEquals(partition.getVisibleVersion(), replica.getVersion());
        Assertions.assertEquals(-1L, replica.getLastFailedVersion());
    }

    @Test
    public void testRepairLastFailedVersionByReport() throws Exception {
        TableInfo info = prepareTableForTest("tbl_repair_last_fail_version_by_report");
        Partition partition = info.partition;
        Tablet tablet = info.tablet;
        Replica replica = info.replica;

        replica.updateLastFailedVersion(replica.getVersion() + 1);
        Assertions.assertEquals(partition.getCommittedVersion() + 1, replica.getLastFailedVersion());

        TTabletInfo tTabletInfo = new TTabletInfo();
        tTabletInfo.setTabletId(tablet.getId());
        tTabletInfo.setSchemaHash(replica.getSchemaHash());
        tTabletInfo.setVersion(replica.getVersion());
        tTabletInfo.setPathHash(replica.getPathHash());
        tTabletInfo.setPartitionId(partition.getId());
        tTabletInfo.setReplicaId(replica.getId());

        TTablet tTablet = new TTablet();
        tTablet.addToTabletInfos(tTabletInfo);
        Map<Long, TTablet> tablets = Maps.newHashMap();
        tablets.put(tablet.getId(), tTablet);
        Assertions.assertEquals(partition.getVisibleVersion(), replica.getVersion());

        ReportHandler.tabletReport(replica.getBackendId(), tablets, Maps.newHashMap(), 100L);

        Assertions.assertEquals(partition.getVisibleVersion(), replica.getVersion());
        Assertions.assertEquals(-1L, replica.getLastFailedVersion());
    }

    @Test
    public void testVersionRegressive() throws Exception {
        TableInfo info = prepareTableForTest("tbl_version_regressive");
        Partition partition = info.partition;
        Tablet tablet = info.tablet;
        Replica replica = info.replica;

        Assertions.assertEquals(partition.getVisibleVersion(), replica.getVersion());
        Assertions.assertEquals(-1L, replica.getLastFailedVersion());
        Assertions.assertTrue(replica.getVersion() > 1L);

        TTabletInfo tTabletInfo = new TTabletInfo();
        tTabletInfo.setTabletId(tablet.getId());
        tTabletInfo.setSchemaHash(replica.getSchemaHash());
        tTabletInfo.setVersion(1L); // be report version = 1 which less than fe version
        tTabletInfo.setPathHash(replica.getPathHash());
        tTabletInfo.setPartitionId(partition.getId());
        tTabletInfo.setReplicaId(replica.getId());

        TTablet tTablet = new TTablet();
        tTablet.addToTabletInfos(tTabletInfo);
        Map<Long, TTablet> tablets = Maps.newHashMap();
        tablets.put(tablet.getId(), tTablet);

        ReportHandler.tabletReport(replica.getBackendId(), tablets, Maps.newHashMap(), 100L);
        Assertions.assertEquals(-1L, replica.getLastFailedVersion());

        DebugPointUtil.addDebugPoint("Replica.regressive_version_immediately", new DebugPoint());
        ReportHandler.tabletReport(replica.getBackendId(), tablets, Maps.newHashMap(), 100L);
        Assertions.assertEquals(replica.getVersion() + 1, replica.getLastFailedVersion());

        Assertions.assertEquals(partition.getVisibleVersion(), replica.getVersion());
    }

    private TableInfo prepareTableForTest(String tableName) throws Exception {
        createTable("CREATE TABLE test." + tableName + " (k INT) DISTRIBUTED BY HASH(k) "
                + " BUCKETS 1 PROPERTIES ( \"replication_num\" = \"2\" )");

        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException("test");
        OlapTable tbl = (OlapTable) db.getTableOrMetaException(tableName);
        Assertions.assertNotNull(tbl);
        Partition partition = tbl.getPartitions().iterator().next();
        Tablet tablet = partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL).iterator().next()
                .getTablets().iterator().next();

        long visibleVersion = 2L;
        partition.updateVisibleVersion(visibleVersion);
        partition.setNextVersion(visibleVersion + 1);
        tablet.getReplicas().forEach(replica -> replica.updateVersion(visibleVersion));

        Replica replica = tablet.getReplicas().iterator().next();
        Assertions.assertEquals(visibleVersion, replica.getVersion());
        Assertions.assertEquals(-1L, replica.getLastFailedVersion());

        TableInfo info = new TableInfo();
        info.partition = partition;
        info.tablet = tablet;
        info.replica = replica;

        return info;
    }
}
