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
import org.apache.doris.common.FeConstants;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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
        FeConstants.tablet_checker_interval_ms = 100;
        FeConstants.tablet_schedule_interval_ms = 100;
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

    private TableInfo prepareTableForTest(String tableName) throws Exception {
        createTable("CREATE TABLE test." + tableName + " (k INT) DISTRIBUTED BY HASH(k) "
                + " BUCKETS 1 PROPERTIES ( \"replication_num\" = \"2\" )");

        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException("default_cluster:test");
        OlapTable tbl = (OlapTable) db.getTableOrMetaException(tableName);
        Assertions.assertNotNull(tbl);
        Partition partition = tbl.getPartitions().iterator().next();
        Tablet tablet = partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL).iterator().next()
                .getTablets().iterator().next();

        long visibleVersion = 2L;
        partition.updateVisibleVersion(visibleVersion);
        partition.setNextVersion(visibleVersion + 1);
        tablet.getReplicas().forEach(replica -> replica.updateVersionInfo(visibleVersion, 1L, 1L, 1L));

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
