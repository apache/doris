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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

public class AdminSetReplicaVersionCommandTest extends TestWithFeService {
    @BeforeAll
    protected void runBeforeAll() throws Exception {
        createDatabase("test");

        createTable("CREATE TABLE test.tbl1 (\n"
                + "  `id` int(11) NULL COMMENT \"\",\n"
                + "  `id2` bitmap bitmap_union\n"
                + ") ENGINE=OLAP\n"
                + "AGGREGATE KEY(`id`)\n"
                + "DISTRIBUTED BY HASH(`id`) BUCKETS 3\n"
                + "PROPERTIES (\n"
                + " \"replication_num\" = \"1\"\n"
                + ");");

        createTable("CREATE TABLE test.tbl2 (\n"
                + "  `id` int(11) NULL COMMENT \"\",\n"
                + "  `name` varchar(20) NULL\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`id`, `name`)\n"
                + "DISTRIBUTED BY HASH(`id`) BUCKETS 3\n"
                + "PROPERTIES (\n"
                + " \"replication_num\" = \"1\"\n"
                + ");");

        // for test set replica version
        createTable("CREATE TABLE test.tbl3 (\n"
                + "  `id` int(11) NULL COMMENT \"\",\n"
                + "  `name` varchar(20) NULL\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`id`, `name`)\n"
                + "DISTRIBUTED BY HASH(`id`) BUCKETS 3\n"
                + "PROPERTIES (\n"
                + " \"replication_num\" = \"1\"\n"
                + ");");
    }

    @Test
    public void testAdminSetReplicaVersion() throws Exception {
        Database db = Env.getCurrentInternalCatalog().getDbNullable("test");
        Assertions.assertNotNull(db);
        OlapTable tbl = (OlapTable) db.getTableNullable("tbl3");
        Assertions.assertNotNull(tbl);
        // tablet id, backend id
        List<Pair<Long, Long>> tabletToBackendList = Lists.newArrayList();
        for (Partition partition : tbl.getPartitions()) {
            for (MaterializedIndex index : partition.getMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE)) {
                for (Tablet tablet : index.getTablets()) {
                    for (Replica replica : tablet.getReplicas()) {
                        tabletToBackendList.add(Pair.of(tablet.getId(), replica.getBackendId()));
                    }
                }
            }
        }
        Assertions.assertEquals(3, tabletToBackendList.size());
        long tabletId = tabletToBackendList.get(0).first;
        long backendId = tabletToBackendList.get(0).second;
        Replica replica = Env.getCurrentInvertedIndex().getReplica(tabletId, backendId);

        String sql = "admin set replica version properties ('tablet_id' = '" + tabletId + "', 'backend_id' = '"
                + backendId + "', 'version' = '10', 'last_failed_version' = '100');";
        LogicalPlan plan = new NereidsParser().parseSingle(sql);
        Assertions.assertTrue(plan instanceof AdminSetReplicaVersionCommand);
        Env.getCurrentEnv().setReplicaVersion((AdminSetReplicaVersionCommand) plan);
        Assertions.assertEquals(10L, replica.getVersion());
        Assertions.assertEquals(10L, replica.getLastSuccessVersion());
        Assertions.assertEquals(100L, replica.getLastFailedVersion());

        sql = "admin set replica version properties ('tablet_id' = '" + tabletId + "', 'backend_id' = '"
            + backendId + "', 'version' = '50');";
        plan = new NereidsParser().parseSingle(sql);
        Assertions.assertTrue(plan instanceof AdminSetReplicaVersionCommand);

        Env.getCurrentEnv().setReplicaVersion((AdminSetReplicaVersionCommand) plan);
        Assertions.assertEquals(50L, replica.getVersion());
        Assertions.assertEquals(50L, replica.getLastSuccessVersion());
        Assertions.assertEquals(100L, replica.getLastFailedVersion());

        sql = "admin set replica version properties ('tablet_id' = '" + tabletId + "', 'backend_id' = '"
            + backendId + "', 'version' = '200');";
        plan = new NereidsParser().parseSingle(sql);
        Assertions.assertTrue(plan instanceof AdminSetReplicaVersionCommand);

        Env.getCurrentEnv().setReplicaVersion((AdminSetReplicaVersionCommand) plan);
        Assertions.assertEquals(200L, replica.getVersion());
        Assertions.assertEquals(200L, replica.getLastSuccessVersion());
        Assertions.assertEquals(-1L, replica.getLastFailedVersion());

        sql = "admin set replica version properties ('tablet_id' = '" + tabletId + "', 'backend_id' = '"
            + backendId + "', 'last_failed_version' = '300');";
        plan = new NereidsParser().parseSingle(sql);
        Assertions.assertTrue(plan instanceof AdminSetReplicaVersionCommand);

        Env.getCurrentEnv().setReplicaVersion((AdminSetReplicaVersionCommand) plan);
        Assertions.assertEquals(300L, replica.getLastFailedVersion());

        sql = "admin set replica version properties ('tablet_id' = '" + tabletId + "', 'backend_id' = '"
            + backendId + "', 'last_failed_version' = '-1');";
        plan = new NereidsParser().parseSingle(sql);
        Assertions.assertTrue(plan instanceof AdminSetReplicaVersionCommand);

        Env.getCurrentEnv().setReplicaVersion((AdminSetReplicaVersionCommand) plan);
        Assertions.assertEquals(-1L, replica.getLastFailedVersion());
    }
}
