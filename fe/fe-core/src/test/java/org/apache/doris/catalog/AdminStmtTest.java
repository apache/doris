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

package org.apache.doris.catalog;

import org.apache.doris.analysis.AdminSetReplicaStatusStmt;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.Replica.ReplicaStatus;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Pair;
import org.apache.doris.persist.SetReplicaStatusOperationLog;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class AdminStmtTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        createTable("CREATE TABLE test.tbl1 (\n"
                + "  `id` int(11) NULL COMMENT \"\",\n"
                + "  `id2` bitmap bitmap_union NULL\n"
                + ") ENGINE=OLAP\n"
                + "AGGREGATE KEY(`id`)\n"
                + "DISTRIBUTED BY HASH(`id`) BUCKETS 3\n"
                + "PROPERTIES (\n"
                + " \"replication_num\" = \"1\"\n"
                + ");");
    }

    @Test
    public void testAdminSetReplicaStatus() throws Exception {
        Database db = Env.getCurrentInternalCatalog().getDbNullable("default_cluster:test");
        Assertions.assertNotNull(db);
        OlapTable tbl = (OlapTable) db.getTableNullable("tbl1");
        Assertions.assertNotNull(tbl);
        // tablet id, backend id
        List<Pair<Long, Long>> tabletToBackendList = Lists.newArrayList();
        for (Partition partition : tbl.getPartitions()) {
            for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
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
        Assertions.assertFalse(replica.isBad());

        // set replica to bad
        String adminStmt = "admin set replica status properties ('tablet_id' = '" + tabletId + "', 'backend_id' = '"
                + backendId + "', 'status' = 'bad');";
        AdminSetReplicaStatusStmt stmt = (AdminSetReplicaStatusStmt) parseAndAnalyzeStmt(adminStmt);
        Env.getCurrentEnv().setReplicaStatus(stmt);
        replica = Env.getCurrentInvertedIndex().getReplica(tabletId, backendId);
        Assertions.assertTrue(replica.isBad());

        // set replica to ok
        adminStmt = "admin set replica status properties ('tablet_id' = '" + tabletId + "', 'backend_id' = '"
                + backendId + "', 'status' = 'ok');";
        stmt = (AdminSetReplicaStatusStmt) parseAndAnalyzeStmt(adminStmt);
        Env.getCurrentEnv().setReplicaStatus(stmt);
        replica = Env.getCurrentInvertedIndex().getReplica(tabletId, backendId);
        Assertions.assertFalse(replica.isBad());
    }

    @Test
    public void testSetReplicaStatusOperationLog() throws IOException, AnalysisException {
        String fileName = "./SetReplicaStatusOperationLog";
        Path path = Paths.get(fileName);
        try {
            // 1. Write objects to file
            Files.createFile(path);
            DataOutputStream out = new DataOutputStream(Files.newOutputStream(path));

            SetReplicaStatusOperationLog log = new SetReplicaStatusOperationLog(10000, 100001, ReplicaStatus.BAD);
            log.write(out);
            out.flush();
            out.close();

            // 2. Read objects from file
            DataInputStream in = new DataInputStream(Files.newInputStream(path));

            SetReplicaStatusOperationLog readLog = SetReplicaStatusOperationLog.read(in);
            Assertions.assertEquals(log.getBackendId(), readLog.getBackendId());
            Assertions.assertEquals(log.getTabletId(), readLog.getTabletId());
            Assertions.assertEquals(log.getReplicaStatus(), readLog.getReplicaStatus());

            in.close();
        } finally {
            Files.deleteIfExists(path);
        }
    }

}
