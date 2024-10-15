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
import org.apache.doris.analysis.AdminSetReplicaVersionStmt;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.Replica.ReplicaStatus;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Pair;
import org.apache.doris.persist.SetPartitionVersionOperationLog;
import org.apache.doris.persist.SetReplicaStatusOperationLog;
import org.apache.doris.persist.SetReplicaVersionOperationLog;
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
    public void testAdminSetReplicaStatus() throws Exception {
        Database db = Env.getCurrentInternalCatalog().getDbNullable("test");
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
    public void testAdminSetReplicaVersion() throws Exception {
        Database db = Env.getCurrentInternalCatalog().getDbNullable("test");
        Assertions.assertNotNull(db);
        OlapTable tbl = (OlapTable) db.getTableNullable("tbl3");
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

        String adminStmt = "admin set replica version properties ('tablet_id' = '" + tabletId + "', 'backend_id' = '"
                + backendId + "', 'version' = '10', 'last_failed_version' = '100');";
        AdminSetReplicaVersionStmt stmt = (AdminSetReplicaVersionStmt) parseAndAnalyzeStmt(adminStmt);
        Env.getCurrentEnv().setReplicaVersion(stmt);
        Assertions.assertEquals(10L, replica.getVersion());
        Assertions.assertEquals(10L, replica.getLastSuccessVersion());
        Assertions.assertEquals(100L, replica.getLastFailedVersion());

        adminStmt = "admin set replica version properties ('tablet_id' = '" + tabletId + "', 'backend_id' = '"
                + backendId + "', 'version' = '50');";
        stmt = (AdminSetReplicaVersionStmt) parseAndAnalyzeStmt(adminStmt);
        Env.getCurrentEnv().setReplicaVersion(stmt);
        Assertions.assertEquals(50L, replica.getVersion());
        Assertions.assertEquals(50L, replica.getLastSuccessVersion());
        Assertions.assertEquals(100L, replica.getLastFailedVersion());

        adminStmt = "admin set replica version properties ('tablet_id' = '" + tabletId + "', 'backend_id' = '"
                + backendId + "', 'version' = '200');";
        stmt = (AdminSetReplicaVersionStmt) parseAndAnalyzeStmt(adminStmt);
        Env.getCurrentEnv().setReplicaVersion(stmt);
        Assertions.assertEquals(200L, replica.getVersion());
        Assertions.assertEquals(200L, replica.getLastSuccessVersion());
        Assertions.assertEquals(-1L, replica.getLastFailedVersion());

        adminStmt = "admin set replica version properties ('tablet_id' = '" + tabletId + "', 'backend_id' = '"
                + backendId + "', 'last_failed_version' = '300');";
        stmt = (AdminSetReplicaVersionStmt) parseAndAnalyzeStmt(adminStmt);
        Env.getCurrentEnv().setReplicaVersion(stmt);
        Assertions.assertEquals(300L, replica.getLastFailedVersion());

        adminStmt = "admin set replica version properties ('tablet_id' = '" + tabletId + "', 'backend_id' = '"
                + backendId + "', 'last_failed_version' = '-1');";
        stmt = (AdminSetReplicaVersionStmt) parseAndAnalyzeStmt(adminStmt);
        Env.getCurrentEnv().setReplicaVersion(stmt);
        Assertions.assertEquals(-1L, replica.getLastFailedVersion());
    }

    @Test
    public void testSetReplicaVersionOperationLog() throws IOException, AnalysisException {
        String fileName = "./SetReplicaVersionOperationLog";
        Path path = Paths.get(fileName);
        List<Long> versions = Lists.newArrayList(null, 10L, 1000L);
        for (int i = 0; i < versions.size(); i++) {
            for (int j = 0; j < versions.size(); j++) {
                for (int k = 0; k < versions.size(); k++) {
                    try {
                        // 1. Write objects to file
                        Files.createFile(path);
                        DataOutputStream out = new DataOutputStream(Files.newOutputStream(path));

                        Long version = versions.get(i);
                        Long lastSuccessVersion = versions.get(j);
                        Long lastFailedVersion = versions.get(k);
                        if (version != null) {
                            version = version + 1;
                        }
                        if (lastSuccessVersion != null) {
                            lastSuccessVersion = lastSuccessVersion + 2;
                        }
                        if (lastFailedVersion != null) {
                            lastFailedVersion = lastFailedVersion + 3;
                        }
                        SetReplicaVersionOperationLog log = new SetReplicaVersionOperationLog(123L, 567L, version,
                                lastSuccessVersion, lastFailedVersion, 101112L);
                        log.write(out);
                        out.flush();
                        out.close();

                        // 2. Read objects from file
                        DataInputStream in = new DataInputStream(Files.newInputStream(path));

                        SetReplicaVersionOperationLog readLog = SetReplicaVersionOperationLog.read(in);
                        Assertions.assertEquals(log, readLog);

                        in.close();
                    } finally {
                        Files.deleteIfExists(path);
                    }
                }
            }
        }
    }

    @Test
    public void testSetReplicaStatusOperationLog() throws IOException, AnalysisException {
        String fileName = "./SetReplicaStatusOperationLog";
        Path path = Paths.get(fileName);
        try {
            // 1. Write objects to file
            Files.createFile(path);
            DataOutputStream out = new DataOutputStream(Files.newOutputStream(path));

            SetReplicaStatusOperationLog log = new SetReplicaStatusOperationLog(10000, 100001, ReplicaStatus.BAD, 100L);
            log.write(out);
            out.flush();
            out.close();

            // 2. Read objects from file
            DataInputStream in = new DataInputStream(Files.newInputStream(path));

            SetReplicaStatusOperationLog readLog = SetReplicaStatusOperationLog.read(in);
            Assertions.assertEquals(log.getBackendId(), readLog.getBackendId());
            Assertions.assertEquals(log.getTabletId(), readLog.getTabletId());
            Assertions.assertEquals(log.getReplicaStatus(), readLog.getReplicaStatus());
            Assertions.assertEquals(log.getUserDropTime(), readLog.getUserDropTime());

            in.close();
        } finally {
            Files.deleteIfExists(path);
        }
    }

    @Test
    public void testAdminSetPartitionVersion() throws Exception {
        Database db = Env.getCurrentInternalCatalog().getDbNullable("test");
        Assertions.assertNotNull(db);
        OlapTable tbl = (OlapTable) db.getTableNullable("tbl2");
        Assertions.assertNotNull(tbl);
        Partition partition = tbl.getPartitions().iterator().next();
        long partitionId = partition.getId();
        long oldVersion = partition.getVisibleVersion();
        // origin version is 1
        Assertions.assertEquals(1, oldVersion);
        // set partition version to 100
        long newVersion = 100;
        String adminStmt = "admin set table test.tbl2 partition version properties ('partition_id' = '"
                + partitionId + "', " + "'visible_version' = '" + newVersion + "');";
        Assertions.assertNotNull(getSqlStmtExecutor(adminStmt));
        Assertions.assertEquals(newVersion, partition.getVisibleVersion());
        adminStmt = "admin set table test.tbl2 partition version properties ('partition_id' = '"
                + partitionId + "', " + "'visible_version' = '" + oldVersion + "');";
        Assertions.assertNotNull(getSqlStmtExecutor(adminStmt));
        Assertions.assertEquals(oldVersion, partition.getVisibleVersion());
    }

    @Test
    public void testSetPartitionVersionOperationLog() throws IOException, AnalysisException {
        String fileName = "./SetPartitionVersionOperationLog";
        Path path = Paths.get(fileName);
        try {
            // 1. Write objects to file
            Files.createFile(path);
            DataOutputStream out = new DataOutputStream(Files.newOutputStream(path));

            SetPartitionVersionOperationLog log = new SetPartitionVersionOperationLog(
                    "test", "tbl2", 10002, 100);
            log.write(out);
            out.flush();
            out.close();

            // 2. Read objects from file
            DataInputStream in = new DataInputStream(Files.newInputStream(path));

            SetPartitionVersionOperationLog readLog = SetPartitionVersionOperationLog.read(in);
            Assertions.assertEquals(log.getDatabase(), readLog.getDatabase());
            Assertions.assertEquals(log.getTable(), readLog.getTable());
            Assertions.assertEquals(log.getPartitionId(), readLog.getPartitionId());
            Assertions.assertEquals(log.getVisibleVersion(), readLog.getVisibleVersion());

            in.close();
        } finally {
            Files.deleteIfExists(path);
        }
    }

}
