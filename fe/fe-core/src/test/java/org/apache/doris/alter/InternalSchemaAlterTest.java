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

package org.apache.doris.alter;

import org.apache.doris.analysis.ColumnDef;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.InternalSchema;
import org.apache.doris.catalog.InternalSchemaInitializer;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.OlapTable.OlapTableState;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.plugin.audit.AuditLoader;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.StatisticConstants;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class InternalSchemaAlterTest extends TestWithFeService {

    @Override
    protected int backendNum() {
        return 3;
    }

    @Override
    protected void beforeCreatingConnectContext() throws Exception {
        // the schema change handler takes this interval when the Env is built; a job then finishes in seconds
        FeConstants.default_scheduler_interval_millisecond = 100;
    }

    @Override
    protected void runBeforeAll() throws Exception {
        Config.allow_replica_on_same_host = true;
        FeConstants.runningUnitTest = true;
        InternalSchemaInitializer.createDb();
        InternalSchemaInitializer.createTbl();
    }

    @Test
    public void testModifyTblReplicaCount() throws AnalysisException {
        Database db = Env.getCurrentEnv().getCatalogMgr()
                .getInternalCatalog().getDbNullable(FeConstants.INTERNAL_DB_NAME);

        InternalSchemaInitializer.modifyTblReplicaCount(db, StatisticConstants.TABLE_STATISTIC_TBL_NAME);
        InternalSchemaInitializer.modifyTblReplicaCount(db, AuditLoader.AUDIT_LOG_TABLE);

        checkReplicationNum(db, StatisticConstants.TABLE_STATISTIC_TBL_NAME);
        checkReplicationNum(db, AuditLoader.AUDIT_LOG_TABLE);
    }

    private void checkReplicationNum(Database db, String tblName) throws AnalysisException {
        OlapTable olapTable = db.getOlapTableOrAnalysisException(tblName);
        PartitionInfo partitionInfo = olapTable.getPartitionInfo();
        Assertions.assertEquals((short) 3, olapTable.getTableProperty().getReplicaAllocation().getTotalReplicaNum(),
                tblName);
        Assertions.assertFalse(olapTable.getTableProperty().getProperties()
                .containsKey("default." + PropertyAnalyzer.PROPERTIES_REPLICATION_NUM), tblName);
        Assertions.assertTrue(olapTable.getTableProperty().getProperties()
                .containsKey("default." + PropertyAnalyzer.PROPERTIES_REPLICATION_ALLOCATION), tblName);
        for (Partition partition : olapTable.getPartitions()) {
            Assertions.assertEquals((short) 3,
                    partitionInfo.getReplicaAllocation(partition.getId()).getTotalReplicaNum());
        }
    }

    @Test
    public void testCheckAuditLogTable() throws AnalysisException {
        Database db = Env.getCurrentEnv().getCatalogMgr()
                .getInternalCatalog().getDbNullable(FeConstants.INTERNAL_DB_NAME);
        Assertions.assertNotNull(db);
        OlapTable table = db.getOlapTableOrAnalysisException(AuditLoader.AUDIT_LOG_TABLE);
        Assertions.assertNotNull(table);
        for (ColumnDef def : InternalSchema.AUDIT_SCHEMA) {
            Assertions.assertNotNull(table.getColumn(def.getName()));
        }
    }

    @Test
    public void testCheckPartitionStatisticsTable() throws AnalysisException {
        Database db = Env.getCurrentEnv().getCatalogMgr()
                .getInternalCatalog().getDbNullable(FeConstants.INTERNAL_DB_NAME);
        Assertions.assertNotNull(db);
        OlapTable table = db.getOlapTableOrAnalysisException(StatisticConstants.PARTITION_STATISTIC_TBL_NAME);
        Assertions.assertEquals(PrimitiveType.HLL, table.getColumn("ndv").getType().getPrimitiveType());
    }

    // A table an earlier version created lacks the columns added since, and has the others in order. They are
    // added in place, by a light schema change: no job to wait for, and the table keeps its index and tablets.
    @Test
    public void testAuditTableMissingColumnsAddedInPlace() throws Exception {
        OlapTable table = auditTable();
        try {
            alterTableSync("ALTER TABLE __internal_schema.audit_log"
                    + " DROP COLUMN queue_time_ms, DROP COLUMN cpu_time_ms, DROP COLUMN compute_group");
            Assertions.assertNull(table.getColumn("queue_time_ms"));
            long baseIndexId = table.getBaseIndexId();
            Set<Long> tabletIds = baseTabletIds(table);

            Assertions.assertTrue(alterAuditSchemaAsInitializer(table));

            Assertions.assertEquals(OlapTableState.NORMAL, table.getState());
            Assertions.assertEquals(auditSchemaColumnNames(), baseColumnNames(table));
            Assertions.assertEquals(baseIndexId, table.getBaseIndexId());
            Assertions.assertEquals(tabletIds, baseTabletIds(table));
            for (String columnName : Lists.newArrayList("queue_time_ms", "cpu_time_ms", "compute_group")) {
                ColumnDef columnDef = InternalSchema.AUDIT_SCHEMA.stream()
                        .filter(def -> def.getName().equals(columnName)).findFirst().get();
                Column column = table.getColumn(columnName);
                Assertions.assertEquals(columnDef.getType(), column.getType(), columnName);
                Assertions.assertEquals(columnDef.isAllowNull(), column.isAllowNull(), columnName);
            }
        } finally {
            waitAlterJobsDone(table);
        }
    }

    // A table whose columns are out of order needs the add + reorder job, which rewrites it. The initializer
    // creates the job in a session of its own: in cloud mode the job picks its backends from the compute group
    // of the session, and the initializer's daemon thread has no session.
    @Test
    public void testAuditTableOutOfOrderReorderedInOwnSession() throws Exception {
        OlapTable table = auditTable();
        try {
            String catalogType = table.getColumn("catalog").getType().toSql();
            alterTableSync("ALTER TABLE __internal_schema.audit_log DROP COLUMN catalog, DROP COLUMN queue_time_ms");
            alterTableSync("ALTER TABLE __internal_schema.audit_log ADD COLUMN catalog " + catalogType + " NULL");
            List<String> columnNames = baseColumnNames(table);
            Assertions.assertEquals("catalog", columnNames.get(columnNames.size() - 1));
            Set<Long> jobIds = schemaChangeJobs(table).stream().map(AlterJobV2::getJobId)
                    .collect(Collectors.toSet());

            Assertions.assertTrue(alterAuditSchemaAsInitializer(table));

            List<AlterJobV2> newJobs = schemaChangeJobs(table).stream()
                    .filter(job -> !jobIds.contains(job.getJobId())).collect(Collectors.toList());
            Assertions.assertEquals(1, newJobs.size());
            Assertions.assertEquals(UserIdentity.ADMIN, newJobs.get(0).userIdentity);
            waitAlterJobsDone(table);
            Assertions.assertEquals(AlterJobV2.JobState.FINISHED, newJobs.get(0).getJobState());
            Assertions.assertEquals(auditSchemaColumnNames(), baseColumnNames(table));
        } finally {
            waitAlterJobsDone(table);
        }
    }

    private static OlapTable auditTable() throws AnalysisException {
        return Env.getCurrentEnv().getCatalogMgr().getInternalCatalog().getDbNullable(FeConstants.INTERNAL_DB_NAME)
                .getOlapTableOrAnalysisException(AuditLoader.AUDIT_LOG_TABLE);
    }

    // Runs alterAuditSchemaIfNeeded the way InternalSchemaInitializer does: on a thread of its own, which
    // has no session.
    private static boolean alterAuditSchemaAsInitializer(OlapTable table) throws Exception {
        FutureTask<Boolean> task = new FutureTask<>(() -> {
            Assertions.assertNull(ConnectContext.get());
            boolean altered = InternalSchemaInitializer.alterAuditSchemaIfNeeded(table);
            Assertions.assertNull(ConnectContext.get(), "the thread keeps the session built for the alter");
            return altered;
        });
        new Thread(task, "InternalSchemaInitializer").start();
        return task.get(60, TimeUnit.SECONDS);
    }

    private static List<String> auditSchemaColumnNames() {
        return InternalSchema.AUDIT_SCHEMA.stream().map(ColumnDef::getName).collect(Collectors.toList());
    }

    private static List<String> baseColumnNames(OlapTable table) {
        return table.getBaseSchema().stream().map(Column::getName).collect(Collectors.toList());
    }

    private static Set<Long> baseTabletIds(OlapTable table) {
        return table.getPartitions().stream().flatMap(partition -> partition.getBaseIndex().getTablets().stream())
                .map(Tablet::getId).collect(Collectors.toSet());
    }

    private static List<AlterJobV2> schemaChangeJobs(OlapTable table) {
        return Env.getCurrentEnv().getSchemaChangeHandler().getAlterJobsV2().values().stream()
                .filter(job -> job.getTableId() == table.getId()).collect(Collectors.toList());
    }

    private static void waitAlterJobsDone(OlapTable table) throws InterruptedException {
        for (AlterJobV2 job : schemaChangeJobs(table)) {
            for (int i = 0; i < 600 && !job.getJobState().isFinalState(); i++) {
                Thread.sleep(100);
            }
            Assertions.assertTrue(job.getJobState().isFinalState(), job.toString());
        }
        for (int i = 0; i < 600 && table.getState() != OlapTableState.NORMAL; i++) {
            Thread.sleep(100);
        }
        Assertions.assertEquals(OlapTableState.NORMAL, table.getState());
    }
}
