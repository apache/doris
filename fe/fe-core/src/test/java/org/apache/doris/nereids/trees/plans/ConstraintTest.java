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

package org.apache.doris.nereids.trees.plans;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.OlapTable.OlapTableState;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.constraint.Constraint;
import org.apache.doris.catalog.constraint.ConstraintManager;
import org.apache.doris.catalog.constraint.DistributionMappingConstraint;
import org.apache.doris.catalog.constraint.ForeignKeyConstraint;
import org.apache.doris.catalog.constraint.PrimaryKeyConstraint;
import org.apache.doris.catalog.constraint.UniqueConstraint;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.util.Util;
import org.apache.doris.mtmv.BaseTableInfo;
import org.apache.doris.mtmv.MTMVUtil;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.SqlCacheContext;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.commands.AddConstraintCommand;
import org.apache.doris.nereids.trees.plans.commands.DropConstraintCommand;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanPatternMatchSupported;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.GlobalVariable;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

class ConstraintTest extends TestWithFeService implements PlanPatternMatchSupported {

    private ConstraintManager getConstraintMgr() {
        return Env.getCurrentEnv().getConstraintManager();
    }

    private TableNameInfo tableNameInfoOf(TableIf table) {
        String tblName = table.getName();
        if (GlobalVariable.isStoredTableNamesLowerCase()) {
            tblName = tblName.toLowerCase();
        }
        return new TableNameInfo(
                table.getDatabase().getCatalog().getName(), table.getDatabase().getFullName(), tblName);
    }

    @Override
    public void runBeforeAll() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("test");
        createTable("create table t1 (\n"
                + "    k1 int,\n"
                + "    k2 int\n"
                + ")\n"
                + "unique key(k1, k2)\n"
                + "distributed by hash(k1) buckets 4\n"
                + "properties(\n"
                + "    \"replication_num\"=\"1\"\n"
                + ")");
        createTable("create table t2 (\n"
                + "    k1 int,\n"
                + "    k2 int\n"
                + ")\n"
                + "unique key(k1, k2)\n"
                + "distributed by hash(k1) buckets 4\n"
                + "properties(\n"
                + "    \"replication_num\"=\"1\"\n"
                + ")");
        createTable("create table t3 (\n"
                + "    k1 int,\n"
                + "    k2 int\n"
                + ")\n"
                + "unique key(k1, k2)\n"
                + "distributed by hash(k1) buckets 4\n"
                + "properties(\n"
                + "    \"replication_num\"=\"1\"\n"
                + ")");
        createTable("create table t4 (\n"
                + "    k1 int,\n"
                + "    k2 int\n"
                + ")\n"
                + "duplicate key(k1)\n"
                + "distributed by hash(k1) buckets 4\n"
                + "properties(\n"
                + "    \"replication_num\"=\"1\",\n"
                + "    \"light_schema_change\"=\"true\"\n"
                + ")");
    }

    @Test
    void primaryKeyConstraintTest() throws Exception {
        AddConstraintCommand addCommand = (AddConstraintCommand) new NereidsParser().parseSingle(
                "alter table t1 add constraint pk primary key (k1)");
        addCommand.run(connectContext, null);
        PlanChecker.from(connectContext).parse("select * from t1").analyze().matches(logicalOlapScan().when(o -> {
            TableNameInfo tni = tableNameInfoOf(o.getTable());
            Constraint c = getConstraintMgr().getConstraint(tni, "pk");
            if (c instanceof PrimaryKeyConstraint) {
                Set<String> columns = ((PrimaryKeyConstraint) c).getPrimaryKeyNames();
                return columns.size() == 1 && columns.iterator().next().equals("k1");
            }
            return false;
        }));

        DropConstraintCommand dropCommand = (DropConstraintCommand) new NereidsParser().parseSingle(
                "alter table t1 drop constraint pk");
        dropCommand.run(connectContext, null);
        PlanChecker.from(connectContext).parse("select * from t1").analyze().matches(
                logicalOlapScan().when(o -> getConstraintMgr()
                        .getConstraints(tableNameInfoOf(o.getTable())).isEmpty()));
    }

    @Test
    void uniqueConstraintTest() throws Exception {
        AddConstraintCommand command = (AddConstraintCommand) new NereidsParser().parseSingle(
                "alter table t1 add constraint un unique (k1)");
        command.run(connectContext, null);
        PlanChecker.from(connectContext).parse("select * from t1").analyze().matches(logicalOlapScan().when(o -> {
            TableNameInfo tni = tableNameInfoOf(o.getTable());
            Constraint c = getConstraintMgr().getConstraint(tni, "un");
            if (c instanceof UniqueConstraint) {
                Set<String> columns = ((UniqueConstraint) c).getUniqueColumnNames();
                return columns.size() == 1 && columns.iterator().next().equals("k1");
            }
            return false;
        }));

        DropConstraintCommand dropCommand = (DropConstraintCommand) new NereidsParser().parseSingle(
                "alter table t1 drop constraint un");
        dropCommand.run(connectContext, null);
        PlanChecker.from(connectContext).parse("select * from t1").analyze().matches(
                logicalOlapScan().when(o -> getConstraintMgr()
                        .getConstraints(tableNameInfoOf(o.getTable())).isEmpty()));
    }

    @Test
    void distributionMappingConstraintTest() throws Exception {
        AddConstraintCommand command = (AddConstraintCommand) new NereidsParser().parseSingle(
                "alter table t1 add constraint mapping_constraint "
                        + "colocate mapping tenant_by_user (k2) determines distribution key (k1) not enforced");
        command.run(connectContext, null);
        PlanChecker.from(connectContext).parse("select * from t1").analyze().matches(logicalOlapScan().when(o -> {
            TableNameInfo tableNameInfo = tableNameInfoOf(o.getTable());
            Constraint constraint = getConstraintMgr().getConstraint(tableNameInfo, "mapping_constraint");
            if (!(constraint instanceof DistributionMappingConstraint)) {
                return false;
            }
            DistributionMappingConstraint mapping = (DistributionMappingConstraint) constraint;
            return mapping.getMappingId().equals("tenant_by_user")
                    && mapping.getDeterminantColumnNames().equals(java.util.List.of("k2"))
                    && mapping.getDistributionColumnNames().equals(java.util.List.of("k1"))
                    && getConstraintMgr().getDistributionMappingConstraints(o.getTable()).size() == 1;
        }));
        TableIf table = Env.getCurrentInternalCatalog()
                .getDbOrDdlException("test").getTableOrDdlException("t1");
        TableNameInfo tableNameInfo = tableNameInfoOf(table);
        getConstraintMgr().dropTableConstraints(tableNameInfo);
        Assertions.assertNull(getConstraintMgr().getConstraint(tableNameInfo, "mapping_constraint"));
        Assertions.assertEquals(1, getConstraintMgr().getDistributionMappingConstraints(table).size());
        getConstraintMgr().restoreTableConstraints(tableNameInfo, table);
        Assertions.assertNotNull(getConstraintMgr().getConstraint(tableNameInfo, "mapping_constraint"));

        DropConstraintCommand dropCommand = (DropConstraintCommand) new NereidsParser().parseSingle(
                "alter table t1 drop constraint mapping_constraint");
        dropCommand.run(connectContext, null);
        PlanChecker.from(connectContext).parse("select * from t1").analyze().matches(
                logicalOlapScan().when(o ->
                        getConstraintMgr().getConstraints(tableNameInfoOf(o.getTable())).isEmpty()
                                && getConstraintMgr().getDistributionMappingConstraints(o.getTable()).isEmpty()));
    }

    @Test
    void distributionMappingConstraintFencesColumnAndSchemaChanges() throws Exception {
        AddConstraintCommand command = (AddConstraintCommand) new NereidsParser().parseSingle(
                "alter table t4 add constraint mapping_fence "
                        + "colocate mapping mapping_fence (k2) determines distribution key (k1) not enforced");
        command.run(connectContext, null);
        OlapTable table = (OlapTable) Env.getCurrentInternalCatalog()
                .getDbOrDdlException("test").getTableOrDdlException("t4");

        Exception drop = Assertions.assertThrows(Exception.class,
                () -> executeNereidsSql("alter table t4 drop column K2"));
        Assertions.assertTrue(Util.getRootCauseMessage(drop).contains("mapping_fence"));
        Assertions.assertNotNull(table.getColumn("k2"));

        Exception rename = Assertions.assertThrows(Exception.class,
                () -> executeNereidsSql("alter table t4 rename column K2 K3"));
        Assertions.assertTrue(Util.getRootCauseMessage(rename).contains("mapping_fence"));
        Assertions.assertNotNull(table.getColumn("k2"));

        ((DropConstraintCommand) new NereidsParser().parseSingle(
                "alter table t4 drop constraint mapping_fence")).run(connectContext, null);
        table.setState(OlapTableState.SCHEMA_CHANGE);
        try {
            Exception add = Assertions.assertThrows(Exception.class, () ->
                    ((AddConstraintCommand) new NereidsParser().parseSingle(
                            "alter table t4 add constraint mapping_during_schema_change "
                                    + "colocate mapping mapping_fence (k2) "
                                    + "determines distribution key (k1) not enforced"))
                            .run(connectContext, null));
            Assertions.assertTrue(add.getMessage().contains("SCHEMA_CHANGE"));
        } finally {
            table.setState(OlapTableState.NORMAL);
        }
    }

    @Test
    void distributionMappingDropReadsConstraintUnderTableWriteLock() throws Exception {
        addConstraint("alter table t1 add constraint mapping_drop_lock "
                + "colocate mapping mapping_drop_lock (k2) "
                + "determines distribution key (k1) not enforced");
        OlapTable table = (OlapTable) Env.getCurrentInternalCatalog()
                .getDbOrDdlException("test").getTableOrDdlException("t1");

        try (MockedStatic<MTMVUtil> mtmvUtil =
                Mockito.mockStatic(MTMVUtil.class, Mockito.CALLS_REAL_METHODS)) {
            mtmvUtil.when(() -> MTMVUtil.getDependentMtmvsByConstraint(
                            Mockito.any(), Mockito.any()))
                    .thenAnswer(invocation -> {
                        Assertions.assertTrue(table.isWriteLockHeldByCurrentThread());
                        return java.util.List.of();
                    });

            dropConstraint("alter table t1 drop constraint mapping_drop_lock");
        }
        Assertions.assertNull(getConstraintMgr().getConstraint(
                tableNameInfoOf(table), "mapping_drop_lock"));
    }

    @Test
    void invalidDistributionMappingConstraintTest() {
        Exception duplicateDeterminant = Assertions.assertThrows(Exception.class, () ->
                ((AddConstraintCommand) new NereidsParser().parseSingle(
                        "alter table t1 add constraint duplicate_determinant "
                                + "colocate mapping mapping_1 (k2, K2) "
                                + "determines distribution key (k1) not enforced"))
                        .run(connectContext, null));
        Assertions.assertTrue(duplicateDeterminant.getMessage().contains(
                "Determinant columns in distribution mapping constraint must be unique"));

        Exception invalidDistributionColumn = Assertions.assertThrows(Exception.class, () ->
                ((AddConstraintCommand) new NereidsParser().parseSingle(
                        "alter table t1 add constraint invalid_distribution_column "
                                + "colocate mapping mapping_1 (k2) "
                                + "determines distribution key (k2) not enforced"))
                        .run(connectContext, null));
        Assertions.assertTrue(invalidDistributionColumn.getMessage().contains(
                "must be an ordered subset of table distribution columns"));
    }

    @Test
    void foreignKeyConstraintTest() throws Exception {
        AddConstraintCommand command = (AddConstraintCommand) new NereidsParser().parseSingle(
                "alter table t1 add constraint fk foreign key (k1) references t2 (k1)");
        try {
            command.run(connectContext, null);
        } catch (Exception e) {
            Assertions.assertTrue(e.getMessage().contains(
                    "Foreign key constraint requires a primary key constraint [k1] in"));
        }
        ((AddConstraintCommand) new NereidsParser().parseSingle(
                "alter table t2 add constraint pk primary key (k1, k2)")).run(connectContext, null);
        ((AddConstraintCommand) new NereidsParser().parseSingle(
                "alter table t1 add constraint fk foreign key (k1, k2) references t2(k1, k2)")).run(connectContext,
                null);

        PlanChecker.from(connectContext).parse("select * from t1").analyze().matches(logicalOlapScan().when(o -> {
            TableNameInfo tni = tableNameInfoOf(o.getTable());
            Constraint c = getConstraintMgr().getConstraint(tni, "fk");
            if (c instanceof ForeignKeyConstraint) {
                ForeignKeyConstraint f = (ForeignKeyConstraint) c;
                Column ref1 = f.getReferencedColumn(((SlotReference) o.getOutput().get(0))
                        .getOriginalColumn().get().getName());
                Column ref2 = f.getReferencedColumn(((SlotReference) o.getOutput().get(1))
                        .getOriginalColumn().get().getName());
                return ref1.getName().equals("k1") && ref2.getName().equals("k2");
            }
            return false;
        }));

        PlanChecker.from(connectContext).parse("select * from t2").analyze().matches(logicalOlapScan().when(o -> {
            TableNameInfo tni = tableNameInfoOf(o.getTable());
            Constraint c = getConstraintMgr().getConstraint(tni, "pk");
            if (c instanceof PrimaryKeyConstraint) {
                Set<String> columnNames = ((PrimaryKeyConstraint) c).getPrimaryKeyNames();
                java.util.List<TableNameInfo> foreignTableInfos
                        = ((PrimaryKeyConstraint) c).getForeignTableInfos();
                return columnNames.size() == 2
                        && columnNames.equals(Sets.newHashSet("k1", "k2"))
                        && foreignTableInfos.size() == 1;
            }
            return false;
        }));

        // drop fk
        DropConstraintCommand dropCommand = (DropConstraintCommand) new NereidsParser().parseSingle(
                "alter table t1 drop constraint fk");
        dropCommand.run(connectContext, null);
        PlanChecker.from(connectContext).parse("select * from t1").analyze().matches(
                logicalOlapScan().when(o -> getConstraintMgr()
                        .getConstraints(tableNameInfoOf(o.getTable())).isEmpty()));
        // drop pk and fk referenced it also should be dropped
        ((AddConstraintCommand) new NereidsParser().parseSingle(
                "alter table t1 add constraint fk foreign key (k1, k2) references t2(k1, k2)")).run(connectContext,
                null);
        ((DropConstraintCommand) new NereidsParser().parseSingle("alter table t2 drop constraint pk"))
                .run(connectContext, null);

        PlanChecker.from(connectContext).parse("select * from t1").analyze().matches(
                logicalOlapScan().when(o -> getConstraintMgr()
                        .getConstraints(tableNameInfoOf(o.getTable())).isEmpty()));
        PlanChecker.from(connectContext).parse("select * from t2").analyze().matches(
                logicalOlapScan().when(o -> getConstraintMgr()
                        .getConstraints(tableNameInfoOf(o.getTable())).isEmpty()));
    }

    @Test
    void foreignKeyAddHoldsBothTableWriteLocks() throws Exception {
        addConstraint("alter table t2 add constraint pk_for_lock primary key (k1, k2)");
        Database database = Env.getCurrentInternalCatalog().getDbOrDdlException("test");
        TableIf foreignKeyTable = database.getTableOrDdlException("t1");
        TableIf referencedTable = database.getTableOrDdlException("t2");

        try {
            try (MockedStatic<MTMVUtil> mtmvUtil =
                    Mockito.mockStatic(MTMVUtil.class, Mockito.CALLS_REAL_METHODS)) {
                mtmvUtil.when(() -> MTMVUtil.getDependentMtmvsByConstraint(
                                Mockito.any(), Mockito.any()))
                        .thenAnswer(invocation -> {
                            Assertions.assertTrue(
                                    foreignKeyTable.isWriteLockHeldByCurrentThread());
                            Assertions.assertTrue(
                                    referencedTable.isWriteLockHeldByCurrentThread());
                            return java.util.List.of();
                        });
                mtmvUtil.when(() -> MTMVUtil.invalidateRewriteCachesBestEffort(
                                Mockito.anyList(), Mockito.anyString()))
                        .thenAnswer(invocation -> {
                            Assertions.assertFalse(
                                    foreignKeyTable.isWriteLockHeldByCurrentThread());
                            Assertions.assertFalse(
                                    referencedTable.isWriteLockHeldByCurrentThread());
                            Assertions.assertTrue(database.tryWriteLock(1, TimeUnit.SECONDS));
                            database.writeUnlock();
                            return null;
                        });

                addConstraint("alter table t1 add constraint fk_for_lock "
                        + "foreign key (k1, k2) references t2(k1, k2)");
            }
        } finally {
            TableNameInfo foreignKeyTableInfo = tableNameInfoOf(foreignKeyTable);
            if (getConstraintMgr().getConstraint(
                    foreignKeyTableInfo, "fk_for_lock") != null) {
                dropConstraint("alter table t1 drop constraint fk_for_lock");
            }
            TableNameInfo referencedTableInfo = tableNameInfoOf(referencedTable);
            if (getConstraintMgr().getConstraint(
                    referencedTableInfo, "pk_for_lock") != null) {
                dropConstraint("alter table t2 drop constraint pk_for_lock");
            }
        }
    }

    @Test
    void primaryKeyDropUsesLockedCascadeSnapshot() throws Exception {
        addConstraint("alter table t2 add constraint pk_drop_snapshot primary key (k1, k2)");
        addConstraint("alter table t1 add constraint fk_drop_snapshot "
                + "foreign key (k1, k2) references t2(k1, k2)");
        Database database = Env.getCurrentInternalCatalog().getDbOrDdlException("test");
        TableIf primaryKeyTable = database.getTableOrDdlException("t2");
        AccessControllerManager originalAccessManager = Env.getCurrentEnv().getAccessManager();
        AccessControllerManager accessManager = Mockito.spy(originalAccessManager);
        setEnvAccessManager(accessManager);

        try (MockedStatic<MTMVUtil> mtmvUtil =
                Mockito.mockStatic(MTMVUtil.class, Mockito.CALLS_REAL_METHODS)) {
            Mockito.doAnswer(invocation -> {
                String tableName = invocation.getArgument(3);
                if ("t1".equals(tableName)) {
                    Assertions.assertTrue(primaryKeyTable.isWriteLockHeldByCurrentThread());
                }
                return true;
            }).when(accessManager).checkTblPriv(
                    Mockito.any(ConnectContext.class),
                    Mockito.anyString(),
                    Mockito.anyString(),
                    Mockito.anyString(),
                    Mockito.any(PrivPredicate.class));
            mtmvUtil.when(() -> MTMVUtil.getDependentMtmvsByBaseTables(Mockito.anyList()))
                    .thenAnswer(invocation -> {
                        Assertions.assertTrue(primaryKeyTable.isWriteLockHeldByCurrentThread());
                        List<BaseTableInfo> baseTables = invocation.getArgument(0);
                        Assertions.assertEquals(
                                Sets.newHashSet("t1", "t2"),
                                baseTables.stream()
                                        .map(BaseTableInfo::getTableName)
                                        .collect(java.util.stream.Collectors.toSet()));
                        return java.util.List.of();
                    });
            mtmvUtil.when(() -> MTMVUtil.invalidateRewriteCachesBestEffort(
                            Mockito.anyList(), Mockito.anyString()))
                    .thenAnswer(invocation -> {
                        Assertions.assertFalse(primaryKeyTable.isWriteLockHeldByCurrentThread());
                        Assertions.assertTrue(database.tryWriteLock(1, TimeUnit.SECONDS));
                        database.writeUnlock();
                        return null;
                    });

            dropConstraint("alter table t2 drop constraint pk_drop_snapshot");
        } finally {
            setEnvAccessManager(originalAccessManager);
            TableNameInfo foreignKeyTableInfo =
                    tableNameInfoOf(database.getTableOrDdlException("t1"));
            if (getConstraintMgr().getConstraint(
                    foreignKeyTableInfo, "fk_drop_snapshot") != null) {
                dropConstraint("alter table t1 drop constraint fk_drop_snapshot");
            }
            TableNameInfo primaryKeyTableInfo = tableNameInfoOf(primaryKeyTable);
            if (getConstraintMgr().getConstraint(
                    primaryKeyTableInfo, "pk_drop_snapshot") != null) {
                dropConstraint("alter table t2 drop constraint pk_drop_snapshot");
            }
        }
        Assertions.assertNull(getConstraintMgr().getConstraint(
                tableNameInfoOf(primaryKeyTable), "pk_drop_snapshot"));
    }

    @Test
    void constraintAddWaitsForAtomicReplacementDatabaseLock() throws Exception {
        Database database = Env.getCurrentInternalCatalog()
                .getDbOrDdlException("test");
        CountDownLatch addStarted = new CountDownLatch(1);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        database.writeLock();
        Future<?> addFuture = executor.submit(() -> {
            connectContext.setThreadLocalInfo();
            try {
                addStarted.countDown();
                addConstraint("alter table t1 add constraint uk_after_restore unique (k2)");
                return null;
            } finally {
                ConnectContext.remove();
            }
        });
        try {
            Assertions.assertTrue(addStarted.await(5, TimeUnit.SECONDS));
            Assertions.assertThrows(
                    TimeoutException.class,
                    () -> addFuture.get(100, TimeUnit.MILLISECONDS));
        } finally {
            database.writeUnlock();
        }
        try {
            addFuture.get(5, TimeUnit.SECONDS);
            Assertions.assertNotNull(getConstraintMgr().getConstraint(
                    tableNameInfoOf(database.getTableOrDdlException("t1")),
                    "uk_after_restore"));
        } finally {
            executor.shutdownNow();
            Assertions.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
            if (getConstraintMgr().getConstraint(
                    tableNameInfoOf(database.getTableOrDdlException("t1")),
                    "uk_after_restore") != null) {
                dropConstraint("alter table t1 drop constraint uk_after_restore");
            }
        }
    }

    private void setEnvAccessManager(AccessControllerManager accessManager) throws Exception {
        Field field = Env.class.getDeclaredField("accessManager");
        field.setAccessible(true);
        field.set(Env.getCurrentEnv(), accessManager);
    }

    @Test
    void cascadeDropTest() throws Exception {
        addConstraint("alter table t1 add constraint pk primary key (k1)");
        addConstraint("alter table t2 add constraint fk foreign key (k1) references t1(k1)");
        dropConstraint("alter table t1 drop constraint pk");

        PlanChecker.from(connectContext).parse("select * from t2").analyze().matches(
                logicalOlapScan().when(o -> getConstraintMgr()
                        .getConstraints(tableNameInfoOf(o.getTable())).isEmpty()));

        addConstraint("alter table t1 add constraint pk primary key (k1)");
        addConstraint("alter table t1 add constraint fk foreign key (k1) references t1(k1)");
        addConstraint("alter table t2 add constraint fk foreign key (k1) references t1(k1)");
        addConstraint("alter table t3 add constraint fk foreign key (k1) references t1(k1)");
        dropConstraint("alter table t1 drop constraint pk");
        PlanChecker.from(connectContext).parse("select * from t1").analyze().matches(
                logicalOlapScan().when(o -> getConstraintMgr()
                        .getConstraints(tableNameInfoOf(o.getTable())).isEmpty()));
        PlanChecker.from(connectContext).parse("select * from t2").analyze().matches(
                logicalOlapScan().when(o -> getConstraintMgr()
                        .getConstraints(tableNameInfoOf(o.getTable())).isEmpty()));
        PlanChecker.from(connectContext).parse("select * from t3").analyze().matches(
                logicalOlapScan().when(o -> getConstraintMgr()
                        .getConstraints(tableNameInfoOf(o.getTable())).isEmpty()));
    }

    @Test
    void dropTableBlockedByForeignKeyTest() throws Exception {
        // Setup: PK on t1, FK on t2 referencing t1
        addConstraint("alter table t1 add constraint pk_dt primary key (k1)");
        addConstraint("alter table t2 add constraint fk_dt foreign key (k1) references t1(k1)");

        // Drop t1 should fail because t2's FK references t1's PK
        Assertions.assertThrows(Exception.class, () -> {
            executeSql("drop table t1");
        });

        // Verify t1 still exists and constraints are intact
        TableNameInfo t1Info = new TableNameInfo("internal", "test", "t1");
        Assertions.assertNotNull(getConstraintMgr().getConstraint(t1Info, "pk_dt"));

        // Cleanup: drop FK first, then PK
        dropConstraint("alter table t2 drop constraint fk_dt");
        dropConstraint("alter table t1 drop constraint pk_dt");
    }

    @Test
    void forceDropTableCascadesForeignKeyTest() throws Exception {
        // Create new tables for this test to avoid affecting other tests
        createTable("create table t_pk (\n"
                + "    k1 int,\n"
                + "    k2 int\n"
                + ")\n"
                + "unique key(k1, k2)\n"
                + "distributed by hash(k1) buckets 4\n"
                + "properties(\n"
                + "    \"replication_num\"=\"1\"\n"
                + ")");
        createTable("create table t_fk (\n"
                + "    k1 int,\n"
                + "    k2 int\n"
                + ")\n"
                + "unique key(k1, k2)\n"
                + "distributed by hash(k1) buckets 4\n"
                + "properties(\n"
                + "    \"replication_num\"=\"1\"\n"
                + ")");

        addConstraint("alter table t_pk add constraint pk_force primary key (k1)");
        addConstraint("alter table t_fk add constraint fk_force foreign key (k1) references t_pk(k1)");

        // Force drop t_pk should succeed and cascade-drop FK on t_fk
        executeSql("drop table t_pk force");

        // Verify FK on t_fk was cascade-dropped
        TableNameInfo tFkInfo = new TableNameInfo("internal", "test", "t_fk");
        Assertions.assertTrue(getConstraintMgr().getConstraints(tFkInfo).isEmpty());

        // Cleanup
        executeSql("drop table t_fk force");
    }

    @Test
    void dropColumnBlockedByConstraintTest() throws Exception {
        // Create a table with non-key columns
        createTable("create table t_schema (\n"
                + "    k1 int,\n"
                + "    v1 int,\n"
                + "    v2 int\n"
                + ")\n"
                + "unique key(k1)\n"
                + "distributed by hash(k1) buckets 4\n"
                + "properties(\n"
                + "    \"replication_num\"=\"1\"\n"
                + ")");

        // Add UNIQUE constraint on v1
        addConstraint("alter table t_schema add constraint un_v1 unique (v1)");

        // Try to drop column v1 -> should fail because of constraint
        Assertions.assertThrows(Exception.class, () -> {
            executeSql("alter table t_schema drop column v1");
        });

        // Drop constraint first, then drop column should not throw during validation
        dropConstraint("alter table t_schema drop constraint un_v1");

        // Cleanup
        executeSql("drop table t_schema force");
    }

    @Test
    void replaceTableWithConstraintsTest() throws Exception {
        // Create tables for replace test
        createTable("create table t_orig (\n"
                + "    k1 int,\n"
                + "    k2 int\n"
                + ")\n"
                + "unique key(k1, k2)\n"
                + "distributed by hash(k1) buckets 4\n"
                + "properties(\n"
                + "    \"replication_num\"=\"1\"\n"
                + ")");
        createTable("create table t_repl (\n"
                + "    k1 int,\n"
                + "    k2 int\n"
                + ")\n"
                + "unique key(k1, k2)\n"
                + "distributed by hash(k1) buckets 4\n"
                + "properties(\n"
                + "    \"replication_num\"=\"1\"\n"
                + ")");
        createTable("create table t_ref (\n"
                + "    k1 int,\n"
                + "    k2 int\n"
                + ")\n"
                + "unique key(k1, k2)\n"
                + "distributed by hash(k1) buckets 4\n"
                + "properties(\n"
                + "    \"replication_num\"=\"1\"\n"
                + ")");

        // Add PK on t_orig, FK on t_ref referencing t_orig
        addConstraint("alter table t_orig add constraint pk_orig primary key (k1)");
        addConstraint("alter table t_ref add constraint fk_ref foreign key (k1) references t_orig(k1)");

        // Replace t_orig with t_repl (no swap, t_orig gets dropped) -> should fail
        // because t_orig's PK is referenced by t_ref's FK
        Assertions.assertThrows(Exception.class, () -> {
            executeSql("alter table t_orig replace with table t_repl "
                    + "properties(\"swap\"=\"false\")");
        });

        // Drop FK first
        dropConstraint("alter table t_ref drop constraint fk_ref");

        // Now replace should succeed
        executeSql("alter table t_orig replace with table t_repl "
                + "properties(\"swap\"=\"false\")");

        // After replace: t_repl is renamed to t_orig, old t_orig is dropped
        TableNameInfo tOrigInfo = new TableNameInfo("internal", "test", "t_orig");
        Assertions.assertTrue(getConstraintMgr().getConstraints(tOrigInfo).isEmpty());

        // Cleanup
        executeSql("drop table if exists t_orig force");
        executeSql("drop table if exists t_repl force");
        executeSql("drop table if exists t_ref force");
    }

    @Test
    void distributionMappingDropUsesCurrentTableAfterSwap() throws Exception {
        createTable("create table mapping_swap_a (k1 int, k2 int) "
                + "duplicate key(k1) distributed by hash(k1) buckets 4 "
                + "properties('replication_num'='1')");
        createTable("create table mapping_swap_b (k1 int, k2 int) "
                + "duplicate key(k1) distributed by hash(k1) buckets 4 "
                + "properties('replication_num'='1')");
        addConstraint("alter table mapping_swap_a add constraint mapping_a "
                + "colocate mapping swap_mapping_a (k2) determines distribution key (k1) not enforced");
        addConstraint("alter table mapping_swap_b add constraint mapping_b "
                + "colocate mapping swap_mapping_b (k2) determines distribution key (k1) not enforced");

        OlapTable originalA = (OlapTable) Env.getCurrentInternalCatalog()
                .getDbOrDdlException("test").getTableOrDdlException("mapping_swap_a");
        OlapTable originalB = (OlapTable) Env.getCurrentInternalCatalog()
                .getDbOrDdlException("test").getTableOrDdlException("mapping_swap_b");
        executeSql("alter table mapping_swap_a replace with table mapping_swap_b "
                + "properties('swap'='true')");

        OlapTable currentA = (OlapTable) Env.getCurrentInternalCatalog()
                .getDbOrDdlException("test").getTableOrDdlException("mapping_swap_a");
        OlapTable currentB = (OlapTable) Env.getCurrentInternalCatalog()
                .getDbOrDdlException("test").getTableOrDdlException("mapping_swap_b");
        Assertions.assertEquals(originalB.getId(), currentA.getId());
        Assertions.assertEquals(originalA.getId(), currentB.getId());

        SqlCacheContext cacheA = new SqlCacheContext(new UserIdentity("admin", "127.0.0.1"));
        cacheA.addUsedTable(currentA);
        SqlCacheContext cacheB = new SqlCacheContext(new UserIdentity("admin", "127.0.0.1"));
        cacheB.addUsedTable(currentB);
        Env.getCurrentEnv().getSqlCacheManager().getSqlCaches().put("mapping_swap_a_cache", cacheA);
        Env.getCurrentEnv().getSqlCacheManager().getSqlCaches().put("mapping_swap_b_cache", cacheB);

        dropConstraint("alter table mapping_swap_a drop constraint mapping_b");

        Assertions.assertTrue(getConstraintMgr().getDistributionMappingConstraints(currentA).isEmpty());
        Assertions.assertEquals(1, getConstraintMgr().getDistributionMappingConstraints(currentB).size());
        Assertions.assertNull(Env.getCurrentEnv().getSqlCacheManager()
                .getSqlCaches().getIfPresent("mapping_swap_a_cache"));
        Assertions.assertNotNull(Env.getCurrentEnv().getSqlCacheManager()
                .getSqlCaches().getIfPresent("mapping_swap_b_cache"));

        dropConstraint("alter table mapping_swap_b drop constraint mapping_a");
        executeSql("drop table mapping_swap_a force");
        executeSql("drop table mapping_swap_b force");
    }

    @Test
    void dropConstraintOnNonExistentTableTest() throws Exception {
        // Simulate an external table scenario: a constraint exists in the manager
        // but the table has been deleted by another system.
        ConstraintManager mgr = getConstraintMgr();
        TableNameInfo ghostTable = new TableNameInfo("internal", "test", "ghost_table");
        PrimaryKeyConstraint pk = new PrimaryKeyConstraint("ghost_pk", Sets.newHashSet("col1"));
        // Add via replay path to bypass table validation
        mgr.addConstraint(ghostTable, "ghost_pk", pk, true);
        Assertions.assertNotNull(mgr.getConstraint(ghostTable, "ghost_pk"));

        // Drop constraint via SQL — the table does not exist, but the command should still succeed
        DropConstraintCommand dropCmd = (DropConstraintCommand) new NereidsParser().parseSingle(
                "alter table test.ghost_table drop constraint ghost_pk");
        dropCmd.run(connectContext, null);

        // Constraint should be removed
        Assertions.assertNull(mgr.getConstraint(ghostTable, "ghost_pk"));
    }

    @Test
    void renameTableUpdatesConstraintsTest() throws Exception {
        // Create dedicated tables for rename test
        createTable("create table t_rename_src (\n"
                + "    k1 int,\n"
                + "    k2 int\n"
                + ")\n"
                + "unique key(k1, k2)\n"
                + "distributed by hash(k1) buckets 4\n"
                + "properties(\n"
                + "    \"replication_num\"=\"1\"\n"
                + ")");

        addConstraint("alter table t_rename_src add constraint pk_rename primary key (k1)");
        TableNameInfo oldInfo = new TableNameInfo("internal", "test", "t_rename_src");
        Assertions.assertNotNull(getConstraintMgr().getConstraint(oldInfo, "pk_rename"));

        // Rename the table
        executeSql("alter table t_rename_src rename t_rename_dst");

        // Constraint should be accessible under the new name
        TableNameInfo newInfo = new TableNameInfo("internal", "test", "t_rename_dst");
        Assertions.assertNotNull(getConstraintMgr().getConstraint(newInfo, "pk_rename"));

        // Old name should no longer have constraints
        Assertions.assertTrue(getConstraintMgr().getConstraints(oldInfo).isEmpty());

        // Cleanup
        dropConstraint("alter table t_rename_dst drop constraint pk_rename");
        executeSql("drop table t_rename_dst force");
    }
}
