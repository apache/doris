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

package org.apache.doris.backup;

import org.apache.doris.backup.BackupJobInfo.BackupIndexInfo;
import org.apache.doris.backup.BackupJobInfo.BackupOlapTableInfo;
import org.apache.doris.backup.BackupJobInfo.BackupPartitionInfo;
import org.apache.doris.backup.BackupJobInfo.BackupTabletInfo;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.catalog.Resource;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.catalog.constraint.ConstraintManager;
import org.apache.doris.catalog.constraint.ForeignKeyConstraint;
import org.apache.doris.catalog.constraint.PrimaryKeyConstraint;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.MarkedCountDownLatch;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.datasource.storage.StorageAdapter;
import org.apache.doris.persist.EditLog;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.thrift.TStorageMedium;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.Adler32;

public class RestoreJobTest {

    private Database db;
    private BackupJobInfo jobInfo;
    private RestoreJob job;
    private String label = "test_label";

    private AtomicLong id = new AtomicLong(50000);

    private OlapTable expectedRestoreTbl;

    private long repoId = 20000;

    private Env env = Mockito.mock(Env.class);
    private InternalCatalog catalog = Mockito.mock(InternalCatalog.class);

    private MockBackupHandler backupHandler;

    private MockRepositoryMgr repoMgr;

    public RestoreJobTest() throws UserException {
    }

    // Thread is not mockable in Jmockit, use subclass instead
    private final class MockBackupHandler extends BackupHandler {
        public MockBackupHandler(Env env) {
            super(env);
        }

        @Override
        public RepositoryMgr getRepoMgr() {
            return repoMgr;
        }
    }

    // Thread is not mockable in Jmockit, use subclass instead
    private final class MockRepositoryMgr extends RepositoryMgr {
        public MockRepositoryMgr() {
            super();
        }

        @Override
        public Repository getRepo(long repoId) {
            return repo;
        }
    }

    private EditLog editLog = Mockito.mock(EditLog.class);
    private SystemInfoService systemInfoService = Mockito.mock(SystemInfoService.class);

    private Repository repo = Mockito.spy(new Repository(repoId, "repo", false, "bos://my_repo",
            StorageAdapter.ofBroker("broker", Maps.newHashMap())));

    private BackupMeta backupMeta;

    private MockedStatic<Env> mockedEnvStatic;
    @SuppressWarnings("rawtypes")
    private MockedConstruction<MarkedCountDownLatch> mockedMarkedCountDownLatch;

    @Before
    public void setUp() throws Exception {
        db = CatalogMocker.mockDb();
        backupHandler = new MockBackupHandler(env);
        repoMgr = new MockRepositoryMgr();

        Deencapsulation.setField(env, "backupHandler", backupHandler);

        mockedEnvStatic = Mockito.mockStatic(Env.class);
        mockedEnvStatic.when(Env::getCurrentEnvJournalVersion).thenReturn(FeConstants.meta_version);
        mockedEnvStatic.when(Env::getCurrentSystemInfo).thenReturn(systemInfoService);

        Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getDbNullable(Mockito.anyLong())).thenReturn(db);
        Mockito.when(catalog.getDbOrMetaException(Mockito.anyLong())).thenReturn(db);
        Mockito.when(env.getNextId()).thenAnswer(inv -> id.getAndIncrement());
        Mockito.when(env.getEditLog()).thenReturn(editLog);

        Mockito.doAnswer(inv -> {
            List<Long> beIds = Lists.newArrayList();
            beIds.add(CatalogMocker.BACKEND1_ID);
            beIds.add(CatalogMocker.BACKEND2_ID);
            beIds.add(CatalogMocker.BACKEND3_ID);
            return beIds;
        }).when(systemInfoService).selectBackendIdsForReplicaCreation(
                Mockito.any(ReplicaAllocation.class),
                Mockito.anyMap(),
                Mockito.any(TStorageMedium.class),
                Mockito.eq(false),
                Mockito.eq(true));

        Mockito.doAnswer(inv -> {
            BackupJob job = inv.getArgument(0);
            System.out.println("log backup job: " + job);
            return null;
        }).when(editLog).logBackupJob(Mockito.any(BackupJob.class));

        Mockito.doReturn(Status.OK).when(repo).upload(Mockito.anyString(), Mockito.anyString());
        Mockito.doAnswer(inv -> {
            List<BackupMeta> metas = inv.getArgument(1);
            metas.add(backupMeta);
            return Status.OK;
        }).when(repo).getSnapshotMetaFile(Mockito.eq(label), Mockito.anyList(), Mockito.eq(-1));

        mockedMarkedCountDownLatch = Mockito.mockConstruction(MarkedCountDownLatch.class,
                Mockito.withSettings().defaultAnswer(Mockito.CALLS_REAL_METHODS),
                (mock, context) -> {
                    Mockito.doReturn(true).when(mock).await(Mockito.anyLong(), Mockito.any(TimeUnit.class));
                });

        // gen BackupJobInfo
        jobInfo = new BackupJobInfo();
        jobInfo.backupTime = System.currentTimeMillis();
        jobInfo.dbId = CatalogMocker.TEST_DB_ID;
        jobInfo.dbName = CatalogMocker.TEST_DB_NAME;
        jobInfo.name = label;
        jobInfo.success = true;

        expectedRestoreTbl = (OlapTable) db.getTableNullable(CatalogMocker.TEST_TBL2_ID);
        BackupOlapTableInfo tblInfo = new BackupOlapTableInfo();
        tblInfo.id = CatalogMocker.TEST_TBL2_ID;
        jobInfo.backupOlapTableObjects.put(CatalogMocker.TEST_TBL2_NAME, tblInfo);

        for (Partition partition : expectedRestoreTbl.getPartitions()) {
            BackupPartitionInfo partInfo = new BackupPartitionInfo();
            partInfo.id = partition.getId();
            tblInfo.partitions.put(partition.getName(), partInfo);

            for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                BackupIndexInfo idxInfo = new BackupIndexInfo();
                idxInfo.id = index.getId();
                idxInfo.schemaHash = expectedRestoreTbl.getSchemaHashByIndexId(index.getId());
                partInfo.indexes.put(expectedRestoreTbl.getIndexNameById(index.getId()), idxInfo);

                for (Tablet tablet : index.getTablets()) {
                    List<String> files = Lists.newArrayList(tablet.getId() + ".dat",
                            tablet.getId() + ".idx", tablet.getId() + ".hdr");
                    BackupTabletInfo tabletInfo = new BackupTabletInfo(tablet.getId(), files);
                    idxInfo.sortedTabletInfoList.add(tabletInfo);
                }
            }
        }

        // drop this table, cause we want to try restoring this table
        db.unregisterTable(expectedRestoreTbl.getName());

        job = new RestoreJob(label, "2018-01-01 01:01:01", db.getId(), db.getFullName(), jobInfo, false,
                new ReplicaAllocation((short) 3), 100000, -1, false, false, false, false, false, false, false, false,
                env, repo.getId());

        List<Table> tbls = Lists.newArrayList();
        List<Resource> resources = Lists.newArrayList();
        tbls.add(expectedRestoreTbl);
        backupMeta = new BackupMeta(tbls, resources);
    }

    @After
    public void tearDown() {
        if (mockedEnvStatic != null) {
            mockedEnvStatic.close();
        }
        if (mockedMarkedCountDownLatch != null) {
            mockedMarkedCountDownLatch.close();
        }
    }

    @Test
    public void testSignature() throws AnalysisException {
        Adler32 sig1 = new Adler32();
        sig1.update("name1".getBytes());
        sig1.update("name2".getBytes());
        System.out.println("sig1: " + Math.abs((int) sig1.getValue()));

        Adler32 sig2 = new Adler32();
        sig2.update("name2".getBytes());
        sig2.update("name1".getBytes());
        System.out.println("sig2: " + Math.abs((int) sig2.getValue()));

        OlapTable tbl = db.getOlapTableOrAnalysisException(CatalogMocker.TEST_TBL_NAME);
        List<String> partNames = Lists.newArrayList(tbl.getPartitionNames());
        System.out.println(partNames);
        System.out.println("tbl signature: " + tbl.getSignature(BackupHandler.SIGNATURE_VERSION, partNames));
        tbl.setName("newName");
        partNames = Lists.newArrayList(tbl.getPartitionNames());
        System.out.println("tbl signature: " + tbl.getSignature(BackupHandler.SIGNATURE_VERSION, partNames));
    }

    @Test
    public void testSerialization() throws IOException, AnalysisException {
        // 1. Write objects to file
        final Path path = Files.createTempFile("restoreJob", "tmp");
        DataOutputStream out = new DataOutputStream(Files.newOutputStream(path));

        job.write(out);
        out.flush();
        out.close();

        // 2. Read objects from file
        DataInputStream in = new DataInputStream(Files.newInputStream(path));

        RestoreJob job2 = RestoreJob.read(in);

        Assert.assertEquals(job.getJobId(), job2.getJobId());
        Assert.assertEquals(job.getDbId(), job2.getDbId());
        Assert.assertEquals(job.getCreateTime(), job2.getCreateTime());
        Assert.assertEquals(job.getType(), job2.getType());

        // 3. delete files
        in.close();
        Files.delete(path);
    }

    @Test
    public void testResetPartitionVisibleAndNextVersionForRestore() throws Exception {
        long visibleVersion = 1234;
        long remotePartId = 123;
        String partName = "p20240723";
        MaterializedIndex index = new MaterializedIndex();
        Partition remotePart = new Partition(remotePartId, partName, index, new HashDistributionInfo());
        remotePart.setVisibleVersionAndTime(visibleVersion, 0);
        remotePart.setNextVersion(visibleVersion + 10);

        OlapTable localTbl = new OlapTable();
        localTbl.setPartitionInfo(new PartitionInfo(PartitionType.RANGE));
        OlapTable remoteTbl = new OlapTable();
        remoteTbl.addPartition(remotePart);
        remoteTbl.setPartitionInfo(new PartitionInfo(PartitionType.RANGE));

        ReplicaAllocation alloc = new ReplicaAllocation();
        job.resetPartitionForRestore(localTbl, remoteTbl, partName, alloc);

        Partition localPart = remoteTbl.getPartition(partName);
        Assert.assertEquals(localPart.getVisibleVersion(), visibleVersion);
        Assert.assertEquals(localPart.getNextVersion(), visibleVersion + 1);
    }

    @Test
    public void testReplayRestoreRebuildsConstraintIndex() {
        ConstraintManager constraintManager = Mockito.mock(ConstraintManager.class);
        TabletInvertedIndex invertedIndex = Mockito.mock(TabletInvertedIndex.class);
        Mockito.when(env.getConstraintManager()).thenReturn(constraintManager);
        mockedEnvStatic.when(Env::getCurrentEnv).thenReturn(env);
        mockedEnvStatic.when(Env::getCurrentInvertedIndex).thenReturn(invertedIndex);
        Deencapsulation.setField(job, "restoredTbls", Lists.newArrayList(expectedRestoreTbl));

        Deencapsulation.invoke(job, "replayCheckAndPrepareMeta");

        Mockito.verify(constraintManager).restoreTableConstraints(
                Mockito.any(), Mockito.same(expectedRestoreTbl));
    }

    @Test
    public void testCancelRestoreDropsConstraintIndex() {
        ConstraintManager constraintManager = Mockito.mock(ConstraintManager.class);
        TabletInvertedIndex invertedIndex = Mockito.mock(TabletInvertedIndex.class);
        Mockito.when(env.getConstraintManager()).thenReturn(constraintManager);
        mockedEnvStatic.when(Env::getCurrentEnv).thenReturn(env);
        mockedEnvStatic.when(Env::getCurrentInvertedIndex).thenReturn(invertedIndex);
        db.registerTable(expectedRestoreTbl);
        Deencapsulation.setField(job, "restoredTbls", Lists.newArrayList(expectedRestoreTbl));

        job.cleanMetaObjects(false);

        Mockito.verify(constraintManager).dropTableConstraints(Mockito.any());
    }

    @Test
    public void testCancelRestoreDoesNotDropConstraintsForReplacementTable() {
        ConstraintManager constraintManager = Mockito.mock(ConstraintManager.class);
        TabletInvertedIndex invertedIndex = Mockito.mock(TabletInvertedIndex.class);
        OlapTable replacementTable = Mockito.mock(OlapTable.class);
        Mockito.when(replacementTable.getId()).thenReturn(expectedRestoreTbl.getId() + 1);
        Mockito.when(replacementTable.getName()).thenReturn(expectedRestoreTbl.getName());
        Mockito.when(env.getConstraintManager()).thenReturn(constraintManager);
        mockedEnvStatic.when(Env::getCurrentEnv).thenReturn(env);
        mockedEnvStatic.when(Env::getCurrentInvertedIndex).thenReturn(invertedIndex);
        db.registerTable(replacementTable);
        Deencapsulation.setField(job, "restoredTbls", Lists.newArrayList(expectedRestoreTbl));

        job.cleanMetaObjects(false);

        Assert.assertSame(replacementTable, db.getTableNullable(expectedRestoreTbl.getName()));
        Mockito.verifyNoInteractions(constraintManager);
    }

    @Test
    public void testAtomicRestoreRejectsReferencedOriginBeforeReplacement() throws Exception {
        ConstraintManager constraintManager = new ConstraintManager();
        Database database = Mockito.mock(Database.class);
        OlapTable restoredTable = Mockito.mock(OlapTable.class);
        OlapTable originTable = Mockito.mock(OlapTable.class);
        String originName = CatalogMocker.TEST_TBL2_NAME;
        String aliasName = RestoreJob.tableAliasWithAtomicRestore(originName);
        TableNameInfo originTableInfo = new TableNameInfo(
                InternalCatalog.INTERNAL_CATALOG_NAME, CatalogMocker.TEST_DB_NAME, originName);
        TableNameInfo referencingTableInfo = new TableNameInfo(
                InternalCatalog.INTERNAL_CATALOG_NAME, CatalogMocker.TEST_DB_NAME, "referencing_table");
        constraintManager.addConstraint(originTableInfo, "pk",
                new PrimaryKeyConstraint("pk", ImmutableSet.of("k1")), true);
        constraintManager.addConstraint(referencingTableInfo, "fk",
                new ForeignKeyConstraint("fk", ImmutableList.of("k1"),
                        originTableInfo, ImmutableList.of("k1")), true);

        Mockito.when(env.getConstraintManager()).thenReturn(constraintManager);
        mockedEnvStatic.when(Env::getCurrentEnv).thenReturn(env);
        Mockito.when(database.writeLockIfExist()).thenReturn(true);
        Mockito.when(database.isWriteLockHeldByCurrentThread()).thenReturn(true);
        Mockito.when(database.getFullName()).thenReturn(CatalogMocker.TEST_DB_NAME);
        Mockito.when(database.getTableNullable(aliasName)).thenReturn(restoredTable);
        Mockito.when(database.getTableNullable(originName)).thenReturn(originTable);
        Mockito.when(restoredTable.getType()).thenReturn(Table.TableType.OLAP);
        Mockito.when(originTable.getType()).thenReturn(Table.TableType.OLAP);
        Deencapsulation.setField(job, "isAtomicRestore", true);

        Status status = Deencapsulation.invoke(job, "atomicReplaceOlapTables", database, false);

        Assert.assertFalse(status.ok());
        Assert.assertNotNull(constraintManager.getConstraint(originTableInfo, "pk"));
        Assert.assertNotNull(constraintManager.getConstraint(referencingTableInfo, "fk"));
        Mockito.verify(database, Mockito.never()).unregisterTable(Mockito.anyString());
        Mockito.verify(database, Mockito.never()).registerTable(Mockito.any());
    }

    @Test
    public void testAtomicRestorePrevalidatesEveryTargetBeforeLeaderOrReplayMutation()
            throws Exception {
        ConstraintManager constraintManager = new ConstraintManager();
        Database database = Mockito.mock(Database.class);
        String firstOriginName = "first_origin";
        String secondOriginName = "second_origin";
        String firstAliasName = RestoreJob.tableAliasWithAtomicRestore(firstOriginName);
        String secondAliasName = RestoreJob.tableAliasWithAtomicRestore(secondOriginName);
        OlapTable firstRestoredTable = Mockito.mock(OlapTable.class);
        OlapTable secondRestoredTable = Mockito.mock(OlapTable.class);
        OlapTable firstOriginTable = Mockito.mock(OlapTable.class);
        OlapTable secondOriginTable = Mockito.mock(OlapTable.class);
        TableNameInfo secondOriginTableInfo = new TableNameInfo(
                InternalCatalog.INTERNAL_CATALOG_NAME,
                CatalogMocker.TEST_DB_NAME, secondOriginName);
        TableNameInfo referencingTableInfo = new TableNameInfo(
                InternalCatalog.INTERNAL_CATALOG_NAME,
                CatalogMocker.TEST_DB_NAME, "referencing_table");
        constraintManager.addConstraint(secondOriginTableInfo, "pk",
                new PrimaryKeyConstraint("pk", ImmutableSet.of("k1")), true);
        constraintManager.addConstraint(referencingTableInfo, "fk",
                new ForeignKeyConstraint("fk", ImmutableList.of("k1"),
                        secondOriginTableInfo, ImmutableList.of("k1")), true);

        jobInfo.backupOlapTableObjects.clear();
        jobInfo.backupOlapTableObjects.put(firstOriginName, new BackupOlapTableInfo());
        jobInfo.backupOlapTableObjects.put(secondOriginName, new BackupOlapTableInfo());
        Mockito.when(env.getConstraintManager()).thenReturn(constraintManager);
        mockedEnvStatic.when(Env::getCurrentEnv).thenReturn(env);
        Mockito.when(database.isWriteLockHeldByCurrentThread()).thenReturn(true);
        Mockito.when(database.getFullName()).thenReturn(CatalogMocker.TEST_DB_NAME);
        Mockito.when(database.getTableNullable(firstAliasName)).thenReturn(firstRestoredTable);
        Mockito.when(database.getTableNullable(secondAliasName)).thenReturn(secondRestoredTable);
        Mockito.when(database.getTableNullable(firstOriginName)).thenReturn(firstOriginTable);
        Mockito.when(database.getTableNullable(secondOriginName)).thenReturn(secondOriginTable);
        Mockito.when(firstRestoredTable.getType()).thenReturn(Table.TableType.OLAP);
        Mockito.when(secondRestoredTable.getType()).thenReturn(Table.TableType.OLAP);
        Mockito.when(firstOriginTable.getType()).thenReturn(Table.TableType.OLAP);
        Mockito.when(secondOriginTable.getType()).thenReturn(Table.TableType.OLAP);
        Deencapsulation.setField(job, "isAtomicRestore", true);

        Status leaderStatus = Deencapsulation.invoke(
                job, "atomicReplaceOlapTables", database, false);
        Status replayStatus = Deencapsulation.invoke(
                job, "atomicReplaceOlapTables", database, true);

        Assert.assertFalse(leaderStatus.ok());
        Assert.assertFalse(replayStatus.ok());
        Assert.assertNotNull(constraintManager.getConstraint(secondOriginTableInfo, "pk"));
        Assert.assertNotNull(constraintManager.getConstraint(referencingTableInfo, "fk"));
        Mockito.verify(database, Mockito.never()).unregisterTable(Mockito.anyString());
        Mockito.verify(database, Mockito.never()).registerTable(Mockito.any());
    }

    @Test
    public void testAtomicRestorePrevalidatesCleanTablesBeforeReplacement() throws Exception {
        ConstraintManager constraintManager = new ConstraintManager();
        Database database = Mockito.mock(Database.class);
        String originName = "restore_target";
        String aliasName = RestoreJob.tableAliasWithAtomicRestore(originName);
        String cleanTableName = "clean_target";
        OlapTable restoredTable = Mockito.mock(OlapTable.class);
        OlapTable originTable = Mockito.mock(OlapTable.class);
        OlapTable cleanTable = Mockito.mock(OlapTable.class);
        TableNameInfo cleanTableInfo = new TableNameInfo(
                InternalCatalog.INTERNAL_CATALOG_NAME,
                CatalogMocker.TEST_DB_NAME, cleanTableName);
        TableNameInfo referencingTableInfo = new TableNameInfo(
                InternalCatalog.INTERNAL_CATALOG_NAME,
                "another_db", "referencing_table");
        constraintManager.addConstraint(cleanTableInfo, "pk",
                new PrimaryKeyConstraint("pk", ImmutableSet.of("k1")), true);
        constraintManager.addConstraint(referencingTableInfo, "fk",
                new ForeignKeyConstraint("fk", ImmutableList.of("k1"),
                        cleanTableInfo, ImmutableList.of("k1")), true);

        jobInfo.backupOlapTableObjects.clear();
        jobInfo.backupOlapTableObjects.put(originName, new BackupOlapTableInfo());
        Mockito.when(env.getConstraintManager()).thenReturn(constraintManager);
        mockedEnvStatic.when(Env::getCurrentEnv).thenReturn(env);
        Mockito.when(database.isWriteLockHeldByCurrentThread()).thenReturn(true);
        Mockito.when(database.getFullName()).thenReturn(CatalogMocker.TEST_DB_NAME);
        Mockito.when(database.getTableNullable(aliasName)).thenReturn(restoredTable);
        Mockito.when(database.getTableNullable(originName)).thenReturn(originTable);
        Mockito.when(database.getTables()).thenReturn(ImmutableList.of(cleanTable));
        Mockito.when(restoredTable.getType()).thenReturn(Table.TableType.OLAP);
        Mockito.when(originTable.getType()).thenReturn(Table.TableType.OLAP);
        Mockito.when(cleanTable.getType()).thenReturn(Table.TableType.OLAP);
        Mockito.when(cleanTable.getName()).thenReturn(cleanTableName);
        Deencapsulation.setField(job, "isAtomicRestore", true);
        Deencapsulation.setField(job, "isCleanTables", true);

        Status status = Deencapsulation.invoke(
                job, "atomicReplaceOlapTables", database, false);

        Assert.assertFalse(status.ok());
        Assert.assertNotNull(constraintManager.getConstraint(cleanTableInfo, "pk"));
        Assert.assertNotNull(constraintManager.getConstraint(referencingTableInfo, "fk"));
        Mockito.verify(database, Mockito.never()).unregisterTable(Mockito.anyString());
        Mockito.verify(database, Mockito.never()).registerTable(Mockito.any());
    }

    @Test
    public void testAtomicRestoreWritesFinishedJournalUnderDatabaseLock() {
        Mockito.when(env.getConstraintManager()).thenReturn(new ConstraintManager());
        mockedEnvStatic.when(Env::getCurrentEnv).thenReturn(env);
        jobInfo.backupOlapTableObjects.clear();
        Deencapsulation.setField(job, "isAtomicRestore", true);
        com.google.common.collect.Table<Long, Long, SnapshotInfo> snapshotInfos =
                HashBasedTable.create();
        snapshotInfos.put(1L, 2L,
                new SnapshotInfo(db.getId(), 3L, 4L, 5L, 2L,
                        1L, 6, "/snapshot", ImmutableList.of()));
        Deencapsulation.setField(job, "snapshotInfos", snapshotInfos);
        Mockito.doAnswer(invocation -> {
            Assert.assertTrue(db.isWriteLockHeldByCurrentThread());
            return null;
        }).when(editLog).logRestoreJob(job);

        Status status;
        try (MockedStatic<AgentTaskExecutor> agentTaskExecutor =
                Mockito.mockStatic(AgentTaskExecutor.class)) {
            agentTaskExecutor.when(() -> AgentTaskExecutor.submit(
                            Mockito.any(AgentBatchTask.class)))
                    .thenAnswer(invocation -> {
                        Assert.assertFalse(db.isWriteLockHeldByCurrentThread());
                        return null;
                    });
            status = job.allTabletCommitted(false);
            agentTaskExecutor.verify(() -> AgentTaskExecutor.submit(
                    Mockito.any(AgentBatchTask.class)));
        }

        Assert.assertTrue(status.ok());
        Mockito.verify(editLog).logRestoreJob(job);
    }
}
