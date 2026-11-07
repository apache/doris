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
import org.apache.doris.catalog.ColocateGroupSchema;
import org.apache.doris.catalog.ColocateTableIndex;
import org.apache.doris.catalog.ColocateTableIndex.GroupId;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.LocalTabletInvertedIndex;
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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.MarkedCountDownLatch;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.datasource.storage.StorageAdapter;
import org.apache.doris.persist.ColocatePersistInfo;
import org.apache.doris.persist.EditLog;
import org.apache.doris.persist.OperationType;
import org.apache.doris.persist.TablePropertyInfo;
import org.apache.doris.resource.Tag;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TStorageMedium;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

    @BeforeEach
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

    @AfterEach
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

        Assertions.assertEquals(job.getJobId(), job2.getJobId());
        Assertions.assertEquals(job.getDbId(), job2.getDbId());
        Assertions.assertEquals(job.getCreateTime(), job2.getCreateTime());
        Assertions.assertEquals(job.getType(), job2.getType());

        // 3. delete files
        in.close();
        Files.delete(path);
    }

    @Test
    public void testRestoreBuildsBaseAndRollupReplicasUsingPublishedWinner() throws Exception {
        checkRestoreMembershipReplay(false);
    }

    @Test
    public void testRestoreMembershipReplayPreservesExistingSchemaAndNewerSequence() throws Exception {
        checkRestoreMembershipReplay(true);
    }

    private void checkRestoreMembershipReplay(boolean existingSchema) throws Exception {
        ColocateTableIndex colocateIndex = prepareRestoreColocateIndex();
        List<ColocatePersistInfo> initialized = resetColocateTableForRestore(colocateIndex);
        Assertions.assertEquals(1, initialized.size());
        GroupId groupId = colocateIndex.getGroup(expectedRestoreTbl.getId());
        Map<Tag, List<List<Long>>> newer = Map.of(Tag.DEFAULT_BACKEND_TAG, List.of(List.of(81L, 82L, 83L)));
        Assertions.assertTrue(colocateIndex.addBackendsPerBucketSeqByTag(groupId, Tag.DEFAULT_BACKEND_TAG,
                newer.get(Tag.DEFAULT_BACKEND_TAG), new ReplicaAllocation((short) 3)));
        ColocatePersistInfo sequenceUpdate = roundTripColocateInfo(
                ColocatePersistInfo.createForBackendsPerBucketSeq(groupId, newer));
        Pair<RestoreJob, ColocatePersistInfo> logs = finishRestoreSnapshots();
        Assertions.assertEquals(newer, logs.second.getBackendsPerBucketSeq(),
                "The ADD emitted after DOWNLOAD must snapshot Q, not the reset-time P or an empty descriptor");

        Database replayDb = new Database(db.getId(), db.getFullName());
        ColocateTableIndex replay = new ColocateTableIndex();
        if (existingSchema) {
            OlapTable existingMember = newColocateMember();
            String fullGroupName = GroupId.getFullGroupName(db.getId(), expectedRestoreTbl.getColocateGroup());
            Assertions.assertTrue(replayDb.registerTable(existingMember));
            Assertions.assertTrue(db.registerTable(existingMember));
            replay.addTableToGroup(db.getId(), existingMember, fullGroupName, groupId);
            colocateIndex.addTableToGroup(db.getId(), existingMember, fullGroupName, groupId);
        }
        ColocateGroupSchema originalSchema = replay.getGroupSchema(groupId);
        Assertions.assertEquals(existingSchema, originalSchema != null);
        replay.replayAddBackendsPerBucketSeq(initialized.get(0));
        Assertions.assertEquals(initialized.get(0).getBackendsPerBucketSeq(), replay.getBackendsPerBucketSeq(groupId));
        replay.replayAddBackendsPerBucketSeq(sequenceUpdate);
        Assertions.assertEquals(newer, replay.getBackendsPerBucketSeq(groupId));
        replayDownload(logs.first, replayDb, replay);
        Assertions.assertSame(originalSchema, replay.getGroupSchema(groupId));
        replay.replayAddTableToGroup(logs.second);
        assertRestoredColocateState(colocateIndex, replay);
        if (existingSchema) {
            Assertions.assertSame(originalSchema, replay.getGroupSchema(groupId));
        }
        assertRestoredColocateState(colocateIndex, roundTripColocateIndex(replay));
    }

    @Test
    public void testRestoreMembershipReplayRecoversSequenceLostByCheckpoint() throws Exception {
        ColocateTableIndex colocateIndex = prepareRestoreColocateIndex();
        List<ColocatePersistInfo> initialized = resetColocateTableForRestore(colocateIndex);
        Assertions.assertEquals(1, initialized.size());
        GroupId groupId = colocateIndex.getGroup(expectedRestoreTbl.getId());
        ColocateTableIndex beforeCheckpoint = new ColocateTableIndex();
        beforeCheckpoint.replayAddBackendsPerBucketSeq(initialized.get(0));
        Assertions.assertEquals(initialized.get(0).getBackendsPerBucketSeq(),
                beforeCheckpoint.getBackendsPerBucketSeq(groupId));
        Assertions.assertNull(beforeCheckpoint.getGroupSchema(groupId));
        Assertions.assertTrue(beforeCheckpoint.getAllGroupIds().isEmpty());

        // An early P log has no group name; checkpoints save only named groups, so verify that P is actually lost.
        ColocateTableIndex replay = roundTripColocateIndex(beforeCheckpoint);
        Assertions.assertTrue(replay.getBackendsPerBucketSeq(groupId).isEmpty());
        Assertions.assertNull(replay.getGroupSchema(groupId));
        Assertions.assertTrue(replay.getAllGroupIds().isEmpty());
        Pair<RestoreJob, ColocatePersistInfo> logs = finishRestoreSnapshots();
        Assertions.assertEquals(initialized.get(0).getBackendsPerBucketSeq(), logs.second.getBackendsPerBucketSeq());
        Database replayDb = new Database(db.getId(), db.getFullName());
        replayDownload(logs.first, replayDb, replay);
        Assertions.assertFalse(replay.isColocateTable(expectedRestoreTbl.getId()));
        Assertions.assertTrue(replay.getBackendsPerBucketSeq(groupId).isEmpty());
        replay.replayAddTableToGroup(logs.second);
        assertRestoredColocateState(colocateIndex, replay);
        assertRestoredColocateState(colocateIndex, roundTripColocateIndex(replay));
    }

    @Test
    public void testRestoreMembershipReplayRecoversGroupAfterLastDurableMemberRemoved() throws Exception {
        ColocateTableIndex colocateIndex = prepareRestoreColocateIndex();
        OlapTable durableMember = newColocateMember();
        Assertions.assertTrue(db.registerTable(durableMember));
        String fullGroupName = GroupId.getFullGroupName(db.getId(), durableMember.getColocateGroup());
        GroupId groupId = colocateIndex.addTableToGroup(db.getId(), durableMember, fullGroupName, null);
        Map<Tag, List<List<Long>>> winner = Map.of(Tag.DEFAULT_BACKEND_TAG, List.of(List.of(71L, 72L, 73L)));
        colocateIndex.setBackendsPerBucketSeq(groupId, winner);
        ColocatePersistInfo durableAdd = roundTripColocateInfo(
                ColocatePersistInfo.createForAddTable(groupId, durableMember.getId(), winner));
        Database replayDb = new Database(db.getId(), db.getFullName());
        OlapTable replayDurableMember = newColocateMember();
        Assertions.assertNotSame(durableMember, replayDurableMember);
        Assertions.assertEquals(durableMember.getId(), replayDurableMember.getId());
        Assertions.assertTrue(replayDb.registerTable(replayDurableMember));
        Mockito.when(catalog.getDbOrMetaException(db.getId())).thenReturn(replayDb);
        ColocateTableIndex replay = new ColocateTableIndex();
        replay.replayAddTableToGroup(durableAdd);
        Assertions.assertEquals(winner, replay.getBackendsPerBucketSeq(groupId));

        Assertions.assertTrue(resetColocateTableForRestore(colocateIndex).isEmpty(),
                "Restore into an initialized group must reuse P without another sequence initialization");
        Assertions.assertEquals(groupId, colocateIndex.getGroup(expectedRestoreTbl.getId()));
        List<TablePropertyInfo> departures = Lists.newArrayList();
        EditLog.EditLogItem departureLog = Mockito.mock(EditLog.EditLogItem.class);
        Mockito.when(editLog.submitEdit(Mockito.eq(OperationType.OP_MODIFY_TABLE_COLOCATE),
                Mockito.any(TablePropertyInfo.class))).thenAnswer(invocation -> {
                    TablePropertyInfo info = invocation.getArgument(1);
                    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                    try (DataOutputStream out = new DataOutputStream(bytes)) {
                        info.write(out);
                    }
                    try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
                        departures.add(TablePropertyInfo.read(in));
                    }
                    return departureLog;
                });
        // B is already registered on the main side, so A leaving preserves P. Replay has not seen DOWNLOAD yet.
        EditLog.EditLogItem submittedDeparture;
        durableMember.writeLock();
        try {
            submittedDeparture = colocateIndex.modifyTableColocate(db.getId(), durableMember, "", false, null);
        } finally {
            durableMember.writeUnlock();
        }
        Assertions.assertSame(departureLog, submittedDeparture);
        submittedDeparture.await();
        Mockito.verify(departureLog).await();
        Assertions.assertFalse(colocateIndex.isColocateTable(durableMember.getId()));
        Assertions.assertNull(durableMember.getColocateGroup());
        Assertions.assertSame(durableMember, db.getTableNullable(durableMember.getId()));
        Assertions.assertEquals(winner, colocateIndex.getBackendsPerBucketSeq(groupId));
        Assertions.assertEquals(1, departures.size());
        TablePropertyInfo departure = departures.get(0);
        Assertions.assertEquals(db.getId(), departure.getDbId());
        Assertions.assertEquals(durableMember.getId(), departure.getTableId());
        Assertions.assertEquals(groupId, departure.getGroupId());
        Assertions.assertEquals(Map.of(PropertyAnalyzer.PROPERTIES_COLOCATE_WITH, ""), departure.getPropertyMap());
        Assertions.assertEquals("restore_winner", replayDurableMember.getColocateGroup());
        // Match the MODIFY dispatcher with the deserialized group ID and properties, without logging during replay.
        replayDurableMember.writeLock();
        try {
            Assertions.assertNull(replay.modifyTableColocate(replayDb.getId(), replayDurableMember,
                    departure.getPropertyMap().get(PropertyAnalyzer.PROPERTIES_COLOCATE_WITH),
                    true, departure.getGroupId()));
        } finally {
            replayDurableMember.writeUnlock();
        }
        Mockito.verify(editLog).submitEdit(Mockito.eq(OperationType.OP_MODIFY_TABLE_COLOCATE),
                Mockito.any(TablePropertyInfo.class));
        Assertions.assertFalse(replay.isColocateTable(replayDurableMember.getId()));
        Assertions.assertNull(replayDurableMember.getColocateGroup());
        Assertions.assertSame(replayDurableMember, replayDb.getTableNullable(departure.getTableId()));
        Assertions.assertNull(replay.getGroupSchema(groupId));
        Assertions.assertTrue(replay.getAllGroupIds().isEmpty());
        Assertions.assertTrue(replay.getBackendsPerBucketSeq(groupId).isEmpty());

        Pair<RestoreJob, ColocatePersistInfo> logs = finishRestoreSnapshots();
        Assertions.assertEquals(winner, logs.second.getBackendsPerBucketSeq());
        replayDownload(logs.first, replayDb, replay);
        Assertions.assertNull(replay.getGroupSchema(groupId));
        Assertions.assertTrue(replay.getBackendsPerBucketSeq(groupId).isEmpty());
        replay.replayAddTableToGroup(logs.second);
        assertRestoredColocateState(colocateIndex, replay);
        assertRestoredColocateState(colocateIndex, roundTripColocateIndex(replay));
    }

    private ColocateTableIndex prepareRestoreColocateIndex() {
        ColocateTableIndex colocateIndex = new ColocateTableIndex();
        mockedEnvStatic.when(Env::getCurrentEnv).thenReturn(env);
        mockedEnvStatic.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
        mockedEnvStatic.when(Env::getCurrentColocateIndex).thenReturn(colocateIndex);
        mockedEnvStatic.when(Env::getCurrentInvertedIndex).thenReturn(new LocalTabletInvertedIndex());
        expectedRestoreTbl.setColocateGroup("restore_winner");
        expectedRestoreTbl.getDefaultDistributionInfo().setBucketNum(1);
        return colocateIndex;
    }

    private OlapTable newColocateMember() throws UserException {
        OlapTable member = (OlapTable) CatalogMocker.mockDb().getTableNullable(CatalogMocker.TEST_TBL2_ID);
        member.setName("durable_colocate_member");
        member.setColocateGroup(expectedRestoreTbl.getColocateGroup());
        member.getDefaultDistributionInfo().setBucketNum(1);
        return member;
    }

    private List<ColocatePersistInfo> resetColocateTableForRestore(ColocateTableIndex colocateIndex) throws Exception {
        Map<Tag, List<List<Long>>> winner = Map.of(Tag.DEFAULT_BACKEND_TAG, List.of(List.of(71L, 72L, 73L)));
        boolean initializedGroup = !colocateIndex.getAllGroupIds().isEmpty();
        List<ColocatePersistInfo> initialized = Lists.newArrayList();
        Mockito.when(editLog.submitEdit(Mockito.eq(OperationType.OP_COLOCATE_BACKENDS_PER_BUCKETSEQ),
                Mockito.any(ColocatePersistInfo.class))).thenAnswer(invocation -> {
                    initialized.add(roundTripColocateInfo(invocation.getArgument(1)));
                    return Mockito.mock(EditLog.EditLogItem.class);
                });
        Mockito.doAnswer(invocation -> {
            // Another publisher installs P during replica selection; restore must use the winner, not the candidate.
            GroupId groupId = colocateIndex.getGroup(expectedRestoreTbl.getId());
            Assertions.assertTrue(colocateIndex.getOrInitializeBackendsPerBucketSeq(groupId, winner).second);
            return Pair.of(Map.of(Tag.DEFAULT_BACKEND_TAG, List.of(CatalogMocker.BACKEND1_ID,
                    CatalogMocker.BACKEND2_ID, CatalogMocker.BACKEND3_ID)), TStorageMedium.HDD);
        }).when(systemInfoService).selectBackendIdsForReplicaCreation(Mockito.any(ReplicaAllocation.class),
                Mockito.anyMap(), Mockito.isNull(), Mockito.eq(false), Mockito.eq(false));
        Status status = expectedRestoreTbl.resetIdsForRestore(env, db, new ReplicaAllocation((short) 3),
                true, true, job.getColocatePersistInfos(), db.getName());
        Assertions.assertTrue(status.ok(), status.toString());
        Assertions.assertEquals(winner, colocateIndex.getBackendsPerBucketSeq(
                colocateIndex.getGroup(expectedRestoreTbl.getId())));
        // Both partitions have base and rollup indexes; every real replica must follow the same winning sequence.
        int restoredIndexes = 0;
        for (Partition partition : expectedRestoreTbl.getPartitions()) {
            for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                restoredIndexes++;
                Assertions.assertEquals(1, index.getTablets().size());
                Tablet tablet = index.getTablets().get(0);
                Assertions.assertEquals(3, tablet.getReplicas().size());
                Assertions.assertEquals(Set.of(71L, 72L, 73L), tablet.getBackendIds());
            }
        }
        Assertions.assertEquals(4, restoredIndexes);
        Assertions.assertEquals(1, job.getColocatePersistInfos().size());
        Assertions.assertTrue(job.getColocatePersistInfos().get(0).getBackendsPerBucketSeq().isEmpty(),
                "Reset retains a descriptor, not the eventual ADD journal record");
        Assertions.assertEquals(initializedGroup ? 0 : 1, initialized.size());
        initialized.forEach(info -> Assertions.assertEquals(winner, info.getBackendsPerBucketSeq()));
        Mockito.verify(systemInfoService, Mockito.times(initializedGroup ? 0 : 1)).selectBackendIdsForReplicaCreation(
                Mockito.any(ReplicaAllocation.class), Mockito.anyMap(), Mockito.isNull(),
                Mockito.eq(false), Mockito.eq(false));
        Assertions.assertTrue(db.registerTable(expectedRestoreTbl));
        job.restoredTbls.add(expectedRestoreTbl);
        return initialized;
    }

    private Pair<RestoreJob, ColocatePersistInfo> finishRestoreSnapshots() throws Exception {
        List<RestoreJob> downloads = Lists.newArrayList();
        List<ColocatePersistInfo> additions = Lists.newArrayList();
        EditLog.EditLogItem addLog = Mockito.mock(EditLog.EditLogItem.class);
        Mockito.doAnswer(invocation -> {
            RestoreJob logged = invocation.getArgument(0);
            Assertions.assertEquals(RestoreJob.RestoreJobState.DOWNLOAD, logged.getState());
            Assertions.assertTrue(additions.isEmpty(), "DOWNLOAD must be durable before the colocate ADD");
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream out = new DataOutputStream(bytes)) {
                logged.write(out);
            }
            try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
                downloads.add((RestoreJob) AbstractJob.read(in));
            }
            return null;
        }).when(editLog).logRestoreJob(Mockito.any(RestoreJob.class));
        Mockito.when(editLog.submitEdit(Mockito.eq(OperationType.OP_COLOCATE_ADD_TABLE),
                Mockito.any(ColocatePersistInfo.class))).thenAnswer(invocation -> {
                    Assertions.assertEquals(1, downloads.size());
                    additions.add(roundTripColocateInfo(invocation.getArgument(1)));
                    return addLog;
                });
        job.repo = repo;
        job.state = RestoreJob.RestoreJobState.SNAPSHOTING;
        job.unfinishedSignatureToId.put(1L, 1L);
        job.run();
        Assertions.assertEquals(RestoreJob.RestoreJobState.SNAPSHOTING, job.getState());
        Assertions.assertTrue(downloads.isEmpty());
        Assertions.assertTrue(additions.isEmpty());
        // Run the real state machine from the final snapshot boundary instead of calling persistence helpers.
        job.unfinishedSignatureToId.clear();
        job.run();
        Assertions.assertTrue(job.getStatus().ok(), job.getStatus().toString());
        Assertions.assertEquals(RestoreJob.RestoreJobState.DOWNLOAD, job.getState());
        Assertions.assertEquals(1, downloads.size());
        Assertions.assertEquals(1, additions.size());
        Mockito.verify(addLog).await();
        Assertions.assertEquals(expectedRestoreTbl.getId(), additions.get(0).getTableId());
        Assertions.assertEquals(job.getColocatePersistInfos().get(0).getGroupId(), additions.get(0).getGroupId());
        Assertions.assertFalse(additions.get(0).getBackendsPerBucketSeq().isEmpty());
        return Pair.of(downloads.get(0), additions.get(0));
    }

    private void replayDownload(RestoreJob download, Database replayDb, ColocateTableIndex replay) throws Exception {
        Mockito.when(catalog.getDbOrMetaException(db.getId())).thenReturn(replayDb);
        mockedEnvStatic.when(Env::getCurrentColocateIndex).thenReturn(replay);
        TabletInvertedIndex invertedIndex = new LocalTabletInvertedIndex();
        mockedEnvStatic.when(Env::getCurrentInvertedIndex).thenReturn(invertedIndex);
        Assertions.assertNull(replayDb.getTableNullable(expectedRestoreTbl.getId()));
        Assertions.assertEquals(RestoreJob.RestoreJobState.DOWNLOAD, download.getState());
        download.setEnv(env);
        download.replayRun();
        OlapTable restored = (OlapTable) replayDb.getTableOrMetaException(expectedRestoreTbl.getId(), Table.TableType.OLAP);
        Assertions.assertNotSame(expectedRestoreTbl, restored, "Replay must register the deserialized table");
        Assertions.assertEquals(expectedRestoreTbl.getColocateGroup(), restored.getColocateGroup());
        int restoredIndexes = 0;
        for (Partition partition : restored.getPartitions()) {
            for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                restoredIndexes++;
                Assertions.assertEquals(1, index.getTablets().size());
                Tablet tablet = index.getTablets().get(0);
                Assertions.assertEquals(3, tablet.getReplicas().size());
                Assertions.assertEquals(Set.of(71L, 72L, 73L), tablet.getBackendIds());
                Assertions.assertEquals(restored.getId(), invertedIndex.getTabletMeta(tablet.getId()).getTableId());
                for (long backendId : tablet.getBackendIds()) {
                    Assertions.assertNotNull(invertedIndex.getReplica(tablet.getId(), backendId));
                }
            }
        }
        Assertions.assertEquals(4, restoredIndexes);
    }

    private void assertRestoredColocateState(ColocateTableIndex main, ColocateTableIndex replay) {
        GroupId groupId = main.getGroup(expectedRestoreTbl.getId());
        Assertions.assertEquals(groupId, replay.getGroup(expectedRestoreTbl.getId()));
        Assertions.assertEquals(main.getAllGroupIds(), replay.getAllGroupIds());
        Assertions.assertEquals(Set.copyOf(main.getAllTableIds(groupId)), Set.copyOf(replay.getAllTableIds(groupId)));
        Assertions.assertEquals(main.getBackendsPerBucketSeq(groupId), replay.getBackendsPerBucketSeq(groupId));
        ColocateGroupSchema schema = main.getGroupSchema(groupId);
        ColocateGroupSchema replaySchema = replay.getGroupSchema(groupId);
        Assertions.assertNotNull(replaySchema);
        Assertions.assertEquals(schema.getGroupId(), replaySchema.getGroupId());
        Assertions.assertEquals(schema.getBucketsNum(), replaySchema.getBucketsNum());
        Assertions.assertEquals(schema.getDistributionColTypes(), replaySchema.getDistributionColTypes());
        Assertions.assertEquals(schema.getReplicaAlloc(), replaySchema.getReplicaAlloc());
    }

    private static ColocateTableIndex roundTripColocateIndex(ColocateTableIndex index) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            index.write(out);
        }
        ColocateTableIndex restored = new ColocateTableIndex();
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            restored.readFields(in);
        }
        return restored;
    }

    private static ColocatePersistInfo roundTripColocateInfo(ColocatePersistInfo info) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            info.write(out);
        }
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            return ColocatePersistInfo.read(in);
        }
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
        Assertions.assertEquals(localPart.getVisibleVersion(), visibleVersion);
        Assertions.assertEquals(localPart.getNextVersion(), visibleVersion + 1);
    }
}
