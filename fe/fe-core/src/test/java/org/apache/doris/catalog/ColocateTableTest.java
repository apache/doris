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

import org.apache.doris.catalog.ColocateTableIndex.GroupId;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.lock.MonitoredReentrantReadWriteLock;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.AlterColocateGroupCommand;
import org.apache.doris.nereids.trees.plans.commands.AlterTableCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateDatabaseCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateTableCommand;
import org.apache.doris.nereids.trees.plans.commands.DropDatabaseCommand;
import org.apache.doris.nereids.trees.plans.commands.info.DropDatabaseInfo;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.persist.ColocatePersistInfo;
import org.apache.doris.persist.EditLog;
import org.apache.doris.persist.OperationType;
import org.apache.doris.persist.TablePropertyInfo;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.resource.Tag;
import org.apache.doris.system.Backend;
import org.apache.doris.utframe.UtFrameUtils;

import com.google.common.collect.Multimap;
import com.google.common.collect.Table;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class ColocateTableTest {
    private static String runningDir = "fe/mocked/ColocateTableTest" + UUID.randomUUID().toString() + "/";

    private static ConnectContext connectContext;
    private static String dbName = "testDb";
    private static String fullDbName = "" + dbName;
    private static String tableName1 = "t1";
    private static String tableName2 = "t2";
    private static String groupName = "group1";

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createDorisCluster(runningDir);
        connectContext = UtFrameUtils.createDefaultCtx();

    }

    @AfterAll
    public static void tearDown() {
        File file = new File(runningDir);
        file.delete();
    }

    @BeforeEach
    public void createDb() throws Exception {
        String createDbStmtStr = "create database " + dbName;
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan logicalPlan = nereidsParser.parseSingle(createDbStmtStr);
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, createDbStmtStr);
        if (logicalPlan instanceof CreateDatabaseCommand) {
            ((CreateDatabaseCommand) logicalPlan).run(connectContext, stmtExecutor);
        }
        Env.getCurrentEnv().setColocateTableIndex(new ColocateTableIndex());
    }

    @AfterEach
    public void dropDb() throws Exception {
        String dropDbStmtStr = "drop database " + dbName;
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan logicalPlan = nereidsParser.parseSingle(dropDbStmtStr);
        DropDatabaseCommand command = (DropDatabaseCommand) logicalPlan;
        DropDatabaseInfo dropDatabaseInfo = command.getDropDatabaseInfo();
        Env.getCurrentEnv().dropDb(
                dropDatabaseInfo.getCatalogName(),
                dropDatabaseInfo.getDatabaseName(),
                dropDatabaseInfo.isIfExists(),
                dropDatabaseInfo.isForce());
    }

    private static void createTable(String sql) throws Exception {
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan parsed = nereidsParser.parseSingle(sql);
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, sql);
        if (parsed instanceof CreateTableCommand) {
            ((CreateTableCommand) parsed).run(connectContext, stmtExecutor);
        }
    }

    private static void alterTable(String sql) throws Exception {
        alterTable(sql, connectContext);
    }

    private static void alterTable(String sql, ConnectContext context) throws Exception {
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan parsed = nereidsParser.parseSingle(sql);
        StmtExecutor stmtExecutor = new StmtExecutor(context, sql);
        if (parsed instanceof AlterTableCommand) {
            ((AlterTableCommand) parsed).run(context, stmtExecutor);
        }
    }

    private static void alterColocateGroup(String sql) throws Exception {
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan parsed = nereidsParser.parseSingle(sql);
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, sql);
        if (parsed instanceof AlterColocateGroupCommand) {
            ((AlterColocateGroupCommand) parsed).run(connectContext, stmtExecutor);
        } else {
            Assertions.fail("Expected AlterColocateGroupCommand, but parsed: " + parsed.getClass().getSimpleName());
        }
    }

    private static void createSingleReplicaColocateTable(String tableName) throws Exception {
        createTable("create table " + dbName + "." + tableName + " (\n"
                + " `k1` int NULL COMMENT \"\",\n"
                + " `k2` varchar(10) NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`k1`, `k2`)\n"
                + "COMMENT \"OLAP\"\n"
                + "DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + " \"replication_num\" = \"1\",\n"
                + " \"colocate_with\" = \"" + groupName + "\"\n"
                + ");");
    }

    private static Map<Tag, List<List<Long>>> copyBackendsPerBucketSeq(
            Map<Tag, List<List<Long>>> backendsPerBucketSeq) {
        Map<Tag, List<List<Long>>> copied = new HashMap<>();
        for (Map.Entry<Tag, List<List<Long>>> entry : backendsPerBucketSeq.entrySet()) {
            List<List<Long>> copiedBuckets = new ArrayList<>();
            for (List<Long> backends : entry.getValue()) {
                copiedBuckets.add(new ArrayList<>(backends));
            }
            copied.put(entry.getKey(), copiedBuckets);
        }
        return copied;
    }

    @Test
    public void testSameColocateGroupDoesNotAcquireIndexLock() throws Exception {
        createSingleReplicaColocateTable(tableName1);
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException(fullDbName);
        OlapTable table = (OlapTable) db.getTableOrMetaException(tableName1);
        checkNoOpDoesNotAcquireIndexLock(db.getId(), table, groupName);
    }

    @Test
    public void testClearEmptyColocateGroupDoesNotAcquireIndexLock() throws Exception {
        createTable("CREATE TABLE " + dbName + "." + tableName1
                + " (k1 INT) DISTRIBUTED BY HASH(k1) BUCKETS 1 PROPERTIES (\"replication_num\"=\"1\")");
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException(fullDbName);
        OlapTable table = (OlapTable) db.getTableOrMetaException(tableName1);
        checkNoOpDoesNotAcquireIndexLock(db.getId(), table, "");
        checkNoOpDoesNotAcquireIndexLock(db.getId(), table, null);
    }

    private void checkNoOpDoesNotAcquireIndexLock(long dbId, OlapTable table, String assignedGroup) throws Exception {
        ColocateTableIndex index = Env.getCurrentColocateIndex();
        MonitoredReentrantReadWriteLock lock = Deencapsulation.getField(index, "lock");
        String oldGroup = table.getColocateGroup();
        CountDownLatch tableLocked = new CountDownLatch(1);
        ExecutorService worker = Executors.newSingleThreadExecutor();
        Future<EditLog.EditLogItem> modification;
        lock.writeLock().lock();
        try {
            modification = worker.submit(() -> {
                table.writeLock();
                try {
                    tableLocked.countDown();
                    return index.modifyTableColocate(dbId, table, assignedGroup, false, null);
                } finally {
                    table.writeUnlock();
                }
            });
            Assertions.assertTrue(tableLocked.await(30, TimeUnit.SECONDS));
            // The worker must release the table lock while this thread still owns the index lock.
            Assertions.assertTrue(table.tryWriteLock(5, TimeUnit.SECONDS),
                    "No-op colocate change must not wait for the index lock while holding the table lock");
            try {
                Assertions.assertEquals(oldGroup, table.getColocateGroup());
            } finally {
                table.writeUnlock();
            }
        } finally {
            // Release the index lock before waiting, so a failing test cannot leave the worker deadlocked.
            lock.writeLock().unlock();
            worker.shutdown();
            Assertions.assertTrue(worker.awaitTermination(30, TimeUnit.SECONDS));
        }
        Assertions.assertNull(modification.get(30, TimeUnit.SECONDS));
    }

    @Test
    public void testConcurrentFirstJoinRejectsDifferentType() throws Exception {
        checkConcurrentFirstJoin(false);
    }

    @Test
    public void testConcurrentFirstJoinPreservesSequenceAndReplay() throws Exception {
        checkConcurrentFirstJoin(true);
    }

    private void checkConcurrentFirstJoin(boolean compatible) throws Exception {
        createTable("CREATE TABLE " + dbName + "." + tableName1
                + " (k1 INT) DISTRIBUTED BY HASH(k1) BUCKETS 1 PROPERTIES (\"replication_num\"=\"1\")");
        createTable("CREATE TABLE " + dbName + "." + tableName2
                + " (k1 " + (compatible ? "INT" : "BIGINT")
                + ") DISTRIBUTED BY HASH(k1) BUCKETS 1 PROPERTIES (\"replication_num\"=\"1\")");
        Env env = Env.getCurrentEnv();
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException(fullDbName);
        OlapTable first = (OlapTable) db.getTableOrMetaException(tableName1);
        OlapTable second = (OlapTable) db.getTableOrMetaException(tableName2);
        String originalFirstGroup = first.getColocateGroup();
        String originalSecondGroup = second.getColocateGroup();
        ColocateTableIndex index = Env.getCurrentColocateIndex();
        MonitoredReentrantReadWriteLock lock = Deencapsulation.getField(index, "lock");
        EditLog originalEditLog = env.getEditLog();
        EditLog journal = Mockito.mock(EditLog.class);
        EditLog.EditLogItem firstItem = Mockito.mock(EditLog.EditLogItem.class);
        EditLog.EditLogItem secondItem = Mockito.mock(EditLog.EditLogItem.class);
        Queue<TablePropertyInfo> entries = new ConcurrentLinkedQueue<>();
        CountDownLatch submitting = new CountDownLatch(1);
        CountDownLatch allowSubmit = new CountDownLatch(1);
        CountDownLatch awaiting = new CountDownLatch(1);
        CountDownLatch allowAwait = new CountDownLatch(1);
        CountDownLatch secondStarted = new CountDownLatch(1);
        AtomicReference<Thread> secondThread = new AtomicReference<>();
        ExecutorService workers = Executors.newFixedThreadPool(2);
        Backend backend = new Backend(env.getNextId(), "127.0.0.2", 9050);
        backend.setAlive(true);
        Tablet tablet = second.getPartitions().iterator().next().getBaseIndex().getTablets().get(0);
        Replica originalReplica = tablet.getReplicas().get(0);
        Env.getCurrentSystemInfo().addBackend(backend);
        try {
            // Use real replicas to produce different sequences so that both tables landing on the same BE
            // cannot mask the overwrite issue.
            tablet.deleteReplica(originalReplica);
            tablet.addReplica(new LocalReplica(env.getNextId(), backend.getId(), Replica.ReplicaState.NORMAL,
                    originalReplica.getVersion(), originalReplica.getSchemaHash()));
            Map<Tag, List<List<Long>>> firstSeq = copyBackendsPerBucketSeq(first.getArbitraryTabletBucketsSeq());
            Assertions.assertNotEquals(firstSeq, second.getArbitraryTabletBucketsSeq());
            Mockito.when(journal.submitEdit(Mockito.eq(OperationType.OP_MODIFY_TABLE_COLOCATE),
                    Mockito.any(TablePropertyInfo.class))).thenAnswer(invocation -> {
                        TablePropertyInfo info = invocation.getArgument(1);
                        Assertions.assertTrue(lock.isWriteLockedByCurrentThread());
                        if (info.getTableId() == first.getId()) {
                            submitting.countDown();
                            Assertions.assertTrue(allowSubmit.await(30, TimeUnit.SECONDS));
                        }
                        // Capture actual submissions and pass them through persistence serialization and
                        // deserialization instead of manually constructing logs for replay.
                        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                        try (DataOutputStream out = new DataOutputStream(bytes)) {
                            info.write(out);
                        }
                        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
                            entries.add(TablePropertyInfo.read(in));
                        }
                        return info.getTableId() == first.getId() ? firstItem : secondItem;
                    });
            Mockito.when(firstItem.await()).thenAnswer(invocation -> {
                awaiting.countDown();
                Assertions.assertTrue(allowAwait.await(30, TimeUnit.SECONDS));
                return 1L;
            });
            env.setEditLog(journal);
            Future<?> firstAlter = workers.submit(() -> {
                try {
                    alterTable("ALTER TABLE " + dbName + "." + tableName1
                            + " SET (\"colocate_with\"=\"" + groupName + "\")", UtFrameUtils.createDefaultCtx());
                    return null;
                } finally {
                    ConnectContext.remove();
                }
            });
            Assertions.assertTrue(submitting.await(30, TimeUnit.SECONDS));
            Future<?> secondAlter = workers.submit(() -> {
                try {
                    ConnectContext context = UtFrameUtils.createDefaultCtx();
                    secondThread.set(Thread.currentThread());
                    secondStarted.countDown();
                    alterTable("ALTER TABLE " + dbName + "." + tableName2
                            + " SET (\"colocate_with\"=\"" + groupName + "\")", context);
                    return null;
                } finally {
                    ConnectContext.remove();
                }
            });
            Assertions.assertTrue(secondStarted.await(30, TimeUnit.SECONDS));
            // Confirm the second ALTER is blocked on the same index lock rather than relying on sleep
            // to guess the execution order.
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (!lock.hasQueuedThread(secondThread.get()) && !secondAlter.isDone()
                    && System.nanoTime() < deadline) {
                Thread.yield();
            }
            Assertions.assertTrue(lock.hasQueuedThread(secondThread.get()));
            Assertions.assertFalse(secondAlter.isDone());
            Assertions.assertTrue(entries.isEmpty());
            allowSubmit.countDown();
            Assertions.assertTrue(awaiting.await(30, TimeUnit.SECONDS));
            if (compatible) {
                secondAlter.get(30, TimeUnit.SECONDS);
            } else {
                ExecutionException failure = Assertions.assertThrows(ExecutionException.class,
                        () -> secondAlter.get(30, TimeUnit.SECONDS));
                Assertions.assertInstanceOf(DdlException.class, failure.getCause());
                Assertions.assertTrue(failure.getCause().getMessage().contains("same data type"));
            }
            // The second ALTER finishes while the first log awaits persistence, proving that await
            // does not hold the index lock.
            Assertions.assertFalse(firstAlter.isDone());
            allowAwait.countDown();
            firstAlter.get(30, TimeUnit.SECONDS);
            GroupId groupId = index.getGroup(first.getId());
            Assertions.assertEquals(compatible ? 2 : 1, entries.size());
            Assertions.assertEquals(first.getId(), entries.peek().getTableId());
            Assertions.assertEquals(groupId, entries.peek().getGroupId());
            Assertions.assertEquals(firstSeq, index.getBackendsPerBucketSeq(groupId));
            Assertions.assertEquals(groupName, first.getColocateGroup());
            Assertions.assertEquals(compatible ? groupName : originalSecondGroup, second.getColocateGroup());
            Assertions.assertEquals(compatible, index.isColocateTable(second.getId()));
            List<Long> members = compatible ? List.of(first.getId(), second.getId()) : List.of(first.getId());
            Assertions.assertEquals(new HashSet<>(members), new HashSet<>(index.getAllTableIds(groupId)));

            ColocateTableIndex replay = new ColocateTableIndex();
            first.setColocateGroup(originalFirstGroup);
            second.setColocateGroup(originalSecondGroup);
            env.setColocateTableIndex(replay);
            for (TablePropertyInfo entry : entries) {
                env.replayModifyTableColocate(entry);
            }
            Assertions.assertEquals(index.getAllGroupIds(), replay.getAllGroupIds());
            Assertions.assertEquals(new HashSet<>(members), new HashSet<>(replay.getAllTableIds(groupId)));
            Assertions.assertEquals(groupId, replay.getGroup(first.getId()));
            Assertions.assertEquals(firstSeq, replay.getBackendsPerBucketSeq(groupId));
            Assertions.assertEquals(groupName, first.getColocateGroup());
            Assertions.assertEquals(compatible ? groupName : originalSecondGroup, second.getColocateGroup());
            Assertions.assertEquals(compatible, replay.isColocateTable(second.getId()));
            if (compatible) {
                Assertions.assertEquals(groupId, replay.getGroup(second.getId()));
            }
            Assertions.assertEquals(compatible ? 2 : 1, entries.size());
        } finally {
            allowSubmit.countDown();
            allowAwait.countDown();
            workers.shutdownNow();
            try {
                Assertions.assertTrue(workers.awaitTermination(30, TimeUnit.SECONDS));
            } finally {
                env.setEditLog(originalEditLog);
                env.setColocateTableIndex(index);
                first.setColocateGroup(index.isColocateTable(first.getId()) ? groupName : originalFirstGroup);
                second.setColocateGroup(index.isColocateTable(second.getId()) ? groupName : originalSecondGroup);
                tablet.deleteReplicaByBackendId(backend.getId());
                tablet.addReplica(originalReplica);
                Env.getCurrentSystemInfo().dropBackend(backend.getId());
            }
        }
    }

    @Test
    public void testCreateAfterAlterFirstJoinRejectsDifferentType() throws Exception {
        checkCreateAfterAlterFirstJoin(false);
    }

    @Test
    public void testCreateAfterAlterFirstJoinAcceptsSameType() throws Exception {
        checkCreateAfterAlterFirstJoin(true);
    }

    private void checkCreateAfterAlterFirstJoin(boolean compatible) throws Exception {
        createTable("CREATE TABLE " + dbName + "." + tableName2
                + " (k1 " + (compatible ? "INT" : "BIGINT")
                + ") DISTRIBUTED BY HASH(k1) BUCKETS 1 PROPERTIES (\"replication_num\"=\"1\")");
        Env env = Env.getCurrentEnv();
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException(fullDbName);
        OlapTable altered = (OlapTable) db.getTableOrMetaException(tableName2);
        Map<Tag, List<List<Long>>> expectedSeq = copyBackendsPerBucketSeq(altered.getArbitraryTabletBucketsSeq());
        ColocateTableIndex originalIndex = Env.getCurrentColocateIndex();
        ConnectContext originalContext = ConnectContext.get();
        CountDownLatch createChecked = new CountDownLatch(1);
        CountDownLatch alterFinished = new CountDownLatch(1);
        AtomicReference<OlapTable> attempted = new AtomicReference<>();
        AtomicReference<ColocateGroupSchema> alterSchema = new AtomicReference<>();
        ColocateTableIndex index = new ColocateTableIndex() {
            @Override
            public GroupId addTableToGroup(long dbId, OlapTable table, String fullGroupName, GroupId assigned)
                    throws DdlException {
                if (table.getName().equals(tableName1)) {
                    attempted.set(table);
                    Assertions.assertNull(getGroupSchema(fullGroupName));
                    createChecked.countDown();
                    try {
                        Assertions.assertTrue(alterFinished.await(30, TimeUnit.SECONDS));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new DdlException("Interrupted while waiting for ALTER", e);
                    }
                }
                return super.addTableToGroup(dbId, table, fullGroupName, assigned);
            }
        };
        ExecutorService worker = Executors.newSingleThreadExecutor();
        env.setColocateTableIndex(index);
        try {
            connectContext.setThreadLocalInfo();
            Future<?> alter = worker.submit(() -> {
                try {
                    Assertions.assertTrue(createChecked.await(30, TimeUnit.SECONDS));
                    alterTable("ALTER TABLE " + dbName + "." + tableName2
                            + " SET (\"colocate_with\"=\"" + groupName + "\")", UtFrameUtils.createDefaultCtx());
                    alterSchema.set(index.getGroupSchema(index.getGroup(altered.getId())));
                    return null;
                } finally {
                    ConnectContext.remove();
                    alterFinished.countDown();
                }
            });
            // CREATE has observed no group; ALTER establishes it before CREATE's membership insertion.
            String sql = "CREATE TABLE " + dbName + "." + tableName1
                    + " (k1 INT) DISTRIBUTED BY HASH(k1) BUCKETS 1"
                    + " PROPERTIES (\"replication_num\"=\"1\", \"colocate_with\"=\"" + groupName + "\")";
            if (compatible) {
                createTable(sql);
            } else {
                DdlException failure = Assertions.assertThrows(DdlException.class, () -> createTable(sql));
                Assertions.assertTrue(failure.getMessage().contains("same data type"));
                Assertions.assertNull(db.getTableNullable(tableName1));
            }
            alter.get(30, TimeUnit.SECONDS);
            Assertions.assertNotNull(attempted.get());
            GroupId groupId = index.getGroup(altered.getId());
            Assertions.assertNotNull(groupId);
            Assertions.assertEquals(groupName, altered.getColocateGroup());
            Assertions.assertEquals(compatible, index.isColocateTable(attempted.get().getId()));
            List<Long> members = compatible ? List.of(altered.getId(), attempted.get().getId())
                    : List.of(altered.getId());
            Assertions.assertEquals(new HashSet<>(members), new HashSet<>(index.getAllTableIds(groupId)));
            Assertions.assertEquals(1, index.getAllGroupIds().size());
            Assertions.assertSame(alterSchema.get(), index.getGroupSchema(groupId));
            Assertions.assertEquals(List.of(compatible ? Type.INT : Type.BIGINT),
                    index.getGroupSchema(groupId).getDistributionColTypes());
            Assertions.assertEquals(expectedSeq, index.getBackendsPerBucketSeq(groupId));
            if (compatible) {
                Assertions.assertSame(attempted.get(), db.getTableOrMetaException(tableName1));
                Assertions.assertEquals(groupId, index.getGroup(attempted.get().getId()));
            }
        } finally {
            createChecked.countDown();
            alterFinished.countDown();
            worker.shutdown();
            try {
                Assertions.assertTrue(worker.awaitTermination(30, TimeUnit.SECONDS));
            } finally {
                env.setColocateTableIndex(originalIndex);
                ConnectContext.remove();
                if (originalContext != null) {
                    originalContext.setThreadLocalInfo();
                }
            }
        }
    }

    @Test
    public void testReplayAddTablePreservesHistoricalIncompatibleMembership() throws Exception {
        createSingleReplicaColocateTable(tableName1);
        createTable("CREATE TABLE " + dbName + "." + tableName2
                + " (k1 BIGINT, k2 VARCHAR(10)) DISTRIBUTED BY HASH(k1, k2) BUCKETS 1"
                + " PROPERTIES (\"replication_num\"=\"1\")");
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException(fullDbName);
        OlapTable first = (OlapTable) db.getTableOrMetaException(tableName1);
        OlapTable second = (OlapTable) db.getTableOrMetaException(tableName2);
        GroupId groupId = Env.getCurrentColocateIndex().getGroup(first.getId());
        Map<Tag, List<List<Long>>> seq = copyBackendsPerBucketSeq(first.getArbitraryTabletBucketsSeq());
        ColocateTableIndex replay = new ColocateTableIndex();
        String originalGroup = second.getColocateGroup();
        try {
            // Model historical metadata without writing an incompatible membership to the live journal.
            second.setColocateGroup(groupName);
            replay.replayAddTableToGroup(ColocatePersistInfo.createForAddTable(groupId, first.getId(), seq));
            ColocateGroupSchema schema = replay.getGroupSchema(groupId);
            Assertions.assertThrows(DdlException.class, () -> schema.checkColocateSchema(second));
            replay.replayAddTableToGroup(ColocatePersistInfo.createForAddTable(groupId, second.getId(), seq));
            Assertions.assertEquals(new HashSet<>(List.of(first.getId(), second.getId())),
                    new HashSet<>(replay.getAllTableIds(groupId)));
            Assertions.assertEquals(groupId, replay.getGroup(second.getId()));
            Assertions.assertSame(schema, replay.getGroupSchema(groupId));
            Assertions.assertEquals(seq, replay.getBackendsPerBucketSeq(groupId));
        } finally {
            second.setColocateGroup(originalGroup);
        }
    }

    @Test
    public void testRejectedMovePreservesOldGroup() throws Exception {
        createSingleReplicaColocateTable(tableName1);
        createTable("CREATE TABLE " + dbName + "." + tableName2
                + " (k1 BIGINT, k2 VARCHAR(10)) DISTRIBUTED BY HASH(k1, k2) BUCKETS 1"
                + " PROPERTIES (\"replication_num\"=\"1\", \"colocate_with\"=\"incompatible\")");
        Env env = Env.getCurrentEnv();
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException(fullDbName);
        OlapTable table = (OlapTable) db.getTableOrMetaException(tableName1);
        ColocateTableIndex index = Env.getCurrentColocateIndex();
        GroupId oldGroupId = index.getGroup(table.getId());
        ColocateGroupSchema oldSchema = index.getGroupSchema(oldGroupId);
        Map<Tag, List<List<Long>>> oldSeq = copyBackendsPerBucketSeq(index.getBackendsPerBucketSeq(oldGroupId));
        Replica replica = table.getPartitions().iterator().next().getBaseIndex().getTablets().get(0)
                .getReplicas().get(0);
        EditLog originalEditLog = env.getEditLog();
        EditLog journal = Mockito.mock(EditLog.class);
        Queue<TablePropertyInfo> entries = new ConcurrentLinkedQueue<>();
        Mockito.when(journal.submitEdit(Mockito.eq(OperationType.OP_MODIFY_TABLE_COLOCATE),
                Mockito.any(TablePropertyInfo.class))).thenAnswer(invocation -> {
                    entries.add(invocation.getArgument(1));
                    return Mockito.mock(EditLog.EditLogItem.class);
                });
        env.setEditLog(journal);
        try {
            for (String target : List.of("incompatible", "new_group")) {
                // A bad replica causes actual sequence extraction to fail, verifying that extraction happens
                // before leaving the old group.
                replica.setBad(target.equals("new_group"));
                DdlException failure = Assertions.assertThrows(DdlException.class,
                        () -> alterTable("ALTER TABLE " + dbName + "." + tableName1
                                + " SET (\"colocate_with\"=\"" + target + "\")"));
                Assertions.assertTrue(failure.getMessage().contains(target.equals("new_group")
                        ? "Normal replica number" : "same data type"));
                Assertions.assertEquals(groupName, table.getColocateGroup());
                Assertions.assertEquals(oldGroupId, index.getGroup(table.getId()));
                Assertions.assertEquals(List.of(table.getId()), index.getAllTableIds(oldGroupId));
                Assertions.assertSame(oldSchema, index.getGroupSchema(oldGroupId));
                Assertions.assertEquals(oldSeq, index.getBackendsPerBucketSeq(oldGroupId));
                Assertions.assertEquals(2, index.getAllGroupIds().size());
                Assertions.assertNull(index.getGroupSchema(GroupId.getFullGroupName(db.getId(), "new_group")));
                Assertions.assertTrue(entries.isEmpty());
            }
        } finally {
            replica.setBad(false);
            env.setEditLog(originalEditLog);
        }
    }

    @Test
    public void testCreateOneTable() throws Exception {
        createTable("create table " + dbName + "." + tableName1 + " (\n"
                + " `k1` int NULL COMMENT \"\",\n"
                + " `k2` varchar(10) NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`k1`, `k2`)\n"
                + "COMMENT \"OLAP\"\n"
                + "DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + " \"replication_num\" = \"1\",\n"
                + " \"colocate_with\" = \"" + groupName + "\"\n"
                + ");");

        ColocateTableIndex index = Env.getCurrentColocateIndex();
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException(fullDbName);
        long tableId = db.getTableOrMetaException(tableName1).getId();

        Assertions.assertEquals(1, Deencapsulation.<Multimap<GroupId, Long>>getField(index, "group2Tables").size());
        Assertions.assertEquals(1, index.getAllGroupIds().size());
        Assertions.assertEquals(1, Deencapsulation.<Map<Long, GroupId>>getField(index, "table2Group").size());
        Assertions.assertEquals(1, Deencapsulation.<Table<GroupId, Tag, List<List<Long>>>>getField(index, "group2BackendsPerBucketSeq").size());
        Assertions.assertEquals(1, Deencapsulation.<Map<GroupId, ColocateGroupSchema>>getField(index, "group2Schema").size());
        Assertions.assertEquals(0, index.getUnstableGroupIds().size());

        Assertions.assertTrue(index.isColocateTable(tableId));

        Long dbId = db.getId();
        Assertions.assertEquals(dbId, index.getGroup(tableId).dbId);

        GroupId groupId = index.getGroup(tableId);
        Map<Tag, List<List<Long>>> backendIds = index.getBackendsPerBucketSeq(groupId);
        Assertions.assertEquals(1, backendIds.get(Tag.DEFAULT_BACKEND_TAG).get(0).size());

        String fullGroupName = GroupId.getFullGroupName(dbId, groupName);
        Assertions.assertEquals(tableId, index.getTableIdByGroup(fullGroupName));
        ColocateGroupSchema groupSchema = index.getGroupSchema(fullGroupName);
        Assertions.assertNotNull(groupSchema);
        Assertions.assertEquals(dbId, groupSchema.getGroupId().dbId);
        Assertions.assertEquals(1, groupSchema.getBucketsNum());
        Assertions.assertEquals((short) 1, groupSchema.getReplicaAlloc().getTotalReplicaNum());
    }

    @Test
    public void testAlterColocateGroupReplicaAllocationLogsEditLog() throws Exception {
        createSingleReplicaColocateTable(tableName1);

        Env env = Env.getCurrentEnv();
        EditLog originalEditLog = env.getEditLog();
        EditLog mockEditLog = Mockito.mock(EditLog.class);
        env.setEditLog(mockEditLog);
        try {
            alterColocateGroup("ALTER COLOCATE GROUP " + dbName + "." + groupName
                    + " SET (\"replication_num\" = \"1\")");

            Mockito.verify(mockEditLog, Mockito.times(1))
                    .logColocateModifyRepliaAlloc(Mockito.any(ColocatePersistInfo.class));

            ColocateTableIndex index = Env.getCurrentColocateIndex();
            Database db = Env.getCurrentInternalCatalog().getDbOrMetaException(fullDbName);
            String fullGroupName = GroupId.getFullGroupName(db.getId(), groupName);
            Assertions.assertEquals((short) 1,
                    index.getGroupSchema(fullGroupName).getReplicaAlloc().getTotalReplicaNum());
        } finally {
            env.setEditLog(originalEditLog);
        }
    }

    @Test
    public void testReplayModifyReplicaAllocationDoesNotLogEditLog() throws Exception {
        createSingleReplicaColocateTable(tableName1);

        ColocateTableIndex index = Env.getCurrentColocateIndex();
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException(fullDbName);
        long tableId = db.getTableOrMetaException(tableName1).getId();
        GroupId groupId = index.getGroup(tableId);
        ColocatePersistInfo info = ColocatePersistInfo.createForModifyReplicaAlloc(
                groupId, new ReplicaAllocation((short) 1),
                copyBackendsPerBucketSeq(index.getBackendsPerBucketSeq(groupId)));

        Env env = Env.getCurrentEnv();
        EditLog originalEditLog = env.getEditLog();
        EditLog mockEditLog = Mockito.mock(EditLog.class);
        env.setEditLog(mockEditLog);
        try {
            index.replayModifyReplicaAlloc(info);

            Mockito.verify(mockEditLog, Mockito.never())
                    .logColocateModifyRepliaAlloc(Mockito.any(ColocatePersistInfo.class));
        } finally {
            env.setEditLog(originalEditLog);
        }
    }

    @Test
    public void testCreateTwoTableWithSameGroup() throws Exception {
        createTable("create table " + dbName + "." + tableName1 + " (\n"
                + " `k1` int NULL COMMENT \"\",\n"
                + " `k2` varchar(10) NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`k1`, `k2`)\n"
                + "COMMENT \"OLAP\"\n"
                + "DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + " \"replication_num\" = \"1\",\n"
                + " \"colocate_with\" = \"" + groupName + "\"\n"
                + ");");

        createTable("create table " + dbName + "." + tableName2 + " (\n"
                + " `k1` int NULL COMMENT \"\",\n"
                + " `k2` varchar(10) NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`k1`, `k2`)\n"
                + "COMMENT \"OLAP\"\n"
                + "DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + " \"replication_num\" = \"1\",\n"
                + " \"colocate_with\" = \"" + groupName + "\"\n"
                + ");");

        ColocateTableIndex index = Env.getCurrentColocateIndex();
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException(fullDbName);
        long firstTblId = db.getTableOrMetaException(tableName1).getId();
        long secondTblId = db.getTableOrMetaException(tableName2).getId();

        Assertions.assertEquals(2, Deencapsulation.<Multimap<GroupId, Long>>getField(index, "group2Tables").size());
        Assertions.assertEquals(1, index.getAllGroupIds().size());
        Assertions.assertEquals(2, Deencapsulation.<Map<Long, GroupId>>getField(index, "table2Group").size());
        Assertions.assertEquals(1, Deencapsulation.<Table<GroupId, Tag, List<List<Long>>>>getField(index, "group2BackendsPerBucketSeq").size());
        Assertions.assertEquals(1, Deencapsulation.<Map<GroupId, ColocateGroupSchema>>getField(index, "group2Schema").size());
        Assertions.assertEquals(0, index.getUnstableGroupIds().size());

        Assertions.assertTrue(index.isColocateTable(firstTblId));
        Assertions.assertTrue(index.isColocateTable(secondTblId));

        Assertions.assertTrue(index.isSameGroup(firstTblId, secondTblId));

        // drop first
        index.removeTable(firstTblId);
        Assertions.assertEquals(1, Deencapsulation.<Multimap<GroupId, Long>>getField(index, "group2Tables").size());
        Assertions.assertEquals(1, index.getAllGroupIds().size());
        Assertions.assertEquals(1, Deencapsulation.<Map<Long, GroupId>>getField(index, "table2Group").size());
        Assertions.assertEquals(1,
                Deencapsulation.<Table<GroupId, Tag, List<List<Long>>>>getField(index, "group2BackendsPerBucketSeq").size());
        Assertions.assertEquals(0, index.getUnstableGroupIds().size());

        Assertions.assertFalse(index.isColocateTable(firstTblId));
        Assertions.assertTrue(index.isColocateTable(secondTblId));
        Assertions.assertFalse(index.isSameGroup(firstTblId, secondTblId));

        // drop second
        index.removeTable(secondTblId);
        Assertions.assertEquals(0, Deencapsulation.<Multimap<GroupId, Long>>getField(index, "group2Tables").size());
        Assertions.assertEquals(0, index.getAllGroupIds().size());
        Assertions.assertEquals(0, Deencapsulation.<Map<Long, GroupId>>getField(index, "table2Group").size());
        Assertions.assertEquals(0,
                Deencapsulation.<Table<GroupId, Tag, List<List<Long>>>>getField(index, "group2BackendsPerBucketSeq").size());
        Assertions.assertEquals(0, index.getUnstableGroupIds().size());

        Assertions.assertFalse(index.isColocateTable(firstTblId));
        Assertions.assertFalse(index.isColocateTable(secondTblId));
    }

    @Test
    public void testBucketNum() throws Exception {
        createTable("create table " + dbName + "." + tableName1 + " (\n"
                + " `k1` int NULL COMMENT \"\",\n"
                + " `k2` varchar(10) NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`k1`, `k2`)\n"
                + "COMMENT \"OLAP\"\n"
                + "DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + " \"replication_num\" = \"1\",\n"
                + " \"colocate_with\" = \"" + groupName + "\"\n"
                + ");");

        DdlException e = Assertions.assertThrows(DdlException.class, () -> {
            createTable("create table " + dbName + "." + tableName2 + " (\n"
                    + " `k1` int NULL COMMENT \"\",\n"
                    + " `k2` varchar(10) NULL COMMENT \"\"\n"
                    + ") ENGINE=OLAP\n"
                    + "DUPLICATE KEY(`k1`, `k2`)\n"
                    + "COMMENT \"OLAP\"\n"
                    + "DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 2\n"
                    + "PROPERTIES (\n"
                    + " \"replication_num\" = \"1\",\n"
                    + " \"colocate_with\" = \"" + groupName + "\"\n"
                    + ");");
        });
        Assertions.assertTrue(e.getMessage().contains("Colocate tables must have same bucket num: 2 should be 1"),
                "unexpected message: " + e.getMessage());
    }

    @Test
    public void testReplicationNum() throws Exception {
        createTable("create table " + dbName + "." + tableName1 + " (\n"
                + " `k1` int NULL COMMENT \"\",\n"
                + " `k2` varchar(10) NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`k1`, `k2`)\n"
                + "COMMENT \"OLAP\"\n"
                + "DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + " \"replication_num\" = \"1\",\n"
                + " \"colocate_with\" = \"" + groupName + "\"\n"
                + ");");

        DdlException e = Assertions.assertThrows(DdlException.class, () -> {
            createTable("create table " + dbName + "." + tableName2 + " (\n"
                    + " `k1` int NULL COMMENT \"\",\n"
                    + " `k2` varchar(10) NULL COMMENT \"\"\n"
                    + ") ENGINE=OLAP\n"
                    + "DUPLICATE KEY(`k1`, `k2`)\n"
                    + "COMMENT \"OLAP\"\n"
                    + "DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 1\n"
                    + "PROPERTIES (\n"
                    + " \"replication_num\" = \"2\",\n"
                    + " \"colocate_with\" = \"" + groupName + "\"\n"
                    + ");");
        });
        Assertions.assertTrue(e.getMessage().contains("Colocate tables must have same replication allocation: { tag.location.default: 2 }"
                + " should be { tag.location.default: 1 }"),
                "unexpected message: " + e.getMessage());
    }

    @Test
    public void testDistributionColumnsSize() throws Exception {
        createTable("create table " + dbName + "." + tableName1 + " (\n"
                + " `k1` int NULL COMMENT \"\",\n"
                + " `k2` varchar(10) NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`k1`, `k2`)\n"
                + "COMMENT \"OLAP\"\n"
                + "DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + " \"replication_num\" = \"1\",\n"
                + " \"colocate_with\" = \"" + groupName + "\"\n"
                + ");");

        DdlException e = Assertions.assertThrows(DdlException.class, () -> {
            createTable("create table " + dbName + "." + tableName2 + " (\n"
                    + " `k1` int NULL COMMENT \"\",\n"
                    + " `k2` varchar(10) NULL COMMENT \"\"\n"
                    + ") ENGINE=OLAP\n"
                    + "DUPLICATE KEY(`k1`, `k2`)\n"
                    + "COMMENT \"OLAP\"\n"
                    + "DISTRIBUTED BY HASH(`k1`) BUCKETS 1\n"
                    + "PROPERTIES (\n"
                    + " \"replication_num\" = \"1\",\n"
                    + " \"colocate_with\" = \"" + groupName + "\"\n"
                    + ");");
        });
        Assertions.assertTrue(e.getMessage().contains("Colocate tables distribution columns size must be same: 1 should be 2"),
                "unexpected message: " + e.getMessage());
    }

    @Test
    public void testDistributionColumnsType() throws Exception {
        createTable("create table " + dbName + "." + tableName1 + " (\n"
                + " `k1` int NULL COMMENT \"\",\n"
                + " `k2` int NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`k1`, `k2`)\n"
                + "COMMENT \"OLAP\"\n"
                + "DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + " \"replication_num\" = \"1\",\n"
                + " \"colocate_with\" = \"" + groupName + "\"\n"
                + ");");

        DdlException e = Assertions.assertThrows(DdlException.class, () -> {
            createTable("create table " + dbName + "." + tableName2 + " (\n"
                    + " `k1` int NULL COMMENT \"\",\n"
                    + " `k2` varchar(10) NULL COMMENT \"\"\n"
                    + ") ENGINE=OLAP\n"
                    + "DUPLICATE KEY(`k1`, `k2`)\n"
                    + "COMMENT \"OLAP\"\n"
                    + "DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 1\n"
                    + "PROPERTIES (\n"
                    + " \"replication_num\" = \"1\",\n"
                    + " \"colocate_with\" = \"" + groupName + "\"\n"
                    + ");");
        });
        Assertions.assertTrue(e.getMessage().contains("Colocate tables distribution columns must have the same data type: k2(varchar(10)) should be int"),
                "unexpected message: " + e.getMessage());
    }


    @Test
    public void testModifyGroupNameForBucketSeqInconsistent() throws Exception {
        createTable("create table " + dbName + "." + tableName1 + " (\n"
                + " `k1` int NULL COMMENT \"\",\n"
                + " `k2` varchar(10) NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`k1`, `k2`)\n"
                + "COMMENT \"OLAP\"\n"
                + "DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + " \"replication_num\" = \"1\",\n"
                + " \"colocate_with\" = \"" + groupName + "\"\n"
                + ");");

        ColocateTableIndex index = Env.getCurrentColocateIndex();
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException(fullDbName);
        long tableId = db.getTableOrMetaException(tableName1).getId();
        GroupId groupId1 = index.getGroup(tableId);

        Map<Tag, List<List<Long>>> backendIds1 = index.getBackendsPerBucketSeq(groupId1);
        Assertions.assertEquals(1, backendIds1.get(Tag.DEFAULT_BACKEND_TAG).get(0).size());

        // set same group name
        alterTable("ALTER TABLE " + dbName + "." + tableName1
                + " SET (" + "\"colocate_with\" = \"" + groupName + "\")");
        GroupId groupId2 = index.getGroup(tableId);

        // verify groupId group2BackendsPerBucketSeq
        Map<Tag, List<List<Long>>> backendIds2 = index.getBackendsPerBucketSeq(groupId2);
        Assertions.assertEquals(1, backendIds2.get(Tag.DEFAULT_BACKEND_TAG).get(0).size());
        Assertions.assertEquals(groupId1, groupId2);
        Assertions.assertEquals(backendIds1, backendIds2);
    }
}
