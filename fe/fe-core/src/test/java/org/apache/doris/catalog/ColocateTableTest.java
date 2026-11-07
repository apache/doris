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
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;
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
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

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
        UtFrameUtils.createDorisCluster(runningDir, 3);
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
        alterColocateGroup(sql, connectContext);
    }

    private static void alterColocateGroup(String sql, ConnectContext context) throws Exception {
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan parsed = nereidsParser.parseSingle(sql);
        StmtExecutor stmtExecutor = new StmtExecutor(context, sql);
        if (parsed instanceof AlterColocateGroupCommand) {
            ((AlterColocateGroupCommand) parsed).run(context, stmtExecutor);
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
    public void testAlterInitializesSequenceAfterCreateRegistersThenFails() throws Exception {
        createTable("CREATE TABLE " + dbName + "." + tableName2
                + " (k1 INT) DISTRIBUTED BY HASH(k1) BUCKETS 1 PROPERTIES (\"replication_num\"=\"1\")");
        Env env = Env.getCurrentEnv();
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException(fullDbName);
        OlapTable altered = (OlapTable) db.getTableOrMetaException(tableName2);
        String originalGroup = altered.getColocateGroup();
        Map<Tag, List<List<Long>>> expected = copyBackendsPerBucketSeq(altered.getArbitraryTabletBucketsSeq());
        ColocateTableIndex originalIndex = Env.getCurrentColocateIndex();
        EditLog originalEditLog = env.getEditLog();
        ConnectContext originalContext = ConnectContext.get();
        long originalQuota = db.getReplicaQuota();
        CountDownLatch registered = new CountDownLatch(1);
        CountDownLatch alteredGroup = new CountDownLatch(1);
        AtomicReference<OlapTable> attempted = new AtomicReference<>();
        AtomicReference<TablePropertyInfo> recorded = new AtomicReference<>();
        ColocateTableIndex index = new ColocateTableIndex() {
            @Override
            public GroupId addTableToGroup(long dbId, OlapTable table, String fullGroupName, GroupId assigned)
                    throws DdlException {
                GroupId groupId = super.addTableToGroup(dbId, table, fullGroupName, assigned);
                if (table.getName().equals(tableName1)) {
                    attempted.set(table);
                    Assertions.assertNotNull(getGroupSchema(groupId));
                    Assertions.assertTrue(getBackendsPerBucketSeq(groupId).isEmpty());
                    registered.countDown();
                    try {
                        Assertions.assertTrue(alteredGroup.await(30, TimeUnit.SECONDS));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new DdlException("Interrupted while waiting for ALTER", e);
                    }
                }
                return groupId;
            }
        };
        EditLog journal = Mockito.mock(EditLog.class);
        Mockito.when(journal.submitEdit(Mockito.eq(OperationType.OP_MODIFY_TABLE_COLOCATE),
                Mockito.any(TablePropertyInfo.class))).thenAnswer(invocation -> {
                    TablePropertyInfo info = invocation.getArgument(1);
                    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                    try (DataOutputStream out = new DataOutputStream(bytes)) {
                        info.write(out);
                    }
                    try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
                        recorded.set(TablePropertyInfo.read(in));
                    }
                    return Mockito.mock(EditLog.EditLogItem.class);
                });
        ExecutorService worker = Executors.newSingleThreadExecutor();
        env.setColocateTableIndex(index);
        env.setEditLog(journal);
        try {
            // Initial quota validation succeeds; the post-registration replica-count check fails.
            db.setReplicaQuota(db.getReplicaCount() + 1);
            Assertions.assertEquals(1, db.getReplicaQuotaLeftWithLock());
            connectContext.setThreadLocalInfo();
            Future<?> alter = worker.submit(() -> {
                try {
                    Assertions.assertTrue(registered.await(30, TimeUnit.SECONDS));
                    alterTable("ALTER TABLE " + dbName + "." + tableName2
                            + " SET (\"colocate_with\"=\"" + groupName + "\")", UtFrameUtils.createDefaultCtx());
                    return null;
                } finally {
                    ConnectContext.remove();
                    alteredGroup.countDown();
                }
            });
            DdlException failure = Assertions.assertThrows(DdlException.class, () -> createTable(
                    "CREATE TABLE " + dbName + "." + tableName1
                            + " (k1 INT) DISTRIBUTED BY HASH(k1) BUCKETS 1"
                            + " PROPERTIES (\"replication_num\"=\"1\", \"colocate_with\"=\"" + groupName + "\")"));
            Assertions.assertTrue(failure.getMessage().contains("increasing 1 of replica exceeds quota"));
            alter.get(30, TimeUnit.SECONDS);
            GroupId groupId = index.getGroup(altered.getId());
            Assertions.assertNotNull(attempted.get());
            Assertions.assertNull(db.getTableNullable(tableName1));
            Assertions.assertFalse(index.isColocateTable(attempted.get().getId()));
            Assertions.assertEquals(List.of(altered.getId()), index.getAllTableIds(groupId));
            Assertions.assertFalse(expected.isEmpty());
            Assertions.assertEquals(expected, index.getBackendsPerBucketSeq(groupId));
            Assertions.assertNotNull(recorded.get());
            Assertions.assertEquals(altered.getId(), recorded.get().getTableId());
            ColocateTableIndex replay = new ColocateTableIndex();
            altered.setColocateGroup(originalGroup);
            env.setColocateTableIndex(replay);
            env.replayModifyTableColocate(recorded.get());
            Assertions.assertEquals(groupId, replay.getGroup(altered.getId()));
            Assertions.assertEquals(List.of(altered.getId()), replay.getAllTableIds(groupId));
            Assertions.assertFalse(replay.isColocateTable(attempted.get().getId()));
            Assertions.assertEquals(expected, replay.getBackendsPerBucketSeq(groupId));
        } finally {
            registered.countDown();
            alteredGroup.countDown();
            worker.shutdown();
            try {
                Assertions.assertTrue(worker.awaitTermination(30, TimeUnit.SECONDS));
            } finally {
                db.setReplicaQuota(originalQuota);
                env.setEditLog(originalEditLog);
                env.setColocateTableIndex(originalIndex);
                ConnectContext.remove();
                if (originalContext != null) {
                    originalContext.setThreadLocalInfo();
                }
            }
        }
    }

    @Test
    public void testCreateBuildsReplicasUsingAlterWinnerInsteadOfCandidate() throws Exception {
        createTable("CREATE TABLE " + dbName + "." + tableName2
                + " (k1 INT) DISTRIBUTED BY HASH(k1) BUCKETS 1 PROPERTIES (\"replication_num\"=\"1\")");
        Env env = Env.getCurrentEnv();
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException(fullDbName);
        OlapTable altered = (OlapTable) db.getTableOrMetaException(tableName2);
        ColocateTableIndex originalIndex = Env.getCurrentColocateIndex();
        ConnectContext originalContext = ConnectContext.get();
        CountDownLatch selected = new CountDownLatch(1);
        CountDownLatch published = new CountDownLatch(1);
        AtomicReference<Map<Tag, List<List<Long>>>> candidateSeq = new AtomicReference<>();
        AtomicReference<Map<Tag, List<List<Long>>>> winnerSeq = new AtomicReference<>();
        ColocateTableIndex index = new ColocateTableIndex() {
            @Override
            public Pair<Map<Tag, List<List<Long>>>, Boolean> getOrInitializeBackendsPerBucketSeq(
                    GroupId groupId, Map<Tag, List<List<Long>>> candidate) {
                if (!candidate.isEmpty()) {
                    candidateSeq.set(copyBackendsPerBucketSeq(candidate));
                    selected.countDown();
                    try {
                        Assertions.assertTrue(published.await(30, TimeUnit.SECONDS));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new AssertionError(e);
                    }
                }
                return super.getOrInitializeBackendsPerBucketSeq(groupId, candidate);
            }
        };
        ExecutorService worker = Executors.newSingleThreadExecutor();
        env.setColocateTableIndex(index);
        try {
            connectContext.setThreadLocalInfo();
            Future<?> alter = worker.submit(() -> {
                try {
                    Assertions.assertTrue(selected.await(30, TimeUnit.SECONDS));
                    long candidateBackend = candidateSeq.get().get(Tag.DEFAULT_BACKEND_TAG).get(0).get(0);
                    long winnerBackend = Env.getCurrentSystemInfo().getAllBackendIds(true).stream()
                            .filter(id -> id != candidateBackend).findFirst().orElseThrow();
                    Tablet tablet = altered.getPartitions().iterator().next().getBaseIndex().getTablets().get(0);
                    Replica original = tablet.getReplicas().get(0);
                    // Give the healthy ALTER table a different, live mocked backend from CREATE's candidate.
                    tablet.deleteReplica(original);
                    tablet.addReplica(new LocalReplica(env.getNextId(), winnerBackend, Replica.ReplicaState.NORMAL,
                            original.getVersion(), original.getSchemaHash()));
                    winnerSeq.set(copyBackendsPerBucketSeq(altered.getArbitraryTabletBucketsSeq()));
                    alterTable("ALTER TABLE " + dbName + "." + tableName2
                            + " SET (\"colocate_with\"=\"" + groupName + "\")", UtFrameUtils.createDefaultCtx());
                    return null;
                } finally {
                    ConnectContext.remove();
                    published.countDown();
                }
            });
            createTable("CREATE TABLE " + dbName + "." + tableName1
                    + " (k1 INT) DISTRIBUTED BY HASH(k1) BUCKETS 1"
                    + " PROPERTIES (\"replication_num\"=\"1\", \"colocate_with\"=\"" + groupName + "\")");
            alter.get(30, TimeUnit.SECONDS);
            OlapTable created = (OlapTable) db.getTableOrMetaException(tableName1);
            GroupId groupId = index.getGroup(altered.getId());
            Assertions.assertNotEquals(candidateSeq.get(), winnerSeq.get());
            Assertions.assertEquals(winnerSeq.get(), index.getBackendsPerBucketSeq(groupId));
            Assertions.assertEquals(winnerSeq.get(), created.getArbitraryTabletBucketsSeq());
            Assertions.assertEquals(groupId, index.getGroup(created.getId()));
            Assertions.assertEquals(new HashSet<>(List.of(created.getId(), altered.getId())),
                    new HashSet<>(index.getAllTableIds(groupId)));
        } finally {
            selected.countDown();
            published.countDown();
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
    public void testInitializerPublishesOnceAndAwaitsOutsideIndexLock() throws Exception {
        Env env = Env.getCurrentEnv();
        EditLog originalEditLog = env.getEditLog();
        ColocateTableIndex index = Env.getCurrentColocateIndex();
        MonitoredReentrantReadWriteLock lock = Deencapsulation.getField(index, "lock");
        GroupId groupId = new GroupId(1, 2);
        Map<Tag, List<List<Long>>> candidate = copyBackendsPerBucketSeq(
                Map.of(Tag.DEFAULT_BACKEND_TAG, List.of(List.of(10L))));
        Map<Tag, List<List<Long>>> expected = copyBackendsPerBucketSeq(candidate);
        Map<Tag, List<List<Long>>> loser = Map.of(Tag.DEFAULT_BACKEND_TAG, List.of(List.of(20L)));
        EditLog journal = Mockito.mock(EditLog.class);
        EditLog.EditLogItem item = Mockito.mock(EditLog.EditLogItem.class);
        ExecutorService worker = Executors.newSingleThreadExecutor();
        Mockito.when(journal.submitEdit(Mockito.eq(OperationType.OP_COLOCATE_BACKENDS_PER_BUCKETSEQ),
                Mockito.any(ColocatePersistInfo.class))).thenAnswer(invocation -> {
                    Assertions.assertTrue(lock.isWriteLockedByCurrentThread());
                    return item;
                });
        Mockito.when(item.await()).thenAnswer(invocation -> {
            Assertions.assertFalse(lock.isWriteLockedByCurrentThread());
            // A separate thread must be able to read the published winner before persistence completes.
            Pair<Map<Tag, List<List<Long>>>, Boolean> concurrent = worker.submit(
                    () -> index.getOrInitializeBackendsPerBucketSeq(groupId, loser)).get(30, TimeUnit.SECONDS);
            Assertions.assertEquals(expected, concurrent.first);
            Assertions.assertFalse(concurrent.second);
            return 1L;
        });
        env.setEditLog(journal);
        try {
            Pair<Map<Tag, List<List<Long>>>, Boolean> empty = index.getOrInitializeBackendsPerBucketSeq(
                    groupId, Map.of());
            Assertions.assertTrue(empty.first.isEmpty());
            Assertions.assertFalse(empty.second);
            Pair<Map<Tag, List<List<Long>>>, Boolean> installed = index.getOrInitializeBackendsPerBucketSeq(
                    groupId, candidate);
            Assertions.assertTrue(installed.second);
            Assertions.assertEquals(expected, installed.first);
            candidate.get(Tag.DEFAULT_BACKEND_TAG).get(0).set(0, 30L);
            installed.first.get(Tag.DEFAULT_BACKEND_TAG).get(0).set(0, 40L);
            Pair<Map<Tag, List<List<Long>>>, Boolean> read = index.getOrInitializeBackendsPerBucketSeq(
                    groupId, Map.of());
            Assertions.assertFalse(read.second);
            Assertions.assertEquals(expected, read.first);
            read.first.get(Tag.DEFAULT_BACKEND_TAG).get(0).set(0, 50L);
            Assertions.assertEquals(expected, index.getBackendsPerBucketSeq(groupId));
            Mockito.verify(journal, Mockito.times(1)).submitEdit(
                    Mockito.eq(OperationType.OP_COLOCATE_BACKENDS_PER_BUCKETSEQ), Mockito.any(ColocatePersistInfo.class));
            Mockito.verify(item, Mockito.times(1)).await();
        } finally {
            worker.shutdown();
            try {
                Assertions.assertTrue(worker.awaitTermination(30, TimeUnit.SECONDS));
            } finally {
                env.setEditLog(originalEditLog);
            }
        }
    }

    @Test
    public void testRestoredMembershipSnapshotsGlobalGroupAndCompleteSequence() throws Exception {
        checkRestoredMembershipSnapshot(false);
    }

    @Test
    public void testRestoredMembershipAllowsEmptyPartitionSequence() throws Exception {
        checkRestoredMembershipSnapshot(true);
    }

    private void checkRestoredMembershipSnapshot(boolean emptySequence) throws Exception {
        ColocateTableIndex index = new ColocateTableIndex();
        MonitoredReentrantReadWriteLock lock = Deencapsulation.getField(index, "lock");
        List<Column> columns = List.of(new Column("k1", Type.INT));
        OlapTable first = new OlapTable(11, "restore_first", columns, KeysType.DUP_KEYS,
                new RangePartitionInfo(columns), new HashDistributionInfo(2, columns));
        OlapTable second = new OlapTable(12, "restore_second", columns, KeysType.DUP_KEYS,
                new RangePartitionInfo(columns), new HashDistributionInfo(2, columns));
        String globalGroup = "__global__restore_snapshot";
        GroupId groupId = index.addTableToGroup(101, first, globalGroup, new GroupId(0, 99));
        // The pending descriptor predates both the second database member and sequence initialization.
        ColocatePersistInfo pending = roundTripColocateInfo(
                ColocatePersistInfo.createForAddTable(groupId, first.getId(), Map.of()));
        index.addTableToGroup(102, second, globalGroup, null);
        Assertions.assertEquals(1, pending.getGroupId().getTblId2DbIdSize());
        Tag otherTag = Tag.create(Tag.TYPE_LOCATION, "restore_other");
        Map<Tag, List<List<Long>>> sequence = copyBackendsPerBucketSeq(Map.of(
                Tag.DEFAULT_BACKEND_TAG, List.of(List.of(1L, 2L, 3L), List.of(4L, 5L, 6L)),
                otherTag, List.of(List.of(7L, 8L, 9L), List.of(10L, 11L, 12L))));
        Map<Tag, List<List<Long>>> expected = emptySequence ? Map.of() : copyBackendsPerBucketSeq(sequence);
        index.addBackendsPerBucketSeq(groupId, emptySequence ? Map.of() : sequence);
        Assertions.assertTrue(first.getAllPartitions().isEmpty());
        Env env = Env.getCurrentEnv();
        EditLog originalEditLog = env.getEditLog();
        EditLog journal = Mockito.mock(EditLog.class);
        EditLog.EditLogItem item = Mockito.mock(EditLog.EditLogItem.class);
        AtomicReference<ColocatePersistInfo> captured = new AtomicReference<>();
        Mockito.when(journal.submitEdit(Mockito.eq(OperationType.OP_COLOCATE_ADD_TABLE),
                Mockito.any(ColocatePersistInfo.class))).thenAnswer(invocation -> {
                    Assertions.assertTrue(lock.isWriteLockedByCurrentThread());
                    captured.set(invocation.getArgument(1));
                    return item;
                });
        Mockito.when(item.await()).thenAnswer(invocation -> {
            Assertions.assertFalse(lock.isWriteLockedByCurrentThread());
            Assertions.assertEquals(0, lock.getReadHoldCount());
            // Mutate live membership and every sequence container before serializing the captured reference.
            index.removeTable(second.getId());
            index.addTableToGroup(103, second, globalGroup, null);
            sequence.get(Tag.DEFAULT_BACKEND_TAG).get(0).set(0, 99L);
            sequence.get(otherTag).add(new ArrayList<>(List.of(100L)));
            index.setBackendsPerBucketSeq(groupId, Map.of(Tag.DEFAULT_BACKEND_TAG, List.of(List.of(200L))));
            ColocatePersistInfo restored = roundTripColocateInfo(captured.get());
            Assertions.assertNotSame(groupId, captured.get().getGroupId());
            Assertions.assertEquals(groupId, restored.getGroupId());
            Assertions.assertEquals(first.getId(), restored.getTableId());
            Assertions.assertEquals(2, restored.getGroupId().getTblId2DbIdSize());
            Assertions.assertEquals(101L, restored.getGroupId().getDbIdByTblId(first.getId()));
            Assertions.assertEquals(102L, restored.getGroupId().getDbIdByTblId(second.getId()));
            Assertions.assertEquals(103L, groupId.getDbIdByTblId(second.getId()));
            Assertions.assertEquals(expected, restored.getBackendsPerBucketSeq());
            Assertions.assertEquals(expected, captured.get().getBackendsPerBucketSeq());
            return 1L;
        });
        env.setEditLog(journal);
        try {
            index.persistRestoredTableMembership(pending);
            Mockito.verify(journal, Mockito.times(1)).submitEdit(
                    Mockito.eq(OperationType.OP_COLOCATE_ADD_TABLE), Mockito.any(ColocatePersistInfo.class));
            Mockito.verify(item, Mockito.times(1)).await();
        } finally {
            env.setEditLog(originalEditLog);
        }
    }

    @Test
    public void testReplayModifyPreservesSequencePublishedBeforeSchema() throws Exception {
        createTable("CREATE TABLE " + dbName + "." + tableName1
                + " (k1 INT) DISTRIBUTED BY HASH(k1) BUCKETS 1 PROPERTIES (\"replication_num\"=\"1\")");
        Env env = Env.getCurrentEnv();
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException(fullDbName);
        OlapTable table = (OlapTable) db.getTableOrMetaException(tableName1);
        ColocateTableIndex originalIndex = Env.getCurrentColocateIndex();
        ColocateTableIndex replay = new ColocateTableIndex();
        GroupId groupId = new GroupId(db.getId(), env.getNextId());
        long tableBackend = table.getArbitraryTabletBucketsSeq().get(Tag.DEFAULT_BACKEND_TAG).get(0).get(0);
        long winnerBackend = Env.getCurrentSystemInfo().getAllBackendIds(true).stream()
                .filter(id -> id != tableBackend).findFirst().orElseThrow();
        Map<Tag, List<List<Long>>> expected = Map.of(Tag.DEFAULT_BACKEND_TAG, List.of(List.of(winnerBackend)));
        replay.replayAddBackendsPerBucketSeq(ColocatePersistInfo.createForBackendsPerBucketSeq(groupId, expected));
        Assertions.assertNull(replay.getGroupSchema(groupId));
        env.setColocateTableIndex(replay);
        try {
            env.replayModifyTableColocate(new TablePropertyInfo(db.getId(), table.getId(), groupId,
                    Map.of("colocate_with", groupName)));
            Assertions.assertNotNull(replay.getGroupSchema(groupId));
            Assertions.assertEquals(groupId, replay.getGroup(table.getId()));
            Assertions.assertEquals(expected, replay.getBackendsPerBucketSeq(groupId));
        } finally {
            env.setColocateTableIndex(originalIndex);
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
        Tag recordedTag = Tag.create(Tag.TYPE_LOCATION, "historical_recorded");
        Tag unrelatedTag = Tag.create(Tag.TYPE_LOCATION, "historical_unrelated");
        Map<Tag, List<List<Long>>> seq = Map.of(
                Tag.DEFAULT_BACKEND_TAG, List.of(List.of(11L)),
                recordedTag, List.of(List.of(21L)),
                unrelatedTag, List.of(List.of(31L)));
        Map<Tag, List<List<Long>>> recorded = Map.of(
                Tag.DEFAULT_BACKEND_TAG, List.of(List.of(41L)),
                recordedTag, List.of(List.of(51L)));
        ColocateTableIndex replay = new ColocateTableIndex();
        String originalGroup = second.getColocateGroup();
        try {
            // Model historical metadata without writing an incompatible membership to the live journal.
            second.setColocateGroup(groupName);
            replay.replayAddTableToGroup(roundTripColocateInfo(
                    ColocatePersistInfo.createForAddTable(groupId, first.getId(), seq)));
            Assertions.assertEquals(seq, replay.getBackendsPerBucketSeq(groupId));
            ColocateGroupSchema schema = replay.getGroupSchema(groupId);
            Assertions.assertThrows(DdlException.class, () -> schema.checkColocateSchema(second));
            // Legacy nonempty ADD records replace their recorded tags, not the complete sequence map.
            replay.replayAddTableToGroup(roundTripColocateInfo(
                    ColocatePersistInfo.createForAddTable(groupId, second.getId(), recorded)));
            Assertions.assertEquals(new HashSet<>(List.of(first.getId(), second.getId())),
                    new HashSet<>(replay.getAllTableIds(groupId)));
            Assertions.assertEquals(groupId, replay.getGroup(second.getId()));
            Assertions.assertSame(schema, replay.getGroupSchema(groupId));
            Map<Tag, List<List<Long>>> expected = new HashMap<>(recorded);
            expected.put(unrelatedTag, seq.get(unrelatedTag));
            Assertions.assertEquals(expected, replay.getBackendsPerBucketSeq(groupId));
        } finally {
            second.setColocateGroup(originalGroup);
        }
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
    public void testReplicaAllocationRevalidatesDepartedTable() throws Exception {
        checkReplicaAllocationMembershipChange(false, false);
    }

    @Test
    public void testReplayReplicaAllocationRevalidatesDepartedTable() throws Exception {
        checkReplicaAllocationMembershipChange(true, false);
    }

    @Test
    public void testReplicaAllocationLocksJoiningTableBeforeLogging() throws Exception {
        checkReplicaAllocationMembershipChange(false, true);
    }

    @Test
    public void testReplayReplicaAllocationIncludesJoiningTable() throws Exception {
        checkReplicaAllocationMembershipChange(true, true);
    }

    private void checkReplicaAllocationMembershipChange(boolean replay, boolean joining) throws Exception {
        boolean previousAllowReplicaOnSameHost = Config.allow_replica_on_same_host;
        try {
            // The three mock backends share localhost; preserve real three-replica selection.
            Config.allow_replica_on_same_host = true;
            OlapTable first = createReplicaRaceTable(dbName, "replica_race_t1", "replica_race_group");
            OlapTable second = createReplicaRaceTable(dbName, "replica_race_t2", "replica_race_group");
            OlapTable third = createReplicaRaceTable(dbName, "replica_race_t3", "");
            ColocateTableIndex index = Env.getCurrentColocateIndex();
            GroupId source = index.getGroup(first.getId());
            AtomicReference<GroupId> destination = new AtomicReference<>();
            AtomicReference<Map<Tag, List<List<Long>>>> destinationSeq = new AtomicReference<>();
            List<OlapTable> members = joining ? List.of(first, second, third) : List.of(second);
            runReplicaAllocationRace(replay, "replica_race_group", first, members, () -> {
                // Change membership via real ALTER while the worker waits on t1's reentrant table lock.
                alterTable("ALTER TABLE " + dbName + "." + (joining ? third.getName() : first.getName())
                        + " SET (\"colocate_with\"=\""
                        + (joining ? "replica_race_group" : "replica_race_destination") + "\")");
                if (!joining) {
                    destination.set(index.getGroup(first.getId()));
                    destinationSeq.set(copyBackendsPerBucketSeq(index.getBackendsPerBucketSeq(destination.get())));
                }
                return null;
            });
            Assertions.assertEquals(members.stream().map(OlapTable::getId).collect(Collectors.toSet()),
                    new HashSet<>(index.getAllTableIds(source)));
            if (!joining) {
                assertReplicaAllocation(first, 3);
                assertReplicaAllocation(third, 3);
                Assertions.assertNotEquals(source, destination.get());
                Assertions.assertEquals(destination.get(), index.getGroup(first.getId()));
                Assertions.assertEquals((short) 3, index.getGroupSchema(destination.get())
                        .getReplicaAlloc().getTotalReplicaNum());
                Assertions.assertEquals(destinationSeq.get(), index.getBackendsPerBucketSeq(destination.get()));
            }
        } finally {
            Config.allow_replica_on_same_host = previousAllowReplicaOnSameHost;
        }
    }

    private static OlapTable createReplicaRaceTable(String database, String table, String group) throws Exception {
        createTable("CREATE TABLE " + database + "." + table
                + " (k1 INT) DISTRIBUTED BY HASH(k1) BUCKETS 1"
                + " PROPERTIES (\"replication_num\"=\"3\", \"colocate_with\"=\"" + group + "\")");
        return (OlapTable) Env.getCurrentInternalCatalog().getDbOrMetaException(database)
                .getTableOrMetaException(table);
    }

    private static void assertReplicaAllocation(OlapTable table, int replicas) {
        Assertions.assertEquals((short) replicas, table.getDefaultReplicaAllocation().getTotalReplicaNum());
        for (ReplicaAllocation allocation : table.getPartitionInfo().getPartitionReplicaAllocations().values()) {
            Assertions.assertEquals((short) replicas, allocation.getTotalReplicaNum());
        }
    }

    private void runReplicaAllocationRace(boolean replay, String groupName, OlapTable first,
            List<OlapTable> expectedMembers, Callable<Void> changeMembership) throws Exception {
        Env env = Env.getCurrentEnv();
        ColocateTableIndex index = Env.getCurrentColocateIndex();
        GroupId groupId = index.getGroup(first.getId());
        Map<Tag, List<List<Long>>> replaySeq = copyBackendsPerBucketSeq(index.getBackendsPerBucketSeq(groupId));
        replaySeq.values().forEach(buckets -> buckets.forEach(bucket -> bucket.subList(1, bucket.size()).clear()));
        // Use a separate GroupId membership map to exercise lookup after global-group deserialization.
        ColocatePersistInfo info = ColocatePersistInfo.createForModifyReplicaAlloc(
                new GroupId(groupId.dbId, groupId.grpId), new ReplicaAllocation((short) 1), replaySeq);
        ReentrantReadWriteLock tableLock = Deencapsulation.getField(first, "rwLock");
        ReentrantReadWriteLock indexLock = Deencapsulation.getField(index, "lock");
        EditLog originalEditLog = env.getEditLog();
        EditLog journal = Mockito.mock(EditLog.class);
        Mockito.when(journal.submitEdit(Mockito.eq(OperationType.OP_MODIFY_TABLE_COLOCATE),
                Mockito.any(TablePropertyInfo.class))).thenReturn(Mockito.mock(EditLog.EditLogItem.class));
        Mockito.doAnswer(invocation -> {
            Assertions.assertTrue(indexLock.isWriteLockedByCurrentThread());
            for (OlapTable member : expectedMembers) {
                Assertions.assertTrue(member.isWriteLockHeldByCurrentThread(), member.getName());
                assertReplicaAllocation(member, 1);
            }
            ColocatePersistInfo logged = invocation.getArgument(0);
            Assertions.assertEquals(groupId, logged.getGroupId());
            Assertions.assertEquals((short) 1, logged.getReplicaAlloc().getTotalReplicaNum());
            return null;
        }).when(journal).logColocateModifyRepliaAlloc(Mockito.any(ColocatePersistInfo.class));
        ExecutorService worker = Executors.newSingleThreadExecutor();
        AtomicReference<Thread> workerThread = new AtomicReference<>();
        CountDownLatch started = new CountDownLatch(1);
        env.setEditLog(journal);
        try {
            Future<?> modification;
            first.writeLock();
            try {
                modification = worker.submit(() -> {
                    try {
                        ConnectContext context = UtFrameUtils.createDefaultCtx();
                        workerThread.set(Thread.currentThread());
                        started.countDown();
                        if (replay) {
                            index.replayModifyReplicaAlloc(info);
                        } else {
                            String qualifiedGroup = GroupId.isGlobalGroupName(groupName)
                                    ? groupName : dbName + "." + groupName;
                            alterColocateGroup("ALTER COLOCATE GROUP " + qualifiedGroup
                                    + " SET (\"replication_num\"=\"1\")", context);
                        }
                        return null;
                    } finally {
                        ConnectContext.remove();
                    }
                });
                Assertions.assertTrue(started.await(30, TimeUnit.SECONDS));
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
                while (!tableLock.hasQueuedThread(workerThread.get()) && !modification.isDone()
                        && System.nanoTime() < deadline) {
                    Thread.yield();
                }
                Assertions.assertTrue(tableLock.hasQueuedThread(workerThread.get()),
                        "Replica modification must queue on the actual first table lock");
                // The old implementation holds the index lock; release the table lock if the bounded probe fails.
                Assertions.assertTrue(indexLock.readLock().tryLock(5, TimeUnit.SECONDS),
                        "Replica modification must not hold the index lock while waiting for a table");
                indexLock.readLock().unlock();
                changeMembership.call();
            } finally {
                first.writeUnlock();
            }
            modification.get(30, TimeUnit.SECONDS);
            for (OlapTable member : expectedMembers) {
                assertReplicaAllocation(member, 1);
            }
            Assertions.assertEquals((short) 1, index.getGroupSchema(groupId).getReplicaAlloc().getTotalReplicaNum());
            Map<Tag, List<List<Long>>> sequence = index.getBackendsPerBucketSeq(groupId);
            Assertions.assertEquals(1, sequence.size());
            Assertions.assertEquals(1, sequence.get(Tag.DEFAULT_BACKEND_TAG).size());
            Assertions.assertEquals(1, sequence.get(Tag.DEFAULT_BACKEND_TAG).get(0).size());
            if (replay) {
                Assertions.assertEquals(replaySeq, sequence);
            }
            Mockito.verify(journal, Mockito.times(replay ? 0 : 1))
                    .logColocateModifyRepliaAlloc(Mockito.any(ColocatePersistInfo.class));
        } finally {
            // Release contended locks and let the worker finish without interrupting mock journal waits.
            worker.shutdown();
            try {
                Assertions.assertTrue(worker.awaitTermination(30, TimeUnit.SECONDS));
            } finally {
                env.setEditLog(originalEditLog);
            }
        }
    }

    @Test
    public void testGlobalReplicaAllocationLocksBothDatabases() throws Exception {
        checkGlobalReplicaAllocationRace(false, false, false);
    }

    @Test
    public void testReplayGlobalReplicaAllocationAfterDatabaseDrop() throws Exception {
        checkGlobalReplicaAllocationRace(true, true, false);
    }

    @Test
    public void testReplayGlobalReplicaAllocationSkipsMissingDatabase() throws Exception {
        checkGlobalReplicaAllocationRace(true, false, true);
    }

    private void checkGlobalReplicaAllocationRace(boolean replay, boolean dropDatabase, boolean missingDatabase)
            throws Exception {
        String otherDb = "replica_race_db";
        String sql = "CREATE DATABASE " + otherDb;
        ((CreateDatabaseCommand) new NereidsParser().parseSingle(sql))
                .run(connectContext, new StmtExecutor(connectContext, sql));
        Database other = Env.getCurrentInternalCatalog().getDbOrMetaException(otherDb);
        Map<Long, Database> databases = Deencapsulation.getField(Env.getCurrentInternalCatalog(), "idToDb");
        boolean previousAllowReplicaOnSameHost = Config.allow_replica_on_same_host;
        try {
            // The three mock backends share localhost; preserve real three-replica selection.
            Config.allow_replica_on_same_host = true;
            String globalGroup = "__global__replica_race";
            OlapTable first = createReplicaRaceTable(dbName, "global_replica_t1", globalGroup);
            OlapTable second = createReplicaRaceTable(otherDb, "global_replica_t2", globalGroup);
            ColocateTableIndex index = Env.getCurrentColocateIndex();
            GroupId groupId = index.getGroup(first.getId());
            Assertions.assertEquals(0L, groupId.dbId.longValue());
            Assertions.assertEquals(groupId, index.getGroup(second.getId()));
            runReplicaAllocationRace(replay, globalGroup, first,
                    dropDatabase || missingDatabase ? List.of(first) : List.of(first, second), () -> {
                        if (dropDatabase) {
                            Env.getCurrentInternalCatalog().dropDb(otherDb, false, true);
                            Assertions.assertNull(Env.getCurrentInternalCatalog().getDbNullable(otherDb));
                        } else if (missingDatabase) {
                            // Keep indexed members while hiding the database; retries must converge on its absence.
                            Assertions.assertSame(other, databases.remove(other.getId()));
                            Assertions.assertEquals(2, index.getAllTableIds(groupId).size());
                        }
                        return null;
                    });
            // Dropping the database leaves indexed IDs until recycling; absent members are skipped.
            Assertions.assertEquals(2, index.getAllTableIds(groupId).size());
            if (dropDatabase || missingDatabase) {
                assertReplicaAllocation(second, 3);
            }
        } finally {
            Config.allow_replica_on_same_host = previousAllowReplicaOnSameHost;
            if (missingDatabase) {
                databases.put(other.getId(), other);
            }
            Env.getCurrentInternalCatalog().dropDb(otherDb, true, true);
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
