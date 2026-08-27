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

package org.apache.doris.catalog.constraint;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.cloud.snapshot.CloudSnapshotHandler;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.log.MetaIdMappingsLog;
import org.apache.doris.persist.AlterConstraintLog;
import org.apache.doris.persist.EditLog;
import org.apache.doris.persist.OperationType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

class ConstraintManagerMetastoreMutationTest {
    private static final TableNameInfo PARENT = new TableNameInfo("ctl", "db", "parent");
    private static final TableNameInfo CHILD = new TableNameInfo("ctl", "db", "child");

    @Test
    void tableRenameTransitionReplaysBeforeCursor() {
        TableNameInfo renamedParent = new TableNameInfo("ctl", "db", "renamed_parent");
        ConstraintManager leader = managerWithPrimaryAndForeignKey(PARENT, CHILD);
        ConstraintManager follower = managerWithPrimaryAndForeignKey(PARENT, CHILD);

        List<EditLog.EditLogOperation> operations = captureTransition(leader,
                ConstraintManager.MetastoreConstraintMutation.renameTable(PARENT, renamedParent));

        Assertions.assertEquals(List.of(
                OperationType.OP_DROP_CONSTRAINT,
                OperationType.OP_DROP_CONSTRAINT,
                OperationType.OP_ADD_CONSTRAINT,
                OperationType.OP_ADD_CONSTRAINT), operationCodes(operations));
        assertConstraintLog(operations.get(0), CHILD, "fk");
        assertConstraintLog(operations.get(1), PARENT, "pk");
        assertConstraintLog(operations.get(2), renamedParent, "pk");
        assertConstraintLog(operations.get(3), CHILD, "fk");

        // The edit-log writer serializes asynchronously, so later in-memory mutations must not alter this payload.
        leader.renameTable(renamedParent, new TableNameInfo("ctl", "db", "renamed_again"));
        replayConstraintOperations(follower, operations);
        Assertions.assertTrue(follower.getConstraints(PARENT).isEmpty());
        assertPrimaryAndForeignKeyState(follower, renamedParent, CHILD);

        // Before the old name is reused, a duplicate descriptor remains a no-op. The atomic cursor below
        // prevents retries after a replacement table can acquire constraints under that old name.
        follower.renameTable(PARENT, renamedParent);
        assertPrimaryAndForeignKeyState(follower, renamedParent, CHILD);
    }

    @Test
    void retriedRenameDoesNotSubmitRedundantTransition() {
        TableNameInfo renamedParent = new TableNameInfo("ctl", "db", "renamed_parent");
        ConstraintManager manager = managerWithPrimaryAndForeignKey(PARENT, CHILD);
        captureTransition(manager,
                ConstraintManager.MetastoreConstraintMutation.renameTable(PARENT, renamedParent));

        assertNoTransition(manager,
                ConstraintManager.MetastoreConstraintMutation.renameTable(PARENT, renamedParent));

        assertPrimaryAndForeignKeyState(manager, renamedParent, CHILD);
    }

    @Test
    void tableRenameCollisionTransitionPreservesTargetOnFollower() {
        TableNameInfo target = new TableNameInfo("ctl", "db", "target");
        ConstraintManager leader = managerWithPrimaryAndForeignKey(PARENT, CHILD);
        ConstraintManager follower = managerWithPrimaryAndForeignKey(PARENT, CHILD);
        leader.addConstraint(target, "target_uk",
                new UniqueConstraint("target_uk", ImmutableSet.of("value")), true);
        follower.addConstraint(target, "target_uk",
                new UniqueConstraint("target_uk", ImmutableSet.of("value")), true);

        List<EditLog.EditLogOperation> operations = captureTransition(leader,
                ConstraintManager.MetastoreConstraintMutation.renameTable(PARENT, target));

        Assertions.assertEquals(List.of(
                OperationType.OP_DROP_CONSTRAINT,
                OperationType.OP_DROP_CONSTRAINT), operationCodes(operations));
        assertConstraintLog(operations.get(0), CHILD, "fk");
        assertConstraintLog(operations.get(1), PARENT, "pk");
        replayConstraintOperations(follower, operations);
        for (ConstraintManager manager : List.of(leader, follower)) {
            Assertions.assertTrue(manager.getConstraints(PARENT).isEmpty());
            Assertions.assertTrue(manager.getConstraints(CHILD).isEmpty());
            Assertions.assertNotNull(manager.getConstraint(target, "target_uk"));
            Assertions.assertNull(manager.getConstraint(target, "pk"));
        }
    }

    @Test
    void renameTransitionAndCursorUseOneAtomicRequest() {
        TableNameInfo renamedParent = new TableNameInfo("ctl", "db", "renamed_parent");
        ConstraintManager manager = managerWithPrimaryAndForeignKey(PARENT, CHILD);
        MetaIdMappingsLog cursorLog = eventCursor(7L);

        List<EditLog.EditLogOperation> operations = captureTransition(manager,
                ConstraintManager.MetastoreConstraintMutation.renameTable(PARENT, renamedParent),
                new EditLog.EditLogOperation(OperationType.OP_ADD_META_ID_MAPPINGS, cursorLog));

        Assertions.assertEquals(List.of(
                OperationType.OP_DROP_CONSTRAINT,
                OperationType.OP_DROP_CONSTRAINT,
                OperationType.OP_ADD_CONSTRAINT,
                OperationType.OP_ADD_CONSTRAINT,
                OperationType.OP_ADD_META_ID_MAPPINGS), operationCodes(operations));
        Assertions.assertSame(cursorLog, getWritable(operations.get(4)));
    }

    @Test
    void noOpTransitionStillPersistsCursor() {
        ConstraintManager manager = new ConstraintManager();
        MetaIdMappingsLog cursorLog = eventCursor(9L);

        List<EditLog.EditLogOperation> operations = captureTransition(manager,
                ConstraintManager.MetastoreConstraintMutation.renameTable(
                        PARENT, new TableNameInfo("ctl", "db", "renamed_parent")),
                new EditLog.EditLogOperation(OperationType.OP_ADD_META_ID_MAPPINGS, cursorLog));

        Assertions.assertEquals(List.of(OperationType.OP_ADD_META_ID_MAPPINGS), operationCodes(operations));
        Assertions.assertSame(cursorLog, getWritable(operations.get(0)));
    }

    @Test
    void databaseRenameTransitionSurvivesOldCodeApplyAndFollowerRestart() throws Exception {
        TableNameInfo oldParent = new TableNameInfo("ctl", "old_db", "parent");
        TableNameInfo newParent = new TableNameInfo("ctl", "new_db", "parent");
        TableNameInfo externalChild = new TableNameInfo("ctl", "other_db", "child");
        ConstraintManager leader = managerWithPrimaryAndForeignKey(oldParent, externalChild);
        ConstraintManager follower = managerWithPrimaryAndForeignKey(oldParent, externalChild);

        List<EditLog.EditLogOperation> operations = captureTransition(leader,
                ConstraintManager.MetastoreConstraintMutation.renameDatabase(
                        "ctl", "old_db", "new_db"));

        Assertions.assertEquals(List.of(
                OperationType.OP_DROP_CONSTRAINT,
                OperationType.OP_DROP_CONSTRAINT,
                OperationType.OP_ADD_CONSTRAINT,
                OperationType.OP_ADD_CONSTRAINT), operationCodes(operations));
        replayConstraintOperations(follower, operations);
        // An older follower's structural event body only updates catalog caches, leaving this state untouched.
        ConstraintManager restartedFollower = roundTrip(follower);

        Assertions.assertTrue(restartedFollower.getConstraints(oldParent).isEmpty());
        assertPrimaryAndForeignKeyState(restartedFollower, newParent, externalChild);
    }

    @Test
    void tableDropTransitionReplaysCascadeBeforeCursor() {
        ConstraintManager leader = managerWithPrimaryAndForeignKey(PARENT, CHILD);
        ConstraintManager follower = managerWithPrimaryAndForeignKey(PARENT, CHILD);

        List<EditLog.EditLogOperation> operations = captureTransition(leader,
                ConstraintManager.MetastoreConstraintMutation.dropTable(PARENT));

        Assertions.assertEquals(List.of(
                OperationType.OP_DROP_CONSTRAINT,
                OperationType.OP_DROP_CONSTRAINT), operationCodes(operations));
        assertConstraintLog(operations.get(0), CHILD, "fk");
        assertConstraintLog(operations.get(1), PARENT, "pk");
        replayConstraintOperations(follower, operations);
        Assertions.assertTrue(follower.getConstraints(PARENT).isEmpty());
        Assertions.assertTrue(follower.getConstraints(CHILD).isEmpty());
    }

    @Test
    void databaseDropTransitionReplaysCrossDatabaseCascadeBeforeCursor() {
        TableNameInfo databaseParent = new TableNameInfo("ctl", "drop_db", "parent");
        TableNameInfo databaseUnique = new TableNameInfo("ctl", "drop_db", "unique_table");
        TableNameInfo externalChild = new TableNameInfo("ctl", "keep_db", "child");
        ConstraintManager leader = managerWithPrimaryAndForeignKey(databaseParent, externalChild);
        ConstraintManager follower = managerWithPrimaryAndForeignKey(databaseParent, externalChild);
        UniqueConstraint unique = new UniqueConstraint("uk", ImmutableSet.of("value"));
        leader.addConstraint(databaseUnique, "uk", unique, true);
        follower.addConstraint(databaseUnique, "uk",
                new UniqueConstraint("uk", ImmutableSet.of("value")), true);

        List<EditLog.EditLogOperation> operations = captureTransition(leader,
                ConstraintManager.MetastoreConstraintMutation.dropDatabase("ctl", "drop_db"));

        Assertions.assertEquals(List.of(
                OperationType.OP_DROP_CONSTRAINT,
                OperationType.OP_DROP_CONSTRAINT,
                OperationType.OP_DROP_CONSTRAINT), operationCodes(operations));
        replayConstraintOperations(follower, operations);
        Assertions.assertTrue(follower.getConstraints(databaseParent).isEmpty());
        Assertions.assertTrue(follower.getConstraints(databaseUnique).isEmpty());
        Assertions.assertTrue(follower.getConstraints(externalChild).isEmpty());
    }

    @Test
    void catalogDropTransitionReplaysCrossCatalogCascadeBeforeCursor() {
        TableNameInfo otherCatalogChild = new TableNameInfo("other_ctl", "db", "child");
        TableNameInfo otherCatalogTable = new TableNameInfo("other_ctl", "db", "table1");
        ConstraintManager leader = managerWithPrimaryAndForeignKey(PARENT, otherCatalogChild);
        ConstraintManager follower = managerWithPrimaryAndForeignKey(PARENT, otherCatalogChild);
        leader.addConstraint(otherCatalogTable, "uk",
                new UniqueConstraint("uk", ImmutableSet.of("value")), true);
        follower.addConstraint(otherCatalogTable, "uk",
                new UniqueConstraint("uk", ImmutableSet.of("value")), true);

        List<EditLog.EditLogOperation> operations = captureTransition(leader,
                ConstraintManager.MetastoreConstraintMutation.dropCatalog("ctl"));

        Assertions.assertEquals(List.of(
                OperationType.OP_DROP_CONSTRAINT,
                OperationType.OP_DROP_CONSTRAINT), operationCodes(operations));
        replayConstraintOperations(follower, operations);
        Assertions.assertTrue(follower.getConstraints(PARENT).isEmpty());
        Assertions.assertTrue(follower.getConstraints(otherCatalogChild).isEmpty());
        Assertions.assertNotNull(follower.getConstraint(otherCatalogTable, "uk"));
    }

    @Test
    void removedColumnTransitionDoesNotJournalDerivedPrimaryKeyUpdate() {
        ConstraintManager leader = managerWithPrimaryAndForeignKey(PARENT, CHILD);
        ConstraintManager follower = managerWithPrimaryAndForeignKey(PARENT, CHILD);
        UniqueConstraint unique = new UniqueConstraint("uk", ImmutableSet.of("kept"));
        leader.addConstraint(CHILD, "uk", unique, true);
        follower.addConstraint(CHILD, "uk",
                new UniqueConstraint("uk", ImmutableSet.of("kept")), true);

        List<EditLog.EditLogOperation> operations = captureTransition(leader,
                ConstraintManager.MetastoreConstraintMutation.dropColumns(CHILD, List.of("foreign_key")));

        Assertions.assertEquals(List.of(OperationType.OP_DROP_CONSTRAINT), operationCodes(operations));
        assertConstraintLog(operations.get(0), CHILD, "fk");
        replayConstraintOperations(follower, operations);
        Assertions.assertNotNull(follower.getConstraint(PARENT, "pk"));
        Assertions.assertTrue(((PrimaryKeyConstraint) follower.getConstraint(PARENT, "pk"))
                .getForeignTableInfos().isEmpty());
        Assertions.assertNull(follower.getConstraint(CHILD, "fk"));
        Assertions.assertNotNull(follower.getConstraint(CHILD, "uk"));
    }

    @Test
    void unrelatedRemovedColumnDoesNotSubmitTransition() {
        ConstraintManager manager = managerWithPrimaryAndForeignKey(PARENT, CHILD);
        List<TableNameInfo> affectedTables = assertNoTransition(manager,
                ConstraintManager.MetastoreConstraintMutation.dropColumns(
                        CHILD, List.of("unrelated")));

        Assertions.assertTrue(affectedTables.isEmpty());
        assertPrimaryAndForeignKeyState(manager, PARENT, CHILD);
    }

    @Test
    void catalogQuarantineHidesOwnedAndCrossCatalogConstraintsUntilReconciled() {
        TableNameInfo externalParent = new TableNameInfo("event_ctl", "db", "parent");
        TableNameInfo otherCatalogChild = new TableNameInfo("other_ctl", "db", "child");
        ConstraintManager manager = managerWithPrimaryAndForeignKey(externalParent, otherCatalogChild);
        manager.addConstraint(externalParent, "uk",
                new UniqueConstraint("uk", ImmutableSet.of("unique_key")), true);

        Assertions.assertTrue(manager.markCatalogConstraintsUntrusted("event_ctl"));
        Assertions.assertFalse(manager.markCatalogConstraintsUntrusted("event_ctl"));

        Assertions.assertTrue(manager.getConstraints(externalParent).isEmpty());
        Assertions.assertNull(manager.getConstraint(externalParent, "pk"));
        Assertions.assertTrue(manager.getPrimaryKeyConstraints(externalParent).isEmpty());
        Assertions.assertTrue(manager.getUniqueConstraints(externalParent).isEmpty());
        Assertions.assertNull(manager.findConstraintWithColumn(externalParent, "primary_key"));
        Assertions.assertTrue(manager.getConstraints(otherCatalogChild).isEmpty());
        Assertions.assertNull(manager.getConstraint(otherCatalogChild, "fk"));
        Assertions.assertTrue(manager.getForeignKeyConstraints(otherCatalogChild).isEmpty());
        Assertions.assertFalse(manager.isEmpty());
        Assertions.assertEquals(ImmutableSet.of(externalParent, otherCatalogChild),
                ImmutableSet.copyOf(manager.getCatalogConstraintRelatedTables("event_ctl")));

        Assertions.assertEquals(ImmutableSet.of(externalParent, otherCatalogChild),
                ImmutableSet.copyOf(manager.reconcileUntrustedCatalogConstraints("event_ctl")));

        Assertions.assertTrue(manager.isEmpty());
        Assertions.assertTrue(manager.reconcileUntrustedCatalogConstraints("event_ctl").isEmpty());
    }

    @Test
    void metastoreTransitionIsEnqueuedBeforeConcurrentConstraintDdl() throws Exception {
        ConstraintManager manager = managerWithPrimaryAndForeignKey(PARENT, CHILD);
        ConstraintManager follower = managerWithPrimaryAndForeignKey(PARENT, CHILD);
        EditLog editLog = Mockito.mock(EditLog.class);
        EditLog.EditLogItem transitionItem = Mockito.mock(EditLog.EditLogItem.class);
        EditLog.EditLogItem ddlItem = Mockito.mock(EditLog.EditLogItem.class);
        Env env = Mockito.mock(Env.class);
        Table childTable = Mockito.mock(Table.class);
        List<EditLog.EditLogOperation> journalOperations =
                Collections.synchronizedList(new ArrayList<>());
        CountDownLatch transitionSubmitEntered = new CountDownLatch(1);
        CountDownLatch ddlStarted = new CountDownLatch(1);
        CountDownLatch releaseTransitionSubmit = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);

        Mockito.when(env.getEditLog()).thenReturn(editLog);
        Mockito.when(childTable.getColumn("unique_key")).thenReturn(Mockito.mock(org.apache.doris.catalog.Column.class));
        Mockito.when(editLog.submitAtomicEdits(Mockito.anyList())).thenAnswer(invocation -> {
            transitionSubmitEntered.countDown();
            Assertions.assertTrue(releaseTransitionSubmit.await(5, TimeUnit.SECONDS));
            journalOperations.addAll(List.copyOf(invocation.getArgument(0)));
            return transitionItem;
        });
        Mockito.when(editLog.submitEdit(Mockito.eq(OperationType.OP_ADD_CONSTRAINT), Mockito.any()))
                .thenAnswer(invocation -> {
                    journalOperations.add(new EditLog.EditLogOperation(
                            invocation.getArgument(0), invocation.getArgument(1)));
                    return ddlItem;
                });

        CloudSnapshotHandler.setSnapshotEnv(env);
        try {
            Future<List<TableNameInfo>> transition = executor.submit(
                    () -> manager.applyMetastoreConstraintMutation(
                            ConstraintManager.MetastoreConstraintMutation.dropTable(PARENT)));
            Assertions.assertTrue(transitionSubmitEntered.await(5, TimeUnit.SECONDS));
            Future<?> ddl = executor.submit(() -> {
                ddlStarted.countDown();
                manager.addConstraintWithResolvedTables(CHILD, "fk",
                        new UniqueConstraint("fk", ImmutableSet.of("unique_key")),
                        childTable, null).await();
            });
            Assertions.assertTrue(ddlStarted.await(5, TimeUnit.SECONDS));
            releaseTransitionSubmit.countDown();
            transition.get(5, TimeUnit.SECONDS);
            ddl.get(5, TimeUnit.SECONDS);
        } finally {
            releaseTransitionSubmit.countDown();
            executor.shutdownNow();
            CloudSnapshotHandler.setSnapshotEnv(null);
        }

        Assertions.assertEquals(List.of(
                OperationType.OP_DROP_CONSTRAINT,
                OperationType.OP_DROP_CONSTRAINT,
                OperationType.OP_ADD_CONSTRAINT), operationCodes(journalOperations));
        replayConstraintOperations(follower, journalOperations);
        Assertions.assertTrue(follower.getConstraints(PARENT).isEmpty());
        Assertions.assertTrue(follower.getConstraint(CHILD, "fk") instanceof UniqueConstraint);
    }

    private static ConstraintManager managerWithPrimaryAndForeignKey(
            TableNameInfo parent, TableNameInfo child) {
        ConstraintManager manager = new ConstraintManager();
        manager.addConstraint(parent, "pk",
                new PrimaryKeyConstraint("pk", ImmutableSet.of("primary_key")), true);
        manager.addConstraint(child, "fk",
                new ForeignKeyConstraint("fk", ImmutableList.of("foreign_key"),
                        parent, ImmutableList.of("primary_key")), true);
        return manager;
    }

    private static List<EditLog.EditLogOperation> captureTransition(
            ConstraintManager manager,
            ConstraintManager.MetastoreConstraintMutation mutation) {
        return captureTransition(manager, mutation, null);
    }

    private static List<EditLog.EditLogOperation> captureTransition(
            ConstraintManager manager,
            ConstraintManager.MetastoreConstraintMutation mutation,
            EditLog.EditLogOperation trailingOperation) {
        Env env = Mockito.mock(Env.class);
        EditLog editLog = Mockito.mock(EditLog.class);
        EditLog.EditLogItem editLogItem = Mockito.mock(EditLog.EditLogItem.class);
        AtomicReference<List<EditLog.EditLogOperation>> capturedOperations = new AtomicReference<>();
        Mockito.when(env.getEditLog()).thenReturn(editLog);
        Mockito.when(editLog.submitAtomicEdits(Mockito.anyList())).thenAnswer(invocation -> {
            capturedOperations.set(List.copyOf(invocation.getArgument(0)));
            return editLogItem;
        });
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            if (trailingOperation == null) {
                manager.applyMetastoreConstraintMutation(mutation);
            } else {
                manager.applyMetastoreConstraintMutation(mutation, trailingOperation);
            }
        }
        List<EditLog.EditLogOperation> operations = capturedOperations.get();
        Assertions.assertNotNull(operations);
        Assertions.assertFalse(operations.isEmpty());
        if (trailingOperation == null) {
            for (EditLog.EditLogOperation operation : operations) {
                short opCode = operationCode(operation);
                Assertions.assertTrue(opCode == OperationType.OP_ADD_CONSTRAINT
                        || opCode == OperationType.OP_DROP_CONSTRAINT);
            }
        }
        return operations;
    }

    private static MetaIdMappingsLog eventCursor(long eventId) {
        MetaIdMappingsLog cursorLog = new MetaIdMappingsLog();
        cursorLog.setCatalogId(1L);
        cursorLog.setFromHmsEvent(true);
        cursorLog.setLastSyncedEventId(eventId);
        cursorLog.setConstraintTransitionsPersisted(true);
        return cursorLog;
    }

    private static List<TableNameInfo> assertNoTransition(
            ConstraintManager manager, ConstraintManager.MetastoreConstraintMutation mutation) {
        Env env = Mockito.mock(Env.class);
        EditLog editLog = Mockito.mock(EditLog.class);
        Mockito.when(env.getEditLog()).thenReturn(editLog);
        List<TableNameInfo> affectedTables;
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            affectedTables = manager.applyMetastoreConstraintMutation(mutation);
        }
        Mockito.verifyNoInteractions(editLog);
        return affectedTables;
    }

    private static List<Short> operationCodes(List<EditLog.EditLogOperation> operations) {
        List<Short> opCodes = new ArrayList<>(operations.size());
        for (EditLog.EditLogOperation operation : operations) {
            opCodes.add(operationCode(operation));
        }
        return opCodes;
    }

    private static short operationCode(EditLog.EditLogOperation operation) {
        return Deencapsulation.getField(operation, "op");
    }

    private static void assertConstraintLog(EditLog.EditLogOperation operation,
            TableNameInfo tableNameInfo, String constraintName) {
        AlterConstraintLog log = (AlterConstraintLog) getWritable(operation);
        Assertions.assertEquals(tableNameInfo, log.getTableNameInfo());
        Assertions.assertEquals(constraintName, log.getConstraint().getName());
    }

    private static void replayConstraintOperations(ConstraintManager manager,
            List<EditLog.EditLogOperation> operations) {
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getConstraintManager()).thenReturn(manager);
        for (EditLog.EditLogOperation operation : operations) {
            short opCode = operationCode(operation);
            if (opCode != OperationType.OP_ADD_CONSTRAINT
                    && opCode != OperationType.OP_DROP_CONSTRAINT) {
                continue;
            }
            AlterConstraintLog log = (AlterConstraintLog) getWritable(operation);
            Deencapsulation.invoke(EditLog.class, "replayConstraint", env,
                    log.getTableNameInfo(), log.getConstraint(),
                    opCode == OperationType.OP_ADD_CONSTRAINT);
        }
    }

    private static Writable getWritable(EditLog.EditLogOperation operation) {
        return Deencapsulation.getField(operation, "writable");
    }

    private static ConstraintManager roundTrip(ConstraintManager manager) throws Exception {
        ByteArrayOutputStream image = new ByteArrayOutputStream();
        manager.write(new DataOutputStream(image));
        return ConstraintManager.read(new DataInputStream(
                new ByteArrayInputStream(image.toByteArray())));
    }

    private static void assertPrimaryAndForeignKeyState(ConstraintManager manager,
            TableNameInfo parent, TableNameInfo child) {
        PrimaryKeyConstraint primaryKey =
                (PrimaryKeyConstraint) manager.getConstraint(parent, "pk");
        ForeignKeyConstraint foreignKey =
                (ForeignKeyConstraint) manager.getConstraint(child, "fk");
        Assertions.assertNotNull(primaryKey);
        Assertions.assertNotNull(foreignKey);
        Assertions.assertEquals(parent, foreignKey.getReferencedTableName());
        Assertions.assertEquals(List.of(child), primaryKey.getForeignTableInfos());
    }
}
