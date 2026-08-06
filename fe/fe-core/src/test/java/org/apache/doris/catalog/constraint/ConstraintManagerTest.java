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

import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.DdlException;
import org.apache.doris.nereids.exceptions.AnalysisException;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.util.Map;

/**
 * Unit tests for ConstraintManager, testing direct API methods
 * without requiring a full FE environment.
 * All mutations use replay=true to bypass table validation.
 */
class ConstraintManagerTest {
    private ConstraintManager mgr;

    private static final TableNameInfo T1 = new TableNameInfo("ctl", "db", "t1");
    private static final TableNameInfo T2 = new TableNameInfo("ctl", "db", "t2");
    private static final TableNameInfo T3 = new TableNameInfo("ctl", "db", "t3");

    @BeforeEach
    void setUp() {
        mgr = new ConstraintManager();
    }

    // ==================== isEmpty ====================

    @Test
    void isEmptyOnNewManager() {
        Assertions.assertTrue(mgr.isEmpty());
    }

    @Test
    void isEmptyAfterAdd() {
        mgr.addConstraint(T1, "pk", newPk("pk", "k1"), true);
        Assertions.assertFalse(mgr.isEmpty());
    }

    @Test
    void isEmptyAfterAddAndDrop() {
        mgr.addConstraint(T1, "pk", newPk("pk", "k1"), true);
        mgr.dropConstraint(T1, "pk", true);
        Assertions.assertTrue(mgr.isEmpty());
    }

    // ==================== addConstraint / getConstraint ====================

    @Test
    void addAndGetPrimaryKey() {
        PrimaryKeyConstraint pk = newPk("pk", "k1");
        mgr.addConstraint(T1, "pk", pk, true);
        Assertions.assertSame(pk, mgr.getConstraint(T1, "pk"));
    }

    @Test
    void addAndGetUniqueKey() {
        UniqueConstraint uk = new UniqueConstraint("uk", ImmutableSet.of("c1"));
        mgr.addConstraint(T1, "uk", uk, true);
        Assertions.assertSame(uk, mgr.getConstraint(T1, "uk"));
    }

    @Test
    void addDuplicateConstraintThrows() {
        mgr.addConstraint(T1, "pk", newPk("pk", "k1"), true);
        Assertions.assertThrows(AnalysisException.class,
                () -> mgr.addConstraint(T1, "pk", newPk("pk", "k1"), true));
    }

    @Test
    void getConstraintNonExistentTableReturnsNull() {
        Assertions.assertNull(mgr.getConstraint(T1, "anything"));
    }

    @Test
    void getConstraintNonExistentNameReturnsNull() {
        mgr.addConstraint(T1, "pk", newPk("pk", "k1"), true);
        Assertions.assertNull(mgr.getConstraint(T1, "nonexistent"));
    }

    // ==================== getConstraints ====================

    @Test
    void getConstraintsReturnsImmutableCopy() {
        mgr.addConstraint(T1, "pk", newPk("pk", "k1"), true);
        Map<String, Constraint> result = mgr.getConstraints(T1);
        Assertions.assertEquals(1, result.size());
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> result.put("x", newPk("x", "x")));
    }

    @Test
    void getConstraintsForNonExistentTableReturnsEmpty() {
        Assertions.assertTrue(mgr.getConstraints(T1).isEmpty());
    }

    // ==================== Type-specific getters ====================

    @Test
    void getPrimaryKeyConstraints() {
        mgr.addConstraint(T1, "pk", newPk("pk", "k1"), true);
        mgr.addConstraint(T1, "uk", new UniqueConstraint("uk", ImmutableSet.of("c1")), true);
        mgr.addConstraint(T2, "pk2", newPk("pk2", "k1"), true);
        mgr.addConstraint(T1, "fk", newFk("fk", T2, "c1", "k1"), true);

        Assertions.assertEquals(1, mgr.getPrimaryKeyConstraints(T1).size());
        Assertions.assertInstanceOf(PrimaryKeyConstraint.class,
                mgr.getPrimaryKeyConstraints(T1).get(0));
    }

    @Test
    void getForeignKeyConstraints() {
        mgr.addConstraint(T1, "pk", newPk("pk", "k1"), true);
        mgr.addConstraint(T2, "pk2", newPk("pk2", "k1"), true);
        mgr.addConstraint(T1, "fk", newFk("fk", T2, "c1", "k1"), true);

        Assertions.assertEquals(1, mgr.getForeignKeyConstraints(T1).size());
        Assertions.assertInstanceOf(ForeignKeyConstraint.class,
                mgr.getForeignKeyConstraints(T1).get(0));
    }

    @Test
    void getUniqueConstraints() {
        mgr.addConstraint(T1, "pk", newPk("pk", "k1"), true);
        mgr.addConstraint(T1, "uk", new UniqueConstraint("uk", ImmutableSet.of("c1")), true);

        Assertions.assertEquals(1, mgr.getUniqueConstraints(T1).size());
        Assertions.assertInstanceOf(UniqueConstraint.class,
                mgr.getUniqueConstraints(T1).get(0));
    }

    @Test
    void typeSpecificGettersReturnEmptyForUnknownTable() {
        Assertions.assertTrue(mgr.getPrimaryKeyConstraints(T1).isEmpty());
        Assertions.assertTrue(mgr.getForeignKeyConstraints(T1).isEmpty());
        Assertions.assertTrue(mgr.getUniqueConstraints(T1).isEmpty());
    }

    // ==================== dropConstraint ====================

    @Test
    void dropConstraintRemoves() {
        mgr.addConstraint(T1, "pk", newPk("pk", "k1"), true);
        mgr.dropConstraint(T1, "pk", true);
        Assertions.assertNull(mgr.getConstraint(T1, "pk"));
    }

    @Test
    void dropNonExistentConstraintThrowsInNonReplay() {
        Assertions.assertThrows(AnalysisException.class,
                () -> mgr.dropConstraint(T1, "missing", false));
    }

    @Test
    void dropNonExistentConstraintSilentInReplay() {
        // Should not throw
        mgr.dropConstraint(T1, "missing", true);
    }

    // ==================== FK bidirectional references ====================

    @Test
    void addForeignKeyRegistersBidirectionalReference() {
        PrimaryKeyConstraint pk = newPk("pk", "k1");
        mgr.addConstraint(T1, "pk", pk, true);
        ForeignKeyConstraint fk = newFk("fk", T1, "c1", "k1");
        mgr.addConstraint(T2, "fk", fk, true);

        // PK on T1 should have T2 in its foreign table list
        PrimaryKeyConstraint loadedPk = (PrimaryKeyConstraint) mgr.getConstraint(T1, "pk");
        Assertions.assertTrue(loadedPk.getForeignTableInfos().stream()
                .anyMatch(t -> t.getTbl().equals("t2")));
    }

    @Test
    void dropForeignKeyRemovesBidirectionalReference() {
        mgr.addConstraint(T1, "pk", newPk("pk", "k1"), true);
        mgr.addConstraint(T2, "fk", newFk("fk", T1, "c1", "k1"), true);
        mgr.dropConstraint(T2, "fk", true);

        PrimaryKeyConstraint loadedPk = (PrimaryKeyConstraint) mgr.getConstraint(T1, "pk");
        Assertions.assertTrue(loadedPk.getForeignTableInfos().isEmpty());
    }

    @Test
    void dropPrimaryKeyCascadesDropForeignKeys() {
        mgr.addConstraint(T1, "pk", newPk("pk", "k1"), true);
        mgr.addConstraint(T2, "fk1", newFk("fk1", T1, "c1", "k1"), true);
        mgr.addConstraint(T3, "fk2", newFk("fk2", T1, "c1", "k1"), true);

        mgr.dropConstraint(T1, "pk", true);

        // FK on T2 and T3 should also be removed
        Assertions.assertTrue(mgr.getConstraints(T2).isEmpty());
        Assertions.assertTrue(mgr.getConstraints(T3).isEmpty());
    }

    // ==================== dropTableConstraints ====================

    @Test
    void dropTableConstraintsRemovesAll() {
        mgr.addConstraint(T1, "pk", newPk("pk", "k1"), true);
        mgr.addConstraint(T1, "uk", new UniqueConstraint("uk", ImmutableSet.of("c1")), true);
        mgr.dropTableConstraints(T1);
        Assertions.assertTrue(mgr.getConstraints(T1).isEmpty());
    }

    @Test
    void dropTableConstraintsCascadesFKReferences() {
        mgr.addConstraint(T1, "pk", newPk("pk", "k1"), true);
        mgr.addConstraint(T2, "fk", newFk("fk", T1, "c1", "k1"), true);
        // Drop T1's constraints → PK dropped → FK on T2 cascade-dropped
        mgr.dropTableConstraints(T1);
        Assertions.assertTrue(mgr.getConstraints(T2).isEmpty());
    }

    @Test
    void dropTableConstraintsOnNonExistentTableIsNoop() {
        // Should not throw
        mgr.dropTableConstraints(new TableNameInfo("x", "y", "z"));
    }

    // ==================== checkAndDropTableConstraints ====================

    @Test
    void checkAndDropBlocksWhenFKExists() {
        mgr.addConstraint(T1, "pk", newPk("pk", "k1"), true);
        mgr.addConstraint(T2, "fk", newFk("fk", T1, "c1", "k1"), true);
        Assertions.assertThrows(DdlException.class,
                () -> mgr.checkAndDropTableConstraints(T1, true));
        // Constraints should still be intact
        Assertions.assertNotNull(mgr.getConstraint(T1, "pk"));
    }

    @Test
    void checkAndDropWithoutCheckDropsEvenWithFK() {
        mgr.addConstraint(T1, "pk", newPk("pk", "k1"), true);
        mgr.addConstraint(T2, "fk", newFk("fk", T1, "c1", "k1"), true);
        Assertions.assertDoesNotThrow(
                () -> mgr.checkAndDropTableConstraints(T1, false));
        Assertions.assertTrue(mgr.getConstraints(T1).isEmpty());
        Assertions.assertTrue(mgr.getConstraints(T2).isEmpty());
    }

    @Test
    void checkAndDropSucceedsWhenNoFK() throws DdlException {
        mgr.addConstraint(T1, "pk", newPk("pk", "k1"), true);
        mgr.checkAndDropTableConstraints(T1, true);
        Assertions.assertTrue(mgr.getConstraints(T1).isEmpty());
    }

    // ==================== checkNoReferencingForeignKeys ====================

    @Test
    void checkNoReferencingForeignKeysPassesWithoutFK() throws DdlException {
        mgr.addConstraint(T1, "pk", newPk("pk", "k1"), true);
        mgr.checkNoReferencingForeignKeys(T1); // no exception
    }

    @Test
    void checkNoReferencingForeignKeysThrowsWithFK() {
        mgr.addConstraint(T1, "pk", newPk("pk", "k1"), true);
        mgr.addConstraint(T2, "fk", newFk("fk", T1, "c1", "k1"), true);
        Assertions.assertThrows(DdlException.class,
                () -> mgr.checkNoReferencingForeignKeys(T1));
    }

    @Test
    void checkNoReferencingForeignKeysOnEmptyTableIsNoop() throws DdlException {
        mgr.checkNoReferencingForeignKeys(T1); // no exception
    }

    // ==================== findConstraintWithColumn ====================

    @Test
    void findConstraintWithColumnFindsPK() {
        mgr.addConstraint(T1, "pk", newPk("pk", "k1"), true);
        Assertions.assertEquals("pk", mgr.findConstraintWithColumn(T1, "k1"));
    }

    @Test
    void findConstraintWithColumnFindsUnique() {
        mgr.addConstraint(T1, "uk", new UniqueConstraint("uk", ImmutableSet.of("c1")), true);
        Assertions.assertEquals("uk", mgr.findConstraintWithColumn(T1, "c1"));
    }

    @Test
    void findConstraintWithColumnFindsFK() {
        mgr.addConstraint(T2, "pk2", newPk("pk2", "k1"), true);
        mgr.addConstraint(T1, "fk", newFk("fk", T2, "c1", "k1"), true);
        Assertions.assertEquals("fk", mgr.findConstraintWithColumn(T1, "c1"));
    }

    @Test
    void findConstraintWithColumnReturnsNullForUnknownColumn() {
        mgr.addConstraint(T1, "pk", newPk("pk", "k1"), true);
        Assertions.assertNull(mgr.findConstraintWithColumn(T1, "nonexistent"));
    }

    @Test
    void findConstraintWithColumnReturnsNullForUnknownTable() {
        Assertions.assertNull(mgr.findConstraintWithColumn(T1, "k1"));
    }

    // ==================== dropCatalogConstraints ====================

    @Test
    void dropCatalogConstraintsRemovesOnlyMatchingCatalog() {
        TableNameInfo extT1 = new TableNameInfo("extCtl", "db", "t1");
        TableNameInfo extT2 = new TableNameInfo("extCtl", "db", "t2");
        mgr.addConstraint(extT1, "pk", newPk("pk", "k1"), true);
        mgr.addConstraint(extT2, "uk", new UniqueConstraint("uk", ImmutableSet.of("c1")), true);
        mgr.addConstraint(T1, "pk", newPk("pk", "k1"), true);

        mgr.dropCatalogConstraints("extCtl");

        Assertions.assertTrue(mgr.getConstraints(extT1).isEmpty());
        Assertions.assertTrue(mgr.getConstraints(extT2).isEmpty());
        // T1 is in "ctl" catalog — should be unaffected
        Assertions.assertNotNull(mgr.getConstraint(T1, "pk"));
    }

    @Test
    void dropCatalogConstraintsCascadesFKsAcrossCatalogs() {
        TableNameInfo extT = new TableNameInfo("extCtl", "db", "t1");
        // PK on extT, FK on T1 referencing extT
        mgr.addConstraint(extT, "pk", newPk("pk", "k1"), true);
        mgr.addConstraint(T1, "fk", newFk("fk", extT, "c1", "k1"), true);

        mgr.dropCatalogConstraints("extCtl");

        Assertions.assertTrue(mgr.getConstraints(extT).isEmpty());
        // FK on T1 referencing extT is cascade-dropped because the referenced PK was removed
        Assertions.assertTrue(mgr.getConstraints(T1).isEmpty(),
                "FK on T1 should be cascade-dropped when referenced PK's catalog is dropped");
    }

    @Test
    void dropCatalogConstraintsOnNonExistentCatalogIsNoop() {
        mgr.addConstraint(T1, "pk", newPk("pk", "k1"), true);
        mgr.dropCatalogConstraints("nonExistent");
        Assertions.assertNotNull(mgr.getConstraint(T1, "pk"));
    }

    // ==================== dropDatabaseConstraints ====================

    @Test
    void dropDatabaseConstraintsRemovesAllInDatabase() {
        mgr.addConstraint(T1, "pk1", newPk("pk1", "k1"), true);
        mgr.addConstraint(T2, "pk2", newPk("pk2", "k1"), true);
        // T3 is in same db
        mgr.addConstraint(T3, "uk3", new UniqueConstraint("uk3", ImmutableSet.of("c1")), true);
        // Table in different database
        TableNameInfo otherDbTable = new TableNameInfo("ctl", "other_db", "t1");
        mgr.addConstraint(otherDbTable, "pk_other", newPk("pk_other", "k1"), true);

        mgr.dropDatabaseConstraints("ctl", "db");

        Assertions.assertTrue(mgr.getConstraints(T1).isEmpty());
        Assertions.assertTrue(mgr.getConstraints(T2).isEmpty());
        Assertions.assertTrue(mgr.getConstraints(T3).isEmpty());
        // Other database unaffected
        Assertions.assertNotNull(mgr.getConstraint(otherDbTable, "pk_other"));
    }

    @Test
    void dropDatabaseConstraintsCascadesFKsAcrossDatabase() {
        // PK in db, FK in other_db referencing the PK
        mgr.addConstraint(T1, "pk", newPk("pk", "k1"), true);
        TableNameInfo otherDbTable = new TableNameInfo("ctl", "other_db", "t1");
        mgr.addConstraint(otherDbTable, "fk", newFk("fk", T1, "c1", "k1"), true);

        mgr.dropDatabaseConstraints("ctl", "db");

        Assertions.assertTrue(mgr.getConstraints(T1).isEmpty());
        // FK in other_db should be cascade-dropped because the referenced PK was removed
        Assertions.assertTrue(mgr.getConstraints(otherDbTable).isEmpty(),
                "FK in other_db should be cascade-dropped when referenced PK's database is dropped");
    }

    // ==================== renameTable ====================

    @Test
    void renameTableMovesConstraints() {
        mgr.addConstraint(T1, "pk", newPk("pk", "k1"), true);
        TableNameInfo newT = new TableNameInfo("ctl", "db", "t1_renamed");
        mgr.renameTable(T1, newT);

        Assertions.assertTrue(mgr.getConstraints(T1).isEmpty());
        Assertions.assertNotNull(mgr.getConstraint(newT, "pk"));
    }

    @Test
    void renameTableUpdatesFKReferencesInOtherTables() {
        mgr.addConstraint(T1, "pk", newPk("pk", "k1"), true);
        mgr.addConstraint(T2, "fk", newFk("fk", T1, "c1", "k1"), true);

        TableNameInfo newT1 = new TableNameInfo("ctl", "db", "t1_renamed");
        mgr.renameTable(T1, newT1);

        ForeignKeyConstraint fk = (ForeignKeyConstraint) mgr.getConstraint(T2, "fk");
        Assertions.assertEquals("t1_renamed", fk.getReferencedTableName().getTbl());
    }

    @Test
    void renameTableUpdatesPKForeignTableListInOtherTables() {
        // T2 has PK, T1 has FK referencing T2. Rename T1.
        mgr.addConstraint(T2, "pk", newPk("pk", "k1"), true);
        mgr.addConstraint(T1, "fk", newFk("fk", T2, "c1", "k1"), true);

        TableNameInfo newT1 = new TableNameInfo("ctl", "db", "t1_renamed");
        mgr.renameTable(T1, newT1);

        PrimaryKeyConstraint pk = (PrimaryKeyConstraint) mgr.getConstraint(T2, "pk");
        Assertions.assertTrue(pk.getForeignTableInfos().stream()
                .anyMatch(t -> t.getTbl().equals("t1_renamed")));
        Assertions.assertFalse(pk.getForeignTableInfos().stream()
                .anyMatch(t -> t.getTbl().equals("t1")));
    }

    @Test
    void renameNonExistentTableIsNoop() {
        TableNameInfo ghost = new TableNameInfo("ctl", "db", "ghost");
        TableNameInfo newGhost = new TableNameInfo("ctl", "db", "ghost2");
        mgr.renameTable(ghost, newGhost); // should not throw
        Assertions.assertTrue(mgr.isEmpty());
    }

    // ==================== swapTableConstraints ====================

    @Test
    void swapTableConstraintsExchangesMappings() {
        mgr.addConstraint(T1, "pk1", newPk("pk1", "k1"), true);
        mgr.addConstraint(T2, "uk2", new UniqueConstraint("uk2", ImmutableSet.of("c1")), true);

        mgr.swapTableConstraints(T1, T2);

        // pk1 should now be under T2
        Assertions.assertNotNull(mgr.getConstraint(T2, "pk1"));
        // uk2 should now be under T1
        Assertions.assertNotNull(mgr.getConstraint(T1, "uk2"));
    }

    @Test
    void swapTableConstraintsWhenOneSideEmpty() {
        mgr.addConstraint(T1, "pk", newPk("pk", "k1"), true);

        mgr.swapTableConstraints(T1, T2);

        Assertions.assertTrue(mgr.getConstraints(T1).isEmpty());
        Assertions.assertNotNull(mgr.getConstraint(T2, "pk"));
    }

    @Test
    void swapTableConstraintsUpdatesFKReferences() {
        // T1 has PK, T3 has FK referencing T1. Swap T1 and T2.
        mgr.addConstraint(T1, "pk1", newPk("pk1", "k1"), true);
        mgr.addConstraint(T2, "uk2", new UniqueConstraint("uk2", ImmutableSet.of("c1")), true);
        mgr.addConstraint(T3, "fk", newFk("fk", T1, "c1", "k1"), true);

        mgr.swapTableConstraints(T1, T2);

        // T3's FK should now reference T2 (was T1)
        ForeignKeyConstraint fk = (ForeignKeyConstraint) mgr.getConstraint(T3, "fk");
        Assertions.assertEquals("t2", fk.getReferencedTableName().getTbl());
    }

    // ==================== dropAndRenameConstraints ====================

    @Test
    void dropAndRenameDropsOldAndMovesNew() {
        mgr.addConstraint(T1, "pk_old", newPk("pk_old", "k1"), true);
        mgr.addConstraint(T2, "pk_new", newPk("pk_new", "k2"), true);

        mgr.dropAndRenameConstraints(T1, T2);

        // T2's constraints should now be under T1's key
        Assertions.assertNotNull(mgr.getConstraint(T1, "pk_new"));
        // T1's old constraint should be gone
        Assertions.assertNull(mgr.getConstraint(T1, "pk_old"));
        // T2 should have no constraints
        Assertions.assertTrue(mgr.getConstraints(T2).isEmpty());
    }

    @Test
    void dropAndRenameUpdatesFKReferences() {
        // T2 has PK, T3 has FK referencing T2. Replace T1 with T2 (no swap).
        mgr.addConstraint(T2, "pk", newPk("pk", "k1"), true);
        mgr.addConstraint(T3, "fk", newFk("fk", T2, "c1", "k1"), true);

        mgr.dropAndRenameConstraints(T1, T2);

        // T3's FK should now reference T1 (was T2, since T2 was renamed to T1)
        ForeignKeyConstraint fk = (ForeignKeyConstraint) mgr.getConstraint(T3, "fk");
        Assertions.assertEquals("t1", fk.getReferencedTableName().getTbl());
    }

    // ==================== migrateFromTable ====================

    @Test
    void migrateFromTableAddsConstraints() {
        PrimaryKeyConstraint pk = newPk("pk", "k1");
        Map<String, Constraint> existing = ImmutableMap.of("pk", pk);
        mgr.migrateFromTable(T1, existing);
        Assertions.assertSame(pk, mgr.getConstraint(T1, "pk"));
    }

    @Test
    void migrateFromTableWithEmptyMapIsNoop() {
        mgr.migrateFromTable(T1, ImmutableMap.of());
        Assertions.assertTrue(mgr.isEmpty());
    }

    @Test
    void migrateFromTableWithNullIsNoop() {
        mgr.migrateFromTable(T1, null);
        Assertions.assertTrue(mgr.isEmpty());
    }

    // ==================== rebuildForeignKeyReferences ====================

    @Test
    void rebuildForeignKeyReferencesWiresFKToPK() {
        // Simulate migration: PK on T1, FK on T2 referencing T1,
        // but PK doesn't know about T2 yet (as during per-table migration)
        PrimaryKeyConstraint pk = newPk("pk", "k1");
        mgr.addConstraint(T1, "pk", pk, true);
        // Add FK without registering bidirectional reference
        ForeignKeyConstraint fk = newFk("fk", T1, "c1", "k1");
        Map<String, Constraint> t2Map = new java.util.HashMap<>();
        t2Map.put("fk", fk);
        mgr.migrateFromTable(T2, t2Map);

        // Before rebuild: PK doesn't know about T2
        PrimaryKeyConstraint pkBefore = (PrimaryKeyConstraint) mgr.getConstraint(T1, "pk");
        Assertions.assertTrue(pkBefore.getForeignTableInfos().isEmpty(),
                "Before rebuild, PK should not know about T2");

        // After rebuild: PK should know about T2
        mgr.rebuildForeignKeyReferences();

        PrimaryKeyConstraint loadedPk = (PrimaryKeyConstraint) mgr.getConstraint(T1, "pk");
        Assertions.assertFalse(loadedPk.getForeignTableInfos().isEmpty());
    }

    @Test
    void rebuildForeignKeyReferencesDoesNotDuplicateEntries() {
        // PK on T1, FK on T2 referencing T1 — registered via addConstraint
        mgr.addConstraint(T1, "pk", newPk("pk", "k1"), true);
        mgr.addConstraint(T2, "fk", newFk("fk", T1, "c1", "k1"), true);

        // addConstraint already registered T2 in PK's foreignTableInfos
        PrimaryKeyConstraint pk = (PrimaryKeyConstraint) mgr.getConstraint(T1, "pk");
        Assertions.assertEquals(1, pk.getForeignTableInfos().size());

        // rebuild should NOT add duplicates
        mgr.rebuildForeignKeyReferences();
        Assertions.assertEquals(1, pk.getForeignTableInfos().size(),
                "rebuildForeignKeyReferences should not duplicate entries");
    }

    // ==================== Serialization ====================

    @Test
    void writeAndReadRoundTrip() throws Exception {
        mgr.addConstraint(T1, "pk", newPk("pk", "k1"), true);
        mgr.addConstraint(T1, "uk", new UniqueConstraint("uk", ImmutableSet.of("c1")), true);
        mgr.addConstraint(T2, "fk", newFk("fk", T1, "c1", "k1"), true);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutput out = new DataOutputStream(baos);
        mgr.write(out);

        DataInput in = new DataInputStream(
                new ByteArrayInputStream(baos.toByteArray()));
        ConstraintManager loaded = ConstraintManager.read(in);

        Assertions.assertEquals(2, loaded.getConstraints(T1).size());
        Assertions.assertEquals(1, loaded.getConstraints(T2).size());
        Assertions.assertInstanceOf(PrimaryKeyConstraint.class,
                loaded.getConstraint(T1, "pk"));
        Assertions.assertInstanceOf(ForeignKeyConstraint.class,
                loaded.getConstraint(T2, "fk"));
    }

    @Test
    void writeAndReadEmptyManager() throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutput out = new DataOutputStream(baos);
        mgr.write(out);

        DataInput in = new DataInputStream(
                new ByteArrayInputStream(baos.toByteArray()));
        ConstraintManager loaded = ConstraintManager.read(in);

        Assertions.assertTrue(loaded.isEmpty());
    }

    // ==================== Helpers ====================

    private static PrimaryKeyConstraint newPk(String name, String... columns) {
        return new PrimaryKeyConstraint(name, ImmutableSet.copyOf(columns));
    }

    private static ForeignKeyConstraint newFk(String name, TableNameInfo refTable,
            String fkCol, String pkCol) {
        return new ForeignKeyConstraint(name,
                ImmutableList.of(fkCol), refTable, ImmutableList.of(pkCol));
    }
}
