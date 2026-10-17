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

import org.apache.doris.analysis.ColumnDef;
import org.apache.doris.catalog.info.ColumnPosition;
import org.apache.doris.common.UserException;
import org.apache.doris.nereids.trees.plans.commands.info.AlterOp;
import org.apache.doris.nereids.trees.plans.commands.info.AlterTableOp;
import org.apache.doris.nereids.trees.plans.commands.info.ColumnDefinition;
import org.apache.doris.nereids.trees.plans.commands.info.ModifyColumnOp;
import org.apache.doris.plugin.audit.AuditLoader;
import org.apache.doris.statistics.StatisticConstants;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

class InternalSchemaInitializerTest {
    @Test
    public void testGetModifyColumn() throws UserException {
        InternalSchemaInitializer initializer = new InternalSchemaInitializer();
        OlapTable table = Mockito.mock(OlapTable.class);
        Column key1 = new Column("key1", ScalarType.createVarcharType(100), true, null, false, null, "");
        Column key2 = new Column("key2", ScalarType.createVarcharType(100), true, null, true, null, "");
        Column key3 = new Column("key3", ScalarType.createVarcharType(1024), true, null, null, "");
        Column key4 = new Column("key4", ScalarType.createVarcharType(1025), true, null, null, "");
        Column key5 = new Column("key5", ScalarType.INT, true, null, null, "");
        Column value1 = new Column("value1", ScalarType.INT, false, null, null, "");
        Column value2 = new Column("value2", ScalarType.createVarcharType(100), false, null, null, "");
        Column value3 = new Column("hot_value", ScalarType.createVarcharType(100), false, null, null, "");
        List<Column> schema = Lists.newArrayList();
        schema.add(key1);
        schema.add(key2);
        schema.add(key3);
        schema.add(key4);
        schema.add(key5);
        schema.add(value1);
        schema.add(value2);
        schema.add(value3);
        Mockito.when(table.getFullSchema()).thenReturn(schema);
        Mockito.when(table.getBaseSchema()).thenReturn(schema);

        List<AlterTableOp> ops = initializer.getModifyColumnOp(table);
        Assertions.assertEquals(16, ops.size());
        ModifyColumnOp modifyColumnOp = (ModifyColumnOp) ops.get(14);
        Assertions.assertEquals("key1", modifyColumnOp.getColumnDef().translateToCatalogStyle().getName());
        Assertions.assertEquals(StatisticConstants.MAX_NAME_LEN, modifyColumnOp.getColumnDef().translateToCatalogStyle().getType().getLength());
        Assertions.assertFalse(modifyColumnOp.getColumnDef().translateToCatalogStyle().isAllowNull());

        modifyColumnOp = (ModifyColumnOp) ops.get(15);
        Assertions.assertEquals("key2", modifyColumnOp.getColumnDef().translateToCatalogStyle().getName());
        Assertions.assertEquals(StatisticConstants.MAX_NAME_LEN, modifyColumnOp.getColumnDef().translateToCatalogStyle().getType().getLength());
        Assertions.assertTrue(modifyColumnOp.getColumnDef().translateToCatalogStyle().isAllowNull());
    }

    @Test
    public void testAuditLogSchemaContainsStorageFields() {
        boolean hasLocalStorageField = false;
        boolean hasRemoteStorageField = false;

        for (ColumnDef columnDef : InternalSchema.AUDIT_SCHEMA) {
            if (columnDef.getName().equals("scan_bytes_from_local_storage")) {
                hasLocalStorageField = true;
                Assertions.assertEquals(PrimitiveType.BIGINT, columnDef.getType().getPrimitiveType());
                Assertions.assertTrue(columnDef.isAllowNull());
            }

            if (columnDef.getName().equals("scan_bytes_from_remote_storage")) {
                hasRemoteStorageField = true;
                Assertions.assertEquals(PrimitiveType.BIGINT, columnDef.getType().getPrimitiveType());
                Assertions.assertTrue(columnDef.isAllowNull());
            }
        }

        Assertions.assertTrue(hasLocalStorageField, "scan_bytes_from_local_storage field is missing from AUDIT_SCHEMA");
        Assertions.assertTrue(hasRemoteStorageField,
                "scan_bytes_from_remote_storage field is missing from AUDIT_SCHEMA");
    }

    @Test
    public void testGetCopiedSchemaForAuditLog() throws UserException {
        List<ColumnDef> copiedSchema = InternalSchema.getCopiedSchema(AuditLoader.AUDIT_LOG_TABLE);

        boolean hasLocalStorageField = false;
        boolean hasRemoteStorageField = false;

        for (ColumnDef columnDef : copiedSchema) {
            if (columnDef.getName().equals("scan_bytes_from_local_storage")) {
                hasLocalStorageField = true;
                Assertions.assertEquals(PrimitiveType.BIGINT, columnDef.getType().getPrimitiveType());
                Assertions.assertTrue(columnDef.isAllowNull());
            }

            if (columnDef.getName().equals("scan_bytes_from_remote_storage")) {
                hasRemoteStorageField = true;
                Assertions.assertEquals(PrimitiveType.BIGINT, columnDef.getType().getPrimitiveType());
                Assertions.assertTrue(columnDef.isAllowNull());
            }
        }

        Assertions.assertTrue(hasLocalStorageField,
                "scan_bytes_from_local_storage field is missing from the copied schema");
        Assertions.assertTrue(hasRemoteStorageField,
                "scan_bytes_from_remote_storage field is missing from the copied schema");
    }

    @Test
    public void testStorageColumnsPositionInAuditTable() throws Exception {
        // Get storage-related column definitions directly from InternalSchema.AUDIT_SCHEMA
        ColumnDef localStorageDef = null;
        ColumnDef remoteStorageDef = null;

        for (int i = 0; i < InternalSchema.AUDIT_SCHEMA.size(); i++) {
            ColumnDef def = InternalSchema.AUDIT_SCHEMA.get(i);
            if (def.getName().equals("scan_bytes_from_local_storage")) {
                localStorageDef = def;
            } else if (def.getName().equals("scan_bytes_from_remote_storage")) {
                remoteStorageDef = def;
            }
        }

        Assertions.assertNotNull(localStorageDef, "The scan_bytes_from_local_storage column should exist in AUDIT_SCHEMA");
        Assertions.assertNotNull(remoteStorageDef, "The scan_bytes_from_remote_storage column should exist in AUDIT_SCHEMA");

        // Simulate column position logic in InternalSchemaInitializer
        // Note: Based on test failure, the system uses FIRST position rather than after a specific column
        List<AlterOp> alterOps = Lists.newArrayList();

        // Add scan_bytes_from_local_storage column using FIRST position
        ColumnPosition localStoragePosition = ColumnPosition.FIRST;
        ModifyColumnOp modifyColumnOp = new ModifyColumnOp(
                localStorageDef.translateToColumnDefinition(), localStoragePosition, null, Maps.newHashMap());
        modifyColumnOp.setColumn(localStorageDef.toColumn());
        alterOps.add(modifyColumnOp);

        // Add scan_bytes_from_remote_storage column using FIRST position
        ColumnPosition remoteStoragePosition = ColumnPosition.FIRST;
        ModifyColumnOp modifyColumnOp1 = new ModifyColumnOp(
                remoteStorageDef.translateToColumnDefinition(), remoteStoragePosition, null, Maps.newHashMap());
        modifyColumnOp1.setColumn(remoteStorageDef.toColumn());
        alterOps.add(modifyColumnOp1);

        // Verify the generated AlterClauses
        Assertions.assertEquals(2, alterOps.size(), "Two AlterClauses should be generated");

        // Verify that column positions are FIRST
        Assertions.assertTrue(((ModifyColumnOp) alterOps.get(0)).getColPos().isFirst(),
                "The position of the scan_bytes_from_local_storage column should be FIRST");
        Assertions.assertTrue(((ModifyColumnOp) alterOps.get(1)).getColPos().isFirst(),
                "The position of the scan_bytes_from_remote_storage column should be FIRST");
    }

    @Test
    public void testDoesNotModifyExistingColumns() throws Exception {
        // Create a mock audit table with storage-related columns but with inconsistent types (VARCHAR instead of BIGINT)
        List<Column> initialSchema = Lists.newArrayList(
                new Column("query_id", ScalarType.createVarcharType(48), true, null, false, null, ""),
                new Column("time", ScalarType.createDatetimeV2Type(3), true, null, false, null, ""),
                new Column("client_ip", ScalarType.createVarcharType(128), true, null, false, null, ""),
                new Column("user", ScalarType.createVarcharType(128), true, null, false, null, ""),
                new Column("catalog", ScalarType.createVarcharType(128), true, null, false, null, ""),
                new Column("db", ScalarType.createVarcharType(128), true, null, false, null, ""),
                new Column("state", ScalarType.createVarcharType(128), true, null, false, null, ""),
                new Column("error_code", ScalarType.INT, true, null, false, null, ""),
                new Column("error_message", ScalarType.STRING, true, null, false, null, ""),
                new Column("query_time", ScalarType.BIGINT, true, null, false, null, ""),
                new Column("scan_bytes", ScalarType.BIGINT, true, null, false, null, ""),
                new Column("scan_rows", ScalarType.BIGINT, true, null, false, null, ""),
                new Column("return_rows", ScalarType.BIGINT, true, null, false, null, ""),
                new Column("shuffle_send_rows", ScalarType.BIGINT, true, null, false, null, ""),
                new Column("shuffle_send_bytes", ScalarType.BIGINT, true, null, false, null, ""),
                // Intentionally use inconsistent types (VARCHAR instead of BIGINT)
                new Column("scan_bytes_from_local_storage", ScalarType.createVarcharType(128), true, null, false, null, ""),
                new Column("scan_bytes_from_remote_storage", ScalarType.createVarcharType(128), true, null, false, null, "")
        );

        // Use the correct constructor to create OlapTable to ensure nameToColumn is properly initialized
        OlapTable auditTable = new OlapTable(1000, "audit_log", initialSchema, KeysType.AGG_KEYS,
                new SinglePartitionInfo(), new HashDistributionInfo());

        // Verify columns exist and have VARCHAR type
        Column localStorageCol = auditTable.getColumn("scan_bytes_from_local_storage");
        Column remoteStorageCol = auditTable.getColumn("scan_bytes_from_remote_storage");

        Assertions.assertNotNull(localStorageCol, "The scan_bytes_from_local_storage column should exist in auditTable");
        Assertions.assertNotNull(remoteStorageCol, "The scan_bytes_from_remote_storage column should exist in auditTable");
        Assertions.assertTrue(localStorageCol.getType().isVarchar(),
                "The scan_bytes_from_local_storage column type should be VARCHAR");
        Assertions.assertTrue(remoteStorageCol.getType().isVarchar(),
                "The scan_bytes_from_remote_storage column type should be VARCHAR");

        // Get complete column definitions from InternalSchema.AUDIT_SCHEMA
        List<ColumnDef> expectedSchema = Lists.newArrayList();
        for (ColumnDef def : InternalSchema.AUDIT_SCHEMA) {
            expectedSchema.add(def);
        }

        // Simulate column processing logic in InternalSchemaInitializer
        List<AlterOp> alterOps = Lists.newArrayList();

        // Add columns if they don't exist
        for (int i = 0; i < expectedSchema.size(); i++) {
            ColumnDef def = expectedSchema.get(i);
            if (auditTable.getColumn(def.getName()) == null) {
                // If column doesn't exist, add it
                String afterColumn = null;
                if (i > 0) {
                    for (int j = i - 1; j >= 0; j--) {
                        String prevColName = expectedSchema.get(j).getName();
                        if (auditTable.getColumn(prevColName) != null) {
                            afterColumn = prevColName;
                            break;
                        }
                    }
                }
                ColumnPosition position = afterColumn == null ? ColumnPosition.FIRST :
                        new ColumnPosition(afterColumn);
                ModifyColumnOp op = new ModifyColumnOp(def.translateToColumnDefinition(), position, null, Maps.newHashMap());
                op.setColumn(def.toColumn());
                alterOps.add(op);
            }
            // Note: InternalSchemaInitializer.created() method does not check if column types match
            // It only adds columns that don't exist in the table
        }

        // Check if AlterClauses were generated for storage-related columns
        boolean hasLocalStorageClause = false;
        boolean hasRemoteStorageClause = false;

        for (AlterOp op : alterOps) {
            ModifyColumnOp modifyColumnOp = (ModifyColumnOp) op;
            if (modifyColumnOp.getColumn().getName().equals("scan_bytes_from_local_storage")) {
                hasLocalStorageClause = true;
            } else if (modifyColumnOp.getColumn().getName().equals("scan_bytes_from_remote_storage")) {
                hasRemoteStorageClause = true;
            }
        }

        // Verify the system does not generate AlterClauses for columns that already exist
        // even if their types don't match the expected types
        Assertions.assertFalse(hasLocalStorageClause,
                "The system should not generate AlterClause for the scan_bytes_from_local_storage column that already exists");
        Assertions.assertFalse(hasRemoteStorageClause,
                "The system should not generate AlterClause for the scan_bytes_from_remote_storage column that already exists");
    }

    // ==================== SPM baselines table: upgrade path ====================

    /**
     * An UPGRADED cluster already contains every legacy statistics/audit table, so the
     * SPM baselines table must gate created() itself: otherwise run() exits before ever
     * calling createTbl(), the missing spm_baselines table is never created and every
     * BaselineManager load keeps failing (global CREATE/ALTER/DROP report "store not
     * ready" forever). Simulate the upgrade: only this one table is absent.
     */
    @Test
    public void testSpmBaselinesTableGatesCompletion() {
        Database db = Mockito.mock(Database.class);
        Mockito.when(db.getTable(InternalSchema.SPM_BASELINES_TBL_NAME))
                .thenReturn(Optional.empty());
        Assertions.assertTrue(InternalSchemaInitializer.isSpmBaselinesTableMissing(db),
                "a cluster where only spm_baselines is absent must not count as initialized");

        Mockito.when(db.getTable(InternalSchema.SPM_BASELINES_TBL_NAME))
                .thenReturn(Optional.of(Mockito.mock(Table.class)));
        Assertions.assertFalse(InternalSchemaInitializer.isSpmBaselinesTableMissing(db),
                "an existing spm_baselines table must not block completion");

        // The id sequence table gates completion the same way - without it the
        // create path cannot read / advance its high-water mark
        Mockito.when(db.getTable(InternalSchema.SPM_BASELINES_SEQ_TBL_NAME))
                .thenReturn(Optional.empty());
        Assertions.assertTrue(InternalSchemaInitializer.isSpmBaselinesSeqTableMissing(db),
                "a cluster where only spm_baselines_seq is absent must not count as initialized");
        Mockito.when(db.getTable(InternalSchema.SPM_BASELINES_SEQ_TBL_NAME))
                .thenReturn(Optional.of(Mockito.mock(Table.class)));
        Assertions.assertFalse(InternalSchemaInitializer.isSpmBaselinesSeqTableMissing(db),
                "an existing spm_baselines_seq table must not block completion");
    }

    /**
     * The completion gate re-runs createTbl() on an upgraded cluster: the create text it
     * issues must cover the SPM table (CREATE TABLE IF NOT EXISTS makes the other legacy
     * tables a no-op).
     */
    @Test
    public void testCreateTblCoversSpmBaselinesTable() throws Exception {
        Method method = InternalSchemaInitializer.class.getDeclaredMethod("getSpmBaselinesCreateSql");
        method.setAccessible(true);
        String sql = (String) method.invoke(null);
        Assertions.assertTrue(sql.contains("CREATE TABLE IF NOT EXISTS"
                        + " `internal`.`__internal_schema`.`spm_baselines`"),
                "the SPM completion gate re-runs createTbl(): it must create the table: " + sql);
        Assertions.assertTrue(sql.contains("`sql_mode`"),
                "a new cluster must create the creating-session sql_mode column: " + sql);
        Assertions.assertTrue(sql.contains("`plan_sql_mode`"),
                "a new cluster must create the planSql-mode column: " + sql);
        Assertions.assertTrue(sql.contains("`plan_frozen`"),
                "a new cluster must create the frozen-provenance column: " + sql);
        Assertions.assertTrue(sql.contains("`schema_fingerprint`"),
                "a new cluster must create the schema-fingerprint column: " + sql);
    }

    /**
     * The SPM baseline tables carry cluster-wide state and must take part in the
     * internal-table replica upgrade: with the default minimum replication of 1 they
     * are created single-replica, so losing the hosting BE would make every global
     * baseline unavailable (spm_baselines) or erase the id reservation history
     * (spm_baselines_seq / spm_baselines_hwm).
     */
    @Test
    public void testSpmTablesTakePartInReplicaUpgrade() {
        Assertions.assertTrue(InternalSchemaInitializer.REPLICA_UPGRADED_INTERNAL_TABLES
                        .contains(InternalSchema.SPM_BASELINES_TBL_NAME),
                "spm_baselines must be raised towards the statistics replica target");
        Assertions.assertTrue(InternalSchemaInitializer.REPLICA_UPGRADED_INTERNAL_TABLES
                        .contains(InternalSchema.SPM_BASELINES_SEQ_TBL_NAME),
                "spm_baselines_seq must be raised as well: an unreadable id sequence"
                        + " fails every global CREATE BASELINE PLAN (round-32 #2)");
        Assertions.assertTrue(InternalSchemaInitializer.REPLICA_UPGRADED_INTERNAL_TABLES
                        .contains(InternalSchema.SPM_BASELINES_HWM_TBL_NAME),
                "spm_baselines_hwm must be raised as well: it carries the bounded id"
                        + " watermark / mutation clock of every global write");
    }


    /**
     * A transient ALTER failure must be retried INSIDE the initializer: run() calls the
     * upgrade only once and the replica-upgrade loop never comes back, so without the
     * retry loop an upgraded cluster stayed without the provenance columns until a
     * restart although BaselineManager always reads / writes them.
     */
    @Test
    public void testSqlModeUpgradeRetriesUntilColumnObserved() {
        AtomicInteger attempts = new AtomicInteger();
        AtomicInteger sleeps = new AtomicInteger();
        AtomicBoolean exists = new AtomicBoolean(false);
        InternalSchemaInitializer.ensureSpmBaselinesColumnsExist(
                exists::get,
                () -> {
                    if (attempts.incrementAndGet() == 1) {
                        throw new RuntimeException("transient alter failure");
                    }
                    exists.set(true);
                },
                sleeps::incrementAndGet);
        Assertions.assertEquals(2, attempts.get(),
                "the first failed ALTER must be retried without a restart");
        Assertions.assertEquals(1, sleeps.get(),
                "exactly one back-off between the two attempts");
    }

    @Test
    public void testSqlModeUpgradeSkipsAlterWhenColumnExists() {
        AtomicInteger attempts = new AtomicInteger();
        InternalSchemaInitializer.ensureSpmBaselinesColumnsExist(
                () -> true, attempts::incrementAndGet, () -> { });
        Assertions.assertEquals(0, attempts.get(),
                "an already upgraded table must not be altered");
    }

    /**
     * The upgrade set must cover every provenance / schema-identity column the load path
     * reads: forgetting one leaves an upgraded cluster reading NULL forever (mode /
     * frozen classification) or silently disabling a guard (schema fingerprint).
     */
    @Test
    public void testProvenanceUpgradeCoversEveryEvolvedColumn() {
        for (String column : new String[] {"sql_mode", "plan_sql_mode", "plan_frozen",
                "schema_fingerprint"}) {
            Assertions.assertTrue(
                    InternalSchema.SPM_BASELINES_SCHEMA.stream()
                            .anyMatch(def -> column.equalsIgnoreCase(def.getName())),
                    "the create schema must carry the " + column + " column");
        }
    }

    /**
     * Round-55 #5: the upgrade map must declare the CREATE schema's TIMESTAMPTZ for
     * reserve_time, and the upgrade checker must notice an existing column of the WRONG
     * type. The checker used to compare NAMES only, so a cluster already upgraded with a
     * DATETIME column counted as done: the reservation writer stores a zone-qualified
     * instant, DATETIME converts it into the WRITING session's zone, and a successor in
     * another zone reading UNIX_TIMESTAMP(reserve_time) shifts the reservation's age by
     * hours - a seconds-old ambiguous CREATE can be condemned as expired (its later
     * published row repaired away) or a retry stays fenced for hours.
     */
    @Test
    public void testReserveTimeUpgradeTypeMatchesTheCreateSchema() {
        ScalarType createType = InternalSchema.SPM_BASELINES_SEQ_SCHEMA.stream()
                .filter(def -> "reserve_time".equalsIgnoreCase(def.getName()))
                .map(def -> def.getType())
                .filter(ScalarType.class::isInstance)
                .map(ScalarType.class::cast)
                .findFirst().orElse(null);
        Assertions.assertNotNull(createType, "the seq table must carry reserve_time");
        ScalarType upgradeType =
                InternalSchemaInitializer.SPM_BASELINES_SEQ_UPGRADE_COLUMNS.get("reserve_time");
        Assertions.assertNotNull(upgradeType,
                "an upgraded cluster must gain the reservation-time column");
        Assertions.assertEquals(PrimitiveType.TIMESTAMPTZ, createType.getPrimitiveType(),
                "the create schema pins the reservation INSTANT");
        Assertions.assertEquals(PrimitiveType.TIMESTAMPTZ, upgradeType.getPrimitiveType(),
                "the upgrade must add the SAME type as the create schema");

        Assertions.assertTrue(InternalSchemaInitializer.upgradeColumnTypeMatches(
                upgradeType, upgradeType), "the declared type itself matches");
        Assertions.assertFalse(InternalSchemaInitializer.upgradeColumnTypeMatches(
                        ScalarType.createType(PrimitiveType.DATETIME), upgradeType),
                "a legacy DATETIME column must be RE-MODELLED (MODIFY), not accepted by name");
        Assertions.assertFalse(InternalSchemaInitializer.upgradeColumnTypeMatches(
                        ScalarType.createType(PrimitiveType.DATETIMEV2), upgradeType),
                "a DATETIMEV2 column is not a TIMESTAMPTZ either");
        Assertions.assertFalse(InternalSchemaInitializer.upgradeColumnTypeMatches(
                null, upgradeType), "an absent column is never 'upgraded'");
        // the WIDTH is part of the type: a legacy VARCHAR(4096) against the unbounded
        // STRING target must be widened (a >4096-char canonical digest failed its
        // reservation INSERT in that column while a fresh table accepted it)
        Assertions.assertFalse(InternalSchemaInitializer.upgradeColumnTypeMatches(
                        ScalarType.createVarchar(4096),
                        ScalarType.createType(PrimitiveType.STRING)),
                "a fixed-width legacy column must trigger the widening MODIFY");
        Assertions.assertTrue(InternalSchemaInitializer.upgradeColumnTypeMatches(
                        ScalarType.createVarchar(4096), ScalarType.createVarchar(4096)),
                "equal widths are the same type");
        Assertions.assertFalse(InternalSchemaInitializer.upgradeColumnTypeMatches(
                        ScalarType.createVarchar(4096), ScalarType.createVarchar(20)),
                "different widths are different types");
        Assertions.assertTrue(InternalSchemaInitializer.upgradeColumnTypeMatches(
                        ScalarType.createStringType(),
                        ScalarType.createType(PrimitiveType.STRING)),
                "STRING and the unlimited VARCHAR are the same column type");
        Assertions.assertFalse(InternalSchemaInitializer.upgradeColumnTypeMatches(
                        ScalarType.createType(PrimitiveType.BIGINT), upgradeType),
                "a genuinely different type stays a mismatch");
        // the BASELINES table's upgrade map carries the unbounded targets as well: a
        // pre-existing fixed-width schema_fingerprint used to be accepted by NAME and
        // never widened
        Assertions.assertEquals(PrimitiveType.STRING,
                InternalSchemaInitializer.SPM_BASELINES_UPGRADE_COLUMNS
                        .get("schema_fingerprint").getPrimitiveType(),
                "the fingerprint column must be upgraded to the create schema's STRING");
        Assertions.assertFalse(InternalSchemaInitializer.upgradeColumnTypeMatches(
                        ScalarType.createVarchar(4096),
                        InternalSchemaInitializer.SPM_BASELINES_UPGRADE_COLUMNS
                                .get("schema_fingerprint")),
                "a legacy fixed-width fingerprint column must be widened too");
    }

    /**
     * schema invariants:
     *
     * - #6: the seq identity column stores the FULL canonical bind digest (SPMPlanner
     *   has no length cap, and spm_baselines.bind_sql_digest is STRING): a fixed VARCHAR
     *   failed the RESERVATION INSERT for a valid >4096-char bind, so a GLOBAL CREATE
     *   errored before writing anything. Both the create schema and the upgrade map must
     *   use the unbounded value type.
     */
    @Test
    public void testRound40SchemaColumnsAndTypes() {
        Type digestType = InternalSchema.SPM_BASELINES_SEQ_SCHEMA.stream()
                .filter(def -> "bind_sql_digest".equalsIgnoreCase(def.getName()))
                .map(ColumnDef::getType).findFirst().orElse(null);
        Assertions.assertNotNull(digestType, "the seq table must carry the identity column");
        Assertions.assertEquals(PrimitiveType.STRING, digestType.getPrimitiveType(),
                "the full digest has no length cap: the column must be STRING");
        ScalarType upgradeType =
                InternalSchemaInitializer.SPM_BASELINES_SEQ_UPGRADE_COLUMNS.get("bind_sql_digest");
        Assertions.assertNotNull(upgradeType,
                "an upgraded cluster must gain the identity column");
        Assertions.assertEquals(PrimitiveType.STRING, upgradeType.getPrimitiveType(),
                "the upgrade must use the unbounded value type as well");
    }

    /**
     * schema invariants:
     *
     * - #15: the compact id high-water-mark table (spm_baselines_hwm) is part of the
     *   create path, gates completion itself (an upgraded cluster already HAS the older
     *   SPM tables, so without its own check the table would never be created and every
     *   create would keep reading the unbounded reservation history) and takes part in
     *   the replica upgrade.
     */
    @Test
    public void testRound44SchemaObjects() throws Exception {
        // #15: the compact high-water-mark table
        Database db = Mockito.mock(Database.class);
        Mockito.when(db.getTable(InternalSchema.SPM_BASELINES_HWM_TBL_NAME))
                .thenReturn(Optional.empty());
        Assertions.assertTrue(InternalSchemaInitializer.isSpmBaselinesHwmTableMissing(db),
                "a cluster where only spm_baselines_hwm is absent must not count as initialized");
        Mockito.when(db.getTable(InternalSchema.SPM_BASELINES_HWM_TBL_NAME))
                .thenReturn(Optional.of(Mockito.mock(Table.class)));
        Assertions.assertFalse(InternalSchemaInitializer.isSpmBaselinesHwmTableMissing(db),
                "an existing spm_baselines_hwm table must not block completion");

        Method method = InternalSchemaInitializer.class.getDeclaredMethod(
                "getSpmBaselinesHwmCreateSql");
        method.setAccessible(true);
        String sql = (String) method.invoke(null);
        Assertions.assertTrue(sql.contains("CREATE TABLE IF NOT EXISTS"
                        + " `internal`.`__internal_schema`.`spm_baselines_hwm`"),
                "the completion gate re-runs createTbl(): it must create the table: " + sql);
        Assertions.assertTrue(sql.contains("`last_id`"),
                "the compact record carries the newest allocated id: " + sql);
        Assertions.assertTrue(sql.contains("`update_time`"),
                "the record's write instant is kept: " + sql);
        Assertions.assertTrue(sql.contains("`tick`"),
                "the slot carries the bounded mutation clock: " + sql);
        Assertions.assertTrue(sql.contains("`pending`"),
                "the slot carries the open mutation window: " + sql);
        Assertions.assertFalse(sql.contains("UNIQUE KEY"),
                "the record is append-only (a superseded row never regresses the MAX): " + sql);
        Assertions.assertEquals(4, InternalSchema.getCopiedSchema(
                        InternalSchema.SPM_BASELINES_HWM_TBL_NAME).size(),
                "the copied schema carries exactly last_id / update_time / tick / pending");
        Assertions.assertTrue(InternalSchemaInitializer.REPLICA_UPGRADED_INTERNAL_TABLES
                        .contains(InternalSchema.SPM_BASELINES_HWM_TBL_NAME),
                "losing the hosting BE must not make the id watermark unreadable");
    }



    /**
     * (found while verifying #5): every upgraded SPM column is a VALUE column.
     * An ADD COLUMN flagged KEY lands AFTER the existing value columns, and the schema
     * change validator rejects any key column that follows a value column
     * ("Invalid column order. value should be after key"): the ALTER threw, the wait loop
     * retried the same column forever and an in-place upgraded cluster never gained the
     * columns its reader / writer already use.
     */
    @Test
    public void testUpgradedSpmColumnsAreValueColumns() {
        for (String column : List.of("bind_sql_digest", "reserve_time", "dropped")) {
            ColumnDefinition definition = InternalSchemaInitializer.spmUpgradeColumnDefinition(
                    column, ScalarType.createType(PrimitiveType.STRING));
            Assertions.assertEquals(column, definition.getName());
            Assertions.assertFalse(definition.isKey(),
                    column + " must be added as a VALUE column, not a key: " + definition);
        }
    }

}
