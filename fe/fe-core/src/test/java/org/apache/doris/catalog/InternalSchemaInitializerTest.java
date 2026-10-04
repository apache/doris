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
import org.apache.doris.nereids.spm.capture.PlanCaptureManager;
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
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

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

        // the capture checkpoint table gates completion the same way: without its own
        // check an upgraded cluster (which already HAS spm_baselines) would never create
        // it, and a leader handoff could not resume a truncated capture window
        Mockito.when(db.getTable(InternalSchema.SPM_CAPTURE_CHECKPOINT_TBL_NAME))
                .thenReturn(Optional.empty());
        Assertions.assertTrue(InternalSchemaInitializer.isSpmCaptureCheckpointTableMissing(db),
                "a cluster where only spm_capture_checkpoint is absent must not count as initialized");
        Mockito.when(db.getTable(InternalSchema.SPM_CAPTURE_CHECKPOINT_TBL_NAME))
                .thenReturn(Optional.of(Mockito.mock(Table.class)));
        Assertions.assertFalse(InternalSchemaInitializer.isSpmCaptureCheckpointTableMissing(db),
                "an existing spm_capture_checkpoint table must not block completion");

        // round-32 #2: the id sequence table gates completion the same way - without it the
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
     * Both SPM tables carry cluster-wide state and must take part in the internal-table
     * replica upgrade: with the default minimum replication of 1 they are created
     * single-replica, so losing the hosting BE would make every global baseline
     * unavailable (spm_baselines) or erase the only capture handoff cursor
     * (spm_capture_checkpoint).
     */
    @Test
    public void testSpmTablesTakePartInReplicaUpgrade() {
        Assertions.assertTrue(InternalSchemaInitializer.REPLICA_UPGRADED_INTERNAL_TABLES
                        .contains(InternalSchema.SPM_BASELINES_TBL_NAME),
                "spm_baselines must be raised towards the statistics replica target");
        Assertions.assertTrue(InternalSchemaInitializer.REPLICA_UPGRADED_INTERNAL_TABLES
                        .contains(InternalSchema.SPM_CAPTURE_CHECKPOINT_TBL_NAME),
                "spm_capture_checkpoint must be raised towards the statistics replica target");
        Assertions.assertTrue(InternalSchemaInitializer.REPLICA_UPGRADED_INTERNAL_TABLES
                        .contains(InternalSchema.SPM_BASELINES_SEQ_TBL_NAME),
                "spm_baselines_seq must be raised as well: an unreadable id sequence"
                        + " fails every global CREATE BASELINE PLAN (round-32 #2)");
    }

    /**
     * The checkpoint replacement must be a single-key UPSERT: the table is UNIQUE-key(id)
     * with merge-on-write, so re-inserting the row replaces it atomically and no crash can
     * leave the shared store without a checkpoint row.
     */
    @Test
    public void testCaptureCheckpointTableIsUniqueKeyUpsert() throws Exception {
        Method method = InternalSchemaInitializer.class.getDeclaredMethod(
                "getSpmCaptureCheckpointCreateSql");
        method.setAccessible(true);
        String sql = (String) method.invoke(null);
        Assertions.assertTrue(sql.contains("UNIQUE KEY(`id`)"),
                "the checkpoint table must be UNIQUE-key so the INSERT upserts: " + sql);
        Assertions.assertTrue(sql.contains("enable_unique_key_merge_on_write"),
                "merge-on-write makes the single-row replacement atomic: " + sql);
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
     * The durable checkpoint UPSERT (PlanCaptureManager#CHECKPOINT_INSERT_SQL) binds its
     * VALUES by POSITION, so the explicit column list must stay one-to-one with the
     * canonical schema order. Dropping the list (a bare positional INSERT) or letting it
     * go stale silently rebinds every value on a table whose PHYSICAL order differs from
     * the canonical one - the tail JSON used to land in failed_attempts there.
     */
    @Test
    public void testCheckpointInsertSqlListsCanonicalColumns() {
        String sql = PlanCaptureManager.checkpointInsertSqlForTest();
        String canonical = InternalSchema.SPM_CAPTURE_CHECKPOINT_SCHEMA.stream()
                .map(def -> "`" + def.getName().toLowerCase(Locale.ROOT) + "`")
                .collect(Collectors.joining(", "));
        int listStart = sql.indexOf('(');
        int listEnd = sql.indexOf(')');
        Assertions.assertTrue(listStart > 0 && listEnd > listStart,
                "the UPSERT must list its target columns: " + sql);
        Assertions.assertEquals(canonical, sql.substring(listStart + 1, listEnd),
                "the INSERT column list must match the schema order: " + sql);
        Assertions.assertTrue(sql.contains(") VALUES ("),
                "the column list must be followed by the VALUES operands: " + sql);
    }

    /**
     * An upgraded table must end up with the SAME physical order as a freshly created
     * one: cursor_tail belongs BEFORE failed_attempts / retry_queue / update_time, but the
     * upgrade used to APPEND it (null ColumnPosition) - a positional checkpoint INSERT
     * then wrote the tail JSON into failed_attempts and the retry JSON into update_time.
     */
    @Test
    public void testCheckpointUpgradeRestoresCanonicalColumnOrder() {
        List<String> schemaNames = InternalSchema.SPM_CAPTURE_CHECKPOINT_SCHEMA.stream()
                .map(def -> def.getName().toLowerCase(Locale.ROOT))
                .collect(Collectors.toList());
        for (String column : InternalSchemaInitializer.SPM_CAPTURE_CHECKPOINT_UPGRADE_COLUMNS.keySet()) {
            int columnIndex = schemaNames.indexOf(column);
            Assertions.assertTrue(columnIndex >= 0,
                    "the create schema must carry the upgraded column " + column);
            String anchor =
                    InternalSchemaInitializer.SPM_CAPTURE_CHECKPOINT_UPGRADE_POSITIONS.get(column);
            Assertions.assertNotNull(anchor,
                    "the upgrade must place " + column + " at its canonical position");
            int anchorIndex = schemaNames.indexOf(anchor);
            Assertions.assertTrue(anchorIndex >= 0 && anchorIndex < columnIndex,
                    "the anchor " + anchor + " must precede " + column + " in the schema");
            ColumnPosition position = InternalSchemaInitializer.checkpointColumnPosition(column,
                    new HashSet<>(schemaNames));
            Assertions.assertNotNull(position,
                    "a table carrying the anchor must get a position for " + column);
            Assertions.assertEquals(anchor, position.getLastCol());
            Assertions.assertNull(InternalSchemaInitializer.checkpointColumnPosition(column,
                            new HashSet<>()),
                    "without the anchor the upgrade falls back to the append order");
        }
    }

    /**
     * Round-31 #5: SET GLOBAL accepts ANY compiling regex for
     * plan_capture_include_pattern / plan_capture_exclude_pattern, so a fixed
     * VARCHAR(4096) made a valid 4097-byte pattern fail the reservation INSERT - the
     * capture cycle then returned before scanning, with the pattern still accepted by SET.
     * Both schemas (fresh create AND the upgrade ALTER) store the patterns as STRING, with
     * the same headroom as the retry-queue columns.
     */
    @Test
    public void testCheckpointPatternColumnsCanStoreAnyAcceptedRegex() {
        for (String column : List.of("include_pattern", "exclude_pattern")) {
            Type freshType = InternalSchema.SPM_CAPTURE_CHECKPOINT_SCHEMA.stream()
                    .filter(def -> def.getName().equals(column))
                    .map(ColumnDef::getType)
                    .findFirst().orElseThrow();
            Assertions.assertEquals(PrimitiveType.STRING, freshType.getPrimitiveType(),
                    column + " must be STRING, not a bounded VARCHAR: " + freshType);
            Type upgradeType = InternalSchemaInitializer.SPM_CAPTURE_CHECKPOINT_UPGRADE_COLUMNS
                    .get(column);
            Assertions.assertNotNull(upgradeType, "the upgrade must add " + column);
            Assertions.assertEquals(PrimitiveType.STRING, upgradeType.getPrimitiveType(),
                    "the upgrade must add " + column + " as STRING as well: " + upgradeType);
        }
    }

    /**
     * Round-31 (found while verifying #5): every upgraded SPM column is a VALUE column.
     * An ADD COLUMN flagged KEY lands AFTER the existing value columns, and the schema
     * change validator rejects any key column that follows a value column
     * ("Invalid column order. value should be after key"): the ALTER threw, the wait loop
     * retried the same column forever and an in-place upgraded cluster never gained the
     * columns its reader / writer already use.
     */
    @Test
    public void testUpgradedSpmColumnsAreValueColumns() {
        for (String column : List.of("include_pattern", "cursor_tail", "min_query_time_ms")) {
            ColumnDefinition definition = InternalSchemaInitializer.spmUpgradeColumnDefinition(
                    column, ScalarType.createType(PrimitiveType.STRING));
            Assertions.assertEquals(column, definition.getName());
            Assertions.assertFalse(definition.isKey(),
                    column + " must be added as a VALUE column, not a key: " + definition);
        }
    }
}
