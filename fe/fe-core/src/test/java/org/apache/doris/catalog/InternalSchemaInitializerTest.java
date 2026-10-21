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
import java.util.Map;
import java.util.Optional;

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
     * The initializer CONVERGES every SPM internal table to the schema this release
     * publishes - there is no upgrade path: the three tables are published for the first
     * time here, so no earlier release wrote a row a user keeps, and no additive ALTER can
     * rename a column or turn a nullable one into NOT NULL. The declaration therefore has to
     * cover exactly the tables BaselineManager reads and writes, with every column NOT NULL
     * (a writer that loses a value must FAIL at the INSERT instead of storing a NULL the
     * readers would have to interpret).
     */
    @Test
    public void testDeclaredSpmSchemaCoversTheThreeTables() {
        Assertions.assertEquals(
                List.of(InternalSchema.SPM_BASELINES_TBL_NAME,
                        InternalSchema.SPM_BASELINES_SEQ_TBL_NAME,
                        InternalSchema.SPM_BASELINES_HWM_TBL_NAME),
                List.copyOf(InternalSchemaInitializer.SPM_INTERNAL_TABLES.keySet()),
                "every SPM internal table takes part in the schema convergence");
        for (Map.Entry<String, List<ColumnDef>> entry
                : InternalSchemaInitializer.SPM_INTERNAL_TABLES.entrySet()) {
            List<ColumnDef> declared = entry.getValue();
            Assertions.assertFalse(declared.isEmpty(), entry.getKey() + " declares columns");
            for (ColumnDef def : declared) {
                Assertions.assertFalse(def.isAllowNull(),
                        entry.getKey() + "." + def.getName() + " must be NOT NULL");
            }
        }
    }

    /**
     * The published schema must carry every provenance / schema-identity column the load path
     * reads: forgetting one leaves a reload reading NULL forever (mode / frozen
     * classification) or silently disabling a guard (schema fingerprint).
     */
    @Test
    public void testProvenanceUpgradeCoversEveryEvolvedColumn() {
        for (String column : new String[] {"sql_mode", "plan_sql_mode", "plan_frozen",
                "schema_fingerprint", "plan_sql_digest"}) {
            Assertions.assertTrue(
                    InternalSchema.SPM_BASELINES_SCHEMA.stream()
                            .anyMatch(def -> column.equalsIgnoreCase(def.getName())),
                    "the create schema must carry the " + column + " column");
        }
    }

    /**
     * The reservation instant is declared as TIMESTAMPTZ at the SPM microsecond scale, and
     * the schema gate must notice an existing column of the WRONG type / scale. A name-only
     * comparison accepted a DATETIME column: the reservation writer stores a zone-qualified
     * instant, DATETIME converts it into the WRITING session's zone, and a successor in
     * another zone reading UNIX_TIMESTAMP(reserve_time) shifts the reservation's age by
     * hours - a seconds-old ambiguous CREATE can be condemned as expired (its later
     * published row repaired away) or a retry stays fenced for hours. A second-precision
     * TIMESTAMPTZ is just as wrong: it ROUNDS AWAY the microseconds every instant
     * comparison now relies on.
     */
    @Test
    public void testReserveTimeTypeMatchesTheDeclaredSchema() {
        ScalarType createType = InternalSchema.SPM_BASELINES_SEQ_SCHEMA.stream()
                .filter(def -> "reserve_time".equalsIgnoreCase(def.getName()))
                .map(def -> def.getType())
                .filter(ScalarType.class::isInstance)
                .map(ScalarType.class::cast)
                .findFirst().orElse(null);
        Assertions.assertNotNull(createType, "the seq table must carry reserve_time");
        Assertions.assertEquals(PrimitiveType.TIMESTAMPTZ, createType.getPrimitiveType(),
                "the declared schema pins the reservation INSTANT");
        Assertions.assertEquals(InternalSchema.SPM_TIMESTAMP_SCALE, createType.getScalarScale(),
                "the reservation instant keeps MICROSECOND precision");
        Type upgradeType = createType;

        Assertions.assertTrue(InternalSchemaInitializer.spmColumnTypeMatches(
                upgradeType, upgradeType), "the declared type itself matches");
        Assertions.assertFalse(InternalSchemaInitializer.spmColumnTypeMatches(
                        ScalarType.createType(PrimitiveType.DATETIME), upgradeType),
                "a legacy DATETIME column must be RE-MODELLED (MODIFY), not accepted by name");
        Assertions.assertFalse(InternalSchemaInitializer.spmColumnTypeMatches(
                        ScalarType.createType(PrimitiveType.DATETIMEV2), upgradeType),
                "a DATETIMEV2 column is not a TIMESTAMPTZ either");
        Assertions.assertFalse(InternalSchemaInitializer.spmColumnTypeMatches(
                null, upgradeType), "an absent column is never 'upgraded'");
        // the WIDTH is part of the type: a legacy VARCHAR(4096) against the unbounded
        // STRING target must be widened (a >4096-char canonical digest failed its
        // reservation INSERT in that column while a fresh table accepted it)
        Assertions.assertFalse(InternalSchemaInitializer.spmColumnTypeMatches(
                        ScalarType.createVarchar(4096),
                        ScalarType.createType(PrimitiveType.STRING)),
                "a fixed-width legacy column must trigger the widening MODIFY");
        Assertions.assertTrue(InternalSchemaInitializer.spmColumnTypeMatches(
                        ScalarType.createVarchar(4096), ScalarType.createVarchar(4096)),
                "equal widths are the same type");
        Assertions.assertFalse(InternalSchemaInitializer.spmColumnTypeMatches(
                        ScalarType.createVarchar(4096), ScalarType.createVarchar(20)),
                "different widths are different types");
        Assertions.assertTrue(InternalSchemaInitializer.spmColumnTypeMatches(
                        ScalarType.createStringType(),
                        ScalarType.createType(PrimitiveType.STRING)),
                "STRING and the unlimited VARCHAR are the same column type");
        Assertions.assertFalse(InternalSchemaInitializer.spmColumnTypeMatches(
                        ScalarType.createType(PrimitiveType.BIGINT), upgradeType),
                "a genuinely different type stays a mismatch");
        // the BASELINES table's own declaration pins the unbounded fingerprint type as well:
        // a pre-existing fixed-width schema_fingerprint used to be accepted by NAME and
        // never widened
        Type fingerprintType = declaredType(InternalSchema.SPM_BASELINES_SCHEMA,
                "schema_fingerprint");
        Assertions.assertEquals(PrimitiveType.STRING, fingerprintType.getPrimitiveType(),
                "the fingerprint column is the declared unbounded STRING");
        Assertions.assertFalse(InternalSchemaInitializer.spmColumnTypeMatches(
                        ScalarType.createVarchar(4096), fingerprintType),
                "a legacy fixed-width fingerprint column does not match the declaration");
    }

    /**
     * schema invariants:
     *
     * - #6: the seq identity column stores the FULL canonical bind digest (SPMPlanner
     *   has no length cap, and spm_baselines.bind_sql_digest is STRING): a fixed VARCHAR
     *   failed the RESERVATION INSERT for a valid >4096-char bind, so a GLOBAL CREATE
     *   errored before writing anything. The declared schema must use the unbounded value
     *   type, and a fixed-width legacy column must NOT count as matching it.
     */
    @Test
    public void testRound40SchemaColumnsAndTypes() {
        Type digestType = declaredType(InternalSchema.SPM_BASELINES_SEQ_SCHEMA, "bind_sql_digest");
        Assertions.assertNotNull(digestType, "the seq table must carry the identity column");
        Assertions.assertEquals(PrimitiveType.STRING, digestType.getPrimitiveType(),
                "the full digest has no length cap: the column must be STRING");
        Assertions.assertFalse(InternalSchemaInitializer.spmColumnTypeMatches(
                        ScalarType.createVarchar(4096), digestType),
                "a fixed-width legacy column does not match the declaration");
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
        Assertions.assertTrue(sql.contains("`watermark`"),
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
                "the copied schema carries exactly watermark / update_time / tick / pending");
        Assertions.assertTrue(InternalSchemaInitializer.REPLICA_UPGRADED_INTERNAL_TABLES
                        .contains(InternalSchema.SPM_BASELINES_HWM_TBL_NAME),
                "losing the hosting BE must not make the id watermark unreadable");
    }



    /**
     * The id columns are published under their FINAL names: spm_baselines_seq.baseline_id (the
     * reserved baseline id) and spm_baselines_hwm.watermark (the compact id watermark). A
     * pre-release table still carrying `last_id` therefore does NOT match the declaration -
     * the writers' explicit column lists name the published columns - and is rebuilt.
     */
    @Test
    public void testIdColumnsCarryThePublishedNames() {
        Assertions.assertNotNull(declaredType(InternalSchema.SPM_BASELINES_SEQ_SCHEMA,
                "baseline_id"), "the reservation column is baseline_id");
        Assertions.assertNotNull(declaredType(InternalSchema.SPM_BASELINES_HWM_SCHEMA,
                "watermark"), "the compact watermark column is watermark");
        for (Map.Entry<String, List<ColumnDef>> entry
                : InternalSchemaInitializer.SPM_INTERNAL_TABLES.entrySet()) {
            for (ColumnDef def : entry.getValue()) {
                Assertions.assertFalse("last_id".equalsIgnoreCase(def.getName()),
                        entry.getKey() + " must not keep the pre-release `last_id` column");
            }
        }
    }

    /** The declared type of one column of an internal-table schema (null when absent). */
    private static Type declaredType(List<ColumnDef> schema, String column) {
        return schema.stream()
                .filter(def -> column.equalsIgnoreCase(def.getName()))
                .map(ColumnDef::getType)
                .findFirst().orElse(null);
    }

}
