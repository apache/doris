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
import org.apache.doris.analysis.ColumnNullableType;
import org.apache.doris.common.UserException;
import org.apache.doris.nereids.trees.plans.commands.info.AddColumnOp;
import org.apache.doris.nereids.trees.plans.commands.info.AddColumnsOp;
import org.apache.doris.nereids.trees.plans.commands.info.AlterTableOp;
import org.apache.doris.nereids.trees.plans.commands.info.ModifyColumnOp;
import org.apache.doris.nereids.trees.plans.commands.info.ReorderColumnsOp;
import org.apache.doris.plugin.audit.AuditLoader;
import org.apache.doris.statistics.StatisticConstants;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
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

    // An up-to-date table, also one with columns beyond the schema after it, needs nothing.
    @Test
    public void testAuditTableUpToDate() {
        List<ColumnDef> schema = schema("a", "b", "c");
        Assertions.assertEquals(Collections.emptyList(), describe(alterOps(schema, "a", "b", "c")));
        Assertions.assertEquals(Collections.emptyList(), describe(alterOps(schema, "a", "b", "c", "x")));
    }

    // The columns the table has of the schema are in order and come first: each missing column is added right
    // after the column before it in the schema, by a light schema change.
    @Test
    public void testAuditTableMissingColumnsAddedInPlace() {
        List<ColumnDef> schema = schema("a", "b", "c", "d", "e");
        Assertions.assertEquals(Lists.newArrayList("ADD b AFTER a", "ADD d AFTER c"),
                describe(alterOps(schema, "a", "c", "e")));
        Assertions.assertEquals(Lists.newArrayList("ADD e AFTER d"), describe(alterOps(schema, "a", "b", "c", "d")));
        // the second of two adjacent columns follows the first, which the same ALTER adds
        Assertions.assertEquals(Lists.newArrayList("ADD b AFTER a", "ADD c AFTER b"),
                describe(alterOps(schema, "a", "d", "e")));
        // a column beyond the schema stays at the end
        Assertions.assertEquals(Lists.newArrayList("ADD b AFTER a", "ADD d AFTER c"),
                describe(alterOps(schema, "a", "c", "e", "x")));
    }

    // Any other table gets the missing columns appended and is reordered, which rewrites it.
    @Test
    public void testAuditTableOutOfOrderReordered() {
        List<ColumnDef> schema = schema("a", "b", "c", "d", "e");
        // two columns of the schema swapped
        Assertions.assertEquals(Lists.newArrayList("ADD (d)", "ORDER BY (a, b, c, d, e)"),
                describe(alterOps(schema, "a", "c", "b", "e")));
        Assertions.assertEquals(Lists.newArrayList("ORDER BY (a, b, c, d, e)"),
                describe(alterOps(schema, "b", "a", "c", "d", "e")));
        // a column beyond the schema between two of it
        Assertions.assertEquals(Lists.newArrayList("ADD (b)", "ORDER BY (a, b, c, d, e, x)"),
                describe(alterOps(schema, "a", "x", "c", "d", "e")));
        // the first column missing: there is no column for it to follow
        Assertions.assertEquals(Lists.newArrayList("ADD (a)", "ORDER BY (a, b, c, d, e)"),
                describe(alterOps(schema, "b", "c", "d", "e")));
    }

    // A table 4.1.3 created lacks the four columns added since. Each is added in place, as the audit schema
    // defines it.
    @Test
    public void testAuditTableOf413AddedInPlace() {
        List<String> added = Lists.newArrayList("queue_time_ms", "spill_write_bytes_to_remote_storage",
                "spill_read_bytes_from_remote_storage", "protocol");
        List<String> currentColumnNames = auditColumnNames();
        currentColumnNames.removeAll(added);

        List<AlterTableOp> ops = InternalSchemaInitializer.getAuditTableAlterOps(currentColumnNames,
                InternalSchema.AUDIT_SCHEMA);

        Assertions.assertEquals(Lists.newArrayList("ADD queue_time_ms AFTER query_time",
                "ADD spill_write_bytes_to_remote_storage AFTER spill_read_bytes_from_local_storage",
                "ADD spill_read_bytes_from_remote_storage AFTER spill_write_bytes_to_remote_storage",
                "ADD protocol AFTER compute_group"), describe(ops));
        for (AlterTableOp op : ops) {
            Column column = ((AddColumnOp) op).getColumn();
            ColumnDef columnDef = InternalSchema.AUDIT_SCHEMA.stream()
                    .filter(def -> def.getName().equals(column.getName())).findFirst().get();
            Assertions.assertEquals(columnDef.getType(), column.getType());
            Assertions.assertEquals(columnDef.isAllowNull(), column.isAllowNull());
            Assertions.assertFalse(column.isKey());
        }
    }

    // A table 3.0 created has its columns in the order the audit schema had before #51363 moved them around. The
    // missing columns are appended and the table is reordered.
    @Test
    public void testAuditTableOf30Reordered() {
        List<String> currentColumnNames = Lists.newArrayList("query_id", "time", "client_ip", "user", "catalog", "db",
                "state", "error_code", "error_message", "query_time", "scan_bytes", "scan_rows", "return_rows",
                "shuffle_send_rows", "shuffle_send_bytes", "scan_bytes_from_local_storage",
                "scan_bytes_from_remote_storage", "stmt_id", "stmt_type", "is_query", "is_nereids", "frontend_ip",
                "cpu_time_ms", "sql_hash", "sql_digest", "peak_memory_bytes", "workload_group", "compute_group",
                "stmt");
        List<String> missing = auditColumnNames();
        missing.removeAll(currentColumnNames);

        List<AlterTableOp> ops = InternalSchemaInitializer.getAuditTableAlterOps(currentColumnNames,
                InternalSchema.AUDIT_SCHEMA);

        Assertions.assertEquals(Lists.newArrayList("ADD (" + String.join(", ", missing) + ")",
                "ORDER BY (" + String.join(", ", auditColumnNames()) + ")"), describe(ops));
    }

    private static List<ColumnDef> schema(String... columnNames) {
        return Arrays.stream(columnNames).map(name -> new ColumnDef(name, ScalarType.createType(PrimitiveType.BIGINT),
                ColumnNullableType.NULLABLE)).collect(Collectors.toList());
    }

    private static List<AlterTableOp> alterOps(List<ColumnDef> schema, String... currentColumnNames) {
        return InternalSchemaInitializer.getAuditTableAlterOps(Lists.newArrayList(currentColumnNames), schema);
    }

    private static List<String> auditColumnNames() {
        return InternalSchema.AUDIT_SCHEMA.stream().map(ColumnDef::getName).collect(Collectors.toList());
    }

    private static List<String> describe(List<AlterTableOp> ops) {
        return ops.stream().map(op -> {
            if (op instanceof AddColumnOp) {
                AddColumnOp addColumnOp = (AddColumnOp) op;
                return "ADD " + addColumnOp.getColumn().getName() + " AFTER " + addColumnOp.getColPos().getLastCol();
            }
            if (op instanceof AddColumnsOp) {
                return "ADD (" + ((AddColumnsOp) op).getColumns().stream().map(Column::getName)
                        .collect(Collectors.joining(", ")) + ")";
            }
            return "ORDER BY (" + String.join(", ", ((ReorderColumnsOp) op).getColumnsByPos()) + ")";
        }).collect(Collectors.toList());
    }
}
