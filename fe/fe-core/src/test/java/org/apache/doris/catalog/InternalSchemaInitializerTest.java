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

import org.apache.doris.analysis.AlterClause;
import org.apache.doris.analysis.ColumnDef;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.analysis.ModifyColumnClause;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.plugin.audit.AuditLoader;
import org.apache.doris.statistics.StatisticConstants;

import com.google.common.collect.Lists;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.List;

class InternalSchemaInitializerTest {
    @Test
    public void testGetModifyColumn() {
        new MockUp<HMSExternalTable>() {
            @Mock
            public HMSExternalTable.DLAType getDlaType() {
                return HMSExternalTable.DLAType.HUDI;
            }
        };

        InternalSchemaInitializer initializer = new InternalSchemaInitializer();
        OlapTable table = new OlapTable();
        Column key1 = new Column("key1", ScalarType.createVarcharType(100), true, null, false, null, "");
        Column key2 = new Column("key2", ScalarType.createVarcharType(100), true, null, true, null, "");
        Column key3 = new Column("key3", ScalarType.createVarcharType(1024), true, null, null, "");
        Column key4 = new Column("key4", ScalarType.createVarcharType(1025), true, null, null, "");
        Column key5 = new Column("key5", ScalarType.INT, true, null, null, "");
        Column value1 = new Column("value1", ScalarType.INT, false, null, null, "");
        Column value2 = new Column("value2", ScalarType.createVarcharType(100), false, null, null, "");
        List<Column> schema = Lists.newArrayList();
        schema.add(key1);
        schema.add(key2);
        schema.add(key3);
        schema.add(key4);
        schema.add(key5);
        schema.add(value1);
        schema.add(value2);
        table.fullSchema = schema;
        List<AlterClause> modifyColumnClauses = initializer.getModifyColumnClauses(table);
        Assertions.assertEquals(2, modifyColumnClauses.size());
        ModifyColumnClause clause1 = (ModifyColumnClause) modifyColumnClauses.get(0);
        Assertions.assertEquals("key1", clause1.getColumn().getName());
        Assertions.assertEquals(StatisticConstants.MAX_NAME_LEN, clause1.getColumn().getType().getLength());
        Assertions.assertFalse(clause1.getColumn().isAllowNull());

        ModifyColumnClause clause2 = (ModifyColumnClause) modifyColumnClauses.get(1);
        Assertions.assertEquals("key2", clause2.getColumn().getName());
        Assertions.assertEquals(StatisticConstants.MAX_NAME_LEN, clause2.getColumn().getType().getLength());
        Assertions.assertTrue(clause2.getColumn().isAllowNull());
    }

    @Test
    public void testAuditLogSchemaContainsStorageFields() {
        boolean hasLocalStorageField = false;
        boolean hasRemoteStorageField = false;

        for (ColumnDef columnDef : InternalSchema.AUDIT_SCHEMA) {
            if (columnDef.getName().equals("scan_bytes_from_local_storage")) {
                hasLocalStorageField = true;
                Assertions.assertEquals(PrimitiveType.BIGINT, columnDef.getTypeDef().getType().getPrimitiveType());
                Assertions.assertTrue(columnDef.isAllowNull());
            }

            if (columnDef.getName().equals("scan_bytes_from_remote_storage")) {
                hasRemoteStorageField = true;
                Assertions.assertEquals(PrimitiveType.BIGINT, columnDef.getTypeDef().getType().getPrimitiveType());
                Assertions.assertTrue(columnDef.isAllowNull());
            }
        }

        Assertions.assertTrue(hasLocalStorageField, "scan_bytes_from_local_storage field is missing from AUDIT_SCHEMA");
        Assertions.assertTrue(hasRemoteStorageField,
                "scan_bytes_from_remote_storage field is missing from AUDIT_SCHEMA");
    }

    @Test
    public void testAuditLogTableCreationWithStorageFields() throws Exception {
        Method buildAuditTblStmtMethod = InternalSchemaInitializer.class.getDeclaredMethod("buildAuditTblStmt");
        buildAuditTblStmtMethod.setAccessible(true);

        CreateTableStmt createTableStmt = (CreateTableStmt) buildAuditTblStmtMethod.invoke(null);

        List<Column> columns = createTableStmt.getColumns();

        boolean hasLocalStorageField = false;
        boolean hasRemoteStorageField = false;

        for (Column column : columns) {
            if (column.getName().equals("scan_bytes_from_local_storage")) {
                hasLocalStorageField = true;
                Assertions.assertEquals(PrimitiveType.BIGINT, column.getType().getPrimitiveType());
                Assertions.assertTrue(column.isAllowNull());
            }

            if (column.getName().equals("scan_bytes_from_remote_storage")) {
                hasRemoteStorageField = true;
                Assertions.assertEquals(PrimitiveType.BIGINT, column.getType().getPrimitiveType());
                Assertions.assertTrue(column.isAllowNull());
            }
        }

        Assertions.assertTrue(hasLocalStorageField,
                "scan_bytes_from_local_storage field is missing from the created audit log table");
        Assertions.assertTrue(hasRemoteStorageField,
                "scan_bytes_from_remote_storage field is missing from the created audit log table");
    }

    @Test
    public void testGetCopiedSchemaForAuditLog() throws UserException {
        List<ColumnDef> copiedSchema = InternalSchema.getCopiedSchema(AuditLoader.AUDIT_LOG_TABLE);

        boolean hasLocalStorageField = false;
        boolean hasRemoteStorageField = false;

        for (ColumnDef columnDef : copiedSchema) {
            if (columnDef.getName().equals("scan_bytes_from_local_storage")) {
                hasLocalStorageField = true;
                Assertions.assertEquals(PrimitiveType.BIGINT, columnDef.getTypeDef().getType().getPrimitiveType());
                Assertions.assertTrue(columnDef.isAllowNull());
            }

            if (columnDef.getName().equals("scan_bytes_from_remote_storage")) {
                hasRemoteStorageField = true;
                Assertions.assertEquals(PrimitiveType.BIGINT, columnDef.getTypeDef().getType().getPrimitiveType());
                Assertions.assertTrue(columnDef.isAllowNull());
            }
        }

        Assertions.assertTrue(hasLocalStorageField,
                "scan_bytes_from_local_storage field is missing from the copied schema");
        Assertions.assertTrue(hasRemoteStorageField,
                "scan_bytes_from_remote_storage field is missing from the copied schema");
    }
}
