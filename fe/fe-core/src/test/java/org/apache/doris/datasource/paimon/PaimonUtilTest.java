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

package org.apache.doris.datasource.paimon;

import org.apache.doris.catalog.Type;
import org.apache.doris.thrift.TPrimitiveType;
import org.apache.doris.thrift.schema.external.TFieldPtr;
import org.apache.doris.thrift.schema.external.TSchema;

import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.VarCharType;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class PaimonUtilTest {
    private static final String TABLE_READ_SEQUENCE_NUMBER_ENABLED = "table-read.sequence-number.enabled";

    @Test
    public void testSchemaForVarcharAndChar() {
        DataField c1 = new DataField(1, "c1", new VarCharType(32));
        DataField c2 = new DataField(2, "c2", new CharType(14));
        Type type1 = PaimonUtil.paimonTypeToDorisType(c1.type(), true, true);
        Type type2 = PaimonUtil.paimonTypeToDorisType(c2.type(), true, true);
        Assert.assertTrue(type1.isVarchar());
        Assert.assertEquals(32, type1.getLength());
        Assert.assertEquals(14, type2.getLength());
    }

    @Test
    public void testBinlogHistorySchemaWithSequenceNumber() {
        PaimonSysExternalTable binlogTable = Mockito.mock(PaimonSysExternalTable.class);
        Mockito.when(binlogTable.getSysTableType()).thenReturn("binlog");
        Mockito.when(binlogTable.getTableProperties()).thenReturn(
                Collections.singletonMap(TABLE_READ_SEQUENCE_NUMBER_ENABLED, "true"));
        Mockito.when(binlogTable.getName()).thenReturn("mock_binlog");

        List<DataField> sourceFields = Arrays.asList(
                new DataField(0, "id", DataTypes.INT()),
                new DataField(1, "name", DataTypes.STRING()));
        TableSchema sourceSchema = new TableSchema(1L, sourceFields, 1, Collections.emptyList(),
                Collections.emptyList(), Collections.emptyMap(), "");
        TSchema historySchema = PaimonUtil.getHistorySchemaInfo(binlogTable, sourceSchema, true, true);
        List<TFieldPtr> fields = historySchema.getRootField().getFields();

        Assert.assertEquals("rowkind", fields.get(0).getFieldPtr().getName());
        Assert.assertEquals("_SEQUENCE_NUMBER", fields.get(1).getFieldPtr().getName());
        Assert.assertEquals("id", fields.get(2).getFieldPtr().getName());
        Assert.assertEquals(TPrimitiveType.ARRAY, fields.get(2).getFieldPtr().getType().getType());
        Assert.assertEquals("name", fields.get(3).getFieldPtr().getName());
        Assert.assertEquals(TPrimitiveType.ARRAY, fields.get(3).getFieldPtr().getType().getType());
    }

    @Test
    public void testAuditLogHistorySchemaWithoutSequenceNumber() {
        PaimonSysExternalTable auditLogTable = Mockito.mock(PaimonSysExternalTable.class);
        Mockito.when(auditLogTable.getSysTableType()).thenReturn("audit_log");
        Mockito.when(auditLogTable.getTableProperties()).thenReturn(Collections.emptyMap());
        Mockito.when(auditLogTable.getName()).thenReturn("mock_audit_log");

        List<DataField> sourceFields = Arrays.asList(
                new DataField(0, "id", DataTypes.INT()),
                new DataField(1, "name", DataTypes.STRING()));
        TableSchema sourceSchema = new TableSchema(1L, sourceFields, 1, Collections.emptyList(),
                Collections.emptyList(), Collections.emptyMap(), "");
        TSchema historySchema = PaimonUtil.getHistorySchemaInfo(auditLogTable, sourceSchema, true, true);
        List<TFieldPtr> fields = historySchema.getRootField().getFields();

        Assert.assertEquals(3, fields.size());
        Assert.assertEquals("rowkind", fields.get(0).getFieldPtr().getName());
        Assert.assertEquals("id", fields.get(1).getFieldPtr().getName());
        Assert.assertEquals("name", fields.get(2).getFieldPtr().getName());
    }
}
