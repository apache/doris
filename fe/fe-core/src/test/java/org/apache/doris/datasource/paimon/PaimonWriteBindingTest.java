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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.qe.ConnectContext;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.TypeUtils;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class PaimonWriteBindingTest {

    @Test
    public void testFullOverwriteUsesStaticSemanticsByDefault() {
        FileStoreTable table = Mockito.mock(FileStoreTable.class);
        FileStoreTable configuredTable = Mockito.mock(FileStoreTable.class);
        Mockito.when(table.options()).thenReturn(Collections.emptyMap());
        Mockito.when(table.copy(Collections.singletonMap(
                CoreOptions.DYNAMIC_PARTITION_OVERWRITE.key(), Boolean.FALSE.toString())))
                .thenReturn(configuredTable);

        Assert.assertSame(configuredTable, PaimonWriteBinding.configureTableForWrite(
                table, true, Collections.emptyMap()));
    }

    @Test
    public void testFullOverwriteHonorsExplicitDynamicSemantics() {
        FileStoreTable table = Mockito.mock(FileStoreTable.class);
        Mockito.when(table.options()).thenReturn(Collections.singletonMap(
                CoreOptions.DYNAMIC_PARTITION_OVERWRITE.key(), Boolean.TRUE.toString()));

        Assert.assertSame(table, PaimonWriteBinding.configureTableForWrite(
                table, true, Collections.emptyMap()));
        Mockito.verify(table, Mockito.never()).copy(Mockito.anyMap());
    }

    @Test
    public void testStaticPartitionAlwaysUsesStaticSemantics() {
        FileStoreTable table = Mockito.mock(FileStoreTable.class);
        FileStoreTable configuredTable = Mockito.mock(FileStoreTable.class);
        Mockito.when(table.options()).thenReturn(Collections.singletonMap(
                CoreOptions.DYNAMIC_PARTITION_OVERWRITE.key(), Boolean.TRUE.toString()));
        Mockito.when(table.copy(Collections.singletonMap(
                CoreOptions.DYNAMIC_PARTITION_OVERWRITE.key(), Boolean.FALSE.toString())))
                .thenReturn(configuredTable);

        Assert.assertSame(configuredTable, PaimonWriteBinding.configureTableForWrite(
                table, true, Collections.singletonMap("pt", "1")));
    }

    @Test
    public void testStaticPartitionUsesValueAfterTargetTypeCast()
            throws Exception {
        FileStoreTable table = mockPartitionTable(
                Collections.emptyMap(), DataTypes.FIELD(0, "part", DataTypes.INT()));
        Map<String, org.apache.doris.catalog.Type> writeTypes =
                Collections.singletonMap("part", Type.INT);
        Map<String, Expression> partition =
                Collections.singletonMap("part", BooleanLiteral.TRUE);

        Map<String, String> resolved = PaimonWriteBinding.resolveStaticPartition(
                table, writeTypes, partition, true);

        Assert.assertEquals("1", resolved.get("part"));
    }

    @Test
    public void testStaticPartitionNullUsesPaimonDefaultName()
            throws Exception {
        String defaultName = "__CUSTOM_DEFAULT__";
        FileStoreTable table = mockPartitionTable(
                Collections.singletonMap("partition.default-name", defaultName),
                DataTypes.FIELD(0, "part", DataTypes.STRING()));

        Map<String, String> resolved = PaimonWriteBinding.resolveStaticPartition(
                table,
                Collections.singletonMap("part", Type.STRING),
                Collections.singletonMap("part", new NullLiteral()),
                true);

        Assert.assertEquals(defaultName, resolved.get("part"));
    }

    @Test
    public void testStaticOverwriteRejectsLiteralDefaultName()
            throws Exception {
        String defaultName = "__CUSTOM_DEFAULT__";
        FileStoreTable table = mockPartitionTable(
                Collections.singletonMap("partition.default-name", defaultName),
                DataTypes.FIELD(0, "part", DataTypes.STRING()));

        AnalysisException exception = Assert.assertThrows(
                AnalysisException.class,
                () -> PaimonWriteBinding.resolveStaticPartition(
                        table,
                        Collections.singletonMap("part", Type.STRING),
                        Collections.singletonMap(
                                "part", new StringLiteral(defaultName)),
                        true));

        Assert.assertTrue(exception.getMessage().contains("cannot be represented"));
    }

    @Test
    public void testStaticLtzPartitionUsesSdkZoneAndPaimonFormat()
            throws Exception {
        ConnectContext previousContext = ConnectContext.get();
        ConnectContext context = new ConnectContext();
        context.getSessionVariable().setTimeZone("Asia/Shanghai");
        context.setThreadLocalInfo();
        try {
            FileStoreTable table = mockPartitionTable(
                    Collections.emptyMap(),
                    DataTypes.FIELD(0, "part",
                            DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(6)));
            String input = "2024-01-15 08:30:45.123456";

            Map<String, String> resolved = PaimonWriteBinding.resolveStaticPartition(
                    table,
                    Collections.singletonMap(
                            "part", org.apache.doris.catalog.ScalarType.createDatetimeV2Type(6)),
                    Collections.singletonMap(
                            "part", new DateTimeV2Literal(input)),
                    true);

            String expected = LocalDateTime.parse(
                            input.replace(' ', 'T'), DateTimeFormatter.ISO_LOCAL_DATE_TIME)
                    .atZone(ZoneId.of("Asia/Shanghai"))
                    .withZoneSameInstant(ZoneId.systemDefault())
                    .toLocalDateTime()
                    .format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
                    .replace('T', ' ');
            String value = resolved.get("part");
            Assert.assertEquals(expected, value);
            Assert.assertFalse(value.contains("T"));
            Assert.assertNotNull(TypeUtils.castFromString(
                    value, table.rowType().getTypeAt(0)));
        } finally {
            if (previousContext == null) {
                ConnectContext.remove();
            } else {
                previousContext.setThreadLocalInfo();
            }
        }
    }

    private static FileStoreTable mockPartitionTable(
            Map<String, String> options,
            org.apache.paimon.types.DataField field) {
        FileStoreTable table = Mockito.mock(FileStoreTable.class);
        Mockito.when(table.partitionKeys())
                .thenReturn(Collections.singletonList(field.name()));
        Mockito.when(table.rowType()).thenReturn(DataTypes.ROW(field));
        Mockito.when(table.options()).thenReturn(new HashMap<>(options));
        return table;
    }
}
