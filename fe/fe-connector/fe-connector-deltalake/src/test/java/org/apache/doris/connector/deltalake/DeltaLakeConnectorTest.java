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


package org.apache.doris.connector.deltalake;

import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.ConnectorProvider;
import org.apache.doris.connector.spi.ConnectorTableSchema;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.mvcc.ConnectorMvccSnapshot;

import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.ServiceLoader;

class DeltaLakeConnectorTest {
    @TempDir
    Path warehouse;

    @Test
    void providerDiscoversTablesAndReadsSchemaFromDeltaLog() throws Exception {
        ConnectorProvider provider = ServiceLoader.load(ConnectorProvider.class).stream()
                .map(ServiceLoader.Provider::get).filter(value -> "deltalake".equals(value.getType()))
                .findFirst().orElseThrow();
        Assertions.assertNotNull(getClass().getResource("/" + provider.name() + ".conf.template"));
        DeltaLakeTestTables.commit(warehouse.resolve("db/table"), 0, "first",
                new StructType().add("ID", LongType.LONG), Collections.singletonList("ID"));
        Files.createDirectories(warehouse.resolve("db/parquet_only"));
        try (Connector connector = provider.create(Collections.singletonMap("warehouse", warehouse.toString()),
                new RecordingConnectorContext());
                ConnectorMetadata metadata = connector.getMetadata(null)) {
            Assertions.assertEquals(Collections.singletonList("db"), metadata.listDatabaseNames(null));
            Assertions.assertEquals(Collections.singletonList("table"), metadata.listTableNames(null, "db"));
            Assertions.assertTrue(metadata.getTableHandle(null, "db", "parquet_only").isEmpty());
            ConnectorTableHandle handle = metadata.getTableHandle(null, "db", "table").orElseThrow();
            Assertions.assertTrue(connector.ownsHandle(handle));
            ConnectorTableSchema schema = metadata.getTableSchema(null, handle);
            Assertions.assertEquals("ID", schema.getColumns().get(0).getName());
            Assertions.assertEquals("ID", schema.getProperties().get(ConnectorTableSchema.PARTITION_COLUMNS_KEY));
            Assertions.assertEquals(Collections.singleton("ID"), metadata.getColumnHandles(null, handle).keySet());
            Assertions.assertEquals("db", metadata.buildTableDescriptor(null, 1, "table", "db",
                    "table", 1, 42).getHiveTable().getDbName());
        }
    }

    @Test
    void statementPinsHandleSchemaAndMvccAcrossAppends() throws Exception {
        Path table = warehouse.resolve("db/table");
        StructType original = new StructType().add("id", LongType.LONG);
        DeltaLakeTestTables.commit(table, 0, "first", original);
        try (Connector connector = connector(); ConnectorMetadata metadata = connector.getMetadata(null)) {
            TestingConnectorSession statement = new TestingConnectorSession("first");
            ConnectorTableHandle first = metadata.getTableHandle(statement, "db", "table").orElseThrow();
            ConnectorMvccSnapshot pin = metadata.beginQuerySnapshot(statement, first).orElseThrow();
            DeltaLakeTestTables.commit(table, 1, "first", original.add("added", StringType.STRING));
            Assertions.assertSame(first, metadata.getTableHandle(statement, "db", "table").orElseThrow());
            Assertions.assertEquals(1, metadata.getTableSchema(statement, first).getColumns().size());
            connector.invalidateTable("db", "table");
            TestingConnectorSession next = new TestingConnectorSession("next");
            ConnectorTableHandle latest = metadata.getTableHandle(next, "db", "table").orElseThrow();
            Assertions.assertEquals(2, metadata.getTableSchema(next, latest).getColumns().size());
            ConnectorTableHandle pinned = metadata.applySnapshot(next, latest, pin);
            Assertions.assertEquals(0, ((DeltaLakeTableHandle) pinned).getVersion());
            Assertions.assertEquals(1, metadata.getTableSchema(next, latest, pin).getColumns().size());
            DeltaLakeTestTables.commit(warehouse.resolve("db/other"), 0, "other", original);
            ConnectorTableHandle other = metadata.getTableHandle(next, "db", "other").orElseThrow();
            Assertions.assertThrows(DorisConnectorException.class, () -> metadata.applySnapshot(next, other, pin));
        }
    }

    @Test
    void closedUnusedConnectorCannotAllocateResources() throws Exception {
        Connector connector = connector();
        connector.close();
        connector.close();
        Assertions.assertThrows(DorisConnectorException.class, () -> connector.getMetadata(null));
    }

    @Test
    void statementNamespacesAreTypePrefixed() throws Exception {
        String prefix = new DeltaLakeConnectorProvider().getType() + ".";
        int checked = 0;
        for (Field field : DeltaLakeStatementScope.class.getDeclaredFields()) {
            if (Modifier.isStatic(field.getModifiers()) && field.getType() == String.class
                    && field.getName().endsWith("_NAMESPACE")) {
                field.setAccessible(true);
                Assertions.assertTrue(((String) field.get(null)).startsWith(prefix), field.getName());
                checked++;
            }
        }
        Assertions.assertTrue(checked > 0);
    }

    private Connector connector() {
        return new DeltaLakeConnectorProvider().create(Collections.singletonMap("warehouse", warehouse.toString()),
                new RecordingConnectorContext());
    }
}
