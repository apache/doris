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

import org.apache.doris.connector.spi.ConnectorTableSchema;
import org.apache.doris.connector.spi.DorisConnectorException;

import io.delta.kernel.Snapshot;
import io.delta.kernel.TableManager;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Path;
import java.util.Collections;

class DeltaLakeSnapshotPinTest {
    @TempDir
    Path table;

    private final Engine engine = DefaultEngine.create(new Configuration());

    @Test
    void retainsSchemaAndVersionAcrossNewCommitsAndSerialization() throws Exception {
        StructType original = new StructType().add("ID", LongType.LONG, true);
        commit(0, "original", original);
        Snapshot first = TableManager.loadSnapshot(table.toString()).build(engine);
        DeltaLakeTableHandle handle = new DeltaLakeTableHandle("db", "table", first);
        commit(1, "original", original.add("ADDED", StringType.STRING, true));
        Snapshot latest = TableManager.loadSnapshot(table.toString()).build(engine);
        Assertions.assertEquals(1, latest.getVersion());
        Assertions.assertEquals(2, latest.getSchema().length());
        Assertions.assertSame(first, handle.snapshot(engine));

        DeltaLakeTableHandle restored = roundTrip(handle);
        Assertions.assertEquals(0, restored.snapshot(engine).getVersion());
        Assertions.assertEquals(1, restored.snapshot(engine).getSchema().length());
        DeltaLakeCatalogProperties properties =
                DeltaLakeCatalogProperties.of(Collections.singletonMap("warehouse", table.toString()));
        ConnectorTableSchema schema = DeltaLakeSchemaBuilder.build("table", restored.snapshot(engine), properties);
        Assertions.assertEquals("ID", schema.getColumns().get(0).getName());
        Assertions.assertEquals("BIGINT", schema.getColumns().get(0).getType().getTypeName());
        Assertions.assertEquals(first.getPath(), schema.getProperties().get(ConnectorTableSchema.SHOW_LOCATION_KEY));
    }

    @Test
    void deserializedHandleRejectsRecreationAtSamePathAndVersion() throws Exception {
        commit(0, "original", new StructType().add("id", LongType.LONG));
        DeltaLakeTableHandle handle = roundTrip(new DeltaLakeTableHandle("db", "table",
                TableManager.loadSnapshot(table.toString()).build(engine)));
        commit(0, "replacement", new StructType().add("id", LongType.LONG));
        Assertions.assertThrows(DorisConnectorException.class, () -> handle.snapshot(engine));
    }

    private void commit(long version, String id, StructType schema) throws Exception {
        DeltaLakeTestTables.commit(table, version, id, schema);
    }

    private DeltaLakeTableHandle roundTrip(DeltaLakeTableHandle handle) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(handle);
        }
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            return (DeltaLakeTableHandle) in.readObject();
        }
    }
}
