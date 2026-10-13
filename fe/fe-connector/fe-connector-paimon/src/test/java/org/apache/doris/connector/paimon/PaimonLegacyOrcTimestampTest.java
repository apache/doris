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

package org.apache.doris.connector.paimon;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.format.OrcOptions;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.RawFile;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

class PaimonLegacyOrcTimestampTest {
    @Test
    void routeCurrentAndHistoricalNestedLtzToSdk(@TempDir Path warehouse) throws Exception {
        try (Catalog catalog = new FileSystemCatalog(LocalFileIO.create(),
                new org.apache.paimon.fs.Path(warehouse.toUri()))) {
            catalog.createDatabase("db", false);
            Identifier id = Identifier.create("db", "events");
            catalog.createTable(id, Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .column("events", DataTypes.ARRAY(new org.apache.paimon.types.LocalZonedTimestampType(6)))
                    .option(OrcOptions.ORC_TIMESTAMP_LTZ_LEGACY_TYPE.key(), "true")
                    .build(), false);
            FileStoreTable original = (FileStoreTable) catalog.getTable(id);
            Optional<List<RawFile>> oldFiles = Optional.of(Collections.singletonList(
                    new RawFile("part.orc", 0L, 100L, 1L, "orc", original.schema().id(), 0L)));
            Assertions.assertTrue(PaimonScanPlanProvider.requiresLegacyOrcTimestampReader(
                    original, oldFiles, Collections.singleton(1), new HashMap<>()));
            new SchemaManager(original.fileIO(), original.location())
                    .commitChanges(SchemaChange.dropColumn("events"));
            FileStoreTable evolved = (FileStoreTable) catalog.getTable(id);
            Map<Long, Boolean> schemaCache = new HashMap<>();
            // Historical field IDs, rather than all file columns, determine whether LTZ is decoded.
            Assertions.assertFalse(PaimonScanPlanProvider.requiresLegacyOrcTimestampReader(
                    evolved, oldFiles, Collections.singleton(0), new HashMap<>()));
            Assertions.assertTrue(PaimonScanPlanProvider.requiresLegacyOrcTimestampReader(
                    evolved, oldFiles, Collections.singleton(1), schemaCache));
            Assertions.assertEquals(Collections.singletonMap(original.schema().id(), true), schemaCache);
            Assertions.assertFalse(PaimonScanPlanProvider.requiresLegacyOrcTimestampReader(
                    evolved.copy(Collections.singletonMap(OrcOptions.ORC_TIMESTAMP_LTZ_LEGACY_TYPE.key(), "false")),
                    oldFiles, Collections.singleton(1), new HashMap<>()));
            Assertions.assertFalse(PaimonScanPlanProvider.requiresLegacyOrcTimestampReader(evolved,
                    Optional.of(Collections.singletonList(
                            new RawFile("part.parquet", 0L, 100L, 1L, "parquet", original.schema().id(), 0L))),
                    Collections.singleton(1), new HashMap<>()));
        }
    }

    @Test
    void routeEveryNestedLtzShapeAndMixedSplitToSdk(@TempDir Path warehouse) throws Exception {
        org.apache.paimon.types.DataType ltz = DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(6);
        List<org.apache.paimon.types.DataType> shapes = Arrays.asList(ltz, DataTypes.ARRAY(ltz),
                DataTypes.MULTISET(ltz), DataTypes.MAP(ltz, DataTypes.INT()), DataTypes.MAP(DataTypes.INT(), ltz),
                DataTypes.ROW(DataTypes.FIELD(1, "ts", ltz)));
        try (Catalog catalog = new FileSystemCatalog(LocalFileIO.create(),
                new org.apache.paimon.fs.Path(warehouse.toUri()))) {
            catalog.createDatabase("db", false);
            for (int i = 0; i < shapes.size(); i++) {
                // An absent option means Paimon's legacy default, so both spellings must route to the SDK.
                for (boolean explicit : new boolean[] {false, true}) {
                    Identifier id = Identifier.create("db", "t" + i + (explicit ? "_explicit" : "_default"));
                    Schema.Builder schema = Schema.newBuilder()
                            .column("id", DataTypes.INT())
                            .column("c", shapes.get(i));
                    if (explicit) {
                        schema.option(OrcOptions.ORC_TIMESTAMP_LTZ_LEGACY_TYPE.key(), "true");
                    }
                    catalog.createTable(id, schema.build(), false);
                    FileStoreTable table = (FileStoreTable) catalog.getTable(id);
                    Set<Integer> readC = Collections.singleton(table.rowType().getField("c").id());
                    String message = shapes.get(i) + (explicit ? " explicit" : " default");
                    Assertions.assertTrue(requiresSdk(table, readC, "data.orc"), message);
                    // One legacy ORC file is enough to route a mixed split away from the native reader.
                    Assertions.assertTrue(requiresSdk(table, readC, "data.parquet", "data.orc"), message);
                    Assertions.assertFalse(requiresSdk(table, readC, "data.parquet"), message);
                }
            }
        }
    }

    private static boolean requiresSdk(FileStoreTable table, Set<Integer> readFieldIds, String... paths) {
        List<RawFile> files = new ArrayList<>();
        for (String path : paths) {
            files.add(new RawFile(path, 0L, 100L, 1L, path.substring(path.lastIndexOf('.') + 1),
                    table.schema().id(), 0L));
        }
        return PaimonScanPlanProvider.requiresLegacyOrcTimestampReader(
                table, Optional.of(files), readFieldIds, new HashMap<>());
    }
}
