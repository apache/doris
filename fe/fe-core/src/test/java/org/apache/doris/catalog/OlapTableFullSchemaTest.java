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

import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.thrift.TStorageType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class OlapTableFullSchemaTest {
    @Test
    public void testSameLayoutAfterMetadataRestoreAndAddingMv() {
        OlapTable original = createTable();
        addIndex(original, 1, "mv_a", column("mv_a", 1));
        addIndex(original, 2, "mv_b", column("mv_b", 2));
        original.rebuildFullSchema();

        OlapTable restored = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(original), OlapTable.class);
        // Gson restores Map<Long, ...> as LinkedHashMap. A new index can appear before
        // existing indexes in the original HashMap, but after them in the restored map.
        for (OlapTable table : List.of(original, restored)) {
            addIndex(table, 16, "mv_c", column("mv_c", 3));
            table.rebuildFullSchema();
        }
        Assertions.assertEquals(original.getFullSchema(), restored.getFullSchema());
        Assertions.assertEquals(List.of("k", "mv_a", "mv_b", "mv_c"), names(original));

        OlapTable restarted = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(restored), OlapTable.class);
        Assertions.assertEquals(original.getFullSchema(), restarted.getFullSchema());
        for (OlapTable table : List.of(original, restored, restarted)) {
            table.deleteIndexInfo("mv_a");
            Assertions.assertEquals(List.of("k", "mv_b", "mv_c"), names(table));
        }
    }

    @Test
    public void testBaseColumnsFirstAndSharedMvColumnSelection() {
        OlapTable table = createTable();
        Column baseColumn = table.getBaseSchema().get(0);
        Column firstShared = column("mv_shared", 1);
        Column secondShared = column("mv_shared", 2);
        // Index 16 precedes index 1 in HashMap traversal. Both contain the same
        // MV expression with different index-local IDs, and a copy of the base column.
        addIndex(table, 16, "mv_second", column("k", 9), secondShared, column("mv_b", 3));
        addIndex(table, 1, "mv_first", column("k", 8), firstShared, column("mv_a", 4));
        table.rebuildFullSchema();

        Assertions.assertEquals(List.of("k", "mv_shared", "mv_a", "mv_b"), names(table));
        Assertions.assertSame(baseColumn, table.getFullSchema().get(0));
        Assertions.assertSame(firstShared, table.getColumn("mv_shared"));
        Assertions.assertEquals(2, table.getIndexMetaByIndexId(16).getSchema().get(1).getUniqueId());

        table.deleteIndexInfo("mv_first");
        Assertions.assertEquals(List.of("k", "mv_shared", "mv_b"), names(table));
        Assertions.assertSame(secondShared, table.getColumn("mv_shared"));
    }

    @Test
    public void testSameLayoutAfterRestoreRemapsIndexIds() {
        OlapTable table = createTable();
        Column baseColumn = table.getBaseSchema().get(0);
        Column firstShared = column("mv_shared", 1);
        Column secondShared = column("mv_shared", 2);
        addIndex(table, 1, "mv_first", firstShared, column("mv_a", 3));
        addIndex(table, 16, "mv_second", secondShared, column("mv_b", 4));
        table.rebuildFullSchema();
        Assertions.assertEquals(List.of("k", "mv_shared", "mv_a", "mv_b"), names(table));
        Assertions.assertSame(firstShared, table.getColumn("mv_shared"));

        Env env = Mockito.mock(Env.class);
        AtomicLong nextId = new AtomicLong(1000);
        Mockito.when(env.getNextId()).thenAnswer(invocation -> nextId.getAndIncrement());
        // RESTORE allocates new IDs in HashMap order, visiting index 16 before index 1.
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentColocateIndex).thenReturn(new ColocateTableIndex());
            Assertions.assertTrue(table.resetIdsForRestore(env, new Database(2000, "restored_db"),
                    ReplicaAllocation.DEFAULT_ALLOCATION, false, false, new ArrayList<>(), "source_db").ok());
        }
        Assertions.assertTrue(table.getIndexIdByName("mv_second") < table.getIndexIdByName("mv_first"));

        // The leader publishes this object; followers deserialize the same metadata.
        OlapTable follower = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(table), OlapTable.class);
        Assertions.assertEquals(follower.getFullSchema(), table.getFullSchema());
        Assertions.assertEquals(List.of("k", "mv_shared", "mv_b", "mv_a"), names(table));
        Assertions.assertSame(baseColumn, table.getFullSchema().get(0));
        Assertions.assertSame(secondShared, table.getColumn("mv_shared"));
        Assertions.assertEquals(follower.getColumn("mv_shared").getUniqueId(),
                table.getColumn("mv_shared").getUniqueId());
    }

    private static OlapTable createTable() {
        List<Column> schema = new ArrayList<>(List.of(column("k", 0)));
        OlapTable table = new OlapTable(100, "tbl", schema, KeysType.DUP_KEYS,
                new SinglePartitionInfo(), new RandomDistributionInfo(1));
        // Keep the base index ID above the MV IDs to verify that base columns stay first.
        table.setBaseIndexId(32);
        addIndex(table, 32, "tbl", schema.get(0));
        return table;
    }

    private static Column column(String name, int uniqueId) {
        Column column = new Column(name, PrimitiveType.INT);
        column.setUniqueId(uniqueId);
        return column;
    }

    private static void addIndex(OlapTable table, long id, String name, Column... columns) {
        table.setIndexMeta(id, name, new ArrayList<>(List.of(columns)), 0, 1, (short) 1,
                TStorageType.COLUMN, KeysType.DUP_KEYS);
    }

    private static List<String> names(OlapTable table) {
        return table.getFullSchema().stream().map(Column::getName).toList();
    }
}
