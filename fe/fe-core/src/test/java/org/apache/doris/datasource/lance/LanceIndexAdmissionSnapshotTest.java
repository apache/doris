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

package org.apache.doris.datasource.lance;

import org.apache.arrow.memory.BufferAllocator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.lance.Dataset;
import org.lance.schema.LanceField;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Covers the admission snapshot's constructor-time bounds and the section 3.4 fail-closed rule
 * for duplicate physical names: a stale post-REPLACE physical entry may legitimately coexist
 * with its replacement until VACUUM, so the snapshot must refuse the ambiguous pair instead of
 * silently keeping whichever the provider returned first.
 */
public class LanceIndexAdmissionSnapshotTest {

    @Test
    public void testConstructionIsImmutableAndDefensivelyCopied() {
        List<LanceLogicalIndex> logical = new ArrayList<>(Collections.singletonList(
                new LanceLogicalIndex("idx", Collections.singletonList("embedding"),
                        "IVF_PQ", "{}")));
        List<LanceIndexAdmissionSnapshot.PhysicalIndexInfo> physical = new ArrayList<>(
                Collections.singletonList(physical("idx", "uuid-1", 7, "VECTOR")));
        List<LanceField> fields = new ArrayList<>(Collections.singletonList(field(1, "embedding")));

        LanceIndexAdmissionSnapshot snapshot = new LanceIndexAdmissionSnapshot(
                7, "file:///tmp/table.lance", logical, physical, fields);

        Assertions.assertEquals(7, snapshot.getDatasetVersion());
        Assertions.assertEquals("file:///tmp/table.lance", snapshot.getDatasetUri());
        Assertions.assertEquals("idx", snapshot.getLogicalIndexes().get(0).getName());
        Assertions.assertEquals("uuid-1", snapshot.getPhysicalIndexes().get(0).getUuid());
        Assertions.assertEquals(7, snapshot.getPhysicalIndexes().get(0).getIndexDatasetVersion());
        Assertions.assertEquals("VECTOR", snapshot.getPhysicalIndexes().get(0).getIndexTypeName());
        Assertions.assertEquals("embedding", snapshot.getTopLevelFields().get(0).getName());

        logical.clear();
        physical.clear();
        fields.clear();
        Assertions.assertEquals(1, snapshot.getLogicalIndexes().size());
        Assertions.assertEquals(1, snapshot.getPhysicalIndexes().size());
        Assertions.assertEquals(1, snapshot.getTopLevelFields().size());
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> snapshot.getLogicalIndexes().clear());
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> snapshot.getPhysicalIndexes().clear());
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> snapshot.getTopLevelFields().clear());
    }

    @Test
    public void testRejectsInvalidVersionAndUri() {
        List<LanceLogicalIndex> logical = Collections.emptyList();
        List<LanceIndexAdmissionSnapshot.PhysicalIndexInfo> physical = Collections.emptyList();
        List<LanceField> fields = Collections.emptyList();
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new LanceIndexAdmissionSnapshot(
                        0, "file:///tmp/t.lance", logical, physical, fields));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new LanceIndexAdmissionSnapshot(
                        -1, "file:///tmp/t.lance", logical, physical, fields));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new LanceIndexAdmissionSnapshot(
                        1, null, logical, physical, fields));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new LanceIndexAdmissionSnapshot(
                        1, "", logical, physical, fields));
    }

    @Test
    public void testRejectsNullListsAndNullElements() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new LanceIndexAdmissionSnapshot(1, "file:///tmp/t.lance",
                        null, Collections.emptyList(), Collections.emptyList()));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new LanceIndexAdmissionSnapshot(1, "file:///tmp/t.lance",
                        Collections.emptyList(), null, Collections.emptyList()));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new LanceIndexAdmissionSnapshot(1, "file:///tmp/t.lance",
                        Collections.emptyList(), Collections.emptyList(), null));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new LanceIndexAdmissionSnapshot(1, "file:///tmp/t.lance",
                        Collections.singletonList(null), Collections.emptyList(),
                        Collections.emptyList()));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new LanceIndexAdmissionSnapshot(1, "file:///tmp/t.lance",
                        Collections.emptyList(), Collections.singletonList(null),
                        Collections.emptyList()));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new LanceIndexAdmissionSnapshot(1, "file:///tmp/t.lance",
                        Collections.emptyList(), Collections.emptyList(),
                        Collections.singletonList(null)));
    }

    @Test
    public void testPhysicalIndexInfoValidationIsBounded() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> physical(null, "uuid", 1, "BTREE"));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> physical("", "uuid", 1, "BTREE"));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> physical("idx", null, 1, "BTREE"));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> physical("idx", "", 1, "BTREE"));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> physical("idx", "uuid", 0, "BTREE"));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> physical("idx", "uuid", -3, "BTREE"));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> physical("idx", "uuid", 1, null));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> physical("idx", "uuid", 1, ""));

        String overLimit = repeat("x", 1025);
        IllegalArgumentException nameTooLong = Assertions.assertThrows(
                IllegalArgumentException.class, () -> physical(overLimit, "uuid", 1, "BTREE"));
        Assertions.assertTrue(nameTooLong.getMessage().contains("1024"));
        IllegalArgumentException uuidTooLong = Assertions.assertThrows(
                IllegalArgumentException.class, () -> physical("idx", overLimit, 1, "BTREE"));
        Assertions.assertTrue(uuidTooLong.getMessage().contains("1024"));
        IllegalArgumentException typeTooLong = Assertions.assertThrows(
                IllegalArgumentException.class, () -> physical("idx", "uuid", 1, overLimit));
        Assertions.assertTrue(typeTooLong.getMessage().contains("1024"));
        String multibyte = repeat("界", 342);
        Assertions.assertTrue(multibyte.length() < 1024);
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> physical(multibyte, "uuid", 1, "BTREE"));
        String withinUtf8Limit = repeat("界", 341);
        Assertions.assertEquals(withinUtf8Limit,
                physical(withinUtf8Limit, "uuid", 1, "BTREE").getName());
    }

    @Test
    public void testDuplicatePhysicalNameFailsClosedAtConstruction() {
        // Two physical entries sharing one logical name (a stale post-REPLACE entry alongside
        // its replacement) must be rejected, never silently collapsed by first-wins map put.
        List<LanceIndexAdmissionSnapshot.PhysicalIndexInfo> duplicates = Arrays.asList(
                physical("idx", "uuid-1", 7, "VECTOR"),
                physical("idx", "uuid-2", 9, "VECTOR"));
        IllegalArgumentException exception = Assertions.assertThrows(IllegalArgumentException.class,
                () -> new LanceIndexAdmissionSnapshot(9, "file:///tmp/t.lance",
                        Collections.emptyList(), duplicates, Collections.emptyList()));
        Assertions.assertTrue(exception.getMessage().contains("Duplicate"));
        Assertions.assertTrue(exception.getMessage().contains("idx"));

        List<LanceIndexAdmissionSnapshot.PhysicalIndexInfo> sameUuid = Arrays.asList(
                physical("idx", "uuid-1", 7, "VECTOR"),
                physical("idx", "uuid-1", 7, "VECTOR"));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new LanceIndexAdmissionSnapshot(9, "file:///tmp/t.lance",
                        Collections.emptyList(), sameUuid, Collections.emptyList()));

        // Case-distinct names are distinct stored names; section 4.1 ambiguity is admission's
        // call, not the snapshot's.
        LanceIndexAdmissionSnapshot snapshot = new LanceIndexAdmissionSnapshot(9,
                "file:///tmp/t.lance", Collections.emptyList(),
                Arrays.asList(physical("idx", "uuid-1", 7, "VECTOR"),
                        physical("IDX", "uuid-2", 9, "VECTOR")),
                Collections.emptyList());
        Assertions.assertEquals(2, snapshot.getPhysicalIndexes().size());
    }

    @Test
    public void testEntryCountsAreBounded() {
        List<LanceLogicalIndex> logicalAtLimit = new ArrayList<>();
        for (int index = 0; index < 256; ++index) {
            logicalAtLimit.add(new LanceLogicalIndex("idx_" + index,
                    Collections.singletonList("c"), "BTREE", "{}"));
        }
        new LanceIndexAdmissionSnapshot(1, "file:///tmp/t.lance", logicalAtLimit,
                Collections.emptyList(), Collections.emptyList());
        logicalAtLimit.add(new LanceLogicalIndex("idx_over_limit",
                Collections.singletonList("c"), "BTREE", "{}"));
        IllegalArgumentException logicalOver = Assertions.assertThrows(IllegalArgumentException.class,
                () -> new LanceIndexAdmissionSnapshot(1, "file:///tmp/t.lance", logicalAtLimit,
                        Collections.emptyList(), Collections.emptyList()));
        Assertions.assertTrue(logicalOver.getMessage().contains("256"));

        List<LanceIndexAdmissionSnapshot.PhysicalIndexInfo> physicalAtLimit = new ArrayList<>();
        for (int index = 0; index < 16384; ++index) {
            physicalAtLimit.add(physical("idx_" + index, "uuid-" + index, 1, "BTREE"));
        }
        new LanceIndexAdmissionSnapshot(1, "file:///tmp/t.lance", Collections.emptyList(),
                physicalAtLimit, Collections.emptyList());
        physicalAtLimit.add(physical("idx_over_limit", "uuid-over-limit", 1, "BTREE"));
        IllegalArgumentException physicalOver = Assertions.assertThrows(IllegalArgumentException.class,
                () -> new LanceIndexAdmissionSnapshot(1, "file:///tmp/t.lance", Collections.emptyList(),
                        physicalAtLimit, Collections.emptyList()));
        Assertions.assertTrue(physicalOver.getMessage().contains("16384"));

        LanceField singleField = field(1, "c");
        new LanceIndexAdmissionSnapshot(1, "file:///tmp/t.lance", Collections.emptyList(),
                Collections.emptyList(), Collections.nCopies(16384, singleField));
        IllegalArgumentException fieldsOver = Assertions.assertThrows(IllegalArgumentException.class,
                () -> new LanceIndexAdmissionSnapshot(1, "file:///tmp/t.lance", Collections.emptyList(),
                        Collections.emptyList(), Collections.nCopies(16385, singleField)));
        Assertions.assertTrue(fieldsOver.getMessage().contains("16384"));
    }

    @Test
    public void testSnapshotHoldsNoNativeHandles() {
        // The snapshot outlives the Dataset it was read from, so no field of it (transitively,
        // including list element types) may reference the Dataset, an Arrow allocator, or any
        // other Lance type beyond the pure-POJO LanceField.
        Set<Class<?>> seen = new HashSet<>();
        for (Class<?> snapshotClass : Arrays.asList(
                LanceIndexAdmissionSnapshot.class,
                LanceIndexAdmissionSnapshot.PhysicalIndexInfo.class)) {
            for (Field field : snapshotClass.getDeclaredFields()) {
                collectTypes(field.getGenericType(), seen);
            }
        }
        Assertions.assertTrue(seen.contains(LanceField.class));
        for (Class<?> type : seen) {
            Assertions.assertNotEquals(Dataset.class, type);
            Assertions.assertFalse(BufferAllocator.class.isAssignableFrom(type));
            if (type.getName().startsWith("org.lance.")) {
                Assertions.assertEquals(LanceField.class, type);
            }
        }
    }

    private static void collectTypes(Type type, Set<Class<?>> seen) {
        if (type instanceof Class) {
            seen.add((Class<?>) type);
            return;
        }
        if (type instanceof ParameterizedType) {
            ParameterizedType parameterized = (ParameterizedType) type;
            collectTypes(parameterized.getRawType(), seen);
            for (Type argument : parameterized.getActualTypeArguments()) {
                collectTypes(argument, seen);
            }
        }
    }

    private static LanceIndexAdmissionSnapshot.PhysicalIndexInfo physical(
            String name, String uuid, long indexDatasetVersion, String indexTypeName) {
        return new LanceIndexAdmissionSnapshot.PhysicalIndexInfo(
                name, uuid, indexDatasetVersion, indexTypeName);
    }

    private static LanceField field(int id, String name) {
        LanceField field = Mockito.mock(LanceField.class);
        Mockito.when(field.getId()).thenReturn(id);
        Mockito.when(field.getName()).thenReturn(name);
        return field;
    }

    private static String repeat(String value, int count) {
        return String.join("", Collections.nCopies(count, value));
    }
}
