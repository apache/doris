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

import org.apache.doris.catalog.Type;
import org.apache.doris.datasource.mvcc.MvccTable;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.lance.Version;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.TreeMap;

public class LanceSnapshotTest {

    @Test
    public void testVersionSelectorRequiresPositiveNumericVersion() {
        Assertions.assertEquals(7, LanceSnapshotResolver.parseVersion("7"));
        Assertions.assertThrows(
                IllegalArgumentException.class, () -> LanceSnapshotResolver.parseVersion("tag_name"));
        Assertions.assertThrows(
                IllegalArgumentException.class, () -> LanceSnapshotResolver.parseVersion("0"));
    }

    @Test
    public void testTimeSelectorResolvesLatestVersionNotAfterTimestamp() {
        ZonedDateTime first = ZonedDateTime.of(2026, 8, 1, 10, 0, 0, 0, ZoneOffset.UTC);
        ZonedDateTime second = first.plusHours(1);
        ZonedDateTime third = second.plusHours(1);
        Version version1 = new Version(1, first, new TreeMap<>());
        Version version2 = new Version(2, second, new TreeMap<>());
        Version version3 = new Version(3, third, new TreeMap<>());

        Assertions.assertEquals(2, LanceSnapshotResolver.versionAtOrBefore(
                Arrays.asList(version3, version1, version2), second.plusMinutes(30).toInstant().toEpochMilli()));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> LanceSnapshotResolver.versionAtOrBefore(
                        Arrays.asList(version1, version2, version3), first.minusNanos(1).toInstant().toEpochMilli()));
    }

    @Test
    public void testBoundSnapshotCarriesItsOwnSchema() {
        LanceTableMetadata intMetadata = metadata(10,
                Field.nullable("value", new ArrowType.Int(32, true)));
        LanceTableMetadata floatMetadata = metadata(11,
                Field.nullable("value", new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)));

        Assertions.assertEquals(Type.INT,
                LanceExternalTable.toDorisColumns(intMetadata).get(0).getType());
        Assertions.assertEquals(Type.FLOAT,
                LanceExternalTable.toDorisColumns(floatMetadata).get(0).getType());

        LanceMvccSnapshot version10 = new LanceMvccSnapshot(intMetadata);
        Assertions.assertSame(intMetadata, version10.getMetadata());
        Assertions.assertEquals(10, version10.getMetadata().getVersion());
        Assertions.assertEquals(10, version10.getMetadata().getFragments().get(0).getId());
        Assertions.assertEquals("http://minio:9000",
                version10.getMetadata().getLanceStorageOptions().get("aws_endpoint"));
        Assertions.assertTrue(version10.isSameSnapshot(new LanceMvccSnapshot(metadata(10,
                Field.nullable("value", new ArrowType.Int(32, true))))));
        Assertions.assertFalse(version10.isSameSnapshot(new LanceMvccSnapshot(floatMetadata)));
        Assertions.assertTrue(MvccTable.class.isAssignableFrom(LanceExternalTable.class));
    }

    private static LanceTableMetadata metadata(long version, Field field) {
        return LanceTableMetadata.withoutIndexSegments("s3://bucket/table.lance", version,
                new Schema(Collections.singletonList(field)),
                Collections.singletonList(new LanceFragmentInfo(version, 1, 1)),
                Collections.singletonMap("aws_endpoint", "http://minio:9000"));
    }
}
