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

package org.apache.doris.cloud.datasource;

import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.DdlException;
import org.apache.doris.proto.OlapFile;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class CloudInternalCatalogTest {
    private static final long ROW_TTL_DURATION_MICROS = 86_400_000_000L;

    @Test
    public void testSetRowTtlSchemaFieldsPreservesOffsetPresence() throws Exception {
        List<Column> rowTtlColumns = Arrays.asList(
                new Column("k1", ScalarType.createType(PrimitiveType.INT), true,
                        null, false, null, ""),
                new Column(Column.TTL_COL, ScalarType.createDatetimeV2Type(6),
                        false, AggregateType.NONE, true, "row ttl", false));

        OlapFile.TabletSchemaCloudPB.Builder legacySchema =
                OlapFile.TabletSchemaCloudPB.newBuilder();
        CloudInternalCatalog.setRowTtlSchemaFields(
                legacySchema, rowTtlColumns, ROW_TTL_DURATION_MICROS, Optional.empty(), true);
        Assertions.assertEquals(1, legacySchema.getTtlColIdx());
        Assertions.assertEquals(ROW_TTL_DURATION_MICROS, legacySchema.getRowTtlDurationUs());
        Assertions.assertFalse(legacySchema.hasRowTtlTimeZoneOffsetSeconds());

        OlapFile.TabletSchemaCloudPB.Builder ordinaryLegacySchema =
                OlapFile.TabletSchemaCloudPB.newBuilder();
        DdlException missingOffset = Assertions.assertThrows(DdlException.class,
                () -> CloudInternalCatalog.setRowTtlSchemaFields(
                        ordinaryLegacySchema, rowTtlColumns, ROW_TTL_DURATION_MICROS,
                        Optional.empty(), false));
        Assertions.assertTrue(missingOffset.getMessage().contains("row ttl time zone is missing"));

        OlapFile.TabletSchemaCloudPB.Builder utcSchema =
                OlapFile.TabletSchemaCloudPB.newBuilder();
        CloudInternalCatalog.setRowTtlSchemaFields(
                utcSchema, rowTtlColumns, ROW_TTL_DURATION_MICROS, Optional.of(0), false);
        Assertions.assertTrue(utcSchema.hasRowTtlTimeZoneOffsetSeconds());
        Assertions.assertEquals(0, utcSchema.getRowTtlTimeZoneOffsetSeconds());
    }

    @Test
    public void testSetRowTtlSchemaFieldsIgnoresOffsetForNonTtlSchema() throws Exception {
        Column keyColumn = new Column("k1", ScalarType.createType(PrimitiveType.INT), true,
                null, false, null, "");
        OlapFile.TabletSchemaCloudPB.Builder schema = OlapFile.TabletSchemaCloudPB.newBuilder();

        CloudInternalCatalog.setRowTtlSchemaFields(
                schema, Collections.singletonList(keyColumn), ROW_TTL_DURATION_MICROS,
                Optional.of(28_800), false);

        Assertions.assertFalse(schema.hasTtlColIdx());
        Assertions.assertFalse(schema.hasRowTtlDurationUs());
        Assertions.assertFalse(schema.hasRowTtlTimeZoneOffsetSeconds());
    }

    @Test
    public void testLegacyDirectRowTtlRequiresRestoreMode() throws Exception {
        List<Column> directColumns = Arrays.asList(
                new Column("k1", ScalarType.createType(PrimitiveType.INT), true,
                        null, false, null, ""),
                new Column(Column.TTL_COL, ScalarType.createType(PrimitiveType.BIGINT),
                        false, AggregateType.NONE, true, "row ttl", false));

        OlapFile.TabletSchemaCloudPB.Builder ordinarySchema =
                OlapFile.TabletSchemaCloudPB.newBuilder();
        DdlException exception = Assertions.assertThrows(DdlException.class,
                () -> CloudInternalCatalog.setRowTtlSchemaFields(
                        ordinarySchema, directColumns, -1, Optional.empty(), false));
        Assertions.assertTrue(exception.getMessage().contains("direct row ttl is not supported"));

        OlapFile.TabletSchemaCloudPB.Builder restoreSchema =
                OlapFile.TabletSchemaCloudPB.newBuilder();
        CloudInternalCatalog.setRowTtlSchemaFields(
                restoreSchema, directColumns, -1, Optional.empty(), true);
        Assertions.assertEquals(1, restoreSchema.getTtlColIdx());
        Assertions.assertEquals(-1, restoreSchema.getRowTtlDurationUs());
        Assertions.assertFalse(restoreSchema.hasRowTtlTimeZoneOffsetSeconds());
    }
}
