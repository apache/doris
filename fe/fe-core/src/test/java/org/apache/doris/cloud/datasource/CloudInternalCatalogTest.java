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

import org.apache.doris.analysis.DataSortInfo;
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.common.Config;
import org.apache.doris.proto.OlapFile;
import org.apache.doris.thrift.TCompressionType;
import org.apache.doris.thrift.TInvertedIndexFileStorageFormat;
import org.apache.doris.thrift.TSortType;
import org.apache.doris.thrift.TStorageFormat;
import org.apache.doris.thrift.TTabletType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class CloudInternalCatalogTest {
    private static final long ROW_TTL_DURATION_MICROS = 86_400_000_000L;

    @Test
    public void testCreateTabletMetaUsesCurrentSchemaVersionAndFormat() throws Exception {
        Tablet tablet = Mockito.mock(Tablet.class);
        Replica replica = Mockito.mock(Replica.class);
        Mockito.when(tablet.getId()).thenReturn(100L);
        Mockito.when(tablet.getReplicas()).thenReturn(Collections.singletonList(replica));
        Mockito.when(replica.getId()).thenReturn(200L);

        boolean original = Config.enable_partition_inverted_index_storage_format_rollout;
        try {
            Config.enable_partition_inverted_index_storage_format_rollout = false;
            OlapFile.TabletMetaCloudPB disabledTabletMeta = createTabletMeta(tablet);
            Assertions.assertEquals(17, disabledTabletMeta.getSchemaVersion());
            Assertions.assertEquals(17, disabledTabletMeta.getSchema().getSchemaVersion());
            Assertions.assertEquals(OlapFile.InvertedIndexStorageFormatPB.SNII,
                    disabledTabletMeta.getSchema().getInvertedIndexStorageFormat());
            Assertions.assertFalse(disabledTabletMeta.hasInvertedIndexStorageFormat());
            Assertions.assertEquals(1, disabledTabletMeta.getRsMetasCount());
            Assertions.assertFalse(disabledTabletMeta.getRsMetas(0).hasInvertedIndexStorageFormat());

            Config.enable_partition_inverted_index_storage_format_rollout = true;
            OlapFile.TabletMetaCloudPB enabledTabletMeta = createTabletMeta(tablet);
            Assertions.assertTrue(enabledTabletMeta.hasInvertedIndexStorageFormat());
            Assertions.assertEquals(OlapFile.InvertedIndexStorageFormatPB.SNII,
                    enabledTabletMeta.getInvertedIndexStorageFormat());
            Assertions.assertTrue(enabledTabletMeta.getRsMetas(0).hasInvertedIndexStorageFormat());
            Assertions.assertEquals(OlapFile.InvertedIndexStorageFormatPB.SNII,
                    enabledTabletMeta.getRsMetas(0).getInvertedIndexStorageFormat());
        } finally {
            Config.enable_partition_inverted_index_storage_format_rollout = original;
        }
    }

    @Test
    public void testSetRowTtlSchemaFieldsUsesUtcOffset() throws Exception {
        List<Column> rowTtlColumns = Arrays.asList(
                new Column("k1", ScalarType.createType(PrimitiveType.INT), true,
                        null, false, null, ""),
                new Column(Column.TTL_COL, ScalarType.createDatetimeV2Type(6),
                        false, AggregateType.NONE, true, "row ttl", false));

        OlapFile.TabletSchemaCloudPB.Builder utcSchema =
                OlapFile.TabletSchemaCloudPB.newBuilder();
        CloudInternalCatalog.setRowTtlSchemaFields(
                utcSchema, rowTtlColumns, ROW_TTL_DURATION_MICROS, 0);
        Assertions.assertEquals(0, utcSchema.getRowTtlTimeZoneOffsetSeconds());
    }

    @Test
    public void testSetRowTtlSchemaFieldsIgnoresOffsetForNonTtlSchema() throws Exception {
        Column keyColumn = new Column("k1", ScalarType.createType(PrimitiveType.INT), true,
                null, false, null, "");
        OlapFile.TabletSchemaCloudPB.Builder schema = OlapFile.TabletSchemaCloudPB.newBuilder();

        CloudInternalCatalog.setRowTtlSchemaFields(
                schema, Collections.singletonList(keyColumn), ROW_TTL_DURATION_MICROS,
                28_800);

        Assertions.assertEquals(-1, schema.getTtlColIdx());
        Assertions.assertEquals(-1, schema.getRowTtlDurationUs());
        Assertions.assertEquals(0, schema.getRowTtlTimeZoneOffsetSeconds());
    }

    @Test
    public void testDirectRowTtlSchema() throws Exception {
        List<Column> directColumns = Arrays.asList(
                new Column("k1", ScalarType.createType(PrimitiveType.INT), true,
                        null, false, null, ""),
                new Column(Column.TTL_COL, ScalarType.createType(PrimitiveType.BIGINT),
                        false, AggregateType.NONE, true, "row ttl", false));

        OlapFile.TabletSchemaCloudPB.Builder ordinarySchema =
                OlapFile.TabletSchemaCloudPB.newBuilder();
        CloudInternalCatalog.setRowTtlSchemaFields(
                ordinarySchema, directColumns, -1, 0);
        Assertions.assertEquals(1, ordinarySchema.getTtlColIdx());
        Assertions.assertEquals(-1, ordinarySchema.getRowTtlDurationUs());
        Assertions.assertEquals(0, ordinarySchema.getRowTtlTimeZoneOffsetSeconds());

    }

    private OlapFile.TabletMetaCloudPB createTabletMeta(Tablet tablet) throws Exception {
        return new CloudInternalCatalog().createTabletMetaBuilder(
                1L, 2L, 3L, tablet, TTabletType.TABLET_TYPE_DISK, 23, KeysType.DUP_KEYS, (short) 1,
                Collections.emptySet(), 0.05, Collections.emptyList(), Collections.emptyList(),
                new DataSortInfo(TSortType.LEXICAL, 0), TCompressionType.LZ4F, TStorageFormat.DEFAULT,
                "", false, false,
                "table", 0L, -1L, 0, false, false, 17, null, "size_based",
                0L, 0L, 0L, 0L, 0L, false, Collections.emptyList(),
                TInvertedIndexFileStorageFormat.SNII, 0L, false, Collections.emptyList(), 0L,
                OlapFile.EncryptionAlgorithmPB.PLAINTEXT, 0L, true, Collections.emptyMap(), 0,
                OlapFile.TabletRolePB.TABLET_ROLE_DATA).build();
    }
}
