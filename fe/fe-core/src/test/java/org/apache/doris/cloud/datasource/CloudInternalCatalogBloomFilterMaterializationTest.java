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
import org.apache.doris.catalog.Index;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.cloud.catalog.CloudReplica;
import org.apache.doris.cloud.catalog.CloudTablet;
import org.apache.doris.proto.OlapFile;
import org.apache.doris.proto.OlapFile.EncryptionAlgorithmPB;
import org.apache.doris.thrift.TCompressionType;
import org.apache.doris.thrift.TInvertedIndexFileStorageFormat;
import org.apache.doris.thrift.TSortType;
import org.apache.doris.thrift.TStorageFormat;
import org.apache.doris.thrift.TTabletType;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class CloudInternalCatalogBloomFilterMaterializationTest {

    @Test
    public void testCreateTabletMetaBuilderMaterializesNamedBloomFilter() throws Exception {
        // Named-only BF indexes carry their own per-index FPP. The table-level bfFpp is only
        // relevant for legacy bloom_filter_columns, so the cloud schema builder should NOT
        // set it when bfColumns is null.
        CloudInternalCatalog catalog = new CloudInternalCatalog();
        CloudTablet tablet = new CloudTablet(100L);
        tablet.addReplica(new CloudReplica(101L, 1L, org.apache.doris.catalog.Replica.ReplicaState.NORMAL,
                1L, 1, 1L, 1L, 1L, 1L, 0L), true);
        Column keyColumn = new Column("k1", ScalarType.createType(PrimitiveType.INT), true, null, "1", "");
        Column valueColumn = new Column("v1", ScalarType.createType(PrimitiveType.INT), false,
                AggregateType.NONE, "1", "");

        OlapFile.TabletMetaCloudPB tabletMeta = catalog.createTabletMetaBuilder(1L, 1L, 1L, tablet,
                TTabletType.TABLET_TYPE_DISK, 1, KeysType.DUP_KEYS, (short) 1, null, 0,
                Lists.newArrayList(new Index(1L, "bf_k1", Lists.newArrayList("k1"),
                        org.apache.doris.catalog.info.IndexType.BLOOMFILTER,
                        Map.of("bloom_filter_fpp", "0.02"), "")),
                Lists.newArrayList(keyColumn, valueColumn), new DataSortInfo(TSortType.LEXICAL, 1),
                TCompressionType.LZ4F, TStorageFormat.V2, "", false, false, "tbl", 0L, false, false, 1, "",
                0L, 0L, 0L, 0L, 0L, false, null, TInvertedIndexFileStorageFormat.V2, 16384L, false, null,
                65536L, EncryptionAlgorithmPB.PLAINTEXT, 262144L, false, Collections.emptyMap(), 5).build();

        Assert.assertFalse(tabletMeta.getSchema().hasBfFpp());
        Assert.assertTrue(tabletMeta.getSchema().getColumn(0).getIsBfColumn());
        Assert.assertFalse(tabletMeta.getSchema().getColumn(1).getIsBfColumn());
        Assert.assertEquals("0.02", tabletMeta.getSchema().getIndex(0).getPropertiesMap().get("bloom_filter_fpp"));
    }

    @Test
    public void testCreateTabletMetaBuilderMaterializesNamedBloomFilterWithEmptyLegacySet() throws Exception {
        // An empty legacy BF set should not change the named-BF behavior: the BF column still
        // materializes, while the schema-level bfFpp remains whatever FE passed in.
        CloudInternalCatalog catalog = new CloudInternalCatalog();
        CloudTablet tablet = new CloudTablet(100L);
        tablet.addReplica(new CloudReplica(101L, 1L, org.apache.doris.catalog.Replica.ReplicaState.NORMAL,
                1L, 1, 1L, 1L, 1L, 1L, 0L), true);
        Column keyColumn = new Column("k1", ScalarType.createType(PrimitiveType.INT), true, null, "1", "");

        OlapFile.TabletMetaCloudPB tabletMeta = catalog.createTabletMetaBuilder(1L, 1L, 1L, tablet,
                TTabletType.TABLET_TYPE_DISK, 1, KeysType.DUP_KEYS, (short) 1, new HashSet<>(), 0,
                Lists.newArrayList(new Index(1L, "bf_k1", Lists.newArrayList("k1"),
                        org.apache.doris.catalog.info.IndexType.BLOOMFILTER, null, "")),
                Lists.newArrayList(keyColumn), new DataSortInfo(TSortType.LEXICAL, 1),
                TCompressionType.LZ4F, TStorageFormat.V2, "", false, false, "tbl", 0L, false, false, 1, "",
                0L, 0L, 0L, 0L, 0L, false, null, TInvertedIndexFileStorageFormat.V2, 16384L, false, null,
                65536L, EncryptionAlgorithmPB.PLAINTEXT, 262144L, false, Collections.emptyMap(), 5).build();

        Assert.assertTrue(tabletMeta.getSchema().hasBfFpp());
        Assert.assertEquals(0, tabletMeta.getSchema().getBfFpp(), 0);
        Assert.assertTrue(tabletMeta.getSchema().getColumn(0).getIsBfColumn());
    }

    @Test
    public void testCreateTabletMetaBuilderDoesNotSetBfFppWithoutBloomFilter() throws Exception {
        CloudInternalCatalog catalog = new CloudInternalCatalog();
        CloudTablet tablet = new CloudTablet(100L);
        tablet.addReplica(new CloudReplica(101L, 1L, org.apache.doris.catalog.Replica.ReplicaState.NORMAL,
                1L, 1, 1L, 1L, 1L, 1L, 0L), true);
        Column keyColumn = new Column("k1", ScalarType.createType(PrimitiveType.INT), true, null, "1", "");

        OlapFile.TabletMetaCloudPB tabletMeta = catalog.createTabletMetaBuilder(1L, 1L, 1L, tablet,
                TTabletType.TABLET_TYPE_DISK, 1, KeysType.DUP_KEYS, (short) 1, null, 0, Lists.newArrayList(),
                Lists.newArrayList(keyColumn), new DataSortInfo(TSortType.LEXICAL, 1),
                TCompressionType.LZ4F, TStorageFormat.V2, "", false, false, "tbl", 0L, false, false, 1, "",
                0L, 0L, 0L, 0L, 0L, false, null, TInvertedIndexFileStorageFormat.V2, 16384L, false, null,
                65536L, EncryptionAlgorithmPB.PLAINTEXT, 262144L, false, Collections.emptyMap(), 5).build();

        Assert.assertFalse(tabletMeta.getSchema().hasBfFpp());
        Assert.assertFalse(tabletMeta.getSchema().getColumn(0).getIsBfColumn());
    }

    @Test
    public void testCreateTabletMetaBuilderMaterializesLegacyBloomFilterWithExplicitFpp() throws Exception {
        CloudInternalCatalog catalog = new CloudInternalCatalog();
        CloudTablet tablet = new CloudTablet(100L);
        tablet.addReplica(new CloudReplica(101L, 1L, org.apache.doris.catalog.Replica.ReplicaState.NORMAL,
                1L, 1, 1L, 1L, 1L, 1L, 0L), true);
        Column keyColumn = new Column("k1", ScalarType.createType(PrimitiveType.INT), true, null, "1", "");
        Column valueColumn = new Column("v1", ScalarType.createType(PrimitiveType.INT), false,
                AggregateType.NONE, "1", "");
        Set<String> legacyBloomFilterColumns = new HashSet<>();
        legacyBloomFilterColumns.add("k1");

        OlapFile.TabletMetaCloudPB tabletMeta = catalog.createTabletMetaBuilder(1L, 1L, 1L, tablet,
                TTabletType.TABLET_TYPE_DISK, 1, KeysType.DUP_KEYS, (short) 1, legacyBloomFilterColumns, 0.02,
                Lists.newArrayList(), Lists.newArrayList(keyColumn, valueColumn),
                new DataSortInfo(TSortType.LEXICAL, 1), TCompressionType.LZ4F, TStorageFormat.V2,
                "", false, false, "tbl", 0L, false, false, 1, "", 0L, 0L, 0L, 0L, 0L,
                false, null, TInvertedIndexFileStorageFormat.V2, 16384L, false, null, 65536L,
                EncryptionAlgorithmPB.PLAINTEXT, 262144L, false, Collections.emptyMap(), 5).build();

        Assert.assertTrue(tabletMeta.getSchema().hasBfFpp());
        Assert.assertEquals(0.02, tabletMeta.getSchema().getBfFpp(), 0);
        Assert.assertTrue(tabletMeta.getSchema().getColumn(0).getIsBfColumn());
        Assert.assertFalse(tabletMeta.getSchema().getColumn(1).getIsBfColumn());
    }

    @Test
    public void testCreateTabletMetaBuilderMaterializesShadowNamedBloomFilterWithIndexes() throws Exception {
        // Shadow schema columns lose the FE-only shadow prefix when serialized, but the named BF
        // index still matches on the non-shadow column name and keeps its index property.
        CloudInternalCatalog catalog = new CloudInternalCatalog();
        CloudTablet tablet = new CloudTablet(100L);
        tablet.addReplica(new CloudReplica(101L, 1L, org.apache.doris.catalog.Replica.ReplicaState.NORMAL,
                1L, 1, 1L, 1L, 1L, 1L, 0L), true);
        Column shadowKeyColumn = new Column(Column.SHADOW_NAME_PREFIX + "k1",
                ScalarType.createType(PrimitiveType.INT), true, null, "1", "");
        Column valueColumn = new Column("v1", ScalarType.createType(PrimitiveType.INT), false,
                AggregateType.NONE, "1", "");
        OlapFile.TabletMetaCloudPB tabletMeta = catalog.createTabletMetaBuilder(1L, 2L, 1L, tablet,
                TTabletType.TABLET_TYPE_DISK, 1, KeysType.DUP_KEYS, (short) 1, null, 0,
                Lists.newArrayList(new Index(1L, "bf_k1", Lists.newArrayList("k1"),
                        org.apache.doris.catalog.info.IndexType.BLOOMFILTER,
                        Map.of("bloom_filter_fpp", "0.03"), "")),
                Lists.newArrayList(shadowKeyColumn, valueColumn),
                new DataSortInfo(TSortType.LEXICAL, 1), TCompressionType.LZ4F, TStorageFormat.V2,
                "", false, true, "tbl", 0L, false, false, 1, "", 0L, 0L, 0L, 0L, 0L,
                false, null, TInvertedIndexFileStorageFormat.V2, 16384L, false, null, 65536L,
                EncryptionAlgorithmPB.PLAINTEXT, 262144L, false, Collections.emptyMap(), 5).build();

        Assert.assertFalse(tabletMeta.getSchema().hasBfFpp());
        Assert.assertTrue(tabletMeta.getSchema().getColumn(0).getIsBfColumn());
        Assert.assertFalse(tabletMeta.getSchema().getColumn(1).getIsBfColumn());
        Assert.assertEquals("k1", tabletMeta.getSchema().getColumn(0).getName());
        Assert.assertEquals("0.03", tabletMeta.getSchema().getIndex(0).getPropertiesMap().get("bloom_filter_fpp"));
    }
}
