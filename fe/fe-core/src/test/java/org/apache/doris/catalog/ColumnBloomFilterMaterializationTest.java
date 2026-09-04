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

import org.apache.doris.catalog.info.IndexType;
import org.apache.doris.common.DdlException;
import org.apache.doris.proto.OlapFile;
import org.apache.doris.thrift.TColumn;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Set;

public class ColumnBloomFilterMaterializationTest {

    @Test
    public void testSetIndexFlagMarksBfIndexOnShadowColumn() {
        OlapTable olapTable = new OlapTable();
        olapTable.setIndexes(Lists.newArrayList(new Index(1L, "bf_k1", Lists.newArrayList("k1"),
                IndexType.BLOOMFILTER, null, "")));

        Column shadowColumn = new Column(Column.SHADOW_NAME_PREFIX + "k1", ScalarType.createType(PrimitiveType.INT),
                true, null, "1", "");
        TColumn tColumn = ColumnToThrift.toThrift(shadowColumn);
        ColumnToThrift.setIndexFlag(tColumn, olapTable);

        Assert.assertEquals("k1", tColumn.getColumnName());
        Assert.assertTrue(tColumn.isIsBloomFilterColumn());
    }

    @Test
    public void testSetIndexFlagFallsBackToBfIndexMetadataWhenCopiedBfColumnsUnavailable() {
        OlapTable olapTable = Mockito.mock(OlapTable.class);
        Mockito.when(olapTable.getCopiedBfColumns()).thenReturn(null);
        Mockito.when(olapTable.getIndexes()).thenReturn(Lists.newArrayList(
                new Index(1L, "bf_k1", Lists.newArrayList("k1"), IndexType.BLOOMFILTER, null, "")));

        Column shadowColumn = new Column(Column.SHADOW_NAME_PREFIX + "k1", ScalarType.createType(PrimitiveType.INT),
                true, null, "1", "");
        TColumn tColumn = ColumnToThrift.toThrift(shadowColumn);
        ColumnToThrift.setIndexFlag(tColumn, olapTable);

        Assert.assertEquals("k1", tColumn.getColumnName());
        Assert.assertTrue(tColumn.isIsBloomFilterColumn());
    }

    @Test
    public void testColumnToProtobufMarksBfIndexOnShadowColumn() throws DdlException {
        Column shadowColumn = new Column(Column.SHADOW_NAME_PREFIX + "k1", ScalarType.createType(PrimitiveType.INT),
                true, null, "1", "");

        OlapFile.ColumnPB columnPb = ColumnToProtobuf.toPb(shadowColumn, null,
                Lists.newArrayList(new Index(1L, "bf_k1", Lists.newArrayList("k1"),
                        IndexType.BLOOMFILTER, null, "")));

        Assert.assertEquals("k1", columnPb.getName());
        Assert.assertTrue(columnPb.getIsBfColumn());
    }

    @Test
    public void testColumnToProtobufMarksBfColumnsWithoutBfIndexes() throws DdlException {
        // The protobuf builder should also work when only the effective BF column set is passed
        // down and there is no BfIndex metadata at all.
        Column shadowColumn = new Column(Column.SHADOW_NAME_PREFIX + "k1", ScalarType.createType(PrimitiveType.INT),
                true, null, "1", "");

        OlapFile.ColumnPB columnPb = ColumnToProtobuf.toPb(shadowColumn, Sets.newHashSet("k1"),
                Lists.newArrayList());

        Assert.assertEquals("k1", columnPb.getName());
        Assert.assertTrue(columnPb.getIsBfColumn());
    }

    @Test
    public void testOlapTableBloomFilterColumnGetters() {
        OlapTable olapTable = new OlapTable();
        olapTable.setBloomFilterInfo(Sets.newHashSet("k1"), 0.05);
        olapTable.setIndexes(Lists.newArrayList(new Index(1L, "bf_v1", Lists.newArrayList("v1"),
                IndexType.BLOOMFILTER, null, "")));

        Set<String> bfColumns = olapTable.getCopiedBfColumns();
        Set<String> bfIndexColumns = Index.getBfIndexColumns(olapTable.getIndexes());

        Assert.assertEquals(Sets.newHashSet("k1"), bfColumns);
        Assert.assertEquals(Sets.newHashSet("v1"), bfIndexColumns);
    }

    @Test
    public void testIndexBloomFilterHelpersIgnoreNonBloomFilterIndexesAndHandleNulls() {
        Set<String> emptyBfIndexColumns = Index.getBfIndexColumns(null);
        Assert.assertTrue(emptyBfIndexColumns.isEmpty());

        Set<String> bfIndexColumns = Index.getBfIndexColumns(Lists.newArrayList(
                new Index(1L, "bf_v1", Lists.newArrayList("v1"), IndexType.BLOOMFILTER, null, ""),
                new Index(2L, "bitmap_k1", Lists.newArrayList("k1"), IndexType.BITMAP, null, ""),
                new Index(3L, "bf_v2", Lists.newArrayList("V2"), IndexType.BLOOMFILTER, null, "")));

        Assert.assertEquals(Sets.newHashSet("v1", "V2"), bfIndexColumns);
        Assert.assertTrue(Index.getBfIndexColumns(Lists.newArrayList(
                new Index(1L, "bf_v2", Lists.newArrayList("V2"), IndexType.BLOOMFILTER, null, ""))).contains("v2"));
        Assert.assertFalse(Index.getBfIndexColumns(Lists.newArrayList(
                new Index(1L, "bitmap_k1", Lists.newArrayList("k1"), IndexType.BITMAP, null, ""))).contains("k1"));
    }

}
