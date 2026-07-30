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
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.proto.OlapFile;
import org.apache.doris.thrift.TCompressionType;
import org.apache.doris.thrift.TInvertedIndexFileStorageFormat;
import org.apache.doris.thrift.TSortType;
import org.apache.doris.thrift.TStorageFormat;
import org.apache.doris.thrift.TTabletType;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;

public class CloudInternalCatalogTest {
    @Test
    public void testCreateTabletMetaUsesCurrentSchemaVersionAndFormat() throws Exception {
        Tablet tablet = Mockito.mock(Tablet.class);
        Replica replica = Mockito.mock(Replica.class);
        Mockito.when(tablet.getId()).thenReturn(100L);
        Mockito.when(tablet.getReplicas()).thenReturn(Collections.singletonList(replica));
        Mockito.when(replica.getId()).thenReturn(200L);

        OlapFile.TabletMetaCloudPB tabletMeta = new CloudInternalCatalog().createTabletMetaBuilder(
                1L, 2L, 3L, tablet, TTabletType.TABLET_TYPE_DISK, 23, KeysType.DUP_KEYS, (short) 1,
                Collections.emptySet(), 0.05, Collections.emptyList(), Collections.emptyList(),
                new DataSortInfo(TSortType.LEXICAL, 0), TCompressionType.LZ4F, TStorageFormat.V2,
                "", false, false, "table", 0L, false, false, 17, "size_based",
                0L, 0L, 0L, 0L, 0L, false, Collections.emptyList(),
                TInvertedIndexFileStorageFormat.V3, 0L, false, Collections.emptyList(), 0L,
                null, 0L, false, Collections.emptyMap(), 1).build();

        Assert.assertEquals(17, tabletMeta.getSchemaVersion());
        Assert.assertEquals(17, tabletMeta.getSchema().getSchemaVersion());
        Assert.assertEquals(OlapFile.InvertedIndexStorageFormatPB.V3,
                tabletMeta.getSchema().getInvertedIndexStorageFormat());
    }
}
