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

package org.apache.doris.alter;

import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.thrift.TInvertedIndexFileStorageFormat;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Map;

public class CloudSchemaChangeJobV2Test {
    private static final long BASE_INDEX_ID = 100L;
    private static final long SHADOW_INDEX_ID = 101L;
    private static final long V2_PARTITION_ID = 200L;
    private static final long V3_PARTITION_ID = 201L;

    @Test
    public void testBaseShadowVersionsFollowPartitionFormats() throws Exception {
        CloudSchemaChangeJobV2 job = newJob(V2_PARTITION_ID, V3_PARTITION_ID);
        OlapTable table = tableWithBaseSchemaVersion(2, TInvertedIndexFileStorageFormat.V3,
                Map.of(V2_PARTITION_ID, TInvertedIndexFileStorageFormat.V2,
                        V3_PARTITION_ID, TInvertedIndexFileStorageFormat.V3));

        job.configureBaseShadowSchemaVersion(table);

        Assert.assertEquals(Map.of(TInvertedIndexFileStorageFormat.V2, 3,
                        TInvertedIndexFileStorageFormat.V3, 4),
                job.getBaseShadowSchemaVersionByFormat());
        Assert.assertEquals(4,
                job.indexSchemaVersionAndHashMap.get(SHADOW_INDEX_ID).schemaVersion);
    }

    @Test
    public void testCurrentFormatGetsSchemaVersionWithoutLivePartition() throws Exception {
        CloudSchemaChangeJobV2 job = newJob(V2_PARTITION_ID);
        OlapTable table = tableWithBaseSchemaVersion(2, TInvertedIndexFileStorageFormat.V3,
                Map.of(V2_PARTITION_ID, TInvertedIndexFileStorageFormat.V2));

        job.configureBaseShadowSchemaVersion(table);

        Assert.assertEquals(Map.of(TInvertedIndexFileStorageFormat.V2, 3,
                        TInvertedIndexFileStorageFormat.V3, 4),
                job.getBaseShadowSchemaVersionByFormat());
    }

    @Test
    public void testRollupOnlySchemaChangeDoesNotAllocateBaseShadowVersions() throws Exception {
        CloudSchemaChangeJobV2 job = new CloudSchemaChangeJobV2("", 1L, 2L, 3L, "tbl", 4L);
        job.addIndexSchema(SHADOW_INDEX_ID, BASE_INDEX_ID + 1, "__doris_shadow_rollup", 3, 4,
                (short) 1, null);
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(table.getBaseIndexId()).thenReturn(BASE_INDEX_ID);

        job.configureBaseShadowSchemaVersion(table);

        Assert.assertTrue(job.getBaseShadowSchemaVersionByFormat().isEmpty());
    }

    @Test
    public void testBaseShadowVersionPlanSurvivesJobSerialization() throws Exception {
        CloudSchemaChangeJobV2 job = newJob();
        Map<TInvertedIndexFileStorageFormat, Integer> shadowVersions = Map.of(
                TInvertedIndexFileStorageFormat.V2, 3,
                TInvertedIndexFileStorageFormat.V3, 4);
        job.setBaseShadowSchemaVersionByFormat(shadowVersions);

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            job.write(output);
        }

        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            CloudSchemaChangeJobV2 replayedJob = (CloudSchemaChangeJobV2) AlterJobV2.read(input);
            Assert.assertEquals(shadowVersions, replayedJob.getBaseShadowSchemaVersionByFormat());
        }
    }

    private CloudSchemaChangeJobV2 newJob(long... partitionIds) {
        CloudSchemaChangeJobV2 job = new CloudSchemaChangeJobV2("", 1L, 2L, 3L, "tbl", 4L);
        job.addIndexSchema(SHADOW_INDEX_ID, BASE_INDEX_ID, "__doris_shadow_tbl", 3, 4, (short) 1, null);
        for (long partitionId : partitionIds) {
            job.addPartitionShadowIndex(partitionId, SHADOW_INDEX_ID, Mockito.mock(MaterializedIndex.class));
        }
        return job;
    }

    private OlapTable tableWithBaseSchemaVersion(int schemaVersion,
            TInvertedIndexFileStorageFormat currentFormat,
            Map<Long, TInvertedIndexFileStorageFormat> partitionFormats) {
        OlapTable table = Mockito.mock(OlapTable.class);
        MaterializedIndexMeta baseIndexMeta = Mockito.mock(MaterializedIndexMeta.class);
        PartitionInfo partitionInfo = Mockito.mock(PartitionInfo.class);
        Mockito.when(table.getBaseIndexId()).thenReturn(BASE_INDEX_ID);
        Mockito.when(table.getIndexMetaByIndexId(BASE_INDEX_ID)).thenReturn(baseIndexMeta);
        Mockito.when(baseIndexMeta.getSchemaVersion()).thenReturn(schemaVersion);
        Mockito.when(table.getPartitionInvertedIndexFileStorageFormat()).thenReturn(currentFormat);
        Mockito.when(table.getPartitionInfo()).thenReturn(partitionInfo);
        Mockito.when(table.getInvertedIndexFileStorageFormatForPartition(Mockito.anyLong()))
                .thenAnswer(invocation -> partitionFormats.get(invocation.getArgument(0)));
        return table;
    }
}
