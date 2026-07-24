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

import org.apache.doris.alter.AlterJobV2.JobState;
import org.apache.doris.catalog.CatalogTestUtil;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FakeEditLog;
import org.apache.doris.catalog.FakeEnv;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.MaterializedIndex.IndexState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.cloud.alter.CloudSchemaChangeHandler;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.io.Text;
import org.apache.doris.thrift.TInvertedIndexFileStorageFormat;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

public class CloudSchemaChangeJobV2Test {

    @Test
    public void testAssignPartitionBaseSchemaVersionsDoesNotMergeSameFormatVersions() throws Exception {
        FakeEnv.setMetaVersion(FeMetaVersion.VERSION_CURRENT);
        FakeEditLog fakeEditLog = new FakeEditLog();
        FakeEnv fakeEnv = new FakeEnv();
        try {
            Env env = CatalogTestUtil.createTestCatalog();
            Database db = env.getInternalCatalog().getDbOrMetaException(CatalogTestUtil.testDbId1);
            OlapTable table = (OlapTable) db.getTableOrMetaException(CatalogTestUtil.testTableId1);
            long baseIndexId = table.getBaseIndexId();
            table.getIndexMetaByIndexId(baseIndexId).setSchemaVersion(3);
            table.setPartitionInvertedIndexFileStorageFormat(TInvertedIndexFileStorageFormat.V2);

            Partition partition1 = table.getPartition(CatalogTestUtil.testPartitionId1);
            partition1.setSchemaVersion(baseIndexId, 1);
            table.getPartitionInfo().setInvertedIndexFileStorageFormat(partition1.getId(),
                    TInvertedIndexFileStorageFormat.V2);

            Partition partition2 = new Partition(101, "p2",
                    new MaterializedIndex(baseIndexId, IndexState.NORMAL), table.getDefaultDistributionInfo());
            partition2.setSchemaVersion(baseIndexId, 2);
            table.addPartition(partition2);
            table.getPartitionInfo().setInvertedIndexFileStorageFormat(partition2.getId(),
                    TInvertedIndexFileStorageFormat.V2);

            Partition partition3 = new Partition(102, "p3",
                    new MaterializedIndex(baseIndexId, IndexState.NORMAL), table.getDefaultDistributionInfo());
            partition3.setSchemaVersion(baseIndexId, 3);
            table.addPartition(partition3);
            table.getPartitionInfo().setInvertedIndexFileStorageFormat(partition3.getId(),
                    TInvertedIndexFileStorageFormat.V2);

            CloudSchemaChangeJobV2 job = new CloudSchemaChangeJobV2("", 1, db.getId(), table.getId(),
                    table.getName(), 1000);
            job.addIndexSchema(100, baseIndexId, "__doris_shadow_testIndex1", 4,
                    table.getSchemaHashByIndexId(baseIndexId),
                    table.getIndexMetaByIndexId(baseIndexId).getShortKeyColumnCount(),
                    table.getSchemaByIndexId(baseIndexId));

            Method method = CloudSchemaChangeHandler.class.getDeclaredMethod("assignPartitionBaseSchemaVersions",
                    CloudSchemaChangeJobV2.class, OlapTable.class);
            method.setAccessible(true);
            method.invoke(new CloudSchemaChangeHandler(), job, table);

            Assert.assertEquals(Map.of(partition1.getId(), 4, partition2.getId(), 5, partition3.getId(), 6),
                    job.getPartitionIdToBaseSchemaVersion());
        } finally {
            fakeEnv.close();
            fakeEditLog.close();
        }
    }

    @Test
    public void testPartitionBaseSchemaVersionsSurviveJobSerialization() throws Exception {
        CloudSchemaChangeJobV2 job = new CloudSchemaChangeJobV2("", 1, 2, 3, "tbl", 4);
        Map<Long, Integer> partitionIdToBaseSchemaVersion = new HashMap<>();
        partitionIdToBaseSchemaVersion.put(100L, 12);
        partitionIdToBaseSchemaVersion.put(101L, 13);
        job.setPartitionIdToBaseSchemaVersion(partitionIdToBaseSchemaVersion);

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            job.write(output);
        }

        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            String json = Text.readString(input);
            Assert.assertTrue(json.contains("\"psv\""));
            Assert.assertFalse(json.contains("\"iifsv\""));
        }

        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            CloudSchemaChangeJobV2 replayedJob = (CloudSchemaChangeJobV2) AlterJobV2.read(input);
            Assert.assertEquals(partitionIdToBaseSchemaVersion,
                    replayedJob.getPartitionIdToBaseSchemaVersion());
        }
    }

    @Test
    public void testReplayUsesPartitionBaseSchemaVersion() throws Exception {
        FakeEnv.setMetaVersion(FeMetaVersion.VERSION_CURRENT);
        FakeEditLog fakeEditLog = new FakeEditLog();
        FakeEnv fakeEnv = new FakeEnv();
        try {
            Env env = CatalogTestUtil.createTestCatalog();
            Database db = env.getInternalCatalog().getDbOrMetaException(CatalogTestUtil.testDbId1);
            OlapTable table = (OlapTable) db.getTableOrMetaException(CatalogTestUtil.testTableId1);
            Partition partition = table.getPartition(CatalogTestUtil.testPartitionId1);
            long shadowIndexId = 100;

            CloudSchemaChangeJobV2 job = new CloudSchemaChangeJobV2("", 1, db.getId(), table.getId(),
                    table.getName(), 1000);
            job.addIndexSchema(shadowIndexId, table.getBaseIndexId(), "__doris_shadow_testIndex1", 13,
                    table.getSchemaHashByIndexId(table.getBaseIndexId()),
                    table.getIndexMetaByIndexId(table.getBaseIndexId()).getShortKeyColumnCount(),
                    table.getSchemaByIndexId(table.getBaseIndexId()));
            job.addPartitionShadowIndex(partition.getId(), shadowIndexId,
                    new MaterializedIndex(shadowIndexId, IndexState.SHADOW));
            job.setPartitionIdToBaseSchemaVersion(Map.of(partition.getId(), 12));
            job.setJobState(JobState.WAITING_TXN);

            AlterJobV2 replayedJob = readJob(job);
            replayedJob.replay(replayedJob);

            Assert.assertEquals(JobState.WAITING_TXN, replayedJob.getJobState());
            Assert.assertEquals(1, partition.getMaterializedIndices(IndexExtState.SHADOW).size());
            Assert.assertEquals(12, partition.getSchemaVersion(shadowIndexId, -1));
        } finally {
            fakeEnv.close();
            fakeEditLog.close();
        }
    }

    private AlterJobV2 readJob(AlterJobV2 job) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            job.write(output);
        }
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            return AlterJobV2.read(input);
        }
    }
}
